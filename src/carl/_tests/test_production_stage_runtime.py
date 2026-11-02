"""Offline production dispatch crosses saturated resources without waiting tasks."""

# Reuse exact acquisition/evidence fixtures and inspect bounded ownership ledgers.
# pyright: reportPrivateUsage=false

from dataclasses import replace
from pathlib import Path
from time import time_ns
from uuid import uuid4

import anyio
import pytest

from carl import work_runtime
from carl._tests.test_ebay_item_workers import (
    _image_payload,
    _retain_records,
)
from carl._tests.test_ebay_item_workers import (
    _reference as ebay_reference,
)
from carl._tests.test_facebook_analysis_stages import _evidence
from carl._tests.test_facebook_images import _reference as facebook_reference
from carl._tests.test_facebook_stateless_stages import _Capture, _seed, _Sessions
from carl._tests.test_item_analysis import _fake_claude, _payload
from carl._tests.test_worker import _definition, _requester
from carl.core.ebay_items import collect_ebay_image_work
from carl.core.http import RequestPlan
from carl.core.item_analysis import analyze_item_work
from carl.core.models import JsonValue
from carl.core.pipeline import SEARCH_PIPELINE_WORK_KIND
from carl.core.resources import ResourceRequest
from carl.core.work import WorkDefinition
from carl.io.claude import ClaudeCli
from carl.io.httpx import Acquisition, IdentifierFactory
from carl.io.paths import CarlDirectories
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


class _BlockedCapture(_Capture):
    def __init__(self) -> None:
        super().__init__()
        self.release = anyio.Event()
        self.full = anyio.Event()
        self.active = 0
        self.maximum = 0
        self.calls = 0

    async def capture(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        self.active += 1
        self.calls += 1
        self.maximum = max(self.maximum, self.active)
        if self.active == 25:
            self.full.set()
        try:
            await self.release.wait()
            return await super().capture(plan, new_identifier)
        finally:
            self.active -= 1


async def _enqueue(database: Database, definition: WorkDefinition) -> None:
    await database.enqueue_work(
        definition,
        _requester(definition.identifier),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )


async def _terminal(database: Database, identifier: str) -> dict[str, JsonValue]:
    with anyio.fail_after(30):
        while True:
            row = (await database.work_progress((identifier,)))[0]
            if row["state"] not in {"pending", "leased", "cancelling"}:
                return row
            await anyio.sleep(0.02)


def _directories(root: Path) -> CarlDirectories:
    return CarlDirectories(
        config=root / "config",
        data=root,
        cache=root / "cache",
        state=root / "state",
        runtime=root / "runtime",
    )


async def test_actual_mixed_images_at_http25_allow_analysis_and_coordinator_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = _BlockedCapture()
    sessions = _Sessions(capture)
    original = work_runtime.build_stateless_stage_registry

    def build(
        *,
        facebook: work_runtime.FacebookStatelessStageDependencies,
        ebay: work_runtime.EbayItemWorkerDependencies,
    ) -> StageHandlerRegistry:
        return original(
            facebook=replace(facebook, session_factory=sessions),
            ebay=replace(ebay, image_acquirer=capture),
        )

    monkeypatch.setattr(work_runtime, "build_stateless_stage_registry", build)
    monkeypatch.setattr(work_runtime, "user_directories", lambda: _directories(tmp_path))
    executable = tmp_path / "fake-claude"
    _fake_claude(executable, expect_hard_link=True)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _evidence(database, tmp_path, external=False)
        # Seed actual versioned stage definitions before starting any workers.
        registry = original(
            facebook=work_runtime.FacebookStatelessStageDependencies(
                database,
                _directories(tmp_path),
                lambda: str(uuid4()),
                work_runtime.SharedProtonWireproxyManager(),
                session_factory=sessions,
            ),
            ebay=work_runtime.EbayItemWorkerDependencies(
                database,
                _directories(tmp_path),
                lambda: str(uuid4()),
                work_runtime.SharedProtonWireproxyManager(),
                image_acquirer=capture,
            ),
        )
        image_ids: list[str] = []
        for index in range(15):
            reference = facebook_reference(
                listing_id=str(1000 + index), observation=f"fb-observation-{index}"
            )
            reference = reference.model_copy(
                update={
                    "photo_id": f"runtime-photo-{index}",
                    "original_url": reference.original_url.replace(".jpg", f"_{index}.jpg")
                    + f"&fixture={index}",
                }
            )
            stage, _ = await _seed(database, registry, reference=reference)
            image_ids.append(stage.work_item_identifier)
            record = ebay_reference(
                f"ebay-reference-{index}",
                item=str(200000000000 + index),
                observation=f"ebay-observation-{index}",
            )
            record = record.model_copy(
                update={"value": {**record.value, "url": record.value["url"] + f"?fixture={index}"}}
            )
            await _retain_records(database, (record,), lambda: str(uuid4()))
            payload = _image_payload(
                record.identifier,
                item=str(200000000000 + index),
                observation=f"ebay-observation-{index}",
            )
            payload = payload.model_copy(update={"url": record.value["url"]})
            definition = collect_ebay_image_work(identifier=str(uuid4()), payload=payload)
            await _enqueue(database, definition)
            image_ids.append(definition.identifier)
        async with work_runtime.managed_worker_pool(
            database, tmp_path, claude=ClaudeCli(str(executable))
        ):
            with anyio.fail_after(30):
                await capture.full.wait()
            assert capture.active == 25
            analysis = analyze_item_work(identifier="independent-analysis", payload=_payload())
            await _enqueue(database, analysis)
            coordinator = _definition(
                identifier="independent-coordinator", kind=SEARCH_PIPELINE_WORK_KIND
            )
            await _enqueue(database, coordinator)
            analyzed = await _terminal(database, analysis.identifier)
            coordinated = await _terminal(database, coordinator.identifier)
            assert analyzed["state"] == "completed", analyzed
            # Strict payload rejection proves the real resource-free selector ran.
            assert coordinated["state"] == "terminal_failure"
            assert coordinated["error"]["kind"] == "unhandled_handler_error"
            assert capture.active == 25
            assert capture.maximum == 25
            calls = capture.calls
            capture.release.set()
            for identifier in image_ids:
                assert (await _terminal(database, identifier))["state"] == "completed"
            assert capture.calls == 30
            assert calls == 25
        assert capture.active == 0
        assert all(session.closed for session in sessions.sessions)


async def test_invalid_oversized_legacy_payload_is_terminal_and_does_not_stop_pool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(work_runtime, "user_directories", lambda: _directories(tmp_path))
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        invalid = _definition(
            identifier="oversized", kind=("carl", "facebook", "work", "collect_image")
        )
        invalid = invalid.model_copy(
            update={"payload": {"sensitive_fixture": "PRIVATE_INPUT" * 10_000}}
        )
        await _enqueue(database, invalid)
        async with work_runtime.managed_worker_pool(database, tmp_path):
            failed = await _terminal(database, invalid.identifier)
            assert failed["attempt"] == 1
            assert failed["error"]["kind"] == "invalid_initial_stage"
            assert "PRIVATE_INPUT" not in str(failed)
            following = _definition(identifier="following", kind=SEARCH_PIPELINE_WORK_KIND)
            await _enqueue(database, following)
            assert (await _terminal(database, following.identifier))["state"] == "terminal_failure"


async def test_ten_thousand_cpu_blocked_rows_have_fixed_tasks_and_no_payload_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(work_runtime, "user_directories", lambda: _directories(tmp_path))
    original = Database.dispatch_candidates
    scanned = anyio.Event()
    task_counts: list[int] = []
    snapshot_reads = 0

    async def discover(self: Database, **kwargs: JsonValue) -> JsonValue:
        page = await original(self, **kwargs)
        task_counts.append(len(anyio.get_running_tasks()))
        assert len(page.items) <= 32
        if page.items and not page.has_more:
            scanned.set()
        return page

    async def snapshot(self: Database, identifier: str) -> JsonValue:
        nonlocal snapshot_reads
        snapshot_reads += 1
        raise AssertionError(f"Blocked discovery loaded payload {identifier}")

    monkeypatch.setattr(Database, "dispatch_candidates", discover)
    monkeypatch.setattr(Database, "work_dispatch_snapshot", snapshot)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        template = _definition(
            identifier="template", kind=("carl", "facebook", "work", "extract_image")
        )
        await _enqueue(database, template)
        async with database._connections.writer() as connection:
            await connection.executemany(
                """INSERT INTO work_items(id,kind_parts_json,payload_schema_version,payload_json,
                deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns)
                SELECT ?,kind_parts_json,payload_schema_version,'{}',json_array(?),'pending',0,0,created_at_utc_ns
                FROM work_items WHERE id='template'""",
                [(f"queued-{index:05d}", f"queued-{index:05d}") for index in range(10_000)],
            )
        held, release = anyio.Event(), anyio.Event()

        async def hold_cpu() -> None:
            async with stage_manager(database).resources.admit(
                ResourceRequest(name="cpu", units=4)
            ):
                held.set()
                await release.wait()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(hold_cpu)
            await held.wait()
            try:
                async with work_runtime.managed_worker_pool(database, tmp_path):
                    with anyio.fail_after(30):
                        await scanned.wait()
                    assert snapshot_reads == 0
                    assert len(task_counts) >= 313
                    assert max(task_counts) < 80
                    assert max(task_counts) - min(task_counts) <= 2
                    occupied = await database.resource_occupancy(
                        stage_manager(database).resources.scope
                    )
                    assert occupied == {"cpu": 4}
            finally:
                release.set()


async def test_outage_probe_releases_execution_resources_and_review_survives_pool_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = _Capture(partial_failure=True)
    sessions = _Sessions(capture)
    original_build = work_runtime.build_stateless_stage_registry

    def build(
        *,
        facebook: work_runtime.FacebookStatelessStageDependencies,
        ebay: work_runtime.EbayItemWorkerDependencies,
    ) -> StageHandlerRegistry:
        return original_build(facebook=replace(facebook, session_factory=sessions), ebay=ebay)

    monkeypatch.setattr(work_runtime, "build_stateless_stage_registry", build)
    monkeypatch.setattr(work_runtime, "user_directories", lambda: _directories(tmp_path))
    probing = anyio.Event()
    release = anyio.Event()
    monitor_type = work_runtime.ConnectivityMonitor

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:

        async def probe() -> dict[str, JsonValue]:
            occupied = await database.resource_occupancy(stage_manager(database).resources.scope)
            assert occupied == {}
            probing.set()
            await release.wait()
            return {"reachable": False, "probes": []}

        monkeypatch.setattr(
            work_runtime, "ConnectivityMonitor", lambda *args: monitor_type(*args, probe=probe)
        )
        registry = build(
            facebook=work_runtime.FacebookStatelessStageDependencies(
                database,
                _directories(tmp_path),
                lambda: str(uuid4()),
                work_runtime.SharedProtonWireproxyManager(),
            ),
            ebay=work_runtime.EbayItemWorkerDependencies(
                database,
                _directories(tmp_path),
                lambda: str(uuid4()),
                work_runtime.SharedProtonWireproxyManager(),
            ),
        )
        initial, _ = await _seed(database, registry)
        async with work_runtime.managed_worker_pool(database, tmp_path):
            with anyio.fail_after(10):
                await probing.wait()
            progress = (await database.work_progress((initial.work_item_identifier,)))[0]
            assert progress["attempt"] == 1
            assert progress["state"] == "leased"
            assert await database.resource_occupancy(stage_manager(database).resources.scope) == {}
        # Stop during the probe, leaving its checkpoint and same operation durable.
        assert capture.captures == 1
        release.set()
        async with work_runtime.managed_worker_pool(database, tmp_path):
            with anyio.fail_after(10):
                while True:
                    progress = (await database.work_progress((initial.work_item_identifier,)))[0]
                    if progress["state"] == "pending":
                        break
                    await anyio.sleep(0.02)
            assert progress["attempt"] == 1
            assert progress["result"]["state"] == "waiting_for_connectivity"
            assert capture.captures == 1
            assert await database.resource_occupancy(stage_manager(database).resources.scope) == {}
