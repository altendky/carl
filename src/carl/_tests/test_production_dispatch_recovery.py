"""The real worker runtime preserves paid checkpoints and fences cancelled executors."""

# Exercise committed checkpoints, resource ownership and heartbeat revocation offline.
# pyright: reportPrivateUsage=false

import hashlib
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from time import time_ns
from typing import final, override

import anyio
import pytest

from carl import work_runtime
from carl._tests.test_facebook_images import _png, _provenance, _reference
from carl._tests.test_facebook_stateless_stages import _Capture, _registry, _seed, _Sessions
from carl.core.http import RequestPlan
from carl.core.models import CodeProvenance, JsonValue, NamedOutput, RecordDraft
from carl.core.resources import ResourceRequest
from carl.core.stages import StageAttemptContext, StageDefinition, StageLease
from carl.core.work import WorkCancellationRequestedError
from carl.ebay_item_workers import EbayItemWorkerDependencies
from carl.facebook_stateless_stages import FacebookStatelessStageDependencies
from carl.io.httpx import Acquisition, IdentifierFactory
from carl.io.paths import CarlDirectories
from carl.io.sqlite import Database, LeaseLostError
from carl.io.stage_publication import StageHandlerResult
from carl.io.stage_workers import StageHandler, StageHandlerRegistry
from carl.io.stages import StageManager, stage_manager
from carl.stateless_stages import build_stateless_stage_registry

pytestmark = pytest.mark.anyio
_PUBLISH = ("carl", "facebook", "stage", "image", "publish")


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _directories(root: Path) -> CarlDirectories:
    return CarlDirectories(
        config=root / "config",
        data=root,
        cache=root / "cache",
        state=root / "state",
        runtime=root / "runtime",
    )


def _install(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    sessions: _Sessions,
    transform: Callable[[StageHandlerRegistry], StageHandlerRegistry] | None = None,
) -> None:
    original = build_stateless_stage_registry

    def build(
        *, facebook: FacebookStatelessStageDependencies, ebay: EbayItemWorkerDependencies
    ) -> StageHandlerRegistry:
        result = original(facebook=replace(facebook, session_factory=sessions), ebay=ebay)
        return result if transform is None else transform(result)

    async def provenance(_root: Path) -> CodeProvenance:
        return _provenance()

    def fast_manager(database: Database) -> StageManager:
        return replace(stage_manager(database), renewal_interval_ns=20_000_000)

    monkeypatch.setattr(work_runtime, "build_stateless_stage_registry", build)
    monkeypatch.setattr(work_runtime, "user_directories", lambda: _directories(root))
    monkeypatch.setattr(work_runtime, "collect_code_provenance_async", provenance)
    monkeypatch.setattr(work_runtime, "stage_manager", fast_manager)


async def _terminal(database: Database, identifier: str) -> dict[str, JsonValue]:
    with anyio.fail_after(15):
        while True:
            work = (await database.work_progress((identifier,)))[0]
            if work["state"] not in {"pending", "leased", "cancelling"}:
                return work
            await anyio.sleep(0.01)


async def _held(database: Database, name: str) -> int:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                "SELECT coalesce(sum(units),0) FROM resource_allocations "
                + "JOIN resource_permits ON token=permit_token WHERE name=? AND state='held'",
                (name,),
            )
        ).fetchall()
    return int(str(rows[0][0]))


async def _pending_publish(database: Database, initial: StageDefinition) -> StageDefinition:
    with anyio.fail_after(10):
        while True:
            stage = await database.active_stage_for_work(
                initial.work_item_identifier, initial.operation_identifier
            )
            if stage is not None and stage.kind == _PUBLISH:
                return stage
            await anyio.sleep(0.01)


@final
class _GatedCapture(_Capture):
    def __init__(self) -> None:
        super().__init__()
        self.entered, self.release = anyio.Event(), anyio.Event()

    @override
    async def capture(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        self.entered.set()
        await self.release.wait()
        assert self.captures == 0, "A retained paid capture was dispatched again"
        return await super().capture(plan, new_identifier)


async def test_runtime_stop_database_restart_preserves_paid_checkpoint_without_execution_permits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "carl.sqlite3"
    capture = _GatedCapture()
    sessions = _Sessions(capture)
    _install(monkeypatch, tmp_path, sessions)
    async with Database.managed(
        path, initialize=True, resource_capacities={"cpu": 1, "http": 1}
    ) as database:
        initial, _ = await _seed(
            database, _registry(database, tmp_path, capture, sessions=sessions)
        )
        manager = stage_manager(database)
        held, release, released = anyio.Event(), anyio.Event(), anyio.Event()

        async def reserve_cpu() -> None:
            async with manager.resources.admit(ResourceRequest(name="cpu")):
                held.set()
                await release.wait()
            released.set()

        async with anyio.create_task_group() as holders:
            try:
                async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
                    with anyio.fail_after(10):
                        await capture.entered.wait()
                        _ = holders.start_soon(reserve_cpu)
                        await held.wait()
                    capture.release.set()
                    publish = await _pending_publish(database, initial)
                    assert capture.captures == 1 and capture.acquires == 0
                    assert len(sessions.sessions) == 1 and sessions.sessions[0].closed
                    assert await _held(database, "http") == 0
                    assert await _held(database, "cpu") == 1  # Only the external holder.
                    async with database._connections.reader() as connection:
                        assert await (
                            await connection.execute(
                                "SELECT count(*) FROM work_stages WHERE state='running'"
                            )
                        ).fetchall() == [(0,)]
                        assert await (
                            await connection.execute("SELECT count(*) FROM content")
                        ).fetchall() == [(0,)]
                    checkpoint = publish.payload["checkpoint_identifier"]
                    kind, version, value = await database.get_record(checkpoint)
                    assert kind == ("carl", "http", "stage_acquisition") and version == 1
                    assert value["body_identifiers"]
                    metadata, body = await database.get_artifact(value["body_identifiers"][0])
                    assert body and metadata["storage"]["backend"] == "filesystem"
                # Shutdown joins execution tasks without consuming this waiting
                # stage or requiring its saturated CPU permit to become free.
                assert (await database.work(initial.work_item_identifier))["attempt"] == 1
                assert await _held(database, "http") == 0
                assert (await database.stage(publish.identifier)) == publish
            finally:
                release.set()
            with anyio.fail_after(5):
                await released.wait()
    async with Database.managed(path, resource_capacities={"cpu": 1, "http": 1}) as database:
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            work = await _terminal(database, initial.work_item_identifier)
            assert work["state"] == "completed"
            assert work["attempt"] == 1
            assert work["result"]["sha256"] == hashlib.sha256(_png()).hexdigest()
            assert (await database.operation(initial.operation_identifier))["state"] == "completed"
            assert capture.captures == 1 and capture.acquires == 0
            assert len(sessions.sessions) == 1
        assert await _held(database, "cpu") == await _held(database, "http") == 0


@dataclass
class _FailingHandler:
    kind: tuple[str, ...]
    schema_version: int
    original: StageHandler
    work_identifier: str

    async def execute(self, payload: JsonValue, context: StageAttemptContext) -> StageHandlerResult:
        if context.work_item_identifier == self.work_identifier:
            raise ExceptionGroup(
                "offline handler failure", [ValueError("first"), RuntimeError("second")]
            )
        return await self.original.execute(payload, context)


async def test_handler_exception_group_is_terminal_and_runtime_processes_next_paid_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture = _Capture()
    sessions = _Sessions(capture)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, capture, sessions=sessions)
        failed, _ = await _seed(database, registry)
        reference = _reference(listing_id="456", observation="different-observation").model_copy(
            update={
                "photo_id": "other",
                "original_url": _reference().original_url + "&fixture=other",
            }
        )
        following, _ = await _seed(database, registry, reference=reference)

        def transform(value: StageHandlerRegistry) -> StageHandlerRegistry:
            return StageHandlerRegistry(
                value.adapters,
                tuple(
                    _FailingHandler(
                        handler.kind, handler.schema_version, handler, failed.work_item_identifier
                    )
                    for handler in value.handlers
                ),
            )

        _install(monkeypatch, tmp_path, sessions, transform)
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            bad = await _terminal(database, failed.work_item_identifier)
            good = await _terminal(database, following.work_item_identifier)
            assert bad["state"] == "terminal_failure"
            assert bad["error"]["kind"] == "unhandled_handler_error"
            assert bad["error"]["type"] == "ExceptionGroup"
            assert good["state"] == "completed"
            assert capture.captures == 1
        assert all(session.closed for session in sessions.sessions)
        assert await _held(database, "cpu") == await _held(database, "http") == 0


@final
class _RevokedCapture(_Capture):
    def __init__(self, database: Database, url: str) -> None:
        super().__init__()
        self.database, self.url = database, url
        self.entered, self.cleaned, self.rejected = anyio.Event(), anyio.Event(), anyio.Event()
        self.calls: list[str] = []
        self.lease: StageLease | None = None

    @override
    async def capture(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        self.calls.append(plan.url)
        if plan.url != self.url:
            return await super().capture(plan, new_identifier)
        self.entered.set()
        try:
            await anyio.sleep_forever()
        finally:
            with anyio.CancelScope(shield=True):
                lease = self.lease
                assert lease is not None
                with pytest.raises((LeaseLostError, WorkCancellationRequestedError)):
                    await self.database.publish_leased_operation_checkpoint(
                        work_item_identifier=lease.definition.work_item_identifier,
                        lease_token=lease.token,
                        worker_identifier=lease.work_lease.worker_identifier,
                        utc_now_ns=time_ns,
                        operation_id=lease.definition.operation_identifier,
                        records=(
                            RecordDraft(
                                identifier="stale-publication",
                                kind=("test", "stale"),
                                schema_version=1,
                                value={"stale": True},
                            ),
                        ),
                        artifacts=(),
                        outputs=(
                            NamedOutput(name=("stale",), object_identifier="stale-publication"),
                        ),
                        checkpoint_result={"stale": True},
                    )
                self.rejected.set()
                self.cleaned.set()
        raise AssertionError("Revoked capture was allowed to finish")


async def test_heartbeat_revocation_cancels_capture_fences_publication_and_keeps_runtime_alive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"http": 1, "cpu": 1}
    ) as database:
        reference = _reference()
        capture = _RevokedCapture(database, reference.original_url)
        sessions = _Sessions(capture)
        registry = _registry(database, tmp_path, capture, sessions=sessions)
        cancelled, _ = await _seed(database, registry, reference=reference)
        following, _ = await _seed(
            database,
            registry,
            reference=_reference(listing_id="987", observation="after-revocation").model_copy(
                update={
                    "photo_id": "after",
                    "original_url": reference.original_url + "&fixture=after",
                }
            ),
        )
        renew = database.renew_stage
        revoked = anyio.Event()

        async def revoke_once(
            lease: StageLease, *, now_utc_ns: int, lease_duration_ns: int
        ) -> StageLease | None:
            if (
                lease.definition.work_item_identifier == cancelled.work_item_identifier
                and capture.entered.is_set()
                and not revoked.is_set()
            ):
                capture.lease = lease
                _ = await database.cancel_work(
                    cancelled.work_item_identifier,
                    reason="offline heartbeat revocation",
                    caller="test",
                    requested_at_utc_ns=now_utc_ns,
                )
                revoked.set()
                assert (
                    await renew(lease, now_utc_ns=now_utc_ns, lease_duration_ns=lease_duration_ns)
                    is None
                )
                return None
            return await renew(lease, now_utc_ns=now_utc_ns, lease_duration_ns=lease_duration_ns)

        monkeypatch.setattr(database, "renew_stage", revoke_once)
        _install(monkeypatch, tmp_path, sessions)
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            with anyio.fail_after(10):
                await revoked.wait()
                await capture.cleaned.wait()
            bad = await _terminal(database, cancelled.work_item_identifier)
            good = await _terminal(database, following.work_item_identifier)
            assert bad["state"] == "cancelled"
            assert good["state"] == "completed"
            assert capture.rejected.is_set()
            assert capture.calls.count(reference.original_url) == 1
            assert len(capture.calls) == 2
            with pytest.raises(KeyError):
                _ = await database.get_record("stale-publication")
        assert all(session.closed for session in sessions.sessions)
        assert await _held(database, "cpu") == await _held(database, "http") == 0
