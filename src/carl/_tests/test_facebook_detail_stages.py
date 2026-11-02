"""Facebook detail capture keeps raw evidence and offline extraction boundaries."""

# Tests share bounded offline stage fixtures and inspect exact output ownership.
# pyright: reportPrivateUsage=false

import gzip
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest

import carl.facebook_stateless_stages as stages
from carl._tests.test_ebay_workers import _directories
from carl._tests.test_facebook import HTML
from carl._tests.test_facebook_stateless_stages import (
    _Capture,
    _capture_stage,
    _provenance,
    _register,
    _run,
)
from carl._tests.test_facebook_stateless_stages import (
    _registry as _image_registry,
)
from carl.core.facebook_work import CollectItemPayload, collect_item_work
from carl.core.http import RequestPlan
from carl.core.models import JsonValue
from carl.core.stages import StageDefinition, WaitWork
from carl.core.work import WorkCapability, WorkDefinition
from carl.core.worker import CompletedWork, RetryWork, TerminalFailureWork
from carl.facebook_stateless_stages import (
    FacebookStatelessStageDependencies,
    ImageCaptureSession,
    build_facebook_stateless_stage_registry,
)
from carl.io.facebook_images import FacebookImageSessionFailure
from carl.io.http_checkpoints import HttpCheckpoint
from carl.io.httpx import (
    AcquiredBody,
    Acquisition,
    AcquisitionFailure,
    HttpCaptureAcquirer,
    IdentifierFactory,
)
from carl.io.proton import SharedProtonWireproxyManager
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, WorkStageAdapter
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


@dataclass
class _DetailCapture:
    content: bytes = field(default_factory=lambda: HTML.encode())
    mode: str = "success"
    captures: int = 0

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        raise AssertionError("Detail stages must capture raw bodies before offline extraction")

    async def capture(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        self.captures += 1
        if self.mode == "transport":
            raise AcquisitionFailure(
                "synthetic transport failure", result={"stopping_condition": "transport_failure"}
            )
        identifier = new_identifier()
        content = gzip.compress(b"redirect prefix" if self.mode == "partial" else self.content)
        response: dict[str, JsonValue] = {
            "status_code": 302 if self.mode == "partial" else 200,
            "headers": [
                {"name_latin1": "Content-Type", "value_latin1": "text/html; charset=utf-8"},
                {"name_latin1": "Content-Encoding", "value_latin1": "gzip"},
            ],
            "body": {
                "state": "available",
                "artifact_id": identifier,
                "bytes": len(content),
                "received_content_bytes": len(content),
            },
        }
        hops = [{"request": {"url": plan.url}, "response": response}]
        if self.mode == "partial":
            hops.append(
                {
                    "request": {"url": plan.url + "redirect"},
                    "response": {"body": {"state": "unavailable"}},
                }
            )
        acquisition = Acquisition(
            record={
                "effective_url": plan.url,
                "hops": hops,
                "stopping_condition": "terminal_response",
            },
            bodies=(
                AcquiredBody(
                    identifier,
                    "text/html",
                    {
                        "kind": "http_message_content",
                        "content_decoded": False,
                        "exact_wire_bytes": False,
                    },
                    content,
                ),
            ),
        )
        if self.mode in {"partial", "response_close"}:
            acquisition.record.update(
                {
                    "stopping_condition": "transport_failure",
                    "failure_phase": "response_close"
                    if self.mode == "response_close"
                    else "redirect_request",
                }
            )
            raise AcquisitionFailure(
                "synthetic capture failure", result=acquisition.record, bodies=acquisition.bodies
            )
        return acquisition


@dataclass
class _Session:
    acquirer: HttpCaptureAcquirer
    closed: bool = False

    def completed_observation(self) -> dict[str, JsonValue]:
        assert self.closed
        return {"state": "closed"}


@dataclass
class _Sessions:
    capture: _DetailCapture
    sessions: list[_Session] = field(default_factory=list)
    cleanup: str = "provider"

    @asynccontextmanager
    async def __call__(self, identifier: str) -> AsyncIterator[ImageCaptureSession]:
        session = _Session(self.capture)
        self.sessions.append(session)
        try:
            yield session
        finally:
            session.closed = True
        if self.cleanup in {"runtime", "os"}:
            exception = RuntimeError if self.cleanup == "runtime" else OSError
            raise exception("secret fixture /private/credential-file")
        raise FacebookImageSessionFailure("synthetic_cleanup_failure", {"closed": True})


def _registry(
    database: Database, root: Path, capture: _DetailCapture, sessions: _Sessions | None = None
) -> StageHandlerRegistry:
    return build_facebook_stateless_stage_registry(
        FacebookStatelessStageDependencies(
            database=database,
            directories=_directories(root),
            new_identifier=lambda: str(uuid4()),
            proton_manager=SharedProtonWireproxyManager(),
            item_acquirer=capture if sessions is None else None,
            item_session_factory=sessions,
        )
    )


async def _seed(
    database: Database, registry: StageHandlerRegistry
) -> tuple[StageDefinition, WorkStageAdapter]:
    payload = CollectItemPayload(
        listing_id="123",
        request_plan=RequestPlan(
            url="https://www.facebook.com/marketplace/item/123/", routing=("fixture", "detail")
        ),
    )
    work = collect_item_work(identifier=str(uuid4()), payload=payload, not_before_utc_ns=0)
    # Exercise dispatch-time overlap reuse rather than canonical queue deduplication.
    work = work.model_copy(update={"deduplication_identity": (work.identifier,)})
    return await _register(database, registry, work)


async def _finish_capture(
    database: Database,
    registry: StageHandlerRegistry,
    first: StageDefinition,
    adapter: WorkStageAdapter,
):
    manager = stage_manager(database)
    prepared = await _run(registry, manager, first, adapter)
    if not isinstance(prepared, WaitWork):
        return prepared
    captured = await _run(registry, manager, prepared.continuation, adapter)
    if not isinstance(captured, WaitWork):
        return captured
    return await _run(registry, manager, captured.continuation, adapter)


async def _extract(database: Database, registry: StageHandlerRegistry, work: WorkDefinition):
    adapter = registry.require_adapter(
        WorkCapability(kind=work.kind, payload_schema_version=work.payload_schema_version)
    )
    stage = adapter.initial_stage(work, str(uuid4()), str(uuid4()))
    await database.register_stage(stage, created_at_utc_ns=time_ns())
    return await _run(registry, stage_manager(database), stage, adapter)


@pytest.mark.parametrize("login", (False, True))
async def test_raw_detail_checkpoint_survives_restart_and_enqueues_offline_extraction(
    tmp_path: Path, login: bool
) -> None:
    capture = _DetailCapture(
        content=b'<html><form id="login_form"></form></html>' if login else HTML.encode()
    )
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(
        path, initialize=True, resource_capacities={"http": 1, "cpu": 1}
    ) as database:
        registry = _registry(database, tmp_path, capture)
        first, adapter = await _seed(database, registry)
        manager = stage_manager(database)
        prepared = await _run(registry, manager, first, adapter)
        assert isinstance(prepared, WaitWork)
        async with manager.resources.admit(stages.ResourceRequest(name="cpu")):
            captured = await _run(registry, manager, prepared.continuation, adapter)
            assert isinstance(captured, WaitWork)
            assert capture.captures == 1
            async with manager.admit(
                captured.continuation.identifier,
                component=adapter.component,
                provenance=_provenance(),
                invocation={},
            ) as blocked:
                assert blocked.execution is None
        publish = captured.continuation
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT name_parts_json FROM operation_outputs WHERE operation_id=? ORDER BY name_parts_json",
                (first.operation_identifier,),
            )
            assert await cursor.fetchall() == [
                ('["response","0","body"]',),
                ('["stage_acquisition"]',),
            ]
            cursor = await connection.execute("SELECT count(*) FROM content")
            assert await cursor.fetchall() == [(0,)]
    async with Database.managed(path, resource_capacities={"http": 1, "cpu": 1}) as database:
        registry = _registry(database, tmp_path, capture)
        completed = await _run(registry, stage_manager(database), publish, adapter)
        assert isinstance(completed, CompletedWork)
        assert not completed.artifacts and len(completed.follow_on_work) == 1
        assert [output.name for output in completed.outputs] == [("acquisition",)]
        assert (await database.work(first.work_item_identifier))["state"] == "completed"
        extracted = await _extract(database, registry, completed.follow_on_work[0].definition)
        assert isinstance(extracted, TerminalFailureWork if login else CompletedWork)
        assert capture.captures == 1
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                'SELECT count(*) FROM operation_outputs WHERE operation_id=? AND name_parts_json=\'["response","0","body"]\'',
                (first.operation_identifier,),
            )
            assert await cursor.fetchall() == [(1,)]


async def test_overlapping_detail_reuses_usable_body_but_later_refresh_fetches_again(
    tmp_path: Path,
) -> None:
    capture = _DetailCapture()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, capture)
        first, adapter = await _seed(database, registry)
        overlap, overlap_adapter = await _seed(database, registry)
        completed = await _finish_capture(database, registry, first, adapter)
        assert isinstance(completed, CompletedWork)
        reused = await _run(registry, stage_manager(database), overlap, overlap_adapter)
        assert isinstance(reused, CompletedWork) and reused.result["reused_acquisition"] is True
        assert capture.captures == 1
        later, later_adapter = await _seed(database, registry)
        assert isinstance(
            await _finish_capture(database, registry, later, later_adapter), CompletedWork
        )
        assert capture.captures == 2


@pytest.mark.parametrize(
    "mode", ("partial", "response_close", "provider_close", "runtime_close", "os_close")
)
async def test_capture_failure_preserves_partial_or_completed_response(
    tmp_path: Path, mode: str
) -> None:
    capture = _DetailCapture(
        mode="success" if mode.endswith("_close") and mode != "response_close" else mode
    )
    sessions = (
        _Sessions(capture, cleanup=mode.removesuffix("_close"))
        if mode in {"provider_close", "runtime_close", "os_close"}
        else None
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, capture, sessions)
        first, adapter = await _seed(database, registry)
        outcome = await _finish_capture(database, registry, first, adapter)
        assert isinstance(outcome, RetryWork if mode == "partial" else CompletedWork)
        assert capture.captures == 1
        if sessions is not None:
            assert sessions.sessions[0].closed
        if mode in {"runtime_close", "os_close"}:
            _, _, value = await database.get_record(outcome.result["acquisition_record_identifier"])
            assert value["session_cleanup_failure"] == {
                "type": "RuntimeError" if mode == "runtime_close" else "OSError"
            }
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                'SELECT object_id FROM operation_outputs WHERE operation_id=? AND name_parts_json=\'["response","0","body"]\'',
                (first.operation_identifier,),
            )
            body = await cursor.fetchall()
        assert len(body) == 1 and isinstance(body[0][0], str)
        metadata, raw = await database.get_artifact(body[0][0])
        assert metadata["representation"]["content_decoded"] is False
        assert gzip.decompress(raw) == (b"redirect prefix" if mode == "partial" else HTML.encode())


async def test_detail_transport_retry_keeps_legacy_three_attempt_ceiling(tmp_path: Path) -> None:
    capture = _DetailCapture(mode="transport")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, capture)
        first, adapter = await _seed(database, registry)
        for attempt in range(1, 4):
            outcome = await _finish_capture(database, registry, first, adapter)
            assert isinstance(outcome, TerminalFailureWork if attempt == 3 else RetryWork)
            assert (await database.work(first.work_item_identifier))["attempt"] == attempt
            if attempt < 3:
                first = first.model_copy(
                    update={"identifier": str(uuid4()), "operation_identifier": str(uuid4())}
                )
                await database.register_stage(first, created_at_utc_ns=time_ns())
        assert capture.captures == 3


@pytest.mark.parametrize("detail", (False, True))
async def test_lost_paid_capture_cannot_repeat_in_same_logical_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, detail: bool
) -> None:
    class LostCapture(BaseException):
        pass

    async def lose_capture(
        acquisition: Acquisition, *, data_directory: Path, new_identifier: IdentifierFactory
    ) -> HttpCheckpoint:
        raise LostCapture()

    monkeypatch.setattr(stages, "checkpoint_acquisition", lose_capture)
    capture = _DetailCapture() if detail else _Capture()
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        if detail:
            assert isinstance(capture, _DetailCapture)
            registry = _registry(database, tmp_path, capture)
            first, adapter = await _seed(database, registry)
            prepared = await _run(registry, stage_manager(database), first, adapter)
            assert isinstance(prepared, WaitWork)
            capture_stage = prepared.continuation
        else:
            assert isinstance(capture, _Capture)
            registry = _image_registry(database, tmp_path, capture)
            capture_stage, adapter = await _capture_stage(database, registry)
        with pytest.raises(LostCapture):
            _ = await _run(registry, stage_manager(database), capture_stage, adapter)
        assert capture.captures == 1
    async with Database.managed(path) as database:
        if detail:
            assert isinstance(capture, _DetailCapture)
            registry = _registry(database, tmp_path, capture)
        else:
            assert isinstance(capture, _Capture)
            registry = _image_registry(database, tmp_path, capture)
        result = await _run(registry, stage_manager(database), capture_stage, adapter)
        assert isinstance(result, RetryWork)
        assert result.result["acquisition"]["failure_phase"] == "uncertain_previous_dispatch"
        assert (await database.work(capture_stage.work_item_identifier))["attempt"] == 1
        assert capture.captures == 1
