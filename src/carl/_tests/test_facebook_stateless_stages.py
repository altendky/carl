"""Facebook image stages retain captured bytes across offline scheduling waits."""

# Fixture seeding and bounded storage inspection exercise private integration boundaries.
# pyright: reportPrivateUsage=false

import gzip
import hashlib
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from time import time_ns
from typing import cast, final
from uuid import uuid4

import anyio
import pytest

import carl.facebook_stateless_stages as facebook_stages
from carl._tests.test_ebay_workers import _directories
from carl._tests.test_facebook import HTML
from carl._tests.test_facebook_images import _png, _provenance, _reference
from carl.core.components import Component, ComponentId
from carl.core.facebook_images import (
    CollectImagePayload,
    ExtractImagePayload,
    GalleryImageReference,
    collect_image_work,
    extract_image_work,
)
from carl.core.facebook_work import ExtractItemPayload, extract_item_work
from carl.core.http import RequestPlan
from carl.core.models import (
    ArtifactDraft,
    BytesDraft,
    ExternalFileDraft,
    JsonValue,
    NamedOutput,
    RecordDraft,
)
from carl.core.resources import ResourceRequest
from carl.core.stages import StageDefinition, StageOutcome, StageWaitKind, WaitWork
from carl.core.work import WorkCapability, WorkDefinition, WorkRequester
from carl.core.worker import CompletedWork, RetryWork, TerminalFailureWork
from carl.facebook_stateless_stages import (
    FacebookStatelessStageDependencies,
    ImageCaptureSession,
    build_facebook_stateless_stage_registry,
)
from carl.facebook_workers import EXTRACT_FACEBOOK
from carl.io.facebook_images import FacebookImageSessionFailure
from carl.io.http_checkpoints import HttpCheckpoint
from carl.io.httpx import (
    AcquiredBody,
    Acquisition,
    AcquisitionFailure,
    HttpCaptureAcquirer,
    IdentifierFactory,
)
from carl.io.image_files import ImageFileStore
from carl.io.proton import SharedProtonWireproxyManager
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, WorkStageAdapter
from carl.io.stages import StageManager, stage_manager

pytestmark = pytest.mark.anyio
_SEED_COMPONENT = Component(ComponentId(("test", "seed_gallery")), 1, lambda: None)


@dataclass
class _Clock:
    now: int

    def __call__(self) -> int:
        return self.now


@dataclass
class _Capture:
    native_decoded: bool = False
    partial_failure: bool = False
    response_close_failure: bool = False
    invalid_image: bool = False
    captures: int = 0
    acquires: int = 0

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        del plan, new_identifier
        self.acquires += 1
        raise AssertionError("A transport stage must capture before offline validation")

    async def capture(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        self.captures += 1
        image = b"not a PNG" if self.invalid_image else _png()
        content = image if self.native_decoded else gzip.compress(image)
        identifier = new_identifier()
        representation: dict[str, JsonValue] = {
            "kind": "content_decoded_http_body" if self.native_decoded else "http_message_content",
            "content_decoded": self.native_decoded,
            "content_encodings_removed": ["br"] if self.native_decoded else [],
            "exact_wire_bytes": not self.native_decoded,
        }
        acquisition = Acquisition(
            record={
                "effective_url": plan.url,
                "hops": [
                    {
                        "request": {"url": plan.url},
                        "response": {
                            "status_code": 200,
                            "headers": [
                                {"name_latin1": "Content-Type", "value_latin1": "image/png"},
                                {
                                    "name_latin1": "Content-Encoding",
                                    "value_latin1": "br" if self.native_decoded else "gzip",
                                },
                            ],
                            "body": {
                                "state": "available",
                                "artifact_id": identifier,
                                "bytes": len(content),
                                "received_content_bytes": len(content),
                            },
                        },
                    }
                ],
                "stopping_condition": "completed",
            },
            bodies=(AcquiredBody(identifier, "image/png", representation, content),),
        )
        if self.partial_failure or self.response_close_failure:
            acquisition.record["stopping_condition"] = "transport_failure"
            acquisition.record["failure_phase"] = (
                "response_close" if self.response_close_failure else "redirect_request"
            )
            raise AcquisitionFailure(
                "offline partial transport failure",
                result=acquisition.record,
                bodies=acquisition.bodies,
            )
        return acquisition


@dataclass
class _Session:
    acquirer: HttpCaptureAcquirer
    closed: bool = False
    observations: int = 0

    def completed_observation(self) -> dict[str, JsonValue]:
        assert self.closed, "provider observation must describe an already-closed session"
        self.observations += 1
        return {"state": "released", "fixture": True}


@final
class _Sessions:
    def __init__(
        self,
        acquirer: _Capture,
        *,
        cleanup_failure: bool = False,
        cleanup_acquisition_failure: bool = False,
        unexpected_cleanup: str | None = None,
    ):
        self.acquirer = acquirer
        self.cleanup_failure = cleanup_failure
        self.cleanup_acquisition_failure = cleanup_acquisition_failure
        self.unexpected_cleanup = unexpected_cleanup
        self.sessions: list[_Session] = []

    @asynccontextmanager
    async def __call__(self, identifier: str) -> AsyncGenerator[ImageCaptureSession]:
        assert identifier
        session = _Session(self.acquirer)
        self.sessions.append(session)
        try:
            yield session
        finally:
            session.closed = True
        if self.cleanup_failure:
            raise FacebookImageSessionFailure("wireproxy_cleanup_failed", {"fixture": True})
        if self.unexpected_cleanup is not None:
            exception = RuntimeError if self.unexpected_cleanup == "runtime" else OSError
            raise exception("secret fixture /private/credential-file")
        if self.cleanup_acquisition_failure:
            # Decodo's manager raises this type with empty bodies after a
            # completed capture when closing its HTTP client fails.
            raise AcquisitionFailure(
                "fixture client cleanup failed",
                result={
                    "hops": [],
                    "stopping_condition": "transport_failure",
                    "failure_phase": "client_close",
                    "exception_type": "RuntimeError",
                },
            )


def _registry(
    database: Database,
    root: Path,
    acquirer: _Capture,
    *,
    clock: _Clock | None = None,
    sessions: Callable[[str], AbstractAsyncContextManager[ImageCaptureSession]] | None = None,
) -> StageHandlerRegistry:
    return build_facebook_stateless_stage_registry(
        FacebookStatelessStageDependencies(
            database=database,
            directories=_directories(root),
            new_identifier=lambda: str(uuid4()),
            proton_manager=SharedProtonWireproxyManager(),
            acquirer=None if sessions is not None else acquirer,
            session_factory=sessions,
            utc_now_ns=time_ns if clock is None else clock,
        )
    )


async def _store(
    database: Database,
    records: tuple[RecordDraft, ...],
    artifacts: tuple[ArtifactDraft, ...] = (),
) -> None:
    operation = str(uuid4())
    await database.begin_operation(
        operation_id=operation,
        component=_SEED_COMPONENT,
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-11-01T00:00:00+00:00",
    )
    await database.complete_operation(
        operation_id=operation,
        records=records,
        artifacts=artifacts,
        outputs=tuple(
            NamedOutput(name=("seed", str(index)), object_identifier=record.identifier)
            for index, record in enumerate(records)
        ),
        result={},
        ended_at_utc="2026-11-01T00:00:01+00:00",
        duration_ns=1_000_000_000,
    )


async def _register(
    database: Database, registry: StageHandlerRegistry, work: WorkDefinition
) -> tuple[StageDefinition, WorkStageAdapter]:
    _ = await database.enqueue_work(
        work,
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier=work.identifier, context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    adapter = registry.require_adapter(
        WorkCapability(kind=work.kind, payload_schema_version=work.payload_schema_version)
    )
    stage = adapter.initial_stage(work, str(uuid4()), str(uuid4()))
    await database.register_stage(stage, created_at_utc_ns=time_ns())
    return stage, adapter


async def _seed(
    database: Database,
    registry: StageHandlerRegistry,
    *,
    reference: GalleryImageReference | None = None,
) -> tuple[StageDefinition, WorkStageAdapter]:
    reference = _reference() if reference is None else reference
    identifier = str(uuid4())
    await _store(
        database,
        (
            RecordDraft(
                identifier=identifier,
                kind=("carl", "facebook", "gallery_image_reference"),
                schema_version=1,
                value=reference.model_dump(mode="json"),
            ),
        ),
    )
    payload = CollectImagePayload(
        reference_record_identifier=identifier,
        reference=reference,
        request_plan=RequestPlan(
            url=reference.original_url,
            follow_redirects=False,
            compression=("identity",),
            routing=("proton", "fixture", "images"),
        ),
    )
    return await _register(
        database,
        registry,
        collect_image_work(identifier=str(uuid4()), payload=payload, not_before_utc_ns=0),
    )


async def _run(
    registry: StageHandlerRegistry,
    manager: StageManager,
    stage: StageDefinition,
    adapter: WorkStageAdapter,
) -> StageOutcome:
    with anyio.fail_after(5):
        while True:
            async with manager.admit(
                stage.identifier,
                component=adapter.component,
                provenance=_provenance(),
                invocation={},
            ) as admission:
                if admission.execution is not None:
                    return await registry.execute(admission.execution)
                next_eligible = admission.result.next_eligible_at_utc_ns
            delay = (
                0.01
                if next_eligible is None
                else max(0.001, (next_eligible - time_ns()) / 1_000_000_000)
            )
            await anyio.sleep(min(delay, 0.1))


async def _capture_stage(
    database: Database, registry: StageHandlerRegistry
) -> tuple[StageDefinition, WorkStageAdapter]:
    first, adapter = await _seed(database, registry)
    prepared = await _run(registry, stage_manager(database), first, adapter)
    assert isinstance(prepared, WaitWork)
    return prepared.continuation, adapter


@pytest.mark.parametrize("native_decoded", [False, True])
async def test_captured_image_survives_cpu_wait_database_restart_and_provider_release(
    tmp_path: Path, native_decoded: bool
) -> None:
    path = tmp_path / "evidence.sqlite"
    capture = _Capture(native_decoded=native_decoded)
    sessions = _Sessions(capture)
    async with Database.managed(
        path, initialize=True, resource_capacities={"cpu": 1, "http": 1}
    ) as database:
        registry = _registry(database, tmp_path, capture, sessions=sessions)
        stage, adapter = await _capture_stage(database, registry)
        manager = stage_manager(database)
        async with manager.resources.admit(ResourceRequest(name="cpu")):
            captured = await _run(registry, manager, stage, adapter)
            assert isinstance(captured, WaitWork)
            assert sessions.sessions[0].closed and sessions.sessions[0].observations == 1
            assert capture.captures == 1 and capture.acquires == 0
            async with manager.admit(
                captured.continuation.identifier,
                component=adapter.component,
                provenance=_provenance(),
                invocation={},
            ) as blocked:
                assert blocked.execution is None
                assert blocked.result.wait_kind == StageWaitKind.RESOURCE
            async with database._connections.reader() as connection:
                cursor = await connection.execute(
                    "SELECT count(*) FROM resource_allocations JOIN resource_permits ON resource_permits.token=permit_token WHERE name='http' AND state='held'"
                )
                assert await cursor.fetchall() == [(0,)]
                cursor = await connection.execute("SELECT count(*) FROM content")
                assert await cursor.fetchall() == [(0,)]
            assert captured.artifacts and all(
                (tmp_path / artifact.locator).exists()
                for artifact in captured.artifacts
                if isinstance(artifact, ExternalFileDraft)
            )
            work_identifier = stage.work_item_identifier
            publish = captured.continuation
            operation = stage.operation_identifier
    async with Database.managed(path, resource_capacities={"cpu": 1, "http": 1}) as database:
        registry = _registry(database, tmp_path, capture, sessions=sessions)
        outcome = await _run(registry, stage_manager(database), publish, adapter)
        assert isinstance(outcome, CompletedWork)
        assert isinstance(outcome.result, dict)
        summary = cast(dict[str, JsonValue], outcome.result)
        assert summary["state"] == "saved"
        assert summary["sha256"] == hashlib.sha256(_png()).hexdigest()
        result_id = cast(str, summary["image_result_record_identifier"])
        _, _, result = await database.get_record(result_id)
        assert isinstance(result, dict)
        assert result["mime_type"] == "image/png"
        assert result["producer"] == {
            "component_parts": ["carl", "facebook", "collect", "gallery_image"],
            "output_schema_version": 4,
        }
        work = await database.work(work_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        assert len(cast(list[JsonValue], work["operations"])) == 1
        assert (await database.operation(operation))["state"] == "completed"
        assert capture.captures == 1 and capture.acquires == 0


async def test_expired_url_is_not_fetched_and_reuse_precedes_expiry_rejection(
    tmp_path: Path,
) -> None:
    capture = _Capture()
    clock = _Clock(time_ns())
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture, clock=clock)
        stage, adapter = await _capture_stage(database, registry)
        captured = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(captured, WaitWork)
        saved = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(saved, CompletedWork)
        clock.now = (int("FFFFFFFF", 16) + 1) * 1_000_000_000
        reuse_reference = _reference(listing_id="456", observation="observation-456")
        reuse_stage, reuse_adapter = await _seed(database, registry, reference=reuse_reference)
        reused = await _run(registry, stage_manager(database), reuse_stage, reuse_adapter)
        assert isinstance(reused, CompletedWork)
        assert isinstance(reused.result, dict) and "reused_image_result_record_identifier" in cast(
            dict[str, JsonValue], reused.result
        )
        expired = _reference(listing_id="789", observation="observation-789").model_copy(
            update={
                "original_url": _reference().original_url.replace("oe=FFFFFFFF", "oe=00000001"),
                "photo_id": "never-saved",
            }
        )
        expired_stage, expired_adapter = await _seed(database, registry, reference=expired)
        rejected = await _run(registry, stage_manager(database), expired_stage, expired_adapter)
        assert isinstance(rejected, TerminalFailureWork)
        assert (
            isinstance(rejected.result, dict)
            and cast(dict[str, JsonValue], rejected.result)["state"] == "gallery_stale"
        )
        assert capture.captures == 1


async def test_captured_response_remains_valid_after_signed_url_expires_during_cpu_wait(
    tmp_path: Path,
) -> None:
    capture = _Capture()
    clock = _Clock(time_ns())
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture, clock=clock)
        stage, adapter = await _capture_stage(database, registry)
        captured = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(captured, WaitWork)
        clock.now = (int("FFFFFFFF", 16) + 1) * 1_000_000_000
        outcome = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(outcome, CompletedWork)
        assert capture.captures == 1


async def test_second_image_consumer_waits_for_publication_then_reuses_one_capture(
    tmp_path: Path,
) -> None:
    capture = _Capture()
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"cpu": 1, "http": 1}
    ) as database:
        registry = _registry(database, tmp_path, capture)
        manager = stage_manager(database)
        first, first_adapter = await _capture_stage(database, registry)
        captured = await _run(registry, manager, first, first_adapter)
        assert isinstance(captured, WaitWork)
        # A renewed signature gives the consumer separate logical work while
        # identifying the same photo rendition and provider route.
        reference = _reference(listing_id="456", observation="observation-456").model_copy(
            update={
                "original_url": _reference().original_url.replace(
                    "oh=signed%2Bvalue", "oh=renewed-signature"
                ),
                "declared_width": 3,
                "declared_height": 2,
            }
        )
        second, second_adapter = await _seed(database, registry, reference=reference)
        assert first.work_item_identifier != second.work_item_identifier
        async with manager.admit(
            second.identifier,
            component=second_adapter.component,
            provenance=_provenance(),
            invocation={},
        ) as blocked:
            assert blocked.execution is None
            assert blocked.result.wait_kind == StageWaitKind.OWNERSHIP
        assert (await database.work(second.work_item_identifier))["attempt"] == 0
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT count(*) FROM resource_permits WHERE state='held'"
            )
            assert await cursor.fetchall() == [(0,)]
        saved = await _run(registry, manager, captured.continuation, first_adapter)
        assert isinstance(saved, CompletedWork)
        reused = await _run(registry, manager, second, second_adapter)
        assert isinstance(reused, CompletedWork)
        assert "reused_image_result_record_identifier" in cast(dict[str, JsonValue], reused.result)
        assert capture.captures == 1 and capture.acquires == 0


async def test_provider_startup_expiry_closes_session_without_capturing(tmp_path: Path) -> None:
    capture = _Capture()
    clock = _Clock(time_ns())
    sessions: list[_Session] = []

    @asynccontextmanager
    async def expired_session(identifier: str) -> AsyncGenerator[ImageCaptureSession]:
        assert identifier
        session = _Session(capture)
        sessions.append(session)
        clock.now = (int("FFFFFFFF", 16) + 1) * 1_000_000_000
        try:
            yield session
        finally:
            session.closed = True

    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture, clock=clock, sessions=expired_session)
        stage, adapter = await _capture_stage(database, registry)
        outcome = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(outcome, TerminalFailureWork)
        assert cast(dict[str, JsonValue], outcome.result)["state"] == "gallery_stale"
        assert capture.captures == 0 and len(sessions) == 1 and sessions[0].closed


@pytest.mark.parametrize("corruption", ["missing", "corrupt", "metadata"])
async def test_unusable_checkpoint_is_terminal_and_never_refetches(
    tmp_path: Path, corruption: str
) -> None:
    capture = _Capture()
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture)
        stage, adapter = await _capture_stage(database, registry)
        captured = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(captured, WaitWork)
        artifact = captured.artifacts[0]
        assert isinstance(artifact, ExternalFileDraft)
        path = tmp_path / artifact.locator
        if corruption == "missing":
            path.unlink()
        elif corruption == "corrupt":
            _ = path.write_bytes(b"corrupted checkpoint")
        else:
            identifier = captured.records[0].identifier
            await database.replace_record_value(
                identifier=identifier,
                expected_kind=("carl", "http", "stage_acquisition"),
                value={"acquisition": {}, "body_identifiers": ["missing-body"]},
            )
        outcome = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(outcome, TerminalFailureWork)
        assert (
            isinstance(outcome.error, dict)
            and cast(dict[str, JsonValue], outcome.error)["kind"] == "image_checkpoint_unavailable"
        )
        assert capture.captures == 1 and capture.acquires == 0
        assert (await database.work(stage.work_item_identifier))["state"] == "terminal_failure"


async def test_provider_teardown_failure_keeps_a_completed_response(tmp_path: Path) -> None:
    capture = _Capture()
    sessions = _Sessions(capture, cleanup_failure=True)
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture, sessions=sessions)
        stage, adapter = await _capture_stage(database, registry)
        captured = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(captured, WaitWork)
        value = cast(dict[str, JsonValue], captured.records[0].value)
        acquisition = cast(dict[str, JsonValue], value["acquisition"])
        assert "session_cleanup_failure" in acquisition
        outcome = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(outcome, CompletedWork)
        assert capture.captures == 1 and sessions.sessions[0].closed


@pytest.mark.parametrize("cleanup", ("runtime", "os"))
async def test_unexpected_provider_cleanup_preserves_paid_image_without_secret_diagnostics(
    tmp_path: Path,
    cleanup: str,
) -> None:
    capture = _Capture()
    sessions = _Sessions(capture, unexpected_cleanup=cleanup)
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture, sessions=sessions)
        stage, adapter = await _capture_stage(database, registry)
        captured = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(captured, WaitWork)
        diagnostic = captured.records[0].value["acquisition"]["session_cleanup_failure"]
        assert diagnostic == {"type": "RuntimeError" if cleanup == "runtime" else "OSError"}
        outcome = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(outcome, CompletedWork)
        assert capture.captures == 1 and sessions.sessions[0].closed


@pytest.mark.parametrize("valid_image", [False, True])
async def test_decodo_client_cleanup_failure_validates_completed_body_without_refetch(
    tmp_path: Path, valid_image: bool
) -> None:
    capture = _Capture(invalid_image=not valid_image)
    sessions = _Sessions(capture, cleanup_acquisition_failure=True)
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture, sessions=sessions)
        stage, adapter = await _capture_stage(database, registry)
        captured = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(captured, WaitWork)
        value = cast(dict[str, JsonValue], captured.records[0].value)
        acquisition = cast(dict[str, JsonValue], value["acquisition"])
        assert acquisition["stopping_condition"] == "completed"
        assert len(cast(list[JsonValue], acquisition["hops"])) == 1
        assert cast(dict[str, JsonValue], acquisition["session_cleanup_failure"]) == {
            "type": "AcquisitionFailure",
            "failure_phase": "client_close",
            "exception_type": "RuntimeError",
            "stopping_condition": "transport_failure",
        }
        outcome = await _run(registry, stage_manager(database), captured.continuation, adapter)
        if valid_image:
            assert isinstance(outcome, CompletedWork)
            assert (
                cast(dict[str, JsonValue], outcome.result)["sha256"]
                == hashlib.sha256(_png()).hexdigest()
            )
        else:
            assert isinstance(outcome, TerminalFailureWork)
            assert (
                cast(dict[str, JsonValue], outcome.error)["kind"]
                == "image_validation_or_storage_failure"
            )
        assert capture.captures == 1 and capture.acquires == 0
        assert sessions.sessions[0].closed
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == ("completed" if valid_image else "terminal_failure")
        assert work["attempt"] == 1


async def test_complete_response_close_failure_is_published_without_refetch(tmp_path: Path) -> None:
    capture = _Capture(response_close_failure=True)
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture)
        stage, adapter = await _capture_stage(database, registry)
        captured = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(captured, WaitWork)
        value = cast(dict[str, JsonValue], captured.records[0].value)
        acquisition = cast(dict[str, JsonValue], value["acquisition"])
        assert acquisition["collection_completeness"] == "complete"
        assert acquisition["stopping_condition"] == "terminal_response"
        assert (
            cast(dict[str, JsonValue], acquisition["session_cleanup_failure"])["failure_phase"]
            == "response_close"
        )
        outcome = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(outcome, CompletedWork)
        assert (
            cast(dict[str, JsonValue], outcome.result)["sha256"]
            == hashlib.sha256(_png()).hexdigest()
        )
        assert capture.captures == 1 and capture.acquires == 0
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1


async def test_partial_capture_failure_retains_external_evidence_with_retry(tmp_path: Path) -> None:
    capture = _Capture(partial_failure=True)
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture)
        stage, adapter = await _capture_stage(database, registry)
        outcome = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(outcome, RetryWork)
        assert outcome.records and outcome.artifacts
        checkpoint = next(
            record
            for record in outcome.records
            if record.kind == ("carl", "http", "stage_acquisition")
        )
        assert (await database.get_record(checkpoint.identifier))[2] == checkpoint.value
        async with database._connections.reader() as connection:
            cursor = await connection.execute("SELECT count(*) FROM content")
            assert await cursor.fetchall() == [(0,)]
        assert (await database.work(stage.work_item_identifier))["attempt"] == 1
        assert capture.captures == 1


@pytest.mark.parametrize("partial_failure", [False, True])
async def test_checkpoint_storage_failure_is_terminal_after_one_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, partial_failure: bool
) -> None:
    capture = _Capture(partial_failure=partial_failure)

    async def unavailable_checkpoint(
        acquisition: Acquisition, *, data_directory: Path, new_identifier: Callable[[], str]
    ) -> HttpCheckpoint:
        del acquisition, data_directory, new_identifier
        raise OSError("fixture storage is full")

    monkeypatch.setattr(facebook_stages, "checkpoint_acquisition", unavailable_checkpoint)
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture)
        stage, adapter = await _capture_stage(database, registry)
        outcome = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(outcome, TerminalFailureWork)
        assert (
            cast(dict[str, JsonValue], outcome.error)["kind"] == "image_checkpoint_storage_failure"
        )
        result = cast(dict[str, JsonValue], outcome.result)
        assert result["state"] == "checkpoint_failed"
        acquisition = cast(dict[str, JsonValue], result["acquisition"])
        assert isinstance(acquisition["network_activity_identifier"], str)
        assert (await database.work(stage.work_item_identifier))["state"] == "terminal_failure"
        async with stage_manager(database).admit(
            stage.identifier, component=adapter.component, provenance=_provenance(), invocation={}
        ) as completed:
            assert completed.execution is None
        assert capture.captures == 1 and capture.acquires == 0


async def test_cpu_image_storage_failure_retains_the_committed_raw_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture = _Capture()
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture)
        stage, adapter = await _capture_stage(database, registry)
        captured = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(captured, WaitWork)

        async def unavailable_image(
            self: ImageFileStore, *, content: bytes, media_type: str, sha256: str
        ) -> None:
            del self, content, media_type, sha256
            raise OSError("fixture image storage is full")

        monkeypatch.setattr(ImageFileStore, "publish", unavailable_image)
        outcome = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(outcome, TerminalFailureWork)
        assert (
            cast(dict[str, JsonValue], outcome.error)["kind"]
            == "image_validation_or_storage_failure"
        )
        checkpoint = captured.records[0]
        assert (await database.get_record(checkpoint.identifier))[2] == checkpoint.value
        artifact = captured.artifacts[0]
        assert isinstance(artifact, ExternalFileDraft)
        assert (tmp_path / artifact.locator).exists()
        assert (await database.work(stage.work_item_identifier))["state"] == "terminal_failure"
        assert capture.captures == 1


async def test_cancel_after_capture_preserves_checkpoint_without_refetch(tmp_path: Path) -> None:
    capture = _Capture()
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture)
        stage, adapter = await _capture_stage(database, registry)
        captured = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(captured, WaitWork)
        _ = await database.cancel_work(
            stage.work_item_identifier,
            reason="fixture cancellation",
            caller="test",
            requested_at_utc_ns=time_ns(),
        )
        checkpoint = captured.records[0]
        assert (await database.get_record(checkpoint.identifier))[2] == checkpoint.value
        assert (await database.work(stage.work_item_identifier))["state"] == "cancelled"
        assert capture.captures == 1
        assert (await database.operation(stage.operation_identifier))["result"] == {
            "state": "cancelled"
        }


async def test_standalone_item_extraction_stage_runs_real_offline_handler(tmp_path: Path) -> None:
    capture = _Capture()
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture)
        acquisition_id = str(uuid4())
        body_id = str(uuid4())
        await _store(
            database,
            (
                RecordDraft(
                    identifier=acquisition_id,
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value={
                        "requested_listing_id": "123",
                        "effective_url": "https://www.facebook.com/marketplace/item/123/",
                        "hops": [
                            {
                                "response": {
                                    "status_code": 200,
                                    "headers": [
                                        {
                                            "name_latin1": "Content-Type",
                                            "value_latin1": "text/html; charset=utf-8",
                                        }
                                    ],
                                    "body": {"state": "available", "artifact_id": body_id},
                                }
                            }
                        ],
                    },
                ),
            ),
            (
                BytesDraft(
                    identifier=body_id,
                    kind=("carl", "http", "response_body"),
                    media_type="text/html; charset=utf-8",
                    representation={"kind": "content_decoded_http_body", "content_decoded": True},
                    content=HTML.encode(),
                ),
            ),
        )
        work = extract_item_work(
            identifier=str(uuid4()),
            payload=ExtractItemPayload(acquisition_record_identifier=acquisition_id),
            extractor_identifier=EXTRACT_FACEBOOK.parts,
            extractor_schema_version=1,
            not_before_utc_ns=0,
        )
        stage, adapter = await _register(database, registry, work)
        assert stage.resources == (ResourceRequest(name="cpu"),)
        outcome = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(outcome, CompletedWork)
        observations = await database.records_by_kind(("carl", "facebook", "listing_observation"))
        assert len(observations) == 1
        assert isinstance(observations[0][1], dict) and observations[0][1]["listing_id"] == "123"
        assert capture.captures == capture.acquires == 0


async def test_standalone_image_extraction_stage_runs_real_offline_handler(tmp_path: Path) -> None:
    capture = _Capture()
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        registry = _registry(database, tmp_path, capture)
        stage, adapter = await _capture_stage(database, registry)
        captured = await _run(registry, stage_manager(database), stage, adapter)
        assert isinstance(captured, WaitWork)
        saved = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(saved, CompletedWork)
        image_result = next(
            record
            for record in saved.records
            if record.kind == ("carl", "facebook", "image_result")
        )
        value = cast(dict[str, JsonValue], image_result.value)
        reference_identifier = cast(str, value["image_reference_record_identifier"])
        _, _, reference = await database.get_record(reference_identifier)
        # Refresh-produced references retain extractor/operation provenance in
        # addition to the strict GalleryImageReference payload fields.
        await database.replace_record_value(
            identifier=reference_identifier,
            expected_kind=("carl", "facebook", "gallery_image_reference"),
            value={
                **cast(dict[str, JsonValue], reference),
                "reference_extractor": {
                    "component_parts": ["carl", "facebook", "extract", "gallery_references"],
                    "output_schema_version": 1,
                },
                "operation_identifier": str(uuid4()),
            },
        )
        work = extract_image_work(
            identifier=str(uuid4()),
            payload=ExtractImagePayload(
                acquisition_record_identifier=cast(str, value["acquisition_record_identifier"]),
                reference_record_identifier=cast(str, value["image_reference_record_identifier"]),
            ),
            not_before_utc_ns=0,
        )
        extraction_stage, extraction_adapter = await _register(database, registry, work)
        assert extraction_stage.resources == (ResourceRequest(name="cpu"),)
        extracted = await _run(
            registry, stage_manager(database), extraction_stage, extraction_adapter
        )
        assert isinstance(extracted, CompletedWork)
        assert (
            cast(dict[str, JsonValue], extracted.result)["sha256"]
            == hashlib.sha256(_png()).hexdigest()
        )
        assert capture.captures == 1 and capture.acquires == 0
