"""Coordinator continuations admit byte-reading phases before invoking legacy work."""

# Retained legacy checkpoints are seeded directly; no old handler is dispatched.
# pyright: reportPrivateUsage=false

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest

from carl._tests.test_artifact_read_bounds import _publish
from carl._tests.test_stage_execution import _COMPONENT, _admit, _record
from carl._tests.test_worker import _requester
from carl.coordinator_stages import (
    COORDINATOR_CPU_STAGE_KIND,
    COORDINATOR_SELECT_STAGE_KIND,
    build_coordinator_stage_registry,
)
from carl.core.analysis_batch import REQUEST_MISSING_ANALYSES_WORK_KIND
from carl.core.components import Component
from carl.core.ebay_refresh import REFRESH_EBAY_SEARCH_WORK_KIND
from carl.core.facebook_listing import REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND
from carl.core.facebook_refresh import REFRESH_SEARCH_WORK_KIND
from carl.core.json import encode_json
from carl.core.models import BytesDraft, JsonValue, NamedInput, NamedOutput
from carl.core.pipeline import LISTING_PIPELINE_WORK_KIND, SEARCH_PIPELINE_WORK_KIND
from carl.core.resources import ResourceRequest
from carl.core.stages import StageDefinition, StageWaitKind, WaitWork
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkCapability, WorkDefinition
from carl.core.worker import AttemptContext, CompletedWork, RetryWork, WorkOutcome
from carl.io.image_reuse import image_artifact_available
from carl.io.resource_permits import ResourceManager
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import StageManager, stage_manager
from carl.io.worker import WorkHandlerRegistry

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@dataclass
class _Handler:
    capability: WorkCapability
    action: Callable[[JsonValue, AttemptContext], Awaitable[WorkOutcome]]
    component: Component = _COMPONENT

    async def execute(self, payload: JsonValue, context: AttemptContext) -> WorkOutcome:
        return await self.action(payload, context)


async def _seed(
    database: Database,
    registry: StageHandlerRegistry,
    capability: WorkCapability,
    *,
    result: JsonValue = None,
    payload: JsonValue = None,
) -> StageDefinition:
    identifier = str(uuid4())
    work = WorkDefinition(
        identifier=identifier,
        kind=capability.kind,
        payload_schema_version=capability.payload_schema_version,
        payload={} if payload is None else payload,
        deduplication_identity=(identifier,),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=capability.kind),
        ),
    )
    _ = await database.enqueue_work(
        work, _requester(identifier), event_identifier=str(uuid4()), enqueued_at_utc_ns=time_ns()
    )
    async with database._connections.writer() as connection:
        await connection.execute(
            "UPDATE work_items SET result_json=? WHERE id=?", (encode_json(result), identifier)
        )
    adapter = registry.require_adapter(capability)
    assert adapter.initial_resources == ()
    stage = adapter.initial_stage(work, str(uuid4()), str(uuid4()))
    await database.register_stage(stage, created_at_utc_ns=time_ns())
    return stage


async def _run(registry: StageHandlerRegistry, manager: StageManager, stage: StageDefinition):
    async with _admit(manager, stage) as admission:
        assert admission.execution is not None
        outcome = await registry.run(admission.execution)
        await admission.execution.apply(outcome)
        return outcome


@pytest.mark.parametrize(
    ("kind", "stage", "cpu"),
    (
        (REFRESH_SEARCH_WORK_KIND, None, False),
        (REFRESH_SEARCH_WORK_KIND, "collecting_search", False),
        (REFRESH_SEARCH_WORK_KIND, "search_failed", False),
        (REFRESH_SEARCH_WORK_KIND, "search_complete", True),
        (REFRESH_SEARCH_WORK_KIND, "collecting_items", True),
        (REFRESH_SEARCH_WORK_KIND, "extracting_items", True),
        (REFRESH_SEARCH_WORK_KIND, "items_complete", True),
        (REFRESH_SEARCH_WORK_KIND, "extracting_resumed_images", True),
        (REFRESH_SEARCH_WORK_KIND, "collecting_images", True),
        (REFRESH_SEARCH_WORK_KIND, "extracting_images", True),
        (REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND, "collecting_items", False),
        (REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND, "extracting_items", False),
        (REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND, "items_complete", True),
        (REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND, "images_planned", True),
        (REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND, "collecting_images", True),
        (REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND, "extracting_images", True),
        (REFRESH_EBAY_SEARCH_WORK_KIND, "collecting_search", False),
        (REFRESH_EBAY_SEARCH_WORK_KIND, "search_complete", False),
        (REFRESH_EBAY_SEARCH_WORK_KIND, "collecting_items", False),
        (REFRESH_EBAY_SEARCH_WORK_KIND, "extracting_items", False),
        (REFRESH_EBAY_SEARCH_WORK_KIND, "items_complete", True),
        (REFRESH_EBAY_SEARCH_WORK_KIND, "collecting_images", True),
        (REQUEST_MISSING_ANALYSES_WORK_KIND, "requesting", True),
        (REQUEST_MISSING_ANALYSES_WORK_KIND, "waiting_for_analyses", True),
        (SEARCH_PIPELINE_WORK_KIND, "processing", False),
        (LISTING_PIPELINE_WORK_KIND, "collecting_details", True),
        (LISTING_PIPELINE_WORK_KIND, "extracting_images", True),
        (LISTING_PIPELINE_WORK_KIND, "analyzing", True),
    ),
)
async def test_selector_preserves_checkpoint_and_yields_before_byte_reading_branch(
    tmp_path: Path, kind: tuple[str, ...], stage: str | None, cpu: bool
) -> None:
    calls: list[JsonValue] = []

    async def action(payload: JsonValue, _context: AttemptContext) -> WorkOutcome:
        calls.append(payload)
        return CompletedWork(result={"completed": True})

    capability = WorkCapability(kind=kind, payload_schema_version=1)
    checkpoint: JsonValue = None if stage is None else {"stage": stage, "retained": [1, "exact"]}
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        handler = _Handler(capability, action)
        registry = build_coordinator_stage_registry(database, WorkHandlerRegistry((handler,)))
        assert registry.adapters[0].component is handler.component
        initial = await _seed(database, registry, capability, result=checkpoint)
        outcome = await _run(registry, stage_manager(database), initial)
        if cpu:
            assert calls == [] and isinstance(outcome, WaitWork)
            assert outcome.continuation.kind == COORDINATOR_CPU_STAGE_KIND
            assert outcome.continuation.resources == (ResourceRequest(name="cpu"),)
            assert outcome.result == checkpoint
            assert (await database.work_progress((initial.work_item_identifier,)))[0][
                "result"
            ] == checkpoint
        else:
            assert calls == [{}] and isinstance(outcome, CompletedWork)


async def test_exhausted_facebook_item_image_budget_does_not_require_cpu(tmp_path: Path) -> None:
    async def action(_payload: JsonValue, _context: AttemptContext) -> WorkOutcome:
        return CompletedWork(result={"metadata": True})

    capability = WorkCapability(kind=REFRESH_SEARCH_WORK_KIND, payload_schema_version=2)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_coordinator_stage_registry(
            database, WorkHandlerRegistry((_Handler(capability, action),))
        )
        stage = await _seed(
            database,
            registry,
            capability,
            payload={"maximum_images": 3},
            result={"stage": "collecting_items", "prior_image_collection_count": 3},
        )
        assert isinstance(await _run(registry, stage_manager(database), stage), CompletedWork)


async def test_cpu_scarcity_never_runs_validator_or_blocks_metadata_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[AttemptContext] = []
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        _ = await _publish(database, content=b"retained image", external=False)

        async def image_action(_payload: JsonValue, context: AttemptContext) -> WorkOutcome:
            calls.append(context)
            assert await image_artifact_available(database, "body")
            return CompletedWork(result={"state": "saved"})

        async def metadata_action(_payload: JsonValue, _context: AttemptContext) -> WorkOutcome:
            return CompletedWork(result={"metadata_progress": True})

        image = WorkCapability(
            kind=REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND, payload_schema_version=1
        )
        metadata = WorkCapability(kind=SEARCH_PIPELINE_WORK_KIND, payload_schema_version=1)
        registry = build_coordinator_stage_registry(
            database,
            WorkHandlerRegistry(
                (_Handler(image, image_action), _Handler(metadata, metadata_action))
            ),
        )
        stage = await _seed(database, registry, image, result={"stage": "images_planned"})
        other = await _seed(database, registry, metadata)
        manager = stage_manager(database)
        async with manager.resources.admit(ResourceRequest(name="cpu")):
            outcome = await _run(registry, manager, stage)
            assert isinstance(outcome, WaitWork)
            async with _admit(manager, outcome.continuation) as admission:
                assert admission.execution is None
                assert admission.result.wait_kind is StageWaitKind.RESOURCE
            assert not calls
            assert isinstance(await _run(registry, manager, other), CompletedWork)

        def no_second_admission(*_args: object, **_kwargs: object):
            raise AssertionError("Image reuse helper tried to acquire another CPU permit")

        monkeypatch.setattr(ResourceManager, "admit", no_second_admission)
        assert isinstance(await _run(registry, manager, outcome.continuation), CompletedWork)
        assert len(calls) == 1 and calls[0].attempt == 1
        assert calls[0].operation_identifier == stage.operation_identifier


async def test_large_retained_payload_survives_restart_without_copying_into_stage(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    payload: JsonValue = {"observation_ids": ["observation-" + str(i) for i in range(6000)]}
    calls: list[AttemptContext] = []

    async def action(value: JsonValue, context: AttemptContext) -> WorkOutcome:
        assert value == payload
        calls.append(context)
        return CompletedWork(result={"complete": True})

    capability = WorkCapability(kind=REQUEST_MISSING_ANALYSES_WORK_KIND, payload_schema_version=4)
    handler = _Handler(capability, action)
    async with Database.managed(path, initialize=True) as database:
        registry = build_coordinator_stage_registry(database, WorkHandlerRegistry((handler,)))
        stage = await _seed(
            database, registry, capability, payload=payload, result={"next_observation_index": 37}
        )
        assert len(encode_json(payload)) > 65_536
        assert len(stage.model_dump_json()) < 1024
        outcome = await _run(registry, stage_manager(database), stage)
        assert isinstance(outcome, WaitWork)
        continuation = outcome.continuation
    async with Database.managed(path) as database:
        registry = build_coordinator_stage_registry(database, WorkHandlerRegistry((handler,)))
        assert isinstance(
            await _run(registry, stage_manager(database), continuation), CompletedWork
        )
        work = await database.work(stage.work_item_identifier)
        assert work["attempt"] == 1 and len(work["operations"]) == 1
        assert calls[0].operation_identifier == stage.operation_identifier


@pytest.mark.parametrize("known", (False, True))
@pytest.mark.parametrize(
    ("kind", "control_reason"),
    (
        (REFRESH_SEARCH_WORK_KIND, "waiting_for_item_collections"),
        (REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND, "waiting_for_image_extractions"),
        (REFRESH_EBAY_SEARCH_WORK_KIND, "waiting_for_item_descriptions"),
        (REQUEST_MISSING_ANALYSES_WORK_KIND, "waiting_for_listing_analyses"),
        (SEARCH_PIPELINE_WORK_KIND, "pipeline_continuation"),
        (LISTING_PIPELINE_WORK_KIND, "listing_pipeline_continuation"),
    ),
)
async def test_only_control_plane_retry_becomes_wait_and_preserves_evidence_and_delay(
    tmp_path: Path, known: bool, kind: tuple[str, ...], control_reason: str
) -> None:
    record = _record()
    artifact = BytesDraft(
        identifier=str(uuid4()),
        kind=("test", "bytes"),
        media_type="application/octet-stream",
        representation={},
        content=b"evidence",
    )
    retry = RetryWork(
        inputs=(NamedInput(name=("source",), object_identifier="body"),),
        records=(record,),
        artifacts=(artifact,),
        outputs=(
            NamedOutput(name=("checkpoint",), object_identifier=record.identifier),
            NamedOutput(name=("bytes",), object_identifier=artifact.identifier),
        ),
        delay_ns=123_456_789,
        reason={"kind": control_reason if known else "http_transport_failure"},
        result={"stage": "processing", "cursor": 51},
    )

    async def action(_payload: JsonValue, _context: AttemptContext) -> WorkOutcome:
        return retry

    capability = WorkCapability(kind=kind, payload_schema_version=1)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _ = await _publish(database, content=b"source", external=False)
        clock = time_ns()
        registry = build_coordinator_stage_registry(
            database, WorkHandlerRegistry((_Handler(capability, action),)), utc_now_ns=lambda: clock
        )
        stage = await _seed(database, registry, capability)
        outcome = await _run(registry, stage_manager(database), stage)
        if isinstance(outcome, WaitWork) and outcome.wait_kind is StageWaitKind.RESOURCE:
            outcome = await _run(registry, stage_manager(database), outcome.continuation)
        if known:
            assert isinstance(outcome, WaitWork)
            assert outcome.continuation.kind == COORDINATOR_SELECT_STAGE_KIND
            assert outcome.continuation.resources == ()
            assert outcome.continuation.not_before_utc_ns == clock + retry.delay_ns
            assert outcome.inputs == retry.inputs and outcome.outputs == retry.outputs
            assert outcome.records == retry.records and outcome.artifacts == retry.artifacts
            assert outcome.result == retry.result
            assert (await database.operation(stage.operation_identifier))["state"] == "started"
        else:
            assert outcome is retry
        assert (await database.get_record(record.identifier))[2] == record.value
        assert (await database.get_artifact(artifact.identifier))[1] == b"evidence"


async def test_cpu_handler_refuses_stage_without_cpu_allocation(tmp_path: Path) -> None:
    calls: list[AttemptContext] = []

    async def action(_payload: JsonValue, context: AttemptContext) -> WorkOutcome:
        calls.append(context)
        return CompletedWork(result={})

    capability = WorkCapability(kind=REQUEST_MISSING_ANALYSES_WORK_KIND, payload_schema_version=1)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_coordinator_stage_registry(
            database, WorkHandlerRegistry((_Handler(capability, action),))
        )
        stage = await _seed(database, registry, capability)
        malformed = stage.model_copy(update={"kind": COORDINATOR_CPU_STAGE_KIND})
        # A caller can register its own definition. The CPU handler must refuse
        # this shape instead of allowing its legacy helper to acquire inline.
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_stages SET definition_json=? WHERE id=?",
                (malformed.model_dump_json(), stage.identifier),
            )
        with pytest.raises(ValueError, match="requires its admitted CPU stage"):
            _ = await _run(registry, stage_manager(database), malformed)
        assert not calls


async def test_acquisition_handler_is_not_adapted_or_retry_reclassified(tmp_path: Path) -> None:
    async def action(_payload: JsonValue, _context: AttemptContext) -> WorkOutcome:
        return RetryWork(delay_ns=0, reason={"kind": "waiting_for_search_collection"}, result={})

    capability = WorkCapability(
        kind=("carl", "facebook", "work", "collect_image"), payload_schema_version=1
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_coordinator_stage_registry(
            database, WorkHandlerRegistry((_Handler(capability, action),))
        )
        assert registry.adapters == ()
