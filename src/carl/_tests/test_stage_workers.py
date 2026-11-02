"""Opt-in stage adapters retain typed payloads, evidence and retry semantics."""

# The retry fixture records the same durable budget-reset event as refresh recovery.
# pyright: reportPrivateUsage=false

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest
from pydantic import TypeAdapter, ValidationError

from carl._tests.test_stage_execution import _COMPONENT, _PROVENANCE, _admit, _record
from carl._tests.test_stages import _network
from carl.core.json import encode_json
from carl.core.models import BytesDraft, JsonValue, NamedInput, NamedOutput, StrictModel
from carl.core.resources import ResourceRequest
from carl.core.stages import (
    StageAttemptContext,
    StageDefinition,
    StageOutcome,
    StageWaitKind,
    WaitWork,
    initial_work_stage,
)
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.core.worker import CompletedWork, RetryWork, TerminalFailureWork
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler, WorkStageAdapter
from carl.io.stages import stage_manager

_WORK_KIND = ("test", "staged_work")
_STAGE_KIND = ("test", "initial_stage")
_RESUME_KIND = ("test", "resume_stage")


class _Payload(StrictModel):
    value: int


class _Envelope(StrictModel):
    work_kind: tuple[str, ...]
    payload_schema_version: int
    payload: _Payload


def _initial(work: WorkDefinition, identifier: str, operation: str) -> StageDefinition:
    return initial_work_stage(
        work,
        identifier=identifier,
        operation_identifier=operation,
        kind=_STAGE_KIND,
        resources=(ResourceRequest(name="cpu"),),
    )


def _adapter() -> WorkStageAdapter:
    return WorkStageAdapter(
        capability=WorkCapability(kind=_WORK_KIND, payload_schema_version=7),
        component=_COMPONENT,
        initial_stage=_initial,
    )


async def _completed(payload: _Envelope, context: StageAttemptContext) -> StageOutcome:
    return CompletedWork(result={"value": payload.payload.value, "attempt": context.attempt})


def _handler() -> TypedStageHandler[_Envelope]:
    return TypedStageHandler(
        kind=_STAGE_KIND, schema_version=1, payload_type=_Envelope, handler=_completed
    )


def _context() -> StageAttemptContext:
    return StageAttemptContext(
        work_item_identifier="work",
        lease_token="token",
        worker_identifier="worker",
        attempt=1,
        operation_identifier="operation",
        stage_identifier="stage",
        execution=2,
    )


async def _register(database: Database, *, network: bool = False) -> StageDefinition:
    identifier = str(uuid4())
    work = WorkDefinition(
        identifier=identifier,
        kind=_WORK_KIND,
        payload_schema_version=7,
        payload={"value": 3},
        deduplication_identity=(identifier,),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=_WORK_KIND),
        ),
    )
    _ = await database.enqueue_work(
        work,
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier=identifier, context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    adapter = _adapter()
    stage = adapter.initial_stage(work, str(uuid4()), str(uuid4()))
    if network:
        stage = stage.model_copy(
            update={
                "resources": (ResourceRequest(name="http"),),
                "network_activity": _network().model_copy(
                    update={"operation_identifier": stage.operation_identifier}
                ),
            }
        )
    await database.register_stage(stage, created_at_utc_ns=time_ns())
    return stage


@pytest.mark.anyio
@pytest.mark.parametrize("value", ("3", 3.0, True))
async def test_stage_handler_decodes_payload_strictly_before_calling_handler(
    value: JsonValue,
) -> None:
    payload = {
        "work_kind": list(_WORK_KIND),
        "payload_schema_version": 7,
        "payload": {"value": value},
    }
    with pytest.raises(ValidationError):
        _ = await _handler().execute(payload, _context())
    with pytest.raises(ValidationError):
        _ = await _handler().execute(
            {**payload, "payload": {"value": 3}, "unexpected": True}, _context()
        )


@pytest.mark.anyio
async def test_stage_handler_accepts_valid_json_payload() -> None:
    outcome = await _handler().execute(
        {"work_kind": list(_WORK_KIND), "payload_schema_version": 7, "payload": {"value": 3}},
        _context(),
    )
    assert isinstance(outcome, CompletedWork) and outcome.result == {"value": 3, "attempt": 1}


def test_registry_resolves_exact_work_and_stage_capabilities() -> None:
    adapter, handler = _adapter(), _handler()
    registry = StageHandlerRegistry(adapters=(adapter,), handlers=(handler,))
    assert registry.require_adapter(adapter.capability) is adapter
    stage = StageDefinition(
        identifier="stage",
        work_item_identifier="work",
        operation_identifier="operation",
        kind=_STAGE_KIND,
        payload={},
    )
    assert registry.require_stage(stage) is handler
    with pytest.raises(KeyError):
        _ = registry.require_adapter(WorkCapability(kind=_WORK_KIND, payload_schema_version=8))
    with pytest.raises(KeyError):
        _ = registry.require_stage(stage.model_copy(update={"kind": ("unknown",)}))
    with pytest.raises(ValueError, match="Duplicate work-stage"):
        _ = StageHandlerRegistry(adapters=(adapter, adapter), handlers=(handler,))
    with pytest.raises(ValueError, match="Duplicate stage-handler"):
        _ = StageHandlerRegistry(adapters=(adapter,), handlers=(handler, handler))
    other_version = replace(
        adapter, capability=WorkCapability(kind=_WORK_KIND, payload_schema_version=8)
    )
    versions = StageHandlerRegistry(
        adapters=(adapter, other_version), handlers=(handler, replace(handler, schema_version=2))
    )
    assert versions.require_adapter(other_version.capability) is other_version


@pytest.mark.parametrize("changes", ({"kind": ()}, {"kind": ("test", "")}, {"schema_version": 0}))
def test_invalid_stage_handler_capabilities_are_rejected(changes: dict[str, JsonValue]) -> None:
    with pytest.raises(ValueError):
        _ = replace(_handler(), **changes)


@pytest.mark.parametrize(
    "outcome",
    (
        CompletedWork(result={}),
        RetryWork(delay_ns=1, reason={"kind": "fixture"}, result={}),
        TerminalFailureWork(error={"kind": "fixture"}, result={}),
        WaitWork(
            continuation=StageDefinition(
                identifier="stage",
                work_item_identifier="work",
                operation_identifier="operation",
                kind=_STAGE_KIND,
                payload={},
            ),
            wait_kind=StageWaitKind.TIME,
            result={},
        ),
    ),
)
def test_stage_outcome_discriminator_roundtrips_every_outcome(outcome: StageOutcome) -> None:
    adapter = TypeAdapter(StageOutcome)
    assert adapter.validate_json(adapter.dump_json(outcome)) == outcome
    with pytest.raises(ValidationError):
        _ = adapter.validate_python({"kind": "unknown", "result": {}})


@pytest.mark.anyio
async def test_wait_publishes_checkpoint_evidence_and_resumes_the_same_attempt(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _register(database)
        source = _record()
        timestamp = datetime.now(UTC).isoformat()
        await database.publish_records_operation(
            component=_COMPONENT,
            operation_identifier=str(uuid4()),
            records=(source,),
            inputs=(),
            outputs=(),
            provenance=_PROVENANCE,
            invocation={},
            started_at_utc=timestamp,
            ended_at_utc=timestamp,
            duration_ns=0,
            result={},
        )
        checkpoint = _record()
        artifact = BytesDraft(
            identifier=str(uuid4()),
            kind=("test", "body"),
            media_type="text/plain",
            representation={"kind": "fixture"},
            content=b"retained body",
        )
        continuation = stage.model_copy(
            update={
                "identifier": str(uuid4()),
                "kind": _RESUME_KIND,
                "payload": {"value": 4},
                "checkpoint_inputs": (
                    NamedInput(name=("checkpoint",), object_identifier=checkpoint.identifier),
                ),
            }
        )
        contexts: list[StageAttemptContext] = []

        async def collect(payload: _Envelope, context: StageAttemptContext) -> StageOutcome:
            assert payload.payload.value == 3
            contexts.append(context)
            return WaitWork(
                continuation=continuation,
                wait_kind=StageWaitKind.RESOURCE,
                inputs=(NamedInput(name=("source",), object_identifier=source.identifier),),
                records=(checkpoint,),
                artifacts=(artifact,),
                outputs=(
                    NamedOutput(name=("checkpoint",), object_identifier=checkpoint.identifier),
                    NamedOutput(name=("body",), object_identifier=artifact.identifier),
                ),
                result={"state": "waiting"},
            )

        async def resume(payload: _Payload, context: StageAttemptContext) -> StageOutcome:
            contexts.append(context)
            assert payload.value == 4
            assert (await database.get_record(checkpoint.identifier))[2] == checkpoint.value
            assert (await database.get_artifact(artifact.identifier))[1] == artifact.content
            return CompletedWork(
                inputs=(NamedInput(name=("body",), object_identifier=artifact.identifier),),
                result={"state": "complete"},
            )

        registry = StageHandlerRegistry(
            adapters=(_adapter(),),
            handlers=(
                TypedStageHandler(
                    kind=_STAGE_KIND, schema_version=1, payload_type=_Envelope, handler=collect
                ),
                TypedStageHandler(
                    kind=_RESUME_KIND, schema_version=1, payload_type=_Payload, handler=resume
                ),
            ),
        )
        manager = stage_manager(database)
        async with _admit(manager, stage) as admission:
            assert admission.execution is not None
            outcome = await registry.execute(admission.execution)
            assert isinstance(outcome, WaitWork) and admission.execution.finalized
            adapter = TypeAdapter(StageOutcome)
            assert adapter.validate_json(adapter.dump_json(outcome)) == outcome
        assert (await database.operation(stage.operation_identifier))["state"] == "started"
        assert await database.operation_output_count(stage.operation_identifier) == 2
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT 1 FROM operation_inputs WHERE operation_id = ? AND object_id = ?",
                (stage.operation_identifier, source.identifier),
            )
            assert await cursor.fetchall() == [(1,)]
        async with _admit(manager, continuation) as admission:
            assert admission.execution is not None
            _ = await registry.execute(admission.execution)
        assert [context.attempt for context in contexts] == [1, 1]
        assert contexts[0].lease_token != contexts[1].lease_token
        assert contexts[0].operation_identifier == contexts[1].operation_identifier
        assert contexts[1].stage_identifier == continuation.identifier
        assert (await database.work(stage.work_item_identifier))["state"] == "completed"


@pytest.mark.anyio
@pytest.mark.parametrize("terminal", (False, True))
async def test_registry_applies_retry_and_terminal_outcomes(tmp_path: Path, terminal: bool) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _register(database)
        outcome = (
            TerminalFailureWork(error={"kind": "fixture"}, result={"failed": True})
            if terminal
            else RetryWork(delay_ns=0, reason={"kind": "fixture"}, result={"retry": True})
        )

        async def execute(_payload: _Envelope, _context: StageAttemptContext) -> StageOutcome:
            return outcome

        registry = StageHandlerRegistry(
            adapters=(_adapter(),),
            handlers=(
                TypedStageHandler(
                    kind=_STAGE_KIND, schema_version=1, payload_type=_Envelope, handler=execute
                ),
            ),
        )
        async with _admit(stage_manager(database), stage) as admission:
            assert admission.execution is not None
            assert await registry.execute(admission.execution) is outcome
            assert admission.execution.finalized
        assert (await database.work(stage.work_item_identifier))["state"] == (
            "terminal_failure" if terminal else "pending"
        )


@pytest.mark.anyio
async def test_context_uses_exact_stage_execution_and_network_admission(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _register(database, network=True)
        contexts: list[StageAttemptContext] = []

        async def execute(_payload: _Envelope, context: StageAttemptContext) -> StageOutcome:
            contexts.append(context)
            return CompletedWork(result={})

        registry = StageHandlerRegistry(
            adapters=(_adapter(),),
            handlers=(
                TypedStageHandler(
                    kind=_STAGE_KIND, schema_version=1, payload_type=_Envelope, handler=execute
                ),
            ),
        )
        manager = stage_manager(database)
        async with _admit(manager, stage) as first_admission:
            assert first_admission.execution is not None
            assert first_admission.execution.lease.execution == 1
        async with _admit(manager, stage) as admission:
            execution = admission.execution
            assert execution is not None
            _ = await registry.execute(execution)
            context = contexts[0]
            assert context.network_admission == execution.lease.network_admission
            assert context.network_admission is not None
            assert context.network_admission.activity_identifier.endswith(
                f":stage:{stage.identifier}:execution:{execution.lease.execution}"
            )
            assert context.stage_identifier == stage.identifier
            assert context.execution == execution.lease.execution == 2
            assert context.work_item_identifier == execution.lease.work_lease.work_item_identifier
            assert context.worker_identifier == execution.lease.work_lease.worker_identifier
            assert context.lease_token == execution.lease.token
            assert context.operation_identifier == stage.operation_identifier


@pytest.mark.anyio
async def test_context_preserves_retained_outage_exemptions_and_retry_budget_reset(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _register(database)
        contexts: list[StageAttemptContext] = []

        async def execute(_payload: _Envelope, context: StageAttemptContext) -> StageOutcome:
            contexts.append(context)
            if context.attempt == 5:
                return CompletedWork(result={})
            return RetryWork(
                delay_ns=0,
                reason={
                    "kind": "connectivity_outage" if context.attempt in (1, 3) else "transport"
                },
                result={},
            )

        registry = StageHandlerRegistry(
            adapters=(_adapter(),),
            handlers=(
                TypedStageHandler(
                    kind=_STAGE_KIND, schema_version=1, payload_type=_Envelope, handler=execute
                ),
            ),
        )
        manager = stage_manager(database)
        for attempt in range(1, 6):
            async with _admit(manager, stage) as admission:
                assert admission.execution is not None
                _ = await registry.execute(admission.execution)
            if attempt == 2:
                async with database._connections.writer() as connection:
                    _ = await connection.execute(
                        "INSERT INTO work_events(id, work_item_id, event_kind, recorded_at_utc_ns, data_json) VALUES (?, ?, 'enqueued', ?, ?)",
                        (
                            str(uuid4()),
                            stage.work_item_identifier,
                            time_ns(),
                            encode_json({"retry_budget_start_attempt": 2}),
                        ),
                    )
            if attempt < 5:
                stage = stage.model_copy(
                    update={"identifier": str(uuid4()), "operation_identifier": str(uuid4())}
                )
                await database.register_stage(stage, created_at_utc_ns=time_ns())
        assert contexts[-1].attempt == 5
        assert contexts[-1].outage_attempts == (1, 3)
        assert contexts[-1].retry_budget_start_attempt == 2
        assert contexts[-1].retry_attempt() == 2
        assert contexts[-1].network_admission is None
