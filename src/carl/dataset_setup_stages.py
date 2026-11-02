"""Opt-in dataset setup phases, admitted only through their global file owner."""

import re
from time import time_ns
from typing import Literal, override
from urllib.parse import urlsplit
from uuid import uuid4

from carl.core.components import Component, ComponentId
from carl.core.dataset_downloads import DatasetFileError
from carl.core.dataset_setup import DatasetSetupReference, DatasetSetupState
from carl.core.dataset_workflows import DatasetWorkflowBinding
from carl.core.json import encode_json
from carl.core.models import CodeProvenance, JsonValue, StrictModel
from carl.core.resources import ResourceRequest
from carl.core.stages import (
    StageAttemptContext,
    StageDefinition,
    StageOutcome,
    StageWaitKind,
    WaitWork,
    initial_work_stage,
)
from carl.core.travel_data import (
    TRAVEL_DATA_SETUP_WORK_KIND,
    TravelDataFailure,
    TravelDataSetupRequest,
    TravelDataSetupResult,
)
from carl.core.work import (
    NetworkActivityDefinition,
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
)
from carl.core.worker import CompletedWork, TerminalFailureWork
from carl.io.configuration import ConfigurationFailure
from carl.io.dataset_setup import (
    DatasetSetupEngine,
    dataset_setup_resources,
    prepare_dataset_setup,
)
from carl.io.dataset_setup_files import (
    DatasetSetupFileError,
    read_setup_state,
    write_setup_state,
)
from carl.io.dataset_workflow_owner import (
    DatasetWorkflowOwner,
    DatasetWorkflowService,
    get_dataset_workflow_service,
)
from carl.io.geonames import GeonamesFailure
from carl.io.native_tools import NativeBuildToolError
from carl.io.osrm_build import OsrmBuildError
from carl.io.sqlite import Database
from carl.io.stage_publication import StageHandlerResult
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler, WorkStageAdapter
from carl.io.stages import StageManager

_PREPARE_STAGE_KIND = ("carl", "travel_data", "setup", "prepare")
OWNED_STAGE_KIND = ("carl", "travel_data", "setup", "owned")
_CAPABILITY = WorkCapability(kind=TRAVEL_DATA_SETUP_WORK_KIND, payload_schema_version=1)
_EXPECTED_FAILURES = (
    TravelDataFailure,
    DatasetFileError,
    DatasetSetupFileError,
    ConfigurationFailure,
    GeonamesFailure,
    OsrmBuildError,
    NativeBuildToolError,
    OSError,
    ValueError,
)


class _Envelope(StrictModel):
    work_kind: tuple[str, ...]
    payload_schema_version: Literal[1]
    payload: TravelDataSetupRequest


class _SafeTypedHandler[PayloadT: StrictModel](TypedStageHandler[PayloadT]):
    @override
    async def execute(self, payload: JsonValue, context: StageAttemptContext) -> StageHandlerResult:
        try:
            return await super().execute(payload, context)
        except _EXPECTED_FAILURES as error:
            return _failure(error)


def is_dataset_setup_owned_stage(stage: StageDefinition) -> bool:
    return stage.kind == OWNED_STAGE_KIND and stage.schema_version == 1


def _failure(error: Exception) -> TerminalFailureWork:
    if isinstance(error, ConfigurationFailure):
        reason = error.code
    elif isinstance(error, DatasetSetupFileError):
        reason = "dataset_setup_metadata_invalid"
    elif isinstance(error, OSError):
        reason = "dataset_setup_storage_failure"
    else:
        reason = getattr(error, "reason", "dataset_setup_invalid_state")
    if isinstance(reason, str):
        reason = reason.partition(":")[0]
    if not isinstance(reason, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,127}", reason) is None:
        reason = "dataset_setup_failed"
    return TerminalFailureWork(
        error={
            "kind": reason,
            "message": "Dataset setup failed; retained workflow files are available for review.",
        },
        result={"state": "failed", "reason": reason},
    )


def _binding(database: Database, context: StageAttemptContext) -> DatasetWorkflowBinding:
    return DatasetWorkflowBinding(
        database_path=database.path.absolute(),
        work_item_identifier=context.work_item_identifier,
        operation_identifier=context.operation_identifier,
    )


async def _request(database: Database, work_identifier: str) -> TravelDataSetupRequest:
    work = await database.work_definition(work_identifier)
    if work.kind != _CAPABILITY.kind or work.payload_schema_version != 1:
        raise ValueError("Dataset setup work capability differs from its retained definition")
    return TravelDataSetupRequest.model_validate_json(encode_json(work.payload))


def _network(
    state: DatasetSetupState, context: StageAttemptContext
) -> NetworkActivityDefinition | None:
    if state.phase != "capture_http":
        return None
    if not isinstance(state.value, dict) or not isinstance(state.value.get("capture"), dict):
        raise ValueError("Dataset capture has no source plan")
    capture = state.value["capture"]
    source = capture.get("source")
    if not isinstance(source, dict) or not isinstance(source.get("url"), str):
        raise ValueError("Dataset capture has no source URL")
    parsed = urlsplit(source["url"])
    origin = (parsed.scheme, parsed.hostname or "", str(parsed.port or 443))
    kind = ("carl", "travel_data", "network", "source")
    return NetworkActivityDefinition(
        identifier=str(uuid4()),
        kind=kind,
        operation_identifier=context.operation_identifier,
        network_session_identifier=f"dataset:{context.operation_identifier}",
        ordinal=1,
        attempt=context.attempt,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND, identity=kind),
            SchedulingScope(kind=SchedulingScopeKind.REMOTE_ORIGIN, identity=origin),
        ),
    )


def _wait(
    engine: DatasetSetupEngine, state: DatasetSetupState, context: StageAttemptContext
) -> WaitWork:
    reference = write_setup_state(engine.directories.travel_data_directory, state)
    deadline = state.value.get("wait_until", 0) if isinstance(state.value, dict) else 0
    if type(deadline) is not int or deadline < 0:
        raise ValueError("Dataset phase wait deadline must be a nonnegative integer")
    return WaitWork(
        continuation=StageDefinition(
            identifier=str(uuid4()),
            work_item_identifier=context.work_item_identifier,
            operation_identifier=context.operation_identifier,
            kind=OWNED_STAGE_KIND,
            payload=reference.model_dump(mode="json"),
            resources=dataset_setup_resources(state),
            not_before_utc_ns=deadline,
            network_activity=_network(state, context),
        ),
        wait_kind=StageWaitKind.TIME if deadline > time_ns() else StageWaitKind.RESOURCE,
        result={"state": "dataset_setup_waiting", "phase": state.phase},
    )


def _initial(work: WorkDefinition, identifier: str, operation: str) -> StageDefinition:
    return initial_work_stage(
        work,
        identifier=identifier,
        operation_identifier=operation,
        kind=_PREPARE_STAGE_KIND,
        resources=(ResourceRequest(name="cpu"),),
    )


def build_dataset_setup_prepare_registry(
    database: Database, engine: DatasetSetupEngine
) -> StageHandlerRegistry:
    async def prepare(envelope: _Envelope, context: StageAttemptContext) -> StageOutcome:
        try:
            stage = await database.stage(context.stage_identifier)
            if (
                stage.kind != _PREPARE_STAGE_KIND
                or stage.work_item_identifier != context.work_item_identifier
                or stage.operation_identifier != context.operation_identifier
                or stage.resources != (ResourceRequest(name="cpu"),)
                or stage.network_activity is not None
                or stage.resident_session_identifier is not None
            ):
                raise ValueError("Dataset setup preparation requires its exact CPU stage")
            request = await _request(database, context.work_item_identifier)
            if envelope.work_kind != _CAPABILITY.kind or envelope.payload != request:
                raise ValueError("Dataset setup envelope differs from retained work")
            state = await prepare_dataset_setup(
                engine.directories, request, binding=_binding(database, context)
            )
            return _wait(engine, state, context)
        except _EXPECTED_FAILURES as error:
            return _failure(error)

    component = Component(ComponentId(TRAVEL_DATA_SETUP_WORK_KIND), 1, prepare)
    return StageHandlerRegistry(
        adapters=(WorkStageAdapter(_CAPABILITY, component, _initial),),
        handlers=(_SafeTypedHandler(_PREPARE_STAGE_KIND, 1, _Envelope, prepare),),
    )


async def _state_for_stage(
    database: Database, engine: DatasetSetupEngine, stage: StageDefinition
) -> DatasetSetupState:
    if not is_dataset_setup_owned_stage(stage) or stage.resident_session_identifier is not None:
        raise ValueError("Dataset setup continuation requires its file owner")
    reference = DatasetSetupReference.model_validate_json(encode_json(stage.payload))
    state = read_setup_state(engine.directories.travel_data_directory, reference)
    binding = DatasetWorkflowBinding(
        database_path=database.path.absolute(),
        work_item_identifier=stage.work_item_identifier,
        operation_identifier=stage.operation_identifier,
    )
    if state.definition.binding != binding or state.definition.request != await _request(
        database, stage.work_item_identifier
    ):
        raise ValueError("Dataset setup state differs from its genuine queued binding")
    if stage.resources != dataset_setup_resources(state):
        raise ValueError("Dataset setup phase does not match its admitted resource requirements")
    if (state.phase == "capture_http") != (stage.network_activity is not None):
        raise ValueError("Dataset setup phase has mismatched network admission")
    return state


def build_dataset_setup_owned_registry(
    database: Database, engine: DatasetSetupEngine, owner: DatasetWorkflowOwner
) -> StageHandlerRegistry:
    async def step(reference: DatasetSetupReference, context: StageAttemptContext) -> StageOutcome:
        try:
            stage = await database.stage(context.stage_identifier)
            if (
                stage.work_item_identifier != context.work_item_identifier
                or stage.operation_identifier != context.operation_identifier
                or stage.payload != reference.model_dump(mode="json")
            ):
                raise ValueError("Dataset setup stage and execution context differ")
            state = await _state_for_stage(database, engine, stage)
            if state.definition != owner.journal.definition or state.definition.binding != _binding(
                database, context
            ):
                raise ValueError("Dataset setup continuation belongs to a different workflow owner")
            await owner.checkpoint(
                state.phase, {"state_reference": reference.model_dump(mode="json")}
            )
            state = await engine.step(owner, state)
            if state.definition != owner.journal.definition:
                raise ValueError("Dataset engine changed the immutable workflow definition")
            if state.phase == "done":
                value = state.value
                if not isinstance(value, dict) or "result" not in value:
                    raise ValueError("Completed dataset setup has no verified result")
                result = TravelDataSetupResult.model_validate_json(encode_json(value["result"]))
                return CompletedWork(result=result.model_dump(mode="json"))
            return _wait(engine, state, context)
        except _EXPECTED_FAILURES as error:
            return _failure(error)

    return StageHandlerRegistry(
        adapters=(), handlers=(_SafeTypedHandler(OWNED_STAGE_KIND, 1, DatasetSetupReference, step),)
    )


async def submit_dataset_setup_stage(
    manager: StageManager,
    engine: DatasetSetupEngine,
    stage: StageDefinition,
    component: Component,
    provenance: CodeProvenance,
    invocation: dict[str, JsonValue],
    *,
    service: DatasetWorkflowService | None = None,
) -> bool:
    if service is not None and (
        service.manager.database is not manager.database
        or service.manager.resources is not manager.resources
        or service.directories != engine.directories
    ):
        raise ValueError("Dataset setup service must share its database, resources and directories")
    state = await _state_for_stage(manager.database, engine, stage)
    if service is None:
        service = get_dataset_workflow_service(
            manager,
            engine.directories,
            lambda owner: build_dataset_setup_owned_registry(manager.database, engine, owner),
            provenance,
            invocation,
        )
    return await service.submit(
        stage, component=component, definition=state.definition, resume=True
    )
