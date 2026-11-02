"""Explicit opt-in adapters and typed handlers for durable execution stages."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from carl.core.components import Component
from carl.core.json import encode_json
from carl.core.models import JsonValue, StrictModel
from carl.core.resources import ResourceRequest
from carl.core.stages import StageAttemptContext, StageDefinition, StageOutcome
from carl.core.work import WorkCapability, WorkDefinition
from carl.io.connectivity import work_retry_budget
from carl.io.stage_publication import StageHandlerResult, StagePublication
from carl.io.stages import StageExecution


@dataclass(frozen=True, slots=True)
class WorkStageAdapter:
    capability: WorkCapability
    component: Component
    initial_stage: Callable[[WorkDefinition, str, str], StageDefinition]
    initial_resources: tuple[ResourceRequest, ...] | None = None


class StageHandler(Protocol):
    kind: tuple[str, ...]
    schema_version: int

    async def execute(
        self, payload: JsonValue, context: StageAttemptContext
    ) -> StageHandlerResult: ...


@dataclass(frozen=True, slots=True)
class TypedStageHandler[PayloadT: StrictModel](StageHandler):
    kind: tuple[str, ...]
    schema_version: int
    payload_type: type[PayloadT]
    handler: Callable[[PayloadT, StageAttemptContext], Awaitable[StageHandlerResult]]

    def __post_init__(self) -> None:
        if not self.kind or any(not part for part in self.kind):
            raise ValueError("Stage handler kinds require nonempty parts")
        if self.schema_version < 1:
            raise ValueError("Stage handler schema versions must be positive")

    async def execute(self, payload: JsonValue, context: StageAttemptContext) -> StageHandlerResult:
        decoded = self.payload_type.model_validate_json(encode_json(payload))
        return await self.handler(decoded, context)


@dataclass(frozen=True, slots=True)
class StageHandlerRegistry:
    adapters: tuple[WorkStageAdapter, ...]
    handlers: tuple[StageHandler, ...]

    def __post_init__(self) -> None:
        work_capabilities = tuple(
            (adapter.capability.kind, adapter.capability.payload_schema_version)
            for adapter in self.adapters
        )
        if len(set(work_capabilities)) != len(work_capabilities):
            raise ValueError("Duplicate work-stage adapter capability")
        stage_capabilities = tuple(
            (handler.kind, handler.schema_version) for handler in self.handlers
        )
        if len(set(stage_capabilities)) != len(stage_capabilities):
            raise ValueError("Duplicate stage-handler capability")

    def require_adapter(self, capability: WorkCapability) -> WorkStageAdapter:
        for adapter in self.adapters:
            if adapter.capability == capability:
                return adapter
        raise KeyError((capability.kind, capability.payload_schema_version))

    def require_stage(self, definition: StageDefinition) -> StageHandler:
        for handler in self.handlers:
            if (
                handler.kind == definition.kind
                and handler.schema_version == definition.schema_version
            ):
                return handler
        raise KeyError((definition.kind, definition.schema_version))

    async def run(self, execution: StageExecution) -> StageHandlerResult:
        """Build one outcome; dispatch policy can inspect it before publication."""

        lease = execution.lease
        definition = lease.definition
        work = lease.work_lease
        handler = self.require_stage(definition)
        outage_attempts, retry_budget_start_attempt = await work_retry_budget(
            execution.manager.database, work.work_item_identifier
        )
        context = StageAttemptContext(
            work_item_identifier=work.work_item_identifier,
            lease_token=work.token,
            worker_identifier=work.worker_identifier,
            attempt=work.attempt,
            operation_identifier=definition.operation_identifier,
            outage_attempts=outage_attempts,
            retry_budget_start_attempt=retry_budget_start_attempt,
            stage_identifier=definition.identifier,
            execution=lease.execution,
            network_admission=lease.network_admission,
            resident_session_identifier=definition.resident_session_identifier,
        )
        return await handler.execute(definition.payload, context)

    async def execute(self, execution: StageExecution) -> StageOutcome:
        """Run and publish one continuation through its current execution fence."""

        outcome = await self.run(execution)
        await execution.apply(outcome)
        return outcome.outcome if isinstance(outcome, StagePublication) else outcome
