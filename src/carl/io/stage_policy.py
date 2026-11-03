"""Shared outcome policy without probing connectivity inside execution permits."""

from dataclasses import dataclass, field
from time import time_ns
from uuid import uuid4

import anyio

from carl.core.connectivity import connectivity_failure, network_work_kind
from carl.core.marketplace_timing import immediate_marketplace_retry
from carl.core.models import RecordDraft, StrictModel
from carl.core.stages import StageAttemptContext, StageDefinition, StageWaitKind, WaitWork
from carl.core.work import WorkCancellationRequestedError
from carl.core.worker import RetryWork, TerminalFailureWork
from carl.io.connectivity import PROBE_INTERVAL_NS, ConnectivityMonitor
from carl.io.mcp_errors import redact_diagnostic
from carl.io.resource_permits import ResourceLeaseLost
from carl.io.sqlite import LeaseLostError
from carl.io.stage_publication import StageHandlerResult, StagePublication
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler
from carl.io.stages import StageExecution
from carl.io.worker import _exception_shape

_REVIEW = ("carl", "runtime", "connectivity_review")
_RECORD = ("carl", "runtime", "connectivity_failure")
_INVALID = ("carl", "runtime", "invalid_initial_stage")


class _InvalidInitial(StrictModel):
    error_type: str
    message: str


def invalid_initial_stage(
    work_identifier: str, identifier: str, operation: str, error: Exception
) -> StageDefinition:
    shape = _exception_shape(error)
    return StageDefinition(
        identifier=identifier,
        work_item_identifier=work_identifier,
        operation_identifier=operation,
        kind=_INVALID,
        payload=_InvalidInitial(
            error_type=type(error).__name__,
            message=redact_diagnostic(str(shape.get("message", type(error).__name__)))[:2048],
        ).model_dump(mode="json"),
    )


async def _invalid_initial(
    payload: _InvalidInitial, context: StageAttemptContext
) -> TerminalFailureWork:
    del context
    return TerminalFailureWork(
        error={
            "kind": "invalid_initial_stage",
            "type": payload.error_type,
            "message": payload.message,
        },
        result={"state": "terminal_failure"},
    )


def invalid_initial_handler() -> TypedStageHandler[_InvalidInitial]:
    return TypedStageHandler(_INVALID, 1, _InvalidInitial, _invalid_initial)


class _Review(StrictModel):
    record_identifier: str
    epoch: str
    generation: int


@dataclass
class StagePolicy:
    monitor: ConnectivityMonitor
    epoch: str = field(default_factory=lambda: str(uuid4()))
    generation: int = 0
    requested: int = 0
    available: bool = True
    wake: anyio.Event = field(default_factory=anyio.Event)

    def request(self) -> int:
        self.requested = self.generation + 1
        self.wake.set()
        return self.requested

    async def run(self) -> None:
        """One coalescing probe task also resumes a persisted database outage."""
        while True:
            requested = self.requested
            self.available = await self.monitor.available(after_failure=requested > self.generation)
            self.generation = max(self.generation, requested)
            event, self.wake = self.wake, anyio.Event()
            with anyio.move_on_after(1):
                await event.wait()

    async def review(self, payload: _Review, context: StageAttemptContext) -> StageHandlerResult:
        stage = await self.monitor.database.stage(context.stage_identifier)
        if payload.epoch != self.epoch:
            payload = payload.model_copy(update={"epoch": self.epoch, "generation": self.request()})
        if payload.generation > self.generation:
            self.request()
            return WaitWork(
                continuation=stage.model_copy(
                    update={
                        "identifier": str(uuid4()),
                        "payload": payload.model_dump(mode="json"),
                        "not_before_utc_ns": time_ns() + 100_000_000,
                    }
                ),
                wait_kind=StageWaitKind.CONNECTIVITY,
                result=(await self.monitor.database.work_progress((context.work_item_identifier,)))[
                    0
                ]["result"],
            )
        kind, version, value = await self.monitor.database.get_record(payload.record_identifier)
        if kind != _RECORD or version != 1:
            raise ValueError("Invalid retained connectivity outcome")
        if not self.available:
            return RetryWork(
                delay_ns=PROBE_INTERVAL_NS,
                reason={"kind": "connectivity_outage", "dispatched": True},
                result={"state": "waiting_for_connectivity", "failure": value["result"]},
            )
        if value["kind"] == "retry":
            outcome = RetryWork(
                delay_ns=value["delay_ns"], reason=value["reason"], result=value["result"]
            )
            return immediate_marketplace_retry(
                await self.monitor.database.work_kind(context.work_item_identifier), outcome
            )
        return TerminalFailureWork(error=value["error"], result=value["result"])

    async def transform(
        self, execution: StageExecution, result: StageHandlerResult
    ) -> StageHandlerResult:
        outcome = result.outcome if isinstance(result, StagePublication) else result
        work = execution.lease.work_lease
        if (
            execution.lease.definition.kind == _REVIEW
            or isinstance(outcome, WaitWork)
            or not network_work_kind(work.kind)
            or not connectivity_failure(outcome)
        ):
            return result
        identifier = str(uuid4())
        value = outcome.model_dump(
            mode="json", exclude={"inputs", "records", "artifacts", "outputs"}
        )
        wait = WaitWork(
            inputs=outcome.inputs,
            records=(
                *outcome.records,
                RecordDraft(identifier=identifier, kind=_RECORD, schema_version=1, value=value),
            ),
            artifacts=outcome.artifacts,
            outputs=outcome.outputs,
            continuation=execution.lease.definition.model_copy(
                update={
                    "identifier": str(uuid4()),
                    "kind": _REVIEW,
                    "resources": (),
                    "network_activity": None,
                    "payload": _Review(
                        record_identifier=identifier, epoch="", generation=0
                    ).model_dump(mode="json"),
                    "not_before_utc_ns": time_ns(),
                }
            ),
            wait_kind=StageWaitKind.CONNECTIVITY,
            result=outcome.result,
        )
        return (
            StagePublication(wait, result.children)
            if isinstance(result, StagePublication)
            else wait
        )

    def handler(self) -> TypedStageHandler[_Review]:
        return TypedStageHandler(_REVIEW, 1, _Review, self.review)


@dataclass(frozen=True, slots=True)
class PolicyStageRegistry(StageHandlerRegistry):
    policy: StagePolicy

    async def run(self, execution: StageExecution) -> StageHandlerResult:
        try:
            result = await super(PolicyStageRegistry, self).run(execution)
        except (WorkCancellationRequestedError, ResourceLeaseLost, LeaseLostError):
            raise
        except Exception as error:
            checkpoint = (
                await execution.manager.database.work_progress(
                    (execution.lease.definition.work_item_identifier,)
                )
            )[0]["result"]
            result = TerminalFailureWork(
                error={
                    "kind": "unhandled_handler_error",
                    "stage": "handler",
                    "type": type(error).__name__,
                    "exception": _exception_shape(error),
                },
                result=checkpoint if checkpoint is not None else {"state": "terminal_failure"},
            )
        return await self.policy.transform(execution, result)
