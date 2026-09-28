"""Bounded AnyIO worker execution over Carl's durable SQLite queue."""

from __future__ import annotations

import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import anyio

from carl.core.components import Component
from carl.core.json import encode_json
from carl.core.models import CodeProvenance, JsonValue, StrictModel
from carl.core.work import WorkCapability, WorkLease
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkerSettings,
    WorkOutcome,
)
from carl.io.sqlite import Database, LeaseLostError


class WorkHandler(Protocol):
    capability: WorkCapability
    component: Component

    async def execute(self, payload: JsonValue, context: AttemptContext) -> WorkOutcome: ...


@dataclass(frozen=True, slots=True)
class TypedWorkHandler[PayloadT: StrictModel](WorkHandler):
    capability: WorkCapability
    component: Component
    payload_type: type[PayloadT]
    handler: Callable[[PayloadT, AttemptContext], Awaitable[WorkOutcome]]

    async def execute(self, payload: JsonValue, context: AttemptContext) -> WorkOutcome:
        decoded = self.payload_type.model_validate_json(encode_json(payload))
        return await self.handler(decoded, context)


@dataclass(frozen=True, slots=True)
class WorkHandlerRegistry:
    handlers: tuple[WorkHandler, ...]

    def __post_init__(self) -> None:
        capabilities = tuple(handler.capability for handler in self.handlers)
        if not capabilities:
            raise ValueError("At least one work handler is required")
        if len(set((item.kind, item.payload_schema_version) for item in capabilities)) != len(
            capabilities
        ):
            raise ValueError("Duplicate work-handler capability")

    @property
    def capabilities(self) -> tuple[WorkCapability, ...]:
        return tuple(handler.capability for handler in self.handlers)

    def require(self, lease: WorkLease) -> WorkHandler:
        for handler in self.handlers:
            if (
                handler.capability.kind == lease.kind
                and handler.capability.payload_schema_version == lease.payload_schema_version
            ):
                return handler
        raise KeyError((lease.kind, lease.payload_schema_version))


@dataclass(frozen=True, slots=True)
class WorkerRuntimeServices:
    new_identifier: Callable[[], str]
    utc_now_ns: Callable[[], int]
    monotonic_ns: Callable[[], int]
    code_provenance: Callable[[], Awaitable[CodeProvenance]]
    invocation: Callable[[], dict[str, JsonValue]]


def _utc_text(utc_ns: int) -> str:
    return datetime.fromtimestamp(utc_ns / 1_000_000_000, tz=UTC).isoformat()


def _exception_shape(error: BaseException) -> dict[str, JsonValue]:
    """Describe exception types without retaining possibly sensitive messages."""

    shape: dict[str, JsonValue] = {"type": type(error).__name__}
    if isinstance(error, BaseExceptionGroup):
        shape["exceptions"] = [_exception_shape(child) for child in error.exceptions]
    return shape


async def _mark_cancelled_operation(
    *,
    database: Database,
    operation_identifier: str,
    services: WorkerRuntimeServices,
    started_monotonic_ns: int,
    reason: str,
) -> bool:
    with anyio.move_on_after(10, shield=True) as cleanup_scope:
        # The committing transaction may have completed while cancellation
        # prevented its result from reaching this task. Durable state wins.
        try:
            await database.fail_operation(
                operation_id=operation_identifier,
                error={"kind": reason, "possibly_dispatched": True},
                result={"state": reason},
                ended_at_utc=_utc_text(services.utc_now_ns()),
                duration_ns=max(0, services.monotonic_ns() - started_monotonic_ns),
            )
        except RuntimeError:
            return False
    return not cleanup_scope.cancel_called


async def _release_interrupted_lease(
    *,
    database: Database,
    lease: WorkLease,
    operation_identifier: str,
    services: WorkerRuntimeServices,
    reason: str,
) -> None:
    with anyio.move_on_after(10, shield=True):
        released_at_utc_ns = services.utc_now_ns()
        try:
            await database.release_lease(
                work_item_identifier=lease.work_item_identifier,
                lease_token=lease.token,
                worker_identifier=lease.worker_identifier,
                utc_now_ns=lambda: released_at_utc_ns,
                eligible_at_utc_ns=released_at_utc_ns,
                reason={
                    "kind": reason,
                    "operation_identifier": operation_identifier,
                    "possibly_dispatched": True,
                    "decision": "retry",
                },
                event_identifier=services.new_identifier(),
            )
        except LeaseLostError:
            return
        except Exception as error:
            print(
                f"Carl could not release an interrupted work lease: {type(error).__name__}",
                file=sys.stderr,
                flush=True,
            )


async def execute_lease(
    *,
    database: Database,
    registry: WorkHandlerRegistry,
    settings: WorkerSettings,
    services: WorkerRuntimeServices,
    lease: WorkLease,
) -> WorkOutcome | None:
    handler = registry.require(lease)
    operation_identifier = services.new_identifier()
    started_utc_ns = services.utc_now_ns()
    started_monotonic_ns = services.monotonic_ns()
    context = AttemptContext(
        work_item_identifier=lease.work_item_identifier,
        lease_token=lease.token,
        worker_identifier=lease.worker_identifier,
        attempt=lease.attempt,
        operation_identifier=operation_identifier,
    )
    stop_renewal = anyio.Event()
    lease_lost = anyio.Event()

    async def renew(attempt_scope: anyio.CancelScope) -> None:
        while True:
            with anyio.move_on_after(settings.renewal_interval_ns / 1_000_000_000):
                await stop_renewal.wait()
            if stop_renewal.is_set():
                return
            try:
                await database.renew_lease(
                    work_item_identifier=lease.work_item_identifier,
                    lease_token=lease.token,
                    worker_identifier=lease.worker_identifier,
                    lease_duration_ns=settings.lease_duration_ns,
                    utc_now_ns=services.utc_now_ns,
                    event_identifier=services.new_identifier(),
                )
            except LeaseLostError:
                lease_lost.set()
                attempt_scope.cancel("durable work lease was lost")
                return

    outcome: WorkOutcome | None = None
    try:
        await database.begin_leased_operation(
            work_item_identifier=lease.work_item_identifier,
            lease_token=lease.token,
            worker_identifier=lease.worker_identifier,
            lease_duration_ns=settings.lease_duration_ns,
            utc_now_ns=services.utc_now_ns,
            event_identifier=services.new_identifier(),
            operation_id=operation_identifier,
            component=handler.component,
            provenance=await services.code_provenance(),
            invocation=services.invocation(),
            configuration={
                "work_item_identifier": lease.work_item_identifier,
                "attempt": lease.attempt,
                "work_kind": list(lease.kind),
                "payload_schema_version": lease.payload_schema_version,
                "payload": lease.payload,
            },
            started_at_utc=_utc_text(started_utc_ns),
        )
        with anyio.CancelScope() as attempt_scope:
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(renew, attempt_scope)
                try:
                    try:
                        outcome = await handler.execute(lease.payload, context)
                    except anyio.get_cancelled_exc_class():
                        raise
                    except Exception as error:
                        outcome = TerminalFailureWork(
                            error={
                                "kind": "unhandled_handler_error",
                                "stage": "handler",
                                "type": type(error).__name__,
                                "exception": _exception_shape(error),
                            },
                            result={"state": "terminal_failure"},
                        )
                finally:
                    stop_renewal.set()
        if lease_lost.is_set():
            await _mark_cancelled_operation(
                database=database,
                operation_identifier=operation_identifier,
                services=services,
                started_monotonic_ns=started_monotonic_ns,
                reason="lease_lost",
            )
            return
        if outcome is None:
            raise RuntimeError("Work handler exited without an outcome")

        ended_at_utc = _utc_text(services.utc_now_ns())
        duration_ns = max(0, services.monotonic_ns() - started_monotonic_ns)
        if isinstance(outcome, CompletedWork):
            await database.complete_leased_operation(
                work_item_identifier=lease.work_item_identifier,
                lease_token=lease.token,
                worker_identifier=lease.worker_identifier,
                utc_now_ns=services.utc_now_ns,
                event_identifier=services.new_identifier(),
                operation_id=operation_identifier,
                records=outcome.records,
                artifacts=outcome.artifacts,
                inputs=outcome.inputs,
                outputs=outcome.outputs,
                follow_on_work=outcome.follow_on_work,
                result=outcome.result,
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )
        elif isinstance(outcome, RetryWork):
            await database.retry_leased_operation(
                work_item_identifier=lease.work_item_identifier,
                lease_token=lease.token,
                worker_identifier=lease.worker_identifier,
                utc_now_ns=services.utc_now_ns,
                delay_ns=outcome.delay_ns,
                event_identifier=services.new_identifier(),
                operation_id=operation_identifier,
                reason=outcome.reason,
                result=outcome.result,
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
                inputs=outcome.inputs,
                records=outcome.records,
                artifacts=outcome.artifacts,
                outputs=outcome.outputs,
            )
        elif isinstance(outcome, TerminalFailureWork):
            await database.terminally_fail_leased_operation(
                work_item_identifier=lease.work_item_identifier,
                lease_token=lease.token,
                worker_identifier=lease.worker_identifier,
                utc_now_ns=services.utc_now_ns,
                event_identifier=services.new_identifier(),
                operation_id=operation_identifier,
                error=outcome.error,
                result=outcome.result,
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
                inputs=outcome.inputs,
                records=outcome.records,
                artifacts=outcome.artifacts,
                outputs=outcome.outputs,
            )
    except anyio.get_cancelled_exc_class():
        operation_marked = await _mark_cancelled_operation(
            database=database,
            operation_identifier=operation_identifier,
            services=services,
            started_monotonic_ns=started_monotonic_ns,
            reason="cancelled",
        )
        if operation_marked:
            await _release_interrupted_lease(
                database=database,
                lease=lease,
                operation_identifier=operation_identifier,
                services=services,
                reason="worker_cancelled",
            )
        raise
    except LeaseLostError:
        _ = await _mark_cancelled_operation(
            database=database,
            operation_identifier=operation_identifier,
            services=services,
            started_monotonic_ns=started_monotonic_ns,
            reason="lease_lost",
        )
    except Exception:
        operation_marked = await _mark_cancelled_operation(
            database=database,
            operation_identifier=operation_identifier,
            services=services,
            started_monotonic_ns=started_monotonic_ns,
            reason="worker_runtime_error",
        )
        if operation_marked:
            await _release_interrupted_lease(
                database=database,
                lease=lease,
                operation_identifier=operation_identifier,
                services=services,
                reason="worker_runtime_error",
            )
        raise
    return outcome


async def run_worker_pool(
    *,
    database: Database,
    registry: WorkHandlerRegistry,
    settings: WorkerSettings,
    services: WorkerRuntimeServices,
    stop: anyio.Event,
) -> None:
    claim_gate = anyio.Lock()

    async def run_worker() -> None:
        worker_identifier = services.new_identifier()
        while not stop.is_set():
            async with claim_gate:
                if stop.is_set():
                    return
                claim = await database.claim_work(
                    supported_capabilities=registry.capabilities,
                    worker_identifier=worker_identifier,
                    lease_token=services.new_identifier(),
                    lease_duration_ns=settings.lease_duration_ns,
                    utc_now_ns=services.utc_now_ns,
                    event_identifier=services.new_identifier(),
                )
                if claim.lease is None:
                    delay_ns = settings.idle_poll_interval_ns
                    if claim.next_eligible_at_utc_ns is not None:
                        delay_ns = min(
                            delay_ns,
                            max(0, claim.next_eligible_at_utc_ns - services.utc_now_ns()),
                        )
                    # Keep the gate while idle so one pool performs one queue probe per
                    # interval instead of every worker repeating the same blocked scan.
                    with anyio.move_on_after(delay_ns / 1_000_000_000):
                        await stop.wait()
                    continue
            await execute_lease(
                database=database,
                registry=registry,
                settings=settings,
                services=services,
                lease=claim.lease,
            )

    async with anyio.create_task_group() as task_group:
        for _ in range(settings.worker_count):
            _ = task_group.start_soon(run_worker)
