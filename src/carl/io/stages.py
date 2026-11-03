"""Nonblocking stage execution with atomic durable admission and physical guards."""

from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import perf_counter_ns, time_ns
from typing import Literal, Protocol
from uuid import uuid4

import anyio
from anyio import sleep as stage_sleep

from carl.core.components import Component
from carl.core.models import (
    ArtifactDraft,
    CodeProvenance,
    JsonValue,
    NamedInput,
    NamedOutput,
    RecordDraft,
)
from carl.core.residents import ResidentLease
from carl.core.stages import StageClaimResult, StageLease, WaitWork
from carl.core.work_dispatch import ExpiryCursor
from carl.core.worker import CompletedWork, RetryWork, TerminalFailureWork
from carl.io.resource_permits import (
    ExecutionGuard,
    ResourceLeaseLost,
    ResourceManager,
    current_resource_manager,
)
from carl.io.sqlite import Database
from carl.io.stage_publication import StageHandlerResult, StagePublication


def _identifier() -> str:
    return str(uuid4())


@dataclass(slots=True)
class StageExecution:
    manager: "StageManager"
    lease: StageLease
    guard: ExecutionGuard
    provenance: CodeProvenance
    invocation: dict[str, JsonValue]
    finalized: bool = False
    finalizing: bool = False
    started_monotonic_ns: int = field(default_factory=perf_counter_ns)

    def _phase_duration_ns(self) -> int | None:
        elapsed = self.manager.monotonic_now_ns() - self.started_monotonic_ns
        return elapsed if elapsed >= 0 else None

    async def apply(self, outcome: StageHandlerResult) -> None:
        """Publish an outcome through this execution's continuation or final fence."""

        if isinstance(outcome, StagePublication):
            if isinstance(outcome.outcome, WaitWork):
                # Persist large watch descriptors in bounded, fenced batches
                # before the child-publication transaction. Suspension attaches
                # them atomically and releases this execution for metadata-only
                # construction of the normalized subscriptions.
                await self.manager.database.prepare_stage_progress_wait(
                    self.lease, outcome.outcome, now_utc_ns=self.manager.utc_now_ns()
                )
            await self._publish_children(outcome)
            return
        if isinstance(outcome, WaitWork):
            await self.manager.database.prepare_stage_progress_wait(
                self.lease, outcome, now_utc_ns=self.manager.utc_now_ns()
            )
            await self.suspend(
                outcome,
                records=outcome.records,
                artifacts=outcome.artifacts,
                outputs=outcome.outputs,
                inputs=outcome.inputs,
            )
        else:
            await self.finish(outcome)

    async def _publish_children(self, publication: StagePublication) -> None:
        self._seal()
        database = self.manager.database
        committed = False
        try:
            async with database.transaction():
                if not await database.validate_stage(
                    self.lease, now_utc_ns=self.manager.utc_now_ns()
                ):
                    raise ResourceLeaseLost("Cannot publish children through an expired stage")
                for child in publication.children:
                    await database.begin_operation(
                        operation_id=child.identifier,
                        component=child.component,
                        provenance=self.provenance,
                        invocation=self.invocation,
                        configuration=child.configuration,
                        started_at_utc=child.started_at_utc,
                        inputs=tuple((item.name, item.object_identifier) for item in child.inputs),
                    )
                    if child.error is None:
                        await database.complete_operation(
                            operation_id=child.identifier,
                            records=child.records,
                            artifacts=child.artifacts,
                            outputs=child.outputs,
                            result=child.result,
                            ended_at_utc=child.ended_at_utc,
                            duration_ns=child.duration_ns,
                        )
                    else:
                        await database.fail_operation(
                            operation_id=child.identifier,
                            error=child.error,
                            result=child.result,
                            ended_at_utc=child.ended_at_utc,
                            duration_ns=child.duration_ns,
                            records=child.records,
                            artifacts=child.artifacts,
                            outputs=child.outputs,
                        )
                await self.apply(publication.outcome)
            committed = True
        finally:
            if not committed:
                self.finalized = False

    def _seal(self) -> None:
        if self.finalized:
            raise ResourceLeaseLost("Stage execution was already finalized")
        if not self.guard.seal():
            raise ResourceLeaseLost("Stage still has an inherited physical executor")
        self.finalizing = True

    async def suspend(
        self,
        wait: WaitWork,
        *,
        records: Sequence[RecordDraft] = (),
        artifacts: Sequence[ArtifactDraft] = (),
        outputs: Sequence[NamedOutput] = (),
        inputs: Sequence[NamedInput] = (),
    ) -> None:
        """Commit partial evidence and wait within this same logical attempt."""

        self._seal()
        await self.manager.database.suspend_stage(
            self.lease,
            wait,
            now_utc_ns=self.manager.utc_now_ns(),
            phase_duration_ns=self._phase_duration_ns(),
            records=records,
            artifacts=artifacts,
            outputs=outputs,
            inputs=inputs,
        )
        self.finalized = True

    async def finish(self, outcome: CompletedWork | RetryWork | TerminalFailureWork) -> None:
        """Fence stage finalization and the logical operation's publication together."""

        self._seal()
        database = self.manager.database
        now = self.manager.utc_now_ns()
        operation = await database.operation(self.lease.definition.operation_identifier)
        started = operation.get("started_at_utc")
        if not isinstance(started, str):
            raise ValueError("Logical stage operation has no start timestamp")
        ended = datetime.fromtimestamp(now / 1_000_000_000, tz=UTC)
        duration = max(
            0, int((ended - datetime.fromisoformat(started)).total_seconds() * 1_000_000_000)
        )
        work = self.lease.work_lease
        phase_outcome: Literal["completed", "retry", "terminal_failure"] = (
            "completed"
            if isinstance(outcome, CompletedWork)
            else "retry"
            if isinstance(outcome, RetryWork)
            else "terminal_failure"
        )
        async with database.transaction():
            await database.complete_stage(
                self.lease,
                now_utc_ns=now,
                phase_duration_ns=self._phase_duration_ns(),
                phase_outcome=phase_outcome,
            )
            if isinstance(outcome, CompletedWork):
                await database.complete_leased_operation(
                    work_item_identifier=work.work_item_identifier,
                    lease_token=work.token,
                    worker_identifier=work.worker_identifier,
                    utc_now_ns=self.manager.utc_now_ns,
                    event_identifier=self.manager.new_identifier(),
                    operation_id=self.lease.definition.operation_identifier,
                    records=outcome.records,
                    artifacts=outcome.artifacts,
                    outputs=outcome.outputs,
                    inputs=outcome.inputs,
                    follow_on_work=outcome.follow_on_work,
                    result=outcome.result,
                    ended_at_utc=ended.isoformat(),
                    duration_ns=duration,
                )
            elif isinstance(outcome, RetryWork):
                await database.retry_leased_operation(
                    work_item_identifier=work.work_item_identifier,
                    lease_token=work.token,
                    worker_identifier=work.worker_identifier,
                    utc_now_ns=self.manager.utc_now_ns,
                    event_identifier=self.manager.new_identifier(),
                    operation_id=self.lease.definition.operation_identifier,
                    records=outcome.records,
                    artifacts=outcome.artifacts,
                    outputs=outcome.outputs,
                    inputs=outcome.inputs,
                    result=outcome.result,
                    reason=outcome.reason,
                    delay_ns=outcome.delay_ns,
                    ended_at_utc=ended.isoformat(),
                    duration_ns=duration,
                )
            else:
                await database.terminally_fail_leased_operation(
                    work_item_identifier=work.work_item_identifier,
                    lease_token=work.token,
                    worker_identifier=work.worker_identifier,
                    utc_now_ns=self.manager.utc_now_ns,
                    event_identifier=self.manager.new_identifier(),
                    operation_id=self.lease.definition.operation_identifier,
                    records=outcome.records,
                    artifacts=outcome.artifacts,
                    outputs=outcome.outputs,
                    inputs=outcome.inputs,
                    result=outcome.result,
                    error=outcome.error,
                    ended_at_utc=ended.isoformat(),
                    duration_ns=duration,
                )
        self.finalized = True


@dataclass(frozen=True, slots=True)
class StageAdmission:
    result: StageClaimResult
    execution: StageExecution | None


class StageHeartbeat(Protocol):
    """A pool-owned heartbeat avoids nesting task scopes around resident clients."""

    def register(self, execution: StageExecution) -> None: ...

    def unregister(self, execution: StageExecution) -> None: ...


_RECOVERY_PAGE_SIZE = 32


@dataclass(slots=True)
class _StageRecovery:
    lock: anyio.Lock = field(default_factory=anyio.Lock)
    after: ExpiryCursor | None = None
    now_utc_ns: int | None = None


@dataclass(frozen=True, slots=True)
class StageManager:
    database: Database
    resources: ResourceManager
    new_identifier: Callable[[], str] = _identifier
    utc_now_ns: Callable[[], int] = time_ns
    lease_duration_ns: int = 60_000_000_000
    renewal_interval_ns: int = 20_000_000_000
    monotonic_now_ns: Callable[[], int] = perf_counter_ns

    def __post_init__(self) -> None:
        if self.resources.database is not self.database:
            raise ValueError("Stage execution must use its database's resource manager")
        if not 0 < self.renewal_interval_ns < self.lease_duration_ns:
            raise ValueError("Stage renewal interval must precede expiry")

    async def recover(self) -> None:
        """Advance one bounded recovery page; a busy guard cannot pin the frontier."""

        state = self.resources.get_or_create_service("stage-expiry-recovery", _StageRecovery)
        try:
            state.lock.acquire_nowait()
        except anyio.WouldBlock:
            # Another caller owns this short sweep. Admission need not wait for it.
            await self.resources._recover()
            return
        try:
            if state.now_utc_ns is None:
                state.now_utc_ns = self.utc_now_ns()
            page = await self.database.expired_stage_candidates(
                coordination_scope=self.resources.scope,
                now_utc_ns=state.now_utc_ns,
                limit=_RECOVERY_PAGE_SIZE,
                after=state.after,
            )
            for candidate in page.items:
                try:
                    async with self.resources.execution_guard(
                        candidate.token, owner=candidate.worker_identifier
                    ):
                        # A lease contains the original work payload. Load it only
                        # after the matching physical executor has relinquished it.
                        lease = await self.database.stage_execution_lease(
                            candidate.stage_identifier,
                            expected_token=candidate.token,
                            expected_execution=candidate.execution,
                            coordination_scope=self.resources.scope,
                        )
                        if lease is not None:
                            await self.database.recover_stage_execution(
                                lease, now_utc_ns=self.utc_now_ns()
                            )
                except BlockingIOError:
                    pass
                finally:
                    state.after = candidate.cursor
            if not page.has_more:
                state.after = None
                state.now_utc_ns = None
        finally:
            state.lock.release()
        await self.resources._recover()

    async def _renew(self, execution: StageExecution) -> None:
        while True:
            await stage_sleep(self.renewal_interval_ns / 1_000_000_000)
            if execution.finalizing:
                return
            lease = await self.database.renew_stage(
                execution.lease,
                now_utc_ns=self.utc_now_ns(),
                lease_duration_ns=self.lease_duration_ns,
            )
            if lease is None:
                if execution.finalizing:
                    return
                raise ResourceLeaseLost("Stage execution ownership expired or changed")
            execution.lease = lease

    @asynccontextmanager
    async def admit(
        self,
        stage_identifier: str,
        *,
        component: Component,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        worker_identifier: str | None = None,
        resident_lease: ResidentLease | None = None,
        heartbeat: StageHeartbeat | None = None,
    ) -> AsyncIterator[StageAdmission]:
        """Try once; a blocked stage remains durable and consumes no execution task."""

        if current_resource_manager() is not self.resources:
            raise ValueError("Stage adapters require their database's active resource context")
        await self.recover()
        token = self.new_identifier()
        owner = worker_identifier or self.resources.owner
        async with self.resources.execution_guard(token, owner=owner) as guard:
            lease: StageLease | None = None
            result: StageClaimResult | None = None
            execution: StageExecution | None = None
            try:
                with anyio.CancelScope(shield=True):
                    result = await self.database.try_claim_stage(
                        stage_identifier=stage_identifier,
                        token=token,
                        worker_identifier=owner,
                        resource_scope=self.resources.scope,
                        capacities=self.resources.capacities,
                        lease_duration_ns=self.lease_duration_ns,
                        now_utc_ns=self.utc_now_ns(),
                        component=component,
                        provenance=provenance,
                        invocation=invocation,
                        resident_lease=resident_lease,
                    )
                    lease = result.lease
                if result is None:
                    raise RuntimeError("Stage admission ended before ownership was received")
                if lease is None:
                    guard.discard_unadmitted = True
                    yield StageAdmission(result, None)
                    return
                execution = StageExecution(
                    self,
                    lease,
                    guard,
                    provenance,
                    deepcopy(invocation),
                    started_monotonic_ns=self.monotonic_now_ns(),
                )
                body_error: BaseException | None = None
                async with AsyncExitStack() as stack:
                    if lease.resource_lease is not None:
                        await stack.enter_async_context(
                            self.resources.adopt(lease.resource_lease, guard)
                        )
                    if heartbeat is not None:
                        heartbeat.register(execution)
                        try:
                            await anyio.lowlevel.checkpoint()
                            yield StageAdmission(result, execution)
                        finally:
                            heartbeat.unregister(execution)
                    else:
                        async with anyio.create_task_group() as tasks:
                            tasks.start_soon(self._renew, execution)
                            try:
                                await anyio.lowlevel.checkpoint()
                                yield StageAdmission(result, execution)
                            except BaseException as error:
                                body_error = error
                            finally:
                                tasks.cancel_scope.cancel()
                        if body_error is not None:
                            raise body_error
            finally:
                await guard.wait_idle()
                if (
                    lease is not None
                    and (execution is None or not execution.finalized)
                    and guard.seal()
                ):
                    with anyio.fail_after(40, shield=True):
                        await self.database.release_stage_execution(
                            lease,
                            now_utc_ns=self.utc_now_ns(),
                            reason={"kind": "execution_interrupted"},
                        )


def stage_manager(database: Database) -> StageManager:
    resources = current_resource_manager()
    if resources is None or resources.database is not database:
        raise ValueError("Stage execution requires an active Database.managed context")
    return StageManager(database, resources)
