"""Nonblocking stage execution with atomic durable admission and physical guards."""

from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from time import time_ns
from typing import Protocol
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

    async def apply(self, outcome: StageHandlerResult) -> None:
        """Publish an outcome through this execution's continuation or final fence."""

        if isinstance(outcome, StagePublication):
            await self._publish_children(outcome)
            return
        if isinstance(outcome, WaitWork):
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
        async with database.transaction():
            await database.complete_stage(self.lease, now_utc_ns=now)
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


@dataclass(frozen=True, slots=True)
class StageManager:
    database: Database
    resources: ResourceManager
    new_identifier: Callable[[], str] = _identifier
    utc_now_ns: Callable[[], int] = time_ns
    lease_duration_ns: int = 60_000_000_000
    renewal_interval_ns: int = 20_000_000_000

    def __post_init__(self) -> None:
        if self.resources.database is not self.database:
            raise ValueError("Stage execution must use its database's resource manager")
        if not 0 < self.renewal_interval_ns < self.lease_duration_ns:
            raise ValueError("Stage renewal interval must precede expiry")

    async def recover(self) -> None:
        """Recover only expired executions whose physical guard can be obtained."""

        expired = await self.database.expired_stage_executions(
            coordination_scope=self.resources.scope, now_utc_ns=self.utc_now_ns()
        )
        for lease in expired:
            try:
                async with self.resources.execution_guard(
                    lease.token, owner=lease.work_lease.worker_identifier
                ):
                    await self.database.recover_stage_execution(lease, now_utc_ns=self.utc_now_ns())
            except BlockingIOError:
                continue
        await self.resources._recover()

    async def _renew(self, execution: StageExecution) -> None:
        while True:
            await stage_sleep(self.renewal_interval_ns / 1_000_000_000)
            if execution.finalized:
                return
            lease = await self.database.renew_stage(
                execution.lease,
                now_utc_ns=self.utc_now_ns(),
                lease_duration_ns=self.lease_duration_ns,
            )
            if lease is None:
                if execution.finalized:
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
                execution = StageExecution(self, lease, guard, provenance, deepcopy(invocation))
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
