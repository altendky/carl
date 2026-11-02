"""Bounded, serial session actors with independent durable ownership.

Only admitted residents have tasks or clients. The pool's single heartbeat owns
renewal outside actor task scopes so a provider context can span stage boundaries.
"""

from __future__ import annotations

import fcntl
import os
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import Protocol, cast
from uuid import uuid4

import anyio

from carl.core.components import Component
from carl.core.models import CodeProvenance, JsonValue
from carl.core.residents import ResidentDefinition, ResidentLease, ResidentState
from carl.core.stages import StageDefinition, WaitWork
from carl.core.worker import TerminalFailureWork
from carl.io.resource_permits import ResourceLeaseLost
from carl.io.stage_publication import StagePublication
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import StageExecution, StageManager


class ResidentValue(Protocol):
    def retained_bytes(self) -> int: ...

    def is_closed(self) -> bool: ...


class ResidentStateLimitExceeded(ValueError):
    """The completed phase must be retained before this session is disposed."""


@dataclass(frozen=True, slots=True)
class ResidentHandle[T: ResidentValue]:
    value: T
    incarnation: str


@dataclass(slots=True)
class _Actor:
    lease: ResidentLease
    descriptor: int
    stage: StageDefinition
    component: Component
    stack: AsyncExitStack = field(default_factory=AsyncExitStack)
    handle: ResidentHandle[ResidentValue] | None = None
    task_identifier: int | None = None
    cancel_scope: anyio.CancelScope | None = None
    execution: StageExecution | None = None
    error: str | None = None
    parked: bool = False
    closing: bool = False
    done: anyio.Event = field(default_factory=anyio.Event)


@dataclass(slots=True)
class ResidentSessionPool:
    manager: StageManager
    registry: StageHandlerRegistry
    provenance: CodeProvenance
    invocation: dict[str, JsonValue]
    state_bytes: int = 4 * 1024 * 1024
    maximum_idle_ns: int = 30_000_000_000
    poll_seconds: float = 0.25
    _actors: dict[str, _Actor] = field(default_factory=dict, init=False)
    _quarantined: dict[str, _Actor] = field(default_factory=dict, init=False)
    _tasks: anyio.abc.TaskGroup | None = field(default=None, init=False)
    _closing: bool = field(default=False, init=False)
    errors: dict[str, str] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if self.state_bytes < 1 or self.maximum_idle_ns < 1 or self.poll_seconds <= 0:
            raise ValueError("Resident pool limits must be positive")
        capacity = self.manager.resources.capacities.get("resident_state_bytes", 0)
        if self.state_bytes > capacity:
            raise ValueError("Per-resident byte reservation exceeds the shared state capacity")

    @property
    def active_count(self) -> int:
        return len(self._actors) + len(self._quarantined)

    async def wait_idle(self) -> None:
        """Wait for currently admitted actors; queued database work has no task."""

        for actor in tuple(self._actors.values()):
            await actor.done.wait()

    @asynccontextmanager
    async def managed(self) -> AsyncIterator[ResidentSessionPool]:
        if self._tasks is not None or self._closing:
            raise ValueError("Resident pool can be started only once")
        async with anyio.create_task_group() as tasks:
            self._tasks = tasks
            tasks.start_soon(self._heartbeat)
            try:
                yield self
            finally:
                self._closing = True
                for actor in tuple(self._actors.values()):
                    if actor.cancel_scope is not None:
                        actor.cancel_scope.cancel()
                with anyio.CancelScope(shield=True):
                    for actor in tuple(self._actors.values()):
                        await actor.done.wait()
                tasks.cancel_scope.cancel()
                self._tasks = None

    def _owned_actor(self, identifier: str) -> _Actor:
        actor = self._actors.get(identifier)
        if actor is None or actor.task_identifier != anyio.get_current_task().id:
            raise ResourceLeaseLost("Resident session belongs to another actor")
        if actor.error is not None:
            raise ResourceLeaseLost("Resident session ownership was lost")
        return actor

    def peek(self, identifier: str) -> ResidentHandle[ResidentValue] | None:
        return self._owned_actor(identifier).handle

    async def get_or_open[T: ResidentValue](
        self,
        identifier: str,
        factory: Callable[[str], AbstractAsyncContextManager[T]],
    ) -> ResidentHandle[T]:
        actor = self._owned_actor(identifier)
        if actor.execution is None or not any(
            request.name == "http" for request in actor.execution.lease.definition.resources
        ):
            raise ResourceLeaseLost("Resident clients may open only in an admitted HTTP stage")
        if actor.handle is not None:
            # Each incarnation has one marketplace value type, set by its factory.
            return cast(ResidentHandle[T], actor.handle)
        value = await actor.stack.enter_async_context(factory(identifier))
        handle = ResidentHandle(value, identifier)
        actor.handle = cast(ResidentHandle[ResidentValue], handle)
        self._check_retained(actor)
        lease = await self.manager.database.transition_resident_session(
            actor.lease, state=ResidentState.READY, now_utc_ns=self.manager.utc_now_ns()
        )
        if lease is None:
            raise ResourceLeaseLost("Resident ownership changed while opening its client")
        actor.lease = lease
        return handle

    async def close(self, identifier: str) -> None:
        actor = self._owned_actor(identifier)
        await self._close(actor)

    async def _close(self, actor: _Actor) -> None:
        try:
            await actor.stack.aclose()
        except BaseException:
            if actor.handle is not None and actor.handle.value.is_closed():
                actor.stack = AsyncExitStack()
                actor.handle = None
            raise
        if actor.handle is not None and not actor.handle.value.is_closed():
            raise ResourceLeaseLost("Provider finalizer did not prove physical client disposal")
        actor.stack = AsyncExitStack()
        actor.handle = None

    def _check_retained(self, actor: _Actor) -> None:
        if actor.handle is None:
            return
        size = actor.handle.value.retained_bytes()
        if type(size) is not int or not 0 <= size <= actor.lease.definition.state_bytes:
            raise ResidentStateLimitExceeded(
                "Resident application state exceeds its reserved byte budget"
            )

    def register(self, execution: StageExecution) -> None:
        identifier = execution.lease.definition.resident_session_identifier
        if identifier is None:
            raise ValueError("Resident heartbeat requires a resident-affined stage")
        actor = self._owned_actor(identifier)
        if actor.execution is not None:
            raise ResourceLeaseLost("A resident already has an active stage")
        actor.execution = execution

    def unregister(self, execution: StageExecution) -> None:
        identifier = execution.lease.definition.resident_session_identifier
        if identifier is not None:
            actor = self._actors.get(identifier)
            if actor is not None and actor.execution is execution:
                actor.execution = None

    async def submit(self, stage: StageDefinition, *, component: Component) -> bool:
        """Try admission before allocating a task; the caller retains blocked work."""

        if self._tasks is None or self._closing:
            raise ValueError("Resident pool is not running")
        await self.recover()
        identifier, token = str(uuid4()), str(uuid4())
        resources = self.manager.resources
        descriptor = resources._guard(token)
        lease: ResidentLease | None = None
        committed = False
        ledger_committed = False
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with anyio.CancelScope(shield=True):
                async with self.manager.database.transaction():
                    lease = await self.manager.database.try_acquire_resident_session(
                        ResidentDefinition(
                            identifier=identifier,
                            work_item_identifier=stage.work_item_identifier,
                            operation_identifier=stage.operation_identifier,
                            state_bytes=self.state_bytes,
                        ),
                        token=token,
                        owner=resources.owner,
                        scope=resources.scope,
                        capacities=resources.capacities,
                        now_utc_ns=self.manager.utc_now_ns(),
                        lease_duration_ns=self.manager.lease_duration_ns,
                    )
                    if lease is None:
                        return False
                    try:
                        existing = await self.manager.database.stage(stage.identifier)
                    except KeyError:
                        stage = stage.model_copy(update={"resident_session_identifier": identifier})
                        await self.manager.database.register_stage(
                            stage, created_at_utc_ns=self.manager.utc_now_ns()
                        )
                    else:
                        previous = existing.resident_session_identifier
                        if (
                            previous is None
                            or not await self.manager.database.rebind_resident_stage(
                                stage.identifier,
                                previous,
                                lease,
                                now_utc_ns=self.manager.utc_now_ns(),
                            )
                        ):
                            raise ResourceLeaseLost(
                                "Pending stage cannot transfer resident ownership"
                            )
                        stage = await self.manager.database.stage(stage.identifier)
                ledger_committed = True
                actor = _Actor(lease, descriptor, stage, component)
                self._actors[identifier] = actor
                self._tasks.start_soon(self._run, actor)
                committed = True
                return True
        finally:
            if not committed:
                os.close(descriptor)
                if not ledger_committed:
                    (resources.guard_directory / f"{token}.lock").unlink(missing_ok=True)
        return False

    async def _run(self, actor: _Actor) -> None:
        identifier = actor.lease.definition.identifier
        actor.task_identifier = anyio.get_current_task().id
        try:
            with anyio.CancelScope() as scope:
                actor.cancel_scope = scope
                try:
                    await self._run_stages(actor)
                finally:
                    actor.closing = True
                    await self._close(actor)
        except BaseException as error:
            # Tracebacks can keep whole response buffers and cookie jars alive.
            # Keep only a bounded diagnostic class name after a failed phase.
            actor.error = type(error).__name__
        finally:
            with anyio.CancelScope(shield=True):
                if actor.error is not None:
                    self.errors[identifier] = actor.error
                    if len(self.errors) > 32:
                        del self.errors[next(iter(self.errors))]
                try:
                    if actor.handle is None:
                        await self.manager.database.finish_resident_session(
                            actor.lease,
                            state=ResidentState.PARKED if actor.parked else ResidentState.CLOSED,
                            now_utc_ns=self.manager.utc_now_ns(),
                        )
                        os.close(actor.descriptor)
                    else:
                        await self.manager.database.transition_resident_session(
                            actor.lease,
                            state=ResidentState.QUARANTINED,
                            now_utc_ns=self.manager.utc_now_ns(),
                        )
                        # Retain the guard and failed client until physical process death.
                        self._quarantined[identifier] = actor
                except BaseException:
                    # Ledger failure must not leave shutdown waiting on an event
                    # that no task can ever set, nor discard uncertain ownership.
                    self._quarantined[identifier] = actor
                    raise
                finally:
                    self._actors.pop(identifier, None)
                    actor.done.set()

    async def _run_stages(self, actor: _Actor) -> None:
        idle_since = self.manager.utc_now_ns()
        while not self._closing:
            if await self.manager.database.work_cancellation_requested(
                actor.stage.work_item_identifier
            ):
                return
            stage = actor.stage
            wait_until: int | None = None
            async with self.manager.admit(
                stage.identifier,
                component=actor.component,
                provenance=self.provenance,
                invocation=self.invocation,
                worker_identifier=actor.lease.owner,
                resident_lease=actor.lease,
                heartbeat=self,
            ) as admission:
                execution = admission.execution
                if execution is None:
                    wait_until = admission.result.next_eligible_at_utc_ns
                else:
                    result = await self.registry.run(execution)
                    outcome = result.outcome if isinstance(result, StagePublication) else result
                    try:
                        self._check_retained(actor)
                    except ResidentStateLimitExceeded:
                        # A completed HTTP phase can already have paid for a
                        # response. Keep its evidence and stop, rather than
                        # interrupting publication and later fetching it again.
                        outcome = TerminalFailureWork(
                            inputs=outcome.inputs,
                            records=outcome.records,
                            artifacts=outcome.artifacts,
                            outputs=outcome.outputs,
                            error={"kind": "resident_state_limit"},
                            result={
                                "state": "resident_state_limit",
                                "completed_phase": outcome.result,
                            },
                        )
                        result = (
                            StagePublication(outcome, result.children)
                            if isinstance(result, StagePublication)
                            else outcome
                        )
                    if isinstance(outcome, WaitWork) and (
                        outcome.continuation.resident_session_identifier
                        != actor.lease.definition.identifier
                    ):
                        raise ValueError(
                            "Resident continuations must retain exact session affinity"
                        )
                    if not isinstance(outcome, WaitWork):
                        # The committed work can become terminal before the
                        # publishing coroutine resumes from transaction exit.
                        # Stop renewal before that window, through guard cleanup.
                        actor.closing = True
                    await execution.apply(result)
                    if not isinstance(outcome, WaitWork):
                        return
                    continuation = outcome.continuation
                    actor.stage = continuation
                    # Admission-only continuations can poll the same workflow
                    # state repeatedly. They must not keep residency forever by
                    # describing each poll as fresh progress.
                    if stage.resources or continuation.kind != stage.kind:
                        idle_since = self.manager.utc_now_ns()
                    wait_until = continuation.not_before_utc_ns
                    # Publication can include large decoded BytesDraft values.
                    # Only the small continuation may survive an execution wait.
                    del result, outcome
            now = self.manager.utc_now_ns()
            if now - idle_since >= self.maximum_idle_ns or (
                wait_until is not None and wait_until - now > self.maximum_idle_ns
            ):
                actor.parked = True
                return
            if wait_until is not None and wait_until > now:
                await anyio.sleep(min(self.poll_seconds, (wait_until - now) / 1_000_000_000))
            elif admission.execution is None:
                await anyio.sleep(self.poll_seconds)

    async def _heartbeat(self) -> None:
        interval = self.manager.renewal_interval_ns / 1_000_000_000
        while True:
            await anyio.sleep(interval)
            for actor in tuple(self._actors.values()):
                if actor.closing:
                    continue
                try:
                    lease = await self.manager.database.renew_resident_session(
                        actor.lease,
                        now_utc_ns=self.manager.utc_now_ns(),
                        lease_duration_ns=self.manager.lease_duration_ns,
                    )
                    if lease is None:
                        if actor.closing:
                            continue
                        raise ResourceLeaseLost("Resident ownership expired or changed")
                    actor.lease = lease
                    execution = actor.execution
                    if execution is not None and not execution.finalizing:
                        renewed = await self.manager.database.renew_stage(
                            execution.lease,
                            now_utc_ns=self.manager.utc_now_ns(),
                            lease_duration_ns=self.manager.lease_duration_ns,
                        )
                        if renewed is None and not execution.finalizing:
                            raise ResourceLeaseLost("Resident stage ownership expired or changed")
                        if renewed is not None:
                            execution.lease = renewed
                except Exception as error:
                    if actor.closing:
                        continue
                    actor.error = type(error).__name__
                    if actor.cancel_scope is not None:
                        actor.cancel_scope.cancel()

    async def recover(self) -> None:
        """Expiry is only a recovery candidate; guard locks prove physical disposal."""

        resources = self.manager.resources
        for lease in await self.manager.database.expired_resident_sessions(
            scope=resources.scope, now_utc_ns=self.manager.utc_now_ns()
        ):
            descriptor = resources._guard(lease.token)
            try:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                stage_descriptor: int | None = None
                try:
                    if lease.active_stage_token is not None:
                        stage_descriptor = resources._guard(lease.active_stage_token)
                        try:
                            fcntl.flock(stage_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            continue
                    await self.manager.database.recover_resident_session(
                        lease, now_utc_ns=self.manager.utc_now_ns()
                    )
                finally:
                    if stage_descriptor is not None:
                        os.close(stage_descriptor)
            finally:
                os.close(descriptor)
