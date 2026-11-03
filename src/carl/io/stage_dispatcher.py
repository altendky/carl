"""Bounded discovery and fixed execution tasks for the durable stage queue."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import cast

import anyio

from carl.core.fair_scheduling import FairDispatchCursor
from carl.core.models import CodeProvenance, JsonValue
from carl.core.resources import ResourceRequest
from carl.core.scheduler_activity import (
    SchedulerBlocker,
    SchedulerResourceName,
    SchedulerWaitObservation,
)
from carl.core.work import WorkCancellationRequestedError, WorkCapability
from carl.core.work_dispatch import DispatchCandidate, ExpiryCursor
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.resource_permits import ResourceLeaseLost
from carl.io.sqlite import LeaseLostError
from carl.io.stage_policy import invalid_initial_stage
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import StageManager


@dataclass(frozen=True, slots=True)
class _PendingAdmission:
    requirements: tuple[ResourceRequest, ...]
    reservations: tuple[ResourceRequest, ...]


@dataclass
class StageDispatcher:
    manager: StageManager
    registry: StageHandlerRegistry
    residents: ResidentSessionPool
    provenance: CodeProvenance
    invocation: dict[str, JsonValue]
    submit_owned: Callable[[str], Awaitable[bool]]
    is_owned: Callable[[tuple[str, ...]], bool]
    eligible_identifiers: tuple[str, ...] | None = None
    execution_count: int | None = None
    metadata_count: int = 4
    page_size: int = 32
    poll_seconds: float = 0.25
    _queued: set[str] = field(default_factory=set, init=False)
    _pending_admissions: dict[str, _PendingAdmission] = field(default_factory=dict, init=False)
    _after: FairDispatchCursor | None = field(default=None, init=False)
    _pass_time: int | None = field(default=None, init=False)
    _expired_after: ExpiryCursor | None = field(default=None, init=False)
    _expired_time: int | None = field(default=None, init=False)
    _progress_after: str | None = field(default=None, init=False)
    _progress_reconcile_at: int = field(default=0, init=False)

    async def _progress(self) -> bool:
        now = self.manager.utc_now_ns()
        drained = await self.manager.database.drain_progress_notifications(now_utc_ns=now)
        more = drained.more or drained.woke > 0
        if now >= self._progress_reconcile_at:
            await self.manager.database.prune_scheduler_telemetry(now_utc_ns=now, limit=128)
            page = await self.manager.database.reconcile_progress_waits(
                after=self._progress_after, limit=self.page_size, now_utc_ns=now
            )
            self._progress_after = page.next_cursor if page.more else None
            # Reconciliation only inspects bounded durable metadata. A quiet
            # dependency never needs another handler turn to stay asleep.
            self._progress_reconcile_at = now + 1_000_000_000
            more = more or page.woke > 0
        return more

    def _wait_observation(
        self,
        candidate: DispatchCandidate,
        reason: SchedulerBlocker,
        resource_names: tuple[str, ...] = (),
    ) -> SchedulerWaitObservation:
        known = {
            "http",
            "cpu",
            "analysis",
            "routing_engines",
            "resident_sessions",
            "resident_state_bytes",
        }
        return SchedulerWaitObservation(
            work_identifier=candidate.work_item_identifier,
            stage_identifier=candidate.stage_identifier,
            reason=reason,
            resource_names=cast(
                tuple[SchedulerResourceName, ...],
                tuple(name for name in resource_names if name in known)[:8],
            ),
            source="advisory",
        )

    def _requirements(self, candidate: DispatchCandidate) -> tuple[ResourceRequest, ...]:
        if candidate.stage_identifier is not None:
            return candidate.stage_resources
        return self.registry.require_adapter(candidate.capability).initial_resources or ()

    async def _recover(self) -> None:
        await self.manager.recover()
        await self.residents.recover()
        if self._expired_time is None:
            self._expired_time = self.manager.utc_now_ns()
        page = await self.manager.database.expired_legacy_work_candidates(
            now_utc_ns=self._expired_time,
            limit=self.page_size,
            after=self._expired_after,
            eligible_identifiers=self.eligible_identifiers,
        )
        for candidate in page.items:
            await self.manager.database.recover_expired_legacy_work(
                candidate,
                now_utc_ns=self.manager.utc_now_ns(),
                event_identifier=self.manager.new_identifier(),
            )
            self._expired_after = candidate.cursor
        if not page.has_more:
            self._expired_after = None
            self._expired_time = None

    async def _execute(
        self, receive: anyio.abc.ObjectReceiveStream[DispatchCandidate], metadata_lane: bool = False
    ) -> None:
        async with receive:
            async for candidate in receive:
                try:
                    adapter = self.registry.require_adapter(candidate.capability)
                    identifier = candidate.stage_identifier
                    if identifier is None:
                        snapshot = await self.manager.database.work_dispatch_snapshot(
                            candidate.work_item_identifier
                        )
                        try:
                            adapter = self.registry.require_adapter(
                                WorkCapability(
                                    kind=snapshot.definition.kind,
                                    payload_schema_version=snapshot.definition.payload_schema_version,
                                )
                            )
                        except KeyError:
                            # A manual retry/cutover can change capability after
                            # metadata discovery. Unsupported work stays queued.
                            continue
                        stage_identifier, operation_identifier = (
                            self.manager.new_identifier(),
                            self.manager.new_identifier(),
                        )
                        try:
                            stage = adapter.initial_stage(
                                snapshot.definition, stage_identifier, operation_identifier
                            )
                        except Exception as error:
                            stage = invalid_initial_stage(
                                candidate.work_item_identifier,
                                stage_identifier,
                                operation_identifier,
                                error,
                            )
                        if stage.resident_session_identifier is not None:
                            await self.residents.submit(
                                stage, component=adapter.component, expected_snapshot=snapshot
                            )
                            continue
                        registration = await self.manager.database.try_register_initial_stage(
                            stage,
                            expected_snapshot=snapshot,
                            created_at_utc_ns=self.manager.utc_now_ns(),
                        )
                        identifier = registration.stage_identifier
                        if identifier is None:
                            continue
                    stage = await self.manager.database.stage(identifier)
                    if metadata_lane and stage.resources:
                        # Fresh payload/version changes may invalidate a page's
                        # routing hint. Keep execution out of the metadata lane.
                        continue
                    if stage.resident_session_identifier is not None:
                        if not self.residents.owns(stage.resident_session_identifier):
                            snapshot = await self.manager.database.work_dispatch_snapshot(
                                candidate.work_item_identifier
                            )
                            await self.residents.submit(
                                stage, component=adapter.component, expected_snapshot=snapshot
                            )
                        continue
                    if self.is_owned(stage.kind):
                        await self.submit_owned(identifier)
                        continue
                    async with self.manager.admit(
                        identifier,
                        component=adapter.component,
                        provenance=self.provenance,
                        invocation=self.invocation,
                    ) as admission:
                        # Atomic admission now owns the physical ledger, or has
                        # declined. Its hint must not survive handler execution.
                        self._pending_admissions.pop(candidate.work_item_identifier, None)
                        if admission.execution is not None:
                            await self.registry.execute(admission.execution)
                except (WorkCancellationRequestedError, ResourceLeaseLost, LeaseLostError):
                    # Exact fences/recovery own cancellation and interrupted phases.
                    # A race with another dispatcher is an ordinary declined turn.
                    pass
                except ExceptionGroup as error:
                    _, unexpected = error.split(
                        (WorkCancellationRequestedError, ResourceLeaseLost, LeaseLostError)
                    )
                    if unexpected is not None:
                        raise unexpected from error
                finally:
                    self._pending_admissions.pop(candidate.work_item_identifier, None)
                    self._queued.discard(candidate.work_item_identifier)

    async def run(self) -> None:
        capacities = self.manager.resources.capacities
        capacity_count = sum(capacities.get(name, 0) for name in ("cpu", "http", "analysis"))
        count = (
            capacity_count
            if self.execution_count is None
            else min(self.execution_count, capacity_count)
        )
        if count < 1 or self.metadata_count < 1:
            raise ValueError("Dispatcher runner counts must be positive")
        # Zero-buffer channels reserve an idle runner without creating per-row
        # tasks. Each runner admits and executes in its own resource context.
        execution_send, execution_receive = anyio.create_memory_object_stream[DispatchCandidate](0)
        metadata_send, metadata_receive = anyio.create_memory_object_stream[DispatchCandidate](0)
        async with anyio.create_task_group() as tasks, execution_send, metadata_send:
            for _ in range(count):
                tasks.start_soon(self._execute, execution_receive.clone())
            for _ in range(self.metadata_count):
                tasks.start_soon(self._execute, metadata_receive.clone(), True)
            await execution_receive.aclose()
            await metadata_receive.aclose()
            while True:
                # Recovery is independent of the advisory capacity filter.
                await self._recover()
                progress_pending = await self._progress()
                if self._pass_time is None:
                    self._pass_time = self.manager.utc_now_ns()
                page = await self.manager.database.fair_dispatch_candidates(
                    scope=self.manager.resources.scope,
                    supported_capabilities=tuple(
                        adapter.capability for adapter in self.registry.adapters
                    ),
                    now_utc_ns=self._pass_time,
                    limit=self.page_size,
                    after=self._after,
                    eligible_identifiers=self.eligible_identifiers,
                    excluded_identifiers=tuple(self._queued)
                    + self.residents.owned_work_identifiers,
                )
                execution_ready = execution_send.statistics().tasks_waiting_receive > 0
                metadata_ready = metadata_send.statistics().tasks_waiting_receive > 0
                offers = tuple(
                    candidate.work_item_identifier
                    for candidate in page.items
                    if (execution_ready if self._requirements(candidate) else metadata_ready)
                    and (candidate.stage_kind is None or not self.is_owned(candidate.stage_kind))
                )
                if offers:
                    # Advertise the alternatives this idle driver can execute
                    # before a stale page can bypass the current fair winner.
                    # Busy lanes do not renew speculative readiness leases.
                    await self.manager.database.offer_fair_dispatch_candidates(
                        scope=self.manager.resources.scope,
                        work_identifiers=offers,
                        now_utc_ns=self.manager.utc_now_ns(),
                    )
                # Capture hints before the ledger read: a runner may commit and
                # clear its hint while that read is in flight. Full reservations
                # may count twice for one page, rather than disappear from both.
                pending = tuple(self._pending_admissions.values())
                occupied = await self.manager.database.resource_occupancy(
                    self.manager.resources.scope
                )
                for admission in pending:
                    for request in admission.reservations:
                        occupied[request.name] = occupied.get(request.name, 0) + request.units
                weighted_probe_pending = any(
                    not admission.reservations
                    and any(
                        request.name == "cpu" and request.units > 1
                        for request in admission.requirements
                    )
                    for admission in pending
                )
                handed_off = False
                observations: list[SchedulerWaitObservation] = []
                for candidate in page.items:
                    if candidate.work_item_identifier in self._queued:
                        continue
                    if candidate.resident_session_identifier is not None and self.residents.owns(
                        candidate.resident_session_identifier
                    ):
                        continue
                    requirements = self._requirements(candidate)
                    if not (execution_ready if requirements else metadata_ready):
                        # A receiver can become idle during the occupancy read.
                        # Refresh and advertise that lane before handing it work.
                        if len(observations) < 32:
                            observations.append(
                                self._wait_observation(
                                    candidate,
                                    "execution_runner" if requirements else "metadata_runner",
                                )
                            )
                        continue
                    weighted_cpu = any(
                        request.name == "cpu" and request.units > 1 for request in requirements
                    )
                    weighted_probe = weighted_cpu and any(
                        request.name == "cpu"
                        and occupied.get("cpu", 0) + request.units > capacities.get("cpu", 0)
                        for request in requirements
                    )
                    unavailable = tuple(
                        request.name
                        for request in requirements
                        if (request.name != "cpu" or not weighted_cpu)
                        and occupied.get(request.name, 0) + request.units
                        > capacities.get(request.name, 0)
                    )
                    if weighted_probe and weighted_probe_pending:
                        unavailable += ("cpu",)
                    if unavailable:
                        if len(observations) < 32:
                            observations.append(
                                self._wait_observation(
                                    candidate,
                                    "resource",
                                    unavailable,
                                )
                            )
                        continue
                    if candidate.resident_session_identifier is not None and occupied.get(
                        "resident_sessions", 0
                    ) >= capacities.get("resident_sessions", 0):
                        if len(observations) < 32:
                            observations.append(
                                self._wait_observation(
                                    candidate, "resource", ("resident_sessions",)
                                )
                            )
                        continue
                    # Initial adapters may need a payload point read. They use
                    # the execution lane until their actual stage is registered.
                    channel = metadata_send if not requirements else execution_send
                    self._queued.add(candidate.work_item_identifier)
                    try:
                        channel.send_nowait(candidate)
                        # One nonfitting CPU probe can establish an aged drain,
                        # but cannot reserve any budget while it awaits that
                        # decision. It may race a later physical grant; atomic
                        # admission remains authoritative for that bounded probe.
                        reservations = () if weighted_probe else requirements
                        self._pending_admissions[candidate.work_item_identifier] = (
                            _PendingAdmission(requirements, reservations)
                        )
                        for request in reservations:
                            occupied[request.name] = occupied.get(request.name, 0) + request.units
                        weighted_probe_pending = weighted_probe_pending or weighted_probe
                        handed_off = True
                    except anyio.WouldBlock:
                        self._queued.remove(candidate.work_item_identifier)
                        if len(observations) < 32:
                            observations.append(
                                self._wait_observation(
                                    candidate,
                                    "execution_runner" if requirements else "metadata_runner",
                                )
                            )
                if observations:
                    await self.manager.database.observe_scheduler_waits(
                        coordination_scope=self.manager.resources.scope,
                        observations=tuple(observations),
                        now_utc_ns=self.manager.utc_now_ns(),
                    )
                self._after = page.next_cursor
                if not page.has_more:
                    self._after = None
                    self._pass_time = None
                    if handed_off or progress_pending:
                        await anyio.lowlevel.checkpoint()
                    else:
                        await anyio.sleep(self.poll_seconds)
                else:
                    await anyio.lowlevel.checkpoint()
