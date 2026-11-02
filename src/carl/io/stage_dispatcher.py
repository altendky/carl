"""Bounded discovery and fixed execution tasks for the durable stage queue."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import anyio

from carl.core.models import CodeProvenance, JsonValue
from carl.core.work import WorkCancellationRequestedError, WorkCapability
from carl.core.work_dispatch import DispatchCandidate, DispatchCursor, ExpiryCursor
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.resource_permits import ResourceLeaseLost
from carl.io.sqlite import LeaseLostError
from carl.io.stage_policy import invalid_initial_stage
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import StageManager


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
    _after: DispatchCursor | None = field(default=None, init=False)
    _pass_time: int | None = field(default=None, init=False)
    _expired_after: ExpiryCursor | None = field(default=None, init=False)
    _expired_time: int | None = field(default=None, init=False)

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
                    self._queued.discard(candidate.work_item_identifier)

    async def run(self) -> None:
        capacities = self.manager.resources.capacities
        count = self.execution_count
        if count is None:
            count = sum(capacities.get(name, 0) for name in ("cpu", "http", "analysis"))
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
                if self._pass_time is None:
                    self._pass_time = self.manager.utc_now_ns()
                page = await self.manager.database.dispatch_candidates(
                    supported_capabilities=tuple(
                        adapter.capability for adapter in self.registry.adapters
                    ),
                    now_utc_ns=self._pass_time,
                    limit=self.page_size,
                    after=self._after,
                    eligible_identifiers=self.eligible_identifiers,
                )
                occupied = await self.manager.database.resource_occupancy(
                    self.manager.resources.scope
                )
                for candidate in page.items:
                    if candidate.work_item_identifier in self._queued:
                        continue
                    if candidate.resident_session_identifier is not None and self.residents.owns(
                        candidate.resident_session_identifier
                    ):
                        continue
                    adapter = self.registry.require_adapter(candidate.capability)
                    requirements = (
                        candidate.stage_resources
                        if candidate.stage_identifier is not None
                        else adapter.initial_resources
                    )
                    requirements = () if requirements is None else requirements
                    if any(
                        occupied.get(request.name, 0) + request.units
                        > capacities.get(request.name, 0)
                        for request in requirements
                    ):
                        continue
                    if candidate.resident_session_identifier is not None and occupied.get(
                        "resident_sessions", 0
                    ) >= capacities.get("resident_sessions", 0):
                        continue
                    # Initial adapters may need a payload point read. They use
                    # the execution lane until their actual stage is registered.
                    channel = metadata_send if not requirements else execution_send
                    self._queued.add(candidate.work_item_identifier)
                    try:
                        channel.send_nowait(candidate)
                    except anyio.WouldBlock:
                        self._queued.remove(candidate.work_item_identifier)
                self._after = page.next_cursor
                if not page.has_more:
                    self._after = None
                    self._pass_time = None
                    await anyio.sleep(self.poll_seconds)
                else:
                    await anyio.lowlevel.checkpoint()
