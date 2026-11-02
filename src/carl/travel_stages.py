"""Opt-in durable travel refresh with separate HTTP, local-query and CPU phases."""

import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from time import time_ns
from urllib.parse import urlsplit

import anyio
from pydantic import Field

from carl.core.http import RequestPlan
from carl.core.json import encode_json
from carl.core.models import StrictModel
from carl.core.native_routing import NativeRoutingDataset
from carl.core.resources import ResourceRequest
from carl.core.stages import (
    StageAttemptContext,
    StageDefinition,
    StageOutcome,
    StageWaitKind,
    WaitWork,
)
from carl.core.travel import TravelEstimate
from carl.core.travel_phases import (
    Complete,
    Geocode,
    Matrix,
    MatrixObserved,
    TravelEvent,
    TravelState,
    start_travel,
)
from carl.core.travel_refresh import REFRESH_LISTING_ROUTE_WORK_KIND, TravelRefreshPayload
from carl.core.work import (
    NetworkActivityDefinition,
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
)
from carl.core.worker import TerminalFailureWork
from carl.io.configuration import ConfigurationFailure
from carl.io.http_checkpoints import checkpoint_acquisition, restore_acquisition
from carl.io.native_routing_pool import NativeRoutingBorrowExpired, native_routing_pool
from carl.io.native_routing_protocol import NativeRoutingProtocolError
from carl.io.resource_permits import current_resource_manager, run_cpu
from carl.io.routing import ConfiguredTravelService
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler, WorkStageAdapter
from carl.io.travel_checkpoints import checkpoint_travel_state, restore_travel_state
from carl.io.travel_queries import (
    MAX_TRAVEL_RESPONSE_BYTES,
    HostedRequest,
    hosted_request,
    query_event,
)
from carl.travel_workers import (
    TravelWorkerDependencies,
    build_travel_worker_registry,
    travel_refresh_outcome,
)

_KINDS = {
    phase: ("carl", "travel", "stage", phase)
    for phase in ("prepare", "capture", "native_gate", "local")
}
_CAPABILITY = WorkCapability(kind=REFRESH_LISTING_ROUTE_WORK_KIND, payload_schema_version=1)


class TravelStageReference(StrictModel):
    state_record_identifier: str | None = Field(default=None, min_length=1)
    acquisition_record_identifier: str | None = Field(default=None, min_length=1)
    capture_reason: str | None = Field(default=None, max_length=256)
    configuration_signature: tuple[int, ...] | None = Field(default=None, max_length=5)
    observed_at_utc_ns: int | None = Field(default=None, ge=0)
    request_plan: RequestPlan | None = None
    request_content: str | None = Field(default=None, max_length=16_384)
    credential_reference: str | None = Field(
        default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"
    )


@dataclass(frozen=True, slots=True)
class TravelStageDependencies:
    database: Database
    service: ConfiguredTravelService
    new_identifier: Callable[[], str]
    utc_now_ns: Callable[[], int] = time_ns


def build_travel_stage_registry(dependencies: TravelStageDependencies) -> StageHandlerRegistry:
    database, queries = dependencies.database, dependencies.service.queries
    legacy = TravelWorkerDependencies(
        dependencies.service,
        dependencies.new_identifier,
        lambda: datetime.fromtimestamp(dependencies.utc_now_ns() / 1_000_000_000, UTC),
    )
    component = build_travel_worker_registry(legacy).handlers[0].component

    async def signature() -> tuple[int, ...] | None:
        def read() -> tuple[int, ...] | None:
            try:
                value = queries.directories.configuration_file.stat(follow_symlinks=False)
                return (
                    value.st_dev,
                    value.st_ino,
                    value.st_size,
                    value.st_mtime_ns,
                    value.st_ctime_ns,
                )
            except OSError:
                return None

        return await anyio.to_thread.run_sync(read)

    async def payload(context: StageAttemptContext) -> TravelRefreshPayload:
        work = await database.work_definition(context.work_item_identifier)
        if work.kind != _CAPABILITY.kind or work.payload_schema_version != 1:
            raise ValueError("Travel continuation does not match its work capability")
        return TravelRefreshPayload.model_validate_json(encode_json(work.payload))

    async def restored(
        reference: TravelStageReference, context: StageAttemptContext, work: TravelRefreshPayload
    ) -> TravelState:
        if reference.state_record_identifier is None:
            raise ValueError("Travel continuation has no state checkpoint")
        state = await restore_travel_state(
            database,
            reference.state_record_identifier,
            operation_identifier=context.operation_identifier,
        )
        if (
            state.mode != "refresh"
            or state.destinations != (work.destination,)
            or state.expected_origin_identity != work.origin_identity
        ):
            raise ValueError("Travel checkpoint differs from its immutable work inputs")
        return state

    def continuation(
        context: StageAttemptContext,
        phase: str,
        reference: TravelStageReference,
        *,
        action: Geocode | Matrix | None = None,
        not_before: int = 0,
    ) -> StageDefinition:
        resources = (
            ()
            if phase == "native_gate"
            else (ResourceRequest(name="http" if phase == "capture" else "cpu"),)
        )
        network = None
        if phase == "capture":
            assert action is not None
            lane = "geocode" if isinstance(action, Geocode) else "matrix"
            kind = ("carl", "travel", "openrouteservice", lane)
            network = NetworkActivityDefinition(
                identifier=dependencies.new_identifier(),
                kind=kind,
                operation_identifier=context.operation_identifier,
                network_session_identifier=f"travel:{context.operation_identifier}",
                ordinal=action.action_id,
                attempt=context.attempt,
                scopes=(
                    SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                    SchedulingScope(kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND, identity=kind),
                ),
            )
        return StageDefinition(
            identifier=dependencies.new_identifier(),
            work_item_identifier=context.work_item_identifier,
            operation_identifier=context.operation_identifier,
            kind=_KINDS[phase],
            payload=reference.model_dump(mode="json"),
            resources=resources,
            not_before_utc_ns=not_before,
            network_activity=network,
        )

    def wait(
        context: StageAttemptContext,
        phase: str,
        reference: TravelStageReference,
        *,
        action: Geocode | Matrix | None = None,
        not_before: int = 0,
        kind: StageWaitKind = StageWaitKind.RESOURCE,
    ) -> WaitWork:
        return WaitWork(
            continuation=continuation(
                context, phase, reference, action=action, not_before=not_before
            ),
            wait_kind=kind,
            result={"state": "travel_waiting", "phase": phase},
        )

    async def settle(
        work: TravelRefreshPayload,
        state: TravelState,
        context: StageAttemptContext,
        event: TravelEvent | None = None,
        observed_at_utc_ns: int | None = None,
    ) -> StageOutcome:
        # Consume retained results before a configuration-race continuation.
        # Their cache keys bind the original snapshot, and no new query starts
        # here. A later retry can then resume without losing paid evidence.
        step = (
            await queries.advance(state, event, observed_at_utc_ns=observed_at_utc_ns)
            if event is not None
            else None
        )
        if step is not None:
            state = step.state
        before = await signature()
        snapshot = await queries.snapshot()
        current_signature = await signature()
        if before != current_signature:
            checkpoint = await checkpoint_travel_state(
                database, state, new_identifier=dependencies.new_identifier
            )
            delayed = wait(
                context,
                "prepare",
                TravelStageReference(state_record_identifier=checkpoint.record_identifier),
                not_before=dependencies.utc_now_ns() + 100_000_000,
                kind=StageWaitKind.TIME,
            )
            return delayed.model_copy(
                update={
                    "records": checkpoint.records,
                    "artifacts": checkpoint.artifacts,
                    "outputs": checkpoint.outputs,
                }
            )
        if snapshot.unavailable_reason is not None or snapshot.configuration is None:
            state = start_travel(
                snapshot,
                (work.destination,),
                mode="refresh",
                expected_origin_identity=work.origin_identity,
            )
            step = None
        elif state.snapshot != snapshot:
            return travel_refresh_outcome(
                work,
                TravelEstimate(status="unavailable", unavailable_reason="origin_changed"),
                legacy,
            )
        if step is None:
            step = await queries.advance(state)
        action = step.action
        if isinstance(action, Complete):
            if len(action.estimates) != 1:
                raise ValueError("Travel stage returned a mismatched estimate count")
            return travel_refresh_outcome(work, action.estimates[0], legacy)
        assert isinstance(action, (Geocode, Matrix))
        checkpoint = await checkpoint_travel_state(
            database, step.state, new_identifier=dependencies.new_identifier
        )
        reference = TravelStageReference(
            state_record_identifier=checkpoint.record_identifier,
            configuration_signature=current_signature,
        )
        if action.backend == "openrouteservice":
            request = await run_cpu(hosted_request, action)
            reference = reference.model_copy(
                update={
                    "request_plan": request.plan,
                    "request_content": request.content.decode("utf-8")
                    if request.content is not None
                    else None,
                    "credential_reference": await queries.credential_reference(),
                }
            )
        phase = (
            "capture"
            if action.backend == "openrouteservice"
            else "native_gate"
            if isinstance(action, Matrix)
            else "local"
        )
        outcome = wait(context, phase, reference, action=action)
        return outcome.model_copy(
            update={
                "records": checkpoint.records,
                "artifacts": checkpoint.artifacts,
                "outputs": checkpoint.outputs,
            }
        )

    async def prepare(
        reference: TravelStageReference, context: StageAttemptContext
    ) -> StageOutcome:
        work = await payload(context)
        if reference.state_record_identifier is None:
            state = start_travel(
                await queries.snapshot(),
                (work.destination,),
                mode="refresh",
                expected_origin_identity=work.origin_identity,
            )
        else:
            state = await restored(reference, context, work)
        event = None
        if (
            reference.acquisition_record_identifier is not None
            or reference.capture_reason is not None
        ):
            action = state.awaiting
            if not isinstance(action, (Geocode, Matrix)) or action.backend != "openrouteservice":
                raise ValueError("Travel capture does not match a hosted query")
            acquisition = None
            if reference.acquisition_record_identifier is not None:
                operation, _, _ = await database.object_operation_relations(
                    reference.acquisition_record_identifier, maximum_outputs=0
                )
                if operation != context.operation_identifier:
                    raise ValueError("Travel response belongs to another logical attempt")
                acquisition = await restore_acquisition(
                    database, reference.acquisition_record_identifier
                )
            event = await run_cpu(query_event, action, acquisition, reference.capture_reason)
            if event.reason == "provider_network_failure":
                await queries.cache.block_until(queries.now() + 30)
        return await settle(work, state, context, event, reference.observed_at_utc_ns)

    async def capture(
        reference: TravelStageReference, context: StageAttemptContext
    ) -> StageOutcome:
        work = await payload(context)
        state = await restored(reference, context, work)
        action = state.awaiting
        if not isinstance(action, (Geocode, Matrix)) or action.backend != "openrouteservice":
            raise ValueError("Travel HTTP stage does not match a hosted query")
        if reference.configuration_signature != await signature():
            return wait(context, "prepare", reference)

        async def authorize() -> bool:
            charge = await database.charge_stage_request(
                context,
                request_identity=("travel", "openrouteservice", str(action.action_id)),
                maximum_attempts=1,
                now_utc_ns=dependencies.utc_now_ns(),
            )
            return charge is not None and charge.newly_charged

        plan = reference.request_plan
        if plan is None:
            raise ValueError("Travel HTTP stage has no CPU-prepared request")
        endpoint = urlsplit(plan.url)
        geocode = isinstance(action, Geocode)
        if (
            endpoint.scheme != "https"
            or endpoint.netloc != "api.openrouteservice.org"
            or endpoint.path != ("/geocode/search" if geocode else "/v2/matrix/driving-car")
            or plan.method != ("GET" if geocode else "POST")
            or plan.follow_redirects
            or plan.max_body_bytes > MAX_TRAVEL_RESPONSE_BYTES
            or (reference.request_content is None) != geocode
        ):
            raise ValueError("Travel request does not match its hosted action")
        request = HostedRequest(
            action,
            "geocode" if geocode else "matrix",
            plan,
            reference.request_content.encode("utf-8")
            if reference.request_content is not None
            else None,
        )
        result = await queries.capture(
            request,
            new_identifier=dependencies.new_identifier,
            action_identifier=f"{context.operation_identifier}:{action.action_id}",
            credential_id=reference.credential_reference,
            authorize=authorize,
        )
        if result.next_eligible_at is not None:
            # Wake CPU preparation at the persisted deadline to recheck the
            # configuration before seeking another HTTP reservation.
            return wait(
                context,
                "prepare",
                reference,
                not_before=int(result.next_eligible_at * 1_000_000_000),
                kind=StageWaitKind.TIME,
            )
        checkpoint = None
        if result.acquisition is not None:
            checkpoint = await checkpoint_acquisition(
                result.acquisition,
                data_directory=database.path.parent,
                new_identifier=dependencies.new_identifier,
            )
        next_reference = reference.model_copy(
            update={
                "acquisition_record_identifier": checkpoint.record_identifier
                if checkpoint is not None
                else None,
                "capture_reason": result.reason,
                "observed_at_utc_ns": int(queries.now() * 1_000_000_000),
            }
        )
        outcome = wait(context, "prepare", next_reference)
        if checkpoint is not None:
            outcome = outcome.model_copy(
                update={
                    "records": checkpoint.records,
                    "artifacts": checkpoint.artifacts,
                    "outputs": tuple(
                        output.model_copy(
                            update={"name": ("travel", "http", str(action.action_id), *output.name)}
                        )
                        for output in checkpoint.outputs
                    ),
                }
            )
        return outcome

    async def local(reference: TravelStageReference, context: StageAttemptContext) -> StageOutcome:
        work = await payload(context)
        state = await restored(reference, context, work)
        action = state.awaiting
        if not isinstance(action, (Geocode, Matrix)) or action.backend == "openrouteservice":
            raise ValueError("Travel local stage does not match a local query")
        if reference.configuration_signature != await signature():
            return wait(context, "prepare", reference)
        if isinstance(action, Matrix):
            manager = current_resource_manager()
            configuration = state.snapshot.osrm
            if manager is None or configuration is None:
                raise ValueError("Native travel stage requires its managed dataset")
            borrow = await native_routing_pool(manager).try_borrow(
                NativeRoutingDataset(
                    dataset_path=configuration.dataset_path,
                    dataset_identifier=configuration.dataset_identifier,
                    use_mmap=configuration.use_mmap,
                )
            )
            if borrow is None:
                return wait(
                    context,
                    "native_gate",
                    reference,
                    not_before=dependencies.utc_now_ns() + 100_000_000,
                    kind=StageWaitKind.OWNERSHIP,
                )
            try:
                async with borrow:
                    event = await queries.local(state, action, borrow=borrow)
            except NativeRoutingBorrowExpired:
                return wait(
                    context,
                    "native_gate",
                    reference,
                    not_before=dependencies.utc_now_ns() + 100_000_000,
                    kind=StageWaitKind.OWNERSHIP,
                )
            except TimeoutError:
                event = MatrixObserved(action_id=action.action_id, reason="osrm_timeout")
            except NativeRoutingProtocolError:
                event = MatrixObserved(action_id=action.action_id, reason="osrm_unavailable")
        else:
            event = await queries.local(state, action)
        return await settle(work, state, context, event, int(queries.now() * 1_000_000_000))

    async def native_gate(
        reference: TravelStageReference, context: StageAttemptContext
    ) -> StageOutcome:
        work = await payload(context)
        state = await restored(reference, context, work)
        action = state.awaiting
        configuration = state.snapshot.osrm
        manager = current_resource_manager()
        if (
            not isinstance(action, Matrix)
            or action.backend != "osrm"
            or configuration is None
            or manager is None
        ):
            raise ValueError("Native travel gate requires a managed local matrix")
        if reference.configuration_signature != await signature():
            return wait(context, "prepare", reference)
        borrow = await native_routing_pool(manager).try_borrow(
            NativeRoutingDataset(
                dataset_path=configuration.dataset_path,
                dataset_identifier=configuration.dataset_identifier,
                use_mmap=configuration.use_mmap,
            )
        )
        if borrow is None:
            return wait(
                context,
                "native_gate",
                reference,
                not_before=dependencies.utc_now_ns() + 100_000_000,
                kind=StageWaitKind.OWNERSHIP,
            )
        await borrow.aclose()
        return wait(context, "local", reference)

    def safe(
        handler: Callable[[TravelStageReference, StageAttemptContext], Awaitable[StageOutcome]],
    ) -> Callable[[TravelStageReference, StageAttemptContext], Awaitable[StageOutcome]]:
        async def run(
            reference: TravelStageReference, context: StageAttemptContext
        ) -> StageOutcome:
            try:
                return await handler(reference, context)
            except (
                KeyError,
                ValueError,
                OSError,
                sqlite3.Error,
                ConfigurationFailure,
                NativeRoutingProtocolError,
            ) as error:
                return TerminalFailureWork(
                    error={
                        "kind": "travel_stage_input_or_checkpoint_failure",
                        "type": type(error).__name__,
                    },
                    result={"state": "failed"},
                )

        return run

    def initial(work: WorkDefinition, identifier: str, operation: str) -> StageDefinition:
        return StageDefinition(
            identifier=identifier,
            work_item_identifier=work.identifier,
            operation_identifier=operation,
            kind=_KINDS["prepare"],
            payload=TravelStageReference().model_dump(mode="json"),
            resources=(ResourceRequest(name="cpu"),),
            not_before_utc_ns=work.not_before_utc_ns,
        )

    return StageHandlerRegistry(
        adapters=(
            WorkStageAdapter(capability=_CAPABILITY, component=component, initial_stage=initial),
        ),
        handlers=tuple(
            TypedStageHandler(
                kind=_KINDS[phase],
                schema_version=1,
                payload_type=TravelStageReference,
                handler=safe(handler),
            )
            for phase, handler in (
                ("prepare", prepare),
                ("capture", capture),
                ("native_gate", native_gate),
                ("local", local),
            )
        ),
    )
