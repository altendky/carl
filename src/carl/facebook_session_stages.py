"""Resident Facebook sessions with durable capture and price-partition phases."""

from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, replace
from time import time_ns
from typing import Literal, Protocol, cast

from pydantic import Field, model_validator

from carl.core.facebook import parse_json_blocks
from carl.core.facebook_search import (
    OverlappingPricePartitionSearchTraversalStrategy,
    PricePartitionSearchTraversalState,
    account_search_elapsed_interval,
    advance_price_partition_search_traversal,
    plan_overlapping_price_partitions,
)
from carl.core.facebook_search_protocol import (
    SearchBootstrapResponseKind,
    SearchSessionBootstrapResponseKind,
    classify_search_route_definition_response,
    classify_search_session_bootstrap_response,
    extract_search_route_definition,
    traversal_page_facts,
)
from carl.core.facebook_search_requests import (
    search_bootstrap_plan,
    search_route_definition_request,
    search_session_bootstrap_plan,
)
from carl.core.facebook_search_retry import PartitionSearchRetryCheckpoint
from carl.core.facebook_search_session import (
    ProtectedSearchSessionMaterial,
    SearchSessionMaterialEvidence,
    extract_search_session_material,
)
from carl.core.facebook_search_support import (
    UnsupportedFacebookSearchMode,
    require_supported_facebook_search_acquisition,
)
from carl.core.facebook_work import (
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_SEARCH_WORK_KIND,
    LEGACY_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    PREVIOUS_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    RETRYABLE_SEARCH_SESSION_FAILURE_CODES,
    SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
    SEARCH_SESSION_BOOTSTRAP_NETWORK_ACTIVITY_KIND,
    CollectSearchPayload,
    SearchPriceRange,
    facebook_network_policy_constraints,
    facebook_search_network_activity,
    legacy_facebook_network_constraint_identifiers,
)
from carl.core.json import encode_json
from carl.core.models import Header, JsonValue, NamedInput, NamedOutput, RecordDraft, StrictModel
from carl.core.resources import ResourceRequest
from carl.core.stages import (
    StageAttemptContext,
    StageDefinition,
    StageOutcome,
    StageWaitKind,
    WaitWork,
    initial_work_stage,
)
from carl.core.work import NetworkActivityDefinition, WorkCapability, WorkDefinition
from carl.core.worker import CompletedWork, RetryWork, TerminalFailureWork, WorkOutcome
from carl.facebook_search_workers import (
    SearchCheckpointDrafts,
    _decode_acquisition,
    _partition_checkpoint,
    _response_failure_work,
    _search_retry_delay_ns,
    build_resumed_partition_checkpoint,
    build_search_bootstrap_checkpoint,
    build_search_page_checkpoint,
    search_acquisition_failure_work,
    search_session_failure_work,
)
from carl.facebook_stateless_stages import RetainedWorkPayload
from carl.facebook_workers import (
    COLLECT_FACEBOOK_SEARCH,
    EXTRACT_FACEBOOK_JSON_BLOCKS,
    EXTRACT_FACEBOOK_SEARCH_ROUTE_DEFINITION,
    PLAN_FACEBOOK_OVERLAPPING_PRICE_PARTITIONS,
    build_component_registry,
)
from carl.io.browser_identity import brave_navigation_headers
from carl.io.configuration import (
    ConfigurationFailure,
    decodo_settings,
    load_configuration,
    proton_settings,
)
from carl.io.decodo import DecodoSessionManager
from carl.io.facebook_search import (
    DecodoFacebookSearchSessionFactory,
    FacebookSearchSessionFailure,
    ProtonFacebookSearchSessionFactory,
)
from carl.io.http_checkpoints import checkpoint_acquisition, restore_acquisition
from carl.io.httpx import (
    Acquisition,
    AcquisitionFailure,
    HttpFormCaptureAcquirer,
    IdentifierFactory,
    RouteConfigurationFailure,
    completed_capture_after_close_failure,
)
from carl.io.network_activity import acquire_for_network_activity, network_activity_definition
from carl.io.paths import CarlDirectories
from carl.io.proton import ProtonSessionManager
from carl.io.resident_sessions import ResidentHandle, ResidentValue
from carl.io.resource_permits import run_cpu
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler, WorkStageAdapter

_PREPARE = ("carl", "facebook", "stage", "search", "prepare")
_ROUTE = ("carl", "facebook", "stage", "search", "route")
_OPEN = ("carl", "facebook", "stage", "search", "open")
_BOOTSTRAP_CAPTURE = ("carl", "facebook", "stage", "search", "bootstrap_capture")
_BOOTSTRAP_PARSE = ("carl", "facebook", "stage", "search", "bootstrap_parse")
_PARTITION_CAPTURE = ("carl", "facebook", "stage", "search", "partition_capture")
_PARTITION_PARSE = ("carl", "facebook", "stage", "search", "partition_parse")
_FINALIZE = ("carl", "facebook", "stage", "search", "finalize")
_PROGRESS_KIND = ("carl", "facebook", "search_stage_progress")
_NAVIGATION_HEADERS = frozenset(
    {
        b"user-agent",
        b"accept",
        b"accept-language",
        b"upgrade-insecure-requests",
        b"sec-fetch-dest",
        b"sec-fetch-mode",
        b"sec-fetch-site",
        b"sec-fetch-user",
        b"sec-ch-ua",
        b"sec-ch-ua-mobile",
        b"sec-ch-ua-platform",
    }
)


class BoundedFacebookSession(Protocol):
    @property
    def identifier(self) -> str: ...

    @property
    def acquirer(self) -> HttpFormCaptureAcquirer: ...

    def active_observation(self) -> dict[str, JsonValue]: ...

    def completed_observation(self) -> dict[str, JsonValue]: ...

    def retained_bytes(self) -> int: ...

    def is_closed(self) -> bool: ...


@dataclass(slots=True)
class FacebookResident:
    session: BoundedFacebookSession
    navigation_headers: tuple[Header, ...]
    material: ProtectedSearchSessionMaterial | None = None
    evidence: SearchSessionMaterialEvidence | None = None
    form_requests: int = 0

    def is_closed(self) -> bool:
        return self.session.is_closed()

    def retained_bytes(self) -> int:
        material = () if self.material is None else self.material.reveal_graphql_form_fields()
        return (
            self.session.retained_bytes()
            + 8
            + sum(len(name.encode()) + len(value.encode()) for name, value in material)
            + sum(len(header.name) + len(header.value) for header in self.navigation_headers)
            + (0 if self.evidence is None else len(self.evidence.model_dump_json().encode()))
        )


class FacebookResidentPool(Protocol):
    async def get_or_open[T: ResidentValue](
        self, identifier: str, factory: Callable[[str], AbstractAsyncContextManager[T]]
    ) -> ResidentHandle[T]: ...

    def peek(self, identifier: str) -> ResidentHandle[ResidentValue] | None: ...

    async def close(self, identifier: str) -> None: ...


@dataclass(frozen=True, slots=True)
class FacebookSessionStageDependencies:
    database: Database
    directories: CarlDirectories
    new_identifier: IdentifierFactory
    proton_manager: ProtonSessionManager
    residents: FacebookResidentPool
    session_factory: Callable[[str], AbstractAsyncContextManager[BoundedFacebookSession]] | None = (
        None
    )
    navigation_headers: tuple[Header, ...] | None = None
    retry_scope: Literal["stack", "independent_session"] = "stack"
    utc_now_ns: Callable[[], int] = time_ns


class SearchProgress(StrictModel):
    search: CollectSearchPayload
    traversal: PricePartitionSearchTraversalState
    pages: tuple[dict[str, JsonValue], ...] = ()
    retry_checkpoint: PartitionSearchRetryCheckpoint | None = None
    navigation_headers: tuple[Header, ...]
    retry_scope: Literal["stack", "independent_session"]
    accounted_at_utc_ns: int = Field(ge=0)
    session_observation: dict[str, JsonValue] | None = None

    @model_validator(mode="after")
    def validate_bounds(self) -> "SearchProgress":
        if len(self.pages) > 10_000 or len(self.traversal.partitions) > 10_000:
            raise ValueError("Facebook search progress exceeds its page/partition bound")
        if len(self.model_dump_json().encode("utf-8")) > 16 * 1024 * 1024:
            raise ValueError("Facebook search progress exceeds its 16 MiB record bound")
        return self


class SearchPhasePayload(StrictModel):
    progress_identifier: str = Field(min_length=1)
    checkpoint_identifier: str | None = Field(default=None, min_length=1)
    incarnation: str | None = Field(default=None, min_length=1)


def _resident_identifier(context: StageAttemptContext) -> str:
    identifier = context.resident_session_identifier
    if identifier is None:
        raise ValueError("Facebook session stages require an admitted resident actor")
    return identifier


def _peek(
    dependencies: FacebookSessionStageDependencies, context: StageAttemptContext
) -> ResidentHandle[FacebookResident] | None:
    handle = dependencies.residents.peek(_resident_identifier(context))
    if handle is None:
        return None
    if not isinstance(handle.value, FacebookResident):
        raise ValueError("Resident actor does not own a Facebook session")
    return cast(ResidentHandle[FacebookResident], handle)


async def _progress(database: Database, payload: SearchPhasePayload) -> SearchProgress:
    kind, schema, value = await database.get_record(payload.progress_identifier)
    if kind != _PROGRESS_KIND or schema != 1:
        raise ValueError("Not a Facebook search progress checkpoint")
    return SearchProgress.model_validate_json(encode_json(value))


def _requested(progress: SearchProgress):
    partition = progress.traversal.partitions[progress.traversal.partitions_processed]
    return progress.search.request.model_copy(
        update={
            "price": SearchPriceRange(
                currency=partition.currency,
                minimum=partition.minimum,
                maximum=partition.maximum,
            )
        }
    )


def _network(
    progress: SearchProgress,
    context: StageAttemptContext,
    phase: tuple[str, ...],
    dependencies: FacebookSessionStageDependencies,
) -> NetworkActivityDefinition | None:
    if phase == _OPEN:
        return network_activity_definition(
            identifier=dependencies.new_identifier(),
            kind=("carl", "facebook", "network_activity", "search_session_open"),
            operation_identifier=context.operation_identifier,
            network_session_identifier=_resident_identifier(context),
            network_path=progress.search.routing,
            attempt=context.attempt,
        )
    if phase not in {_BOOTSTRAP_CAPTURE, _PARTITION_CAPTURE}:
        return None
    return facebook_search_network_activity(
        identifier=dependencies.new_identifier(),
        kind=SEARCH_SESSION_BOOTSTRAP_NETWORK_ACTIVITY_KIND
        if phase == _BOOTSTRAP_CAPTURE
        else SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
        operation_identifier=context.operation_identifier,
        network_session_identifier=_resident_identifier(context),
        ordinal=1 if phase == _BOOTSTRAP_CAPTURE else progress.traversal.partitions_processed + 2,
        attempt=context.attempt,
        routing=progress.search.routing,
    )


def _next(
    dependencies: FacebookSessionStageDependencies,
    context: StageAttemptContext,
    progress: SearchProgress,
    phase: tuple[str, ...],
    *,
    checkpoint_identifier: str | None = None,
    incarnation: str | None = None,
    evidence: SearchCheckpointDrafts | None = None,
) -> WaitWork:
    progress = SearchProgress.model_validate_json(progress.model_dump_json())
    progress_identifier = dependencies.new_identifier()
    record = RecordDraft(
        identifier=progress_identifier,
        kind=_PROGRESS_KIND,
        schema_version=1,
        value=progress.model_dump(mode="json"),
    )
    network = _network(progress, context, phase, dependencies)
    cpu = phase in {_PREPARE, _BOOTSTRAP_PARSE, _PARTITION_PARSE}
    inputs = [NamedInput(name=("progress",), object_identifier=progress_identifier)]
    if checkpoint_identifier is not None:
        inputs.append(NamedInput(name=("capture",), object_identifier=checkpoint_identifier))
    continuation = StageDefinition(
        identifier=dependencies.new_identifier(),
        work_item_identifier=context.work_item_identifier,
        operation_identifier=context.operation_identifier,
        kind=phase,
        payload=SearchPhasePayload(
            progress_identifier=progress_identifier,
            checkpoint_identifier=checkpoint_identifier,
            incarnation=incarnation,
        ).model_dump(mode="json"),
        resources=(ResourceRequest(name="http"),)
        if network is not None
        else ((ResourceRequest(name="cpu"),) if cpu else ()),
        checkpoint_inputs=tuple(inputs),
        network_activity=network,
        resident_session_identifier=_resident_identifier(context),
    )
    evidence = evidence or SearchCheckpointDrafts(result={})
    return WaitWork(
        continuation=continuation,
        wait_kind=StageWaitKind.RESOURCE,
        records=(*evidence.records, record),
        artifacts=evidence.artifacts,
        inputs=evidence.inputs,
        outputs=(
            *evidence.outputs,
            NamedOutput(
                name=("search_stage", progress_identifier, "progress"),
                object_identifier=progress_identifier,
            ),
        ),
        result={
            "state": "collecting",
            "progress_record_identifier": progress_identifier,
            "partition_retry_checkpoint": None
            if progress.retry_checkpoint is None
            else progress.retry_checkpoint.model_dump(mode="json"),
        },
    )


def _merge(outcome: WorkOutcome, evidence: SearchCheckpointDrafts) -> StageOutcome:
    return outcome.model_copy(
        update={
            "records": (*outcome.records, *evidence.records),
            "artifacts": (*outcome.artifacts, *evidence.artifacts),
            "inputs": (*outcome.inputs, *evidence.inputs),
            "outputs": (*outcome.outputs, *evidence.outputs),
        }
    )


def _policy_attempt(progress: SearchProgress, context: StageAttemptContext) -> int:
    return context.retry_attempt(progress.search.retry_attempt_offset)


async def _transport_failure(
    dependencies: FacebookSessionStageDependencies,
    context: StageAttemptContext,
    progress: SearchProgress,
    error: AcquisitionFailure | FacebookSearchSessionFailure,
) -> StageOutcome:
    attempt = max(
        1,
        _policy_attempt(progress, context)
        - (
            0
            if progress.retry_checkpoint is None
            else progress.retry_checkpoint.challenge_retry_attempts
        ),
    )
    if isinstance(error, AcquisitionFailure):
        retryable = error.result.get("stopping_condition") == "transport_failure"
        outcome = search_acquisition_failure_work(
            error=error, context=context, policy_attempt=attempt, retry_scope=progress.retry_scope
        )
    else:
        retryable = error.code in RETRYABLE_SEARCH_SESSION_FAILURE_CODES
        outcome = search_session_failure_work(
            error=error,
            context=context,
            routing=progress.search.routing,
            policy_attempt=attempt,
            retry_scope=progress.retry_scope,
        )
    if retryable and progress.retry_scope != "independent_session":
        now = dependencies.utc_now_ns()
        await dependencies.database.defer_pending_collect_search_work_for_route(
            routing=progress.search.routing,
            eligible_at_utc_ns=now + _search_retry_delay_ns(attempt),
            recorded_at_utc_ns=now,
            event_batch_identifier=dependencies.new_identifier(),
            reason={
                "kind": "shared_search_route_transport_backoff",
                "triggering_work_identifier": context.work_item_identifier,
            },
        )
    if isinstance(outcome, RetryWork) and progress.retry_checkpoint is not None:
        outcome = outcome.model_copy(
            update={
                "result": {
                    **outcome.result,
                    "partition_retry_checkpoint": progress.retry_checkpoint.model_dump(mode="json"),
                }
            }
        )
    if isinstance(error, AcquisitionFailure) and error.bodies:
        checkpoint = await checkpoint_acquisition(
            Acquisition(error.result, error.bodies),
            data_directory=dependencies.database.path.parent,
            new_identifier=dependencies.new_identifier,
        )
        outcome = outcome.model_copy(update={"artifacts": ()})
        return _merge(
            outcome,
            SearchCheckpointDrafts(
                result={},
                records=checkpoint.records,
                artifacts=checkpoint.artifacts,
                outputs=tuple(
                    item.model_copy(
                        update={
                            "name": ("search_capture", checkpoint.record_identifier, *item.name)
                        }
                    )
                    for item in checkpoint.outputs
                ),
            ),
        )
    return outcome


def _factory(dependencies: FacebookSessionStageDependencies, progress: SearchProgress):
    routing = progress.search.routing
    navigation_headers = progress.navigation_headers

    @asynccontextmanager
    async def open_resident(identifier: str):
        factory = dependencies.session_factory
        if factory is None:
            loaded = load_configuration(dependencies.directories.configuration_file)
            if loaded.configuration.resolve_network_path(routing) != routing:
                raise RouteConfigurationFailure("stage_route_changed_before_capture")
            if routing[0] == "decodo":
                settings, credentials = decodo_settings(loaded, dependencies.directories, routing)
                factory = cast(
                    Callable[[str], AbstractAsyncContextManager[BoundedFacebookSession]],
                    DecodoFacebookSearchSessionFactory(
                        manager=DecodoSessionManager(),
                        settings=settings,
                        credential_source=credentials,
                    ),
                )
            else:
                factory = cast(
                    Callable[[str], AbstractAsyncContextManager[BoundedFacebookSession]],
                    ProtonFacebookSearchSessionFactory(
                        manager=dependencies.proton_manager,
                        settings=proton_settings(loaded, dependencies.directories, routing),
                    ),
                )
            del loaded
        async with factory(identifier) as session:
            if (
                not callable(getattr(session, "retained_bytes", None))
                or not callable(getattr(session, "is_closed", None))
                or not callable(getattr(session.acquirer, "capture_form", None))
            ):
                raise ValueError("Facebook session requires bounded state and raw form capture")
            resident = FacebookResident(session, navigation_headers)
            try:
                yield resident
            finally:
                resident.material = None
                resident.evidence = None

    return open_resident


def _initial(work: WorkDefinition, identifier: str, operation_identifier: str) -> StageDefinition:
    return initial_work_stage(
        work,
        identifier=identifier,
        operation_identifier=operation_identifier,
        kind=_PREPARE,
        resources=(ResourceRequest(name="cpu"),),
    ).model_copy(update={"resident_session_identifier": work.identifier})


def _bounded_handler[PayloadT: StrictModel](
    handler: Callable[[PayloadT, StageAttemptContext], Awaitable[StageOutcome]],
) -> Callable[[PayloadT, StageAttemptContext], Awaitable[StageOutcome]]:
    async def execute(payload: PayloadT, context: StageAttemptContext) -> StageOutcome:
        try:
            return await handler(payload, context)
        except ConfigurationFailure as error:
            return TerminalFailureWork(
                error={"kind": "configuration_failure", "code": error.code},
                result={"state": "configuration_failed"},
            )
        except RouteConfigurationFailure as error:
            return TerminalFailureWork(
                error={"kind": "route_configuration_failure", "code": error.code},
                result={"state": "configuration_failed"},
            )
        except (OSError, KeyError, ValueError) as error:
            return TerminalFailureWork(
                error={
                    "kind": "search_configuration_or_response_failure",
                    "type": type(error).__name__,
                },
                result={"state": "failed"},
            )

    return execute


def build_facebook_session_stage_registry(
    dependencies: FacebookSessionStageDependencies,
) -> StageHandlerRegistry:
    components = build_component_registry()

    async def prepare(payload: RetainedWorkPayload, context: StageAttemptContext) -> StageOutcome:
        search = CollectSearchPayload.model_validate_json(encode_json(payload.payload))
        if payload.work_kind != COLLECT_SEARCH_WORK_KIND:
            raise ValueError("Not a retained Facebook search work payload")
        try:
            require_supported_facebook_search_acquisition(search.request, search.traversal_strategy)
        except UnsupportedFacebookSearchMode as error:
            return TerminalFailureWork(
                error={"kind": "unsupported_facebook_search_mode", "message": str(error)},
                result={"state": "unsupported"},
            )
        if context.retry_attempt(search.retry_attempt_offset) < 1:
            raise ValueError("Search retry offset is inconsistent with its logical attempt")
        assert isinstance(
            search.traversal_strategy, OverlappingPricePartitionSearchTraversalStrategy
        )
        price = search.request.price
        assert price is not None and price.maximum is not None
        scope: Literal["stack", "independent_session"] = dependencies.retry_scope
        if dependencies.session_factory is None:
            loaded = load_configuration(dependencies.directories.configuration_file)
            search = search.model_copy(
                update={"routing": loaded.configuration.resolve_network_path(search.routing)}
            )
            if search.routing[0] == "decodo":
                settings, _ = decodo_settings(loaded, dependencies.directories, search.routing)
                scope = (
                    "independent_session"
                    if settings.session_retry_scope() == "independent_session"
                    else "stack"
                )
        headers = dependencies.navigation_headers
        if headers is None:
            headers = await run_cpu(brave_navigation_headers)
        headers = tuple(header for header in headers if header.name.lower() in _NAVIGATION_HEADERS)
        strategy = search.traversal_strategy
        assert isinstance(strategy, OverlappingPricePartitionSearchTraversalStrategy)
        if (
            price.maximum - (price.minimum or 0)
            > strategy.width + (strategy.width - strategy.overlap) * 9_999
        ):
            raise ValueError("Facebook search exceeds its partition bound")
        await dependencies.database.supersede_constraints(
            retired_identifiers=legacy_facebook_network_constraint_identifiers(search.routing),
            replacements=facebook_network_policy_constraints(search.routing),
            operation_identifier=context.operation_identifier,
            at_utc_ns=dependencies.utc_now_ns(),
            reason="Apply Marketplace policy to the resident search route",
        )
        state = PricePartitionSearchTraversalState(
            search_run_identifier=dependencies.new_identifier(),
            attempt=context.attempt,
            policy=search.traversal,
            strategy=strategy,
            partitions=plan_overlapping_price_partitions(
                currency=price.currency,
                minimum=price.minimum,
                maximum=price.maximum,
                strategy=strategy,
            ),
        )
        checkpoint = _partition_checkpoint(payload=search, context=context, state=state, pages=[])
        evidence = SearchCheckpointDrafts(result={})
        if context.attempt > 1:
            result = (await dependencies.database.work(context.work_item_identifier)).get("result")
            saved = result.get("partition_retry_checkpoint") if isinstance(result, dict) else None
            if saved is not None:
                restored = PartitionSearchRetryCheckpoint.model_validate_json(encode_json(saved))
                if checkpoint is None or restored.request_sha256 != checkpoint.request_sha256:
                    return TerminalFailureWork(
                        error={"kind": "search_partition_retry_checkpoint_invalid"},
                        result={"state": "failed"},
                    )
                restored = restored.model_copy(
                    update={
                        "traversal_state": restored.traversal_state.model_copy(
                            update={"attempt": context.attempt}
                        ),
                        "challenge_attempts": restored.challenge_attempts
                        if restored.retry_budget_start_attempt
                        == checkpoint.retry_budget_start_attempt
                        else 0,
                        "challenge_retry_attempts": restored.challenge_retry_attempts
                        if restored.retry_budget_start_attempt
                        == checkpoint.retry_budget_start_attempt
                        else 0,
                        "retry_budget_start_attempt": checkpoint.retry_budget_start_attempt,
                    }
                )
                checkpoint, evidence = await build_resumed_partition_checkpoint(
                    dependencies=dependencies, context=context, checkpoint=restored
                )
                state = checkpoint.traversal_state
        progress = SearchProgress(
            search=search,
            traversal=state,
            pages=() if checkpoint is None else checkpoint.page_references,
            retry_checkpoint=checkpoint,
            navigation_headers=headers,
            retry_scope=scope,
            accounted_at_utc_ns=dependencies.utc_now_ns(),
        )
        return _next(dependencies, context, progress, _ROUTE, evidence=evidence)

    async def admit_route(
        payload: SearchPhasePayload, context: StageAttemptContext
    ) -> StageOutcome:
        progress = await _progress(dependencies.database, payload)
        admitted = await dependencies.database.try_admit_facebook_search_route(
            work_item_identifier=context.work_item_identifier,
            lease_token=context.lease_token,
            worker_identifier=context.worker_identifier,
            routing=progress.search.routing,
            utc_now_ns=dependencies.utc_now_ns,
        )
        outcome = _next(dependencies, context, progress, _OPEN if admitted else _ROUTE)
        if not admitted:
            outcome = outcome.model_copy(
                update={
                    "wait_kind": StageWaitKind.TIME,
                    "continuation": outcome.continuation.model_copy(
                        update={"not_before_utc_ns": dependencies.utc_now_ns() + 1_000_000_000}
                    ),
                }
            )
        return outcome

    async def open_session(
        payload: SearchPhasePayload, context: StageAttemptContext
    ) -> StageOutcome:
        progress = await _progress(dependencies.database, payload)
        try:
            handle = await dependencies.residents.get_or_open(
                _resident_identifier(context), _factory(dependencies, progress)
            )
        except (AcquisitionFailure, FacebookSearchSessionFailure) as error:
            return await _transport_failure(dependencies, context, progress, error)
        except TimeoutError:
            return await _transport_failure(
                dependencies,
                context,
                progress,
                AcquisitionFailure(
                    "Search session setup timed out",
                    result={
                        "stopping_condition": "transport_failure",
                        "exception_type": "TimeoutError",
                    },
                ),
            )
        return _next(
            dependencies, context, progress, _BOOTSTRAP_CAPTURE, incarnation=handle.incarnation
        )

    async def capture(
        payload: SearchPhasePayload, context: StageAttemptContext, *, bootstrap: bool
    ) -> StageOutcome:
        progress = await _progress(dependencies.database, payload)
        now = dependencies.utc_now_ns()
        state = account_search_elapsed_interval(
            progress.traversal,
            interval_elapsed_duration_ns=max(0, now - progress.accounted_at_utc_ns),
        )
        progress = progress.model_copy(update={"traversal": state, "accounted_at_utc_ns": now})
        if state.stopping_reason is not None:
            return _next(dependencies, context, progress, _FINALIZE)
        handle = _peek(dependencies, context)
        if handle is None:
            return _next(dependencies, context, progress, _OPEN)
        if not bootstrap and handle.value.material is None:
            return _next(dependencies, context, progress, _BOOTSTRAP_CAPTURE)
        admission = context.network_admission
        if admission is None:
            raise ValueError("Facebook capture requires exact network admission")
        plan = search_session_bootstrap_plan(
            routing=progress.search.routing, navigation_headers=progress.navigation_headers
        )
        request = None
        if not bootstrap:
            material = handle.value.material
            assert material is not None
            handle.value.form_requests += 1
            request = search_route_definition_request(
                bootstrap_plan=search_bootstrap_plan(
                    _requested(progress),
                    routing=progress.search.routing,
                    navigation_headers=progress.navigation_headers,
                ),
                navigation_headers=progress.navigation_headers,
                session_material=material,
                request_number=handle.value.form_requests,
            )
            plan = request.plan
        request_identity = (
            ("facebook", "bootstrap", str(progress.traversal.partitions_processed))
            if bootstrap
            else (
                "facebook",
                "price_partition",
                str(progress.traversal.partitions[progress.traversal.partitions_processed].ordinal),
            )
        )
        charge = await dependencies.database.charge_resident_request(
            context,
            request_identity=request_identity,
            maximum_attempts=1,
            now_utc_ns=dependencies.utc_now_ns(),
        )
        if charge is None or not charge.newly_charged:
            return await _transport_failure(
                dependencies,
                context,
                progress,
                AcquisitionFailure(
                    "A previous dispatch at this search frontier has an uncertain outcome",
                    result={
                        "kind": "uncertain_previous_dispatch",
                        "stopping_condition": "transport_failure",
                        "failure_phase": "uncertain_previous_dispatch",
                        "request_identity": list(request_identity),
                    },
                ),
            )
        try:
            if bootstrap:
                acquisition = await acquire_for_network_activity(
                    handle.value.session.acquirer.capture(
                        plan,
                        dependencies.new_identifier,
                    ),
                    admission,
                )
            else:
                assert request is not None
                acquisition = await acquire_for_network_activity(
                    handle.value.session.acquirer.capture_form(
                        request.plan, request.form_fields, dependencies.new_identifier
                    ),
                    admission,
                )
        except AcquisitionFailure as error:
            completed = completed_capture_after_close_failure(error, plan=plan)
            if completed is None:
                return await _transport_failure(dependencies, context, progress, error)
            acquisition = completed
        except FacebookSearchSessionFailure as error:
            return await _transport_failure(dependencies, context, progress, error)
        except TimeoutError:
            return await _transport_failure(
                dependencies,
                context,
                progress,
                AcquisitionFailure(
                    "Search request timed out",
                    result={
                        "stopping_condition": "transport_failure",
                        "exception_type": "TimeoutError",
                    },
                ),
            )
        progress = progress.model_copy(
            update={"session_observation": handle.value.session.active_observation()}
        )
        checkpoint = await checkpoint_acquisition(
            acquisition,
            data_directory=dependencies.database.path.parent,
            new_identifier=dependencies.new_identifier,
        )
        evidence = SearchCheckpointDrafts(
            result={},
            records=checkpoint.records,
            artifacts=checkpoint.artifacts,
            outputs=tuple(
                item.model_copy(
                    update={"name": ("search_capture", checkpoint.record_identifier, *item.name)}
                )
                for item in checkpoint.outputs
            ),
        )
        return _next(
            dependencies,
            context,
            progress,
            _BOOTSTRAP_PARSE if bootstrap else _PARTITION_PARSE,
            checkpoint_identifier=checkpoint.record_identifier,
            incarnation=handle.incarnation,
            evidence=evidence,
        )

    async def bootstrap_capture(
        payload: SearchPhasePayload, context: StageAttemptContext
    ) -> StageOutcome:
        return await capture(payload, context, bootstrap=True)

    async def partition_capture(
        payload: SearchPhasePayload, context: StageAttemptContext
    ) -> StageOutcome:
        return await capture(payload, context, bootstrap=False)

    async def bootstrap_parse(
        payload: SearchPhasePayload, context: StageAttemptContext
    ) -> StageOutcome:
        progress = await _progress(dependencies.database, payload)
        handle = _peek(dependencies, context)
        if handle is None or handle.incarnation != payload.incarnation:
            return _next(dependencies, context, progress, _OPEN)
        if payload.checkpoint_identifier is None:
            raise ValueError("Bootstrap parse has no raw response checkpoint")
        acquisition = await restore_acquisition(
            dependencies.database, payload.checkpoint_identifier
        )
        decoded = await run_cpu(_decode_acquisition, acquisition)
        blocks = await run_cpu(parse_json_blocks, decoded.text)
        effective_url = decoded.terminal_response.get("url")
        if not isinstance(effective_url, str):
            raise ValueError("Bootstrap effective URL is malformed")
        classification = classify_search_session_bootstrap_response(
            decoded.text, effective_url=effective_url
        )
        acquisition_identifier = dependencies.new_identifier()
        extraction = extract_search_session_material(
            blocks, acquisition_record_identifier=acquisition_identifier
        )
        evidence = build_search_bootstrap_checkpoint(
            dependencies=dependencies,
            context=context,
            extractor_component=components.require(EXTRACT_FACEBOOK_JSON_BLOCKS),
            search_run_identifier=progress.traversal.search_run_identifier,
            network_session_identifier=handle.value.session.identifier,
            acquisition_identifier=acquisition_identifier,
            acquisition=Acquisition(acquisition.record, ()),
            decoded=decoded,
            blocks=blocks,
            response_classification=classification.model_dump(mode="json"),
            session_evidence=extraction.evidence,
            session_issues=extraction.issues,
            partition_retry_checkpoint=progress.retry_checkpoint,
        )
        evidence = replace_checkpoint_outputs(
            evidence, ("resident_bootstrap", payload.checkpoint_identifier)
        )
        if classification.kind is not SearchSessionBootstrapResponseKind.MARKETPLACE_LANDING:
            outcome = _response_failure_work(
                error={
                    "kind": "search_session_bootstrap_response_failure",
                    "response_kind": classification.kind.value,
                },
                result={
                    "state": "failed",
                    "response_classification": classification.model_dump(mode="json"),
                },
                checkpoint=progress.retry_checkpoint,
                policy_attempt=_policy_attempt(progress, context),
                routing=progress.search.routing,
                now_utc_ns=dependencies.utc_now_ns(),
                retry_scope=progress.retry_scope,
            )
            return _merge(outcome, evidence)
        if extraction.material is None:
            return _merge(
                TerminalFailureWork(
                    error={"kind": "search_session_material_extraction_failure"},
                    result={"state": "failed"},
                ),
                evidence,
            )
        handle.value.material, handle.value.evidence = extraction.material, extraction.evidence
        return _next(dependencies, context, progress, _PARTITION_CAPTURE, evidence=evidence)

    async def partition_parse(
        payload: SearchPhasePayload, context: StageAttemptContext
    ) -> StageOutcome:
        progress = await _progress(dependencies.database, payload)
        if payload.checkpoint_identifier is None:
            raise ValueError("Partition parse has no raw response checkpoint")
        acquisition = await restore_acquisition(
            dependencies.database, payload.checkpoint_identifier
        )
        decoded = await run_cpu(_decode_acquisition, acquisition)
        acquisition_identifier = dependencies.new_identifier()
        extraction = await run_cpu(
            lambda: extract_search_route_definition(
                decoded.text, acquisition_record_identifier=acquisition_identifier
            )
        )
        effective_url = decoded.terminal_response.get("url")
        if not isinstance(effective_url, str):
            raise ValueError("Partition effective URL is malformed")
        classification = classify_search_route_definition_response(
            decoded.text, effective_url=effective_url, extraction=extraction
        )
        handle = _peek(dependencies, context)
        partition = progress.traversal.partitions[progress.traversal.partitions_processed]
        state = progress.traversal
        if extraction.page is not None and extraction.applied_configuration is not None:
            state = advance_price_partition_search_traversal(
                state=state,
                partition=partition,
                page=traversal_page_facts(
                    extraction.page,
                    interval_elapsed_duration_ns=max(
                        0, dependencies.utc_now_ns() - progress.accounted_at_utc_ns
                    ),
                    transferred_bytes=decoded.received_content_bytes,
                    decoded_body_bytes=len(decoded.content),
                ),
            ).state
        evidence = build_search_page_checkpoint(
            dependencies=dependencies,
            context=context,
            extractor_component=components.require(EXTRACT_FACEBOOK_SEARCH_ROUTE_DEFINITION),
            search_run_identifier=state.search_run_identifier,
            network_session_identifier=payload.incarnation or _resident_identifier(context),
            page_ordinal=progress.traversal.partitions_processed + 1,
            page_kind="route_definition",
            acquisition_identifier=acquisition_identifier,
            acquisition=Acquisition(acquisition.record, ()),
            decoded=decoded,
            page=extraction.page,
            extraction=extraction,
            applied_configuration=extraction.applied_configuration,
            session_evidence=None if handle is None else handle.value.evidence,
            response_classification=classification.model_dump(mode="json"),
            traversal_state=state,
            requested_search=_requested(progress),
            price_partition=partition,
            price_partition_planner_component=components.require(
                PLAN_FACEBOOK_OVERLAPPING_PRICE_PARTITIONS
            ),
            partition_retry_checkpoint=progress.retry_checkpoint,
        )
        if extraction.page is None or extraction.applied_configuration is None:
            outcome = _response_failure_work(
                error={
                    "kind": "search_price_partition_extraction_failure"
                    if classification.kind is SearchBootstrapResponseKind.SEARCH_RESULTS
                    else "search_route_definition_response_failure",
                    "response_kind": classification.kind.value,
                },
                result={
                    "state": "failed",
                    "page_ordinal": progress.traversal.partitions_processed + 1,
                    "protocol_issues": [
                        issue.model_dump(mode="json") for issue in extraction.issues
                    ],
                },
                checkpoint=progress.retry_checkpoint,
                policy_attempt=_policy_attempt(progress, context),
                routing=progress.search.routing,
                now_utc_ns=dependencies.utc_now_ns(),
                retry_scope=progress.retry_scope,
                guest_session_established=True,
            )
            return _merge(outcome, evidence)
        pages = (
            *progress.pages,
            {
                "page_ordinal": progress.traversal.partitions_processed + 1,
                "listing_occurrence_records": list(evidence.occurrence_identifiers),
                "requested_search": _requested(progress).model_dump(mode="json"),
                "price_partition": partition.model_dump(mode="json"),
            },
        )
        checkpoint = _partition_checkpoint(
            payload=progress.search,
            context=context,
            state=state,
            pages=list(pages),
            previous=progress.retry_checkpoint,
        )
        progress = progress.model_copy(
            update={
                "traversal": state,
                "pages": pages,
                "retry_checkpoint": checkpoint,
                "accounted_at_utc_ns": dependencies.utc_now_ns(),
            }
        )
        return _next(
            dependencies,
            context,
            progress,
            _FINALIZE if state.stopping_reason is not None else _PARTITION_CAPTURE,
            evidence=evidence,
        )

    async def finalize(payload: SearchPhasePayload, context: StageAttemptContext) -> StageOutcome:
        progress = await _progress(dependencies.database, payload)
        handle = _peek(dependencies, context)
        observation = progress.session_observation or {"state": "resident_not_available"}
        cleanup: dict[str, JsonValue] | None = None
        if handle is not None:
            try:
                await dependencies.residents.close(_resident_identifier(context))
            except Exception as error:
                cleanup = {"kind": "resident_cleanup_failure", "type": type(error).__name__}
            try:
                observation = handle.value.session.completed_observation()
            except (OSError, RuntimeError, ValueError) as error:
                observation = {
                    **observation,
                    "completion_observation_unavailable": type(error).__name__,
                }
        session_identifier, run_identifier = (
            dependencies.new_identifier(),
            dependencies.new_identifier(),
        )
        state = progress.traversal
        return CompletedWork(
            records=(
                RecordDraft(
                    identifier=session_identifier,
                    kind=("carl", "network", "session_observation"),
                    schema_version=1,
                    value=observation,
                ),
                RecordDraft(
                    identifier=run_identifier,
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value={
                        "search_run_identifier": state.search_run_identifier,
                        "work_item_identifier": context.work_item_identifier,
                        "attempt": context.attempt,
                        "request": progress.search.request.model_dump(mode="json"),
                        "traversal_strategy": progress.search.traversal_strategy.model_dump(
                            mode="json"
                        ),
                        "traversal": state.model_dump(mode="json"),
                        "applied_configuration": None,
                        "price_partition_plan": {
                            "planner": {
                                "component_parts": list(
                                    PLAN_FACEBOOK_OVERLAPPING_PRICE_PARTITIONS.parts
                                ),
                                "output_schema_version": 1,
                            },
                            "partitions": [
                                partition.model_dump(mode="json") for partition in state.partitions
                            ],
                        },
                        "network_session_record_identifier": session_identifier,
                        "pages": list(progress.pages),
                        "session_cleanup_failure": cleanup,
                    },
                ),
            ),
            outputs=(
                NamedOutput(
                    name=("search", "network_session"), object_identifier=session_identifier
                ),
                NamedOutput(name=("search", "run"), object_identifier=run_identifier),
            ),
            result={
                "state": "complete",
                "search_run_identifier": state.search_run_identifier,
                "search_run_record_identifier": run_identifier,
                "pages_processed": state.partitions_processed,
                "listing_observations": state.listing_observations,
                "unique_listings": len(state.unique_listing_identifiers),
                "stopping_reason": state.stopping_reason,
                "saturated_price_partitions": list(state.saturated_partition_ordinals),
                "session_cleanup_failure": cleanup,
            },
        )

    return StageHandlerRegistry(
        adapters=tuple(
            WorkStageAdapter(
                WorkCapability(kind=COLLECT_SEARCH_WORK_KIND, payload_schema_version=version),
                components.require(COLLECT_FACEBOOK_SEARCH),
                _initial,
            )
            for version in (
                LEGACY_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
                PREVIOUS_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
                COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
            )
        ),
        handlers=(
            TypedStageHandler(
                kind=_ROUTE,
                schema_version=1,
                payload_type=SearchPhasePayload,
                handler=_bounded_handler(admit_route),
            ),
            TypedStageHandler(
                kind=_PREPARE,
                schema_version=1,
                payload_type=RetainedWorkPayload,
                handler=_bounded_handler(prepare),
            ),
            TypedStageHandler(
                kind=_OPEN,
                schema_version=1,
                payload_type=SearchPhasePayload,
                handler=_bounded_handler(open_session),
            ),
            TypedStageHandler(
                kind=_BOOTSTRAP_CAPTURE,
                schema_version=1,
                payload_type=SearchPhasePayload,
                handler=_bounded_handler(bootstrap_capture),
            ),
            TypedStageHandler(
                kind=_BOOTSTRAP_PARSE,
                schema_version=1,
                payload_type=SearchPhasePayload,
                handler=_bounded_handler(bootstrap_parse),
            ),
            TypedStageHandler(
                kind=_PARTITION_CAPTURE,
                schema_version=1,
                payload_type=SearchPhasePayload,
                handler=_bounded_handler(partition_capture),
            ),
            TypedStageHandler(
                kind=_PARTITION_PARSE,
                schema_version=1,
                payload_type=SearchPhasePayload,
                handler=_bounded_handler(partition_parse),
            ),
            TypedStageHandler(
                kind=_FINALIZE,
                schema_version=1,
                payload_type=SearchPhasePayload,
                handler=_bounded_handler(finalize),
            ),
        ),
    )


def replace_checkpoint_outputs(
    evidence: SearchCheckpointDrafts, prefix: tuple[str, ...]
) -> SearchCheckpointDrafts:
    return replace(
        evidence,
        outputs=tuple(
            output.model_copy(update={"name": (*prefix, *output.name)})
            for output in evidence.outputs
        ),
    )
