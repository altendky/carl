"""Serial resident eBay clients with durable HTTP/CPU continuations."""

# The legacy adapters provide the same pure publication and retry policies.
# pyright: reportPrivateUsage=false

from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from random import uniform
from time import perf_counter_ns, time_ns
from typing import Literal, Protocol, cast

from pydantic import Field, model_validator

from carl.core.configuration import EbayItemConfiguration, EbaySearchConfiguration
from carl.core.ebay import (
    COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_EBAY_SEARCH_WORK_KIND,
    EBAY_SEARCH_FAILURE_KINDS,
    LEGACY_COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
    CollectEbaySearchPayload,
    ebay_homepage_plan,
    ebay_search_plan,
)
from carl.core.ebay_items import (
    COLLECT_EBAY_ITEM_WORK_KIND,
    CollectEbayItemPayload,
    EbayItemResponseKind,
    ebay_item_plan,
)
from carl.core.ebay_navigation import ebay_fetch_plan, ebay_pagination_token, is_ebay_search_url
from carl.core.ebay_search_support import (
    UnsupportedEbaySearchMode,
    require_supported_ebay_search_acquisition,
)
from carl.core.http import RequestPlan
from carl.core.json import encode_json
from carl.core.models import Header, JsonValue, NamedInput, NamedOutput, RecordDraft, StrictModel
from carl.core.resources import ResourceRequest
from carl.core.stages import (
    StageAttemptContext,
    StageDefinition,
    StageWaitKind,
    WaitWork,
    initial_work_stage,
)
from carl.core.work import WorkCapability, WorkDefinition
from carl.core.worker import RetryWork, TerminalFailureWork
from carl.ebay import (
    COLLECT_EBAY_SEARCH,
    COLLECT_EBAY_SEARCH_HOMEPAGE,
    RUN_EBAY_SEARCH_WORK,
    _attempt_summary,
    _terminal_response,
    build_ebay_component_registry,
    build_search_document_draft,
    build_search_extraction_draft,
    build_search_finalization_draft,
)
from carl.ebay_item_workers import (
    EbayItemWorkerDependencies,
    _failure,
    _item_response,
    build_ebay_item_worker_registry,
    complete_item_collection,
    prepare_item_collection,
)
from carl.ebay_workers import (
    EbaySearchWorkerDependencies,
    search_acquisition_failure_work,
    search_result_work,
)
from carl.io.configuration import (
    ConfigurationFailure,
    decodo_wreq_stack_settings,
    load_configuration,
)
from carl.io.decodo import ManagedDecodoWreqAcquirer
from carl.io.http_checkpoints import HttpCheckpoint, checkpoint_acquisition, restore_acquisition
from carl.io.httpx import (
    Acquisition,
    AcquisitionFailure,
    HttpCaptureAcquirer,
    RouteConfigurationFailure,
    completed_capture_after_close_failure,
)
from carl.io.network_activity import acquire_for_network_activity, network_activity_definition
from carl.io.paths import CarlDirectories
from carl.io.proton import ProtonSessionManager
from carl.io.resident_sessions import ResidentHandle, ResidentValue
from carl.io.resource_permits import run_cpu
from carl.io.sqlite import Database
from carl.io.stage_publication import ChildOperationDraft, StageHandlerResult, StagePublication
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler, WorkStageAdapter

_PREPARE = ("carl", "ebay", "stage", "resident", "prepare")
_CAPTURE = ("carl", "ebay", "stage", "resident", "capture")
_PROCESS = ("carl", "ebay", "stage", "resident", "process")
_PROGRESS = ("carl", "ebay", "session_stage_progress")


class BoundedEbayAcquirer(HttpCaptureAcquirer, Protocol):
    def retained_bytes(self) -> int: ...

    def is_closed(self) -> bool: ...


@dataclass(slots=True)
class EbayResident:
    acquirer: BoundedEbayAcquirer
    pagination_token: str | None = None
    previous_url: str | None = None
    homepage_requests: int = 0

    def is_closed(self) -> bool:
        return self.acquirer.is_closed()

    def retained_bytes(self) -> int:
        return self.acquirer.retained_bytes() + sum(
            len(value.encode("utf-8"))
            for value in (self.pagination_token, self.previous_url)
            if value
        )


class EbayResidentPool(Protocol):
    async def get_or_open[T: ResidentValue](
        self, identifier: str, factory: Callable[[str], AbstractAsyncContextManager[T]]
    ) -> ResidentHandle[T]: ...

    def peek(self, identifier: str) -> ResidentHandle[ResidentValue] | None: ...

    async def close(self, identifier: str) -> None: ...


@dataclass(frozen=True, slots=True)
class EbaySessionStageDependencies:
    database: Database
    directories: CarlDirectories
    new_identifier: Callable[[], str]
    proton_manager: ProtonSessionManager
    residents: EbayResidentPool
    session_factory: Callable[[str], AbstractAsyncContextManager[BoundedEbayAcquirer]] | None = None
    network_path: tuple[str, ...] = ("decodo", "personal", "carl")
    search_policy: EbaySearchConfiguration | None = None
    item_policy: EbayItemConfiguration | None = None
    retry_scope: Literal["stack", "independent_session"] = "stack"
    fetch_pagination: bool = False
    utc_now_ns: Callable[[], int] = time_ns


class SessionProgress(StrictModel):
    search: CollectEbaySearchPayload | None = None
    item: CollectEbayItemPayload | None = None
    network_path: tuple[str, ...]
    search_policy: EbaySearchConfiguration
    item_policy: EbayItemConfiguration
    retry_scope: Literal["stack", "independent_session"]
    fetch_pagination: bool = False
    run_identifier: str
    phase: Literal["homepage", "page", "item"]
    homepage_attempt: int = Field(default=0, ge=0, le=20)
    page_attempt: int = Field(default=0, ge=0, le=20)
    page_number: int = Field(default=1, ge=1, le=20)
    homepage: dict[str, JsonValue] | None = None
    home_attempts: tuple[dict[str, JsonValue], ...] = ()
    pages: tuple[dict[str, JsonValue], ...] = ()
    page_attempts: tuple[dict[str, JsonValue], ...] = ()
    item_acquisitions: tuple[str, ...] = ()
    previous_item_acquisitions: tuple[str, ...] = ()
    previous_item_request_attempts: int = Field(default=0, ge=0, le=20)
    acquisition_identifier: str | None = None
    acquisition_summary: dict[str, JsonValue] | None = None
    pagination: dict[str, JsonValue] | None = None
    bootstrap: bool = False

    @model_validator(mode="after")
    def bounds(self) -> "SessionProgress":
        if (self.search is None) == (self.item is None):
            raise ValueError("eBay progress requires exactly one work payload")
        if (self.item is not None) != (self.phase == "item"):
            raise ValueError("eBay phase and work payload disagree")
        if any(
            len(values) > 20
            for values in (
                self.pages,
                self.page_attempts,
                self.home_attempts,
                self.item_acquisitions,
            )
        ):
            raise ValueError("eBay session progress exceeds its request bound")
        if len(self.model_dump_json().encode()) > 16 * 1024 * 1024:
            raise ValueError("eBay session progress exceeds its record bound")
        return self


class SessionPhasePayload(StrictModel):
    progress_identifier: str
    checkpoint_identifier: str | None = None


class _Envelope(StrictModel):
    work_kind: tuple[str, ...]
    payload_schema_version: int
    payload: JsonValue


def _resident(context: StageAttemptContext) -> str:
    if context.resident_session_identifier is None:
        raise ValueError("eBay session stages require a resident actor")
    return context.resident_session_identifier


def _peek(deps: EbaySessionStageDependencies, context: StageAttemptContext) -> EbayResident | None:
    handle = deps.residents.peek(_resident(context))
    if handle is None:
        return None
    if not isinstance(handle.value, EbayResident):
        raise ValueError("Resident actor does not own an eBay session")
    return handle.value


async def _load(
    deps: EbaySessionStageDependencies, payload: SessionPhasePayload
) -> SessionProgress:
    kind, schema, value = await deps.database.get_record(payload.progress_identifier)
    if kind != _PROGRESS or schema != 1:
        raise ValueError("Not an eBay session progress record")
    return SessionProgress.model_validate_json(encode_json(value))


def _item_dependencies(deps: EbaySessionStageDependencies) -> EbayItemWorkerDependencies:
    return EbayItemWorkerDependencies(
        database=deps.database,
        directories=deps.directories,
        new_identifier=deps.new_identifier,
        proton_manager=deps.proton_manager,
        item_network_path=deps.network_path,
        item_policy=deps.item_policy,
    )


def _search_dependencies(deps: EbaySessionStageDependencies) -> EbaySearchWorkerDependencies:
    return EbaySearchWorkerDependencies(
        deps.database, deps.directories, deps.new_identifier, utc_now_ns=deps.utc_now_ns
    )


def _factory(deps: EbaySessionStageDependencies, stack_identifier: str):
    # Capture only the immutable stack name and dependency bundle. Progress,
    # response buffers and page arrays must not survive in this generator frame.
    @asynccontextmanager
    async def opened(identifier: str) -> AsyncGenerator[EbayResident]:
        if deps.session_factory is not None:
            async with deps.session_factory(identifier) as acquirer:
                if not isinstance(acquirer, HttpCaptureAcquirer):
                    raise RouteConfigurationFailure("http_acquirer_does_not_support_capture")
                yield EbayResident(acquirer)
        else:
            loaded = load_configuration(deps.directories.configuration_file)
            route, credentials, transport = decodo_wreq_stack_settings(
                loaded, deps.directories, stack_identifier
            )
            acquirer = ManagedDecodoWreqAcquirer(
                settings=route, credential_source=credentials, transport_settings=transport
            )
            del loaded, route, credentials, transport
            async with acquirer.session(deps.new_identifier) as session:
                yield EbayResident(session)

    return opened


def _next(
    deps: EbaySessionStageDependencies,
    context: StageAttemptContext,
    progress: SessionProgress,
    phase: tuple[str, ...],
    *,
    checkpoint: HttpCheckpoint | None = None,
    records: tuple[RecordDraft, ...] = (),
    outputs: tuple[NamedOutput, ...] = (),
    delay: bool = False,
    child_bodies: bool = False,
) -> WaitWork:
    progress = SessionProgress.model_validate_json(progress.model_dump_json())
    identifier = deps.new_identifier()
    activity = None
    if phase == _CAPTURE:
        attempt = (
            progress.homepage_attempt + 1
            if progress.phase == "homepage"
            else progress.page_attempt + 1
        )
        activity = network_activity_definition(
            identifier=deps.new_identifier(),
            kind=(
                "carl",
                "ebay",
                "network_activity",
                "search_homepage"
                if progress.phase == "homepage"
                else "item_page"
                if progress.phase == "item"
                else "search_page",
            ),
            operation_identifier=context.operation_identifier,
            network_session_identifier=_resident(context),
            network_path=progress.network_path,
            ordinal=progress.page_attempt + 1 if progress.item else len(progress.pages) + 1,
            attempt=attempt,
        )
    holdoff = progress.item_policy if progress.item else progress.search_policy
    deadline = (
        deps.utc_now_ns()
        + int(
            uniform(holdoff.minimum_holdoff_seconds, holdoff.maximum_holdoff_seconds)
            * 1_000_000_000
        )
        if delay
        else 0
    )
    return WaitWork(
        continuation=StageDefinition(
            identifier=deps.new_identifier(),
            work_item_identifier=context.work_item_identifier,
            operation_identifier=context.operation_identifier,
            kind=phase,
            payload=SessionPhasePayload(
                progress_identifier=identifier,
                checkpoint_identifier=None if checkpoint is None else checkpoint.record_identifier,
            ).model_dump(mode="json"),
            resources=(ResourceRequest(name="http" if phase == _CAPTURE else "cpu"),),
            not_before_utc_ns=deadline,
            resident_session_identifier=_resident(context),
            network_activity=activity,
            checkpoint_inputs=(NamedInput(name=("progress",), object_identifier=identifier),)
            + (
                ()
                if checkpoint is None
                else (
                    NamedInput(
                        name=("http_checkpoint",), object_identifier=checkpoint.record_identifier
                    ),
                )
            ),
        ),
        wait_kind=StageWaitKind.TIME if deadline else StageWaitKind.RESOURCE,
        records=(
            *records,
            *(() if checkpoint is None else checkpoint.records),
            RecordDraft(
                identifier=identifier,
                kind=_PROGRESS,
                schema_version=1,
                value=progress.model_dump(mode="json"),
            ),
        ),
        artifacts=() if checkpoint is None or child_bodies else checkpoint.artifacts,
        outputs=(
            *outputs,
            *(
                ()
                if checkpoint is None
                else tuple(
                    output
                    for output in checkpoint.outputs
                    if not child_bodies or output.object_identifier == checkpoint.record_identifier
                )
            ),
            NamedOutput(name=("session_progress", identifier), object_identifier=identifier),
        ),
        result={"state": "collecting", "progress_record_identifier": identifier},
    )


def _checkpoint_failure(error: Exception) -> TerminalFailureWork:
    return TerminalFailureWork(
        error={"kind": "ebay_checkpoint_failure", "exception_type": type(error).__name__},
        result={"state": "checkpoint_failed"},
    )


def _plan(progress: SessionProgress, resident: EbayResident) -> RequestPlan:
    if progress.item is not None:
        return ebay_item_plan(progress.item.request, network_path=progress.network_path)
    assert progress.search is not None
    if progress.phase == "homepage":
        plan = ebay_homepage_plan(network_path=progress.network_path)
        if resident.homepage_requests > 0:
            plan = plan.model_copy(
                update={
                    "headers": (
                        Header(name=b"Referer", value=plan.url.encode("ascii")),
                        Header(name=b"Sec-Fetch-Site", value=b"same-origin"),
                    )
                }
            )
        return plan
    if (
        progress.page_number > 1
        and resident.pagination_token is not None
        and resident.previous_url is not None
    ):
        return ebay_fetch_plan(
            progress.search.request,
            network_path=progress.network_path,
            page_number=progress.page_number,
            token=resident.pagination_token,
            referer=resident.previous_url,
        )
    return ebay_search_plan(
        progress.search.request,
        network_path=progress.network_path,
        page_number=progress.page_number,
    )


def build_ebay_session_stage_registry(deps: EbaySessionStageDependencies) -> StageHandlerRegistry:
    async def prepare(payload: _Envelope, context: StageAttemptContext) -> StageHandlerResult:
        try:
            search = (
                CollectEbaySearchPayload.model_validate_json(encode_json(payload.payload))
                if payload.work_kind == COLLECT_EBAY_SEARCH_WORK_KIND
                else None
            )
            item = (
                CollectEbayItemPayload.model_validate_json(encode_json(payload.payload))
                if payload.work_kind == COLLECT_EBAY_ITEM_WORK_KIND
                else None
            )
            if search is not None:
                require_supported_ebay_search_acquisition(search.request)
                if context.retry_attempt(search.retry_attempt_offset) < 1:
                    return TerminalFailureWork(
                        error={"kind": "invalid_retry_attempt_offset"},
                        result={"state": "configuration_failed"},
                    )
            previous: tuple[str, ...] = ()
            previous_requests = 0
            if item is not None:
                prepared = await prepare_item_collection(item, context, _item_dependencies(deps))
                if not isinstance(prepared, tuple):
                    return prepared
                previous = prepared
                previous_requests = await deps.database.retained_ebay_item_request_attempts(
                    context.work_item_identifier,
                    before_attempt=context.attempt,
                    retry_budget_start_attempt=context.retry_budget_start_attempt,
                )
            network, retry_scope, fetch = deps.network_path, deps.retry_scope, deps.fetch_pagination
            search_policy, item_policy = (
                deps.search_policy or EbaySearchConfiguration(),
                deps.item_policy or EbayItemConfiguration(),
            )
            if deps.session_factory is None:
                loaded = load_configuration(deps.directories.configuration_file)
                request = search.request if search else item.request if item else None
                if request is None:
                    raise ValueError("Unknown eBay work kind")
                route, _, transport = decodo_wreq_stack_settings(
                    loaded, deps.directories, request.stack_identifier
                )
                network = route.route.network_path
                retry_scope = cast(
                    Literal["stack", "independent_session"], route.session_retry_scope()
                )
                fetch = (
                    transport.emulation_profile == "Chrome154"
                    and transport.emulation_platform == "Linux"
                )
                search_policy, item_policy = (
                    deps.search_policy or loaded.configuration.ebay_search,
                    deps.item_policy or loaded.configuration.ebay_item,
                )
            if item is not None and previous_requests >= item_policy.maximum_page_attempts:
                return TerminalFailureWork(
                    error={
                        "kind": "ebay_item_attempts_exhausted",
                        "maximum_attempts": item_policy.maximum_page_attempts,
                    },
                    result={
                        "state": "terminal_failure",
                        "acquisition_record_identifiers": list(previous),
                        "request_attempts": previous_requests,
                    },
                )
            progress = SessionProgress(
                search=search,
                item=item,
                network_path=network,
                search_policy=search_policy,
                item_policy=item_policy,
                retry_scope=retry_scope,
                fetch_pagination=fetch,
                run_identifier=deps.new_identifier(),
                phase="homepage" if search else "item",
                item_acquisitions=previous,
                previous_item_acquisitions=previous,
                previous_item_request_attempts=previous_requests,
                page_attempt=previous_requests if item else 0,
                pagination={"mode": "document", "fetch_pages": 0} if fetch and search else None,
            )
            records: tuple[RecordDraft, ...] = ()
            outputs: tuple[NamedOutput, ...] = ()
            if search is not None:
                binding = deps.new_identifier()
                records = (
                    RecordDraft(
                        identifier=binding,
                        kind=("carl", "ebay", "search_attempt"),
                        schema_version=1,
                        value={
                            "work_identifier": context.work_item_identifier,
                            "attempt": context.attempt,
                            "search_run_record_identifier": progress.run_identifier,
                        },
                    ),
                )
                outputs = (NamedOutput(name=("search", "attempt"), object_identifier=binding),)
            return _next(deps, context, progress, _CAPTURE, records=records, outputs=outputs)
        except (
            ConfigurationFailure,
            RouteConfigurationFailure,
            UnsupportedEbaySearchMode,
        ) as error:
            return TerminalFailureWork(
                error={"kind": "configuration_failure", "type": type(error).__name__},
                result={"state": "configuration_failed"},
            )

    async def capture(
        payload: SessionPhasePayload, context: StageAttemptContext
    ) -> StageHandlerResult:
        plan: RequestPlan | None = None
        started, started_utc = perf_counter_ns(), datetime.now(UTC).isoformat()
        try:
            progress = await _load(deps, payload)
        except (KeyError, ValueError, OSError) as error:
            return _checkpoint_failure(error)
        try:
            resident = _peek(deps, context)
            if resident is None and progress.phase == "page":
                # A parked/crashed incarnation lost its cookie jar and token.
                # Bootstrap explicitly, preserving all page/request budgets.
                if progress.homepage_attempt >= progress.search_policy.maximum_homepage_attempts:
                    assert progress.search is not None
                    return search_acquisition_failure_work(
                        progress.search,
                        context,
                        _search_dependencies(deps),
                        AcquisitionFailure(
                            "Resident bootstrap budget exhausted",
                            result={
                                "stopping_condition": "transport_failure",
                                "retry_scope": progress.retry_scope,
                            },
                        ),
                    )
                return _next(
                    deps,
                    context,
                    progress.model_copy(update={"phase": "homepage", "bootstrap": True}),
                    _CAPTURE,
                )
            request = (
                progress.search.request
                if progress.search
                else progress.item.request
                if progress.item
                else None
            )
            assert request is not None
            offset = progress.previous_item_request_attempts if progress.item else 0
            maximum = (
                progress.item_policy.maximum_page_attempts - offset
                if progress.item
                else progress.search_policy.maximum_homepage_attempts
                if progress.phase == "homepage"
                else progress.search_policy.maximum_page_attempts
            )
            identity = (
                ("ebay", "item")
                if progress.item
                else ("ebay", "homepage")
                if progress.phase == "homepage"
                else ("ebay", "page", str(progress.page_number))
            )
            charge = (
                await deps.database.charge_resident_request(
                    context,
                    request_identity=identity,
                    maximum_attempts=maximum,
                    now_utc_ns=deps.utc_now_ns(),
                )
                if maximum > 0
                else None
            )
            progress = progress.model_copy(
                update={
                    "homepage_attempt" if progress.phase == "homepage" else "page_attempt": offset
                    + (maximum if charge is None else charge.ordinal),
                }
            )
            if charge is None or not charge.newly_charged:
                raise AcquisitionFailure(
                    "A previous dispatch has exhausted this request's durable budget",
                    result={
                        "stopping_condition": "transport_failure",
                        "failure_phase": "uncertain_previous_dispatch",
                        "request_identity": list(identity),
                    },
                )
            handle = await deps.residents.get_or_open(
                _resident(context), _factory(deps, request.stack_identifier)
            )
            plan = _plan(progress, handle.value)
            if progress.phase == "homepage":
                handle.value.homepage_requests += 1
            started, started_utc = perf_counter_ns(), datetime.now(UTC).isoformat()
            try:
                assert context.network_admission is not None
                acquisition = await acquire_for_network_activity(
                    handle.value.acquirer.capture(plan, deps.new_identifier),
                    context.network_admission,
                )
            except AcquisitionFailure as error:
                retained = completed_capture_after_close_failure(error, plan=plan)
                if retained is None:
                    raise
                acquisition = retained
        except (
            AcquisitionFailure,
            ConfigurationFailure,
            RouteConfigurationFailure,
            TimeoutError,
        ) as error:
            if isinstance(error, TimeoutError):
                error = AcquisitionFailure(
                    "eBay transport timed out",
                    result={
                        "stopping_condition": "transport_failure",
                        "exception_type": "TimeoutError",
                    },
                )
            if progress.search is None:
                outcome = _failure(
                    error,
                    context,
                    inputs=tuple(
                        NamedInput(name=("previous_acquisition", str(i)), object_identifier=value)
                        for i, value in enumerate(progress.item_acquisitions)
                    ),
                )
                if not isinstance(error, AcquisitionFailure):
                    return outcome
                try:
                    checkpoint = await checkpoint_acquisition(
                        Acquisition(error.result, error.bodies),
                        data_directory=deps.database.path.parent,
                        new_identifier=deps.new_identifier,
                    )
                except (OSError, ValueError) as failure:
                    return _checkpoint_failure(failure)
                assert isinstance(outcome.result, dict)
                return outcome.model_copy(
                    update={
                        "records": checkpoint.records,
                        "artifacts": checkpoint.artifacts,
                        "outputs": checkpoint.outputs,
                        "result": {
                            **outcome.result,
                            "acquisition_record_identifiers": list(progress.item_acquisitions),
                            "request_attempts": progress.page_attempt,
                        },
                    }
                )
            if not isinstance(error, AcquisitionFailure):
                return TerminalFailureWork(
                    error={"kind": "configuration_failure", "type": type(error).__name__},
                    result={"state": "configuration_failed"},
                )
            error.result.update(
                {
                    "retry_scope": progress.retry_scope,
                    "homepage_acquisition": progress.homepage,
                    "homepage_attempts": [] if progress.homepage else list(progress.home_attempts),
                    "search_page_attempts": [
                        *[
                            attempt
                            for page in progress.pages
                            for attempt in cast(list[JsonValue], page["attempts"])
                        ],
                        *progress.page_attempts,
                    ],
                }
            )
            outcome = search_acquisition_failure_work(
                progress.search, context, _search_dependencies(deps), error
            )
            if plan is None:
                return outcome
            try:
                checkpoint = await checkpoint_acquisition(
                    Acquisition(error.result, error.bodies),
                    data_directory=deps.database.path.parent,
                    new_identifier=deps.new_identifier,
                )
            except (OSError, ValueError) as failure:
                return _checkpoint_failure(failure)
            operation_identifier, acquisition_identifier = (
                deps.new_identifier(),
                deps.new_identifier(),
            )
            homepage = progress.phase == "homepage"
            failed_record = RecordDraft(
                identifier=acquisition_identifier,
                kind=("carl", "http", "acquisition"),
                schema_version=1,
                value={
                    **error.result,
                    "operation_identifier": operation_identifier,
                    "purpose": "ebay_search_homepage" if homepage else "ebay_search_page",
                    "ebay_search_request": progress.search.request.model_dump(mode="json"),
                    "acquisition_stack_identifier": progress.search.request.stack_identifier,
                    "page_number": None if homepage else progress.page_number,
                    "page_ordinal": None if homepage else len(progress.pages) + 1,
                    "request_attempt": progress.homepage_attempt
                    if homepage
                    else progress.page_attempt,
                    "search_run_record_identifier": progress.run_identifier,
                },
            )
            child = ChildOperationDraft(
                identifier=operation_identifier,
                component=build_ebay_component_registry().require(
                    COLLECT_EBAY_SEARCH_HOMEPAGE if homepage else COLLECT_EBAY_SEARCH
                ),
                configuration={"request_plan": plan.as_json()},
                inputs=(),
                records=(failed_record,),
                artifacts=checkpoint.artifacts,
                outputs=(
                    NamedOutput(name=("acquisition",), object_identifier=acquisition_identifier),
                    *(
                        output
                        for output in checkpoint.outputs
                        if output.object_identifier != checkpoint.record_identifier
                    ),
                ),
                result={
                    "acquisition_record_identifier": acquisition_identifier,
                    "acquisition": error.result,
                },
                started_at_utc=started_utc,
                ended_at_utc=datetime.now(UTC).isoformat(),
                duration_ns=perf_counter_ns() - started,
                error={
                    "type": "AcquisitionFailure",
                    "failure_phase": error.result.get("failure_phase"),
                },
            )
            return StagePublication(
                outcome.model_copy(
                    update={
                        "records": checkpoint.records,
                        "artifacts": (),
                        "outputs": tuple(
                            output
                            for output in checkpoint.outputs
                            if output.object_identifier == checkpoint.record_identifier
                        ),
                        "inputs": (
                            *outcome.inputs,
                            NamedInput(
                                name=("failed_acquisition",),
                                object_identifier=acquisition_identifier,
                            ),
                        ),
                    }
                ),
                (child,),
            )
        try:
            checkpoint = await checkpoint_acquisition(
                acquisition,
                data_directory=deps.database.path.parent,
                new_identifier=deps.new_identifier,
            )
            if progress.search is not None:
                attempt = (
                    progress.homepage_attempt
                    if progress.phase == "homepage"
                    else progress.page_attempt
                )
                child, summary = build_search_document_draft(
                    acquisition=acquisition,
                    request=progress.search.request,
                    plan=plan,
                    page_number=None if progress.phase == "homepage" else progress.page_number,
                    page_ordinal=None if progress.phase == "homepage" else len(progress.pages) + 1,
                    search_run_identifier=progress.run_identifier,
                    operation_identifier=deps.new_identifier(),
                    new_identifier=deps.new_identifier,
                    started_at_utc=started_utc,
                    ended_at_utc=datetime.now(UTC).isoformat(),
                    duration_ns=perf_counter_ns() - started,
                    artifacts=checkpoint.artifacts,
                    request_attempt=attempt,
                )
                updated = {
                    "acquisition_identifier": summary["acquisition_record_identifier"],
                    "acquisition_summary": summary,
                    "homepage_attempt" if progress.phase == "homepage" else "page_attempt": attempt,
                }
                if progress.pagination is not None and any(
                    header.name.lower() == b"x-pl-config" for header in plan.headers
                ):
                    updated["pagination"] = {
                        **progress.pagination,
                        "mode": "fetch",
                        "fetch_pages": int(progress.pagination["fetch_pages"]) + 1,
                    }
                outcome = _next(
                    deps,
                    context,
                    progress.model_copy(update=updated),
                    _PROCESS,
                    checkpoint=checkpoint,
                    child_bodies=True,
                )
                return StagePublication(outcome, (child,))
            assert progress.item is not None
            identifier = deps.new_identifier()
            identifiers = (*progress.item_acquisitions, identifier)
            record = RecordDraft(
                identifier=identifier,
                kind=("carl", "http", "acquisition"),
                schema_version=1,
                value={
                    **acquisition.record,
                    "purpose": "ebay_item",
                    "ebay_item_request": progress.item.request.model_dump(mode="json"),
                    "request_attempt": progress.page_attempt,
                    "collection_policy": progress.item_policy.model_dump(mode="json"),
                },
            )
            return _next(
                deps,
                context,
                progress.model_copy(
                    update={"item_acquisitions": identifiers, "acquisition_identifier": identifier}
                ),
                _PROCESS,
                checkpoint=checkpoint,
                records=(record,),
                outputs=(
                    NamedOutput(
                        name=("request_attempt", str(progress.page_attempt), "acquisition"),
                        object_identifier=identifier,
                    ),
                ),
            )
        except (ValueError, OSError) as error:
            # Retrying this local failure would buy an already-completed body.
            return _checkpoint_failure(error)

    async def process(
        payload: SessionPhasePayload, context: StageAttemptContext
    ) -> StageHandlerResult:
        try:
            progress = await _load(deps, payload)
            if payload.checkpoint_identifier is None:
                raise ValueError("Missing HTTP checkpoint")
            acquisition = await restore_acquisition(deps.database, payload.checkpoint_identifier)
            if progress.item is not None:
                extracted = await run_cpu(_item_response, acquisition, progress.item.request)
                blocked = (
                    extracted is not None
                    and extracted.classification == EbayItemResponseKind.CHALLENGE
                )
                if blocked and progress.page_attempt < progress.item_policy.maximum_page_attempts:
                    return _next(deps, context, progress, _CAPTURE, delay=True)
                outcome = complete_item_collection(
                    progress.item,
                    context,
                    _item_dependencies(deps),
                    list(progress.item_acquisitions),
                    extracted,
                    blocked,
                    progress.item_policy,
                    inputs=tuple(
                        NamedInput(name=("previous_acquisition", str(i)), object_identifier=value)
                        for i, value in enumerate(progress.previous_item_acquisitions)
                    ),
                )
                result = outcome.result
                assert isinstance(result, dict)
                result = {**result, "request_attempts": progress.page_attempt}
                if (
                    isinstance(outcome, RetryWork)
                    and progress.page_attempt >= progress.item_policy.maximum_page_attempts
                ):
                    reason = outcome.reason
                    assert isinstance(reason, dict)
                    return TerminalFailureWork(
                        inputs=outcome.inputs,
                        error={**reason, "policy_attempt": progress.page_attempt},
                        result={**result, "state": "terminal_failure"},
                    )
                return outcome.model_copy(update={"result": result})
            assert progress.search is not None and progress.acquisition_summary is not None
            summary = progress.acquisition_summary
            if progress.phase == "homepage":
                attempts = (
                    *progress.home_attempts,
                    _attempt_summary(summary, progress.homepage_attempt),
                )
                progress = progress.model_copy(
                    update={
                        "home_attempts": attempts,
                        "homepage": {**summary, "attempts": list(attempts)},
                    }
                )
                if (
                    summary["http_status"] == 403
                    and progress.homepage_attempt < progress.search_policy.maximum_homepage_attempts
                ):
                    return _next(deps, context, progress, _CAPTURE, delay=True)
                return _next(
                    deps,
                    context,
                    progress.model_copy(update={"phase": "page", "bootstrap": False}),
                    _CAPTURE,
                    delay=True,
                )
            response = _terminal_response(acquisition.record)
            body = response.get("body")
            body_identifier = body.get("artifact_id") if isinstance(body, dict) else None
            retained_body = next(
                (body for body in acquisition.bodies if body.identifier == body_identifier), None
            )
            if retained_body is None or not isinstance(progress.acquisition_identifier, str):
                raise ValueError("Missing terminal search body")
            metadata = await deps.database.get_external_artifact_metadata(retained_body.identifier)
            if metadata is None:
                raise ValueError("Missing external search body metadata")
            _, _, acquisition_value = await deps.database.get_record(
                progress.acquisition_identifier
            )
            if not isinstance(acquisition_value, dict):
                raise ValueError("Malformed search acquisition")
            child, extracted, html = await build_search_extraction_draft(
                acquisition_value=acquisition_value,
                acquisition_identifier=progress.acquisition_identifier,
                body_identifier=retained_body.identifier,
                body_metadata=metadata,
                stored_body=retained_body.content,
                request=progress.search.request,
                search_run_identifier=progress.run_identifier,
                page_number=progress.page_number,
                page_ordinal=len(progress.pages) + 1,
                operation_identifier=deps.new_identifier(),
                new_identifier=deps.new_identifier,
                started_at_utc=datetime.now(UTC).isoformat(),
                started_monotonic_ns=perf_counter_ns(),
            )
            page = {**summary, **extracted}
            attempts = (*progress.page_attempts, _attempt_summary(page, progress.page_attempt))
            classification = page.get("response_classification")
            kind = classification.get("kind") if isinstance(classification, dict) else None
            if (
                kind == "challenge" or page.get("http_status") in {403, 429}
            ) and progress.page_attempt < progress.search_policy.maximum_page_attempts:
                return StagePublication(
                    _next(
                        deps,
                        context,
                        progress.model_copy(update={"page_attempts": attempts}),
                        _CAPTURE,
                        delay=True,
                    ),
                    (child,),
                )
            page["attempts"] = list(attempts)
            pages = (*progress.pages, page)
            next_page = page.get("next_page_number")
            stop: str | None = None
            if kind in {"empty_results", "challenge", "error_page", "http_error"}:
                stop = str(kind)
            elif kind != "usable_results":
                stop = "unrecognized_response"
            elif len(pages) >= progress.search.request.maximum_pages:
                stop = "maximum_pages"
            elif next_page is None:
                stop = "no_next_page"
            elif type(next_page) is not int or next_page != progress.page_number + 1:
                stop = "invalid_next_page"
            if stop is None:
                resident = _peek(deps, context)
                pagination = progress.pagination
                if progress.fetch_pagination and resident is not None:
                    url = acquisition.record.get("effective_url")
                    if isinstance(url, str) and is_ebay_search_url(url):
                        resident.previous_url = url
                        if len(pages) == 1:
                            try:
                                resident.pagination_token = ebay_pagination_token(html)
                            except ValueError as error:
                                assert pagination is not None
                                pagination = {**pagination, "fallback_reason": str(error)}
                    else:
                        resident.previous_url = resident.pagination_token = None
                        assert pagination is not None
                        pagination = {
                            **pagination,
                            "fallback_reason": "previous_page_not_ebay_search",
                        }
                return StagePublication(
                    _next(
                        deps,
                        context,
                        progress.model_copy(
                            update={
                                "pages": pages,
                                "page_number": next_page,
                                "page_attempt": 0,
                                "page_attempts": (),
                                "pagination": pagination,
                            }
                        ),
                        _CAPTURE,
                        delay=True,
                    ),
                    (child,),
                )
            finalized, final_summary = build_search_finalization_draft(
                request=progress.search.request,
                search_run_identifier=progress.run_identifier,
                pages=pages,
                stopping_reason=stop,
                new_identifier=deps.new_identifier,
                policy=progress.search_policy,
                homepage_acquisition=progress.homepage,
                pagination=progress.pagination,
            )
            first = pages[0]
            result: dict[str, JsonValue] = {
                "state": "response_failed"
                if kind in EBAY_SEARCH_FAILURE_KINDS or stop == "invalid_next_page"
                else "completed",
                "acquisition_operation_identifier": first["acquisition_operation_identifier"],
                "acquisition_record_identifier": first["acquisition_record_identifier"],
                "acquisition_record_identifiers": [
                    page["acquisition_record_identifier"] for page in pages
                ],
                "extraction_record_identifier": first["extraction_record_identifier"],
                "extraction_record_identifiers": [
                    page["extraction_record_identifier"] for page in pages
                ],
                "response_classification": pages[-1]["response_classification"],
                "homepage_acquisition": progress.homepage,
                "retry_scope": progress.retry_scope,
                **final_summary,
            }
            return StagePublication(
                search_result_work(progress.search, context, _search_dependencies(deps), result),
                (child, finalized),
            )
        except (ValueError, KeyError, OSError) as error:
            return _checkpoint_failure(error)

    def initial(work: WorkDefinition, identifier: str, operation: str) -> StageDefinition:
        return initial_work_stage(
            work,
            identifier=identifier,
            operation_identifier=operation,
            kind=_PREPARE,
            resources=(ResourceRequest(name="cpu"),),
        ).model_copy(update={"resident_session_identifier": work.identifier})

    item_component = build_ebay_item_worker_registry(_item_dependencies(deps)).handlers[0].component
    return StageHandlerRegistry(
        adapters=(
            *tuple(
                WorkStageAdapter(
                    WorkCapability(
                        kind=COLLECT_EBAY_SEARCH_WORK_KIND, payload_schema_version=schema
                    ),
                    build_ebay_component_registry().require(RUN_EBAY_SEARCH_WORK),
                    initial,
                )
                for schema in (
                    LEGACY_COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
                    COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
                )
            ),
            WorkStageAdapter(
                WorkCapability(kind=COLLECT_EBAY_ITEM_WORK_KIND, payload_schema_version=1),
                item_component,
                initial,
            ),
        ),
        handlers=(
            TypedStageHandler(_PREPARE, 1, _Envelope, prepare),
            TypedStageHandler(_CAPTURE, 1, SessionPhasePayload, capture),
            TypedStageHandler(_PROCESS, 1, SessionPhasePayload, process),
        ),
    )
