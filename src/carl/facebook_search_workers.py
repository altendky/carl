"""Durable worker handler for bounded Facebook Marketplace searches."""

from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass
from functools import partial

import anyio

from carl.core.components import Component
from carl.core.content_encoding import decoded_stored_body
from carl.core.facebook import parse_json_blocks
from carl.core.facebook_search import (
    OverlappingPricePartitionSearchTraversalStrategy,
    PricePartitionSearchTraversalState,
    SearchPricePartition,
    SearchTraversalAction,
    SearchTraversalState,
    account_search_elapsed_interval,
    advance_price_partition_search_traversal,
    advance_search_traversal,
    plan_overlapping_price_partitions,
)
from carl.core.facebook_search_protocol import (
    AppliedSearchConfiguration,
    SearchBootstrapExtraction,
    SearchBootstrapResponseKind,
    SearchPageExtraction,
    SearchPaginationExtraction,
    SearchProtocolIssueKind,
    SearchRouteDefinitionExtraction,
    SearchSessionBootstrapResponseKind,
    classify_search_route_definition_response,
    classify_search_session_bootstrap_response,
    extract_search_pagination,
    extract_search_route_definition,
    traversal_page_facts,
)
from carl.core.facebook_search_requests import (
    search_bootstrap_plan,
    search_pagination_request,
    search_route_definition_request,
    search_session_bootstrap_plan,
)
from carl.core.facebook_search_session import (
    SearchSessionMaterialEvidence,
    SearchSessionMaterialIssue,
    extract_search_session_material,
)
from carl.core.facebook_work import (
    RETRYABLE_SEARCH_SESSION_FAILURE_CODES,
    SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND,
    SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
    SEARCH_SESSION_BOOTSTRAP_NETWORK_ACTIVITY_KIND,
    SEARCH_TRANSPORT_MAXIMUM_ATTEMPTS,
    CollectSearchPayload,
    FacebookSearchRequest,
    SearchPriceRange,
    facebook_search_network_activity,
)
from carl.core.http import stored_content_encodings, stored_response_charset
from carl.core.models import BytesDraft, Header, JsonValue, NamedOutput, RecordDraft
from carl.core.work import NetworkActivityAdmission
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.io.facebook_search import FacebookSearchSessionFactory, FacebookSearchSessionFailure
from carl.io.httpx import Acquisition, AcquisitionFailure, IdentifierFactory
from carl.io.network_activity import NetworkActivityScheduler, network_activity_definition
from carl.io.sqlite import Database

SEARCH_RETRY_DELAY_NS = 1_000_000_000


def _search_retry_delay_ns(attempt: int) -> int:
    return min(60, 2 ** (attempt - 1)) * SEARCH_RETRY_DELAY_NS


def search_session_failure_work(
    *,
    error: FacebookSearchSessionFailure,
    context: AttemptContext,
    search_run_identifier: str,
    routing: tuple[str, ...],
    policy_attempt: int | None = None,
) -> RetryWork | TerminalFailureWork:
    """Classify a failure opening or closing a routed search session."""

    retryable = error.code in RETRYABLE_SEARCH_SESSION_FAILURE_CODES
    failure: dict[str, JsonValue] = {
        "kind": "network_session_failure",
        "provider": error.provider,
        "code": error.code,
        "exit_code": error.exit_code,
        "diagnostic": error.diagnostic,
    }
    result: dict[str, JsonValue] = {
        "state": "transport_failed",
        "search_run_identifier": search_run_identifier,
        "attempt": context.attempt,
        "network_path": list(routing),
    }
    effective_attempt = context.retry_attempt() if policy_attempt is None else policy_attempt
    if retryable and effective_attempt < SEARCH_TRANSPORT_MAXIMUM_ATTEMPTS:
        return RetryWork(
            delay_ns=_search_retry_delay_ns(effective_attempt),
            reason={**failure, "decision": "retry"},
            result=result,
        )
    return TerminalFailureWork(
        error={
            **failure,
            "decision": "retry_exhausted" if retryable else "terminal",
        },
        result=result,
    )


def search_acquisition_failure_work(
    *,
    error: AcquisitionFailure,
    context: AttemptContext,
    search_run_identifier: str,
    policy_attempt: int | None = None,
) -> RetryWork | TerminalFailureWork:
    """Classify a failed search request while retaining its transport evidence."""

    failure = {
        "kind": "search_acquisition_failure",
        "stopping_condition": error.result.get("stopping_condition"),
        "exception_type": error.result.get("exception_type"),
    }
    result: dict[str, JsonValue] = {
        "state": "acquisition_failed",
        "search_run_identifier": search_run_identifier,
        "attempt": context.attempt,
        "acquisition": error.result,
    }
    artifacts = _body_artifacts(Acquisition(record=error.result, bodies=error.bodies))
    effective_attempt = context.retry_attempt() if policy_attempt is None else policy_attempt
    if (
        error.result.get("stopping_condition") == "transport_failure"
        and effective_attempt < SEARCH_TRANSPORT_MAXIMUM_ATTEMPTS
    ):
        return RetryWork(
            artifacts=artifacts,
            delay_ns=_search_retry_delay_ns(effective_attempt),
            reason={**failure, "decision": "retry"},
            result=result,
        )
    return TerminalFailureWork(
        artifacts=artifacts,
        error={
            **failure,
            "decision": (
                "retry_exhausted"
                if error.result.get("stopping_condition") == "transport_failure"
                else "terminal"
            ),
        },
        result=result,
    )


@dataclass(frozen=True, slots=True)
class FacebookSearchWorkerDependencies:
    database: Database
    session_factory: FacebookSearchSessionFactory
    navigation_headers: tuple[Header, ...]
    new_identifier: IdentifierFactory
    utc_now_ns: Callable[[], int]
    monotonic_ns: Callable[[], int]
    network_activity_scheduler: NetworkActivityScheduler


@dataclass(frozen=True, slots=True)
class _DecodedResponse:
    text: str
    content: bytes
    charset: str
    charset_source: str
    received_content_bytes: int
    body_identifier: str
    terminal_response: dict[str, JsonValue]


def _terminal_response(acquisition: Acquisition) -> dict[str, JsonValue]:
    hops = acquisition.record.get("hops")
    if not isinstance(hops, list) or not hops or not isinstance(hops[-1], dict):
        raise ValueError("Search acquisition has no terminal response")
    response = hops[-1].get("response")
    if not isinstance(response, dict):
        raise ValueError("Search acquisition terminal response is malformed")
    return response


def _decode_acquisition(acquisition: Acquisition) -> _DecodedResponse:
    response = _terminal_response(acquisition)
    status = response.get("status_code")
    if status != 200:
        raise ValueError("Search acquisition did not return HTTP 200")
    body = response.get("body")
    if not isinstance(body, dict) or body.get("state") != "available":
        raise ValueError("Search acquisition has no complete terminal body")
    body_identifier = body.get("artifact_id")
    if not isinstance(body_identifier, str):
        raise ValueError("Search acquisition body reference is malformed")
    acquired_body = next(
        (candidate for candidate in acquisition.bodies if candidate.identifier == body_identifier),
        None,
    )
    if acquired_body is None:
        raise ValueError("Search acquisition body artifact is missing")
    headers = response.get("headers")
    decoded = decoded_stored_body(
        acquired_body.content,
        acquired_body.representation,
        stored_content_encodings(headers),
    )
    received_content_bytes = body.get("received_content_bytes", len(acquired_body.content))
    if not isinstance(received_content_bytes, int) or received_content_bytes < 0:
        raise ValueError("Search acquisition received-content size is invalid")
    charset, charset_source = stored_response_charset(headers)
    try:
        text = decoded.decode(charset)
    except (LookupError, UnicodeError):
        text = decoded.decode("utf-8", errors="replace")
        charset = "utf-8"
        charset_source = "fallback_after_decode_failure"
    return _DecodedResponse(
        text=text,
        content=decoded,
        charset=charset,
        charset_source=charset_source,
        received_content_bytes=received_content_bytes,
        body_identifier=body_identifier,
        terminal_response=response,
    )


def _body_artifacts(acquisition: Acquisition) -> tuple[BytesDraft, ...]:
    return tuple(
        BytesDraft(
            identifier=body.identifier,
            kind=("carl", "http", "response_body"),
            media_type=body.media_type,
            representation=body.representation,
            content=body.content,
        )
        for body in acquisition.bodies
    )


def _bind_network_activity(
    acquisition: Acquisition,
    admission: NetworkActivityAdmission,
) -> Acquisition:
    return Acquisition(
        record={
            **acquisition.record,
            "network_activity_identifier": admission.activity_identifier,
            "network_activity_admitted_at_utc_ns": admission.admitted_at_utc_ns,
        },
        bodies=acquisition.bodies,
    )


async def _acquire_for_network_activity(
    acquisition: Awaitable[Acquisition],
    admission: NetworkActivityAdmission,
) -> Acquisition:
    try:
        result = await acquisition
    except AcquisitionFailure as error:
        raise AcquisitionFailure(
            str(error),
            result={
                **error.result,
                "network_activity_identifier": admission.activity_identifier,
                "network_activity_admitted_at_utc_ns": admission.admitted_at_utc_ns,
            },
            bodies=error.bodies,
        ) from error
    return _bind_network_activity(result, admission)


async def _checkpoint_session_bootstrap(
    *,
    dependencies: FacebookSearchWorkerDependencies,
    context: AttemptContext,
    extractor_component: Component,
    search_run_identifier: str,
    network_session_identifier: str,
    acquisition_identifier: str,
    acquisition: Acquisition,
    decoded: _DecodedResponse,
    blocks: tuple[dict[str, JsonValue], ...],
    response_classification: dict[str, JsonValue],
    session_evidence: SearchSessionMaterialEvidence | None,
    session_issues: tuple[SearchSessionMaterialIssue, ...],
) -> None:
    decoded_identifier = dependencies.new_identifier()
    extraction_identifier = dependencies.new_identifier()
    acquisition_record = dict(acquisition.record)
    acquisition_record.update(
        {
            "operation_identifier": context.operation_identifier,
            "search_run_identifier": search_run_identifier,
            "network_session_identifier": network_session_identifier,
            "purpose": "marketplace_session_bootstrap",
        }
    )
    records: list[RecordDraft] = [
        RecordDraft(
            identifier=acquisition_identifier,
            kind=("carl", "http", "acquisition"),
            schema_version=1,
            value=acquisition_record,
        )
    ]
    outputs = [
        NamedOutput(
            name=("search", "session_bootstrap", "acquisition"),
            object_identifier=acquisition_identifier,
        ),
        NamedOutput(
            name=("search", "session_bootstrap", "decoded_body"),
            object_identifier=decoded_identifier,
        ),
        NamedOutput(
            name=("search", "session_bootstrap", "extraction"),
            object_identifier=extraction_identifier,
        ),
    ]
    block_identifiers: list[str] = []
    for block in blocks:
        block_identifier = dependencies.new_identifier()
        block_identifiers.append(block_identifier)
        records.append(
            RecordDraft(
                identifier=block_identifier,
                kind=("carl", "html", "embedded_json_block"),
                schema_version=1,
                value={
                    **block,
                    "acquisition_record_identifier": acquisition_identifier,
                    "decoded_body_artifact_identifier": decoded_identifier,
                    "extractor": {
                        "component_parts": list(extractor_component.identifier.parts),
                        "output_schema_version": extractor_component.output_schema_version,
                    },
                },
            )
        )
        outputs.append(
            NamedOutput(
                name=(
                    "search",
                    "session_bootstrap",
                    "json_block",
                    str(block["block_index"]),
                ),
                object_identifier=block_identifier,
            )
        )
    records.append(
        RecordDraft(
            identifier=extraction_identifier,
            kind=("carl", "facebook", "search_session_bootstrap_extraction"),
            schema_version=1,
            value={
                "search_run_identifier": search_run_identifier,
                "acquisition_record_identifier": acquisition_identifier,
                "decoded_body_artifact_identifier": decoded_identifier,
                "extractor": {
                    "component_parts": list(extractor_component.identifier.parts),
                    "output_schema_version": extractor_component.output_schema_version,
                },
                "response_classification": response_classification,
                "json_block_record_identifiers": block_identifiers,
                "parse_error_count": sum(block["parse_error"] is not None for block in blocks),
                "protected_session_material_evidence": (
                    None if session_evidence is None else session_evidence.model_dump(mode="json")
                ),
                "session_material_issues": [
                    issue.model_dump(mode="json") for issue in session_issues
                ],
            },
        )
    )
    artifacts = [*_body_artifacts(acquisition)]
    artifacts.append(
        BytesDraft(
            identifier=decoded_identifier,
            kind=("carl", "http", "decoded_text"),
            media_type="text/html; charset=utf-8",
            representation={
                "kind": "content_decoded_utf8_text",
                "derived_from_artifact_identifier": decoded.body_identifier,
                "content_encodings_removed": list(
                    stored_content_encodings(decoded.terminal_response.get("headers"))
                ),
                "source_charset": decoded.charset,
                "charset_source": decoded.charset_source,
                "exact_wire_bytes": False,
            },
            content=decoded.text.encode("utf-8"),
        )
    )
    outputs.extend(
        NamedOutput(
            name=("search", "session_bootstrap", "response_body", str(index)),
            object_identifier=body.identifier,
        )
        for index, body in enumerate(acquisition.bodies)
    )
    await dependencies.database.publish_leased_operation_checkpoint(
        work_item_identifier=context.work_item_identifier,
        lease_token=context.lease_token,
        worker_identifier=context.worker_identifier,
        utc_now_ns=dependencies.utc_now_ns,
        operation_id=context.operation_identifier,
        records=records,
        artifacts=artifacts,
        outputs=outputs,
        checkpoint_result={
            "state": "collecting",
            "search_run_identifier": search_run_identifier,
            "session_bootstrap_persisted": True,
            "pages_persisted": 0,
            "traversal_state": None,
        },
    )


async def _checkpoint_page(
    *,
    dependencies: FacebookSearchWorkerDependencies,
    context: AttemptContext,
    extractor_component: Component,
    search_run_identifier: str,
    network_session_identifier: str,
    page_ordinal: int,
    page_kind: str,
    acquisition_identifier: str,
    acquisition: Acquisition,
    decoded: _DecodedResponse,
    page: SearchPageExtraction | None,
    extraction: (
        SearchBootstrapExtraction | SearchRouteDefinitionExtraction | SearchPaginationExtraction
    ),
    applied_configuration: AppliedSearchConfiguration | None,
    session_evidence: SearchSessionMaterialEvidence | None,
    response_classification: dict[str, JsonValue] | None,
    traversal_state: SearchTraversalState | PricePartitionSearchTraversalState | None,
    requested_search: FacebookSearchRequest,
    price_partition: SearchPricePartition | None = None,
    price_partition_planner_component: Component | None = None,
) -> tuple[str, ...]:
    decoded_identifier = dependencies.new_identifier()
    extraction_identifier = dependencies.new_identifier()
    record = dict(acquisition.record)
    record.update(
        {
            "operation_identifier": context.operation_identifier,
            "search_run_identifier": search_run_identifier,
            "network_session_identifier": network_session_identifier,
            "page_ordinal": page_ordinal,
            "page_kind": page_kind,
            "requested_search": requested_search.model_dump(mode="json"),
            "price_partition": (
                None if price_partition is None else price_partition.model_dump(mode="json")
            ),
        }
    )
    records: list[RecordDraft] = [
        RecordDraft(
            identifier=acquisition_identifier,
            kind=("carl", "http", "acquisition"),
            schema_version=1,
            value=record,
        )
    ]
    outputs = [
        NamedOutput(
            name=("search", "page", str(page_ordinal), "acquisition"),
            object_identifier=acquisition_identifier,
        ),
        NamedOutput(
            name=("search", "page", str(page_ordinal), "decoded_body"),
            object_identifier=decoded_identifier,
        ),
        NamedOutput(
            name=("search", "page", str(page_ordinal), "extraction"),
            object_identifier=extraction_identifier,
        ),
    ]
    occurrence_identifiers: list[str] = []
    if page is not None:
        for occurrence in page.listing_occurrences:
            identifier = dependencies.new_identifier()
            occurrence_identifiers.append(identifier)
            records.append(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "facebook", "search_listing_occurrence"),
                    schema_version=1,
                    value={
                        **occurrence.model_dump(mode="json"),
                        "search_run_identifier": search_run_identifier,
                        "page_ordinal": page_ordinal,
                        "requested_search": requested_search.model_dump(mode="json"),
                        "price_partition": (
                            None
                            if price_partition is None
                            else price_partition.model_dump(mode="json")
                        ),
                        "acquisition_record_identifier": acquisition_identifier,
                        "extractor": {
                            "component_parts": list(extractor_component.identifier.parts),
                            "output_schema_version": extractor_component.output_schema_version,
                        },
                    },
                )
            )
            outputs.append(
                NamedOutput(
                    name=(
                        "search",
                        "page",
                        str(page_ordinal),
                        "listing_occurrence",
                        str(occurrence.edge_index),
                    ),
                    object_identifier=identifier,
                )
            )
    extraction_value: dict[str, JsonValue] = {
        "search_run_identifier": search_run_identifier,
        "page_ordinal": page_ordinal,
        "page_kind": page_kind,
        "requested_search": requested_search.model_dump(mode="json"),
        "price_partition": (
            None if price_partition is None else price_partition.model_dump(mode="json")
        ),
        "price_partition_planner": (
            None
            if price_partition_planner_component is None
            else {
                "component_parts": list(price_partition_planner_component.identifier.parts),
                "output_schema_version": (price_partition_planner_component.output_schema_version),
            }
        ),
        "acquisition_record_identifier": acquisition_identifier,
        "decoded_body_artifact_identifier": decoded_identifier,
        "extractor": {
            "component_parts": list(extractor_component.identifier.parts),
            "output_schema_version": extractor_component.output_schema_version,
        },
        "issues": [issue.model_dump(mode="json") for issue in extraction.issues],
        "page": (
            None
            if page is None
            else {
                "sources": [source.model_dump(mode="json") for source in page.sources],
                "page_information": page.page_information.model_dump(mode="json"),
                "feed_session_identifier": page.feed_session_identifier,
                "listing_occurrence_record_identifiers": occurrence_identifiers,
                "ignored_edges": [edge.model_dump(mode="json") for edge in page.ignored_edges],
            }
        ),
        "applied_configuration": (
            None if applied_configuration is None else applied_configuration.model_dump(mode="json")
        ),
        "protected_session_material_evidence": (
            None if session_evidence is None else session_evidence.model_dump(mode="json")
        ),
        "response_classification": response_classification,
        "traversal_state": (
            None if traversal_state is None else traversal_state.model_dump(mode="json")
        ),
    }
    if isinstance(extraction, SearchBootstrapExtraction):
        block_identifiers: list[str] = []
        for block in extraction.blocks:
            identifier = dependencies.new_identifier()
            block_identifiers.append(identifier)
            records.append(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "html", "embedded_json_block"),
                    schema_version=1,
                    value={
                        **block,
                        "acquisition_record_identifier": acquisition_identifier,
                        "decoded_body_artifact_identifier": decoded_identifier,
                        "extractor": {
                            "component_parts": list(extractor_component.identifier.parts),
                            "output_schema_version": (extractor_component.output_schema_version),
                        },
                    },
                )
            )
            outputs.append(
                NamedOutput(
                    name=(
                        "search",
                        "page",
                        str(page_ordinal),
                        "json_block",
                        str(block["block_index"]),
                    ),
                    object_identifier=identifier,
                )
            )
        extraction_value["json_block_record_identifiers"] = block_identifiers
    elif isinstance(extraction, SearchRouteDefinitionExtraction):
        frame_identifiers: list[str] = []
        for frame in extraction.frames:
            identifier = dependencies.new_identifier()
            frame_identifiers.append(identifier)
            records.append(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "facebook", "route_definition_json_frame"),
                    schema_version=1,
                    value={
                        **frame.model_dump(mode="json"),
                        "acquisition_record_identifier": acquisition_identifier,
                        "decoded_body_artifact_identifier": decoded_identifier,
                        "extractor": {
                            "component_parts": list(extractor_component.identifier.parts),
                            "output_schema_version": extractor_component.output_schema_version,
                        },
                    },
                )
            )
            outputs.append(
                NamedOutput(
                    name=(
                        "search",
                        "page",
                        str(page_ordinal),
                        "json_frame",
                        str(frame.frame_index),
                    ),
                    object_identifier=identifier,
                )
            )
        extraction_value["json_frame_record_identifiers"] = frame_identifiers
    else:
        extraction_value["parsed_response"] = extraction.parsed_response
        extraction_value["graphql_errors"] = list(extraction.graphql_errors)
    records.append(
        RecordDraft(
            identifier=extraction_identifier,
            kind=("carl", "facebook", "search_page_extraction"),
            schema_version=1,
            value=extraction_value,
        )
    )
    artifacts = [*_body_artifacts(acquisition)]
    artifacts.append(
        BytesDraft(
            identifier=decoded_identifier,
            kind=("carl", "http", "decoded_text"),
            media_type=(
                "text/html; charset=utf-8"
                if page_kind == "bootstrap"
                else "application/json; charset=utf-8"
            ),
            representation={
                "kind": "content_decoded_utf8_text",
                "derived_from_artifact_identifier": decoded.body_identifier,
                "content_encodings_removed": list(
                    stored_content_encodings(decoded.terminal_response.get("headers"))
                ),
                "source_charset": decoded.charset,
                "charset_source": decoded.charset_source,
                "exact_wire_bytes": False,
            },
            content=decoded.text.encode("utf-8"),
        )
    )
    outputs.extend(
        NamedOutput(
            name=("search", "page", str(page_ordinal), "response_body", str(index)),
            object_identifier=body.identifier,
        )
        for index, body in enumerate(acquisition.bodies)
    )
    await dependencies.database.publish_leased_operation_checkpoint(
        work_item_identifier=context.work_item_identifier,
        lease_token=context.lease_token,
        worker_identifier=context.worker_identifier,
        utc_now_ns=dependencies.utc_now_ns,
        operation_id=context.operation_identifier,
        records=records,
        artifacts=artifacts,
        outputs=outputs,
        checkpoint_result={
            "state": "collecting",
            "search_run_identifier": search_run_identifier,
            "pages_persisted": page_ordinal,
            "traversal_state": (
                None if traversal_state is None else traversal_state.model_dump(mode="json")
            ),
        },
    )
    return tuple(occurrence_identifiers)


def build_search_handler(
    dependencies: FacebookSearchWorkerDependencies,
    *,
    session_bootstrap_extractor_component: Component,
    route_definition_extractor_component: Component,
    pagination_extractor_component: Component,
    price_partition_planner_component: Component,
) -> Callable[[CollectSearchPayload, AttemptContext], Awaitable[WorkOutcome]]:
    async def collect(payload: CollectSearchPayload, context: AttemptContext) -> WorkOutcome:
        policy_attempt = context.retry_attempt(payload.retry_attempt_offset)
        if policy_attempt < 1:
            raise ValueError("Search retry attempt offset is inconsistent with durable work")
        search_run_identifier = dependencies.new_identifier()
        network_session_identifier = dependencies.new_identifier()
        price_partitions: tuple[SearchPricePartition, ...] = ()
        current_price_partition: SearchPricePartition | None = None
        current_search_request = payload.request
        if isinstance(
            payload.traversal_strategy,
            OverlappingPricePartitionSearchTraversalStrategy,
        ):
            price = payload.request.price
            if price is None or price.maximum is None:
                raise ValueError("Price partition traversal requires a maximum price")
            price_partitions = plan_overlapping_price_partitions(
                currency=price.currency,
                minimum=price.minimum,
                maximum=price.maximum,
                strategy=payload.traversal_strategy,
            )
            current_price_partition = price_partitions[0]
            current_search_request = payload.request.model_copy(
                update={
                    "price": SearchPriceRange(
                        currency=current_price_partition.currency,
                        minimum=current_price_partition.minimum,
                        maximum=current_price_partition.maximum,
                    )
                }
            )
            traversal_state: SearchTraversalState | PricePartitionSearchTraversalState = (
                PricePartitionSearchTraversalState(
                    search_run_identifier=search_run_identifier,
                    attempt=context.attempt,
                    policy=payload.traversal,
                    strategy=payload.traversal_strategy,
                    partitions=price_partitions,
                )
            )
        else:
            traversal_state = SearchTraversalState(
                search_run_identifier=search_run_identifier,
                attempt=context.attempt,
                policy=payload.traversal,
            )
        bootstrap_plan = search_bootstrap_plan(
            current_search_request,
            routing=payload.routing,
            navigation_headers=dependencies.navigation_headers,
        )
        session_bootstrap_plan = search_session_bootstrap_plan(
            routing=payload.routing,
            navigation_headers=dependencies.navigation_headers,
        )
        page_references: list[dict[str, JsonValue]] = []
        applied_configuration: AppliedSearchConfiguration | None = None
        completed_session_observation: dict[str, JsonValue] | None = None
        elapsed_accounted_at = dependencies.monotonic_ns()
        try:
            session = None
            async with AsyncExitStack() as session_stack:
                setup_activity = network_activity_definition(
                    identifier=dependencies.new_identifier(),
                    kind=("carl", "facebook", "network_activity", "search_session_open"),
                    operation_identifier=context.operation_identifier,
                    network_session_identifier=network_session_identifier,
                    network_path=payload.routing,
                    attempt=context.attempt,
                )
                async with dependencies.network_activity_scheduler.admit(setup_activity) as permit:
                    permit.mark_dispatched()
                    session = await session_stack.enter_async_context(
                        dependencies.session_factory(network_session_identifier)
                    )
                session_bootstrap_activity = facebook_search_network_activity(
                    identifier=dependencies.new_identifier(),
                    kind=SEARCH_SESSION_BOOTSTRAP_NETWORK_ACTIVITY_KIND,
                    operation_identifier=context.operation_identifier,
                    network_session_identifier=network_session_identifier,
                    ordinal=1,
                    attempt=context.attempt,
                    routing=payload.routing,
                )
                async with dependencies.network_activity_scheduler.admit(
                    session_bootstrap_activity
                ) as permit:
                    permit.mark_dispatched()
                    session_bootstrap_acquisition = await _acquire_for_network_activity(
                        session.acquirer.acquire(
                            session_bootstrap_plan,
                            dependencies.new_identifier,
                        ),
                        permit.admission,
                    )
                session_bootstrap_decoded = await anyio.to_thread.run_sync(
                    _decode_acquisition,
                    session_bootstrap_acquisition,
                    abandon_on_cancel=True,
                )
                session_bootstrap_acquisition_identifier = dependencies.new_identifier()
                session_bootstrap_blocks = await anyio.to_thread.run_sync(
                    parse_json_blocks,
                    session_bootstrap_decoded.text,
                    abandon_on_cancel=True,
                )
                session_bootstrap_effective_url = session_bootstrap_decoded.terminal_response.get(
                    "url"
                )
                if not isinstance(session_bootstrap_effective_url, str):
                    raise ValueError("Search session bootstrap effective URL is malformed")
                session_bootstrap_classification = classify_search_session_bootstrap_response(
                    session_bootstrap_decoded.text,
                    effective_url=session_bootstrap_effective_url,
                )
                material_extraction = extract_search_session_material(
                    session_bootstrap_blocks,
                    acquisition_record_identifier=session_bootstrap_acquisition_identifier,
                )
                await _checkpoint_session_bootstrap(
                    dependencies=dependencies,
                    context=context,
                    extractor_component=session_bootstrap_extractor_component,
                    search_run_identifier=search_run_identifier,
                    network_session_identifier=network_session_identifier,
                    acquisition_identifier=session_bootstrap_acquisition_identifier,
                    acquisition=session_bootstrap_acquisition,
                    decoded=session_bootstrap_decoded,
                    blocks=session_bootstrap_blocks,
                    response_classification=session_bootstrap_classification.model_dump(
                        mode="json"
                    ),
                    session_evidence=material_extraction.evidence,
                    session_issues=material_extraction.issues,
                )
                if (
                    session_bootstrap_classification.kind
                    is not SearchSessionBootstrapResponseKind.MARKETPLACE_LANDING
                ):
                    return TerminalFailureWork(
                        error={
                            "kind": "search_session_bootstrap_response_failure",
                            "response_kind": session_bootstrap_classification.kind.value,
                        },
                        result={
                            "state": "failed",
                            "search_run_identifier": search_run_identifier,
                            "response_classification": (
                                session_bootstrap_classification.model_dump(mode="json")
                            ),
                        },
                    )
                if material_extraction.material is None:
                    return TerminalFailureWork(
                        error={"kind": "search_session_material_extraction_failure"},
                        result={
                            "state": "failed",
                            "search_run_identifier": search_run_identifier,
                            "session_material_issues": [
                                issue.model_dump(mode="json")
                                for issue in material_extraction.issues
                            ],
                        },
                    )
                page_ordinal = 1
                route_definition = search_route_definition_request(
                    bootstrap_plan=bootstrap_plan,
                    navigation_headers=dependencies.navigation_headers,
                    session_material=material_extraction.material,
                    request_number=1,
                )
                route_definition_activity = facebook_search_network_activity(
                    identifier=dependencies.new_identifier(),
                    kind=SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
                    operation_identifier=context.operation_identifier,
                    network_session_identifier=network_session_identifier,
                    ordinal=2,
                    attempt=context.attempt,
                    routing=payload.routing,
                )
                async with dependencies.network_activity_scheduler.admit(
                    route_definition_activity
                ) as permit:
                    permit.mark_dispatched()
                    acquisition = await _acquire_for_network_activity(
                        session.acquirer.acquire_form(
                            route_definition.plan,
                            route_definition.form_fields,
                            dependencies.new_identifier,
                        ),
                        permit.admission,
                    )
                decoded = await anyio.to_thread.run_sync(
                    _decode_acquisition,
                    acquisition,
                    abandon_on_cancel=True,
                )
                acquisition_identifier = dependencies.new_identifier()
                extraction = await anyio.to_thread.run_sync(
                    partial(
                        extract_search_route_definition,
                        decoded.text,
                        acquisition_record_identifier=acquisition_identifier,
                    ),
                    abandon_on_cancel=True,
                )
                applied_configuration = extraction.applied_configuration
                effective_url = decoded.terminal_response.get("url")
                if not isinstance(effective_url, str):
                    raise ValueError("Search acquisition effective URL is malformed")
                response_classification = classify_search_route_definition_response(
                    decoded.text,
                    effective_url=effective_url,
                    extraction=extraction,
                )
                if extraction.page is None or applied_configuration is None:
                    await _checkpoint_page(
                        dependencies=dependencies,
                        context=context,
                        extractor_component=route_definition_extractor_component,
                        search_run_identifier=search_run_identifier,
                        network_session_identifier=network_session_identifier,
                        page_ordinal=page_ordinal,
                        page_kind="route_definition",
                        acquisition_identifier=acquisition_identifier,
                        acquisition=acquisition,
                        decoded=decoded,
                        page=extraction.page,
                        extraction=extraction,
                        applied_configuration=applied_configuration,
                        session_evidence=material_extraction.evidence,
                        response_classification=response_classification.model_dump(mode="json"),
                        traversal_state=None,
                        requested_search=current_search_request,
                        price_partition=current_price_partition,
                        price_partition_planner_component=(
                            price_partition_planner_component
                            if current_price_partition is not None
                            else None
                        ),
                    )
                    return TerminalFailureWork(
                        error={
                            "kind": (
                                "search_route_definition_extraction_failure"
                                if response_classification.kind
                                is SearchBootstrapResponseKind.SEARCH_RESULTS
                                else "search_route_definition_response_failure"
                            ),
                            "response_kind": response_classification.kind.value,
                        },
                        result={
                            "state": "failed",
                            "search_run_identifier": search_run_identifier,
                            "response_classification": response_classification.model_dump(
                                mode="json"
                            ),
                            "protocol_issues": [
                                issue.model_dump(mode="json") for issue in extraction.issues
                            ],
                            "session_material_issues": [],
                        },
                    )
                page_completed_at = dependencies.monotonic_ns()
                page_facts = traversal_page_facts(
                    extraction.page,
                    interval_elapsed_duration_ns=page_completed_at - elapsed_accounted_at,
                    transferred_bytes=decoded.received_content_bytes,
                    decoded_body_bytes=len(decoded.content),
                )
                if isinstance(traversal_state, PricePartitionSearchTraversalState):
                    if current_price_partition is None:
                        raise AssertionError("Partition traversal has no current partition")
                    partition_decision = advance_price_partition_search_traversal(
                        state=traversal_state,
                        partition=current_price_partition,
                        page=page_facts,
                    )
                    traversal_state = partition_decision.state
                    decision = None
                else:
                    decision = advance_search_traversal(
                        state=traversal_state,
                        page=page_facts,
                    )
                    traversal_state = decision.state
                    partition_decision = None
                elapsed_accounted_at = page_completed_at
                occurrences = await _checkpoint_page(
                    dependencies=dependencies,
                    context=context,
                    extractor_component=route_definition_extractor_component,
                    search_run_identifier=search_run_identifier,
                    network_session_identifier=network_session_identifier,
                    page_ordinal=page_ordinal,
                    page_kind="route_definition",
                    acquisition_identifier=acquisition_identifier,
                    acquisition=acquisition,
                    decoded=decoded,
                    page=extraction.page,
                    extraction=extraction,
                    applied_configuration=applied_configuration,
                    session_evidence=material_extraction.evidence,
                    response_classification=response_classification.model_dump(mode="json"),
                    traversal_state=traversal_state,
                    requested_search=current_search_request,
                    price_partition=current_price_partition,
                    price_partition_planner_component=(
                        price_partition_planner_component
                        if current_price_partition is not None
                        else None
                    ),
                )
                page_references.append(
                    {
                        "page_ordinal": page_ordinal,
                        "listing_occurrence_records": list(occurrences),
                        "requested_search": current_search_request.model_dump(mode="json"),
                        "price_partition": (
                            None
                            if current_price_partition is None
                            else current_price_partition.model_dump(mode="json")
                        ),
                    }
                )
                if partition_decision is not None:
                    while partition_decision.action is SearchTraversalAction.CONTINUE:
                        now = dependencies.monotonic_ns()
                        traversal_state = account_search_elapsed_interval(
                            partition_decision.state,
                            interval_elapsed_duration_ns=now - elapsed_accounted_at,
                        )
                        elapsed_accounted_at = now
                        if traversal_state.stopping_reason is not None:
                            break
                        current_price_partition = partition_decision.next_partition
                        if current_price_partition is None:
                            raise AssertionError("Partition continuation has no next interval")
                        current_search_request = payload.request.model_copy(
                            update={
                                "price": SearchPriceRange(
                                    currency=current_price_partition.currency,
                                    minimum=current_price_partition.minimum,
                                    maximum=current_price_partition.maximum,
                                )
                            }
                        )
                        bootstrap_plan = search_bootstrap_plan(
                            current_search_request,
                            routing=payload.routing,
                            navigation_headers=dependencies.navigation_headers,
                        )
                        page_ordinal += 1
                        route_definition = search_route_definition_request(
                            bootstrap_plan=bootstrap_plan,
                            navigation_headers=dependencies.navigation_headers,
                            session_material=material_extraction.material,
                            request_number=page_ordinal,
                        )
                        route_definition_activity = facebook_search_network_activity(
                            identifier=dependencies.new_identifier(),
                            kind=SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
                            operation_identifier=context.operation_identifier,
                            network_session_identifier=network_session_identifier,
                            ordinal=page_ordinal + 1,
                            attempt=context.attempt,
                            routing=payload.routing,
                        )
                        async with dependencies.network_activity_scheduler.admit(
                            route_definition_activity
                        ) as permit:
                            admitted_at = dependencies.monotonic_ns()
                            traversal_state = account_search_elapsed_interval(
                                traversal_state,
                                interval_elapsed_duration_ns=(admitted_at - elapsed_accounted_at),
                            )
                            elapsed_accounted_at = admitted_at
                            if traversal_state.stopping_reason is not None:
                                break
                            permit.mark_dispatched()
                            acquisition = await _acquire_for_network_activity(
                                session.acquirer.acquire_form(
                                    route_definition.plan,
                                    route_definition.form_fields,
                                    dependencies.new_identifier,
                                ),
                                permit.admission,
                            )
                        decoded = await anyio.to_thread.run_sync(
                            _decode_acquisition,
                            acquisition,
                            abandon_on_cancel=True,
                        )
                        acquisition_identifier = dependencies.new_identifier()
                        extraction = await anyio.to_thread.run_sync(
                            partial(
                                extract_search_route_definition,
                                decoded.text,
                                acquisition_record_identifier=acquisition_identifier,
                            ),
                            abandon_on_cancel=True,
                        )
                        partition_applied_configuration = extraction.applied_configuration
                        effective_url = decoded.terminal_response.get("url")
                        if not isinstance(effective_url, str):
                            raise ValueError("Search acquisition effective URL is malformed")
                        response_classification = classify_search_route_definition_response(
                            decoded.text,
                            effective_url=effective_url,
                            extraction=extraction,
                        )
                        if extraction.page is None or partition_applied_configuration is None:
                            await _checkpoint_page(
                                dependencies=dependencies,
                                context=context,
                                extractor_component=route_definition_extractor_component,
                                search_run_identifier=search_run_identifier,
                                network_session_identifier=network_session_identifier,
                                page_ordinal=page_ordinal,
                                page_kind="route_definition",
                                acquisition_identifier=acquisition_identifier,
                                acquisition=acquisition,
                                decoded=decoded,
                                page=extraction.page,
                                extraction=extraction,
                                applied_configuration=partition_applied_configuration,
                                session_evidence=material_extraction.evidence,
                                response_classification=(
                                    response_classification.model_dump(mode="json")
                                ),
                                traversal_state=traversal_state,
                                requested_search=current_search_request,
                                price_partition=current_price_partition,
                                price_partition_planner_component=(
                                    price_partition_planner_component
                                ),
                            )
                            return TerminalFailureWork(
                                error={
                                    "kind": "search_price_partition_extraction_failure",
                                    "response_kind": response_classification.kind.value,
                                },
                                result={
                                    "state": "failed",
                                    "search_run_identifier": search_run_identifier,
                                    "page_ordinal": page_ordinal,
                                    "price_partition": current_price_partition.model_dump(
                                        mode="json"
                                    ),
                                    "protocol_issues": [
                                        issue.model_dump(mode="json") for issue in extraction.issues
                                    ],
                                },
                            )
                        page_completed_at = dependencies.monotonic_ns()
                        partition_decision = advance_price_partition_search_traversal(
                            state=traversal_state,
                            partition=current_price_partition,
                            page=traversal_page_facts(
                                extraction.page,
                                interval_elapsed_duration_ns=(
                                    page_completed_at - elapsed_accounted_at
                                ),
                                transferred_bytes=decoded.received_content_bytes,
                                decoded_body_bytes=len(decoded.content),
                            ),
                        )
                        traversal_state = partition_decision.state
                        elapsed_accounted_at = page_completed_at
                        occurrences = await _checkpoint_page(
                            dependencies=dependencies,
                            context=context,
                            extractor_component=route_definition_extractor_component,
                            search_run_identifier=search_run_identifier,
                            network_session_identifier=network_session_identifier,
                            page_ordinal=page_ordinal,
                            page_kind="route_definition",
                            acquisition_identifier=acquisition_identifier,
                            acquisition=acquisition,
                            decoded=decoded,
                            page=extraction.page,
                            extraction=extraction,
                            applied_configuration=partition_applied_configuration,
                            session_evidence=material_extraction.evidence,
                            response_classification=response_classification.model_dump(mode="json"),
                            traversal_state=traversal_state,
                            requested_search=current_search_request,
                            price_partition=current_price_partition,
                            price_partition_planner_component=(price_partition_planner_component),
                        )
                        page_references.append(
                            {
                                "page_ordinal": page_ordinal,
                                "listing_occurrence_records": list(occurrences),
                                "requested_search": current_search_request.model_dump(mode="json"),
                                "price_partition": current_price_partition.model_dump(mode="json"),
                            }
                        )
                else:
                    if decision is None:
                        raise AssertionError("Cursor traversal has no decision")
                    cursor_traversal_state = decision.state
                    while decision.action is SearchTraversalAction.CONTINUE:
                        now = dependencies.monotonic_ns()
                        cursor_traversal_state = account_search_elapsed_interval(
                            cursor_traversal_state,
                            interval_elapsed_duration_ns=now - elapsed_accounted_at,
                        )
                        elapsed_accounted_at = now
                        if cursor_traversal_state.stopping_reason is not None:
                            break
                        assert decision.next_cursor is not None
                        page_ordinal += 1
                        requested_page_size = payload.traversal.requested_page_size
                        pagination = search_pagination_request(
                            bootstrap_plan=bootstrap_plan,
                            navigation_headers=dependencies.navigation_headers,
                            applied_configuration=applied_configuration,
                            session_material=material_extraction.material,
                            cursor=decision.next_cursor,
                            request_number=page_ordinal,
                            requested_page_size=requested_page_size,
                        )
                        pagination_activity = facebook_search_network_activity(
                            identifier=dependencies.new_identifier(),
                            kind=SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND,
                            operation_identifier=context.operation_identifier,
                            network_session_identifier=network_session_identifier,
                            ordinal=page_ordinal + 1,
                            attempt=context.attempt,
                            routing=payload.routing,
                        )
                        async with dependencies.network_activity_scheduler.admit(
                            pagination_activity
                        ) as permit:
                            admitted_at = dependencies.monotonic_ns()
                            cursor_traversal_state = account_search_elapsed_interval(
                                cursor_traversal_state,
                                interval_elapsed_duration_ns=admitted_at - elapsed_accounted_at,
                            )
                            elapsed_accounted_at = admitted_at
                            if cursor_traversal_state.stopping_reason is not None:
                                break
                            permit.mark_dispatched()
                            acquisition = await _acquire_for_network_activity(
                                session.acquirer.acquire_form(
                                    pagination.plan,
                                    pagination.form_fields,
                                    dependencies.new_identifier,
                                ),
                                permit.admission,
                            )
                        decoded = await anyio.to_thread.run_sync(
                            _decode_acquisition,
                            acquisition,
                            abandon_on_cancel=True,
                        )
                        acquisition_identifier = dependencies.new_identifier()
                        page_extraction = await anyio.to_thread.run_sync(
                            partial(
                                extract_search_pagination,
                                decoded.text,
                                acquisition_record_identifier=acquisition_identifier,
                            ),
                            abandon_on_cancel=True,
                        )
                        if page_extraction.page is None:
                            rate_limited = any(
                                issue.kind is SearchProtocolIssueKind.GRAPHQL_RATE_LIMITED
                                for issue in page_extraction.issues
                            )
                            await _checkpoint_page(
                                dependencies=dependencies,
                                context=context,
                                extractor_component=pagination_extractor_component,
                                search_run_identifier=search_run_identifier,
                                network_session_identifier=network_session_identifier,
                                page_ordinal=page_ordinal,
                                page_kind="pagination",
                                acquisition_identifier=acquisition_identifier,
                                acquisition=acquisition,
                                decoded=decoded,
                                page=None,
                                extraction=page_extraction,
                                applied_configuration=applied_configuration,
                                session_evidence=None,
                                response_classification=None,
                                traversal_state=cursor_traversal_state,
                                requested_search=current_search_request,
                            )
                            return TerminalFailureWork(
                                error={
                                    "kind": (
                                        "search_pagination_rate_limited"
                                        if rate_limited
                                        else "search_pagination_extraction_failure"
                                    )
                                },
                                result={
                                    "state": "rate_limited" if rate_limited else "failed",
                                    "search_run_identifier": search_run_identifier,
                                    "page_ordinal": page_ordinal,
                                    "protocol_issues": [
                                        issue.model_dump(mode="json")
                                        for issue in page_extraction.issues
                                    ],
                                },
                            )
                        page_completed_at = dependencies.monotonic_ns()
                        decision = advance_search_traversal(
                            state=cursor_traversal_state,
                            page=traversal_page_facts(
                                page_extraction.page,
                                interval_elapsed_duration_ns=(
                                    page_completed_at - elapsed_accounted_at
                                ),
                                transferred_bytes=decoded.received_content_bytes,
                                decoded_body_bytes=len(decoded.content),
                            ),
                        )
                        cursor_traversal_state = decision.state
                        elapsed_accounted_at = page_completed_at
                        occurrences = await _checkpoint_page(
                            dependencies=dependencies,
                            context=context,
                            extractor_component=pagination_extractor_component,
                            search_run_identifier=search_run_identifier,
                            network_session_identifier=network_session_identifier,
                            page_ordinal=page_ordinal,
                            page_kind="pagination",
                            acquisition_identifier=acquisition_identifier,
                            acquisition=acquisition,
                            decoded=decoded,
                            page=page_extraction.page,
                            extraction=page_extraction,
                            applied_configuration=applied_configuration,
                            session_evidence=None,
                            response_classification=None,
                            traversal_state=cursor_traversal_state,
                            requested_search=current_search_request,
                        )
                        page_references.append(
                            {
                                "page_ordinal": page_ordinal,
                                "listing_occurrence_records": list(occurrences),
                                "requested_search": current_search_request.model_dump(mode="json"),
                                "price_partition": None,
                            }
                        )
                    traversal_state = cursor_traversal_state
            if session is None:
                raise AssertionError("Search session was not created")
            completed_session_observation = session.completed_observation()
        except AcquisitionFailure as error:
            if error.result.get("stopping_condition") == "transport_failure":
                delay_ns = _search_retry_delay_ns(policy_attempt)
                now_utc_ns = dependencies.utc_now_ns()
                await dependencies.database.defer_pending_collect_search_work_for_route(
                    routing=payload.routing,
                    eligible_at_utc_ns=now_utc_ns + delay_ns,
                    recorded_at_utc_ns=now_utc_ns,
                    event_batch_identifier=dependencies.new_identifier(),
                    reason={
                        "kind": "shared_search_route_transport_backoff",
                        "triggering_work_identifier": context.work_item_identifier,
                        "exception_type": error.result.get("exception_type"),
                    },
                )
            return search_acquisition_failure_work(
                error=error,
                context=context,
                search_run_identifier=search_run_identifier,
                policy_attempt=policy_attempt,
            )
        except FacebookSearchSessionFailure as error:
            if error.code in RETRYABLE_SEARCH_SESSION_FAILURE_CODES:
                delay_ns = _search_retry_delay_ns(policy_attempt)
                now_utc_ns = dependencies.utc_now_ns()
                await dependencies.database.defer_pending_collect_search_work_for_route(
                    routing=payload.routing,
                    eligible_at_utc_ns=now_utc_ns + delay_ns,
                    recorded_at_utc_ns=now_utc_ns,
                    event_batch_identifier=dependencies.new_identifier(),
                    reason={
                        "kind": "shared_search_route_session_backoff",
                        "triggering_work_identifier": context.work_item_identifier,
                        "code": error.code,
                    },
                )
            return search_session_failure_work(
                error=error,
                context=context,
                search_run_identifier=search_run_identifier,
                routing=payload.routing,
                policy_attempt=policy_attempt,
            )
        except ValueError as error:
            return TerminalFailureWork(
                error={
                    "kind": "search_configuration_or_response_failure",
                    "type": type(error).__name__,
                },
                result={
                    "state": "failed",
                    "search_run_identifier": search_run_identifier,
                },
            )

        session_record_identifier = dependencies.new_identifier()
        run_record_identifier = dependencies.new_identifier()
        if completed_session_observation is None:
            raise AssertionError("Completed search has no network session observation")
        return CompletedWork(
            records=(
                RecordDraft(
                    identifier=session_record_identifier,
                    kind=("carl", "network", "session_observation"),
                    schema_version=1,
                    value=completed_session_observation,
                ),
                RecordDraft(
                    identifier=run_record_identifier,
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value={
                        "search_run_identifier": search_run_identifier,
                        "work_item_identifier": context.work_item_identifier,
                        "attempt": context.attempt,
                        "request": payload.request.model_dump(mode="json"),
                        "traversal_strategy": payload.traversal_strategy.model_dump(mode="json"),
                        "traversal": traversal_state.model_dump(mode="json"),
                        "applied_configuration": (
                            None
                            if applied_configuration is None
                            or isinstance(
                                traversal_state,
                                PricePartitionSearchTraversalState,
                            )
                            else applied_configuration.model_dump(mode="json")
                        ),
                        "price_partition_plan": (
                            None
                            if not price_partitions
                            else {
                                "planner": {
                                    "component_parts": list(
                                        price_partition_planner_component.identifier.parts
                                    ),
                                    "output_schema_version": (
                                        price_partition_planner_component.output_schema_version
                                    ),
                                },
                                "partitions": [
                                    partition.model_dump(mode="json")
                                    for partition in price_partitions
                                ],
                            }
                        ),
                        "network_session_record_identifier": session_record_identifier,
                        "pages": page_references,
                    },
                ),
            ),
            outputs=(
                NamedOutput(
                    name=("search", "network_session"),
                    object_identifier=session_record_identifier,
                ),
                NamedOutput(
                    name=("search", "run"),
                    object_identifier=run_record_identifier,
                ),
            ),
            result={
                "state": "complete",
                "search_run_identifier": search_run_identifier,
                "search_run_record_identifier": run_record_identifier,
                "pages_processed": (
                    traversal_state.partitions_processed
                    if isinstance(
                        traversal_state,
                        PricePartitionSearchTraversalState,
                    )
                    else traversal_state.pages_processed
                ),
                "listing_observations": traversal_state.listing_observations,
                "unique_listings": len(traversal_state.unique_listing_identifiers),
                "stopping_reason": traversal_state.stopping_reason,
                "saturated_price_partitions": (
                    list(traversal_state.saturated_partition_ordinals)
                    if isinstance(
                        traversal_state,
                        PricePartitionSearchTraversalState,
                    )
                    else []
                ),
            },
        )

    return collect
