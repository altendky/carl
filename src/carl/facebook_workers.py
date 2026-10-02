"""Application wiring for durable Facebook Marketplace work."""

from dataclasses import dataclass

import anyio

from carl.core.components import Component, ComponentId, Registry
from carl.core.content_encoding import decoded_stored_body
from carl.core.facebook import (
    Extraction,
    FacebookItemResponseClassification,
    FacebookItemResponseKind,
    classify_item_response,
    extract_listing,
    parse_json_blocks,
)
from carl.core.facebook_search import plan_overlapping_price_partitions
from carl.core.facebook_search_protocol import (
    extract_search_bootstrap,
    extract_search_pagination,
    extract_search_route_definition,
)
from carl.core.facebook_work import (
    COLLECT_ITEM_PAYLOAD_SCHEMA_VERSION,
    COLLECT_ITEM_WORK_KIND,
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_SEARCH_WORK_KIND,
    EXTRACT_ITEM_PAYLOAD_SCHEMA_VERSION,
    EXTRACT_ITEM_WORK_KIND,
    CollectItemPayload,
    CollectSearchPayload,
    ExtractItemPayload,
    extract_item_work,
    facebook_item_network_activity,
    plan_item_page_followups,
)
from carl.core.http import stored_content_encodings, stored_response_charset
from carl.core.models import BytesDraft, JsonValue, NamedInput, NamedOutput, RecordDraft
from carl.core.work import WorkCapability, WorkRequester
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    FollowOnWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.facebook_search_workers import (
    FacebookSearchWorkerDependencies,
    build_search_handler,
)
from carl.io.httpx import (
    AcquiredBody,
    Acquisition,
    AcquisitionFailure,
    HttpAcquirer,
    IdentifierFactory,
    RouteConfigurationFailure,
)
from carl.io.network_activity import NetworkActivityScheduler
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandler, WorkHandlerRegistry

ACQUIRE_HTTP = ComponentId(("carl", "http", "acquire", "httpx"))
COLLECT_FACEBOOK_ITEM = ComponentId(("carl", "facebook", "collect", "item_page"))
COLLECT_FACEBOOK_SEARCH = ComponentId(("carl", "facebook", "collect", "search"))
EXTRACT_FACEBOOK = ComponentId(("carl", "facebook", "extract", "embedded_json"))
EXTRACT_FACEBOOK_JSON_BLOCKS = ComponentId(("carl", "facebook", "extract", "embedded_json_blocks"))
EXTRACT_FACEBOOK_SEARCH_BOOTSTRAP = ComponentId(("carl", "facebook", "extract", "search_bootstrap"))
EXTRACT_FACEBOOK_SEARCH_ROUTE_DEFINITION = ComponentId(
    ("carl", "facebook", "extract", "search_route_definition")
)
EXTRACT_FACEBOOK_SEARCH_PAGINATION = ComponentId(
    ("carl", "facebook", "extract", "search_pagination")
)
PLAN_FACEBOOK_OVERLAPPING_PRICE_PARTITIONS = ComponentId(
    ("carl", "facebook", "plan", "overlapping_price_partitions")
)
PLAN_FACEBOOK_ITEM_PAGE_FOLLOWUPS = ComponentId(("carl", "facebook", "plan", "item_page_followups"))
MAX_ACQUISITION_ATTEMPTS = 3
ACQUISITION_RETRY_DELAY_NS = 1_000_000_000


def build_component_registry() -> Registry:
    return Registry(
        (
            Component(ACQUIRE_HTTP, 1, _acquire_component),
            Component(COLLECT_FACEBOOK_ITEM, 1, _collect_item_component),
            Component(COLLECT_FACEBOOK_SEARCH, 2, _collect_search_component),
            Component(EXTRACT_FACEBOOK, 1, extract_listing),
            Component(EXTRACT_FACEBOOK_JSON_BLOCKS, 1, parse_json_blocks),
            Component(EXTRACT_FACEBOOK_SEARCH_BOOTSTRAP, 1, extract_search_bootstrap),
            Component(
                EXTRACT_FACEBOOK_SEARCH_ROUTE_DEFINITION,
                1,
                extract_search_route_definition,
            ),
            Component(EXTRACT_FACEBOOK_SEARCH_PAGINATION, 1, extract_search_pagination),
            Component(
                PLAN_FACEBOOK_OVERLAPPING_PRICE_PARTITIONS,
                1,
                plan_overlapping_price_partitions,
            ),
            Component(
                PLAN_FACEBOOK_ITEM_PAGE_FOLLOWUPS,
                1,
                plan_item_page_followups,
            ),
        )
    )


def _acquire_component() -> None:
    """Identity anchor for the application-owned acquisition component."""


def _collect_item_component() -> None:
    """Identity anchor for the Facebook item collection workflow."""


def _collect_search_component() -> None:
    """Identity anchor for the Facebook search collection workflow."""


@dataclass(frozen=True, slots=True)
class FacebookWorkerDependencies:
    database: Database
    acquirer: HttpAcquirer
    new_identifier: IdentifierFactory
    network_activity_scheduler: NetworkActivityScheduler | None = None
    network_session_identifier: str | None = None


@dataclass(frozen=True, slots=True)
class _DecodedListing:
    html: str
    charset: str
    charset_source: str
    warnings: tuple[JsonValue, ...]
    classification: FacebookItemResponseClassification
    extraction: Extraction


def _decode_and_extract(
    stored_body: bytes,
    representation: JsonValue,
    headers: JsonValue,
    listing_id: str,
    acquisition_record_identifier: str,
    effective_url: str,
) -> _DecodedListing:
    decoded = decoded_stored_body(stored_body, representation, stored_content_encodings(headers))
    charset, charset_source = stored_response_charset(headers)
    try:
        html = decoded.decode(charset)
        warnings: tuple[JsonValue, ...] = ()
    except (LookupError, UnicodeError):
        html = decoded.decode("utf-8", errors="replace")
        charset = "utf-8"
        charset_source = "fallback_after_decode_failure"
        warnings = ({"kind": "text_decoding_failure", "fallback": "utf-8-replace"},)
    return _DecodedListing(
        html=html,
        charset=charset,
        charset_source=charset_source,
        warnings=warnings,
        classification=classify_item_response(
            html,
            requested_listing_id=listing_id,
            effective_url=effective_url,
        ),
        extraction=extract_listing(
            html,
            listing_id=listing_id,
            acquisition_record_id=acquisition_record_identifier,
        ),
    )


def _terminal_response(acquisition: Acquisition) -> dict[str, JsonValue]:
    hops = acquisition.record.get("hops")
    if not isinstance(hops, list) or not hops or not isinstance(hops[-1], dict):
        raise ValueError("Acquisition has no terminal response metadata")
    response = hops[-1].get("response")
    if not isinstance(response, dict):
        raise ValueError("Acquisition terminal response metadata is malformed")
    return response


def _body_artifacts(bodies: tuple[AcquiredBody, ...]) -> tuple[BytesDraft, ...]:
    return tuple(
        BytesDraft(
            identifier=body.identifier,
            kind=("carl", "http", "response_body"),
            media_type=body.media_type,
            representation=body.representation,
            content=body.content,
        )
        for body in bodies
    )


def _acquisition_outcome(
    *,
    acquisition: Acquisition,
    payload: CollectItemPayload,
    context: AttemptContext,
    component: Component,
    new_identifier: IdentifierFactory,
) -> CompletedWork:
    acquisition_record_identifier = new_identifier()
    record_value = dict(acquisition.record)
    record_value.update(
        {
            "operation_id": context.operation_identifier,
            "requested_listing_id": payload.listing_id,
        }
    )
    records = (
        RecordDraft(
            identifier=acquisition_record_identifier,
            kind=("carl", "http", "acquisition"),
            schema_version=1,
            value=record_value,
        ),
    )
    artifacts = _body_artifacts(acquisition.bodies)
    outputs = [NamedOutput(name=("acquisition",), object_identifier=acquisition_record_identifier)]
    outputs.extend(
        NamedOutput(name=("response", str(index), "body"), object_identifier=body.identifier)
        for index, body in enumerate(acquisition.bodies)
    )

    extraction_payload = ExtractItemPayload(
        acquisition_record_identifier=acquisition_record_identifier,
    )
    extraction_work = extract_item_work(
        identifier=new_identifier(),
        payload=extraction_payload,
        extractor_identifier=EXTRACT_FACEBOOK.parts,
        extractor_schema_version=component.output_schema_version,
        not_before_utc_ns=0,
    )
    extraction_requester = WorkRequester(
        request_identifier=new_identifier(),
        kind=("carl", "http", "acquisition"),
        identifier=acquisition_record_identifier,
        context={"operation_identifier": context.operation_identifier},
    )
    response = _terminal_response(acquisition)
    return CompletedWork(
        records=records,
        artifacts=artifacts,
        outputs=tuple(outputs),
        follow_on_work=(
            FollowOnWork(
                definition=extraction_work,
                requester=extraction_requester,
                event_identifier=new_identifier(),
            ),
        ),
        result={
            "acquisition_record_identifier": acquisition_record_identifier,
            "http_status": response.get("status_code"),
            "extraction_work_identifier": extraction_work.identifier,
        },
    )


def _acquisition_failure_outcome(
    *, error: AcquisitionFailure, context: AttemptContext
) -> RetryWork | TerminalFailureWork:
    stopping_condition = error.result.get("stopping_condition")
    result: dict[str, JsonValue] = {
        "state": "acquisition_failed",
        "attempt": context.attempt,
        "acquisition": error.result,
    }
    failure = {
        "kind": "acquisition_failure",
        "type": type(error).__name__,
        "stopping_condition": stopping_condition,
    }
    artifacts = _body_artifacts(error.bodies)
    outputs = tuple(
        NamedOutput(name=("response", str(index), "body"), object_identifier=body.identifier)
        for index, body in enumerate(error.bodies)
    )
    if (
        stopping_condition == "transport_failure"
        and context.retry_attempt() < MAX_ACQUISITION_ATTEMPTS
    ):
        return RetryWork(
            artifacts=artifacts,
            outputs=outputs,
            delay_ns=ACQUISITION_RETRY_DELAY_NS,
            reason={**failure, "decision": "retry"},
            result=result,
        )
    return TerminalFailureWork(
        artifacts=artifacts,
        outputs=outputs,
        error={**failure, "decision": "terminal"},
        result=result,
    )


def _extraction_failure_outcome(
    *,
    payload: ExtractItemPayload,
    inputs: list[NamedInput],
    error: Exception,
) -> TerminalFailureWork:
    return TerminalFailureWork(
        inputs=tuple(inputs),
        error={
            "kind": "extraction_failure",
            "type": type(error).__name__,
        },
        result={
            "state": "extraction_failed",
            "acquisition_record_identifier": payload.acquisition_record_identifier,
        },
    )


async def _extract_outcome(
    *,
    database: Database,
    payload: ExtractItemPayload,
    context: AttemptContext,
    component: Component,
    new_identifier: IdentifierFactory,
) -> WorkOutcome:
    inputs: list[NamedInput] = []
    try:
        kind, _, acquisition = await database.get_record(payload.acquisition_record_identifier)
    except KeyError as error:
        return _extraction_failure_outcome(payload=payload, inputs=inputs, error=error)
    inputs.append(
        NamedInput(
            name=("acquisition",),
            object_identifier=payload.acquisition_record_identifier,
        )
    )
    try:
        if kind != ("carl", "http", "acquisition") or not isinstance(acquisition, dict):
            raise ValueError("Input is not an HTTP acquisition record")
        requested_listing_id = acquisition.get("requested_listing_id")
        if not isinstance(requested_listing_id, str) or not requested_listing_id.isdecimal():
            raise ValueError("Acquisition has no requested listing identifier")
        hops = acquisition.get("hops")
        if not isinstance(hops, list) or not hops or not isinstance(hops[-1], dict):
            raise ValueError("Acquisition has no terminal response")
        response = hops[-1].get("response")
        if not isinstance(response, dict):
            raise ValueError("Acquisition terminal response is malformed")
        body = response.get("body")
        if not isinstance(body, dict) or body.get("state") != "available":
            raise ValueError("Acquisition has no complete terminal response body")
        body_identifier = body.get("artifact_id")
        if not isinstance(body_identifier, str):
            raise ValueError("Acquisition body reference is malformed")
    except ValueError as error:
        return _extraction_failure_outcome(payload=payload, inputs=inputs, error=error)
    try:
        body_metadata, stored_body = await database.get_artifact(body_identifier)
    except KeyError as error:
        return _extraction_failure_outcome(payload=payload, inputs=inputs, error=error)
    except ValueError as error:
        inputs.append(
            NamedInput(name=("terminal_response", "body"), object_identifier=body_identifier)
        )
        return _extraction_failure_outcome(payload=payload, inputs=inputs, error=error)
    inputs.append(NamedInput(name=("terminal_response", "body"), object_identifier=body_identifier))
    try:
        headers = response.get("headers")
        effective_url = acquisition.get("effective_url")
        if not isinstance(effective_url, str):
            raise ValueError("Acquisition has no effective URL")
        decoded_listing = await anyio.to_thread.run_sync(
            _decode_and_extract,
            stored_body,
            body_metadata.get("representation"),
            headers,
            requested_listing_id,
            payload.acquisition_record_identifier,
            effective_url,
            abandon_on_cancel=True,
        )
    except ValueError as error:
        return _extraction_failure_outcome(
            payload=payload,
            inputs=inputs,
            error=error,
        )

    decoded_identifier = new_identifier()
    block_identifiers = tuple(new_identifier() for _ in decoded_listing.extraction.blocks)
    observation_identifier = new_identifier()
    observation = dict(decoded_listing.extraction.observation)
    warnings = observation["warnings"]
    if not isinstance(warnings, list):
        raise AssertionError("Extraction warnings must be a list")
    warnings.extend(decoded_listing.warnings)
    observation.update(
        {
            "operation_id": context.operation_identifier,
            "extractor": {
                "component_parts": list(component.identifier.parts),
                "output_schema_version": component.output_schema_version,
            },
            "json_blocks": [
                {"block_index": index, "record_id": identifier}
                for index, identifier in enumerate(block_identifiers)
            ],
            "decoded_html_artifact_id": decoded_identifier,
            "response_classification": decoded_listing.classification.as_json(),
        }
    )
    records = (
        *(
            RecordDraft(
                identifier=identifier,
                kind=("carl", "html", "embedded_json_block"),
                schema_version=1,
                value={
                    **block,
                    "acquisition_record_id": payload.acquisition_record_identifier,
                    "decoded_html_artifact_id": decoded_identifier,
                },
            )
            for identifier, block in zip(
                block_identifiers, decoded_listing.extraction.blocks, strict=True
            )
        ),
        RecordDraft(
            identifier=observation_identifier,
            kind=("carl", "facebook", "listing_observation"),
            schema_version=1,
            value=observation,
        ),
    )
    decoded_artifact = BytesDraft(
        identifier=decoded_identifier,
        kind=("carl", "http", "decoded_text"),
        media_type="text/html; charset=utf-8",
        representation={
            "kind": "content_decoded_utf8_text",
            "derived_from_artifact_id": body_identifier,
            "content_encodings_removed": list(stored_content_encodings(headers)),
            "source_charset": decoded_listing.charset,
            "charset_source": decoded_listing.charset_source,
            "exact_wire_bytes": False,
        },
        content=decoded_listing.html.encode("utf-8"),
    )
    outputs = [NamedOutput(name=("decoded_html",), object_identifier=decoded_identifier)]
    outputs.extend(
        NamedOutput(name=("json_block", str(index)), object_identifier=identifier)
        for index, identifier in enumerate(block_identifiers)
    )
    outputs.append(
        NamedOutput(name=("listing_observation",), object_identifier=observation_identifier)
    )
    result: dict[str, JsonValue] = {
        "observation_record_identifier": observation_identifier,
        "state": observation["state"],
        "response_classification": decoded_listing.classification.kind.value,
        "warning_count": len(warnings),
        "json_block_count": len(block_identifiers),
    }
    if decoded_listing.classification.kind in {
        FacebookItemResponseKind.FULL_LISTING,
        FacebookItemResponseKind.LISTING_UNAVAILABLE,
    }:
        return CompletedWork(
            inputs=tuple(inputs),
            records=records,
            artifacts=(decoded_artifact,),
            outputs=tuple(outputs),
            result=result,
        )
    return TerminalFailureWork(
        inputs=tuple(inputs),
        records=records,
        artifacts=(decoded_artifact,),
        outputs=tuple(outputs),
        error={
            "kind": "unusable_item_response",
            "response_classification": decoded_listing.classification.kind.value,
        },
        result=result,
    )


def build_facebook_worker_registry(
    dependencies: FacebookWorkerDependencies,
    search_dependencies: FacebookSearchWorkerDependencies | None = None,
) -> WorkHandlerRegistry:
    components = build_component_registry()
    acquire_component = components.require(COLLECT_FACEBOOK_ITEM)
    extract_component = components.require(EXTRACT_FACEBOOK)

    async def acquire(payload: CollectItemPayload, context: AttemptContext) -> WorkOutcome:
        try:
            if dependencies.network_activity_scheduler is None:
                acquisition = await dependencies.acquirer.acquire(
                    payload.request_plan, dependencies.new_identifier
                )
            else:
                activity = facebook_item_network_activity(
                    identifier=dependencies.new_identifier(),
                    operation_identifier=context.operation_identifier,
                    network_session_identifier=(
                        dependencies.network_session_identifier or dependencies.new_identifier()
                    ),
                    attempt=context.attempt,
                    routing=payload.request_plan.routing,
                )
                async with dependencies.network_activity_scheduler.admit(activity) as permit:
                    permit.mark_dispatched()
                    acquisition = await dependencies.acquirer.acquire(
                        payload.request_plan, dependencies.new_identifier
                    )
        except RouteConfigurationFailure as error:
            return TerminalFailureWork(
                error={
                    "kind": "route_configuration_failure",
                    "code": error.code,
                    "decision": "terminal",
                },
                result={"state": "route_configuration_failed"},
            )
        except AcquisitionFailure as error:
            return _acquisition_failure_outcome(error=error, context=context)
        return _acquisition_outcome(
            acquisition=acquisition,
            payload=payload,
            context=context,
            component=extract_component,
            new_identifier=dependencies.new_identifier,
        )

    async def extract(payload: ExtractItemPayload, context: AttemptContext) -> WorkOutcome:
        return await _extract_outcome(
            database=dependencies.database,
            payload=payload,
            context=context,
            component=extract_component,
            new_identifier=dependencies.new_identifier,
        )

    handlers: list[WorkHandler] = [
        TypedWorkHandler(
            capability=WorkCapability(
                kind=COLLECT_ITEM_WORK_KIND,
                payload_schema_version=COLLECT_ITEM_PAYLOAD_SCHEMA_VERSION,
            ),
            component=acquire_component,
            payload_type=CollectItemPayload,
            handler=acquire,
        ),
        TypedWorkHandler(
            capability=WorkCapability(
                kind=EXTRACT_ITEM_WORK_KIND,
                payload_schema_version=EXTRACT_ITEM_PAYLOAD_SCHEMA_VERSION,
            ),
            component=extract_component,
            payload_type=ExtractItemPayload,
            handler=extract,
        ),
    ]
    if search_dependencies is not None:
        handlers.append(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=COLLECT_SEARCH_WORK_KIND,
                    payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
                ),
                component=components.require(COLLECT_FACEBOOK_SEARCH),
                payload_type=CollectSearchPayload,
                handler=build_search_handler(
                    search_dependencies,
                    session_bootstrap_extractor_component=components.require(
                        EXTRACT_FACEBOOK_JSON_BLOCKS
                    ),
                    route_definition_extractor_component=components.require(
                        EXTRACT_FACEBOOK_SEARCH_ROUTE_DEFINITION
                    ),
                    pagination_extractor_component=components.require(
                        EXTRACT_FACEBOOK_SEARCH_PAGINATION
                    ),
                    price_partition_planner_component=components.require(
                        PLAN_FACEBOOK_OVERLAPPING_PRICE_PARTITIONS
                    ),
                ),
            )
        )
    return WorkHandlerRegistry(handlers=tuple(handlers))
