"""Bounded eBay search traversal and immutable evidence publication."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter_ns
from typing import cast
from uuid import uuid4

from carl.core.components import Component, ComponentId, Registry
from carl.core.content_encoding import decoded_stored_body
from carl.core.ebay import (
    EBAY_SEARCH_FAILURE_KINDS,
    EbaySearchRequest,
    ebay_search_plan,
    extract_ebay_search,
)
from carl.core.ebay_search_support import require_supported_ebay_search_acquisition
from carl.core.http import stored_content_encodings, stored_response_charset
from carl.core.models import (
    BytesDraft,
    CodeProvenance,
    JsonValue,
    NamedOutput,
    RecordDraft,
)
from carl.io.configuration import decodo_wreq_stack_settings, load_configuration
from carl.io.decodo import ManagedDecodoWreqAcquirer
from carl.io.httpx import Acquisition, AcquisitionFailure, HttpAcquirer
from carl.io.network_activity import (
    acquire_for_network_activity,
    network_activity_definition,
    network_activity_scheduler,
)
from carl.io.paths import CarlDirectories
from carl.io.provenance import collect_code_provenance_async, process_invocation
from carl.io.sqlite import Database

COLLECT_EBAY_SEARCH = ComponentId(("carl", "ebay", "collect", "search_page"))
EXTRACT_EBAY_SEARCH = ComponentId(("carl", "ebay", "extract", "search_page"))
FINALIZE_EBAY_SEARCH = ComponentId(("carl", "ebay", "finalize", "search"))
RUN_EBAY_SEARCH_WORK = ComponentId(("carl", "ebay", "run", "search_work"))


def _collect_component() -> None:
    """Identity anchor for one-page eBay collection."""


def _run_work_component() -> None:
    """Identity anchor for durable eBay search work."""


def _finalize_component() -> None:
    """Identity anchor for one bounded eBay search-run manifest."""


def build_ebay_component_registry() -> Registry:
    return Registry(
        (
            Component(COLLECT_EBAY_SEARCH, 4, _collect_component),
            Component(EXTRACT_EBAY_SEARCH, 3, extract_ebay_search),
            Component(FINALIZE_EBAY_SEARCH, 1, _finalize_component),
            Component(RUN_EBAY_SEARCH_WORK, 3, _run_work_component),
        )
    )


def _identifier() -> str:
    return str(uuid4())


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


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


def _terminal_response(acquisition: dict[str, JsonValue]) -> dict[str, JsonValue]:
    hops = acquisition.get("hops")
    if not isinstance(hops, list) or not hops or not isinstance(hops[-1], dict):
        raise ValueError("eBay acquisition has no terminal response")
    hop = cast(dict[str, JsonValue], hops[-1])
    response = hop.get("response")
    if not isinstance(response, dict):
        raise ValueError("eBay acquisition terminal response is malformed")
    return cast(dict[str, JsonValue], response)


async def collect_configured_ebay_search(
    database: Database,
    *,
    directories: CarlDirectories,
    request: EbaySearchRequest,
    new_identifier: Callable[[], str] = _identifier,
    search_run_identifier: str | None = None,
) -> dict[str, JsonValue]:
    require_supported_ebay_search_acquisition(request)
    loaded = load_configuration(directories.configuration_file)
    route, credential_source, transport = decodo_wreq_stack_settings(
        loaded,
        directories,
        request.stack_identifier,
    )
    acquirer = ManagedDecodoWreqAcquirer(
        settings=route,
        credential_source=credential_source,
        transport_settings=transport,
    )
    async with acquirer.session(new_identifier) as session:
        return await collect_ebay_search(
            database,
            request=request,
            network_path=route.route.network_path,
            acquirer=session,
            new_identifier=new_identifier,
            search_run_identifier=search_run_identifier,
        )


async def collect_ebay_search(
    database: Database,
    *,
    request: EbaySearchRequest,
    network_path: tuple[str, ...],
    acquirer: HttpAcquirer,
    new_identifier: Callable[[], str] = _identifier,
    provenance: CodeProvenance | None = None,
    search_run_identifier: str | None = None,
) -> dict[str, JsonValue]:
    resolved_provenance = provenance or await collect_code_provenance_async(_repository_root())
    search_run_identifier = search_run_identifier or new_identifier()
    pages: list[dict[str, JsonValue]] = []
    page_number = 1
    seen_page_numbers: set[int] = set()
    stopping_reason = "maximum_pages"
    while True:
        page_ordinal = len(pages) + 1
        seen_page_numbers.add(page_number)
        page = await _collect_ebay_search_page(
            database,
            request=request,
            network_path=network_path,
            page_number=page_number,
            page_ordinal=page_ordinal,
            search_run_identifier=search_run_identifier,
            acquirer=acquirer,
            new_identifier=new_identifier,
            provenance=resolved_provenance,
        )
        pages.append(page)
        classification = page.get("response_classification")
        classification_kind = (
            cast(dict[str, JsonValue], classification).get("kind")
            if isinstance(classification, dict)
            else None
        )
        next_page_number = page.get("next_page_number")
        if classification_kind == "empty_results":
            stopping_reason = "empty_results"
            break
        if classification_kind == "challenge":
            stopping_reason = "challenge"
            break
        if classification_kind in {"error_page", "http_error"}:
            stopping_reason = str(classification_kind)
            break
        if classification_kind != "usable_results":
            stopping_reason = "unrecognized_response"
            break
        if page_ordinal >= request.maximum_pages:
            stopping_reason = "maximum_pages"
            break
        if next_page_number is None:
            stopping_reason = "no_next_page"
            break
        if (
            not isinstance(next_page_number, int)
            or isinstance(next_page_number, bool)
            or next_page_number != page_number + 1
            or next_page_number in seen_page_numbers
        ):
            stopping_reason = "invalid_next_page"
            break
        page_number = next_page_number

    finalized = await _finalize_ebay_search(
        database,
        request=request,
        search_run_identifier=search_run_identifier,
        pages=pages,
        stopping_reason=stopping_reason,
        new_identifier=new_identifier,
        provenance=resolved_provenance,
    )
    first_page = pages[0]
    last_page = pages[-1]
    return {
        "state": (
            "response_failed"
            if classification_kind in EBAY_SEARCH_FAILURE_KINDS
            or stopping_reason == "invalid_next_page"
            else "completed"
        ),
        "acquisition_operation_identifier": first_page["acquisition_operation_identifier"],
        "acquisition_record_identifier": first_page["acquisition_record_identifier"],
        "acquisition_record_identifiers": [page["acquisition_record_identifier"] for page in pages],
        "extraction_record_identifier": first_page["extraction_record_identifier"],
        "extraction_record_identifiers": [page["extraction_record_identifier"] for page in pages],
        "response_classification": last_page["response_classification"],
        **finalized,
    }


async def _collect_ebay_search_page(
    database: Database,
    *,
    request: EbaySearchRequest,
    network_path: tuple[str, ...],
    page_number: int,
    page_ordinal: int,
    search_run_identifier: str,
    acquirer: HttpAcquirer,
    new_identifier: Callable[[], str],
    provenance: CodeProvenance,
) -> dict[str, JsonValue]:
    plan = ebay_search_plan(
        request,
        network_path=network_path,
        page_number=page_number,
    )
    registry = build_ebay_component_registry()
    component = registry.require(COLLECT_EBAY_SEARCH)
    operation_identifier = new_identifier()
    started_at_utc = _utc_now()
    started = perf_counter_ns()
    await database.begin_operation(
        operation_id=operation_identifier,
        component=component,
        provenance=provenance,
        invocation=process_invocation(),
        configuration={
            "request": request.model_dump(mode="json"),
            "page_number": page_number,
            "page_ordinal": page_ordinal,
            "search_run_record_identifier": search_run_identifier,
            "request_plan": plan.as_json(),
        },
        started_at_utc=started_at_utc,
    )
    try:
        activity = network_activity_definition(
            identifier=new_identifier(),
            kind=("carl", "ebay", "network_activity", "search_page"),
            operation_identifier=operation_identifier,
            network_session_identifier=search_run_identifier,
            network_path=network_path,
            ordinal=page_ordinal,
        )
        async with network_activity_scheduler(database, new_identifier).admit(activity) as permit:
            permit.mark_dispatched()
            acquisition = await acquire_for_network_activity(
                acquirer.acquire(plan, new_identifier), permit.admission
            )
    except AcquisitionFailure as error:
        artifacts = _body_artifacts(Acquisition(record=error.result, bodies=error.bodies))
        await database.fail_operation(
            operation_id=operation_identifier,
            error={"type": type(error).__name__, "message": str(error)},
            result={
                "state": "acquisition_failed",
                "acquisition": error.result,
            },
            ended_at_utc=_utc_now(),
            duration_ns=perf_counter_ns() - started,
            artifacts=artifacts,
            outputs=tuple(
                NamedOutput(name=("body", str(index)), object_identifier=artifact.identifier)
                for index, artifact in enumerate(artifacts)
            ),
        )
        raise

    acquisition_identifier = new_identifier()
    acquisition_value = dict(acquisition.record)
    acquisition_value.update(
        {
            "operation_identifier": operation_identifier,
            "purpose": "ebay_search_page",
            "ebay_search_request": request.model_dump(mode="json"),
            "acquisition_stack_identifier": request.stack_identifier,
            "page_number": page_number,
            "page_ordinal": page_ordinal,
            "search_run_record_identifier": search_run_identifier,
        }
    )
    records = (
        RecordDraft(
            identifier=acquisition_identifier,
            kind=("carl", "http", "acquisition"),
            schema_version=1,
            value=acquisition_value,
        ),
    )
    artifacts = _body_artifacts(acquisition)
    outputs = [NamedOutput(name=("acquisition",), object_identifier=acquisition_identifier)]
    outputs.extend(
        NamedOutput(name=("response", str(index), "body"), object_identifier=body.identifier)
        for index, body in enumerate(acquisition.bodies)
    )
    terminal_response = _terminal_response(acquisition_value)
    await database.complete_operation(
        operation_id=operation_identifier,
        records=records,
        artifacts=artifacts,
        outputs=outputs,
        result={
            "acquisition_record_identifier": acquisition_identifier,
            "http_status": terminal_response.get("status_code"),
        },
        ended_at_utc=_utc_now(),
        duration_ns=perf_counter_ns() - started,
    )
    extraction = await extract_ebay_search_acquisition(
        database,
        acquisition_identifier=acquisition_identifier,
        search_run_identifier=search_run_identifier,
        page_number=page_number,
        page_ordinal=page_ordinal,
        new_identifier=new_identifier,
        provenance=provenance,
    )
    return {
        "acquisition_operation_identifier": operation_identifier,
        "acquisition_record_identifier": acquisition_identifier,
        **extraction,
    }


async def extract_ebay_search_acquisition(
    database: Database,
    *,
    acquisition_identifier: str,
    search_run_identifier: str,
    page_number: int,
    page_ordinal: int,
    new_identifier: Callable[[], str] = _identifier,
    provenance: CodeProvenance | None = None,
) -> dict[str, JsonValue]:
    kind, _, stored_acquisition = await database.get_record(acquisition_identifier)
    if kind != ("carl", "http", "acquisition") or not isinstance(stored_acquisition, dict):
        raise ValueError("Input is not an HTTP acquisition record")
    acquisition_value = cast(dict[str, JsonValue], stored_acquisition)
    request = EbaySearchRequest.model_validate(acquisition_value.get("ebay_search_request"))
    terminal_response = _terminal_response(acquisition_value)
    body = terminal_response.get("body")
    if not isinstance(body, dict):
        raise ValueError("eBay acquisition has no complete terminal response body")
    body_value = cast(dict[str, JsonValue], body)
    if body_value.get("state") != "available":
        raise ValueError("eBay acquisition has no complete terminal response body")
    body_identifier = body_value.get("artifact_id")
    if not isinstance(body_identifier, str):
        raise ValueError("eBay acquisition body reference is malformed")
    body_metadata, stored_body = await database.get_artifact(body_identifier)

    registry = build_ebay_component_registry()
    component = registry.require(EXTRACT_EBAY_SEARCH)
    resolved_provenance = provenance or await collect_code_provenance_async(_repository_root())
    operation_identifier = new_identifier()
    started_at_utc = _utc_now()
    started = perf_counter_ns()
    await database.begin_operation(
        operation_id=operation_identifier,
        component=component,
        provenance=resolved_provenance,
        invocation=process_invocation(),
        configuration={
            "acquisition_record_identifier": acquisition_identifier,
            "request": request.model_dump(mode="json"),
            "search_run_record_identifier": search_run_identifier,
            "page_number": page_number,
            "page_ordinal": page_ordinal,
        },
        started_at_utc=started_at_utc,
        inputs=(
            (("acquisition",), acquisition_identifier),
            (("terminal_response", "body"), body_identifier),
        ),
    )
    try:
        headers = terminal_response.get("headers")
        decoded = decoded_stored_body(
            stored_body,
            body_metadata.get("representation"),
            stored_content_encodings(headers),
        )
        charset, charset_source = stored_response_charset(headers)
        try:
            html = decoded.decode(charset)
            decoding_issues: tuple[JsonValue, ...] = ()
        except (LookupError, UnicodeError):
            html = decoded.decode("utf-8", errors="replace")
            charset = "utf-8"
            charset_source = "fallback_after_decode_failure"
            decoding_issues = ({"kind": "text_decoding_failure", "fallback": "utf-8-replace"},)
        status_code = terminal_response.get("status_code")
        if not isinstance(status_code, int) or isinstance(status_code, bool):
            raise ValueError("eBay acquisition terminal response has no HTTP status")
        extraction = extract_ebay_search(
            html, status_code=status_code, listing_state=request.listing_state
        )
    except Exception as error:
        await database.fail_operation(
            operation_id=operation_identifier,
            error={"type": type(error).__name__, "message": str(error)},
            result={"acquisition_record_identifier": acquisition_identifier},
            ended_at_utc=_utc_now(),
            duration_ns=perf_counter_ns() - started,
        )
        raise

    decoded_identifier = new_identifier()
    extraction_identifier = new_identifier()
    occurrence_identifiers = tuple(new_identifier() for _ in extraction.listing_occurrences)
    extractor = {
        "component_parts": list(component.identifier.parts),
        "output_schema_version": component.output_schema_version,
    }
    classification = extraction.response_classification.model_dump(mode="json")
    occurrence_records = tuple(
        RecordDraft(
            identifier=record_identifier,
            kind=("carl", "ebay", "search_listing_occurrence"),
            schema_version=1,
            value={
                **occurrence.model_dump(mode="json"),
                "search_run_record_identifier": search_run_identifier,
                "acquisition_record_identifier": acquisition_identifier,
                "page_number": page_number,
                "page_ordinal": page_ordinal,
                "extractor": extractor,
            },
        )
        for record_identifier, occurrence in zip(
            occurrence_identifiers,
            extraction.listing_occurrences,
            strict=True,
        )
    )
    issues = (*extraction.issues, *decoding_issues)
    records = (
        RecordDraft(
            identifier=extraction_identifier,
            kind=("carl", "ebay", "search_extraction"),
            schema_version=1,
            value={
                "request": request.model_dump(mode="json"),
                "search_run_record_identifier": search_run_identifier,
                "page_number": page_number,
                "page_ordinal": page_ordinal,
                "acquisition_record_identifier": acquisition_identifier,
                "decoded_body_artifact_identifier": decoded_identifier,
                "extractor": extractor,
                "response_classification": classification,
                "next_page_number": extraction.next_page_number,
                "listing_occurrence_record_identifiers": list(occurrence_identifiers),
                "issues": list(issues),
            },
        ),
        *occurrence_records,
    )
    decoded_artifact = BytesDraft(
        identifier=decoded_identifier,
        kind=("carl", "http", "decoded_text"),
        media_type="text/html; charset=utf-8",
        representation={
            "kind": "content_decoded_utf8_text",
            "derived_from_artifact_identifier": body_identifier,
            "source_charset": charset,
            "charset_source": charset_source,
            "exact_wire_bytes": False,
        },
        content=html.encode("utf-8"),
    )
    outputs = [
        NamedOutput(name=("decoded_body",), object_identifier=decoded_identifier),
        NamedOutput(name=("extraction",), object_identifier=extraction_identifier),
    ]
    outputs.extend(
        NamedOutput(
            name=("listing_occurrence", str(index)),
            object_identifier=identifier,
        )
        for index, identifier in enumerate(occurrence_identifiers)
    )
    await database.complete_operation(
        operation_id=operation_identifier,
        records=records,
        artifacts=(decoded_artifact,),
        outputs=outputs,
        result={
            "extraction_record_identifier": extraction_identifier,
            "response_classification": classification,
            "next_page_number": extraction.next_page_number,
            "listing_count": len(occurrence_identifiers),
        },
        ended_at_utc=_utc_now(),
        duration_ns=perf_counter_ns() - started,
    )
    return {
        "extraction_operation_identifier": operation_identifier,
        "extraction_record_identifier": extraction_identifier,
        "page_number": page_number,
        "page_ordinal": page_ordinal,
        "response_classification": classification,
        "next_page_number": extraction.next_page_number,
        "listing_count": len(occurrence_identifiers),
        "listing_identifiers": [
            occurrence.item_identifier for occurrence in extraction.listing_occurrences
        ],
        "listing_occurrence_record_identifiers": list(occurrence_identifiers),
    }


async def _finalize_ebay_search(
    database: Database,
    *,
    request: EbaySearchRequest,
    search_run_identifier: str,
    pages: Sequence[dict[str, JsonValue]],
    stopping_reason: str,
    new_identifier: Callable[[], str],
    provenance: CodeProvenance,
) -> dict[str, JsonValue]:
    if not pages:
        raise ValueError("An eBay search run requires at least one retained page")
    registry = build_ebay_component_registry()
    component = registry.require(FINALIZE_EBAY_SEARCH)
    operation_identifier = new_identifier()
    started_at_utc = _utc_now()
    started = perf_counter_ns()
    inputs: list[tuple[tuple[str, ...], str]] = []
    occurrence_identifiers: list[str] = []
    unique_listing_identifiers: list[str] = []
    for index, page in enumerate(pages, start=1):
        acquisition_identifier = page.get("acquisition_record_identifier")
        extraction_identifier = page.get("extraction_record_identifier")
        if not isinstance(acquisition_identifier, str) or not isinstance(
            extraction_identifier, str
        ):
            raise ValueError("An eBay search page has malformed evidence references")
        inputs.extend(
            (
                (("page", str(index), "acquisition"), acquisition_identifier),
                (("page", str(index), "extraction"), extraction_identifier),
            )
        )
        page_occurrences = page.get("listing_occurrence_record_identifiers")
        if not isinstance(page_occurrences, list):
            raise ValueError("An eBay search page has malformed occurrence references")
        page_occurrence_values = cast(list[JsonValue], page_occurrences)
        typed_page_occurrences = tuple(
            identifier for identifier in page_occurrence_values if isinstance(identifier, str)
        )
        if len(typed_page_occurrences) != len(page_occurrence_values):
            raise ValueError("An eBay search page has malformed occurrence references")
        occurrence_identifiers.extend(typed_page_occurrences)
        page_listings = page.get("listing_identifiers")
        if not isinstance(page_listings, list):
            raise ValueError("An eBay search page has malformed listing identifiers")
        page_listing_values = cast(list[JsonValue], page_listings)
        typed_page_listings = tuple(
            identifier for identifier in page_listing_values if isinstance(identifier, str)
        )
        if len(typed_page_listings) != len(page_listing_values):
            raise ValueError("An eBay search page has malformed listing identifiers")
        for identifier in typed_page_listings:
            if identifier not in unique_listing_identifiers:
                unique_listing_identifiers.append(identifier)

    retained_pages: list[JsonValue] = []
    for page in pages:
        retained_pages.append(
            {
                name: page[name]
                for name in (
                    "page_ordinal",
                    "page_number",
                    "acquisition_record_identifier",
                    "extraction_record_identifier",
                    "response_classification",
                    "next_page_number",
                    "listing_count",
                    "listing_occurrence_record_identifiers",
                )
            }
        )
    record = {
        "request": request.model_dump(mode="json"),
        "acquisition_stack_identifier": request.stack_identifier,
        "pages": retained_pages,
        "listing_occurrence_record_identifiers": occurrence_identifiers,
        "listing_count": len(unique_listing_identifiers),
        "listing_occurrence_count": len(occurrence_identifiers),
        "page_count": len(pages),
        "stopping_reason": stopping_reason,
    }
    await database.begin_operation(
        operation_id=operation_identifier,
        component=component,
        provenance=provenance,
        invocation=process_invocation(),
        configuration={
            "request": request.model_dump(mode="json"),
            "search_run_record_identifier": search_run_identifier,
            "page_count": len(pages),
            "stopping_reason": stopping_reason,
        },
        started_at_utc=started_at_utc,
        inputs=tuple(inputs),
    )
    await database.complete_operation(
        operation_id=operation_identifier,
        records=(
            RecordDraft(
                identifier=search_run_identifier,
                kind=("carl", "ebay", "search_run"),
                schema_version=1,
                value=record,
            ),
        ),
        artifacts=(),
        outputs=(NamedOutput(name=("search_run",), object_identifier=search_run_identifier),),
        result={
            "search_run_record_identifier": search_run_identifier,
            "page_count": len(pages),
            "listing_count": len(unique_listing_identifiers),
            "listing_occurrence_count": len(occurrence_identifiers),
            "stopping_reason": stopping_reason,
        },
        ended_at_utc=_utc_now(),
        duration_ns=perf_counter_ns() - started,
    )
    return {
        "finalization_operation_identifier": operation_identifier,
        "search_run_record_identifier": search_run_identifier,
        "page_count": len(pages),
        "listing_count": len(unique_listing_identifiers),
        "listing_occurrence_count": len(occurrence_identifiers),
        "stopping_reason": stopping_reason,
        "listing_occurrence_record_identifiers": occurrence_identifiers,
    }
