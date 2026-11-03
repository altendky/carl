"""Bounded eBay search traversal and immutable evidence publication."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from time import perf_counter_ns
from typing import cast
from uuid import uuid4

from carl.core.components import Component, ComponentId, Registry
from carl.core.configuration import EbaySearchConfiguration
from carl.core.content_encoding import decoded_stored_body
from carl.core.ebay import (
    EBAY_SEARCH_FAILURE_KINDS,
    EbaySearchRequest,
    ebay_homepage_plan,
    ebay_search_plan,
    extract_ebay_search,
)
from carl.core.ebay_navigation import ebay_fetch_plan, ebay_pagination_token, is_ebay_search_url
from carl.core.ebay_search_support import require_supported_ebay_search_acquisition
from carl.core.http import RequestPlan, stored_content_encodings, stored_response_charset
from carl.core.models import (
    ArtifactDraft,
    BytesDraft,
    CodeProvenance,
    Header,
    JsonValue,
    NamedInput,
    NamedOutput,
    RecordDraft,
)
from carl.io.configuration import decodo_wreq_stack_settings, load_configuration
from carl.io.decodo import ManagedDecodoWreqAcquirer
from carl.io.httpx import Acquisition, AcquisitionFailure, HttpAcquirer
from carl.io.network_activity import (
    NetworkActivityFinalizationTimeout,
    acquire_for_network_activity,
    network_activity_definition,
    network_activity_scheduler,
)
from carl.io.paths import CarlDirectories
from carl.io.provenance import collect_code_provenance_async, process_invocation
from carl.io.resource_permits import run_cpu
from carl.io.sqlite import Database
from carl.io.stage_publication import ChildOperationDraft

COLLECT_EBAY_SEARCH = ComponentId(("carl", "ebay", "collect", "search_page"))
COLLECT_EBAY_SEARCH_HOMEPAGE = ComponentId(("carl", "ebay", "collect", "search_homepage"))
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
            Component(COLLECT_EBAY_SEARCH, 7, _collect_component),
            Component(COLLECT_EBAY_SEARCH_HOMEPAGE, 3, _collect_component),
            Component(EXTRACT_EBAY_SEARCH, 4, extract_ebay_search),
            Component(FINALIZE_EBAY_SEARCH, 4, _finalize_component),
            Component(RUN_EBAY_SEARCH_WORK, 8, _run_work_component),
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


def _attempt_summary(value: dict[str, JsonValue], attempt: int) -> dict[str, JsonValue]:
    return {
        "attempt": attempt,
        **{
            key: value[key]
            for key in (
                "acquisition_record_identifier",
                "extraction_record_identifier",
                "http_status",
                "response_classification",
            )
            if key in value
        },
    }


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
    policy = loaded.configuration.ebay_search
    route, credential_source, transport = decodo_wreq_stack_settings(
        loaded,
        directories,
        request.stack_identifier,
    )
    retry_scope = route.session_retry_scope()
    acquirer = ManagedDecodoWreqAcquirer(
        settings=route,
        credential_source=credential_source,
        transport_settings=transport,
    )
    search_run_identifier = search_run_identifier or new_identifier()
    provenance = await collect_code_provenance_async(_repository_root())
    async with acquirer.session(new_identifier) as session:
        home_plan = ebay_homepage_plan(network_path=route.route.network_path)
        home_attempts: list[JsonValue] = []
        homepage: dict[str, JsonValue] | None = None
        for attempt in range(1, policy.maximum_homepage_attempts + 1):
            try:
                homepage = await _collect_ebay_search_document(
                    database,
                    request=request,
                    plan=home_plan,
                    page_number=None,
                    page_ordinal=None,
                    search_run_identifier=search_run_identifier,
                    acquirer=session,
                    new_identifier=new_identifier,
                    provenance=provenance,
                    request_attempt=attempt,
                )
            except AcquisitionFailure as error:
                error.result["retry_scope"] = retry_scope
                if home_attempts:
                    error.result["homepage_attempts"] = home_attempts
                raise
            home_attempts.append(_attempt_summary(homepage, attempt))
            if homepage["http_status"] != 403:
                break
            home_plan = home_plan.model_copy(
                update={
                    "headers": (
                        Header(name=b"Referer", value=home_plan.url.encode("ascii")),
                        Header(name=b"Sec-Fetch-Site", value=b"same-origin"),
                    )
                }
            )
        assert homepage is not None
        homepage["attempts"] = home_attempts
        try:
            result = await collect_ebay_search(
                database,
                request=request,
                network_path=route.route.network_path,
                acquirer=session,
                new_identifier=new_identifier,
                search_run_identifier=search_run_identifier,
                provenance=provenance,
                homepage_acquisition=homepage,
                policy=policy,
                fetch_pagination=(
                    transport.emulation_profile == "Chrome154"
                    and transport.emulation_platform == "Linux"
                ),
            )
            return {**result, "retry_scope": retry_scope}
        except AcquisitionFailure as error:
            error.result["retry_scope"] = retry_scope
            error.result["homepage_acquisition"] = homepage
            raise


async def collect_ebay_search(
    database: Database,
    *,
    request: EbaySearchRequest,
    network_path: tuple[str, ...],
    acquirer: HttpAcquirer,
    new_identifier: Callable[[], str] = _identifier,
    provenance: CodeProvenance | None = None,
    search_run_identifier: str | None = None,
    homepage_acquisition: dict[str, JsonValue] | None = None,
    fetch_pagination: bool = False,
    policy: EbaySearchConfiguration | None = None,
) -> dict[str, JsonValue]:
    policy = policy or EbaySearchConfiguration()
    resolved_provenance = provenance or await collect_code_provenance_async(_repository_root())
    search_run_identifier = search_run_identifier or new_identifier()
    pages: list[dict[str, JsonValue]] = []
    page_number = 1
    seen_page_numbers: set[int] = set()
    stopping_reason = "maximum_pages"
    pagination_token: str | None = None
    previous_url: str | None = None
    pagination: dict[str, JsonValue] | None = (
        {"mode": "document", "fetch_pages": 0} if fetch_pagination else None
    )
    while True:
        page_ordinal = len(pages) + 1
        seen_page_numbers.add(page_number)
        plan = ebay_search_plan(request, network_path=network_path, page_number=page_number)
        if page_number > 1 and pagination_token is not None and previous_url is not None:
            plan = ebay_fetch_plan(
                request,
                network_path=network_path,
                page_number=page_number,
                token=pagination_token,
                referer=previous_url,
            )
            assert pagination is not None
            pagination["mode"] = "fetch"
            pagination["fetch_pages"] = int(pagination["fetch_pages"]) + 1
        attempts: list[JsonValue] = []
        page: dict[str, JsonValue] | None = None
        for attempt in range(1, policy.maximum_page_attempts + 1):
            try:
                page = await _collect_ebay_search_page(
                    database,
                    request=request,
                    plan=plan,
                    page_number=page_number,
                    page_ordinal=page_ordinal,
                    search_run_identifier=search_run_identifier,
                    acquirer=acquirer,
                    new_identifier=new_identifier,
                    provenance=resolved_provenance,
                    request_attempt=attempt,
                )
            except AcquisitionFailure as error:
                retained_attempts: list[JsonValue] = [
                    *[
                        item
                        for previous in pages
                        for item in cast(list[JsonValue], previous["attempts"])
                    ],
                    *attempts,
                ]
                if retained_attempts:
                    error.result["search_page_attempts"] = retained_attempts
                raise
            attempts.append(_attempt_summary(page, attempt))
            response = page.get("response_classification")
            if not isinstance(response, dict) or not (
                response.get("kind") == "challenge" or page.get("http_status") in {403, 429}
            ):
                break
        assert page is not None
        page["attempts"] = attempts
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
        if fetch_pagination and (page_ordinal == 1 or pagination_token is not None):
            _, _, acquisition = await database.get_record(
                str(page["acquisition_record_identifier"])
            )
            assert isinstance(acquisition, dict)
            effective_url = acquisition.get("effective_url")
            if isinstance(effective_url, str) and is_ebay_search_url(effective_url):
                previous_url = effective_url
                if page_ordinal == 1:
                    _, _, extraction = await database.get_record(
                        str(page["extraction_record_identifier"])
                    )
                    assert isinstance(extraction, dict)
                    _, decoded_body = await database.get_artifact(
                        str(extraction["decoded_body_artifact_identifier"])
                    )
                    try:
                        pagination_token = ebay_pagination_token(decoded_body.decode("utf-8"))
                    except ValueError as error:
                        assert pagination is not None
                        pagination["fallback_reason"] = str(error)
            else:
                previous_url = None
                pagination_token = None
                assert pagination is not None
                pagination["fallback_reason"] = "previous_page_not_ebay_search"
        page_number = next_page_number

    finalized = await _finalize_ebay_search(
        database,
        request=request,
        search_run_identifier=search_run_identifier,
        pages=pages,
        stopping_reason=stopping_reason,
        new_identifier=new_identifier,
        provenance=resolved_provenance,
        homepage_acquisition=homepage_acquisition,
        pagination=pagination,
        policy=policy,
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
        **({} if homepage_acquisition is None else {"homepage_acquisition": homepage_acquisition}),
        **finalized,
    }


async def _acquire_search_request(
    acquirer: HttpAcquirer, plan: RequestPlan, new_identifier: Callable[[], str]
) -> Acquisition:
    try:
        return await acquirer.acquire(plan, new_identifier)
    except TimeoutError as error:
        raise AcquisitionFailure(
            "eBay search request timed out",
            result={
                "hops": [],
                "stopping_condition": "transport_failure",
                "exception_type": "TimeoutError",
                "failure_phase": "request_timeout",
                "failed_request": {"method": plan.method, "url": plan.url},
            },
        ) from error


async def _collect_ebay_search_document(
    database: Database,
    *,
    request: EbaySearchRequest,
    plan: RequestPlan,
    page_number: int | None,
    page_ordinal: int | None,
    search_run_identifier: str,
    acquirer: HttpAcquirer,
    new_identifier: Callable[[], str],
    provenance: CodeProvenance,
    request_attempt: int = 1,
) -> dict[str, JsonValue]:
    homepage = page_number is None
    purpose = "ebay_search_homepage" if homepage else "ebay_search_page"
    registry = build_ebay_component_registry()
    component = registry.require(COLLECT_EBAY_SEARCH_HOMEPAGE if homepage else COLLECT_EBAY_SEARCH)
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
            "purpose": purpose,
            "page_number": page_number,
            "page_ordinal": page_ordinal,
            "request_attempt": request_attempt,
            "search_run_record_identifier": search_run_identifier,
            "request_plan": plan.as_json(),
        },
        started_at_utc=started_at_utc,
    )
    acquisition: Acquisition | None = None
    try:
        activity = network_activity_definition(
            identifier=new_identifier(),
            kind=(
                "carl",
                "ebay",
                "network_activity",
                "search_homepage" if homepage else "search_page",
            ),
            operation_identifier=operation_identifier,
            network_session_identifier=search_run_identifier,
            network_path=plan.routing,
            ordinal=1 if page_ordinal is None else page_ordinal,
            attempt=request_attempt,
        )
        async with network_activity_scheduler(database, new_identifier).admit(activity) as permit:
            permit.mark_dispatched()
            acquisition = await acquire_for_network_activity(
                _acquire_search_request(acquirer, plan, new_identifier), permit.admission
            )
    except (AcquisitionFailure, NetworkActivityFinalizationTimeout) as error:
        if isinstance(error, NetworkActivityFinalizationTimeout):
            if acquisition is None:
                raise AssertionError("Finalized search activity has no acquisition") from error
            acquisition_identifier = new_identifier()
            # Retain the completed HTTP response separately from the local
            # bookkeeping failure, without relabeling it as a site error.
            records = (
                RecordDraft(
                    identifier=acquisition_identifier,
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value={
                        **acquisition.record,
                        "operation_identifier": operation_identifier,
                        "purpose": purpose,
                        "ebay_search_request": request.model_dump(mode="json"),
                        "acquisition_stack_identifier": request.stack_identifier,
                        "page_number": page_number,
                        "page_ordinal": page_ordinal,
                        "request_attempt": request_attempt,
                        "search_run_record_identifier": search_run_identifier,
                    },
                ),
            )
            failure = AcquisitionFailure(
                "eBay search activity finalization timed out",
                result={
                    **acquisition.record,
                    "stopping_condition": "network_activity_failure",
                    "exception_type": "NetworkActivityFinalizationTimeout",
                    "failure_phase": "network_activity_finalization_timeout",
                    "acquisition_record_identifier": acquisition_identifier,
                },
                bodies=acquisition.bodies,
            )
            if homepage:
                failure.result["homepage_acquisition"] = {
                    "acquisition_operation_identifier": operation_identifier,
                    "acquisition_record_identifier": acquisition_identifier,
                    "http_status": _terminal_response(acquisition.record).get("status_code"),
                }
            retained_outputs = (
                NamedOutput(name=("acquisition",), object_identifier=acquisition_identifier),
            )
        else:
            failure = error
            records = ()
            retained_outputs = ()
        artifacts = _body_artifacts(Acquisition(record=failure.result, bodies=failure.bodies))
        await database.fail_operation(
            operation_id=operation_identifier,
            error={"type": type(error).__name__, "message": str(error)},
            result={
                "state": "acquisition_failed",
                "acquisition": failure.result,
            },
            ended_at_utc=_utc_now(),
            duration_ns=perf_counter_ns() - started,
            artifacts=artifacts,
            records=records,
            outputs=retained_outputs
            + tuple(
                NamedOutput(name=("body", str(index)), object_identifier=artifact.identifier)
                for index, artifact in enumerate(artifacts)
            ),
        )
        if failure is error:
            raise
        raise failure from error

    draft, result = build_search_document_draft(
        acquisition=acquisition,
        request=request,
        plan=plan,
        page_number=page_number,
        page_ordinal=page_ordinal,
        search_run_identifier=search_run_identifier,
        operation_identifier=operation_identifier,
        new_identifier=new_identifier,
        started_at_utc=started_at_utc,
        ended_at_utc=_utc_now(),
        duration_ns=perf_counter_ns() - started,
        artifacts=_body_artifacts(acquisition),
        request_attempt=request_attempt,
    )
    await complete_search_child_draft(database, draft)
    return result


def build_search_document_draft(
    *,
    acquisition: Acquisition,
    request: EbaySearchRequest,
    plan: RequestPlan,
    page_number: int | None,
    page_ordinal: int | None,
    search_run_identifier: str,
    operation_identifier: str,
    new_identifier: Callable[[], str],
    started_at_utc: str,
    ended_at_utc: str,
    duration_ns: int,
    artifacts: tuple[ArtifactDraft, ...],
    request_attempt: int = 1,
) -> tuple[ChildOperationDraft, dict[str, JsonValue]]:
    """Prepare one captured document's original child-operation evidence."""
    homepage = page_number is None
    purpose = "ebay_search_homepage" if homepage else "ebay_search_page"
    component = build_ebay_component_registry().require(
        COLLECT_EBAY_SEARCH_HOMEPAGE if homepage else COLLECT_EBAY_SEARCH
    )
    acquisition_identifier = new_identifier()
    acquisition_value = dict(acquisition.record)
    acquisition_value.update(
        {
            "operation_identifier": operation_identifier,
            "purpose": purpose,
            "ebay_search_request": request.model_dump(mode="json"),
            "acquisition_stack_identifier": request.stack_identifier,
            "page_number": page_number,
            "page_ordinal": page_ordinal,
            "request_attempt": request_attempt,
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
    outputs = [NamedOutput(name=("acquisition",), object_identifier=acquisition_identifier)]
    outputs.extend(
        NamedOutput(name=("response", str(index), "body"), object_identifier=body.identifier)
        for index, body in enumerate(acquisition.bodies)
    )
    terminal_response = _terminal_response(acquisition_value)
    draft = ChildOperationDraft(
        identifier=operation_identifier,
        component=component,
        configuration={
            "request": request.model_dump(mode="json"),
            "purpose": purpose,
            "page_number": page_number,
            "page_ordinal": page_ordinal,
            "request_attempt": request_attempt,
            "search_run_record_identifier": search_run_identifier,
            "request_plan": plan.as_json(),
        },
        inputs=(),
        records=records,
        artifacts=artifacts,
        outputs=tuple(outputs),
        result={
            "acquisition_record_identifier": acquisition_identifier,
            "http_status": terminal_response.get("status_code"),
        },
        started_at_utc=started_at_utc,
        ended_at_utc=ended_at_utc,
        duration_ns=duration_ns,
    )
    result: dict[str, JsonValue] = {
        "acquisition_operation_identifier": operation_identifier,
        "acquisition_record_identifier": acquisition_identifier,
        "http_status": terminal_response.get("status_code"),
    }

    return draft, result


async def _collect_ebay_search_page(
    database: Database,
    *,
    request: EbaySearchRequest,
    plan: RequestPlan,
    page_number: int,
    page_ordinal: int,
    search_run_identifier: str,
    acquirer: HttpAcquirer,
    new_identifier: Callable[[], str],
    provenance: CodeProvenance,
    request_attempt: int = 1,
) -> dict[str, JsonValue]:
    acquisition = await _collect_ebay_search_document(
        database,
        request=request,
        plan=plan,
        page_number=page_number,
        page_ordinal=page_ordinal,
        search_run_identifier=search_run_identifier,
        acquirer=acquirer,
        new_identifier=new_identifier,
        provenance=provenance,
        request_attempt=request_attempt,
    )
    acquisition_identifier = acquisition["acquisition_record_identifier"]
    assert isinstance(acquisition_identifier, str)
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
        **acquisition,
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
        draft, result, _ = await build_search_extraction_draft(
            acquisition_value=acquisition_value,
            acquisition_identifier=acquisition_identifier,
            body_identifier=body_identifier,
            body_metadata=body_metadata,
            stored_body=stored_body,
            request=request,
            search_run_identifier=search_run_identifier,
            page_number=page_number,
            page_ordinal=page_ordinal,
            operation_identifier=operation_identifier,
            new_identifier=new_identifier,
            started_at_utc=started_at_utc,
            started_monotonic_ns=started,
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

    await complete_search_child_draft(database, draft)
    return result


async def build_search_extraction_draft(
    *,
    acquisition_value: dict[str, JsonValue],
    acquisition_identifier: str,
    body_identifier: str,
    body_metadata: dict[str, JsonValue],
    stored_body: bytes,
    request: EbaySearchRequest,
    search_run_identifier: str,
    page_number: int,
    page_ordinal: int,
    operation_identifier: str,
    new_identifier: Callable[[], str],
    started_at_utc: str,
    started_monotonic_ns: int,
) -> tuple[ChildOperationDraft, dict[str, JsonValue], str]:
    """Decode and prepare immutable search extraction evidence, with no writes."""
    component = build_ebay_component_registry().require(EXTRACT_EBAY_SEARCH)
    terminal_response = _terminal_response(acquisition_value)
    headers = terminal_response.get("headers")
    decoded = await run_cpu(
        decoded_stored_body,
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
    extraction = await run_cpu(
        partial(
            extract_ebay_search,
            html,
            status_code=status_code,
            listing_state=request.listing_state,
            redirect_target=(
                terminal_response["redirect_target"]
                if isinstance(terminal_response.get("redirect_target"), str)
                else None
            ),
        )
    )
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
    draft = ChildOperationDraft(
        identifier=operation_identifier,
        component=component,
        configuration={
            "acquisition_record_identifier": acquisition_identifier,
            "request": request.model_dump(mode="json"),
            "search_run_record_identifier": search_run_identifier,
            "page_number": page_number,
            "page_ordinal": page_ordinal,
        },
        inputs=(
            NamedInput(name=("acquisition",), object_identifier=acquisition_identifier),
            NamedInput(name=("terminal_response", "body"), object_identifier=body_identifier),
        ),
        records=records,
        artifacts=(decoded_artifact,),
        outputs=tuple(outputs),
        result={
            "extraction_record_identifier": extraction_identifier,
            "response_classification": classification,
            "next_page_number": extraction.next_page_number,
            "listing_count": len(occurrence_identifiers),
        },
        started_at_utc=started_at_utc,
        ended_at_utc=_utc_now(),
        duration_ns=perf_counter_ns() - started_monotonic_ns,
    )
    result: dict[str, JsonValue] = {
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

    return draft, result, html


async def _finalize_ebay_search(
    database: Database,
    *,
    request: EbaySearchRequest,
    search_run_identifier: str,
    pages: Sequence[dict[str, JsonValue]],
    stopping_reason: str,
    new_identifier: Callable[[], str],
    provenance: CodeProvenance,
    policy: EbaySearchConfiguration,
    homepage_acquisition: dict[str, JsonValue] | None = None,
    pagination: dict[str, JsonValue] | None = None,
) -> dict[str, JsonValue]:
    draft, result = build_search_finalization_draft(
        request=request,
        search_run_identifier=search_run_identifier,
        pages=pages,
        stopping_reason=stopping_reason,
        new_identifier=new_identifier,
        policy=policy,
        homepage_acquisition=homepage_acquisition,
        pagination=pagination,
    )
    await database.begin_operation(
        operation_id=draft.identifier,
        component=draft.component,
        provenance=provenance,
        invocation=process_invocation(),
        configuration=draft.configuration,
        started_at_utc=draft.started_at_utc,
        inputs=tuple((item.name, item.object_identifier) for item in draft.inputs),
    )
    await complete_search_child_draft(database, draft)
    return result


def build_search_finalization_draft(
    *,
    request: EbaySearchRequest,
    search_run_identifier: str,
    pages: Sequence[dict[str, JsonValue]],
    stopping_reason: str,
    new_identifier: Callable[[], str],
    policy: EbaySearchConfiguration,
    homepage_acquisition: dict[str, JsonValue] | None = None,
    pagination: dict[str, JsonValue] | None = None,
) -> tuple[ChildOperationDraft, dict[str, JsonValue]]:
    """Prepare the original manifest and its complete page-input provenance."""
    if not pages:
        raise ValueError("An eBay search run requires at least one retained page")
    registry = build_ebay_component_registry()
    component = registry.require(FINALIZE_EBAY_SEARCH)
    operation_identifier = new_identifier()
    started_at_utc = _utc_now()
    started = perf_counter_ns()
    inputs: list[tuple[tuple[str, ...], str]] = []
    if homepage_acquisition is not None:
        homepage_identifier = homepage_acquisition.get("acquisition_record_identifier")
        if not isinstance(homepage_identifier, str):
            raise ValueError("An eBay homepage has a malformed evidence reference")
        inputs.append((("homepage", "acquisition"), homepage_identifier))
        for attempt in cast(list[dict[str, JsonValue]], homepage_acquisition.get("attempts", [])):
            inputs.append(
                (
                    ("homepage", "attempt", str(attempt["attempt"]), "acquisition"),
                    str(attempt["acquisition_record_identifier"]),
                )
            )
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
        for attempt in cast(list[dict[str, JsonValue]], page["attempts"]):
            for kind in ("acquisition", "extraction"):
                inputs.append(
                    (
                        ("page", str(index), "attempt", str(attempt["attempt"]), kind),
                        str(attempt[f"{kind}_record_identifier"]),
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
                    "attempts",
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
        "collection_policy": policy.model_dump(mode="json"),
        **({} if homepage_acquisition is None else {"homepage_acquisition": homepage_acquisition}),
        **({} if pagination is None else {"pagination": pagination}),
    }
    draft = ChildOperationDraft(
        identifier=operation_identifier,
        component=component,
        configuration={
            "request": request.model_dump(mode="json"),
            "search_run_record_identifier": search_run_identifier,
            "page_count": len(pages),
            "stopping_reason": stopping_reason,
            "collection_policy": policy.model_dump(mode="json"),
        },
        inputs=tuple(
            NamedInput(name=name, object_identifier=identifier) for name, identifier in inputs
        ),
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
        started_at_utc=started_at_utc,
        ended_at_utc=_utc_now(),
        duration_ns=perf_counter_ns() - started,
    )
    result: dict[str, JsonValue] = {
        "finalization_operation_identifier": operation_identifier,
        "search_run_record_identifier": search_run_identifier,
        "page_count": len(pages),
        "listing_count": len(unique_listing_identifiers),
        "listing_occurrence_count": len(occurrence_identifiers),
        "stopping_reason": stopping_reason,
        "listing_occurrence_record_identifiers": occurrence_identifiers,
        **({} if pagination is None else {"pagination": pagination}),
    }

    return draft, result


async def complete_search_child_draft(database: Database, draft: ChildOperationDraft) -> None:
    """Complete a legacy operation that has already been started by its wrapper."""
    await database.complete_operation(
        operation_id=draft.identifier,
        records=draft.records,
        artifacts=draft.artifacts,
        outputs=draft.outputs,
        result=draft.result,
        ended_at_utc=draft.ended_at_utc,
        duration_ns=draft.duration_ns,
    )
