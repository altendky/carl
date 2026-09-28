"""Pure protocol extraction for Facebook Marketplace search pages."""

from urllib.parse import urlsplit

from pydantic import Field

from carl.core.facebook import parse_json_blocks
from carl.core.facebook_search import SearchPageFacts
from carl.core.json import decode_json
from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel

SEARCH_QUERY_NAME = "CometMarketplaceSearchContentContainerQuery"
_SEARCH_CONNECTION_PATH = ("data", "marketplace_search", "feed_units")


class SearchProtocolIssueKind(JsonStringEnumeration):
    EMBEDDED_JSON_PARSE_FAILURE = "embedded_json_parse_failure"
    ROUTE_DEFINITION_FRAME_PARSE_FAILURE = "route_definition_frame_parse_failure"
    MISSING_QUERY_PLAN = "missing_query_plan"
    AMBIGUOUS_QUERY_PLAN = "ambiguous_query_plan"
    MALFORMED_QUERY_PLAN = "malformed_query_plan"
    MISSING_SEARCH_CONNECTION = "missing_search_connection"
    AMBIGUOUS_SEARCH_CONNECTION = "ambiguous_search_connection"
    MALFORMED_PAGE_INFORMATION = "malformed_page_information"
    MISSING_NEXT_CURSOR = "missing_next_cursor"
    MALFORMED_SEARCH_EDGE = "malformed_search_edge"
    MALFORMED_LISTING_IDENTIFIER = "malformed_listing_identifier"
    MALFORMED_PAGINATION_RESPONSE = "malformed_pagination_response"
    GRAPHQL_ERRORS = "graphql_errors"
    GRAPHQL_RATE_LIMITED = "graphql_rate_limited"


class SearchBootstrapResponseKind(JsonStringEnumeration):
    SEARCH_RESULTS = "search_results"
    LOGIN_PAGE = "login_page"
    BOT_CHALLENGE = "bot_challenge"
    GENERIC_ERROR_PAGE = "generic_error_page"
    SEARCH_PAYLOAD_ABSENT = "search_payload_absent"
    MALFORMED_RESPONSE = "malformed_response"


class SearchSessionBootstrapResponseKind(JsonStringEnumeration):
    MARKETPLACE_LANDING = "marketplace_landing"
    LOGIN_PAGE = "login_page"
    BOT_CHALLENGE = "bot_challenge"
    GENERIC_ERROR_PAGE = "generic_error_page"
    UNRECOGNIZED = "unrecognized"


class SearchSessionBootstrapResponseClassification(StrictModel):
    kind: SearchSessionBootstrapResponseKind
    effective_url: str
    evidence: tuple[str, ...]


class SearchBootstrapResponseClassification(StrictModel):
    kind: SearchBootstrapResponseKind
    effective_url: str
    evidence: tuple[str, ...]


def classify_search_session_bootstrap_response(
    html: str,
    *,
    effective_url: str,
) -> SearchSessionBootstrapResponseClassification:
    path = urlsplit(effective_url).path.casefold()
    folded = html.casefold()

    def result(
        kind: SearchSessionBootstrapResponseKind,
        *evidence: str,
    ) -> SearchSessionBootstrapResponseClassification:
        return SearchSessionBootstrapResponseClassification(
            kind=kind,
            effective_url=effective_url,
            evidence=evidence,
        )

    if "/checkpoint/" in path or any(
        marker in folded
        for marker in ('id="captcha"', "checkpointsubmitbutton", "security check required")
    ):
        return result(SearchSessionBootstrapResponseKind.BOT_CHALLENGE, "challenge_marker")
    if "/login" in path:
        return result(SearchSessionBootstrapResponseKind.LOGIN_PAGE, "login_effective_url")
    if any(
        marker in folded
        for marker in ("something went wrong", "facebook.com/error", "temporarily blocked")
    ):
        return result(SearchSessionBootstrapResponseKind.GENERIC_ERROR_PAGE, "error_marker")
    if path.rstrip("/") == "/marketplace":
        return result(SearchSessionBootstrapResponseKind.MARKETPLACE_LANDING, "marketplace_url")
    if any(marker in folded for marker in ('id="login_form"', 'name="login"', "login/?next=")):
        return result(SearchSessionBootstrapResponseKind.LOGIN_PAGE, "login_marker")
    return result(SearchSessionBootstrapResponseKind.UNRECOGNIZED, "marketplace_marker_absent")


class SearchSourceReference(StrictModel):
    acquisition_record_identifier: str = Field(min_length=1)
    block_index: int | None = Field(default=None, ge=0)
    frame_index: int | None = Field(default=None, ge=0)
    json_path: tuple[str | int, ...]


class SearchProtocolIssue(StrictModel):
    kind: SearchProtocolIssueKind
    source: SearchSourceReference | None = None
    edge_index: int | None = Field(default=None, ge=0)
    provider_code: int | None = None


class AppliedSearchConfiguration(StrictModel):
    query_identifier: str = Field(min_length=1)
    query_name: str = Field(min_length=1)
    preloader_identifier: str | None = Field(default=None, min_length=1)
    complete_variables: dict[str, JsonValue]
    browse_request_parameters: dict[str, JsonValue] | None
    sources: tuple[SearchSourceReference, ...]


class SearchPageInformation(StrictModel):
    has_next_page: bool
    end_cursor: str | None = Field(default=None, min_length=1)


class SearchListingOccurrence(StrictModel):
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    edge_index: int = Field(ge=0)
    edge_cursor: str | None
    story_identifier: str | None
    source: SearchSourceReference
    original: dict[str, JsonValue]


class IgnoredSearchEdge(StrictModel):
    edge_index: int = Field(ge=0)
    source: SearchSourceReference
    original: JsonValue


class SearchPageExtraction(StrictModel):
    sources: tuple[SearchSourceReference, ...]
    page_information: SearchPageInformation
    feed_session_identifier: str | None
    listing_occurrences: tuple[SearchListingOccurrence, ...]
    ignored_edges: tuple[IgnoredSearchEdge, ...]


class SearchBootstrapExtraction(StrictModel):
    blocks: tuple[dict[str, JsonValue], ...]
    applied_configuration: AppliedSearchConfiguration | None
    page: SearchPageExtraction | None
    issues: tuple[SearchProtocolIssue, ...]


class SearchRouteDefinitionFrame(StrictModel):
    frame_index: int = Field(ge=0)
    line_index: int = Field(ge=0)
    anti_hijacking_prefix_present: bool
    value: dict[str, JsonValue] | None
    parse_error: dict[str, JsonValue] | None
    unparsed_text: str | None = None


class SearchRouteDefinitionExtraction(StrictModel):
    frames: tuple[SearchRouteDefinitionFrame, ...]
    applied_configuration: AppliedSearchConfiguration | None
    page: SearchPageExtraction | None
    issues: tuple[SearchProtocolIssue, ...]


class SearchPaginationExtraction(StrictModel):
    parsed_response: dict[str, JsonValue] | None
    graphql_errors: tuple[JsonValue, ...]
    page: SearchPageExtraction | None
    issues: tuple[SearchProtocolIssue, ...]


def classify_search_bootstrap_response(
    html: str,
    *,
    effective_url: str,
    extraction: SearchBootstrapExtraction,
) -> SearchBootstrapResponseClassification:
    """Classify a complete bootstrap response before interpreting extraction failures."""

    path = urlsplit(effective_url).path.casefold()
    folded = html.casefold()

    def result(
        kind: SearchBootstrapResponseKind,
        *evidence: str,
    ) -> SearchBootstrapResponseClassification:
        return SearchBootstrapResponseClassification(
            kind=kind,
            effective_url=effective_url,
            evidence=evidence,
        )

    if "/checkpoint/" in path or any(
        marker in folded
        for marker in ('id="captcha"', "checkpointsubmitbutton", "security check required")
    ):
        return result(SearchBootstrapResponseKind.BOT_CHALLENGE, "challenge_marker")
    if "/login" in path or any(
        marker in folded for marker in ('id="login_form"', 'name="login"', "login/?next=")
    ):
        return result(SearchBootstrapResponseKind.LOGIN_PAGE, "login_marker")
    if any(
        marker in folded
        for marker in ("something went wrong", "facebook.com/error", "temporarily blocked")
    ):
        return result(SearchBootstrapResponseKind.GENERIC_ERROR_PAGE, "error_marker")
    if extraction.page is not None and extraction.applied_configuration is not None:
        return result(
            SearchBootstrapResponseKind.SEARCH_RESULTS,
            "search_connection",
            "query_plan",
        )
    return result(
        SearchBootstrapResponseKind.SEARCH_PAYLOAD_ABSENT,
        "search_connection_or_query_plan_absent",
    )


def traversal_page_facts(
    page: SearchPageExtraction,
    *,
    interval_elapsed_duration_ns: int,
    transferred_bytes: int | None,
    decoded_body_bytes: int,
) -> SearchPageFacts:
    """Convert a retained page extraction into traversal input without deduplicating it."""

    return SearchPageFacts(
        listing_identifiers=tuple(
            occurrence.listing_identifier for occurrence in page.listing_occurrences
        ),
        has_next_page=page.page_information.has_next_page,
        end_cursor=page.page_information.end_cursor,
        interval_elapsed_duration_ns=interval_elapsed_duration_ns,
        transferred_bytes=transferred_bytes,
        decoded_body_bytes=decoded_body_bytes,
    )


def _walk_objects(
    value: JsonValue,
) -> tuple[tuple[dict[str, JsonValue], tuple[str | int, ...]], ...]:
    found: list[tuple[dict[str, JsonValue], tuple[str | int, ...]]] = []
    pending: list[tuple[JsonValue, tuple[str | int, ...]]] = [(value, ())]
    while pending:
        node, path = pending.pop()
        if isinstance(node, dict):
            found.append((node, path))
            pending.extend((child, (*path, key)) for key, child in reversed(tuple(node.items())))
        elif isinstance(node, list):
            pending.extend(
                (child, (*path, index)) for index, child in reversed(tuple(enumerate(node)))
            )
    return tuple(found)


def _source(
    acquisition_record_identifier: str,
    path: tuple[str | int, ...],
    *,
    block_index: int | None,
    frame_index: int | None = None,
) -> SearchSourceReference:
    return SearchSourceReference(
        acquisition_record_identifier=acquisition_record_identifier,
        block_index=block_index,
        frame_index=frame_index,
        json_path=path,
    )


def _query_candidates(
    blocks: tuple[dict[str, JsonValue], ...],
    *,
    acquisition_record_identifier: str,
) -> tuple[
    tuple[dict[str, JsonValue], SearchSourceReference],
    ...,
]:
    candidates: list[tuple[dict[str, JsonValue], SearchSourceReference]] = []
    for block in blocks:
        value = block.get("value")
        block_index = block.get("block_index")
        if value is None or not isinstance(block_index, int):
            continue
        for obj, path in _walk_objects(value):
            preloaders = obj.get("expectedPreloaders")
            if not isinstance(preloaders, list):
                continue
            for index, candidate in enumerate(preloaders):
                if isinstance(candidate, dict) and candidate.get("queryName") == SEARCH_QUERY_NAME:
                    candidates.append(
                        (
                            candidate,
                            _source(
                                acquisition_record_identifier,
                                (*path, "expectedPreloaders", index),
                                block_index=block_index,
                            ),
                        )
                    )
    return tuple(candidates)


def _extract_applied_configuration(
    blocks: tuple[dict[str, JsonValue], ...],
    *,
    acquisition_record_identifier: str,
) -> tuple[AppliedSearchConfiguration | None, tuple[SearchProtocolIssue, ...]]:
    candidates = _query_candidates(
        blocks,
        acquisition_record_identifier=acquisition_record_identifier,
    )
    if not candidates:
        return None, (SearchProtocolIssue(kind=SearchProtocolIssueKind.MISSING_QUERY_PLAN),)

    valid: list[tuple[str, str, dict[str, JsonValue], SearchSourceReference]] = []
    issues: list[SearchProtocolIssue] = []
    for candidate, source in candidates:
        query_identifier = candidate.get("queryID")
        query_name = candidate.get("queryName")
        variables = candidate.get("variables")
        if (
            not isinstance(query_identifier, str)
            or not query_identifier
            or not isinstance(query_name, str)
            or not isinstance(variables, dict)
        ):
            issues.append(
                SearchProtocolIssue(
                    kind=SearchProtocolIssueKind.MALFORMED_QUERY_PLAN,
                    source=source,
                )
            )
            continue
        valid.append((query_identifier, query_name, variables, source))
    if not valid:
        return None, tuple(issues)

    first_identifier, first_name, first_variables, _ = valid[0]
    if any(
        identifier != first_identifier or name != first_name or variables != first_variables
        for identifier, name, variables, _ in valid[1:]
    ):
        issues.append(SearchProtocolIssue(kind=SearchProtocolIssueKind.AMBIGUOUS_QUERY_PLAN))
        return None, tuple(issues)

    parameters = first_variables.get("params")
    browse_request_parameters: dict[str, JsonValue] | None = None
    if isinstance(parameters, dict):
        candidate_parameters = parameters.get("browse_request_params")
        if isinstance(candidate_parameters, dict):
            browse_request_parameters = candidate_parameters
    return (
        AppliedSearchConfiguration(
            query_identifier=first_identifier,
            query_name=first_name,
            preloader_identifier=None,
            complete_variables=first_variables,
            browse_request_parameters=browse_request_parameters,
            sources=tuple(source for _, _, _, source in valid),
        ),
        tuple(issues),
    )


def _route_query_candidates(
    frames: tuple[SearchRouteDefinitionFrame, ...],
    *,
    acquisition_record_identifier: str,
) -> tuple[tuple[dict[str, JsonValue], SearchSourceReference], ...]:
    candidates: list[tuple[dict[str, JsonValue], SearchSourceReference]] = []
    for frame in frames:
        if frame.value is None:
            continue
        preloaders = frame.value.get("preloaders")
        if not isinstance(preloaders, list):
            continue
        for index, candidate in enumerate(preloaders):
            if isinstance(candidate, dict) and candidate.get("queryName") == SEARCH_QUERY_NAME:
                candidates.append(
                    (
                        candidate,
                        _source(
                            acquisition_record_identifier,
                            ("preloaders", index),
                            block_index=None,
                            frame_index=frame.frame_index,
                        ),
                    )
                )
    return tuple(candidates)


def _extract_route_applied_configuration(
    frames: tuple[SearchRouteDefinitionFrame, ...],
    *,
    acquisition_record_identifier: str,
) -> tuple[AppliedSearchConfiguration | None, tuple[SearchProtocolIssue, ...]]:
    candidates = _route_query_candidates(
        frames,
        acquisition_record_identifier=acquisition_record_identifier,
    )
    if not candidates:
        return None, (SearchProtocolIssue(kind=SearchProtocolIssueKind.MISSING_QUERY_PLAN),)

    valid: list[tuple[str, str, str, dict[str, JsonValue], SearchSourceReference]] = []
    issues: list[SearchProtocolIssue] = []
    for candidate, source in candidates:
        query_identifier = candidate.get("queryID")
        query_name = candidate.get("queryName")
        preloader_identifier = candidate.get("preloaderID")
        variables = candidate.get("variables")
        if (
            not isinstance(query_identifier, str)
            or not query_identifier
            or not isinstance(query_name, str)
            or not isinstance(variables, dict)
            or not isinstance(preloader_identifier, str)
            or not preloader_identifier
        ):
            issues.append(
                SearchProtocolIssue(
                    kind=SearchProtocolIssueKind.MALFORMED_QUERY_PLAN,
                    source=source,
                )
            )
            continue
        valid.append((query_identifier, query_name, preloader_identifier, variables, source))
    if not valid:
        return None, tuple(issues)

    first_identifier, first_name, first_preloader_identifier, first_variables, _ = valid[0]
    if any(
        identifier != first_identifier
        or name != first_name
        or preloader_identifier != first_preloader_identifier
        or variables != first_variables
        for identifier, name, preloader_identifier, variables, _ in valid[1:]
    ):
        issues.append(SearchProtocolIssue(kind=SearchProtocolIssueKind.AMBIGUOUS_QUERY_PLAN))
        return None, tuple(issues)

    parameters = first_variables.get("params")
    browse_request_parameters: dict[str, JsonValue] | None = None
    if isinstance(parameters, dict):
        candidate_parameters = parameters.get("browse_request_params")
        if isinstance(candidate_parameters, dict):
            browse_request_parameters = candidate_parameters
    return (
        AppliedSearchConfiguration(
            query_identifier=first_identifier,
            query_name=first_name,
            preloader_identifier=first_preloader_identifier,
            complete_variables=first_variables,
            browse_request_parameters=browse_request_parameters,
            sources=tuple(source for _, _, _, _, source in valid),
        ),
        tuple(issues),
    )


def _connection_candidates(
    values: tuple[tuple[JsonValue, int | None, int | None], ...],
    *,
    acquisition_record_identifier: str,
) -> tuple[tuple[dict[str, JsonValue], SearchSourceReference], ...]:
    candidates: list[tuple[dict[str, JsonValue], SearchSourceReference]] = []
    for value, block_index, frame_index in values:
        for obj, path in _walk_objects(value):
            data = obj.get("data")
            if not isinstance(data, dict):
                continue
            marketplace_search = data.get("marketplace_search")
            if not isinstance(marketplace_search, dict):
                continue
            feed_units = marketplace_search.get("feed_units")
            if isinstance(feed_units, dict):
                candidates.append(
                    (
                        feed_units,
                        _source(
                            acquisition_record_identifier,
                            (*path, *_SEARCH_CONNECTION_PATH),
                            block_index=block_index,
                            frame_index=frame_index,
                        ),
                    )
                )
    return tuple(candidates)


def _extract_page(
    values: tuple[tuple[JsonValue, int | None, int | None], ...],
    *,
    acquisition_record_identifier: str,
) -> tuple[SearchPageExtraction | None, tuple[SearchProtocolIssue, ...]]:
    candidates = _connection_candidates(
        values,
        acquisition_record_identifier=acquisition_record_identifier,
    )
    if not candidates:
        return None, (SearchProtocolIssue(kind=SearchProtocolIssueKind.MISSING_SEARCH_CONNECTION),)
    first_feed, _ = candidates[0]
    if any(feed != first_feed for feed, _ in candidates[1:]):
        return None, (
            SearchProtocolIssue(kind=SearchProtocolIssueKind.AMBIGUOUS_SEARCH_CONNECTION),
        )

    sources = tuple(source for _, source in candidates)
    source = sources[0]
    page_information_value = first_feed.get("page_info")
    edges = first_feed.get("edges")
    if not isinstance(page_information_value, dict) or not isinstance(edges, list):
        return None, (
            SearchProtocolIssue(
                kind=SearchProtocolIssueKind.MALFORMED_PAGE_INFORMATION,
                source=source,
            ),
        )
    has_next_page = page_information_value.get("has_next_page")
    end_cursor = page_information_value.get("end_cursor")
    if not isinstance(has_next_page, bool) or not (
        end_cursor is None or (isinstance(end_cursor, str) and end_cursor)
    ):
        return None, (
            SearchProtocolIssue(
                kind=SearchProtocolIssueKind.MALFORMED_PAGE_INFORMATION,
                source=_source(
                    acquisition_record_identifier,
                    (*source.json_path, "page_info"),
                    block_index=source.block_index,
                ),
            ),
        )

    issues: list[SearchProtocolIssue] = []
    if has_next_page and end_cursor is None:
        issues.append(
            SearchProtocolIssue(
                kind=SearchProtocolIssueKind.MISSING_NEXT_CURSOR,
                source=_source(
                    acquisition_record_identifier,
                    (*source.json_path, "page_info"),
                    block_index=source.block_index,
                ),
            )
        )
    occurrences: list[SearchListingOccurrence] = []
    ignored: list[IgnoredSearchEdge] = []
    for edge_index, edge in enumerate(edges):
        edge_source = _source(
            acquisition_record_identifier,
            (*source.json_path, "edges", edge_index),
            block_index=source.block_index,
        )
        if not isinstance(edge, dict):
            issues.append(
                SearchProtocolIssue(
                    kind=SearchProtocolIssueKind.MALFORMED_SEARCH_EDGE,
                    source=edge_source,
                    edge_index=edge_index,
                )
            )
            ignored.append(
                IgnoredSearchEdge(
                    edge_index=edge_index,
                    source=edge_source,
                    original=edge,
                )
            )
            continue
        node = edge.get("node")
        listing = node.get("listing") if isinstance(node, dict) else None
        listing_identifier = listing.get("id") if isinstance(listing, dict) else None
        listing_shaped = isinstance(listing, dict) and any(
            isinstance(listing.get(field), str)
            for field in ("marketplace_listing_title", "custom_title")
        )
        if not listing_shaped:
            ignored.append(
                IgnoredSearchEdge(
                    edge_index=edge_index,
                    source=edge_source,
                    original=edge,
                )
            )
            continue
        assert isinstance(listing, dict)
        if (
            not isinstance(listing_identifier, str)
            or not listing_identifier.isascii()
            or not listing_identifier.isdecimal()
        ):
            issues.append(
                SearchProtocolIssue(
                    kind=SearchProtocolIssueKind.MALFORMED_LISTING_IDENTIFIER,
                    source=edge_source,
                    edge_index=edge_index,
                )
            )
            ignored.append(
                IgnoredSearchEdge(
                    edge_index=edge_index,
                    source=edge_source,
                    original=edge,
                )
            )
            continue
        edge_cursor = edge.get("cursor")
        story_identifier = node.get("id") if isinstance(node, dict) else None
        occurrences.append(
            SearchListingOccurrence(
                listing_identifier=listing_identifier,
                edge_index=edge_index,
                edge_cursor=edge_cursor if isinstance(edge_cursor, str) else None,
                story_identifier=story_identifier if isinstance(story_identifier, str) else None,
                source=_source(
                    acquisition_record_identifier,
                    (*edge_source.json_path, "node", "listing"),
                    block_index=edge_source.block_index,
                ),
                original=listing,
            )
        )

    feed_session_identifier = first_feed.get("session_id")
    return (
        SearchPageExtraction(
            sources=sources,
            page_information=SearchPageInformation(
                has_next_page=has_next_page,
                end_cursor=end_cursor,
            ),
            feed_session_identifier=(
                feed_session_identifier if isinstance(feed_session_identifier, str) else None
            ),
            listing_occurrences=tuple(occurrences),
            ignored_edges=tuple(ignored),
        ),
        tuple(issues),
    )


def extract_search_bootstrap(
    html: str,
    *,
    acquisition_record_identifier: str,
) -> SearchBootstrapExtraction:
    blocks = parse_json_blocks(html)
    issues = [
        SearchProtocolIssue(
            kind=SearchProtocolIssueKind.EMBEDDED_JSON_PARSE_FAILURE,
            source=_source(
                acquisition_record_identifier,
                (),
                block_index=block["block_index"] if isinstance(block["block_index"], int) else None,
            ),
        )
        for block in blocks
        if block.get("parse_error") is not None or not block.get("closed")
    ]
    configuration, configuration_issues = _extract_applied_configuration(
        blocks,
        acquisition_record_identifier=acquisition_record_identifier,
    )
    issues.extend(configuration_issues)
    values = tuple(
        (block["value"], block["block_index"], None)
        for block in blocks
        if block.get("value") is not None and isinstance(block.get("block_index"), int)
    )
    page, page_issues = _extract_page(
        values,
        acquisition_record_identifier=acquisition_record_identifier,
    )
    issues.extend(page_issues)
    return SearchBootstrapExtraction(
        blocks=blocks,
        applied_configuration=configuration,
        page=page,
        issues=tuple(issues),
    )


def parse_route_definition_frames(body: str) -> tuple[SearchRouteDefinitionFrame, ...]:
    frames: list[SearchRouteDefinitionFrame] = []
    for line_index, raw_line in enumerate(body.splitlines()):
        if not raw_line.strip():
            continue
        frame_index = len(frames)
        prefix_present = raw_line.startswith("for (;;);")
        parse_error: dict[str, JsonValue] | None = None
        value: dict[str, JsonValue] | None = None
        if not prefix_present:
            parse_error = {
                "type": "MissingAntiHijackingPrefix",
                "message": "Route-definition frame is missing the anti-hijacking prefix",
            }
        else:
            try:
                decoded = decode_json(raw_line.removeprefix("for (;;);"))
                if not isinstance(decoded, dict):
                    raise ValueError("Route-definition frame must be a JSON object")
                value = decoded
            except (ValueError, RecursionError) as error:
                parse_error = {"type": type(error).__name__, "message": str(error)}
        frames.append(
            SearchRouteDefinitionFrame(
                frame_index=frame_index,
                line_index=line_index,
                anti_hijacking_prefix_present=prefix_present,
                value=value,
                parse_error=parse_error,
                unparsed_text=raw_line if parse_error is not None else None,
            )
        )
    return tuple(frames)


def extract_search_route_definition(
    body: str,
    *,
    acquisition_record_identifier: str,
) -> SearchRouteDefinitionExtraction:
    frames = parse_route_definition_frames(body)
    issues = [
        SearchProtocolIssue(
            kind=SearchProtocolIssueKind.ROUTE_DEFINITION_FRAME_PARSE_FAILURE,
            source=_source(
                acquisition_record_identifier,
                (),
                block_index=None,
                frame_index=frame.frame_index,
            ),
        )
        for frame in frames
        if frame.parse_error is not None
    ]
    configuration, configuration_issues = _extract_route_applied_configuration(
        frames,
        acquisition_record_identifier=acquisition_record_identifier,
    )
    issues.extend(configuration_issues)
    matched_frames = tuple(
        frame
        for frame in frames
        if frame.value is not None
        and configuration is not None
        and frame.value.get("__type") == "preloader"
        and frame.value.get("id") == configuration.preloader_identifier
    )
    page, page_issues = _extract_page(
        tuple(
            (frame.value, None, frame.frame_index)
            for frame in matched_frames
            if frame.value is not None
        ),
        acquisition_record_identifier=acquisition_record_identifier,
    )
    issues.extend(page_issues)
    return SearchRouteDefinitionExtraction(
        frames=frames,
        applied_configuration=configuration,
        page=page,
        issues=tuple(issues),
    )


def classify_search_route_definition_response(
    body: str,
    *,
    effective_url: str,
    extraction: SearchRouteDefinitionExtraction,
) -> SearchBootstrapResponseClassification:
    folded = body.casefold()
    if extraction.page is not None and extraction.applied_configuration is not None:
        return SearchBootstrapResponseClassification(
            kind=SearchBootstrapResponseKind.SEARCH_RESULTS,
            effective_url=effective_url,
            evidence=("matched_search_preloader", "search_connection", "query_plan"),
        )
    if any(frame.parse_error is not None for frame in extraction.frames):
        kind = SearchBootstrapResponseKind.MALFORMED_RESPONSE
        evidence = ("route_definition_frame_parse_failure",)
    elif any(marker in folded for marker in ("checkpoint", "captcha", "security check")):
        kind = SearchBootstrapResponseKind.BOT_CHALLENGE
        evidence = ("challenge_marker",)
    elif any(marker in folded for marker in ("login", "log in")):
        kind = SearchBootstrapResponseKind.LOGIN_PAGE
        evidence = ("login_marker",)
    elif any(marker in folded for marker in ("something went wrong", "temporarily blocked")):
        kind = SearchBootstrapResponseKind.GENERIC_ERROR_PAGE
        evidence = ("error_marker",)
    else:
        kind = SearchBootstrapResponseKind.SEARCH_PAYLOAD_ABSENT
        evidence = ("matched_search_preloader_or_payload_absent",)
    return SearchBootstrapResponseClassification(
        kind=kind,
        effective_url=effective_url,
        evidence=evidence,
    )


def extract_search_pagination(
    body: str,
    *,
    acquisition_record_identifier: str,
) -> SearchPaginationExtraction:
    text = body.removeprefix("for (;;);")
    try:
        value = decode_json(text)
    except ValueError:
        return SearchPaginationExtraction(
            parsed_response=None,
            graphql_errors=(),
            page=None,
            issues=(
                SearchProtocolIssue(
                    kind=SearchProtocolIssueKind.MALFORMED_PAGINATION_RESPONSE,
                ),
            ),
        )
    if not isinstance(value, dict):
        return SearchPaginationExtraction(
            parsed_response=None,
            graphql_errors=(),
            page=None,
            issues=(
                SearchProtocolIssue(
                    kind=SearchProtocolIssueKind.MALFORMED_PAGINATION_RESPONSE,
                ),
            ),
        )
    errors = value.get("errors")
    graphql_errors = tuple(errors) if isinstance(errors, list) else ()
    if graphql_errors:
        rate_limit_codes = tuple(
            error.get("code")
            for error in graphql_errors
            if isinstance(error, dict) and error.get("code") == 1675004
        )
        return SearchPaginationExtraction(
            parsed_response=value,
            graphql_errors=graphql_errors,
            page=None,
            issues=(
                SearchProtocolIssue(
                    kind=(
                        SearchProtocolIssueKind.GRAPHQL_RATE_LIMITED
                        if rate_limit_codes
                        else SearchProtocolIssueKind.GRAPHQL_ERRORS
                    ),
                    provider_code=rate_limit_codes[0] if rate_limit_codes else None,
                ),
            ),
        )
    page, issues = _extract_page(
        ((value, None, None),),
        acquisition_record_identifier=acquisition_record_identifier,
    )
    return SearchPaginationExtraction(
        parsed_response=value,
        graphql_errors=(),
        page=page,
        issues=issues,
    )
