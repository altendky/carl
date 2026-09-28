import json

import pytest

from carl.core.facebook import parse_json_blocks
from carl.core.facebook_search_protocol import (
    SearchBootstrapResponseKind,
    SearchProtocolIssue,
    SearchProtocolIssueKind,
    SearchSessionBootstrapResponseKind,
    classify_search_bootstrap_response,
    classify_search_session_bootstrap_response,
    extract_search_bootstrap,
    extract_search_pagination,
    extract_search_route_definition,
    parse_route_definition_frames,
    traversal_page_facts,
)
from carl.facebook_workers import (
    EXTRACT_FACEBOOK_JSON_BLOCKS,
    EXTRACT_FACEBOOK_SEARCH_BOOTSTRAP,
    EXTRACT_FACEBOOK_SEARCH_PAGINATION,
    EXTRACT_FACEBOOK_SEARCH_ROUTE_DEFINITION,
    build_component_registry,
)


def test_session_bootstrap_prefers_marketplace_landing_over_login_modal() -> None:
    classification = classify_search_session_bootstrap_response(
        '<html><form id="login_form">Log in</form><h1>Marketplace</h1></html>',
        effective_url="https://www.facebook.com/marketplace/",
    )

    assert classification.kind is SearchSessionBootstrapResponseKind.MARKETPLACE_LANDING


def test_session_bootstrap_detects_login_redirect() -> None:
    classification = classify_search_session_bootstrap_response(
        '<html><form id="login_form"></form></html>',
        effective_url="https://www.facebook.com/login/?next=marketplace",
    )

    assert classification.kind is SearchSessionBootstrapResponseKind.LOGIN_PAGE


def _feed(*, cursor: str | None = "next", has_next_page: bool = True) -> dict[str, object]:
    return {
        "session_id": "feed-session",
        "page_info": {"has_next_page": has_next_page, "end_cursor": cursor},
        "edges": [
            {
                "cursor": "edge-one",
                "node": {
                    "id": "story-not-listing-id",
                    "listing": {
                        "id": "123",
                        "marketplace_listing_title": "Telescope",
                        "listing_price": {"amount": "100", "currency": "USD"},
                    },
                },
            },
            {
                "cursor": "edge-two",
                "node": {
                    "id": "another-story",
                    "listing": {
                        "id": "123",
                        "custom_title": "Repeated telescope observation",
                        "is_partnership": True,
                    },
                },
            },
            {"node": {"advertisement": {"id": "advertisement"}}},
        ],
    }


def _bootstrap_html(*, feed: dict[str, object] | None = None) -> str:
    query = {
        "require": [
            {
                "expectedPreloaders": [
                    {
                        "queryName": "CometMarketplaceSearchContentContainerQuery",
                        "queryID": "987654321",
                        "variables": {
                            "count": 24,
                            "cursor": None,
                            "savedSearchQuery": "telescope",
                            "buyLocation": {"latitude": 1.0, "longitude": 2.0},
                            "topicPageParams": {"location_id": "456"},
                            "params": {
                                "browse_request_params": {
                                    "filter_radius_km": 97,
                                    "filter_price_upper_bound": 60000,
                                }
                            },
                        },
                    }
                ]
            }
        ]
    }
    response = {
        "decoy": {"page_info": {"has_next_page": False, "end_cursor": None}},
        "result": {"data": {"marketplace_search": {"feed_units": feed or _feed()}}},
    }
    return "".join(
        (
            '<script type="application/json">',
            json.dumps(query),
            "</script>",
            '<script type="application/json">',
            json.dumps(response),
            "</script>",
        )
    )


def _route_definition_response(*, preloader_identifier: str = "search-preloader") -> str:
    variables = {
        "count": 24,
        "cursor": None,
        "savedSearchQuery": "telescope",
        "buyLocation": {"latitude": 1.0, "longitude": 2.0},
        "topicPageParams": {"location_id": "456"},
        "params": {
            "browse_request_params": {
                "filter_radius_km": 97,
                "filter_price_upper_bound": 60000,
            }
        },
    }
    frames = (
        {
            "__type": "first_response",
            "preloaders": [
                {
                    "preloaderID": preloader_identifier,
                    "queryName": "CometMarketplaceSearchContentContainerQuery",
                    "queryID": "987654321",
                    "variables": variables,
                }
            ],
        },
        {
            "__type": "preloader",
            "id": "search-preloader",
            "result": {"result": {"data": {"marketplace_search": {"feed_units": _feed()}}}},
        },
        {"__type": "last_response"},
    )
    return "\n".join(f"for (;;);{json.dumps(frame)}" for frame in frames)


def test_bootstrap_selects_structural_search_connection_and_listing_identity() -> None:
    extraction = extract_search_bootstrap(
        _bootstrap_html(),
        acquisition_record_identifier="acquisition-1",
    )

    assert extraction.issues == ()
    assert extraction.applied_configuration is not None
    assert extraction.applied_configuration.query_identifier == "987654321"
    assert extraction.applied_configuration.browse_request_parameters == {
        "filter_radius_km": 97,
        "filter_price_upper_bound": 60000,
    }
    assert extraction.page is not None
    assert extraction.page.page_information.end_cursor == "next"
    assert [item.listing_identifier for item in extraction.page.listing_occurrences] == [
        "123",
        "123",
    ]
    assert extraction.page.listing_occurrences[0].story_identifier == "story-not-listing-id"
    assert extraction.page.listing_occurrences[0].source.acquisition_record_identifier == (
        "acquisition-1"
    )
    assert extraction.page.listing_occurrences[1].original["is_partnership"] is True
    assert len(extraction.page.ignored_edges) == 1

    facts = traversal_page_facts(
        extraction.page,
        interval_elapsed_duration_ns=50,
        transferred_bytes=1_000,
        decoded_body_bytes=5_000,
    )
    assert facts.listing_identifiers == ("123", "123")
    assert facts.end_cursor == "next"


def test_bootstrap_retains_malformed_blocks_and_missing_cursor_issue() -> None:
    html = _bootstrap_html(feed=_feed(cursor=None))
    html += '<script type="application/json">{"broken":</script>'

    extraction = extract_search_bootstrap(html, acquisition_record_identifier="acquisition-1")

    assert extraction.page is not None
    assert {issue.kind for issue in extraction.issues} == {
        SearchProtocolIssueKind.EMBEDDED_JSON_PARSE_FAILURE,
        SearchProtocolIssueKind.MISSING_NEXT_CURSOR,
    }
    assert extraction.blocks[-1]["value"] is None
    assert extraction.blocks[-1]["parse_error"] is not None


def test_divergent_search_connections_are_an_extraction_failure() -> None:
    html = _bootstrap_html()
    conflicting = {
        "data": {
            "marketplace_search": {
                "feed_units": _feed(cursor="different-cursor"),
            }
        }
    }
    html += '<script type="application/json">' + json.dumps(conflicting) + "</script>"

    extraction = extract_search_bootstrap(html, acquisition_record_identifier="acquisition-1")

    assert extraction.page is None
    assert SearchProtocolIssueKind.AMBIGUOUS_SEARCH_CONNECTION in {
        issue.kind for issue in extraction.issues
    }


@pytest.mark.parametrize(
    ("html", "effective_url", "kind"),
    (
        (
            '<html><form id="login_form"></form></html>',
            "https://www.facebook.com/login/?next=marketplace",
            SearchBootstrapResponseKind.LOGIN_PAGE,
        ),
        (
            '<html><form id="checkpointSubmitButton"></form></html>',
            "https://www.facebook.com/checkpoint/",
            SearchBootstrapResponseKind.BOT_CHALLENGE,
        ),
        (
            "<html>Something went wrong</html>",
            "https://www.facebook.com/marketplace/search/",
            SearchBootstrapResponseKind.GENERIC_ERROR_PAGE,
        ),
    ),
)
def test_bootstrap_response_classifies_non_search_pages(
    html: str,
    effective_url: str,
    kind: SearchBootstrapResponseKind,
) -> None:
    extraction = extract_search_bootstrap(
        html,
        acquisition_record_identifier="acquisition-1",
    )

    classification = classify_search_bootstrap_response(
        html,
        effective_url=effective_url,
        extraction=extraction,
    )

    assert classification.kind is kind


def test_bootstrap_response_classifies_structured_search_results() -> None:
    html = _bootstrap_html()
    extraction = extract_search_bootstrap(html, acquisition_record_identifier="acquisition-1")

    classification = classify_search_bootstrap_response(
        html,
        effective_url="https://www.facebook.com/marketplace/456/search/?query=telescope",
        extraction=extraction,
    )

    assert classification.kind is SearchBootstrapResponseKind.SEARCH_RESULTS


def test_pagination_accepts_anti_hijacking_prefix_and_preserves_native_json() -> None:
    response = {"data": {"marketplace_search": {"feed_units": _feed()}}}

    extraction = extract_search_pagination(
        "for (;;);" + json.dumps(response),
        acquisition_record_identifier="acquisition-2",
    )

    assert extraction.parsed_response == response
    assert extraction.page is not None
    assert extraction.page.sources[0].block_index is None
    assert extraction.page.listing_occurrences[0].listing_identifier == "123"


def test_route_definition_extracts_correlated_preloader_and_frame_provenance() -> None:
    extraction = extract_search_route_definition(
        _route_definition_response(),
        acquisition_record_identifier="acquisition-route",
    )

    assert extraction.issues == ()
    assert extraction.applied_configuration is not None
    assert extraction.applied_configuration.query_identifier == "987654321"
    assert extraction.applied_configuration.preloader_identifier == "search-preloader"
    assert extraction.applied_configuration.sources[0].frame_index == 0
    assert extraction.page is not None
    assert extraction.page.sources[0].frame_index == 1
    assert extraction.page.page_information.end_cursor == "next"
    assert [item.listing_identifier for item in extraction.page.listing_occurrences] == [
        "123",
        "123",
    ]
    assert [frame.value["__type"] for frame in extraction.frames if frame.value is not None] == [
        "first_response",
        "preloader",
        "last_response",
    ]


def test_route_definition_retains_malformed_frames_for_offline_analysis() -> None:
    frames = parse_route_definition_frames(_route_definition_response() + "\nfor (;;);{malformed")
    extraction = extract_search_route_definition(
        _route_definition_response() + "\nfor (;;);{malformed",
        acquisition_record_identifier="acquisition-route",
    )

    assert frames[-1].value is None
    assert frames[-1].unparsed_text == "for (;;);{malformed"
    assert frames[-1].parse_error is not None
    assert SearchProtocolIssueKind.ROUTE_DEFINITION_FRAME_PARSE_FAILURE in {
        issue.kind for issue in extraction.issues
    }


def test_route_definition_rejects_unmatched_preloader_result() -> None:
    extraction = extract_search_route_definition(
        _route_definition_response(preloader_identifier="missing-preloader"),
        acquisition_record_identifier="acquisition-route",
    )

    assert extraction.applied_configuration is not None
    assert extraction.page is None
    assert SearchProtocolIssueKind.MISSING_SEARCH_CONNECTION in {
        issue.kind for issue in extraction.issues
    }


def test_pagination_surfaces_graphql_errors_before_connection_extraction() -> None:
    extraction = extract_search_pagination(
        json.dumps({"errors": [{"message": "Persisted query is unavailable"}]}),
        acquisition_record_identifier="acquisition-2",
    )

    assert extraction.page is None
    assert extraction.graphql_errors == ({"message": "Persisted query is unavailable"},)
    assert [issue.kind for issue in extraction.issues] == [SearchProtocolIssueKind.GRAPHQL_ERRORS]


def test_pagination_classifies_facebook_rate_limit_error() -> None:
    extraction = extract_search_pagination(
        json.dumps(
            {
                "errors": [
                    {
                        "message": "Rate limit exceeded",
                        "severity": "CRITICAL",
                        "code": 1675004,
                    }
                ]
            }
        ),
        acquisition_record_identifier="acquisition-rate-limited",
    )

    assert extraction.page is None
    assert extraction.graphql_errors[0]["code"] == 1675004
    assert extraction.issues == (
        SearchProtocolIssue(
            kind=SearchProtocolIssueKind.GRAPHQL_RATE_LIMITED,
            provider_code=1675004,
        ),
    )


def test_malformed_pagination_response_is_findable() -> None:
    extraction = extract_search_pagination(
        "not JSON",
        acquisition_record_identifier="acquisition-2",
    )

    assert extraction.parsed_response is None
    assert extraction.page is None
    assert [issue.kind for issue in extraction.issues] == [
        SearchProtocolIssueKind.MALFORMED_PAGINATION_RESPONSE
    ]


def test_search_extractors_have_registered_structured_code_identities() -> None:
    registry = build_component_registry()

    assert registry.require(EXTRACT_FACEBOOK_JSON_BLOCKS).implementation is parse_json_blocks
    assert registry.require(EXTRACT_FACEBOOK_SEARCH_BOOTSTRAP).implementation is (
        extract_search_bootstrap
    )
    assert registry.require(EXTRACT_FACEBOOK_SEARCH_ROUTE_DEFINITION).implementation is (
        extract_search_route_definition
    )
    assert registry.require(EXTRACT_FACEBOOK_SEARCH_PAGINATION).implementation is (
        extract_search_pagination
    )
