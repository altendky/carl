import pytest
from pydantic import ValidationError

from carl.core.facebook import FacebookItemResponseKind
from carl.core.facebook_work import (
    SearchRunListingCandidates,
    SuccessfulItemPageResult,
    plan_item_page_followups,
)
from carl.facebook_workers import PLAN_FACEBOOK_ITEM_PAGE_FOLLOWUPS, build_component_registry


def _result(
    listing_id: str,
    *,
    acquisition_sequence: int,
    extraction_sequence: int,
    classification: FacebookItemResponseKind = FacebookItemResponseKind.FULL_LISTING,
) -> SuccessfulItemPageResult:
    return SuccessfulItemPageResult(
        listing_id=listing_id,
        acquisition_record_identifier=f"acquisition-{listing_id}-{acquisition_sequence}",
        observation_record_identifier=(
            f"observation-{listing_id}-{acquisition_sequence}-{extraction_sequence}"
        ),
        response_classification=classification,
        acquisition_completion_sequence=acquisition_sequence,
        extraction_completion_sequence=extraction_sequence,
    )


def test_followup_plan_unions_searches_and_selects_latest_success() -> None:
    older = _result("1", acquisition_sequence=10, extraction_sequence=11)
    newer_extraction = _result("1", acquisition_sequence=20, extraction_sequence=21)
    latest = _result("1", acquisition_sequence=20, extraction_sequence=22)
    unavailable = _result(
        "2",
        acquisition_sequence=30,
        extraction_sequence=31,
        classification=FacebookItemResponseKind.LISTING_UNAVAILABLE,
    )

    plan = plan_item_page_followups(
        (
            SearchRunListingCandidates(
                search_run_record_identifier="search-a",
                listing_identifiers=("1", "2"),
            ),
            SearchRunListingCandidates(
                search_run_record_identifier="search-b",
                listing_identifiers=("2", "3"),
            ),
        ),
        (latest, older, unavailable, newer_extraction),
    )

    assert plan.listing_references == 4
    assert plan.unique_listings == 3
    assert tuple(decision.listing_id for decision in plan.decisions) == ("1", "2", "3")
    assert plan.decisions[0].search_run_record_identifiers == ("search-a",)
    assert plan.decisions[0].latest_successful_result == latest
    assert plan.decisions[1].search_run_record_identifiers == ("search-a", "search-b")
    assert plan.decisions[1].latest_successful_result == unavailable
    assert plan.decisions[2].search_run_record_identifiers == ("search-b",)
    assert plan.decisions[2].latest_successful_result is None


def test_followup_limit_applies_after_exact_identifier_union() -> None:
    plan = plan_item_page_followups(
        (
            SearchRunListingCandidates(
                search_run_record_identifier="search-a",
                listing_identifiers=("1", "2"),
            ),
            SearchRunListingCandidates(
                search_run_record_identifier="search-b",
                listing_identifiers=("2", "3"),
            ),
        ),
        (),
        maximum_items=2,
    )

    assert plan.listing_references == 4
    assert plan.unique_listings == 3
    assert tuple(decision.listing_id for decision in plan.decisions) == ("1", "2")


def test_login_page_is_not_a_successful_item_page_result() -> None:
    with pytest.raises(ValidationError, match="semantically usable"):
        _ = _result(
            "1",
            acquisition_sequence=1,
            extraction_sequence=2,
            classification=FacebookItemResponseKind.LOGIN_PAGE,
        )


def test_followup_planner_has_a_registered_structured_code_identity() -> None:
    component = build_component_registry().require(PLAN_FACEBOOK_ITEM_PAGE_FOLLOWUPS)

    assert component.implementation is plan_item_page_followups
