from decimal import Decimal

import pytest
from pydantic import ValidationError

from carl.core.facebook_search import (
    OverlappingPricePartitionSearchTraversalStrategy,
    PricePartitionSearchTraversalState,
    SearchPageFacts,
    SearchPricePartitionOrder,
    SearchStoppingReason,
    SearchTraversalAction,
    SearchTraversalPolicy,
    SearchTraversalState,
    account_search_elapsed_interval,
    advance_price_partition_search_traversal,
    advance_search_traversal,
    plan_overlapping_price_partitions,
)


def _page(
    *,
    listing_identifiers: tuple[str, ...] = ("1", "2"),
    has_next_page: bool = True,
    end_cursor: str | None = "cursor-1",
    interval_elapsed_duration_ns: int = 10,
    transferred_bytes: int | None = 100,
    decoded_body_bytes: int = 500,
) -> SearchPageFacts:
    return SearchPageFacts(
        listing_identifiers=listing_identifiers,
        has_next_page=has_next_page,
        end_cursor=end_cursor,
        interval_elapsed_duration_ns=interval_elapsed_duration_ns,
        transferred_bytes=transferred_bytes,
        decoded_body_bytes=decoded_body_bytes,
    )


def _state(policy: SearchTraversalPolicy) -> SearchTraversalState:
    return SearchTraversalState(
        search_run_identifier="search-run",
        attempt=1,
        policy=policy,
    )


def test_page_advances_and_retains_observations_separately_from_unique_ids() -> None:
    policy = SearchTraversalPolicy(maximum_pages=3)
    first = advance_search_traversal(
        state=_state(policy),
        page=_page(listing_identifiers=("1", "1", "2")),
    )

    assert first.action is SearchTraversalAction.CONTINUE
    assert first.next_cursor == "cursor-1"
    assert first.new_listing_identifiers == ("1", "2")
    assert first.state.listing_observations == 3
    assert first.state.unique_listing_identifiers == ("1", "2")

    second = advance_search_traversal(
        state=first.state,
        page=_page(
            listing_identifiers=("2", "3"),
            end_cursor="cursor-2",
            interval_elapsed_duration_ns=15,
        ),
    )

    assert second.action is SearchTraversalAction.CONTINUE
    assert second.new_listing_identifiers == ("3",)
    assert second.state.listing_observations == 5
    assert second.state.unique_listing_identifiers == ("1", "2", "3")
    assert second.state.transferred_bytes == 200
    assert second.state.decoded_body_bytes == 1_000
    assert second.state.elapsed_duration_ns == 25


def test_overlapping_price_partition_plan_is_clipped_and_deterministic() -> None:
    strategy = OverlappingPricePartitionSearchTraversalStrategy(
        width=Decimal("100"),
        overlap=Decimal("10"),
        order=SearchPricePartitionOrder.ASCENDING,
    )

    partitions = plan_overlapping_price_partitions(
        currency="USD",
        minimum=None,
        maximum=Decimal("250"),
        strategy=strategy,
    )

    assert [
        (partition.ordinal, partition.minimum, partition.maximum) for partition in partitions
    ] == [
        (1, Decimal("0"), Decimal("100")),
        (2, Decimal("90"), Decimal("190")),
        (3, Decimal("180"), Decimal("250")),
    ]


def test_price_partition_strategy_accepts_json_decimal_scalars() -> None:
    strategy = OverlappingPricePartitionSearchTraversalStrategy.model_validate_json(
        '{"width": 10, "overlap": "2", "order": "balanced"}'
    )

    assert strategy.width == Decimal("10")
    assert strategy.overlap == Decimal("2")


def test_balanced_price_partition_order_spreads_bounded_prefixes() -> None:
    partitions = plan_overlapping_price_partitions(
        currency="USD",
        minimum=Decimal("0"),
        maximum=Decimal("600"),
        strategy=OverlappingPricePartitionSearchTraversalStrategy(
            width=Decimal("100"),
            overlap=Decimal("5"),
        ),
    )

    assert [(partition.minimum, partition.maximum) for partition in partitions] == [
        (Decimal("285"), Decimal("385")),
        (Decimal("95"), Decimal("195")),
        (Decimal("475"), Decimal("575")),
        (Decimal("0"), Decimal("100")),
        (Decimal("190"), Decimal("290")),
        (Decimal("380"), Decimal("480")),
        (Decimal("570"), Decimal("600")),
    ]


def test_price_partition_traversal_preserves_overlapping_occurrences() -> None:
    strategy = OverlappingPricePartitionSearchTraversalStrategy(
        width=Decimal("100"),
        overlap=Decimal("10"),
    )
    partitions = plan_overlapping_price_partitions(
        currency="USD",
        minimum=Decimal("0"),
        maximum=Decimal("190"),
        strategy=strategy,
    )
    state = PricePartitionSearchTraversalState(
        search_run_identifier="search-run",
        attempt=1,
        policy=SearchTraversalPolicy(maximum_pages=5),
        strategy=strategy,
        partitions=partitions,
    )

    first = advance_price_partition_search_traversal(
        state=state,
        partition=partitions[0],
        page=_page(listing_identifiers=("1", "2"), has_next_page=True),
    )
    second = advance_price_partition_search_traversal(
        state=first.state,
        partition=partitions[1],
        page=_page(
            listing_identifiers=("2", "3"),
            has_next_page=False,
            end_cursor=None,
        ),
    )

    assert first.next_partition == partitions[1]
    assert second.state.listing_observations == 4
    assert second.state.unique_listing_identifiers == ("1", "2", "3")
    assert second.state.saturated_partition_ordinals == (1,)
    assert second.state.stopping_reason is SearchStoppingReason.PRICE_PARTITIONS_EXHAUSTED


def test_price_partition_strategy_rejects_nonprogressing_overlap() -> None:
    with pytest.raises(ValidationError, match="smaller than"):
        _ = OverlappingPricePartitionSearchTraversalStrategy(
            width=Decimal("10"),
            overlap=Decimal("10"),
        )


@pytest.mark.parametrize(
    ("policy", "page", "reason"),
    (
        (
            SearchTraversalPolicy(maximum_pages=1),
            _page(),
            SearchStoppingReason.MAXIMUM_PAGES,
        ),
        (
            SearchTraversalPolicy(maximum_results=2),
            _page(),
            SearchStoppingReason.MAXIMUM_RESULTS,
        ),
        (
            SearchTraversalPolicy(maximum_pages=2, maximum_elapsed_duration_ns=10),
            _page(),
            SearchStoppingReason.MAXIMUM_ELAPSED_DURATION,
        ),
        (
            SearchTraversalPolicy(maximum_pages=2, maximum_transferred_bytes=100),
            _page(),
            SearchStoppingReason.MAXIMUM_TRANSFERRED_BYTES,
        ),
        (
            SearchTraversalPolicy(maximum_pages=2, maximum_transferred_bytes=100),
            _page(transferred_bytes=None),
            SearchStoppingReason.TRANSFERRED_BYTES_UNAVAILABLE,
        ),
        (
            SearchTraversalPolicy(maximum_pages=2, maximum_decoded_body_bytes=500),
            _page(),
            SearchStoppingReason.MAXIMUM_DECODED_BODY_BYTES,
        ),
        (
            SearchTraversalPolicy(
                maximum_pages=2,
                maximum_consecutive_pages_without_new_listings=1,
            ),
            _page(listing_identifiers=()),
            SearchStoppingReason.MAXIMUM_CONSECUTIVE_PAGES_WITHOUT_NEW_LISTINGS,
        ),
    ),
)
def test_configured_bound_stops_after_complete_page(
    policy: SearchTraversalPolicy,
    page: SearchPageFacts,
    reason: SearchStoppingReason,
) -> None:
    decision = advance_search_traversal(
        state=_state(policy),
        page=page,
    )

    assert decision.action is SearchTraversalAction.STOP
    assert decision.next_cursor is None
    assert decision.state.stopping_reason is reason
    assert decision.state.pages_processed == 1


def test_source_exhaustion_takes_precedence_over_configured_bound() -> None:
    policy = SearchTraversalPolicy(maximum_pages=1)
    decision = advance_search_traversal(
        state=_state(policy),
        page=_page(has_next_page=False, end_cursor=None),
    )

    assert decision.state.stopping_reason is SearchStoppingReason.NO_NEXT_PAGE


def test_missing_cursor_stops_when_source_claims_another_page() -> None:
    policy = SearchTraversalPolicy(maximum_pages=2)
    decision = advance_search_traversal(
        state=_state(policy),
        page=_page(end_cursor=None),
    )

    assert decision.state.stopping_reason is SearchStoppingReason.MISSING_CURSOR


def test_repeated_cursor_stops_before_following_it_again() -> None:
    policy = SearchTraversalPolicy(maximum_pages=3)
    first = advance_search_traversal(
        state=_state(policy),
        page=_page(),
    )
    repeated = advance_search_traversal(
        state=first.state,
        page=_page(end_cursor="cursor-1", interval_elapsed_duration_ns=20),
    )

    assert repeated.state.stopping_reason is SearchStoppingReason.REPEATED_CURSOR
    assert repeated.state.seen_end_cursors == ("cursor-1",)


def test_no_new_listing_streak_resets_after_a_new_listing() -> None:
    policy = SearchTraversalPolicy(
        maximum_pages=4,
        maximum_consecutive_pages_without_new_listings=2,
    )
    first = advance_search_traversal(
        state=_state(policy),
        page=_page(listing_identifiers=(), end_cursor="cursor-1"),
    )
    second = advance_search_traversal(
        state=first.state,
        page=_page(
            listing_identifiers=("1",),
            end_cursor="cursor-2",
            interval_elapsed_duration_ns=20,
        ),
    )

    assert first.state.consecutive_pages_without_new_listings == 1
    assert second.state.consecutive_pages_without_new_listings == 0
    assert second.action is SearchTraversalAction.CONTINUE


def test_stopped_traversal_cannot_advance() -> None:
    policy = SearchTraversalPolicy(maximum_pages=1)
    stopped = advance_search_traversal(
        state=_state(policy),
        page=_page(),
    )
    with pytest.raises(ValueError, match="cannot be advanced"):
        _ = advance_search_traversal(state=stopped.state, page=_page())


def test_configured_bound_takes_precedence_when_no_cursor_will_be_followed() -> None:
    policy = SearchTraversalPolicy(maximum_pages=1)
    decision = advance_search_traversal(
        state=_state(policy),
        page=_page(end_cursor=None),
    )

    assert decision.state.stopping_reason is SearchStoppingReason.MAXIMUM_PAGES


def test_elapsed_duration_accumulates_across_page_intervals() -> None:
    policy = SearchTraversalPolicy(maximum_pages=3, maximum_elapsed_duration_ns=10)
    first = advance_search_traversal(
        state=_state(policy),
        page=_page(interval_elapsed_duration_ns=6),
    )
    second = advance_search_traversal(
        state=first.state,
        page=_page(end_cursor="cursor-2", interval_elapsed_duration_ns=6),
    )

    assert first.action is SearchTraversalAction.CONTINUE
    assert second.state.elapsed_duration_ns == 12
    assert second.state.stopping_reason is SearchStoppingReason.MAXIMUM_ELAPSED_DURATION


def test_elapsed_time_between_pages_is_accounted_before_another_request() -> None:
    policy = SearchTraversalPolicy(maximum_pages=3, maximum_elapsed_duration_ns=10)
    first = advance_search_traversal(
        state=_state(policy),
        page=_page(interval_elapsed_duration_ns=6),
    )

    stopped = account_search_elapsed_interval(
        first.state,
        interval_elapsed_duration_ns=4,
    )

    assert stopped.elapsed_duration_ns == 10
    assert stopped.stopping_reason is SearchStoppingReason.MAXIMUM_ELAPSED_DURATION


def test_elapsed_time_between_pages_accumulates_without_a_deadline() -> None:
    first = advance_search_traversal(
        state=_state(SearchTraversalPolicy(maximum_pages=3)),
        page=_page(interval_elapsed_duration_ns=6),
    )

    continued = account_search_elapsed_interval(
        first.state,
        interval_elapsed_duration_ns=4,
    )

    assert continued.elapsed_duration_ns == 10
    assert continued.stopping_reason is None


def test_traversal_state_round_trips_with_run_attempt_and_policy() -> None:
    policy = SearchTraversalPolicy(maximum_pages=3, maximum_results=10)
    decision = advance_search_traversal(state=_state(policy), page=_page())

    restored = SearchTraversalState.model_validate_json(decision.state.model_dump_json())

    assert restored == decision.state
    assert restored.search_run_identifier == "search-run"
    assert restored.attempt == 1
    assert restored.policy == policy


def test_traversal_models_reject_ambiguous_or_invalid_values() -> None:
    with pytest.raises(ValidationError, match="stopping bound"):
        _ = SearchTraversalPolicy()
    with pytest.raises(ValidationError):
        _ = SearchTraversalPolicy(maximum_pages=0)
    with pytest.raises(ValidationError):
        _ = SearchTraversalPolicy(maximum_pages=True)
    with pytest.raises(ValidationError):
        _ = SearchPageFacts(
            listing_identifiers=("not-numeric",),
            has_next_page=True,
            end_cursor="cursor",
            interval_elapsed_duration_ns=0,
            decoded_body_bytes=0,
        )
    with pytest.raises(ValidationError):
        _ = SearchTraversalState(
            search_run_identifier="search-run",
            attempt=1,
            policy=SearchTraversalPolicy(maximum_pages=2),
            pages_processed=1,
            listing_observations=1,
            unique_listing_identifiers=("1", "1"),
        )
    with pytest.raises(ValidationError):
        _ = SearchPageFacts(
            listing_identifiers=("\N{ARABIC-INDIC DIGIT ONE}",),
            has_next_page=True,
            end_cursor="cursor",
            interval_elapsed_duration_ns=0,
            decoded_body_bytes=0,
        )
    with pytest.raises(ValidationError):
        _ = SearchTraversalState(
            search_run_identifier="search-run",
            attempt=1,
            policy=SearchTraversalPolicy(maximum_pages=2),
            pages_processed=1,
            transferred_bytes=-1,
        )
