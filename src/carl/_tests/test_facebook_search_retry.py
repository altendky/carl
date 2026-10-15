"""Challenge retry schedules and durable partition checkpoint invariants."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from carl.core.facebook_search import (
    OverlappingPricePartitionSearchTraversalStrategy,
    PricePartitionSearchTraversalState,
    SearchStoppingReason,
    SearchTraversalPolicy,
    plan_overlapping_price_partitions,
)
from carl.core.facebook_search_retry import (
    FACEBOOK_CHALLENGE_MAXIMUM_ATTEMPTS,
    PartitionSearchRetryCheckpoint,
    facebook_challenge_retry_delay_ns,
)


def _checkpoint(*, completed: int = 1) -> PartitionSearchRetryCheckpoint:
    strategy = OverlappingPricePartitionSearchTraversalStrategy(
        width=Decimal("100"), overlap=Decimal("10")
    )
    partitions = plan_overlapping_price_partitions(
        currency="USD", minimum=None, maximum=Decimal("250"), strategy=strategy
    )
    state = PricePartitionSearchTraversalState(
        search_run_identifier="search-run",
        attempt=1,
        policy=SearchTraversalPolicy(maximum_pages=10),
        strategy=strategy,
        partitions=partitions,
        partitions_processed=completed,
    )
    return PartitionSearchRetryCheckpoint(
        request_sha256="a" * 64,
        traversal_state=state,
        page_references=tuple(
            {
                "page_ordinal": ordinal,
                "listing_occurrence_records": [f"occurrence-{ordinal}"],
                "requested_search": {"query": "telescope"},
                "price_partition": partitions[ordinal - 1].model_dump(mode="json"),
            }
            for ordinal in range(1, completed + 1)
        ),
        challenge_attempts=1,
        challenge_page_ordinal=completed + 1,
    )


def test_challenge_retry_budget_and_delay_schedule() -> None:
    assert FACEBOOK_CHALLENGE_MAXIMUM_ATTEMPTS == 5
    minute_ns = 60_000_000_000
    assert [facebook_challenge_retry_delay_ns(attempt) // minute_ns for attempt in range(1, 6)] == [
        10,
        20,
        40,
        60,
        60,
    ]
    assert facebook_challenge_retry_delay_ns(100_000) == 60 * minute_ns


@pytest.mark.parametrize("attempt", [0, -1])
def test_challenge_delay_rejects_invalid_attempt(attempt: int) -> None:
    with pytest.raises(ValueError, match="positive"):
        facebook_challenge_retry_delay_ns(attempt)


@pytest.mark.parametrize("completed", [0, 1, 2])
def test_checkpoint_round_trips_compact_json_with_completed_pages(completed: int) -> None:
    checkpoint = _checkpoint(completed=completed)
    encoded_json = checkpoint.model_dump_json()
    assert "\n" not in encoded_json
    restored = PartitionSearchRetryCheckpoint.model_validate_json(encoded_json)
    assert restored == checkpoint
    assert restored.challenge_page_ordinal == completed + 1
    assert restored.retry_budget_start_attempt == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("version", 2),
        ("request_sha256", "not-a-hash"),
        ("request_sha256", "g" * 64),
        ("challenge_attempts", -1),
        ("challenge_page_ordinal", 0),
        ("challenge_page_ordinal", 3),
        ("retry_budget_start_attempt", -1),
        ("page_references", ()),
    ],
)
def test_checkpoint_rejects_invalid_resume_fields(field: str, value: object) -> None:
    data = _checkpoint().model_dump()
    data[field] = value
    with pytest.raises(ValidationError):
        PartitionSearchRetryCheckpoint.model_validate(data)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("page_ordinal", 2),
        ("page_ordinal", True),
        ("listing_occurrence_records", "occurrence-1"),
        ("listing_occurrence_records", [" "]),
        ("listing_occurrence_records", [123]),
        ("requested_search", None),
        ("price_partition", {}),
    ],
)
def test_checkpoint_rejects_invalid_completed_page_references(field: str, value: object) -> None:
    data = _checkpoint().model_dump()
    data["page_references"][0][field] = value
    with pytest.raises(ValidationError):
        PartitionSearchRetryCheckpoint.model_validate(data)


def test_checkpoint_rejects_stopped_or_fully_processed_traversal() -> None:
    checkpoint = _checkpoint()
    for state in (
        checkpoint.traversal_state.model_copy(
            update={"stopping_reason": SearchStoppingReason.MAXIMUM_PAGES}
        ),
        checkpoint.traversal_state.model_copy(update={"partitions_processed": 3}),
    ):
        with pytest.raises(ValidationError, match="incomplete, unstopped"):
            PartitionSearchRetryCheckpoint.model_validate(
                {**checkpoint.model_dump(), "traversal_state": state}
            )
