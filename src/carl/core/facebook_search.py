"""Pure Facebook Marketplace search traversal decisions."""

from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from carl.core.models import JsonStringEnumeration, StrictModel


class SearchTraversalAction(JsonStringEnumeration):
    CONTINUE = "continue"
    STOP = "stop"


class SearchTraversalStrategyKind(JsonStringEnumeration):
    CURSOR = "cursor"
    OVERLAPPING_PRICE_PARTITIONS = "overlapping_price_partitions"


class SearchPricePartitionOrder(JsonStringEnumeration):
    BALANCED = "balanced"
    ASCENDING = "ascending"


class CursorSearchTraversalStrategy(StrictModel):
    kind: Literal[SearchTraversalStrategyKind.CURSOR] = SearchTraversalStrategyKind.CURSOR


class OverlappingPricePartitionSearchTraversalStrategy(StrictModel):
    kind: Literal[SearchTraversalStrategyKind.OVERLAPPING_PRICE_PARTITIONS] = (
        SearchTraversalStrategyKind.OVERLAPPING_PRICE_PARTITIONS
    )
    width: Decimal = Field(gt=0)
    overlap: Decimal = Field(default=Decimal(0), ge=0)
    order: SearchPricePartitionOrder = SearchPricePartitionOrder.BALANCED

    @field_validator("width", "overlap", mode="before")
    @classmethod
    def parse_json_decimal(cls, value: object) -> object:
        if isinstance(value, (Decimal, bool)):
            return value
        if isinstance(value, str) and (not value or value != value.strip()):
            raise ValueError("Price partition values must be trimmed decimals")
        if isinstance(value, (str, int, float)):
            try:
                parsed = Decimal(str(value))
            except InvalidOperation as error:
                raise ValueError("Price partition values must be decimals") from error
            if not parsed.is_finite():
                raise ValueError("Price partition values must be finite")
            return parsed
        return value

    @model_validator(mode="after")
    def validate_interval_geometry(self) -> "OverlappingPricePartitionSearchTraversalStrategy":
        if not self.width.is_finite() or not self.overlap.is_finite():
            raise ValueError("Price partition values must be finite")
        if self.overlap >= self.width:
            raise ValueError("Price partition overlap must be smaller than its width")
        return self


type SearchTraversalStrategy = Annotated[
    CursorSearchTraversalStrategy | OverlappingPricePartitionSearchTraversalStrategy,
    Field(discriminator="kind"),
]


class SearchStoppingReason(JsonStringEnumeration):
    NO_NEXT_PAGE = "no_next_page"
    MISSING_CURSOR = "missing_cursor"
    REPEATED_CURSOR = "repeated_cursor"
    MAXIMUM_PAGES = "maximum_pages"
    MAXIMUM_RESULTS = "maximum_results"
    MAXIMUM_ELAPSED_DURATION = "maximum_elapsed_duration"
    MAXIMUM_TRANSFERRED_BYTES = "maximum_transferred_bytes"
    TRANSFERRED_BYTES_UNAVAILABLE = "transferred_bytes_unavailable"
    MAXIMUM_DECODED_BODY_BYTES = "maximum_decoded_body_bytes"
    MAXIMUM_CONSECUTIVE_PAGES_WITHOUT_NEW_LISTINGS = (
        "maximum_consecutive_pages_without_new_listings"
    )
    PRICE_PARTITIONS_EXHAUSTED = "price_partitions_exhausted"


class SearchPricePartition(StrictModel):
    ordinal: int = Field(ge=1)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    minimum: Decimal = Field(ge=0)
    maximum: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def validate_bounds(self) -> "SearchPricePartition":
        if not self.minimum.is_finite() or not self.maximum.is_finite():
            raise ValueError("Price partition bounds must be finite")
        if self.minimum > self.maximum:
            raise ValueError("Price partition minimum cannot exceed its maximum")
        return self


def plan_overlapping_price_partitions(
    *,
    currency: str,
    minimum: Decimal | None,
    maximum: Decimal,
    strategy: OverlappingPricePartitionSearchTraversalStrategy,
) -> tuple[SearchPricePartition, ...]:
    """Plan deterministic, clipped price intervals without string identities."""

    lower = Decimal(0) if minimum is None else minimum
    if not lower.is_finite() or not maximum.is_finite():
        raise ValueError("Price partition bounds must be finite")
    if lower < 0 or maximum < lower:
        raise ValueError("Price partition bounds are invalid")
    if lower == maximum:
        return (
            SearchPricePartition(
                ordinal=1,
                currency=currency,
                minimum=lower,
                maximum=maximum,
            ),
        )

    step = strategy.width - strategy.overlap
    bounds: list[tuple[Decimal, Decimal]] = []
    partition_minimum = lower
    while True:
        partition_maximum = min(partition_minimum + strategy.width, maximum)
        bounds.append((partition_minimum, partition_maximum))
        if partition_maximum == maximum:
            break
        partition_minimum += step
    if strategy.order is SearchPricePartitionOrder.ASCENDING:
        indexes = tuple(range(len(bounds)))
    else:
        pending = [(0, len(bounds) - 1)]
        balanced_indexes: list[int] = []
        while pending:
            first, last = pending.pop(0)
            middle = (first + last) // 2
            balanced_indexes.append(middle)
            if first < middle:
                pending.append((first, middle - 1))
            if middle < last:
                pending.append((middle + 1, last))
        indexes = tuple(balanced_indexes)
    return tuple(
        SearchPricePartition(
            ordinal=ordinal,
            currency=currency,
            minimum=bounds[index][0],
            maximum=bounds[index][1],
        )
        for ordinal, index in enumerate(indexes, start=1)
    )


class SearchTraversalPolicy(StrictModel):
    """Caller-selected bounds for one sequential search cursor chain."""

    maximum_pages: int | None = Field(default=None, ge=1)
    maximum_results: int | None = Field(default=None, ge=1)
    maximum_elapsed_duration_ns: int | None = Field(default=None, gt=0)
    maximum_transferred_bytes: int | None = Field(default=None, gt=0)
    maximum_decoded_body_bytes: int | None = Field(default=None, gt=0)
    maximum_consecutive_pages_without_new_listings: int | None = Field(default=None, ge=1)
    requested_page_size: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def require_a_bound(self) -> "SearchTraversalPolicy":
        if all(
            value is None
            for value in (
                self.maximum_pages,
                self.maximum_results,
                self.maximum_elapsed_duration_ns,
                self.maximum_transferred_bytes,
                self.maximum_decoded_body_bytes,
                self.maximum_consecutive_pages_without_new_listings,
            )
        ):
            raise ValueError("A search traversal requires at least one stopping bound")
        return self


class SearchPageFacts(StrictModel):
    """Facts from one completely acquired and protocol-parsed search page."""

    listing_identifiers: tuple[str, ...]
    has_next_page: bool
    end_cursor: str | None = Field(default=None, min_length=1)
    interval_elapsed_duration_ns: int = Field(ge=0)
    transferred_bytes: int | None = Field(default=None, ge=0)
    decoded_body_bytes: int = Field(ge=0)

    @field_validator("listing_identifiers")
    @classmethod
    def validate_listing_identifiers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not identifier.isascii() or not identifier.isdecimal() for identifier in value):
            raise ValueError("Facebook listing identifiers must be numeric strings")
        return value


class SearchTraversalState(StrictModel):
    """Durable state after zero or more complete search pages."""

    search_run_identifier: str = Field(min_length=1)
    attempt: int = Field(ge=1)
    policy: SearchTraversalPolicy
    pages_processed: int = Field(default=0, ge=0)
    listing_observations: int = Field(default=0, ge=0)
    unique_listing_identifiers: tuple[str, ...] = ()
    seen_end_cursors: tuple[str, ...] = ()
    elapsed_duration_ns: int = Field(default=0, ge=0)
    transferred_bytes: int | None = Field(default=0, ge=0)
    decoded_body_bytes: int = Field(default=0, ge=0)
    consecutive_pages_without_new_listings: int = Field(default=0, ge=0)
    stopping_reason: SearchStoppingReason | None = None

    @field_validator("unique_listing_identifiers")
    @classmethod
    def validate_listing_identifiers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not identifier.isascii() or not identifier.isdecimal() for identifier in value):
            raise ValueError("Facebook listing identifiers must be numeric strings")
        if len(set(value)) != len(value):
            raise ValueError("Traversal state must contain unique listing identifiers")
        return value

    @field_validator("seen_end_cursors")
    @classmethod
    def validate_cursors(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not cursor for cursor in value):
            raise ValueError("Stored cursors must be nonempty")
        if len(set(value)) != len(value):
            raise ValueError("Traversal state must contain unique cursors")
        return value

    @model_validator(mode="after")
    def validate_counts(self) -> "SearchTraversalState":
        if self.listing_observations < len(self.unique_listing_identifiers):
            raise ValueError("Listing observation count cannot be smaller than the unique count")
        if self.pages_processed == 0 and (
            self.listing_observations
            or self.unique_listing_identifiers
            or self.seen_end_cursors
            or self.elapsed_duration_ns
            or self.decoded_body_bytes
            or self.consecutive_pages_without_new_listings
            or self.stopping_reason is not None
            or self.transferred_bytes not in (0, None)
        ):
            raise ValueError("An initial traversal state cannot contain page results")
        if self.consecutive_pages_without_new_listings > self.pages_processed:
            raise ValueError("Empty-page streak cannot exceed processed pages")
        return self


class PricePartitionSearchTraversalState(StrictModel):
    """Durable aggregate state for a fixed sequence of price partitions."""

    search_run_identifier: str = Field(min_length=1)
    attempt: int = Field(ge=1)
    policy: SearchTraversalPolicy
    strategy: OverlappingPricePartitionSearchTraversalStrategy
    partitions: tuple[SearchPricePartition, ...] = Field(min_length=1)
    partitions_processed: int = Field(default=0, ge=0)
    saturated_partition_ordinals: tuple[int, ...] = ()
    listing_observations: int = Field(default=0, ge=0)
    unique_listing_identifiers: tuple[str, ...] = ()
    elapsed_duration_ns: int = Field(default=0, ge=0)
    transferred_bytes: int | None = Field(default=0, ge=0)
    decoded_body_bytes: int = Field(default=0, ge=0)
    consecutive_pages_without_new_listings: int = Field(default=0, ge=0)
    stopping_reason: SearchStoppingReason | None = None

    @field_validator("unique_listing_identifiers")
    @classmethod
    def validate_listing_identifiers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not identifier.isascii() or not identifier.isdecimal() for identifier in value):
            raise ValueError("Facebook listing identifiers must be numeric strings")
        if len(set(value)) != len(value):
            raise ValueError("Traversal state must contain unique listing identifiers")
        return value

    @field_validator("saturated_partition_ordinals")
    @classmethod
    def validate_saturated_ordinals(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if any(ordinal < 1 for ordinal in value) or len(set(value)) != len(value):
            raise ValueError("Saturated partition ordinals must be unique and positive")
        return value

    @model_validator(mode="after")
    def validate_state(self) -> "PricePartitionSearchTraversalState":
        if self.partitions_processed > len(self.partitions):
            raise ValueError("Processed partition count exceeds the plan")
        expected_ordinals = tuple(range(1, len(self.partitions) + 1))
        if tuple(partition.ordinal for partition in self.partitions) != expected_ordinals:
            raise ValueError("Price partitions must have contiguous ordinals")
        if any(
            ordinal > self.partitions_processed for ordinal in self.saturated_partition_ordinals
        ):
            raise ValueError("An unprocessed price partition cannot be saturated")
        if self.listing_observations < len(self.unique_listing_identifiers):
            raise ValueError("Listing observation count cannot be smaller than the unique count")
        if self.consecutive_pages_without_new_listings > self.partitions_processed:
            raise ValueError("Empty-page streak cannot exceed processed partitions")
        return self


class SearchTraversalDecision(StrictModel):
    action: SearchTraversalAction
    state: SearchTraversalState
    new_listing_identifiers: tuple[str, ...]
    next_cursor: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_decision(self) -> "SearchTraversalDecision":
        if self.action is SearchTraversalAction.CONTINUE:
            if self.next_cursor is None or self.state.stopping_reason is not None:
                raise ValueError("A continuation requires a cursor and no stopping reason")
        elif self.next_cursor is not None or self.state.stopping_reason is None:
            raise ValueError("A stop requires a reason and cannot expose a next cursor")
        return self


class PricePartitionSearchTraversalDecision(StrictModel):
    action: SearchTraversalAction
    state: PricePartitionSearchTraversalState
    new_listing_identifiers: tuple[str, ...]
    next_partition: SearchPricePartition | None = None

    @model_validator(mode="after")
    def validate_decision(self) -> "PricePartitionSearchTraversalDecision":
        if self.action is SearchTraversalAction.CONTINUE:
            if self.next_partition is None or self.state.stopping_reason is not None:
                raise ValueError("A continuation requires a partition and no stopping reason")
        elif self.next_partition is not None or self.state.stopping_reason is None:
            raise ValueError("A stop requires a reason and cannot expose a next partition")
        return self


def _new_listing_identifiers(known: tuple[str, ...], observed: tuple[str, ...]) -> tuple[str, ...]:
    seen = set(known)
    new: list[str] = []
    for identifier in observed:
        if identifier not in seen:
            seen.add(identifier)
            new.append(identifier)
    return tuple(new)


def _stopping_reason(
    *,
    state: SearchTraversalState,
    page: SearchPageFacts,
    repeated_cursor: bool,
) -> SearchStoppingReason | None:
    if not page.has_next_page:
        return SearchStoppingReason.NO_NEXT_PAGE
    policy = state.policy
    if policy.maximum_pages is not None and state.pages_processed >= policy.maximum_pages:
        return SearchStoppingReason.MAXIMUM_PAGES
    if (
        policy.maximum_results is not None
        and len(state.unique_listing_identifiers) >= policy.maximum_results
    ):
        return SearchStoppingReason.MAXIMUM_RESULTS
    if (
        policy.maximum_elapsed_duration_ns is not None
        and state.elapsed_duration_ns >= policy.maximum_elapsed_duration_ns
    ):
        return SearchStoppingReason.MAXIMUM_ELAPSED_DURATION
    if policy.maximum_transferred_bytes is not None:
        if state.transferred_bytes is None:
            return SearchStoppingReason.TRANSFERRED_BYTES_UNAVAILABLE
        if state.transferred_bytes >= policy.maximum_transferred_bytes:
            return SearchStoppingReason.MAXIMUM_TRANSFERRED_BYTES
    if (
        policy.maximum_decoded_body_bytes is not None
        and state.decoded_body_bytes >= policy.maximum_decoded_body_bytes
    ):
        return SearchStoppingReason.MAXIMUM_DECODED_BODY_BYTES
    if (
        policy.maximum_consecutive_pages_without_new_listings is not None
        and state.consecutive_pages_without_new_listings
        >= policy.maximum_consecutive_pages_without_new_listings
    ):
        return SearchStoppingReason.MAXIMUM_CONSECUTIVE_PAGES_WITHOUT_NEW_LISTINGS
    if page.end_cursor is None:
        return SearchStoppingReason.MISSING_CURSOR
    if repeated_cursor:
        return SearchStoppingReason.REPEATED_CURSOR
    return None


def advance_search_traversal(
    *,
    state: SearchTraversalState,
    page: SearchPageFacts,
) -> SearchTraversalDecision:
    """Apply one complete page and decide whether its cursor may be followed."""

    if state.stopping_reason is not None:
        raise ValueError("A stopped search traversal cannot be advanced")

    new_identifiers = _new_listing_identifiers(
        state.unique_listing_identifiers,
        page.listing_identifiers,
    )
    repeated_cursor = page.end_cursor in state.seen_end_cursors
    cursors = state.seen_end_cursors
    if page.end_cursor is not None and not repeated_cursor:
        cursors = (*cursors, page.end_cursor)
    if state.transferred_bytes is None or page.transferred_bytes is None:
        transferred_bytes = None
    else:
        transferred_bytes = state.transferred_bytes + page.transferred_bytes
    next_state = SearchTraversalState(
        search_run_identifier=state.search_run_identifier,
        attempt=state.attempt,
        policy=state.policy,
        pages_processed=state.pages_processed + 1,
        listing_observations=state.listing_observations + len(page.listing_identifiers),
        unique_listing_identifiers=(*state.unique_listing_identifiers, *new_identifiers),
        seen_end_cursors=cursors,
        elapsed_duration_ns=state.elapsed_duration_ns + page.interval_elapsed_duration_ns,
        transferred_bytes=transferred_bytes,
        decoded_body_bytes=state.decoded_body_bytes + page.decoded_body_bytes,
        consecutive_pages_without_new_listings=(
            0 if new_identifiers else state.consecutive_pages_without_new_listings + 1
        ),
    )
    reason = _stopping_reason(
        state=next_state,
        page=page,
        repeated_cursor=repeated_cursor,
    )
    if reason is None:
        return SearchTraversalDecision(
            action=SearchTraversalAction.CONTINUE,
            state=next_state,
            new_listing_identifiers=new_identifiers,
            next_cursor=page.end_cursor,
        )
    return SearchTraversalDecision(
        action=SearchTraversalAction.STOP,
        state=next_state.model_copy(update={"stopping_reason": reason}),
        new_listing_identifiers=new_identifiers,
    )


def _price_partition_stopping_reason(
    state: PricePartitionSearchTraversalState,
) -> SearchStoppingReason | None:
    if state.partitions_processed >= len(state.partitions):
        return SearchStoppingReason.PRICE_PARTITIONS_EXHAUSTED
    policy = state.policy
    if policy.maximum_pages is not None and state.partitions_processed >= policy.maximum_pages:
        return SearchStoppingReason.MAXIMUM_PAGES
    if (
        policy.maximum_results is not None
        and len(state.unique_listing_identifiers) >= policy.maximum_results
    ):
        return SearchStoppingReason.MAXIMUM_RESULTS
    if (
        policy.maximum_elapsed_duration_ns is not None
        and state.elapsed_duration_ns >= policy.maximum_elapsed_duration_ns
    ):
        return SearchStoppingReason.MAXIMUM_ELAPSED_DURATION
    if policy.maximum_transferred_bytes is not None:
        if state.transferred_bytes is None:
            return SearchStoppingReason.TRANSFERRED_BYTES_UNAVAILABLE
        if state.transferred_bytes >= policy.maximum_transferred_bytes:
            return SearchStoppingReason.MAXIMUM_TRANSFERRED_BYTES
    if (
        policy.maximum_decoded_body_bytes is not None
        and state.decoded_body_bytes >= policy.maximum_decoded_body_bytes
    ):
        return SearchStoppingReason.MAXIMUM_DECODED_BODY_BYTES
    if (
        policy.maximum_consecutive_pages_without_new_listings is not None
        and state.consecutive_pages_without_new_listings
        >= policy.maximum_consecutive_pages_without_new_listings
    ):
        return SearchStoppingReason.MAXIMUM_CONSECUTIVE_PAGES_WITHOUT_NEW_LISTINGS
    return None


def advance_price_partition_search_traversal(
    *,
    state: PricePartitionSearchTraversalState,
    partition: SearchPricePartition,
    page: SearchPageFacts,
) -> PricePartitionSearchTraversalDecision:
    """Account one complete partition page and select the next planned interval."""

    if state.stopping_reason is not None:
        raise ValueError("A stopped search traversal cannot be advanced")
    if state.partitions_processed >= len(state.partitions):
        raise ValueError("All price partitions have already been processed")
    if partition != state.partitions[state.partitions_processed]:
        raise ValueError("Price partitions must be processed in planned order")

    new_identifiers = _new_listing_identifiers(
        state.unique_listing_identifiers,
        page.listing_identifiers,
    )
    if state.transferred_bytes is None or page.transferred_bytes is None:
        transferred_bytes = None
    else:
        transferred_bytes = state.transferred_bytes + page.transferred_bytes
    saturated = state.saturated_partition_ordinals
    if page.has_next_page:
        saturated = (*saturated, partition.ordinal)
    next_state = state.model_copy(
        update={
            "partitions_processed": state.partitions_processed + 1,
            "saturated_partition_ordinals": saturated,
            "listing_observations": state.listing_observations + len(page.listing_identifiers),
            "unique_listing_identifiers": (
                *state.unique_listing_identifiers,
                *new_identifiers,
            ),
            "elapsed_duration_ns": state.elapsed_duration_ns + page.interval_elapsed_duration_ns,
            "transferred_bytes": transferred_bytes,
            "decoded_body_bytes": state.decoded_body_bytes + page.decoded_body_bytes,
            "consecutive_pages_without_new_listings": (
                0 if new_identifiers else state.consecutive_pages_without_new_listings + 1
            ),
        }
    )
    reason = _price_partition_stopping_reason(next_state)
    if reason is None:
        return PricePartitionSearchTraversalDecision(
            action=SearchTraversalAction.CONTINUE,
            state=next_state,
            new_listing_identifiers=new_identifiers,
            next_partition=next_state.partitions[next_state.partitions_processed],
        )
    return PricePartitionSearchTraversalDecision(
        action=SearchTraversalAction.STOP,
        state=next_state.model_copy(update={"stopping_reason": reason}),
        new_listing_identifiers=new_identifiers,
    )


def account_search_elapsed_interval[
    SearchTraversalStateType: (SearchTraversalState, PricePartitionSearchTraversalState)
](
    state: SearchTraversalStateType,
    *,
    interval_elapsed_duration_ns: int,
) -> SearchTraversalStateType:
    """Account time between pages and stop before another request when bounded."""

    if interval_elapsed_duration_ns < 0:
        raise ValueError("Elapsed duration cannot be negative")
    if state.stopping_reason is not None:
        raise ValueError("A stopped search traversal cannot be advanced")
    elapsed_duration_ns = state.elapsed_duration_ns + interval_elapsed_duration_ns
    maximum = state.policy.maximum_elapsed_duration_ns
    stopping_reason = (
        SearchStoppingReason.MAXIMUM_ELAPSED_DURATION
        if maximum is not None and elapsed_duration_ns >= maximum
        else None
    )
    return state.model_copy(
        update={
            "elapsed_duration_ns": elapsed_duration_ns,
            "stopping_reason": stopping_reason,
        }
    )
