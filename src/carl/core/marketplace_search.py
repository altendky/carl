"""Marketplace-neutral search groups, targets, and retained executions."""

from __future__ import annotations

import base64
from collections.abc import Sequence
from datetime import date
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from carl.core.components import Component, ComponentId, Registry
from carl.core.ebay import EbayListingState, EbaySearchRequest, EbaySearchResponseClassification
from carl.core.facebook_work import CreateSearchRequest as CreateFacebookSearchRequest
from carl.core.json import decode_json, encode_json
from carl.core.models import JsonStringEnumeration, StrictModel
from carl.core.work import WorkState

CREATE_MARKETPLACE_SEARCH = ComponentId(("carl", "marketplace", "create", "search"))
ADD_MARKETPLACE_SEARCH_TARGET = ComponentId(("carl", "marketplace", "add", "search_target"))
SET_MARKETPLACE_SEARCH_TARGET_ENABLED = ComponentId(
    ("carl", "marketplace", "set", "search_target_enabled")
)
RUN_MARKETPLACE_SEARCH = ComponentId(("carl", "marketplace", "run", "search"))

MARKETPLACE_SEARCH_KIND = ("carl", "marketplace", "search")
MARKETPLACE_SEARCH_TARGET_KIND = ("carl", "marketplace", "search_target")
MARKETPLACE_SEARCH_TARGET_STATE_KIND = ("carl", "marketplace", "search_target_state")
MARKETPLACE_SEARCH_EXECUTION_KIND = ("carl", "marketplace", "search_execution")


class Marketplace(JsonStringEnumeration):
    FACEBOOK = "facebook"
    EBAY = "ebay"


class FacebookSearchTargetSpecification(StrictModel):
    marketplace: Literal[Marketplace.FACEBOOK] = Marketplace.FACEBOOK
    search: CreateFacebookSearchRequest


class EbaySearchTargetSpecification(StrictModel):
    marketplace: Literal[Marketplace.EBAY] = Marketplace.EBAY
    search: EbaySearchRequest


type SearchTargetSpecification = Annotated[
    FacebookSearchTargetSpecification | EbaySearchTargetSpecification,
    Field(discriminator="marketplace"),
]


class CreateMarketplaceSearchRequest(StrictModel):
    targets: Sequence[SearchTargetSpecification] = Field(min_length=1, max_length=20)

    @field_validator("targets")
    @classmethod
    def validate_unique_targets(
        cls, value: Sequence[SearchTargetSpecification]
    ) -> Sequence[SearchTargetSpecification]:
        serialized = tuple(target.model_dump_json() for target in value)
        if len(serialized) != len(set(serialized)):
            raise ValueError("A search may not contain duplicate targets")
        return value


class AddMarketplaceSearchTargetRequest(StrictModel):
    search_record_identifier: str = Field(min_length=1)
    target: SearchTargetSpecification


class SetMarketplaceSearchTargetEnabledRequest(StrictModel):
    search_record_identifier: str = Field(min_length=1)
    target_record_identifier: str = Field(min_length=1)
    enabled: bool


class RunMarketplaceSearchRequest(StrictModel):
    search_record_identifier: str = Field(min_length=1)


class MarketplaceSearchRecord(StrictModel):
    record_identifier: str = Field(min_length=1)
    created_at_utc: str


class MarketplaceSearchTargetRecord(StrictModel):
    record_identifier: str = Field(min_length=1)
    search_record_identifier: str = Field(min_length=1)
    specification: SearchTargetSpecification
    created_at_utc: str


class MarketplaceSearchTargetStateRecord(StrictModel):
    record_identifier: str = Field(min_length=1)
    search_record_identifier: str = Field(min_length=1)
    target_record_identifier: str = Field(min_length=1)
    enabled: bool
    recorded_at_utc: str


class MarketplaceSearchExecutionRecord(StrictModel):
    record_identifier: str = Field(min_length=1)
    search_record_identifier: str = Field(min_length=1)
    target_record_identifier: str = Field(min_length=1)
    work_identifier: str = Field(min_length=1)
    requested_at_utc: str


class MarketplaceSearchExecution(StrictModel):
    record_identifier: str = Field(min_length=1)
    work_identifier: str = Field(min_length=1)
    state: WorkState
    requested_at_utc: str
    search_run_record_identifier: str | None = Field(default=None, min_length=1)
    response_classification: EbaySearchResponseClassification | None = None
    stopping_reason: str | None = None
    collection_succeeded: bool | None = None


class MarketplaceSearchExecutionWarning(StrictModel):
    target_record_identifier: str = Field(min_length=1)
    reason: Literal["collection_failure", "work_failure", "collection_pending"]
    execution: MarketplaceSearchExecution


class MarketplaceSearchTarget(StrictModel):
    record_identifier: str = Field(min_length=1)
    specification: SearchTargetSpecification
    enabled: bool
    created_at_utc: str
    executions: tuple[MarketplaceSearchExecution, ...]


class MarketplaceSearch(StrictModel):
    record_identifier: str = Field(min_length=1)
    created_at_utc: str
    targets: tuple[MarketplaceSearchTarget, ...]
    historical_search_run_record_identifiers: tuple[str, ...]


class MarketplaceSearchResultOccurrence(StrictModel):
    """One retained source-card occurrence supporting a projected result."""

    occurrence_record_identifier: str = Field(min_length=1)
    source_search_run_record_identifier: str = Field(min_length=1)
    target_record_identifier: str = Field(min_length=1)
    execution_record_identifier: str = Field(min_length=1)
    acquisition_record_identifier: str | None = Field(default=None, min_length=1)
    completion_sequence: int = Field(ge=1)
    page_ordinal: int = Field(ge=0)
    position: int = Field(ge=0)
    listing_state: EbayListingState | None = None
    displayed_price: str | None = None
    sold_price: str | None = None
    sold_date: date | None = None
    sold_date_text: str | None = None
    sold_price_status: Literal["displayed", "best_offer_accepted", "unavailable"] | None = None


class MarketplaceSearchResult(StrictModel):
    """Marketplace-neutral view of one deduplicated retained search card."""

    marketplace: Marketplace
    external_identifier: str = Field(min_length=1)
    canonical_url: str = Field(min_length=1)
    title: str | None = None
    displayed_price: str | None = None
    location: str | None = None
    shipping_text: str | None = None
    condition: str | None = None
    preview_image_url: str | None = None
    promoted: bool | None = None
    listing_state: EbayListingState | None = None
    sold_price: str | None = None
    sold_date: date | None = None
    sold_date_text: str | None = None
    sold_price_status: Literal["displayed", "best_offer_accepted", "unavailable"] | None = None
    sold_occurrence_record_identifier: str | None = None
    occurrences: tuple[MarketplaceSearchResultOccurrence, ...] = Field(min_length=1)


class MarketplaceSearchResultsCursor(StrictModel):
    """Opaque stable position within one search group's completion boundary."""

    search_record_identifier: str = Field(min_length=1)
    as_of_completion_sequence: int = Field(ge=0)
    after_completion_sequence: int | None = Field(default=None, ge=1)
    after_marketplace: Marketplace | None = None
    after_external_identifier: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_position(self) -> MarketplaceSearchResultsCursor:
        values = (
            self.after_completion_sequence,
            self.after_marketplace,
            self.after_external_identifier,
        )
        if any(value is None for value in values) and any(value is not None for value in values):
            raise ValueError("Search-results cursor position must be entirely present or absent")
        return self


def encode_marketplace_search_results_cursor(cursor: MarketplaceSearchResultsCursor) -> str:
    content = encode_json(cursor.model_dump(mode="json")).encode("utf-8")
    return base64.urlsafe_b64encode(content).decode("ascii").rstrip("=")


def decode_marketplace_search_results_cursor(value: str) -> MarketplaceSearchResultsCursor:
    if not value or any(character.isspace() for character in value):
        raise ValueError("Search-results cursor must be nonempty URL-safe base64")
    try:
        raw = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
        decoded = decode_json(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise ValueError("Search-results cursor is malformed") from error
    return MarketplaceSearchResultsCursor.model_validate(decoded)


class ListMarketplaceSearchResultsRequest(StrictModel):
    search_record_identifier: str = Field(min_length=1)
    page_size: int = Field(default=25, ge=1, le=100)
    cursor: str | None = None


class MarketplaceSearchResultsPage(StrictModel):
    search_record_identifier: str = Field(min_length=1)
    as_of_completion_sequence: int = Field(ge=0)
    total_distinct_results: int = Field(ge=0)
    results: tuple[MarketplaceSearchResult, ...]
    next_cursor: str | None
    execution_warnings: tuple[MarketplaceSearchExecutionWarning, ...] = Field(
        default=(),
        description="Live execution warnings; unlike listing cards, not frozen by the cursor.",
    )


def _create_search_component() -> None:
    """Identity anchor for marketplace search creation."""


def _add_target_component() -> None:
    """Identity anchor for marketplace target addition."""


def _set_target_enabled_component() -> None:
    """Identity anchor for marketplace target state changes."""


def _run_search_component() -> None:
    """Identity anchor for marketplace search execution requests."""


def build_marketplace_search_component_registry() -> Registry:
    return Registry(
        (
            Component(CREATE_MARKETPLACE_SEARCH, 1, _create_search_component),
            Component(ADD_MARKETPLACE_SEARCH_TARGET, 1, _add_target_component),
            Component(
                SET_MARKETPLACE_SEARCH_TARGET_ENABLED,
                1,
                _set_target_enabled_component,
            ),
            Component(RUN_MARKETPLACE_SEARCH, 1, _run_search_component),
        )
    )
