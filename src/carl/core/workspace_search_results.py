"""Cheap, source-neutral pages of retained cards from one workspace track."""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Sequence
from datetime import date
from typing import Literal

from pydantic import Field, field_validator

from carl.core.ebay import EbayListingState, EbaySearchResponseClassification
from carl.core.json import decode_json, encode_json
from carl.core.marketplace_search import Marketplace
from carl.core.models import JsonValue, StrictModel


class ListWorkspaceSearchResultsRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    track_identifier: str = Field(min_length=1)
    listing_state: EbayListingState | None = Field(
        default=None,
        description="completed includes positively identified sold and ended cards; omit to retain unknown cards too",
    )
    required_title_keywords: Sequence[str] = Field(default=(), max_length=20)
    excluded_title_keywords: Sequence[str] = Field(default=(), max_length=20)
    page_size: int = Field(default=25, ge=1, le=100)
    cursor: str | None = None

    @field_validator("required_title_keywords", "excluded_title_keywords")
    @classmethod
    def validate_keywords(cls, value: Sequence[str]) -> Sequence[str]:
        if any(not word or word != word.strip() or len(word) > 100 for word in value):
            raise ValueError("Title keywords must be nonempty, trimmed, and at most 100 characters")
        return value


class WorkspaceSearchResultOccurrence(StrictModel):
    occurrence_record_identifier: str
    acquisition_record_identifier: str | None = None
    page_ordinal: int = Field(ge=0)
    position: int = Field(ge=0)
    listing_state: EbayListingState | None = None
    sold_price: str | None = None
    sold_price_value: JsonValue = None
    sold_date: date | None = None
    sold_date_text: str | None = None
    sold_price_status: Literal["displayed", "best_offer_accepted", "unavailable"] | None = None


class WorkspaceSearchResult(StrictModel):
    marketplace: Marketplace
    listing_identifier: str
    external_identifier: str
    canonical_url: str
    title: str | None = None
    price: JsonValue = None
    displayed_price: str | None = None
    location: JsonValue = None
    condition: str | None = None
    shipping_text: str | None = None
    preview_image_url: str | None = None
    listing_state: EbayListingState | None = None
    last_sale: JsonValue = None
    occurrences: tuple[WorkspaceSearchResultOccurrence, ...] = Field(min_length=1)


class WorkspaceSearchResultsPage(StrictModel):
    workspace_record_identifier: str
    track_identifier: str
    source_search_run_record_identifier: str | None
    as_of_completion_sequence: int = Field(ge=0)
    total_distinct_results: int = Field(ge=0)
    collection_succeeded: bool | None = None
    response_classification: EbaySearchResponseClassification | None = None
    stopping_reason: str | None = None
    results: tuple[WorkspaceSearchResult, ...] = Field(max_length=100)
    next_cursor: str | None
    warnings: tuple[str, ...] = ()


class WorkspaceSearchResultsCursor(StrictModel):
    scope_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_search_run_record_identifier: str = Field(min_length=1)
    as_of_completion_sequence: int = Field(ge=0)
    maximum_object_rowid: int = Field(ge=0)
    after_listing_identifier: str = Field(min_length=1)


def workspace_search_results_scope(request: ListWorkspaceSearchResultsRequest) -> str:
    # Page size may change between pages; source, track, and filters may not.
    value = request.model_dump(mode="json", exclude={"cursor", "page_size"})
    return hashlib.sha256(encode_json(value).encode("utf-8")).hexdigest()


def encode_workspace_search_results_cursor(cursor: WorkspaceSearchResultsCursor) -> str:
    return base64.urlsafe_b64encode(cursor.model_dump_json().encode()).decode().rstrip("=")


def decode_workspace_search_results_cursor(value: str) -> WorkspaceSearchResultsCursor:
    try:
        decoded = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
        return WorkspaceSearchResultsCursor.model_validate_json(
            encode_json(decode_json(decoded.decode()))
        )
    except (ValueError, UnicodeDecodeError) as error:
        raise ValueError("Workspace search-results cursor is malformed") from error
