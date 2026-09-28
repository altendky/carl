"""Pure request and work models for a complete Marketplace search refresh."""

from __future__ import annotations

import hashlib

from pydantic import Field

from carl.core.facebook_search import SearchTraversalPolicy, SearchTraversalStrategy
from carl.core.facebook_work import CollectSearchPayload
from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel
from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    WorkDefinition,
)

REFRESH_SEARCH_WORK_KIND = ("carl", "facebook", "work", "refresh_search")
LEGACY_REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION = 1
REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION = 2
REFRESH_SEARCH_MAXIMUM_ACTIVE = 1


def refresh_search_work_constraints() -> tuple[ConcurrencyConstraint, ...]:
    """Permit one search-refresh coordinator across all worker processes."""

    return (
        ConcurrencyConstraint(
            identifier=("carl", "facebook", "search_refresh", "work_concurrency", "v1"),
            subject_kind=SchedulingSubjectKind.WORK_ITEM,
            scope=SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND,
                identity=REFRESH_SEARCH_WORK_KIND,
            ),
            maximum_active=REFRESH_SEARCH_MAXIMUM_ACTIVE,
        ),
    )


class SearchRefreshRequest(StrictModel):
    """Refresh one retained search and its listing and image evidence."""

    base_search_run_record_identifier: str = Field(min_length=1)
    traversal: SearchTraversalPolicy | None = None
    traversal_strategy: SearchTraversalStrategy | None = None
    maximum_items: int | None = Field(default=None, ge=1)
    maximum_images: int | None = Field(default=None, ge=1)
    proton_route: str = Field(default="carl", min_length=1)
    decodo_route: str = Field(default="carl", min_length=1)


class RefreshSearchPayload(StrictModel):
    base_search_run_record_identifier: str = Field(min_length=1)
    search_work_identifier: str = Field(min_length=1)
    search: CollectSearchPayload
    maximum_items: int | None = Field(default=None, ge=1)
    maximum_images: int | None = Field(default=None, ge=1)
    item_routing: tuple[str, ...]
    image_routing: tuple[str, ...]


class SearchRefreshRequestResult(StrictModel):
    work_identifier: str
    created: bool
    state: str
    base_search_run_record_identifier: str


class SearchRunOrigin(JsonStringEnumeration):
    FRESH = "fresh"
    REFRESH = "refresh"
    UNKNOWN = "unknown"


class SearchRunSummary(StrictModel):
    record_identifier: str
    completion_sequence: int = Field(ge=1)
    started_at_utc: str
    ended_at_utc: str | None
    origin: SearchRunOrigin
    refresh_source_run_record_identifier: str | None
    query: str
    facebook_location_identifier: str
    radius_value: int
    radius_unit: str
    minimum_price: JsonValue
    maximum_price: JsonValue
    traversal_strategy: JsonValue
    unique_listings: int = Field(ge=0)
    stopping_reason: str | None


class SearchRunPage(StrictModel):
    search_runs: tuple[SearchRunSummary, ...]


class SearchRunListingsPage(StrictModel):
    search_run_record_identifier: str
    total_listing_identifiers: int = Field(ge=0)
    offset: int = Field(ge=0)
    listing_identifiers: tuple[str, ...]
    next_offset: int | None = Field(default=None, ge=0)


def refresh_search_work(
    *, identifier: str, payload: RefreshSearchPayload, not_before_utc_ns: int
) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=REFRESH_SEARCH_WORK_KIND,
        payload_schema_version=REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION,
        payload=payload.model_dump(mode="json"),
        deduplication_identity=(
            "facebook_marketplace",
            "search_refresh",
            hashlib.sha256(
                payload.model_dump_json(exclude={"search_work_identifier"}).encode("utf-8")
            ).hexdigest(),
        ),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND,
                identity=REFRESH_SEARCH_WORK_KIND,
            ),
        ),
    )
