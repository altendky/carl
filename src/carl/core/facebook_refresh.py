"""Pure request and work models for a complete Marketplace search refresh."""

from __future__ import annotations

import hashlib
from typing import Literal

from pydantic import Field, model_validator
from pydantic.json_schema import SkipJsonSchema

from carl.core.facebook_search import SearchTraversalPolicy, SearchTraversalStrategy
from carl.core.facebook_work import CollectSearchPayload
from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel
from carl.core.network_defaults import (
    DEFAULT_DATACENTER_NETWORK_PATH,
    AcquisitionNetworkPath,
    LegacyProtonRoute,
    populate_legacy_proton_paths,
    validate_legacy_proton_paths,
)
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
    search_network_path: AcquisitionNetworkPath | None = Field(
        default=None, description="Explicit Facebook search route; otherwise use the current spec."
    )
    image_network_path: AcquisitionNetworkPath = Field(
        default=DEFAULT_DATACENTER_NETWORK_PATH,
        description="Gallery acquisition route; defaults directly to Decodo datacenter.",
    )
    proton_route: SkipJsonSchema[LegacyProtonRoute | None] = Field(default=None, exclude=True)
    decodo_route: str = Field(default="carl", min_length=1)
    acquisition_stack: str | None = Field(default=None, min_length=1)
    maximum_pages: int | None = Field(default=None, ge=1, le=20)

    @model_validator(mode="before")
    @classmethod
    def populate_legacy_route(cls, value: object) -> object:
        return populate_legacy_proton_paths(
            value,
            legacy_field="proton_route",
            network_fields=("search_network_path", "image_network_path"),
        )

    @model_validator(mode="after")
    def validate_legacy_route(self) -> SearchRefreshRequest:
        validate_legacy_proton_paths(
            self.proton_route, self.search_network_path, self.image_network_path
        )
        return self

    @property
    def requested_search_network_path(self) -> tuple[str, ...] | None:
        return self.search_network_path

    @property
    def requested_image_network_path(self) -> tuple[str, ...]:
        return self.image_network_path


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
    marketplace: Literal["facebook", "ebay"] = "facebook"
    listing_state: Literal["active", "sold", "completed"] | None = None
    record_identifier: str
    completion_sequence: int = Field(ge=0)
    started_at_utc: str
    ended_at_utc: str | None
    origin: SearchRunOrigin
    refresh_source_run_record_identifier: str | None
    query: str
    facebook_location_identifier: str | None
    radius_value: int | None
    radius_unit: str | None
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
