"""Durable, independently refreshable road estimates for exact listings."""

import hashlib
import json
from collections.abc import Sequence
from typing import Protocol

from pydantic import Field, field_validator, model_validator

from carl.core.listing_identity import LISTING_IDENTIFIER_PATTERN
from carl.core.models import StrictModel
from carl.core.travel import TravelDestination, TravelEstimate
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkDefinition, WorkState

REFRESH_LISTING_ROUTE_WORK_KIND = ("carl", "travel", "refresh_listing")
TRAVEL_ESTIMATE_RECORD_KIND = ("carl", "travel", "estimate")


class RequestListingRouteRefresh(StrictModel):
    request_identifier: str = Field(min_length=1, max_length=200)
    listing_identifier: str = Field(pattern=LISTING_IDENTIFIER_PATTERN)
    workspace_record_identifier: str | None = Field(default=None, min_length=1)

    @field_validator("request_identifier")
    @classmethod
    def validate_request_identifier(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Route refresh request identifier must be trimmed")
        return value


def travel_destination_identity(destination: TravelDestination) -> str:
    """Bind an estimate to the exact retained location inputs, including uncertainty."""

    serialized = json.dumps(
        destination.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class TravelRefreshPayload(StrictModel):
    request: RequestListingRouteRefresh
    destination: TravelDestination
    origin_identity: str = Field(min_length=1)
    destination_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_record_identifiers: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_frozen_inputs(self) -> "TravelRefreshPayload":
        if self.destination_identity != travel_destination_identity(self.destination):
            raise ValueError("Route destination does not match its identity")
        if any(not identifier for identifier in self.source_record_identifiers):
            raise ValueError("Route source record identifiers must be nonempty")
        if len(set(self.source_record_identifiers)) != len(self.source_record_identifiers):
            raise ValueError("Route source record identifiers must be unique")
        return self


class TravelEstimateRecord(StrictModel):
    record_identifier: str = Field(min_length=1)
    listing_id: str = Field(pattern=LISTING_IDENTIFIER_PATTERN)
    origin_identity: str = Field(min_length=1)
    destination_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    destination: TravelDestination
    estimate: TravelEstimate
    recorded_at_utc: str = Field(min_length=1)


class RequestListingRouteRefreshResult(StrictModel):
    work_identifier: str = Field(min_length=1)
    created: bool
    state: WorkState
    listing_identifier: str = Field(pattern=LISTING_IDENTIFIER_PATTERN)


def travel_refresh_work_identifier(request_identifier: str) -> str:
    """Keep exact request replay addressable even after work becomes terminal."""

    return "travel-refresh-" + hashlib.sha256(request_identifier.encode("utf-8")).hexdigest()


def refresh_listing_route_work(
    payload: TravelRefreshPayload, *, not_before_utc_ns: int = 0
) -> WorkDefinition:
    return WorkDefinition(
        identifier=travel_refresh_work_identifier(payload.request.request_identifier),
        kind=REFRESH_LISTING_ROUTE_WORK_KIND,
        payload_schema_version=1,
        payload=payload.model_dump(mode="json"),
        deduplication_identity=(payload.request.request_identifier,),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND, identity=REFRESH_LISTING_ROUTE_WORK_KIND
            ),
        ),
    )


class TravelRefreshService(Protocol):
    async def refresh_many(
        self, destinations: Sequence[TravelDestination], *, expected_origin_identity: str
    ) -> tuple[TravelEstimate, ...]:
        """Bypass route-result cache while refusing a changed effective origin."""
        ...
