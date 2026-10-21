"""Provider-independent road travel estimates and explicit unknown handling."""

from collections.abc import Sequence
from datetime import datetime
from typing import Literal, Protocol

from pydantic import Field, field_validator, model_validator

from carl.core.models import StrictModel


class TravelCoordinate(StrictModel):
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)

    @field_validator("latitude", "longitude", mode="before")
    @classmethod
    def reject_boolean(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("Coordinates must be numbers")
        return value


class TravelDestination(StrictModel):
    coordinate: TravelCoordinate | None = None
    label: str | None = Field(default=None, max_length=500)
    precision: Literal["source_approximate", "geocoded_locality", "unavailable"] = "unavailable"
    unavailable_reason: str | None = None


class TravelEstimate(StrictModel):
    status: Literal["ready", "unavailable"]
    provider: Literal["openrouteservice", "osrm"] = "openrouteservice"
    distance_meters: float | None = Field(default=None, ge=0)
    duration_seconds: float | None = Field(default=None, ge=0)
    approximate: bool = True
    destination_label: str | None = None
    location_precision: Literal["source_approximate", "geocoded_locality", "unavailable"] = (
        "unavailable"
    )
    estimated_at_utc: str | None = None
    unavailable_reason: str | None = None
    record_identifier: str | None = Field(default=None, min_length=1)
    expires_at_utc: str | None = None
    dataset_identifier: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_metrics(self) -> "TravelEstimate":
        if (
            self.expires_at_utc is not None
            and datetime.fromisoformat(self.expires_at_utc).tzinfo is None
        ):
            raise ValueError("Travel expiry must include a time zone")
        if self.status == "ready" and (
            self.distance_meters is None
            or self.duration_seconds is None
            or self.unavailable_reason is not None
        ):
            raise ValueError("Ready travel estimates require distance and duration")
        if self.status == "unavailable" and (
            self.distance_meters is not None
            or self.duration_seconds is not None
            or self.unavailable_reason is None
        ):
            raise ValueError("Unavailable travel estimates require a reason and no metrics")
        return self


class TravelFilters(StrictModel):
    maximum_driving_distance_miles: float | None = Field(default=None, ge=0)
    maximum_driving_duration_minutes: float | None = Field(default=None, ge=0)
    include_unknown: bool = True

    @property
    def active(self) -> bool:
        return (
            self.maximum_driving_distance_miles is not None
            or self.maximum_driving_duration_minutes is not None
        )


def travel_matches_filters(estimate: TravelEstimate | None, filters: TravelFilters) -> bool:
    if not filters.active:
        return True
    if estimate is None or estimate.status != "ready":
        return filters.include_unknown
    if estimate.distance_meters is None or estimate.duration_seconds is None:
        return filters.include_unknown
    return (
        filters.maximum_driving_distance_miles is None
        or estimate.distance_meters <= filters.maximum_driving_distance_miles * 1609.344
    ) and (
        filters.maximum_driving_duration_minutes is None
        or estimate.duration_seconds <= filters.maximum_driving_duration_minutes * 60
    )


class TravelService(Protocol):
    async def origin_identity(self) -> str:
        """Return the effective origin/provider identity for cursor scoping."""
        ...

    async def estimate_many(
        self, destinations: Sequence[TravelDestination]
    ) -> tuple[TravelEstimate, ...]: ...
