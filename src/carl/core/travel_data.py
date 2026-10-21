"""Project-owned routing and geocoding dataset lifecycle contracts."""

import hashlib
from typing import Literal, Protocol

from pydantic import Field, field_validator

from carl.core.models import JsonValue, StrictModel
from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScope,
    SchedulingScopeKind,
    WorkDefinition,
    WorkState,
)

type TravelDataProvider = Literal["osrm", "geonames"]

TRAVEL_DATA_SETUP_WORK_KIND = ("carl", "travel", "setup_data")


class TravelDataSetupRequest(StrictModel):
    request_identifier: str = Field(min_length=1, max_length=200)
    provider: TravelDataProvider
    activate: bool = True
    radius_miles: float = Field(default=250, ge=1, le=1000, allow_inf_nan=False)
    threads: int = Field(default=4, ge=1, le=16)

    @field_validator("request_identifier")
    @classmethod
    def require_trimmed_identifier(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Travel data request identifier must be trimmed")
        return value


class TravelDataSetupResult(StrictModel):
    provider: TravelDataProvider
    dataset_identifier: str = Field(min_length=1)
    manifest_path: str = Field(min_length=1)
    activated: bool
    message: str


class TravelDataFailure(Exception):
    """An expected lifecycle failure with a manager-sanitized reason and message."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason
        self.message = message


class TravelDataService(Protocol):
    async def setup(self, request: TravelDataSetupRequest) -> TravelDataSetupResult:
        """Prepare, validate, and optionally activate a versioned dataset."""
        ...


class RequestTravelDataSetupResult(StrictModel):
    work_identifier: str = Field(min_length=1)
    created: bool
    state: WorkState
    message: str = "Poll get_work_status with the work identifier for progress and the result."
    worker_required: bool = True


class TravelProviderStatus(StrictModel):
    provider: TravelDataProvider
    state: Literal["unconfigured", "missing", "invalid", "incompatible", "ready"]
    dataset_identifier: str | None = None
    reason: str | None = None
    message: str
    actions: tuple[str, ...] = ()
    size_bytes: int = Field(default=0, ge=0)
    created_at_utc: str | None = None
    outdated: bool = False


class TravelDataStatus(StrictModel):
    routing_provider: str | None
    geocoding_provider: str | None
    origin_configured: bool = False
    configuration_message: str | None = None
    providers: tuple[TravelProviderStatus, ...]
    build: dict[str, JsonValue] | None = None


class TravelDatasetSummary(StrictModel):
    provider: TravelDataProvider
    dataset_identifier: str = Field(min_length=1)
    active: bool
    size_bytes: int = Field(ge=0)
    created_at_utc: str | None


class TravelDatasetPage(StrictModel):
    items: tuple[TravelDatasetSummary, ...]
    next_cursor: str | None = None


def travel_data_work_identifier(request_identifier: str) -> str:
    """Keep exact setup-request replay addressable after terminal completion."""

    return "travel-data-" + hashlib.sha256(request_identifier.encode("utf-8")).hexdigest()


def travel_data_work(request: TravelDataSetupRequest) -> WorkDefinition:
    return WorkDefinition(
        identifier=travel_data_work_identifier(request.request_identifier),
        kind=TRAVEL_DATA_SETUP_WORK_KIND,
        payload_schema_version=1,
        payload=request.model_dump(mode="json"),
        deduplication_identity=(request.request_identifier,),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND, identity=TRAVEL_DATA_SETUP_WORK_KIND
            ),
        ),
    )


def travel_data_constraint() -> ConcurrencyConstraint:
    """Serialize resource-intensive builds across providers and worker processes."""

    return ConcurrencyConstraint(
        identifier=(*TRAVEL_DATA_SETUP_WORK_KIND, "work_concurrency", "v1"),
        scope=SchedulingScope(
            kind=SchedulingScopeKind.WORK_KIND, identity=TRAVEL_DATA_SETUP_WORK_KIND
        ),
        maximum_active=1,
    )
