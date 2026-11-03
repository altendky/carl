"""Explicit scheduling intent, independent of acquisition identity and constraints."""

from pydantic import Field, model_validator

from carl.core.models import JsonStringEnumeration, StrictModel

FAIR_AGING_NS = 30_000_000_000
FAIR_OFFER_NS = 1_000_000_000
FAIR_INTERACTIVE_BURST = 3


class SchedulingServiceClass(JsonStringEnumeration):
    INTERACTIVE = "interactive"
    BACKGROUND = "background"


class SchedulingContext(StrictModel):
    root_request_identifier: str = Field(min_length=1)
    workspace_identifier: str | None = Field(default=None, min_length=1)
    service_class: SchedulingServiceClass = SchedulingServiceClass.BACKGROUND


class FairLane(JsonStringEnumeration):
    HTTP = "http"
    CPU = "cpu"
    ANALYSIS = "analysis"
    METADATA = "metadata"
    RESIDENT = "resident"


class FairDispatchLaneHorizon(StrictModel):
    lane: FairLane
    sequence: int = Field(ge=0)
    interactive_streak: int = Field(ge=0)


class FairDispatchHorizon(StrictModel):
    """Immutable membership and ordering bounds for one ephemeral discovery pass."""

    scope: str = Field(min_length=1)
    captured_at_utc_ns: int = Field(ge=0)
    workspace_rowid_high_water: int = Field(ge=0)
    lanes: tuple[FairDispatchLaneHorizon, ...] = Field(
        min_length=len(FairLane), max_length=len(FairLane)
    )

    @model_validator(mode="after")
    def validate_lanes(self) -> "FairDispatchHorizon":
        if {lane.lane for lane in self.lanes} != set(FairLane):
            raise ValueError("A dispatch horizon requires each resource lane exactly once")
        return self


class FairDispatchCursor(StrictModel):
    # Cursors are process-local, not stored in SQLite. A legacy cursor without
    # its pass horizon must be rejected instead of resuming a mutable scan.
    horizon: FairDispatchHorizon
    preference: int = Field(default=0, ge=0, le=1)
    lane: FairLane
    service_class: SchedulingServiceClass
    workspace_sequence: int = Field(ge=0)
    workspace_key: str
    root_sequence: int = Field(ge=0)
    root_request_identifier: str


class FairDispatchPage[CandidateT](StrictModel):
    items: tuple[CandidateT, ...]
    next_cursor: FairDispatchCursor | None
    has_more: bool


class FairAdmission(StrictModel):
    scope: str
    lane: FairLane
    sequence: int = Field(ge=1)
    work_item_identifier: str
    token: str
    scheduling: SchedulingContext
    admitted_at_utc_ns: int = Field(ge=0)
