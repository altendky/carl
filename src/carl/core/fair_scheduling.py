"""Explicit scheduling intent, independent of acquisition identity and constraints."""

from pydantic import Field

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


class FairDispatchCursor(StrictModel):
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
