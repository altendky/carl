"""Bounded scheduler observations distinguish ownership from consumed resources."""

from typing import Literal

from pydantic import Field, model_validator

from carl.core.models import StrictModel
from carl.core.stages import StageWaitKind
from carl.core.work import WorkState

type SchedulerResourceName = Literal[
    "http", "cpu", "analysis", "routing_engines", "resident_sessions", "resident_state_bytes"
]
type SchedulerBlocker = Literal[
    "dependency",
    "building_dependencies",
    "resource",
    "execution_runner",
    "metadata_runner",
    "time",
    "connectivity",
    "resident_ownership",
    "fairness",
    "logical_policy",
    "running",
    "none_of_checked_conditions",
    "terminal",
    "unknown",
]
type SchedulerMetric = Literal[
    "queue_delay", "observed_resource_wait", "phase_runtime", "physical_hold"
]
SCHEDULER_DURATION_BUCKETS_NS = (
    1_000_000,
    5_000_000,
    10_000_000,
    50_000_000,
    100_000_000,
    500_000_000,
    1_000_000_000,
    5_000_000_000,
    10_000_000_000,
    30_000_000_000,
    60_000_000_000,
    300_000_000_000,
    1_800_000_000_000,
)


class SchedulerCoverage(StrictModel):
    method: Literal["current_metadata", "observed_transitions", "recent_sample", "unavailable"]
    examined_rows: int = Field(default=0, ge=0)
    row_limit: int = Field(default=0, ge=0)
    truncated: bool = False
    instrumented_since_utc_ns: int | None = Field(default=None, ge=0)
    censored_count: int = Field(default=0, ge=0)


class SchedulerResourceRequirement(StrictModel):
    name: SchedulerResourceName
    units: int = Field(ge=1)


class SchedulerResourceActivity(StrictModel):
    name: SchedulerResourceName
    capacity: int | None = Field(default=None, ge=0)
    held_units: int = Field(ge=0)
    expired_held_units: int = Field(default=0, ge=0)
    oldest_held_age_ns: int | None = Field(default=None, ge=0)
    complete: bool = True

    @model_validator(mode="after")
    def validate_expired(self) -> "SchedulerResourceActivity":
        if self.expired_held_units > self.held_units:
            raise ValueError("Expired held units are part of the held allocation")
        return self


class SchedulerWorkCounts(StrictModel):
    pending: int = Field(default=0, ge=0)
    leased: int = Field(default=0, ge=0)
    completed: int = Field(default=0, ge=0)
    terminal_failure: int = Field(default=0, ge=0)
    cancelling: int = Field(default=0, ge=0)
    cancelled: int = Field(default=0, ge=0)
    complete: bool = True


class SchedulerStageCounts(StrictModel):
    pending: int = Field(default=0, ge=0)
    running: int = Field(default=0, ge=0)
    completed: int = Field(default=0, ge=0)
    cancelled: int = Field(default=0, ge=0)
    progress_waiting: int = Field(default=0, ge=0)
    complete: bool = True


class SchedulerWorkStatus(StrictModel):
    work_handle: str = Field(min_length=1, max_length=64)
    work_identifier: str | None = Field(default=None, max_length=128)
    stage_handle: str | None = Field(default=None, max_length=64)
    stage_identifier: str | None = Field(default=None, max_length=128)
    logical_state: WorkState
    stage_state: Literal["unregistered", "pending", "running", "completed", "cancelled", "none"]
    phase_family: Literal[
        "http", "cpu", "analysis", "coordinator", "resident", "dataset", "travel", "other"
    ] = "other"
    execution: int = Field(default=0, ge=0)
    execution_started_at_utc_ns: int | None = Field(default=None, ge=0)
    execution_age_ns: int | None = Field(default=None, ge=0)
    execution_age_method: Literal["utc_estimate", "unavailable"] = "unavailable"
    scope_handle: str | None = Field(default=None, max_length=64)
    resources: tuple[SchedulerResourceRequirement, ...] = Field(default=(), max_length=8)
    progress_state: Literal["building", "armed"] | None = None
    recorded_wait_kind: StageWaitKind | None = None
    recorded_observation_reason: SchedulerBlocker | None = None
    recorded_observation_at_utc_ns: int | None = Field(default=None, ge=0)
    observed_blockers: tuple[SchedulerBlocker, ...] = Field(default=(), max_length=8)
    blocked_resources: tuple[SchedulerResourceName, ...] = Field(default=(), max_length=8)
    observed_at_utc_ns: int = Field(ge=0)
    phase_ready_age_ns: int | None = Field(default=None, ge=0)
    wait_age_ns: int | None = Field(default=None, ge=0)
    wait_observation_source: Literal["advisory", "admission", "transition", "unavailable"] = (
        "unavailable"
    )
    checks_complete: bool = False
    identity_omitted: bool = False


class SchedulerWaitGroup(StrictModel):
    reason: SchedulerBlocker
    count: int = Field(ge=0)
    oldest_age_ns: int | None = Field(default=None, ge=0)
    sampled: bool = True


class SchedulerTimingSummary(StrictModel):
    metric: SchedulerMetric
    count: int = Field(default=0, ge=0)
    total_ns: int = Field(default=0, ge=0)
    maximum_ns: int | None = Field(default=None, ge=0)
    bucket_upper_bounds_ns: tuple[int, ...] = Field(
        default=SCHEDULER_DURATION_BUCKETS_NS, max_length=16
    )
    histogram_counts: tuple[int, ...] = Field(default=(), max_length=17)
    censored_count: int = Field(default=0, ge=0)
    method: Literal["monotonic", "utc_estimate", "observed_lower_bound", "unavailable"] = (
        "unavailable"
    )

    @model_validator(mode="after")
    def validate_histogram(self) -> "SchedulerTimingSummary":
        if (
            self.histogram_counts
            and len(self.histogram_counts) != len(self.bucket_upper_bounds_ns) + 1
        ):
            raise ValueError("A duration histogram needs one overflow bucket")
        if any(count < 0 for count in self.histogram_counts):
            raise ValueError("Histogram counts cannot be negative")
        return self


class SchedulerThroughput(StrictModel):
    logical_scope: Literal["database"] = "database"
    execution_scope: Literal["selected_host"] = "selected_host"
    window_start_utc_ns: int = Field(ge=0)
    window_end_utc_ns: int = Field(ge=0)
    logical_completed: int = Field(default=0, ge=0)
    logical_failed: int = Field(default=0, ge=0)
    logical_cancelled: int = Field(default=0, ge=0)
    stage_completed: int = Field(default=0, ge=0)
    stage_failed: int = Field(default=0, ge=0)
    stage_cancelled: int = Field(default=0, ge=0)
    admissions: int = Field(default=0, ge=0)
    complete: bool = False


class SchedulerFailureGroup(StrictModel):
    reason: Literal[
        "connectivity",
        "throttled",
        "provider_challenge",
        "capacity",
        "cancelled",
        "invalid_output",
        "other",
    ]
    count: int = Field(ge=0)
    cooldown_remaining_ns: int | None = Field(default=None, ge=0)


class SchedulerActivity(StrictModel):
    captured_at_utc_ns: int = Field(ge=0)
    coordination_boundary: Literal["database_and_host"] = "database_and_host"
    scope_handle: str = Field(min_length=1, max_length=64)
    scope_configured: bool
    pending_scope: Literal["database"] = "database"
    work_counts_scope: Literal["database"] = "database"
    stage_counts_scope: Literal["database"] = "database"
    resources: tuple[SchedulerResourceActivity, ...] = Field(default=(), max_length=8)
    work_counts: SchedulerWorkCounts = Field(default_factory=SchedulerWorkCounts)
    stage_counts: SchedulerStageCounts = Field(default_factory=SchedulerStageCounts)
    wait_groups: tuple[SchedulerWaitGroup, ...] = Field(default=(), max_length=8)
    samples: tuple[SchedulerWorkStatus, ...] = Field(default=(), max_length=8)
    timings: tuple[SchedulerTimingSummary, ...] = Field(default=(), max_length=8)
    failures: tuple[SchedulerFailureGroup, ...] = Field(default=(), max_length=8)
    throughput: SchedulerThroughput | None = None
    coverage: SchedulerCoverage
    omitted_resource_names: int = Field(default=0, ge=0)


class SchedulerWaitObservation(StrictModel):
    """Internal bounded-page input; raw identities never enter public dimensions."""

    work_identifier: str = Field(min_length=1)
    stage_identifier: str | None = Field(default=None, min_length=1)
    reason: SchedulerBlocker
    resource_names: tuple[SchedulerResourceName, ...] = Field(default=(), max_length=8)
    source: Literal["advisory", "admission"]
