"""Bounded inputs and public results for synthetic offline scheduler benchmarks."""

from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from carl.core.json import encode_json
from carl.core.models import JsonStringEnumeration, StrictModel
from carl.core.scheduler_activity import SchedulerActivity


class BenchmarkScenario(JsonStringEnumeration):
    HTTP_IMAGES = "http-images"
    CPU_DECODE_PRESSURE = "cpu-decode-pressure"
    ANALYSIS_PRESSURE = "analysis-pressure"
    REQUESTER_FAIRNESS = "requester-fairness"
    IDLE_PROGRESS_ROOTS = "idle-progress-roots"
    WATCH_HISTORY_FANOUT = "watch-history-fanout"
    OUTAGE_AND_PACING = "outage-and-pacing"
    CANCEL_CHECKPOINT_RESTART = "cancel-checkpoint-restart"


class BenchmarkMode(JsonStringEnumeration):
    VERIFY = "verify"
    THROUGHPUT = "throughput"


class BenchmarkConfig(StrictModel):
    scenario: BenchmarkScenario
    mode: BenchmarkMode = BenchmarkMode.VERIFY
    seed: int = Field(default=1, ge=0, le=2**32 - 1)
    items: int = Field(default=32, ge=2, le=10_000)
    repetitions: int = Field(default=1, ge=1, le=10)
    warmups: int = Field(default=0, ge=0, le=2)
    image_width: int = Field(default=1024, ge=8, le=4096)
    image_height: int = Field(default=1024, ge=8, le=4096)
    chunk_bytes: int = Field(default=16_384, ge=1024, le=262_144)
    chunk_delay_ms: int = Field(default=1, ge=0, le=1000)
    analysis_delay_ms: int = Field(default=50, ge=0, le=10_000)
    history_rounds: int = Field(default=8, ge=1, le=1000)
    timeout_seconds: int = Field(default=120, ge=5, le=600)
    capacities: dict[str, int] = Field(default_factory=dict, max_length=6)

    @field_validator("capacities")
    @classmethod
    def capacity_bounds(cls, value: dict[str, int]) -> dict[str, int]:
        maxima = {
            "http": 256,
            "cpu": 64,
            "analysis": 64,
            "routing_engines": 16,
            "resident_sessions": 256,
            "resident_state_bytes": 2**31,
        }
        if any(key not in maxima or not 1 <= count <= maxima[key] for key, count in value.items()):
            raise ValueError(
                "Benchmark resource overrides require known positive bounded capacities"
            )
        return value

    @model_validator(mode="after")
    def bounded_history(self) -> Self:
        if (
            self.scenario == BenchmarkScenario.WATCH_HISTORY_FANOUT
            and self.items * self.history_rounds > 100_000
        ):
            raise ValueError("Historical membership setup exceeds100000 rows")
        if self.scenario == BenchmarkScenario.REQUESTER_FAIRNESS and self.items > 1000:
            raise ValueError("Fairness audit is bounded to1000 turns")
        return self


class BenchmarkIteration(StrictModel):
    setup_ns: int = Field(ge=0)
    elapsed_ns: int = Field(ge=1)
    cleanup_ns: int = Field(ge=0)
    completed: int = Field(ge=0)
    failed: int = Field(ge=0)
    cancelled: int = Field(ge=0)
    completed_per_second: float | None = Field(default=None, ge=0)
    capture_invocations: int = Field(ge=0)
    analysis_invocations: int = Field(ge=0)
    synthetic_parent_executions: int = Field(ge=0)
    sampled_peak_held_units: dict[str, int] = Field(max_length=6)
    sample_interval_ns: int = Field(default=10_000_000, ge=1)
    counters: dict[str, int] = Field(default_factory=dict, max_length=24)
    scheduler_activity: SchedulerActivity | None = None
    checks: tuple[str, ...] = Field(max_length=24)


class BenchmarkReport(StrictModel):
    schema_version: Literal[1] = 1
    workload: Literal["synthetic_offline"] = "synthetic_offline"
    timed_scope: Literal["dispatch_release_or_settled_wait_through_observation"] = (
        "dispatch_release_or_settled_wait_through_observation"
    )
    configuration: BenchmarkConfig
    configuration_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_commit: str | None = None
    source_worktree_state: str
    python_version: str
    pillow_version: str
    sqlite_version: str
    capacities: dict[str, int] = Field(max_length=6)
    iterations: tuple[BenchmarkIteration, ...] = Field(min_length=1, max_length=10)
    coverage: tuple[str, ...] = Field(max_length=16)

    @model_validator(mode="after")
    def bounded_output(self) -> Self:
        if len(encode_json(self.model_dump(mode="json")).encode()) > 262_144:
            raise ValueError("Benchmark report exceeds256KiB")
        return self
