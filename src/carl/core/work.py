"""Typed values for durable work scheduling."""

from typing import Annotated, Literal

from pydantic import Field, TypeAdapter, field_validator, model_validator

from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel


class WorkState(JsonStringEnumeration):
    PENDING = "pending"
    LEASED = "leased"
    COMPLETED = "completed"
    TERMINAL_FAILURE = "terminal_failure"


class WorkEventKind(JsonStringEnumeration):
    ENQUEUED = "enqueued"
    REQUESTER_ATTACHED = "requester_attached"
    CLAIMED = "claimed"
    LEASE_EXPIRED = "lease_expired"
    LEASE_RENEWED = "lease_renewed"
    RELEASED = "released"
    COMPLETED = "completed"
    TERMINAL_FAILURE = "terminal_failure"


class SchedulingScopeKind(JsonStringEnumeration):
    OVERALL = "overall"
    NETWORK_PATH = "network_path"
    WORK_KIND = "work_kind"
    NETWORK_ACTIVITY_KIND = "network_activity_kind"
    REMOTE_ORIGIN = "remote_origin"


class SchedulingSubjectKind(JsonStringEnumeration):
    WORK_ITEM = "work_item"
    NETWORK_ACTIVITY = "network_activity"


class ConstraintKind(JsonStringEnumeration):
    CONCURRENCY = "concurrency"
    SLIDING_WINDOW_RATE = "sliding_window_rate"
    UNIFORM_HOLDOFF = "uniform_holdoff"


def _validate_parts(value: tuple[str, ...], *, allow_empty: bool = False) -> tuple[str, ...]:
    if (not value and not allow_empty) or any(not part for part in value):
        raise ValueError("Identity parts must be nonempty strings")
    return value


class SchedulingScope(StrictModel):
    kind: SchedulingScopeKind
    identity: tuple[str, ...]

    @model_validator(mode="after")
    def validate_identity(self) -> "SchedulingScope":
        _validate_parts(self.identity, allow_empty=self.kind is SchedulingScopeKind.OVERALL)
        if self.kind is SchedulingScopeKind.OVERALL and self.identity:
            raise ValueError("The overall scope has an empty identity")
        return self


class WorkDefinition(StrictModel):
    identifier: str = Field(min_length=1)
    kind: tuple[str, ...]
    payload_schema_version: int = Field(ge=1)
    payload: JsonValue
    deduplication_identity: tuple[str, ...]
    priority: int = 0
    not_before_utc_ns: int = Field(ge=0)
    scopes: tuple[SchedulingScope, ...]

    @field_validator("kind", "deduplication_identity")
    @classmethod
    def validate_parts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_parts(value)

    @model_validator(mode="after")
    def validate_scopes(self) -> "WorkDefinition":
        identities = {(scope.kind, scope.identity) for scope in self.scopes}
        if len(identities) != len(self.scopes):
            raise ValueError("Work scopes must be unique")
        if (SchedulingScopeKind.OVERALL, ()) not in identities:
            raise ValueError("Every work item requires the overall scope")
        if not any(scope.kind is SchedulingScopeKind.WORK_KIND for scope in self.scopes):
            raise ValueError("Every work item requires a work-kind scope")
        return self


class WorkRequester(StrictModel):
    request_identifier: str = Field(min_length=1)
    kind: tuple[str, ...]
    identifier: str = Field(min_length=1)
    context: JsonValue

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_parts(value)


class WorkCapability(StrictModel):
    kind: tuple[str, ...]
    payload_schema_version: int = Field(ge=1)

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_parts(value)


class ConcurrencyConstraint(StrictModel):
    kind: Literal[ConstraintKind.CONCURRENCY] = ConstraintKind.CONCURRENCY
    identifier: tuple[str, ...]
    schema_version: int = Field(default=1, ge=1)
    subject_kind: SchedulingSubjectKind = SchedulingSubjectKind.WORK_ITEM
    scope: SchedulingScope
    maximum_active: int = Field(ge=1)

    @field_validator("identifier")
    @classmethod
    def validate_identifier(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_parts(value)


class SlidingWindowRateConstraint(StrictModel):
    kind: Literal[ConstraintKind.SLIDING_WINDOW_RATE] = ConstraintKind.SLIDING_WINDOW_RATE
    identifier: tuple[str, ...]
    schema_version: int = Field(default=1, ge=1)
    subject_kind: SchedulingSubjectKind = SchedulingSubjectKind.WORK_ITEM
    scope: SchedulingScope
    maximum_starts: int = Field(ge=1)
    period_ns: int = Field(gt=0)

    @field_validator("identifier")
    @classmethod
    def validate_identifier(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_parts(value)


class UniformHoldoffConstraint(StrictModel):
    kind: Literal[ConstraintKind.UNIFORM_HOLDOFF] = ConstraintKind.UNIFORM_HOLDOFF
    identifier: tuple[str, ...]
    schema_version: int = Field(default=1, ge=1)
    subject_kind: SchedulingSubjectKind = SchedulingSubjectKind.WORK_ITEM
    scope: SchedulingScope
    minimum_ns: int = Field(ge=0)
    maximum_ns: int = Field(ge=0)

    @field_validator("identifier")
    @classmethod
    def validate_identifier(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_parts(value)

    @model_validator(mode="after")
    def validate_range(self) -> "UniformHoldoffConstraint":
        if self.maximum_ns < self.minimum_ns:
            raise ValueError("Holdoff maximum must not be less than its minimum")
        return self


type Constraint = Annotated[
    ConcurrencyConstraint | SlidingWindowRateConstraint | UniformHoldoffConstraint,
    Field(discriminator="kind"),
]

CONSTRAINT_ADAPTER = TypeAdapter(Constraint)


class HoldoffDecision(StrictModel):
    constraint_identifier: tuple[str, ...]
    sampled_delay_ns: int = Field(ge=0)

    @field_validator("constraint_identifier")
    @classmethod
    def validate_identifier(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_parts(value)


class NetworkActivityState(JsonStringEnumeration):
    PENDING = "pending"
    ADMITTED = "admitted"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"
    CANCELLED = "cancelled"


class NetworkActivityEventKind(JsonStringEnumeration):
    CREATED = "created"
    ADMITTED = "admitted"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"
    CANCELLED = "cancelled"


class NetworkActivityDefinition(StrictModel):
    identifier: str = Field(min_length=1)
    kind: tuple[str, ...]
    operation_identifier: str = Field(min_length=1)
    network_session_identifier: str = Field(min_length=1)
    ordinal: int = Field(ge=1)
    attempt: int = Field(default=1, ge=1)
    not_before_utc_ns: int = Field(default=0, ge=0)
    scopes: tuple[SchedulingScope, ...]

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_parts(value)

    @model_validator(mode="after")
    def validate_scopes(self) -> "NetworkActivityDefinition":
        identities = {(scope.kind, scope.identity) for scope in self.scopes}
        if len(identities) != len(self.scopes):
            raise ValueError("Network activity scopes must be unique")
        if (SchedulingScopeKind.OVERALL, ()) not in identities:
            raise ValueError("Every network activity requires the overall scope")
        if not any(
            scope.kind is SchedulingScopeKind.NETWORK_ACTIVITY_KIND for scope in self.scopes
        ):
            raise ValueError("Every network activity requires an activity-kind scope")
        return self


class NetworkActivityAdmission(StrictModel):
    activity_identifier: str = Field(min_length=1)
    token: str = Field(min_length=1)
    admitted_at_utc_ns: int = Field(ge=0)
    permit_expires_at_utc_ns: int = Field(ge=0)


class NetworkActivityAdmissionResult(StrictModel):
    admission: NetworkActivityAdmission | None
    next_eligible_at_utc_ns: int | None


class WorkLease(StrictModel):
    work_item_identifier: str = Field(min_length=1)
    token: str = Field(min_length=1)
    worker_identifier: str = Field(min_length=1)
    attempt: int = Field(ge=1)
    expires_at_utc_ns: int = Field(ge=0)
    kind: tuple[str, ...]
    payload_schema_version: int = Field(ge=1)
    payload: JsonValue

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_parts(value)


class ClaimResult(StrictModel):
    lease: WorkLease | None
    next_eligible_at_utc_ns: int | None


class EnqueueResult(StrictModel):
    work_item_identifier: str = Field(min_length=1)
    created: bool
    eligible_at_utc_ns: int = Field(ge=0)
