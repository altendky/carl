"""Pure models for interpreting one durable work attempt."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from carl.core.models import (
    ArtifactDraft,
    JsonStringEnumeration,
    JsonValue,
    NamedInput,
    NamedOutput,
    RecordDraft,
    StrictModel,
)
from carl.core.work import HoldoffDecision, WorkDefinition, WorkRequester


class WorkOutcomeKind(JsonStringEnumeration):
    COMPLETED = "completed"
    RETRY = "retry"
    TERMINAL_FAILURE = "terminal_failure"


class AttemptContext(StrictModel):
    work_item_identifier: str = Field(min_length=1)
    lease_token: str = Field(min_length=1)
    worker_identifier: str = Field(min_length=1)
    attempt: int = Field(ge=1)
    operation_identifier: str = Field(min_length=1)
    outage_attempts: tuple[int, ...] = ()
    retry_budget_start_attempt: int = Field(default=0, ge=0)

    def retry_attempt(self, offset: int = 0) -> int:
        """Keep physical ordinals, but exempt confirmed outage attempts from budgets."""

        baseline = max(offset, self.retry_budget_start_attempt)
        return (
            self.attempt
            - baseline
            - sum(baseline < attempt < self.attempt for attempt in self.outage_attempts)
        )


class WorkerSettings(StrictModel):
    worker_count: int = Field(ge=1)
    lease_duration_ns: int = Field(gt=0)
    renewal_interval_ns: int = Field(gt=0)
    idle_poll_interval_ns: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_renewal_interval(self) -> "WorkerSettings":
        if self.renewal_interval_ns >= self.lease_duration_ns:
            raise ValueError("Lease renewal must occur before lease expiration")
        return self


class FollowOnWork(StrictModel):
    definition: WorkDefinition
    requester: WorkRequester
    event_identifier: str = Field(min_length=1)
    holdoff_decisions: tuple[HoldoffDecision, ...] = ()


class CompletedWork(StrictModel):
    kind: Literal[WorkOutcomeKind.COMPLETED] = WorkOutcomeKind.COMPLETED
    inputs: tuple[NamedInput, ...] = ()
    records: tuple[RecordDraft, ...] = ()
    artifacts: tuple[ArtifactDraft, ...] = ()
    outputs: tuple[NamedOutput, ...] = ()
    follow_on_work: tuple[FollowOnWork, ...] = ()
    result: JsonValue


class RetryWork(StrictModel):
    kind: Literal[WorkOutcomeKind.RETRY] = WorkOutcomeKind.RETRY
    inputs: tuple[NamedInput, ...] = ()
    records: tuple[RecordDraft, ...] = ()
    artifacts: tuple[ArtifactDraft, ...] = ()
    outputs: tuple[NamedOutput, ...] = ()
    delay_ns: int = Field(ge=0)
    reason: JsonValue
    result: JsonValue


class TerminalFailureWork(StrictModel):
    kind: Literal[WorkOutcomeKind.TERMINAL_FAILURE] = WorkOutcomeKind.TERMINAL_FAILURE
    inputs: tuple[NamedInput, ...] = ()
    records: tuple[RecordDraft, ...] = ()
    artifacts: tuple[ArtifactDraft, ...] = ()
    outputs: tuple[NamedOutput, ...] = ()
    error: JsonValue
    result: JsonValue


type WorkOutcome = Annotated[
    CompletedWork | RetryWork | TerminalFailureWork,
    Field(discriminator="kind"),
]
