"""Bounded queue metadata and optimistic initial-registration fences."""

from typing import Literal

from pydantic import Field

from carl.core.models import StrictModel
from carl.core.work import WorkCapability, WorkDefinition, WorkState


class DispatchCursor(StrictModel):
    priority: int
    eligible_at_utc_ns: int = Field(ge=0)
    created_at_utc_ns: int = Field(ge=0)
    work_item_identifier: str = Field(min_length=1)


class DispatchCandidate(StrictModel):
    work_item_identifier: str
    capability: WorkCapability
    state: WorkState
    attempt: int = Field(ge=0)
    priority: int
    eligible_at_utc_ns: int = Field(ge=0)
    created_at_utc_ns: int = Field(ge=0)
    stage_identifier: str | None = None
    operation_identifier: str | None = None
    stage_kind: tuple[str, ...] | None = None
    stage_schema_version: int | None = None
    resident_session_identifier: str | None = None

    @property
    def cursor(self) -> DispatchCursor:
        return DispatchCursor(
            priority=self.priority,
            eligible_at_utc_ns=self.eligible_at_utc_ns,
            created_at_utc_ns=self.created_at_utc_ns,
            work_item_identifier=self.work_item_identifier,
        )


class DispatchPage(StrictModel):
    items: tuple[DispatchCandidate, ...]
    next_cursor: DispatchCursor | None
    has_more: bool


class WorkDispatchSnapshot(StrictModel):
    definition: WorkDefinition
    state: WorkState
    attempt: int = Field(ge=0)
    active_operation_identifier: str | None = None
    active_attempt_state: str | None = None
    active_stage_identifier: str | None = None
    active_stage_state: str | None = None
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class WorkSnapshotStale(ValueError):
    """Abort an enclosing registration/resident transaction before declining."""

    def __init__(self, existing_stage_identifier: str | None = None) -> None:
        super().__init__("Work dispatch snapshot changed before initial registration")
        self.existing_stage_identifier: str | None = existing_stage_identifier


class InitialStageRegistration(StrictModel):
    status: Literal["registered", "existing", "stale"]
    stage_identifier: str | None = None


class ExpiryCursor(StrictModel):
    expires_at_utc_ns: int = Field(ge=0)
    identifier: str = Field(min_length=1)


class ExpiredStageCandidate(StrictModel):
    stage_identifier: str
    token: str
    worker_identifier: str
    coordination_scope: str
    execution: int = Field(ge=1)
    expires_at_utc_ns: int = Field(ge=0)

    @property
    def cursor(self) -> ExpiryCursor:
        return ExpiryCursor(
            expires_at_utc_ns=self.expires_at_utc_ns, identifier=self.stage_identifier
        )


class ExpiredStagePage(StrictModel):
    items: tuple[ExpiredStageCandidate, ...]
    next_cursor: ExpiryCursor | None
    has_more: bool


class ExpiredLegacyWorkCandidate(StrictModel):
    work_item_identifier: str
    state: WorkState
    attempt: int = Field(ge=1)
    lease_token: str
    worker_identifier: str
    lease_expires_at_utc_ns: int = Field(ge=0)
    operation_identifier: str | None = None

    @property
    def cursor(self) -> ExpiryCursor:
        return ExpiryCursor(
            expires_at_utc_ns=self.lease_expires_at_utc_ns, identifier=self.work_item_identifier
        )


class ExpiredLegacyWorkPage(StrictModel):
    items: tuple[ExpiredLegacyWorkCandidate, ...]
    next_cursor: ExpiryCursor | None
    has_more: bool
