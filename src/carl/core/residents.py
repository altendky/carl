"""Immutable resident incarnations and ownership separate from stage execution."""

from pydantic import Field, field_validator, model_validator

from carl.core.models import JsonStringEnumeration, StrictModel
from carl.core.resources import ResourceLease, ResourceRequest
from carl.core.stages import StageRequestCharge

ResidentRequestCharge = StageRequestCharge


class ResidentState(JsonStringEnumeration):
    OPENING = "opening"
    READY = "ready"
    CLOSING = "closing"
    CLOSED = "closed"
    PARKED = "parked"
    LOST = "lost"
    QUARANTINED = "quarantined"


class ResidentDefinition(StrictModel):
    """Safe accounting metadata; credentials, cookies and actor state stay in memory."""

    identifier: str = Field(min_length=1)
    work_item_identifier: str = Field(min_length=1)
    operation_identifier: str = Field(min_length=1)
    state_bytes: int = Field(gt=0, le=2**63 - 1)

    @field_validator("identifier", "work_item_identifier", "operation_identifier")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Resident identities must be trimmed")
        return value


class ResidentLease(StrictModel):
    definition: ResidentDefinition
    generation: int = Field(ge=1)
    token: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    coordination_scope: str = Field(min_length=1)
    state: ResidentState
    active_stage_token: str | None = Field(default=None, min_length=1)
    resource_lease: ResourceLease

    @model_validator(mode="after")
    def validate_resource_fence(self) -> "ResidentLease":
        resource = self.resource_lease
        if (resource.token, resource.owner, resource.scope) != (
            self.token,
            self.owner,
            self.coordination_scope,
        ):
            raise ValueError("Resident and resource ownership must agree")
        if resource.allocations != (
            ResourceRequest(name="resident_sessions"),
            ResourceRequest(name="resident_state_bytes", units=self.definition.state_bytes),
        ):
            raise ValueError("Resident reservation must account for one actor and all its state")
        return self

    @property
    def identifier(self) -> str:
        return self.definition.identifier

    @property
    def expires_at_utc_ns(self) -> int:
        return self.resource_lease.expires_at_utc_ns
