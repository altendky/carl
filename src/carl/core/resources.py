"""Scoped resource allocations and fenced durable permit ownership."""

from pydantic import Field, field_validator, model_validator

from carl.core.models import StrictModel


class ResourceRequest(StrictModel):
    name: str = Field(min_length=1, max_length=100)
    units: int = Field(default=1, gt=0, strict=True)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Resource names must be trimmed")
        return value


class ResourceLease(StrictModel):
    """One atomic allocation within a database and explicit coordination scope."""

    scope: str = Field(min_length=1)
    token: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    allocations: tuple[ResourceRequest, ...] = Field(min_length=1)
    acquired_at_utc_ns: int = Field(ge=0)
    expires_at_utc_ns: int = Field(ge=0)

    @field_validator("scope", "token", "owner")
    @classmethod
    def validate_identity(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Resource permit identities must be trimmed")
        return value

    @model_validator(mode="after")
    def validate_lease(self) -> "ResourceLease":
        names = tuple(request.name for request in self.allocations)
        if len(names) != len(set(names)):
            raise ValueError("Resource allocations may not repeat a budget")
        if self.expires_at_utc_ns <= self.acquired_at_utc_ns:
            raise ValueError("Resource permit expiry must follow acquisition")
        return self
