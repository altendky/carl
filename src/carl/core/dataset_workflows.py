"""Bounded durable ownership contracts for a local dataset workflow."""

from pathlib import Path
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from carl.core.json import encode_json
from carl.core.models import JsonValue, StrictModel
from carl.core.travel_data import TravelDataSetupRequest

MAX_DATASET_JOURNAL_BYTES = 1_048_576
MAX_DATASET_CHECKPOINT_BYTES = 65_536
type DatasetWorkflowState = Literal[
    "running", "waiting", "parked", "quarantined", "completed", "failed", "cancelled"
]
TERMINAL_DATASET_WORKFLOW_STATES = frozenset(("completed", "failed", "cancelled"))


class DatasetWorkflowBinding(StrictModel):
    database_path: Path
    work_item_identifier: str = Field(min_length=1, max_length=200)
    operation_identifier: str = Field(min_length=1, max_length=200)

    @field_validator("database_path")
    @classmethod
    def absolute_database(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("Dataset workflow database path must be absolute")
        return value


class DatasetWorkflowDefinition(StrictModel):
    workflow_identifier: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,199}$")
    request: TravelDataSetupRequest
    recipe_identifier: str = Field(min_length=1, max_length=256)
    configuration_identity: str = Field(min_length=1, max_length=256)
    binding: DatasetWorkflowBinding | None = None

    @field_validator("recipe_identifier", "configuration_identity")
    @classmethod
    def trimmed_identity(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Dataset identities must be trimmed")
        return value


class DatasetWorkflowFence(StrictModel):
    workflow_identifier: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,199}$")
    generation: int = Field(ge=1)
    token: str = Field(pattern=r"^[0-9a-f]{32}$")


class DatasetWorkflowJournal(StrictModel):
    schema_version: Literal[1] = 1
    definition: DatasetWorkflowDefinition
    fence: DatasetWorkflowFence
    lock_identity: tuple[int, int]
    revision: int = Field(ge=1)
    state: DatasetWorkflowState = "running"
    phase: str | None = Field(default=None, min_length=1, max_length=200)
    plan: JsonValue = None
    plan_frozen: bool = False
    checkpoint: JsonValue = None
    receipt_references: tuple[str, ...] = Field(default=(), max_length=1000)
    current_stage_identifier: str | None = Field(default=None, min_length=1, max_length=200)
    next_eligible_at_utc_ns: int | None = Field(default=None, ge=0)
    updated_at_utc_ns: int = Field(ge=0)

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if self.fence.workflow_identifier != self.definition.workflow_identifier:
            raise ValueError("Dataset workflow fence and definition must agree")
        if any(part < 0 for part in self.lock_identity):
            raise ValueError("Invalid dataset lock identity")
        for value in (self.plan, self.checkpoint):
            if len(encode_json(value).encode()) > MAX_DATASET_CHECKPOINT_BYTES:
                raise ValueError("Dataset plan/checkpoint exceeds 64 KiB")
        if any(not ref or len(ref) > 2000 for ref in self.receipt_references):
            raise ValueError("Dataset receipt references must be bounded nonempty identities")
        if len(encode_json(self.model_dump(mode="json")).encode()) > MAX_DATASET_JOURNAL_BYTES:
            raise ValueError("Dataset journal exceeds 1 MiB")
        return self
