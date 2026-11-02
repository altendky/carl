"""Bounded references and replayable coordinator state for dataset setup."""

from typing import Self

from pydantic import Field, model_validator

from carl.core.dataset_workflows import DatasetWorkflowDefinition
from carl.core.json import encode_json
from carl.core.models import JsonValue, StrictModel

MAX_DATASET_SETUP_VALUE_BYTES = 1_048_576
MAX_DATASET_SETUP_STATE_BYTES = 65_536


class DatasetSetupReference(StrictModel):
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    bytes: int = Field(ge=1, le=MAX_DATASET_SETUP_VALUE_BYTES)

    @property
    def locator(self) -> str:
        return f"setup-values/{self.sha256}.json"


class DatasetSetupState(StrictModel):
    definition: DatasetWorkflowDefinition
    phase: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}$")
    value: JsonValue = Field(default_factory=dict)

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if len(encode_json(self.value).encode()) > MAX_DATASET_SETUP_STATE_BYTES:
            raise ValueError("Dataset setup value exceeds 64 KiB")
        if len(self.model_dump_json().encode()) > MAX_DATASET_SETUP_STATE_BYTES:
            raise ValueError("Dataset setup state exceeds 64 KiB")
        return self
