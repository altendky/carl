"""Bounded shared values for durable analysis input preparation."""

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal

from pydantic import Field, field_validator, model_validator

from carl.core.models import JsonValue, NamedInput, StrictModel

MAX_ANALYSIS_INPUT_BYTES = 100_000_000
MAX_ANALYSIS_INPUT_FILES = 1000
MAX_ANALYSIS_TEXT_BYTES = 4 * 1024 * 1024
MAX_ANALYSIS_STDOUT_BYTES = 8 * 1024 * 1024
MAX_ANALYSIS_STDERR_BYTES = 1024 * 1024
MAX_ANALYSIS_OUTPUT_BYTES = 8 * 1024 * 1024


class AnalysisImage(StrictModel):
    artifact_identifier: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    media_type: str = Field(min_length=1, max_length=256)
    byte_length: int = Field(ge=0, le=MAX_ANALYSIS_INPUT_BYTES)
    filename: str = Field(min_length=1, max_length=256)
    gallery_order: int = Field(ge=0)

    @field_validator("filename")
    @classmethod
    def safe_filename(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            path.parts[:1] != ("images",)
            or len(path.parts) != 2
            or path.name in {".", ".."}
            or "\\" in value
            or any(ord(character) < 32 or ord(character) == 127 for character in value)
            or value != str(path)
        ):
            raise ValueError("Analysis images require a single safe filename in images/")
        return value


@dataclass(frozen=True, slots=True)
class PreparedAnalysis:
    listing_input: bytes
    prompt: bytes
    images: tuple[AnalysisImage, ...]
    inputs: tuple[NamedInput, ...] = ()


class AnalysisInputManifest(StrictModel):
    marketplace: Literal["facebook", "ebay"]
    configuration: dict[str, JsonValue]
    inputs: tuple[NamedInput, ...] = Field(max_length=1000)
    images: tuple[AnalysisImage, ...] = Field(max_length=MAX_ANALYSIS_INPUT_FILES)
    listing_input_identifier: str = Field(min_length=1)
    prompt_identifier: str = Field(min_length=1)

    @model_validator(mode="after")
    def unique_images(self) -> "AnalysisInputManifest":
        for values in (
            tuple(image.filename for image in self.images),
            tuple(image.gallery_order for image in self.images),
        ):
            if len(values) != len(set(values)):
                raise ValueError("Analysis images require unique filenames and gallery order")
        if len({item.name for item in self.inputs}) != len(self.inputs):
            raise ValueError("Analysis source input names must be unique")
        return self
