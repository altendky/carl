"""Frozen GeoNames inputs and bounded candidate evidence between build phases."""

from typing import Literal

from pydantic import Field, model_validator

from carl.core.dataset_downloads import MAX_DATASET_FILE_BYTES, DatasetSource
from carl.core.models import StrictModel


class GeonamesBuildPlan(StrictModel):
    schema_version: Literal[2] = 2
    country_code: Literal["US"] = "US"
    sources: tuple[DatasetSource, ...] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def bounded_sources(self) -> "GeonamesBuildPlan":
        if (
            len({source.identifier for source in self.sources}) != len(self.sources)
            or len({source.name for source in self.sources}) != len(self.sources)
            or len(self.model_dump_json().encode()) > 65_536
        ):
            raise ValueError("GeoNames sources must be unique and fit a frozen 64 KiB plan")
        return self


class GeonamesCounts(StrictModel):
    places: int = Field(ge=1, le=MAX_DATASET_FILE_BYTES)
    postal_records: int = Field(ge=1, le=MAX_DATASET_FILE_BYTES)
    states: int = Field(ge=1, le=MAX_DATASET_FILE_BYTES)


class GeonamesCandidate(StrictModel):
    schema_version: Literal[1] = 1
    dataset_identifier: str = Field(pattern=r"^geonames-[0-9a-f]{24}$")
    candidate_locator: str = Field(pattern=r"^candidates/geonames/[A-Za-z0-9][A-Za-z0-9_-]{0,199}$")
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    database_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    database_bytes: int = Field(ge=1, le=MAX_DATASET_FILE_BYTES)
    counts: GeonamesCounts
