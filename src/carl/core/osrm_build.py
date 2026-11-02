"""Frozen OSRM input plans and coarse native output receipts."""

import hashlib
from datetime import date, timedelta
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from carl.core.dataset_downloads import FileReceipt
from carl.core.models import StrictModel
from carl.core.travel import TravelCoordinate

OSRM_BUILD_RECIPE = "carl-osrm-build-v1"
MAX_OSRM_SOURCES = 128
MAX_OSRM_RECEIPT_BYTES = 1_048_576


class OsrmExecutable(StrictModel):
    path: Path
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("path")
    @classmethod
    def absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute() or len(str(value).encode()) > 4096:
            raise ValueError("OSRM executable path must be bounded and absolute")
        return value


class OsrmToolchain(StrictModel):
    osmium: OsrmExecutable
    extract: OsrmExecutable
    partition: OsrmExecutable
    customize: OsrmExecutable


class OsrmPlanSeed(StrictModel):
    schema_version: Literal[1] = 1
    recipe_identifier: Literal["carl-osrm-build-v1"] = OSRM_BUILD_RECIPE
    origin: TravelCoordinate
    radius_miles: float = Field(ge=1, le=1000, allow_inf_nan=False)
    threads: int = Field(ge=1, le=16)
    anchor_date: date
    coverage_locator: Literal["osrm-coverage.geojson"] = "osrm-coverage.geojson"
    coverage_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    profile_locator: Literal["osrm-profile/car.lua"] = "osrm-profile/car.lua"
    profile_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    package_version: str = Field(min_length=1, max_length=100)
    toolchain: OsrmToolchain


class OsrmSourceCandidate(StrictModel):
    id: str = Field(pattern=r"^us/[a-z-]{1,100}$")
    latest_url: str = Field(
        pattern=r"^https://download\.geofabrik\.de/north-america/us/[a-z-]{1,100}-latest\.osm\.pbf$"
    )

    @model_validator(mode="after")
    def matching_name(self) -> Self:
        if self.latest_url.rsplit("/", 1)[-1] != self.id[3:] + "-latest.osm.pbf":
            raise ValueError("OSRM source URL and state identifier differ")
        return self


class OsrmDatedSource(StrictModel):
    id: str = Field(pattern=r"^us/[a-z-]{1,100}$")
    url: str = Field(
        pattern=r"^https://download\.geofabrik\.de/north-america/us/[a-z-]{1,100}-[0-9]{6}\.osm\.pbf$"
    )
    snapshot: str = Field(pattern=r"^[0-9]{6}$")
    md5: str = Field(pattern=r"^[0-9a-f]{32}$")

    @model_validator(mode="after")
    def matching_name(self) -> Self:
        if self.url.rsplit("/", 1)[-1] != self.id[3:] + "-" + self.snapshot + ".osm.pbf":
            raise ValueError("OSRM dated source identity differs")
        return self


class OsrmBuildPlan(StrictModel):
    seed: OsrmPlanSeed
    sources: tuple[OsrmDatedSource, ...] = Field(min_length=1, max_length=MAX_OSRM_SOURCES)

    @model_validator(mode="after")
    def coherent_sources(self) -> Self:
        identifiers = tuple(source.id for source in self.sources)
        if identifiers != tuple(sorted(set(identifiers))):
            raise ValueError("OSRM sources must be unique and sorted")
        if len({source.snapshot for source in self.sources}) != 1:
            raise ValueError("OSRM sources require one dated snapshot")
        if self.sources[0].snapshot not in {
            (self.seed.anchor_date - timedelta(days=age)).strftime("%y%m%d") for age in range(1, 8)
        }:
            raise ValueError("OSRM snapshot must belong to the frozen seven-day probe window")
        if len(self.model_dump_json().encode()) > 65_536:
            raise ValueError("OSRM frozen plan exceeds 64 KiB")
        return self

    @property
    def identifier(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()


class OsrmBuildAttempt(StrictModel):
    identifier: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    plan_identifier: str = Field(pattern=r"^[0-9a-f]{64}$")

    @property
    def locator(self) -> str:
        return "osrm-attempts/" + self.identifier


class OsrmAttemptInputs(StrictModel):
    schema_version: Literal[1] = 1
    plan_identifier: str = Field(pattern=r"^[0-9a-f]{64}$")
    sources: tuple[FileReceipt, ...] = Field(min_length=1, max_length=MAX_OSRM_SOURCES)

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if len(self.model_dump_json().encode()) > MAX_OSRM_RECEIPT_BYTES:
            raise ValueError("OSRM source receipt manifest exceeds 1 MiB")
        return self


class OsrmSourceEvidence(OsrmDatedSource):
    bytes: int = Field(ge=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    data_timestamp_utc: str = Field(
        pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
    )


class OsrmOutputFile(StrictModel):
    name: str = Field(pattern=r"^region\.osrm\.[A-Za-z0-9_.-]{1,200}$")
    bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class OsrmBuildMeasurement(StrictModel):
    stage: str = Field(min_length=1, max_length=128)
    wall_seconds: float = Field(ge=0, allow_inf_nan=False)


class OsrmCoarseReceipt(StrictModel):
    schema_version: Literal[1] = 1
    plan_identifier: str = Field(pattern=r"^[0-9a-f]{64}$")
    attempt_identifier: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    dataset_identifier: str = Field(pattern=r"^osrm-[0-9a-f]{24}$")
    osrm_version: str = Field(min_length=1, max_length=100)
    sources: tuple[OsrmSourceEvidence, ...] = Field(min_length=1, max_length=MAX_OSRM_SOURCES)
    files: tuple[OsrmOutputFile, ...] = Field(min_length=1, max_length=1000)
    measurements: tuple[OsrmBuildMeasurement, ...] = Field(default=(), max_length=300)
    created_at_utc: str = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def bounded_and_sorted(self) -> Self:
        names = tuple(file.name for file in self.files)
        if names != tuple(sorted(set(names))):
            raise ValueError("OSRM output inventory must be unique and sorted")
        if len({source.data_timestamp_utc for source in self.sources}) != 1:
            raise ValueError("OSRM source replication timestamps must agree")
        if len(self.model_dump_json().encode()) > MAX_OSRM_RECEIPT_BYTES:
            raise ValueError("OSRM coarse receipt exceeds 1 MiB")
        return self
