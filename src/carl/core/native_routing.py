"""Bounded immutable inputs to one owned local routing engine."""

from pathlib import Path

from pydantic import Field, field_validator

from carl.core.models import StrictModel
from carl.core.travel import TravelCoordinate

MAX_NATIVE_DESTINATIONS = 64


class NativeRoutingDataset(StrictModel):
    dataset_path: Path
    dataset_identifier: str = Field(min_length=1, max_length=200)
    use_mmap: bool = True

    @field_validator("dataset_path")
    @classmethod
    def absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute() or len(str(value).encode()) > 4096:
            raise ValueError("Native routing requires a bounded absolute dataset path")
        return value


class NativeRoutingQuery(StrictModel):
    request_identifier: str = Field(min_length=1, max_length=100)
    origin: TravelCoordinate
    destinations: tuple[TravelCoordinate, ...] = Field(
        min_length=1, max_length=MAX_NATIVE_DESTINATIONS
    )
    maximum_snap_distance_meters: float = Field(gt=0, le=20000)


type NativeRoutingMetric = tuple[float, float] | str
