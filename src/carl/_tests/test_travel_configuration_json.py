"""Durable travel snapshots and TOML loading accept the same absolute paths."""

import json
from pathlib import Path

import pytest

from carl.core.configuration import GeonamesConfiguration, OsrmConfiguration


@pytest.mark.parametrize(
    ("model", "field"),
    ((OsrmConfiguration, "dataset_path"), (GeonamesConfiguration, "database_path")),
)
def test_travel_dataset_paths_roundtrip_python_and_json(
    tmp_path: Path, model: type[OsrmConfiguration] | type[GeonamesConfiguration], field: str
) -> None:
    values = {field: str(tmp_path / "dataset"), "dataset_identifier": "fixture-v1"}
    configuration = model.model_validate(values)
    assert getattr(configuration, field) == tmp_path / "dataset"
    assert model.model_validate_json(configuration.model_dump_json()) == configuration


@pytest.mark.parametrize(
    ("model", "field"),
    ((OsrmConfiguration, "dataset_path"), (GeonamesConfiguration, "database_path")),
)
def test_relative_travel_dataset_paths_remain_invalid(
    model: type[OsrmConfiguration] | type[GeonamesConfiguration], field: str
) -> None:
    values = {field: "relative/dataset", "dataset_identifier": "fixture-v1"}
    with pytest.raises(ValueError, match="absolute"):
        _ = model.model_validate(values)
    with pytest.raises(ValueError, match="absolute"):
        _ = model.model_validate_json(json.dumps(values))
