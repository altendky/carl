"""OSRM planning preserves dated coherent sources and exact coverage/profile bytes."""

# These provider helpers preserve the original dataset identity algorithm.
# pyright: reportPrivateUsage=false

import datetime
import hashlib
import json
import shutil
import sys
from pathlib import Path

import httpx
import pytest
from shapely.geometry import mapping

from carl.core.osrm_build import OsrmPlanSeed, OsrmToolchain
from carl.core.travel import TravelCoordinate
from carl.io import osrm_build as osrm_data

ORIGIN = TravelCoordinate(latitude=40, longitude=-77)


def _index() -> dict[str, object]:
    return {
        "features": [
            {
                "properties": {
                    "id": "us/" + name,
                    "urls": {
                        "pbf": "https://download.geofabrik.de/north-america/us/"
                        + name
                        + "-latest.osm.pbf"
                    },
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-80, 38], [-74, 38], [-74, 42], [-80, 42], [-80, 38]]],
                },
            }
            for name in ("maryland", "pennsylvania")
        ]
    }


def _seed(tmp_path: Path) -> OsrmPlanSeed:
    profile = tmp_path / "profiles" / "car.lua"
    profile.parent.mkdir(mode=0o700)
    _ = profile.write_text("return {}")
    executable = osrm_data._executable(Path(sys.executable))
    tools = OsrmToolchain(
        osmium=executable, extract=executable, partition=executable, customize=executable
    )
    return osrm_data.prepare_osrm_seed(
        tmp_path / "workspace",
        origin=ORIGIN,
        toolchain=tools,
        profile=profile,
        package_version="26.10.0",
        anchor_date=datetime.date(2026, 10, 18),
    )


def test_frozen_date_and_checksum_plan_never_downloads_moving_latest(tmp_path: Path) -> None:
    seed = _seed(tmp_path)
    candidates = osrm_data.select_osrm_sources(json.dumps(_index()).encode(), seed)
    assert tuple(candidate.id for candidate in candidates) == ("us/maryland", "us/pennsylvania")
    for age in range(1, 8):
        probes = osrm_data.osrm_probe_sources(candidates, anchor_date=seed.anchor_date, age=age)
        expected = (seed.anchor_date - datetime.timedelta(days=age)).strftime("%y%m%d")
        assert all(expected in source.url and "latest" not in source.url for source in probes)
        assert all(
            source.maximum_bytes == 4096 and not source.follow_redirects for source in probes
        )
    plan = osrm_data.freeze_osrm_plan(seed, candidates, age=2, checksums=("A" * 32, "b" * 32))
    assert {source.snapshot for source in plan.sources} == {"261016"}
    assert plan.sources[0].md5 == "a" * 32
    assert osrm_data.osrm_download_sources(plan)[0].expected_md5 == "a" * 32
    assert seed == OsrmPlanSeed.model_validate_json(seed.model_dump_json())
    with pytest.raises(osrm_data.OsrmBuildError, match="coherent_snapshot_unavailable"):
        _ = osrm_data.osrm_probe_sources(candidates, anchor_date=seed.anchor_date, age=8)


@pytest.mark.parametrize("value", (b"", b"not-a-checksum", b"a" * 4097, b"\xff"))
def test_invalid_checksums_are_bounded_and_classified(value: bytes) -> None:
    with pytest.raises(osrm_data.OsrmBuildError, match="osrm_source_checksum_invalid"):
        _ = osrm_data.parse_osrm_checksum(value)


def test_coverage_and_profile_snapshots_preserve_exact_legacy_bytes(tmp_path: Path) -> None:
    seed = _seed(tmp_path)
    directory = tmp_path / "workspace"
    expected = (
        json.dumps(
            {
                "type": "Feature",
                "properties": {"radius_miles": 250},
                "geometry": mapping(osrm_data._coverage(ORIGIN, 250)),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    assert (directory / seed.coverage_locator).read_bytes() == expected
    assert seed.coverage_sha256 == hashlib.sha256(expected).hexdigest()
    assert seed.profile_sha256 == osrm_data._profile_hash(tmp_path / "profiles" / "car.lua")
    _ = (tmp_path / "profiles" / "car.lua").write_text("modified installed profile")
    assert (directory / seed.profile_locator).read_text() == "return {}"
    assert seed.profile_sha256 == osrm_data._profile_hash(directory / seed.profile_locator)
    with pytest.raises(osrm_data.OsrmBuildError, match="coverage_crosses_date_line"):
        _ = osrm_data._coverage(TravelCoordinate(latitude=60, longitude=179), 250)


def test_missing_native_prerequisite_never_creates_download_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def missing(_command: object) -> None:
        return None

    monkeypatch.setattr(shutil, "which", missing)
    with pytest.raises(osrm_data.OsrmBuildError, match="install_osmium-tool"):
        _ = osrm_data.prepare_osrm_seed(tmp_path / "workspace", origin=ORIGIN)
    assert not (tmp_path / "workspace").exists()


@pytest.mark.anyio
async def test_public_builder_preserves_arguments_for_owned_controller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from carl.io import osrm_data as facade

    transport = httpx.MockTransport(lambda _: httpx.Response(200))
    result = tmp_path / "manifest.json"

    async def controlled(
        root: Path,
        *,
        origin: TravelCoordinate,
        radius_miles: float,
        threads: int,
        osmium_executable: str,
        transport: httpx.AsyncBaseTransport | None,
    ) -> Path:
        assert root == tmp_path and origin == ORIGIN
        assert (radius_miles, threads, osmium_executable) == (125, 2, "custom-osmium")
        assert transport is not None
        return result

    monkeypatch.setattr(facade, "build_osrm_dataset_direct", controlled)
    assert (
        await facade.build_osrm_dataset(
            tmp_path,
            origin=ORIGIN,
            radius_miles=125,
            threads=2,
            osmium_executable="custom-osmium",
            transport=transport,
        )
        == result
    )
