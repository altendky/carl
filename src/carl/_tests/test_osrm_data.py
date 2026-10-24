"""Road-map setup is explicit, coherent, verified and atomically published."""

import datetime
import hashlib
import json
import stat
import sys
from pathlib import Path
from typing import Any

import anyio
import httpx
import pytest

from carl.core.travel import TravelCoordinate
from carl.io import osrm_data

ORIGIN = TravelCoordinate(latitude=40, longitude=-77)


def _index() -> dict[str, Any]:
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


class BuildFixture:
    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.calls: list[list[str]] = []
        self.requests: list[str] = []
        self.fail_stage: str | None = None
        self.bad_checksum = False
        self.mismatched_timestamps = False
        self.missing_latest = False
        self.cancel_stage: str | None = None
        self.tool_log = "tool progress"
        self.profile = tmp_path / "profiles" / "car.lua"
        self.profile.parent.mkdir()
        self.profile.write_text("return {}")
        monkeypatch.setattr(osrm_data, "_profile", lambda: self.profile)
        monkeypatch.setattr(osrm_data.shutil, "which", lambda _: "/usr/bin/osmium")
        monkeypatch.setattr(osrm_data.importlib.metadata, "version", lambda _: "26.10.0")
        monkeypatch.setattr(osrm_data, "_run", self.run)

    def respond(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        if request.url.path == "/index-v1.json":
            return httpx.Response(200, json=_index())
        if request.url.path.endswith(".md5"):
            latest = (
                datetime.datetime.now(datetime.UTC).date() - datetime.timedelta(days=1)
            ).strftime("%y%m%d")
            if self.missing_latest and "pennsylvania-" + latest in request.url.path:
                return httpx.Response(404)
            content = (
                "0" * 32
                if self.bad_checksum
                else hashlib.md5(b"pbf", usedforsecurity=False).hexdigest()
            )
            return httpx.Response(200, text=content + "  fixture.osm.pbf\n")
        return httpx.Response(200, content=b"pbf")

    async def run(self, arguments: list[str], log: Path, *, threads: int) -> str:
        assert threads == 4
        self.calls.append(arguments)
        log.write_text(self.tool_log)
        if log.stem == self.fail_stage:
            raise RuntimeError("unsafe injected command stderr with credentials")
        if log.stem == self.cancel_stage:
            await anyio.sleep_forever()
        if "fileinfo" in arguments:
            if self.mismatched_timestamps and "pennsylvania" in arguments[-1]:
                return "2026-10-17T21:00:00Z"
            return "2026-10-17T20:20:51Z"
        if "--version" in arguments:
            return "v26.10.0"
        if "-o" in arguments:
            Path(arguments[arguments.index("-o") + 1]).write_bytes(b"crop")
        if log.stem == "osrm-customize":
            for name in osrm_data._REQUIRED_FILES:
                (log.parent / name).write_bytes(b"native graph " + name.encode())
        return ""


@pytest.mark.anyio
async def test_build_publishes_verified_immutable_graph_and_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = BuildFixture(tmp_path, monkeypatch)
    root = tmp_path / "data"
    path = await osrm_data.build_osrm_dataset(
        root, origin=ORIGIN, transport=httpx.MockTransport(fixture.respond)
    )
    manifest = json.loads(path.read_text())
    assert path.parent.name == manifest["dataset_identifier"]
    assert manifest["algorithm"] == "MLD"
    assert manifest["provider"] == "osrm"
    assert len(manifest["sources"]) == 2
    assert len({source["snapshot"] for source in manifest["sources"]}) == 1
    assert len({source["data_timestamp_utc"] for source in manifest["sources"]}) == 1
    assert all(
        source["sha256"] == hashlib.sha256(b"pbf").hexdigest() for source in manifest["sources"]
    )
    assert not list(root.glob(".building-*"))
    assert not list(path.parent.glob("*.pbf"))
    assert all("latest" not in request for request in fixture.requests)
    assert any("complete_ways" in call for call in fixture.calls)
    assert any("check-refs" in call for call in fixture.calls)
    assert all(
        "--threads" in call for call in fixture.calls if "-m" in call and "--version" not in call
    )
    assert json.loads((root / "progress.json").read_text())["stage"] == "verify_outputs"
    original = path.read_bytes()
    second = await osrm_data.build_osrm_dataset(
        root, origin=ORIGIN, transport=httpx.MockTransport(fixture.respond)
    )
    assert second == path
    assert path.read_bytes() == original


@pytest.mark.anyio
async def test_coherent_date_falls_back_for_every_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = BuildFixture(tmp_path, monkeypatch)
    fixture.missing_latest = True
    path = await osrm_data.build_osrm_dataset(
        tmp_path / "data", origin=ORIGIN, transport=httpx.MockTransport(fixture.respond)
    )
    sources = json.loads(path.read_text())["sources"]
    expected = (datetime.datetime.now(datetime.UTC).date() - datetime.timedelta(days=2)).strftime(
        "%y%m%d"
    )
    assert {source["snapshot"] for source in sources} == {expected}


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["checksum", "timestamp", "tool"])
async def test_failure_preserves_old_map_and_only_safe_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    fixture = BuildFixture(tmp_path, monkeypatch)
    fixture.bad_checksum = failure == "checksum"
    fixture.mismatched_timestamps = failure == "timestamp"
    fixture.fail_stage = "osrm-extract" if failure == "tool" else None
    root = tmp_path / "data"
    previous = root / "osrm-old" / "manifest.json"
    previous.parent.mkdir(parents=True)
    previous.write_text("previous dataset")
    with pytest.raises(osrm_data.OsrmBuildError) as error:
        await osrm_data.build_osrm_dataset(
            root, origin=ORIGIN, transport=httpx.MockTransport(fixture.respond)
        )
    assert "credentials" not in str(error.value)
    assert previous.read_text() == "previous dataset"
    assert not list(root.glob(".building-*"))
    reports = list((root / "build-reports").glob("*.json"))
    assert len(reports) == 1
    assert json.loads(reports[0].read_text())["state"] == "failed"
    assert "credentials" not in reports[0].read_text()


@pytest.mark.anyio
async def test_cancellation_removes_only_owned_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = BuildFixture(tmp_path, monkeypatch)
    fixture.cancel_stage = "osrm-extract"
    root = tmp_path / "data"
    with anyio.move_on_after(0.1) as scope:
        await osrm_data.build_osrm_dataset(
            root, origin=ORIGIN, transport=httpx.MockTransport(fixture.respond)
        )
    assert scope.cancel_called
    assert not list(root.glob(".building-*"))
    report = json.loads(next((root / "build-reports").glob("*.json")).read_text())
    assert report["state"] == "cancelled"


@pytest.mark.anyio
async def test_failed_native_tool_retains_only_bounded_private_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = BuildFixture(tmp_path, monkeypatch)
    fixture.fail_stage = "osrm-extract"
    fixture.tool_log = (
        "prefix discarded\n" * 10_000 + "private native diagnostic: credentials\n" * 3000
    )
    root = tmp_path / "data"
    with pytest.raises(osrm_data.OsrmBuildError) as error:
        await osrm_data.build_osrm_dataset(
            root, origin=ORIGIN, transport=httpx.MockTransport(fixture.respond)
        )
    report_path = next((root / "build-reports").glob("*.json"))
    report = json.loads(report_path.read_text())
    retained = root / report["log_file"]
    assert retained.read_bytes() == fixture.tool_log.encode()[-65_536:]
    assert stat.S_IMODE(retained.stat().st_mode) == 0o600
    assert not list(root.glob(".building-*"))
    public_progress = (root / "progress.json").read_text()
    assert json.loads(public_progress)["state"] == "failed"
    assert json.loads(public_progress)["stage"] == "osrm-extract"
    assert json.loads(public_progress)["log_file"] == report["log_file"]
    assert "credentials" not in public_progress
    assert "credentials" not in report_path.read_text()
    assert "credentials" not in str(error.value)


@pytest.mark.anyio
async def test_missing_prerequisite_is_clear_and_never_downloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(osrm_data.shutil, "which", lambda _: None)
    with pytest.raises(osrm_data.OsrmBuildError, match="install_osmium-tool"):
        await osrm_data.build_osrm_dataset(tmp_path / "data", origin=ORIGIN)
    assert not (tmp_path / "data").exists()


@pytest.mark.anyio
async def test_real_tool_wrapper_cancellation_also_stops_native_child(tmp_path: Path) -> None:
    marker = tmp_path / "orphan-marker"
    log = tmp_path / "native.log"
    child = "import pathlib,time; time.sleep(.6); pathlib.Path(" + repr(str(marker)) + ").touch()"
    wrapper = (
        "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',"
        + repr(child)
        + "]); print('ready',flush=True); time.sleep(30)"
    )
    async with anyio.create_task_group() as tasks:

        async def run() -> None:
            await osrm_data._run([sys.executable, "-c", wrapper], log, threads=4)

        tasks.start_soon(run)
        with anyio.fail_after(5):
            while not log.exists() or "ready" not in log.read_text():
                await anyio.sleep(0.01)
        tasks.cancel_scope.cancel()
    await anyio.sleep(0.7)
    assert not marker.exists()


@pytest.mark.anyio
async def test_real_tool_nonzero_is_sanitized(tmp_path: Path) -> None:
    with pytest.raises(osrm_data.OsrmBuildError, match="osrm_build_tool_failed:extract"):
        await osrm_data._run(
            [
                sys.executable,
                "-c",
                "import sys; print('private stderr',file=sys.stderr); sys.exit(2)",
            ],
            tmp_path / "extract.log",
            threads=4,
        )


def test_date_line_coverage_does_not_silently_select_wrong_states() -> None:
    with pytest.raises(osrm_data.OsrmBuildError, match="crosses_date_line"):
        osrm_data._coverage(TravelCoordinate(latitude=60, longitude=-179), 250)
