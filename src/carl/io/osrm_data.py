"""Explicit, reproducible preparation of immutable local OSRM road datasets.

Builds are deliberately independent of configuration and worker startup. A caller
can inspect ``.building-*/progress.json`` while a build runs. Completed and failed
build reports remain in ``build-reports``; downloaded PBFs are never retained.
"""

import datetime
import hashlib
import importlib.metadata
import importlib.resources
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

import anyio
import httpx
from shapely.geometry import Polygon, mapping, shape

from carl.core.travel import TravelCoordinate

_INDEX_URL = "https://download.geofabrik.de/index-v1.json"
_REQUIRED_FILES = ("region.osrm.partition", "region.osrm.cell_metrics", "region.osrm.mldgr")


class OsrmBuildError(RuntimeError):
    """A stable, secret-free explanation suitable for CLI and durable work status."""

    def __init__(self, value: str) -> None:
        self.reason = value.split(":", maxsplit=1)[0]
        self.message = (
            "Install the osmium-tool package, ensure osmium is on PATH, then retry setup."
            if self.reason == "osrm_osmium_tool_missing"
            else value
        )
        super().__init__(value)


def _json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".new")
    temporary.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(path)


def _coverage(origin: TravelCoordinate, radius_miles: float) -> Polygon:
    latitude, longitude = map(math.radians, (origin.latitude, origin.longitude))
    angular = radius_miles * 1609.344 / 6371008.8
    ring: list[tuple[float, float]] = []
    for index in range(257):
        bearing = math.tau * index / 256
        lat = math.asin(
            math.sin(latitude) * math.cos(angular)
            + math.cos(latitude) * math.sin(angular) * math.cos(bearing)
        )
        lon = longitude + math.atan2(
            math.sin(bearing) * math.sin(angular) * math.cos(latitude),
            math.cos(angular) - math.sin(latitude) * math.sin(lat),
        )
        degrees = math.degrees(lon)
        # A polygon crossing the date line needs explicit splitting. Do not
        # silently select almost the whole country for an Alaska origin.
        if not -180 <= degrees <= 180:
            raise OsrmBuildError("osrm_coverage_crosses_date_line")
        ring.append((degrees, math.degrees(lat)))
    return Polygon(ring)


def _profile() -> Path:
    profile = Path(str(importlib.resources.files("osrm") / "share/profiles/car.lua"))
    if not profile.is_file():
        raise OsrmBuildError("osrm_packaged_car_profile_missing")
    return profile


def _profile_hash(profile: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(profile.parent.rglob("*.lua")):
        digest.update(str(path.relative_to(profile.parent)).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _file(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return {"name": path.name, "bytes": path.stat().st_size, "sha256": digest}


async def _run(arguments: list[str], log: Path, *, threads: int) -> str:
    """Kill the entire owned process group on cancellation, including wrappers."""
    with log.open("wb") as output:
        process = await anyio.open_process(
            arguments,
            stdout=output,
            stderr=subprocess.STDOUT,
            env={**os.environ, "OSMIUM_POOL_THREADS": str(threads)},
            start_new_session=True,
        )
        try:
            returncode = await process.wait()
        finally:
            if process.returncode is None:
                with anyio.CancelScope(shield=True):
                    with suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGTERM)
                    with anyio.move_on_after(5):
                        await process.wait()
                    if process.returncode is None:
                        with suppress(ProcessLookupError):
                            os.killpg(process.pid, signal.SIGKILL)
                        await process.wait()
            await process.aclose()
    if returncode:
        raise OsrmBuildError("osrm_build_tool_failed:" + log.stem)
    # Only version/fileinfo calls need captured text, not multi-megabyte logs.
    with log.open("rb") as stream:
        return stream.read(4096).decode("utf-8", errors="replace").strip()


def _sources(index: Any, polygon: Polygon) -> list[dict[str, str]]:
    sources: list[dict[str, str]] = []
    for feature in index["features"]:
        properties = feature["properties"]
        identifier = properties.get("id", "")
        if not re.fullmatch(r"us/[a-z-]+", identifier):
            continue
        if shape(feature["geometry"]).intersects(polygon):
            url = properties["urls"]["pbf"]
            if not re.fullmatch(
                r"https://download\.geofabrik\.de/north-america/us/[a-z-]+-latest\.osm\.pbf",
                url,
            ):
                raise OsrmBuildError("osrm_source_index_invalid")
            sources.append({"id": identifier, "latest_url": url})
    if not sources:
        raise OsrmBuildError("osrm_no_intersecting_us_sources")
    return sorted(sources, key=lambda source: source["id"])


async def _dated_sources(
    client: httpx.AsyncClient, sources: list[dict[str, str]]
) -> list[dict[str, str]]:
    """Never mix moving -latest files; find one date published for every state."""
    today = datetime.datetime.now(datetime.UTC).date()
    for age in range(1, 8):
        stamp = (today - datetime.timedelta(days=age)).strftime("%y%m%d")
        candidates: list[dict[str, str]] = []
        for source in sources:
            url = source["latest_url"].replace("-latest.osm.pbf", f"-{stamp}.osm.pbf")
            response = await client.get(url + ".md5")
            if response.status_code == 404:
                break
            response.raise_for_status()
            checksum = response.text.split(maxsplit=1)[0] if response.text.strip() else ""
            if not re.fullmatch(r"[0-9a-fA-F]{32}", checksum):
                raise OsrmBuildError("osrm_source_checksum_invalid")
            candidates.append(
                {"id": source["id"], "url": url, "snapshot": stamp, "md5": checksum.lower()}
            )
        else:
            return candidates
    raise OsrmBuildError("osrm_coherent_snapshot_unavailable")


async def _download(
    client: httpx.AsyncClient, source: dict[str, str], target: Path
) -> dict[str, Any]:
    md5 = hashlib.md5(usedforsecurity=False)
    sha256 = hashlib.sha256()
    async with client.stream("GET", source["url"]) as response:
        response.raise_for_status()
        async with await anyio.open_file(target, "wb") as stream:
            async for chunk in response.aiter_bytes(1024 * 1024):
                await stream.write(chunk)
                md5.update(chunk)
                sha256.update(chunk)
    if md5.hexdigest() != source["md5"]:
        raise OsrmBuildError("osrm_source_checksum_mismatch:" + target.stem)
    return {**source, "bytes": target.stat().st_size, "sha256": sha256.hexdigest()}


async def build_osrm_dataset(
    root: Path,
    *,
    origin: TravelCoordinate,
    radius_miles: float = 250,
    threads: int = 4,
    osmium_executable: str = "osmium",
    transport: httpx.AsyncBaseTransport | None = None,
) -> Path:
    """Download, verify and build a new immutable dataset; never edit config.

    Requires the external ``osmium-tool`` package and potentially tens of GiB of
    build RAM/disk. Downloaded and intermediate files are removed after a build.
    The previous dataset remains untouched even when a build fails or is stopped.
    """
    if not math.isfinite(radius_miles) or not 1 <= radius_miles <= 1000:
        raise OsrmBuildError("osrm_radius_miles_must_be_between_1_and_1000")
    if isinstance(threads, bool) or not 1 <= threads <= 16:
        raise OsrmBuildError("osrm_threads_must_be_between_1_and_16")
    executable = shutil.which(osmium_executable)
    if executable is None:
        raise OsrmBuildError("osrm_osmium_tool_missing:install_osmium-tool_then_retry")
    profile = _profile()
    polygon = _coverage(origin, radius_miles)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    identifier = uuid.uuid4().hex
    stage = root / (".building-" + identifier)
    stage.mkdir(mode=0o700)
    reports = root / "build-reports"
    reports.mkdir(exist_ok=True, mode=0o700)
    measurements: list[dict[str, Any]] = []
    current_stage = "prepare"
    started = time.monotonic()

    def progress(name: str, **details: Any) -> None:
        nonlocal current_stage
        current_stage = name
        value = {"build_identifier": identifier, "stage": name, "threads": threads, **details}
        _json(stage / "progress.json", value)
        _json(root / "progress.json", value)

    async def run(name: str, arguments: list[str]) -> str:
        progress(name, log_file=name + ".log")
        begin = time.monotonic()
        result = await _run(arguments, stage / (name + ".log"), threads=threads)
        measurements.append({"stage": name, "wall_seconds": round(time.monotonic() - begin, 3)})
        return result

    try:
        progress("source_index")
        _json(
            stage / "coverage.geojson",
            {
                "type": "Feature",
                "properties": {"radius_miles": radius_miles},
                "geometry": mapping(polygon),
            },
        )
        async with httpx.AsyncClient(
            transport=transport, timeout=120, follow_redirects=False
        ) as client:
            response = await client.get(_INDEX_URL)
            response.raise_for_status()
            selected = _sources(response.json(), polygon)
            progress("select_snapshot", source_count=len(selected))
            dated = await _dated_sources(client, selected)
            sources: list[dict[str, Any]] = []
            crops: list[str] = []
            timestamps: set[str] = set()
            for number, source in enumerate(dated, 1):
                name = source["id"].split("/")[-1]
                progress("download", source=name, source_number=number, source_count=len(dated))
                downloaded = await _download(client, source, stage / (name + ".osm.pbf"))
                timestamp = await run(
                    "timestamp-" + name,
                    [
                        executable,
                        "fileinfo",
                        "-g",
                        "header.option.osmosis_replication_timestamp",
                        str(stage / (name + ".osm.pbf")),
                    ],
                )
                if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timestamp):
                    raise OsrmBuildError("osrm_source_replication_timestamp_missing")
                timestamps.add(timestamp)
                if len(timestamps) != 1:
                    raise OsrmBuildError("osrm_incoherent_source_timestamps")
                sources.append({**downloaded, "data_timestamp_utc": timestamp})
                output = stage / (name + "-crop.osm.pbf")
                await run(
                    "crop-" + name,
                    [
                        executable,
                        "extract",
                        "-p",
                        str(stage / "coverage.geojson"),
                        "-s",
                        "complete_ways",
                        "-o",
                        str(output),
                        str(stage / (name + ".osm.pbf")),
                    ],
                )
                crops.append(str(output))
        await run("merge", [executable, "merge", *crops, "-o", str(stage / "region.osm.pbf")])
        await run("check-references", [executable, "check-refs", str(stage / "region.osm.pbf")])
        version = await run("osrm-version", [sys.executable, "-m", "osrm", "extract", "--version"])
        if not re.fullmatch(r"v?\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?", version):
            raise OsrmBuildError("osrm_tool_version_invalid")
        if version.removeprefix("v") != importlib.metadata.version("osrm-bindings"):
            raise OsrmBuildError("osrm_packaged_tool_version_mismatch")
        for operation in ("extract", "partition", "customize"):
            arguments = [
                sys.executable,
                "-m",
                "osrm",
                operation,
                str(stage / ("region.osm.pbf" if operation == "extract" else "region.osrm")),
            ]
            if operation == "extract":
                arguments.extend(["-p", str(profile)])
            arguments.extend(["--threads", str(threads)])
            await run("osrm-" + operation, arguments)
        if not all((stage / name).is_file() for name in _REQUIRED_FILES):
            raise OsrmBuildError("osrm_build_outputs_missing")
        progress("verify_outputs")
        files = await anyio.to_thread.run_sync(
            lambda: [_file(path) for path in sorted(stage.glob("region.osrm.*")) if path.is_file()]
        )
        profile_sha256 = await anyio.to_thread.run_sync(_profile_hash, profile)
        identity = hashlib.sha256(
            json.dumps(
                {
                    "sources": sources,
                    "profile_sha256": profile_sha256,
                    "osrm_version": version,
                    "coverage_sha256": _file(stage / "coverage.geojson")["sha256"],
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()
        dataset_identifier = "osrm-" + identity[:24]
        destination = root / dataset_identifier
        manifest = {
            "dataset_identifier": dataset_identifier,
            "provider": "osrm",
            "created_at_utc": datetime.datetime.now(datetime.UTC).isoformat(),
            "osrm_version": version,
            "algorithm": "MLD",
            "profile": "car",
            "profile_sha256": profile_sha256,
            "coverage_radius_miles": radius_miles,
            "coverage_file": "coverage.geojson",
            "coverage_scope": "US state extracts intersecting buffered circle; Canada excluded",
            "sources": sources,
            "files": files,
            "total_dataset_bytes": sum(file["bytes"] for file in files),
            "measurements": measurements,
            "license": "OpenStreetMap contributors, ODbL 1.0",
            "source_url": "https://www.openstreetmap.org/copyright",
        }
        _json(stage / "manifest.json", manifest)
        # Publish only graph files and provenance, never the large input PBFs.
        keep = {file["name"] for file in files} | {"manifest.json", "coverage.geojson"}
        for path in stage.iterdir():
            if path.name not in keep:
                path.unlink()
        if destination.exists():
            # Identical inputs produce the same name; never replace live files.
            previous = json.loads((destination / "manifest.json").read_text())
            if previous.get("files") != files:
                raise OsrmBuildError("osrm_existing_dataset_integrity_mismatch")
            existing_files = await anyio.to_thread.run_sync(
                lambda: [_file(destination / file["name"]) for file in files]
            )
            if existing_files != files:
                raise OsrmBuildError("osrm_existing_dataset_integrity_mismatch")
            await anyio.to_thread.run_sync(shutil.rmtree, stage)
        else:
            stage.rename(destination)
        _json(
            reports / (identifier + ".json"),
            {
                "build_identifier": identifier,
                "state": "completed",
                "dataset_identifier": dataset_identifier,
                "wall_seconds": round(time.monotonic() - started, 3),
                "measurements": measurements,
            },
        )
        return destination / "manifest.json"
    except BaseException as error:
        cancelled = isinstance(error, anyio.get_cancelled_exc_class())
        reason = (
            str(error)
            if isinstance(error, OsrmBuildError)
            else "osrm_build_cancelled"
            if cancelled
            else "osrm_build_failed:inspect_prerequisites_network_memory_and_disk"
        )
        with anyio.CancelScope(shield=True):
            log_file = None
            failure_log = stage / (current_stage + ".log")
            if not cancelled and failure_log.is_file():
                # Native stderr may explain OOM, bad map input or missing road
                # data, but must never enter public work results or MCP status.
                # Retain only this failing stage's last 64 KiB in a private file.
                with suppress(OSError):
                    retained_log = reports / (identifier + ".log")
                    with failure_log.open("rb") as stream:
                        stream.seek(max(0, failure_log.stat().st_size - 65_536))
                        tail = stream.read(65_536)
                    with open(
                        retained_log, "xb", opener=lambda path, flags: os.open(path, flags, 0o600)
                    ) as stream:
                        stream.write(tail)
                    log_file = str(retained_log.relative_to(root))
            report = {
                "build_identifier": identifier,
                "state": "cancelled" if cancelled else "failed",
                "stage": current_stage,
                "reason": reason.split(":", maxsplit=1)[0],
                "log_file": log_file,
                "wall_seconds": round(time.monotonic() - started, 3),
                "measurements": measurements,
            }
            _json(
                reports / (identifier + ".json"),
                report,
            )
            _json(root / "progress.json", report)
            if stage.exists():
                await anyio.to_thread.run_sync(shutil.rmtree, stage)
        if cancelled or isinstance(error, OsrmBuildError):
            raise
        raise OsrmBuildError(reason) from None
