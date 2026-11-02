"""Frozen OSRM planning, supervised coarse builds and immutable publication.

Source PBFs and command receipts stay in the owning workflow directory. Individual
native command completion never substitutes for the final graph inventory.
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
import stat
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

from shapely.geometry import Polygon, mapping, shape

from carl.core.dataset_downloads import DatasetSource, FileReceipt, dataset_source_fingerprint
from carl.core.native_build import NativeBuildSpec
from carl.core.osrm_build import (
    MAX_OSRM_RECEIPT_BYTES,
    MAX_OSRM_SOURCES,
    OSRM_BUILD_RECIPE,
    OsrmAttemptInputs,
    OsrmBuildAttempt,
    OsrmBuildMeasurement,
    OsrmBuildPlan,
    OsrmCoarseReceipt,
    OsrmDatedSource,
    OsrmExecutable,
    OsrmOutputFile,
    OsrmPlanSeed,
    OsrmSourceCandidate,
    OsrmSourceEvidence,
    OsrmToolchain,
)
from carl.core.travel import TravelCoordinate
from carl.io.native_build_receipts import (
    NativeBuildReceiptError,
    NativeBuildUncertain,
    command_fingerprint,
    read_build_receipt,
    verify_build_logs,
)
from carl.io.native_builds import BuildCommandCapture
from carl.io.native_tools import (
    _native_executable,  # pyright: ignore[reportPrivateUsage]
    osrm_build_executable,
)
from carl.io.resource_permits import run_cpu
from carl.io.streaming_http import open_dataset_directory, open_dataset_file

_INDEX_URL = "https://download.geofabrik.de/index-v1.json"
_REQUIRED_FILES = ("region.osrm.partition", "region.osrm.cell_metrics", "region.osrm.mldgr")


class OsrmBuildError(RuntimeError):
    """A stable, secret-free explanation suitable for CLI and durable work status."""

    def __init__(self, value: str) -> None:
        self.reason: str = value.split(":", maxsplit=1)[0]
        self.message: str = (
            "Install the osmium-tool package, ensure osmium is on PATH, then retry setup."
            if self.reason == "osrm_osmium_tool_missing"
            else value
        )
        super().__init__(value)


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


def _private_bytes(directory: Path, locator: str, maximum: int, *, private: bool = True) -> bytes:
    descriptor = open_dataset_file(directory, locator)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or (private and before.st_mode & 0o077)
            or before.st_size > maximum
        ):
            raise OsrmBuildError("osrm_build_file_invalid")
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            content = stream.read(maximum + 1)
        if len(content) != before.st_size or _file_identity(os.fstat(descriptor)) != _file_identity(
            before
        ):
            raise OsrmBuildError("osrm_build_file_changed")
        return content
    finally:
        os.close(descriptor)


def _write_once(directory: Path, locator: str, content: bytes) -> None:
    target = directory / locator
    parent = open_dataset_directory(target.parent, create=True)
    temporary = ".osrm-" + uuid.uuid4().hex
    try:
        descriptor = os.open(
            temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600, dir_fd=parent
        )
        with os.fdopen(descriptor, "wb") as stream:
            _ = stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(
                temporary, target.name, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False
            )
        except FileExistsError:
            if _private_bytes(directory, locator, len(content)) != content:
                raise OsrmBuildError("osrm_frozen_input_changed") from None
        os.fsync(parent)
    finally:
        with suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=parent)
        os.close(parent)


def _hash_private(directory: Path, locator: str, *, private: bool = True) -> tuple[int, str, str]:
    descriptor = open_dataset_file(directory, locator)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or (private and before.st_mode & 0o077)
        ):
            raise OsrmBuildError("osrm_build_file_invalid")
        sha256 = hashlib.sha256()
        md5 = hashlib.md5(usedforsecurity=False)
        observed = 0
        while chunk := os.read(descriptor, 256 * 1024):
            observed += len(chunk)
            sha256.update(chunk)
            md5.update(chunk)
        if observed != before.st_size or _file_identity(os.fstat(descriptor)) != _file_identity(
            before
        ):
            raise OsrmBuildError("osrm_build_file_changed")
        return observed, sha256.hexdigest(), md5.hexdigest()
    finally:
        os.close(descriptor)


def _file_identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _executable(path: Path) -> OsrmExecutable:
    path = _native_executable(path)
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return OsrmExecutable(path=path, sha256=digest)


def prepare_osrm_seed(
    directory: Path,
    *,
    origin: TravelCoordinate,
    radius_miles: float = 250,
    threads: int = 4,
    anchor_date: datetime.date | None = None,
    osmium_executable: str = "osmium",
    toolchain: OsrmToolchain | None = None,
    profile: Path | None = None,
    package_version: str | None = None,
) -> OsrmPlanSeed:
    """CPU preparation only: freeze coverage/profile/tool identities, never query."""

    if not math.isfinite(radius_miles) or not 1 <= radius_miles <= 1000:
        raise OsrmBuildError("osrm_radius_miles_must_be_between_1_and_1000")
    if isinstance(threads, bool) or not 1 <= threads <= 16:
        raise OsrmBuildError("osrm_threads_must_be_between_1_and_16")
    if toolchain is None:
        osmium = shutil.which(osmium_executable)
        if osmium is None:
            raise OsrmBuildError("osrm_osmium_tool_missing:install_osmium-tool_then_retry")
        toolchain = OsrmToolchain(
            osmium=_executable(Path(osmium)),
            extract=_executable(osrm_build_executable("extract")),
            partition=_executable(osrm_build_executable("partition")),
            customize=_executable(osrm_build_executable("customize")),
        )
    profile = (profile or _profile()).resolve(strict=True)
    coverage = (
        json.dumps(
            {
                "type": "Feature",
                "properties": {"radius_miles": radius_miles},
                "geometry": mapping(_coverage(origin, radius_miles)),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    _write_once(directory, "osrm-coverage.geojson", coverage)
    files = sorted(profile.parent.rglob("*.lua"))
    if len(files) > 2048 or sum(path.stat().st_size for path in files) > 16 * 1024 * 1024:
        raise OsrmBuildError("osrm_profile_too_large")
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise OsrmBuildError("osrm_profile_invalid")
        _write_once(
            directory,
            "osrm-profile/" + path.relative_to(profile.parent).as_posix(),
            path.read_bytes(),
        )
    profile_hash = _profile_hash(directory / "osrm-profile" / "car.lua")
    if profile.name != "car.lua" or profile_hash != _profile_hash(profile):
        raise OsrmBuildError("osrm_profile_changed")
    return OsrmPlanSeed(
        origin=origin,
        radius_miles=radius_miles,
        threads=threads,
        anchor_date=anchor_date or datetime.datetime.now(datetime.UTC).date(),
        coverage_sha256=hashlib.sha256(coverage).hexdigest(),
        profile_sha256=profile_hash,
        package_version=package_version or importlib.metadata.version("osrm-bindings"),
        toolchain=toolchain,
    )


def osrm_index_source() -> DatasetSource:
    return DatasetSource(
        identifier="geofabrik-index",
        name="index-v1.json",
        url=_INDEX_URL,
        maximum_bytes=16 * 1024 * 1024,
    )


def select_osrm_sources(index_bytes: bytes, seed: OsrmPlanSeed) -> tuple[OsrmSourceCandidate, ...]:
    if len(index_bytes) > 16 * 1024 * 1024:
        raise OsrmBuildError("osrm_source_index_invalid")
    try:
        selected = _sources(json.loads(index_bytes), _coverage(seed.origin, seed.radius_miles))
        if len(selected) > MAX_OSRM_SOURCES:
            raise ValueError
        result = tuple(OsrmSourceCandidate.model_validate(value) for value in selected)
        if len({source.id for source in result}) != len(result):
            raise ValueError
        return result
    except (ValueError, KeyError, TypeError, OverflowError):
        raise OsrmBuildError("osrm_source_index_invalid") from None


def osrm_probe_sources(
    candidates: tuple[OsrmSourceCandidate, ...], *, anchor_date: datetime.date, age: int
) -> tuple[DatasetSource, ...]:
    if isinstance(age, bool) or not 1 <= age <= 7:
        raise OsrmBuildError("osrm_coherent_snapshot_unavailable")
    snapshot = (anchor_date - datetime.timedelta(days=age)).strftime("%y%m%d")
    return tuple(
        DatasetSource(
            identifier="checksum-" + candidate.id[3:],
            name=candidate.id[3:] + ".md5",
            url=candidate.latest_url.replace("-latest.osm.pbf", "-" + snapshot + ".osm.pbf.md5"),
            maximum_bytes=4096,
        )
        for candidate in candidates
    )


def parse_osrm_checksum(content: bytes) -> str:
    try:
        if len(content) > 4096:
            raise ValueError
        value = content.decode("utf-8").split(maxsplit=1)[0]
        if re.fullmatch(r"[0-9a-fA-F]{32}", value) is None:
            raise ValueError
        return value.lower()
    except (UnicodeError, ValueError, IndexError):
        raise OsrmBuildError("osrm_source_checksum_invalid") from None


def freeze_osrm_plan(
    seed: OsrmPlanSeed,
    candidates: tuple[OsrmSourceCandidate, ...],
    *,
    age: int,
    checksums: tuple[str, ...],
) -> OsrmBuildPlan:
    probes = osrm_probe_sources(candidates, anchor_date=seed.anchor_date, age=age)
    if len(checksums) != len(probes):
        raise OsrmBuildError("osrm_source_checksum_invalid")
    snapshot = (seed.anchor_date - datetime.timedelta(days=age)).strftime("%y%m%d")
    return OsrmBuildPlan(
        seed=seed,
        sources=tuple(
            OsrmDatedSource(
                id=candidate.id,
                url=probe.url.removesuffix(".md5"),
                snapshot=snapshot,
                md5=parse_osrm_checksum(checksum.encode("utf-8")),
            )
            for candidate, probe, checksum in zip(candidates, probes, checksums, strict=True)
        ),
    )


def osrm_download_sources(plan: OsrmBuildPlan) -> tuple[DatasetSource, ...]:
    return tuple(
        DatasetSource(
            identifier=source.id,
            name=source.id[3:] + ".osm.pbf",
            url=source.url,
            expected_md5=source.md5,
        )
        for source in plan.sources
    )


def create_osrm_attempt(
    directory: Path, plan: OsrmBuildPlan, *, identifier: str
) -> OsrmBuildAttempt:
    attempt = OsrmBuildAttempt(identifier=identifier, plan_identifier=plan.identifier)
    parent = open_dataset_directory(directory / "osrm-attempts", create=True)
    try:
        try:
            os.mkdir(identifier, mode=0o700, dir_fd=parent)
        except FileExistsError:
            raise OsrmBuildError("osrm_build_attempt_already_exists") from None
        os.fsync(parent)
    finally:
        os.close(parent)
    _write_once(
        directory,
        attempt.locator + "/graph/coverage.geojson",
        _private_bytes(directory, plan.seed.coverage_locator, 65_536),
    )
    return attempt


def _require_plan(
    directory: Path, plan: OsrmBuildPlan, attempt: OsrmBuildAttempt, *, verify_tools: bool = False
) -> None:
    if attempt.plan_identifier != plan.identifier:
        raise OsrmBuildError("osrm_build_plan_changed")
    _, coverage, _ = _hash_private(directory, plan.seed.coverage_locator)
    if (
        coverage != plan.seed.coverage_sha256
        or _profile_hash(directory / plan.seed.profile_locator) != plan.seed.profile_sha256
    ):
        raise OsrmBuildError("osrm_frozen_input_changed")
    if verify_tools:
        for executable in (
            plan.seed.toolchain.osmium,
            plan.seed.toolchain.extract,
            plan.seed.toolchain.partition,
            plan.seed.toolchain.customize,
        ):
            if _executable(executable.path) != executable:
                raise OsrmBuildError("osrm_toolchain_changed")


def _verify_source_receipts(
    directory: Path, plan: OsrmBuildPlan, receipts: tuple[FileReceipt, ...]
) -> None:
    sources = osrm_download_sources(plan)
    if len(receipts) != len(sources):
        raise OsrmBuildError("osrm_source_receipts_invalid")
    for source, receipt in zip(sources, receipts, strict=True):
        if (
            receipt.source_fingerprint != dataset_source_fingerprint(source)
            or receipt.source_identifier != source.identifier
            or receipt.requested_url != source.url
        ):
            raise OsrmBuildError("osrm_source_receipts_invalid")
        size, sha256, md5 = _hash_private(directory, receipt.locator)
        if (size, sha256, md5) != (
            receipt.bytes,
            receipt.sha256,
            source.expected_md5,
        ) or receipt.md5 != md5:
            raise OsrmBuildError("osrm_source_checksum_mismatch")


def _commands(
    directory: Path,
    plan: OsrmBuildPlan,
    attempt: OsrmBuildAttempt,
    receipts: tuple[FileReceipt, ...],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    graph = directory / attempt.locator / "graph"
    osmium = str(plan.seed.toolchain.osmium.path)
    commands: list[tuple[str, tuple[str, ...]]] = []
    crops: list[str] = []
    for source, receipt in zip(plan.sources, receipts, strict=True):
        name = source.id[3:]
        commands.append(
            (
                "timestamp-" + name,
                (
                    osmium,
                    "fileinfo",
                    "-g",
                    "header.option.osmosis_replication_timestamp",
                    str(directory / receipt.locator),
                ),
            )
        )
        crop = str(graph / (name + "-crop.osm.pbf"))
        commands.append(
            (
                "crop-" + name,
                (
                    osmium,
                    "extract",
                    "-p",
                    str(graph / "coverage.geojson"),
                    "-s",
                    "complete_ways",
                    "-o",
                    crop,
                    str(directory / receipt.locator),
                ),
            )
        )
        crops.append(crop)
    commands.extend(
        (
            ("merge", (osmium, "merge", *crops, "-o", str(graph / "region.osm.pbf"))),
            ("check-references", (osmium, "check-refs", str(graph / "region.osm.pbf"))),
            ("osrm-version", (str(plan.seed.toolchain.extract.path), "--version")),
            (
                "osrm-extract",
                (
                    str(plan.seed.toolchain.extract.path),
                    str(graph / "region.osm.pbf"),
                    "-p",
                    str(directory / plan.seed.profile_locator),
                    "--threads",
                    str(plan.seed.threads),
                ),
            ),
            (
                "osrm-partition",
                (
                    str(plan.seed.toolchain.partition.path),
                    str(graph / "region.osrm"),
                    "--threads",
                    str(plan.seed.threads),
                ),
            ),
            (
                "osrm-customize",
                (
                    str(plan.seed.toolchain.customize.path),
                    str(graph / "region.osrm"),
                    "--threads",
                    str(plan.seed.threads),
                ),
            ),
        )
    )
    return tuple(commands)


def osrm_command_spec(
    directory: Path,
    plan: OsrmBuildPlan,
    attempt: OsrmBuildAttempt,
    phase: str,
    *,
    source_receipts: tuple[FileReceipt, ...],
) -> NativeBuildSpec:
    if attempt.plan_identifier != plan.identifier:
        raise OsrmBuildError("osrm_build_plan_changed")
    arguments = dict(_commands(directory, plan, attempt, source_receipts)).get(phase)
    if arguments is None:
        raise OsrmBuildError("osrm_build_phase_invalid")
    suffix = hashlib.sha256(phase.encode()).hexdigest()[:16]
    return NativeBuildSpec(
        recipe_identifier=OSRM_BUILD_RECIPE,
        phase_identifier="osrm:" + attempt.identifier + ":" + suffix,
        input_identity=hashlib.sha256(
            (plan.identifier + "".join(receipt.sha256 for receipt in source_receipts)).encode()
        ).hexdigest(),
        execution_identifier=attempt.identifier + "-" + suffix,
        argv=arguments,
        directory=directory / attempt.locator / "graph",
        receipt_directory=directory / attempt.locator / "receipts" / phase,
        threads=plan.seed.threads,
        timeout_seconds=120
        if phase.startswith("timestamp-") or phase == "osrm-version"
        else 86_400,
        on_output_limit="truncate",
    )


def _attempt_inputs(
    directory: Path, plan: OsrmBuildPlan, attempt: OsrmBuildAttempt
) -> OsrmAttemptInputs:
    try:
        inputs = OsrmAttemptInputs.model_validate_json(
            _private_bytes(directory, attempt.locator + "/inputs.json", MAX_OSRM_RECEIPT_BYTES)
        )
    except (FileNotFoundError, ValueError):
        raise OsrmBuildError("osrm_build_attempt_uncertain") from None
    if inputs.plan_identifier != plan.identifier:
        raise OsrmBuildError("osrm_build_plan_changed")
    return inputs


def _completed_commands(
    directory: Path, plan: OsrmBuildPlan, attempt: OsrmBuildAttempt, inputs: OsrmAttemptInputs
) -> dict[str, BuildCommandCapture]:
    completed: dict[str, BuildCommandCapture] = {}
    for phase, _ in _commands(directory, plan, attempt, inputs.sources):
        spec = osrm_command_spec(directory, plan, attempt, phase, source_receipts=inputs.sources)
        try:
            receipt = read_build_receipt(spec)
            if receipt is None or receipt.result is None:
                raise NativeBuildUncertain
            verify_build_logs(spec, receipt)
        except (NativeBuildReceiptError, NativeBuildUncertain, OSError):
            raise OsrmBuildError("osrm_build_attempt_uncertain") from None
        if receipt.result.reason != "completed":
            raise OsrmBuildError("osrm_build_tool_failed:" + phase)
        completed[phase] = BuildCommandCapture(receipt.result, receipt, True)
    return completed


def _legacy_dataset_identifier(
    plan: OsrmBuildPlan, version: str, sources: tuple[OsrmSourceEvidence, ...]
) -> str:
    # Preserve the existing public dataset IDs, including json.dumps' DEFAULT
    # separators and the native version's optional leading 'v'.
    identity = hashlib.sha256(
        json.dumps(
            {
                "sources": [source.model_dump(mode="json") for source in sources],
                "profile_sha256": plan.seed.profile_sha256,
                "osrm_version": version,
                "coverage_sha256": plan.seed.coverage_sha256,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    return "osrm-" + identity[:24]


def _graph_inventory(
    directory: Path, attempt: OsrmBuildAttempt, *, fsync: bool = False
) -> tuple[OsrmOutputFile, ...]:
    graph = directory / attempt.locator / "graph"
    descriptor = open_dataset_directory(graph)
    try:
        names = sorted(name for name in os.listdir(descriptor) if name.startswith("region.osrm."))
        if len(names) > 1000 or not set(_REQUIRED_FILES) <= set(names):
            raise OsrmBuildError("osrm_build_outputs_missing")
        files: list[OsrmOutputFile] = []
        for name in names:
            if fsync:
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
                try:
                    info = os.fstat(fd)
                    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
                        raise OsrmBuildError("osrm_build_file_invalid")
                    os.fchmod(fd, 0o600)
                finally:
                    os.close(fd)
            size, sha256, _ = _hash_private(directory, attempt.locator + "/graph/" + name)
            files.append(OsrmOutputFile(name=name, bytes=size, sha256=sha256))
            if fsync:
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=descriptor)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        if fsync:
            os.fsync(descriptor)
        return tuple(files)
    finally:
        os.close(descriptor)


def _check_coarse_identity(
    plan: OsrmBuildPlan, attempt: OsrmBuildAttempt, receipt: OsrmCoarseReceipt
) -> None:
    if (
        receipt.plan_identifier != plan.identifier
        or receipt.attempt_identifier != attempt.identifier
        or tuple(
            OsrmDatedSource.model_validate(
                source.model_dump(include={"id", "url", "snapshot", "md5"})
            )
            for source in receipt.sources
        )
        != plan.sources
        or receipt.dataset_identifier
        != _legacy_dataset_identifier(plan, receipt.osrm_version, receipt.sources)
        or receipt.osrm_version.removeprefix("v") != plan.seed.package_version
    ):
        raise OsrmBuildError("osrm_build_plan_changed")


def _read_coarse(
    directory: Path, plan: OsrmBuildPlan, attempt: OsrmBuildAttempt
) -> OsrmCoarseReceipt | None:
    if attempt.plan_identifier != plan.identifier:
        raise OsrmBuildError("osrm_build_plan_changed")
    try:
        receipt = OsrmCoarseReceipt.model_validate_json(
            _private_bytes(directory, attempt.locator + "/complete.json", MAX_OSRM_RECEIPT_BYTES)
        )
    except FileNotFoundError:
        return None
    except ValueError:
        raise OsrmBuildError("osrm_build_receipt_invalid") from None
    _check_coarse_identity(plan, attempt, receipt)
    return receipt


def _seal_osrm_attempt(
    directory: Path, plan: OsrmBuildPlan, attempt: OsrmBuildAttempt
) -> OsrmCoarseReceipt:
    _require_plan(directory, plan, attempt)
    inputs = _attempt_inputs(directory, plan, attempt)
    _verify_source_receipts(directory, plan, inputs.sources)
    completed = _completed_commands(directory, plan, attempt, inputs)
    evidence: list[OsrmSourceEvidence] = []
    for source, receipt in zip(plan.sources, inputs.sources, strict=True):
        timestamp = completed["timestamp-" + source.id[3:]].result.stdout_head.strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timestamp) is None:
            raise OsrmBuildError("osrm_source_replication_timestamp_missing")
        evidence.append(
            OsrmSourceEvidence(
                **source.model_dump(),
                bytes=receipt.bytes,
                sha256=receipt.sha256,
                data_timestamp_utc=timestamp,
            )
        )
    if len({source.data_timestamp_utc for source in evidence}) != 1:
        raise OsrmBuildError("osrm_incoherent_source_timestamps")
    version = completed["osrm-version"].result.stdout_head.strip()
    if re.fullmatch(r"v?\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?", version) is None:
        raise OsrmBuildError("osrm_tool_version_invalid")
    if version.removeprefix("v") != plan.seed.package_version:
        raise OsrmBuildError("osrm_packaged_tool_version_mismatch")
    _, coverage_hash, _ = _hash_private(directory, attempt.locator + "/graph/coverage.geojson")
    if coverage_hash != plan.seed.coverage_sha256:
        raise OsrmBuildError("osrm_frozen_input_changed")
    files = _graph_inventory(directory, attempt, fsync=True)
    final_time = completed["osrm-customize"].receipt.completed_at_utc_ns
    assert final_time is not None
    sources = tuple(evidence)
    result = OsrmCoarseReceipt(
        plan_identifier=plan.identifier,
        attempt_identifier=attempt.identifier,
        dataset_identifier=_legacy_dataset_identifier(plan, version, sources),
        osrm_version=version,
        sources=sources,
        files=files,
        created_at_utc=datetime.datetime.fromtimestamp(final_time / 1e9, datetime.UTC).isoformat(),
        measurements=tuple(
            OsrmBuildMeasurement(
                stage=phase,
                wall_seconds=round(
                    (capture.receipt.completed_at_utc_ns - capture.receipt.started_at_utc_ns) / 1e9,
                    3,
                ),
            )
            for phase, capture in completed.items()
            if capture.receipt.completed_at_utc_ns is not None
        ),
    )
    _write_once(directory, attempt.locator + "/complete.json", result.model_dump_json().encode())
    return result


def _verify_osrm_attempt(
    directory: Path, plan: OsrmBuildPlan, attempt: OsrmBuildAttempt
) -> OsrmCoarseReceipt:
    receipt = _read_coarse(directory, plan, attempt)
    if receipt is None:
        # The last native COMPLETE may be durable before the coarse checkpoint.
        # Reconstruct from all command receipts and final outputs; never execute.
        return _seal_osrm_attempt(directory, plan, attempt)
    if _graph_inventory(directory, attempt) != receipt.files:
        raise OsrmBuildError("osrm_build_outputs_changed")
    _, coverage_hash, _ = _hash_private(directory, attempt.locator + "/graph/coverage.geojson")
    if coverage_hash != plan.seed.coverage_sha256:
        raise OsrmBuildError("osrm_frozen_input_changed")
    return receipt


async def verify_osrm_attempt(
    directory: Path, plan: OsrmBuildPlan, attempt: OsrmBuildAttempt
) -> OsrmCoarseReceipt:
    """Verify a complete coarse build, or seal all-complete receipts, without dispatch."""
    return await run_cpu(_verify_osrm_attempt, directory, plan, attempt)


def _prepare_native_attempt(
    directory: Path,
    plan: OsrmBuildPlan,
    attempt: OsrmBuildAttempt,
    source_receipts: tuple[FileReceipt, ...],
) -> bool:
    try:
        _ = _private_bytes(directory, attempt.locator + "/abandoned.json", 4096)
    except FileNotFoundError:
        pass
    else:
        raise OsrmBuildError("osrm_build_attempt_abandoned")
    if _read_coarse(directory, plan, attempt) is not None:
        return False
    inputs = OsrmAttemptInputs(plan_identifier=plan.identifier, sources=source_receipts)
    _write_once(directory, attempt.locator + "/inputs.json", inputs.model_dump_json().encode())
    for phase, _ in _commands(directory, plan, attempt, source_receipts):
        spec = osrm_command_spec(directory, plan, attempt, phase, source_receipts=source_receipts)
        try:
            if read_build_receipt(spec) is not None:
                return False
        except FileNotFoundError:
            continue
        except (NativeBuildReceiptError, NativeBuildUncertain):
            return False
    _require_plan(directory, plan, attempt, verify_tools=True)
    _verify_source_receipts(directory, plan, source_receipts)
    return True


async def build_osrm_attempt(
    directory: Path,
    plan: OsrmBuildPlan,
    attempt: OsrmBuildAttempt,
    source_receipts: tuple[FileReceipt, ...],
    *,
    run_command: Callable[[NativeBuildSpec], Awaitable[BuildCommandCapture]],
) -> OsrmCoarseReceipt:
    """Run a fresh coarse attempt under the caller's weighted CPU and file owner.

    Any existing native receipt switches to verification-only recovery. Incomplete
    or failed attempts require an explicitly new directory selected by the owner.
    """
    if not await run_cpu(_prepare_native_attempt, directory, plan, attempt, source_receipts):
        return await verify_osrm_attempt(directory, plan, attempt)
    timestamps: set[str] = set()
    for phase, _ in _commands(directory, plan, attempt, source_receipts):
        spec = osrm_command_spec(directory, plan, attempt, phase, source_receipts=source_receipts)
        capture = await run_command(spec)
        if (
            capture.receipt.command_fingerprint != command_fingerprint(spec)
            or capture.result.reason != "completed"
        ):
            raise OsrmBuildError("osrm_build_tool_failed:" + phase)
        if phase.startswith("timestamp-"):
            timestamp = capture.result.stdout_head.strip()
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timestamp) is None:
                raise OsrmBuildError("osrm_source_replication_timestamp_missing")
            timestamps.add(timestamp)
            if len(timestamps) != 1:
                raise OsrmBuildError("osrm_incoherent_source_timestamps")
        if phase == "osrm-version":
            version = capture.result.stdout_head.strip()
            if re.fullmatch(r"v?\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?", version) is None:
                raise OsrmBuildError("osrm_tool_version_invalid")
            if version.removeprefix("v") != plan.seed.package_version:
                raise OsrmBuildError("osrm_packaged_tool_version_mismatch")
    return await run_cpu(_seal_osrm_attempt, directory, plan, attempt)


def _dataset_manifest(plan: OsrmBuildPlan, receipt: OsrmCoarseReceipt) -> dict[str, Any]:
    return {
        "dataset_identifier": receipt.dataset_identifier,
        "provider": "osrm",
        "created_at_utc": receipt.created_at_utc,
        "osrm_version": receipt.osrm_version,
        "algorithm": "MLD",
        "profile": "car",
        "profile_sha256": plan.seed.profile_sha256,
        "coverage_radius_miles": plan.seed.radius_miles,
        "coverage_file": "coverage.geojson",
        "coverage_scope": "US state extracts intersecting buffered circle; Canada excluded",
        "sources": [source.model_dump(mode="json") for source in receipt.sources],
        "files": [file.model_dump(mode="json") for file in receipt.files],
        "total_dataset_bytes": sum(file.bytes for file in receipt.files),
        "measurements": [
            measurement.model_dump(mode="json") for measurement in receipt.measurements
        ],
        "license": "OpenStreetMap contributors, ODbL 1.0",
        "source_url": "https://www.openstreetmap.org/copyright",
    }


def _verify_published(destination: Path, plan: OsrmBuildPlan, receipt: OsrmCoarseReceipt) -> None:
    try:
        manifest = json.loads(
            _private_bytes(destination, "manifest.json", MAX_OSRM_RECEIPT_BYTES, private=False)
        )
        expected = _dataset_manifest(plan, receipt)
        # Lifecycle timestamps/measurements can differ for identical build inputs.
        for field in (
            "dataset_identifier",
            "provider",
            "osrm_version",
            "profile_sha256",
            "sources",
            "files",
            "coverage_file",
            "algorithm",
        ):
            if manifest.get(field) != expected[field]:
                raise ValueError
        for file in receipt.files:
            size, digest, _ = _hash_private(destination, file.name, private=False)
            if (size, digest) != (file.bytes, file.sha256):
                raise ValueError
        if (
            _hash_private(destination, "coverage.geojson", private=False)[1]
            != plan.seed.coverage_sha256
        ):
            raise ValueError
    except (OSError, ValueError, OsrmBuildError):
        raise OsrmBuildError("osrm_existing_dataset_integrity_mismatch") from None


def _publish_osrm_dataset(
    root: Path, directory: Path, plan: OsrmBuildPlan, receipt: OsrmCoarseReceipt
) -> Path:
    attempt = OsrmBuildAttempt(
        identifier=receipt.attempt_identifier, plan_identifier=plan.identifier
    )
    stored = _read_coarse(directory, plan, attempt)
    if stored != receipt:
        raise OsrmBuildError("osrm_build_receipt_invalid")
    os.close(open_dataset_directory(root, create=True))
    destination = root / receipt.dataset_identifier
    if destination.exists() or destination.is_symlink():
        _verify_published(destination, plan, receipt)
        return destination / "manifest.json"
    # Check the published target FIRST: a prior rename may have consumed graph/.
    _ = _verify_osrm_attempt(directory, plan, attempt)
    graph = directory / attempt.locator / "graph"
    _write_once(
        graph,
        "manifest.json",
        (
            json.dumps(_dataset_manifest(plan, receipt), sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode(),
    )
    descriptors: list[int] = []
    try:
        graph_fd = open_dataset_directory(graph)
        descriptors.append(graph_fd)
        intermediate_fd = open_dataset_directory(
            directory / attempt.locator / "intermediates", create=True
        )
        descriptors.append(intermediate_fd)
        root_fd = open_dataset_directory(root)
        descriptors.append(root_fd)
        attempt_fd = open_dataset_directory(directory / attempt.locator)
        descriptors.append(attempt_fd)
        keep = {file.name for file in receipt.files} | {"manifest.json", "coverage.geojson"}
        for name in os.listdir(graph_fd):
            if name not in keep:
                info = os.stat(name, dir_fd=graph_fd, follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode):
                    raise OsrmBuildError("osrm_build_file_invalid")
                os.rename(name, name, src_dir_fd=graph_fd, dst_dir_fd=intermediate_fd)
        os.fsync(graph_fd)
        os.fsync(intermediate_fd)
        os.rename("graph", receipt.dataset_identifier, src_dir_fd=attempt_fd, dst_dir_fd=root_fd)
        os.fsync(attempt_fd)
        os.fsync(root_fd)
    finally:
        for fd in reversed(descriptors):
            os.close(fd)
    return destination / "manifest.json"


async def publish_osrm_dataset(
    root: Path, directory: Path, plan: OsrmBuildPlan, receipt: OsrmCoarseReceipt
) -> Path:
    """Publish or verify an existing immutable target, retaining inputs/receipts."""
    return await run_cpu(_publish_osrm_dataset, root, directory, plan, receipt)


def _abandon_osrm_attempt(directory: Path, plan: OsrmBuildPlan, attempt: OsrmBuildAttempt) -> None:
    """Only called after the controller reacquires the physical global build gate."""
    if attempt.plan_identifier != plan.identifier:
        raise OsrmBuildError("osrm_build_plan_changed")
    try:
        _ = _verify_osrm_attempt(directory, plan, attempt)
    except OsrmBuildError as error:
        if error.reason != "osrm_build_attempt_uncertain":
            raise
    else:
        raise OsrmBuildError("osrm_completed_build_cannot_be_abandoned")
    _write_once(
        directory,
        attempt.locator + "/abandoned.json",
        json.dumps(
            {
                "plan_identifier": plan.identifier,
                "attempt_identifier": attempt.identifier,
            },
            sort_keys=True,
        ).encode(),
    )
    graph = directory / attempt.locator / "graph"
    try:
        descriptor = open_dataset_directory(graph)
    except FileNotFoundError:
        return
    try:
        allowed = {"coverage.geojson", "region.osm.pbf", "region.osrm"} | {
            source.id[3:] + "-crop.osm.pbf" for source in plan.sources
        }
        names = os.listdir(descriptor)
        if len(names) > 2000:
            raise OsrmBuildError("osrm_build_scratch_invalid")
        # Validate the entire scratch inventory before deleting anything. Do not
        # recurse through unfamiliar tool output, directories, or symlinks.
        for name in names:
            info = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            if (
                (name not in allowed and not name.startswith("region.osrm."))
                or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
            ):
                raise OsrmBuildError("osrm_build_scratch_invalid")
        for name in names:
            os.unlink(name, dir_fd=descriptor)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


async def abandon_osrm_attempt(
    directory: Path, plan: OsrmBuildPlan, attempt: OsrmBuildAttempt
) -> None:
    """Delete only proven-owned uncertain graph scratch, retaining receipts/sources.

    The caller must hold its freshly acquired global workflow gate: an incomplete
    receipt alone is not proof that a native process has stopped using scratch.
    """
    await run_cpu(_abandon_osrm_attempt, directory, plan, attempt)


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
