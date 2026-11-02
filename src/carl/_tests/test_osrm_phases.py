"""Offline phase tests use real command receipts without native builds."""

# Fixtures publish the exact command receipts expected by the provider adapter.
# pyright: reportPrivateUsage=false

import hashlib
import json
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import final, override

import httpx
import pytest

from carl._tests.test_osrm_data import _index, _seed
from carl.core.dataset_downloads import FileReceipt
from carl.core.native_build import NativeBuildResult, NativeBuildSpec
from carl.core.osrm_build import OsrmBuildAttempt, OsrmBuildPlan
from carl.io import osrm_build as osrm_data
from carl.io.dataset_downloads import download_dataset_source
from carl.io.native_build_receipts import (
    open_receipt_directory,
    write_complete_receipt,
    write_start_receipt,
)
from carl.io.native_builds import BuildCommandCapture
from carl.io.resource_permits import run_cpu
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


class _Body(httpx.AsyncByteStream):
    @override
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"pbf"


@final
class _Commands:
    def __init__(self) -> None:
        self.calls: list[NativeBuildSpec] = []
        self.failure: str | None = None
        self.mismatched_timestamps = False

    async def run(self, spec: NativeBuildSpec) -> BuildCommandCapture:
        return await run_cpu(self._capture, spec)

    def _capture(self, spec: NativeBuildSpec) -> BuildCommandCapture:
        self.calls.append(spec)
        assert spec.threads == 4 and spec.on_output_limit == "truncate"
        os.close(open_receipt_directory(spec.receipt_directory, create=True))
        started = write_start_receipt(spec)
        phase = spec.receipt_directory.name
        stdout = b""
        if phase.startswith("timestamp-"):
            stdout = b"2026-10-17T20:20:51Z\n"
            if self.mismatched_timestamps and "pennsylvania" in phase:
                stdout = b"2026-10-17T21:00:00Z\n"
        if phase == "osrm-version":
            stdout = b"v26.10.0\n"
        if "-o" in spec.argv:
            output = Path(spec.argv[spec.argv.index("-o") + 1])
            _ = output.write_bytes(b"crop")
            output.chmod(0o600)
        if phase == "osrm-extract":
            _ = (spec.directory / "region.osrm.partition").write_bytes(b"before partition")
        if phase == "osrm-customize":
            for name in osrm_data._REQUIRED_FILES:
                _ = (spec.directory / name).write_bytes(b"native graph " + name.encode())
                (spec.directory / name).chmod(0o644)
        stderr = b"private diagnostic credentials" if phase == self.failure else b""
        for name, data in (("stdout.log", stdout), ("stderr.log", stderr)):
            descriptor = os.open(
                spec.receipt_directory / name, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
            )
            with os.fdopen(descriptor, "wb") as stream:
                _ = stream.write(data)
        result = NativeBuildResult(
            execution_identifier=spec.execution_identifier,
            reason="exit_nonzero" if phase == self.failure else "completed",
            returncode=2 if phase == self.failure else 0,
            stdout_head=stdout[:4096].decode(),
            stdout_bytes=len(stdout),
            stderr_bytes=len(stderr),
            stdout_sha256=hashlib.sha256(stdout).hexdigest(),
            stderr_sha256=hashlib.sha256(stderr).hexdigest(),
        )
        receipt = write_complete_receipt(spec, result, started_at_utc_ns=started.started_at_utc_ns)
        return BuildCommandCapture(result, receipt, False)


async def _inputs(
    tmp_path: Path,
) -> tuple[Path, OsrmBuildPlan, tuple[FileReceipt, ...], OsrmBuildAttempt]:
    seed = await run_cpu(_seed, tmp_path)
    directory = tmp_path / "workspace"
    candidates = osrm_data.select_osrm_sources(json.dumps(_index()).encode(), seed)
    checksum = hashlib.md5(b"pbf", usedforsecurity=False).hexdigest()
    plan = osrm_data.freeze_osrm_plan(seed, candidates, age=1, checksums=(checksum, checksum))
    receipts = tuple(
        [
            await download_dataset_source(
                source,
                directory=directory,
                execution_identifier="source-" + str(index),
                transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=_Body())),
            )
            for index, source in enumerate(osrm_data.osrm_download_sources(plan))
        ]
    )
    attempt = await run_cpu(
        lambda: osrm_data.create_osrm_attempt(directory, plan, identifier="native-0")
    )
    return directory, plan, receipts, attempt


async def test_build_publish_and_rename_replay_preserve_sources_and_legacy_id(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        directory, plan, sources, attempt = await _inputs(tmp_path)
        commands = _Commands()
        receipt = await osrm_data.build_osrm_attempt(
            directory, plan, attempt, sources, run_command=commands.run
        )
        expected = hashlib.sha256(
            json.dumps(
                {
                    "sources": [source.model_dump(mode="json") for source in receipt.sources],
                    "profile_sha256": plan.seed.profile_sha256,
                    "osrm_version": "v26.10.0",
                    "coverage_sha256": plan.seed.coverage_sha256,
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()
        assert receipt.dataset_identifier == "osrm-" + expected[:24]
        calls = len(commands.calls)
        assert await osrm_data.verify_osrm_attempt(directory, plan, attempt) == receipt
        assert (
            await osrm_data.build_osrm_attempt(
                directory, plan, attempt, sources, run_command=commands.run
            )
            == receipt
        )
        assert len(commands.calls) == calls
        manifest = await osrm_data.publish_osrm_dataset(
            tmp_path / "published", directory, plan, receipt
        )
        original = manifest.read_bytes()
        # Legacy builds used normal umask file permissions inside private0700
        # dataset directories. Reuse must still verify them after graph/ moved.
        for path in manifest.parent.iterdir():
            path.chmod(0o644)
        assert (
            await osrm_data.publish_osrm_dataset(tmp_path / "published", directory, plan, receipt)
            == manifest
        )
        assert manifest.read_bytes() == original
        assert all((directory / source.locator).read_bytes() == b"pbf" for source in sources)
        assert not list(manifest.parent.glob("*.pbf"))
        assert list((directory / attempt.locator / "intermediates").glob("*.pbf"))
        assert any("complete_ways" in call.argv for call in commands.calls)
        assert any("check-refs" in call.argv for call in commands.calls)
        assert all("-m" not in call.argv for call in commands.calls)
        _ = (manifest.parent / receipt.files[0].name).write_bytes(b"corrupted legacy output")
        with pytest.raises(osrm_data.OsrmBuildError, match="existing_dataset_integrity_mismatch"):
            _ = await osrm_data.publish_osrm_dataset(
                tmp_path / "published", directory, plan, receipt
            )


async def test_all_native_complete_recovers_missing_coarse_marker_without_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        directory, plan, sources, attempt = await _inputs(tmp_path)
        original = osrm_data._write_once

        def fail(directory: Path, locator: str, content: bytes) -> None:
            if locator == attempt.locator + "/complete.json":
                raise OSError("injected after completed customize")
            original(directory, locator, content)

        monkeypatch.setattr(osrm_data, "_write_once", fail)
        commands = _Commands()
        with pytest.raises(OSError):
            _ = await osrm_data.build_osrm_attempt(
                directory, plan, attempt, sources, run_command=commands.run
            )
        calls = len(commands.calls)
        monkeypatch.setattr(osrm_data, "_write_once", original)
        receipt = await osrm_data.verify_osrm_attempt(directory, plan, attempt)
        assert receipt.files and len(commands.calls) == calls
        with pytest.raises(osrm_data.OsrmBuildError, match="completed_build_cannot_be_abandoned"):
            await osrm_data.abandon_osrm_attempt(directory, plan, attempt)


async def test_incomplete_native_attempt_requires_explicit_fresh_scratch(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        directory, plan, sources, attempt = await _inputs(tmp_path)
        commands = _Commands()

        async def interrupted(spec: NativeBuildSpec) -> BuildCommandCapture:
            if spec.receipt_directory.name == "osrm-partition":
                os.close(open_receipt_directory(spec.receipt_directory, create=True))
                _ = write_start_receipt(spec)
                raise OSError("crashed native dispatch")
            return await commands.run(spec)

        with pytest.raises(OSError):
            _ = await osrm_data.build_osrm_attempt(
                directory, plan, attempt, sources, run_command=interrupted
            )
        calls = len(commands.calls)
        with pytest.raises(osrm_data.OsrmBuildError, match="attempt_uncertain"):
            _ = await osrm_data.build_osrm_attempt(
                directory, plan, attempt, sources, run_command=commands.run
            )
        assert len(commands.calls) == calls
        await osrm_data.abandon_osrm_attempt(directory, plan, attempt)
        assert list((directory / attempt.locator / "graph").iterdir()) == []
        assert (directory / attempt.locator / "receipts" / "osrm-partition" / "start.json").exists()
        assert all((directory / source.locator).exists() for source in sources)
        fresh = osrm_data.create_osrm_attempt(directory, plan, identifier="native-1")
        receipt = await osrm_data.build_osrm_attempt(
            directory, plan, fresh, sources, run_command=commands.run
        )
        assert receipt.dataset_identifier.startswith("osrm-")


@pytest.mark.parametrize("failure", ("checksum", "timestamp", "tool", "tamper"))
async def test_validation_failure_preserves_old_map_and_sanitizes_errors(
    tmp_path: Path, failure: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        directory, plan, sources, attempt = await _inputs(tmp_path)
        previous = tmp_path / "published" / "osrm-old" / "manifest.json"
        (tmp_path / "published").mkdir(mode=0o700)
        previous.parent.mkdir(mode=0o700)
        _ = previous.write_bytes(b"previous map")
        commands = _Commands()
        if failure == "checksum":
            _ = (directory / sources[0].locator).write_bytes(b"bad source")
        commands.mismatched_timestamps = failure == "timestamp"
        commands.failure = "osrm-extract" if failure == "tool" else None
        with pytest.raises(osrm_data.OsrmBuildError) as error:
            receipt = await osrm_data.build_osrm_attempt(
                directory, plan, attempt, sources, run_command=commands.run
            )
            assert failure == "tamper"
            _ = (directory / attempt.locator / "graph" / receipt.files[0].name).write_bytes(
                b"tampered"
            )
            _ = await osrm_data.publish_osrm_dataset(
                tmp_path / "published", directory, plan, receipt
            )
        assert previous.read_bytes() == b"previous map"
        assert "credentials" not in str(error.value)
