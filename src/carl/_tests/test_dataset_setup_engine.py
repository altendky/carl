"""Real offline GeoNames construction exercises the shared foreground phase engine."""

# Resource and physical journal assertions cover the public setup lifecycle.
# pyright: reportPrivateUsage=false

import datetime
import hashlib
import json
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Literal, override

import anyio
import httpx
import pytest

import carl.io.dataset_setup as setup_engine_module
import carl.io.geonames as geonames
import carl.io.osrm_build as osrm
from carl._tests.test_configuration import _write_configuration
from carl._tests.test_geonames import _transport
from carl._tests.test_osrm_data import ORIGIN, _index
from carl._tests.test_osrm_phases import _Commands
from carl._tests.test_travel_stages import _held
from carl.core.dataset_setup import DatasetSetupState
from carl.core.native_build import NativeBuildSpec
from carl.core.osrm_build import OsrmToolchain
from carl.core.travel_data import TravelDataFailure, TravelDataSetupRequest
from carl.io.configuration import ConfigurationFailure, load_configuration
from carl.io.dataset_setup import (
    DatasetSetupEngine,
    prepare_dataset_setup,
    run_dataset_setup_direct,
)
from carl.io.dataset_workflow_files import active_journal, dataset_workflow_gate, private_directory
from carl.io.dataset_workflow_owner import DatasetWorkflowOwner, open_dataset_workflow
from carl.io.native_build_receipts import open_receipt_directory, write_start_receipt
from carl.io.native_builds import BuildCommandCapture
from carl.io.resource_permits import cpu_reservation
from carl.io.sqlite import Database
from carl.io.travel_data import TravelDataManager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


class _ProcessLoss(BaseException):
    """An interrupted caller cannot run ordinary exception finalization."""


@pytest.mark.parametrize("terminal", ("failed", "cancelled"))
async def test_terminal_journal_overrides_interrupted_old_progress(
    tmp_path: Path, terminal: Literal["failed", "cancelled"]
) -> None:
    directories, _ = _write_configuration(tmp_path)
    private_directory(directories.travel_data_directory)
    request = TravelDataSetupRequest(request_identifier="terminal-status", provider="geonames")
    manager = TravelDataManager(directories)
    async with Database.managed(directories.database_file, initialize=True):
        state = await prepare_dataset_setup(directories, request)
        async with open_dataset_workflow(directories, state.definition) as owner:
            manager._progress(
                provider="geonames", state="building", request_identifier=request.request_identifier
            )
            await owner.finish(terminal)
        # Process loss before the separate advisory progress write must not
        # turn a terminal request into an apparent resumable interruption.
        status = await manager.status()
        assert status.build is not None and status.build["state"] == terminal
        assert status.build["running"] is False


async def test_failed_initial_configuration_releases_unprepared_reservation(tmp_path: Path) -> None:
    directories, configuration = _write_configuration(tmp_path)
    private_directory(directories.travel_data_directory)
    request = TravelDataSetupRequest(request_identifier="unprepared-failure", provider="osrm")
    async with Database.managed(directories.database_file, initialize=True):
        state = await prepare_dataset_setup(directories, request, origin=ORIGIN)
        async with open_dataset_workflow(directories, state.definition):
            pass  # A process can disappear before its first state checkpoint.
        configuration.unlink()
        engine = DatasetSetupEngine(
            directories,
            transport=httpx.MockTransport(
                lambda _: pytest.fail("No HTTP before valid preparation")
            ),
        )
        with pytest.raises(ConfigurationFailure):
            await run_dataset_setup_direct(engine, request)
        journal = active_journal(directories)
        assert journal is not None and journal.state == "failed"
        with dataset_workflow_gate(directories):
            pass


async def test_source_http_failure_keeps_expected_geonames_classification(tmp_path: Path) -> None:
    directories, _ = _write_configuration(tmp_path)
    private_directory(directories.travel_data_directory)
    request = TravelDataSetupRequest(
        request_identifier="source-http-failure", provider="geonames", activate=False
    )
    calls = 0

    def failed(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    async with Database.managed(directories.database_file, initialize=True):
        engine = DatasetSetupEngine(directories, transport=httpx.MockTransport(failed))
        with pytest.raises(TravelDataFailure) as failure:
            await run_dataset_setup_direct(engine, request)
        assert failure.value.reason == "geonames_http_failed" and calls == 1


async def test_explicit_resume_of_incomplete_source_waits_without_execution_permits(
    tmp_path: Path,
) -> None:
    directories, _ = _write_configuration(tmp_path)
    private_directory(directories.travel_data_directory)
    request = TravelDataSetupRequest(
        request_identifier="incomplete-source", provider="geonames", activate=False
    )
    fixture = _transport()
    calls = 0

    class Interrupted(httpx.AsyncByteStream):
        @override
        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b"incomplete"
            raise _ProcessLoss

    async def wire(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return (
            httpx.Response(200, stream=Interrupted())
            if calls == 1
            else await fixture.handle_async_request(request)
        )

    async with Database.managed(directories.database_file, initialize=True) as database:
        engine = DatasetSetupEngine(directories, transport=httpx.MockTransport(wire))
        with pytest.raises(_ProcessLoss):
            await run_dataset_setup_direct(engine, request)
        finished = anyio.Event()

        async def resume() -> None:
            result = await run_dataset_setup_direct(engine, request)
            assert result.dataset_identifier.startswith("geonames-")
            finished.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(resume)
            with anyio.fail_after(10):
                while (
                    journal := active_journal(directories)
                ) is None or journal.state != "waiting":
                    await anyio.sleep(0.01)
                assert calls == 1  # No immediate retry while waiting for the persisted deadline.
                assert await _held(database, "http") == await _held(database, "cpu") == 0
                async with cpu_reservation(4):
                    pass
                await finished.wait()
        assert calls == 4  # One incomplete request plus one request per original source.


@pytest.mark.parametrize("interrupted", ("capture_http", "geonames_build", "activate"))
async def test_completed_phases_replay_without_another_download_or_sqlite_build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interrupted: Literal["capture_http", "geonames_build", "activate"],
) -> None:
    directories, configuration = _write_configuration(tmp_path)
    private_directory(directories.travel_data_directory)
    manager = TravelDataManager(directories)
    fixture = _transport()
    requests: list[str] = []
    sqlite_builds = 0
    original_build = geonames._build_database

    def build(*args, **kwargs):
        nonlocal sqlite_builds
        sqlite_builds += 1
        return original_build(*args, **kwargs)

    monkeypatch.setattr(geonames, "_build_database", build)
    request = TravelDataSetupRequest(
        request_identifier="resumable-real-build", provider="geonames", activate=True
    )
    async with Database.managed(directories.database_file, initialize=True) as database:

        async def wire(request: httpx.Request) -> httpx.Response:
            assert await _held(database, "http") == 1
            assert await _held(database, "cpu") == 0
            requests.append(str(request.url))
            return await fixture.handle_async_request(request)

        engine = DatasetSetupEngine(directories, manager, httpx.MockTransport(wire))
        original_step = DatasetSetupEngine.step
        stopped = False

        async def step(
            self: DatasetSetupEngine, owner: DatasetWorkflowOwner, state: DatasetSetupState
        ) -> DatasetSetupState:
            nonlocal stopped
            following = await original_step(self, owner, state)
            if state.phase == interrupted and not stopped:
                stopped = True
                raise _ProcessLoss
            return following

        with monkeypatch.context() as fault:
            fault.setattr(DatasetSetupEngine, "step", step)
            with pytest.raises(_ProcessLoss):
                await run_dataset_setup_direct(engine, request)
        journal = active_journal(directories)
        assert journal is not None and journal.state == "parked"
        first_generation = journal.fence.generation
        result = await run_dataset_setup_direct(engine, request)
        assert result.activated and result.dataset_identifier.startswith("geonames-")
        assert len(requests) == 3 and len(set(requests)) == 3
        assert sqlite_builds == 1
        journal = active_journal(directories)
        assert journal is not None and journal.state == "completed"
        assert journal.fence.generation == first_generation + 1
        selected = load_configuration(configuration).configuration.geonames
        assert selected is not None and selected.dataset_identifier == result.dataset_identifier
        assert await run_dataset_setup_direct(engine, request) == result
        assert len(requests) == 3 and sqlite_builds == 1
        assert await _held(database, "cpu") == await _held(database, "http") == 0
        workspace = (
            directories.travel_data_directory
            / ".workflows"
            / journal.definition.workflow_identifier
        )
        assert not (workspace / "downloads").exists()
        assert (workspace / "workflow.json").is_file()


async def test_completed_request_replay_rechecks_published_integrity(tmp_path: Path) -> None:
    directories, _ = _write_configuration(tmp_path)
    private_directory(directories.travel_data_directory)
    request = TravelDataSetupRequest(
        request_identifier="completed-integrity", provider="geonames", activate=False
    )
    async with Database.managed(directories.database_file, initialize=True):
        engine = DatasetSetupEngine(directories, TravelDataManager(directories), _transport())
        result = await run_dataset_setup_direct(engine, request)
        database = Path(result.manifest_path).parent / "places.sqlite3"
        content = bytearray(database.read_bytes())
        content[-1] ^= 1
        database.write_bytes(content)
        engine.transport = httpx.MockTransport(
            lambda _: pytest.fail("Completed replay queried HTTP")
        )
        with pytest.raises(TravelDataFailure, match="missing or invalid"):
            await run_dataset_setup_direct(engine, request)


@pytest.mark.parametrize("interrupted", ("osrm_build", "publish", "partition", "cleanup"))
async def test_osrm_pipeline_pins_fallback_snapshot_and_replays_without_redownload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interrupted: Literal["osrm_build", "publish", "partition", "cleanup"],
) -> None:
    directories, _ = _write_configuration(tmp_path)
    private_directory(directories.travel_data_directory)
    profile = tmp_path / "fixture-profiles" / "car.lua"
    profile.parent.mkdir(mode=0o700)
    profile.write_text("return {}")
    executable = osrm._executable(Path(sys.executable))
    tools = OsrmToolchain(
        osmium=executable, extract=executable, partition=executable, customize=executable
    )
    original_prepare = osrm.prepare_osrm_seed

    def prepare(directory: Path, **kwargs):
        return original_prepare(
            directory, **kwargs, toolchain=tools, profile=profile, package_version="26.10.0"
        )

    monkeypatch.setattr(setup_engine_module, "prepare_osrm_seed", prepare)
    commands = _Commands()
    calls: list[str] = []
    fallback = False
    interrupted_native = False
    request = TravelDataSetupRequest(
        request_identifier="osrm-full-replay", provider="osrm", activate=False
    )
    async with Database.managed(directories.database_file, initialize=True) as database:

        async def wire(request: httpx.Request) -> httpx.Response:
            nonlocal fallback
            assert await _held(database, "http") == 1 and await _held(database, "cpu") == 0
            path = request.url.path
            calls.append(path)
            if path.endswith("index-v1.json"):
                return httpx.Response(200, json=_index())
            if path.endswith(".md5"):
                if "pennsylvania" in path and not fallback:
                    fallback = True
                    return httpx.Response(404)
                return httpx.Response(
                    200, content=hashlib.md5(b"pbf", usedforsecurity=False).hexdigest().encode()
                )
            assert path.endswith(".osm.pbf") and "-latest" not in path
            return httpx.Response(200, content=b"pbf")

        async def command(spec: NativeBuildSpec) -> BuildCommandCapture:
            nonlocal interrupted_native
            assert await _held(database, "cpu") == 4 and await _held(database, "http") == 0
            if (
                interrupted == "partition"
                and not interrupted_native
                and spec.receipt_directory.name == "osrm-partition"
            ):
                interrupted_native = True
                os.close(open_receipt_directory(spec.receipt_directory, create=True))
                write_start_receipt(spec)
                raise _ProcessLoss
            return await commands.run(spec)

        engine = DatasetSetupEngine(
            directories, transport=httpx.MockTransport(wire), command_runner=command
        )
        original_step = DatasetSetupEngine.step
        stopped = False

        async def step(
            self: DatasetSetupEngine, owner: DatasetWorkflowOwner, state: DatasetSetupState
        ) -> DatasetSetupState:
            nonlocal stopped
            following = await original_step(self, owner, state)
            if state.phase == interrupted and not stopped:
                stopped = True
                raise _ProcessLoss
            return following

        with monkeypatch.context() as fault:
            fault.setattr(DatasetSetupEngine, "step", step)
            with pytest.raises(_ProcessLoss):
                await run_dataset_setup_direct(engine, request, origin=ORIGIN)
        before = len(commands.calls)
        result = await run_dataset_setup_direct(engine, request, origin=ORIGIN)
        if interrupted != "partition":
            assert len(commands.calls) == before
        assert len(calls) == 7  # index, two date candidates, two dated PBFs
        manifest = json.loads(Path(result.manifest_path).read_bytes())
        journal = active_journal(directories)
        assert journal is not None and journal.state == "completed"
        state = DatasetSetupState.model_validate_json(json.dumps(journal.checkpoint))
        anchor = datetime.date.fromisoformat(state.value["plan"]["seed"]["anchor_date"])
        assert {source["snapshot"] for source in manifest["sources"]} == {
            (anchor - datetime.timedelta(days=2)).strftime("%y%m%d")
        }
        assert {source["data_timestamp_utc"] for source in manifest["sources"]} == {
            "2026-10-17T20:20:51Z"
        }
        workspace = (
            directories.travel_data_directory / ".workflows" / state.definition.workflow_identifier
        )
        assert not (workspace / "downloads").exists()
        assert list((workspace / "osrm-attempts").glob("*/receipts/osrm-version/complete.json"))
        assert await run_dataset_setup_direct(engine, request, origin=ORIGIN) == result
        assert len(calls) == 7
