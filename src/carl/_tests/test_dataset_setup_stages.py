"""Opt-in setup phases retain file ownership and actual phase admission on replay."""

# Integration assertions inspect durable resource and operation identity.
# pyright: reportPrivateUsage=false

from pathlib import Path
from time import time_ns
from typing import override
from uuid import uuid4

import anyio
import httpx
import pytest

from carl._tests.test_dataset_workflow_owner import _directories
from carl._tests.test_stage_execution import _PROVENANCE
from carl._tests.test_travel_stages import _held
from carl.core.components import Component
from carl.core.dataset_downloads import DatasetSource, DatasetTransfer
from carl.core.dataset_setup import DatasetSetupReference, DatasetSetupState
from carl.core.json import encode_json
from carl.core.stages import StageDefinition, StageOutcome, WaitWork
from carl.core.travel_data import TravelDataSetupRequest, TravelDataSetupResult, travel_data_work
from carl.core.work import WorkRequester
from carl.core.worker import CompletedWork, TerminalFailureWork
from carl.dataset_setup_stages import (
    build_dataset_setup_owned_registry,
    build_dataset_setup_prepare_registry,
    is_dataset_setup_owned_stage,
    submit_dataset_setup_stage,
)
from carl.io.configuration import ConfigurationFailure
from carl.io.dataset_downloads import verify_dataset_file
from carl.io.dataset_setup import DatasetSetupEngine
from carl.io.dataset_setup_files import read_setup_state, read_setup_value
from carl.io.dataset_workflow_files import active_journal, dataset_workflow_gate
from carl.io.dataset_workflow_owner import (
    DatasetWorkflowBusy,
    DatasetWorkflowOwner,
    open_dataset_workflow,
)
from carl.io.native_build_protocol import NativeBuildCleanupError
from carl.io.paths import CarlDirectories
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import stage_manager
from carl.io.travel_data import TravelDataManager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


class _Engine(DatasetSetupEngine):
    def __init__(
        self, database: Database, directories: CarlDirectories, transport: httpx.AsyncBaseTransport
    ):
        super().__init__(directories, transport=transport)
        self.database = database
        self.phases: list[str] = []
        self.verify_started = anyio.Event()
        self.verify_release: anyio.Event | None = None
        self.failure: Exception | None = None

    @override
    async def step(
        self, owner: DatasetWorkflowOwner, state: DatasetSetupState
    ) -> DatasetSetupState:
        _ = owner.lock_fd
        self.phases.append(state.phase)
        if self.failure is not None:
            raise self.failure
        if state.phase == "prepare":
            assert await _held(self.database, "cpu") == 1
            source = DatasetSource(
                identifier="fixture",
                name="source.zip",
                url="https://dataset.invalid/source.zip",
                maximum_bytes=1024,
            )
            return DatasetSetupState(
                definition=state.definition,
                phase="capture_http",
                value={
                    "capture": {
                        "source": source.model_dump(mode="json"),
                        "purpose": "fixture",
                        "key": "source",
                        "attempt": 0,
                    }
                },
            )
        if state.phase in ("capture_http", "capture_prepare"):
            return await super().step(owner, state)
        assert state.phase == "capture_verify"
        assert await _held(self.database, "http") == 0
        assert await _held(self.database, "cpu") == 1
        self.verify_started.set()
        if self.verify_release is not None:
            await self.verify_release.wait()
        assert isinstance(state.value, dict)
        capture = state.value["capture"]
        assert isinstance(capture, dict)
        source = DatasetSource.model_validate_json(encode_json(capture["source"]))
        reference = DatasetSetupReference.model_validate_json(encode_json(capture["transfer"]))
        transfer = DatasetTransfer.model_validate_json(
            encode_json(read_setup_value(owner.directory, reference))
        )
        receipt = await verify_dataset_file(source, transfer, directory=owner.directory)
        assert receipt.bytes == len(b"fixture source data")
        result = TravelDataSetupResult(
            provider="geonames",
            dataset_identifier="fixture-dataset",
            manifest_path=str(owner.directory / "manifest.json"),
            activated=False,
            message="Fixture verified",
        )
        return DatasetSetupState(
            definition=state.definition,
            phase="done",
            value={"result": result.model_dump(mode="json")},
        )


async def _prepare(
    database: Database, engine: DatasetSetupEngine, *, request: TravelDataSetupRequest | None = None
) -> tuple[StageOutcome, Component, StageDefinition]:
    request = request or TravelDataSetupRequest(
        request_identifier=str(uuid4()), provider="geonames", activate=False
    )
    work = travel_data_work(request)
    await database.enqueue_work(
        work,
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier="fixture", context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    registry = build_dataset_setup_prepare_registry(database, engine)
    adapter = registry.adapters[0]
    initial = adapter.initial_stage(work, str(uuid4()), str(uuid4()))
    await database.register_stage(initial, created_at_utc_ns=time_ns())
    outcome = await _run(database, registry, initial, adapter.component)
    return outcome, adapter.component, initial


async def _run(
    database: Database, registry: StageHandlerRegistry, stage: StageDefinition, component: Component
) -> StageOutcome:
    async with stage_manager(database).admit(
        stage.identifier, component=component, provenance=_PROVENANCE, invocation={}
    ) as admission:
        assert admission.execution is not None
        return await registry.execute(admission.execution)


async def _wait_terminal(database: Database, work: str, state: str = "completed") -> None:
    with anyio.fail_after(10):
        while (await database.work(work))["state"] != state:
            await anyio.sleep(0.01)
        while (
            journal := active_journal(_directories(database.path.parent.parent))
        ) is not None and journal.state not in ("completed", "failed", "cancelled"):
            await anyio.sleep(0.01)


async def test_root_owner_separates_http_cpu_and_preserves_one_attempt(tmp_path: Path) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    calls = 0
    async with Database.managed(directories.database_file, initialize=True) as database:

        async def wire(_: httpx.Request) -> httpx.Response:
            nonlocal calls
            assert await _held(database, "http") == 1
            assert await _held(database, "cpu") == 0
            calls += 1
            return httpx.Response(200, content=b"fixture source data")

        engine = _Engine(database, directories, httpx.MockTransport(wire))
        engine.verify_release = anyio.Event()
        outcome, component, initial = await _prepare(database, engine)
        assert isinstance(outcome, WaitWork) and is_dataset_setup_owned_stage(outcome.continuation)
        assert not (directories.travel_data_state_directory / "active-workflow.json").exists()
        assert await submit_dataset_setup_stage(
            stage_manager(database), engine, outcome.continuation, component, _PROVENANCE, {}
        )
        with anyio.fail_after(10):
            await engine.verify_started.wait()
        status = await TravelDataManager(directories).status()
        assert status.build is not None
        assert status.build["state"] == "building" and status.build["stage"] == "capture_verify"
        assert status.build["physical_owner_present"] is True
        with pytest.raises(DatasetWorkflowBusy), dataset_workflow_gate(directories):
            pass
        assert await _held(database, "http") == 0
        engine.verify_release.set()
        await _wait_terminal(database, initial.work_item_identifier)
        work = await database.work(initial.work_item_identifier)
        assert work["attempt"] == 1 and calls == 1
        assert engine.phases == ["prepare", "capture_http", "capture_verify"]
        assert (await database.operation(initial.operation_identifier))["state"] == "completed"


async def test_prepare_cpu_capacity_fails_before_owner_or_http(tmp_path: Path) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    async with Database.managed(
        directories.database_file, initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        engine = DatasetSetupEngine(
            directories,
            transport=httpx.MockTransport(
                lambda _: pytest.fail("No HTTP during rejected preparation")
            ),
        )
        outcome, _, _ = await _prepare(
            database,
            engine,
            request=TravelDataSetupRequest(
                request_identifier="too-many-threads", provider="osrm", threads=3
            ),
        )
        assert isinstance(outcome, TerminalFailureWork)
        assert outcome.error["kind"] == "osrm_cpu_capacity_exceeded"
        assert not (directories.travel_data_state_directory / "build.lock").exists()
        assert not directories.travel_data_directory.exists()


async def test_reopen_resumes_verified_wire_file_without_another_http_query(tmp_path: Path) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    async with Database.managed(directories.database_file, initialize=True) as database:
        engine = _Engine(
            database,
            directories,
            httpx.MockTransport(lambda _: httpx.Response(200, content=b"fixture source data")),
        )
        outcome, component, initial = await _prepare(database, engine)
        assert isinstance(outcome, WaitWork)
        state = read_setup_state(
            directories.travel_data_directory,
            DatasetSetupReference.model_validate_json(encode_json(outcome.continuation.payload)),
        )
        async with open_dataset_workflow(directories, state.definition) as owner:
            generation = owner.fence.generation
            registry = build_dataset_setup_owned_registry(database, engine, owner)
            capture = await _run(database, registry, outcome.continuation, component)
            assert isinstance(capture, WaitWork)
            verify = await _run(database, registry, capture.continuation, component)
            assert isinstance(verify, WaitWork)
            saved = verify.continuation
    async with Database.managed(directories.database_file) as database:
        engine = _Engine(
            database,
            directories,
            httpx.MockTransport(lambda _: pytest.fail("Restart must retain the completed source")),
        )
        assert await submit_dataset_setup_stage(
            stage_manager(database), engine, saved, component, _PROVENANCE, {}
        )
        await _wait_terminal(database, initial.work_item_identifier)
        journal = active_journal(directories)
        assert journal is not None and journal.fence.generation == generation + 1
        assert engine.phases == ["capture_verify"]
        assert (await database.work(initial.work_item_identifier))["attempt"] == 1


async def test_next_file_before_sql_suspend_does_not_advance_old_http_phase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    calls = 0
    async with Database.managed(directories.database_file, initialize=True) as database:

        def wire(_: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(200, content=b"fixture source data")

        engine = _Engine(database, directories, httpx.MockTransport(wire))
        outcome, component, initial = await _prepare(database, engine)
        assert isinstance(outcome, WaitWork)
        state = read_setup_state(
            directories.travel_data_directory,
            DatasetSetupReference.model_validate_json(encode_json(outcome.continuation.payload)),
        )
        async with open_dataset_workflow(directories, state.definition) as owner:
            registry = build_dataset_setup_owned_registry(database, engine, owner)
            first = await _run(database, registry, outcome.continuation, component)
            assert isinstance(first, WaitWork)
            http_stage = first.continuation
            original = Database.suspend_stage

            async def fail(*args: object, **kwargs: object) -> None:
                raise OSError("injected SQL publication failure")

            monkeypatch.setattr(Database, "suspend_stage", fail)
            with pytest.raises(OSError, match="publication failure"):
                await _run(database, registry, http_stage, component)
            assert await database.stage(http_stage.identifier) == http_stage
            assert calls == 1 and http_stage.resources[0].name == "http"
            monkeypatch.setattr(Database, "suspend_stage", original)
            resumed = await _run(database, registry, http_stage, component)
            assert isinstance(resumed, WaitWork)
            assert resumed.continuation.resources[0].name == "cpu"
            recovered = await _run(database, registry, resumed.continuation, component)
            assert isinstance(recovered, WaitWork)
            finished = await _run(database, registry, recovered.continuation, component)
            assert isinstance(finished, CompletedWork) and calls == 1
            await owner.finish()
        assert (await database.work(initial.work_item_identifier))["attempt"] == 1


@pytest.mark.parametrize("failure", ("configuration", "storage", "native", "native_group"))
async def test_expected_errors_terminal_but_unknown_native_cleanup_propagates(
    tmp_path: Path, failure: str
) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    async with Database.managed(directories.database_file, initialize=True) as database:
        engine = _Engine(
            database, directories, httpx.MockTransport(lambda _: pytest.fail("No query"))
        )
        outcome, component, _ = await _prepare(database, engine)
        assert isinstance(outcome, WaitWork)
        state = read_setup_state(
            directories.travel_data_directory,
            DatasetSetupReference.model_validate_json(encode_json(outcome.continuation.payload)),
        )
        engine.failure = {
            "configuration": ConfigurationFailure("invalid_configuration"),
            "storage": OSError("private-path-secret"),
            "native": NativeBuildCleanupError(),
            "native_group": ExceptionGroup("fixture", [NativeBuildCleanupError()]),
        }[failure]
        if failure.startswith("native"):
            with pytest.raises((NativeBuildCleanupError, ExceptionGroup)):
                async with open_dataset_workflow(directories, state.definition) as owner:
                    _ = await _run(
                        database,
                        build_dataset_setup_owned_registry(database, engine, owner),
                        outcome.continuation,
                        component,
                    )
            journal = active_journal(directories)
            assert journal is not None and journal.state == "quarantined"
        else:
            async with open_dataset_workflow(directories, state.definition) as owner:
                failed = await _run(
                    database,
                    build_dataset_setup_owned_registry(database, engine, owner),
                    outcome.continuation,
                    component,
                )
                assert isinstance(failed, TerminalFailureWork)
                assert "private-path-secret" not in failed.model_dump_json()
                await owner.finish("failed")


async def test_wrong_resource_or_workflow_reference_never_steps_engine(tmp_path: Path) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    async with Database.managed(directories.database_file, initialize=True) as database:
        engine = _Engine(
            database, directories, httpx.MockTransport(lambda _: pytest.fail("No query"))
        )
        outcome, component, _ = await _prepare(database, engine)
        assert isinstance(outcome, WaitWork)
        with pytest.raises(ValueError, match="resource"):
            await submit_dataset_setup_stage(
                stage_manager(database),
                engine,
                outcome.continuation.model_copy(update={"resources": ()}),
                component,
                _PROVENANCE,
                {},
            )
        assert not (directories.travel_data_state_directory / "active-workflow.json").exists()
        assert not engine.phases
        state = read_setup_state(
            directories.travel_data_directory,
            DatasetSetupReference.model_validate_json(encode_json(outcome.continuation.payload)),
        )
        wrong = state.definition.model_copy(update={"workflow_identifier": "other-workflow"})
        async with open_dataset_workflow(directories, wrong) as owner:
            result = await _run(
                database,
                build_dataset_setup_owned_registry(database, engine, owner),
                outcome.continuation,
                component,
            )
            assert isinstance(result, TerminalFailureWork) and not engine.phases
            await owner.finish("failed")


async def test_malformed_initial_request_becomes_terminal_before_owner(tmp_path: Path) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    async with Database.managed(directories.database_file, initialize=True) as database:
        engine = DatasetSetupEngine(directories)
        work = travel_data_work(
            TravelDataSetupRequest(request_identifier="invalid", provider="geonames")
        ).model_copy(update={"payload": {"provider": "unsupported"}})
        await database.enqueue_work(
            work,
            WorkRequester(
                request_identifier="invalid", kind=("test",), identifier="invalid", context={}
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        registry = build_dataset_setup_prepare_registry(database, engine)
        adapter = registry.adapters[0]
        stage = adapter.initial_stage(work, str(uuid4()), str(uuid4()))
        await database.register_stage(stage, created_at_utc_ns=time_ns())
        result = await _run(database, registry, stage, adapter.component)
        assert isinstance(result, TerminalFailureWork)
        assert not (directories.travel_data_state_directory / "build.lock").exists()


async def test_active_cancel_closes_root_owner_and_keeps_http_files(tmp_path: Path) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    async with Database.managed(directories.database_file, initialize=True) as database:
        engine = _Engine(
            database,
            directories,
            httpx.MockTransport(lambda _: httpx.Response(200, content=b"fixture source data")),
        )
        engine.verify_release = anyio.Event()
        outcome, component, initial = await _prepare(database, engine)
        assert isinstance(outcome, WaitWork)
        assert await submit_dataset_setup_stage(
            stage_manager(database), engine, outcome.continuation, component, _PROVENANCE, {}
        )
        with anyio.fail_after(10):
            await engine.verify_started.wait()
        await database.cancel_work(
            initial.work_item_identifier,
            reason="test",
            caller="test",
            requested_at_utc_ns=time_ns(),
        )
        await _wait_terminal(database, initial.work_item_identifier, "cancelled")
        journal = active_journal(directories)
        assert journal is not None and journal.state == "cancelled"
        assert tuple(directories.travel_data_directory.glob(".workflows/*/downloads/*/*.data"))
        assert await _held(database, "cpu") == await _held(database, "http") == 0
