"""Worker-scoped dataset services join their own actors without closing shared state."""

# Inspect bounded service ownership and retained workflow guards during shutdown.
# pyright: reportPrivateUsage=false

import os
from dataclasses import replace
from pathlib import Path
from time import time_ns
from uuid import uuid4

import anyio
import httpx
import pytest

import carl.dataset_setup_stages as setup_stages
from carl._tests.test_dataset_setup_stages import _Engine, _prepare
from carl._tests.test_dataset_workflow_owner import _bound, _directories, _Envelope
from carl._tests.test_dataset_workflows import _definition
from carl._tests.test_stage_execution import _COMPONENT, _PROVENANCE, _seed
from carl.core.stages import StageAttemptContext, StageOutcome, StageWaitKind, WaitWork
from carl.core.worker import CompletedWork
from carl.dataset_setup_stages import build_dataset_setup_owned_registry, submit_dataset_setup_stage
from carl.io.dataset_setup import DatasetSetupEngine
from carl.io.dataset_workflow_files import dataset_workflow_gate
from carl.io.dataset_workflow_owner import (
    DatasetWorkflowBusy,
    DatasetWorkflowOwner,
    DatasetWorkflowService,
    managed_dataset_workflow_service,
    open_dataset_workflow,
)
from carl.io.native_build_protocol import NativeBuildCleanupError
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def test_scoped_service_reentry_has_no_cached_service_or_root_closer(
    tmp_path: Path,
) -> None:
    directories = _directories(tmp_path)
    async with Database.managed(directories.database_file, initialize=True) as database:
        manager = stage_manager(database)
        services, closers = dict(manager.resources._services), dict(manager.resources._closers)
        previous: DatasetWorkflowService | None = None
        async with open_dataset_workflow(directories, _definition()) as foreground:
            fence, revision = foreground.fence, foreground.journal.revision
            for runtime in range(3):

                def registry(_owner: DatasetWorkflowOwner) -> StageHandlerRegistry:
                    return StageHandlerRegistry((), ())

                async with managed_dataset_workflow_service(
                    manager, directories, registry, _PROVENANCE, {"runtime": runtime}
                ) as service:
                    assert service is not previous
                    assert not service._closing
                    assert service.registry_for_owner is registry
                    assert service.invocation == {"runtime": runtime}
                    assert manager.resources._services == services
                    assert manager.resources._closers == closers
                    assert foreground.lock_fd >= 0
                assert service._closing and service._done.is_set()
                assert foreground.fence == fence and foreground.journal.revision == revision
                previous = service
            await foreground.checkpoint("still-owned", {"foreground": True})
            with pytest.raises(DatasetWorkflowBusy), dataset_workflow_gate(directories):
                pass
            assert manager.resources._services == services
            assert manager.resources._closers == closers


async def test_stop_and_reenter_scoped_service_preserves_attempt_and_verified_files(
    tmp_path: Path,
) -> None:
    directories = _directories(tmp_path)
    async with Database.managed(directories.database_file, initialize=True) as database:
        manager = stage_manager(database)
        stage = await _seed(database, resources=())
        definition = _bound(database, stage)
        owners: list[DatasetWorkflowOwner] = []
        waiting, reopened = anyio.Event(), anyio.Event()

        async def first_phase(_payload: _Envelope, _context: StageAttemptContext) -> StageOutcome:
            _ = (owners[0].directory / "verified-source").write_bytes(b"retained verified bytes")
            await owners[0].checkpoint("captured", {"receipt": "verified-source"})
            waiting.set()
            return WaitWork(
                continuation=stage.model_copy(
                    update={
                        "identifier": str(uuid4()),
                        "not_before_utc_ns": time_ns() + 60_000_000_000,
                    }
                ),
                wait_kind=StageWaitKind.TIME,
                result={"captured": True},
            )

        def first_registry(owner: DatasetWorkflowOwner) -> StageHandlerRegistry:
            owners.append(owner)
            return StageHandlerRegistry(
                (), (TypedStageHandler(stage.kind, 1, _Envelope, first_phase),)
            )

        async with managed_dataset_workflow_service(
            manager, directories, first_registry, _PROVENANCE, {"runtime": "first"}
        ) as first:
            assert await first.submit(stage, component=_COMPONENT, definition=definition)
            with anyio.fail_after(5):
                await waiting.wait()
                while owners[0].journal.state != "waiting":
                    await anyio.sleep(0.01)
        assert first._done.is_set() and first._active is None
        assert owners[0].journal.state == "parked"
        generation = owners[0].fence.generation
        continuation = await database.active_stage_for_work(
            stage.work_item_identifier, stage.operation_identifier
        )
        assert continuation is not None

        def second_registry(owner: DatasetWorkflowOwner) -> StageHandlerRegistry:
            assert owner.fence.generation == generation + 1
            assert owner.journal.checkpoint == {"receipt": "verified-source"}
            assert (owner.directory / "verified-source").read_bytes() == b"retained verified bytes"
            reopened.set()
            return StageHandlerRegistry((), ())

        async with managed_dataset_workflow_service(
            manager, directories, second_registry, _PROVENANCE, {"runtime": "second"}
        ) as second:
            assert await second.submit(
                continuation, component=_COMPONENT, definition=definition, resume=True
            )
            with anyio.fail_after(5):
                await reopened.wait()
        assert second._done.is_set() and second._active is None
        assert (await database.work(stage.work_item_identifier))["attempt"] == 1
        assert (await database.operation(stage.operation_identifier))["state"] == "started"


async def test_fresh_scoped_service_does_not_bypass_quarantined_physical_owner(
    tmp_path: Path,
) -> None:
    directories = _directories(tmp_path)
    retained = -1
    async with Database.managed(directories.database_file, initialize=True) as database:
        stage = await _seed(database, resources=())
        manager = stage_manager(database)
        definition = _bound(database, stage)
        owners: list[DatasetWorkflowOwner] = []

        async def failed(_payload: _Envelope, _context: StageAttemptContext) -> StageOutcome:
            nonlocal retained
            retained = os.dup(owners[0].lock_fd)
            raise NativeBuildCleanupError

        def first_registry(owner: DatasetWorkflowOwner) -> StageHandlerRegistry:
            owners.append(owner)
            return StageHandlerRegistry((), (TypedStageHandler(stage.kind, 1, _Envelope, failed),))

        async def completed(_payload: _Envelope, _context: StageAttemptContext) -> CompletedWork:
            return CompletedWork(result={"recovered": True})

        def second_registry(_owner: DatasetWorkflowOwner) -> StageHandlerRegistry:
            return StageHandlerRegistry(
                (), (TypedStageHandler(stage.kind, 1, _Envelope, completed),)
            )

        try:
            async with managed_dataset_workflow_service(
                manager, directories, first_registry, _PROVENANCE, {}
            ) as first:
                assert await first.submit(stage, component=_COMPONENT, definition=definition)
                with anyio.fail_after(5):
                    await first._done.wait()
            assert owners[0].journal.state == "quarantined" and retained >= 0
            async with managed_dataset_workflow_service(
                manager, directories, second_registry, _PROVENANCE, {}
            ) as second:
                assert not await second.submit(
                    stage, component=_COMPONENT, definition=definition, resume=True
                )
                os.close(retained)
                retained = -1
                assert await second.submit(
                    stage, component=_COMPONENT, definition=definition, resume=True
                )
                with anyio.fail_after(5):
                    await second._done.wait()
            assert (await database.work(stage.work_item_identifier))["state"] == "completed"
        finally:
            if retained >= 0:
                os.close(retained)


@pytest.mark.parametrize("mismatch", ("database", "resources", "directories"))
async def test_injected_setup_service_mismatch_fails_before_state_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mismatch: str
) -> None:
    directories = _directories(tmp_path)
    async with Database.managed(directories.database_file, initialize=True) as database:
        manager = stage_manager(database)
        async with Database.managed(tmp_path / "other.sqlite3", initialize=True) as other:
            service_manager = (
                stage_manager(other)
                if mismatch == "database"
                else replace(manager, resources=replace(manager.resources))
                if mismatch == "resources"
                else manager
            )
            service = DatasetWorkflowService(
                service_manager,
                _directories(tmp_path / "other") if mismatch == "directories" else directories,
                lambda _owner: StageHandlerRegistry((), ()),
                _PROVENANCE,
                {},
                _register_root_closer=False,
            )

            async def no_state_io(*_args: object) -> None:
                raise AssertionError("Mismatched service reached durable setup state")

            monkeypatch.setattr(setup_stages, "_state_for_stage", no_state_io)
            stage = await _seed(database, resources=())
            with pytest.raises(ValueError, match="share its database, resources and directories"):
                _ = await submit_dataset_setup_stage(
                    manager,
                    DatasetSetupEngine(directories),
                    stage,
                    _COMPONENT,
                    _PROVENANCE,
                    {},
                    service=service,
                )


async def test_setup_uses_injected_scoped_service_and_never_creates_root_cache(
    tmp_path: Path,
) -> None:
    directories = _directories(tmp_path)
    calls = 0

    def wire(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=b"fixture source data")

    async with Database.managed(directories.database_file, initialize=True) as database:
        manager = stage_manager(database)
        engine = _Engine(database, directories, httpx.MockTransport(wire))
        outcome, component, initial = await _prepare(database, engine)
        assert isinstance(outcome, WaitWork)
        async with managed_dataset_workflow_service(
            manager,
            directories,
            lambda owner: build_dataset_setup_owned_registry(database, engine, owner),
            _PROVENANCE,
            {},
        ) as service:
            assert await submit_dataset_setup_stage(
                manager, engine, outcome.continuation, component, _PROVENANCE, {}, service=service
            )
            with anyio.fail_after(10):
                await service._done.wait()
            assert (await database.work(initial.work_item_identifier))["state"] == "completed"
        assert calls == 1
        assert "dataset-workflow" not in manager.resources._services
        assert "dataset-workflow" not in manager.resources._closers
