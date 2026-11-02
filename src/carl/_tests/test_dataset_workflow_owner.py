"""Physical workflow gates, task-affine journals and ordinary durable stage ownership."""

# Tests inspect transfer/finalization fences and inject interrupted publications.
# pyright: reportPrivateUsage=false, reportPrivateLocalImportUsage=false

import os
from dataclasses import replace
from pathlib import Path
from time import time_ns
from typing import Literal
from uuid import uuid4

import anyio
import pytest

from carl._tests.test_dataset_workflows import _definition
from carl._tests.test_stage_execution import _COMPONENT, _PROVENANCE, _admit, _seed
from carl.core.dataset_workflows import DatasetWorkflowBinding, DatasetWorkflowDefinition
from carl.core.models import JsonValue, StrictModel
from carl.core.resources import ResourceRequest
from carl.core.stages import (
    StageAttemptContext,
    StageDefinition,
    StageOutcome,
    StageWaitKind,
    WaitWork,
)
from carl.core.worker import CompletedWork, RetryWork
from carl.io.dataset_workflow_files import dataset_workflow_gate, read_journal, write_journal
from carl.io.dataset_workflow_owner import (
    DatasetWorkflowBusy,
    DatasetWorkflowFenceLost,
    DatasetWorkflowOwner,
    DatasetWorkflowService,
    open_dataset_workflow,
)
from carl.io.native_build_protocol import NativeBuildCleanupError
from carl.io.paths import CarlDirectories
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _directories(tmp_path: Path) -> CarlDirectories:
    return CarlDirectories(
        **{name: tmp_path / name for name in ("config", "data", "cache", "state", "runtime")}
    )


async def test_park_resume_keeps_files_and_old_generation_cannot_mutate(tmp_path: Path) -> None:
    directories = _directories(tmp_path)
    definition = _definition()
    async with open_dataset_workflow(directories, definition) as first:
        mutable: dict[str, JsonValue] = {"source": ["original"]}
        await first.freeze_plan(mutable)
        mutable["source"] = "changed"
        assert first.journal.plan == {"source": ["original"]}
        await first.checkpoint("downloaded", {"receipt": "sha256"})
        _ = (first.directory / "verified-source").write_bytes(b"retained source")
        fence = first.fence
        with pytest.raises(DatasetWorkflowBusy), dataset_workflow_gate(directories):
            pass
    assert first.journal.state == "parked"
    with pytest.raises(DatasetWorkflowBusy):
        async with open_dataset_workflow(directories, definition):
            pass
    async with open_dataset_workflow(directories, definition, resume=True) as second:
        assert second.fence.generation == fence.generation + 1
        assert second.fence.token != fence.token
        assert second.journal.checkpoint == {"receipt": "sha256"}
        assert (second.directory / "verified-source").read_bytes() == b"retained source"
        with pytest.raises(DatasetWorkflowFenceLost):
            await first.checkpoint("stale", {})
        with pytest.raises(DatasetWorkflowFenceLost):
            _ = write_journal(
                second.directory, second.journal, expected_fence=fence, expected_revision=1
            )
        await second.finish("cancelled")
    with dataset_workflow_gate(directories):
        pass


async def test_task_affinity_and_one_explicit_transfer(tmp_path: Path) -> None:
    async with open_dataset_workflow(_directories(tmp_path), _definition()) as owner:

        async def unowned() -> None:
            with pytest.raises(DatasetWorkflowFenceLost):
                await owner.checkpoint("other-task", {})

        async with anyio.create_task_group() as group:
            _ = group.start_soon(unowned)
        await owner.finish()


@pytest.mark.parametrize("ancestor", ("state", "routing", "workflows"))
async def test_symlink_ancestors_never_receive_lock_or_journal(
    tmp_path: Path, ancestor: str
) -> None:
    directories = _directories(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    if ancestor == "state":
        directories.state.symlink_to(outside, target_is_directory=True)
    else:
        directories.data.mkdir(mode=0o700)
        if ancestor == "routing":
            directories.travel_data_directory.symlink_to(outside, target_is_directory=True)
        else:
            directories.travel_data_directory.mkdir(mode=0o700)
            (directories.travel_data_directory / ".workflows").symlink_to(
                outside, target_is_directory=True
            )
    with pytest.raises(OSError):
        async with open_dataset_workflow(directories, _definition()):
            pytest.fail("Symlinked ancestor was accepted")
    assert list(outside.iterdir()) == []


async def test_interrupted_initial_pointer_publication_can_resume_without_any_phase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import carl.io.dataset_workflow_owner as module

    directories = _directories(tmp_path)
    original = module.activate_journal

    def fail(*_: object) -> None:
        raise OSError("injected publication failure")

    monkeypatch.setattr(module, "activate_journal", fail)
    with pytest.raises(OSError):
        async with open_dataset_workflow(directories, _definition()):
            pytest.fail("Owner escaped before reservation was durable")
    with dataset_workflow_gate(directories):
        pass
    monkeypatch.setattr(module, "activate_journal", original)
    async with open_dataset_workflow(directories, _definition(), resume=True) as owner:
        assert owner.fence.generation == 2
        assert owner.journal.phase is None and owner.journal.checkpoint is None
        await owner.finish("cancelled")


async def test_unknown_cleanup_quarantines_but_own_fd_does_not_pin_after_child_proof(
    tmp_path: Path,
) -> None:
    directories = _directories(tmp_path)
    retained = -1
    owner: DatasetWorkflowOwner | None = None
    try:
        with pytest.raises(NativeBuildCleanupError):
            async with open_dataset_workflow(directories, _definition()) as owner:
                retained = os.dup(owner.lock_fd)
                raise NativeBuildCleanupError
        assert owner is not None and owner.journal.state == "quarantined"
        with pytest.raises(DatasetWorkflowBusy):
            async with open_dataset_workflow(directories, _definition(), resume=True):
                pass
        os.close(retained)
        retained = -1
        async with open_dataset_workflow(directories, _definition(), resume=True) as resumed:
            assert resumed.fence.generation == 2
            await resumed.finish("cancelled")
    finally:
        if retained >= 0:
            os.close(retained)


class _Payload(StrictModel):
    phase: str = "first"


class _Envelope(StrictModel):
    work_kind: tuple[str, ...]
    payload_schema_version: int
    payload: JsonValue


def _bound(database: Database, stage: StageDefinition) -> DatasetWorkflowDefinition:
    return _definition().model_copy(
        update={
            "binding": DatasetWorkflowBinding(
                database_path=database.path.absolute(),
                work_item_identifier=stage.work_item_identifier,
                operation_identifier=stage.operation_identifier,
            )
        }
    )


async def test_service_wait_retains_only_global_gate_and_shutdown_parks(tmp_path: Path) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    async with Database.managed(
        directories.database_file, initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        stage = await _seed(database, resources=())
        # Existing registration is retained, with its real work/op IDs.
        waiting = anyio.Event()
        owners: list[DatasetWorkflowOwner] = []

        async def handle(_: _Payload, _context: StageAttemptContext) -> StageOutcome:
            continuation = stage.model_copy(
                update={
                    "identifier": str(uuid4()),
                    "payload": {"phase": "later"},
                    "not_before_utc_ns": time_ns() + 3_600_000_000_000,
                }
            )
            await owners[0].checkpoint("ready", {"verified": "file-reference"})
            waiting.set()
            return WaitWork(continuation=continuation, wait_kind=StageWaitKind.TIME, result={})

        # Seed payload is the legacy adapter envelope; use a handler accepting it.
        async def envelope(_: _Envelope, context: StageAttemptContext) -> StageOutcome:
            return await handle(_Payload(), context)

        def registry(owner: DatasetWorkflowOwner) -> StageHandlerRegistry:
            owners.append(owner)
            return StageHandlerRegistry(
                adapters=(), handlers=(TypedStageHandler(stage.kind, 1, _Envelope, envelope),)
            )

        admissions = 0

        def execution_token() -> str:
            nonlocal admissions
            admissions += 1
            return str(uuid4())

        manager = replace(stage_manager(database), new_identifier=execution_token)
        service = DatasetWorkflowService(manager, directories, registry, _PROVENANCE, {})
        assert await service.submit(stage, component=_COMPONENT, definition=_bound(database, stage))
        with anyio.fail_after(5):
            await waiting.wait()
            while owners[0].journal.state != "waiting":
                await anyio.sleep(0.01)
        revision, admitted = owners[0].journal.revision, admissions
        await anyio.sleep(0.35)
        assert owners[0].journal.revision == revision
        assert admissions == admitted == 1
        async with manager.resources.admit(ResourceRequest(name="cpu")):
            assert owners[0].journal.state == "waiting"
        with pytest.raises(DatasetWorkflowBusy), dataset_workflow_gate(directories):
            pass
        await service.aclose()
        assert owners[0].journal.state == "parked"
        work = await database.work(stage.work_item_identifier)
        assert work["attempt"] == 1
        assert owners[0].journal.current_stage_identifier != stage.identifier


@pytest.mark.parametrize("outcome", ("completed", "retry", "retry_newer_completed"))
async def test_resume_reconciles_db_completion_before_journal_publication(
    tmp_path: Path, outcome: Literal["completed", "retry", "retry_newer_completed"]
) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    async with Database.managed(directories.database_file, initialize=True) as database:
        stage = await _seed(database, resources=())
        definition = _bound(database, stage)
        async with (
            open_dataset_workflow(directories, definition) as old,
            _admit(stage_manager(database), stage) as admission,
        ):
            assert admission.execution is not None
            await admission.execution.finish(
                CompletedWork(result={})
                if outcome == "completed"
                else RetryWork(delay_ns=0, reason={}, result={})
            )
            # Simulate process loss after DB commit, before journal.finish.
        if outcome == "retry_newer_completed":
            newer = stage.model_copy(
                update={"identifier": str(uuid4()), "operation_identifier": str(uuid4())}
            )
            await database.register_stage(newer, created_at_utc_ns=time_ns())
            async with _admit(stage_manager(database), newer) as admission:
                assert admission.execution is not None
                await admission.execution.finish(CompletedWork(result={"newer": True}))

        def unused(_: DatasetWorkflowOwner) -> StageHandlerRegistry:
            pytest.fail("A completed bound operation must not start a replacement phase")

        service = DatasetWorkflowService(
            stage_manager(database), directories, unused, _PROVENANCE, {}
        )
        assert await service.submit(
            stage.model_copy(update={"identifier": str(uuid4())}),
            component=_COMPONENT,
            definition=definition,
            resume=True,
        )
        journal = read_journal(old.directory)
        assert journal is not None and journal.state == (
            "completed" if outcome == "completed" else "failed"
        )
        with dataset_workflow_gate(directories):
            pass


@pytest.mark.parametrize("failure", ("cancel", "unknown_cleanup"))
async def test_active_stage_cleanup_precedes_terminal_or_quarantined_journal(
    tmp_path: Path, failure: str
) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    retained = -1
    async with Database.managed(directories.database_file, initialize=True) as database:
        stage = await _seed(database, resources=())
        definition = _bound(database, stage)
        owners: list[DatasetWorkflowOwner] = []
        entered, cleaned = anyio.Event(), anyio.Event()

        async def handle(_: _Envelope, _context: StageAttemptContext) -> StageOutcome:
            nonlocal retained
            entered.set()
            try:
                if failure == "unknown_cleanup":
                    retained = os.dup(owners[0].lock_fd)
                    raise NativeBuildCleanupError
                await anyio.sleep_forever()
            finally:
                with anyio.CancelScope(shield=True):
                    await anyio.sleep(0.02)
                    assert owners[0].journal.state not in ("completed", "cancelled")
                    cleaned.set()
            raise AssertionError("The blocked handler unexpectedly returned")

        def registry(owner: DatasetWorkflowOwner) -> StageHandlerRegistry:
            owners.append(owner)
            return StageHandlerRegistry(
                adapters=(), handlers=(TypedStageHandler(stage.kind, 1, _Envelope, handle),)
            )

        service = DatasetWorkflowService(
            stage_manager(database), directories, registry, _PROVENANCE, {}
        )
        try:
            assert await service.submit(stage, component=_COMPONENT, definition=definition)
            with anyio.fail_after(5):
                await entered.wait()
                if failure == "cancel":
                    _ = await database.cancel_work(
                        stage.work_item_identifier,
                        reason="test",
                        caller="offline",
                        requested_at_utc_ns=time_ns(),
                    )
                await service._done.wait()
            assert cleaned.is_set()
            assert owners[0].journal.state == (
                "cancelled" if failure == "cancel" else "quarantined"
            )
            if retained >= 0:
                with pytest.raises(DatasetWorkflowBusy):
                    async with open_dataset_workflow(directories, definition, resume=True):
                        pass
                os.close(retained)
                retained = -1
                async with open_dataset_workflow(directories, definition, resume=True) as resumed:
                    await resumed.finish("cancelled")
            with dataset_workflow_gate(directories):
                pass
        finally:
            if retained >= 0:
                os.close(retained)


async def test_resume_continuation_reconciles_cursor_and_preserves_one_logical_attempt(
    tmp_path: Path,
) -> None:
    directories = _directories(tmp_path)
    directories.data.mkdir(mode=0o700)
    owners: list[DatasetWorkflowOwner] = []
    calls: list[str] = []

    async def handle(_: _Envelope, context: StageAttemptContext) -> StageOutcome:
        owner = owners[-1]
        calls.append(owner.journal.phase or "first")
        if owner.journal.phase == "prepared":
            assert context.attempt == 1
            return CompletedWork(result={"resumed": True})
        await owner.checkpoint("prepared", {"receipt": "retained-file-ref"})
        continuation = stage.model_copy(
            update={
                "identifier": str(uuid4()),
                "not_before_utc_ns": time_ns() + 300_000_000,
            }
        )
        return WaitWork(continuation=continuation, wait_kind=StageWaitKind.TIME, result={})

    def registry(owner: DatasetWorkflowOwner) -> StageHandlerRegistry:
        owners.append(owner)
        return StageHandlerRegistry(
            adapters=(), handlers=(TypedStageHandler(stage.kind, 1, _Envelope, handle),)
        )

    async with Database.managed(directories.database_file, initialize=True) as database:
        stage = await _seed(database, resources=())
        definition = _bound(database, stage)
        service = DatasetWorkflowService(
            stage_manager(database), directories, registry, _PROVENANCE, {}
        )
        assert await service.submit(stage, component=_COMPONENT, definition=definition)
        with anyio.fail_after(5):
            while not owners or owners[0].journal.state != "waiting":
                await anyio.sleep(0.01)
        await service.aclose()
        assert owners[0].journal.current_stage_identifier != stage.identifier
    async with Database.managed(directories.database_file) as database:
        service = DatasetWorkflowService(
            stage_manager(database), directories, registry, _PROVENANCE, {}
        )
        assert await service.submit(stage, component=_COMPONENT, definition=definition, resume=True)
        with anyio.fail_after(5):
            await service._done.wait()
        assert owners[-1].journal.state == "completed"
        assert owners[-1].fence.generation == 2
        work = await database.work(stage.work_item_identifier)
        assert work["attempt"] == 1 and len(work["operations"]) == 1
    assert calls == ["first", "prepared"]
