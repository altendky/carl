"""Stage adapters retain logical attempts and physical ownership through cleanup."""

# Recovery deliberately opens the same physical execution guard as another process.
# pyright: reportPrivateUsage=false

import fcntl
import os
import threading
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, replace
from pathlib import Path
from time import time_ns
from uuid import uuid4

import anyio
import apsw
import pytest

from carl.core.components import Component, ComponentId
from carl.core.models import CodeProvenance, NamedInput, NamedOutput, RecordDraft
from carl.core.resources import ResourceRequest
from carl.core.stages import StageDefinition, StageWaitKind, WaitWork, initial_work_stage
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkDefinition, WorkRequester
from carl.core.worker import CompletedWork, RetryWork
from carl.io.resource_permits import ResourceLeaseLost, run_cpu
from carl.io.sqlite import Database, LeaseLostError
from carl.io.stages import StageAdmission, StageManager, stage_manager

pytestmark = pytest.mark.anyio

_COMPONENT = Component(ComponentId(("test", "stage_execution")), 1, lambda: None)
_PROVENANCE = CodeProvenance(
    repository_url=None,
    commit_hash=None,
    worktree_state="dirty",
    package_version="test",
    python_implementation="test",
    python_version="test",
    dependencies=(),
    lockfile_sha256=None,
)


@dataclass
class _Clock:
    now: int

    def __call__(self) -> int:
        return self.now


async def _seed(
    database: Database,
    *,
    resources: tuple[ResourceRequest, ...] = (ResourceRequest(name="cpu"),),
    not_before_utc_ns: int = 0,
) -> StageDefinition:
    identifier = str(uuid4())
    work = WorkDefinition(
        identifier=identifier,
        kind=("retained", "work"),
        payload_schema_version=7,
        payload={"retained": "original"},
        deduplication_identity=(identifier,),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=("retained", "work")),
        ),
    )
    _ = await database.enqueue_work(
        work,
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier=identifier, context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    stage = initial_work_stage(
        work,
        identifier=str(uuid4()),
        operation_identifier=str(uuid4()),
        kind=("test", "continuation"),
        resources=resources,
    ).model_copy(update={"not_before_utc_ns": not_before_utc_ns})
    await database.register_stage(stage, created_at_utc_ns=time_ns())
    return stage


def _admit(
    manager: StageManager, stage: StageDefinition
) -> AbstractAsyncContextManager[StageAdmission]:
    return manager.admit(
        stage.identifier, component=_COMPONENT, provenance=_PROVENANCE, invocation={}
    )


def _record() -> RecordDraft:
    return RecordDraft(
        identifier=str(uuid4()),
        kind=("test", "checkpoint"),
        schema_version=1,
        value={"saved": True},
    )


@pytest.mark.parametrize("blocked_by", ("time", "resource"))
async def test_blocked_stage_returns_without_creating_a_logical_attempt(
    tmp_path: Path, blocked_by: str
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        manager = stage_manager(database)
        stage = await _seed(
            database, not_before_utc_ns=time_ns() + 60_000_000_000 if blocked_by == "time" else 0
        )
        async with AsyncExitStack() as stack:
            if blocked_by == "resource":
                _ = await stack.enter_async_context(
                    manager.resources.admit(ResourceRequest(name="cpu"))
                )
            guard_files = set(manager.resources.guard_directory.glob("*.lock"))
            with anyio.fail_after(2):
                for _ in range(4):
                    async with _admit(manager, stage) as admission:
                        assert admission.execution is None and admission.result.lease is None
                        assert admission.result.wait_kind == (
                            StageWaitKind.TIME if blocked_by == "time" else StageWaitKind.RESOURCE
                        )
            assert set(manager.resources.guard_directory.glob("*.lock")) == guard_files
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "pending" and work["attempt"] == 0 and work["operations"] == []
        with pytest.raises(KeyError):
            _ = await database.operation(stage.operation_identifier)


async def test_checkpoint_resume_preserves_one_operation_and_acquisition_attempt(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _seed(database)
        manager = stage_manager(database)
        checkpoint = _record()
        continuation = stage.model_copy(
            update={
                "identifier": str(uuid4()),
                "kind": ("test", "resume"),
                "checkpoint_inputs": (
                    NamedInput(name=("checkpoint",), object_identifier=checkpoint.identifier),
                ),
            }
        )
        async with _admit(manager, stage) as admission:
            execution = admission.execution
            assert execution is not None
            first = execution.lease
            await execution.suspend(
                WaitWork(
                    continuation=continuation,
                    wait_kind=StageWaitKind.TIME,
                    result={"waiting": True},
                ),
                records=(checkpoint,),
                outputs=(
                    NamedOutput(name=("checkpoint",), object_identifier=checkpoint.identifier),
                ),
            )
        assert (await database.operation(stage.operation_identifier))["state"] == "started"
        assert await database.operation_output_count(stage.operation_identifier) == 1
        assert (await database.get_record(checkpoint.identifier))[2] == checkpoint.value
        async with _admit(manager, continuation) as admission:
            execution = admission.execution
            assert execution is not None
            assert execution.lease.token != first.token
            assert execution.lease.work_lease.attempt == first.work_lease.attempt == 1
            assert (
                execution.lease.definition.operation_identifier
                == first.definition.operation_identifier
            )
            result = _record()
            await execution.finish(
                CompletedWork(
                    records=(result,),
                    outputs=(NamedOutput(name=("result",), object_identifier=result.identifier),),
                    result={"complete": True},
                )
            )
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        assert work["payload"] == {"retained": "original"} and work["payload_schema_version"] == 7
        assert work["operations"] == [
            {"operation_identifier": stage.operation_identifier, "attempt": 1}
        ]
        assert await database.operation_output_count(stage.operation_identifier) == 2
        assert (await database.operation(stage.operation_identifier))["state"] == "completed"


async def test_failed_publication_rolls_back_stage_finalization_and_evidence(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _seed(database)
        manager = stage_manager(database)
        record = _record()
        async with _admit(manager, stage) as admission:
            execution = admission.execution
            assert execution is not None
            with pytest.raises(apsw.ConstraintError):
                await execution.finish(
                    CompletedWork(
                        records=(record,),
                        outputs=(
                            NamedOutput(name=("missing",), object_identifier="absent-object"),
                        ),
                        result={"complete": True},
                    )
                )
            assert not execution.finalized
            assert await database.validate_stage(execution.lease, now_utc_ns=time_ns())
            assert (await database.operation(stage.operation_identifier))["state"] == "started"
            assert await database.operation_output_count(stage.operation_identifier) == 0
            with pytest.raises(KeyError):
                _ = await database.get_record(record.identifier)
        async with _admit(manager, stage) as admission:
            assert admission.execution is not None
            assert admission.execution.lease.work_lease.attempt == 1
            await admission.execution.finish(CompletedWork(result={"recovered": True}))


async def test_stage_adopts_resource_for_the_actual_cpu_adapter(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        stage = await _seed(database)
        async with _admit(stage_manager(database), stage) as admission:
            assert admission.execution is not None
            with anyio.fail_after(2):
                thread = await run_cpu(threading.get_ident)
            assert thread != threading.get_ident()
            await admission.execution.finish(CompletedWork(result={"thread": thread}))


async def test_acquisition_retry_starts_a_new_operation_and_attempt(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _seed(database)
        manager = stage_manager(database)
        async with _admit(manager, stage) as admission:
            assert admission.execution is not None
            await admission.execution.finish(
                RetryWork(delay_ns=0, reason={"kind": "transport_failure"}, result={"retry": True})
            )
        assert (await database.work(stage.work_item_identifier))["state"] == "pending"
        replacement = stage.model_copy(
            update={"identifier": str(uuid4()), "operation_identifier": str(uuid4())}
        )
        await database.register_stage(replacement, created_at_utc_ns=time_ns())
        async with _admit(manager, replacement) as admission:
            execution = admission.execution
            assert execution is not None
            assert execution.lease.work_lease.attempt == 2
            assert execution.lease.definition.operation_identifier != stage.operation_identifier
            await execution.finish(CompletedWork(result={"recovered": True}))
        assert len((await database.work(stage.work_item_identifier))["operations"]) == 2


async def test_heartbeat_renews_stage_work_and_resource_fences(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _seed(database)
        manager = replace(
            stage_manager(database), lease_duration_ns=1_000_000_000, renewal_interval_ns=50_000_000
        )
        async with _admit(manager, stage) as admission:
            execution = admission.execution
            assert execution is not None
            original_expiry = execution.lease.work_lease.expires_at_utc_ns
            with anyio.fail_after(3):
                while execution.lease.work_lease.expires_at_utc_ns <= original_expiry:
                    await anyio.sleep(0.01)
            assert execution.lease.resource_lease is not None
            assert (
                execution.lease.resource_lease.expires_at_utc_ns
                == execution.lease.work_lease.expires_at_utc_ns
            )
            assert await database.validate_stage(execution.lease, now_utc_ns=time_ns())
            await execution.finish(CompletedWork(result={"renewed": True}))


async def test_cancelled_executor_releases_capacity_and_resumes_same_attempt(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        stage = await _seed(database)
        manager = stage_manager(database)
        scope = anyio.CancelScope()
        entered = anyio.Event()
        finished = anyio.Event()

        async def execute() -> None:
            with scope:
                async with _admit(manager, stage) as admission:
                    assert admission.execution is not None
                    entered.set()
                    await anyio.sleep_forever()
            finished.set()

        async with anyio.create_task_group() as tasks:
            _ = tasks.start_soon(execute)
            with anyio.fail_after(3):
                await entered.wait()
                scope.cancel()
                await finished.wait()
        assert (await database.operation(stage.operation_identifier))["state"] == "started"
        async with _admit(manager, stage) as admission:
            assert admission.execution is not None
            assert admission.execution.lease.execution == 2
            assert admission.execution.lease.work_lease.attempt == 1
            await admission.execution.finish(CompletedWork(result={"resumed": True}))


async def test_logical_cancellation_prevents_publication_and_is_acknowledged(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _seed(database)
        async with _admit(stage_manager(database), stage) as admission:
            assert admission.execution is not None
            _ = await database.cancel_work(
                stage.work_item_identifier,
                reason="test",
                caller="test",
                requested_at_utc_ns=time_ns(),
            )
            with pytest.raises(LeaseLostError):
                await admission.execution.finish(CompletedWork(result={"should_not_publish": True}))
        assert (await database.work(stage.work_item_identifier))["state"] == "cancelled"
        assert (await database.operation(stage.operation_identifier))["result"] == {
            "state": "cancelled"
        }


async def test_heartbeat_cancels_execution_after_logical_ownership_is_revoked(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _seed(database)
        manager = replace(stage_manager(database), renewal_interval_ns=20_000_000)
        with anyio.fail_after(3):
            with pytest.raises(ExceptionGroup) as captured:
                async with _admit(manager, stage) as admission:
                    assert admission.execution is not None
                    _ = await database.cancel_work(
                        stage.work_item_identifier,
                        reason="test",
                        caller="test",
                        requested_at_utc_ns=time_ns(),
                    )
                    await anyio.sleep_forever()
        assert captured.value.subgroup(ResourceLeaseLost) is not None
        assert (await database.work(stage.work_item_identifier))["state"] == "cancelled"
        assert await database.operation_output_count(stage.operation_identifier) == 0


async def test_closing_stage_drains_active_cpu_and_rejects_queued_borrower(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        stage = await _seed(database)
        manager = stage_manager(database)
        started = threading.Event()
        release = threading.Event()
        queued_entered = threading.Event()
        closing = anyio.Event()
        closed = anyio.Event()
        errors: list[ResourceLeaseLost] = []
        resource_fence: tuple[str, str] | None = None

        def physical_cpu() -> None:
            started.set()
            assert release.wait(3), "test must release the physical CPU operation"

        async def queued_cpu() -> None:
            try:
                await run_cpu(queued_entered.set)
            except ResourceLeaseLost as error:
                errors.append(error)

        async def release_after_cleanup_waits(lease_token: str, owner: str) -> None:
            try:
                await closing.wait()
                await anyio.sleep(0.05)
                assert not closed.is_set()
                assert await database.validate_resource_permit(
                    token=lease_token, owner=owner, now_utc_ns=time_ns()
                )
            finally:
                release.set()

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tasks:
                async with _admit(manager, stage) as admission:
                    execution = admission.execution
                    assert execution is not None
                    resource_fence = (
                        execution.lease.token,
                        execution.lease.work_lease.worker_identifier,
                    )
                    _ = tasks.start_soon(run_cpu, physical_cpu)
                    assert await anyio.to_thread.run_sync(lambda: started.wait(2))
                    _ = tasks.start_soon(queued_cpu)
                    await anyio.sleep(0)
                    _ = tasks.start_soon(
                        release_after_cleanup_waits,
                        execution.lease.token,
                        execution.lease.work_lease.worker_identifier,
                    )
                    with pytest.raises(ResourceLeaseLost, match="physical executor"):
                        await execution.finish(CompletedWork(result={}))
                    closing.set()
                closed.set()
        assert not queued_entered.is_set() and len(errors) == 1
        assert resource_fence is not None
        assert not await database.validate_resource_permit(
            token=resource_fence[0],
            owner=resource_fence[1],
            now_utc_ns=time_ns(),
        )


@pytest.mark.parametrize("has_resources", (False, True))
async def test_restarted_manager_recovers_only_after_expired_physical_guard_is_gone(
    tmp_path: Path, has_resources: bool
) -> None:
    path = tmp_path / "carl.sqlite3"
    clock = _Clock(time_ns())
    token = str(uuid4())
    descriptor: int | None = None
    try:
        async with Database.managed(path, initialize=True) as database:
            stage = await _seed(
                database, resources=(ResourceRequest(name="cpu"),) if has_resources else ()
            )
            clock.now = time_ns()
            manager = replace(stage_manager(database), utc_now_ns=clock)
            descriptor = manager.resources._guard(token)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = await database.try_claim_stage(
                stage_identifier=stage.identifier,
                token=token,
                worker_identifier="crashed-worker",
                resource_scope=manager.resources.scope,
                capacities=manager.resources.capacities,
                lease_duration_ns=1_000_000_000,
                now_utc_ns=clock(),
                component=_COMPONENT,
                provenance=_PROVENANCE,
                invocation={},
            )
            assert result.lease is not None
            original = result.lease
        clock.now = original.work_lease.expires_at_utc_ns + 1
        async with Database.managed(path) as database:
            manager = replace(stage_manager(database), utc_now_ns=clock)
            await manager.recover()
            assert await database.expired_stage_executions(
                coordination_scope=manager.resources.scope, now_utc_ns=clock()
            ) == (original,)
            os.close(descriptor)
            descriptor = None
            await manager.recover()
            assert (
                await database.expired_stage_executions(
                    coordination_scope=manager.resources.scope, now_utc_ns=clock()
                )
                == ()
            )
            async with _admit(manager, stage) as admission:
                execution = admission.execution
                assert execution is not None
                assert execution.lease.token != original.token
                assert execution.lease.execution == 2
                assert execution.lease.work_lease.attempt == 1
                with pytest.raises(LeaseLostError):
                    await database.complete_stage(original, now_utc_ns=clock())
                await execution.finish(CompletedWork(result={"recovered": True}))
            work = await database.work(stage.work_item_identifier)
            assert work["state"] == "completed" and len(work["operations"]) == 1
    finally:
        if descriptor is not None:
            os.close(descriptor)
