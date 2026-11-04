"""Offline continuations keep their logical readiness while sharing fair CPU turns."""

# Fake bounded phases replace providers; fair admission and runtime remain real.
# pyright: reportPrivateUsage=false

from dataclasses import replace
from pathlib import Path
from time import time_ns

import anyio
import pytest

from carl import work_runtime
from carl._tests.test_fair_storage import _seed
from carl._tests.test_production_fairness import _context, _Harness, _Phase
from carl._tests.test_progress_wake_cost import _arm, _drain
from carl._tests.test_stage_execution import _admit, _Clock
from carl._tests.test_stage_execution import _seed as _execution_seed
from carl._tests.test_stage_storage import NOW, _claim, _definition, _release_resource
from carl._tests.test_work_progress_storage import _checkpoint, _setup_shared
from carl.core.resources import ResourceRequest
from carl.core.stages import StageAttemptContext, StageDefinition, StageWaitKind, WaitWork
from carl.core.worker import CompletedWork, RetryWork
from carl.io.network_activity import network_activity_definition
from carl.io.sqlite import Database
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _readiness(database: Database, work: str) -> tuple[int, int]:
    async with database._connections.reader() as connection:
        row = await (
            await connection.execute(
                "SELECT ready_since_utc_ns,available_at_utc_ns FROM fair_ready WHERE work_item_id=?",
                (work,),
            )
        ).fetchone()
    assert row is not None
    assert isinstance(row[0], int) and isinstance(row[1], int)
    return row[0], row[1]


async def test_publish_runs_before_old_same_root_initial_backlog_drains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness()
    harness.install(monkeypatch, tmp_path)
    owner = _context("shared-workspace", "shared-root")
    initial_count = 48
    captures = 0
    first_initial = False
    published = anyio.Event()
    original_phase = harness.phase

    async with Database.managed(
        tmp_path / "carl.sqlite3",
        initialize=True,
        resource_capacities={"cpu": 1, "http": 1, "analysis": 1},
    ) as database:
        work = await harness.enqueue(database, "downloaded", owner, resource="http")
        capture = harness.initial(work, "capture-downloaded", "operation-downloaded")
        capture = capture.model_copy(
            update={
                "network_activity": network_activity_definition(
                    identifier="network-downloaded",
                    kind=("test", "image", "capture"),
                    operation_identifier=capture.operation_identifier,
                    network_session_identifier="session-downloaded",
                    network_path=("test", "offline"),
                )
            }
        )
        await database.register_stage(capture, created_at_utc_ns=harness.clock())
        harness.clock.advance(1)
        for index in range(initial_count):
            await harness.enqueue(database, f"initial-{index:03d}", owner)
        harness.clock.advance(31)

        async def phase(payload: _Phase, context: StageAttemptContext) -> CompletedWork | WaitWork:
            nonlocal captures, first_initial
            if payload.work_identifier == "downloaded":
                if context.stage_identifier == capture.identifier:
                    captures += 1
                    assert context.network_admission is not None
                    assert context.attempt == 1
                    return WaitWork(
                        continuation=StageDefinition(
                            identifier="publish-downloaded",
                            work_item_identifier=context.work_item_identifier,
                            operation_identifier=context.operation_identifier,
                            kind=capture.kind,
                            payload=payload.model_dump(mode="json"),
                            resources=(ResourceRequest(name="cpu"),),
                        ),
                        wait_kind=StageWaitKind.RESOURCE,
                        result={"state": "waiting_for_image_validation"},
                    )
                assert context.stage_identifier == "publish-downloaded"
                assert context.network_admission is None and context.attempt == 1
                harness.order.append("published")
                published.set()
                return CompletedWork(result={"state": "saved"})
            if not first_initial:
                first_initial = True
                # Hold the first CPU slot until capture has durably handed off.
                # This makes a real CPU publication contend with the old backlog.
                with anyio.fail_after(15):
                    while True:
                        try:
                            await database.stage("publish-downloaded")
                            break
                        except KeyError:
                            await anyio.sleep(0.005)
            return await original_phase(payload, context)

        # _Harness installs its handler when the real runtime registry is built.
        monkeypatch.setattr(harness, "phase", phase)
        async with work_runtime.managed_worker_pool(database, tmp_path):
            with anyio.fail_after(20):
                await published.wait()
                while (await database.work("downloaded"))["state"] != "completed":
                    await anyio.sleep(0.005)
            assert captures == 1
            assert harness.order.index("published") < initial_count, harness.order
            assert any(
                f"initial-{index:03d}" not in harness.order for index in range(initial_count)
            )
            completed = await database.work("downloaded")
            assert completed["attempt"] == 1 and len(completed["operations"]) == 1


async def test_continuation_age_survives_resource_polling_and_database_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    continuation = _definition("work", identifier="publish-work")
    async with Database.managed(path, initialize=True) as database:
        stage = await _seed(database, "work", root="root")
        await database.enable_fair_scheduling("test-host", NOW)
        first = (await _claim(database, stage.identifier)).lease
        assert first is not None
        await database.suspend_stage(
            first,
            WaitWork(continuation=continuation, wait_kind=StageWaitKind.RESOURCE, result={}),
            now_utc_ns=NOW + 10_000,
        )
        assert (await _readiness(database, "work"))[0] == NOW
        # The prior execution's CPU permit remains held until physical cleanup.
        blocked = await _claim(database, continuation.identifier, token="poll", now=NOW + 20_000)
        assert blocked.lease is None and blocked.wait_kind is StageWaitKind.RESOURCE
        age, available = await _readiness(database, "work")
        assert age == NOW and available > NOW + 20_000
        assert (await database.work("work"))["attempt"] == 1
        await _release_resource(database, first)
    async with Database.managed(path) as database:
        assert (await _readiness(database, "work"))[0] == NOW
        resumed = await _claim(database, continuation.identifier, token="resumed", now=available)
        assert resumed.lease is not None and resumed.lease.work_lease.attempt == 1
        assert (
            resumed.lease.definition.operation_identifier == first.definition.operation_identifier
        )


async def test_retry_and_explicit_stage_deadlines_bound_logical_age(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        clock = _Clock(time_ns() + 1_000_000_000)
        manager = replace(stage_manager(database), utc_now_ns=clock)
        stage = await _execution_seed(database)
        await database.enable_fair_scheduling(manager.resources.scope, clock())
        async with _admit(manager, stage) as admission:
            assert admission.execution is not None
            await admission.execution.finish(
                RetryWork(delay_ns=20_000_000_000, reason={"kind": "fixture"}, result={})
            )
        retry_due = clock.now + 20_000_000_000
        assert await _readiness(database, stage.work_item_identifier) == (retry_due, retry_due)
        stage_due = retry_due + 10_000_000_000
        replacement = stage.model_copy(
            update={
                "identifier": "retry-stage",
                "operation_identifier": "retry-operation",
                "not_before_utc_ns": stage_due,
            }
        )
        await database.register_stage(replacement, created_at_utc_ns=clock())
        assert await _readiness(database, stage.work_item_identifier) == (stage_due, stage_due)
        async with _admit(manager, replacement) as blocked:
            assert blocked.execution is None and blocked.result.wait_kind is StageWaitKind.TIME
        assert (await _readiness(database, stage.work_item_identifier))[0] == stage_due
        clock.now = stage_due
        async with _admit(manager, replacement) as admission:
            assert admission.execution is not None
            assert admission.execution.lease.work_lease.attempt == 2
            await admission.execution.finish(CompletedWork(result={}))


async def test_progress_wake_floor_survives_another_continuation(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        child, _ = await _setup_shared(database, 1)
        await database.enable_fair_scheduling("test-host", NOW)
        waiting = await _arm(database, "stage-parent-00", cycle=0, group=False)
        assert (await _drain(database, NOW + 5))[1] == 0
        await _checkpoint(database, child, {"pages": 1})
        assert (await _drain(database, NOW + 20))[1] == 1
        assert (await _readiness(database, "parent-00"))[0] == NOW + 20
        resumed = (await _claim(database, waiting.identifier, token="awake", now=NOW + 30)).lease
        assert resumed is not None
        await database.suspend_stage(
            resumed,
            WaitWork(
                continuation=_definition("parent-00", identifier="after-wake", resources=()),
                wait_kind=StageWaitKind.RESOURCE,
                result={},
            ),
            now_utc_ns=NOW + 40,
        )
        assert (await _readiness(database, "parent-00"))[0] == NOW + 20
        assert (await database.work("parent-00"))["attempt"] == 1
