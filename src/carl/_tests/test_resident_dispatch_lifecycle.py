"""Resident dispatch fences rollback reservations and clear caller execution state."""

# Observe the exact admission ledger and retained actor scope, entirely offline.
# pyright: reportPrivateUsage=false

import threading
from pathlib import Path

import anyio
import pytest

from carl._tests.test_resident_sessions import (
    _COMPONENT,
    _PROVENANCE,
    _initial,
    _next,
    _Payload,
)
from carl._tests.test_resource_adoption import _reservation
from carl.core.resources import ResourceRequest
from carl.core.stages import StageAttemptContext, WaitWork
from carl.core.worker import CompletedWork
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.resource_permits import ResourceLeaseLost, run_cpu
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio
_CAPACITIES = {"cpu": 1, "http": 1, "resident_sessions": 1, "resident_state_bytes": 8}


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _count(database: Database, table: str) -> int:
    async with database._connections.reader() as connection:
        rows = await (await connection.execute(f'SELECT count(*) FROM "{table}"')).fetchall()
    return int(str(rows[0][0]))


def _pool(database: Database, registry: StageHandlerRegistry) -> ResidentSessionPool:
    return ResidentSessionPool(
        stage_manager(database), registry, _PROVENANCE, {}, state_bytes=8, poll_seconds=0.01
    )


@pytest.mark.parametrize("change", ("priority", "competing_stage", "cancel"))
async def test_stale_initial_snapshot_leaves_no_resident_or_execution_rows(
    tmp_path: Path, change: str
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities=_CAPACITIES
    ) as database:
        stage = await _initial(database, resources=())
        snapshot = await database.work_dispatch_snapshot(stage.work_item_identifier)
        if change == "priority":
            async with database._connections.writer() as connection:
                _ = await connection.execute("UPDATE work_items SET priority=priority+1")
        elif change == "competing_stage":
            await database.register_stage(
                stage.model_copy(
                    update={"identifier": "competitor", "operation_identifier": "other"}
                ),
                created_at_utc_ns=0,
            )
        else:
            _ = await database.cancel_work(
                stage.work_item_identifier, reason="test", caller="offline", requested_at_utc_ns=0
            )
        attempts = await _count(database, "stage_attempts")
        async with _pool(database, StageHandlerRegistry((), ())).managed() as pool:
            assert not await pool.submit(stage, component=_COMPONENT, expected_snapshot=snapshot)
            assert pool.active_count == 0
            assert await _count(database, "resident_sessions") == 0
            assert await _count(database, "resource_permits") == 0
            assert await _count(database, "resource_allocations") == 0
            assert await _count(database, "operations") == 0
            assert await _count(database, "stage_attempts") == attempts


async def test_fresh_pool_rebind_validates_snapshot_before_reserving_new_generation(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities=_CAPACITIES
    ) as database:
        stage = await _initial(database, resources=())

        async def phase(
            payload: _Payload, context: StageAttemptContext
        ) -> WaitWork | CompletedWork:
            return (
                _next(context, step=1, delay_ns=60_000_000_000)
                if payload.step == 0
                else CompletedWork(result={})
            )

        registry = StageHandlerRegistry((), (TypedStageHandler(stage.kind, 1, _Payload, phase),))
        async with _pool(database, registry).managed() as first:
            assert await first.submit(stage, component=_COMPONENT)
            with anyio.fail_after(5):
                await first.wait_idle()
        continuation = await database.active_stage_for_work(
            stage.work_item_identifier, stage.operation_identifier
        )
        assert continuation is not None
        old_resident = continuation.resident_session_identifier
        assert old_resident is not None and not first.owns(old_resident)
        snapshot = await database.work_dispatch_snapshot(stage.work_item_identifier)
        async with database._connections.writer() as connection:
            _ = await connection.execute("UPDATE work_items SET priority=priority+1")
        before_permits = await _count(database, "resource_permits")
        async with _pool(database, registry).managed() as second:
            assert not await second.submit(
                continuation, component=_COMPONENT, expected_snapshot=snapshot
            )
            assert await _count(database, "resident_sessions") == 1
            assert await _count(database, "resource_permits") == before_permits
            assert (
                await database.stage(continuation.identifier)
            ).resident_session_identifier == old_resident
            current = await database.work_dispatch_snapshot(stage.work_item_identifier)
            assert await second.submit(
                continuation, component=_COMPONENT, expected_snapshot=current
            )
            with anyio.fail_after(5):
                await second.wait_idle()
        assert await _count(database, "resident_sessions") == 2
        assert await _count(database, "operations") == 1
        assert (await database.work(stage.work_item_identifier))["attempt"] == 1


async def test_unaffined_registered_stage_is_rejected_without_leaking_speculative_resident(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities=_CAPACITIES
    ) as database:
        stage = await _initial(database, resources=())
        await database.register_stage(stage, created_at_utc_ns=0)
        async with _pool(database, StageHandlerRegistry((), ())).managed() as pool:
            with pytest.raises(ResourceLeaseLost, match="transfer resident ownership"):
                await pool.submit(stage, component=_COMPONENT)
            assert await _count(database, "resident_sessions") == 0
            assert await _count(database, "resource_permits") == 0
            assert pool.active_count == 0


async def test_actor_does_not_inherit_submitters_adopted_cpu_permit(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities=_CAPACITIES
    ) as database:
        stage = await _initial(database, resources=())
        entered, completed = anyio.Event(), threading.Event()

        async def phase(_payload: _Payload, _context: StageAttemptContext) -> CompletedWork:
            entered.set()
            await run_cpu(completed.set)
            return CompletedWork(result={})

        registry = StageHandlerRegistry((), (TypedStageHandler(stage.kind, 1, _Payload, phase),))
        async with _pool(database, registry).managed() as pool:
            async with (
                _reservation(database, ResourceRequest(name="cpu")) as (manager, lease, guard),
                manager.adopt(lease, guard),
            ):
                assert await pool.submit(stage, component=_COMPONENT)
                with anyio.fail_after(5):
                    await entered.wait()
                identifier = next(iter(pool._actors))
                assert pool.owns(identifier) and not pool.owns("unknown")
                await anyio.sleep(0.05)
                assert not completed.is_set(), "Resident borrowed the caller's CPU reservation"
            with anyio.fail_after(5):
                await pool.wait_idle()
            assert completed.is_set()
            assert not pool.owns(identifier)
