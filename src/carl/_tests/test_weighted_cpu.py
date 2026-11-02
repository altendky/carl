"""Weighted CPU guards bound physical work and adopted-stage fanout together."""

# Exact allocation/ownership assertions exercise the durable resource boundary.
# pyright: reportPrivateUsage=false

import os
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import anyio
import pytest

from carl._tests.test_resource_adoption import _reservation
from carl._tests.test_stage_execution import _admit, _seed
from carl.core.resources import ResourceRequest
from carl.core.worker import CompletedWork, TerminalFailureWork
from carl.io.resource_permits import (
    ResourceLeaseLost,
    cpu_reservation,
    current_cpu_guard_descriptor,
    invalidate_cpu_reservation,
    run_cpu,
    validate_cpu_reservation,
)
from carl.io.sqlite import Database
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@asynccontextmanager
async def _adopted(database: Database, enabled: bool) -> AsyncIterator[None]:
    if not enabled:
        yield
        return
    async with (
        _reservation(database, ResourceRequest(name="cpu", units=4)) as (
            manager,
            lease,
            guard,
        ),
        manager.adopt(lease, guard),
    ):
        yield


async def _count(database: Database) -> int:
    async with database._connections.reader() as connection:
        row = await (await connection.execute("SELECT count(*) FROM resource_permits")).fetchone()
    assert row is not None and isinstance(row[0], int)
    return row[0]


@pytest.mark.parametrize("adopted", (False, True))
async def test_three_plus_two_waits_until_cancelled_physical_thread_really_finishes(
    tmp_path: Path, adopted: bool
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        first_started, physical_release = threading.Event(), threading.Event()
        first_done, second_started = anyio.Event(), anyio.Event()
        scopes: list[anyio.CancelScope] = []

        def physical() -> None:
            first_started.set()
            assert physical_release.wait(5), "Test failed to release its physical CPU thread"

        async def first() -> None:
            try:
                with anyio.CancelScope() as scope:
                    scopes.append(scope)
                    async with cpu_reservation(3):
                        await run_cpu(physical)
            finally:
                first_done.set()

        async def second() -> None:
            async with cpu_reservation(2):
                second_started.set()

        async with _adopted(database, adopted):
            with anyio.fail_after(4):
                async with anyio.create_task_group() as tasks:
                    try:
                        tasks.start_soon(first)
                        while not first_started.is_set():
                            await anyio.sleep(0.001)
                        tasks.start_soon(second)
                        await anyio.sleep(0.03)
                        assert not second_started.is_set()
                        scopes[0].cancel()
                        await anyio.sleep(0.03)
                        assert not first_done.is_set() and not second_started.is_set()
                    finally:
                        physical_release.set()
                    await first_done.wait()
                    await second_started.wait()


async def test_stage_four_units_bound_weighted_children_without_serializing_all_work(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        stage = await _seed(database, resources=(ResourceRequest(name="cpu", units=4),))
        releases = {weight: anyio.Event() for weight in (3, 1, 2)}
        started = {weight: anyio.Event() for weight in (3, 1, 2)}
        active, maximum = 0, 0

        async def child(weight: int) -> None:
            nonlocal active, maximum
            async with cpu_reservation(weight):
                active += weight
                maximum = max(maximum, active)
                try:
                    assert active <= 4
                    started[weight].set()
                    await releases[weight].wait()
                finally:
                    active -= weight

        async with _admit(stage_manager(database), stage) as admission:
            assert admission.execution is not None
            async with anyio.create_task_group() as tasks:
                try:
                    tasks.start_soon(child, 3)
                    await started[3].wait()
                    tasks.start_soon(child, 1)
                    await started[1].wait()
                    assert active == 4
                    tasks.start_soon(child, 2)
                    await anyio.sleep(0.03)
                    assert not started[2].is_set()
                    releases[3].set()
                    with anyio.fail_after(2):
                        await started[2].wait()
                    assert active == 3
                finally:
                    for release in releases.values():
                        release.set()
            assert maximum == 4 and active == 0
            assert await _count(database) == 1


async def test_simultaneous_large_local_borrows_cannot_deadlock_on_partial_units(
    tmp_path: Path,
) -> None:
    async with (
        Database.managed(
            tmp_path / "carl.sqlite", initialize=True, resource_capacities={"cpu": 4}
        ) as database,
        _adopted(database, True),
    ):
        release = (anyio.Event(), anyio.Event())
        send, receive = anyio.create_memory_object_stream[int](2)

        async def child(index: int) -> None:
            async with cpu_reservation(3):
                await send.send(index)
                await release[index].wait()

        async with send, receive, anyio.create_task_group() as tasks:
            try:
                tasks.start_soon(child, 0)
                tasks.start_soon(child, 1)
                with anyio.fail_after(2):
                    first = await receive.receive()
                    release[first].set()
                    second = await receive.receive()
                assert first != second
            finally:
                for event in release:
                    event.set()


@pytest.mark.parametrize("adopted", (False, True))
async def test_cancelling_a_weighted_waiter_spends_no_new_permit_or_partial_units(
    tmp_path: Path, adopted: bool
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        started, release, attempted, cancelled = (
            anyio.Event(),
            anyio.Event(),
            anyio.Event(),
            anyio.Event(),
        )
        scopes: list[anyio.CancelScope] = []

        async def holder() -> None:
            async with cpu_reservation(3):
                started.set()
                await release.wait()

        async def waiter() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                attempted.set()
                async with cpu_reservation(2):
                    raise AssertionError("Two units cannot fit beside a three-unit operation")
            cancelled.set()

        async with _adopted(database, adopted), anyio.create_task_group() as tasks:
            try:
                tasks.start_soon(holder)
                await started.wait()
                count = await _count(database)
                tasks.start_soon(waiter)
                await attempted.wait()
                await anyio.sleep(0.03)
                scopes[0].cancel()
                with anyio.fail_after(2):
                    await cancelled.wait()
                    assert await _count(database) == count
                    async with cpu_reservation(1):
                        assert await current_cpu_guard_descriptor() >= 0
            finally:
                release.set()


async def test_insufficient_stage_allocation_rejects_before_any_other_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        stage = await _seed(database)
        async with _admit(stage_manager(database), stage) as admission:
            assert admission.execution is not None

            async def no_admission(**_kwargs: object):
                raise AssertionError("Insufficient adopted allocation attempted another admission")

            monkeypatch.setattr(database, "try_acquire_resource_permit", no_admission)
            with pytest.raises(ValueError, match="current permit reserves 1"):
                validate_cpu_reservation(3)
            with anyio.fail_after(1), pytest.raises(ValueError, match="current permit reserves 1"):
                async with cpu_reservation(3):
                    raise AssertionError("Insufficient allocation dispatched work")
            assert await _count(database) == 1


async def test_reentrant_weights_may_shrink_but_cannot_upgrade_or_overstate_guard(
    tmp_path: Path,
) -> None:
    async with (
        Database.managed(
            tmp_path / "carl.sqlite", initialize=True, resource_capacities={"cpu": 4}
        ) as database,
        _adopted(database, True),
    ):
        async with cpu_reservation(3) as descriptor:
            assert os.fstat(descriptor).st_ino > 0
            assert await current_cpu_guard_descriptor(required_units=3) == descriptor
            async with cpu_reservation(2) as nested:
                assert nested == descriptor
            with pytest.raises(ResourceLeaseLost, match="required units"):
                await current_cpu_guard_descriptor(required_units=4)
            with anyio.fail_after(1), pytest.raises(ValueError, match="nested CPU"):
                async with cpu_reservation(4):
                    raise AssertionError("A nested reservation increased its weight")
            with pytest.raises(ValueError, match="nested CPU"):
                validate_cpu_reservation(4)
        async with cpu_reservation(1):
            with pytest.raises(ResourceLeaseLost, match="required units"):
                await current_cpu_guard_descriptor(required_units=3)


async def test_direct_reservation_requires_manager_and_valid_units(tmp_path: Path) -> None:
    with pytest.raises(ResourceLeaseLost, match="managed resource context"):
        async with cpu_reservation(1):
            raise AssertionError("Unmanaged reservation was admitted")
    with pytest.raises(ResourceLeaseLost, match="managed resource context"):
        validate_cpu_reservation()
    async with Database.managed(
        tmp_path / "carl.sqlite", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        manager = validate_cpu_reservation(3)
        assert not manager.guard_directory.exists() and await _count(database) == 0
        with pytest.raises(ValueError, match="configured CPU capacity"):
            validate_cpu_reservation(5)
        for invalid in (0, -1, True):
            with pytest.raises(ValueError, match="positive integer"):
                async with cpu_reservation(invalid):
                    raise AssertionError("Invalid CPU weight was admitted")
            with pytest.raises(ValueError, match="positive integer"):
                await current_cpu_guard_descriptor(required_units=invalid)
        async with cpu_reservation(3) as descriptor:
            assert await current_cpu_guard_descriptor(required_units=3) == descriptor
            async with database._connections.reader() as connection:
                row = await (
                    await connection.execute(
                        "SELECT units FROM resource_allocations WHERE name='cpu'"
                    )
                ).fetchone()
            assert row == (3,)


async def test_poisoned_stage_cannot_reuse_units_or_publish_until_unknown_child_guard_is_gone(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        stage = await _seed(database, resources=(ResourceRequest(name="cpu", units=4),))
        inherited: int | None = None
        async with _admit(stage_manager(database), stage) as admission:
            execution = admission.execution
            assert execution is not None
            try:
                failure = RuntimeError("Physical child cleanup is unconfirmed")
                with pytest.raises(RuntimeError) as raised:
                    async with cpu_reservation(4) as descriptor:
                        inherited = os.dup(descriptor)
                        invalidate_cpu_reservation()
                        invalidate_cpu_reservation()
                        assert execution.guard.closing
                        raise failure
                assert raised.value is failure
                with pytest.raises(ResourceLeaseLost):
                    validate_cpu_reservation(4)
                with pytest.raises(ResourceLeaseLost):
                    async with cpu_reservation(4):
                        raise AssertionError("Caught cleanup failure reused poisoned CPU units")
                with pytest.raises(ResourceLeaseLost):
                    await current_cpu_guard_descriptor(required_units=4)
                assert await _count(database) == 1
                with pytest.raises(ResourceLeaseLost, match="inherited physical"):
                    await execution.finish(CompletedWork(result={}))
            finally:
                if inherited is not None:
                    os.close(inherited)
            # Finalization becomes possible after positive physical guard proof;
            # invalidation itself neither published nor released the reservation.
            await execution.finish(
                TerminalFailureWork(error={"kind": "unknown_child"}, result={"state": "failed"})
            )


async def test_cpu_invalidation_requires_the_actual_borrowing_task(tmp_path: Path) -> None:
    with pytest.raises(ResourceLeaseLost, match="active task CPU borrow"):
        invalidate_cpu_reservation()
    async with (
        Database.managed(
            tmp_path / "carl.sqlite", initialize=True, resource_capacities={"cpu": 4}
        ) as database,
        _adopted(database, True),
    ):
        with pytest.raises(ResourceLeaseLost, match="active task CPU borrow"):
            invalidate_cpu_reservation()
        async with cpu_reservation(2) as descriptor:

            async def child() -> None:
                with pytest.raises(ResourceLeaseLost, match="active task CPU borrow"):
                    invalidate_cpu_reservation()

            async with anyio.create_task_group() as tasks:
                tasks.start_soon(child)
            assert await current_cpu_guard_descriptor(required_units=2) == descriptor
            invalidate_cpu_reservation()
            with pytest.raises(ResourceLeaseLost):
                await current_cpu_guard_descriptor()


async def test_poisoned_direct_reservation_keeps_units_charged_while_child_fd_is_held(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        inherited: int | None = None
        try:
            async with cpu_reservation(3) as descriptor:
                inherited = os.dup(descriptor)
                invalidate_cpu_reservation()
                with pytest.raises(ResourceLeaseLost):
                    validate_cpu_reservation(1)
            async with database._connections.reader() as connection:
                rows = await (
                    await connection.execute(
                        "SELECT p.token,p.owner,a.units FROM resource_permits p JOIN resource_allocations a ON a.permit_token=p.token WHERE p.state='held' AND a.name='cpu'"
                    )
                ).fetchall()
            assert len(rows) == 1 and rows[0][2] == 3
        finally:
            if inherited is not None:
                os.close(inherited)
        token, owner, _ = rows[0]
        assert isinstance(token, str) and isinstance(owner, str)
        assert await database.release_resource_permit(token=token, owner=owner)
