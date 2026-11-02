"""Warm residency is shared, exclusive and independent from active CPU work."""

# These offline owner fixtures inspect physical guards and exact ledger allocations.
# pyright: reportPrivateUsage=false

import fcntl
import os
from dataclasses import dataclass, field
from pathlib import Path
from time import monotonic, time_ns
from typing import Any, cast

import anyio
import pytest

import carl.io.native_routing_pool as pool_module
from carl._tests.test_osrm import Settings
from carl.core.native_routing import NativeRoutingDataset, NativeRoutingMetric
from carl.core.resources import ResourceRequest
from carl.core.travel import TravelCoordinate
from carl.io.native_routing_pool import NativeRoutingPool, native_routing_pool
from carl.io.native_routing_protocol import NativeRoutingCleanupError, NativeRoutingProcess
from carl.io.osrm import OsrmRouter
from carl.io.resource_permits import (
    ResourceLeaseLost,
    current_cpu_guard_descriptor,
    current_resource_manager,
    resource_manager_context,
)
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
ORIGIN = TravelCoordinate(latitude=10, longitude=20)
DESTINATION = TravelCoordinate(latitude=11, longitude=21)


@dataclass
class _Process:
    pid: int = 1234
    worker_pid: int = 1235
    closed: bool = False
    fail_close: bool = False
    calls: int = 0
    snapshots: list[float] = field(default_factory=list)
    delay: float = 0

    async def query(
        self,
        origin: TravelCoordinate,
        destinations: tuple[TravelCoordinate, ...],
        *,
        maximum_snap_distance_meters: float,
        cpu_guard_fd: int,
        timeout_seconds: float = 60,
    ) -> tuple[NativeRoutingMetric, ...]:
        del origin, timeout_seconds
        with pytest.raises(ResourceLeaseLost):
            _ = await current_cpu_guard_descriptor()
        descriptor = os.open(f"/proc/self/fd/{cpu_guard_fd}", os.O_RDONLY)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)
        self.calls += 1
        self.snapshots.append(maximum_snap_distance_meters)
        if self.delay:
            await anyio.sleep(self.delay)
        return ((float(self.pid), float(self.calls)),) * len(destinations)

    def is_closed(self) -> bool:
        return self.closed

    async def aclose(self) -> None:
        if self.fail_close:
            raise NativeRoutingCleanupError
        self.closed = True


@dataclass
class _Factory:
    processes: list[_Process] = field(default_factory=list)
    arguments: list[dict[str, Any]] = field(default_factory=list)
    fail_close: bool = False
    delay: float = 0

    async def __call__(self, **kwargs: Any) -> _Process:
        with pytest.raises(ResourceLeaseLost):
            _ = await current_cpu_guard_descriptor()
        self.arguments.append(kwargs)
        process = _Process(
            pid=1234 + len(self.processes), fail_close=self.fail_close, delay=self.delay
        )
        self.processes.append(process)
        return process


def _dataset(root: Path, identifier: str = "fixture-v1") -> NativeRoutingDataset:
    return NativeRoutingDataset(dataset_path=root / "roads.osrm", dataset_identifier=identifier)


async def _held(database: Database, name: str) -> int:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                "SELECT coalesce(sum(a.units),0) FROM resource_allocations a JOIN resource_permits p ON p.token=a.permit_token WHERE p.state='held' AND a.name=?",
                (name,),
            )
        ).fetchall()
    assert isinstance(rows[0][0], int)
    return rows[0][0]


async def _until(condition) -> None:
    with anyio.fail_after(5):
        while not condition():
            await anyio.sleep(0.005)


async def test_lazy_warm_owner_reuses_graph_for_snap_changes_and_releases_idle(
    tmp_path: Path,
) -> None:
    factory = _Factory()
    async with Database.managed(
        tmp_path / "carl.sqlite3",
        initialize=True,
        resource_capacities={"routing_engines": 1, "cpu": 1},
    ) as database:
        manager = current_resource_manager()
        assert manager is not None
        pool = NativeRoutingPool(manager, idle_timeout_seconds=1.0, process_factory=factory)
        async with pool.borrow(_dataset(tmp_path)) as first:
            assert not factory.processes and await _held(database, "routing_engines") == 1
            assert await _held(database, "cpu") == 0
            assert await first.query(ORIGIN, (DESTINATION,), maximum_snap_distance_meters=5000) == (
                (1234.0, 1.0),
            )
        assert await _held(database, "cpu") == 0
        async with pool.borrow(_dataset(tmp_path)) as second:
            assert await second.query(
                ORIGIN, (DESTINATION,), maximum_snap_distance_meters=1000
            ) == ((1234.0, 2.0),)
        assert len(factory.processes) == 1 and factory.processes[0].snapshots == [5000, 1000]
        await _until(lambda: pool.owner_count == 0)
        assert factory.processes[0].closed and await _held(database, "routing_engines") == 0


async def test_busy_waiters_hold_no_cpu_and_create_no_additional_owner_tasks(
    tmp_path: Path,
) -> None:
    factory = _Factory()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        manager = current_resource_manager()
        assert manager is not None
        pool = NativeRoutingPool(manager, process_factory=factory)
        first = await pool.try_borrow(_dataset(tmp_path))
        assert first is not None
        for _ in range(20):
            assert await pool.try_borrow(_dataset(tmp_path)) is None
        assert pool.owner_count == 1 and not factory.processes
        assert await _held(database, "cpu") == 0
        waiting = anyio.Event()

        async def borrow() -> None:
            waiting.set()
            async with pool.borrow(_dataset(tmp_path)):
                pass

        async with anyio.create_task_group() as tasks:
            _ = tasks.start_soon(borrow)
            await waiting.wait()
            await anyio.sleep(0.02)
            assert pool.owner_count == 1 and await _held(database, "cpu") == 0
            await first.aclose()
        await pool.aclose()


async def test_concurrent_initial_borrow_never_overwrites_an_active_handle(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        manager = current_resource_manager()
        assert manager is not None
        pool = NativeRoutingPool(manager, process_factory=_Factory())
        handles = []

        async def claim() -> None:
            handles.append(await pool.try_borrow(_dataset(tmp_path)))

        async with anyio.create_task_group() as tasks:
            for _ in range(20):
                _ = tasks.start_soon(claim)
        borrowed = [handle for handle in handles if handle is not None]
        assert len(borrowed) == 1
        assert await borrowed[0].query(ORIGIN, (DESTINATION,), maximum_snap_distance_meters=5000)
        await borrowed[0].aclose()


async def test_cancelled_first_admission_stops_its_cold_owner_promptly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = anyio.Event(), anyio.Event()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        manager = current_resource_manager()
        assert manager is not None
        original = database.try_acquire_resource_permit

        async def delayed(**kwargs):
            if kwargs["requests"][0].name == "routing_engines":
                entered.set()
                await release.wait()
            return await original(**kwargs)

        monkeypatch.setattr(database, "try_acquire_resource_permit", delayed)
        pool = NativeRoutingPool(manager, process_factory=_Factory())
        scopes: list[anyio.CancelScope] = []

        async def claim() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                _ = await pool.try_borrow(_dataset(tmp_path))

        async with anyio.create_task_group() as tasks:
            _ = tasks.start_soon(claim)
            await entered.wait()
            scopes[0].cancel()
        release.set()
        await _until(lambda: pool.owner_count == 0)
        assert await _held(database, "routing_engines") == 0


async def test_borrow_rejects_other_manager_before_cpu_admission(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        manager = current_resource_manager()
        assert manager is not None
        pool = NativeRoutingPool(manager, process_factory=_Factory())
        borrow = await pool.try_borrow(_dataset(tmp_path))
        assert borrow is not None
        async with resource_manager_context(database):
            with pytest.raises(ResourceLeaseLost, match="another"):
                _ = await borrow.query(ORIGIN, (DESTINATION,), maximum_snap_distance_meters=5000)
            assert await _held(database, "cpu") == 0
        await borrow.aclose()


async def test_dataset_switch_disposes_old_idle_engine_before_admitting_new(tmp_path: Path) -> None:
    factory = _Factory()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        manager = current_resource_manager()
        assert manager is not None
        pool = NativeRoutingPool(manager, process_factory=factory)
        async with pool.borrow(_dataset(tmp_path)) as first:
            _ = await first.query(ORIGIN, (DESTINATION,), maximum_snap_distance_meters=5000)
        assert await pool.try_borrow(_dataset(tmp_path, "fixture-v2")) is None
        async with pool.borrow(_dataset(tmp_path, "fixture-v2")) as replacement:
            assert factory.processes[0].closed
            _ = await replacement.query(ORIGIN, (DESTINATION,), maximum_snap_distance_meters=5000)
        assert len(factory.processes) == 2 and await _held(database, "routing_engines") == 1


async def test_owner_renews_residency_across_long_active_query(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        manager = current_resource_manager()
        assert manager is not None
        manager.lease_duration_ns = 500_000_000
        pool = NativeRoutingPool(manager, process_factory=_Factory(delay=1.1))
        started = time_ns()
        async with pool.borrow(_dataset(tmp_path)) as borrow:
            _ = await borrow.query(ORIGIN, (DESTINATION,), maximum_snap_distance_meters=5000)
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT p.expires_at_utc_ns FROM resource_permits p JOIN resource_allocations a ON a.permit_token=p.token WHERE p.state='held' AND a.name='routing_engines'"
                )
            ).fetchall()
        assert len(rows) == 1 and isinstance(rows[0][0], int)
        assert rows[0][0] > started + 500_000_000


async def test_unconfirmed_cleanup_quarantines_until_explicit_physical_proof(
    tmp_path: Path,
) -> None:
    factory = _Factory(fail_close=True)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        manager = current_resource_manager()
        assert manager is not None
        pool = NativeRoutingPool(
            manager, cleanup_timeout_seconds=0.03, poll_seconds=0.005, process_factory=factory
        )
        async with pool.borrow(_dataset(tmp_path)) as borrow:
            _ = await borrow.query(ORIGIN, (DESTINATION,), maximum_snap_distance_meters=5000)
        with pytest.raises(NativeRoutingCleanupError):
            await pool.aclose()
        assert pool.quarantined_count == 1 and await _held(database, "routing_engines") == 1
        factory.processes[0].closed = True
        await pool.aclose()
        assert pool.owner_count == 0 and await _held(database, "routing_engines") == 0


async def test_shared_service_is_stable_for_the_root_runtime(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        manager = current_resource_manager()
        assert manager is not None
        assert native_routing_pool(manager) is native_routing_pool(manager)


async def test_closed_timeout_result_retires_owner_before_next_borrow(tmp_path: Path) -> None:
    processes: list[_Process] = []

    async def start(**kwargs: Any) -> _Process:
        process = _Process(pid=1234 + len(processes))
        if not processes:
            query = process.query

            async def timeout(*args: Any, **options: Any):
                _ = await query(*args, **options)
                process.closed = True
                return ("osrm_timeout",)

            process.query = timeout
        processes.append(process)
        return process

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        manager = current_resource_manager()
        assert manager is not None
        pool = NativeRoutingPool(manager, process_factory=start)
        async with pool.borrow(_dataset(tmp_path)) as first:
            assert await first.query(ORIGIN, (DESTINATION,), maximum_snap_distance_meters=5000) == (
                "osrm_timeout",
            )
        async with pool.borrow(_dataset(tmp_path)) as second:
            assert await second.query(
                ORIGIN, (DESTINATION,), maximum_snap_distance_meters=5000
            ) == ((1235.0, 1.0),)
        assert len(processes) == 2 and processes[0].closed


async def test_router_reborrows_expired_idle_owner_after_waiting_for_cpu(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now, factory = [monotonic()], _Factory()
    monkeypatch.setattr(pool_module, "monotonic", lambda: now[0])
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        manager = current_resource_manager()
        assert manager is not None
        pool = manager.get_or_create_service(
            "native_routing_pool", lambda: NativeRoutingPool(manager, process_factory=factory)
        )
        router = OsrmRouter(Settings(tmp_path / "roads.osrm"))
        results = []

        async def query() -> None:
            results.append(await router.estimates(ORIGIN, (DESTINATION,)))

        # Release the CPU blocker before joining its waiting caller.
        async with anyio.create_task_group() as tasks:  # noqa: SIM117
            async with manager.admit(ResourceRequest(name="cpu")):
                _ = tasks.start_soon(query)
                await _until(
                    lambda: pool.owner_count == 1 and pool._engines[0].borrower is not None
                )
                assert not factory.processes
                now[0] += 31
                pool._engines[0].wake.set()
                await _until(lambda: pool.owner_count == 0)
                assert not results and await _held(database, "cpu") == 1
        assert results == [((1234.0, 1.0),)] and len(factory.processes) == 1
        assert await _held(database, "cpu") == 0


async def test_standalone_startup_cleanup_failure_retains_admission_until_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    process, calls = _Process(fail_close=True), []

    async def start(**kwargs: Any):
        calls.append(kwargs)
        raise NativeRoutingCleanupError(cast(NativeRoutingProcess, cast(object, process)))

    monkeypatch.setattr(pool_module.NativeRoutingProcess, "start", start)
    router = OsrmRouter(Settings(tmp_path / "roads.osrm"))
    try:
        assert await router.estimates(ORIGIN, (DESTINATION,)) == ("osrm_unavailable",)
        with anyio.move_on_after(0.02) as cancelled:
            _ = await router.estimates(ORIGIN, (DESTINATION,))
        assert cancelled.cancel_called and len(calls) == 1
    finally:
        process.closed = True
        await router.aclose()
    assert not pool_module._UNMANAGED_QUARANTINE
