"""Native lifetime accounting composes with real stages and separate Carl processes."""

# These integration assertions inspect physical reservations and controlled recovery.
# pyright: reportPrivateUsage=false

import json
import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent
from time import time_ns

import anyio
import pytest

import carl.io.native_routing_pool as pool_module
import carl.io.resource_permits as permits_module
from carl._tests.test_osrm import DESTINATION, ORIGIN, Settings
from carl._tests.test_stage_execution import _admit, _seed
from carl.core.native_routing import NativeRoutingDataset
from carl.core.worker import CompletedWork
from carl.io.native_routing_pool import native_routing_pool
from carl.io.native_routing_protocol import NativeRoutingProtocolError
from carl.io.osrm import OsrmRouter
from carl.io.resource_permits import current_resource_manager
from carl.io.sqlite import Database
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio
_BACKEND = "carl._tests.native_routing_backend"


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _held(database: Database, name: str) -> int:
    async with database._connections.reader() as connection:
        cursor = await connection.execute(
            "SELECT coalesce(sum(a.units),0) FROM resource_allocations a "
            "JOIN resource_permits p ON p.token=a.permit_token "
            "WHERE p.state='held' AND a.name=?",
            (name,),
        )
        row = await cursor.fetchone()
    assert row is not None
    return int(row[0])


def _dataset(root: Path) -> NativeRoutingDataset:
    return NativeRoutingDataset(dataset_path=root / "fast", dataset_identifier="fixture-v1")


def _pid_count(result: tuple[tuple[float, float] | str, ...]) -> tuple[int, int]:
    assert len(result) == 1 and isinstance(result[0], tuple)
    return int(result[0][0]), int(result[0][1])


async def test_warm_child_drops_cpu_guard_before_real_stage_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pool_module, "DEFAULT_BACKEND_MODULE", _BACKEND)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        manager = current_resource_manager()
        assert manager is not None
        pool = native_routing_pool(manager)
        borrow = await pool.try_borrow(_dataset(tmp_path))
        assert borrow is not None
        process_ids: list[int] = []
        async with borrow:
            for expected_count in (1, 2):
                stage = await _seed(database)
                async with _admit(stage_manager(database), stage) as admission:
                    execution = admission.execution
                    assert execution is not None
                    result = await borrow.query(
                        ORIGIN, (DESTINATION,), maximum_snap_distance_meters=5000
                    )
                    pid, count = _pid_count(result)
                    process_ids.append(pid)
                    assert count == expected_count
                    # Completion seals the actual stage guard immediately. A
                    # child retaining this query's FD would make sealing fail.
                    await execution.finish(CompletedWork(result={"query": expected_count}))
                assert await _held(database, "cpu") == 0
                assert await _held(database, "routing_engines") == 1
        assert len(set(process_ids)) == 1


async def test_distinct_router_objects_share_one_warm_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pool_module, "DEFAULT_BACKEND_MODULE", _BACKEND)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        settings = Settings(tmp_path / "fast")
        first = _pid_count(await OsrmRouter(settings).estimates(ORIGIN, (DESTINATION,)))
        second = _pid_count(await OsrmRouter(settings).estimates(ORIGIN, (DESTINATION,)))
        assert first[0] == second[0] and (first[1], second[1]) == (1, 2)
        assert await _held(database, "cpu") == 0
        assert await _held(database, "routing_engines") == 1


async def test_permanent_residency_admission_failure_is_not_polled_forever(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        manager = current_resource_manager()
        assert manager is not None
        pool = native_routing_pool(manager)
        calls = 0

        async def fail(**_kwargs: object) -> None:
            nonlocal calls
            calls += 1
            raise ValueError("private configuration diagnostic")

        monkeypatch.setattr(database, "try_acquire_resource_permit", fail)
        with anyio.fail_after(2), pytest.raises(NativeRoutingProtocolError) as error:
            async with pool.borrow(_dataset(tmp_path)):
                pytest.fail("Permanent admission failure opened a native engine")
        assert calls == 1
        assert "private" not in str(error.value)
        assert await _held(database, "routing_engines") == 0


async def test_separate_process_residency_recovers_after_abrupt_owner_death(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pool_module, "DEFAULT_BACKEND_MODULE", _BACKEND)
    path, ready = tmp_path / "carl.sqlite3", tmp_path / "ready.json"
    program = dedent(
        """
        import json, sys
        from pathlib import Path
        import anyio
        import carl.io.native_routing_pool as pools
        from carl.core.native_routing import NativeRoutingDataset
        from carl.core.travel import TravelCoordinate
        from carl.io.resource_permits import current_resource_manager
        from carl.io.sqlite import Database

        async def main():
            path, ready, root = map(Path, sys.argv[1:])
            pools.DEFAULT_BACKEND_MODULE = 'carl._tests.native_routing_backend'
            async with Database.managed(path, resource_capacities={'cpu':1}) as database:
                manager = current_resource_manager()
                pool = pools.native_routing_pool(manager)
                dataset = NativeRoutingDataset(dataset_path=root/'fast', dataset_identifier='fixture-v1')
                async with pool.borrow(dataset) as engine:
                    result = await engine.query(TravelCoordinate(latitude=10,longitude=20),
                        (TravelCoordinate(latitude=11,longitude=21),), maximum_snap_distance_meters=5000)
                    temporary = ready.with_suffix('.new')
                    temporary.write_text(json.dumps(result))
                    temporary.replace(ready)
                    await anyio.sleep_forever()
        anyio.run(main)
        """
    )
    async with Database.managed(path, initialize=True, resource_capacities={"cpu": 1}) as database:
        manager = current_resource_manager()
        assert manager is not None
        pool = native_routing_pool(manager)
        process = await anyio.open_process(
            (sys.executable, "-c", program, str(path), str(ready), str(tmp_path)),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        native_pidfd: int | None = None
        try:
            with anyio.fail_after(15):
                while not ready.exists():
                    if process.returncode is not None:
                        diagnostic = (
                            await process.stderr.receive(4096)
                            if process.stderr is not None
                            else b""
                        )
                        pytest.fail(f"Fixture Carl exited: {diagnostic!r}")
                    await anyio.sleep(0.01)
            saved = json.loads(ready.read_text())
            native_pid = int(saved[0][0])
            native_pidfd = os.pidfd_open(native_pid)
            assert await pool.try_borrow(_dataset(tmp_path)) is None
            assert await _held(database, "routing_engines") == 1
            assert await _held(database, "cpu") == 0

            process.kill()
            await process.wait()
            # Socket EOF must shut down the warm native worker even though its
            # Carl owner had no chance to execute a finally block.
            with anyio.fail_after(10):
                await anyio.wait_readable(native_pidfd)
            with monkeypatch.context() as recovery_clock:
                future = time_ns() + 120_000_000_000
                recovery_clock.setattr(permits_module, "time_ns", lambda: future)
                await manager._recover()
            replacement = await pool.try_borrow(_dataset(tmp_path))
            assert replacement is not None
            async with replacement:
                pid, count = _pid_count(
                    await replacement.query(
                        ORIGIN, (DESTINATION,), maximum_snap_distance_meters=5000
                    )
                )
                assert pid != native_pid and count == 1
                assert await _held(database, "routing_engines") == 1
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
            await process.aclose()
            if native_pidfd is not None:
                os.close(native_pidfd)
