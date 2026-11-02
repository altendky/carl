"""Routing starts are paced atomically without booking slots for denied polls."""

# Cache rows and legacy layouts are inspected at the SQLite allocation boundary.
# pyright: reportPrivateUsage=false

import json
import os
import sqlite3
import stat
import sys
import threading
from pathlib import Path
from typing import cast

import anyio
import pytest

from carl.io.configuration import ConfigurationFailure
from carl.io.paths import CarlDirectories
from carl.io.routing_cache import (
    MAX_CACHE_VALUE_BYTES,
    RoutingCache,
    RoutingGateDecision,
    RoutingLane,
)

pytestmark = pytest.mark.anyio


def _directories(root: Path) -> CarlDirectories:
    return CarlDirectories(
        **{name: root / name for name in ("config", "data", "cache", "state", "runtime")}
    )


def _cache(root: Path, *, now: float = 100) -> RoutingCache:
    return RoutingCache(_directories(root), now=lambda: now)


def _tail(cache: RoutingCache, lane: str) -> float:
    with sqlite3.connect(cache.directories.routing_cache_file) as connection:
        row = connection.execute("SELECT next_start FROM pacing WHERE lane=?", (lane,)).fetchone()
    assert row is not None
    return float(row[0])


@pytest.mark.parametrize("lane,interval", (("geocode", 0.65), ("matrix", 1.6)))
async def test_denied_polls_and_old_action_replays_never_book_future_slots(
    tmp_path: Path, lane: str, interval: float
) -> None:
    cache = _cache(tmp_path)
    first = await cache.try_gate(cast(RoutingLane, lane), action_identifier="original", now=100)
    assert first == RoutingGateDecision("original", 100, 100 + interval, None)
    for action in ("original", "other", "original"):
        denied = await cache.try_gate(cast(RoutingLane, lane), action_identifier=action, now=100.1)
        assert not denied.granted and denied.reason == "pacing"
        assert denied.next_eligible_at == 100 + interval
        assert _tail(cache, lane) == 100 + interval
    second = await cache.try_gate(
        cast(RoutingLane, lane), action_identifier="other", now=100 + interval
    )
    assert second.granted
    replay = await cache.try_gate(
        cast(RoutingLane, lane), action_identifier="original", now=100 + interval
    )
    assert not replay.granted and replay.next_eligible_at == second.next_eligible_at
    # A late caller gets one current slot, without consuming old scheduled slots.
    late = await cache.try_gate(cast(RoutingLane, lane), action_identifier="late", now=1000)
    assert late.granted and late.next_eligible_at == 1000 + interval


async def test_simultaneous_cross_instance_calls_grant_exactly_one_start(tmp_path: Path) -> None:
    caches = [_cache(tmp_path) for _ in range(12)]
    decisions: list[RoutingGateDecision] = []

    async def attempt(index: int) -> None:
        decisions.append(
            await caches[index].try_gate("geocode", action_identifier=str(index), now=100)
        )

    async with anyio.create_task_group() as tasks:
        for index in range(len(caches)):
            tasks.start_soon(attempt, index)
    assert sum(decision.granted for decision in decisions) == 1
    assert {decision.next_eligible_at for decision in decisions} == {100.65}
    assert _tail(caches[0], "geocode") == 100.65
    with sqlite3.connect(caches[0].directories.routing_cache_file, timeout=0) as connection:
        connection.execute("BEGIN IMMEDIATE")


async def test_actual_start_clock_is_sampled_after_waiting_for_a_cache_writer(
    tmp_path: Path,
) -> None:
    clock = [100.0]
    connected = threading.Event()

    class ObservedCache(RoutingCache):
        def _connect(self) -> sqlite3.Connection:
            connection = super()._connect()
            connected.set()
            return connection

    cache = ObservedCache(_directories(tmp_path), now=lambda: clock[0])
    await cache.put("seed", {}, 200)
    decisions: list[RoutingGateDecision] = []

    async def attempt() -> None:
        decisions.append(await cache.try_gate("geocode", action_identifier="delayed", now=100))

    with sqlite3.connect(cache.directories.routing_cache_file) as blocker:
        blocker.execute("BEGIN IMMEDIATE")
        connected.clear()
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(attempt)
            with anyio.fail_after(2):
                while not connected.is_set():
                    await anyio.sleep(0.001)
            clock[0] = 110
            blocker.commit()
    assert decisions == [RoutingGateDecision("delayed", 110, 110.65, None)]


async def test_separate_processes_share_one_atomic_start_gate(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    await cache.put("seed", {}, 200)
    script = """import json, sys, time
from pathlib import Path
from carl.io.paths import CarlDirectories
from carl.io.routing_cache import RoutingCache
root, number = Path(sys.argv[1]), sys.argv[2]
directories = CarlDirectories(**{name:root/name for name in ('config','data','cache','state','runtime')})
cache = RoutingCache(directories, now=lambda:100)
(root / ('ready-' + number)).touch()
while not (root / 'go').exists(): time.sleep(0.001)
result = cache._try_gate('matrix', 'process-' + number, 100)
print(json.dumps({'granted':result.granted,'deadline':result.next_eligible_at}))
"""
    results: list[dict[str, object]] = []

    async def child(number: int) -> None:
        result = await anyio.run_process((sys.executable, "-c", script, str(tmp_path), str(number)))
        results.append(json.loads(result.stdout))

    with anyio.fail_after(15):
        async with anyio.create_task_group() as tasks:
            for number in range(4):
                tasks.start_soon(child, number)
            while len(list(tmp_path.glob("ready-*"))) != 4:
                await anyio.sleep(0.01)
            (tmp_path / "go").touch()
    assert sum(result["granted"] is True for result in results) == 1
    assert {result["deadline"] for result in results} == {101.6}
    assert _tail(cache, "matrix") == 101.6


async def test_lanes_are_independent_but_cooldown_is_shared_persistent_and_never_shortened(
    tmp_path: Path,
) -> None:
    cache = _cache(tmp_path)
    assert (await cache.try_gate("geocode", action_identifier="geo", now=100)).granted
    assert (await cache.try_gate("matrix", action_identifier="matrix", now=100)).granted
    assert await cache.block_until(130) == 130
    assert await _cache(tmp_path).block_until(120) == 130
    assert await _cache(tmp_path).block_until(160) == 160
    reopened = _cache(tmp_path, now=110)
    for lane in ("geocode", "matrix"):
        tail = _tail(cache, lane)
        result = await reopened.try_gate(
            cast(RoutingLane, lane), action_identifier="after-restart", now=110
        )
        assert not result.granted and result.reason == "provider_cooldown"
        assert result.next_eligible_at == 160
        assert _tail(cache, lane) == tail
    assert (await reopened.try_gate("geocode", action_identifier="ready", now=160)).granted


async def test_bounded_json_roundtrip_counts_utf8_bytes_and_oversized_corrupt_reads_miss(
    tmp_path: Path,
) -> None:
    cache = _cache(tmp_path)
    value = "x" * (MAX_CACHE_VALUE_BYTES - 2)
    await cache.put("boundary", value, 200)
    assert await cache.get("boundary", 100) == value
    with pytest.raises(ValueError, match="64 KiB"):
        await cache.put("too-large", "é" * (MAX_CACHE_VALUE_BYTES // 2), 200)
    with sqlite3.connect(cache.directories.routing_cache_file) as connection:
        connection.executemany(
            "INSERT OR REPLACE INTO estimates VALUES (?,200,?)",
            (
                ("oversized", json.dumps("x" * MAX_CACHE_VALUE_BYTES)),
                ("multibyte", json.dumps("é" * (MAX_CACHE_VALUE_BYTES // 2), ensure_ascii=False)),
                ("corrupt", "{bad"),
                ("nonfinite", '{"value":NaN}'),
                ("overflow", '{"value":1e999}'),
            ),
        )
        connection.execute("INSERT INTO estimates VALUES ('bad-utf8',200,CAST(X'80' AS TEXT))")

    class ObservedCache(RoutingCache):
        def _connect(self) -> sqlite3.Connection:
            connection = super()._connect()

            def row(_cursor: sqlite3.Cursor, values: tuple[object, ...]) -> tuple[object, ...]:
                assert not any(
                    (isinstance(item, str) and len(item.encode()) > MAX_CACHE_VALUE_BYTES)
                    or (isinstance(item, bytes) and len(item) > MAX_CACHE_VALUE_BYTES)
                    for item in values
                )
                return values

            connection.row_factory = row
            return connection

    observed = ObservedCache(cache.directories)
    for key in (
        "oversized",
        "multibyte",
        "corrupt",
        "nonfinite",
        "overflow",
        "bad-utf8",
        "missing",
    ):
        assert await observed.get(key, 100) is None


async def test_old_cache_upgrade_preserves_values_pacing_and_legacy_reserve_api(
    tmp_path: Path,
) -> None:
    directories = _directories(tmp_path)
    directories.routing_cache_file.parent.mkdir(mode=0o700, parents=True)
    with sqlite3.connect(directories.routing_cache_file) as connection:
        connection.execute(
            "CREATE TABLE estimates (key TEXT PRIMARY KEY, expires REAL NOT NULL, value TEXT NOT NULL)"
        )
        connection.execute("CREATE TABLE pacing (lane TEXT PRIMARY KEY, next_start REAL NOT NULL)")
        connection.execute("INSERT INTO estimates VALUES ('old',200,'{\"kept\":true}')")
        connection.execute("INSERT INTO pacing VALUES ('matrix',105)")
    directories.routing_cache_file.chmod(0o600)
    cache = RoutingCache(directories, now=lambda: 100)
    assert await cache.get("old", 100) == {"kept": True}
    denied = await cache.try_gate("matrix", action_identifier="new", now=100)
    assert not denied.granted and denied.next_eligible_at == 105
    assert await cache.reserve("matrix", 1.6, 100) == 5
    assert _tail(cache, "matrix") == 106.6
    assert await cache.block_until(130) == 130


async def test_private_permissions_and_bounded_pruning_are_preserved(tmp_path: Path) -> None:
    clock = [100.0]
    cache = RoutingCache(_directories(tmp_path), now=lambda: clock[0])
    for index in range(250):
        await cache.put(str(index), {}, 200)
    assert stat.S_IMODE(cache.directories.routing_cache_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(cache.directories.routing_cache_file.parent.stat().st_mode) == 0o700
    clock[0] = 201
    await cache.put("fresh", {}, 300)
    with sqlite3.connect(cache.directories.routing_cache_file) as connection:
        assert connection.execute("SELECT count(*) FROM estimates").fetchone() == (151,)
    assert await cache.get("fresh", 201) == {}
    assert await cache.get("249", 201) is None
    cache.directories.routing_cache_file.chmod(0o644)
    with pytest.raises(ConfigurationFailure, match="routing_cache_permissions"):
        await cache.get("fresh", 201)


async def test_symlink_cache_is_rejected_without_touching_its_target(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    parent = cache.directories.routing_cache_file.parent
    parent.mkdir(mode=0o700, parents=True)
    target = tmp_path / "other"
    target.write_bytes(b"untouched")
    os.symlink(target, cache.directories.routing_cache_file)
    with pytest.raises(ConfigurationFailure, match="routing_cache_permissions"):
        await cache.get("key", 100)
    assert target.read_bytes() == b"untouched"
