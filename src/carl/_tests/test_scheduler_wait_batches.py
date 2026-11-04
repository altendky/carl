"""Wait pages retain interval semantics without a database handoff per item."""

# Offline storage assertions and counted connection adapters inspect real ledgers.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, cast

import apsw
import pytest

import carl.io.scheduler_activity as scheduler_io
from carl._tests.test_fair_storage import _seed
from carl._tests.test_stage_storage import NOW, _claim
from carl.core.scheduler_activity import SchedulerWaitObservation
from carl.io.sqlite import Database

if TYPE_CHECKING:
    from apsw import AsyncConnection, AsyncCursor

pytestmark = pytest.mark.anyio
SCOPE = "wait-batch-test"
SECOND = 1_000_000_000


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _observation(
    identifier: str,
    *,
    reason: str = "resource",
    source: str = "advisory",
    resources: tuple[str, ...] = ("cpu",),
    initial: bool = False,
) -> SchedulerWaitObservation:
    return SchedulerWaitObservation.model_validate(
        {
            "work_identifier": identifier,
            "stage_identifier": None if initial else "stage-" + identifier,
            "reason": reason,
            "resource_names": resources,
            "source": source,
        }
    )


async def _observe(
    database: Database, observations: tuple[SchedulerWaitObservation, ...], now: int = NOW
) -> None:
    await database.observe_scheduler_waits(
        coordination_scope=SCOPE, observations=observations, now_utc_ns=now
    )


async def _rows(database: Database, table: str) -> list[tuple[apsw.SQLiteValue, ...]]:
    assert table in {"scheduler_waits", "scheduler_rollups", "scheduler_meta"}
    async with database._connections.reader() as connection:
        return [
            tuple(row)
            for row in await (await connection.execute(f"SELECT * FROM {table}")).fetchall()
        ]


class _CountedConnection:
    def __init__(self, connection: AsyncConnection) -> None:
        self.connection: AsyncConnection = connection
        self.execute_calls: int = 0
        self.executemany_calls: int = 0

    async def execute(self, sql: str, bindings: apsw.Bindings | None = None) -> AsyncCursor:
        self.execute_calls += 1
        return await self.connection.execute(sql, bindings)

    async def executemany(self, sql: str, bindings: Iterable[apsw.Bindings]) -> AsyncCursor:
        self.executemany_calls += 1
        return await self.connection.executemany(sql, bindings)


@pytest.mark.parametrize("size", (1, 32))
@pytest.mark.parametrize("mode", ("ordinary", "refresh", "noop"))
async def test_wait_page_handoffs_are_constant_and_rows_keep_exact_intervals(
    tmp_path: Path, size: int, mode: str
) -> None:
    async with Database.managed(tmp_path / "waits.sqlite3", initialize=True) as database:
        async with database.transaction():
            for index in range(size):
                _ = await _seed(database, f"work-{index:02}", root="root")
        observations = tuple(_observation(f"work-{index:02}") for index in range(size))
        now = NOW
        if mode != "ordinary":
            await _observe(database, observations)
            now += SECOND if mode == "refresh" else 1
        async with database._connections.writer() as connection:
            counted = _CountedConnection(connection)
            await scheduler_io.observe(
                cast("AsyncConnection", cast(object, counted)), SCOPE, observations, now
            )
        # Count asynchronous connection calls, rather than SQL statements inside
        # executemany: each bulk insertion still performs one SQLite row per work.
        assert counted.execute_calls + counted.executemany_calls <= (2 if mode == "noop" else 3)
        last_seen = NOW if mode == "noop" else now
        assert sorted(await _rows(database, "scheduler_waits")) == [
            (
                SCOPE,
                f"work-{index:02}",
                f"stage-work-{index:02}",
                "resource",
                '["cpu"]',
                "advisory",
                NOW,
                last_seen,
                last_seen + 5 * SECOND,
            )
            for index in range(size)
        ]
        assert await _rows(database, "scheduler_rollups") == []


async def test_throttle_ttl_boundaries_resources_and_backwards_clock(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "waits.sqlite3", initialize=True) as database:
        _ = await _seed(database, "work", root="root")
        await _observe(database, (_observation("work"),))
        changed_resources = _observation("work", resources=("http",))
        await _observe(database, (changed_resources,), NOW + SECOND - 1)
        assert (await _rows(database, "scheduler_waits"))[0][4:] == (
            '["cpu"]',
            "advisory",
            NOW,
            NOW,
            NOW + 5 * SECOND,
        )
        await _observe(database, (changed_resources,), NOW + SECOND)
        assert (await _rows(database, "scheduler_waits"))[0][4:] == (
            '["http"]',
            "advisory",
            NOW,
            NOW + SECOND,
            NOW + 6 * SECOND,
        )
        # Continuity includes the exact expiry, even though it exceeds throttle.
        await _observe(database, (changed_resources,), NOW + 6 * SECOND)
        assert (await _rows(database, "scheduler_waits"))[0][6] == NOW
        restarted = NOW + 11 * SECOND + 1
        await _observe(database, (changed_resources,), restarted)
        assert (await _rows(database, "scheduler_waits"))[0][6] == restarted
        await _observe(database, (changed_resources,), NOW)
        assert (await _rows(database, "scheduler_waits"))[0][6:] == (NOW, NOW, NOW + 5 * SECOND)
        assert await _rows(database, "scheduler_rollups") == [
            (
                SCOPE,
                NOW // scheduler_io.MINUTE_NS * scheduler_io.MINUTE_NS,
                "observed_resource_wait",
                "observed_lower_bound",
                -1,
                0,
                0,
                0,
                2,
            )
        ]


async def test_duplicate_work_observations_close_intermediate_intervals_in_order(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "waits.sqlite3", initialize=True) as database:
        _ = await _seed(database, "work", root="root")
        await _observe(
            database,
            (
                _observation("work"),
                _observation("work", reason="fairness"),
                _observation("work", source="admission"),
                _observation("work"),
                _observation("work", source="admission", resources=("http",)),
                _observation("work", source="admission", resources=("cpu",)),
            ),
        )
        assert await _rows(database, "scheduler_waits") == [
            (
                SCOPE,
                "work",
                "stage-work",
                "resource",
                '["http"]',
                "admission",
                NOW,
                NOW,
                NOW + 5 * SECOND,
            )
        ]
        assert await _rows(database, "scheduler_rollups") == [
            (
                SCOPE,
                NOW // scheduler_io.MINUTE_NS * scheduler_io.MINUTE_NS,
                "observed_resource_wait",
                "observed_lower_bound",
                -1,
                0,
                0,
                0,
                3,
            )
        ]


async def test_empty_page_still_prunes_cap_and_cap_remains_transactional(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "waits.sqlite3", initialize=True) as database:
        async with database.transaction():
            for index in range(4):
                _ = await _seed(database, f"work-{index}", root="root")
        await _observe(database, tuple(_observation(f"work-{index}") for index in range(4)))
        waits, meta = (
            await _rows(database, "scheduler_waits"),
            await _rows(database, "scheduler_meta"),
        )
        monkeypatch.setattr(scheduler_io, "WAIT_ROW_CAP", 2)
        with pytest.raises(RuntimeError, match="abort cap"):
            async with database.transaction():
                await _observe(database, ())
                assert await _rows(database, "scheduler_waits") == []
                raise RuntimeError("abort cap")
        assert await _rows(database, "scheduler_waits") == waits
        assert await _rows(database, "scheduler_meta") == meta
        await _observe(database, ())
        assert await _rows(database, "scheduler_waits") == []
        assert {str(row[0]): row[1] for row in await _rows(database, "scheduler_meta")}[
            "dropped_samples"
        ] == 4


async def test_interval_rollups_and_final_wait_rows_roll_back_together(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "waits.sqlite3", initialize=True) as database:
        _ = await _seed(database, "work", root="root")
        await _observe(database, (_observation("work"),))
        before = {
            table: await _rows(database, table)
            for table in ("scheduler_waits", "scheduler_rollups", "scheduler_meta")
        }
        with pytest.raises(RuntimeError, match="abort page"):
            async with database.transaction():
                await _observe(database, (_observation("work", reason="fairness"),), NOW + SECOND)
                raise RuntimeError("abort page")
        assert {
            table: await _rows(database, table)
            for table in ("scheduler_waits", "scheduler_rollups", "scheduler_meta")
        } == before


async def test_other_connection_stage_changes_are_rechecked_each_page(tmp_path: Path) -> None:
    path = tmp_path / "waits.sqlite3"
    async with Database.managed(path, initialize=True) as first:
        stage = await _seed(first, "work", root="root", register=False)
        initial = _observation("work", initial=True)
        await _observe(first, (initial,))
        async with Database.managed(path) as second:
            await second.register_stage(stage, created_at_utc_ns=NOW)
            before = await _rows(first, "scheduler_waits")
            await _observe(first, (initial,), NOW + SECOND)
            assert await _rows(first, "scheduler_waits") == before
            await _observe(first, (_observation("work"),), NOW + SECOND)
            assert (await _rows(first, "scheduler_waits"))[0][2] == stage.identifier
            assert (await _claim(second, stage.identifier, now=NOW + SECOND)).lease is not None
            assert await _rows(first, "scheduler_waits") == []
            await _observe(first, (_observation("work"),), NOW + 2 * SECOND)
            assert await _rows(first, "scheduler_waits") == []
