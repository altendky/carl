"""Dispatcher occupancy depends on held permits, including expired owners."""

# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl.io.sqlite import Database


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


async def _cost(database: Database) -> tuple[dict[str, int], int]:
    steps = 0

    def count() -> bool:
        nonlocal steps
        steps += 100
        return False

    async with database._connections.reader() as connection:
        await connection.set_progress_handler(count, nsteps=100, id="occupancy-budget")
        try:
            result = await database.resource_occupancy("test-host")
        finally:
            await connection.set_progress_handler(None, id="occupancy-budget")
    return result, steps


async def _history(database: Database, start: int, count: int) -> None:
    async with database._connections.writer() as connection:
        await connection.execute(
            """WITH RECURSIVE numbers(n) AS (
                SELECT ? UNION ALL SELECT n+1 FROM numbers WHERE n<?
            ) INSERT INTO resource_permits
            SELECT 'history-'||n,CASE WHEN n%3=0 THEN 'other-host' ELSE 'test-host' END,
                   'history-owner',CASE WHEN n%2=0 THEN 'released' ELSE 'reclaimed' END,
                   0,100 FROM numbers""",
            (start, start + count - 1),
        )
        await connection.execute(
            """WITH RECURSIVE numbers(n) AS (
                SELECT ? UNION ALL SELECT n+1 FROM numbers WHERE n<?
            ) INSERT INTO resource_allocations
            SELECT 'history-'||n,CASE WHEN n%3=0 THEN 'other-host' ELSE 'test-host' END,
                   'cpu',1 FROM numbers""",
            (start, start + count - 1),
        )


@pytest.mark.anyio
async def test_occupancy_ignores_released_history_and_other_scopes(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        async with database._connections.writer() as connection:
            await connection.executemany(
                "INSERT INTO resource_budgets VALUES (?,?,?)",
                (("test-host", "cpu", 10), ("test-host", "http", 25), ("other-host", "cpu", 100)),
            )
            await connection.executemany(
                "INSERT INTO resource_permits VALUES (?,?,'owner','held',0,?)",
                (
                    ("active", "test-host", 2**63 - 1),
                    ("expired-but-held", "test-host", 100),
                    ("multiple-resources", "test-host", 2**63 - 1),
                    ("other-owner", "other-host", 2**63 - 1),
                ),
            )
            await connection.executemany(
                "INSERT INTO resource_allocations VALUES (?,?,?,?)",
                (
                    ("active", "test-host", "cpu", 2),
                    ("expired-but-held", "test-host", "cpu", 1),
                    ("multiple-resources", "test-host", "cpu", 1),
                    ("multiple-resources", "test-host", "http", 3),
                    ("other-owner", "other-host", "cpu", 100),
                ),
            )
        await _history(database, 0, 64)
        before, before_steps = await _cost(database)
        await _history(database, 64, 4096)
        after, after_steps = await _cost(database)
        assert before == after == {"cpu": 4, "http": 3}
        assert after_steps <= before_steps + 500

        # Expiry itself never returns capacity. Only a physical owner's release
        # changes occupancy, and a scope with only retained history is cheap too.
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE resource_permits SET state='released' WHERE scope='test-host' AND state='held'"
            )
        empty, empty_steps = await _cost(database)
        assert empty == {}
        assert empty_steps <= before_steps + 500
