from __future__ import annotations

from pathlib import Path

import anyio
import apsw
import pytest

from carl.io.db import DatabaseConnections


async def _values(connection: apsw.AsyncConnection) -> list[int]:
    return [row[0] async for row in await connection.execute("SELECT value FROM counter")]


@pytest.mark.anyio
async def test_nested_writer_rolls_back_only_nested_savepoint(tmp_path: Path) -> None:
    async with DatabaseConnections.managed(tmp_path / "database.sqlite3") as database:
        async with database.writer() as writer:
            await writer.execute("CREATE TABLE counter(value INTEGER NOT NULL) STRICT")
            await writer.execute("INSERT INTO counter VALUES (1)")

            with pytest.raises(RuntimeError, match="nested failure"):
                async with database.writer() as nested:
                    assert nested is writer
                    await nested.execute("INSERT INTO counter VALUES (2)")
                    raise RuntimeError("nested failure")

            assert await _values(writer) == [1]

        async with database.reader() as reader:
            assert await _values(reader) == [1]


@pytest.mark.anyio
async def test_reader_is_query_only_and_holds_snapshot(tmp_path: Path) -> None:
    async with DatabaseConnections.managed(tmp_path / "database.sqlite3") as database:
        async with database.writer() as writer:
            await writer.execute("CREATE TABLE counter(value INTEGER NOT NULL) STRICT")
            await writer.execute("INSERT INTO counter VALUES (1)")

        writer_finished = anyio.Event()
        reader_can_finish = anyio.Event()

        async def write() -> None:
            async with database.writer() as writer:
                await writer.execute("INSERT INTO counter VALUES (2)")
            writer_finished.set()
            await reader_can_finish.wait()

        async with anyio.create_task_group() as task_group, database.reader() as reader:
            assert await _values(reader) == [1]
            with pytest.raises(apsw.ReadOnlyError):
                await reader.execute("INSERT INTO counter VALUES (9)")
            task_group.start_soon(write)
            await writer_finished.wait()
            assert await _values(reader) == [1]
            reader_can_finish.set()

        async with database.reader() as reader:
            assert await _values(reader) == [1, 2]


@pytest.mark.anyio
async def test_cancelled_writer_rolls_back_and_releases_lock(tmp_path: Path) -> None:
    async with DatabaseConnections.managed(tmp_path / "database.sqlite3") as database:
        async with database.writer() as writer:
            await writer.execute("CREATE TABLE counter(value INTEGER NOT NULL) STRICT")

        started = anyio.Event()

        async def cancelled_write() -> None:
            async with database.writer() as writer:
                await writer.execute("INSERT INTO counter VALUES (1)")
                started.set()
                await anyio.sleep_forever()

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(cancelled_write)
            await started.wait()
            task_group.cancel_scope.cancel()

        async with database.writer() as writer:
            assert await _values(writer) == []
            await writer.execute("INSERT INTO counter VALUES (2)")

        async with database.reader() as reader:
            assert await _values(reader) == [2]


@pytest.mark.anyio
async def test_cancelled_reader_returns_connection_to_pool(tmp_path: Path) -> None:
    async with DatabaseConnections.managed(
        tmp_path / "database.sqlite3",
        reader_count=1,
    ) as database:
        async with database.writer() as writer:
            await writer.execute("CREATE TABLE counter(value INTEGER NOT NULL) STRICT")
            await writer.execute("INSERT INTO counter VALUES (1)")

        started = anyio.Event()
        scopes: list[anyio.CancelScope] = []

        async def cancelled_read() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                async with database.reader() as reader:
                    assert await _values(reader) == [1]
                    started.set()
                    await anyio.sleep_forever()

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(cancelled_read)
            await started.wait()
            scopes[0].cancel()

        with anyio.fail_after(1):
            async with database.reader() as reader:
                assert await _values(reader) == [1]


@pytest.mark.anyio
async def test_managed_cleanup_completes_inside_cancelled_scope(tmp_path: Path) -> None:
    path = tmp_path / "database.sqlite3"

    with anyio.CancelScope() as scope:
        async with DatabaseConnections.managed(path) as database:
            async with database.writer() as writer:
                await writer.execute("CREATE TABLE counter(value INTEGER NOT NULL) STRICT")
            scope.cancel()
            await anyio.sleep(0)

    with anyio.fail_after(1):
        async with (
            DatabaseConnections.managed(path, create=False) as database,
            database.reader() as reader,
        ):
            assert await _values(reader) == []
