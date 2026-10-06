from __future__ import annotations

from pathlib import Path
from typing import cast

import anyio
import apsw
import pytest

from carl.io.db import AsyncConnection, DatabaseConnections, DatabaseWrapperError, _transaction


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


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["error", "timeout"])
async def test_close_attempts_every_resource_and_retries_only_failed_close(
    failure: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("carl.io.db._CLEANUP_TIMEOUT_SECONDS", 0.01)
    calls: list[str] = []

    class Connection:
        def __init__(self, name: str):
            self.name = name
            self.fail = name == "reader-one"

        async def aclose(self, *, force: bool) -> None:
            assert force
            calls.append(self.name)
            if self.fail:
                if failure == "timeout":
                    await anyio.sleep_forever()
                raise OSError("private database close details")

    reader_one, reader_two, writer = (
        Connection(name)
        for name in (
            "reader-one",
            "reader-two",
            "writer",
        )
    )
    send, receive = anyio.create_memory_object_stream[AsyncConnection](2)
    database = DatabaseConnections(
        _writer=cast(AsyncConnection, cast(object, writer)),
        _readers=(
            cast(AsyncConnection, cast(object, reader_one)),
            cast(AsyncConnection, cast(object, reader_two)),
        ),
        _reader_send=send,
        _reader_receive=receive,
    )
    expected_error = TimeoutError if failure == "timeout" else OSError
    with pytest.raises(expected_error):
        await database.aclose()
    assert calls == ["reader-one", "reader-two", "writer"]
    assert database._closed and not database._fully_closed
    with pytest.raises(DatabaseWrapperError):
        async with database.writer():
            raise AssertionError("Closing database must reject borrowers")

    reader_one.fail = False
    await database.aclose()
    await database.aclose()
    assert calls == ["reader-one", "reader-two", "writer", "reader-one"]
    assert database._fully_closed


@pytest.mark.anyio
async def test_managed_close_failure_preserves_body_error(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    primary = ValueError("body error")
    closed: list[str] = []
    database: DatabaseConnections | None = None
    readers: tuple[apsw.AsyncConnection, ...] = ()

    class Connection:
        async def aclose(self, *, force: bool) -> None:
            closed.append("reader")
            raise OSError("private cleanup details")

    with pytest.raises(ValueError) as caught:
        async with DatabaseConnections.managed(tmp_path / "database.sqlite3") as database:
            readers = database._readers
            database._readers = (cast(AsyncConnection, cast(object, Connection())), *readers)
            raise primary
    assert caught.value is primary
    assert database is not None
    assert closed == ["reader"]
    assert not database._fully_closed
    assert id(database._writer) in database._closed_resource_identifiers
    assert all(id(reader) in database._closed_resource_identifiers for reader in readers)
    assert "database resource close: OSError" in caplog.text
    assert "private cleanup details" not in caplog.text


@pytest.mark.anyio
async def test_transaction_entry_handoff_survives_cancellation_before_result() -> None:
    events: list[str] = []
    with anyio.CancelScope() as scope:

        class Connection:
            async def __aenter__(self) -> None:
                events.append("savepoint-created")
                scope.cancel()
                await anyio.lowlevel.checkpoint()
                events.append("entry-returned")

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                error: BaseException | None,
                traceback: object,
            ) -> bool:
                assert exc_type is anyio.get_cancelled_exc_class()
                await anyio.lowlevel.checkpoint()
                events.append("rollback-completed")
                return False

        async with _transaction(cast(AsyncConnection, cast(object, Connection()))):
            await anyio.lowlevel.checkpoint()
    assert scope.cancelled_caught
    assert events == ["savepoint-created", "entry-returned", "rollback-completed"]
