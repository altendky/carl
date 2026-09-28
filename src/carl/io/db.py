"""AnyIO/Trio-compatible SQLite connection and transaction management."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import apsw

if TYPE_CHECKING:
    from apsw import AsyncConnection
else:
    AsyncConnection = apsw.Connection


class DatabaseWrapperError(Exception):
    """Base error for database lifecycle and transaction invariants."""


class ReaderPoolClosedError(DatabaseWrapperError):
    pass


_CLEANUP_TIMEOUT_SECONDS = 35


@asynccontextmanager
async def _transaction(connection: AsyncConnection) -> AsyncGenerator[None]:
    """Run an APSW savepoint transaction with cancellation-safe cleanup."""

    await connection.__aenter__()
    try:
        yield
    except BaseException as error:
        suppressed = False
        with anyio.fail_after(_CLEANUP_TIMEOUT_SECONDS, shield=True):
            suppressed = await connection.__aexit__(
                type(error),
                error,
                error.__traceback__,
            )
        if not suppressed:
            raise
    else:
        with anyio.fail_after(_CLEANUP_TIMEOUT_SECONDS, shield=True):
            await connection.__aexit__(None, None, None)


async def _rows(
    connection: AsyncConnection,
    statement: str,
    bindings: apsw.Bindings | None = None,
) -> list[tuple[apsw.SQLiteValue, ...]]:
    cursor = await connection.execute(statement, bindings)
    return [tuple(row) async for row in cursor]


async def _configure_connection(
    connection: AsyncConnection,
    *,
    query_only: bool,
    busy_timeout_ms: int,
) -> None:
    await connection.execute("PRAGMA foreign_keys = ON")
    await connection.set_busy_timeout(busy_timeout_ms)
    await connection.execute(f"PRAGMA query_only = {'ON' if query_only else 'OFF'}")

    foreign_keys = await _rows(connection, "PRAGMA foreign_keys")
    actual_query_only = await _rows(connection, "PRAGMA query_only")
    if foreign_keys != [(1,)]:
        raise DatabaseWrapperError("SQLite foreign-key enforcement was not enabled")
    if actual_query_only != [(1 if query_only else 0,)]:
        raise DatabaseWrapperError("SQLite query-only mode did not match the requested state")


@dataclass
class DatabaseConnections:
    """One serialized writer plus a bounded pool of snapshot-capable readers."""

    _writer: AsyncConnection
    _readers: tuple[AsyncConnection, ...]
    _reader_send: anyio.abc.ObjectSendStream[AsyncConnection]
    _reader_receive: anyio.abc.ObjectReceiveStream[AsyncConnection]
    _writer_lock: anyio.Lock = field(default_factory=anyio.Lock)
    _current_writer_task_identifier: int | None = None
    _reader_by_task_identifier: dict[int, AsyncConnection] = field(default_factory=dict)
    _closed: bool = False

    @classmethod
    @asynccontextmanager
    async def managed(
        cls,
        path: Path,
        *,
        create: bool = True,
        reader_count: int = 4,
        busy_timeout_ms: int = 30_000,
        synchronous: str = "FULL",
    ) -> AsyncGenerator[DatabaseConnections]:
        if reader_count < 1:
            raise ValueError("At least one reader connection is required")
        if create:
            path.parent.mkdir(parents=True, exist_ok=True)

        writer_flags = apsw.SQLITE_OPEN_READWRITE
        if create:
            writer_flags |= apsw.SQLITE_OPEN_CREATE
        writer = await apsw.Connection.as_async(str(path), flags=writer_flags)
        readers: list[AsyncConnection] = []
        send, receive = anyio.create_memory_object_stream[AsyncConnection](reader_count)
        try:
            writer.transaction_mode = "IMMEDIATE"
            await _configure_connection(
                writer,
                query_only=False,
                busy_timeout_ms=busy_timeout_ms,
            )
            journal_mode = await _rows(writer, "PRAGMA journal_mode = WAL")
            if journal_mode != [("wal",)]:
                raise DatabaseWrapperError("SQLite did not enable WAL journal mode")
            await writer.execute(f"PRAGMA synchronous = {synchronous}")

            for _ in range(reader_count):
                reader = await apsw.Connection.as_async(
                    str(path),
                    flags=apsw.SQLITE_OPEN_READONLY,
                )
                readers.append(reader)
                reader.transaction_mode = "DEFERRED"
                await _configure_connection(
                    reader,
                    query_only=True,
                    busy_timeout_ms=busy_timeout_ms,
                )
                await send.send(reader)

        except BaseException:
            with anyio.fail_after(_CLEANUP_TIMEOUT_SECONDS, shield=True):
                await send.aclose()
                await receive.aclose()
                for reader in readers:
                    await reader.aclose(force=True)
                await writer.aclose(force=True)
            raise

        wrapper = cls(
            _writer=writer,
            _readers=tuple(readers),
            _reader_send=send,
            _reader_receive=receive,
        )
        try:
            yield wrapper
        finally:
            await wrapper.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        with anyio.fail_after(_CLEANUP_TIMEOUT_SECONDS, shield=True):
            await self._reader_send.aclose()
            await self._reader_receive.aclose()
            for reader in self._readers:
                await reader.aclose(force=True)
            await self._writer.aclose(force=True)

    def _task_identifier(self) -> int:
        return anyio.get_current_task().id

    @asynccontextmanager
    async def writer(self) -> AsyncGenerator[AsyncConnection]:
        """Open a transaction; a nested writer gets an independent savepoint."""

        if self._closed:
            raise DatabaseWrapperError("The database connection pool is closed")
        task_identifier = self._task_identifier()
        if self._current_writer_task_identifier == task_identifier:
            async with _transaction(self._writer):
                yield self._writer
            return

        async with self._writer_lock:
            self._current_writer_task_identifier = task_identifier
            try:
                async with _transaction(self._writer):
                    yield self._writer
            finally:
                self._current_writer_task_identifier = None

    @asynccontextmanager
    async def writer_maybe_transaction(self) -> AsyncGenerator[AsyncConnection]:
        """Join this task's writer transaction or create an outer transaction."""

        if self._current_writer_task_identifier == self._task_identifier():
            yield self._writer
            return
        async with self.writer() as connection:
            yield connection

    @asynccontextmanager
    async def reader(self) -> AsyncGenerator[AsyncConnection]:
        """Provide a stable read snapshot, nesting into the current transaction."""

        async with self.reader_no_transaction() as connection:
            if await connection.in_transaction:
                yield connection
            else:
                async with _transaction(connection):
                    yield connection

    @asynccontextmanager
    async def reader_no_transaction(self) -> AsyncGenerator[AsyncConnection]:
        """Provide a reader connection without starting a snapshot transaction."""

        task_identifier = self._task_identifier()
        if self._current_writer_task_identifier == task_identifier:
            yield self._writer
            return
        existing = self._reader_by_task_identifier.get(task_identifier)
        if existing is not None:
            yield existing
            return
        if self._closed:
            raise ReaderPoolClosedError("The database connection pool is closed")

        connection = await self._reader_receive.receive()
        self._reader_by_task_identifier[task_identifier] = connection
        try:
            yield connection
        finally:
            del self._reader_by_task_identifier[task_identifier]
            with anyio.fail_after(_CLEANUP_TIMEOUT_SECONDS, shield=True):
                await self._reader_send.send(connection)
