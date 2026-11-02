"""Real SQLite contention yields to lock owners without changing transaction fences."""

# Observe actual busy callback entry and transaction state at ownership boundaries.
# pyright: reportPrivateUsage=false

import subprocess
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

import anyio
import apsw
import pytest

import carl.io.db as db_module
from carl.io.db import DatabaseConnections

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _observe_busy(monkeypatch: pytest.MonkeyPatch, events: list[str]) -> anyio.Event:
    observed = anyio.Event()
    original = db_module._cooperative_busy_handler

    def factory(timeout_ms: int) -> Callable[[int], Awaitable[bool]]:
        handler = original(timeout_ms)

        async def busy(count: int) -> bool:
            if count == 0:
                events.append("sqlite_busy")
                observed.set()
            return await handler(count)

        return busy

    monkeypatch.setattr(db_module, "_cooperative_busy_handler", factory)
    return observed


async def _create(database: DatabaseConnections) -> None:
    async with database.writer() as writer:
        await writer.execute("CREATE TABLE counter(value INTEGER NOT NULL) STRICT")


async def _values(database: DatabaseConnections) -> list[int]:
    async with database.reader() as reader:
        return [
            row[0] async for row in await reader.execute("SELECT value FROM counter ORDER BY value")
        ]


async def test_two_real_wrappers_owner_progresses_during_observed_busy_and_both_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    busy = _observe_busy(monkeypatch, events)
    path = tmp_path / "carl.sqlite3"
    async with (
        DatabaseConnections.managed(path, reader_count=1, busy_timeout_ms=2000) as first,
        DatabaseConnections.managed(path, reader_count=1, busy_timeout_ms=2000) as second,
    ):
        await _create(first)
        completed = anyio.Event()

        async def contender() -> None:
            async with second.writer() as writer:
                events.append("contender_admitted")
                await writer.execute("INSERT INTO counter VALUES (2)")
            completed.set()

        with anyio.fail_after(10):
            async with anyio.create_task_group() as tasks:
                async with first.writer() as writer:
                    tasks.start_soon(contender)
                    await busy.wait()
                    assert not completed.is_set()
                    await writer.execute("INSERT INTO counter VALUES (1)")
                    events.append("owner_mutated")
                await completed.wait()
        assert (
            events.index("sqlite_busy")
            < events.index("owner_mutated")
            < events.index("contender_admitted")
        )
        assert await _values(first) == [1, 2]
        assert not await first._writer.in_transaction and not await second._writer.in_transaction


async def test_cancel_waiter_preserves_shielded_handoff_then_rolls_back_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    busy = _observe_busy(monkeypatch, events)
    path = tmp_path / "carl.sqlite3"
    async with (
        DatabaseConnections.managed(path, reader_count=1, busy_timeout_ms=2000) as first,
        DatabaseConnections.managed(path, reader_count=1, busy_timeout_ms=2000) as second,
    ):
        await _create(first)
        done = anyio.Event()
        scopes: list[anyio.CancelScope] = []

        async def cancelled() -> None:
            try:
                with anyio.CancelScope() as scope:
                    scopes.append(scope)
                    async with second.writer() as writer:
                        await writer.execute("INSERT INTO counter VALUES (99)")
                        await anyio.lowlevel.checkpoint()
            finally:
                done.set()

        with anyio.fail_after(10):
            async with anyio.create_task_group() as tasks:
                async with first.writer() as writer:
                    tasks.start_soon(cancelled)
                    await busy.wait()
                    scopes[0].cancel()
                    await anyio.lowlevel.checkpoint()
                    # Admission is deliberately shielded: cancellation does not
                    # abandon a transaction that SQLite may still grant.
                    assert not done.is_set()
                    await writer.execute("INSERT INTO counter VALUES (1)")
                await done.wait()
        assert scopes[0].cancel_called
        assert not await second._writer.in_transaction
        assert await _values(first) == [1]
        async with second.writer() as writer:
            await writer.execute("INSERT INTO counter VALUES (3)")
        assert await _values(second) == [1, 3]


async def test_nested_and_outer_rollback_do_not_rollback_contending_wrapper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    busy = _observe_busy(monkeypatch, events)
    path = tmp_path / "carl.sqlite3"
    async with (
        DatabaseConnections.managed(path, reader_count=1, busy_timeout_ms=2000) as first,
        DatabaseConnections.managed(path, reader_count=1, busy_timeout_ms=2000) as second,
    ):
        await _create(first)
        async with first.writer() as writer:
            await writer.execute("INSERT INTO counter VALUES (0)")
        done = anyio.Event()

        async def contender() -> None:
            async with second.writer() as writer:
                await writer.execute("INSERT INTO counter VALUES (3)")
            done.set()

        with anyio.fail_after(10):
            async with anyio.create_task_group() as tasks:
                with pytest.raises(RuntimeError, match="outer rollback"):
                    async with first.writer() as writer:
                        await writer.execute("INSERT INTO counter VALUES (1)")
                        with pytest.raises(ValueError, match="nested rollback"):
                            async with first.writer() as nested:
                                assert nested is writer
                                await nested.execute("INSERT INTO counter VALUES (2)")
                                raise ValueError("nested rollback")
                        assert [
                            row[0]
                            async for row in await writer.execute(
                                "SELECT value FROM counter ORDER BY value"
                            )
                        ] == [0, 1]
                        tasks.start_soon(contender)
                        await busy.wait()
                        raise RuntimeError("outer rollback")
                await done.wait()
        assert await _values(first) == [0, 3]
        assert not await first._writer.in_transaction and not await second._writer.in_transaction


async def test_external_writer_timeout_keeps_other_database_and_loop_responsive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    busy = _observe_busy(monkeypatch, events)
    path = tmp_path / "locked.sqlite3"
    async with (
        DatabaseConnections.managed(path, reader_count=1, busy_timeout_ms=1000) as locked,
        DatabaseConnections.managed(tmp_path / "unrelated.sqlite3", reader_count=1) as unrelated,
    ):
        await _create(locked)
        await _create(unrelated)
        code = "import sqlite3,sys; connection=sqlite3.connect(sys.argv[1]); connection.execute('BEGIN IMMEDIATE'); connection.execute('INSERT INTO counter VALUES (99)'); print('locked',flush=True); sys.stdin.readline(); connection.rollback(); connection.close()"
        process = await anyio.open_process(
            (sys.executable, "-u", "-c", code, str(path)),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        done = anyio.Event()

        async def contender() -> None:
            with pytest.raises(apsw.BusyError):
                async with locked.writer():
                    pytest.fail("The external process still owns SQLite's writer lock")
            events.append("contender_timeout")
            done.set()

        try:
            assert process.stdout is not None
            with anyio.fail_after(10):
                ready = bytearray()
                while not ready.endswith(b"\n"):
                    assert len(ready) < 64
                    ready.extend(await process.stdout.receive(64 - len(ready)))
                assert ready == b"locked\n"
                async with anyio.create_task_group() as tasks:
                    tasks.start_soon(contender)
                    await busy.wait()
                    async with unrelated.writer() as writer:
                        await writer.execute("INSERT INTO counter VALUES (7)")
                    events.append("unrelated_committed")
                    assert await _values(unrelated) == [7]
                    await done.wait()
            assert events.index("unrelated_committed") < events.index("contender_timeout")
            assert not await locked._writer.in_transaction
        finally:
            with anyio.CancelScope(shield=True):
                with anyio.move_on_after(2):
                    if process.stdin is not None:
                        await process.stdin.send(b"release\n")
                    await process.wait()
                if process.returncode is None:
                    process.kill()
                    await process.wait()
                await process.aclose()
        assert process.returncode == 0
        assert await _values(locked) == []
        async with locked.writer() as writer:
            await writer.execute("INSERT INTO counter VALUES (8)")
        assert await _values(locked) == [8]
