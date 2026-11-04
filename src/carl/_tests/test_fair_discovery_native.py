"""Native discovery preserves fair pages and reader ownership across cancellation."""

# Read-only callbacks use synthetic databases and inspect their actual pool boundary.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import threading
from pathlib import Path
from typing import TYPE_CHECKING, cast

import anyio
import apsw
import apsw.aio
import pytest

from carl._tests.test_fair_discovery_horizon import _enable, _work
from carl._tests.test_fair_storage import _seed
from carl._tests.test_stage_storage import CAPABILITY, NOW
from carl.core.fair_scheduling import FairDispatchCursor, FairLane
from carl.core.json import encode_json
from carl.io import fair_scheduling
from carl.io.db import DatabaseConnections, _configure_connection, _cooperative_busy_handler
from carl.io.sqlite import Database

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from apsw import AsyncConnection, Connection, SQLiteValue

pytestmark = pytest.mark.anyio
SCOPE = "test-host"
CAPABILITIES = encode_json(
    [[encode_json(list(CAPABILITY.kind)), CAPABILITY.payload_schema_version]]
)
type Send = Callable[[object, Callable[[], object]], Awaitable[object]]


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture(autouse=True, params=("auto", "anyio"))
async def _controller(request: pytest.FixtureRequest) -> AsyncIterator[None]:
    # CLI anyio.run selects AnyIO; pytest's backend-native runner selects Auto's
    # Trio/AsyncIO controllers. Exercise both without relying on stack detection.
    factory = apsw.aio.AnyIO if request.param == "anyio" else apsw.aio.Auto
    token = apsw.async_controller.set(cast("apsw.AsyncConnectionController", cast(object, factory)))
    try:
        yield
    finally:
        apsw.async_controller.reset(token)


async def _page(
    database: Database,
    *,
    after: FairDispatchCursor | None = None,
    eligible: tuple[str, ...] | None = None,
    excluded: tuple[str, ...] = (),
) -> tuple[tuple[str, ...], FairDispatchCursor | None, bool]:
    async with database._connections.reader() as connection:
        return await fair_scheduling.discover(
            connection,
            scope=SCOPE,
            now=NOW,
            limit=1,
            capabilities=CAPABILITIES,
            eligible=None if eligible is None else encode_json(list(eligible)),
            excluded=excluded,
            after=after,
        )


async def test_native_pages_preserve_lane_sequences_cursor_and_sparse_membership(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _work(database, "cpu-a", workspace="a", root="root-a")
        await _work(database, "http-b", workspace="b", root="root-b")
        await _work(database, "cpu-c", workspace="c", root="root-c")
        await _enable(database, cpu=1000, http=10)
        identifiers, cursor, more = await _page(database)
        assert identifiers == ("http-b",) and more and cursor is not None
        assert cursor.lane is FairLane.HTTP
        assert cursor.workspace_sequence == cursor.root_sequence == 10
        assert cursor.workspace_key == encode_json(["workspace", "b"])
        assert cursor.root_request_identifier == encode_json(["root", "root-b"])
        assert {lane.lane: lane.sequence for lane in cursor.horizon.lanes} == {
            FairLane.CPU: 1000,
            FairLane.HTTP: 10,
            FairLane.ANALYSIS: 0,
            FairLane.METADATA: 0,
            FairLane.RESIDENT: 0,
        }
        restored = FairDispatchCursor.model_validate_json(cursor.model_dump_json())
        identifiers, cursor, more = await _page(database, after=restored)
        assert identifiers == ("cpu-a",) and more and cursor is not None
        assert cursor.workspace_sequence == cursor.root_sequence == 1000
        identifiers, cursor, more = await _page(database, after=cursor)
        assert identifiers == ("cpu-c",) and more and cursor is not None
        assert await _page(database, after=cursor) == ((), None, False)
        assert (await _page(database, eligible=("cpu-c",)))[0] == ("cpu-c",)
        assert (await _page(database, excluded=("http-b", "cpu-a")))[0] == ("cpu-c",)
        assert await _page(database, eligible=()) == ((), None, False)


async def test_a_many_statement_page_uses_one_native_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handoffs = 0
    counting = False

    def wrap(original: Send) -> Send:
        async def send(active: object, call: Callable[[], object]) -> object:
            nonlocal handoffs
            if counting:
                handoffs += 1
            return await original(active, call)

        return send

    # Install before connection creation: APSW retains each bound send method.
    for controller in (apsw.aio.AnyIO, apsw.aio.Trio, apsw.aio.AsyncIO):
        monkeypatch.setattr(controller, "send", wrap(cast("Send", cast(object, controller.send))))
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for index in range(64):
            await _seed(database, f"job-{index:03d}", root="root", workspace="workspace")
        await database.enable_fair_scheduling(SCOPE, NOW)
        async with database._connections.reader() as connection:
            statements = 0
            threads: set[int] = set()

            def trace(_event: dict[str, object]) -> None:
                nonlocal statements
                statements += 1
                threads.add(threading.get_native_id())

            await connection.trace_v2(apsw.SQLITE_TRACE_STMT, trace, id="native-page")
            counting = True
            try:
                identifiers, cursor, more = await fair_scheduling.discover(
                    connection,
                    scope=SCOPE,
                    now=NOW + 31_000_000_000,
                    limit=32,
                    capabilities=CAPABILITIES,
                    eligible=None,
                    excluded=(),
                    after=None,
                )
                assert identifiers == tuple(f"job-{index:03d}" for index in range(32))
                assert cursor is not None and more
                assert handoffs == 1
                assert statements > 64
                assert len(threads) == 1 and threading.get_native_id() not in threads
            finally:
                counting = False
                await connection.trace_v2(0, None, id="native-page")


async def test_cancelled_callback_keeps_reader_owned_until_worker_drains(tmp_path: Path) -> None:
    async with DatabaseConnections.managed(tmp_path / "readers.sqlite3", reader_count=1) as pool:
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        contender_started, contender_entered = anyio.Event(), anyio.Event()
        scopes: list[anyio.CancelScope] = []

        def bounded_read(connection: Connection) -> list[tuple[SQLiteValue, ...]]:
            started.set()
            try:
                assert release.wait(2), "Test must release the bounded native callback"
                return connection.execute("SELECT 1").fetchall()
            finally:
                finished.set()

        async def owner() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                async with pool.reader() as connection:
                    await connection.async_run(
                        bounded_read, cast("Connection", cast(object, connection))
                    )

        async def contender() -> None:
            contender_started.set()
            async with pool.reader() as connection:
                contender_entered.set()
                assert finished.is_set()
                assert await (await connection.execute("SELECT 1")).fetchall() == [(1,)]

        async with anyio.create_task_group() as tasks:
            try:
                tasks.start_soon(owner)
                with anyio.fail_after(2):
                    while not started.is_set():
                        await anyio.sleep(0.001)
                scopes[0].cancel()
                tasks.start_soon(contender)
                await contender_started.wait()
                await anyio.sleep(0.02)
                assert not contender_entered.is_set()
            finally:
                release.set()
            with anyio.fail_after(2):
                await contender_entered.wait()


async def test_native_read_keeps_cooperative_busy_handler_on_the_event_loop(tmp_path: Path) -> None:
    writer = await apsw.Connection.as_async(str(tmp_path / "busy.sqlite3"))
    reader: AsyncConnection | None = None
    try:
        await writer.execute("CREATE TABLE counter(value INTEGER)")
        await writer.execute("INSERT INTO counter VALUES (7)")
        reader = await apsw.Connection.as_async(str(tmp_path / "busy.sqlite3"))
        await _configure_connection(reader, query_only=True, busy_timeout_ms=1000)
        busy = _cooperative_busy_handler(1000)
        entered = anyio.Event()
        callback_threads: set[int] = set()

        async def observed_busy(count: int) -> bool:
            callback_threads.add(threading.get_native_id())
            entered.set()
            return await busy(count)

        await reader.set_busy_handler(observed_busy)
        await writer.execute("BEGIN EXCLUSIVE")

        async def unlock() -> None:
            await entered.wait()
            await writer.execute("ROLLBACK")

        def read(connection: Connection) -> list[tuple[SQLiteValue, ...]]:
            return connection.execute("SELECT value FROM counter").fetchall()

        rows: list[tuple[SQLiteValue, ...]] = []
        with anyio.fail_after(2):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(unlock)
                rows = await reader.async_run(read, cast("Connection", cast(object, reader)))
        assert rows == [(7,)]
        assert callback_threads == {threading.get_native_id()}
    finally:
        if reader is not None:
            await reader.aclose()
        await writer.aclose()
