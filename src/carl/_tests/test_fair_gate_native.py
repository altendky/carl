"""One native gate retains live fairness, async compatibility and transaction fences."""

# Native callbacks use isolated ledgers; inspect the actual writer/bridge boundary.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import threading
from pathlib import Path
from typing import TYPE_CHECKING, cast

import anyio
import apsw.aio
import pytest

from carl._tests.test_fair_discovery_native import _controller as _controller
from carl._tests.test_fair_discovery_native import anyio_backend as anyio_backend
from carl._tests.test_fair_storage import _seed
from carl._tests.test_stage_storage import CAPACITIES, NOW
from carl.core.fair_scheduling import FairLane
from carl.core.stages import StageDefinition
from carl.io import fair_gate, fair_scheduling
from carl.io.sqlite import Database

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from apsw import AsyncConnection, Connection

pytestmark = pytest.mark.anyio
SCOPE = "test-host"
type Send = Callable[[object, Callable[[], object]], Awaitable[object]]


async def _gate(
    database: Database, connection: AsyncConnection, stage: StageDefinition
) -> tuple[bool, FairLane | None]:
    return await database._fair_gate(
        connection,
        scope=SCOPE,
        work_identifier=stage.work_item_identifier,
        stage_identifier=stage.identifier,
        signature=stage.model_dump_json(),
        resources=stage.resources,
        capacities=CAPACITIES,
        now_utc_ns=NOW,
    )


def _nothing() -> None:
    pass


def _intercept_finish(
    monkeypatch: pytest.MonkeyPatch,
    *,
    before: Callable[[], None] = _nothing,
    after: Callable[[], None] = _nothing,
) -> None:
    original = fair_gate.finish_admission

    def finish(
        connection: Connection,
        facts: fair_gate.AdmissionFacts,
        eligible_work: frozenset[str],
        *,
        scope: str,
        lane: FairLane,
        work_identifier: str,
        capacities: dict[str, int],
        now: int,
    ) -> bool:
        before()
        try:
            return original(
                connection,
                facts,
                eligible_work,
                scope=scope,
                lane=lane,
                work_identifier=work_identifier,
                capacities=capacities,
                now=now,
            )
        finally:
            after()

    monkeypatch.setattr(fair_gate, "finish_admission", finish)


async def test_live_gate_uses_one_handoff_and_async_callback_compatibility(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handoffs = 0
    counting = False

    def wrap(original: Send) -> Send:
        async def send(controller: object, call: Callable[[], object]) -> object:
            nonlocal handoffs
            if counting:
                handoffs += 1
            return await original(controller, call)

        return send

    for controller in (apsw.aio.AnyIO, apsw.aio.Trio, apsw.aio.AsyncIO):
        monkeypatch.setattr(controller, "send", wrap(cast("Send", cast(object, controller.send))))
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        first = await _seed(database, "first", root="A")
        second = await _seed(database, "second", root="B")
        await database.enable_fair_scheduling(SCOPE, NOW)
        await database.offer_fair_dispatch_candidates(
            scope=SCOPE, work_identifiers=("first", "second"), now_utc_ns=NOW
        )
        async with database._connections.writer() as connection:
            statements = 0
            threads: set[int] = set()

            def trace(_event: dict[str, object]) -> None:
                nonlocal statements
                statements += 1
                threads.add(threading.get_native_id())

            await connection.trace_v2(apsw.SQLITE_TRACE_STMT, trace, id="native-gate")
            counting = True
            try:
                assert await _gate(database, connection, second) == (False, FairLane.CPU)
                assert handoffs == 1 and statements >= 10
                assert len(threads) == 1 and threading.get_native_id() not in threads
            finally:
                counting = False
                await connection.trace_v2(0, None, id="native-gate")

            async def eligible(
                offers: tuple[tuple[str, str | None], ...], lane: FairLane
            ) -> frozenset[str]:
                assert offers == (("first", first.identifier), ("second", second.identifier))
                return await database._fair_offers_eligible(
                    connection, scope=SCOPE, offers=offers, lane=lane, now_utc_ns=NOW
                )

            assert not await fair_scheduling.admission_allowed(
                connection,
                scope=SCOPE,
                lane=FairLane.CPU,
                work_identifier="second",
                capacities=CAPACITIES,
                now=NOW,
                eligible=eligible,
            )
            assert await _gate(database, connection, first) == (True, FairLane.CPU)
        assert await database.fair_admissions(SCOPE) == ()


async def test_partial_native_failure_rolls_back_offer_cleanup_and_cpu_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database, "stale", root="A")
        target = await _seed(database, "target", root="B")
        await database.enable_fair_scheduling(SCOPE, NOW)
        await database.offer_fair_dispatch_candidates(
            scope=SCOPE, work_identifiers=("stale",), now_utc_ns=NOW
        )
        async with database._connections.writer() as connection:
            await connection.execute("UPDATE fair_offers SET signature='stale'")

        def fail() -> None:
            raise ValueError("injected after native gate mutations")

        _intercept_finish(monkeypatch, after=fail)
        with pytest.raises(ValueError, match="injected"):
            async with database._connections.writer() as connection:
                await _gate(database, connection, target)
        async with database._connections.reader() as connection:
            assert await (
                await connection.execute("SELECT work_item_id,signature FROM fair_offers")
            ).fetchall() == [("stale", "stale")]
            assert (
                await (await connection.execute("SELECT * FROM fair_cpu_drains")).fetchall() == []
            )
        assert await database.fair_admissions(SCOPE) == ()


async def test_cancelled_native_gate_retains_writer_until_callback_drains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _seed(database, "target", root="root")
        await database.enable_fair_scheduling(SCOPE, NOW)
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        contender_started, contender_entered = anyio.Event(), anyio.Event()
        scopes: list[anyio.CancelScope] = []

        def block() -> None:
            started.set()
            assert release.wait(2), "Test must release its bounded native callback"

        _intercept_finish(monkeypatch, before=block, after=finished.set)

        async def owner() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                async with database._connections.writer() as connection:
                    await _gate(database, connection, stage)

        async def contender() -> None:
            contender_started.set()
            async with database._connections.writer() as connection:
                assert finished.is_set()
                contender_entered.set()
                assert (
                    await (await connection.execute("SELECT * FROM fair_offers")).fetchall() == []
                )
                assert (
                    await (await connection.execute("SELECT * FROM fair_cpu_drains")).fetchall()
                    == []
                )

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
