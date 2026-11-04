"""Batched claim checks preserve wait precedence and writer ownership."""

# Inspect the real admission ledger and controller boundary.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import threading
from pathlib import Path
from typing import TYPE_CHECKING, cast

import anyio
import apsw.aio
import pytest
from pydantic import ValidationError

from carl._tests.test_fair_discovery_native import _controller as _controller
from carl._tests.test_fair_discovery_native import anyio_backend as anyio_backend
from carl._tests.test_stage_storage import DURATION, NOW, _claim, _definition, _enqueue
from carl.core.resources import ResourceRequest
from carl.core.stages import StageWaitKind
from carl.io import stage_claim
from carl.io.sqlite import Database

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from apsw import Connection

pytestmark = pytest.mark.anyio
type Send = Callable[[object, Callable[[], object]], Awaitable[object]]


async def _pause_network(database: Database) -> None:
    async with database._connections.writer() as connection:
        await connection.execute(
            "INSERT INTO network_connectivity(singleton,paused,next_probe_utc_ns) VALUES (1,1,?) "
            "ON CONFLICT(singleton) DO UPDATE SET paused=1,next_probe_utc_ns=excluded.next_probe_utc_ns",
            (NOW + DURATION,),
        )


@pytest.mark.parametrize(
    ("case", "kind", "deadline", "persisted"),
    (
        ("nonpending", StageWaitKind.OWNERSHIP, NOW + 100_000_000, False),
        ("progress", StageWaitKind.DEPENDENCY, None, False),
        ("terminal_work", StageWaitKind.OWNERSHIP, None, False),
        ("work_time", StageWaitKind.TIME, NOW + DURATION, True),
        ("stage_time", StageWaitKind.TIME, NOW + 2 * DURATION, True),
        ("dependency", StageWaitKind.DEPENDENCY, NOW + 100_000_000, True),
        ("outage", StageWaitKind.CONNECTIVITY, NOW + DURATION, True),
        ("outage_resource", StageWaitKind.CONNECTIVITY, NOW + DURATION, True),
    ),
)
async def test_public_claim_preserves_wait_precedence_and_ledger_effects(
    tmp_path: Path,
    case: str,
    kind: StageWaitKind,
    deadline: int | None,
    persisted: bool,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await _enqueue(database, "dependency")
        definition = _definition(
            network=case != "outage_resource",
            resources=(ResourceRequest(name="http"),),
            dependencies=() if case.startswith("outage") else ("dependency",),
            not_before=NOW + 2 * DURATION if case == "stage_time" else 0,
        )
        await database.register_stage(definition, created_at_utc_ns=NOW)
        await _pause_network(database)
        async with database._connections.writer() as connection:
            if case in {"nonpending", "progress", "terminal_work"}:
                await connection.execute("UPDATE work_items SET state='completed' WHERE id='work'")
                await connection.execute(
                    "UPDATE work_stages SET progress_waiting=1 WHERE id='stage'"
                )
            if case == "nonpending":
                await connection.execute("UPDATE work_stages SET state='completed'")
            elif case == "terminal_work":
                await connection.execute("UPDATE work_stages SET progress_waiting=0")
            elif case in {"work_time", "stage_time"}:
                await connection.execute(
                    "UPDATE work_items SET eligible_at_utc_ns=? WHERE id='work'",
                    (NOW + DURATION,),
                )

        result = await _claim(database)
        assert result.lease is None and result.wait_kind is kind
        assert result.next_eligible_at_utc_ns == deadline
        async with database._connections.reader() as connection:
            stage = await (
                await connection.execute(
                    "SELECT wait_kind,wait_entered_at_utc_ns,eligible_at_utc_ns,execution "
                    "FROM work_stages WHERE id='stage'"
                )
            ).fetchone()
            assert stage is not None
            assert stage[0] == (kind.value if persisted else None)
            assert stage[1] == (NOW if persisted else None)
            assert stage[2] == (deadline if persisted else definition.not_before_utc_ns)
            assert stage[3] == 0
            assert await (
                await connection.execute("SELECT state,attempt FROM stage_attempts")
            ).fetchall() == [("pending", None)]
            assert await (
                await connection.execute("SELECT attempt FROM work_items WHERE id='work'")
            ).fetchone() == (0,)
            for table in ("operations", "resource_permits", "stage_execution_tokens"):
                assert await (
                    await connection.execute(f"SELECT count(*) FROM {table}")
                ).fetchone() == (0,)


async def test_public_cpu_claim_bypasses_paused_network(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(_definition(), created_at_utc_ns=NOW)
        await _pause_network(database)
        result = await _claim(database)
        assert result.lease is not None
        assert result.lease.resource_lease is not None
        assert result.lease.network_admission is None


@pytest.mark.parametrize("case", ("missing", "invalid_nonpending"))
async def test_public_claim_preserves_missing_and_invalid_definition_errors(
    tmp_path: Path, case: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        if case == "missing":
            with pytest.raises(KeyError, match="stage"):
                await _claim(database)
        else:
            await _enqueue(database, "work")
            await database.register_stage(_definition(), created_at_utc_ns=NOW)
            async with database._connections.writer() as connection:
                await connection.execute(
                    "UPDATE work_stages SET state='completed',definition_json='{}'"
                )
            with pytest.raises(ValidationError):
                await _claim(database)


async def test_preflight_uses_one_real_handoff_for_all_reads(
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
        await _enqueue(database, "work")
        definition = _definition(network=True, resources=(ResourceRequest(name="http"),))
        await database.register_stage(definition, created_at_utc_ns=NOW)
        async with database._connections.writer() as connection:
            statements = 0
            threads: set[int] = set()

            def trace(_event: dict[str, object]) -> None:
                nonlocal statements
                statements += 1
                threads.add(threading.get_native_id())

            await connection.trace_v2(apsw.SQLITE_TRACE_STMT, trace, id="claim-preflight")
            counting = True
            try:
                facts = await connection.async_run(
                    stage_claim.preflight,
                    cast("Connection", cast(object, connection)),
                    stage_identifier=definition.identifier,
                    now_utc_ns=NOW,
                )
                assert facts.definition == definition and facts.wait is None
                assert facts.work is not None and facts.network_stage
                assert handoffs == 1 and statements == 4
                assert len(threads) == 1 and threading.get_native_id() not in threads
            finally:
                counting = False
                await connection.trace_v2(0, None, id="claim-preflight")
        assert (await _claim(database)).lease is not None


async def test_cancelled_public_claim_retains_writer_until_preflight_drains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(_definition(), created_at_utc_ns=NOW)
        original = stage_claim.preflight
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        contender_started, contender_entered = anyio.Event(), anyio.Event()
        scopes: list[anyio.CancelScope] = []

        def preflight(
            connection: Connection, *, stage_identifier: str, now_utc_ns: int
        ) -> stage_claim.Preflight:
            started.set()
            try:
                assert release.wait(2), "Test must release its bounded native callback"
                return original(
                    connection, stage_identifier=stage_identifier, now_utc_ns=now_utc_ns
                )
            finally:
                finished.set()

        monkeypatch.setattr(stage_claim, "preflight", preflight)

        async def owner() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                await _claim(database)

        async def contender() -> None:
            contender_started.set()
            async with database._connections.writer() as connection:
                assert finished.is_set()
                assert await (
                    await connection.execute("SELECT state,attempt FROM work_items")
                ).fetchall() == [("pending", 0)]
                assert await (
                    await connection.execute("SELECT count(*) FROM resource_permits")
                ).fetchone() == (0,)
                contender_entered.set()

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
