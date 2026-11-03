"""Progress wakes touch their exact continuation independently of queue history."""

# Cost assertions inspect the genuine storage API's writer, not a substitute query.
# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl._tests.test_stage_storage import NOW, _claim, _definition
from carl._tests.test_work_progress_storage import GROUP, _checkpoint, _setup_shared
from carl.core.stages import StageDefinition, StageWaitKind, WaitWork
from carl.core.work_progress import ProgressWait, WorkProgressWatch
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _arm(
    database: Database,
    source: str,
    *,
    cycle: int,
    group: bool,
    not_before: int = 0,
) -> StageDefinition:
    baseline = await database.work_progress_watermark()
    claim = await _claim(database, source, token=f"parent-{cycle}", now=NOW + cycle * 100)
    assert claim.lease is not None
    continuation = _definition(
        "parent-00", identifier=f"waiting-{cycle}", resources=(), not_before=not_before
    )
    await database.suspend_stage(
        claim.lease,
        WaitWork(
            continuation=continuation,
            wait_kind=StageWaitKind.DEPENDENCY,
            result={"waiting": cycle},
            progress_wait=ProgressWait(
                observed_through_revision=baseline,
                watch=WorkProgressWatch(
                    consumer_requester_kinds=(GROUP,) if group else (),
                    work_identifiers=() if group else ("child",),
                ),
            ),
        ),
        now_utc_ns=NOW + cycle * 100 + 1,
    )
    return continuation


async def _drain(database: Database, now: int) -> tuple[int, int]:
    steps = 0
    woke = 0

    def count() -> bool:
        nonlocal steps
        steps += 100
        return False

    connection = database._connections._writer
    await connection.set_progress_handler(count, nsteps=100, id="progress-wake-budget")
    try:
        for _ in range(20):
            result = await database.drain_progress_notifications(now_utc_ns=now)
            woke += result.woke
            if not result.more:
                return steps, woke
        raise AssertionError("The fixed progress sources did not drain")
    finally:
        await connection.set_progress_handler(None, id="progress-wake-budget")


async def _snapshot(database: Database, stage: str) -> tuple[object, ...]:
    async with database._connections.reader() as connection:
        row = await (
            await connection.execute(
                """SELECT s.state,s.progress_waiting,s.wait_kind,f.active,
                          f.ready_since_utc_ns,f.stage_id
                FROM work_stages s JOIN fair_ready f ON f.work_item_id=s.work_item_id
                WHERE s.id=?""",
                (stage,),
            )
        ).fetchone()
    assert row is not None
    return tuple(row)


async def _history(database: Database, start: int, count: int, *, pending: bool) -> None:
    state = "pending" if pending else "completed"
    async with database._connections.writer() as connection:
        await connection.execute(
            """WITH RECURSIVE numbers(n) AS (
                SELECT ? UNION ALL SELECT n+1 FROM numbers WHERE n<?
            ) INSERT INTO work_items(
                id,kind_parts_json,payload_schema_version,payload_json,
                deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns
            ) SELECT ?||n,'["test","unrelated"]',1,'{}',json_array(?||n),?,0,0,0
              FROM numbers""",
            (start, start + count - 1, f"history-{state}-", f"history-{state}-", state),
        )
        if not pending:
            return
        # Unrelated pending continuations are valid metadata peers, with their
        # own attempt keys. They must not expand a consumer group's lookup.
        await connection.execute(
            """WITH RECURSIVE numbers(n) AS (
                SELECT ? UNION ALL SELECT n+1 FROM numbers WHERE n<?
            ) INSERT INTO stage_attempts
            SELECT 'history-operation-'||n,'history-pending-'||n,NULL,'pending',0
            FROM numbers""",
            (start, start + count - 1),
        )
        await connection.execute(
            """WITH RECURSIVE numbers(n) AS (
                SELECT ? UNION ALL SELECT n+1 FROM numbers WHERE n<?
            ) INSERT INTO work_stages(
                id,work_item_id,operation_id,definition_json,state,
                eligible_at_utc_ns,created_at_utc_ns
            ) SELECT 'history-stage-'||n,'history-pending-'||n,'history-operation-'||n,
                json_object('identifier','history-stage-'||n,
                    'work_item_identifier','history-pending-'||n,
                    'operation_identifier','history-operation-'||n,
                    'kind',json('["test","unrelated"]'),'payload',json('{}')),
                'pending',0,0 FROM numbers""",
            (start, start + count - 1),
        )


@pytest.mark.parametrize("group", (False, True))
async def test_progress_wake_cost_ignores_unrelated_history_and_pending_stages(
    tmp_path: Path, group: bool
) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        child, _ = await _setup_shared(database, 1)
        source = "stage-parent-00"
        costs = []
        for cycle, start, count in ((0, 0, 64), (1, 64, 10_000)):
            await _history(database, start, count, pending=False)
            if group:
                await _history(database, start, count, pending=True)
            deadline = NOW + cycle * 100 + 50
            waiting = await _arm(database, source, cycle=cycle, group=group, not_before=deadline)
            assert (await _snapshot(database, waiting.identifier))[:4] == (
                "pending",
                1,
                "dependency",
                0,
            )
            # Clear unrelated publication notices before measuring this child.
            assert (await _drain(database, NOW + cycle * 100 + 5))[1] == 0
            await _checkpoint(database, child, {"pages": cycle + 1})
            steps, woke = await _drain(database, NOW + cycle * 100 + 20)
            assert woke == 1
            assert await _snapshot(database, waiting.identifier) == (
                "pending",
                0,
                None,
                1,
                deadline,
                waiting.identifier,
            )
            costs.append(steps)
            source = waiting.identifier
        assert costs[1] <= costs[0] + 1_000, costs
        resumed = await _claim(database, source, token="final-resume", now=NOW + 200)
        assert resumed.lease is not None
        assert resumed.lease.work_lease.attempt == 1
        assert resumed.lease.definition.operation_identifier == "operation-parent-00"
        async with database._connections.reader() as connection:
            assert await (await connection.execute("PRAGMA foreign_key_check")).fetchall() == []


@pytest.mark.parametrize("group", (False, True))
async def test_stale_notifications_and_cancelled_continuations_do_not_reset_readiness(
    tmp_path: Path, group: bool
) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        child, _ = await _setup_shared(database, 1)
        first = await _arm(database, "stage-parent-00", cycle=0, group=group)
        assert (await _drain(database, NOW + 5))[1] == 0
        await _checkpoint(database, child, {"pages": 1})
        assert (await _drain(database, NOW + 20))[1] == 1

        # The next watch's baseline includes this revision even though its
        # notification has not drained. Old membership must not wake the new
        # continuation or rewrite its readiness age.
        await _checkpoint(database, child, {"pages": 2})
        second = await _arm(database, first.identifier, cycle=1, group=group)
        before = await _snapshot(database, second.identifier)
        assert before[:4] == ("pending", 1, "dependency", 0)
        assert (await _drain(database, NOW + 120))[1] == 0
        assert await _snapshot(database, second.identifier) == before

        await database.cancel_work(
            "parent-00",
            reason="fixture cancellation",
            caller="test",
            requested_at_utc_ns=NOW + 121,
            include_descendants=False,
        )
        cancelled = await _snapshot(database, second.identifier)
        assert cancelled[0] == "cancelled" and cancelled[3] == 0
        await _checkpoint(database, child, {"pages": 3})
        assert (await _drain(database, NOW + 140))[1] == 0
        assert await _snapshot(database, second.identifier) == cancelled
