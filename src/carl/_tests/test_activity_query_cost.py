"""Recent activity queries must not reread archived work events on every refresh."""

from pathlib import Path

import pytest

from carl.core.activity import ActivitySnapshot
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio

NOW = 10_000
WINDOW = 1_000
SINCE = NOW - WINDOW


@pytest.fixture(params=("asyncio", "trio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _seed_recent_activity(database: Database) -> None:
    """Retain fixed work metadata and terminal events with differing time/sequence order."""
    async with database._connections.writer() as connection:
        await connection.executemany(
            """
            INSERT INTO work_items(
                id,kind_parts_json,payload_schema_version,payload_json,
                deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns
            ) VALUES (?, ?, 1, '{}', ?, ?, 0, 0, 1)
            """,
            (
                ("finished", '["test","finished"]', '["finished"]', "completed"),
                ("failed", '["test","failed"]', '["failed"]', "terminal_failure"),
                ("cancelled", '["test","cancelled"]', '["cancelled"]', "cancelled"),
                ("pending", '["test","pending"]', '["pending"]', "pending"),
            ),
        )
        await connection.executemany(
            """
            INSERT INTO work_events(id,work_item_id,event_kind,recorded_at_utc_ns,data_json)
            VALUES (?, ?, ?, ?, '{}')
            """,
            (
                ("just-before", "finished", "completed", SINCE - 1),
                ("at-cutoff", "finished", "completed", SINCE),
                ("failure", "failed", "terminal_failure", NOW - 1),
                ("duplicate-completion", "finished", "completed", SINCE + 2),
                ("cancellation", "cancelled", "cancelled", SINCE + 1),
                ("not-terminal", "pending", "released", NOW),
            ),
        )


async def _snapshot_cost(database: Database) -> tuple[ActivitySnapshot, int]:
    steps = 0

    def progress() -> bool:
        nonlocal steps
        steps += 100
        return False

    # Nested reader acquisition reuses this connection, measuring the actual public
    # snapshot rather than a duplicate query or an EXPLAIN-only approximation.
    async with database._connections.reader() as connection:
        await connection.set_progress_handler(progress, nsteps=100, id="activity-history-cost")
        try:
            snapshot = await database.activity_snapshot(
                captured_at_utc_ns=NOW, recent_window_ns=WINDOW, maximum_rows=10
            )
        finally:
            await connection.set_progress_handler(None, id="activity-history-cost")
    assert steps > 0
    return snapshot, steps


async def test_activity_snapshot_preserves_recent_event_counts_and_sequence_order(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed_recent_activity(database)
        snapshot, _ = await _snapshot_cost(database)
        limited = await database.activity_snapshot(
            captured_at_utc_ns=NOW, recent_window_ns=WINDOW, maximum_rows=3
        )

    by_kind = {activity.kind: activity for activity in snapshot.work_kinds}
    # The cutoff is inclusive and counters count events, not distinct work IDs.
    assert by_kind[("test", "finished")].recent_completed == 2
    assert by_kind[("test", "finished")].completed == 1
    assert by_kind[("test", "failed")].recent_terminal_failure == 1
    assert by_kind[("test", "cancelled")].recent_cancelled == 1
    assert by_kind[("test", "pending")].recent_completed == 0
    assert [work.identifier for work in snapshot.recent_terminal_work] == [
        "cancelled",
        "finished",
        "failed",
        "finished",
    ]
    assert [work.terminal_at_utc_ns for work in snapshot.recent_terminal_work] == [
        SINCE + 1,
        SINCE + 2,
        NOW - 1,
        SINCE,
    ]
    assert limited.recent_terminal_work == snapshot.recent_terminal_work[:3]


@pytest.mark.parametrize("old_event_kind", ("completed", "lease_renewed"))
async def test_activity_snapshot_cost_excludes_old_event_history(
    tmp_path: Path, old_event_kind: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed_recent_activity(database)
        before, before_steps = await _snapshot_cost(database)
        async with database._connections.writer() as connection:
            await connection.execute(
                """
                WITH RECURSIVE numbers(n) AS (
                    SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<10000
                )
                INSERT INTO work_events(id,work_item_id,event_kind,recorded_at_utc_ns,data_json)
                SELECT 'old-'||n, 'finished', ?, 1, '{}' FROM numbers
                """,
                (old_event_kind,),
            )
        after, after_steps = await _snapshot_cost(database)

    # Work metadata stays fixed: this tests archive-event cost without claiming
    # that exact all-time work/network totals are independent of their own history.
    assert after == before
    assert after_steps <= before_steps + 2_000, (before_steps, after_steps)
