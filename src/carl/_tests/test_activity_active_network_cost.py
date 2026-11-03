"""The live network list must not revisit terminal request history."""

import json
from pathlib import Path
from typing import Any

import apsw
import pytest

from carl._tests.test_activity import _provenance
from carl.core.activity import ActivitySnapshot
from carl.core.components import Component, ComponentId
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
NOW = 10_000


@pytest.fixture(params=("asyncio", "trio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _seed_network(database: Database, include_active: bool) -> None:
    await database.begin_operation(
        operation_id="network-operation",
        component=Component(ComponentId(("test", "network")), 1, lambda: None),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-01-01T00:00:00+00:00",
    )
    rows = [(f"route-history-{index}", "completed", 1) for index in range(17)]
    if include_active:
        # An admitted request without a network_path scope does not belong in this list.
        rows.append(("no-path", "admitted", 1))
        rows.extend(
            (
                ("pending-early", "pending", 1),
                ("pending-late", "pending", 100),
                ("admitted-new", "admitted", 20),
                ("admitted-z", "admitted", 10),
                ("admitted-a", "admitted", 10),
            )
        )
    async with database._connections.writer() as connection:
        await connection.executemany(
            """INSERT INTO network_activities(
                id,kind_parts_json,operation_id,network_session_identifier,ordinal,attempt,state,
                eligible_at_utc_ns,created_at_utc_ns,admission_token,admitted_at_utc_ns,
                permit_expires_at_utc_ns,ended_at_utc_ns
            ) VALUES (?, '["test","network"]', 'network-operation', ?, 1, 1, ?, ?, ?, ?, ?, ?, ?)""",
            (
                (
                    identifier,
                    f"session-{identifier}",
                    state,
                    NOW + 100 if identifier == "pending-early" else 0,
                    created,
                    f"token-{identifier}" if state == "admitted" else None,
                    created + 1 if state == "admitted" else None,
                    (NOW - 1 if identifier == "admitted-a" else NOW + 100)
                    if state == "admitted"
                    else None,
                    1 if state == "completed" else None,
                )
                for identifier, state, created in rows
            ),
        )
        await connection.executemany(
            "INSERT INTO network_activity_scopes VALUES (?, ?, ?)",
            (
                (
                    identifier,
                    "overall" if identifier == "no-path" else "network_path",
                    json.dumps(
                        []
                        if identifier == "no-path"
                        else ["archive", identifier]
                        if state == "completed"
                        else ["z-live"]
                    ),
                )
                for identifier, state, _ in rows
            ),
        )
        if include_active:
            # Preserve multiple path rows for one request, even beyond the
            # separate historical path-summary display limit.
            await connection.execute(
                "INSERT INTO network_activity_scopes VALUES ('pending-late','network_path',?)",
                ('["z-second-path"]',),
            )


async def _live_list_cost(database: Database) -> tuple[ActivitySnapshot, int]:
    costs: list[int] = []

    def profile(event: dict[str, Any]) -> None:
        if " ".join(event["sql"].split()).startswith("SELECT substr(activity.id,1,161)"):
            costs.append(event["stmt_status"]["SQLITE_STMTSTATUS_VM_STEP"])

    # Measure the live-list statement in the real snapshot. All-time totals in
    # the same API intentionally still depend on history and are not this bound.
    async with database._connections.reader() as connection:
        await connection.trace_v2(apsw.SQLITE_TRACE_PROFILE, profile, id="live-network-cost")
        try:
            snapshot = await database.activity_snapshot(
                captured_at_utc_ns=NOW, recent_window_ns=1_000, maximum_rows=10
            )
        finally:
            await connection.trace_v2(0, None, id="live-network-cost")
    assert len(costs) == 1
    return snapshot, costs[0]


@pytest.mark.parametrize("include_active", (False, True), ids=("empty", "active"))
async def test_live_network_list_cost_excludes_terminal_history(
    tmp_path: Path, include_active: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed_network(database, include_active)
        before, before_steps = await _live_list_cost(database)
        if include_active:
            assert [request.identifier for request in before.active_network] == [
                "admitted-a",
                "admitted-z",
                "admitted-new",
                "pending-early",
                "pending-late",
                "pending-late",
            ]
            assert before.active_network[0].permit_expires_at_utc_ns == NOW - 1
            assert before.active_network[3].eligible_at_utc_ns == NOW + 100
            assert {request.path for request in before.active_network} == {
                ("z-live",),
                ("z-second-path",),
            }
            assert all(
                path.path not in {("z-live",), ("z-second-path",)} for path in before.network_paths
            )
            limited = await database.activity_snapshot(
                captured_at_utc_ns=NOW, recent_window_ns=1_000, maximum_rows=3
            )
            assert limited.active_network == before.active_network[:3]
        else:
            assert before.active_network == ()

        async with database._connections.writer() as connection:
            await connection.execute(
                """WITH RECURSIVE numbers(n) AS (
                    SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<10000
                ) INSERT INTO network_activities(
                    id,kind_parts_json,operation_id,network_session_identifier,ordinal,attempt,
                    state,eligible_at_utc_ns,created_at_utc_ns,ended_at_utc_ns
                ) SELECT 'archived-'||n, '["test","network"]', 'network-operation',
                    'archived-session', n, 1,
                    CASE n%4 WHEN 0 THEN 'completed' WHEN 1 THEN 'failed'
                        WHEN 2 THEN 'skipped' ELSE 'cancelled' END, 0, 1, 1
                FROM numbers"""
            )
            await connection.execute(
                """INSERT INTO network_activity_scopes
                SELECT id,'network_path','["archive"]' FROM network_activities
                WHERE id LIKE 'archived-%'"""
            )
        after, after_steps = await _live_list_cost(database)

    assert after.active_network == before.active_network
    assert after_steps <= before_steps + 2_000, (before_steps, after_steps)
