"""Workspace failure counts examine only work requested by that workspace."""

# pyright: reportPrivateUsage=false

import json
from pathlib import Path

import pytest

from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio

PARTIAL = '{"state":"completed_with_failures"}'


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _work(database: Database, identifier: str, state: str, result: str | None) -> None:
    leased = state in {"leased", "cancelling"}
    async with database._connections.writer() as connection:
        await connection.execute(
            """
            INSERT INTO work_items(
                id,kind_parts_json,payload_schema_version,payload_json,
                deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns,
                result_json,lease_token,lease_owner,lease_expires_at_utc_ns
            ) VALUES (?, '["test","work"]', 1, '{}', ?, ?, 0, 0, 0, ?, ?, ?, ?)
            """,
            (
                identifier,
                json.dumps([identifier]),
                state,
                result,
                f"lease-{identifier}" if leased else None,
                "owner" if leased else None,
                1_000 if leased else None,
            ),
        )


async def _request(
    database: Database,
    identifier: str,
    work_identifier: str,
    workspace: str,
    kind: tuple[str, ...] = ("test", "request"),
) -> None:
    async with database._connections.writer() as connection:
        await connection.execute(
            """
            INSERT INTO work_requests(
                id,work_item_id,requester_kind_parts_json,requester_identifier,
                requested_at_utc_ns,context_json
            ) VALUES (?, ?, ?, ?, 0, '{}')
            """,
            (identifier, work_identifier, json.dumps(kind), workspace),
        )


async def test_partial_failures_count_distinct_completed_work_across_requester_kinds(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier, state, result in (
            ("partial", "completed", PARTIAL),
            ("other-requester-kind", "completed", PARTIAL),
            ("successful", "completed", '{"state":"completed"}'),
            ("no-result", "completed", None),
            ("pending", "pending", PARTIAL),
            ("leased", "leased", PARTIAL),
            ("cancelling", "cancelling", PARTIAL),
            ("cancelled", "cancelled", PARTIAL),
            ("failed", "terminal_failure", PARTIAL),
            ("unrequested", "completed", PARTIAL),
            ("other-workspace", "completed", PARTIAL),
        ):
            await _work(database, identifier, state, result)
            if identifier not in {"unrequested", "other-workspace", "other-requester-kind"}:
                await _request(database, f"request-{identifier}", identifier, "workspace")
        await _request(database, "duplicate", "partial", "workspace")
        await _request(database, "different-kind", "partial", "workspace", ("other", "service"))
        await _request(
            database,
            "another-requester-kind",
            "other-requester-kind",
            "workspace",
            ("another", "service"),
        )
        await _request(database, "request-elsewhere", "other-workspace", "elsewhere")

        assert await database.workspace_partial_failure_count("workspace") == 2
        assert await database.workspace_partial_failure_count("elsewhere") == 1
        assert await database.workspace_partial_failure_count("missing") == 0


async def _cost(database: Database, workspace: str) -> tuple[int, int]:
    steps = 0

    def progress() -> bool:
        nonlocal steps
        steps += 1
        return False

    async with database._connections.reader() as connection:
        await connection.set_progress_handler(progress, nsteps=1, id="workspace-failure-cost")
        try:
            count = await database.workspace_partial_failure_count(workspace)
        finally:
            await connection.set_progress_handler(None, id="workspace-failure-cost")
    assert steps > 0
    return count, steps


async def test_workspace_failure_count_cost_excludes_unrelated_completed_history(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _work(database, "partial", "completed", PARTIAL)
        await _request(database, "request", "partial", "workspace")
        before, before_steps = await _cost(database, "workspace")
        missing_before, missing_before_steps = await _cost(database, "missing")

        async with database._connections.writer() as connection:
            await connection.execute(
                """
                WITH RECURSIVE numbers(n) AS (
                    SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<10000
                )
                INSERT INTO work_items(
                    id,kind_parts_json,payload_schema_version,payload_json,
                    deduplication_identity_json,state,priority,eligible_at_utc_ns,
                    created_at_utc_ns,result_json
                ) SELECT 'old-'||n, '["test","work"]', 1, '{}', json_array(n),
                         'completed', 0, 0, 0, ? FROM numbers
                """,
                (json.dumps({"state": "completed_with_failures", "details": "x" * 1_024}),),
            )
            await connection.execute(
                """
                WITH RECURSIVE numbers(n) AS (
                    SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<10000
                )
                INSERT INTO work_requests(
                    id,work_item_id,requester_kind_parts_json,requester_identifier,
                    requested_at_utc_ns,context_json
                ) SELECT 'old-request-'||n, 'old-'||n, '["test","request"]',
                         'elsewhere', 0, '{}' FROM numbers
                """
            )
        after, after_steps = await _cost(database, "workspace")
        missing_after, missing_after_steps = await _cost(database, "missing")

    assert before == after == 1
    assert missing_before == missing_after == 0
    assert after_steps <= before_steps + 500, (before_steps, after_steps)
    assert missing_after_steps <= missing_before_steps + 500, (
        missing_before_steps,
        missing_after_steps,
    )
