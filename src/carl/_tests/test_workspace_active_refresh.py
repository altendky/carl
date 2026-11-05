"""Workspace refresh reuse reads durable track edges inside the queue transaction."""

# Offline storage assertions seed historical work states directly.
# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl.core.json import encode_json
from carl.core.models import JsonValue
from carl.core.work import WorkState
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


async def _seed(
    database: Database,
    identifier: str,
    *,
    state: WorkState = WorkState.PENDING,
    marketplace: str = "facebook",
    kind: tuple[str, ...] | None = None,
    requester_kind: tuple[str, ...] = ("carl", "mcp", "request_workspace_refresh"),
    workspace: str = "workspace",
    track: str = "track",
    base: JsonValue = "original-run",
    created_at: int = 1,
) -> None:
    work_kind = kind or ("carl", marketplace, "work", "refresh_search")
    leased = state in {WorkState.LEASED, WorkState.CANCELLING}
    async with database._connections.writer() as connection:
        _ = await connection.execute(
            """
            INSERT INTO work_items (
                id, kind_parts_json, payload_schema_version, payload_json,
                deduplication_identity_json, state, priority, eligible_at_utc_ns,
                created_at_utc_ns, lease_token, lease_owner, lease_expires_at_utc_ns
            ) VALUES (?, ?, 1, ?, ?, ?, 0, 0, ?, ?, ?, ?)
            """,
            (
                identifier,
                encode_json(list(work_kind)),
                encode_json({"base_search_run_record_identifier": base}),
                encode_json([identifier]),
                state.value,
                created_at,
                "token" if leased else None,
                "worker" if leased else None,
                100 if leased else None,
            ),
        )
        _ = await connection.execute(
            """
            INSERT INTO work_requests (
                id, work_item_id, requester_kind_parts_json, requester_identifier,
                requested_at_utc_ns, context_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "request-" + identifier,
                identifier,
                encode_json(list(requester_kind)),
                workspace,
                created_at,
                encode_json({"track_identifier": track}),
            ),
        )


@pytest.mark.parametrize("marketplace", ("facebook", "ebay"))
@pytest.mark.parametrize("state", (WorkState.PENDING, WorkState.LEASED))
async def test_active_refresh_lookup_joins_writer_and_returns_retained_base(
    tmp_path: Path, marketplace: str, state: WorkState
) -> None:
    async with (
        Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database,
        database.transaction(),
    ):
        await _seed(database, "active", marketplace=marketplace, state=state)
        # A newer completed run must not hide active work with the older base.
        await _seed(
            database,
            "completed",
            marketplace=marketplace,
            state=WorkState.COMPLETED,
            base="newer-run",
            created_at=2,
        )
        assert await database.active_workspace_track_refresh("workspace", "track") == (
            "active",
            state,
            "original-run",
        )


async def test_active_refresh_lookup_excludes_unrelated_and_inactive_work(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for state in (
            WorkState.COMPLETED,
            WorkState.TERMINAL_FAILURE,
            WorkState.CANCELLING,
            WorkState.CANCELLED,
        ):
            await _seed(database, state.value, state=state)
        await _seed(database, "other-workspace", workspace="other-workspace")
        await _seed(database, "other-track", track="other-track")
        await _seed(database, "other-kind", kind=("carl", "facebook", "work", "collect_search"))
        await _seed(database, "other-marketplace", marketplace="unrecognized")
        await _seed(database, "other-requester", requester_kind=("carl", "mcp", "create_search"))
        assert await database.active_workspace_track_refresh("workspace", "track") is None


async def test_active_refresh_lookup_selects_latest_with_deterministic_tie_break(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database, "old", created_at=1)
        await _seed(database, "new-a", base="base-a", created_at=2)
        await _seed(database, "new-b", base="base-b", created_at=2)
        assert await database.active_workspace_track_refresh("workspace", "track") == (
            "new-b",
            WorkState.PENDING,
            "base-b",
        )


@pytest.mark.parametrize("base", (None, 123, {"unexpected": "object"}, ""))
async def test_active_refresh_lookup_rejects_invalid_base(tmp_path: Path, base: JsonValue) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database, "invalid-base", base=base)
        with pytest.raises(ValueError, match="no valid base search run identifier"):
            _ = await database.active_workspace_track_refresh("workspace", "track")
