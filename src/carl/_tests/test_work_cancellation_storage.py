"""Durable cancellation works without workers and preserves unrelated work."""

# pyright: reportPrivateUsage=false

import sqlite3
from pathlib import Path

import pytest

from carl._tests.test_ebay_item_workers import _identifiers, _retain_records
from carl._tests.test_worker import _definition, _requester
from carl.core.models import RecordDraft
from carl.core.work import (
    ClaimResult,
    WorkCancellationRequestedError,
    WorkCapability,
    WorkRequester,
    WorkState,
)
from carl.io import sqlite as sqlite_io
from carl.io.sqlite import Database


async def _enqueue(database: Database, identifier: str, parent: str | None = None) -> None:
    await database.enqueue_work(
        _definition(identifier=identifier, kind=("test", "work")),
        WorkRequester(
            request_identifier=f"request-{identifier}",
            kind=("test", "requester"),
            identifier=parent or "workspace",
            context={},
        ),
        event_identifier=f"event-{identifier}",
        enqueued_at_utc_ns=1,
    )


async def _claim(database: Database) -> ClaimResult:
    return await database.claim_work(
        supported_capabilities=(WorkCapability(kind=("test", "work"), payload_schema_version=1),),
        worker_identifier="worker",
        lease_token="token",
        lease_duration_ns=100,
        utc_now_ns=lambda: 4,
        event_identifier="claim",
    )


@pytest.mark.anyio
async def test_workspace_cancels_78_pending_without_worker_and_new_work_is_claimable(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "db.sqlite3", initialize=True) as database:
        await _enqueue(database, "root")
        for index in range(77):
            await _enqueue(database, f"child-{index}", "root")
        result = await database.cancel_workspace_work(
            "workspace", reason="stop", caller="mcp:test", requested_at_utc_ns=2
        )
        assert result["cancelled_pending"] == 78
        assert result["cancelling_leased"] == 0
        assert set(await database.work_states(("root", "child-0"))) == {WorkState.CANCELLED}
        assert await database.has_cancelled_work_dependencies("root")
        repeated = await database.cancel_workspace_work(
            "workspace", reason="stop", caller="mcp:test", requested_at_utc_ns=3
        )
        assert repeated["already_terminal"] == 1
        assert (await _claim(database)).lease is None
        replacement = _definition(identifier="replacement", kind=("test", "work")).model_copy(
            update={"deduplication_identity": ("root",)}
        )
        assert (
            await database.enqueue_work(
                replacement,
                _requester("replacement"),
                event_identifier="replacement-enqueued",
                enqueued_at_utc_ns=4,
            )
        ).created
        assert (await _claim(database)).lease is not None
        workspace = await database.workspace_cancellation_status(
            workspace_record_identifier="workspace", maximum_rows=10
        )
        assert workspace[:2] == (0, 1)
        assert workspace[2][0].state is WorkState.CANCELLED


@pytest.mark.anyio
async def test_leased_request_is_fenced_acknowledged_and_expired_cancellation_is_not_reclaimed(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "db.sqlite3", initialize=True) as database:
        await _enqueue(database, "root")
        claim = await database.claim_work(
            supported_capabilities=(
                WorkCapability(kind=("test", "work"), payload_schema_version=1),
            ),
            worker_identifier="worker",
            lease_token="token",
            lease_duration_ns=100,
            utc_now_ns=lambda: 2,
            event_identifier="claim",
        )
        assert claim.lease is not None
        result = await database.cancel_work(
            "root", reason="stop", caller="mcp:test", requested_at_utc_ns=3
        )
        assert result["cancelling_leased"] == 1
        assert await database.work_state("root") is WorkState.CANCELLING
        with pytest.raises(WorkCancellationRequestedError):
            await database.renew_lease(
                work_item_identifier="root",
                lease_token="token",
                worker_identifier="worker",
                lease_duration_ns=100,
                utc_now_ns=lambda: 4,
                event_identifier="renew",
            )
        with pytest.raises(WorkCancellationRequestedError):
            await _enqueue(database, "forbidden-child", "root")
        assert (
            await database.claim_work(
                supported_capabilities=(
                    WorkCapability(kind=("test", "work"), payload_schema_version=1),
                ),
                worker_identifier="other",
                lease_token="other",
                lease_duration_ns=100,
                utc_now_ns=lambda: 103,
                event_identifier="claim-again",
            )
        ).lease is None
        assert await database.work_state("root") is WorkState.CANCELLED
        work = await database.work("root")
        assert work["cancellation"] == {
            "reason": "stop",
            "caller": "mcp:test",
            "requested_at_utc_ns": 3,
        }
        assert work["error"] is None


@pytest.mark.anyio
async def test_context_only_dependencies_and_cancel_all_preserve_terminal_states(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "db.sqlite3", initialize=True) as database:
        await _enqueue(database, "root")
        await database.enqueue_work(
            _definition(identifier="context-child", kind=("test", "work")),
            WorkRequester(
                request_identifier="context-request",
                kind=("test", "requester"),
                identifier="synthetic",
                context={"root_work_identifier": "root"},
            ),
            event_identifier="context-event",
            enqueued_at_utc_ns=1,
        )
        result = await database.cancel_work(
            "root", reason="stop", caller="test", requested_at_utc_ns=2
        )
        assert set(result["cancelled_pending_identifiers"]) == {"root", "context-child"}
        await _enqueue(database, "unrelated")
        result = await database.cancel_all_work(
            reason="everything", caller="test", requested_at_utc_ns=3
        )
        assert result["cancelled_pending_identifiers"] == ["unrelated"]


@pytest.mark.anyio
async def test_terminal_root_is_noop_and_workspace_only_walks_active_roots(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "db.sqlite3", initialize=True) as database:
        await _enqueue(database, "root")
        await _enqueue(database, "child", "root")
        async with database._connections.writer() as connection:
            await connection.execute("UPDATE work_items SET state='completed' WHERE id='root'")
        result = await database.cancel_work(
            "root", reason="stop", caller="test", requested_at_utc_ns=2
        )
        assert result["already_terminal_identifiers"] == ["root"]
        assert await database.work_state("child") is WorkState.PENDING
        result = await database.cancel_workspace_work(
            "workspace", reason="stop", caller="test", requested_at_utc_ns=2
        )
        assert result["cancelled_pending"] == 0
        assert await database.work_state("child") is WorkState.PENDING


@pytest.mark.anyio
async def test_acquisition_and_operation_context_edges_are_cascaded(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "db.sqlite3", initialize=True) as database:
        await _enqueue(database, "root")
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier="acquisition",
                    kind=("test", "acquisition"),
                    schema_version=1,
                    value={},
                ),
            ),
            _identifiers(),
        )
        async with database._connections.writer() as connection:
            await connection.execute(
                "INSERT INTO work_operations SELECT created_by_operation_id,'root',1,'lease','worker' FROM objects WHERE id='acquisition'"
            )
            cursor = await connection.execute(
                "SELECT created_by_operation_id FROM objects WHERE id='acquisition'"
            )
            row = await cursor.fetchone()
            assert row is not None
            operation = row[0]
        for index, (requester, context) in enumerate(
            (
                ("acquisition", {}),
                ("synthetic", {"source_operation_identifier": operation}),
                ("synthetic", {"source_search_refresh_work_identifiers": ["root"]}),
            )
        ):
            await database.enqueue_work(
                _definition(identifier=f"child-{index}", kind=("test", "work")),
                WorkRequester(
                    request_identifier=f"child-request-{index}",
                    kind=("test", "requester"),
                    identifier=requester,
                    context=context,
                ),
                event_identifier=f"child-event-{index}",
                enqueued_at_utc_ns=1,
            )
        assert (
            await database.cancel_work("root", reason="stop", caller="test", requested_at_utc_ns=2)
        )["cancelled_pending"] == 4


@pytest.mark.anyio
@pytest.mark.parametrize("cancel_child", [False, True])
async def test_interrupted_finalization_atomically_honors_cancellation(
    tmp_path: Path, cancel_child: bool
) -> None:
    async with Database.managed(tmp_path / "db.sqlite3", initialize=True) as database:
        await _enqueue(database, "root")
        await _enqueue(database, "child", "root")
        claim = await database.claim_work(
            supported_capabilities=(
                WorkCapability(kind=("test", "work"), payload_schema_version=1),
            ),
            worker_identifier="worker",
            lease_token="token",
            lease_duration_ns=100,
            utc_now_ns=lambda: 2,
            event_identifier="claim",
            eligible_identifiers=("root",),
        )
        assert claim.lease is not None
        await database.cancel_work(
            "child" if cancel_child else "root", reason="stop", caller="test", requested_at_utc_ns=3
        )
        assert await database.finalize_interrupted_work(
            work_item_identifier="root",
            lease_token="token",
            worker_identifier="worker",
            operation_id="not-started",
            utc_now_ns=lambda: 4,
            event_identifier="interrupted",
            reason="cancelled",
            ended_at_utc="2026-10-17T00:00:00+00:00",
            duration_ns=1,
            possibly_dispatched=False,
        )
        assert await database.work_state("root") is WorkState.CANCELLED
        assert (await database.work("root"))["error"] is None


@pytest.mark.anyio
async def test_non_cascading_cancel_does_not_block_uncancelled_childs_new_request(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "db.sqlite3", initialize=True) as database:
        await _enqueue(database, "root")
        await _enqueue(database, "child", "root")
        await database.cancel_work(
            "root",
            reason="only root",
            caller="test",
            requested_at_utc_ns=2,
            include_descendants=False,
        )
        result = await database.enqueue_work(
            _definition(identifier="grandchild", kind=("test", "work")),
            WorkRequester(
                request_identifier="grandchild-request",
                kind=("test", "requester"),
                identifier="child",
                context={"root_work_identifier": "root"},
            ),
            event_identifier="grandchild-event",
            enqueued_at_utc_ns=3,
        )
        assert result.created
        assert await database.work_state("grandchild") is WorkState.PENDING


@pytest.mark.anyio
async def test_publishing_one_dependency_request_does_not_scan_unrelated_requests(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "db.sqlite3", initialize=True) as database:
        await _enqueue(database, "root")
        await _enqueue(database, "child", "root")
        async with database._connections.writer() as connection:

            async def cost() -> int:
                steps = 0

                def count() -> bool:
                    nonlocal steps
                    steps += 100
                    return False

                await connection.set_progress_handler(count, nsteps=100, id="dependency-cost")
                try:
                    await database._publish_work_request_dependencies(connection, "request-child")
                finally:
                    await connection.set_progress_handler(None, id="dependency-cost")
                return steps

            before = await cost()
            await connection.executemany(
                "INSERT INTO work_requests VALUES (?,'root','[\"test\"]','unrelated',1,'{}')",
                ((f"noise-{index}",) for index in range(5000)),
            )
            assert await cost() <= before + 300


@pytest.mark.anyio
async def test_cancelled_search_cooldown_preserves_siblings_own_deadline(tmp_path: Path) -> None:
    kind = ("carl", "ebay", "collect", "search")
    async with Database.managed(tmp_path / "db.sqlite3", initialize=True) as database:
        for identifier in ("source", "sibling"):
            definition = _definition(identifier=identifier, kind=kind).model_copy(
                update={
                    "payload": {"request": {"stack_identifier": "ebay_anonymous"}},
                    "not_before_utc_ns": 50 if identifier == "sibling" else 0,
                }
            )
            await database.enqueue_work(
                definition,
                _requester(identifier),
                event_identifier=f"enqueue-{identifier}",
                enqueued_at_utc_ns=1,
            )
        async with database._connections.writer() as connection:
            await database._append_work_event(
                connection,
                event_identifier="source-backoff",
                work_item_identifier="source",
                event_kind=sqlite_io.WorkEventKind.RELEASED,
                recorded_at_utc_ns=2,
                data={
                    "reason": {
                        "cooldown": {"stack_identifier": "ebay_anonymous", "until_utc_ns": 100}
                    }
                },
            )
            await database._append_work_event(
                connection,
                event_identifier="shared-backoff",
                work_item_identifier="sibling",
                event_kind=sqlite_io.WorkEventKind.ENQUEUED,
                recorded_at_utc_ns=2,
                data={
                    "reason": {
                        "kind": "ebay_search_shared_cooldown",
                        "source_work_identifier": "source",
                    },
                    "eligible_at_utc_ns": 100,
                },
            )
            await connection.execute(
                "UPDATE work_items SET eligible_at_utc_ns=100 WHERE id='sibling'"
            )
        late = _definition(identifier="late", kind=kind).model_copy(
            update={
                "payload": {"request": {"stack_identifier": "ebay_anonymous"}},
                "not_before_utc_ns": 50,
            }
        )
        await database.enqueue_work(
            late, _requester("late"), event_identifier="enqueue-late", enqueued_at_utc_ns=3
        )
        assert (await database.work("late"))["eligible_at_utc_ns"] == 100
        await database.cancel_work("source", reason="stop", caller="test", requested_at_utc_ns=3)
        assert (await database.work("sibling"))["eligible_at_utc_ns"] == 50
        assert (await database.work("late"))["eligible_at_utc_ns"] == 50


@pytest.mark.anyio
async def test_v12_migration_rebuilds_checks_preserving_requests_and_foreign_keys(
    tmp_path: Path,
) -> None:
    path = tmp_path / "db.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _enqueue(database, "root")
        await _enqueue(database, "child", "root")
    # Existing table constraints are stricter in v12; emulate them without relying on git.
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE work_cancellation_requests")
        connection.execute("DROP TABLE work_dependencies")
        for name in ("work_items", "work_events"):
            start = sqlite_io._SCHEMA.index(f"CREATE TABLE IF NOT EXISTS {name} (")
            end = sqlite_io._SCHEMA.index(") STRICT;", start) + len(") STRICT;")
            sql = sqlite_io._SCHEMA[start:end].replace(
                f"CREATE TABLE IF NOT EXISTS {name} (", f"CREATE TABLE {name}_v12 ("
            )
            sql = (
                sql.replace(", 'cancelling', 'cancelled'", "")
                .replace("state IN ('leased', 'cancelling')", "state = 'leased'")
                .replace("state NOT IN ('leased', 'cancelling')", "state != 'leased'")
                .replace(",\n            'cancel_requested', 'cancelled'", "")
            )
            connection.execute(sql)
            connection.execute(f"INSERT INTO {name}_v12 SELECT * FROM {name}")
            connection.execute(f"DROP TABLE {name}")
            connection.execute(f"ALTER TABLE {name}_v12 RENAME TO {name}")
        connection.executemany(
            "UPDATE schema_metadata SET value=? WHERE key=?",
            ((value, key) for key, value in Database._v12_metadata().items()),
        )
    async with Database.managed(path) as database:
        assert (
            await database.cancel_work("root", reason="stop", caller="test", requested_at_utc_ns=2)
        )["cancelled_pending"] == 2
        async with database._connections.reader() as connection:
            assert await (await connection.execute("PRAGMA foreign_key_check")).fetchall() == []
