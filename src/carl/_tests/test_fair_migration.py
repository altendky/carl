"""Genuine schema19 upgrades preserve evidence while backfilling scheduling intent."""

# Freeze historical DDL and inspect the exact transaction/ownership boundary.
# pyright: reportPrivateUsage=false

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from carl._tests.test_stage_storage import NOW
from carl._tests.test_work_dispatch_migration import _rows, _seed_live_ownership
from carl._tests.test_worker import _definition, _requester
from carl.core.fair_scheduling import SchedulingContext, SchedulingServiceClass
from carl.core.json import encode_json
from carl.io.db import DatabaseConnections
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
_V19_SHA256 = "e4cba8442c0eb6405811b2e61bc9133f9c0f83593de86331b7c10167452d8845"
_V19_METADATA = {
    "schema_identity_json": encode_json(
        {"namespace": "carl", "domain": "storage", "backend": "sqlite", "version": 19}
    ),
    "schema_version": "19",
    "schema_definition_sha256": _V19_SHA256,
}


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _create_v19(path: Path) -> None:
    # Exact _SCHEMA from merged commit3f669c30f04f3b89485bbe343c2a5daa007961db.
    schema = (Path(__file__).parent / "data" / "schema_v19.sql").read_bytes()
    assert hashlib.sha256(schema).hexdigest() == _V19_SHA256
    assert b"fair_" not in schema and b"parent_work_identifier" not in schema
    with closing(sqlite3.connect(path)) as connection:
        _ = connection.executescript(schema.decode())
        _ = connection.executemany(
            "INSERT INTO schema_metadata VALUES (?,?)", _V19_METADATA.items()
        )
        connection.commit()


def _seed_legacy_owners(path: Path) -> None:
    with closing(sqlite3.connect(path)) as connection:
        for index, identifier in enumerate(
            (
                "z-root",
                "other-root",
                "a-child",
                "evidence-consumer",
                "selection-batch",
                *(f"unknown-{i:03d}" for i in range(260)),
            )
        ):
            _ = connection.execute(
                """INSERT INTO work_items(id,kind_parts_json,payload_schema_version,payload_json,
                deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns)
                VALUES (?, '["test","legacy"]',1,'{}',?,'pending',0,0,?)""",
                (identifier, encode_json([identifier]), NOW - 1000 + index),
            )
        for request, work, kind, identifier, context, requested in (
            (
                "workspace-a",
                "z-root",
                ("carl", "mcp", "create_workspace_search"),
                "workspace-a",
                {},
                NOW - 900,
            ),
            (
                "workspace-b",
                "other-root",
                ("carl", "mcp", "create_workspace_search"),
                "workspace-b",
                {},
                NOW - 900,
            ),
            (
                "z-first-child",
                "a-child",
                ("carl", "pipeline", "listing"),
                "source-evidence",
                {"root_work_identifier": "z-root"},
                NOW - 800,
            ),
            (
                "a-later-child",
                "a-child",
                ("carl", "pipeline", "listing"),
                "different-evidence",
                {"root_work_identifier": "other-root"},
                NOW - 800,
            ),
            (
                "source-only",
                "evidence-consumer",
                ("carl", "mcp", "request_listing_analysis"),
                "z-root",
                {"source_refresh_work_identifier": "z-root"},
                NOW - 800,
            ),
            (
                "selection-request",
                "selection-batch",
                ("carl", "mcp", "request_selection_analyses"),
                "workspace-c",
                {},
                NOW - 800,
            ),
        ):
            _ = connection.execute(
                "INSERT INTO work_requests VALUES (?,?,?,?,?,?)",
                (
                    request,
                    work,
                    encode_json(list(kind)),
                    identifier,
                    requested,
                    encode_json(context),
                ),
            )
        connection.commit()
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


async def test_genuine_v19_preserves_history_leases_and_backfills_one_canonical_owner(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy.sqlite3"
    _create_v19(path)
    resident, stateless = await _seed_live_ownership(path)
    _seed_legacy_owners(path)
    before = _rows(path)
    with closing(sqlite3.connect(path)) as connection:
        keys = dict(
            connection.execute("SELECT id,acquisition_resource_identity_json FROM work_items")
        )
    for _ in range(2):
        async with Database.managed(path) as database:
            assert _rows(path) == before
            assert await database.validate_stage(resident, now_utc_ns=NOW)
            assert await database.validate_stage(stateless, now_utc_ns=NOW)
            root = await database.work_scheduling("z-root")
            assert root.workspace_identifier == "workspace-a"
            assert root.service_class is SchedulingServiceClass.BACKGROUND
            assert await database.work_scheduling("a-child") == root
            evidence_owner = await database.work_scheduling("evidence-consumer")
            assert evidence_owner.root_request_identifier == "evidence-consumer"
            assert evidence_owner.workspace_identifier is None
            assert (
                await database.work_scheduling("selection-batch")
            ).workspace_identifier == "workspace-c"
            async with database._connections.reader() as connection:
                rows = await (
                    await connection.execute(
                        "SELECT count(*),count(DISTINCT root_request_identifier) FROM work_scheduling WHERE work_item_id LIKE 'unknown-%'"
                    )
                ).fetchall()
                assert rows == [(260, 1)]
                assert await (
                    await connection.execute("SELECT count(*) FROM fair_scopes")
                ).fetchall() == [(0,)]
                assert await (
                    await connection.execute(
                        "SELECT count(*) FROM work_requests WHERE parent_work_identifier IS NOT NULL OR scheduling_json IS NOT NULL"
                    )
                ).fetchall() == [(0,)]
    with closing(sqlite3.connect(path)) as connection:
        assert (
            dict(connection.execute("SELECT key,value FROM schema_metadata"))
            == Database._expected_metadata()
        )
        assert (
            dict(connection.execute("SELECT id,acquisition_resource_identity_json FROM work_items"))
            == keys
        )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


async def test_public_legacy_identifier_and_new_attachments_cannot_change_old_owner(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy.sqlite3"
    _create_v19(path)
    _seed_legacy_owners(path)
    async with Database.managed(path) as database:
        work = _definition(identifier="explicit-root", kind=("test", "fair-migration"))
        context = SchedulingContext(
            root_request_identifier="legacy", service_class=SchedulingServiceClass.INTERACTIVE
        )
        await database.enqueue_work(
            work,
            _requester(work.identifier).model_copy(update={"scheduling": context}),
            event_identifier="explicit-root-enqueue",
            enqueued_at_utc_ns=NOW,
        )
        assert await database.work_scheduling(work.identifier) == context
        async with database._connections.reader() as connection:
            before = await (await connection.execute("SELECT count(*) FROM fair_roots")).fetchall()
            owner_keys = await (
                await connection.execute(
                    "SELECT DISTINCT root_request_identifier FROM work_scheduling WHERE work_item_id IN ('explicit-root','unknown-000')"
                )
            ).fetchall()
            assert len(owner_keys) == 2
        owner = await database.work_scheduling("a-child")
        await database.attach_work_request(
            "a-child",
            _requester("new-consumer").model_copy(
                update={
                    "scheduling": SchedulingContext(
                        root_request_identifier="consumer-later",
                        workspace_identifier="workspace-c",
                        service_class=SchedulingServiceClass.INTERACTIVE,
                    )
                }
            ),
            event_identifier="attach-later",
            requested_at_utc_ns=NOW,
        )
        assert await database.work_scheduling("a-child") == owner
        async with database._connections.reader() as connection:
            assert (
                await (await connection.execute("SELECT count(*) FROM fair_roots")).fetchall()
                == before
            )


async def test_unknown_v19_hash_is_rejected_without_mutating_any_history(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    _create_v19(path)
    _seed_legacy_owners(path)
    with closing(sqlite3.connect(path)) as connection:
        _ = connection.execute(
            "UPDATE schema_metadata SET value=? WHERE key='schema_definition_sha256'", ("0" * 64,)
        )
        connection.commit()
        schema = connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall()
        metadata = connection.execute("SELECT * FROM schema_metadata ORDER BY key").fetchall()
    before = _rows(path)
    with pytest.raises(RuntimeError, match="schema"):
        async with Database.managed(path):
            pass
    assert _rows(path) == before
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall() == schema
        assert (
            connection.execute("SELECT * FROM schema_metadata ORDER BY key").fetchall() == metadata
        )


async def test_backfill_failure_rolls_back_tables_columns_and_metadata(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    _create_v19(path)
    _seed_legacy_owners(path)
    before = _rows(path)
    with closing(sqlite3.connect(path)) as connection:
        schema = connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall()

    def fail_owner(_cursor: object, statement: str, _bindings: object) -> bool:
        if "INSERT OR IGNORE INTO work_scheduling" in statement:
            raise RuntimeError("injected ownership backfill failure")
        return True

    async with DatabaseConnections.managed(path, create=False) as connections:
        database = Database(path, connections)
        async with connections.writer() as connection:
            connection.set_exec_trace(fail_owner)
            try:
                with pytest.raises(RuntimeError, match="injected ownership backfill failure"):
                    await database.migrate()
            finally:
                connection.set_exec_trace(None)
    assert _rows(path) == before
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall() == schema
        assert dict(connection.execute("SELECT key,value FROM schema_metadata")) == _V19_METADATA
