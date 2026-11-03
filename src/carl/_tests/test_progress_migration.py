"""The genuine schema20 upgrade preserves execution and scheduling history."""

# Historical DDL, exact publication fences and rollback are intentional checks.
# pyright: reportPrivateUsage=false

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from carl._tests.test_fair_migration import _create_v19
from carl._tests.test_fair_storage import _seed
from carl._tests.test_stage_storage import NOW, _claim
from carl._tests.test_work_dispatch_migration import _seed_live_ownership
from carl.core.json import encode_json
from carl.core.stages import StageLease
from carl.core.work import WorkRequester
from carl.io.db import DatabaseConnections
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
_V20_SHA256 = "b4336f5f4093b362d02befe4ec345ceb34512091a1c04f4ee0904c22c6513059"
_V20_METADATA = {
    "schema_identity_json": encode_json(
        {"namespace": "carl", "domain": "storage", "backend": "sqlite", "version": 20}
    ),
    "schema_version": "20",
    "schema_definition_sha256": _V20_SHA256,
}


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _create_v20(path: Path) -> None:
    # Exact assembled DDL at signed d294abbef9682de03ac7648e067ca875dbf7d701.
    schema = (Path(__file__).parent / "data" / "schema_v20.sql").read_bytes()
    assert hashlib.sha256(schema).hexdigest() == _V20_SHA256
    assert b"progress_waiting" not in schema and b"ready_count" not in schema
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(schema.decode())
        connection.executemany("INSERT INTO schema_metadata VALUES (?,?)", _V20_METADATA.items())
        connection.commit()


def _copy_to_v20(source_path: Path, target_path: Path) -> None:
    """Copy only genuine20 columns, retaining the original20 trigger definitions."""
    with (
        closing(sqlite3.connect(source_path)) as source,
        closing(sqlite3.connect(target_path)) as target,
    ):
        target.execute("PRAGMA foreign_keys=ON")
        target.execute("BEGIN")
        target.execute("PRAGMA defer_foreign_keys=ON")
        triggers = target.execute(
            "SELECT name,sql FROM sqlite_schema WHERE type='trigger' ORDER BY name"
        ).fetchall()
        for name, _ in triggers:
            target.execute(f'DROP TRIGGER "{name}"')
        tables = [
            row[0]
            for row in target.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name!='schema_metadata' ORDER BY name"
            )
        ]
        for table in tables:
            target.execute(f'DELETE FROM "{table}"')
            columns = [row[1] for row in target.execute(f'PRAGMA table_info("{table}")')]
            quoted = ",".join(f'"{name}"' for name in columns)
            cursor = source.execute(f'SELECT {quoted} FROM "{table}"')
            while rows := cursor.fetchmany(128):
                target.executemany(
                    f'INSERT INTO "{table}" ({quoted}) VALUES ({",".join("?" for _ in columns)})',
                    rows,
                )
        for _, sql in triggers:
            target.execute(sql)
        assert target.execute("PRAGMA foreign_key_check").fetchall() == []
        target.commit()


def _snapshot(path: Path) -> dict[str, tuple[tuple[str, ...], tuple[tuple[object, ...], ...]]]:
    with closing(sqlite3.connect(path)) as connection:
        return {
            name: (
                tuple(row[1] for row in connection.execute(f'PRAGMA table_info("{name}")')),
                tuple(sorted(connection.execute(f'SELECT * FROM "{name}"'), key=repr)),
            )
            for (name,) in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name!='schema_metadata'"
            )
        }


def _assert_old_rows(
    path: Path, before: dict[str, tuple[tuple[str, ...], tuple[tuple[object, ...], ...]]]
) -> None:
    with closing(sqlite3.connect(path)) as connection:
        for name, (columns, rows) in before.items():
            quoted = ",".join(f'"{column}"' for column in columns)
            assert (
                tuple(sorted(connection.execute(f'SELECT {quoted} FROM "{name}"'), key=repr))
                == rows
            ), name


async def _fixture(path: Path) -> tuple[StageLease, StageLease]:
    old = path.with_name("evidence-v19.sqlite3")
    _create_v19(old)
    leases = await _seed_live_ownership(old)
    seed = old.with_suffix(".seed.sqlite3")
    async with Database.managed(seed) as database:
        first = await _seed(database, "parent-A", root="A", workspace="workspace-A")
        await _seed(database, "parent-B", root="B", workspace="workspace-B")
        await database.enable_fair_scheduling("test-host", NOW)
        assert (await _claim(database, first.identifier, token="fair-turn")).lease is not None
        for parent in ("parent-A", "parent-B"):
            await database.attach_work_request(
                "pending",
                WorkRequester(
                    request_identifier="consumer-" + parent,
                    kind=("carl", "marketplace", "pipeline_listing"),
                    identifier="pending",
                    context={"root_work_identifier": parent},
                ),
                event_identifier="consumer-event-" + parent,
                requested_at_utc_ns=NOW,
            )
    _create_v20(path)
    _copy_to_v20(seed, path)
    return leases


async def test_v20_upgrade_preserves_all_old_rows_fences_and_fair_turns(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    leases = await _fixture(path)
    before = _snapshot(path)
    assert before["fair_admissions"][1]
    async with Database.managed(path) as database:
        _assert_old_rows(path, before)
        for lease in leases:
            assert await database.validate_stage(lease, now_utc_ns=NOW)
        async with database._connections.reader() as connection:
            edges = await (
                await connection.execute(
                    "SELECT parent_work_id,child_work_id FROM work_consumer_edges WHERE child_work_id='pending' ORDER BY parent_work_id"
                )
            ).fetchall()
            assert edges == [("parent-A", "pending"), ("parent-B", "pending")]
            assert await (await connection.execute("PRAGMA foreign_key_check")).fetchall() == []
            metadata = await (
                await connection.execute("SELECT key,value FROM schema_metadata")
            ).fetchall()
            assert {str(row[0]): str(row[1]) for row in metadata} == Database._expected_metadata()
    # Idempotent reentry cannot duplicate a consumer edge or change a fair turn.
    async with Database.managed(path):
        _assert_old_rows(path, before)


async def test_unknown_v20_hash_rejects_without_changing_ddl_or_rows(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    await _fixture(path)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "UPDATE schema_metadata SET value=? WHERE key='schema_definition_sha256'", ("0" * 64,)
        )
        connection.commit()
        ddl = connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall()
    before = _snapshot(path)
    with pytest.raises(RuntimeError, match="schema"):
        async with Database.managed(path):
            pass
    assert _snapshot(path) == before
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall() == ddl


async def test_consumer_backfill_failure_rolls_back_upgrade_atomically(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    await _fixture(path)
    before = _snapshot(path)
    with closing(sqlite3.connect(path)) as connection:
        ddl = connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall()

    def fail_edges(_cursor: object, statement: str, _bindings: object) -> bool:
        if "INSERT OR IGNORE INTO work_consumer_edges" in statement:
            raise RuntimeError("injected consumer backfill failure")
        return True

    async with DatabaseConnections.managed(path, create=False) as connections:
        database = Database(path, connections)
        async with connections.writer() as connection:
            connection.set_exec_trace(fail_edges)
            try:
                with pytest.raises(RuntimeError, match="injected consumer backfill failure"):
                    await database.migrate()
            finally:
                connection.set_exec_trace(None)
    assert _snapshot(path) == before
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall() == ddl
        assert dict(connection.execute("SELECT key,value FROM schema_metadata")) == _V20_METADATA
