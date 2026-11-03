"""A genuine schema21 upgrade adds telemetry without rewriting durable work."""

# Historical bytes, retained watch fences and atomic migration are intentional.
# pyright: reportPrivateUsage=false

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from carl._tests.test_progress_migration import _assert_old_rows, _fixture, _snapshot
from carl._tests.test_stage_storage import NOW, _definition
from carl.core.json import encode_json
from carl.core.stages import StageWaitKind, WaitWork
from carl.core.work_progress import ProgressWait, WorkProgressWatch
from carl.io.db import DatabaseConnections
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
_V21_SHA256 = "1996b96bed45d25fe4dd507eeb5603e87964ecaec5a9a85fed4ffd4ef8cc0a01"
_V21_METADATA = {
    "schema_identity_json": encode_json(
        {"namespace": "carl", "domain": "storage", "backend": "sqlite", "version": 21}
    ),
    "schema_version": "21",
    "schema_definition_sha256": _V21_SHA256,
}


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _create_v21(path: Path) -> None:
    # Exact assembled DDL at signed520de19eaab6c68cddb633599b0d78cd1959614e.
    schema = (Path(__file__).parent / "data" / "schema_v21.sql").read_bytes()
    assert hashlib.sha256(schema).hexdigest() == _V21_SHA256
    assert b"scheduler_gauges" not in schema
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(schema.decode())
        connection.executemany("INSERT INTO schema_metadata VALUES (?,?)", _V21_METADATA.items())
        connection.commit()


def _copy_columns(source_path: Path, target_path: Path) -> None:
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
        for name in tables:
            target.execute(f'DELETE FROM "{name}"')
            columns = [row[1] for row in target.execute(f'PRAGMA table_info("{name}")')]
            quoted = ",".join(f'"{column}"' for column in columns)
            cursor = source.execute(f'SELECT {quoted} FROM "{name}"')
            while rows := cursor.fetchmany(128):
                if name == "sqlite_sequence":
                    rows = [row for row in rows if row[0] in tables]
                target.executemany(
                    f'INSERT INTO "{name}" ({quoted}) VALUES ({",".join("?" for _ in columns)})',
                    rows,
                )
        for _, sql in triggers:
            target.execute(sql)
        assert target.execute("PRAGMA foreign_key_check").fetchall() == []
        target.commit()


async def _legacy(path: Path) -> None:
    # The reused fixture supplies real live HTTP/resident leases, published
    # evidence and canonical scheduling turns. Copy only genuine21 columns.
    await _fixture(path.with_name("source-v20.sqlite3"))
    seed = path.with_name("evidence-v19.seed.sqlite3")
    async with Database.managed(seed) as database:
        lease = await database.stage_execution_lease(
            "stage-parent-A",
            expected_token="fair-turn",
            expected_execution=1,
            coordination_scope="test-host",
        )
        assert lease is not None
        await database.suspend_stage(
            lease,
            WaitWork(
                continuation=_definition("parent-A", identifier="sleep-parent-A", resources=()),
                wait_kind=StageWaitKind.DEPENDENCY,
                result={"waiting": True},
                progress_wait=ProgressWait(
                    observed_through_revision=await database.work_progress_watermark(),
                    watch=WorkProgressWatch(work_identifiers=("pending",)),
                ),
            ),
            now_utc_ns=NOW + 1,
        )
    _create_v21(path)
    _copy_columns(seed, path)


async def test_v21_upgrade_preserves_all_work_watch_and_fairness_rows(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    await _legacy(path)
    before = _snapshot(path)
    with closing(sqlite3.connect(path)) as connection:
        expected_work = dict(
            connection.execute("SELECT state,count(*) FROM work_items GROUP BY state")
        )
        expected_stages = dict(
            connection.execute("SELECT state,count(*) FROM work_stages GROUP BY state")
        )
    async with Database.managed(path) as database:
        _assert_old_rows(path, before)
        status = await database.scheduler_activity(
            coordination_scope="test-host",
            now_utc_ns=NOW + 2,
        )
        for state, count in expected_work.items():
            assert getattr(status.work_counts, state) == count
        for state, count in expected_stages.items():
            assert getattr(status.stage_counts, state) == count
        assert status.stage_counts.progress_waiting == 1
        assert status.work_counts.complete and status.stage_counts.complete
        assert status.throughput is not None and not status.throughput.complete
        assert all(summary.count == 0 for summary in status.timings)
        async with database._connections.reader() as connection:
            assert await (await connection.execute("PRAGMA foreign_key_check")).fetchall() == []
            metadata = await (
                await connection.execute("SELECT key,value FROM schema_metadata")
            ).fetchall()
            assert {str(row[0]): str(row[1]) for row in metadata} == Database._expected_metadata()
    async with Database.managed(path):
        _assert_old_rows(path, before)


async def test_v21_unknown_hash_is_rejected_without_modification(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    await _legacy(path)
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


async def test_v21_counter_initialization_failure_rolls_back_entire_upgrade(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    await _legacy(path)
    before = _snapshot(path)
    with closing(sqlite3.connect(path)) as connection:
        ddl = connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall()

    def fail_gauges(_cursor: object, statement: str, _bindings: object) -> bool:
        if statement.lstrip().startswith("UPDATE scheduler_gauges "):
            raise RuntimeError("injected scheduler initialization failure")
        return True

    async with DatabaseConnections.managed(path, create=False) as connections:
        database = Database(path, connections)
        async with connections.writer() as connection:
            connection.set_exec_trace(fail_gauges)
            try:
                with pytest.raises(RuntimeError, match="injected scheduler initialization failure"):
                    await database.migrate()
            finally:
                connection.set_exec_trace(None)
    assert _snapshot(path) == before
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall() == ddl
        assert dict(connection.execute("SELECT key,value FROM schema_metadata")) == _V21_METADATA
