"""Schema22 query indexes upgrade without rewriting work, leases or evidence."""

# pyright: reportPrivateUsage=false

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from carl._tests.test_progress_migration import _snapshot
from carl._tests.test_scheduler_migration import _copy_columns, _legacy
from carl.io.db import DatabaseConnections
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
_V22_SHA256 = "c250b9d74c7772b43569ca7342e462e986690aa290d7f1502e2a2525dd52c767"


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _schema22(path: Path) -> None:
    source = path.with_name("source.sqlite3")
    await _legacy(source)
    async with Database.managed(source):
        pass
    schema = (Path(__file__).parent / "data" / "schema_v22.sql").read_bytes()
    assert hashlib.sha256(schema).hexdigest() == _V22_SHA256
    assert b"work_events_terminal_time" not in schema
    assert b"work_items_completed_acquisition" not in schema
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(schema.decode())
        connection.executemany(
            "INSERT INTO schema_metadata VALUES (?,?)", Database._v22_metadata().items()
        )
        connection.commit()
    _copy_columns(source, path)


async def test_schema22_query_indexes_preserve_all_retained_rows(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    await _schema22(path)
    before = _snapshot(path)
    async with Database.managed(path) as database:
        assert _snapshot(path) == before
        async with database._connections.reader() as connection:
            indexes = {
                str(row[0])
                for row in await (
                    await connection.execute("SELECT name FROM sqlite_schema WHERE type='index'")
                ).fetchall()
            }
            assert {"work_events_terminal_time", "work_items_completed_acquisition"} <= indexes
            assert await (await connection.execute("PRAGMA foreign_key_check")).fetchall() == []
    async with Database.managed(path):
        assert _snapshot(path) == before


async def test_schema22_unknown_hash_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    await _schema22(path)
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


async def test_schema22_index_failure_rolls_back_both_indexes(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    await _schema22(path)
    before = _snapshot(path)

    def fail_second_index(_cursor: object, statement: str, _bindings: object) -> bool:
        if statement.lstrip().startswith("CREATE INDEX IF NOT EXISTS work_events_terminal_time"):
            raise RuntimeError("injected terminal index failure")
        return True

    async with DatabaseConnections.managed(path, create=False) as connections:
        database = Database(path, connections)
        async with connections.writer() as connection:
            connection.set_exec_trace(fail_second_index)
            try:
                with pytest.raises(RuntimeError, match="injected terminal index failure"):
                    await database.migrate()
            finally:
                connection.set_exec_trace(None)
    assert _snapshot(path) == before
    with closing(sqlite3.connect(path)) as connection:
        assert (
            dict(connection.execute("SELECT key,value FROM schema_metadata"))
            == Database._v22_metadata()
        )
        assert (
            connection.execute(
                "SELECT name FROM sqlite_schema WHERE name IN "
                "('work_events_terminal_time','work_items_completed_acquisition')"
            ).fetchall()
            == []
        )
