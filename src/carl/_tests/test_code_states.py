"""Normalized binary Git identities remain linked to retained evidence."""

import re
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Literal

import pytest

from carl.core.components import Component, ComponentId
from carl.core.json import decode_json, encode_json
from carl.core.models import BytesDraft, CodeProvenance, NamedOutput, RecordDraft
from carl.io.sqlite import _SCHEMA, Database


def _provenance(
    commit_hash: str | None = "ab" * 20,
    worktree_state: Literal["clean", "dirty", "unknown"] = "clean",
) -> CodeProvenance:
    return CodeProvenance(
        repository_url="https://example.test/carl.git",
        commit_hash=commit_hash,
        worktree_state=worktree_state,
        package_version="test",
        python_implementation="CPython",
        python_version="3.13",
        dependencies=(),
        lockfile_sha256="cd" * 32,
    )


async def _begin(database: Database, identifier: str, provenance: CodeProvenance) -> None:
    await database.begin_operation(
        operation_id=identifier,
        component=Component(ComponentId(("test", "code_states")), 1, lambda: None),
        provenance=provenance,
        invocation={"argv": ["test"]},
        configuration={"setting": True},
        started_at_utc="2026-10-02T00:00:00+00:00",
    )


def _version_ten_database(path: Path) -> None:
    """Build the pre-normalization table layout, rather than relabeling v11."""

    schema, replaced = re.subn(
        r"CREATE TABLE IF NOT EXISTS code_states \(.*?"
        r"ON code_states\(coalesce\(commit_hash, X''\), worktree_state\);\n",
        "",
        _SCHEMA,
        count=1,
        flags=re.DOTALL,
    )
    assert replaced == 1
    schema = schema.replace(
        "error_json TEXT CHECK (error_json IS NULL OR json_valid(error_json)),\n"
        "    code_state_id INTEGER REFERENCES code_states(id)",
        "error_json TEXT CHECK (error_json IS NULL OR json_valid(error_json))",
    ).replace("CREATE INDEX IF NOT EXISTS operations_code_state ON operations(code_state_id);", "")
    assert "code_state" not in schema
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(schema)
        connection.executemany(
            "INSERT INTO schema_metadata(key, value) VALUES (?, ?)",
            Database._v10_metadata().items(),
        )
        connection.commit()


def _legacy_operation(
    connection: sqlite3.Connection,
    identifier: str,
    provenance_json: str,
    *,
    state: str = "completed",
) -> None:
    connection.execute(
        """
        INSERT INTO operations(
            id, component_parts_json, output_schema_version, code_provenance_json,
            invocation_json, configuration_json, state, started_at_utc,
            ended_at_utc, duration_ns, result_json, error_json
        ) VALUES (?, '["test","legacy"]', 1, ?, '{"argv":["legacy"]}',
                  '{"setting":true}', ?, '2026-10-01T00:00:00+00:00',
                  '2026-10-01T00:00:01+00:00', 1000000000, '{"retained":true}', ?)
        """,
        (identifier, provenance_json, state, '{"message":"failed"}' if state == "failed" else None),
    )


@pytest.mark.anyio
async def test_new_operations_deduplicate_binary_code_states(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    inputs = (
        _provenance(),
        _provenance(),
        _provenance(worktree_state="dirty"),
        _provenance(None, "unknown"),
        _provenance(None, "unknown"),
        _provenance("ef" * 32),
    )
    async with Database.managed(path, initialize=True) as database:
        operations = []
        for index, provenance in enumerate(inputs):
            await _begin(database, f"operation-{index}", provenance)
            operation = await database.operation(f"operation-{index}")
            assert operation["code_provenance"] == provenance.as_json()
            operations.append(operation)
    identifiers = [operation["code_state_id"] for operation in operations]
    assert identifiers[0] == identifiers[1]
    assert identifiers[0] != identifiers[2]
    assert identifiers[3] == identifiers[4]
    assert len(set(identifiers)) == 4
    with closing(sqlite3.connect(path)) as connection:
        states = connection.execute(
            "SELECT commit_hash, typeof(commit_hash), length(commit_hash), worktree_state "
            "FROM code_states ORDER BY id"
        ).fetchall()
        assert states == [
            (bytes.fromhex("ab" * 20), "blob", 20, "clean"),
            (bytes.fromhex("ab" * 20), "blob", 20, "dirty"),
            (None, "null", None, "unknown"),
            (bytes.fromhex("ef" * 32), "blob", 32, "clean"),
        ]
        for (raw,) in connection.execute("SELECT code_provenance_json FROM operations"):
            stored = decode_json(raw)
            assert isinstance(stored, dict)
            assert "commit_hash" not in stored
            assert "worktree_state" not in stored
            assert stored["package_version"] == "test"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.anyio
async def test_records_and_artifacts_reach_state_via_producing_operation(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _begin(database, "producer", _provenance(worktree_state="dirty"))
        await database.complete_operation(
            operation_id="producer",
            records=(
                RecordDraft(
                    identifier="record", kind=("test", "record"), schema_version=1, value={}
                ),
            ),
            artifacts=(
                BytesDraft(
                    identifier="artifact",
                    kind=("test", "artifact"),
                    media_type="text/plain",
                    representation={},
                    content=b"retained",
                ),
            ),
            outputs=(
                NamedOutput(name=("record",), object_identifier="record"),
                NamedOutput(name=("artifact",), object_identifier="artifact"),
            ),
            result={},
            ended_at_utc="2026-10-02T00:00:01+00:00",
            duration_ns=1,
        )
        for identifier in ("record", "artifact"):
            producer, _, _ = await database.object_operation_relations(identifier)
            assert producer == "producer"
            operation = await database.operation(producer)
            assert operation["code_provenance"] == _provenance(worktree_state="dirty").as_json()
    with closing(sqlite3.connect(path)) as connection:
        rows = connection.execute(
            "SELECT objects.id, code_states.commit_hash, code_states.worktree_state "
            "FROM objects JOIN operations ON operations.id = objects.created_by_operation_id "
            "JOIN code_states ON code_states.id = operations.code_state_id ORDER BY objects.id"
        ).fetchall()
        assert rows == [
            ("artifact", bytes.fromhex("ab" * 20), "dirty"),
            ("record", bytes.fromhex("ab" * 20), "dirty"),
        ]


@pytest.mark.anyio
async def test_version_ten_backfill_retains_operations_edges_and_metadata(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    _version_ten_database(path)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        _legacy_operation(connection, "completed", encode_json(_provenance().as_json()))
        _legacy_operation(
            connection, "failed", encode_json(_provenance().as_json()), state="failed"
        )
        _legacy_operation(connection, "unknown", "{}", state="failed")
        connection.execute(
            "INSERT INTO objects VALUES ('record', 'record', '[\"test\",\"record\"]', 'completed')"
        )
        connection.execute("INSERT INTO records VALUES ('record', 1, '{\"retained\":true}')")
        connection.execute(
            "INSERT INTO operation_outputs VALUES ('completed', '[\"record\"]', 'record')"
        )
        connection.execute(
            "INSERT INTO operation_inputs VALUES ('failed', '[\"record\"]', 'record')"
        )
        connection.execute(
            "INSERT INTO network_activities(id, kind_parts_json, operation_id, "
            "network_session_identifier, ordinal, attempt, state, eligible_at_utc_ns, "
            "created_at_utc_ns) VALUES ('network', '[\"test\"]', 'failed', 'session', 1, 1, 'pending', 1, 1)"
        )
        before = connection.execute(
            "SELECT id, state, invocation_json, configuration_json, result_json, error_json FROM operations ORDER BY id"
        ).fetchall()
        connection.commit()
    async with Database.managed(path) as database:
        completed = await database.operation("completed")
        failed = await database.operation("failed")
        assert completed["code_state_id"] == failed["code_state_id"]
        assert completed["code_provenance"] == _provenance().as_json()
        assert failed["state"] == "failed"
        unknown = await database.operation("unknown")
        assert unknown["code_provenance"] == {"commit_hash": None, "worktree_state": "unknown"}
        assert await database.get_record("record") == (("test", "record"), 1, {"retained": True})
    with closing(sqlite3.connect(path)) as connection:
        assert (
            connection.execute(
                "SELECT id, state, invocation_json, configuration_json, result_json, error_json FROM operations ORDER BY id"
            ).fetchall()
            == before
        )
        assert connection.execute("SELECT count(*) FROM code_states").fetchone() == (2,)
        assert connection.execute(
            "SELECT operation_id, object_id FROM operation_inputs"
        ).fetchall() == [("failed", "record")]
        assert connection.execute(
            "SELECT operation_id, object_id FROM operation_outputs"
        ).fetchall() == [("completed", "record")]
        assert connection.execute("SELECT operation_id FROM network_activities").fetchall() == [
            ("failed",)
        ]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        first_states = connection.execute("SELECT * FROM code_states ORDER BY id").fetchall()
    async with Database.managed(path) as database:
        assert await database.operation("completed") == completed
    with closing(sqlite3.connect(path)) as connection:
        assert (
            connection.execute("SELECT * FROM code_states ORDER BY id").fetchall() == first_states
        )


@pytest.mark.anyio
async def test_invalid_legacy_hash_rolls_back_entire_migration(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    _version_ten_database(path)
    with closing(sqlite3.connect(path)) as connection:
        _legacy_operation(connection, "valid", encode_json(_provenance().as_json()))
        _legacy_operation(connection, "invalid", encode_json(_provenance("invalid-hash").as_json()))
        connection.commit()
        before = connection.execute("SELECT * FROM operations ORDER BY id").fetchall()
    with pytest.raises(ValueError, match="full Git hexadecimal hash"):
        async with Database.managed(path):
            pass
    with closing(sqlite3.connect(path)) as connection:
        assert (
            dict(connection.execute("SELECT key, value FROM schema_metadata"))
            == Database._v10_metadata()
        )
        assert connection.execute("SELECT * FROM operations ORDER BY id").fetchall() == before
        assert "code_state_id" not in [
            row[1] for row in connection.execute("PRAGMA table_info(operations)")
        ]
        assert (
            connection.execute(
                "SELECT name FROM sqlite_schema WHERE name = 'code_states'"
            ).fetchone()
            is None
        )


@pytest.mark.anyio
async def test_backfill_leaves_existing_normalized_code_state_unchanged(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _begin(database, "normalized", _provenance(worktree_state="dirty"))
        before = await database.operation("normalized")
    with closing(sqlite3.connect(path)) as connection:
        _legacy_operation(connection, "legacy", "{}")
        connection.executemany(
            "UPDATE schema_metadata SET value = ? WHERE key = ?",
            ((value, key) for key, value in Database._v10_metadata().items()),
        )
        connection.commit()
    async with Database.managed(path) as database:
        assert await database.operation("normalized") == before
        legacy = await database.operation("legacy")
        assert legacy["code_provenance"] == {"commit_hash": None, "worktree_state": "unknown"}
        assert legacy["code_state_id"] != before["code_state_id"]
