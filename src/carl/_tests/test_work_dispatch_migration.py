"""Index-only dispatch migration preserves evidence and live ownership fences."""

# Freeze historical DDL and inspect all rows around the migration transaction.
# pyright: reportPrivateUsage=false

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

import carl.io.sqlite as sqlite
from carl._tests.test_resident_storage import _claim as _resident_claim
from carl._tests.test_resident_storage import _context, _resident
from carl._tests.test_resident_storage import _stage as _resident_stage
from carl._tests.test_stage_storage import COMPONENT, NOW, _claim, _definition, _enqueue
from carl._tests.test_worker import _provenance
from carl.core.json import encode_json
from carl.core.models import BytesDraft, ExternalFileDraft, NamedInput, NamedOutput, RecordDraft
from carl.core.resources import ResourceRequest
from carl.core.stages import StageLease, StageRequestCharge
from carl.io.db import DatabaseConnections
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio

# Frozen from ca216bbf2205f96437f548ff0769a49c0658a53f, before schema 18.
_V17_SHA256 = "237aa1054113cb8c61eb1f2d079e741eca10cbb794fac1caa2e74ca312427acf"
_V18_SHA256 = "e1451850f5bd245e419c06b205c2d4f7b2193220dbdf54b8ce122271f3bffc45"
_V17_METADATA = {
    "schema_identity_json": encode_json(
        {"namespace": "carl", "domain": "storage", "backend": "sqlite", "version": 17}
    ),
    "schema_version": "17",
    "schema_definition_sha256": _V17_SHA256,
}
_DISPATCH_INDEX = """CREATE INDEX IF NOT EXISTS work_items_dispatch
ON work_items(state, priority DESC, eligible_at_utc_ns, created_at_utc_ns, id);

"""
_EXPIRY_INDEX = """CREATE INDEX IF NOT EXISTS work_items_expired
ON work_items(state, lease_expires_at_utc_ns, id);

"""
_INDEX_COLUMNS = {
    "work_items_dispatch": (
        ("state", 0),
        ("priority", 1),
        ("eligible_at_utc_ns", 0),
        ("created_at_utc_ns", 0),
        ("id", 0),
    ),
    "work_items_expired": (("state", 0), ("lease_expires_at_utc_ns", 0), ("id", 0)),
}


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _create_v17(path: Path) -> None:
    # Reconstruct the exact prior DDL, rather than labelling current DDL as old.
    # The frozen hash catches any accidental column/table/index discrepancy and
    # avoids depending on Git history being available in an installed checkout.
    # Frozen v18 contains neither the v19 derived column nor its partial index.
    schema = (Path(__file__).parent / "data" / "schema_v18.sql").read_text()
    assert hashlib.sha256(schema.encode()).hexdigest() == _V18_SHA256
    for index in (_DISPATCH_INDEX, _EXPIRY_INDEX):
        assert schema.count(index) == 1
        schema = schema.replace(index, "")
    assert hashlib.sha256(schema.encode()).hexdigest() == _V17_SHA256
    with closing(sqlite3.connect(path)) as connection:
        _ = connection.execute("PRAGMA foreign_keys = ON")
        _ = connection.executescript(schema)
        _ = connection.executemany(
            "INSERT INTO schema_metadata(key, value) VALUES (?, ?)", _V17_METADATA.items()
        )
        connection.commit()
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert not any(
            name in _INDEX_COLUMNS
            for (name,) in connection.execute("SELECT name FROM sqlite_schema WHERE type='index'")
        )


def _rows(path: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    with closing(sqlite3.connect(path)) as connection:
        return {
            name: tuple(
                sorted(
                    connection.execute(
                        "SELECT "
                        + ",".join(
                            '"' + row[1].replace('"', '""') + '"'
                            for row in connection.execute(f'PRAGMA table_info("{name}")')
                            if row[1]
                            not in {
                                "acquisition_resource_identity_json",
                                "parent_work_identifier",
                                "scheduling_json",
                                "progress_waiting",
                            }
                        )
                        + ' FROM "'
                        + name.replace('"', '""')
                        + '"'
                    ),
                    key=repr,
                )
            )
            for (name,) in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name!='schema_metadata'"
            )
            if name not in {"work_scheduling", "work_consumer_edges", "consumer_group_progress"}
            and not name.startswith(("fair_", "work_progress_", "progress_"))
        }


def _assert_current_schema(path: Path) -> None:
    with closing(sqlite3.connect(path)) as connection:
        actual = dict(connection.execute("SELECT key, value FROM schema_metadata"))
        assert actual == Database._expected_metadata()
        assert sqlite.DATABASE_SCHEMA.version == 21
        assert actual["schema_version"] == "21"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        for name, expected in _INDEX_COLUMNS.items():
            # Verify key order and descending priority, not just index names.
            columns = tuple(
                (row[2], row[3])
                for row in connection.execute(f"PRAGMA index_xinfo('{name}')")
                if row[5]
            )
            assert columns == expected


async def _seed_live_ownership(path: Path) -> tuple[StageLease, StageLease]:
    # Use the current API to generate real lease/evidence structures, then copy
    # only the old columns into the genuinely historical schema. New adapters
    # cannot enqueue directly into an old table lacking their derived column.
    seed = path.with_suffix(".seed.sqlite3")
    async with Database.managed(seed, initialize=True) as database:
        await database.begin_operation(
            operation_id="retained-evidence",
            component=COMPONENT,
            provenance=_provenance(),
            invocation={"command": "old-command"},
            configuration={"retained": True},
            started_at_utc="2026-11-01T00:00:00+00:00",
        )
        external = b"external evidence\x00\xff"
        _ = (path.parent / "retained.bin").write_bytes(external)
        await database.complete_operation(
            operation_id="retained-evidence",
            records=(
                RecordDraft(
                    identifier="evidence",
                    kind=("test", "retained_evidence"),
                    schema_version=1,
                    value={"source": "version17", "nested": [None, True, 3]},
                ),
            ),
            artifacts=(
                BytesDraft(
                    identifier="inline",
                    kind=("test", "image"),
                    media_type="image/png",
                    representation={"legacy": True},
                    content=b"inline evidence\x00\xff",
                ),
                ExternalFileDraft(
                    identifier="external",
                    kind=("test", "image"),
                    media_type="image/png",
                    representation={"legacy": True},
                    sha256=hashlib.sha256(external).hexdigest(),
                    size=len(external),
                    locator="retained.bin",
                ),
            ),
            outputs=(
                NamedOutput(name=("record",), object_identifier="evidence"),
                NamedOutput(name=("inline",), object_identifier="inline"),
                NamedOutput(name=("external",), object_identifier="external"),
            ),
            result={"saved": True},
            ended_at_utc="2026-11-01T00:00:01+00:00",
            duration_ns=1_000_000_000,
        )
        await _enqueue(database, "work", payload={"original": "resident payload"})
        _ = await _resident_stage(database, network=True)
        resident = await _resident(database)
        assert resident is not None
        resident_stage = (await _resident_claim(database, resident)).lease
        assert resident_stage is not None
        assert await database.charge_resident_request(
            _context(resident_stage),
            request_identity=("ebay", "homepage"),
            maximum_attempts=2,
            now_utc_ns=NOW,
        ) == StageRequestCharge(ordinal=1, newly_charged=True)
        await _enqueue(database, "stateless", payload={"original": "stateless payload"})
        await database.register_stage(
            _definition(
                "stateless",
                identifier="stateless-stage",
                network=True,
                resources=(ResourceRequest(name="http"),),
                inputs=(NamedInput(name=("evidence",), object_identifier="evidence"),),
            ),
            created_at_utc_ns=NOW,
        )
        stateless_stage = (await _claim(database, "stateless-stage", token="stateless-token")).lease
        assert stateless_stage is not None
        assert await database.charge_stage_request(
            _context(stateless_stage),
            request_identity=("ebay", "description"),
            maximum_attempts=1,
            now_utc_ns=NOW,
        ) == StageRequestCharge(ordinal=1, newly_charged=True)
        await _enqueue(database, "pending")
        await database.register_stage(
            _definition("pending", identifier="pending-stage"), created_at_utc_ns=NOW
        )
        await database.publish_leased_operation_checkpoint(
            work_item_identifier="stateless",
            lease_token=stateless_stage.token,
            worker_identifier=stateless_stage.work_lease.worker_identifier,
            utc_now_ns=lambda: NOW,
            operation_id=stateless_stage.definition.operation_identifier,
            records=(
                RecordDraft(
                    identifier="checkpoint",
                    kind=("test", "checkpoint"),
                    schema_version=1,
                    value={"paid_capture": "already retained"},
                ),
            ),
            artifacts=(),
            outputs=(NamedOutput(name=("checkpoint",), object_identifier="checkpoint"),),
            checkpoint_result={"phase": "captured"},
            inputs=(NamedInput(name=("source",), object_identifier="evidence"),),
        )
    _copy_seed_to_legacy(seed, path)
    return resident_stage, stateless_stage


def _copy_seed_to_legacy(seed: Path, path: Path) -> None:
    """Copy closed fixture evidence into exact legacy DDL without relabeling it."""

    with closing(sqlite3.connect(seed)) as source, closing(sqlite3.connect(path)) as target:
        _ = target.execute("PRAGMA foreign_keys = ON")
        _ = target.execute("BEGIN")
        _ = target.execute("PRAGMA defer_foreign_keys = ON")
        tables = [
            row[0]
            for row in target.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' "
                + "AND name NOT IN ('schema_metadata','sqlite_sequence') ORDER BY name"
            )
        ]
        for name in (*tables, "sqlite_sequence"):
            columns = [row[1] for row in target.execute(f'PRAGMA table_info("{name}")')]
            assert columns
            available = {row[1] for row in source.execute(f'PRAGMA table_info("{name}")')}
            assert set(columns) <= available
            added_columns = {
                "work_items": {"acquisition_resource_identity_json"},
                "work_requests": {"parent_work_identifier", "scheduling_json"},
                "work_stages": {"progress_waiting"},
            }.get(name, set())
            assert available - set(columns) == added_columns - set(columns)
            quoted = ",".join('"' + column.replace('"', '""') + '"' for column in columns)
            if name == "sqlite_sequence":
                _ = target.execute("DELETE FROM sqlite_sequence")
            rows = source.execute(f'SELECT {quoted} FROM "{name}"')
            while batch := rows.fetchmany(128):
                if name == "sqlite_sequence":
                    batch = [row for row in batch if row[0] in tables]
                _ = target.executemany(
                    f'INSERT INTO "{name}" ({quoted}) VALUES ({",".join("?" for _ in columns)})',
                    batch,
                )
        assert target.execute("PRAGMA foreign_key_check").fetchall() == []
        target.commit()


async def test_v17_migration_preserves_all_evidence_ownership_and_request_fences(
    tmp_path: Path,
) -> None:
    path = tmp_path / "old.sqlite3"
    _create_v17(path)
    resident_stage, stateless_stage = await _seed_live_ownership(path)
    before = _rows(path)
    for table in (
        "work_items",
        "stage_attempts",
        "work_stages",
        "stage_execution_tokens",
        "stage_inputs",
        "resource_permits",
        "resource_allocations",
        "resident_sessions",
        "resident_request_charges",
        "network_activities",
        "operation_inputs",
        "operation_outputs",
        "records",
        "artifacts",
        "external_artifacts",
        "content",
    ):
        assert before[table], table
    for _ in range(2):
        async with Database.managed(path) as database:
            _assert_current_schema(path)
            assert _rows(path) == before
            assert await database.validate_stage(resident_stage, now_utc_ns=NOW)
            assert await database.validate_stage(stateless_stage, now_utc_ns=NOW)
            resident = await database.resident_session("resident")
            assert resident is not None
            assert await database.validate_resident_session(resident, now_utc_ns=NOW)
            assert resident.active_stage_token == resident_stage.token
            for stage in (resident_stage, stateless_stage):
                resource = stage.resource_lease
                assert resource is not None
                assert await database.validate_resource_permit(
                    token=resource.token, owner=resource.owner, now_utc_ns=NOW
                )
            assert await database.charge_resident_request(
                _context(resident_stage),
                request_identity=("ebay", "homepage"),
                maximum_attempts=2,
                now_utc_ns=NOW,
            ) == StageRequestCharge(ordinal=1, newly_charged=False)
            assert await database.charge_stage_request(
                _context(stateless_stage),
                request_identity=("ebay", "description"),
                maximum_attempts=1,
                now_utc_ns=NOW,
            ) == StageRequestCharge(ordinal=1, newly_charged=False)
            assert (await database.get_artifact("inline"))[1] == b"inline evidence\x00\xff"
            assert (await database.get_artifact("external"))[1] == b"external evidence\x00\xff"
        assert _rows(path) == before


async def test_fresh_and_reopened_database_have_exact_dispatch_schema(tmp_path: Path) -> None:
    path = tmp_path / "fresh.sqlite3"
    async with Database.managed(path, initialize=True):
        _assert_current_schema(path)
    async with Database.managed(path):
        _assert_current_schema(path)


async def test_unknown_v17_hash_rejected_without_schema_or_evidence_mutation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "unknown.sqlite3"
    _create_v17(path)
    _ = await _seed_live_ownership(path)
    with closing(sqlite3.connect(path)) as connection:
        _ = connection.execute(
            "UPDATE schema_metadata SET value=? WHERE key='schema_definition_sha256'",
            ("0" * 64,),
        )
        connection.commit()
        schema = connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall()
        metadata = connection.execute("SELECT * FROM schema_metadata ORDER BY key").fetchall()
    before = _rows(path)
    with pytest.raises(RuntimeError, match="Unsupported database schema metadata"):
        async with Database.managed(path):
            pytest.fail("Unknown schema hash was accepted")
    assert _rows(path) == before
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall() == schema
        assert (
            connection.execute("SELECT * FROM schema_metadata ORDER BY key").fetchall() == metadata
        )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


async def test_failed_second_index_creation_rolls_back_first_index_and_metadata(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rollback.sqlite3"
    _create_v17(path)
    before = _rows(path)

    def fail_second_index(_cursor: object, statement: str, _bindings: object) -> bool:
        if "CREATE INDEX IF NOT EXISTS work_items_expired" in statement:
            raise RuntimeError("injected index creation failure")
        return True

    async with DatabaseConnections.managed(path, create=False) as connections:
        database = Database(path, connections)
        async with connections.writer() as connection:
            connection.set_exec_trace(fail_second_index)
            try:
                with pytest.raises(RuntimeError, match="injected index creation failure"):
                    await database.migrate()
            finally:
                connection.set_exec_trace(None)
    assert _rows(path) == before
    with closing(sqlite3.connect(path)) as connection:
        assert dict(connection.execute("SELECT key, value FROM schema_metadata")) == _V17_METADATA
        assert (
            connection.execute(
                "SELECT name FROM sqlite_schema WHERE name IN ('work_items_dispatch','work_items_expired')"
            ).fetchall()
            == []
        )
    async with Database.managed(path):
        _assert_current_schema(path)
