"""Schema23 continuations regain logical age without changing execution evidence."""

# Genuine historical triggers, full ledgers and migration rollback are inspected.
# pyright: reportPrivateUsage=false

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from carl._tests.test_fair_storage import _seed
from carl._tests.test_stage_storage import NOW, _claim, _definition, _release_resource
from carl.core.json import encode_json
from carl.core.models import NamedInput, NamedOutput, RecordDraft
from carl.core.resources import ResourceRequest
from carl.core.stages import StageLease, StageWaitKind, WaitWork
from carl.io.db import DatabaseConnections
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
_V23_SHA256 = "a4d3c6e84c5bf3c3fe46e898fc6bd20a7b80b032883fc6e4cb57c0a72085302a"
_V23_METADATA = {
    "schema_identity_json": encode_json(
        {"namespace": "carl", "domain": "storage", "backend": "sqlite", "version": 23}
    ),
    "schema_version": "23",
    "schema_definition_sha256": _V23_SHA256,
}


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _create_v23(path: Path) -> None:
    # Exact assembled DDL from d200bbb, unchanged through base73a3b68.
    schema = (Path(__file__).parent / "data" / "schema_v23.sql").read_bytes()
    assert hashlib.sha256(schema).hexdigest() == _V23_SHA256
    assert b"THEN 0 ELSE coalesce(s.created_at_utc_ns,0)" in schema
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(schema.decode())
        connection.executemany("INSERT INTO schema_metadata VALUES (?,?)", _V23_METADATA.items())
        connection.commit()


def _snapshot(path: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    with closing(sqlite3.connect(path)) as connection:
        return {
            name: tuple(sorted(connection.execute(f'SELECT * FROM "{name}"'), key=repr))
            for (name,) in connection.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        }


def _ddl(path: Path) -> tuple[tuple[object, ...], ...]:
    with closing(sqlite3.connect(path)) as connection:
        return tuple(connection.execute("SELECT * FROM sqlite_schema ORDER BY name"))


async def _ages(database: Database) -> dict[str, int]:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute("SELECT work_item_id,ready_since_utc_ns FROM fair_ready")
        ).fetchall()
    return {str(row[0]): int(str(row[1])) for row in rows}


async def _fixture(path: Path) -> StageLease:
    _create_v23(path)
    # Opening through the deferred-schema boundary exercises the old DDL as-is.
    async with Database.managed(path, prepare_schema=False) as database, database.transaction():
        for identifier in ("publish", "running", "deadline", "retry", "initial"):
            await _seed(database, identifier, root="root", register=False)
        await database.enable_fair_scheduling("test-host", NOW)

        async def capture(identifier: str, at: int) -> StageLease:
            definition = _definition(
                identifier,
                identifier=f"capture-{identifier}",
                network=True,
                resources=(ResourceRequest(name="http"),),
            )
            await database.register_stage(definition, created_at_utc_ns=at)
            lease = (await _claim(database, definition.identifier, token=identifier, now=at)).lease
            assert lease is not None
            return lease

        first = await capture("publish", NOW + 10)
        publish = _definition(
            "publish",
            identifier="publish-stage",
            inputs=(NamedInput(name=("capture",), object_identifier="checkpoint"),),
        )
        await database.suspend_stage(
            first,
            WaitWork(continuation=publish, wait_kind=StageWaitKind.RESOURCE, result={}),
            now_utc_ns=NOW + 20,
            records=(
                RecordDraft(
                    identifier="checkpoint",
                    kind=("test", "capture"),
                    schema_version=1,
                    value={"captured": True},
                ),
            ),
            outputs=(NamedOutput(name=("capture",), object_identifier="checkpoint"),),
        )
        await _release_resource(database, first)
        running_capture = await capture("running", NOW + 30)
        running = _definition("running", identifier="running-stage")
        await database.suspend_stage(
            running_capture,
            WaitWork(continuation=running, wait_kind=StageWaitKind.RESOURCE, result={}),
            now_utc_ns=NOW + 31,
        )
        await _release_resource(database, running_capture)
        held = (await _claim(database, running.identifier, token="running-cpu", now=NOW + 32)).lease
        assert held is not None
        deadline = await capture("deadline", NOW + 40)
        await database.suspend_stage(
            deadline,
            WaitWork(
                continuation=_definition(
                    "deadline", identifier="deadline-stage", not_before=NOW + 500
                ),
                wait_kind=StageWaitKind.TIME,
                result={},
            ),
            now_utc_ns=NOW + 41,
        )
        await _release_resource(database, deadline)
        retry = await capture("retry", NOW + 50)
        async with database.transaction():
            await database.complete_stage(retry, now_utc_ns=NOW + 51, phase_outcome="retry")
            await database.retry_leased_operation(
                work_item_identifier="retry",
                lease_token=retry.token,
                worker_identifier=retry.work_lease.worker_identifier,
                utc_now_ns=lambda: NOW + 51,
                delay_ns=600,
                event_identifier="retry-event",
                operation_id=retry.definition.operation_identifier,
                reason={"kind": "custom_retry"},
                result={"state": "retrying"},
                ended_at_utc="2026-11-03T00:00:00Z",
                duration_ns=1,
            )
        await _release_resource(database, retry)
        await database.offer_fair_dispatch_candidates(
            scope="test-host", work_identifiers=("publish",), now_utc_ns=NOW + 100
        )
        assert await _ages(database) == {
            "publish": NOW + 20,
            "running": NOW + 31,
            "deadline": NOW + 500,
            "retry": NOW,
            "initial": NOW,
        }
        return held


async def test_genuine_v23_repairs_live_ages_preserving_every_other_ledger(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    held = await _fixture(path)
    before = _snapshot(path)
    assert before["records"] and before["fair_admissions"] and before["resource_permits"]
    expected = {
        "publish": NOW,
        "running": NOW,
        "deadline": NOW + 500,
        "retry": NOW + 651,
        "initial": NOW,
    }
    for _ in range(2):
        async with Database.managed(path) as database:
            assert await _ages(database) == expected
            assert await database.validate_stage(held, now_utc_ns=NOW + 100)
            assert (await database.get_record("checkpoint"))[2] == {"captured": True}
            async with database._connections.reader() as connection:
                assert await (await connection.execute("PRAGMA foreign_key_check")).fetchall() == []
        after = _snapshot(path)
        assert after["schema_metadata"] == tuple(
            sorted(Database._expected_metadata().items(), key=repr)
        )
        derived = {"fair_ready", "fair_offers", "schema_metadata"}
        assert {name: rows for name, rows in after.items() if name not in derived} == {
            name: rows for name, rows in before.items() if name not in derived
        }
        # Projection repair only changes age, including its matching live offer.
        for table in ("fair_ready", "fair_offers"):
            with closing(sqlite3.connect(path)) as connection:
                columns = [row[1] for row in connection.execute(f"PRAGMA table_info({table})")]
            age_column = columns.index("ready_since_utc_ns")
            assert tuple(
                tuple(v for i, v in enumerate(row) if i != age_column) for row in after[table]
            ) == tuple(
                tuple(v for i, v in enumerate(row) if i != age_column) for row in before[table]
            )
        with closing(sqlite3.connect(path)) as connection:
            assert connection.execute(
                "SELECT ready_since_utc_ns FROM fair_offers WHERE work_item_id='publish'"
            ).fetchone() == (NOW,)


async def test_replaced_triggers_preserve_poll_age_and_new_continuation_age(tmp_path: Path) -> None:
    path = tmp_path / "triggers.sqlite3"
    held = await _fixture(path)
    async with Database.managed(path) as database:
        # The running CPU owner makes publication poll; polling cannot reset age.
        blocked = await _claim(database, "publish-stage", token="poll", now=NOW + 100)
        assert blocked.lease is None and blocked.wait_kind is StageWaitKind.RESOURCE
        assert (await _ages(database))["publish"] == NOW
        assert await database.release_stage_execution(held, now_utc_ns=NOW + 101, reason={})
        await _release_resource(database, held)
        resumed = (
            await _claim(database, "publish-stage", token="resume", now=NOW + 200_000_000)
        ).lease
        assert resumed is not None and resumed.work_lease.attempt == 1
        await database.suspend_stage(
            resumed,
            WaitWork(
                continuation=_definition("publish", identifier="publish-again"),
                wait_kind=StageWaitKind.RESOURCE,
                result={},
            ),
            now_utc_ns=NOW + 200_000_001,
        )
        await _release_resource(database, resumed)
        assert (await _ages(database))["publish"] == NOW
        assert (await database.work("publish"))["attempt"] == 1


async def test_unknown_v23_hash_rejects_without_mutating_evidence_or_triggers(
    tmp_path: Path,
) -> None:
    path = tmp_path / "unknown.sqlite3"
    await _fixture(path)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "UPDATE schema_metadata SET value=? WHERE key='schema_definition_sha256'", ("0" * 64,)
        )
        connection.commit()
    before, ddl = _snapshot(path), _ddl(path)
    with pytest.raises(RuntimeError, match="Unsupported database schema metadata"):
        async with Database.managed(path):
            pytest.fail("Unknown schema23 hash was accepted")
    assert _snapshot(path) == before
    assert _ddl(path) == ddl


async def test_migration_preserves_explicit_and_timestamp_ambiguous_progress_wakes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "wake.sqlite3"
    held = await _fixture(path)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "UPDATE fair_ready SET ready_since_utc_ns=? WHERE work_item_id='publish'", (NOW + 75,)
        )
        connection.execute(
            "UPDATE fair_ready SET ready_since_utc_ns=? WHERE work_item_id='deadline'", (NOW,)
        )
        connection.execute(
            """INSERT INTO progress_wait_builds(
                id,work_id,operation_id,source_stage_id,source_token,continuation_id,
                stage_id,baseline,digest,groups_json,state)
            VALUES ('wake','running','operation-running','capture-running','running',
                'running-stage',NULL,0,'fixture','[]','inactive')"""
        )
        connection.commit()
    async with Database.managed(path) as database:
        ages = await _ages(database)
        assert ages["publish"] == NOW + 75
        # A released wait build can identify a wake at the stage-creation time.
        assert ages["running"] == NOW + 31
        assert ages["deadline"] == NOW + 500
        async with database._connections.reader() as connection:
            assert await (
                await connection.execute(
                    "SELECT ready_since_utc_ns FROM fair_offers WHERE work_item_id='publish'"
                )
            ).fetchall() == [(NOW + 75,)]
        await database.suspend_stage(
            held,
            WaitWork(
                continuation=_definition("running", identifier="after-wake"),
                wait_kind=StageWaitKind.RESOURCE,
                result={},
            ),
            now_utc_ns=NOW + 200,
        )
        await _release_resource(database, held)
        assert (await _ages(database))["running"] == NOW + 31


async def test_failed_upgrade_rolls_back_triggers_projection_and_metadata(tmp_path: Path) -> None:
    path = tmp_path / "rollback.sqlite3"
    await _fixture(path)
    before, ddl = _snapshot(path), _ddl(path)
    injected = False

    def fail_metadata(_cursor: object, statement: str, bindings: object) -> bool:
        nonlocal injected
        if "UPDATE schema_metadata" in statement and bindings == ("24", "schema_version"):
            injected = True
            raise RuntimeError("injected logical age migration failure")
        return True

    async with DatabaseConnections.managed(path, create=False) as connections:
        database = Database(path, connections)
        async with connections.writer() as connection:
            connection.set_exec_trace(fail_metadata)
            try:
                with pytest.raises(RuntimeError, match="injected logical age migration failure"):
                    await database.migrate()
            finally:
                connection.set_exec_trace(None)
    assert injected
    assert _snapshot(path) == before
    assert _ddl(path) == ddl
