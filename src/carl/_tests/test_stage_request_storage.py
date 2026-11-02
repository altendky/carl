"""Durable HTTP dispatch allowances work with and without resident affinity."""

# Storage and migration assertions inspect the exact retained ledger.
# pyright: reportPrivateUsage=false

import sqlite3
from pathlib import Path

import apsw
import pytest

from carl._tests.test_resident_storage import (
    _claim as _resident_claim,
)
from carl._tests.test_resident_storage import (
    _context,
    _resident,
)
from carl._tests.test_resident_storage import (
    _stage as _resident_stage,
)
from carl._tests.test_stage_storage import (
    DURATION,
    NOW,
    _claim,
    _definition,
    _enqueue,
    _release_resource,
    _scalar,
)
from carl.core.ebay_items import COLLECT_EBAY_ITEM_WORK_KIND
from carl.core.residents import ResidentRequestCharge
from carl.core.resources import ResourceRequest
from carl.core.stages import StageRequestCharge
from carl.io.sqlite import DATABASE_SCHEMA, Database, LeaseLostError

pytestmark = pytest.mark.anyio

_V16_CHARGES = """
CREATE TABLE resident_request_charges (
    operation_id TEXT NOT NULL,
    request_identity_json TEXT NOT NULL CHECK (json_valid(request_identity_json)),
    stage_token TEXT NOT NULL,
    stage_id TEXT NOT NULL,
    execution INTEGER NOT NULL CHECK (execution >= 1),
    resident_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    maximum_attempts INTEGER NOT NULL CHECK (maximum_attempts >= 1 AND ordinal <= maximum_attempts),
    charged_at_utc_ns INTEGER NOT NULL CHECK (charged_at_utc_ns >= 0),
    PRIMARY KEY (operation_id, request_identity_json, stage_token),
    UNIQUE (operation_id, request_identity_json, ordinal),
    FOREIGN KEY (operation_id) REFERENCES operations(id),
    FOREIGN KEY (stage_token) REFERENCES stage_execution_tokens(token),
    FOREIGN KEY (stage_id) REFERENCES work_stages(id),
    FOREIGN KEY (resident_id) REFERENCES resident_sessions(id)
) STRICT, WITHOUT ROWID;
"""


async def test_stateless_charge_commits_once_and_preserves_budget_after_recovery(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(
            _definition(network=True, resources=(ResourceRequest(name="http"),)),
            created_at_utc_ns=NOW,
        )
        lease = (await _claim(database, duration=10)).lease
        assert lease is not None
        context = _context(lease)
        first = await database.charge_stage_request(
            context, request_identity=("ebay", "description"), maximum_attempts=1, now_utc_ns=NOW
        )
        assert first == StageRequestCharge(ordinal=1, newly_charged=True)
        assert ResidentRequestCharge is StageRequestCharge
        assert await _scalar(database, "SELECT resident_id FROM resident_request_charges") is None
        assert await _scalar(database, "SELECT count(*) FROM resident_sessions") == 0
        with pytest.raises(LeaseLostError, match="resident ownership"):
            await database.charge_resident_request(
                context,
                request_identity=("ebay", "description"),
                maximum_attempts=1,
                now_utc_ns=NOW,
            )
    async with Database.managed(path) as database:
        duplicate = await database.charge_stage_request(
            context, request_identity=("ebay", "description"), maximum_attempts=1, now_utc_ns=NOW
        )
        assert duplicate == StageRequestCharge(ordinal=1, newly_charged=False)
        with pytest.raises(ValueError, match="cannot change"):
            await database.charge_stage_request(
                context,
                request_identity=("ebay", "description"),
                maximum_attempts=2,
                now_utc_ns=NOW,
            )
        # The caller has proved the old physical guard free after owner death.
        assert await database.recover_stage_execution(lease, now_utc_ns=NOW + 10)
        replacement = (await _claim(database, token="replacement", now=NOW + 10)).lease
        assert replacement is not None
        assert replacement.work_lease.attempt == lease.work_lease.attempt
        assert replacement.definition.operation_identifier == lease.definition.operation_identifier
        assert (
            await database.charge_stage_request(
                _context(replacement),
                request_identity=("ebay", "description"),
                maximum_attempts=1,
                now_utc_ns=NOW + 10,
            )
            is None
        )
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 1


@pytest.mark.parametrize(
    "invalid",
    (
        "token",
        "worker",
        "execution",
        "operation",
        "work",
        "attempt",
        "resident",
        "network",
        "expiry",
        "scope",
    ),
)
async def test_stateless_charge_rejects_stale_or_forged_execution(
    tmp_path: Path, invalid: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(
            _definition(network=True, resources=(ResourceRequest(name="http"),)),
            created_at_utc_ns=NOW,
        )
        lease = (await _claim(database)).lease
        assert lease is not None
        updates = {
            "token": {"lease_token": "other"},
            "worker": {"worker_identifier": "other"},
            "execution": {"execution": lease.execution + 1},
            "operation": {"operation_identifier": "other"},
            "work": {"work_item_identifier": "other"},
            "attempt": {"attempt": lease.work_lease.attempt + 1},
            "resident": {"resident_session_identifier": "injected-resident"},
            "network": {"network_admission": None},
            "expiry": {},
            "scope": {},
        }
        if invalid == "scope":
            async with database._connections.writer() as connection:
                await connection.execute(
                    "UPDATE work_stages SET coordination_scope = 'another-host' WHERE id = 'stage'"
                )
        with pytest.raises(LeaseLostError):
            await database.charge_stage_request(
                _context(lease).model_copy(update=updates[invalid]),
                request_identity=("http", "item"),
                maximum_attempts=1,
                now_utc_ns=NOW + DURATION if invalid == "expiry" else NOW,
            )
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 0


@pytest.mark.parametrize("resource", ("cpu", "http"))
async def test_generic_charge_requires_network_and_cannot_authorize_in_outer_transaction(
    tmp_path: Path, resource: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(
            _definition(resources=(ResourceRequest(name=resource),)), created_at_utc_ns=NOW
        )
        lease = (await _claim(database)).lease
        assert lease is not None
        with pytest.raises(ValueError, match="HTTP/network"):
            await database.charge_stage_request(
                _context(lease), request_identity=("request",), maximum_attempts=1, now_utc_ns=NOW
            )
        async with database.transaction():
            with pytest.raises(ValueError, match="existing writer transaction"):
                await database.charge_stage_request(
                    _context(lease),
                    request_identity=("request",),
                    maximum_attempts=1,
                    now_utc_ns=NOW,
                )
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 0


async def test_generic_charge_requires_exact_resident_affinity_when_present(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await _resident_stage(database, network=True)
        resident = await _resident(database)
        assert resident is not None
        lease = (await _resident_claim(database, resident)).lease
        assert lease is not None
        with pytest.raises(LeaseLostError):
            await database.charge_stage_request(
                _context(lease).model_copy(update={"resident_session_identifier": None}),
                request_identity=("request",),
                maximum_attempts=1,
                now_utc_ns=NOW,
            )
        charge = await database.charge_stage_request(
            _context(lease), request_identity=("request",), maximum_attempts=1, now_utc_ns=NOW
        )
        assert charge is not None and charge.newly_charged
        duplicate = await database.charge_resident_request(
            _context(lease), request_identity=("request",), maximum_attempts=1, now_utc_ns=NOW
        )
        assert duplicate is not None and not duplicate.newly_charged


async def test_failed_stateless_charge_does_not_spend_allowance(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(
            _definition(network=True, resources=(ResourceRequest(name="http"),)),
            created_at_utc_ns=NOW,
        )
        lease = (await _claim(database)).lease
        assert lease is not None
        async with database._connections.writer() as connection:
            await connection.execute(
                "CREATE TEMP TRIGGER fail_charge BEFORE INSERT ON resident_request_charges BEGIN SELECT RAISE(ABORT, 'offline failure'); END"
            )
        with pytest.raises(apsw.ConstraintError, match="offline failure"):
            await database.charge_stage_request(
                _context(lease), request_identity=("request",), maximum_attempts=1, now_utc_ns=NOW
            )
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 0
        async with database._connections.writer() as connection:
            await connection.execute("DROP TRIGGER fail_charge")
        charge = await database.charge_stage_request(
            _context(lease), request_identity=("request",), maximum_attempts=1, now_utc_ns=NOW
        )
        assert charge is not None and charge.ordinal == 1 and charge.newly_charged


async def test_v16_migration_retains_live_request_fences_and_prior_item_budget(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _enqueue(database, "work", kind=COLLECT_EBAY_ITEM_WORK_KIND)
        await _resident_stage(database, network=True)
        resident = await _resident(database)
        assert resident is not None
        lease = (await _resident_claim(database, resident)).lease
        assert lease is not None
        for ordinal in (1, 2):
            charge = await database.charge_resident_request(
                _context(lease),
                request_identity=("ebay", "item"),
                maximum_attempts=2,
                now_utc_ns=NOW,
            )
            assert charge == StageRequestCharge(ordinal=ordinal, newly_charged=True)
            if ordinal == 1:
                assert await database.release_stage_execution(lease, now_utc_ns=NOW, reason={})
                await _release_resource(database, lease)
                lease = (await _resident_claim(database, resident, token="second-charge")).lease
                assert lease is not None
        context = _context(lease)
        assert await database.charge_resident_request(
            context, request_identity=("ebay", "homepage"), maximum_attempts=1, now_utc_ns=NOW
        ) == StageRequestCharge(ordinal=1, newly_charged=True)
        old_metadata = database._v16_metadata()
    # Reconstruct the exact prior table; all other schema objects are unchanged.
    with sqlite3.connect(path) as connection:
        original = connection.execute(
            "SELECT * FROM resident_request_charges ORDER BY request_identity_json, ordinal"
        ).fetchall()
        connection.execute("ALTER TABLE resident_request_charges RENAME TO current_charges")
        connection.execute(_V16_CHARGES)
        connection.execute("INSERT INTO resident_request_charges SELECT * FROM current_charges")
        connection.execute("DROP TABLE current_charges")
        for key, value in old_metadata.items():
            connection.execute("UPDATE schema_metadata SET value=? WHERE key=?", (value, key))
    async with Database.managed(path) as database:
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT * FROM resident_request_charges ORDER BY request_identity_json, ordinal"
            )
            assert [tuple(row) for row in await cursor.fetchall()] == original
            cursor = await connection.execute("PRAGMA foreign_key_check")
            assert await cursor.fetchall() == []
        assert await _scalar(
            database, "SELECT value FROM schema_metadata WHERE key='schema_version'"
        ) == str(DATABASE_SCHEMA.version)
        for identity, ordinal, maximum in ((("ebay", "item"), 2, 2), (("ebay", "homepage"), 1, 1)):
            duplicate = await database.charge_stage_request(
                context, request_identity=identity, maximum_attempts=maximum, now_utc_ns=NOW
            )
            assert duplicate == StageRequestCharge(ordinal=ordinal, newly_charged=False)
        with pytest.raises(ValueError, match="cannot change"):
            await database.charge_stage_request(
                context, request_identity=("ebay", "item"), maximum_attempts=3, now_utc_ns=NOW
            )
        assert await database.retained_ebay_item_request_attempts("work", before_attempt=2) == 2
        assert await database.release_stage_execution(lease, now_utc_ns=NOW, reason={})
        await _release_resource(database, lease)
        replacement = (await _resident_claim(database, resident, token="replacement")).lease
        assert replacement is not None
        assert (
            await database.charge_stage_request(
                _context(replacement),
                request_identity=("ebay", "item"),
                maximum_attempts=2,
                now_utc_ns=NOW,
            )
            is None
        )
        # The migrated nullable FK also accepts a newly admitted stateless stage.
        await _enqueue(database, "stateless")
        await database.register_stage(
            _definition(
                "stateless",
                identifier="stateless-stage",
                network=True,
                resources=(ResourceRequest(name="http"),),
            ),
            created_at_utc_ns=NOW,
        )
        stateless = (await _claim(database, "stateless-stage", token="stateless-token")).lease
        assert stateless is not None
        assert await database.charge_stage_request(
            _context(stateless), request_identity=("image",), maximum_attempts=1, now_utc_ns=NOW
        ) == StageRequestCharge(ordinal=1, newly_charged=True)
