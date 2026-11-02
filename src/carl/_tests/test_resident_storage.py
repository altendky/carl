"""Resident incarnations reserve memory independently and fence stage ownership."""

# Durable token/history and migration assertions exercise the storage boundary.
# pyright: reportPrivateUsage=false

import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path

import anyio
import pytest

from carl._tests.test_stage_storage import COMPONENT, DURATION, NOW, _definition, _enqueue
from carl._tests.test_worker import _provenance
from carl.core.ebay_items import (
    COLLECT_EBAY_ITEM_WORK_KIND,
    CollectEbayItemPayload,
    EbayItemRequest,
)
from carl.core.models import NamedOutput, RecordDraft
from carl.core.residents import ResidentDefinition, ResidentLease, ResidentState
from carl.core.resources import ResourceRequest
from carl.core.stages import (
    StageAttemptContext,
    StageDefinition,
    StageLease,
    StageWaitKind,
    WaitWork,
)
from carl.core.work import WorkCapability
from carl.io.sqlite import DATABASE_SCHEMA, Database, LeaseLostError

pytestmark = pytest.mark.anyio
SCOPE = "resident-test-host"
CAPACITIES = {"http": 1, "cpu": 1, "resident_sessions": 1, "resident_state_bytes": 64}


async def _resident(
    database: Database,
    identifier: str = "resident",
    *,
    work: str = "work",
    owner: str = "resident-owner",
    scope: str = SCOPE,
    now: int = NOW,
    duration: int = DURATION,
    state_bytes: int = 64,
) -> ResidentLease | None:
    return await database.try_acquire_resident_session(
        ResidentDefinition(
            identifier=identifier,
            work_item_identifier=work,
            operation_identifier=f"operation-{work}",
            state_bytes=state_bytes,
        ),
        token=f"token-{identifier}",
        owner=owner,
        scope=scope,
        capacities=CAPACITIES,
        now_utc_ns=now,
        lease_duration_ns=duration,
    )


async def _stage(
    database: Database,
    resident: str = "resident",
    *,
    identifier: str = "stage",
    resources: tuple[ResourceRequest, ...] = (ResourceRequest(name="http"),),
    network: bool = False,
) -> StageDefinition:
    stage = _definition(identifier=identifier, resources=resources, network=network).model_copy(
        update={"resident_session_identifier": resident}
    )
    await database.register_stage(stage, created_at_utc_ns=NOW)
    return stage


async def _claim(
    database: Database,
    resident: ResidentLease | None,
    *,
    stage: str = "stage",
    token: str = "stage-token",
    now: int = NOW,
    duration: int = DURATION,
    worker: str | None = None,
):
    return await database.try_claim_stage(
        stage_identifier=stage,
        token=token,
        worker_identifier=worker or (resident.owner if resident is not None else "stage-owner"),
        resource_scope=SCOPE,
        capacities=CAPACITIES,
        lease_duration_ns=duration,
        now_utc_ns=now,
        component=COMPONENT,
        provenance=_provenance(),
        invocation={},
        resident_lease=resident,
    )


async def _scalar(database: Database, sql: str, parameters: tuple[str, ...] = ()):
    async with database._connections.reader() as connection:
        cursor = await connection.execute(sql, parameters)
        row = await cursor.fetchone()
    assert row is not None
    return row[0]


def _context(lease: StageLease) -> StageAttemptContext:
    return StageAttemptContext(
        work_item_identifier=lease.definition.work_item_identifier,
        lease_token=lease.token,
        worker_identifier=lease.work_lease.worker_identifier,
        attempt=lease.work_lease.attempt,
        operation_identifier=lease.definition.operation_identifier,
        stage_identifier=lease.definition.identifier,
        execution=lease.execution,
        network_admission=lease.network_admission,
        resident_session_identifier=lease.definition.resident_session_identifier,
    )


async def test_request_charge_is_idempotent_and_budget_configuration_is_immutable(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await _stage(database, network=True)
        resident = await _resident(database)
        assert resident is not None
        lease = (await _claim(database, resident)).lease
        assert lease is not None
        context = _context(lease)
        first = await database.charge_resident_request(
            context, request_identity=("ebay", "homepage"), maximum_attempts=2, now_utc_ns=NOW
        )
        assert first is not None and first.ordinal == 1 and first.newly_charged
        duplicate = await database.charge_resident_request(
            context, request_identity=("ebay", "homepage"), maximum_attempts=2, now_utc_ns=NOW
        )
        assert duplicate is not None and duplicate.ordinal == 1 and not duplicate.newly_charged
        with pytest.raises(ValueError, match="cannot change"):
            await database.charge_resident_request(
                context, request_identity=("ebay", "homepage"), maximum_attempts=3, now_utc_ns=NOW
            )
        page = await database.charge_resident_request(
            context, request_identity=("ebay", "page", "1"), maximum_attempts=2, now_utc_ns=NOW
        )
        assert page is not None and page.ordinal == 1 and page.newly_charged
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 2


@pytest.mark.parametrize(
    "invalid",
    ("token", "worker", "execution", "operation", "work", "resident", "network", "expiry"),
)
async def test_request_charge_rejects_stale_execution_or_resident_context(
    tmp_path: Path,
    invalid: str,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await _stage(database, network=True)
        resident = await _resident(database)
        assert resident is not None
        lease = (await _claim(database, resident)).lease
        assert lease is not None
        context = _context(lease)
        updates = {
            "token": {"lease_token": "stale"},
            "worker": {"worker_identifier": "other-owner"},
            "execution": {"execution": lease.execution + 1},
            "operation": {"operation_identifier": "other"},
            "work": {"work_item_identifier": "other"},
            "resident": {"resident_session_identifier": "other"},
            "network": {"network_admission": None},
            "expiry": {},
        }
        with pytest.raises(LeaseLostError):
            await database.charge_resident_request(
                context.model_copy(update=updates[invalid]),
                request_identity=("ebay", "homepage"),
                maximum_attempts=2,
                now_utc_ns=NOW + DURATION if invalid == "expiry" else NOW,
            )
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 0


async def test_request_charge_requires_actual_http_network_stage_and_rolls_back_on_storage_failure(
    tmp_path: Path,
) -> None:
    import apsw

    async with Database.managed(tmp_path / "cpu.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await _stage(database, resources=(ResourceRequest(name="cpu"),))
        resident = await _resident(database)
        assert resident is not None
        lease = (await _claim(database, resident)).lease
        assert lease is not None
        with pytest.raises(ValueError, match="HTTP/network"):
            await database.charge_resident_request(
                _context(lease),
                request_identity=("ebay", "homepage"),
                maximum_attempts=2,
                now_utc_ns=NOW,
            )
    async with Database.managed(tmp_path / "http.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await _stage(database, network=True)
        resident = await _resident(database)
        assert resident is not None
        lease = (await _claim(database, resident)).lease
        assert lease is not None
        async with database._connections.writer() as connection:
            await connection.execute(
                "CREATE TEMP TRIGGER fail_request_charge BEFORE INSERT ON resident_request_charges BEGIN SELECT RAISE(ABORT, 'offline charge failure'); END"
            )
        with pytest.raises(apsw.ConstraintError, match="offline charge failure"):
            await database.charge_resident_request(
                _context(lease),
                request_identity=("ebay", "homepage"),
                maximum_attempts=2,
                now_utc_ns=NOW,
            )
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 0
        async with database._connections.writer() as connection:
            await connection.execute("DROP TRIGGER fail_request_charge")
        charge = await database.charge_resident_request(
            _context(lease),
            request_identity=("ebay", "homepage"),
            maximum_attempts=2,
            now_utc_ns=NOW,
        )
        assert charge is not None and charge.ordinal == 1 and charge.newly_charged


async def test_uncertain_dispatch_budget_survives_recovery_rebind_and_database_reopen(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _enqueue(database, "work")
        await _stage(database, network=True)
        resident = await _resident(database, duration=10)
        assert resident is not None
        for ordinal in (1, 2):
            lease = (
                await _claim(
                    database,
                    resident,
                    token=f"execution-{ordinal}",
                    now=NOW + (ordinal - 1) * 10,
                    duration=100,
                )
            ).lease
            assert lease is not None
            charge = await database.charge_resident_request(
                _context(lease),
                request_identity=("ebay", "homepage"),
                maximum_attempts=2,
                now_utc_ns=NOW + (ordinal - 1) * 10,
            )
            assert charge is not None and charge.ordinal == ordinal and charge.newly_charged
            # Simulate owner death after durable dispatch charge, before any body
            # checkpoint; external resident and stage guards have been proved free.
            expired = await database.expired_resident_sessions(
                scope=SCOPE, now_utc_ns=NOW + ordinal * 10
            )
            assert len(expired) == 1 and expired[0].active_stage_token == lease.token
            assert await database.recover_resident_session(
                expired[0], now_utc_ns=NOW + ordinal * 10
            )
            previous = resident.identifier
            resident = await _resident(
                database, f"replacement-{ordinal}", now=NOW + ordinal * 10, duration=10
            )
            assert resident is not None
            assert await database.rebind_resident_stage(
                "stage", previous, resident, now_utc_ns=NOW + ordinal * 10
            )
        lease = (
            await _claim(
                database, resident, token="exhausted-execution", now=NOW + 20, duration=100
            )
        ).lease
        assert lease is not None
        assert (
            await database.charge_resident_request(
                _context(lease),
                request_identity=("ebay", "homepage"),
                maximum_attempts=2,
                now_utc_ns=NOW + 20,
            )
            is None
        )
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 2
        assert (await database.work("work"))["attempt"] == 1
    async with Database.managed(path) as database:
        assert (
            await database.charge_resident_request(
                _context(lease),
                request_identity=("ebay", "homepage"),
                maximum_attempts=2,
                now_utc_ns=NOW + 20,
            )
            is None
        )


async def test_admission_reserves_session_and_bytes_atomically_without_starting_work(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier in ("work", "other", "third"):
            await _enqueue(database, identifier)
        lease = await _resident(database, state_bytes=40)
        assert lease is not None and lease.generation == 1 and lease.state is ResidentState.OPENING
        assert lease.resource_lease.allocations == (
            ResourceRequest(name="resident_sessions"),
            ResourceRequest(name="resident_state_bytes", units=40),
        )
        assert await _resident(database, "blocked", work="other", state_bytes=25) is None
        assert (
            await _scalar(
                database, "SELECT count(*) FROM resource_permits WHERE token='token-blocked'"
            )
            == 0
        )
        assert (
            await _scalar(database, "SELECT count(*) FROM resident_sessions WHERE id='blocked'")
            == 0
        )
        # An available session slot still cannot partially reserve a byte budget.
        capacity = {**CAPACITIES, "resident_sessions": 2}
        assert await database.finish_resident_session(
            lease, state=ResidentState.CLOSED, now_utc_ns=NOW
        )
        lease = await database.try_acquire_resident_session(
            ResidentDefinition(
                identifier="larger",
                work_item_identifier="work",
                operation_identifier="operation-work",
                state_bytes=40,
            ),
            token="larger-token",
            owner="resident-owner",
            scope=SCOPE,
            capacities=capacity,
            now_utc_ns=NOW,
            lease_duration_ns=DURATION,
        )
        assert lease is not None
        assert (
            await database.try_acquire_resident_session(
                ResidentDefinition(
                    identifier="too-many-bytes",
                    work_item_identifier="other",
                    operation_identifier="operation-other",
                    state_bytes=25,
                ),
                token="blocked-token",
                owner="resident-owner",
                scope=SCOPE,
                capacities=capacity,
                now_utc_ns=NOW,
                lease_duration_ns=DURATION,
            )
            is None
        )
        assert (
            await _scalar(
                database,
                "SELECT count(*) FROM resource_allocations WHERE permit_token='blocked-token'",
            )
            == 0
        )
        work = await database.work("work")
        assert work["attempt"] == 0 and work["operations"] == [] and work["state"] == "pending"


async def test_one_live_incarnation_per_work_and_stale_fences_never_revive(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        original = await _resident(database)
        assert original is not None
        assert await _resident(database, "duplicate", scope="other-host") is None
        for changed in (
            {"token": "wrong"},
            {"owner": "wrong"},
            {"generation": 2},
            {"coordination_scope": "wrong"},
        ):
            stale = original.model_copy(update=changed)
            assert not await database.validate_resident_session(stale, now_utc_ns=NOW)
            assert (
                await database.renew_resident_session(
                    stale, now_utc_ns=NOW, lease_duration_ns=DURATION
                )
                is None
            )
            assert not await database.finish_resident_session(
                stale, state=ResidentState.CLOSED, now_utc_ns=NOW
            )
        ready = await database.transition_resident_session(
            original, state=ResidentState.READY, now_utc_ns=NOW
        )
        assert ready is not None and await database.validate_resident_session(ready, now_utc_ns=NOW)
        closing = await database.transition_resident_session(
            ready, state=ResidentState.CLOSING, now_utc_ns=NOW
        )
        assert closing is not None and not await database.validate_resident_session(
            closing, now_utc_ns=NOW
        )
        quarantined = await database.transition_resident_session(
            closing, state=ResidentState.QUARANTINED, now_utc_ns=NOW
        )
        assert quarantined is not None and await _resident(database, "still-blocked") is None
        assert (
            await database.transition_resident_session(
                quarantined, state=ResidentState.READY, now_utc_ns=NOW
            )
            is None
        )
        assert await database.finish_resident_session(
            quarantined, state=ResidentState.CLOSED, now_utc_ns=NOW
        )
        replacement = await _resident(database, "new-incarnation")
        assert replacement is not None and replacement.generation == 2
        assert (
            await database.renew_resident_session(
                original, now_utc_ns=NOW, lease_duration_ns=DURATION
            )
            is None
        )
        assert not await database.finish_resident_session(
            original, state=ResidentState.CLOSED, now_utc_ns=NOW
        )
        with pytest.raises(ValueError, match="incarnation"):
            await _resident(database)


@pytest.mark.parametrize(
    "invalid",
    (
        "missing",
        "token",
        "generation",
        "owner",
        "scope",
        "work",
        "operation",
        "closing",
        "stage_owner",
    ),
)
async def test_stage_claim_requires_exact_resident_and_rolls_back_all_admission(
    tmp_path: Path,
    invalid: str,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        stage = await _stage(database, network=True)
        resident = await _resident(database)
        assert resident is not None
        supplied: ResidentLease | None = resident
        if invalid == "missing":
            supplied = None
        elif invalid in {"token", "owner"}:
            supplied = resident.model_copy(update={invalid: "wrong"})
        elif invalid == "generation":
            supplied = resident.model_copy(update={"generation": 2})
        elif invalid == "scope":
            supplied = resident.model_copy(update={"coordination_scope": "wrong"})
        elif invalid in {"work", "operation"}:
            supplied = resident.model_copy(
                update={
                    "definition": resident.definition.model_copy(
                        update={
                            "work_item_identifier"
                            if invalid == "work"
                            else "operation_identifier": "wrong"
                        }
                    )
                }
            )
        elif invalid == "closing":
            supplied = await database.transition_resident_session(
                resident, state=ResidentState.CLOSING, now_utc_ns=NOW
            )
        result = await _claim(
            database, supplied, worker="other-worker" if invalid == "stage_owner" else None
        )
        assert result.lease is None and result.wait_kind is StageWaitKind.OWNERSHIP
        assert (await database.work("work"))["attempt"] == 0
        assert await _scalar(database, "SELECT count(*) FROM network_activities") == 0
        assert await _scalar(database, "SELECT count(*) FROM stage_execution_tokens") == 0
        assert (
            await _scalar(
                database, "SELECT count(*) FROM resource_permits WHERE token='stage-token'"
            )
            == 0
        )
        assert (
            await database.stage(stage.identifier)
        ).resident_session_identifier == resident.identifier


async def test_stage_wait_retains_resident_and_renewal_observes_current_binding(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        stage = await _stage(database, network=True)
        resident = await _resident(database)
        assert resident is not None
        claimed = (await _claim(database, resident)).lease
        assert claimed is not None
        renewed = await database.renew_resident_session(
            resident, now_utc_ns=NOW, lease_duration_ns=2 * DURATION
        )
        assert renewed is not None and renewed.active_stage_token == claimed.token
        assert await database.validate_stage(claimed, now_utc_ns=NOW)
        assert not await database.finish_resident_session(
            resident, state=ResidentState.CLOSED, now_utc_ns=NOW
        )
        continuation = stage.model_copy(
            update={
                "identifier": "next-stage",
                "network_activity": None,
                "resources": (ResourceRequest(name="cpu"),),
            }
        )
        await database.suspend_stage(
            claimed,
            WaitWork(continuation=continuation, wait_kind=StageWaitKind.RESOURCE, result={}),
            now_utc_ns=NOW,
        )
        assert await database.release_resource_permit(
            token=claimed.token, owner=claimed.work_lease.worker_identifier
        )
        current = await database.resident_session(resident.identifier)
        assert current is not None and current.active_stage_token is None
        assert await database.validate_resident_session(current, now_utc_ns=NOW)
        next_claim = (await _claim(database, current, stage="next-stage", token="next-token")).lease
        assert (
            next_claim is not None
            and next_claim.work_lease.attempt == claimed.work_lease.attempt == 1
        )
        assert next_claim.definition.operation_identifier == claimed.definition.operation_identifier
        assert not await database.validate_stage(claimed, now_utc_ns=NOW)
        await database.complete_stage(next_claim, now_utc_ns=NOW)
        finished = await database.resident_session(resident.identifier)
        assert finished is not None and finished.active_stage_token is None


async def test_resident_expiry_fences_publication_and_generic_recovery_cannot_free_it(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await _enqueue(database, "other")
        await _stage(database)
        resident = await _resident(database, duration=10)
        assert resident is not None
        stage = (await _claim(database, resident, duration=100)).lease
        assert stage is not None
        assert not await database.validate_stage(stage, now_utc_ns=NOW + 10)
        assert (
            await database.renew_resident_session(
                resident, now_utc_ns=NOW + 10, lease_duration_ns=10
            )
            is None
        )
        assert await _resident(database, "blocked", work="other", now=NOW + 10) is None
        assert not await database.release_resource_permit(
            token=resident.token, owner=resident.owner
        )
        assert not await database.reclaim_expired_resource_permit(
            token=resident.token, owner=resident.owner, now_utc_ns=NOW + 10
        )
        assert await database.expired_resource_permits(scope=SCOPE, now_utc_ns=NOW + 10) == ()
        expired = await database.expired_resident_sessions(scope=SCOPE, now_utc_ns=NOW + 10)
        assert len(expired) == 1 and expired[0].active_stage_token == stage.token
        assert (
            await database.expired_resident_sessions(scope="other-host", now_utc_ns=NOW + 10) == ()
        )
        # Recovery requires the exact captured binding, after external guard proof.
        assert not await database.recover_resident_session(resident, now_utc_ns=NOW + 10)
        assert await database.recover_resident_session(expired[0], now_utc_ns=NOW + 10)
        recovered = await database.resident_session(resident.identifier)
        assert (
            recovered is not None
            and recovered.state is ResidentState.LOST
            and recovered.active_stage_token is None
        )
        assert (
            await _scalar(
                database, "SELECT state FROM resource_permits WHERE token=?", (resident.token,)
            )
            == "reclaimed"
        )
        assert (
            await _scalar(
                database, "SELECT state FROM resource_permits WHERE token=?", (stage.token,)
            )
            == "released"
        )
        replacement = await _resident(database, "replacement", now=NOW + 10)
        assert replacement is not None and replacement.generation == 2
        assert await database.rebind_resident_stage(
            "stage", resident.identifier, replacement, now_utc_ns=NOW + 10
        )
        assert (await database.stage("stage")).resident_session_identifier == replacement.identifier
        resumed = (await _claim(database, replacement, token="resumed", now=NOW + 10)).lease
        assert resumed is not None and resumed.work_lease.attempt == 1
        assert not await database.validate_stage(stage, now_utc_ns=NOW + 10)


async def test_live_parked_rebind_and_running_rebind_are_rejected(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await _stage(database)
        resident = await _resident(database)
        assert resident is not None
        assert not await database.rebind_resident_stage(
            "stage", resident.identifier, resident, now_utc_ns=NOW
        )
        assert await database.finish_resident_session(
            resident, state=ResidentState.PARKED, now_utc_ns=NOW
        )
        replacement = await _resident(database, "replacement")
        assert replacement is not None
        assert not await database.rebind_resident_stage(
            "stage", "wrong", replacement, now_utc_ns=NOW
        )
        assert await database.rebind_resident_stage(
            "stage", resident.identifier, replacement, now_utc_ns=NOW
        )
        assert (await _claim(database, replacement)).lease is not None
        assert not await database.rebind_resident_stage(
            "stage", resident.identifier, replacement, now_utc_ns=NOW
        )


async def test_resource_shortage_never_leaves_a_stage_bound_to_its_resident(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await _stage(database)
        resident = await _resident(database)
        assert resident is not None
        blocker = await database.try_acquire_resource_permit(
            scope=SCOPE,
            token="http-blocker",
            owner="blocker",
            requests=(ResourceRequest(name="http"),),
            capacities=CAPACITIES,
            now_utc_ns=NOW,
            lease_duration_ns=DURATION,
        )
        assert blocker is not None
        claim = await _claim(database, resident)
        assert claim.lease is None and claim.wait_kind is StageWaitKind.RESOURCE
        current = await database.resident_session(resident.identifier)
        assert current is not None and current.active_stage_token is None
        assert (await database.work("work"))["attempt"] == 0


async def test_cross_process_resident_capacity_and_reopen_keep_fences(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _enqueue(database, "work")
        await _enqueue(database, "other")
        resident = await _resident(database)
        assert resident is not None
        source = """import sys, anyio
from pathlib import Path
from carl.core.residents import ResidentDefinition
from carl.io.sqlite import Database
async def main():
    async with Database.managed(Path(sys.argv[1])) as db:
        lease = await db.try_acquire_resident_session(ResidentDefinition(identifier='child',work_item_identifier='other',operation_identifier='operation-other',state_bytes=1),token='child-token',owner='child-owner',scope='resident-test-host',capacities={'http':1,'cpu':1,'resident_sessions':1,'resident_state_bytes':64},now_utc_ns=1800000000000000000,lease_duration_ns=10000000000)
        print('admitted' if lease is not None else 'blocked')
anyio.run(main, backend='trio')
"""
        child = await anyio.run_process((sys.executable, "-c", source, str(path)))
        assert child.stdout == b"blocked\n"
    async with Database.managed(path) as database:
        assert await database.resident_session(resident.identifier) == resident
        assert await _resident(database, "after-reopen", work="other") is None


async def test_v15_migration_preserves_stage_and_resource_ownership(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _enqueue(database, "work")
        stage = _definition()
        await database.register_stage(stage, created_at_utc_ns=NOW)
        old_metadata = database._v15_metadata()
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE resident_request_charges")
        connection.execute("DROP TABLE resident_events")
        connection.execute("DROP TABLE resident_sessions")
        for key, value in old_metadata.items():
            connection.execute("UPDATE schema_metadata SET value=? WHERE key=?", (value, key))
    async with Database.managed(path) as database:
        assert await database.stage(stage.identifier) == stage
        assert await _scalar(
            database, "SELECT value FROM schema_metadata WHERE key='schema_version'"
        ) == str(DATABASE_SCHEMA.version)
        assert await _resident(database) is not None


async def test_request_charge_refuses_an_outer_writer_before_any_authorization(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await _stage(database, network=True)
        resident = await _resident(database)
        assert resident is not None
        lease = (await _claim(database, resident)).lease
        assert lease is not None
        async with database.transaction():
            with pytest.raises(ValueError, match="existing writer transaction"):
                await database.charge_resident_request(
                    _context(lease),
                    request_identity=("ebay", "item"),
                    maximum_attempts=7,
                    now_utc_ns=NOW,
                )
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 0


async def test_prior_item_request_budget_counts_uncertain_dispatches_without_double_counting(
    tmp_path: Path,
) -> None:
    stamp = datetime.fromtimestamp(NOW / 1_000_000_000, UTC).isoformat()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(
            database,
            "work",
            kind=COLLECT_EBAY_ITEM_WORK_KIND,
            payload=CollectEbayItemPayload(
                request=EbayItemRequest(item_identifier="123456789012")
            ).as_json(),
        )
        old = await database.claim_work(
            supported_capabilities=(
                WorkCapability(kind=COLLECT_EBAY_ITEM_WORK_KIND, payload_schema_version=1),
            ),
            worker_identifier="legacy-owner",
            lease_token="legacy-token",
            lease_duration_ns=DURATION,
            utc_now_ns=lambda: NOW,
            event_identifier="claim-legacy",
        )
        assert old.lease is not None
        await database.begin_leased_operation(
            work_item_identifier="work",
            lease_token="legacy-token",
            worker_identifier="legacy-owner",
            lease_duration_ns=DURATION,
            utc_now_ns=lambda: NOW,
            event_identifier="begin-legacy",
            operation_id="legacy-operation",
            component=COMPONENT,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=stamp,
        )
        await database.retry_leased_operation(
            work_item_identifier="work",
            lease_token="legacy-token",
            worker_identifier="legacy-owner",
            utc_now_ns=lambda: NOW,
            delay_ns=0,
            event_identifier="retry-legacy",
            operation_id="legacy-operation",
            reason={},
            result={},
            ended_at_utc=stamp,
            duration_ns=0,
            records=tuple(
                RecordDraft(
                    identifier=f"legacy-response-{index}",
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value={"purpose": "ebay_item"},
                )
                for index in range(2)
            ),
            outputs=tuple(
                NamedOutput(
                    name=("legacy", str(index)), object_identifier=f"legacy-response-{index}"
                )
                for index in range(2)
            ),
        )
        stage = await _stage(database, network=True)
        resident = await _resident(database)
        assert resident is not None
        first = (await _claim(database, resident)).lease
        assert first is not None and first.work_lease.attempt == 2
        assert (
            await database.charge_resident_request(
                _context(first),
                request_identity=("ebay", "item"),
                maximum_attempts=5,
                now_utc_ns=NOW,
            )
            is not None
        )
        await database.publish_leased_operation_checkpoint(
            work_item_identifier="work",
            lease_token=first.token,
            worker_identifier=first.work_lease.worker_identifier,
            utc_now_ns=lambda: NOW,
            operation_id=stage.operation_identifier,
            records=(
                RecordDraft(
                    identifier="charged-response",
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value={"purpose": "ebay_item"},
                ),
            ),
            artifacts=(),
            outputs=(
                NamedOutput(name=("charged_response",), object_identifier="charged-response"),
            ),
            checkpoint_result={},
        )
        continuation = stage.model_copy(update={"identifier": "second-request"})
        await database.suspend_stage(
            first,
            WaitWork(continuation=continuation, wait_kind=StageWaitKind.RESOURCE, result={}),
            now_utc_ns=NOW,
        )
        assert await database.release_resource_permit(
            token=first.token, owner=first.work_lease.worker_identifier
        )
        second = (
            await _claim(
                database, resident, stage=continuation.identifier, token="second-request-token"
            )
        ).lease
        assert second is not None and second.work_lease.attempt == 2
        charge = await database.charge_resident_request(
            _context(second), request_identity=("ebay", "item"), maximum_attempts=5, now_utc_ns=NOW
        )
        assert charge is not None and charge.ordinal == 2
        # The second charged invocation produced no retained response before retry.
        async with database.transaction():
            await database.complete_stage(second, now_utc_ns=NOW)
            await database.retry_leased_operation(
                work_item_identifier="work",
                lease_token=second.token,
                worker_identifier=second.work_lease.worker_identifier,
                utc_now_ns=lambda: NOW,
                delay_ns=0,
                event_identifier="retry-charged",
                operation_id=stage.operation_identifier,
                reason={},
                result={},
                ended_at_utc=stamp,
                duration_ns=0,
            )
        assert len(await database.retained_ebay_item_acquisitions("work", before_attempt=3)) == 3
        assert await database.retained_ebay_item_request_attempts("work", before_attempt=3) == 4
        assert await database.retained_ebay_item_request_attempts("work", before_attempt=2) == 2
        assert (
            await database.retained_ebay_item_request_attempts(
                "work", before_attempt=3, retry_budget_start_attempt=1
            )
            == 2
        )
        assert (
            await database.retained_ebay_item_request_attempts(
                "work", before_attempt=3, retry_budget_start_attempt=2
            )
            == 0
        )
        with pytest.raises(ValueError, match="bounds"):
            await database.retained_ebay_item_request_attempts(
                "work", before_attempt=2, retry_budget_start_attempt=2
            )
