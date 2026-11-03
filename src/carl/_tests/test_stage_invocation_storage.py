"""Analysis dispatch charges retain exact execution fences without network permits."""

# Storage assertions inspect durable authorization and exact query boundaries.
# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl._tests.test_resident_storage import _context, _resident
from carl._tests.test_stage_storage import COMPONENT, DURATION, NOW, _definition, _enqueue, _scalar
from carl._tests.test_worker import _provenance
from carl.core.residents import ResidentLease
from carl.core.resources import ResourceRequest
from carl.core.stages import StageLease, StageRequestCharge
from carl.core.work import (
    NetworkActivityAdmission,
    SchedulingScope,
    SchedulingScopeKind,
    WorkDefinition,
    WorkRequester,
)
from carl.io.sqlite import DATABASE_SCHEMA, Database, LeaseLostError

pytestmark = pytest.mark.anyio
CAPACITIES = {
    "analysis": 1,
    "http": 1,
    "cpu": 1,
    "resident_sessions": 1,
    "resident_state_bytes": 64,
}


async def _claim(
    database: Database,
    *,
    token: str = "invocation-token",
    now: int = NOW,
    resident: ResidentLease | None = None,
) -> StageLease:
    result = await database.try_claim_stage(
        stage_identifier="stage",
        token=token,
        worker_identifier="owner" if resident is None else resident.owner,
        resource_scope="test-host" if resident is None else resident.coordination_scope,
        capacities=CAPACITIES,
        lease_duration_ns=DURATION,
        now_utc_ns=now,
        component=COMPONENT,
        provenance=_provenance(),
        invocation={},
        resident_lease=resident,
    )
    assert result.lease is not None
    return result.lease


async def _initial(database: Database) -> StageLease:
    await _enqueue(database, "work")
    await database.register_stage(
        _definition(resources=(ResourceRequest(name="analysis"),)), created_at_utc_ns=NOW
    )
    return await _claim(database)


async def test_invocation_charge_survives_restart_and_conservatively_spends_recovered_execution(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        lease = await _initial(database)
        context = _context(lease)
        assert await database.charge_stage_invocation(
            context, request_identity=("analysis", "run"), maximum_attempts=2, now_utc_ns=NOW
        ) == StageRequestCharge(ordinal=1, newly_charged=True)
        assert await _scalar(database, "SELECT count(*) FROM network_activities") == 0
        assert await _scalar(database, "SELECT count(*) FROM resident_sessions") == 0
        with pytest.raises(ValueError, match="HTTP/network"):
            await database.charge_stage_request(
                context, request_identity=("analysis", "run"), maximum_attempts=2, now_utc_ns=NOW
            )
    async with Database.managed(path) as database:
        assert await database.charge_stage_invocation(
            context, request_identity=("analysis", "run"), maximum_attempts=2, now_utc_ns=NOW
        ) == StageRequestCharge(ordinal=1, newly_charged=False)
        with pytest.raises(ValueError, match="cannot change"):
            await database.charge_stage_invocation(
                context, request_identity=("analysis", "run"), maximum_attempts=3, now_utc_ns=NOW
            )
        # External recovery has proved the old physical execution guard free.
        assert await database.recover_stage_execution(lease, now_utc_ns=NOW + DURATION)
        replacement = await _claim(database, token="replacement", now=NOW + DURATION)
        assert replacement.work_lease.attempt == lease.work_lease.attempt
        assert replacement.definition.operation_identifier == lease.definition.operation_identifier
        assert await database.charge_stage_invocation(
            _context(replacement),
            request_identity=("analysis", "run"),
            maximum_attempts=2,
            now_utc_ns=NOW + DURATION,
        ) == StageRequestCharge(ordinal=2, newly_charged=True)
        assert await database.recover_stage_execution(replacement, now_utc_ns=NOW + 2 * DURATION)
        exhausted = await _claim(database, token="exhausted", now=NOW + 2 * DURATION)
        assert (
            await database.charge_stage_invocation(
                _context(exhausted),
                request_identity=("analysis", "run"),
                maximum_attempts=2,
                now_utc_ns=NOW + 2 * DURATION,
            )
            is None
        )
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 2
        assert DATABASE_SCHEMA.version == 22


@pytest.mark.parametrize(
    "invalid",
    (
        "token",
        "owner",
        "work",
        "operation",
        "attempt",
        "execution",
        "resident",
        "scope",
        "expiry",
        "network",
    ),
)
async def test_invocation_charge_rejects_stale_and_forged_contexts(
    tmp_path: Path, invalid: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        lease = await _initial(database)
        updates = {
            "token": {"lease_token": "stale"},
            "owner": {"worker_identifier": "another-owner"},
            "work": {"work_item_identifier": "other"},
            "operation": {"operation_identifier": "other"},
            "attempt": {"attempt": 2},
            "execution": {"execution": 2},
            "resident": {"resident_session_identifier": "injected"},
            "scope": {},
            "expiry": {},
            "network": {
                "network_admission": NetworkActivityAdmission(
                    activity_identifier="injected",
                    token=lease.token,
                    admitted_at_utc_ns=NOW,
                    permit_expires_at_utc_ns=NOW + DURATION,
                )
            },
        }
        if invalid == "scope":
            async with database._connections.writer() as connection:
                await connection.execute(
                    "UPDATE work_stages SET coordination_scope = 'another-host' WHERE id = 'stage'"
                )
        with pytest.raises(LeaseLostError):
            await database.charge_stage_invocation(
                _context(lease).model_copy(update=updates[invalid]),
                request_identity=("analysis", "run"),
                maximum_attempts=1,
                now_utc_ns=NOW + DURATION if invalid == "expiry" else NOW,
            )
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 0


@pytest.mark.parametrize("stage", ("cpu", "http", "network_analysis"))
async def test_invocation_requires_analysis_resource_and_no_network(
    tmp_path: Path, stage: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        resources = (
            (ResourceRequest(name="cpu"),) if stage == "cpu" else (ResourceRequest(name="http"),)
        )
        if stage == "network_analysis":
            resources += (ResourceRequest(name="analysis"),)
        await database.register_stage(
            _definition(resources=resources, network=stage != "cpu"), created_at_utc_ns=NOW
        )
        lease = await _claim(database)
        with pytest.raises(ValueError, match="offline analysis"):
            await database.charge_stage_invocation(
                _context(lease),
                request_identity=("analysis", "run"),
                maximum_attempts=1,
                now_utc_ns=NOW,
            )
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 0


async def test_invocation_requires_exact_resident_affinity_and_commits_outside_outer_writer(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        resident = await _resident(database)
        assert resident is not None
        await database.register_stage(
            _definition(resources=(ResourceRequest(name="analysis"),)).model_copy(
                update={"resident_session_identifier": resident.identifier}
            ),
            created_at_utc_ns=NOW,
        )
        lease = await _claim(database, resident=resident)
        with pytest.raises(LeaseLostError):
            await database.charge_stage_invocation(
                _context(lease).model_copy(update={"resident_session_identifier": None}),
                request_identity=("analysis", "run"),
                maximum_attempts=1,
                now_utc_ns=NOW,
            )
        async with database.transaction():
            with pytest.raises(ValueError, match="existing writer transaction"):
                await database.charge_stage_invocation(
                    _context(lease),
                    request_identity=("analysis", "run"),
                    maximum_attempts=1,
                    now_utc_ns=NOW,
                )
        assert await database.charge_stage_invocation(
            _context(lease),
            request_identity=("analysis", "run"),
            maximum_attempts=1,
            now_utc_ns=NOW,
        ) == StageRequestCharge(ordinal=1, newly_charged=True)
        # The compatibility wrapper continues to authorize HTTP only.
        with pytest.raises(ValueError, match="HTTP/network"):
            await database.charge_resident_request(
                _context(lease),
                request_identity=("analysis", "run"),
                maximum_attempts=1,
                now_utc_ns=NOW,
            )


async def test_work_definition_point_loads_large_payload_and_current_scheduling_without_history(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        definition = WorkDefinition(
            identifier="work",
            kind=("test", "analysis"),
            payload_schema_version=7,
            payload={"brief": "x" * 100_000},
            deduplication_identity=("retained", "input"),
            priority=19,
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=("test", "analysis")),
                SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=("test", "path")),
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            ),
        )
        await database.enqueue_work(
            definition,
            WorkRequester(
                request_identifier="request", kind=("test",), identifier="caller", context={}
            ),
            event_identifier="enqueue",
            enqueued_at_utc_ns=NOW,
        )
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET eligible_at_utc_ns = ? WHERE id = 'work'", (NOW + 5,)
            )
        statements: list[str] = []

        def trace(_cursor: object, statement: str, _bindings: object) -> bool:
            statements.append(statement)
            return True

        async with database._connections.reader() as connection:
            connection.set_exec_trace(trace)
            try:
                loaded = await database.work_definition("work")
            finally:
                connection.set_exec_trace(None)
            scope_query = next(
                statement for statement in statements if "FROM work_scopes" in statement
            )
            cursor = await connection.execute("EXPLAIN QUERY PLAN " + scope_query, ("work",))
            plan = " ".join(str(row[3]) for row in await cursor.fetchall())
            assert "SEARCH work_scopes" in plan and "work_item_id=?" in plan
        assert loaded == definition.model_copy(
            update={
                "not_before_utc_ns": NOW + 5,
                "scopes": tuple(sorted(definition.scopes, key=lambda scope: scope.kind.value)),
            }
        )
        assert not any(
            table in statement
            for statement in statements
            for table in (
                "work_operations",
                "work_events",
                "operations",
                "work_cancellation_requests",
            )
        )
        with pytest.raises(KeyError, match="missing"):
            await database.work_definition("missing")
