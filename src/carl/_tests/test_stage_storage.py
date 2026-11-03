"""Durable stage admission preserves attempt identity and publication fences."""

# Storage assertions inspect the stage ledger and historical schema metadata.
# pyright: reportPrivateUsage=false

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import anyio
import apsw
import pytest

from carl._tests.legacy_schema import without_fair_schema
from carl._tests.test_worker import _provenance
from carl.core.components import Component, ComponentId
from carl.core.models import CodeProvenance, JsonValue, NamedInput, NamedOutput, RecordDraft
from carl.core.resources import ResourceRequest
from carl.core.stages import StageClaimResult, StageDefinition, StageLease, StageWaitKind, WaitWork
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.io.network_activity import network_activity_definition
from carl.io.sqlite import _SCHEMA, DATABASE_SCHEMA, Database, LeaseLostError

NOW = 1_800_000_000_000_000_000
DURATION = 10_000_000_000
KIND = ("test", "logical_stage_work")
CAPABILITY = WorkCapability(kind=KIND, payload_schema_version=1)
COMPONENT = Component(ComponentId(("test", "stage_execution")), 1, lambda: None)
CAPACITIES = {"http": 1, "cpu": 1}


async def _enqueue(
    database: Database,
    identifier: str,
    *,
    kind: tuple[str, ...] = KIND,
    payload: dict[str, JsonValue] | None = None,
) -> None:
    await database.enqueue_work(
        WorkDefinition(
            identifier=identifier,
            kind=kind,
            payload_schema_version=1,
            payload={} if payload is None else payload,
            deduplication_identity=(identifier,),
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
            ),
        ),
        WorkRequester(
            request_identifier=f"request-{identifier}",
            kind=("test",),
            identifier=identifier,
            context={},
        ),
        event_identifier=f"enqueue-{identifier}",
        enqueued_at_utc_ns=NOW,
    )


def _definition(
    work: str = "work",
    *,
    identifier: str = "stage",
    network: bool = False,
    resources: tuple[ResourceRequest, ...] = (ResourceRequest(name="cpu"),),
    not_before: int = 0,
    dependencies: tuple[str, ...] = (),
    inputs: tuple[NamedInput, ...] = (),
) -> StageDefinition:
    operation = f"operation-{work}"
    return StageDefinition(
        identifier=identifier,
        work_item_identifier=work,
        operation_identifier=operation,
        kind=("test", "stage"),
        payload={"step": identifier},
        resources=resources,
        not_before_utc_ns=not_before,
        dependency_work_identifiers=dependencies,
        checkpoint_inputs=inputs,
        network_activity=(
            network_activity_definition(
                identifier=f"activity-{identifier}",
                kind=("test", "stage_http"),
                operation_identifier=operation,
                network_session_identifier="session",
                network_path=("test", "route"),
            )
            if network
            else None
        ),
    )


async def _claim(
    database: Database,
    identifier: str = "stage",
    *,
    token: str = "first-token",
    now: int = NOW,
    duration: int = DURATION,
    provenance: CodeProvenance | None = None,
) -> StageClaimResult:
    return await database.try_claim_stage(
        stage_identifier=identifier,
        token=token,
        worker_identifier=f"owner-{token}",
        resource_scope="test-host",
        capacities=CAPACITIES,
        lease_duration_ns=duration,
        now_utc_ns=now,
        component=COMPONENT,
        provenance=_provenance() if provenance is None else provenance,
        invocation={"kind": "test"},
    )


async def _scalar(database: Database, sql: str, parameters: tuple[str, ...] = ()) -> object:
    async with database._connections.reader() as connection:
        cursor = await connection.execute(sql, parameters)
        row = await cursor.fetchone()
        assert row is not None
        return row[0]


async def _release_resource(database: Database, lease: StageLease) -> None:
    if lease.resource_lease is not None:
        assert await database.release_resource_permit(
            token=lease.token, owner=lease.resource_lease.owner
        )


@pytest.mark.anyio
async def test_registration_and_blocked_resource_admission_do_not_start_attempts(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        definition = _definition(
            resources=(ResourceRequest(name="http"), ResourceRequest(name="cpu"))
        )
        await database.register_stage(definition, created_at_utc_ns=NOW)
        await database.register_stage(definition, created_at_utc_ns=NOW)
        assert await database.stage("stage") == definition
        blocker = await database.try_acquire_resource_permit(
            scope="test-host",
            token="blocker",
            owner="blocker",
            requests=(ResourceRequest(name="http"),),
            capacities=CAPACITIES,
            now_utc_ns=NOW,
            lease_duration_ns=DURATION,
        )
        assert blocker is not None
        blocked = await _claim(database)
        assert blocked.lease is None
        assert blocked.wait_kind is StageWaitKind.RESOURCE
        assert (await database.work("work"))["attempt"] == 0
        assert await _scalar(database, "SELECT count(*) FROM operations") == 0
        assert await _scalar(database, "SELECT count(*) FROM stage_execution_tokens") == 0
        assert (
            await _scalar(
                database,
                "SELECT count(*) FROM resource_allocations WHERE permit_token = 'first-token'",
            )
            == 0
        )
        legacy = await database.claim_work(
            supported_capabilities=(CAPABILITY,),
            worker_identifier="legacy",
            lease_token="legacy",
            lease_duration_ns=DURATION,
            utc_now_ns=lambda: NOW,
            event_identifier="legacy-claim",
        )
        assert legacy.lease is None
        await _enqueue(database, "unrelated")
        legacy = await database.claim_work(
            supported_capabilities=(CAPABILITY,),
            worker_identifier="legacy",
            lease_token="legacy",
            lease_duration_ns=DURATION,
            utc_now_ns=lambda: NOW,
            event_identifier="other-claim",
        )
        assert legacy.lease is not None
        assert legacy.lease.work_item_identifier == "unrelated"
        assert await database.release_resource_permit(token="blocker", owner="blocker")
        admitted = await _claim(database)
        assert admitted.lease is not None
        assert admitted.lease.work_lease.attempt == 1


@pytest.mark.anyio
async def test_http_holdoff_rollback_preserves_eligibility_and_all_reservations(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        definition = _definition(network=True, resources=(ResourceRequest(name="http"),))
        assert definition.network_activity is not None
        await database.register_constraint(
            UniformHoldoffConstraint(
                identifier=("test", "holdoff"),
                subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
                scope=SchedulingScope(
                    kind=SchedulingScopeKind.NETWORK_PATH, identity=("test", "route")
                ),
                minimum_ns=1_000_000_000,
                maximum_ns=1_000_000_000,
            ),
            registered_at_utc_ns=NOW,
        )
        await database.register_stage(definition, created_at_utc_ns=NOW)
        blocked = await _claim(database)
        assert blocked.wait_kind is StageWaitKind.TIME
        assert blocked.next_eligible_at_utc_ns == NOW + 1_000_000_000
        assert (await database.work("work"))["attempt"] == 0
        for table in (
            "operations",
            "network_activities",
            "resource_permits",
            "stage_execution_tokens",
        ):
            assert await _scalar(database, f"SELECT count(*) FROM {table}") == 0
        admitted = await _claim(database, now=NOW + 1_000_000_000)
        assert admitted.lease is not None
        assert admitted.lease.network_admission is not None
        assert (
            admitted.lease.network_admission.activity_identifier
            == "activity-stage:stage:stage:execution:1"
        )
        assert await _scalar(database, "SELECT count(*) FROM operations") == 1


@pytest.mark.anyio
async def test_continuation_preserves_attempt_operation_inputs_and_initial_rate_budget(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_constraint(
            SlidingWindowRateConstraint(
                identifier=("test", "logical_starts"),
                scope=SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=KIND),
                maximum_starts=1,
                period_ns=60_000_000_000,
            ),
            registered_at_utc_ns=NOW,
        )
        await database.register_stage(_definition(), created_at_utc_ns=NOW)
        first = (await _claim(database)).lease
        assert first is not None
        second_definition = _definition(
            identifier="second",
            inputs=(NamedInput(name=("checkpoint",), object_identifier="checkpoint"),),
        )
        await database.suspend_stage(
            first,
            WaitWork(
                continuation=second_definition,
                wait_kind=StageWaitKind.TIME,
                result={"stage": "first_done"},
            ),
            now_utc_ns=NOW + 1,
            records=(
                RecordDraft(
                    identifier="checkpoint",
                    kind=("test", "checkpoint"),
                    schema_version=1,
                    value={"part": 1},
                ),
            ),
            outputs=(NamedOutput(name=("checkpoint",), object_identifier="checkpoint"),),
        )
        await _release_resource(database, first)
        assert not await database.validate_stage(first, now_utc_ns=NOW + 2)
        resumed_provenance = _provenance().model_copy(
            update={"commit_hash": "1" * 40, "worktree_state": "dirty"}
        )
        second = (
            await _claim(
                database, "second", token="second-token", now=NOW + 2, provenance=resumed_provenance
            )
        ).lease
        assert second is not None
        assert second.work_lease.attempt == first.work_lease.attempt == 1
        assert second.definition.operation_identifier == first.definition.operation_identifier
        assert (await database.work("work"))["attempt"] == 1
        assert await _scalar(database, "SELECT count(*) FROM operations") == 1
        assert await _scalar(database, "SELECT count(*) FROM rate_starts") == 1
        operation = await database.operation(first.definition.operation_identifier)
        original_provenance = operation["code_provenance"]
        assert isinstance(original_provenance, dict)
        assert original_provenance["commit_hash"] != resumed_provenance.commit_hash
        execution_event = json.loads(
            str(
                await _scalar(
                    database,
                    "SELECT data_json FROM stage_events WHERE stage_id='second' AND event_kind='claimed'",
                )
            )
        )
        assert execution_event["code_provenance"]["commit_hash"] == resumed_provenance.commit_hash
        assert execution_event["component_parts"] == list(COMPONENT.identifier.parts)
        assert execution_event["invocation"] == {"kind": "test"}
        assert (
            await _scalar(database, "SELECT object_id FROM stage_inputs WHERE stage_id='second'")
            == "checkpoint"
        )
        assert not await database.release_stage_execution(first, now_utc_ns=NOW + 3, reason={})
        with pytest.raises(LeaseLostError):
            await database.publish_leased_operation_checkpoint(
                work_item_identifier="work",
                lease_token=first.token,
                worker_identifier=first.work_lease.worker_identifier,
                utc_now_ns=lambda: NOW + 3,
                operation_id=first.definition.operation_identifier,
                records=(),
                artifacts=(),
                outputs=(),
                checkpoint_result={"stale": True},
            )


@pytest.mark.anyio
async def test_final_publication_failure_rolls_back_stage_and_network_finalization(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(
            _definition(network=True, resources=(ResourceRequest(name="http"),)),
            created_at_utc_ns=NOW,
        )
        lease = (await _claim(database)).lease
        assert lease is not None
        duplicate = RecordDraft(identifier="duplicate", kind=("test",), schema_version=1, value={})
        with pytest.raises(apsw.ConstraintError):
            async with database.transaction():
                await database.complete_stage(lease, now_utc_ns=NOW + 1)
                await database.complete_leased_operation(
                    work_item_identifier="work",
                    lease_token=lease.token,
                    worker_identifier=lease.work_lease.worker_identifier,
                    utc_now_ns=lambda: NOW + 1,
                    event_identifier="failed-publication",
                    operation_id=lease.definition.operation_identifier,
                    records=(duplicate, duplicate),
                    artifacts=(),
                    outputs=(),
                    inputs=(),
                    follow_on_work=(),
                    result={},
                    ended_at_utc="2027-01-15T08:00:01+00:00",
                    duration_ns=1,
                )
        assert await database.validate_stage(lease, now_utc_ns=NOW + 2)
        assert (
            await _scalar(database, "SELECT state FROM work_stages WHERE id='stage'") == "running"
        )
        assert await _scalar(database, "SELECT state FROM network_activities") == "admitted"
        assert (await database.operation(lease.definition.operation_identifier))[
            "state"
        ] == "started"
        assert (
            await _scalar(database, "SELECT count(*) FROM records WHERE object_id='duplicate'") == 0
        )


@pytest.mark.anyio
async def test_expired_stage_requires_guarded_scope_specific_recovery_and_fences_old_owner(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(_definition(resources=()), created_at_utc_ns=NOW)
        original = (await _claim(database, duration=100)).lease
        assert original is not None
        renewed = await database.renew_stage(original, now_utc_ns=NOW + 10, lease_duration_ns=200)
        assert renewed is not None
        assert renewed.work_lease.expires_at_utc_ns == NOW + 210
        assert not await database.recover_stage_execution(original, now_utc_ns=NOW + 110)
        assert (
            await database.expired_stage_executions(
                coordination_scope="another-host", now_utc_ns=NOW + 300
            )
            == ()
        )
        expired = await database.expired_stage_executions(
            coordination_scope="test-host", now_utc_ns=NOW + 300
        )
        assert expired == (renewed,)
        blocked = await _claim(database, token="blocked", now=NOW + 300)
        assert blocked.lease is None
        assert blocked.wait_kind is StageWaitKind.OWNERSHIP
        # No physical executor exists in this ledger-only fixture; manager tests own OS guards.
        assert await database.recover_stage_execution(renewed, now_utc_ns=NOW + 300)
        with pytest.raises(ValueError, match="tokens may never be reused"):
            await _claim(database, token=original.token, now=NOW + 301)
        replacement = (await _claim(database, token="replacement", now=NOW + 301)).lease
        assert replacement is not None
        assert replacement.execution == 2
        assert replacement.work_lease.attempt == 1
        assert not await database.validate_stage(original, now_utc_ns=NOW + 302)
        assert not await database.release_stage_execution(original, now_utc_ns=NOW + 302, reason={})
        assert not await database.recover_stage_execution(original, now_utc_ns=NOW + 302)


@pytest.mark.anyio
async def test_cancelling_a_pending_continuation_closes_its_logical_operation(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(_definition(), created_at_utc_ns=NOW)
        lease = (await _claim(database)).lease
        assert lease is not None
        await database.suspend_stage(
            lease,
            WaitWork(
                continuation=_definition(identifier="next"),
                wait_kind=StageWaitKind.DEPENDENCY,
                result={"waiting": True},
            ),
            now_utc_ns=NOW + 1,
        )
        await _release_resource(database, lease)
        result = await database.cancel_work(
            "work",
            reason="user stopped it",
            caller="test",
            requested_at_utc_ns=NOW + 2,
        )
        assert result["cancelled_pending"] == 1
        assert (await database.work("work"))["state"] == "cancelled"
        operation = await database.operation(lease.definition.operation_identifier)
        assert operation["state"] == "completed"
        assert operation["result"] == {"state": "cancelled"}
        assert (
            await _scalar(database, "SELECT state FROM work_stages WHERE id='next'") == "cancelled"
        )
        assert await _scalar(database, "SELECT state FROM stage_attempts") == "cancelled"


@pytest.mark.anyio
async def test_dependency_and_outage_waits_do_not_claim_work_and_offline_stages_remain_ready(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier in ("work", "dependency", "offline"):
            await _enqueue(database, identifier)
        await database.register_stage(
            _definition(dependencies=("dependency",)), created_at_utc_ns=NOW
        )
        assert (await _claim(database)).wait_kind is StageWaitKind.DEPENDENCY
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET state='completed' WHERE id='dependency'"
            )
            await connection.execute(
                "INSERT INTO network_connectivity(singleton,paused,next_probe_utc_ns) VALUES (1,1,?) ON CONFLICT(singleton) DO UPDATE SET paused=1,next_probe_utc_ns=excluded.next_probe_utc_ns",
                (NOW + DURATION,),
            )
        assert (await _claim(database)).lease is not None  # CPU stage runs during the outage.
        await database.register_stage(
            _definition(
                "offline",
                identifier="http",
                network=True,
                resources=(ResourceRequest(name="http"),),
            ),
            created_at_utc_ns=NOW,
        )
        waiting = await _claim(database, "http", token="http")
        assert waiting.wait_kind is StageWaitKind.CONNECTIVITY
        assert (await database.work("offline"))["attempt"] == 0


@pytest.mark.anyio
async def test_independent_processes_atomically_admit_stages_with_all_budgets(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        for index in range(6):
            await _enqueue(database, f"work-{index}")
            await database.register_stage(
                _definition(
                    f"work-{index}",
                    identifier=f"stage-{index}",
                    resources=(ResourceRequest(name="http"), ResourceRequest(name="cpu")),
                ),
                created_at_utc_ns=NOW,
            )
    child = """
import json,sys
from pathlib import Path
import anyio
from carl._tests.test_stage_storage import _claim
from carl.io.sqlite import Database
async def main():
    async with Database.managed(Path(sys.argv[1])) as database:
        result=await _claim(database,sys.argv[2],token=sys.argv[2])
        print(json.dumps(result.lease is not None))
anyio.run(main,backend="trio")
"""
    admitted: list[bool] = []

    async def contender(index: int) -> None:
        result = await anyio.run_process((sys.executable, "-c", child, str(path), f"stage-{index}"))
        admitted.append(json.loads(result.stdout))

    with anyio.fail_after(30):
        async with anyio.create_task_group() as tasks:
            for index in range(6):
                tasks.start_soon(contender, index)
    assert sum(admitted) == 1
    async with Database.managed(path) as database:
        assert await _scalar(database, "SELECT sum(attempt) FROM work_items") == 1
        for table in ("operations", "stage_execution_tokens"):
            assert await _scalar(database, f"SELECT count(*) FROM {table}") == 1


@pytest.mark.anyio
async def test_v14_migration_preserves_evidence_and_existing_resource_permits(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy.sqlite3"
    content = b"retained stage predecessor"
    digest = hashlib.sha256(content).hexdigest()
    legacy_schema = _SCHEMA[_SCHEMA.index("CREATE TABLE IF NOT EXISTS resource_budgets (") :]
    with sqlite3.connect(path) as connection:
        connection.executescript(without_fair_schema(legacy_schema))
        connection.executemany(
            "INSERT INTO schema_metadata(key,value) VALUES (?,?)", Database._v14_metadata().items()
        )
        connection.execute(
            "INSERT INTO content VALUES (?,?,'sqlite',?,NULL)", (digest, len(content), content)
        )
        connection.execute("INSERT INTO resource_budgets VALUES ('host','cpu',1)")
        connection.execute(
            "INSERT INTO resource_permits VALUES ('old','host','owner','held',1,100)"
        )
        connection.execute("INSERT INTO resource_allocations VALUES ('old','host','cpu',1)")
    for _ in range(2):
        async with Database.managed(path) as database:
            assert await _scalar(
                database, "SELECT value FROM schema_metadata WHERE key='schema_version'"
            ) == str(DATABASE_SCHEMA.version)
            assert (
                await _scalar(
                    database, "SELECT inline_bytes FROM content WHERE sha256=?", (digest,)
                )
                == content
            )
            assert await database.validate_resource_permit(
                token="old", owner="owner", now_utc_ns=50
            )
            assert await _scalar(database, "SELECT count(*) FROM work_stages") == 0


@pytest.mark.anyio
async def test_legacy_cancel_expiry_sweep_cannot_bypass_staged_guard_recovery(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(_definition(), created_at_utc_ns=NOW)
        lease = (await _claim(database, duration=100)).lease
        assert lease is not None
        await database.cancel_work(
            "work",
            reason="stop physical stage",
            caller="test",
            requested_at_utc_ns=NOW + 1,
        )
        # An ordinary probe must not finalize a stage whose physical owner might
        # merely be paused while still holding the execution guard.
        await database.claim_work(
            supported_capabilities=(CAPABILITY,),
            worker_identifier="legacy",
            lease_token="legacy",
            lease_duration_ns=DURATION,
            utc_now_ns=lambda: NOW + 200,
            event_identifier="ordinary-probe",
        )
        assert (await database.work("work"))["state"] == "cancelling"
        assert await _scalar(database, "SELECT state FROM work_stages") == "running"
        assert (await database.operation(lease.definition.operation_identifier))[
            "state"
        ] == "started"
        # No physical executor exists here; the manager tests hold and recover real guards.
        assert await database.recover_stage_execution(lease, now_utc_ns=NOW + 200)
        assert (await database.work("work"))["state"] == "cancelled"
        assert await _scalar(database, "SELECT state FROM work_stages") == "cancelled"
        assert await _scalar(database, "SELECT state FROM stage_attempts") == "cancelled"
        assert not await database.release_stage_execution(lease, now_utc_ns=NOW + 201, reason={})


@pytest.mark.anyio
@pytest.mark.parametrize("marketplace", ("facebook", "ebay"))
async def test_retained_marketplace_cooldowns_block_neither_http_nor_cpu_continuations(
    tmp_path: Path, marketplace: str
) -> None:
    kind = (
        ("carl", "facebook", "work", "collect_search")
        if marketplace == "facebook"
        else ("carl", "ebay", "collect", "search")
    )
    payload: dict[str, JsonValue] = {
        "routing": ["decodo", "personal", "datacenter"],
        "request": {"query": "test", "stack_identifier": "test-stack"},
    }
    if marketplace == "ebay":
        del payload["routing"]
    until = NOW + DURATION
    reason: dict[str, JsonValue] = (
        {
            "kind": "search_pagination_rate_limited",
            "facebook_search_cooldown": {
                "network_path": ["decodo", "personal", "datacenter"],
                "until_utc_ns": until,
            },
        }
        if marketplace == "facebook"
        else {
            "kind": "ebay_search_response_failure",
            "cooldown": {"stack_identifier": "test-stack", "until_utc_ns": until},
        }
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for name, next_resources in (
            ("http-work", (ResourceRequest(name="http"),)),
            ("cpu-work", (ResourceRequest(name="cpu"),)),
        ):
            await _enqueue(database, name, kind=kind, payload=payload)
            await database.register_stage(
                _definition(name, identifier=f"initial-{name}"), created_at_utc_ns=NOW
            )
            lease = (await _claim(database, f"initial-{name}", token=f"initial-{name}")).lease
            assert lease is not None
            await database.suspend_stage(
                lease,
                WaitWork(
                    continuation=_definition(
                        name, identifier=f"next-{name}", resources=next_resources
                    ),
                    wait_kind=StageWaitKind.TIME,
                    result={"checkpoint": True},
                ),
                now_utc_ns=NOW + 1,
            )
            await _release_resource(database, lease)
        await _enqueue(database, "cooldown-source", kind=kind, payload=payload)
        source = await database.claim_work(
            supported_capabilities=(WorkCapability(kind=kind, payload_schema_version=1),),
            worker_identifier="source-owner",
            lease_token="source-token",
            lease_duration_ns=DURATION,
            utc_now_ns=lambda: NOW + 2,
            event_identifier="source-claim",
            eligible_identifiers=("cooldown-source",),
        )
        assert source.lease is not None
        await database.begin_leased_operation(
            work_item_identifier="cooldown-source",
            lease_token="source-token",
            worker_identifier="source-owner",
            lease_duration_ns=DURATION,
            utc_now_ns=lambda: NOW + 2,
            event_identifier="source-operation-started",
            operation_id="source-operation",
            component=COMPONENT,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2027-01-15T08:00:00+00:00",
        )
        await database.retry_leased_operation(
            work_item_identifier="cooldown-source",
            lease_token="source-token",
            worker_identifier="source-owner",
            utc_now_ns=lambda: NOW + 2,
            event_identifier="source-released",
            operation_id="source-operation",
            records=(),
            artifacts=(),
            outputs=(),
            inputs=(),
            result={"state": "failed"},
            reason=reason,
            delay_ns=0,
            ended_at_utc="2027-01-15T08:00:00+00:00",
            duration_ns=1,
        )
        online = await _claim(database, "next-http-work", token="http-resume", now=NOW + 3)
        assert online.lease is not None
        assert online.lease.work_lease.attempt == 1
        assert online.next_eligible_at_utc_ns is None
        offline = await _claim(database, "next-cpu-work", token="cpu-resume", now=NOW + 3)
        assert offline.lease is not None
        assert offline.lease.work_lease.attempt == 1
