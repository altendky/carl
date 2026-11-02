"""Bounded dispatch metadata and writer-fenced registration preserve queue ownership."""

# Bulk fixtures and exact rollback assertions inspect durable storage boundaries.
# pyright: reportPrivateUsage=false

import json
import sys
from pathlib import Path

import anyio
import pytest

import carl.io.sqlite as sqlite_io
from carl._tests.test_stage_storage import COMPONENT, DURATION, NOW, _claim, _scalar
from carl._tests.test_worker import _definition, _provenance, _requester
from carl.core.models import JsonValue, NamedOutput, RecordDraft
from carl.core.resources import ResourceRequest
from carl.core.stages import StageDefinition, StageWaitKind, WaitWork
from carl.core.work import WorkCapability, WorkDefinition, WorkLease
from carl.core.work_dispatch import WorkSnapshotStale
from carl.io.network_activity import network_activity_definition
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
KIND = ("test", "dispatch")
CAPABILITY = WorkCapability(kind=KIND, payload_schema_version=1)


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _enqueue(
    database: Database,
    identifier: str,
    *,
    priority: int = 0,
    eligible: int = 0,
    created: int = NOW,
    kind: tuple[str, ...] = KIND,
) -> WorkDefinition:
    work = _definition(identifier=identifier, kind=kind).model_copy(
        update={"priority": priority, "not_before_utc_ns": eligible}
    )
    await database.enqueue_work(
        work,
        _requester(identifier),
        event_identifier=f"enqueue-{identifier}",
        enqueued_at_utc_ns=created,
    )
    return work


def _stage(work: str, identifier: str = "stage", operation: str = "operation") -> StageDefinition:
    return StageDefinition(
        identifier=identifier,
        work_item_identifier=work,
        operation_identifier=operation,
        kind=("test", "dispatch_stage"),
        payload={"work_ref": work},
        resources=(),
    )


async def _legacy_lease(
    database: Database,
    identifier: str,
    *,
    token: str = "legacy",
    now: int = NOW,
    duration: int = DURATION,
) -> WorkLease:
    claim = await database.claim_work(
        supported_capabilities=(CAPABILITY,),
        eligible_identifiers=(identifier,),
        worker_identifier="legacy-worker",
        lease_token=token,
        lease_duration_ns=duration,
        utc_now_ns=lambda: now,
        event_identifier=f"claim-{token}",
    )
    assert claim.lease is not None
    return claim.lease


async def _begin_legacy(
    database: Database, lease: WorkLease, operation: str = "legacy-operation"
) -> None:
    await database.begin_leased_operation(
        work_item_identifier=lease.work_item_identifier,
        lease_token=lease.token,
        worker_identifier=lease.worker_identifier,
        lease_duration_ns=DURATION,
        utc_now_ns=lambda: NOW,
        event_identifier=f"begin-{operation}",
        operation_id=operation,
        component=COMPONENT,
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2027-01-15T08:00:00+00:00",
    )


async def _finish_stage(
    database: Database, stage: StageDefinition, *, terminal: bool = False
) -> None:
    claim = await _claim(database, stage.identifier)
    assert claim.lease is not None
    lease = claim.lease
    async with database.transaction():
        await database.complete_stage(lease, now_utc_ns=NOW + 1)
        if terminal:
            await database.terminally_fail_leased_operation(
                work_item_identifier=stage.work_item_identifier,
                lease_token=lease.token,
                worker_identifier=lease.work_lease.worker_identifier,
                utc_now_ns=lambda: NOW + 1,
                event_identifier="finished",
                operation_id=stage.operation_identifier,
                records=(),
                artifacts=(),
                outputs=(),
                inputs=(),
                result={"retained": "checkpoint"},
                ended_at_utc="2027-01-15T08:00:00+00:00",
                duration_ns=1,
                error={"kind": "test_failure"},
            )
        else:
            await database.retry_leased_operation(
                work_item_identifier=stage.work_item_identifier,
                lease_token=lease.token,
                worker_identifier=lease.work_lease.worker_identifier,
                utc_now_ns=lambda: NOW + 1,
                event_identifier="finished",
                operation_id=stage.operation_identifier,
                records=(),
                artifacts=(),
                outputs=(),
                inputs=(),
                result={"retained": "checkpoint"},
                ended_at_utc="2027-01-15T08:00:00+00:00",
                duration_ns=1,
                reason={"kind": "actual_retry"},
                delay_ns=0,
            )


async def test_ten_thousand_rows_are_paged_without_payload_or_result_decoding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "template")
        async with database._connections.writer() as connection:
            await connection.executemany(
                "INSERT INTO work_items(id,kind_parts_json,payload_schema_version,payload_json,deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns,result_json,error_json) SELECT ?,kind_parts_json,payload_schema_version,?,json_array(?),'pending',?,0,?, ?,? FROM work_items WHERE id='template'",
                [
                    (
                        f"work-{index:05d}",
                        json.dumps({"BODY_SENTINEL": "x" * 100_000}) if index == 0 else "{}",
                        f"work-{index:05d}",
                        index % 4,
                        NOW,
                        '{"RESULT_SENTINEL":true}',
                        '{"ERROR_SENTINEL":true}',
                    )
                    for index in range(10_000)
                ],
            )
        original = sqlite_io.decode_json

        def decode(value: str | bytes) -> JsonValue:
            assert not any(
                marker in str(value)
                for marker in ("BODY_SENTINEL", "RESULT_SENTINEL", "ERROR_SENTINEL")
            )
            return original(value)

        monkeypatch.setattr(sqlite_io, "decode_json", decode)
        seen: list[str] = []
        cursor = None
        pages = 0
        while True:
            page = await database.dispatch_candidates(
                supported_capabilities=(CAPABILITY,), now_utc_ns=NOW, limit=137, after=cursor
            )
            assert len(page.items) <= 137
            assert all(
                not any(
                    field in item.model_dump()
                    for field in ("payload", "result", "error", "lease_json")
                )
                for item in page.items
            )
            seen.extend(item.work_item_identifier for item in page.items)
            pages += 1
            if not page.has_more:
                break
            assert (
                page.items
                and page.next_cursor == page.items[-1].cursor
                and page.next_cursor != cursor
            )
            cursor = page.next_cursor
        expected = sorted(
            ["template", *(f"work-{index:05d}" for index in range(10_000))],
            key=lambda identifier: (
                -(int(identifier[-5:]) % 4) if identifier != "template" else 0,
                0 if identifier != "template" else NOW,
                identifier,
            ),
        )
        assert seen == expected and pages > 50


async def test_priority_eligibility_ties_scope_and_blocked_head_advance(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier, priority, eligible, created in (
            ("blocked-a", 10, 0, NOW),
            ("blocked-b", 10, 0, NOW),
            ("cpu-tail", 0, 0, NOW),
            ("future", 99, NOW + 1, NOW),
            ("excluded", 100, 0, NOW),
            ("older", 0, 0, NOW - 1),
        ):
            await _enqueue(
                database, identifier, priority=priority, eligible=eligible, created=created
            )
        await _enqueue(database, "other-kind", kind=("other",))
        for identifier in ("blocked-a", "blocked-b"):
            await database.register_stage(
                _stage(identifier, f"stage-{identifier}", f"op-{identifier}").model_copy(
                    update={"resources": (ResourceRequest(name="http"),)}
                ),
                created_at_utc_ns=NOW,
            )
        blocker = await database.try_acquire_resource_permit(
            scope="test-host",
            token="blocker",
            owner="blocker",
            requests=(ResourceRequest(name="http"),),
            capacities={"http": 1, "cpu": 1},
            now_utc_ns=NOW,
            lease_duration_ns=DURATION,
        )
        assert blocker is not None
        scope = ("blocked-a", "blocked-b", "cpu-tail", "future", "older", "other-kind")
        first = await database.dispatch_candidates(
            supported_capabilities=(CAPABILITY,),
            now_utc_ns=NOW,
            limit=2,
            eligible_identifiers=scope,
        )
        assert [item.work_item_identifier for item in first.items] == ["blocked-a", "blocked-b"]
        for item in first.items:
            assert item.stage_identifier is not None
            assert (
                await _claim(
                    database, item.stage_identifier, token=f"token-{item.work_item_identifier}"
                )
            ).lease is None
        later = await database.dispatch_candidates(
            supported_capabilities=(CAPABILITY,),
            now_utc_ns=NOW,
            limit=2,
            after=first.next_cursor,
            eligible_identifiers=scope,
        )
        assert [item.work_item_identifier for item in later.items] == ["older", "cpu-tail"]
        assert all(item.attempt == 0 for item in (*first.items, *later.items))
        assert await _scalar(database, "SELECT count(*) FROM operations") == 0


async def test_stage_due_time_filters_without_changing_work_cursor_order(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "first", eligible=1, created=0)
        await _enqueue(database, "second", eligible=2, created=0)
        stage = _stage("first").model_copy(update={"not_before_utc_ns": NOW + 10})
        await database.register_stage(stage, created_at_utc_ns=NOW)
        early = await database.dispatch_candidates(
            supported_capabilities=(CAPABILITY,), now_utc_ns=NOW, limit=10
        )
        assert [item.work_item_identifier for item in early.items] == ["second"]
        due = await database.dispatch_candidates(
            supported_capabilities=(CAPABILITY,), now_utc_ns=NOW + 10, limit=10
        )
        assert [item.work_item_identifier for item in due.items] == ["first", "second"]
        assert due.items[0].eligible_at_utc_ns == 1


@pytest.mark.parametrize(
    "mutation", ("payload", "route", "scope", "priority", "eligible", "attempt", "cancel", "claim")
)
async def test_stale_registration_snapshot_cannot_mutate_any_stage_ledger(
    tmp_path: Path, mutation: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        snapshot = await database.work_dispatch_snapshot("work")
        if mutation == "cancel":
            await database.cancel_work(
                "work", reason="stop", caller="test", requested_at_utc_ns=NOW + 1
            )
        elif mutation == "claim":
            _ = await _legacy_lease(database, "work")
        else:
            async with database._connections.writer() as connection:
                if mutation in ("payload", "route"):
                    await connection.execute(
                        "UPDATE work_items SET payload_json=? WHERE id='work'",
                        (
                            json.dumps({"routing": ["new", "route"]})
                            if mutation == "route"
                            else '{"changed":true}',
                        ),
                    )
                elif mutation == "scope":
                    await connection.execute(
                        "INSERT INTO work_scopes VALUES ('work','network_path','[\"new\",\"route\"]')"
                    )
                else:
                    column = {
                        "priority": "priority",
                        "eligible": "eligible_at_utc_ns",
                        "attempt": "attempt",
                    }[mutation]
                    await connection.execute(
                        f"UPDATE work_items SET {column}={column}+1 WHERE id='work'"
                    )
        result = await database.try_register_initial_stage(
            _stage("work"), expected_snapshot=snapshot, created_at_utc_ns=NOW + 2
        )
        assert result.status == "stale"
        for table in ("work_stages", "stage_attempts", "operations", "resource_permits"):
            assert await _scalar(database, f"SELECT count(*) FROM {table}") == 0


async def test_two_connections_register_one_initial_and_decline_rolls_back_outer_resources(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as first, Database.managed(path) as second:
        await _enqueue(first, "work")
        snapshots = (
            await first.work_dispatch_snapshot("work"),
            await second.work_dispatch_snapshot("work"),
        )
        results: list[str] = []

        async def contender(database: Database, index: int) -> None:
            result = await database.try_register_initial_stage(
                _stage("work", f"stage-{index}", f"operation-{index}"),
                expected_snapshot=snapshots[index],
                created_at_utc_ns=NOW,
            )
            results.append(result.status)

        # Both independent connections race from the same retained snapshot.
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(contender, first, 0)
            tasks.start_soon(contender, second, 1)
        assert results.count("registered") == 1 and set(results) <= {
            "registered",
            "existing",
            "stale",
        }
        assert (
            await _scalar(first, "SELECT count(*) FROM stage_attempts")
            == await _scalar(first, "SELECT count(*) FROM work_stages")
            == 1
        )
        assert await _scalar(first, "SELECT count(*) FROM operations") == 0
        assert (await first.work("work"))["attempt"] == 0
        with pytest.raises(WorkSnapshotStale):
            async with second.transaction():
                resource = await second.try_acquire_resource_permit(
                    scope="test-host",
                    token="must-rollback",
                    owner="test",
                    requests=(ResourceRequest(name="cpu"),),
                    capacities={"cpu": 1},
                    now_utc_ns=NOW,
                    lease_duration_ns=DURATION,
                )
                assert resource is not None
                await second.try_register_initial_stage(
                    _stage("work", "loser", "orphan"),
                    expected_snapshot=snapshots[1],
                    created_at_utc_ns=NOW,
                )
        assert await _scalar(first, "SELECT count(*) FROM resource_permits") == 0
        assert await _scalar(first, "SELECT count(*) FROM stage_attempts") == 1


async def test_existing_id_is_checked_after_snapshot_fence(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        snapshot = await database.work_dispatch_snapshot("work")
        stage = _stage("work")
        await database.register_stage(stage, created_at_utc_ns=NOW, expected_snapshot=snapshot)
        await database.cancel_work(
            "work", reason="stop", caller="test", requested_at_utc_ns=NOW + 1
        )
        with pytest.raises(WorkSnapshotStale):
            await database.register_stage(
                stage, created_at_utc_ns=NOW + 2, expected_snapshot=snapshot
            )


async def test_exact_pending_continuation_snapshot_can_validate_without_creating_an_attempt(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        initial = _stage("work")
        await database.register_stage(initial, created_at_utc_ns=NOW)
        claim = await _claim(database)
        assert claim.lease is not None
        continuation = initial.model_copy(update={"identifier": "continuation"})
        await database.suspend_stage(
            claim.lease,
            WaitWork(
                continuation=continuation,
                wait_kind=StageWaitKind.RESOURCE,
                result={"retained": "same-operation"},
            ),
            now_utc_ns=NOW + 1,
        )
        snapshot = await database.work_dispatch_snapshot("work")
        assert (
            snapshot.state.value == "leased" and snapshot.active_stage_identifier == "continuation"
        )
        await database.register_stage(
            continuation, created_at_utc_ns=NOW + 2, expected_snapshot=snapshot
        )
        with pytest.raises(WorkSnapshotStale):
            await database.register_stage(
                _stage("work", "new-initial", "new-operation"),
                created_at_utc_ns=NOW + 2,
                expected_snapshot=snapshot,
            )
        assert await _scalar(database, "SELECT count(*) FROM operations") == 1
        assert await _scalar(database, "SELECT count(*) FROM stage_attempts") == 1
        assert await _scalar(database, "SELECT count(*) FROM work_stages") == 2
        resumed = await _claim(database, "continuation", token="resumed", now=NOW + 3)
        assert resumed.lease is not None and resumed.lease.work_lease.attempt == 1
        assert resumed.lease.definition.operation_identifier == initial.operation_identifier


@pytest.mark.parametrize("manual", (False, True))
async def test_actual_retry_registers_fresh_operation_but_wait_retains_one_attempt(
    tmp_path: Path, manual: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        stage = _stage("work")
        await database.register_stage(stage, created_at_utc_ns=NOW)
        await _finish_stage(database, stage, terminal=manual)
        if manual:
            await database.retry_terminal_work_from_operation(
                work_item_identifier="work",
                checkpoint_operation_identifier="operation",
                retried_at_utc_ns=NOW + 2,
                event_identifier="manual-retry",
                reason={"kind": "manual"},
            )
        snapshot = await database.work_dispatch_snapshot("work")
        fresh = _stage("work", "fresh", "fresh-operation")
        assert (
            await database.try_register_initial_stage(
                fresh, expected_snapshot=snapshot, created_at_utc_ns=NOW + 2
            )
        ).status == "registered"
        claim = await _claim(database, fresh.identifier, token="fresh-token", now=NOW + 2)
        assert claim.lease is not None and claim.lease.work_lease.attempt == 2
        continuation = fresh.model_copy(update={"identifier": "continuation"})
        await database.suspend_stage(
            claim.lease,
            WaitWork(
                continuation=continuation,
                wait_kind=StageWaitKind.RESOURCE,
                result={"phase": "same-attempt"},
            ),
            now_utc_ns=NOW + 3,
        )
        resumed = await _claim(database, "continuation", token="resumed", now=NOW + 4)
        assert resumed.lease is not None and resumed.lease.work_lease.attempt == 2
        assert resumed.lease.definition.operation_identifier == "fresh-operation"
        assert await _scalar(database, "SELECT count(*) FROM operations") == 2


@pytest.mark.parametrize("cancelled", (False, True))
@pytest.mark.parametrize("begin_operation", (False, True))
async def test_legacy_expiry_and_cancellation_recover_without_claim_and_outer_rollback(
    tmp_path: Path, cancelled: bool, begin_operation: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        lease = await _legacy_lease(database, "work")
        if begin_operation:
            await _begin_legacy(database, lease)
        if cancelled:
            await database.cancel_work(
                "work", reason="stop", caller="test", requested_at_utc_ns=NOW + 1
            )
        page = await database.expired_legacy_work_candidates(now_utc_ns=NOW + DURATION + 1, limit=1)
        assert len(page.items) == 1
        candidate = page.items[0]
        events_before = await _scalar(database, "SELECT count(*) FROM work_events")
        with pytest.raises(RuntimeError, match="rollback"):
            async with database.transaction():
                assert await database.recover_expired_legacy_work(
                    candidate, now_utc_ns=NOW + DURATION + 1
                )
                raise RuntimeError("rollback")
        assert (await database.work("work"))["state"] == ("cancelling" if cancelled else "leased")
        assert await _scalar(database, "SELECT count(*) FROM work_events") == events_before
        assert await database.recover_expired_legacy_work(candidate, now_utc_ns=NOW + DURATION + 1)
        work = await database.work("work")
        assert work["state"] == ("cancelled" if cancelled else "pending") and work["attempt"] == 1
        if begin_operation:
            operation = await database.operation("legacy-operation")
            assert operation["state"] == ("completed" if cancelled else "failed")
        assert not await database.recover_expired_legacy_work(
            candidate, now_utc_ns=NOW + DURATION + 2
        )


@pytest.mark.parametrize("suspended", (False, True))
async def test_expiry_pages_are_bounded_renewed_fenced_and_staged_work_excluded(
    tmp_path: Path,
    suspended: bool,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier in ("a", "b", "c", "staged"):
            await _enqueue(database, identifier)
        leases = [
            await _legacy_lease(database, identifier, token=identifier)
            for identifier in ("a", "b", "c")
        ]
        staged = _stage("staged")
        await database.register_stage(staged, created_at_utc_ns=NOW)
        claim = await _claim(database)
        assert claim.lease is not None
        if suspended:
            await database.suspend_stage(
                claim.lease,
                WaitWork(
                    continuation=staged.model_copy(update={"identifier": "suspended"}),
                    wait_kind=StageWaitKind.RESOURCE,
                    result={},
                ),
                now_utc_ns=NOW + 1,
            )
        first = await database.expired_legacy_work_candidates(
            now_utc_ns=NOW + DURATION + 1, limit=1
        )
        assert [item.work_item_identifier for item in first.items] == ["a"] and first.has_more
        await database.renew_lease(
            work_item_identifier="a",
            lease_token=leases[0].token,
            worker_identifier=leases[0].worker_identifier,
            lease_duration_ns=DURATION * 2,
            utc_now_ns=lambda: NOW + 1,
            event_identifier="renew-a",
        )
        assert not await database.recover_expired_legacy_work(
            first.items[0], now_utc_ns=NOW + DURATION + 1
        )
        scoped = await database.expired_legacy_work_candidates(
            now_utc_ns=NOW + DURATION + 1,
            limit=1,
            after=first.next_cursor,
            eligible_identifiers=("c", "staged"),
        )
        assert [item.work_item_identifier for item in scoped.items] == ["c"]
        assert not scoped.has_more
        assert await database.recover_expired_legacy_work(
            scoped.items[0], now_utc_ns=NOW + DURATION + 1
        )
        assert (await database.work("staged"))["state"] == "leased"


async def test_cancellation_recovery_keeps_checkpoint_and_rolls_back_network_and_operation_together(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        lease = await _legacy_lease(database, "work")
        await _begin_legacy(database, lease)
        await database.publish_leased_operation_checkpoint(
            work_item_identifier="work",
            lease_token=lease.token,
            worker_identifier=lease.worker_identifier,
            utc_now_ns=lambda: NOW + 1,
            operation_id="legacy-operation",
            records=(
                RecordDraft(
                    identifier="partial",
                    kind=("test", "partial"),
                    schema_version=1,
                    value={"saved": True},
                ),
            ),
            artifacts=(),
            outputs=(NamedOutput(name=("partial",), object_identifier="partial"),),
            checkpoint_result={"phase": "retained_partial"},
        )
        await database.create_network_activity(
            network_activity_definition(
                identifier="activity",
                kind=("test", "http"),
                operation_identifier="legacy-operation",
                network_session_identifier="session",
                network_path=("test", "route"),
            ),
            created_at_utc_ns=NOW + 1,
            event_identifier="network-created",
            sample_uniform_holdoff_ns=lambda minimum, _maximum: minimum,
        )
        admitted = await database.try_admit_network_activity(
            network_activity_identifier="activity",
            admission_token="network-token",
            permit_duration_ns=DURATION,
            now_utc_ns=NOW + 1,
            event_identifier="network-admitted",
        )
        assert admitted.admission is not None
        await database.cancel_work(
            "work", reason="stop", caller="test", requested_at_utc_ns=NOW + 2
        )
        candidates = await database.expired_legacy_work_candidates(
            now_utc_ns=NOW + DURATION + 2, limit=1
        )
        assert len(candidates.items) == 1
        candidate = candidates.items[0].model_copy(
            update={"operation_identifier": "untrusted-discovery-value"}
        )
        events_before = await _scalar(database, "SELECT count(*) FROM work_events")
        with pytest.raises(RuntimeError, match="outer rollback"):
            async with database.transaction():
                assert await database.recover_expired_legacy_work(
                    candidate, now_utc_ns=NOW + DURATION + 2
                )
                raise RuntimeError("outer rollback")
        assert (await database.network_activity("activity"))["state"] == "admitted"
        assert (await database.operation("legacy-operation"))["state"] == "started"
        assert await _scalar(database, "SELECT count(*) FROM work_events") == events_before
        assert await database.recover_expired_legacy_work(candidate, now_utc_ns=NOW + DURATION + 2)
        work = await database.work("work")
        assert work["state"] == "cancelled" and work["result"] == {"phase": "retained_partial"}
        assert (await database.network_activity("activity"))["state"] == "cancelled"
        operation = await database.operation("legacy-operation")
        assert operation["state"] == "completed" and operation["result"] == {"state": "cancelled"}
        assert (await database.get_record("partial"))[2] == {"saved": True}
        assert (
            await _scalar(
                database, "SELECT count(*) FROM operation_outputs WHERE object_id='partial'"
            )
            == 1
        )


async def test_independent_process_initial_registration_has_one_winner_and_no_orphan_operation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _enqueue(database, "work")
    script = """
import json,sys
from pathlib import Path
import anyio
from carl._tests.test_work_dispatch_storage import NOW,_stage
from carl.io.sqlite import Database
async def main():
    async with Database.managed(Path(sys.argv[1])) as database:
        snapshot=await database.work_dispatch_snapshot('work')
        Path(sys.argv[2]).write_text('ready')
        while not Path(sys.argv[3]).exists():
            await anyio.sleep(.01)
        result=await database.try_register_initial_stage(_stage('work',sys.argv[4],sys.argv[4]+'-operation'),expected_snapshot=snapshot,created_at_utc_ns=NOW)
        print(result.status)
anyio.run(main,backend='asyncio')
"""
    processes: list[anyio.abc.Process] = []
    try:
        for index in range(2):
            processes.append(
                await anyio.open_process(
                    (
                        sys.executable,
                        "-c",
                        script,
                        str(path),
                        str(tmp_path / f"ready-{index}"),
                        str(tmp_path / "go"),
                        f"stage-{index}",
                    )
                )
            )
        with anyio.fail_after(15):
            while not all((tmp_path / f"ready-{index}").exists() for index in range(2)):
                await anyio.sleep(0.01)
            (tmp_path / "go").touch()
            states = []
            for process in processes:
                assert process.stdout is not None
                states.append((await process.stdout.receive(4096)).decode().strip())
                assert await process.wait() == 0
        assert states.count("registered") == 1 and set(states) <= {
            "registered",
            "existing",
            "stale",
        }
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
                await process.wait()
            await process.aclose()
    async with Database.managed(path) as database:
        assert (
            await _scalar(database, "SELECT count(*) FROM stage_attempts")
            == await _scalar(database, "SELECT count(*) FROM work_stages")
            == 1
        )
        assert await _scalar(database, "SELECT count(*) FROM operations") == 0
        assert (await database.work("work"))["attempt"] == 0
