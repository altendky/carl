"""Scheduler diagnostics preserve physical accounting and publication fences."""

# Offline storage contracts exercise retained metadata and query bounds.
# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

import carl.io.scheduler_activity as scheduler_io
import carl.io.sqlite as sqlite_io
from carl._tests.test_stage_storage import DURATION, NOW, _claim, _definition, _enqueue
from carl.core.models import JsonValue
from carl.core.resources import ResourceRequest
from carl.core.scheduler_activity import SchedulerWaitObservation
from carl.core.stages import StageWaitKind
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def test_persisted_capacity_expired_hold_and_status_are_read_only(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        held = await database.try_acquire_resource_permit(
            scope="scope",
            token="held",
            owner="owner",
            requests=(ResourceRequest(name="cpu"),),
            capacities={"cpu": 1},
            now_utc_ns=NOW,
            lease_duration_ns=DURATION,
        )
        assert held is not None
        snapshot = await database.scheduler_activity(
            coordination_scope="scope", now_utc_ns=NOW + DURATION + 1
        )
        cpu = next(row for row in snapshot.resources if row.name == "cpu")
        assert cpu.capacity == 1 and cpu.held_units == cpu.expired_held_units == 1
        assert snapshot.scope_configured
        other = await database.scheduler_activity(
            coordination_scope="different-host", now_utc_ns=NOW + DURATION + 1
        )
        assert not other.scope_configured and all(
            row.capacity is None and row.held_units == 0 for row in other.resources
        )
        assert await database.validate_resource_permit(
            token=held.token, owner=held.owner, now_utc_ns=NOW + 1
        )


async def test_observed_wait_monotonic_phase_and_physical_release_are_distinct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(_definition(), created_at_utc_ns=NOW)
        blocker = await database.try_acquire_resource_permit(
            scope="test-host",
            token="blocker",
            owner="owner",
            requests=(ResourceRequest(name="cpu"),),
            capacities={"cpu": 1, "http": 1},
            now_utc_ns=NOW,
            lease_duration_ns=DURATION,
        )
        assert blocker is not None
        assert (await _claim(database)).wait_kind is StageWaitKind.RESOURCE
        status = await database.work_scheduler_status(
            "work", coordination_scope="test-host", now_utc_ns=NOW + 1
        )
        assert (
            status.recorded_wait_kind is StageWaitKind.RESOURCE
            and "resource" in status.observed_blockers
        )
        assert status.wait_age_ns == 1 and status.wait_observation_source == "admission"
        monkeypatch.setattr(sqlite_io, "time_ns", lambda: NOW + 10)
        assert await database.release_resource_permit(token=blocker.token, owner=blocker.owner)
        claim = await _claim(database, token="running", now=NOW + 20)
        assert claim.lease is not None and claim.lease.resource_lease is not None
        active = await database.work_scheduler_status(
            "work", coordination_scope="test-host", now_utc_ns=NOW + 21
        )
        assert active.execution_age_ns == 1 and active.execution_started_at_utc_ns == NOW + 20
        async with database.transaction():
            await database.complete_stage(claim.lease, now_utc_ns=NOW + 30, phase_duration_ns=77)
            await database.complete_leased_operation(
                work_item_identifier="work",
                lease_token=claim.lease.token,
                worker_identifier=claim.lease.work_lease.worker_identifier,
                utc_now_ns=lambda: NOW + 30,
                event_identifier="completed",
                operation_id=claim.lease.definition.operation_identifier,
                records=(),
                artifacts=(),
                outputs=(),
                result={},
                ended_at_utc="2027-01-01T00:00:00+00:00",
                duration_ns=77,
            )
        snapshot = await database.scheduler_activity(
            coordination_scope="test-host", now_utc_ns=NOW + 31
        )
        assert snapshot.work_counts.completed == 1 and snapshot.stage_counts.completed == 1
        assert next(row for row in snapshot.resources if row.name == "cpu").held_units == 1
        phase = next(row for row in snapshot.timings if row.metric == "phase_runtime")
        wait = next(row for row in snapshot.timings if row.metric == "observed_resource_wait")
        assert phase.count == 1 and phase.total_ns == 77 and phase.method == "monotonic"
        assert wait.count == 1 and wait.total_ns == 20 and wait.method == "observed_lower_bound"
        monkeypatch.setattr(sqlite_io, "time_ns", lambda: NOW + 40)
        assert await database.release_resource_permit(
            token=claim.lease.resource_lease.token, owner=claim.lease.resource_lease.owner
        )
        after = await database.scheduler_activity(
            coordination_scope="test-host", now_utc_ns=NOW + 41
        )
        assert next(row for row in after.resources if row.name == "cpu").held_units == 0


class _Abort(Exception):
    pass


async def test_telemetry_claim_and_terminal_roll_back_with_publication(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(_definition(resources=()), created_at_utc_ns=NOW)
        with pytest.raises(_Abort):
            async with database.transaction():
                claim = await _claim(database)
                assert claim.lease is not None
                await database.complete_stage(claim.lease, now_utc_ns=NOW + 1, phase_duration_ns=3)
                raise _Abort
        snapshot = await database.scheduler_activity(
            coordination_scope="test-host", now_utc_ns=NOW + 2
        )
        assert snapshot.work_counts.pending == 1 and snapshot.stage_counts.pending == 1
        assert snapshot.throughput is not None and snapshot.throughput.admissions == 0
        assert snapshot.timings == ()


async def test_new_scheduler_reads_no_work_body_or_lease_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work", payload={"PRIVATE_BODY_SENTINEL": "x" * 100_000})
        await database.register_stage(_definition(resources=()), created_at_utc_ns=NOW)
        original = sqlite_io.decode_json

        def safe(value: str | bytes) -> JsonValue:
            assert "PRIVATE_BODY_SENTINEL" not in str(value)
            return original(value)

        monkeypatch.setattr(sqlite_io, "decode_json", safe)
        monkeypatch.setattr(scheduler_io, "decode_json", safe)
        snapshot = await database.scheduler_activity(coordination_scope="scope", now_utc_ns=NOW)
        status = await database.work_scheduler_status(
            "work", coordination_scope="scope", now_utc_ns=NOW
        )
        assert (
            snapshot.work_counts.pending == 1
            and status.work_identifier is None
            and status.identity_omitted
        )


async def test_running_and_replaced_initial_observations_are_rejected(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work")
        await database.register_stage(_definition(resources=()), created_at_utc_ns=NOW)
        await database.observe_scheduler_waits(
            coordination_scope="test-host",
            now_utc_ns=NOW,
            observations=(
                SchedulerWaitObservation(
                    work_identifier="work",
                    stage_identifier=None,
                    reason="resource",
                    resource_names=("cpu",),
                    source="advisory",
                ),
            ),
        )
        claim = await _claim(database)
        assert claim.lease is not None
        await database.observe_scheduler_waits(
            coordination_scope="test-host",
            now_utc_ns=NOW + 1,
            observations=(
                SchedulerWaitObservation(
                    work_identifier="work",
                    stage_identifier="stage",
                    reason="resource",
                    resource_names=("cpu",),
                    source="advisory",
                ),
            ),
        )
        async with database._connections.reader() as connection:
            row = await (
                await connection.execute("SELECT count(*) FROM scheduler_waits")
            ).fetchone()
        assert row is not None and row[0] == 0


async def test_partial_minute_retention_keeps_the_advertised_boundary(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        boundary = NOW // scheduler_io.MINUTE_NS * scheduler_io.MINUTE_NS
        async with database._connections.writer() as connection:
            await scheduler_io.rollup(connection, "scope", boundary, "admissions")
        observed = boundary + scheduler_io.RETENTION_NS + scheduler_io.MINUTE_NS // 2
        assert not await database.prune_scheduler_telemetry(now_utc_ns=observed)
        snapshot = await database.scheduler_activity(
            coordination_scope="scope",
            now_utc_ns=observed,
            recent_window_ns=scheduler_io.RETENTION_NS,
        )
        assert snapshot.throughput is not None
        assert snapshot.throughput.window_start_utc_ns == boundary
        assert snapshot.throughput.admissions == 1 and snapshot.throughput.complete


async def test_request_longer_than_retention_advertises_incomplete_effective_window(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        boundary = NOW // scheduler_io.MINUTE_NS * scheduler_io.MINUTE_NS
        async with database._connections.writer() as connection:
            await scheduler_io.rollup(connection, "scope", boundary, "admissions")
        observed = boundary + scheduler_io.RETENTION_NS
        snapshot = await database.scheduler_activity(
            coordination_scope="scope",
            now_utc_ns=observed,
            recent_window_ns=7 * scheduler_io.RETENTION_NS,
        )
        assert snapshot.throughput is not None
        assert snapshot.throughput.window_start_utc_ns == boundary
        assert snapshot.throughput.window_end_utc_ns == observed
        assert snapshot.throughput.admissions == 1
        assert not snapshot.throughput.complete and snapshot.coverage.truncated


async def test_finished_sample_eviction_preserves_long_live_phase_and_rollups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(scheduler_io, "SAMPLE_ROW_CAP", 2)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "long")
        await database.register_stage(
            _definition("long", identifier="long-stage", resources=()), created_at_utc_ns=NOW
        )
        long = await _claim(database, "long-stage", token="long")
        assert long.lease is not None
        for index in range(5):
            work, stage = f"short-{index}", f"stage-{index}"
            await _enqueue(database, work)
            await database.register_stage(
                _definition(work, identifier=stage, resources=()), created_at_utc_ns=NOW
            )
            claim = await _claim(database, stage, token=work, now=NOW + index + 1)
            assert claim.lease is not None
            await database.complete_stage(
                claim.lease, now_utc_ns=NOW + index + 2, phase_duration_ns=10
            )
        active = await database.work_scheduler_status(
            "long", coordination_scope="test-host", now_utc_ns=NOW + 20
        )
        assert active.execution_started_at_utc_ns == NOW and active.execution_age_ns == 20
        await database.complete_stage(long.lease, now_utc_ns=NOW + 21, phase_duration_ns=100)
        snapshot = await database.scheduler_activity(
            coordination_scope="test-host", now_utc_ns=NOW + 22, recent_window_ns=22
        )
        assert snapshot.throughput is not None and snapshot.throughput.complete
        assert snapshot.throughput.stage_completed == snapshot.throughput.admissions == 6
        phase = next(row for row in snapshot.timings if row.metric == "phase_runtime")
        assert phase.count == 6 and phase.total_ns == 150
        assert snapshot.coverage.censored_count == 0
        async with database._connections.reader() as connection:
            count = await (
                await connection.execute("SELECT count(*) FROM scheduler_executions")
            ).fetchone()
        assert count is not None and count[0] <= 2


async def test_unavoidable_live_sample_loss_marks_the_interval_incomplete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(scheduler_io, "SAMPLE_ROW_CAP", 1)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        leases = []
        for index in range(2):
            work, stage = f"work-{index}", f"stage-{index}"
            await _enqueue(database, work)
            await database.register_stage(
                _definition(work, identifier=stage, resources=()), created_at_utc_ns=NOW
            )
            claim = await _claim(database, stage, token=work, now=NOW + index)
            assert claim.lease is not None
            leases.append(claim.lease)
        during = await database.scheduler_activity(
            coordination_scope="test-host", now_utc_ns=NOW + 2, recent_window_ns=2
        )
        assert during.coverage.truncated and during.coverage.censored_count >= 1
        assert during.throughput is not None and not during.throughput.complete
        for lease in leases:
            await database.complete_stage(lease, now_utc_ns=NOW + 3, phase_duration_ns=10)
        after = await database.scheduler_activity(
            coordination_scope="test-host", now_utc_ns=NOW + 4, recent_window_ns=4
        )
        assert after.coverage.truncated
        assert after.throughput is not None and not after.throughput.complete
