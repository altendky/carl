import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from uuid import uuid4

import anyio
import apsw
import pytest

from carl.core.components import Component, ComponentId
from carl.core.models import CodeProvenance, NamedInput, NamedOutput, RecordDraft
from carl.core.work import (
    ConcurrencyConstraint,
    HoldoffDecision,
    NetworkActivityDefinition,
    NetworkActivityState,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
    WorkState,
)
from carl.core.worker import FollowOnWork
from carl.io.cleanup import shielded_cleanup
from carl.io.network_activity import NetworkActivityScheduler
from carl.io.sqlite import Database, LeaseLostError


class _CancellationAfterCreateDatabase(Database):
    async def create_network_activity(
        self,
        definition: NetworkActivityDefinition,
        *,
        created_at_utc_ns: int,
        event_identifier: str,
        sample_uniform_holdoff_ns: Callable[[int, int], int],
    ) -> int:
        eligible_at = await super().create_network_activity(
            definition,
            created_at_utc_ns=created_at_utc_ns,
            event_identifier=event_identifier,
            sample_uniform_holdoff_ns=sample_uniform_holdoff_ns,
        )
        await anyio.sleep_forever()
        return eligible_at


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="dirty",
        package_version="test",
        python_implementation="test",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


def _definition() -> WorkDefinition:
    return WorkDefinition(
        identifier="work-1",
        kind=("carl", "test", "work"),
        payload_schema_version=1,
        payload={"value": 1},
        deduplication_identity=("one",),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=("test",)),
        ),
    )


def _requester() -> WorkRequester:
    return WorkRequester(
        request_identifier="request-1",
        kind=("carl", "test", "requester"),
        identifier="source-1",
        context={},
    )


def _capabilities() -> tuple[WorkCapability, ...]:
    return (WorkCapability(kind=("carl", "test", "work"), payload_schema_version=1),)


def _network_activity(identifier: str, *, ordinal: int) -> NetworkActivityDefinition:
    activity_kind = ("carl", "test", "network_activity")
    return NetworkActivityDefinition(
        identifier=identifier,
        kind=activity_kind,
        operation_identifier="network-operation",
        network_session_identifier="network-session",
        ordinal=ordinal,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                identity=activity_kind,
            ),
        ),
    )


@pytest.mark.anyio
async def test_network_activities_share_durable_rate_and_holdoff_constraints(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    overall = SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=())
    activity_kind = SchedulingScope(
        kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
        identity=("carl", "test", "network_activity"),
    )
    async with Database.managed(path, initialize=True) as database:
        await database.begin_operation(
            operation_id="network-operation",
            component=Component(ComponentId(("carl", "test", "network")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.register_constraint(
            SlidingWindowRateConstraint(
                identifier=("test", "network", "rate"),
                subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
                scope=overall,
                maximum_starts=1,
                period_ns=100,
            ),
            registered_at_utc_ns=1,
        )
        await database.register_constraint(
            UniformHoldoffConstraint(
                identifier=("test", "network", "holdoff"),
                subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
                scope=activity_kind,
                minimum_ns=5,
                maximum_ns=9,
            ),
            registered_at_utc_ns=1,
        )

        first_eligible = await database.create_network_activity(
            _network_activity("network-1", ordinal=1),
            created_at_utc_ns=10,
            event_identifier="network-created-1",
            sample_uniform_holdoff_ns=lambda minimum, maximum: maximum,
        )
        assert first_eligible == 19
        first = await database.try_admit_network_activity(
            network_activity_identifier="network-1",
            admission_token="network-token-1",
            permit_duration_ns=100,
            now_utc_ns=19,
            event_identifier="network-admitted-1",
        )
        assert first.admission is not None
        await database.finish_network_activity(
            network_activity_identifier="network-1",
            admission_token="network-token-1",
            state=NetworkActivityState.COMPLETED,
            ended_at_utc_ns=20,
            result={"kind": "complete"},
            event_identifier="network-completed-1",
        )

        await database.create_network_activity(
            _network_activity("network-2", ordinal=2),
            created_at_utc_ns=20,
            event_identifier="network-created-2",
            sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
        )
        held = await database.try_admit_network_activity(
            network_activity_identifier="network-2",
            admission_token="network-token-2",
            permit_duration_ns=100,
            now_utc_ns=25,
            event_identifier="network-blocked-2",
        )
        assert held.admission is None
        assert held.next_eligible_at_utc_ns == 119

        second = await database.try_admit_network_activity(
            network_activity_identifier="network-2",
            admission_token="network-token-2",
            permit_duration_ns=100,
            now_utc_ns=119,
            event_identifier="network-admitted-2",
        )
        assert second.admission is not None

    with closing(sqlite3.connect(path)) as connection:
        reservations = connection.execute(
            """
            SELECT subject_kind, subject_identifier, reserved_at_utc_ns
            FROM rate_starts ORDER BY reserved_at_utc_ns
            """
        ).fetchall()
        created_event = connection.execute(
            """
            SELECT data_json FROM network_activity_events
            WHERE network_activity_id = 'network-1' AND event_kind = 'created'
            """
        ).fetchone()

    assert reservations == [
        ("network_activity", "network-1", 19),
        ("network_activity", "network-2", 119),
    ]
    assert created_event is not None
    assert json.loads(created_event[0])["holdoff_decisions"] == [
        {
            "constraint_identifier": ["test", "network", "holdoff"],
            "sampled_delay_ns": 9,
        }
    ]


@pytest.mark.anyio
async def test_network_activity_wait_is_level_cancellable_and_persisted(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    activity_kind = SchedulingScope(
        kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
        identity=("carl", "test", "network_activity"),
    )
    identifiers = iter(("created-event", "admission-token", "admission-event", "cancel-event"))
    async with Database.managed(path, initialize=True) as database:
        await database.begin_operation(
            operation_id="network-operation",
            component=Component(ComponentId(("carl", "test", "network")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.register_constraint(
            UniformHoldoffConstraint(
                identifier=("test", "network", "long-holdoff"),
                subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
                scope=activity_kind,
                minimum_ns=1_000_000_000,
                maximum_ns=1_000_000_000,
            ),
            registered_at_utc_ns=1,
        )
        scheduler = NetworkActivityScheduler(
            database=database,
            new_identifier=lambda: next(identifiers),
            utc_now_ns=lambda: 10,
            sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
            permit_duration_ns=1_000_000_000,
        )
        with anyio.move_on_after(0.01) as cancellation_scope:
            async with scheduler.admit(_network_activity("network-cancelled", ordinal=1)):
                raise AssertionError("A held network activity was admitted")
        assert cancellation_scope.cancel_called
        activity = await database.network_activity("network-cancelled")

    assert activity["state"] == NetworkActivityState.CANCELLED
    assert activity["result"] == {"kind": "cancelled", "possibly_dispatched": False}


@pytest.mark.anyio
async def test_cancellation_after_activity_creation_persists_terminal_state(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    identifiers = iter(("created-event", "cancel-event"))
    async with _CancellationAfterCreateDatabase.managed(path, initialize=True) as database:
        await database.begin_operation(
            operation_id="network-operation",
            component=Component(ComponentId(("carl", "test", "network")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        scheduler = NetworkActivityScheduler(
            database=database,
            new_identifier=lambda: next(identifiers),
            utc_now_ns=lambda: 10,
            sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
            permit_duration_ns=1_000_000_000,
        )

        with anyio.move_on_after(0.01) as cancellation_scope:
            async with scheduler.admit(_network_activity("cancelled-after-create", ordinal=1)):
                raise AssertionError("Creation checkpoint unexpectedly reached admission")
        assert cancellation_scope.cancel_called
        activity = await database.network_activity("cancelled-after-create")

    assert activity["state"] == NetworkActivityState.CANCELLED
    assert activity["result"] == {"kind": "cancelled", "possibly_dispatched": False}


async def _admission_scheduler(database: Database) -> NetworkActivityScheduler:
    await database.begin_operation(
        operation_id="network-operation",
        component=Component(ComponentId(("carl", "test", "network")), 1, lambda: None),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc=datetime.now(UTC).isoformat(),
    )
    return NetworkActivityScheduler(
        database=database,
        new_identifier=lambda: str(uuid4()),
        utc_now_ns=lambda: 10,
        sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
        permit_duration_ns=1_000_000_000,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("dispatched", [False, True])
async def test_network_activity_normal_exit_finishes_inside_cancelled_scope(
    tmp_path: Path,
    dispatched: bool,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        scheduler = await _admission_scheduler(database)
        with anyio.CancelScope() as scope:
            async with scheduler.admit(_network_activity("normal-exit", ordinal=1)) as permit:
                if dispatched:
                    permit.mark_dispatched()
                scope.cancel()
        activity = await database.network_activity("normal-exit")
    assert activity["state"] == (
        NetworkActivityState.COMPLETED if dispatched else NetworkActivityState.SKIPPED
    )


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["error", "timeout"])
async def test_network_activity_cleanup_failure_preserves_body_error(
    tmp_path: Path,
    failure: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(
        "carl.io.network_activity.shielded_cleanup",
        partial(shielded_cleanup, timeout_seconds=0.01),
    )
    primary = ValueError("body error")

    async def fail_finish(**_kwargs: object) -> None:
        if failure == "timeout":
            await anyio.sleep_forever()
        raise OSError("private activity details")

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        scheduler = await _admission_scheduler(database)
        monkeypatch.setattr(database, "finish_network_activity", fail_finish)
        with pytest.raises(ValueError) as caught:
            async with scheduler.admit(_network_activity("failed-exit", ordinal=1)):
                raise primary
    assert caught.value is primary
    assert "failed network activity" in caplog.text
    assert ("TimeoutError" if failure == "timeout" else "OSError") in caplog.text
    assert "private activity details" not in caplog.text


@pytest.mark.anyio
async def test_network_activity_cleanup_error_preserves_cancellation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    cancelled_errors: list[BaseException] = []

    async def fail_finish(**_kwargs: object) -> None:
        raise OSError("private activity details")

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        scheduler = await _admission_scheduler(database)
        monkeypatch.setattr(database, "finish_network_activity", fail_finish)
        with anyio.CancelScope() as scope:
            async with scheduler.admit(_network_activity("cancelled-exit", ordinal=1)):
                scope.cancel()
                try:
                    await anyio.lowlevel.checkpoint()
                except anyio.get_cancelled_exc_class() as error:
                    cancelled_errors.append(error)
                    raise
        assert scope.cancelled_caught
    assert len(cancelled_errors) == 1
    assert any(
        "cancelled network activity: OSError" in note for note in cancelled_errors[0].__notes__
    )
    assert "private activity details" not in caplog.text


@pytest.mark.anyio
async def test_persisted_constraints_round_trip_across_database_reopen(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    overall = SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=())
    work_kind = SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=("test",))
    constraints = (
        ConcurrencyConstraint(
            identifier=("test", "concurrency"),
            scope=overall,
            maximum_active=1,
        ),
        SlidingWindowRateConstraint(
            identifier=("test", "rate"),
            scope=work_kind,
            maximum_starts=2,
            period_ns=100,
        ),
        UniformHoldoffConstraint(
            identifier=("test", "holdoff"),
            scope=work_kind,
            minimum_ns=5,
            maximum_ns=5,
        ),
    )

    async with Database.managed(path, initialize=True) as database:
        for constraint in constraints:
            await database.register_constraint(constraint, registered_at_utc_ns=1)

    async with Database.managed(path) as database:
        enqueued = await database.enqueue_work(
            _definition(),
            _requester(),
            event_identifier="event-enqueued",
            enqueued_at_utc_ns=10,
            holdoff_decisions=(
                HoldoffDecision(constraint_identifier=("test", "holdoff"), sampled_delay_ns=5),
            ),
        )
        assert enqueued.eligible_at_utc_ns == 15
        claim = await database.claim_work(
            supported_capabilities=_capabilities(),
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=100,
            utc_now_ns=lambda: 15,
            event_identifier="event-claimed",
        )

    assert claim.lease is not None
    assert claim.lease.attempt == 1


@pytest.mark.anyio
async def test_expired_lease_cannot_publish_and_replacement_can_complete(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    component = Component(ComponentId(("carl", "test", "work")), 1, lambda: None)
    record = RecordDraft(
        identifier="record-1",
        kind=("carl", "test", "record"),
        schema_version=1,
        value={"value": 1},
    )

    async with Database.managed(path, initialize=True) as database:
        await database.begin_operation(
            operation_id="source-operation",
            component=component,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.complete_operation(
            operation_id="source-operation",
            records=(
                RecordDraft(
                    identifier="source-record",
                    kind=("carl", "test", "source"),
                    schema_version=1,
                    value={},
                ),
            ),
            artifacts=(),
            outputs=(NamedOutput(name=("source",), object_identifier="source-record"),),
            result={},
            ended_at_utc=datetime.now(UTC).isoformat(),
            duration_ns=1,
        )
        await database.enqueue_work(
            _definition(),
            _requester(),
            event_identifier="event-enqueued",
            enqueued_at_utc_ns=10,
        )
        first = await database.claim_work(
            supported_capabilities=_capabilities(),
            worker_identifier="worker-1",
            lease_token="lease-1",
            lease_duration_ns=10,
            utc_now_ns=lambda: 10,
            event_identifier="event-claimed-1",
        )
        assert first.lease is not None

        await database.begin_leased_operation(
            work_item_identifier="work-1",
            lease_token="lease-1",
            worker_identifier="worker-1",
            lease_duration_ns=10,
            utc_now_ns=lambda: 10,
            event_identifier="event-dispatched-1",
            operation_id="operation-1",
            component=component,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.create_network_activity(
            _network_activity("interrupted-activity", ordinal=1).model_copy(
                update={"operation_identifier": "operation-1"}
            ),
            created_at_utc_ns=10,
            event_identifier="interrupted-activity-created",
            sample_uniform_holdoff_ns=lambda minimum, _maximum: minimum,
        )
        with pytest.raises(LeaseLostError):
            await database.complete_leased_operation(
                work_item_identifier="work-1",
                lease_token="lease-1",
                worker_identifier="worker-1",
                utc_now_ns=lambda: 21,
                event_identifier="event-stale-completion",
                operation_id="operation-1",
                records=(record,),
                artifacts=(),
                outputs=(NamedOutput(name=("record",), object_identifier="record-1"),),
                follow_on_work=(
                    FollowOnWork(
                        definition=_definition().model_copy(
                            update={
                                "identifier": "stale-follow-on",
                                "kind": ("carl", "test", "follow-on"),
                                "deduplication_identity": ("stale-follow-on",),
                            }
                        ),
                        requester=_requester().model_copy(
                            update={
                                "request_identifier": "stale-follow-on-request",
                                "identifier": "operation-1",
                            }
                        ),
                        event_identifier="stale-follow-on-event",
                    ),
                ),
                result={"value": 1},
                ended_at_utc=datetime.now(UTC).isoformat(),
                duration_ns=1,
            )

        assert (await database.operation("operation-1"))["state"] == "started"
        with pytest.raises(KeyError):
            await database.get_record("record-1")

        replacement = await database.claim_work(
            supported_capabilities=_capabilities(),
            worker_identifier="worker-2",
            lease_token="lease-2",
            lease_duration_ns=10,
            utc_now_ns=lambda: 21,
            event_identifier="event-claimed-2",
        )
        assert replacement.lease is not None
        assert replacement.lease.attempt == 2
        interrupted_operation = await database.operation("operation-1")
        assert interrupted_operation["state"] == "failed"
        assert interrupted_operation["duration_ns"] is None
        assert interrupted_operation["error"] == {
            "kind": "work_lease_expired",
            "duration": {
                "state": "unavailable",
                "reason": "worker_process_interrupted",
            },
        }
        interrupted_activity = await database.network_activity("interrupted-activity")
        assert interrupted_activity["state"] == NetworkActivityState.CANCELLED.value
        assert interrupted_activity["result"] == {
            "kind": "owning_work_lease_expired",
            "possibly_dispatched": False,
        }

        with pytest.raises(LeaseLostError, match="not bound"):
            await database.complete_leased_operation(
                work_item_identifier="work-1",
                lease_token="lease-2",
                worker_identifier="worker-2",
                utc_now_ns=lambda: 21,
                event_identifier="event-mismatched-completion",
                operation_id="operation-1",
                records=(record,),
                artifacts=(),
                outputs=(NamedOutput(name=("record",), object_identifier="record-1"),),
                result={"value": 1},
                ended_at_utc=datetime.now(UTC).isoformat(),
                duration_ns=1,
            )
        with pytest.raises(KeyError):
            await database.work_state("stale-follow-on")

        await database.begin_leased_operation(
            work_item_identifier="work-1",
            lease_token="lease-2",
            worker_identifier="worker-2",
            lease_duration_ns=10,
            utc_now_ns=lambda: 21,
            event_identifier="event-dispatched-2",
            operation_id="operation-2",
            component=component,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.complete_leased_operation(
            work_item_identifier="work-1",
            lease_token="lease-2",
            worker_identifier="worker-2",
            utc_now_ns=lambda: 21,
            event_identifier="event-completed",
            operation_id="operation-2",
            records=(record,),
            artifacts=(),
            inputs=(NamedInput(name=("source",), object_identifier="source-record"),),
            outputs=(NamedOutput(name=("record",), object_identifier="record-1"),),
            follow_on_work=(
                FollowOnWork(
                    definition=_definition().model_copy(
                        update={
                            "identifier": "follow-on",
                            "kind": ("carl", "test", "follow-on"),
                            "deduplication_identity": ("follow-on",),
                        }
                    ),
                    requester=_requester().model_copy(
                        update={
                            "request_identifier": "follow-on-request",
                            "identifier": "operation-2",
                        }
                    ),
                    event_identifier="follow-on-event",
                ),
            ),
            result={"value": 1},
            ended_at_utc=datetime.now(UTC).isoformat(),
            duration_ns=1,
        )

        assert (await database.operation("operation-1"))["state"] == "failed"
        assert (await database.operation("operation-2"))["state"] == "completed"
        assert (await database.get_record("record-1"))[2] == {"value": 1}
        assert await database.work_state("follow-on") is WorkState.PENDING

        repeated = await database.enqueue_work(
            _definition().model_copy(update={"identifier": "work-2"}),
            _requester().model_copy(update={"request_identifier": "request-2"}),
            event_identifier="event-enqueued-2",
            enqueued_at_utc_ns=30,
        )
        assert repeated.created
        assert repeated.work_item_identifier == "work-2"

    with closing(sqlite3.connect(path)) as connection:
        work = connection.execute(
            "SELECT state, attempt, lease_token FROM work_items WHERE id = 'work-1'"
        ).fetchone()
        events = connection.execute(
            "SELECT event_kind FROM work_events WHERE work_item_id = 'work-1' ORDER BY sequence"
        ).fetchall()
        inputs = connection.execute(
            "SELECT name_parts_json, object_id FROM operation_inputs WHERE operation_id = ?",
            ("operation-2",),
        ).fetchall()

    assert work == ("completed", 2, None)
    assert inputs == [('["source"]', "source-record")]
    assert events == [
        ("enqueued",),
        ("claimed",),
        ("lease_renewed",),
        ("lease_expired",),
        ("claimed",),
        ("lease_renewed",),
        ("completed",),
    ]


@pytest.mark.anyio
async def test_late_renewal_and_release_are_rejected(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            _definition(),
            _requester(),
            event_identifier="event-enqueued",
            enqueued_at_utc_ns=10,
        )
        claimed = await database.claim_work(
            supported_capabilities=_capabilities(),
            worker_identifier="worker-1",
            lease_token="lease-1",
            lease_duration_ns=10,
            utc_now_ns=lambda: 10,
            event_identifier="event-claimed",
        )
        assert claimed.lease is not None

        with pytest.raises(LeaseLostError):
            await database.renew_lease(
                work_item_identifier="work-1",
                lease_token="lease-1",
                worker_identifier="worker-1",
                lease_duration_ns=10,
                utc_now_ns=lambda: 20,
                event_identifier="event-renewed",
            )
        with pytest.raises(LeaseLostError):
            await database.release_lease(
                work_item_identifier="work-1",
                lease_token="lease-1",
                worker_identifier="worker-1",
                utc_now_ns=lambda: 20,
                eligible_at_utc_ns=20,
                reason={"kind": "cancelled"},
                event_identifier="event-released",
            )


@pytest.mark.anyio
async def test_failed_follow_on_enqueue_rolls_back_entire_completion(tmp_path: Path) -> None:
    component = Component(ComponentId(("carl", "test", "atomic-follow-on")), 1, lambda: None)
    record = RecordDraft(
        identifier="rolled-back-record",
        kind=("carl", "test", "result"),
        schema_version=1,
        value={},
    )

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            _definition(),
            _requester(),
            event_identifier="event-enqueued",
            enqueued_at_utc_ns=10,
        )
        claim = await database.claim_work(
            supported_capabilities=_capabilities(),
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=100,
            utc_now_ns=lambda: 10,
            event_identifier="event-claimed",
        )
        assert claim.lease is not None
        await database.begin_leased_operation(
            work_item_identifier="work-1",
            lease_token="lease",
            worker_identifier="worker",
            lease_duration_ns=100,
            utc_now_ns=lambda: 10,
            event_identifier="event-dispatched",
            operation_id="operation",
            component=component,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )

        with pytest.raises(apsw.ConstraintError):
            await database.complete_leased_operation(
                work_item_identifier="work-1",
                lease_token="lease",
                worker_identifier="worker",
                utc_now_ns=lambda: 10,
                event_identifier="event-completed",
                operation_id="operation",
                records=(record,),
                artifacts=(),
                outputs=(NamedOutput(name=("result",), object_identifier="rolled-back-record"),),
                follow_on_work=(
                    FollowOnWork(
                        definition=_definition().model_copy(
                            update={
                                "identifier": "rolled-back-follow-on",
                                "kind": ("carl", "test", "follow-on"),
                                "deduplication_identity": ("rolled-back-follow-on",),
                            }
                        ),
                        requester=_requester(),
                        event_identifier="follow-on-event",
                    ),
                ),
                result={},
                ended_at_utc=datetime.now(UTC).isoformat(),
                duration_ns=1,
            )

        assert await database.work_state("work-1") is WorkState.LEASED
        assert (await database.operation("operation"))["state"] == "started"
        with pytest.raises(KeyError):
            await database.get_record("rolled-back-record")
        with pytest.raises(KeyError):
            await database.work_state("rolled-back-follow-on")
