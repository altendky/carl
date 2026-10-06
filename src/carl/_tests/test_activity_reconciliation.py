"""Terminal operation owners cannot leave live-looking network activities behind."""

import json
from pathlib import Path
from typing import Literal

import apsw
import pytest

from carl.core.components import Component, ComponentId
from carl.core.models import CodeProvenance
from carl.core.work import (
    NetworkActivityDefinition,
    NetworkActivityState,
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.io.sqlite import Database

_STARTED_AT = "2026-10-04T03:00:00+00:00"
_ENDED_AT = "2026-10-04T03:01:00+00:00"


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


async def _begin(database: Database, identifier: str = "operation") -> None:
    await database.begin_operation(
        operation_id=identifier,
        component=Component(ComponentId(("carl", "test", "reconciliation")), 1, lambda: None),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc=_STARTED_AT,
    )


def _definition(identifier: str, owner: str = "operation") -> NetworkActivityDefinition:
    return NetworkActivityDefinition(
        identifier=identifier,
        kind=("carl", "test", "network"),
        operation_identifier=owner,
        network_session_identifier="session",
        ordinal=1,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                identity=("carl", "test", "network"),
            ),
        ),
    )


async def _create(
    database: Database, identifier: str, *, owner: str = "operation", admitted: bool = False
) -> None:
    await database.create_network_activity(
        _definition(identifier, owner),
        created_at_utc_ns=10,
        event_identifier=f"{identifier}-created",
        sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
    )
    if admitted:
        result = await database.try_admit_network_activity(
            network_activity_identifier=identifier,
            admission_token=f"{identifier}-token",
            permit_duration_ns=1_000,
            now_utc_ns=20,
            event_identifier=f"{identifier}-admitted",
        )
        assert result.admission is not None


async def _end(database: Database, state: Literal["completed", "failed"]) -> None:
    if state == "completed":
        await database.complete_operation(
            operation_id="operation",
            records=(),
            artifacts=(),
            outputs=(),
            result={"outcome": "done"},
            ended_at_utc=_ENDED_AT,
            duration_ns=60_000_000_000,
        )
    else:
        await database.fail_operation(
            operation_id="operation",
            error={"type": "TimeoutError"},
            result={"outcome": "failed"},
            ended_at_utc=_ENDED_AT,
            duration_ns=60_000_000_000,
        )


async def _legacy_end(
    database: Database, state: Literal["completed", "failed"], identifier: str = "operation"
) -> None:
    """Simulate records written before terminal-owner finalization was introduced."""
    async with database._connections.writer() as connection:
        await connection.execute(
            "UPDATE operations SET state = ?, ended_at_utc = ? WHERE id = ?",
            (state, _ENDED_AT, identifier),
        )


async def _events(database: Database, identifier: str) -> list[tuple[object, ...]]:
    async with database._connections.reader() as connection:
        cursor = await connection.execute(
            """
            SELECT id, event_kind, recorded_at_utc_ns, data_json
            FROM network_activity_events WHERE network_activity_id = ? ORDER BY sequence
            """,
            (identifier,),
        )
        return [tuple(row) async for row in cursor]


async def _assert_reconciled(
    database: Database,
    identifier: str,
    *,
    owner_state: str,
    previous_state: str,
) -> None:
    activity = await database.network_activity(identifier)
    assert activity["state"] == "cancelled"
    assert activity["result"] == {
        "kind": "owning_operation_finished",
        "operation_state": owner_state,
        "previous_state": previous_state,
        "possibly_dispatched": previous_state == "admitted",
        "outcome": "unknown",
    }
    events = await _events(database, identifier)
    assert events[-1][1] == "cancelled"
    assert isinstance(events[-1][3], str)
    assert json.loads(events[-1][3]) == activity["result"]
    async with database._connections.reader() as connection:
        cursor = await connection.execute(
            "SELECT admission_token, permit_expires_at_utc_ns FROM network_activities WHERE id = ?",
            (identifier,),
        )
        assert await cursor.fetchone() == (None, None)


@pytest.mark.anyio
@pytest.mark.parametrize("owner_state", ["completed", "failed"])
@pytest.mark.parametrize("admitted", [False, True])
async def test_operation_finalization_reconciles_all_nonterminal_activities(
    tmp_path: Path, owner_state: Literal["completed", "failed"], admitted: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _begin(database)
        await _create(database, "activity", admitted=admitted)
        older_events = await _events(database, "activity")
        await _end(database, owner_state)
        await _assert_reconciled(
            database,
            "activity",
            owner_state=owner_state,
            previous_state="admitted" if admitted else "pending",
        )
        assert (await _events(database, "activity"))[:-1] == older_events
        assert await database.reconcile_terminal_network_activities(recorded_at_utc_ns=40) == ()


@pytest.mark.anyio
async def test_historical_reconciliation_is_scoped_idempotent_and_preserves_history(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _begin(database)
        await _begin(database, "active-operation")
        for identifier in ("chosen", "unchosen", "already-finished"):
            await _create(database, identifier, admitted=True)
        await _create(database, "active", owner="active-operation", admitted=True)
        await database.finish_network_activity(
            network_activity_identifier="already-finished",
            admission_token="already-finished-token",
            state=NetworkActivityState.FAILED,
            ended_at_utc_ns=30,
            result={"kind": "real_failure"},
            event_identifier="already-finished-failed",
        )
        await _legacy_end(database, "failed")
        finished_before = await database.network_activity("already-finished")
        finished_events = await _events(database, "already-finished")
        chosen_events = await _events(database, "chosen")

        # Reconcile an unexpired permit: owner state, not expiry, is authoritative.
        assert await database.reconcile_terminal_network_activities(
            recorded_at_utc_ns=40, activity_identifiers=("chosen", "active", "missing")
        ) == ("chosen",)
        await _assert_reconciled(
            database, "chosen", owner_state="failed", previous_state="admitted"
        )
        assert (await _events(database, "chosen"))[:-1] == chosen_events
        assert (await database.network_activity("chosen"))["ended_at_utc_ns"] == 40
        assert (await database.network_activity("unchosen"))["state"] == "admitted"
        assert (
            await database.reconcile_terminal_network_activities(
                recorded_at_utc_ns=50, activity_identifiers=()
            )
            == ()
        )
        assert (
            await database.reconcile_terminal_network_activities(
                recorded_at_utc_ns=50, activity_identifiers=("chosen",)
            )
            == ()
        )

        # Even an expired permit is not an orphan while its operation is active.
        assert await database.reconcile_terminal_network_activities(recorded_at_utc_ns=2_000) == (
            "unchosen",
        )
        assert (await database.network_activity("active"))["state"] == "admitted"
        assert await database.network_activity("already-finished") == finished_before
        assert await _events(database, "already-finished") == finished_events
        assert await database.reconcile_terminal_network_activities(recorded_at_utc_ns=3_000) == ()


async def _reject_cancel_events(database: Database, *, second_only: bool = False) -> None:
    async with database._connections.writer() as connection:
        await connection.execute(
            f"""
            CREATE TRIGGER reject_reconciliation_event BEFORE INSERT ON network_activity_events
            WHEN NEW.event_kind = 'cancelled'
              {"AND NEW.network_activity_id = 'second'" if second_only else ""}
            BEGIN SELECT RAISE(ABORT, 'injected reconciliation failure'); END
            """
        )


@pytest.mark.anyio
@pytest.mark.parametrize("owner_state", ["completed", "failed"])
async def test_reconciliation_event_failure_rolls_back_operation_finalization(
    tmp_path: Path, owner_state: Literal["completed", "failed"]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _begin(database)
        await _create(database, "activity", admitted=True)
        activity_before = await database.network_activity("activity")
        events_before = await _events(database, "activity")
        await _reject_cancel_events(database)
        with pytest.raises(apsw.ConstraintError, match="injected reconciliation failure"):
            await _end(database, owner_state)
        assert await database.network_activity("activity") == activity_before
        assert await _events(database, "activity") == events_before
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT state, ended_at_utc FROM operations WHERE id = 'operation'"
            )
            assert await cursor.fetchone() == ("started", None)


@pytest.mark.anyio
async def test_historical_reconciliation_event_failure_rolls_back_all_activities(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _begin(database)
        await _create(database, "first")
        await _create(database, "second", admitted=True)
        await _legacy_end(database, "failed")
        activities_before = [await database.network_activity(i) for i in ("first", "second")]
        events_before = [await _events(database, i) for i in ("first", "second")]
        await _reject_cancel_events(database, second_only=True)
        with pytest.raises(apsw.ConstraintError, match="injected reconciliation failure"):
            await database.reconcile_terminal_network_activities(recorded_at_utc_ns=40)
        assert [
            await database.network_activity(i) for i in ("first", "second")
        ] == activities_before
        assert [await _events(database, i) for i in ("first", "second")] == events_before


@pytest.mark.anyio
@pytest.mark.parametrize("owner_state", ["completed", "failed"])
async def test_terminal_owner_cannot_create_or_admit_network_activity(
    tmp_path: Path, owner_state: Literal["completed", "failed"]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _begin(database)
        await _create(database, "pending")
        await _legacy_end(database, owner_state)
        with pytest.raises(RuntimeError):
            await _create(database, "new")
        with pytest.raises(KeyError):
            await database.network_activity("new")
        with pytest.raises(RuntimeError):
            await database.try_admit_network_activity(
                network_activity_identifier="pending",
                admission_token="late-token",
                permit_duration_ns=1_000,
                now_utc_ns=20,
                event_identifier="late-admission",
            )
        assert (await database.network_activity("pending"))["state"] == "pending"
        assert len(await _events(database, "pending")) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("outcome", ["completed", "retry", "terminal_failure"])
async def test_leased_work_outcomes_reconcile_activity_in_same_transition(
    tmp_path: Path, outcome: str
) -> None:
    kind = ("carl", "test", "reconciliation")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            WorkDefinition(
                identifier="work",
                kind=kind,
                payload_schema_version=1,
                payload={},
                deduplication_identity=("reconciliation",),
                not_before_utc_ns=0,
                scopes=(
                    SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                    SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
                ),
            ),
            WorkRequester(
                request_identifier="request",
                kind=kind,
                identifier="requester",
                context={},
            ),
            event_identifier="enqueued",
            enqueued_at_utc_ns=10,
        )
        claim = await database.claim_work(
            supported_capabilities=(WorkCapability(kind=kind, payload_schema_version=1),),
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: 10,
            event_identifier="claimed",
        )
        assert claim.lease is not None
        await database.begin_leased_operation(
            work_item_identifier="work",
            lease_token="lease",
            worker_identifier="worker",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: 10,
            event_identifier="dispatched",
            operation_id="operation",
            component=Component(ComponentId(kind), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=_STARTED_AT,
        )
        await _create(database, "activity", admitted=True)
        if outcome == "completed":
            await database.complete_leased_operation(
                work_item_identifier="work",
                lease_token="lease",
                worker_identifier="worker",
                utc_now_ns=lambda: 30,
                event_identifier="ended",
                operation_id="operation",
                records=(),
                artifacts=(),
                outputs=(),
                result={},
                ended_at_utc=_ENDED_AT,
                duration_ns=1,
            )
        elif outcome == "retry":
            await database.retry_leased_operation(
                work_item_identifier="work",
                lease_token="lease",
                worker_identifier="worker",
                utc_now_ns=lambda: 30,
                delay_ns=10,
                event_identifier="ended",
                operation_id="operation",
                reason={"kind": "retry"},
                result={},
                ended_at_utc=_ENDED_AT,
                duration_ns=1,
            )
        else:
            await database.terminally_fail_leased_operation(
                work_item_identifier="work",
                lease_token="lease",
                worker_identifier="worker",
                utc_now_ns=lambda: 30,
                event_identifier="ended",
                operation_id="operation",
                error={"kind": "terminal_failure"},
                result={},
                ended_at_utc=_ENDED_AT,
                duration_ns=1,
            )
        await _assert_reconciled(
            database,
            "activity",
            owner_state="completed" if outcome == "completed" else "failed",
            previous_state="admitted",
        )
        assert (await database.work("work"))["state"] == (
            "pending" if outcome == "retry" else outcome
        )
