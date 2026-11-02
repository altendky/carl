"""Fairness is an atomic admission ledger, independent of wait/retry counters."""

from pathlib import Path

import pytest

from carl._tests.test_resident_storage import CAPACITIES, SCOPE, _resident
from carl._tests.test_stage_storage import DURATION, NOW, _claim, _definition, _release_resource
from carl.core.fair_scheduling import SchedulingContext, SchedulingServiceClass
from carl.core.resources import ResourceRequest
from carl.core.stages import StageDefinition, StageWaitKind
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _seed(
    database: Database,
    identifier: str,
    *,
    root: str,
    service: SchedulingServiceClass = SchedulingServiceClass.BACKGROUND,
    workspace: str | None = None,
    register: bool = True,
    resident: bool = False,
) -> StageDefinition:
    stage = _definition(identifier, identifier="stage-" + identifier)
    if resident:
        stage = stage.model_copy(update={"resident_session_identifier": "previous-" + identifier})
    await database.enqueue_work(
        WorkDefinition(
            identifier=identifier,
            kind=("test", "logical_stage_work"),
            payload_schema_version=1,
            payload={},
            deduplication_identity=(identifier,),
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(
                    kind=SchedulingScopeKind.WORK_KIND, identity=("test", "logical_stage_work")
                ),
            ),
        ),
        WorkRequester(
            request_identifier="request-" + identifier,
            kind=("test", "root"),
            identifier=root,
            context={},
            scheduling=SchedulingContext(
                root_request_identifier=root, workspace_identifier=workspace, service_class=service
            ),
        ),
        event_identifier="enqueue-" + identifier,
        enqueued_at_utc_ns=NOW,
    )
    if register:
        await database.register_stage(stage, created_at_utc_ns=NOW)
    return stage


async def test_enabled_admission_records_one_turn_and_preserves_logical_attempt(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _seed(database, "one", root="request-one")
        await database.enable_fair_scheduling("test-host", NOW)
        claim = await _claim(database, stage.identifier)
        assert claim.lease is not None
        turns = await database.fair_admissions("test-host")
        assert len(turns) == 1 and turns[0].sequence == 1
        assert turns[0].scheduling.root_request_identifier == "request-one"
        assert (
            await database.renew_stage(claim.lease, now_utc_ns=NOW + 1, lease_duration_ns=DURATION)
            is not None
        )
        assert len(await database.fair_admissions("test-host")) == 1
        assert (await database.work("one"))["attempt"] == 1


async def test_live_capacity_offer_wins_next_turn_without_spending_decline(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        first = await _seed(database, "first", root="A")
        second = await _seed(database, "second", root="B")
        await database.enable_fair_scheduling("test-host", NOW)
        initial = await _claim(database, first.identifier, token="first-initial")
        assert initial.lease is not None
        blocked = await _claim(database, second.identifier, token="second-blocked", now=NOW + 1)
        assert blocked.lease is None and blocked.wait_kind is StageWaitKind.RESOURCE
        assert len(await database.fair_admissions("test-host")) == 1
        assert await database.release_stage_execution(initial.lease, now_utc_ns=NOW + 2, reason={})
        await _release_resource(database, initial.lease)
        # The backoff affects polling, not this live contender's original age or
        # right to the next free unit. Otherwise synchronized arrivals can starve it.
        declined = await _claim(database, first.identifier, token="first-again", now=NOW + 3)
        assert declined.lease is None and declined.wait_kind is StageWaitKind.OWNERSHIP
        assert len(await database.fair_admissions("test-host")) == 1
        admitted = await _claim(
            database, second.identifier, token="second-admitted", now=NOW + 100_000_002
        )
        assert admitted.lease is not None
        assert len(await database.fair_admissions("test-host")) == 2


async def test_rescheduled_resident_offer_cannot_hold_another_ready_actor(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database, "future", root="A", resident=True)
        await _seed(database, "ready", root="B", resident=True)
        await database.enable_fair_scheduling(SCOPE, NOW)
        held = await database.try_acquire_resource_permit(
            scope=SCOPE,
            token="held-resident",
            owner="holder",
            requests=(ResourceRequest(name="resident_sessions"),),
            capacities=CAPACITIES,
            now_utc_ns=NOW,
            lease_duration_ns=DURATION,
        )
        assert held is not None
        assert await _resident(database, "future-actor", work="future") is None
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET eligible_at_utc_ns=? WHERE id='future'",
                (NOW + DURATION,),
            )
        assert await database.release_resource_permit(token=held.token, owner=held.owner)
        admitted = await _resident(database, "ready-actor", work="ready", now=NOW + 1)
        assert admitted is not None
        turns = await database.fair_admissions(SCOPE)
        assert len(turns) == 1 and turns[0].work_item_identifier == "ready"
        async with database._connections.reader() as connection:
            stale = await (
                await connection.execute("SELECT 1 FROM fair_offers WHERE work_item_id='future'")
            ).fetchone()
        assert stale is None


async def test_advertised_page_enforces_class_turns_after_discovery_becomes_stale(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier in ("I0", "I1", "I2", "I3", "B0", "B1"):
            await _seed(
                database,
                identifier,
                root=identifier[0],
                service=(
                    SchedulingServiceClass.INTERACTIVE
                    if identifier[0] == "I"
                    else SchedulingServiceClass.BACKGROUND
                ),
            )
        await database.enable_fair_scheduling("test-host", NOW)
        page = await database.fair_dispatch_candidates(
            scope="test-host",
            supported_capabilities=(
                WorkCapability(kind=("test", "logical_stage_work"), payload_schema_version=1),
            ),
            now_utc_ns=NOW,
            limit=16,
        )
        await database.offer_fair_dispatch_candidates(
            scope="test-host",
            work_identifiers=tuple(item.work_item_identifier for item in page.items),
            now_utc_ns=NOW,
        )
        for index, identifier in enumerate(("I0", "I1", "I2")):
            claim = await _claim(
                database, "stage-" + identifier, token="claim-" + identifier, now=NOW + index
            )
            assert claim.lease is not None
            assert await database.release_stage_execution(
                claim.lease, now_utc_ns=NOW + index, reason={}
            )
            await _release_resource(database, claim.lease)
        assert (await _claim(database, "stage-I3", token="stale-I3", now=NOW + 4)).lease is None
        background = await _claim(database, "stage-B0", token="claim-B0", now=NOW + 5)
        assert background.lease is not None
        assert await database.release_stage_execution(
            background.lease, now_utc_ns=NOW + 6, reason={}
        )
        await _release_resource(database, background.lease)
        assert (await _claim(database, "stage-B1", token="stale-B1", now=NOW + 7)).lease is None
        assert (await _claim(database, "stage-I3", token="claim-I3", now=NOW + 8)).lease is not None
        turns = sorted(await database.fair_admissions("test-host"), key=lambda turn: turn.sequence)
        assert [turn.work_item_identifier for turn in turns] == ["I0", "I1", "I2", "B0", "I3"]


@pytest.mark.parametrize("changed_lane", (False, True))
async def test_fresh_page_offer_promotes_only_matching_initial_registration(
    tmp_path: Path, changed_lane: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _seed(database, "fresh", root="root", register=False)
        await database.enable_fair_scheduling(
            "test-host",
            NOW,
            initial_resources=(
                (
                    WorkCapability(kind=("test", "logical_stage_work"), payload_schema_version=1),
                    (ResourceRequest(name="cpu"),),
                ),
            ),
        )
        await database.offer_fair_dispatch_candidates(
            scope="test-host", work_identifiers=("fresh",), now_utc_ns=NOW
        )
        if changed_lane:
            stage = stage.model_copy(update={"resources": ()})
        await database.register_stage(stage, created_at_utc_ns=NOW + 1)
        async with database._connections.reader() as connection:
            row = await (
                await connection.execute(
                    "SELECT stage_id,lane FROM fair_offers WHERE work_item_id='fresh'"
                )
            ).fetchone()
        if changed_lane:
            assert row is None
        else:
            assert row is not None and tuple(row) == (stage.identifier, "cpu")


async def test_unknown_legacy_bucket_does_not_reserve_public_root_identifier(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database, "explicit", root="legacy", service=SchedulingServiceClass.INTERACTIVE)
        assert (
            await database.work_scheduling("explicit")
        ).service_class is SchedulingServiceClass.INTERACTIVE


class _Abort(Exception):
    pass


async def test_outer_writer_rollback_undoes_claim_turn_and_every_reservation(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _seed(database, "rollback", root="root")
        await database.enable_fair_scheduling("test-host", NOW)
        with pytest.raises(_Abort):
            async with database.transaction():
                assert (await _claim(database, stage.identifier)).lease is not None
                assert len(await database.fair_admissions("test-host")) == 1
                raise _Abort
        assert await database.fair_admissions("test-host") == ()
        work = await database.work("rollback")
        assert work["attempt"] == 0 and work["operations"] == [] and work["state"] == "pending"
        async with database._connections.reader() as connection:
            for table in ("resource_permits", "operations", "stage_execution_tokens"):
                row = await (await connection.execute(f"SELECT count(*) FROM {table}")).fetchone()
                assert row is not None and row[0] == 0
