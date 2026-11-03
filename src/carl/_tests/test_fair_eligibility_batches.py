"""Batched live facts preserve initial, staged and resident admission fences."""

# These tests inspect and mutate durable facts after canonical offer publication.
# pyright: reportPrivateUsage=false

import hashlib
import json
from pathlib import Path

import pytest

from carl._tests.test_fair_storage import _seed
from carl._tests.test_resident_storage import _resident
from carl._tests.test_stage_storage import CAPABILITY, NOW, _claim, _definition, _release_resource
from carl.core.fair_scheduling import FairLane
from carl.core.resources import ResourceRequest
from carl.core.stages import StageDefinition, StageWaitKind, WaitWork
from carl.core.work import SchedulingScope, SchedulingScopeKind, SlidingWindowRateConstraint
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
SCOPE = "test-host"
type Offers = tuple[tuple[str, str | None], ...]


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _eligible(
    database: Database, offers: Offers, *, lane: FairLane = FairLane.CPU, now: int = NOW
) -> frozenset[str]:
    # Exercise the actual writer snapshot used by admission, not a reader cache.
    async with database._connections.writer() as connection:
        return await database._fair_offers_eligible(
            connection, scope=SCOPE, offers=offers, lane=lane, now_utc_ns=now
        )


async def _publish(database: Database, identifiers: tuple[str, ...], *, now: int = NOW) -> None:
    await database.offer_fair_dispatch_candidates(
        scope=SCOPE, work_identifiers=identifiers, now_utc_ns=now
    )


async def _stage(
    database: Database, identifier: str, *, network: bool = False, resident: bool = False
) -> StageDefinition:
    await _seed(database, identifier, root=identifier, register=False)
    stage = _definition(
        identifier,
        identifier="stage-" + identifier,
        network=network,
        resources=(ResourceRequest(name="http" if network else "cpu"),),
    )
    if resident:
        stage = stage.model_copy(update={"resident_session_identifier": "previous-" + identifier})
    await database.register_stage(stage, created_at_utc_ns=NOW)
    return stage


async def test_staged_batch_keeps_work_stage_signature_and_dependency_fences(
    tmp_path: Path,
) -> None:
    identifiers = (
        "ready",
        "future-stage",
        "changed-signature",
        "leased-live",
        "leased-expired",
        "work-time-only",
        "cancelled",
        "completed-stage",
        "dependency-wait",
        "dependency-done",
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stages = {identifier: await _stage(database, identifier) for identifier in identifiers}
        await _seed(database, "parent-wait", root="parent-wait", register=False)
        await _seed(database, "parent-done", root="parent-done", register=False)
        await database.enable_fair_scheduling(SCOPE, NOW)
        await _publish(database, identifiers)
        future = stages["future-stage"].model_copy(update={"not_before_utc_ns": NOW + 1})
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_stages SET definition_json=? WHERE id=?",
                (future.model_dump_json(), future.identifier),
            )
            await connection.execute(
                "UPDATE fair_offers SET signature=? WHERE work_item_id='future-stage'",
                (hashlib.sha256(future.model_dump_json().encode()).hexdigest(),),
            )
            await connection.execute(
                "UPDATE fair_offers SET signature='stale' WHERE work_item_id='changed-signature'"
            )
            for identifier, expiry in (("leased-live", NOW + 1), ("leased-expired", NOW)):
                await connection.execute(
                    "UPDATE work_items SET state='leased',lease_token=?,lease_owner='owner',lease_expires_at_utc_ns=? WHERE id=?",
                    (identifier, expiry, identifier),
                )
            await connection.execute(
                "UPDATE work_items SET eligible_at_utc_ns=? WHERE id='work-time-only'", (NOW + 1,)
            )
            await connection.execute("UPDATE work_items SET state='cancelled' WHERE id='cancelled'")
            await connection.execute(
                "UPDATE work_stages SET state='completed' WHERE work_item_id='completed-stage'"
            )
            await connection.executemany(
                "INSERT INTO stage_dependencies(stage_id,dependency_work_id) VALUES (?,?)",
                (
                    ("stage-dependency-wait", "parent-wait"),
                    ("stage-dependency-done", "parent-done"),
                ),
            )
            await connection.execute(
                "UPDATE work_items SET state='completed' WHERE id='parent-done'"
            )
        offers = tuple((identifier, stages[identifier].identifier) for identifier in identifiers)
        offers += (("missing", "stage-missing"), ("parent-wait", "stage-ready"))
        assert await _eligible(database, offers) == {
            "ready",
            "leased-expired",
            "work-time-only",
            "dependency-done",
        }


async def test_initial_batch_rejects_stale_registration_and_active_attempts(tmp_path: Path) -> None:
    identifiers = (
        "ready",
        "attempt-changed",
        "schema-changed",
        "resources-changed",
        "active",
        "future",
        "staged",
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier in identifiers:
            await _seed(database, identifier, root=identifier, register=False)
        await database.enable_fair_scheduling(
            SCOPE, NOW, initial_resources=((CAPABILITY, (ResourceRequest(name="cpu"),)),)
        )
        await _publish(database, identifiers)
        async with database._connections.writer() as connection:
            await connection.execute("UPDATE work_items SET attempt=1 WHERE id='attempt-changed'")
            await connection.execute(
                "UPDATE work_items SET payload_schema_version=2 WHERE id='schema-changed'"
            )
            await connection.execute(
                "UPDATE fair_offers SET resources_json='[]' WHERE work_item_id='resources-changed'"
            )
            await connection.execute(
                "INSERT INTO stage_attempts VALUES ('operation-active','active',NULL,'pending',?)",
                (NOW,),
            )
            await connection.execute(
                "UPDATE work_items SET eligible_at_utc_ns=? WHERE id='future'", (NOW + 1,)
            )
        await database.register_stage(
            _definition("staged", identifier="stage-staged"), created_at_utc_ns=NOW
        )
        assert await _eligible(
            database, tuple((identifier, None) for identifier in identifiers)
        ) == {"ready"}
        assert await _eligible(database, ()) == frozenset()


async def test_initial_resident_keeps_existing_pending_attempt_without_reusing_stale_offer(
    tmp_path: Path,
) -> None:
    identifiers = ("ready", "pending-attempt", "changed-resources", "future")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier in identifiers:
            await _seed(database, identifier, root=identifier, register=False)
        await database.enable_fair_scheduling(
            SCOPE,
            NOW,
            initial_resources=((CAPABILITY, (ResourceRequest(name="cpu"),)),),
            resident_capabilities=(CAPABILITY,),
        )
        await _publish(database, identifiers)
        async with database._connections.writer() as connection:
            await connection.execute(
                "INSERT INTO stage_attempts VALUES ('operation-pending-attempt','pending-attempt',NULL,'pending',?)",
                (NOW,),
            )
            await connection.execute(
                "UPDATE fair_offers SET resources_json='[]' WHERE work_item_id='changed-resources'"
            )
            await connection.execute(
                "UPDATE work_items SET eligible_at_utc_ns=? WHERE id='future'", (NOW + 1,)
            )
        assert await _eligible(
            database,
            tuple((identifier, None) for identifier in identifiers),
            lane=FairLane.RESIDENT,
        ) == {"ready", "pending-attempt"}


@pytest.mark.parametrize("resident", (False, True))
async def test_signature_uses_each_branch_existing_json_representation(
    tmp_path: Path, resident: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _stage(database, "unicode-é", resident=resident)
        await database.enable_fair_scheduling(
            SCOPE,
            NOW,
            initial_resources=((CAPABILITY, (ResourceRequest(name="cpu"),)),),
            resident_capabilities=(CAPABILITY,) if resident else (),
        )
        await _publish(database, (stage.work_item_identifier,))
        raw = json.dumps(json.loads(stage.model_dump_json()), ensure_ascii=True, indent=2)
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_stages SET definition_json=? WHERE id=?", (raw, stage.identifier)
            )
        lane = FairLane.RESIDENT if resident else FairLane.CPU
        offers = ((stage.work_item_identifier, stage.identifier),)
        assert await _eligible(database, offers, lane=lane) == (
            frozenset() if resident else {"unicode-é"}
        )
        if resident:
            async with database._connections.writer() as connection:
                await connection.execute(
                    "UPDATE fair_offers SET signature=? WHERE work_item_id=?",
                    (hashlib.sha256(raw.encode()).hexdigest(), stage.work_item_identifier),
                )
            assert await _eligible(database, offers, lane=lane) == {"unicode-é"}


async def test_resident_batch_keeps_time_and_live_session_fences(tmp_path: Path) -> None:
    identifiers = ("ready", "future-work", "future-stage", "live-lease", "owned", "no-stage")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stages = {
            identifier: await _stage(database, identifier, resident=True)
            for identifier in identifiers
        }
        await database.enable_fair_scheduling(
            SCOPE,
            NOW,
            initial_resources=((CAPABILITY, (ResourceRequest(name="cpu"),)),),
            resident_capabilities=(CAPABILITY,),
            resident_state_bytes=64,
        )
        await _publish(database, identifiers)
        # An incarnation on another host still fences a duplicate resident here.
        assert (
            await _resident(database, "actor-owned", work="owned", scope="other-host") is not None
        )
        future = stages["future-stage"].model_copy(update={"not_before_utc_ns": NOW + 1})
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET eligible_at_utc_ns=? WHERE id='future-work'", (NOW + 1,)
            )
            await connection.execute(
                "UPDATE work_stages SET definition_json=? WHERE id=?",
                (future.model_dump_json(), future.identifier),
            )
            await connection.execute(
                "UPDATE fair_offers SET signature=? WHERE work_item_id='future-stage'",
                (hashlib.sha256(future.model_dump_json().encode()).hexdigest(),),
            )
            await connection.execute(
                "UPDATE work_items SET state='leased',lease_token='lease',lease_owner='owner',lease_expires_at_utc_ns=? WHERE id='live-lease'",
                (NOW + 1,),
            )
            await connection.execute(
                "UPDATE work_stages SET state='completed' WHERE work_item_id='no-stage'"
            )
        assert await _eligible(
            database,
            tuple((identifier, stages[identifier].identifier) for identifier in identifiers),
            lane=FairLane.RESIDENT,
        ) == {"ready", "live-lease"}


async def test_acquisition_peers_keep_active_attempts_after_expiry(tmp_path: Path) -> None:
    cases = (
        ("live", NOW + 1, "pending"),
        ("expired-active", NOW, "active"),
        ("expired-pending", NOW, "pending"),
        ("cancelling", NOW + 1, "pending"),
        ("other", NOW + 1, "active"),
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stages = {}
        for identifier, _, _ in cases:
            stages[identifier] = await _stage(database, identifier)
            await _stage(database, "peer-" + identifier)
        await database.enable_fair_scheduling(SCOPE, NOW)
        await _publish(database, tuple(stages))
        async with database._connections.writer() as connection:
            for identifier, expiry, state in cases:
                identity = json.dumps(["resource", identifier])
                await connection.execute(
                    "UPDATE work_items SET acquisition_resource_identity_json=? WHERE id=?",
                    (identity, identifier),
                )
                await connection.execute(
                    "UPDATE work_items SET acquisition_resource_identity_json=?,state=?,lease_token=?,lease_owner='owner',lease_expires_at_utc_ns=? WHERE id=?",
                    (
                        json.dumps(["different"]) if identifier == "other" else identity,
                        "cancelling" if identifier == "cancelling" else "leased",
                        identifier,
                        expiry,
                        "peer-" + identifier,
                    ),
                )
                await connection.execute(
                    "UPDATE stage_attempts SET state=? WHERE work_item_id=?",
                    (state, "peer-" + identifier),
                )
        assert await _eligible(
            database, tuple((identifier, stage.identifier) for identifier, stage in stages.items())
        ) == {"expired-pending", "other"}


async def test_network_detection_includes_http_resources_and_templates(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        cpu = await _stage(database, "cpu")
        template = await _stage(database, "template", network=True)
        await _seed(database, "http", root="http", register=False)
        http = _definition(
            "http", identifier="stage-http", resources=(ResourceRequest(name="http"),)
        )
        await database.register_stage(http, created_at_utc_ns=NOW)
        await database.enable_fair_scheduling(SCOPE, NOW)
        await _publish(database, ("cpu", "template", "http"))
        cpu_offers = (("cpu", cpu.identifier),)
        http_offers = (("http", http.identifier), ("template", template.identifier))
        assert await _eligible(database, cpu_offers) == {"cpu"}
        assert await _eligible(database, http_offers, lane=FairLane.HTTP) == {"http", "template"}
        async with database._connections.writer() as connection:
            await connection.execute(
                "INSERT INTO network_connectivity(singleton,paused,next_probe_utc_ns) VALUES (1,1,?)",
                (NOW + 1,),
            )
        assert await _eligible(database, cpu_offers) == {"cpu"}
        assert await _eligible(database, http_offers, lane=FairLane.HTTP) == frozenset()


async def test_committed_other_connection_changes_next_writer_facts_and_winner(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        first = await _stage(database, "first")
        second = await _stage(database, "second")
        await database.enable_fair_scheduling(SCOPE, NOW)
        await _publish(database, ("first", "second"))
        offers = (("first", first.identifier), ("second", second.identifier))
        assert await _eligible(database, offers) == {"first", "second"}
        assert (await _claim(database, second.identifier, token="before-commit")).lease is None
        async with Database.managed(path) as other, other._connections.writer() as connection:
            await connection.execute("UPDATE work_items SET state='cancelled' WHERE id='first'")
        assert await _eligible(database, offers) == {"second"}
        admitted = await _claim(
            database, second.identifier, token="after-commit", now=NOW + 100_000_001
        )
        assert admitted.lease is not None
        assert [turn.work_item_identifier for turn in await database.fair_admissions(SCOPE)] == [
            "second"
        ]


async def test_exhausted_work_rate_blocks_pending_attempt_but_not_active_continuation(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        active = await _stage(database, "active")
        pending = await _stage(database, "pending")
        await database.register_constraint(
            SlidingWindowRateConstraint(
                identifier=("test", "logical-starts"),
                scope=SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                maximum_starts=1,
                period_ns=60_000_000_000,
            ),
            registered_at_utc_ns=NOW,
        )
        await database.enable_fair_scheduling(SCOPE, NOW)
        await _publish(database, ("active",))
        first = (await _claim(database, active.identifier, token="initial-active")).lease
        assert first is not None
        continuation = _definition("active", identifier="continuation-active")
        await database.suspend_stage(
            first,
            WaitWork(continuation=continuation, wait_kind=StageWaitKind.TIME, result={}),
            now_utc_ns=NOW + 1,
        )
        await _release_resource(database, first)
        await _publish(database, ("pending", "active"), now=NOW + 2)
        assert await _eligible(
            database,
            (("pending", pending.identifier), ("active", continuation.identifier)),
            now=NOW + 2,
        ) == {"active"}
        resumed = await _claim(
            database, continuation.identifier, token="resumed-active", now=NOW + 2
        )
        assert resumed.lease is not None
        assert resumed.lease.work_lease.attempt == first.work_lease.attempt == 1
        assert len(await database.fair_admissions(SCOPE)) == 2
