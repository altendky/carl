"""One readiness page publishes all runnable alternatives with bounded SQL calls."""

# Inspect immutable bindings and count awaited calls, not SQLite trigger traces.
# pyright: reportPrivateUsage=false

import hashlib
from pathlib import Path
from typing import cast

import apsw
import pytest

from carl._tests.test_fair_admission_cost import _CountingWriter
from carl._tests.test_resident_storage import _resident
from carl._tests.test_stage_storage import NOW, _definition
from carl._tests.test_worker import _definition as _work_definition
from carl._tests.test_worker import _requester
from carl.core.fair_scheduling import FAIR_OFFER_NS, SchedulingContext
from carl.core.json import encode_json
from carl.core.resources import ResourceRequest
from carl.core.stages import StageDefinition
from carl.core.work import WorkCapability
from carl.io.db import AsyncConnection
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
SCOPE = "test-host"
KIND = ("test", "offers")
RESIDENT_KIND = ("test", "resident-offers")
UNKNOWN_KIND = ("test", "no-profile")
CAPABILITY = WorkCapability(kind=KIND, payload_schema_version=1)
RESIDENT_CAPABILITY = WorkCapability(kind=RESIDENT_KIND, payload_schema_version=1)


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _work(database: Database, identifier: str, *, kind: tuple[str, ...] = KIND) -> None:
    await database.enqueue_work(
        _work_definition(identifier=identifier, kind=kind),
        _requester(identifier).model_copy(
            update={"scheduling": SchedulingContext(root_request_identifier="root")}
        ),
        event_identifier="enqueue-" + identifier,
        enqueued_at_utc_ns=NOW,
    )


async def _stage(
    database: Database,
    identifier: str,
    *,
    kind: tuple[str, ...] = KIND,
    resources: tuple[ResourceRequest, ...] = (ResourceRequest(name="cpu"),),
    resident: bool = False,
    not_before: int = 0,
) -> StageDefinition:
    await _work(database, identifier, kind=kind)
    definition = _definition(
        identifier, identifier="stage-" + identifier, resources=resources, not_before=not_before
    )
    if resident:
        definition = definition.model_copy(
            update={"resident_session_identifier": "old-" + identifier}
        )
    await database.register_stage(definition, created_at_utc_ns=NOW)
    return definition


async def _enable(database: Database) -> None:
    await database.enable_fair_scheduling(
        SCOPE,
        NOW,
        initial_resources=(
            (CAPABILITY, (ResourceRequest(name="cpu", units=2),)),
            (RESIDENT_CAPABILITY, (ResourceRequest(name="cpu"),)),
        ),
        resident_capabilities=(RESIDENT_CAPABILITY,),
        resident_state_bytes=64,
    )


async def _offers(database: Database) -> dict[str, tuple[object, ...]]:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                """SELECT work_item_id,lane,stage_id,signature,resources_json,
                    ready_since_utc_ns,expires_at_utc_ns FROM fair_offers WHERE scope=? ORDER BY rowid""",
                (SCOPE,),
            )
        ).fetchall()
    return {str(row[0]): tuple(row[1:]) for row in rows}


def _signature(value: str, *, initial: bool = False) -> str:
    return ("initial:" if initial else "") + hashlib.sha256(value.encode()).hexdigest()


async def test_mixed_page_preserves_bindings_profiles_all_alternatives_and_input_order(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "mixed.sqlite3", initialize=True) as database:
        async with database.transaction():
            await _work(database, "initial")
            await _work(database, "initial-resident", kind=RESIDENT_KIND)
            http = await _stage(database, "http-é", resources=(ResourceRequest(name="http"),))
            metadata = await _stage(database, "metadata", resources=())
            resident = await _stage(database, "resident", kind=RESIDENT_KIND, resident=True)
            unprofiled = await _stage(database, "staged-unprofiled", kind=UNKNOWN_KIND)
            await _work(database, "initial-unprofiled", kind=UNKNOWN_KIND)
            await _stage(database, "resident-unprofiled", kind=UNKNOWN_KIND, resident=True)
            await _work(database, "inactive")
            await _work(database, "future-work")
            await _stage(database, "future-stage", not_before=NOW + 1)
            await _work(database, "missing-projection")
            await _enable(database)
            async with database._connections.writer() as connection:
                await connection.execute(
                    "UPDATE work_items SET state='completed' WHERE id='inactive'"
                )
                await connection.execute(
                    "UPDATE work_items SET eligible_at_utc_ns=? WHERE id='future-work'", (NOW + 1,)
                )
                await connection.execute(
                    "DELETE FROM fair_ready WHERE work_item_id='missing-projection'"
                )
        identifiers = (
            "http-é",
            "initial",
            "http-é",
            "resident",
            "missing",
            "initial-unprofiled",
            "metadata",
            "inactive",
            "future-work",
            "future-stage",
            "initial-resident",
            "resident-unprofiled",
            "staged-unprofiled",
            "missing-projection",
            "initial",
        )
        await database.offer_fair_dispatch_candidates(
            scope=SCOPE, work_identifiers=identifiers, now_utc_ns=NOW
        )
        offers = await _offers(database)
        assert tuple(offers) == (
            "http-é",
            "initial",
            "resident",
            "metadata",
            "initial-resident",
            "staged-unprofiled",
        )
        resident_resources = encode_json(
            [
                ResourceRequest(name="resident_sessions").model_dump(mode="json"),
                ResourceRequest(name="resident_state_bytes", units=64).model_dump(mode="json"),
            ]
        )
        for identifier, lane, definition in (
            ("http-é", "http", http),
            ("metadata", "metadata", metadata),
            ("resident", "resident", resident),
            ("staged-unprofiled", "cpu", unprofiled),
        ):
            resources = (
                resident_resources
                if identifier == "resident"
                else encode_json([value.model_dump(mode="json") for value in definition.resources])
            )
            assert offers[identifier] == (
                lane,
                definition.identifier,
                _signature(definition.model_dump_json()),
                resources,
                NOW,
                NOW + FAIR_OFFER_NS,
            )
        assert offers["initial"] == (
            "cpu",
            None,
            _signature(encode_json(["initial", encode_json(list(KIND)), 1, 0]), initial=True),
            encode_json([ResourceRequest(name="cpu", units=2).model_dump(mode="json")]),
            NOW,
            NOW + FAIR_OFFER_NS,
        )
        assert offers["initial-resident"] == (
            "resident",
            None,
            _signature(
                encode_json(["initial", encode_json(list(RESIDENT_KIND)), 1, 0]), initial=True
            ),
            resident_resources,
            NOW,
            NOW + FAIR_OFFER_NS,
        )
        # Refresh existing offers and include alternatives whose deadlines passed.
        await database.offer_fair_dispatch_candidates(
            scope=SCOPE, work_identifiers=identifiers, now_utc_ns=NOW + 100
        )
        refreshed = await _offers(database)
        assert set(refreshed) == set(offers) | {"future-work", "future-stage"}
        for identifier in offers:
            assert refreshed[identifier][:-1] == offers[identifier][:-1]
            assert refreshed[identifier][-1] == NOW + 100 + FAIR_OFFER_NS
        assert await database.fair_admissions(SCOPE) == ()


@pytest.mark.parametrize("state", ("opening", "ready", "closing", "quarantined"))
async def test_live_resident_actor_is_not_advertised_by_another_driver(
    tmp_path: Path, state: str
) -> None:
    async with Database.managed(tmp_path / "resident.sqlite3", initialize=True) as database:
        await _stage(database, "owned", kind=RESIDENT_KIND, resident=True)
        await _enable(database)
        assert await _resident(database, "actor", work="owned", scope="other-host") is not None
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE resident_sessions SET state=? WHERE id='actor'", (state,)
            )
        await database.offer_fair_dispatch_candidates(
            scope=SCOPE, work_identifiers=("owned",), now_utc_ns=NOW
        )
        assert await _offers(database) == {}
        async with database._connections.writer() as connection:
            await connection.execute("UPDATE resident_sessions SET state='closed' WHERE id='actor'")
        await database.offer_fair_dispatch_candidates(
            scope=SCOPE, work_identifiers=("owned",), now_utc_ns=NOW
        )
        assert (await _offers(database))["owned"][0] == "resident"


async def test_next_writer_reads_other_connections_committed_definitions_and_profiles(
    tmp_path: Path,
) -> None:
    path = tmp_path / "fresh.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        async with database.transaction():
            await _work(database, "initial")
            stage = await _stage(database, "stage")
            await _work(database, "cancelled")
            await _enable(database)
        changed = stage.model_copy(update={"resources": (ResourceRequest(name="http", units=2),)})
        async with Database.managed(path) as other, other.transaction():
            async with other._connections.writer() as connection:
                await connection.execute(
                    "UPDATE work_items SET payload_schema_version=2,attempt=2 WHERE id='initial'"
                )
                await connection.execute(
                    "UPDATE work_stages SET definition_json=? WHERE id=?",
                    (changed.model_dump_json(), stage.identifier),
                )
                await connection.execute(
                    "UPDATE work_items SET state='cancelled' WHERE id='cancelled'"
                )
            await other.enable_fair_scheduling(
                SCOPE,
                NOW,
                initial_resources=(
                    (
                        WorkCapability(kind=KIND, payload_schema_version=2),
                        (ResourceRequest(name="cpu", units=3),),
                    ),
                ),
            )
        await database.offer_fair_dispatch_candidates(
            scope=SCOPE, work_identifiers=("initial", "stage", "cancelled"), now_utc_ns=NOW
        )
        offers = await _offers(database)
        assert tuple(offers) == ("initial", "stage")
        assert offers["initial"][2:4] == (
            _signature(encode_json(["initial", encode_json(list(KIND)), 2, 2]), initial=True),
            encode_json([ResourceRequest(name="cpu", units=3).model_dump(mode="json")]),
        )
        assert offers["stage"][:4] == (
            "http",
            stage.identifier,
            _signature(changed.model_dump_json()),
            encode_json([ResourceRequest(name="http", units=2).model_dump(mode="json")]),
        )


async def test_empty_disabled_and_invalid_pages_preserve_existing_offers(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "bounds.sqlite3", initialize=True) as database:
        for identifiers in ((), ("one",)):
            with pytest.raises(ValueError, match="not been enabled"):
                await database.offer_fair_dispatch_candidates(
                    scope=SCOPE, work_identifiers=identifiers, now_utc_ns=NOW
                )
        await _work(database, "one")
        await _enable(database)
        await database.offer_fair_dispatch_candidates(
            scope=SCOPE, work_identifiers=("one",), now_utc_ns=NOW
        )
        before = await _offers(database)
        await database.offer_fair_dispatch_candidates(
            scope=SCOPE, work_identifiers=(), now_utc_ns=NOW + 1
        )
        for identifiers, now in ((("one",) * 257, NOW), (("one",), -1), (("one",), True)):
            with pytest.raises(ValueError, match="page limit"):
                await database.offer_fair_dispatch_candidates(
                    scope=SCOPE, work_identifiers=identifiers, now_utc_ns=now
                )
        assert await _offers(database) == before


async def test_failed_bulk_publication_rolls_back_earlier_refresh_and_insert(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "rollback.sqlite3", initialize=True) as database:
        async with database.transaction():
            for identifier in ("existing", "first", "second"):
                await _work(database, identifier)
            await _enable(database)
        await database.offer_fair_dispatch_candidates(
            scope=SCOPE, work_identifiers=("existing",), now_utc_ns=NOW
        )
        before = await _offers(database)
        async with database._connections.writer() as connection:
            await connection.execute("""CREATE TRIGGER fail_offer BEFORE INSERT ON fair_offers
                WHEN NEW.work_item_id='second' BEGIN SELECT RAISE(ABORT,'injected offer failure'); END""")
        with pytest.raises(apsw.ConstraintError, match="injected offer failure"):
            await database.offer_fair_dispatch_candidates(
                scope=SCOPE, work_identifiers=("existing", "first", "second"), now_utc_ns=NOW + 100
            )
        assert await _offers(database) == before
        assert await database.fair_admissions(SCOPE) == ()


async def test_readiness_page_sql_calls_do_not_grow_per_offered_work(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    counts: dict[int, int] = {}
    for size in (1, 32, 256):
        async with Database.managed(tmp_path / f"cost-{size}.sqlite3", initialize=True) as database:
            identifiers = tuple(f"work-{index:03d}" for index in range(size))
            async with database.transaction():
                for identifier in identifiers:
                    await _work(database, identifier)
                await _enable(database)
            original = database._connections._writer
            counter = _CountingWriter(original)
            database._connections._writer = cast(AsyncConnection, cast(object, counter))
            try:
                await database.offer_fair_dispatch_candidates(
                    scope=SCOPE, work_identifiers=identifiers, now_utc_ns=NOW
                )
            finally:
                database._connections._writer = original
            assert tuple(await _offers(database)) == identifiers
            assert await database.fair_admissions(SCOPE) == ()
            counts[size] = len(counter.statements)
    request.node.user_properties.extend(
        (f"offer_sql_calls_{size}", count) for size, count in counts.items()
    )
    assert max(counts.values()) <= min(counts.values()) + 1, counts
    assert max(counts.values()) <= 8, counts
