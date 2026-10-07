"""Offline cutovers rebuild complete definitions without rewriting history."""

import json
from pathlib import Path
from time import perf_counter_ns

import pytest

from carl._tests.test_facebook_images import URL, _reference
from carl._tests.test_facebook_work import _payload
from carl._tests.test_worker import _provenance
from carl.core.components import Component, ComponentId
from carl.core.facebook_images import CollectImagePayload, collect_image_work
from carl.core.facebook_listing import (
    RequestFacebookListingDetailsPayload,
    request_facebook_listing_details_work,
)
from carl.core.facebook_refresh import RefreshSearchPayload, refresh_search_work
from carl.core.facebook_work import CollectItemPayload, collect_item_work, collect_search_work
from carl.core.http import RequestPlan
from carl.core.models import JsonValue
from carl.core.pipeline import LISTING_PIPELINE_WORK_KIND, SEARCH_PIPELINE_WORK_KIND
from carl.core.work import SchedulingScopeKind, WorkCapability, WorkDefinition, WorkRequester
from carl.io.sqlite import Database
from carl.route_cutover import rebuild_work_definition

OLD = ("proton", "personal", "carl")
NEW = ("decodo", "personal", "datacenter")
ITEM = ("decodo", "personal", "carl")


def _definitions() -> tuple[WorkDefinition, ...]:
    search = _payload().model_copy(update={"routing": OLD, "retry_attempt_offset": 3})
    return (
        collect_search_work(identifier="search", payload=search, not_before_utc_ns=7),
        refresh_search_work(
            identifier="refresh",
            payload=RefreshSearchPayload(
                base_search_run_record_identifier="old-run",
                search_work_identifier="search",
                search=search,
                item_routing=ITEM,
                image_routing=OLD,
            ),
            not_before_utc_ns=7,
        ),
        request_facebook_listing_details_work(
            identifier="listing",
            payload=RequestFacebookListingDetailsPayload(
                listing_identifier="123", item_routing=ITEM, image_routing=OLD
            ),
            not_before_utc_ns=7,
        ),
        collect_item_work(
            identifier="item",
            payload=CollectItemPayload(
                listing_id="123",
                request_plan=RequestPlan(
                    url="https://www.facebook.com/marketplace/item/123/", routing=OLD
                ),
            ),
            not_before_utc_ns=7,
        ),
        collect_image_work(
            identifier="image",
            payload=CollectImagePayload(
                reference_record_identifier="reference",
                reference=_reference(),
                request_plan=RequestPlan(url=URL, follow_redirects=False, routing=OLD),
            ),
            not_before_utc_ns=7,
        ),
    )


@pytest.mark.parametrize("definition", _definitions(), ids=lambda definition: definition.identifier)
def test_cutover_rebuilds_typed_payload_scopes_and_identity(definition: WorkDefinition) -> None:
    original_json = json.dumps(definition.payload, sort_keys=True)
    revised = rebuild_work_definition(
        identifier=definition.identifier,
        kind=definition.kind,
        payload=definition.payload,
        not_before_utc_ns=definition.not_before_utc_ns,
        replacements={OLD: NEW},
    )
    assert revised is not None
    assert revised.identifier == definition.identifier
    assert revised.kind == definition.kind
    assert revised.not_before_utc_ns == 7
    assert json.dumps(definition.payload, sort_keys=True) == original_json
    assert json.dumps(list(OLD)) not in json.dumps(revised.payload)
    assert json.dumps(list(NEW)) in json.dumps(revised.payload)
    assert not any(
        scope.kind is SchedulingScopeKind.NETWORK_PATH and scope.identity == OLD
        for scope in revised.scopes
    )
    if definition.identifier in ("search", "item", "image"):
        assert any(scope.identity == NEW for scope in revised.scopes)
    if definition.identifier != "image":
        assert revised.deduplication_identity != definition.deduplication_identity
    if definition.identifier == "search":
        assert isinstance(revised.payload, dict)
        assert revised.payload["retry_attempt_offset"] == 3
    if definition.identifier in ("listing", "refresh"):
        assert isinstance(revised.payload, dict)
        assert revised.payload["item_routing"] == list(ITEM)
    assert (
        rebuild_work_definition(
            identifier=revised.identifier,
            kind=revised.kind,
            payload=revised.payload,
            not_before_utc_ns=7,
            replacements={OLD: NEW},
        )
        is None
    )


@pytest.mark.parametrize("kind", (SEARCH_PIPELINE_WORK_KIND, LISTING_PIPELINE_WORK_KIND))
def test_legacy_pipeline_image_route_is_migrated(kind: tuple[str, ...]) -> None:
    options: dict[str, JsonValue] = {
        "stop_after": "images",
        "facebook_decodo_route": "carl",
        "facebook_image_route": "carl",
    }
    payload: dict[str, JsonValue] = (
        {
            "request_identifier": "request",
            "request_sha256": "0" * 64,
            "intent_record_identifier": "intent",
            "search_work_identifiers": ["search"],
            "options": options,
        }
        if kind == SEARCH_PIPELINE_WORK_KIND
        else {
            "root_work_identifier": "root",
            "marketplace": "facebook",
            "external_identifier": "123",
            "occurrence_record_identifier": "occurrence",
            "maximum_images": 10,
            "analysis_authorized": False,
            "options": options,
        }
    )
    revised = rebuild_work_definition(
        identifier="pipeline",
        kind=kind,
        payload=payload,
        not_before_utc_ns=7,
        replacements={OLD: NEW},
    )
    assert revised is not None and isinstance(revised.payload, dict)
    revised_options = revised.payload["options"]
    assert isinstance(revised_options, dict)
    assert revised_options["facebook_image_network_path"] == list(NEW)
    assert "facebook_image_route" not in revised_options
    assert revised_options["facebook_decodo_route"] == "carl"
    assert options["facebook_image_route"] == "carl"


def test_unknown_unaffected_work_is_not_touched() -> None:
    assert (
        rebuild_work_definition(
            identifier="other",
            kind=("other",),
            payload={"query": "proton/personal/carl", "route_name": "carl"},
            not_before_utc_ns=0,
            replacements={OLD: NEW},
        )
        is None
    )


def test_unknown_affected_work_fails_closed() -> None:
    with pytest.raises(ValueError, match="No route-cutover definition builder"):
        rebuild_work_definition(
            identifier="other",
            kind=("other",),
            payload={"routing": list(OLD)},
            not_before_utc_ns=0,
            replacements={OLD: NEW},
        )


def test_other_proton_route_is_not_implicitly_redirected() -> None:
    original = _payload().model_copy(update={"routing": ("proton", "personal", "explicit")})
    assert (
        rebuild_work_definition(
            identifier="other-route",
            kind=_definitions()[0].kind,
            payload=original.as_json(),
            not_before_utc_ns=0,
            replacements={OLD: NEW},
        )
        is None
    )


def test_ambiguous_pipeline_routes_fail_closed() -> None:
    with pytest.raises(ValueError, match="both legacy and explicit routes"):
        rebuild_work_definition(
            identifier="pipeline",
            kind=SEARCH_PIPELINE_WORK_KIND,
            payload={
                "options": {
                    "facebook_image_route": "carl",
                    "facebook_image_network_path": list(ITEM),
                }
            },
            not_before_utc_ns=0,
            replacements={OLD: NEW},
        )


async def _enqueue(database: Database, definition: WorkDefinition) -> None:
    await database.enqueue_work(
        definition,
        WorkRequester(
            request_identifier=f"request-{definition.identifier}",
            kind=("test",),
            identifier="workspace",
            context={"track_identifier": "unchanged"},
        ),
        event_identifier=f"enqueue-{definition.identifier}",
        enqueued_at_utc_ns=10,
    )


async def _begin(database: Database) -> None:
    await database.begin_operation(
        operation_id="cutover",
        component=Component(
            identifier=ComponentId(("test", "cutover")),
            output_schema_version=1,
            implementation=rebuild_work_definition,
        ),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-10-06T00:00:00+00:00",
    )


async def _cutover(database: Database, now: int = 100) -> int:
    return await database.cutover_work_routes(
        replacements={OLD: NEW},
        operation_identifier="cutover",
        audit_record_identifier="audit",
        at_utc_ns=now,
        started_monotonic_ns=perf_counter_ns(),
    )


@pytest.mark.anyio
async def test_database_cutover_preserves_terminal_errors_requesters_and_completed_work(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pending, terminal, completed = _definitions()[:3]
        for definition in (pending, terminal, completed):
            await _enqueue(database, definition)
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET state='terminal_failure',attempt=3,error_json=? WHERE id=?",
                ('{"kind":"retained_failure"}', terminal.identifier),
            )
            await connection.execute(
                "UPDATE work_items SET state='completed',attempt=1,result_json=? WHERE id=?",
                ('{"state":"completed"}', completed.identifier),
            )
        await _begin(database)
        assert await _cutover(database) == 2
        _, _, audit = await database.get_record("audit")
        assert isinstance(audit, dict) and isinstance(audit["revisions"], list)
        assert len(audit["revisions"]) == 2
        assert {row["work_identifier"] for row in audit["revisions"] if isinstance(row, dict)} == {
            pending.identifier,
            terminal.identifier,
        }
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT id,state,attempt,error_json,payload_json FROM work_items ORDER BY id"
            )
            rows = {str(row[0]): row for row in await cursor.fetchall()}
            cursor = await connection.execute("SELECT count(*) FROM work_requests")
            assert await cursor.fetchall() == [(3,)]
            cursor = await connection.execute("SELECT count(*) FROM work_events")
            assert await cursor.fetchall() == [(3,)]
        assert rows[terminal.identifier][1:4] == (
            "terminal_failure",
            3,
            '{"kind":"retained_failure"}',
        )
        assert json.loads(str(rows[completed.identifier][4])) == completed.payload
        assert json.dumps(list(OLD)) not in str(rows[pending.identifier][4])


@pytest.mark.anyio
async def test_database_cutover_rejects_live_lease_without_partial_changes(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        definition = _definitions()[0]
        await _enqueue(database, definition)
        claimed = await database.claim_work(
            supported_capabilities=(
                WorkCapability(kind=definition.kind, payload_schema_version=3),
            ),
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: 10,
            event_identifier="claim",
        )
        assert claimed.lease is not None
        await _begin(database)
        with pytest.raises(ValueError, match="release its lease"):
            await _cutover(database)
        async with database._connections.reader() as connection:
            cursor = await connection.execute("SELECT state,payload_json FROM work_items")
            rows = await cursor.fetchall()
            cursor = await connection.execute(
                "SELECT count(*) FROM records WHERE object_id='audit'"
            )
            assert await cursor.fetchall() == [(0,)]
        assert rows[0][0] == "leased"
        assert json.loads(str(rows[0][1])) == definition.payload


@pytest.mark.anyio
async def test_database_cutover_recovers_expired_lease_without_consuming_attempt(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        definition = _definitions()[0]
        await _enqueue(database, definition)
        claimed = await database.claim_work(
            supported_capabilities=(
                WorkCapability(kind=definition.kind, payload_schema_version=3),
            ),
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=20,
            utc_now_ns=lambda: 10,
            event_identifier="claim",
        )
        assert claimed.lease is not None
        await _begin(database)
        assert await _cutover(database) == 1
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT state,attempt,lease_token,lease_owner,lease_expires_at_utc_ns FROM work_items"
            )
            assert await cursor.fetchall() == [("pending", 1, None, None, None)]
            cursor = await connection.execute(
                "SELECT event_kind FROM work_events ORDER BY sequence"
            )
            assert await cursor.fetchall() == [("enqueued",), ("claimed",), ("lease_expired",)]


@pytest.mark.anyio
async def test_database_cutover_dedup_collision_rolls_back_all_work(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        original = _definitions()[0]
        target = rebuild_work_definition(
            identifier="target",
            kind=original.kind,
            payload=original.payload,
            not_before_utc_ns=7,
            replacements={OLD: NEW},
        )
        assert target is not None
        await _enqueue(database, original)
        await _enqueue(database, target)
        await _begin(database)
        with pytest.raises(ValueError, match="collide"):
            await _cutover(database)
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT payload_json FROM work_items WHERE id=?", (original.identifier,)
            )
            rows = await cursor.fetchall()
            cursor = await connection.execute(
                "SELECT count(*) FROM records WHERE object_id='audit'"
            )
            assert await cursor.fetchall() == [(0,)]
        assert json.loads(str(rows[0][0])) == original.payload
