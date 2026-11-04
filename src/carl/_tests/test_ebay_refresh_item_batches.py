"""Item-plan resumes batch retained reads while rechecking selection and bindings."""

# Offline handler tests retain real records, requester edges and progress metadata.
# pyright: reportPrivateUsage=false

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import cast

import pytest

from carl._tests.test_ebay_item_workers import _enqueue, _identifiers, _retain_records
from carl._tests.test_ebay_refresh_workers import _source_records
from carl._tests.test_stage_storage import NOW
from carl.core.ebay import EbaySearchRequest
from carl.core.ebay_items import (
    COLLECT_EBAY_ITEM_WORK_KIND,
    EXTRACT_EBAY_ITEM_WORK_KIND,
    CollectEbayItemPayload,
    EbayItemRequest,
    collect_ebay_item_work,
)
from carl.core.ebay_refresh import RefreshEbaySearchPayload, refresh_ebay_search_work
from carl.core.json import encode_json
from carl.core.models import JsonValue, RecordDraft
from carl.core.worker import AttemptContext, RetryWork
from carl.ebay_refresh_workers import (
    EbayRefreshWorkerDependencies,
    _identifier,
    build_ebay_refresh_worker_registry,
)
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
ITEM_REQUESTER = ("carl", "ebay", "search_refresh", "item")
DEADLINE = NOW + 120_000_000_000


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _payload(*, changed: bool = False) -> RefreshEbaySearchPayload:
    return RefreshEbaySearchPayload(
        base_search_run_record_identifier="base",
        search_work_identifier="search",
        search=EbaySearchRequest(query="mount" if changed else "eyepiece"),
    )


async def _parent(
    database: Database, payload: RefreshEbaySearchPayload, identifiers: Callable[[], str]
) -> None:
    _ = await _enqueue(
        database, refresh_ebay_search_work(identifier="refresh", payload=payload), identifiers
    )
    await _checkpoint(
        database,
        {
            "stage": "collecting_items",
            "refreshed_search_run_record_identifier": "fresh",
            # A checkpoint array alone is not authority for current selection.
            "item_collection_work_identifiers": ["stale-child"],
        },
    )


async def _checkpoint(database: Database, value: JsonValue) -> None:
    async with database._connections.writer() as connection:
        _ = await connection.execute(
            "UPDATE work_items SET result_json=? WHERE id='refresh'", (encode_json(value),)
        )


async def _children(database: Database, bindings: tuple[tuple[str, str], ...]) -> None:
    """Seed a large retained plan in batches rather than enqueuing per row."""
    rows: list[JsonValue] = [
        {
            "child": child,
            "requester": _identifier("refresh", "item_requester", item),
            "kind": list(
                EXTRACT_EBAY_ITEM_WORK_KIND if index % 2 == 0 else COLLECT_EBAY_ITEM_WORK_KIND
            ),
        }
        for index, (item, child) in enumerate(bindings)
    ]
    encoded = encode_json(rows)
    async with database._connections.writer() as connection:
        _ = await connection.execute(
            """INSERT INTO work_items(id,kind_parts_json,payload_schema_version,payload_json,
                deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns)
            SELECT json_extract(value,'$.child'),json_extract(value,'$.kind'),1,'{}',
                json_array(json_extract(value,'$.child')),'pending',0,?,? FROM json_each(?)""",
            (DEADLINE, NOW, encoded),
        )
        _ = await connection.execute(
            """INSERT INTO work_events(id,work_item_id,event_kind,recorded_at_utc_ns,data_json)
            SELECT 'event-'||json_extract(value,'$.child'),json_extract(value,'$.child'),
                'enqueued',?,'{}' FROM json_each(?)""",
            (NOW, encoded),
        )
        _ = await connection.execute(
            """INSERT INTO work_requests(id,work_item_id,requester_kind_parts_json,
                requester_identifier,requested_at_utc_ns,context_json,parent_work_identifier)
            SELECT 'request-'||json_extract(value,'$.child'),json_extract(value,'$.child'),?,
                json_extract(value,'$.requester'),?,'{}','refresh' FROM json_each(?)""",
            (encode_json(list(ITEM_REQUESTER)), NOW, encoded),
        )
        _ = await connection.execute(
            """INSERT INTO work_consumer_edges
            SELECT 'refresh',?,json_extract(value,'$.child') FROM json_each(?)""",
            (encode_json(list(ITEM_REQUESTER)), encoded),
        )


async def _run(
    database: Database, payload: RefreshEbaySearchPayload, identifiers: Callable[[], str]
) -> RetryWork:
    registry = build_ebay_refresh_worker_registry(
        EbayRefreshWorkerDependencies(database, identifiers, utc_now_ns=lambda: NOW)
    )
    outcome = await registry.handlers[0].execute(
        payload.model_dump(mode="json"),
        AttemptContext(
            work_item_identifier="refresh",
            lease_token="test-token",
            worker_identifier="test-worker",
            attempt=1,
            operation_identifier="test-operation",
        ),
    )
    assert isinstance(outcome, RetryWork)
    return outcome


def _count_reads(monkeypatch: pytest.MonkeyPatch, database: Database) -> dict[str, list[int]]:
    calls: dict[str, list[int]] = {
        name: [] for name in ("record", "records", "requesters", "progress", "edges", "work")
    }
    original_record = database.get_record
    original_records = database.records_by_identifiers
    original_requesters = database.requested_work_for_requesters
    original_progress = database.work_progress
    original_edges = database.missing_consumer_edges
    original_work = database.work

    async def record(identifier: str) -> tuple[tuple[str, ...], int, JsonValue]:
        assert identifier in {"base", "fresh"}, "Occurrence reads must be batched"
        calls["record"].append(1)
        return await original_record(identifier)

    async def records(
        identifiers: Sequence[str],
    ) -> tuple[tuple[str, tuple[str, ...], int, JsonValue], ...]:
        assert len(identifiers) <= 1000
        calls["records"].append(len(identifiers))
        return await original_records(identifiers)

    async def requesters(
        *, requester_kind: tuple[str, ...], requester_identifiers: Sequence[str]
    ) -> tuple[tuple[str, str], ...]:
        assert len(requester_identifiers) <= 1000
        calls["requesters"].append(len(requester_identifiers))
        return await original_requesters(
            requester_kind=requester_kind, requester_identifiers=requester_identifiers
        )

    async def progress(identifiers: Sequence[str]) -> tuple[dict[str, JsonValue], ...]:
        assert len(identifiers) <= 1000
        calls["progress"].append(len(identifiers))
        return await original_progress(identifiers)

    async def edges(
        parent: str, kind: tuple[str, ...], children: tuple[str, ...]
    ) -> tuple[str, ...]:
        assert len(children) <= 1000
        calls["edges"].append(len(children))
        return await original_edges(parent, kind, children)

    async def work(identifier: str) -> dict[str, JsonValue]:
        assert identifier == "refresh", "Child work hydration must use batched progress"
        calls["work"].append(1)
        return await original_work(identifier)

    monkeypatch.setattr(database, "get_record", record)
    monkeypatch.setattr(database, "records_by_identifiers", records)
    monkeypatch.setattr(database, "requested_work_for_requesters", requesters)
    monkeypatch.setattr(database, "work_progress", progress)
    monkeypatch.setattr(database, "missing_consumer_edges", edges)
    monkeypatch.setattr(database, "work", work)
    return calls


def _sizes(count: int) -> list[int]:
    return [min(1000, count - offset) for offset in range(0, count, 1000)]


@pytest.mark.parametrize("count", (189, 1001))
async def test_item_plan_resumes_use_bounded_reads_and_preserve_order_and_deadlines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, count: int
) -> None:
    identifiers = _identifiers()
    items = tuple(str(100_000_000_000 + index) for index in range(count))
    children = [f"child-{index}" for index in range(count)]
    payload = _payload()
    async with Database.managed(tmp_path / "items.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                *_source_records("base", items),
                *_source_records("fresh", (*items, items[0], items[-1])),
            ),
            identifiers,
        )
        await _parent(database, payload, identifiers)
        await _children(database, tuple(zip(items, children, strict=True)))
        # Repeated requester records for the same child are not multiple children.
        async with database._connections.writer() as connection:
            _ = await connection.execute(
                """INSERT INTO work_requests SELECT 'duplicate-request',work_item_id,
                    requester_kind_parts_json,requester_identifier,requested_at_utc_ns,
                    context_json,scheduling_json,parent_work_identifier
                FROM work_requests WHERE id='request-child-0'"""
            )
        calls = _count_reads(monkeypatch, database)
        for turn in range(2):
            for observed in calls.values():
                observed.clear()
            outcome = await _run(database, payload, identifiers)
            result = cast(dict[str, JsonValue], outcome.result)
            assert result["item_collection_work_identifiers"] == children
            assert result["selected_unique_listings"] == count
            assert result["reused_successful_item_pages"] == (count + 1) // 2
            assert outcome.delay_ns == 120_000_000_000
            assert outcome.reason == {"kind": "waiting_for_item_collections"}
            assert outcome.progress_watch is not None
            assert outcome.progress_watch.work_identifiers == ()
            assert outcome.progress_watch.consumer_requester_kinds == (ITEM_REQUESTER,)
            assert calls == {
                "record": [1, 1, 1],
                "records": _sizes(count) + _sizes(count + 2),
                "requesters": _sizes(count),
                "progress": _sizes(count),
                "edges": _sizes(count),
                "work": [1],
            }
            if turn == 0:
                await _checkpoint(database, outcome.result)
                async with database._connections.writer() as connection:
                    _ = await connection.execute(
                        "UPDATE work_items SET state='completed',result_json='{}' WHERE id='child-0'"
                    )


@pytest.mark.parametrize("changed", (False, True))
async def test_selection_is_recomputed_and_missing_bindings_keep_canonical_dedup(
    tmp_path: Path, changed: bool
) -> None:
    identifiers = _identifiers()
    a, b, c, d = (str(100_000_000_000 + index) for index in range(4))
    payload = _payload(changed=changed)
    async with Database.managed(tmp_path / "items.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (*_source_records("base", (a, b)), *_source_records("fresh", (c, d, a, c))),
            identifiers,
        )
        await _parent(database, payload, identifiers)
        await _children(database, ((a, "child-a"), (b, "child-b")))
        canonical = collect_ebay_item_work(
            identifier="canonical-c",
            payload=CollectEbayItemPayload(
                request=EbayItemRequest(item_identifier=c, maximum_images=0)
            ),
        )
        assert await _enqueue(database, canonical, identifiers) == "canonical-c"
        outcome = await _run(database, payload, identifiers)
        result = cast(dict[str, JsonValue], outcome.result)
        expected = ["canonical-c", _identifier("refresh", "item", d), "child-a"]
        if not changed:
            expected.append("child-b")
        assert result["item_collection_work_identifiers"] == expected
        assert result["selected_unique_listings"] == len(expected)
        assert result["search_scope_changed"] == changed
        assert result["search_scope_comparison_valid"] == (not changed)
        assert result["new_listing_identifiers"] == [c, d]
        assert result["absent_from_refresh_listing_identifiers"] == ([] if changed else [b])
        assert outcome.delay_ns == 5_000_000_000
        assert await database.requested_work_identifiers(
            requester_kind=ITEM_REQUESTER,
            requester_identifier=_identifier("refresh", "item_requester", c),
        ) == ("canonical-c",)
        with pytest.raises(KeyError):
            _ = await database.work(_identifier("refresh", "item", c))
        assert outcome.progress_watch is not None
        assert outcome.progress_watch.work_identifiers == ()


async def test_multiple_distinct_children_for_one_selection_still_fails(tmp_path: Path) -> None:
    identifiers = _identifiers()
    item = "100000000000"
    payload = _payload()
    async with Database.managed(tmp_path / "items.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (*_source_records("base", (item,)), *_source_records("fresh", (item,))),
            identifiers,
        )
        await _parent(database, payload, identifiers)
        await _children(database, ((item, "first"), (item, "second")))
        with pytest.raises(RuntimeError, match="more than one work item"):
            _ = await _run(database, payload, identifiers)


@pytest.mark.parametrize("malformed", ("kind", "item", "missing"))
async def test_malformed_occurrences_fail_before_any_child_is_published(
    tmp_path: Path, malformed: str
) -> None:
    identifiers = _identifiers()
    payload = _payload()
    record = RecordDraft(
        identifier="fresh-occurrence-0",
        kind=("test", "wrong")
        if malformed == "kind"
        else ("carl", "ebay", "search_listing_occurrence"),
        schema_version=1,
        value={"item_identifier": "invalid" if malformed == "item" else "100000000000"},
    )
    records = (*_source_records("base", ()), _source_records("fresh", ("100000000000",))[0])
    if malformed != "missing":
        records = (*records, record)
    async with Database.managed(tmp_path / "items.sqlite3", initialize=True) as database:
        await _retain_records(database, records, identifiers)
        await _parent(database, payload, identifiers)
        with pytest.raises(KeyError if malformed == "missing" else ValueError):
            _ = await _run(database, payload, identifiers)
        async with database._connections.reader() as connection:
            assert await (
                await connection.execute("SELECT count(*) FROM work_items")
            ).fetchone() == (1,)
