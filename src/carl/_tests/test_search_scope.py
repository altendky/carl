"""Scope revisions retain membership history without inventing search absences."""

# Exercise private worker phases with bounded offline doubles.
# pyright: reportPrivateUsage=false

from pathlib import Path
from typing import cast

import pytest

from carl import ebay_refresh_workers, facebook_refresh_workers
from carl._tests.test_composed_projection import _occurrence, _run
from carl._tests.test_ebay_item_workers import _enqueue, _identifiers, _run_next
from carl.core.components import Component, ComponentId
from carl.core.composed_projection import bounded_refresh_ancestry, compose_search_membership
from carl.core.ebay import EbaySearchRequest
from carl.core.ebay_refresh import RefreshEbaySearchPayload
from carl.core.facebook import FacebookItemResponseKind
from carl.core.facebook_refresh import RefreshSearchPayload
from carl.core.facebook_work import (
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_SEARCH_WORK_KIND,
    CollectSearchPayload,
    SuccessfulItemPageResult,
    collect_search_work,
)
from carl.core.models import JsonValue, NamedOutput, RecordDraft
from carl.core.search_scope import search_scope_sha256
from carl.core.work import WorkCapability
from carl.core.worker import AttemptContext, CompletedWork, RetryWork
from carl.ebay_refresh_workers import EbayRefreshWorkerDependencies
from carl.facebook_refresh_workers import RefreshWorkerDependencies
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry


def _facebook_request(maximum: str = "300") -> dict[str, JsonValue]:
    return {
        "query": "foosball",
        "location": {"kind": "facebook_location", "identifier": "123", "label": None},
        "radius": {"value": 60, "unit": "miles"},
        "price": {"currency": "USD", "minimum": None, "maximum": maximum},
        "exact_match": False,
    }


def test_semantic_scope_ignores_transport_labels_and_decimal_spelling() -> None:
    original = _facebook_request("300")
    equivalent = _facebook_request("300.00")
    equivalent["location"] = {"kind": "facebook_location", "identifier": "123", "label": "My city"}
    assert search_scope_sha256("facebook", original) == search_scope_sha256("facebook", equivalent)
    assert search_scope_sha256("facebook", original) != search_scope_sha256(
        "facebook", _facebook_request("3000")
    )
    assert search_scope_sha256("ebay", {"query": "foosball"}) == search_scope_sha256(
        "ebay", {"query": "foosball", "stack_identifier": "other", "maximum_pages": 1}
    )
    assert search_scope_sha256("facebook", {"query": "legacy"}) is None


@pytest.mark.parametrize("later_refresh", [False, True])
def test_membership_keeps_history_but_suppresses_cross_scope_absence(later_refresh: bool) -> None:
    original = _run("original", 1, None).model_copy(
        update={"search_scope_sha256": search_scope_sha256("facebook", _facebook_request("3000"))}
    )
    revised = _run("revised", 2, "original").model_copy(
        update={"search_scope_sha256": search_scope_sha256("facebook", _facebook_request())}
    )
    latest = _run("latest", 3, "revised").model_copy(
        update={"search_scope_sha256": revised.search_scope_sha256}
    )
    runs = (original, revised, latest) if later_refresh else (original, revised)
    ancestry = bounded_refresh_ancestry(
        selected_search_run_record_identifier=runs[-1].record_identifier,
        runs_by_identifier={run.record_identifier: run for run in runs},
        maximum_runs=100,
    )
    membership = compose_search_membership(
        listing_identifier="123", ancestry=ancestry, occurrences=(_occurrence("123", original),)
    )
    assert membership is not None
    assert membership.first_seen_search_run_record_identifier == "original"
    assert membership.last_seen_search_run_record_identifier == "original"
    assert not membership.absence_comparison_valid
    assert membership.warnings == ("search_scope_changed",)
    reappeared = compose_search_membership(
        listing_identifier="123",
        ancestry=ancestry,
        occurrences=(_occurrence("123", original), _occurrence("123", runs[-1])),
    )
    assert reappeared is not None
    assert reappeared.first_seen_search_run_record_identifier == "original"
    assert reappeared.last_seen_search_run_record_identifier == runs[-1].record_identifier
    assert reappeared.absence_comparison_valid


class _Database:
    def __init__(self, marketplace: str) -> None:
        self.marketplace = marketplace
        self.selected: tuple[str, ...] = ()

    async def get_record(self, identifier: str) -> tuple[tuple[str, ...], int, JsonValue]:
        ids = ["1"] if identifier == "base" else ["2"]
        request: JsonValue = (
            _facebook_request("3000" if identifier == "base" else "300")
            if self.marketplace == "facebook"
            else {"query": "old" if identifier == "base" else "new"}
        )
        return (
            ("carl", self.marketplace, "search_run"),
            1,
            {"request": request, "traversal": {"unique_listing_identifiers": ids}},
        )

    async def successful_facebook_item_page_results(
        self, identifiers: tuple[str, ...]
    ) -> tuple[SuccessfulItemPageResult, ...]:
        self.selected = identifiers
        return tuple(
            SuccessfulItemPageResult(
                listing_id=identifier,
                acquisition_record_identifier=f"acquisition-{identifier}",
                observation_record_identifier=f"observation-{identifier}",
                response_classification=FacebookItemResponseKind.LISTING_UNAVAILABLE,
                acquisition_completion_sequence=1,
                extraction_completion_sequence=1,
            )
            for identifier in identifiers
        )

    async def requested_work_identifiers(self, **_kwargs: object) -> tuple[str, ...]:
        return ()

    async def work(self, identifier: str) -> dict[str, JsonValue]:
        self.selected += (identifier,)
        return {
            "kind": ["carl", "ebay", "extract", "item"],
            "state": "completed",
            "result": {"classification": "unavailable"},
        }


@pytest.mark.anyio
async def test_facebook_changed_scope_does_not_fetch_or_compare_old_only_listings() -> None:
    database = _Database("facebook")
    outcome = await facebook_refresh_workers._item_phase(
        RefreshSearchPayload(
            base_search_run_record_identifier="base",
            search_work_identifier="search",
            search=CollectSearchPayload.model_validate(
                {
                    "request": _facebook_request(),
                    "traversal": {"maximum_pages": 1},
                    "routing": ("decodo", "personal", "datacenter"),
                }
            ),
            item_routing=("decodo",),
            image_routing=("decodo",),
        ),
        "new",
        _context(),
        RefreshWorkerDependencies(
            database=cast(Database, cast(object, database)), new_identifier=lambda: "id"
        ),
    )
    assert isinstance(outcome, RetryWork)
    assert database.selected == ("2",)
    assert outcome.result["selected_unique_listings"] == 1
    assert outcome.result["absent_from_refresh_listing_identifiers"] == []
    assert outcome.result["search_scope_comparison_valid"] is False


@pytest.mark.anyio
async def test_ebay_changed_scope_does_not_fetch_or_compare_old_only_listings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _Database("ebay")

    async def listing_ids(_database: Database, identifier: str) -> tuple[str, ...]:
        return ("1",) if identifier == "base" else ("2",)

    async def requested(_database: Database, _stage: str, _requester: str) -> str:
        return "only-new-item"

    monkeypatch.setattr(ebay_refresh_workers, "_listing_identifiers", listing_ids)
    monkeypatch.setattr(ebay_refresh_workers, "_requested", requested)
    outcome = await ebay_refresh_workers._item_phase(
        RefreshEbaySearchPayload(
            base_search_run_record_identifier="base",
            search_work_identifier="search",
            search=EbaySearchRequest(query="new"),
        ),
        {"refreshed_search_run_record_identifier": "new"},
        _context(),
        EbayRefreshWorkerDependencies(
            database=cast(Database, cast(object, database)), new_identifier=lambda: "id"
        ),
    )
    assert isinstance(outcome, RetryWork)
    assert outcome.result["selected_unique_listings"] == 1
    assert outcome.result["absent_from_refresh_listing_identifiers"] == []
    assert outcome.result["search_scope_comparison_valid"] is False


def _context() -> AttemptContext:
    return AttemptContext(
        work_item_identifier="refresh",
        lease_token="lease",
        worker_identifier="worker",
        attempt=1,
        operation_identifier="operation",
    )


@pytest.mark.anyio
@pytest.mark.parametrize("change_query", [False, True])
async def test_database_run_scope_retains_cross_version_membership(
    tmp_path: Path,
    change_query: bool,
) -> None:
    """Use durable search publications, not hand-populated candidate fingerprints."""

    async def collect(payload: CollectSearchPayload, context: AttemptContext) -> CompletedWork:
        identifier = f"run-{context.work_item_identifier}"
        return CompletedWork(
            records=(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value={
                        "search_run_identifier": f"internal-{identifier}",
                        "request": payload.request.model_dump(mode="json"),
                        "traversal": {"stopping_reason": "no_next_page"},
                    },
                ),
            ),
            outputs=(NamedOutput(name=("search_run",), object_identifier=identifier),),
            result={"search_run_record_identifier": identifier},
        )

    registry = WorkHandlerRegistry(
        (
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=COLLECT_SEARCH_WORK_KIND,
                    payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
                ),
                component=Component(ComponentId(("test", "scope", "publication")), 1, collect),
                payload_type=CollectSearchPayload,
                handler=collect,
            ),
        )
    )
    original_request = _facebook_request("3000")
    revised_request = _facebook_request("3000" if change_query else "300")
    if change_query:
        revised_request["query"] = "other foosball"
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier, request in (("original", original_request), ("revised", revised_request)):
            await _enqueue(
                database,
                collect_search_work(
                    identifier=identifier,
                    payload=CollectSearchPayload.model_validate(
                        {
                            "request": request,
                            "traversal": {"maximum_pages": 1},
                            "routing": ("decodo", "personal", "datacenter"),
                        }
                    ),
                    not_before_utc_ns=0,
                ),
                identifiers,
            )
            await _run_next(database, registry, identifiers, COLLECT_SEARCH_WORK_KIND)
        boundary = await database.current_completion_boundary()
        original = await database.facebook_projection_search_run(
            "run-original",
            as_of_completion_sequence=boundary,
        )
        revised = await database.facebook_projection_search_run(
            "run-revised",
            as_of_completion_sequence=boundary,
        )
        assert original.search_scope_sha256 == search_scope_sha256("facebook", original_request)
        assert revised.search_scope_sha256 == search_scope_sha256("facebook", revised_request)
        assert original.search_scope_sha256 != revised.search_scope_sha256
        # Parent inference is covered separately; this test exercises the real
        # DB candidates through the pure composition boundary.
        revised = revised.model_copy(
            update={"refresh_source_run_record_identifier": original.record_identifier}
        )
        ancestry = bounded_refresh_ancestry(
            selected_search_run_record_identifier=revised.record_identifier,
            runs_by_identifier={run.record_identifier: run for run in (original, revised)},
            maximum_runs=100,
        )
        old_only = compose_search_membership(
            listing_identifier="123",
            ancestry=ancestry,
            occurrences=(_occurrence("123", original),),
        )
        assert old_only is not None
        assert old_only.first_seen_search_run_record_identifier == original.record_identifier
        assert old_only.last_seen_search_run_record_identifier == original.record_identifier
        assert not old_only.absence_comparison_valid
        reappeared = compose_search_membership(
            listing_identifier="123",
            ancestry=ancestry,
            occurrences=(_occurrence("123", original), _occurrence("123", revised)),
        )
        assert reappeared is not None
        assert reappeared.first_seen_search_run_record_identifier == original.record_identifier
        assert reappeared.last_seen_search_run_record_identifier == revised.record_identifier
        assert reappeared.absence_comparison_valid
