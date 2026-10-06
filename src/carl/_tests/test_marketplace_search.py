from datetime import date
from pathlib import Path
from typing import cast, final

import anyio
import pytest

from carl._tests.test_mixed_workspace import _complete, _provenance
from carl._tests.test_sold_search_disabled import _historical_group
from carl.core.ebay import EbaySearchRequest, EbaySearchResponseKind
from carl.core.marketplace_search import (
    MARKETPLACE_SEARCH_EXECUTION_KIND,
    MARKETPLACE_SEARCH_KIND,
    MARKETPLACE_SEARCH_TARGET_KIND,
    MARKETPLACE_SEARCH_TARGET_STATE_KIND,
    AddMarketplaceSearchTargetRequest,
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    ListMarketplaceSearchResultsRequest,
)
from carl.core.models import CodeProvenance, JsonValue, RecordDraft
from carl.io.sqlite import Database
from carl.review import ReviewApplication, ReviewInputError


@final
class _Database:
    response_kind: str | None = None
    work_state = "completed"

    async def search_listing_occurrences_for_runs(
        self, marketplace: str, run_identifiers: tuple[str, ...], *, as_of_completion_sequence: int
    ) -> tuple[tuple[str, JsonValue], ...]:
        if not run_identifiers:
            return ()
        return await self.records_by_kind(("carl", marketplace, "search_listing_occurrence"))

    async def current_completion_boundary(self) -> int:
        return 2

    async def get_record(self, identifier: str) -> tuple[tuple[str, ...], int, JsonValue]:
        if identifier == "source-run-1":
            return ("carl", "ebay", "search_run"), 1, {}
        assert identifier == "search-1"
        return (
            MARKETPLACE_SEARCH_KIND,
            1,
            {
                "record_identifier": "search-1",
                "created_at_utc": "2026-09-29T00:00:00+00:00",
            },
        )

    async def records_by_kind(self, kind: tuple[str, ...]) -> tuple[tuple[str, JsonValue], ...]:
        if kind == MARKETPLACE_SEARCH_TARGET_KIND:
            return (
                (
                    "target-1",
                    {
                        "record_identifier": "target-1",
                        "search_record_identifier": "search-1",
                        "specification": {
                            "marketplace": "ebay",
                            "search": {
                                "query": "oscilloscope",
                                "stack_identifier": "ebay_anonymous",
                            },
                        },
                        "created_at_utc": "2026-09-29T00:00:01+00:00",
                    },
                ),
            )
        if kind == MARKETPLACE_SEARCH_TARGET_STATE_KIND:
            return (
                (
                    "state-1",
                    {
                        "record_identifier": "state-1",
                        "search_record_identifier": "search-1",
                        "target_record_identifier": "target-1",
                        "enabled": False,
                        "recorded_at_utc": "2026-09-29T00:00:03+00:00",
                    },
                ),
            )
        if kind == MARKETPLACE_SEARCH_EXECUTION_KIND:
            return tuple(
                (
                    f"execution-{index}",
                    {
                        "record_identifier": f"execution-{index}",
                        "search_record_identifier": "search-1",
                        "target_record_identifier": "target-1",
                        "work_identifier": f"work-{index}",
                        "requested_at_utc": f"2026-09-29T00:00:0{index}+00:00",
                    },
                )
                for index in (1, 2)
            )
        return ()

    async def work(self, identifier: str) -> dict[str, JsonValue]:
        assert identifier in {"work-1", "work-2"}
        result: dict[str, JsonValue] = {"search_run_record_identifier": "source-run-1"}
        if self.response_kind is not None:
            result.update(
                response_classification={"kind": self.response_kind, "evidence": []},
                stopping_reason=(
                    "unrecognized_response"
                    if self.response_kind == "unrecognized"
                    else self.response_kind
                ),
            )
        return {
            "state": self.work_state,
            "result": result,
            "latest_event_sequence": 2,
            "latest_event_kind": "completed"
            if self.work_state == "completed"
            else "retry_scheduled",
        }


@final
class _ProjectionDatabase:
    completion_boundary = 12
    repeated_page = False

    async def search_listing_occurrences_for_runs(
        self, marketplace: str, run_identifiers: tuple[str, ...], *, as_of_completion_sequence: int
    ) -> tuple[tuple[str, JsonValue], ...]:
        if not run_identifiers:
            return ()
        return await self.records_by_kind(("carl", marketplace, "search_listing_occurrence"))

    async def current_completion_boundary(self) -> int:
        return self.completion_boundary

    async def get_record(self, identifier: str) -> tuple[tuple[str, ...], int, JsonValue]:
        records: dict[str, tuple[tuple[str, ...], JsonValue]] = {
            "search-1": (
                MARKETPLACE_SEARCH_KIND,
                {
                    "record_identifier": "search-1",
                    "created_at_utc": "2026-09-29T00:00:00+00:00",
                },
            ),
            "facebook-run": (
                ("carl", "facebook", "search_run"),
                {"search_run_identifier": "facebook-internal-run"},
            ),
            "ebay-run-old": (("carl", "ebay", "search_run"), {}),
            "ebay-run-new": (("carl", "ebay", "search_run"), {}),
            "ebay-run-future": (("carl", "ebay", "search_run"), {}),
        }
        kind, value = records[identifier]
        return kind, 1, value

    async def records_by_kind(self, kind: tuple[str, ...]) -> tuple[tuple[str, JsonValue], ...]:
        if kind == MARKETPLACE_SEARCH_TARGET_KIND:
            return (
                (
                    "target-facebook",
                    {
                        "record_identifier": "target-facebook",
                        "search_record_identifier": "search-1",
                        "specification": {
                            "marketplace": "facebook",
                            "search": {
                                "request": {
                                    "query": "oscilloscope",
                                    "location": {
                                        "kind": "facebook_location",
                                        "identifier": "123",
                                    },
                                    "radius": {"value": 30, "unit": "miles"},
                                },
                                "traversal": {"maximum_pages": 1},
                            },
                        },
                        "created_at_utc": "2026-09-29T00:00:01+00:00",
                    },
                ),
                (
                    "target-ebay",
                    {
                        "record_identifier": "target-ebay",
                        "search_record_identifier": "search-1",
                        "specification": {
                            "marketplace": "ebay",
                            "search": {"query": "oscilloscope"},
                        },
                        "created_at_utc": "2026-09-29T00:00:02+00:00",
                    },
                ),
            )
        if kind == MARKETPLACE_SEARCH_TARGET_STATE_KIND:
            return ()
        if kind == MARKETPLACE_SEARCH_EXECUTION_KIND:
            definitions = (
                ("execution-facebook", "target-facebook", "work-facebook"),
                ("execution-ebay-old", "target-ebay", "work-ebay-old"),
                ("execution-ebay-new", "target-ebay", "work-ebay-new"),
                ("execution-ebay-future", "target-ebay", "work-ebay-future"),
            )
            return tuple(
                (
                    execution,
                    {
                        "record_identifier": execution,
                        "search_record_identifier": "search-1",
                        "target_record_identifier": target,
                        "work_identifier": work,
                        "requested_at_utc": "2026-09-29T00:00:03+00:00",
                    },
                )
                for execution, target, work in definitions
            )
        if kind == ("carl", "facebook", "search_listing_occurrence"):
            return (
                (
                    "facebook-occurrence",
                    {
                        "listing_identifier": "123",
                        "search_run_identifier": "facebook-internal-run",
                        "acquisition_record_identifier": "facebook-acquisition",
                        "page_ordinal": 1,
                        "edge_index": 2,
                        "original": {
                            "marketplace_listing_title": "Facebook scope",
                            "listing_price": {
                                "amount": "75.00",
                                "currency": "USD",
                            },
                            "location": {"text": "Example City"},
                        },
                    },
                ),
            )
        if kind == ("carl", "ebay", "search_listing_occurrence"):
            records: tuple[tuple[str, JsonValue], ...] = (
                (
                    "ebay-occurrence-old",
                    {
                        "item_identifier": "123456789012",
                        "search_run_record_identifier": "ebay-run-old",
                        "acquisition_record_identifier": "ebay-acquisition-old",
                        "position": 3,
                        "canonical_url": "https://www.ebay.com/itm/123456789012",
                        "title": "Retained older title",
                        "displayed_price": "$10.00",
                        "promoted": False,
                    },
                ),
                (
                    "ebay-occurrence-new",
                    {
                        "item_identifier": "123456789012",
                        "search_run_record_identifier": "ebay-run-new",
                        "acquisition_record_identifier": "ebay-acquisition-new",
                        "position": 1,
                        "canonical_url": "https://www.ebay.com/itm/123456789012",
                        "title": None,
                        "displayed_price": "$12.00",
                        "promoted": True,
                    },
                ),
                (
                    "ebay-occurrence-other",
                    {
                        "item_identifier": "223456789012",
                        "search_run_record_identifier": "ebay-run-new",
                        "position": 2,
                        "canonical_url": "https://www.ebay.com/itm/223456789012",
                        "title": "Other eBay result",
                        "promoted": False,
                    },
                ),
                (
                    "ebay-occurrence-future",
                    {
                        "item_identifier": "323456789012",
                        "search_run_record_identifier": "ebay-run-future",
                        "position": 1,
                        "canonical_url": "https://www.ebay.com/itm/323456789012",
                        "title": "Later completion",
                        "promoted": False,
                    },
                ),
            )
            if self.repeated_page:
                return (
                    *records,
                    (
                        "ebay-page-one",
                        {
                            "item_identifier": "123456789012",
                            "search_run_record_identifier": "ebay-run-new",
                            "page_ordinal": 1,
                            "position": 5,
                            "canonical_url": "https://www.ebay.com/itm/123456789012",
                            "displayed_price": "$15.00",
                        },
                    ),
                    (
                        "ebay-page-two",
                        {
                            "item_identifier": "123456789012",
                            "search_run_record_identifier": "ebay-run-new",
                            "page_ordinal": 2,
                            "position": 1,
                            "canonical_url": "https://www.ebay.com/itm/123456789012",
                            "displayed_price": "$20.00",
                        },
                    ),
                )
            return records
        return ()

    async def work(self, identifier: str) -> dict[str, JsonValue]:
        definitions = {
            "work-facebook": ("facebook-run", 10),
            "work-ebay-old": ("ebay-run-old", 8),
            "work-ebay-new": ("ebay-run-new", 12),
            "work-ebay-future": ("ebay-run-future", 14),
        }
        run, sequence = definitions[identifier]
        return {
            "state": "completed",
            "result": {"search_run_record_identifier": run},
            "latest_event_sequence": sequence,
            "latest_event_kind": "completed",
        }


@pytest.mark.anyio
async def test_disabled_target_keeps_all_executions_and_historical_runs(tmp_path: Path) -> None:
    application = ReviewApplication(
        database=cast(Database, cast(object, _Database())),
        repository_root=tmp_path,
    )

    search = await application.get_marketplace_search("search-1")

    assert search.targets[0].enabled is False
    assert [execution.work_identifier for execution in search.targets[0].executions] == [
        "work-1",
        "work-2",
    ]
    assert search.historical_search_run_record_identifiers == ("source-run-1",)


@pytest.mark.anyio
@pytest.mark.parametrize("work_state", ["completed", "pending", "terminal_failure"])
@pytest.mark.parametrize("response_kind", ["unrecognized", "error_page", "challenge", "http_error"])
async def test_failed_search_is_visible_even_when_legacy_work_completed(
    tmp_path: Path, work_state: str, response_kind: str
) -> None:
    database = _Database()
    database.work_state = work_state
    database.response_kind = response_kind
    application = ReviewApplication(cast(Database, cast(object, database)), tmp_path)

    search = await application.get_marketplace_search("search-1")
    execution = search.targets[0].executions[0]
    assert execution.state.value == work_state
    assert execution.collection_succeeded is False
    assert execution.response_classification is not None
    assert execution.response_classification.kind is EbaySearchResponseKind(response_kind)
    assert execution.stopping_reason == (
        "unrecognized_response" if response_kind == "unrecognized" else response_kind
    )
    page = await application.list_marketplace_search_results(
        ListMarketplaceSearchResultsRequest(search_record_identifier="search-1")
    )
    assert page.results == ()
    assert len(page.execution_warnings) == 2
    assert page.execution_warnings[0].reason == "collection_failure"
    assert page.execution_warnings[0].execution == execution


@pytest.mark.anyio
async def test_explicit_empty_search_is_successful_without_failure_warning(tmp_path: Path) -> None:
    database = _Database()
    database.response_kind = "empty_results"
    application = ReviewApplication(cast(Database, cast(object, database)), tmp_path)
    search = await application.get_marketplace_search("search-1")
    assert search.targets[0].executions[0].collection_succeeded is True
    page = await application.list_marketplace_search_results(
        ListMarketplaceSearchResultsRequest(search_record_identifier="search-1")
    )
    assert page.results == ()
    assert page.execution_warnings == ()


@pytest.mark.anyio
async def test_active_and_sold_targets_preserve_paired_sale_history(tmp_path: Path) -> None:
    item = "256123456789"

    async def provenance() -> CodeProvenance:
        return _provenance()

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path, code_provenance=provenance)
        group = await _historical_group(
            app,
            database,
            (
                EbaySearchTargetSpecification(
                    search=EbaySearchRequest(
                        query="Morpheus", listing_state="sold", maximum_pages=1
                    )
                ),
                EbaySearchTargetSpecification(
                    search=EbaySearchRequest(
                        query="Morpheus", listing_state="sold", maximum_pages=2
                    )
                ),
                EbaySearchTargetSpecification(search=EbaySearchRequest(query="Morpheus")),
            ),
        )
        card_fields: tuple[dict[str, JsonValue], ...] = (
            {
                "listing_state": "sold",
                "sold_price": "$150.00",
                "sold_date": "2025-09-01",
                "sold_date_text": "Sep 1, 2025",
                "sold_price_status": "displayed",
                "displayed_price": "$150.00",
            },
            {
                "listing_state": "sold",
                "sold_price": None,
                "sold_date": "2026-09-29",
                "sold_date_text": "Sep 29, 2026",
                "sold_price_status": "best_offer_accepted",
                "displayed_price": "$200.00",
            },
            {"listing_state": "active", "displayed_price": "$240.00"},
        )
        for index, (target, fields) in enumerate(zip(group.targets, card_fields, strict=True)):
            await _complete(
                database,
                target.executions[0].work_identifier,
                records=(
                    RecordDraft(
                        identifier=f"run-{index}",
                        kind=("carl", "ebay", "search_run"),
                        schema_version=1,
                        value={"request": target.specification.search.model_dump(mode="json")},
                    ),
                    RecordDraft(
                        identifier=f"card-{index}",
                        kind=("carl", "ebay", "search_listing_occurrence"),
                        schema_version=1,
                        value={
                            "item_identifier": item,
                            "search_run_record_identifier": f"run-{index}",
                            "canonical_url": f"https://www.ebay.com/itm/{item}",
                            "position": 1,
                            **fields,
                        },
                    ),
                ),
                result={"search_run_record_identifier": f"run-{index}"},
            )
        page = await app.list_marketplace_search_results(
            ListMarketplaceSearchResultsRequest(search_record_identifier=group.record_identifier)
        )
        (result,) = page.results
        assert result.listing_state == "active" and result.displayed_price == "$240.00"
        assert result.sold_price is None and result.sold_price_status == "best_offer_accepted"
        assert result.sold_date == date(2026, 9, 29)
        assert result.sold_occurrence_record_identifier == "card-1"
        assert result.occurrences[-1].sold_price == "$150.00"
        assert result.occurrences[-1].sold_date == date(2025, 9, 1)
        assert result.occurrences[0].sold_date is None


@pytest.mark.anyio
async def test_generic_results_deduplicate_and_keep_cursor_boundary(tmp_path: Path) -> None:
    database = _ProjectionDatabase()
    application = ReviewApplication(
        database=cast(Database, cast(object, database)),
        repository_root=tmp_path,
    )

    first = await application.list_marketplace_search_results(
        ListMarketplaceSearchResultsRequest(
            search_record_identifier="search-1",
            page_size=2,
        )
    )

    assert first.as_of_completion_sequence == 12
    assert first.total_distinct_results == 3
    assert [result.external_identifier for result in first.results] == [
        "123456789012",
        "223456789012",
    ]
    deduplicated = first.results[0]
    assert deduplicated.title == "Retained older title"
    assert deduplicated.displayed_price == "$12.00"
    assert deduplicated.promoted is True
    assert [item.occurrence_record_identifier for item in deduplicated.occurrences] == [
        "ebay-occurrence-new",
        "ebay-occurrence-old",
    ]
    assert first.next_cursor is not None

    database.completion_boundary = 14
    second = await application.list_marketplace_search_results(
        ListMarketplaceSearchResultsRequest(
            search_record_identifier="search-1",
            page_size=2,
            cursor=first.next_cursor,
        )
    )

    assert second.as_of_completion_sequence == 12
    assert second.total_distinct_results == 3
    assert [result.external_identifier for result in second.results] == ["123"]
    assert second.results[0].marketplace.value == "facebook"
    assert second.results[0].displayed_price == "75.00 USD"
    assert second.next_cursor is None

    fresh = await application.list_marketplace_search_results(
        ListMarketplaceSearchResultsRequest(search_record_identifier="search-1")
    )
    assert fresh.as_of_completion_sequence == 14
    assert fresh.total_distinct_results == 4
    assert fresh.results[0].external_identifier == "323456789012"


@pytest.mark.anyio
async def test_live_failure_warning_does_not_move_listing_cursor_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = _ProjectionDatabase()
    application = ReviewApplication(cast(Database, cast(object, database)), tmp_path)
    first = await application.list_marketplace_search_results(
        ListMarketplaceSearchResultsRequest(search_record_identifier="search-1", page_size=2)
    )
    assert first.next_cursor is not None
    assert first.execution_warnings == ()
    original_work = _ProjectionDatabase.work

    async def work(database: _ProjectionDatabase, identifier: str) -> dict[str, JsonValue]:
        value = await original_work(database, identifier)
        if identifier == "work-ebay-future":
            value["state"] = "pending"
            value["latest_event_kind"] = "retry_scheduled"
            value["result"] = {
                "search_run_record_identifier": "ebay-run-future",
                "response_classification": {"kind": "error_page", "evidence": ["http_status_403"]},
                "stopping_reason": "error_page",
            }
        return value

    monkeypatch.setattr(_ProjectionDatabase, "work", work)
    database.completion_boundary = 14
    second = await application.list_marketplace_search_results(
        ListMarketplaceSearchResultsRequest(
            search_record_identifier="search-1", page_size=2, cursor=first.next_cursor
        )
    )
    assert second.as_of_completion_sequence == 12
    assert second.total_distinct_results == 3
    assert [value.external_identifier for value in second.results] == ["123"]
    assert len(second.execution_warnings) == 1
    assert second.execution_warnings[0].execution.work_identifier == "work-ebay-future"
    assert second.execution_warnings[0].reason == "collection_failure"

    fresh = await application.list_marketplace_search_results(
        ListMarketplaceSearchResultsRequest(search_record_identifier="search-1")
    )
    assert fresh.as_of_completion_sequence == 14
    assert fresh.total_distinct_results == 3
    assert fresh.execution_warnings == second.execution_warnings


@pytest.mark.anyio
async def test_generic_results_preserve_ebay_page_recency(tmp_path: Path) -> None:
    database = _ProjectionDatabase()
    database.repeated_page = True
    application = ReviewApplication(
        database=cast(Database, cast(object, database)), repository_root=tmp_path
    )
    page = await application.list_marketplace_search_results(
        ListMarketplaceSearchResultsRequest(search_record_identifier="search-1")
    )
    result = page.results[0]
    assert result.displayed_price == "$20.00"
    assert [item.page_ordinal for item in result.occurrences[:2]] == [2, 1]
    assert [item.position for item in result.occurrences[:2]] == [1, 5]


@pytest.mark.anyio
@pytest.mark.parametrize("at_limit", [False, True])
async def test_concurrent_target_additions_enforce_invariants(
    tmp_path: Path, at_limit: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        application = ReviewApplication(database=database, repository_root=tmp_path)
        search = await application.create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=tuple(
                    EbaySearchTargetSpecification(search=EbaySearchRequest(query=f"scope {index}"))
                    for index in range(19 if at_limit else 1)
                )
            )
        )
        failures: list[str] = []
        successes: list[str] = []

        async def add(query: str) -> None:
            try:
                await application.add_marketplace_search_target(
                    AddMarketplaceSearchTargetRequest(
                        search_record_identifier=search.record_identifier,
                        target=EbaySearchTargetSpecification(search=EbaySearchRequest(query=query)),
                    )
                )
            except ReviewInputError as error:
                failures.append(str(error))
            else:
                successes.append(query)

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(add, "new scope")
            tasks.start_soon(add, "another scope" if at_limit else "new scope")

        assert len(successes) == len(failures) == 1
        assert ("at most 20" if at_limit else "already contains") in failures[0]
        retained = await application.get_marketplace_search(search.record_identifier)
        assert len(retained.targets) == (20 if at_limit else 2)
        assert all(len(target.executions) == 1 for target in retained.targets)
