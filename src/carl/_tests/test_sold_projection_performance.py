"""Sparse status filters must not hydrate the entire mixed workspace."""

# pyright: reportPrivateUsage=false

from pathlib import Path
from time import time_ns
from typing import cast
from uuid import uuid4

import pytest

from carl._tests.test_marketplace_listing import _publish, _record
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_review_workspace import _search_run_value
from carl.core.composed_projection import (
    ComposedListingFilters,
    GetComposedListingRequest,
    ListComposedSearchRequest,
    ListingStatus,
)
from carl.core.ebay import EbaySearchRequest
from carl.core.facebook_work import CreateSearchRequest
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    FacebookSearchTargetSpecification,
)
from carl.core.models import RecordDraft
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkDefinition,
    WorkRequester,
)
from carl.io.sqlite import Database
from carl.marketplace_projection import _facebook_listing_statuses, _facebook_projection
from carl.review import ReviewApplication


async def _facebook_search(
    app: ReviewApplication,
    database: Database,
    name: str,
    items: tuple[str, ...],
    flags: tuple[dict[str, bool], ...],
) -> str:
    run = _search_run_value(name)
    search = CreateSearchRequest.model_validate(
        {"request": run["request"], "traversal": {"maximum_pages": 1}}
    )
    group = await app.create_marketplace_search(
        CreateMarketplaceSearchRequest(targets=(FacebookSearchTargetSpecification(search=search),))
    )
    run["search_run_identifier"] = name
    cast(dict[str, object], run["traversal"])["unique_listing_identifiers"] = list(items)
    await _complete(
        database,
        group.targets[0].executions[0].work_identifier,
        records=(
            RecordDraft(
                identifier=name + "-run",
                kind=("carl", "facebook", "search_run"),
                schema_version=1,
                value=run,
            ),
            *(
                RecordDraft(
                    identifier=f"{name}-card-{index}",
                    kind=("carl", "facebook", "search_listing_occurrence"),
                    schema_version=1,
                    value={
                        "search_run_identifier": name,
                        "acquisition_record_identifier": name + "-acquisition",
                        "listing_identifier": item,
                        "page_ordinal": 0,
                        "edge_index": index,
                        "original": {"marketplace_listing_title": "Eyepiece", **item_flags},
                    },
                )
                for index, (item, item_flags) in enumerate(zip(items, flags, strict=True))
            ),
        ),
        result={"search_run_record_identifier": name + "-run"},
    )
    return name + "-run"


@pytest.mark.anyio
async def test_sparse_sold_filter_skips_hydration_and_preserves_cursor_candidate_limits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    items = tuple(str(100000000000 + index) for index in range(205))
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        group = await app.create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=(EbaySearchTargetSpecification(search=EbaySearchRequest(query="sold")),)
            )
        )
        await _complete(
            database,
            group.targets[0].executions[0].work_identifier,
            records=(
                _record("ebay-run", "search_run", request={"query": "sold"}),
                _record(
                    "ebay-card",
                    "search_listing_occurrence",
                    search_run_record_identifier="ebay-run",
                    item_identifier="256123456789",
                    listing_state="sold",
                    sold_price="$100",
                    title="Sold eyepiece",
                ),
            ),
            result={"search_run_record_identifier": "ebay-run"},
        )
        fb_run = await _facebook_search(
            app,
            database,
            "facebook",
            items,
            tuple({"is_live": True} if index < 200 else {"is_sold": True} for index in range(205)),
        )
        hydrated: list[str] = []

        async def counted(
            application: ReviewApplication,
            request: GetComposedListingRequest,
            runs: tuple[str, ...],
            as_of: int,
        ):
            hydrated.append(request.listing_identifier)
            return await _facebook_projection(application, request, runs, as_of)

        monkeypatch.setattr("carl.marketplace_projection._facebook_projection", counted)
        request = ListComposedSearchRequest(
            search_run_record_identifier=fb_run,
            additional_search_run_record_identifiers=("ebay-run",),
            filters=ComposedListingFilters(statuses=(ListingStatus.SOLD,)),
            maximum_candidate_listings_examined=100,
            maximum_analyses_per_listing=0,
            maximum_gallery_images_per_listing=0,
            page_size=100,
        )
        first = await app.list_composed_search(request)
        assert first.examined_candidate_listing_count == 100
        assert first.candidate_examination_limit_reached
        assert first.listings == () and first.next_cursor
        assert hydrated == []
        # A newer sold observation must not leak into an already frozen cursor.
        _ = await _facebook_search(app, database, "newer", (items[150],), ({"is_sold": True},))
        second = await app.list_composed_search(
            request.model_copy(update={"cursor": first.next_cursor})
        )
        assert second.examined_candidate_listing_count == 100
        assert second.listings == () and second.next_cursor
        assert hydrated == []
        third = await app.list_composed_search(
            request.model_copy(update={"cursor": second.next_cursor})
        )
        assert third.examined_candidate_listing_count == 6
        assert third.next_cursor is None and not third.candidate_examination_limit_reached
        assert hydrated == list(items[200:])
        assert {item.listing_identifier for item in third.listings} == {
            *items[200:],
            "ebay:256123456789",
        }


async def _fixture(database: Database, name: str, records: tuple[RecordDraft, ...]) -> None:
    kind = ("test", "status")
    _ = await database.enqueue_work(
        WorkDefinition(
            identifier=name,
            kind=kind,
            payload_schema_version=1,
            payload={},
            deduplication_identity=(name,),
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
            ),
        ),
        WorkRequester(request_identifier=str(uuid4()), kind=("test",), identifier=name, context={}),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    await _complete(database, name, records=records, result={})


@pytest.mark.anyio
async def test_batched_status_selection_matches_full_projection_and_newer_item_status(
    tmp_path: Path,
) -> None:
    items = ("100000000001", "100000000002", "100000000003", "100000000004")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        run = await _facebook_search(
            app,
            database,
            "status",
            items,
            ({"is_live": True}, {"is_sold": True}, {"is_pending": True}, {}),
        )
        old_boundary = await database.current_completion_boundary()
        await _fixture(
            database,
            "acquisition",
            (
                RecordDraft(
                    identifier="item-acq",
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value={},
                ),
            ),
        )
        await _fixture(
            database,
            "detail",
            (
                RecordDraft(
                    identifier="detail",
                    kind=("carl", "facebook", "listing_observation"),
                    schema_version=1,
                    value={
                        "listing_id": items[0],
                        "acquisition_record_id": "item-acq",
                        "response_classification": {"kind": "full_listing"},
                        "images": [],
                        "fields": {
                            "availability_sold": {
                                "state": "present",
                                "evidence": [{"state": "present", "normalized": True}],
                            }
                        },
                    },
                ),
            ),
        )
        for boundary, expected_first in (
            (old_boundary, ListingStatus.AVAILABLE),
            (await database.current_completion_boundary(), ListingStatus.SOLD),
        ):
            statuses = await _facebook_listing_statuses(app, items, as_of=boundary)
            assert statuses == dict(
                zip(
                    items,
                    (
                        expected_first,
                        ListingStatus.SOLD,
                        ListingStatus.PENDING,
                        ListingStatus.UNKNOWN,
                    ),
                    strict=True,
                )
            )
            for item in items:
                projection = await _facebook_projection(
                    app, GetComposedListingRequest(listing_identifier=item), (run,), boundary
                )
                assert statuses[item] == projection.status.value


@pytest.mark.anyio
async def test_exact_source_run_occurrences_are_indexed_scoped_and_snapshot_bounded(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        fb_run = await _facebook_search(
            app, database, "fb", ("100000000001",), ({"is_live": True},)
        )
        await _publish(
            database,
            (
                _record("run", "search_run"),
                _record("other-run", "search_run"),
                _record(
                    "card",
                    "search_listing_occurrence",
                    search_run_record_identifier="run",
                    item_identifier="256123456789",
                    listing_state="sold",
                ),
                _record(
                    "irrelevant",
                    "search_listing_occurrence",
                    search_run_record_identifier="other-run",
                    item_identifier="256123456790",
                ),
            ),
        )
        boundary = await database.current_completion_boundary()
        row_boundary = await database.current_object_boundary()
        rows = await database.search_listing_occurrences_for_runs(
            "ebay",
            ("run", "run"),
            as_of_completion_sequence=boundary,
            maximum_object_rowid=row_boundary,
        )
        assert [identifier for identifier, _ in rows] == ["card"]
        rows = await database.search_listing_occurrences_for_runs(
            "facebook", (fb_run,), as_of_completion_sequence=boundary
        )
        assert [identifier for identifier, _ in rows] == ["fb-card-0"]
        await _publish(
            database,
            (
                _record(
                    "late-card",
                    "search_listing_occurrence",
                    search_run_record_identifier="run",
                    item_identifier="256123456791",
                ),
            ),
        )
        frozen = await database.search_listing_occurrences_for_runs(
            "ebay", ("run",), as_of_completion_sequence=boundary, maximum_object_rowid=row_boundary
        )
        assert [identifier for identifier, _ in frozen] == ["card"]
        latest = await database.search_listing_occurrences_for_runs(
            "ebay", ("run",), as_of_completion_sequence=boundary
        )
        assert [identifier for identifier, _ in latest] == ["card", "late-card"]
        with pytest.raises(ValueError, match="source kind"):
            _ = await database.search_listing_occurrences_for_runs(
                "ebay", (fb_run,), as_of_completion_sequence=boundary
            )
        with pytest.raises(ValueError, match="bounds"):
            _ = await database.search_listing_occurrences_for_runs(
                "ebay", tuple("run" for _ in range(101)), as_of_completion_sequence=boundary
            )
        assert (
            await database.search_listing_occurrences_for_runs(
                "facebook", (), as_of_completion_sequence=boundary
            )
            == ()
        )
