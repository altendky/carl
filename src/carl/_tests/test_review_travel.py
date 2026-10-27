"""Road estimates preserve listing traversal, unknowns, and review evidence identity."""

# pyright: reportPrivateUsage=false

from collections.abc import Sequence
from pathlib import Path

import pytest

from carl._tests.test_compact_review import _acquisition, _projection
from carl._tests.test_marketplace_listing import _publish, _record
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_review_workspace import _search_run_value
from carl.core.compact_review import compact_workspace_listing
from carl.core.composed_projection import (
    ComposedListingFilters,
    GetComposedListingRequest,
    ListComposedSearchRequest,
    composed_listing_matches_filters,
)
from carl.core.ebay import EbaySearchRequest
from carl.core.facebook_work import CreateSearchRequest
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    FacebookSearchTargetSpecification,
)
from carl.core.models import RecordDraft
from carl.core.review_errors import ReviewInputError
from carl.core.review_evidence import intern_review_batch_item, rehydrate_review_batch_item
from carl.core.review_response_pages import paged_review_acquisition
from carl.core.review_workspace import (
    AcquireReviewBatchRequest,
    CreateReviewWorkspaceRequest,
    GetWorkspaceListingRequest,
    ListWorkspaceListingsRequest,
    ReviewBatchItem,
    ReviewState,
    WorkspaceListingReview,
)
from carl.core.travel import TravelDestination, TravelEstimate, TravelFilters
from carl.io.sqlite import Database
from carl.review import ReviewApplication


class FakeTravelService:
    identity: str = "origin-one"

    def __init__(self) -> None:
        self.batches: list[tuple[TravelDestination, ...]] = []

    async def origin_identity(self) -> str:
        return self.identity

    async def estimate_many(
        self, destinations: Sequence[TravelDestination]
    ) -> tuple[TravelEstimate, ...]:
        self.batches.append(tuple(destinations))
        return tuple(
            TravelEstimate(
                status="ready",
                distance_meters=destination.coordinate.latitude * 1000,
                duration_seconds=destination.coordinate.latitude * 60,
                destination_label=destination.label,
                location_precision=destination.precision,
            )
            if destination.coordinate is not None
            else TravelEstimate(status="unavailable", unavailable_reason="location_unavailable")
            for destination in destinations
        )


async def _listing_records(database: Database, *, mixed: bool) -> None:
    run = _search_run_value("tools")
    run["search_run_identifier"] = "internal-run"
    records = [
        RecordDraft(
            identifier="fb-run",
            kind=("carl", "facebook", "search_run"),
            schema_version=1,
            value=run,
        )
    ]
    for index, identifier in enumerate(("1001", "1002", "1003")):
        original = {
            "marketplace_listing_title": "Tool",
            "is_live": True,
            "location": (
                {"latitude": 1 if index == 0 else 80, "longitude": 10} if index < 2 else None
            ),
        }
        records.append(
            RecordDraft(
                identifier=f"card-{identifier}",
                kind=("carl", "facebook", "search_listing_occurrence"),
                schema_version=1,
                value={
                    "search_run_identifier": "internal-run",
                    "acquisition_record_identifier": "acquisition",
                    "listing_identifier": identifier,
                    "page_ordinal": 0,
                    "edge_index": index,
                    "original": original,
                },
            )
        )
    search = CreateSearchRequest.model_validate(
        {
            "request": run["request"],
            "traversal": {"maximum_pages": 1},
        }
    )
    application = ReviewApplication(database, database.path.parent)
    group = await application.create_marketplace_search(
        CreateMarketplaceSearchRequest(
            targets=(FacebookSearchTargetSpecification(search=search),)
            + (
                (EbaySearchTargetSpecification(search=EbaySearchRequest(query="tools")),)
                if mixed
                else ()
            ),
        )
    )
    await _complete(
        database,
        group.targets[0].executions[0].work_identifier,
        records=tuple(records),
        result={"search_run_record_identifier": "fb-run"},
    )
    if mixed:
        await _complete(
            database,
            group.targets[1].executions[0].work_identifier,
            records=(
                _record("ebay-run", "search_run", request={"query": "tools"}, listing_count=1),
                _record(
                    "ebay-card",
                    "search_listing_occurrence",
                    search_run_record_identifier="ebay-run",
                    item_identifier="256123456789",
                    title="Tool",
                ),
            ),
            result={"search_run_record_identifier": "ebay-run", "state": "completed"},
        )


@pytest.mark.anyio
@pytest.mark.parametrize("mixed", [False, True])
async def test_travel_filters_before_pagination_and_origin_scopes_cursor(
    tmp_path: Path, mixed: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _listing_records(database, mixed=mixed)
        service = FakeTravelService()
        application = ReviewApplication(database, tmp_path, travel_service=service)
        request = ListComposedSearchRequest(
            search_run_record_identifier="fb-run",
            additional_search_run_record_identifiers=("ebay-run",) if mixed else (),
            filters=ComposedListingFilters(
                travel=TravelFilters(
                    maximum_driving_distance_miles=10,
                    maximum_driving_duration_minutes=10,
                    include_unknown=True,
                )
            ),
            page_size=1,
        )
        first = await application.list_composed_search(request)
        assert first.next_cursor is not None
        seen = list(first.listings)
        cursor = first.next_cursor
        while cursor is not None:
            page = await application.list_composed_search(
                request.model_copy(update={"cursor": cursor})
            )
            seen.extend(page.listings)
            cursor = page.next_cursor
        assert {item.listing_identifier for item in seen} == (
            {"1001", "1003", "ebay:256123456789"} if mixed else {"1001", "1003"}
        )
        assert all(item.route_estimate is not None for item in seen)
        service.identity = "origin-two"
        with pytest.raises((ReviewInputError, ValueError), match=r"scope|cursor"):
            _ = await application.list_composed_search(
                request.model_copy(update={"cursor": first.next_cursor})
            )
        strict_page = await application.list_composed_search(
            request.model_copy(
                update={
                    "page_size": 10,
                    "filters": ComposedListingFilters(
                        travel=TravelFilters(
                            maximum_driving_distance_miles=10,
                            include_unknown=False,
                        )
                    ),
                }
            )
        )
        assert [item.listing_identifier for item in strict_page.listings] == ["1001"]
        assert any(len(batch) > 1 for batch in service.batches)


@pytest.mark.anyio
async def test_workspace_reads_batches_and_offline_opt_out(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _listing_records(database, mixed=False)
        service = FakeTravelService()
        application = ReviewApplication(database, tmp_path, travel_service=service)
        workspace = await application.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Tools",
                search_run_record_identifier="fb-run",
            )
        )
        identifier = workspace.record_identifier
        page = await application.list_workspace_listings(
            ListWorkspaceListingsRequest(
                workspace_record_identifier=identifier,
            )
        )
        assert page.listings[0].route_estimate is not None
        full = await application.get_composed_listing(
            GetComposedListingRequest(listing_identifier="1001")
        )
        plain = await application.get_composed_listing(
            GetComposedListingRequest(
                listing_identifier="1001",
                include_travel=False,
            )
        )
        assert full.route_estimate is not None and plain.route_estimate is None
        assert full.projection_revision == plain.projection_revision
        count = len(service.batches)
        detail = await application.get_workspace_listing(
            GetWorkspaceListingRequest(
                workspace_record_identifier=identifier,
                listing_identifier="1001",
                include_travel=False,
            )
        )
        assert isinstance(detail, WorkspaceListingReview)
        assert detail.route_estimate is None and len(service.batches) == count
        acquired = await application.acquire_review_batch(
            AcquireReviewBatchRequest(
                workspace_record_identifier=identifier,
                request_identifier="travel-batch",
                owner_identifier="reviewer",
                travel=TravelFilters(
                    maximum_driving_distance_miles=10,
                    include_unknown=False,
                ),
            )
        )
        assert [item.projection.listing_identifier for item in acquired.batch.items] == ["1001"]
        assert acquired.batch.items[0].projection.route_estimate is not None


def test_travel_estimate_survives_all_review_views_without_revision_change() -> None:
    original = _projection("1001", None)
    estimate = TravelEstimate(status="ready", distance_meters=1200, duration_seconds=240)
    projection = original.model_copy(update={"route_estimate": estimate})
    item = ReviewBatchItem(
        projection=projection,
        review_state=ReviewState.UNREVIEWED,
        prior_review=None,
        changed_components=(),
    )
    assert rehydrate_review_batch_item(intern_review_batch_item(item)) == item
    review = WorkspaceListingReview.model_validate(projection.model_dump(mode="python"))
    assert compact_workspace_listing(review).listing.projection.route_estimate == estimate
    assert (
        paged_review_acquisition(_acquisition((item,))).batch.items[0].projection.route_estimate
        == estimate
    )
    assert projection.projection_revision == original.projection_revision
    assert not composed_listing_matches_filters(
        original,
        ComposedListingFilters(
            travel=TravelFilters(maximum_driving_distance_miles=10, include_unknown=False)
        ),
        matching_analysis_present=False,
    )


@pytest.mark.anyio
async def test_ebay_item_location_reaches_travel_service(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                _record(
                    "detail",
                    "listing_observation",
                    item_identifier="256123456789",
                    classification="detail",
                    title="Tool",
                    location_text="Sample Town, US",
                ),
            ),
        )
        service = FakeTravelService()
        application = ReviewApplication(database, tmp_path, travel_service=service)
        projection = await application.get_composed_listing(
            GetComposedListingRequest(
                listing_identifier="ebay:256123456789",
            )
        )
        assert projection.location is not None
        assert projection.location.value == "Sample Town, US"
        assert service.batches[0][0].label == "Sample Town, US"
        assert projection.route_estimate is not None
        assert projection.route_estimate.status == "unavailable"
