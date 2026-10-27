"""Route-only work retains independent evidence and composes it by exact inputs."""

from collections.abc import Sequence
from pathlib import Path
from time import time_ns

import anyio
import pytest

from carl._tests.test_ebay_item_workers import _run_next
from carl._tests.test_review_travel import FakeTravelService, _listing_records
from carl._tests.test_travel_record_lookup import _completed
from carl.core.composed_projection import GetComposedListingRequest
from carl.core.models import RecordDraft
from carl.core.review_errors import ReviewInputError
from carl.core.review_workspace import CreateReviewWorkspaceRequest
from carl.core.travel import TravelDestination, TravelEstimate
from carl.core.travel_refresh import (
    REFRESH_LISTING_ROUTE_WORK_KIND,
    TRAVEL_ESTIMATE_RECORD_KIND,
    RequestListingRouteRefresh,
)
from carl.core.work import WorkState
from carl.io.sqlite import Database
from carl.review import ReviewApplication
from carl.travel_workers import TravelWorkerDependencies, build_travel_worker_registry


class RefreshService(FakeTravelService):
    refreshes = 0
    unknown = False

    async def refresh_many(
        self, destinations: Sequence[TravelDestination], *, expected_origin_identity: str
    ) -> tuple[TravelEstimate, ...]:
        assert expected_origin_identity == self.identity
        self.refreshes += 1
        if self.unknown:
            return (TravelEstimate(status="unavailable", unavailable_reason="provider_http_502"),)
        return (
            TravelEstimate(
                status="ready",
                distance_meters=10000 * self.refreshes,
                duration_seconds=600,
                destination_label=destinations[0].label,
                location_precision=destinations[0].precision,
            ),
        )


def _read(identifier: str) -> GetComposedListingRequest:
    return GetComposedListingRequest(
        listing_identifier=identifier, maximum_gallery_images=0, maximum_analyses=0
    )


@pytest.mark.anyio
@pytest.mark.parametrize("identifier", ("1001", "ebay:256123456789"))
async def test_exact_refresh_retains_independent_record_and_replays_after_completion(
    tmp_path: Path, identifier: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _listing_records(database, mixed=True)
        service = RefreshService()
        app = ReviewApplication(database, tmp_path, travel_service=service)
        before = await app.get_composed_listing(_read(identifier))
        request = RequestListingRouteRefresh(
            request_identifier="first", listing_identifier=identifier
        )
        queued = await app.request_listing_route_refresh(request)
        assert queued.created and queued.state is WorkState.PENDING
        assert service.refreshes == 0
        assert not (await app.request_listing_route_refresh(request)).created
        registry = build_travel_worker_registry(
            TravelWorkerDependencies(service=service, new_identifier=app.new_identifier)
        )
        finished = await _run_next(
            database, registry, app.new_identifier, REFRESH_LISTING_ROUTE_WORK_KIND
        )
        assert finished["state"] == "completed"
        records = await database.records_by_kind(TRAVEL_ESTIMATE_RECORD_KIND)
        assert len(records) == 1
        record = await app.get_listing_route_estimate(records[0][0])
        assert record.listing_id == identifier
        assert record.estimate.distance_meters == 10000
        calls = len(service.batches)
        after = await app.get_composed_listing(_read(identifier))
        assert len(service.batches) == calls
        assert after.route_estimate is not None
        assert after.route_estimate.record_identifier == record.record_identifier
        assert after.route_estimate.distance_meters == 10000
        assert after.projection_revision == before.projection_revision
        replay = await app.request_listing_route_refresh(request)
        assert not replay.created and replay.state is WorkState.COMPLETED
        assert replay.work_identifier == queued.work_identifier
        with pytest.raises(ReviewInputError, match="different inputs"):
            await app.request_listing_route_refresh(
                request.model_copy(update={"listing_identifier": "1002"})
            )
        next_request = request.model_copy(update={"request_identifier": "second"})
        next_work = await app.request_listing_route_refresh(next_request)
        assert next_work.created and next_work.work_identifier != queued.work_identifier
        service.unknown = True
        await _run_next(database, registry, app.new_identifier, REFRESH_LISTING_ROUTE_WORK_KIND)
        latest = await app.get_composed_listing(_read(identifier))
        assert latest.route_estimate is not None
        assert latest.route_estimate.status == "unavailable"
        assert latest.route_estimate.unavailable_reason == "provider_http_502"
        assert latest.route_estimate.record_identifier != record.record_identifier
        assert len(await database.records_by_kind(TRAVEL_ESTIMATE_RECORD_KIND)) == 2


@pytest.mark.anyio
async def test_changed_origin_or_location_does_not_compose_old_route(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _listing_records(database, mixed=False)
        service = RefreshService()
        app = ReviewApplication(database, tmp_path, travel_service=service)
        await app.request_listing_route_refresh(
            RequestListingRouteRefresh(request_identifier="route", listing_identifier="1001")
        )
        registry = build_travel_worker_registry(
            TravelWorkerDependencies(service=service, new_identifier=app.new_identifier)
        )
        await _run_next(database, registry, app.new_identifier, REFRESH_LISTING_ROUTE_WORK_KIND)
        service.identity = "different-origin"
        changed_origin = await app.get_composed_listing(_read("1001"))
        assert changed_origin.route_estimate is not None
        assert changed_origin.route_estimate.record_identifier is None
        service.identity = "origin-one"
        await _completed(
            database,
            "new-location",
            RecordDraft(
                identifier="new-card",
                kind=("carl", "facebook", "search_listing_occurrence"),
                schema_version=1,
                value={
                    "listing_identifier": "1001",
                    "search_run_identifier": "internal-run",
                    "page_ordinal": 1,
                    "edge_index": 0,
                    "original": {
                        "marketplace_listing_title": "Tool",
                        "is_live": True,
                        "location": {"latitude": 2, "longitude": 10},
                    },
                },
            ),
        )
        changed_location = await app.get_composed_listing(_read("1001"))
        assert changed_location.route_estimate is not None
        assert changed_location.route_estimate.record_identifier is None
        assert changed_location.route_estimate.distance_meters == 2000


@pytest.mark.anyio
async def test_workspace_cancel_stops_route_root_and_new_request_can_run(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _listing_records(database, mixed=False)
        service = RefreshService()
        app = ReviewApplication(database, tmp_path, travel_service=service)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(name="Tools", search_run_record_identifier="fb-run")
        )
        request = RequestListingRouteRefresh(
            request_identifier="cancel-me",
            listing_identifier="1001",
            workspace_record_identifier=workspace.record_identifier,
        )
        queued = await app.request_listing_route_refresh(request)
        cancelled = await app.cancel_workspace_work(workspace.record_identifier, "Stop routes")
        assert queued.work_identifier in cancelled.cancelled_pending_identifiers
        assert await database.work_state(queued.work_identifier) is WorkState.CANCELLED
        assert (await app.request_listing_route_refresh(request)).state is WorkState.CANCELLED
        new = await app.request_listing_route_refresh(
            request.model_copy(update={"request_identifier": "fresh-request"})
        )
        assert new.created and new.state is WorkState.PENDING
        with pytest.raises(ReviewInputError, match="outside"):
            await app.request_listing_route_refresh(
                request.model_copy(
                    update={"request_identifier": "bad", "listing_identifier": "9999"}
                )
            )


@pytest.mark.anyio
async def test_parallel_replay_enqueues_only_one_exact_request(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _listing_records(database, mixed=False)
        app = ReviewApplication(database, tmp_path, travel_service=RefreshService())
        request = RequestListingRouteRefresh(
            request_identifier="parallel", listing_identifier="1001"
        )
        results = []

        async def request_once() -> None:
            results.append(await app.request_listing_route_refresh(request))

        async with anyio.create_task_group() as group:
            for _ in range(8):
                group.start_soon(request_once)
        assert sum(result.created for result in results) == 1
        assert len({result.work_identifier for result in results}) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("unknown", (False, True))
async def test_expired_record_remains_readable_but_no_longer_overrides_live_service(
    tmp_path: Path, unknown: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _listing_records(database, mixed=False)
        clock = [time_ns()]
        service = RefreshService()
        service.unknown = unknown
        app = ReviewApplication(
            database, tmp_path, travel_service=service, utc_now_ns=lambda: clock[0]
        )
        await app.request_listing_route_refresh(
            RequestListingRouteRefresh(request_identifier="expiring", listing_identifier="1001")
        )
        registry = build_travel_worker_registry(
            TravelWorkerDependencies(service=service, new_identifier=app.new_identifier)
        )
        await _run_next(database, registry, app.new_identifier, REFRESH_LISTING_ROUTE_WORK_KIND)
        first = await app.get_composed_listing(_read("1001"))
        assert (
            first.route_estimate is not None and first.route_estimate.record_identifier is not None
        )
        record_identifier = first.route_estimate.record_identifier
        clock[0] += (120 if unknown else 604_900) * 1_000_000_000
        after = await app.get_composed_listing(_read("1001"))
        assert after.route_estimate is not None
        assert after.route_estimate.record_identifier is None
        assert after.route_estimate.distance_meters == 1000
        assert (await app.get_listing_route_estimate(record_identifier)).estimate == (
            first.route_estimate.model_copy(update={"record_identifier": None})
        )


@pytest.mark.anyio
async def test_cancellation_during_provider_request_cannot_publish_selected_route(
    tmp_path: Path,
) -> None:
    class BlockingService(RefreshService):
        def __init__(self) -> None:
            super().__init__()
            self.started = anyio.Event()
            self.resume = anyio.Event()

        async def refresh_many(
            self, destinations: Sequence[TravelDestination], *, expected_origin_identity: str
        ) -> tuple[TravelEstimate, ...]:
            self.started.set()
            await self.resume.wait()
            return await super().refresh_many(
                destinations, expected_origin_identity=expected_origin_identity
            )

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _listing_records(database, mixed=False)
        service = BlockingService()
        app = ReviewApplication(database, tmp_path, travel_service=service)
        request = await app.request_listing_route_refresh(
            RequestListingRouteRefresh(request_identifier="in-flight", listing_identifier="1001")
        )
        registry = build_travel_worker_registry(
            TravelWorkerDependencies(service=service, new_identifier=app.new_identifier)
        )
        async with anyio.create_task_group() as group:
            group.start_soon(
                _run_next, database, registry, app.new_identifier, REFRESH_LISTING_ROUTE_WORK_KIND
            )
            await service.started.wait()
            result = await app.cancel_work(request.work_identifier, "Stop route request")
            assert result.cancelling_leased == 1
            service.resume.set()
        assert await database.work_state(request.work_identifier) is WorkState.CANCELLED
        projection = await app.get_composed_listing(_read("1001"))
        assert projection.route_estimate is not None
        assert projection.route_estimate.record_identifier is None
