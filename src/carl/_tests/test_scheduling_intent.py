"""API intent is explicit; shared evidence and child fanout do not create new roots."""

# Observe the actual requester boundary before storage materializes its immutable owner.
# pyright: reportPrivateUsage=false

from collections.abc import Sequence
from pathlib import Path

import pytest

from carl._tests.test_ebay_item_workers import _async_provenance
from carl._tests.test_review_travel import _listing_records
from carl._tests.test_route_refresh_integration import RefreshService
from carl.core.ebay import COLLECT_EBAY_SEARCH_WORK_KIND, EbaySearchRequest
from carl.core.fair_scheduling import SchedulingContext, SchedulingServiceClass
from carl.core.marketplace_listing import RequestListingDetailsRequest
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    RunMarketplaceSearchRequest,
)
from carl.core.pipeline import (
    SEARCH_PIPELINE_WORK_KIND,
    NewSearchPipelineSource,
    PipelineOptions,
    PipelineStage,
    RequestSearchPipelineRequest,
    SearchWorkPipelineSource,
    WorkspacePipelineSource,
)
from carl.core.review_workspace import CreateReviewWorkspaceRequest, CreateWorkspaceSearchRequest
from carl.core.travel_data import TravelDataSetupRequest
from carl.core.travel_refresh import RequestListingRouteRefresh
from carl.core.work import EnqueueResult, HoldoffDecision, WorkDefinition, WorkRequester
from carl.io.sqlite import Database
from carl.review import ReviewApplication

pytestmark = pytest.mark.anyio


@pytest.fixture
def intents(monkeypatch: pytest.MonkeyPatch) -> list[tuple[WorkDefinition, WorkRequester]]:
    recorded: list[tuple[WorkDefinition, WorkRequester]] = []
    original = Database.enqueue_work

    async def enqueue(
        database: Database,
        definition: WorkDefinition,
        requester: WorkRequester,
        *,
        event_identifier: str,
        enqueued_at_utc_ns: int,
        holdoff_decisions: Sequence[HoldoffDecision] = (),
    ) -> EnqueueResult:
        recorded.append((definition, requester))
        return await original(
            database,
            definition,
            requester,
            event_identifier=event_identifier,
            enqueued_at_utc_ns=enqueued_at_utc_ns,
            holdoff_decisions=holdoff_decisions,
        )

    monkeypatch.setattr(Database, "enqueue_work", enqueue)
    return recorded


async def test_group_targets_share_one_interactive_intent_and_workspace_is_explicit(
    tmp_path: Path, intents: list[tuple[WorkDefinition, WorkRequester]]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path, code_provenance=_async_provenance)
        group = await app.create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=tuple(
                    EbaySearchTargetSpecification(search=EbaySearchRequest(query=query))
                    for query in ("scope", "tripod")
                )
            )
        )
        initial = [
            requester
            for definition, requester in intents
            if definition.kind == COLLECT_EBAY_SEARCH_WORK_KIND
        ]
        assert len(initial) == 2
        assert (
            initial[0].scheduling
            == initial[1].scheduling
            == SchedulingContext(
                root_request_identifier=group.record_identifier,
                service_class=SchedulingServiceClass.INTERACTIVE,
            )
        )
        assert all(requester.parent_work_identifier is None for requester in initial)
        intents.clear()
        await app.run_marketplace_search(
            RunMarketplaceSearchRequest(search_record_identifier=group.record_identifier)
        )
        rerun = [requester for _, requester in intents]
        assert len(rerun) == 2 and rerun[0].scheduling == rerun[1].scheduling
        assert rerun[0].scheduling is not None
        assert rerun[0].scheduling.root_request_identifier != group.record_identifier
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Workspace", search_run_record_identifier=group.record_identifier
            )
        )
        intents.clear()
        queued = await app.create_workspace_search(
            CreateWorkspaceSearchRequest(
                workspace_record_identifier=workspace.record_identifier,
                search=EbaySearchTargetSpecification(search=EbaySearchRequest(query="mount")),
            )
        )
        assert len(intents) == 1
        assert intents[0][1].scheduling == SchedulingContext(
            root_request_identifier=queued.work_identifier,
            workspace_identifier=workspace.record_identifier,
            service_class=SchedulingServiceClass.INTERACTIVE,
        )


async def test_new_search_pipeline_searches_and_coordinator_share_the_accepted_background_intent(
    tmp_path: Path, intents: list[tuple[WorkDefinition, WorkRequester]]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path, code_provenance=_async_provenance)
        request = RequestSearchPipelineRequest(
            request_identifier="explicit-pipeline-intent",
            source=NewSearchPipelineSource(
                targets=tuple(
                    EbaySearchTargetSpecification(search=EbaySearchRequest(query=query))
                    for query in ("scope", "tripod")
                )
            ),
            options=PipelineOptions(stop_after=PipelineStage.DETAILS),
        )
        created = await app.request_search_pipeline(request)
        assert [definition.kind for definition, _ in intents] == [
            COLLECT_EBAY_SEARCH_WORK_KIND,
            COLLECT_EBAY_SEARCH_WORK_KIND,
            SEARCH_PIPELINE_WORK_KIND,
        ]
        expected = SchedulingContext(root_request_identifier=created.work_identifier)
        assert all(requester.scheduling == expected for _, requester in intents)
        assert all(requester.parent_work_identifier is None for _, requester in intents)
        intents.clear()
        replay = await app.request_search_pipeline(request)
        assert replay.work_identifier == created.work_identifier and not replay.created
        assert intents == []


async def test_evidence_requester_is_not_a_scheduling_parent_but_typed_child_is(
    tmp_path: Path, intents: list[tuple[WorkDefinition, WorkRequester]]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path, code_provenance=_async_provenance)
        search = await app.create_ebay_search(EbaySearchRequest(query="scope"))
        pipeline = await app.request_search_pipeline(
            RequestSearchPipelineRequest(
                request_identifier="background-parent",
                source=SearchWorkPipelineSource(search_work_identifiers=(search.work_identifier,)),
                options=PipelineOptions(stop_after=PipelineStage.DETAILS),
            )
        )
        intents.clear()
        direct_result = await app.request_listing_details(
            RequestListingDetailsRequest(external_identifier="256123456789", maximum_images=0),
            requester_identifier=search.work_identifier,
        )
        direct_definition, direct = intents[-1]
        assert direct.parent_work_identifier is None
        assert direct.scheduling == SchedulingContext(
            root_request_identifier=direct_definition.identifier,
            service_class=SchedulingServiceClass.INTERACTIVE,
        )
        assert await database.work_scheduling(direct_result.work_identifier) == direct.scheduling
        child_result = await app.request_listing_details(
            RequestListingDetailsRequest(external_identifier="256123456790", maximum_images=0),
            requester_identifier="synthetic-listing-requester",
            parent_work_identifier=pipeline.work_identifier,
        )
        child = intents[-1][1]
        assert child.parent_work_identifier == pipeline.work_identifier
        assert child.scheduling is None
        assert await database.work_scheduling(child_result.work_identifier) == (
            await database.work_scheduling(pipeline.work_identifier)
        )
        shared = await app.request_listing_details(
            RequestListingDetailsRequest(external_identifier="256123456789", maximum_images=0),
            parent_work_identifier=pipeline.work_identifier,
        )
        assert not shared.created and shared.work_identifier == direct_result.work_identifier
        assert await database.work_scheduling(shared.work_identifier) == direct.scheduling


async def test_dataset_setup_is_one_explicit_background_root_and_replay_adds_no_intent(
    tmp_path: Path, intents: list[tuple[WorkDefinition, WorkRequester]]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path, code_provenance=_async_provenance)
        request = TravelDataSetupRequest(request_identifier="dataset-request", provider="geonames")
        queued = await app.request_travel_data_setup(request)
        assert intents[-1][1].scheduling == SchedulingContext(
            root_request_identifier=queued.work_identifier
        )
        assert intents[-1][1].parent_work_identifier is None
        intents.clear()
        replay = await app.request_travel_data_setup(request)
        assert not replay.created and replay.work_identifier == queued.work_identifier
        assert intents == []


async def test_same_external_request_id_is_independent_across_three_public_apis(
    tmp_path: Path, intents: list[tuple[WorkDefinition, WorkRequester]]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _listing_records(database, mixed=False)
        service = RefreshService()
        app = ReviewApplication(
            database, tmp_path, code_provenance=_async_provenance, travel_service=service
        )
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(name="Tools", search_run_record_identifier="fb-run")
        )
        intents.clear()
        setup_request = TravelDataSetupRequest(request_identifier="daily", provider="geonames")
        setup = await app.request_travel_data_setup(setup_request)
        pipeline_request = RequestSearchPipelineRequest(
            request_identifier="daily",
            source=WorkspacePipelineSource(workspace_record_identifier=workspace.record_identifier),
            options=PipelineOptions(stop_after=PipelineStage.DETAILS),
        )
        pipeline = await app.request_search_pipeline(pipeline_request)
        route_request = RequestListingRouteRefresh(
            request_identifier="daily",
            listing_identifier="1001",
            workspace_record_identifier=workspace.record_identifier,
        )
        route = await app.request_listing_route_refresh(route_request)
        expected = (
            SchedulingContext(root_request_identifier=setup.work_identifier),
            SchedulingContext(
                root_request_identifier=pipeline.work_identifier,
                workspace_identifier=workspace.record_identifier,
            ),
            SchedulingContext(
                root_request_identifier=route.work_identifier,
                workspace_identifier=workspace.record_identifier,
                service_class=SchedulingServiceClass.INTERACTIVE,
            ),
        )
        assert tuple(requester.scheduling for _, requester in intents) == expected
        assert len({context.root_request_identifier for context in expected}) == 3
        assert await database.work_scheduling(setup.work_identifier) == expected[0]
        assert await database.work_scheduling(pipeline.work_identifier) == expected[1]
        assert await database.work_scheduling(route.work_identifier) == expected[2]
        intents.clear()
        assert not (await app.request_travel_data_setup(setup_request)).created
        assert not (await app.request_search_pipeline(pipeline_request)).created
        assert not (await app.request_listing_route_refresh(route_request)).created
        assert intents == [] and service.refreshes == 0
