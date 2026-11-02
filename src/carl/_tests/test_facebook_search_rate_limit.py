"""Historical rate-limited cursor runs remain readable after acquisition is disabled."""

# Fixtures publish pre-disable records; they never invoke or bypass an acquisition handler.
# pyright: reportPrivateUsage=false

from decimal import Decimal
from itertools import count
from pathlib import Path
from time import perf_counter_ns
from typing import cast

import pytest

from carl._tests.test_facebook_search_workers import _async_provenance
from carl.core.components import Component, ComponentId
from carl.core.composed_projection import ListComposedSearchRequest
from carl.core.facebook_search import (
    SearchStoppingReason,
    SearchTraversalPolicy,
    SearchTraversalState,
)
from carl.core.facebook_work import (
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_SEARCH_WORK_KIND,
    CollectSearchPayload,
    CreateSearchRequest,
    FacebookSearchRequest,
    SearchDistanceUnit,
    SearchFacebookLocation,
    SearchPriceRange,
    SearchRadius,
    collect_search_work,
)
from carl.core.models import JsonValue, NamedOutput, RecordDraft
from carl.core.review_workspace import CreateReviewWorkspaceRequest, ListWorkspaceListingsRequest
from carl.core.work import WorkCapability, WorkRequester, WorkState
from carl.core.worker import (
    AttemptContext,
    RetryWork,
    TerminalFailureWork,
    WorkerSettings,
    WorkOutcome,
)
from carl.core.workspace_search_results import ListWorkspaceSearchResultsRequest
from carl.io.sqlite import Database
from carl.io.worker import (
    TypedWorkHandler,
    WorkerRuntimeServices,
    WorkHandlerRegistry,
    execute_lease,
)
from carl.review import ReviewApplication, ReviewInputError

_SETTINGS = WorkerSettings(
    worker_count=1,
    lease_duration_ns=60_000_000_000,
    renewal_interval_ns=10_000_000_000,
    idle_poll_interval_ns=10_000_000,
)


def _payload() -> CollectSearchPayload:
    return CollectSearchPayload(
        request=FacebookSearchRequest(
            query="croquet set",
            location=SearchFacebookLocation(identifier="456"),
            radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
            price=SearchPriceRange(currency="USD", maximum=Decimal(300)),
        ),
        traversal=SearchTraversalPolicy(maximum_pages=10),
        routing=("proton", "personal", "carl"),
    )


def _historical_partial(
    payload: CollectSearchPayload,
    context: AttemptContext,
    *,
    successful_pages: int,
    terminal: bool,
    publish_run: bool,
) -> RetryWork | TerminalFailureWork:
    """Model already-retained records from the retired cursor worker."""

    run_identifier = f"partial-run-{context.attempt}"
    internal_identifier = f"partial-internal-{context.attempt}"
    expected = tuple(str(index) for index in range(1, successful_pages + 2))
    records: list[RecordDraft] = []
    outputs: list[NamedOutput] = []
    pages: list[JsonValue] = []
    for page in range(1, successful_pages + 1):
        acquisition_identifier = f"acquisition-{context.attempt}-{page}"
        records.append(
            RecordDraft(
                identifier=acquisition_identifier,
                kind=("carl", "http", "acquisition"),
                schema_version=1,
                value={"search_run_identifier": internal_identifier, "page_ordinal": page},
            )
        )
        outputs.append(
            NamedOutput(
                name=("search", "page", str(page), "acquisition"),
                object_identifier=acquisition_identifier,
            )
        )
        occurrences: list[str] = []
        for edge, listing in enumerate((str(page), str(page + 1))):
            identifier = f"occurrence-{context.attempt}-{page}-{edge}"
            occurrences.append(identifier)
            records.append(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "facebook", "search_listing_occurrence"),
                    schema_version=1,
                    value={
                        "listing_identifier": listing,
                        "search_run_identifier": internal_identifier,
                        "page_ordinal": page,
                        "edge_index": edge,
                        "acquisition_record_identifier": acquisition_identifier,
                        "original": {
                            "marketplace_listing_title": f"Listing {listing}",
                            "is_sold": False,
                            "is_pending": False,
                            "is_live": True,
                        },
                    },
                )
            )
            outputs.append(
                NamedOutput(
                    name=("search", "page", str(page), "listing_occurrence", str(edge)),
                    object_identifier=identifier,
                )
            )
        pages.append({"page_ordinal": page, "listing_occurrence_records": occurrences})
    if not publish_run:
        return TerminalFailureWork(
            records=tuple(records),
            outputs=tuple(outputs),
            result={"state": "failed", "pages_persisted": successful_pages},
            error={"kind": "search_pagination_extraction_failure"},
        )
    traversal = SearchTraversalState(
        search_run_identifier=internal_identifier,
        attempt=context.attempt,
        policy=payload.traversal,
        pages_processed=successful_pages,
        listing_observations=successful_pages * 2,
        unique_listing_identifiers=expected,
        stopping_reason=SearchStoppingReason.RATE_LIMITED,
    )
    records.append(
        RecordDraft(
            identifier=run_identifier,
            kind=("carl", "facebook", "search_run"),
            schema_version=1,
            value={
                "search_run_identifier": internal_identifier,
                "work_item_identifier": context.work_item_identifier,
                "attempt": context.attempt,
                "request": payload.request.model_dump(mode="json"),
                "traversal_strategy": payload.traversal_strategy.model_dump(mode="json"),
                "traversal": traversal.model_dump(mode="json"),
                "pages": pages,
            },
        )
    )
    outputs.append(NamedOutput(name=("search", "run"), object_identifier=run_identifier))
    result: dict[str, JsonValue] = {
        "state": "partial",
        "search_run_identifier": internal_identifier,
        "search_run_record_identifier": run_identifier,
        "pages_processed": successful_pages,
        "listing_observations": successful_pages * 2,
        "unique_listings": len(expected),
        "stopping_reason": "rate_limited",
    }
    if terminal:
        return TerminalFailureWork(
            records=tuple(records),
            outputs=tuple(outputs),
            result=result,
            error={"kind": "search_pagination_rate_limited", "decision": "retry_exhausted"},
        )
    return RetryWork(
        records=tuple(records),
        outputs=tuple(outputs),
        result=result,
        delay_ns=1,
        reason={"kind": "search_pagination_rate_limited", "decision": "retry"},
    )


class _History:
    """Seed historical outcomes with normal durability and publication fencing."""

    def __init__(self, path: Path):
        self.path = path
        self.now = 1_800_000_000_000_000_000
        self.identifiers = count()

    def new_identifier(self) -> str:
        return f"history-{next(self.identifiers)}"

    def clock(self) -> int:
        return self.now

    def app(self, database: Database) -> ReviewApplication:
        return ReviewApplication(
            database,
            self.path.parent,
            new_identifier=self.new_identifier,
            utc_now_ns=self.clock,
            code_provenance=_async_provenance,
        )

    async def enqueue(self, database: Database) -> None:
        await database.enqueue_work(
            collect_search_work(
                identifier="legacy-search", payload=_payload(), not_before_utc_ns=0
            ),
            WorkRequester(
                request_identifier=self.new_identifier(),
                kind=("carl", "mcp", "create_search"),
                identifier="historical-request",
                context={},
            ),
            event_identifier=self.new_identifier(),
            enqueued_at_utc_ns=self.now,
        )

    async def publish(
        self,
        database: Database,
        *,
        successful_pages: int = 2,
        terminal: bool = False,
        publish_run: bool = True,
    ) -> WorkOutcome:
        async def legacy_outcome(
            payload: CollectSearchPayload, context: AttemptContext
        ) -> WorkOutcome:
            return _historical_partial(
                payload,
                context,
                successful_pages=successful_pages,
                terminal=terminal,
                publish_run=publish_run,
            )

        registry = WorkHandlerRegistry(
            (
                TypedWorkHandler(
                    capability=WorkCapability(
                        kind=COLLECT_SEARCH_WORK_KIND,
                        payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
                    ),
                    component=Component(
                        ComponentId(("test", "retained_cursor_outcome")), 1, legacy_outcome
                    ),
                    payload_type=CollectSearchPayload,
                    handler=legacy_outcome,
                ),
            )
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            eligible_identifiers=("legacy-search",),
            worker_identifier="history-worker",
            lease_token=self.new_identifier(),
            lease_duration_ns=_SETTINGS.lease_duration_ns,
            utc_now_ns=self.clock,
            event_identifier=self.new_identifier(),
        )
        assert claim.lease is not None
        outcome = await execute_lease(
            database=database,
            registry=registry,
            settings=_SETTINGS,
            services=WorkerRuntimeServices(
                new_identifier=self.new_identifier,
                utc_now_ns=self.clock,
                monotonic_ns=perf_counter_ns,
                code_provenance=_async_provenance,
                invocation=lambda: {"kind": "historical_fixture"},
            ),
            lease=claim.lease,
        )
        assert outcome is not None
        self.now += 1
        return outcome


@pytest.mark.anyio
@pytest.mark.parametrize("successful_pages", (1, 2))
@pytest.mark.parametrize("terminal", (False, True))
async def test_historical_rate_limited_run_is_readable_after_restart(
    tmp_path: Path, successful_pages: int, terminal: bool
) -> None:
    history = _History(tmp_path / "carl.sqlite3")
    expected = {str(index) for index in range(1, successful_pages + 2)}
    async with Database.managed(history.path, initialize=True) as database:
        await history.enqueue(database)
        before_publication = await database.current_completion_boundary()
        outcome = await history.publish(
            database, successful_pages=successful_pages, terminal=terminal
        )
        result = cast(dict[str, JsonValue], outcome.result)
        run_identifier = cast(str, result["search_run_record_identifier"])
        with pytest.raises(KeyError):
            await database.facebook_projection_search_run(
                run_identifier, as_of_completion_sequence=before_publication
            )
        partial_boundary = await database.current_completion_boundary()
        assert partial_boundary > before_publication

    async with Database.managed(history.path) as database:
        app = history.app(database)
        runs = await app.list_search_runs()
        assert [run.record_identifier for run in runs.search_runs] == [run_identifier]
        assert runs.search_runs[0].stopping_reason == "rate_limited"
        assert runs.search_runs[0].completion_sequence == partial_boundary
        assert runs.search_runs[0].started_at_utc <= cast(str, runs.search_runs[0].ended_at_utc)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Croquet", search_run_record_identifier=run_identifier
            )
        )
        rows = await app.list_workspace_listings(
            ListWorkspaceListingsRequest(workspace_record_identifier=workspace.record_identifier)
        )
        assert {row.listing_identifier for row in rows.listings} == expected
        composed = await app.list_composed_search(
            ListComposedSearchRequest(search_run_record_identifier=run_identifier)
        )
        assert {row.listing_identifier for row in composed.listings} == expected
        assert all(
            row.title is not None and row.title.value.startswith("Listing ")
            for row in composed.listings
        )
        cards = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=workspace.search_tracks[0].track_identifier,
            )
        )
        assert {card.listing_identifier for card in cards.results} == expected
        pipeline = await database.pipeline_search_occurrences(
            search_work_identifiers=(), search_run_record_identifiers=(run_identifier,)
        )
        assert {card["external_identifier"] for card in pipeline} == expected
        payload = _payload()
        with pytest.raises(ReviewInputError, match="cursor pagination is disabled"):
            await app.create_search(
                CreateSearchRequest(
                    request=payload.request,
                    traversal=payload.traversal,
                    traversal_strategy=payload.traversal_strategy,
                )
            )


@pytest.mark.anyio
async def test_later_historical_partial_does_not_redate_earlier_publication(tmp_path: Path) -> None:
    history = _History(tmp_path / "carl.sqlite3")
    async with Database.managed(history.path, initialize=True) as database:
        await history.enqueue(database)
        first = await history.publish(database)
        first_identifier = cast(dict[str, JsonValue], first.result)["search_run_record_identifier"]
        assert isinstance(first_identifier, str)
        first_boundary = await database.current_completion_boundary()
        final = await history.publish(database, terminal=True)
        final_identifier = cast(dict[str, JsonValue], final.result)["search_run_record_identifier"]
        assert isinstance(final_identifier, str)
        assert (await database.work("legacy-search"))["state"] == WorkState.TERMINAL_FAILURE
        runs = await history.app(database).list_search_runs()
        assert len(runs.search_runs) == 2
        original = await database.facebook_projection_search_run(
            first_identifier, as_of_completion_sequence=first_boundary
        )
        assert original.completion_sequence == first_boundary
        with pytest.raises(KeyError):
            await database.facebook_projection_search_run(
                final_identifier, as_of_completion_sequence=first_boundary
            )


@pytest.mark.anyio
async def test_unpublished_historical_checkpoint_does_not_expose_run_or_cards(
    tmp_path: Path,
) -> None:
    history = _History(tmp_path / "carl.sqlite3")
    async with Database.managed(history.path, initialize=True) as database:
        await history.enqueue(database)
        before_publication = await database.current_completion_boundary()
        outcome = await history.publish(database, publish_run=False)
        result = cast(dict[str, JsonValue], outcome.result)
        assert "search_run_identifier" not in result
        assert "search_run_record_identifier" not in result
        assert await database.current_completion_boundary() == before_publication
        assert (await history.app(database).list_search_runs()).search_runs == ()
        assert (
            await database.facebook_projection_search_cards(
                ("1", "2", "3"),
                as_of_completion_sequence=2**63 - 1,
                maximum_per_listing=10,
            )
            == ()
        )
