"""Price buckets are required for acquisition; cursor history stays readable."""

# pyright: reportPrivateUsage=false

from decimal import Decimal
from pathlib import Path
from time import perf_counter_ns, time_ns
from uuid import uuid4

import pytest

from carl._tests.test_ebay_workers import _directories
from carl._tests.test_facebook_search_workers import _FakeSessionFactory
from carl._tests.test_marketplace_listing import _publish
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_review_workspace import _search_run_value
from carl._tests.test_sold_search_disabled import _counts
from carl.cli import search as cli_search
from carl.core.facebook_refresh import (
    RefreshSearchPayload,
    SearchRefreshRequest,
    refresh_search_work,
)
from carl.core.facebook_search import (
    CursorSearchTraversalStrategy,
    OverlappingPricePartitionSearchTraversalStrategy,
    SearchTraversalPolicy,
)
from carl.core.facebook_search_support import (
    CURSOR_SEARCH_UNSUPPORTED_MESSAGE,
    UnsupportedFacebookSearchMode,
    require_supported_facebook_search_acquisition,
)
from carl.core.facebook_work import (
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
from carl.core.marketplace_search import (
    MARKETPLACE_SEARCH_EXECUTION_KIND,
    MARKETPLACE_SEARCH_KIND,
    MARKETPLACE_SEARCH_TARGET_KIND,
    AddMarketplaceSearchTargetRequest,
    CreateMarketplaceSearchRequest,
    FacebookSearchTargetSpecification,
    RunMarketplaceSearchRequest,
)
from carl.core.models import RecordDraft
from carl.core.pipeline import (
    NewSearchPipelineSource,
    PipelineOptions,
    PipelineStage,
    RequestSearchPipelineRequest,
)
from carl.core.review_errors import ReviewInputError
from carl.core.review_workspace import (
    CreateReviewWorkspaceRequest,
    CreateWorkspaceSearchRequest,
    RequestWorkspaceRefreshRequest,
    RetryWorkspaceSearchTrackRequest,
    ReviseWorkspaceSearchTrackRequest,
)
from carl.core.work import WorkDefinition, WorkRequester
from carl.core.worker import AttemptContext, RetryWork, TerminalFailureWork
from carl.facebook_refresh_workers import RefreshWorkerDependencies, build_refresh_worker_registry
from carl.facebook_routed_workers import build_routed_facebook_worker_registry
from carl.facebook_search_workers import FacebookSearchWorkerDependencies
from carl.facebook_workers import FacebookWorkerDependencies, build_facebook_worker_registry
from carl.io.httpx import DirectHttpxAcquirer
from carl.io.network_activity import NetworkActivityScheduler
from carl.io.sqlite import Database
from carl.review import ReviewApplication


def _request(*, buckets: bool = False) -> CreateSearchRequest:
    return CreateSearchRequest(
        request=FacebookSearchRequest(
            query="croquet set",
            location=SearchFacebookLocation(identifier="123"),
            radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
            price=SearchPriceRange(currency="USD", maximum=Decimal(75)),
        ),
        traversal=SearchTraversalPolicy(maximum_results=100),
        traversal_strategy=(
            OverlappingPricePartitionSearchTraversalStrategy(width=Decimal(10))
            if buckets
            else CursorSearchTraversalStrategy()
        ),
    )


def _payload(request: CreateSearchRequest) -> CollectSearchPayload:
    return CollectSearchPayload(
        request=request.request,
        traversal=request.traversal,
        traversal_strategy=request.traversal_strategy,
        routing=request.requested_network_path,
    )


def _context() -> AttemptContext:
    return AttemptContext(
        work_item_identifier="refresh",
        lease_token="lease",
        worker_identifier="worker",
        attempt=1,
        operation_identifier="operation",
    )


async def _enqueue(
    database: Database, definition: WorkDefinition, *, requester: str = "history"
) -> None:
    _ = await database.enqueue_work(
        definition,
        WorkRequester(
            request_identifier=str(uuid4()),
            kind=("carl", "facebook", "search_refresh", "search"),
            identifier=requester,
            context={},
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )


async def _history(database: Database) -> None:
    timestamp = "2026-10-01T00:00:00+00:00"
    records = [
        RecordDraft(
            identifier="group",
            kind=MARKETPLACE_SEARCH_KIND,
            schema_version=1,
            value={"record_identifier": "group", "created_at_utc": timestamp},
        )
    ]
    for index, buckets in enumerate((True, False)):
        request = _request(buckets=buckets)
        work = f"work-{index}"
        await _enqueue(
            database,
            collect_search_work(identifier=work, payload=_payload(request), not_before_utc_ns=0),
        )
        value = _search_run_value(request.request.query)
        value["traversal_strategy"] = request.traversal_strategy.model_dump(mode="json")
        await _complete(
            database,
            work,
            records=(
                RecordDraft(
                    identifier=f"run-{index}",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value=value,
                ),
            ),
            result={"search_run_record_identifier": f"run-{index}"},
        )
        records.extend(
            (
                RecordDraft(
                    identifier=f"target-{index}",
                    kind=MARKETPLACE_SEARCH_TARGET_KIND,
                    schema_version=1,
                    value={
                        "record_identifier": f"target-{index}",
                        "search_record_identifier": "group",
                        "specification": FacebookSearchTargetSpecification(
                            search=request
                        ).model_dump(mode="json"),
                        "created_at_utc": timestamp,
                    },
                ),
                RecordDraft(
                    identifier=f"execution-{index}",
                    kind=MARKETPLACE_SEARCH_EXECUTION_KIND,
                    schema_version=1,
                    value={
                        "record_identifier": f"execution-{index}",
                        "search_record_identifier": "group",
                        "target_record_identifier": f"target-{index}",
                        "work_identifier": work,
                        "requested_at_utc": timestamp,
                    },
                ),
            )
        )
    await _publish(database, tuple(records))


@pytest.mark.anyio
@pytest.mark.parametrize(
    "ingress",
    (
        "direct",
        "group",
        "add",
        "run",
        "workspace_create",
        "revise",
        "refresh",
        "workspace_refresh",
        "retry",
        "pipeline",
    ),
)
async def test_cursor_acquisition_rejected_before_writes(tmp_path: Path, ingress: str) -> None:
    cursor = _request()
    targets = tuple(
        FacebookSearchTargetSpecification(search=_request(buckets=value)) for value in (True, False)
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _history(database)
        app = ReviewApplication(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(name="History", search_run_record_identifier="group")
        )
        if ingress == "retry":
            async with database._connections.writer() as connection:
                _ = await connection.execute(
                    "UPDATE work_items SET state = 'terminal_failure', error_json = ? WHERE id = ?",
                    ('{"kind":"search_pagination_rate_limited"}', "work-1"),
                )
        before = await _counts(database)
        with pytest.raises(ReviewInputError, match=r"Facebook.*cursor|cursor.*pagination"):
            if ingress == "direct":
                _ = await app.create_search(cursor)
            elif ingress == "group":
                _ = await app.create_marketplace_search(
                    CreateMarketplaceSearchRequest(targets=targets)
                )
            elif ingress == "add":
                _ = await app.add_marketplace_search_target(
                    AddMarketplaceSearchTargetRequest(
                        search_record_identifier="group", target=targets[1]
                    )
                )
            elif ingress == "run":
                _ = await app.run_marketplace_search(
                    RunMarketplaceSearchRequest(search_record_identifier="group")
                )
            elif ingress == "workspace_create":
                _ = await app.create_workspace_search(
                    CreateWorkspaceSearchRequest(
                        workspace_record_identifier=workspace.record_identifier, search=cursor
                    )
                )
            elif ingress == "revise":
                _ = await app.revise_workspace_search_track(
                    ReviseWorkspaceSearchTrackRequest(
                        workspace_record_identifier=workspace.record_identifier,
                        track_identifier="target-0",
                        expected_version=1,
                        search=cursor,
                    )
                )
            elif ingress == "refresh":
                _ = await app.request_search_refresh(
                    SearchRefreshRequest(base_search_run_record_identifier="run-1")
                )
            elif ingress == "workspace_refresh":
                _ = await app.request_workspace_refresh(
                    RequestWorkspaceRefreshRequest(
                        workspace_record_identifier=workspace.record_identifier,
                        track_identifier="target-1",
                    )
                )
            elif ingress == "retry":
                _ = await app.retry_workspace_search_track(
                    RetryWorkspaceSearchTrackRequest(
                        workspace_record_identifier=workspace.record_identifier,
                        track_identifier="target-1",
                    )
                )
            else:
                _ = await app.request_search_pipeline(
                    RequestSearchPipelineRequest(
                        request_identifier="rejected-pipeline",
                        source=NewSearchPipelineSource(targets=targets),
                        options=PipelineOptions(stop_after=PipelineStage.DETAILS),
                    )
                )
        assert await _counts(database) == before
        historical = await app.get_marketplace_search("group")
        assert isinstance(historical.targets[1].specification, FacebookSearchTargetSpecification)
        assert (
            historical.targets[1].specification.search.traversal_strategy
            == CursorSearchTraversalStrategy()
        )
        assert historical.targets[1].executions[0].search_run_record_identifier == "run-1"


@pytest.mark.anyio
@pytest.mark.parametrize("routed", (False, True))
async def test_queued_cursor_search_never_opens_session_or_loads_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, routed: bool
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Cursor work must be rejected before configuration or credential access")

    monkeypatch.setattr("carl.facebook_routed_workers.load_configuration", forbidden)
    factory = _FakeSessionFactory([])
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        if routed:
            registry = build_routed_facebook_worker_registry(
                database=database,
                directories=_directories(tmp_path),
                new_identifier=lambda: str(uuid4()),
            )
        else:
            registry = build_facebook_worker_registry(
                FacebookWorkerDependencies(
                    database=database,
                    acquirer=DirectHttpxAcquirer(),
                    new_identifier=lambda: str(uuid4()),
                ),
                FacebookSearchWorkerDependencies(
                    database=database,
                    session_factory=factory,
                    navigation_headers=(),
                    new_identifier=lambda: str(uuid4()),
                    utc_now_ns=time_ns,
                    monotonic_ns=perf_counter_ns,
                    network_activity_scheduler=NetworkActivityScheduler(
                        database=database,
                        new_identifier=lambda: str(uuid4()),
                        utc_now_ns=time_ns,
                        sample_uniform_holdoff_ns=lambda low, high: low,
                        permit_duration_ns=60_000_000_000,
                    ),
                ),
            )
        before = await _counts(database)
        for handler in registry.handlers:
            if handler.capability.kind != COLLECT_SEARCH_WORK_KIND:
                continue
            outcome = await handler.execute(_payload(_request()).as_json(), _context())
            assert isinstance(outcome, TerminalFailureWork)
            assert outcome.error["kind"] == "unsupported_facebook_search_mode"
            assert outcome.error["message"] == CURSOR_SEARCH_UNSUPPORTED_MESSAGE
        assert factory.opens == 0
        assert await _counts(database) == before


@pytest.mark.anyio
@pytest.mark.parametrize("retained_child", (False, True))
async def test_cursor_refresh_rejects_new_search_but_accepts_completed_child(
    tmp_path: Path, retained_child: bool
) -> None:
    payload = RefreshSearchPayload(
        base_search_run_record_identifier="base-run",
        search_work_identifier="child",
        search=_payload(_request()),
        item_routing=("decodo", "personal", "carl"),
        image_routing=("decodo", "personal", "datacenter"),
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(
            database,
            refresh_search_work(identifier="refresh", payload=payload, not_before_utc_ns=0),
        )
        if retained_child:
            await _enqueue(
                database,
                collect_search_work(
                    identifier="child", payload=payload.search, not_before_utc_ns=0
                ),
                requester="refresh",
            )
            await _complete(database, "child", result={"search_run_record_identifier": "saved-run"})
        before = await _counts(database)
        registry = build_refresh_worker_registry(
            RefreshWorkerDependencies(database=database, new_identifier=lambda: str(uuid4()))
        )
        outcome = await registry.handlers[0].execute(payload.model_dump(mode="json"), _context())
        if retained_child:
            assert isinstance(outcome, RetryWork)
            assert outcome.result["stage"] == "search_complete"
            assert outcome.result["refreshed_search_run_record_identifier"] == "saved-run"
        else:
            assert isinstance(outcome, TerminalFailureWork)
            assert outcome.error["kind"] == "unsupported_facebook_search_mode"
            assert await _counts(database) == before


@pytest.mark.anyio
async def test_explicit_buckets_remain_eligible_and_cursor_history_deserializes(
    tmp_path: Path,
) -> None:
    cursor = _request()
    assert CreateSearchRequest.model_validate_json(cursor.model_dump_json()) == cursor
    with pytest.raises(UnsupportedFacebookSearchMode):
        require_supported_facebook_search_acquisition(cursor.request, cursor.traversal_strategy)
    buckets = _request(buckets=True)
    require_supported_facebook_search_acquisition(buckets.request, buckets.traversal_strategy)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        await _history(database)
        created = await app.create_search(
            buckets.model_copy(
                update={
                    "request": buckets.request.model_copy(update={"query": "new bucket search"})
                }
            )
        )
        assert created.created and created.state == "pending"
        refreshed = await app.request_search_refresh(
            SearchRefreshRequest(
                base_search_run_record_identifier="run-1",
                traversal_strategy=buckets.traversal_strategy,
            )
        )
        assert refreshed.created and refreshed.state == "pending"


@pytest.mark.parametrize("missing", ("width", "maximum_price"))
def test_cli_requires_explicit_buckets_before_creating_database(
    tmp_path: Path, missing: str
) -> None:
    database = tmp_path / "must-not-exist.sqlite3"
    with pytest.raises(ValueError, match=r"price-partition-width|maximum-price"):
        cli_search(
            "croquet set",
            facebook_location="123",
            radius=60,
            maximum_results=100,
            price_partition_width=None if missing == "width" else Decimal(10),
            maximum_price=None if missing == "maximum_price" else Decimal(75),
            database=database,
        )
    assert not database.exists()
