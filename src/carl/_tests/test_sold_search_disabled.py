"""Reject new closed acquisitions while retaining historical request and read support."""

# pyright: reportPrivateUsage=false

from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest

from carl._tests.test_ebay_workers import _directories
from carl._tests.test_marketplace_listing import _publish, _record
from carl._tests.test_mixed_workspace import _complete
from carl.core.ebay import (
    CollectEbaySearchPayload,
    EbayListingState,
    EbaySearchRequest,
    collect_ebay_search_work,
)
from carl.core.ebay_refresh import RefreshEbaySearchPayload, refresh_ebay_search_work
from carl.core.ebay_search_support import (
    CLOSED_SEARCH_UNSUPPORTED_MESSAGE,
    UnsupportedEbaySearchMode,
    require_supported_ebay_search_acquisition,
)
from carl.core.facebook_refresh import SearchRefreshRequest
from carl.core.marketplace_search import (
    MARKETPLACE_SEARCH_EXECUTION_KIND,
    MARKETPLACE_SEARCH_KIND,
    MARKETPLACE_SEARCH_TARGET_KIND,
    AddMarketplaceSearchTargetRequest,
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    MarketplaceSearch,
    RunMarketplaceSearchRequest,
    SetMarketplaceSearchTargetEnabledRequest,
)
from carl.core.models import JsonValue, RecordDraft
from carl.core.review_workspace import (
    CreateReviewWorkspaceRequest,
    CreateWorkspaceSearchRequest,
    GetWorkspaceListingRequest,
    RequestWorkspaceRefreshRequest,
    RetryWorkspaceSearchTrackRequest,
)
from carl.core.work import WorkRequester
from carl.core.worker import AttemptContext, RetryWork, TerminalFailureWork
from carl.core.workspace_search_results import ListWorkspaceSearchResultsRequest
from carl.ebay import collect_configured_ebay_search
from carl.ebay_refresh_workers import (
    EbayRefreshWorkerDependencies,
    build_ebay_refresh_worker_registry,
)
from carl.ebay_workers import EbaySearchWorkerDependencies, build_ebay_worker_registry
from carl.io.sqlite import Database
from carl.review import ReviewApplication, ReviewInputError


async def _historical_group(
    app: ReviewApplication,
    database: Database,
    targets: tuple[EbaySearchTargetSpecification, ...],
) -> MarketplaceSearch:
    """Load pre-disable search history directly, without invoking new-search ingress."""
    timestamp = "2026-09-29T00:00:00+00:00"
    records = [
        RecordDraft(
            identifier="legacy-group",
            kind=MARKETPLACE_SEARCH_KIND,
            schema_version=1,
            value={"record_identifier": "legacy-group", "created_at_utc": timestamp},
        )
    ]
    for index, target in enumerate(targets):
        work = f"legacy-work-{index}"
        _ = await database.enqueue_work(
            collect_ebay_search_work(
                identifier=work,
                payload=CollectEbaySearchPayload(request=target.search),
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("test", "history"),
                identifier=work,
                context={},
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        records.extend(
            (
                RecordDraft(
                    identifier=f"legacy-target-{index}",
                    kind=MARKETPLACE_SEARCH_TARGET_KIND,
                    schema_version=1,
                    value={
                        "record_identifier": f"legacy-target-{index}",
                        "search_record_identifier": "legacy-group",
                        "specification": target.model_dump(mode="json"),
                        "created_at_utc": timestamp,
                    },
                ),
                RecordDraft(
                    identifier=f"legacy-execution-{index}",
                    kind=MARKETPLACE_SEARCH_EXECUTION_KIND,
                    schema_version=1,
                    value={
                        "record_identifier": f"legacy-execution-{index}",
                        "search_record_identifier": "legacy-group",
                        "target_record_identifier": f"legacy-target-{index}",
                        "work_identifier": work,
                        "requested_at_utc": timestamp,
                    },
                ),
            )
        )
    await _publish(database, tuple(records))
    return await app.get_marketplace_search("legacy-group")


async def _counts(database: Database) -> tuple[int, ...]:
    async with database._connections.reader() as connection:
        cursor = await connection.execute(
            "SELECT (SELECT COUNT(*) FROM objects), (SELECT COUNT(*) FROM operations), "
            "(SELECT COUNT(*) FROM work_items), (SELECT COUNT(*) FROM scheduling_constraints)"
        )
        row = await cursor.fetchone()
    assert row is not None
    return tuple(int(value) for value in row)


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ("sold", "completed"))
@pytest.mark.parametrize(
    "ingress",
    (
        "direct",
        "group",
        "add",
        "run",
        "workspace_create",
        "refresh",
        "workspace_refresh",
        "retry",
    ),
)
async def test_sold_acquisition_ingress_rejects_before_any_write(
    tmp_path: Path,
    ingress: str,
    mode: EbayListingState,
) -> None:
    sold = EbaySearchRequest(query="Nagler", listing_state=mode)
    targets = (
        EbaySearchTargetSpecification(search=EbaySearchRequest(query="active Nagler")),
        EbaySearchTargetSpecification(search=sold),
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        group = await _historical_group(app, database, targets)
        for index, target in enumerate(targets):
            await _complete(
                database,
                group.targets[index].executions[0].work_identifier,
                records=(
                    _record(
                        f"run-{index}", "search_run", request=target.search.model_dump(mode="json")
                    ),
                    _record(
                        f"card-{index}",
                        "search_listing_occurrence",
                        search_run_record_identifier=f"run-{index}",
                        item_identifier="256123456789",
                        canonical_url="https://www.ebay.com/itm/256123456789",
                        position=1,
                        listing_state="sold" if index == 1 else "active",
                        sold_price="$180" if index == 1 else None,
                    ),
                ),
                result={"search_run_record_identifier": f"run-{index}"},
            )
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Legacy", search_run_record_identifier=group.record_identifier
            )
        )
        before = await _counts(database)
        with pytest.raises(
            ReviewInputError, match="does not presently support eBay sold/completed"
        ):
            if ingress == "direct":
                await app.create_ebay_search(sold)
            elif ingress == "group":
                await app.create_marketplace_search(CreateMarketplaceSearchRequest(targets=targets))
            elif ingress == "add":
                await app.add_marketplace_search_target(
                    AddMarketplaceSearchTargetRequest(
                        search_record_identifier=group.record_identifier, target=targets[1]
                    )
                )
            elif ingress == "run":
                await app.run_marketplace_search(
                    RunMarketplaceSearchRequest(search_record_identifier=group.record_identifier)
                )
            elif ingress == "workspace_create":
                await app.create_workspace_search(
                    CreateWorkspaceSearchRequest(
                        workspace_record_identifier=workspace.record_identifier, search=targets[1]
                    )
                )
            elif ingress == "refresh":
                await app.request_search_refresh(
                    SearchRefreshRequest(base_search_run_record_identifier="run-1")
                )
            elif ingress == "workspace_refresh":
                await app.request_workspace_refresh(
                    RequestWorkspaceRefreshRequest(
                        workspace_record_identifier=workspace.record_identifier,
                        track_identifier="legacy-target-1",
                    )
                )
            else:
                await app.retry_workspace_search_track(
                    RetryWorkspaceSearchTrackRequest(
                        workspace_record_identifier=workspace.record_identifier,
                        track_identifier="legacy-target-1",
                    )
                )
        assert await _counts(database) == before
        cards = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier="legacy-target-1",
                listing_state="sold",
            )
        )
        assert len(cards.results) == 1 and cards.results[0].last_sale is not None
        listing = await app.get_workspace_listing(
            GetWorkspaceListingRequest(
                workspace_record_identifier=workspace.record_identifier,
                listing_identifier="ebay:256123456789",
            )
        )
        assert listing.status.value.value == "sold" and listing.last_sale is not None


@pytest.mark.anyio
async def test_disabling_historical_sold_target_allows_active_group_rerun(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        group = await _historical_group(
            app,
            database,
            (
                EbaySearchTargetSpecification(search=EbaySearchRequest(query="active Nagler")),
                EbaySearchTargetSpecification(
                    search=EbaySearchRequest(query="sold Nagler", listing_state="sold")
                ),
            ),
        )
        _ = await app.set_marketplace_search_target_enabled(
            SetMarketplaceSearchTargetEnabledRequest(
                search_record_identifier=group.record_identifier,
                target_record_identifier="legacy-target-1",
                enabled=False,
            )
        )
        result = await app.run_marketplace_search(
            RunMarketplaceSearchRequest(search_record_identifier=group.record_identifier)
        )
        assert len(result.targets[0].executions) == 2
        assert len(result.targets[1].executions) == 1 and not result.targets[1].enabled


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ("sold", "completed"))
@pytest.mark.parametrize("schema_version", (1, 2))
async def test_queued_sold_worker_is_terminal_unsupported_without_collector(
    tmp_path: Path,
    schema_version: int,
    mode: EbayListingState,
) -> None:
    async def forbidden(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
        pytest.fail("Unsupported sold work must not invoke an acquisition collector")

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database, _directories(tmp_path), lambda: "unused", collector=forbidden
            )
        )
        handler = next(
            value
            for value in registry.handlers
            if value.capability.payload_schema_version == schema_version
        )
        outcome = await handler.execute(
            CollectEbaySearchPayload(
                request=EbaySearchRequest(query="Nagler", listing_state=mode)
            ).as_json(),
            AttemptContext(
                work_item_identifier="work",
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="operation",
            ),
        )
        assert isinstance(outcome, TerminalFailureWork)
        assert outcome.error["kind"] == "unsupported_ebay_search_mode"
        assert outcome.error["message"] == CLOSED_SEARCH_UNSUPPORTED_MESSAGE
        assert outcome.result == {"state": "unsupported"}


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ("sold", "completed"))
async def test_configured_sold_collector_rejects_before_configuration_or_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: EbayListingState,
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Sold rejection must precede configuration and credential access")

    monkeypatch.setattr("carl.ebay.load_configuration", forbidden)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        with pytest.raises(
            UnsupportedEbaySearchMode, match="does not presently support eBay sold/completed"
        ):
            await collect_configured_ebay_search(
                database,
                directories=_directories(tmp_path),
                request=EbaySearchRequest(query="Nagler", listing_state=mode),
            )


@pytest.mark.anyio
async def test_active_requests_remain_eligible_and_closed_requests_still_deserialize(
    tmp_path: Path,
) -> None:
    for mode in ("sold", "completed"):
        historical = EbaySearchRequest(query="Nagler", listing_state=mode)
        assert EbaySearchRequest.model_validate_json(historical.model_dump_json()) == historical
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        for state in ("active",):
            request = EbaySearchRequest(query="Nagler", listing_state=state)
            require_supported_ebay_search_acquisition(request)
            result = await app.create_ebay_search(request)
            assert result.created and result.state == "pending"


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ("sold", "completed"))
@pytest.mark.parametrize("retained_child", (False, True))
async def test_closed_refresh_stops_before_new_search_but_keeps_retained_child(
    tmp_path: Path,
    mode: EbayListingState,
    retained_child: bool,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        search = EbaySearchRequest(query="Nagler", listing_state=mode)
        payload = RefreshEbaySearchPayload(
            base_search_run_record_identifier="base-run",
            search_work_identifier="search-child",
            search=search,
        )
        _ = await database.enqueue_work(
            refresh_ebay_search_work(identifier="refresh", payload=payload),
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("test", "history"),
                identifier="refresh",
                context={},
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        if retained_child:
            _ = await database.enqueue_work(
                collect_ebay_search_work(
                    identifier="search-child",
                    payload=CollectEbaySearchPayload(request=search),
                    not_before_utc_ns=0,
                ),
                WorkRequester(
                    request_identifier=str(uuid4()),
                    kind=("carl", "ebay", "search_refresh", "search"),
                    identifier="refresh",
                    context={"search_refresh_work_identifier": "refresh"},
                ),
                event_identifier=str(uuid4()),
                enqueued_at_utc_ns=time_ns(),
            )
            await _complete(
                database,
                "search-child",
                records=(
                    _record("retained-run", "search_run", request=search.model_dump(mode="json")),
                ),
                result={"search_run_record_identifier": "retained-run"},
            )
        before = await _counts(database)
        registry = build_ebay_refresh_worker_registry(
            EbayRefreshWorkerDependencies(database, lambda: str(uuid4()))
        )
        outcome = await registry.handlers[0].execute(
            payload.model_dump(mode="json"),
            AttemptContext(
                work_item_identifier="refresh",
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="operation",
            ),
        )
        assert await _counts(database) == before
        if retained_child:
            assert isinstance(outcome, RetryWork)
            assert outcome.result["stage"] == "search_complete"
            assert outcome.result["refreshed_search_run_record_identifier"] == "retained-run"
        else:
            assert isinstance(outcome, TerminalFailureWork)
            assert outcome.error["kind"] == "unsupported_ebay_search_mode"
            assert outcome.error["message"] == CLOSED_SEARCH_UNSUPPORTED_MESSAGE
            assert outcome.result == {"stage": "search_unsupported"}
