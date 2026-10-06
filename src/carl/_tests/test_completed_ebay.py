"""Completed eBay searches include sold and ended-unsold evidence, never guessed sales."""

from pathlib import Path
from time import time_ns
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import pytest

from carl._tests.test_marketplace_listing import _publish, _record
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_review_workspace import _provenance
from carl._tests.test_sold_search_disabled import _historical_group
from carl.core.components import Component, ComponentId
from carl.core.composed_projection import ComposedListingFilters, ListingStatus
from carl.core.ebay import (
    CollectEbaySearchPayload,
    EbaySearchRequest,
    collect_ebay_search_work,
    ebay_search_plan,
    extract_ebay_search,
)
from carl.core.ebay_refresh import RefreshEbaySearchPayload, refresh_ebay_search_work
from carl.core.json import encode_json
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    ListMarketplaceSearchResultsRequest,
)
from carl.core.models import CodeProvenance, RecordDraft
from carl.core.review_workspace import (
    CreateReviewWorkspaceRequest,
    CreateWorkspaceSearchResult,
    GetWorkspaceListingRequest,
    ListWorkspaceListingsRequest,
    RetryWorkspaceSearchTrackRequest,
)
from carl.core.work import WorkCapability, WorkRequester, WorkState
from carl.core.workspace_search_results import ListWorkspaceSearchResultsRequest
from carl.io.sqlite import Database
from carl.review import ReviewApplication, ReviewInputError


def _html(item: str, marker: str, *, title: str = "Tele Vue Nagler", style: str = "s-card") -> str:
    return (
        f'<li class="{style}"><a href="/itm/{item}"></a>'
        f'<div class="{style}__title">{title}</div><span>{marker}</span>'
        f'<span class="{style}__price">$180.00</span><span>Pre-Owned</span>'
        "<span>+$8 shipping</span></li>"
    )


def _card(run: str, item: str, marker: str, *, active: bool = False) -> RecordDraft:
    (occurrence,) = extract_ebay_search(
        _html(item, marker), listing_state="active" if active else "completed"
    ).listing_occurrences
    return _record(
        f"{run}-{item}",
        "search_listing_occurrence",
        **(
            occurrence.model_dump(mode="json")
            | {"search_run_record_identifier": run, "page_ordinal": 0}
        ),
    )


def test_completed_url_is_inclusive_on_every_page_and_has_distinct_work_identity() -> None:
    modes = ("active", "sold", "completed")
    requests = tuple(EbaySearchRequest(query="Nagler", listing_state=mode) for mode in modes)
    identities = tuple(
        collect_ebay_search_work(
            identifier=f"work-{index}",
            payload=CollectEbaySearchPayload(request=request),
            not_before_utc_ns=0,
        ).deduplication_identity
        for index, request in enumerate(requests)
    )
    assert len(set(identities)) == 3
    for page in (1, 2, 5):
        plan = ebay_search_plan(requests[2], network_path=("test",), page_number=page)
        query = parse_qs(urlsplit(plan.url).query)
        assert query["LH_Complete"] == ["1"]
        assert "LH_Sold" not in query
        assert query.get("_pgn") == (None if page == 1 else [str(page)])


@pytest.mark.parametrize("style", ("s-card", "s-item"))
@pytest.mark.parametrize(
    ("marker", "expected_state", "expected_sale_price", "expected_price_status"),
    (
        ("Sold Sep 29, 2026", "sold", "$180.00", "displayed"),
        ("Sold", "sold", "$180.00", "displayed"),
        ("Ended Sep 29, 2026", "completed", None, None),
        ("Ended", "completed", None, None),
        ("Best offer accepted", "sold", None, "best_offer_accepted"),
        ("", None, None, None),
    ),
)
def test_completed_cards_preserve_actual_sale_or_unsold_state(
    style: str,
    marker: str,
    expected_state: str | None,
    expected_sale_price: str | None,
    expected_price_status: str | None,
) -> None:
    extraction = extract_ebay_search(
        _html("256123456789", marker, style=style), listing_state="completed"
    )
    (card,) = extraction.listing_occurrences
    assert card.listing_state == expected_state
    assert card.sold_price == expected_sale_price
    assert card.sold_price_status == expected_price_status
    assert card.displayed_price == "$180.00"
    assert card.condition == "Pre-Owned"
    if expected_state != "sold":
        assert card.sold_date is None and card.sold_date_text is None
    if expected_state is None:
        assert any(
            issue.get("item_identifier") == card.item_identifier for issue in extraction.issues
        )


@pytest.mark.parametrize(
    "title",
    (
        "Ended Sep 29, 2026 vintage catalog",
        "Sold Sep 29, 2026 vintage catalog",
        "Best offer accepted slogan mug",
        "Sold",
        "Ended",
    ),
)
def test_completed_mode_does_not_promote_title_text_to_completion_evidence(title: str) -> None:
    (card,) = extract_ebay_search(
        _html("256123456789", "", title=title), listing_state="completed"
    ).listing_occurrences
    assert card.listing_state is None
    assert card.sold_price is None


@pytest.mark.anyio
async def test_completed_group_refresh_and_workspace_views_preserve_identity_and_actual_states(
    tmp_path: Path,
) -> None:
    async def provenance() -> CodeProvenance:
        return _provenance()

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path, code_provenance=provenance)
        group = await _historical_group(
            app,
            database,
            (
                EbaySearchTargetSpecification(search=EbaySearchRequest(query="Nagler")),
                EbaySearchTargetSpecification(
                    search=EbaySearchRequest(query="Nagler", listing_state="completed")
                ),
            ),
        )
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Completed comparables", search_run_record_identifier=group.record_identifier
            )
        )
        shared, sold, unknown = "256123456780", "256123456781", "256123456782"
        for index, run, cards in (
            (0, "active-run", (_card("active-run", shared, "", active=True),)),
            (
                1,
                "completed-run",
                (
                    _card("completed-run", shared, "Ended Sep 29, 2026"),
                    _card("completed-run", sold, "Sold Sep 29, 2026"),
                    _card("completed-run", unknown, ""),
                ),
            ),
        ):
            request = group.targets[index].specification.search
            await _complete(
                database,
                group.targets[index].executions[0].work_identifier,
                records=(
                    _record(run, "search_run", request=request.model_dump(mode="json")),
                    *cards,
                ),
                result={"state": "completed", "search_run_record_identifier": run},
            )
        current = await app.get_review_workspace(workspace.record_identifier)
        completed_track = next(
            track for track in current.search_tracks if track.listing_state == "completed"
        )
        assert len(current.search_tracks) == 2
        run_summaries = await app.list_search_runs(query="Nagler")
        assert {run.record_identifier: run.listing_state for run in run_summaries.search_runs} == {
            "active-run": "active",
            "completed-run": "completed",
        }
        request = ListWorkspaceSearchResultsRequest(
            workspace_record_identifier=workspace.record_identifier,
            track_identifier=completed_track.track_identifier,
            listing_state="completed",
        )
        cards = await app.list_workspace_search_results(request)
        assert {card.listing_state for card in cards.results} == {"completed", "sold"}
        assert {card.listing_identifier for card in cards.results} == {
            f"ebay:{shared}",
            f"ebay:{sold}",
        }
        ended = next(card for card in cards.results if card.listing_state == "completed")
        assert ended.last_sale is None
        assert ended.price is None
        listing = await app.get_workspace_listing(
            GetWorkspaceListingRequest(
                workspace_record_identifier=workspace.record_identifier,
                listing_identifier=f"ebay:{shared}",
            )
        )
        assert listing.listing_identifier == f"ebay:{shared}"
        assert listing.status.value is ListingStatus.UNAVAILABLE
        assert listing.last_sale is None
        unknown_listing = await app.get_workspace_listing(
            GetWorkspaceListingRequest(
                workspace_record_identifier=workspace.record_identifier,
                listing_identifier=f"ebay:{unknown}",
            )
        )
        assert unknown_listing.status.value is ListingStatus.UNKNOWN
        assert unknown_listing.last_sale is None
        summaries = await app.list_workspace_listings(
            ListWorkspaceListingsRequest(
                workspace_record_identifier=workspace.record_identifier,
                filters=ComposedListingFilters(statuses=(ListingStatus.UNAVAILABLE,)),
            )
        )
        assert [summary.listing_identifier for summary in summaries.listings] == [f"ebay:{shared}"]
        assert summaries.listings[0].last_sale is None
        grouped = await app.list_marketplace_search_results(
            ListMarketplaceSearchResultsRequest(search_record_identifier=group.record_identifier)
        )
        assert grouped.total_distinct_results == 3
        shared_result = next(
            result for result in grouped.results if result.external_identifier == shared
        )
        assert shared_result.listing_state == "completed"
        assert shared_result.sold_price is None
        assert len(shared_result.occurrences) == 2
        # Recreate a refresh queued before closed-search acquisitions were disabled.
        refresh = await database.enqueue_work(
            refresh_ebay_search_work(
                identifier="legacy-refresh",
                payload=RefreshEbaySearchPayload(
                    base_search_run_record_identifier="completed-run",
                    search_work_identifier="legacy-refresh-child",
                    search=EbaySearchRequest(query="Nagler", listing_state="completed"),
                ),
            ),
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("carl", "mcp", "request_workspace_refresh"),
                identifier=workspace.record_identifier,
                context={"track_identifier": completed_track.track_identifier},
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        refresh_work = await database.work(refresh.work_item_identifier)
        assert refresh_work["payload"]["search"]["listing_state"] == "completed"
        await _complete(
            database,
            refresh.work_item_identifier,
            records=(
                _record(
                    "refreshed-run",
                    "search_run",
                    request=EbaySearchRequest(query="Nagler", listing_state="completed").model_dump(
                        mode="json"
                    ),
                ),
                _card("refreshed-run", shared, "Sold Sep 30, 2026"),
            ),
            result={
                "state": "completed",
                "refreshed_search_run_record_identifier": "refreshed-run",
            },
        )
        refreshed = await app.list_workspace_search_results(request)
        assert refreshed.source_search_run_record_identifier == "refreshed-run"
        assert [card.listing_state for card in refreshed.results] == ["sold"]
        assert refreshed.results[0].listing_identifier == ended.listing_identifier
        refreshed_summaries = await app.list_search_runs(query="Nagler")
        assert (
            next(
                run
                for run in refreshed_summaries.search_runs
                if run.record_identifier == "refreshed-run"
            ).listing_state
            == "completed"
        )
        updated = await app.get_workspace_listing(
            GetWorkspaceListingRequest(
                workspace_record_identifier=workspace.record_identifier,
                listing_identifier=f"ebay:{shared}",
            )
        )
        assert updated.status.value is ListingStatus.SOLD
        assert updated.last_sale is not None
        await _publish(
            database,
            (
                _record(
                    "later-unavailable-page",
                    "listing_observation",
                    item_identifier=shared,
                    classification="unavailable",
                ),
            ),
        )
        unavailable_page = await app.get_workspace_listing(
            GetWorkspaceListingRequest(
                workspace_record_identifier=workspace.record_identifier,
                listing_identifier=f"ebay:{shared}",
            )
        )
        # A removed PDP is not evidence that a previously observed sale was unsold.
        assert unavailable_page.status.value is ListingStatus.SOLD
        assert unavailable_page.last_sale == updated.last_sale


@pytest.mark.anyio
async def test_completed_search_retry_is_rejected_and_preserves_historical_work(
    tmp_path: Path,
) -> None:
    async def provenance() -> CodeProvenance:
        return _provenance()

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path, code_provenance=provenance)
        group = await app.create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=(
                    EbaySearchTargetSpecification(search=EbaySearchRequest(query="initial Nagler")),
                )
            )
        )
        await _complete(
            database,
            group.targets[0].executions[0].work_identifier,
            records=(
                _record(
                    "initial-run",
                    "search_run",
                    request=EbaySearchRequest(query="initial Nagler").model_dump(mode="json"),
                ),
            ),
            result={"search_run_record_identifier": "initial-run"},
        )
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Retry completed", search_run_record_identifier="initial-run"
            )
        )
        search = EbaySearchRequest(query="Nagler", listing_state="completed")
        enqueued = await database.enqueue_work(
            collect_ebay_search_work(
                identifier="legacy-closed-work",
                payload=CollectEbaySearchPayload(request=search),
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("carl", "mcp", "create_workspace_search"),
                identifier=workspace.record_identifier,
                context={"marketplace": "ebay", "request": search.model_dump(mode="json")},
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        created = CreateWorkspaceSearchResult(
            workspace_record_identifier=workspace.record_identifier,
            track_identifier=enqueued.work_item_identifier,
            work_identifier=enqueued.work_item_identifier,
            created=True,
            state=WorkState.PENDING,
        )
        before = await database.work(created.work_identifier)
        token, operation = str(uuid4()), str(uuid4())
        claim = await database.claim_work(
            supported_capabilities=(
                WorkCapability(
                    kind=tuple(before["kind"]),
                    payload_schema_version=int(before["payload_schema_version"]),
                ),
            ),
            eligible_identifiers=(created.work_identifier,),
            worker_identifier="test-worker",
            lease_token=token,
            lease_duration_ns=60_000_000_000,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None
        await database.begin_leased_operation(
            work_item_identifier=created.work_identifier,
            lease_token=token,
            worker_identifier="test-worker",
            lease_duration_ns=60_000_000_000,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
            operation_id=operation,
            component=Component(ComponentId(("test", "completed-failure")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-29T00:00:00+00:00",
        )
        await database.terminally_fail_leased_operation(
            work_item_identifier=created.work_identifier,
            lease_token=token,
            worker_identifier="test-worker",
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
            operation_id=operation,
            error={
                "kind": "ebay_search_response_failure",
                "classification": "error_page",
                "http_status": 403,
            },
            result={"state": "response_failed"},
            ended_at_utc="2026-09-29T00:00:01+00:00",
            duration_ns=1,
        )
        with pytest.raises(
            ReviewInputError, match="does not presently support eBay sold/completed"
        ):
            await app.retry_workspace_search_track(
                RetryWorkspaceSearchTrackRequest(
                    workspace_record_identifier=workspace.record_identifier,
                    track_identifier=created.track_identifier,
                )
            )
        after = await database.work(created.work_identifier)
        assert after["state"] == WorkState.TERMINAL_FAILURE.value
        assert after["payload"]["request"]["listing_state"] == "completed"
        definitions = tuple(
            collect_ebay_search_work(
                identifier=created.work_identifier,
                payload=CollectEbaySearchPayload.model_validate_json(encode_json(work["payload"])),
                not_before_utc_ns=0,
            )
            for work in (before, after)
        )
        assert definitions[0].deduplication_identity == definitions[1].deduplication_identity
        refreshed_workspace = await app.get_review_workspace(workspace.record_identifier)
        track = next(
            track
            for track in refreshed_workspace.search_tracks
            if track.track_identifier == created.track_identifier
        )
        assert track.listing_state == "completed"


@pytest.mark.anyio
async def test_unavailable_after_newer_active_detail_does_not_resurrect_old_sale(
    tmp_path: Path,
) -> None:
    item = "256123456789"
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                _record("sale-acquisition", "acquisition"),
                _record(
                    "sold-run",
                    "search_run",
                    request=EbaySearchRequest(query="Nagler", listing_state="sold").model_dump(
                        mode="json"
                    ),
                ),
                _record(
                    "sold-card",
                    "search_listing_occurrence",
                    item_identifier=item,
                    search_run_record_identifier="sold-run",
                    acquisition_record_identifier="sale-acquisition",
                    listing_state="sold",
                    title="Nagler",
                    displayed_price="$180.00",
                    sold_price="$180.00",
                    sold_date="2026-09-29",
                    sold_price_status="displayed",
                ),
            ),
        )
        app = ReviewApplication(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Sale history", search_run_record_identifier="sold-run"
            )
        )
        request = GetWorkspaceListingRequest(
            workspace_record_identifier=workspace.record_identifier,
            listing_identifier=f"ebay:{item}",
        )
        sold = await app.get_workspace_listing(request)
        assert sold.status.value is ListingStatus.SOLD
        assert sold.last_sale is not None
        await _publish(
            database,
            (
                _record("active-acquisition", "acquisition"),
                _record(
                    "newer-active-detail",
                    "listing_observation",
                    item_identifier=item,
                    acquisition_record_identifier="active-acquisition",
                    classification="detail",
                    title="Nagler",
                    displayed_price="$220.00",
                    currency="USD",
                ),
            ),
        )
        active = await app.get_workspace_listing(request)
        assert active.status.value is ListingStatus.AVAILABLE
        assert active.last_sale == sold.last_sale
        await _publish(
            database,
            (
                _record("unavailable-acquisition", "acquisition"),
                _record(
                    "latest-unavailable-detail",
                    "listing_observation",
                    item_identifier=item,
                    acquisition_record_identifier="unavailable-acquisition",
                    classification="unavailable",
                ),
            ),
        )
        unavailable = await app.get_workspace_listing(request)
        assert unavailable.status.value is ListingStatus.UNAVAILABLE
        assert unavailable.last_sale == sold.last_sale
