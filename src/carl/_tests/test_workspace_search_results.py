"""Track-scoped comparables use retained cards, not full listing projections."""

import json
import sqlite3
from datetime import date
from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest

from carl._tests.test_marketplace_listing import _publish, _record
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_review_workspace import _provenance, _search_run_value
from carl.core.components import Component, ComponentId
from carl.core.composed_projection import ScalarFieldValues, scalar_fields_sha256
from carl.core.ebay import EbaySearchRequest
from carl.core.ebay_refresh import RefreshEbaySearchPayload, refresh_ebay_search_work
from carl.core.facebook_work import CreateSearchRequest
from carl.core.json import encode_json
from carl.core.marketplace_search import EbaySearchTargetSpecification, Marketplace
from carl.core.models import CodeProvenance, RecordDraft
from carl.core.review_workspace import (
    CreateReviewWorkspaceRequest,
    CreateWorkspaceSearchRequest,
    GetWorkspaceListingRequest,
    ListingReviewInput,
    RecordListingReviewsRequest,
    ReviewState,
)
from carl.core.work import WorkCapability, WorkRequester
from carl.core.workspace_search_results import ListWorkspaceSearchResultsRequest
from carl.io.sqlite import Database
from carl.review import ReviewApplication, ReviewInputError


def _card(identifier: str, item: str, *, run: str = "sold-run", **fields: object) -> RecordDraft:
    value: dict[str, object] = {
        "item_identifier": item,
        "search_run_record_identifier": run,
        "listing_state": "sold",
        "displayed_price": "$180.00",
        "sold_price": "$180.00",
        "sold_date": "2026-09-29",
        "sold_date_text": "Sep 29, 2026",
        "sold_price_status": "displayed",
    }
    return _record(identifier, "search_listing_occurrence", **(value | fields))


async def _workspace(
    database: Database, path: Path, cards: tuple[RecordDraft, ...]
) -> tuple[ReviewApplication, str]:
    async def provenance() -> CodeProvenance:
        return _provenance()

    await _publish(
        database,
        (
            _record(
                "sold-run",
                "search_run",
                request=EbaySearchRequest(query="Nagler", listing_state="sold").model_dump(
                    mode="json"
                ),
            ),
            *cards,
        ),
    )
    app = ReviewApplication(database, path, code_provenance=provenance)
    workspace = await app.create_review_workspace(
        CreateReviewWorkspaceRequest(name="Astronomy", search_run_record_identifier="sold-run")
    )
    return app, workspace.record_identifier


@pytest.mark.anyio
async def test_retained_error_page_is_not_an_empty_success(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                _record(
                    "failed-run",
                    "search_run",
                    request={"query": "Nagler", "listing_state": "sold"},
                    stopping_reason="error_page",
                    pages=[
                        {
                            "response_classification": {
                                "kind": "error_page",
                                "evidence": ["http_status_403"],
                            }
                        }
                    ],
                ),
            ),
        )
        app = ReviewApplication(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Failed sold",
                search_run_record_identifier="failed-run",
            )
        )
        page = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier="failed-run",
                listing_state="sold",
            )
        )
        assert page.results == () and page.collection_succeeded is False
        assert page.response_classification is not None
        assert page.response_classification.evidence == ("http_status_403",)
        assert page.stopping_reason == "error_page"
        assert "source_search_collection_failed" in page.warnings


@pytest.mark.anyio
async def test_legacy_failed_run_without_pages_is_flagged(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                _record(
                    "legacy-run",
                    "search_run",
                    request={"query": "Nagler", "listing_state": "sold"},
                    stopping_reason="unrecognized_response",
                ),
            ),
        )
        app = ReviewApplication(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Legacy sold",
                search_run_record_identifier="legacy-run",
            )
        )
        page = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier="legacy-run",
            )
        )
        assert page.collection_succeeded is False and page.response_classification is None
        assert "source_search_collection_failed" in page.warnings


@pytest.mark.anyio
async def test_sold_comparables_deduplicate_cards_without_full_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("A cheap card read must not compose or acquire a listing")

    monkeypatch.setattr(ReviewApplication, "get_composed_listing", forbidden)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app, workspace = await _workspace(
            database,
            tmp_path,
            (
                _card("first", "256123456789", title="Old title", page_ordinal=0, position=1),
                _card(
                    "second",
                    "256123456789",
                    title="Tele Vue Nagler 13mm",
                    condition="Pre-Owned",
                    shipping_text="+$8.00 shipping",
                    page_ordinal=1,
                    position=3,
                ),
                _card("foreign", "256123456780", run="other-run", title="Not this track"),
            ),
        )
        page = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace,
                track_identifier="sold-run",
                listing_state="sold",
            )
        )
        assert page.total_distinct_results == 1
        assert page.source_search_run_record_identifier == "sold-run"
        result = page.results[0]
        assert result.marketplace is Marketplace.EBAY
        assert result.listing_identifier == "ebay:256123456789"
        assert result.title == "Tele Vue Nagler 13mm"
        assert result.condition == "Pre-Owned"
        assert result.shipping_text == "+$8.00 shipping"
        assert result.price is None  # Sale evidence is not an asking price.
        assert result.last_sale["sold_price_value"]["amount_decimal"] == "180.00"
        assert result.last_sale["sold_price_value"]["currency"] == "USD"
        assert result.occurrences[0].sold_date == date(2026, 9, 29)
        assert [item.occurrence_record_identifier for item in result.occurrences] == [
            "second",
            "first",
        ]
        assert page.next_cursor is None
        assert page.warnings == ()
        active = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace,
                track_identifier="sold-run",
                listing_state="active",
            )
        )
        assert active.results == ()


@pytest.mark.anyio
async def test_facebook_cards_have_same_track_scoped_read_path(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app, workspace = await _workspace(database, tmp_path, ())
        created = await app.create_workspace_search(
            CreateWorkspaceSearchRequest(
                workspace_record_identifier=workspace,
                search=CreateSearchRequest.model_validate_json(
                    encode_json(
                        {
                            "request": _search_run_value("Nagler")["request"],
                            "traversal": {"maximum_pages": 1},
                        }
                    )
                ),
            )
        )
        run = _search_run_value("Nagler") | {"search_run_identifier": "fb-internal"}
        await _complete(
            database,
            created.work_identifier,
            records=(
                RecordDraft(
                    identifier="fb-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value=run,
                ),
                RecordDraft(
                    identifier="fb-card",
                    kind=("carl", "facebook", "search_listing_occurrence"),
                    schema_version=1,
                    value={
                        "search_run_identifier": "fb-internal",
                        "listing_identifier": "1001137228964859",
                        "edge_index": 0,
                        "original": {
                            "marketplace_listing_title": "Tele Vue Nagler",
                            "is_live": True,
                            "listing_price": {
                                "amount": "120.00",
                                "currency": "USD",
                                "formatted_amount": "$120",
                            },
                        },
                    },
                ),
            ),
            result={"search_run_record_identifier": "fb-run"},
        )
        page = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace, track_identifier=created.track_identifier
            )
        )
        assert page.total_distinct_results == 1
        assert page.results[0].marketplace is Marketplace.FACEBOOK
        assert page.results[0].listing_identifier == "1001137228964859"
        assert page.results[0].price["amount_decimal"] == "120.00"
        assert page.results[0].listing_state == "active"
        assert page.results[0].last_sale is None


@pytest.mark.anyio
async def test_failed_track_returns_empty_page_with_failure_warning(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app, workspace = await _workspace(database, tmp_path, ())
        created = await app.create_workspace_search(
            CreateWorkspaceSearchRequest(
                workspace_record_identifier=workspace,
                search=EbaySearchTargetSpecification(
                    search=EbaySearchRequest(query="blocked Nagler")
                ),
            )
        )
        work = await database.work(created.work_identifier)
        token, operation = str(uuid4()), str(uuid4())
        claim = await database.claim_work(
            supported_capabilities=(
                WorkCapability(
                    kind=tuple(work["kind"]),
                    payload_schema_version=int(work["payload_schema_version"]),
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
            component=Component(ComponentId(("test", "card-failure")), 1, lambda: None),
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
        page = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace, track_identifier=created.track_identifier
            )
        )
        assert page.results == ()
        assert page.source_search_run_record_identifier is None
        assert "track_creation_not_completed" in page.warnings


@pytest.mark.anyio
async def test_active_card_price_and_accepted_offer_are_not_invented_sale_prices(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app, workspace = await _workspace(
            database,
            tmp_path,
            (
                _card(
                    "active",
                    "256123456780",
                    listing_state="active",
                    displayed_price="$209.72",
                    title="ES 24mm 68°",
                    condition="Open Box",
                    shipping_text="Free shipping",
                ),
                _card(
                    "offer",
                    "256123456781",
                    sold_price=None,
                    sold_price_status="best_offer_accepted",
                    title="Tele Vue Nagler",
                ),
            ),
        )
        page = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace, track_identifier="sold-run"
            )
        )
        active, sold = page.results
        assert active.price == {
            "amount_decimal": "209.72",
            "currency": "USD",
            "formatted_amount": "$209.72",
        }
        assert active.condition == "Open Box"
        assert active.shipping_text == "Free shipping"
        assert active.last_sale is None
        assert sold.price is None
        assert sold.last_sale["sold_price"] is None
        assert sold.last_sale["sold_price_value"] is None
        assert sold.last_sale["sold_price_status"] == "best_offer_accepted"


@pytest.mark.anyio
async def test_old_recipe_two_ebay_string_price_review_remains_current(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    listing_identifier = "ebay:256123456780"
    async with Database.managed(path, initialize=True) as database:
        app, workspace = await _workspace(
            database,
            tmp_path,
            (
                _card(
                    "active",
                    "256123456780",
                    listing_state="active",
                    displayed_price="$209.72",
                    title="ES 24mm 68°",
                ),
            ),
        )
        request = GetWorkspaceListingRequest(
            workspace_record_identifier=workspace, listing_identifier=listing_identifier
        )
        listing = await app.get_workspace_listing(request)
        recorded = await app.record_listing_reviews(
            RecordListingReviewsRequest(
                request_identifier="review-price",
                workspace_record_identifier=workspace,
                reviews=(
                    ListingReviewInput(
                        listing_identifier=listing_identifier,
                        projection_revision=listing.projection_revision,
                    ),
                ),
            )
        )
        identifier = recorded.records[0].record_identifier
        # Simulate the previous recipe-2 output in this temporary database only.
        with sqlite3.connect(path) as connection:
            stored = json.loads(
                connection.execute(
                    "SELECT value_json FROM records WHERE object_id=?", (identifier,)
                ).fetchone()[0]
            )
            stored["scalar_fields_snapshot"]["price"] = "$209.72"
            stored["projection_revision"]["scalar_fields_sha256"] = scalar_fields_sha256(
                ScalarFieldValues.model_validate(stored["scalar_fields_snapshot"])
            )
            connection.execute(
                "UPDATE records SET value_json=? WHERE object_id=?",
                (json.dumps(stored), identifier),
            )
        unchanged = await app.get_workspace_listing(request)
        assert unchanged.review_state is ReviewState.CURRENT
        assert unchanged.scalar_comparison_available
        assert unchanged.scalar_field_changes == ()
        await _publish(
            database,
            (
                _card(
                    "changed",
                    "256123456780",
                    listing_state="active",
                    displayed_price="$220.00",
                    title="ES 24mm 68°",
                ),
            ),
        )
        changed = await app.get_workspace_listing(request)
        assert changed.review_state is ReviewState.STALE
        assert len(changed.scalar_field_changes) == 1
        assert changed.scalar_field_changes[0].field == "price"
        assert changed.scalar_field_changes[0].previous_value == {
            "amount_decimal": "209.72",
            "currency": "USD",
        }
        assert changed.scalar_field_changes[0].current_value == {
            "amount_decimal": "220",
            "currency": "USD",
        }


@pytest.mark.anyio
async def test_cursor_freezes_old_track_run_after_refresh_and_allows_new_page_size(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app, workspace = await _workspace(
            database,
            tmp_path,
            tuple(
                _card(f"card-{index}", f"25612345678{index}", title="Nagler") for index in range(3)
            ),
        )
        first = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace, track_identifier="sold-run", page_size=1
            )
        )
        assert first.next_cursor is not None
        # Load a pre-disable historical refresh without invoking acquisition ingress.
        refresh = await database.enqueue_work(
            refresh_ebay_search_work(
                identifier="historical-refresh",
                payload=RefreshEbaySearchPayload(
                    base_search_run_record_identifier="sold-run",
                    search_work_identifier="historical-refresh-child",
                    search=EbaySearchRequest(query="Nagler", listing_state="sold"),
                ),
            ),
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("carl", "mcp", "request_workspace_refresh"),
                identifier=workspace,
                context={"track_identifier": "sold-run"},
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        await _complete(
            database,
            refresh.work_item_identifier,
            records=(
                _record(
                    "new-run",
                    "search_run",
                    request=EbaySearchRequest(query="Nagler", listing_state="sold").model_dump(
                        mode="json"
                    ),
                ),
                _card("new-card", "256123456789", run="new-run", title="New run"),
                _card("late-old-card", "256123456785", title="Added after snapshot"),
            ),
            result={"state": "completed", "refreshed_search_run_record_identifier": "new-run"},
        )
        continued = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace,
                track_identifier="sold-run",
                page_size=2,
                cursor=first.next_cursor,
            )
        )
        assert continued.source_search_run_record_identifier == "sold-run"
        assert continued.as_of_completion_sequence == first.as_of_completion_sequence
        assert continued.total_distinct_results == 3
        assert [item.listing_identifier for item in continued.results] == [
            "ebay:256123456781",
            "ebay:256123456782",
        ]
        assert continued.next_cursor is None
        fresh = await app.list_workspace_search_results(
            ListWorkspaceSearchResultsRequest(
                workspace_record_identifier=workspace, track_identifier="sold-run"
            )
        )
        assert fresh.source_search_run_record_identifier == "new-run"
        assert [item.title for item in fresh.results] == ["New run"]


@pytest.mark.anyio
async def test_title_filters_and_cursor_scope_rejection(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app, workspace = await _workspace(
            database,
            tmp_path,
            tuple(
                _card(f"card-{index}", f"25612345678{index}", title=title)
                for index, title in enumerate(
                    (
                        "Tele Vue NAGLER 13mm",
                        "Tele Vue Nagler 9mm",
                        "Nagler LEGO model",
                        "Eye cream",
                    )
                )
            ),
        )
        request = ListWorkspaceSearchResultsRequest(
            workspace_record_identifier=workspace,
            track_identifier="sold-run",
            page_size=1,
            required_title_keywords=("tele vue", "nagler"),
            excluded_title_keywords=("lego",),
        )
        page = await app.list_workspace_search_results(request)
        assert page.total_distinct_results == 2
        assert page.next_cursor is not None
        negative_filtered = await app.list_workspace_search_results(
            request.model_copy(
                update={
                    "page_size": 100,
                    "required_title_keywords": ("nagler",),
                    "excluded_title_keywords": ("LEGO",),
                }
            )
        )
        assert negative_filtered.total_distinct_results == 2
        assert all("LEGO" not in (item.title or "") for item in negative_filtered.results)
        with pytest.raises(ReviewInputError, match="another track or filter"):
            await app.list_workspace_search_results(
                request.model_copy(
                    update={"cursor": page.next_cursor, "excluded_title_keywords": ("cream",)}
                )
            )
        another = await app.create_workspace_search(
            CreateWorkspaceSearchRequest(
                workspace_record_identifier=workspace,
                search=EbaySearchTargetSpecification(search=EbaySearchRequest(query="other track")),
            )
        )
        with pytest.raises(ReviewInputError, match="another track or filter"):
            await app.list_workspace_search_results(
                request.model_copy(
                    update={
                        "cursor": page.next_cursor,
                        "track_identifier": another.track_identifier,
                    }
                )
            )
        with pytest.raises(ReviewInputError, match="does not belong"):
            await app.list_workspace_search_results(
                request.model_copy(update={"track_identifier": "not-this-workspace"})
            )
