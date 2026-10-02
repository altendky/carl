"""Semantic review staleness across refreshes using retained temporary evidence."""

import json
import sqlite3
from pathlib import Path
from typing import cast

import pytest

from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_review_workspace import _provenance, _search_run_value
from carl.core.composed_projection import ScalarFieldValues, scalar_fields_sha256
from carl.core.facebook_work import CreateSearchRequest
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    FacebookSearchTargetSpecification,
)
from carl.core.models import CodeProvenance, RecordDraft
from carl.core.review_workspace import (
    AcquireReviewBatchRequest,
    CreateReviewBatchRequest,
    CreateReviewWorkspaceRequest,
    GetWorkspaceListingRequest,
    ListingReviewInput,
    RecordListingReviewsRequest,
    RecordWorkspaceBulkReviewRequest,
    ReviewDisposition,
    ReviewState,
)
from carl.io.sqlite import Database
from carl.review import ReviewApplication

LISTING = "1001137228964859"


async def _search(
    app: ReviewApplication,
    database: Database,
    name: str,
    *,
    amount: str = "20.00",
    currency: str | None = "USD",
    formatted: str | None = None,
    title: str = "Astronomy textbook",
) -> str:
    run = _search_run_value(name)
    search = CreateSearchRequest.model_validate(
        {"request": run["request"], "traversal": {"maximum_pages": 1}}
    )
    group = await app.create_marketplace_search(
        CreateMarketplaceSearchRequest(targets=(FacebookSearchTargetSpecification(search=search),))
    )
    run["search_run_identifier"] = name
    run["traversal"] = {
        **cast(dict[str, object], run["traversal"]),
        "unique_listing_identifiers": [LISTING],
    }
    price = {"amount": amount}
    if currency is not None:
        price["currency"] = currency
    if formatted is not None:
        price["formatted_amount"] = formatted
    await _complete(
        database,
        group.targets[0].executions[0].work_identifier,
        records=(
            RecordDraft(
                identifier=f"{name}-run",
                kind=("carl", "facebook", "search_run"),
                schema_version=1,
                value=run,
            ),
            RecordDraft(
                identifier=f"{name}-card",
                kind=("carl", "facebook", "search_listing_occurrence"),
                schema_version=1,
                value={
                    "search_run_identifier": name,
                    "acquisition_record_identifier": f"{name}-acquisition",
                    "listing_identifier": LISTING,
                    "page_ordinal": 0,
                    "edge_index": 0,
                    "original": {
                        "marketplace_listing_title": title,
                        "listing_price": price,
                        "location_text": "Germantown, MD",
                        "redacted_description": "Good",
                        "is_live": True,
                    },
                },
            ),
        ),
        result={"search_run_record_identifier": f"{name}-run"},
    )
    return group.record_identifier


async def _app(database: Database, path: Path) -> tuple[ReviewApplication, str]:
    async def provenance() -> CodeProvenance:
        return _provenance()

    app = ReviewApplication(database, path.parent, code_provenance=provenance)
    group = await _search(app, database, "initial")
    workspace = await app.create_review_workspace(
        CreateReviewWorkspaceRequest(name="Scalar reviews", search_run_record_identifier=group)
    )
    return app, workspace.record_identifier


async def _bulk(app: ReviewApplication, workspace: str) -> str:
    result = await app.record_workspace_bulk_review(
        RecordWorkspaceBulkReviewRequest(
            request_identifier="initial-review",
            workspace_record_identifier=workspace,
            disposition=ReviewDisposition.REJECTED,
        )
    )
    assert result.recorded_count == 1
    records = await app.database.latest_listing_reviews(
        workspace_record_identifier=workspace, listing_identifiers=(LISTING,)
    )
    assert records[LISTING].scalar_fields_snapshot == ScalarFieldValues(
        title="Astronomy textbook",
        price={"amount_decimal": "20", "currency": "USD"},
        location="Germantown, MD",
        description="Good",
    )
    return records[LISTING].record_identifier


def _legacy_review(path: Path, identifier: str, *, invalid_hash: bool = False) -> None:
    """Simulate a pre-upgrade review without rewriting its immutable mutation boundary."""
    with sqlite3.connect(path) as connection:
        value = json.loads(
            connection.execute(
                "SELECT value_json FROM records WHERE object_id = ?", (identifier,)
            ).fetchone()[0]
        )
        value.pop("scalar_fields_snapshot")
        revision = value["projection_revision"]
        revision["recipe_version"] = 1
        revision["scalar_fields_sha256"] = (
            "0" * 64
            if invalid_hash
            else scalar_fields_sha256(
                ScalarFieldValues(
                    title="Astronomy textbook",
                    price={"amount_decimal": "20.00", "currency": "USD"},
                    location="Germantown, MD",
                    description="Good",
                ),
                normalize=False,
            )
        )
        connection.execute(
            "UPDATE records SET value_json = ? WHERE object_id = ?",
            (json.dumps(value), identifier),
        )


@pytest.mark.anyio
@pytest.mark.parametrize("legacy", [False, True])
async def test_refresh_equivalent_price_stays_current(tmp_path: Path, legacy: bool) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        app, workspace = await _app(database, path)
        identifier = await _bulk(app, workspace)
        if legacy:
            _legacy_review(path, identifier)
            boundaries = await database.listing_review_scalar_boundaries((identifier,))
            assert identifier in boundaries
        await _search(app, database, "refresh", amount="20.000", currency=None, formatted="$20")
        listing = await app.get_workspace_listing(
            GetWorkspaceListingRequest(
                workspace_record_identifier=workspace, listing_identifier=LISTING
            )
        )
        assert listing.review_state == ReviewState.CURRENT
        assert listing.prior_review is not None
        assert listing.prior_review.record_identifier == identifier
        assert listing.scalar_comparison_available
        assert listing.scalar_field_changes == ()
        assert listing.changed_components == ()
        batch = await app.create_review_batch(
            CreateReviewBatchRequest(workspace_record_identifier=workspace)
        )
        assert batch.items == ()
        current = await app.create_review_batch(
            CreateReviewBatchRequest(
                workspace_record_identifier=workspace,
                include_review_states=(ReviewState.CURRENT,),
            )
        )
        assert len(current.items) == 1
        assert current.items[0].scalar_comparison_available
        assert current.items[0].scalar_field_changes == ()
        repeat = await app.record_workspace_bulk_review(
            RecordWorkspaceBulkReviewRequest(
                request_identifier="refresh-review",
                workspace_record_identifier=workspace,
                include_review_states=(ReviewState.UNREVIEWED, ReviewState.STALE),
                disposition=ReviewDisposition.REJECTED,
            )
        )
        assert repeat.recorded_count == 0
        assert repeat.review_state_excluded_count == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("changes", "field", "previous", "current"),
    [
        (
            {"amount": "25.00"},
            "price",
            {"amount_decimal": "20", "currency": "USD"},
            {"amount_decimal": "25", "currency": "USD"},
        ),
        (
            {"currency": "CAD"},
            "price",
            {"amount_decimal": "20", "currency": "USD"},
            {"amount_decimal": "20", "currency": "CAD"},
        ),
        (
            {"title": "Revised astronomy textbook"},
            "title",
            "Astronomy textbook",
            "Revised astronomy textbook",
        ),
    ],
)
async def test_real_scalar_change_exposes_old_and_new(
    tmp_path: Path, changes: dict[str, str], field: str, previous: object, current: object
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        app, workspace = await _app(database, path)
        identifier = await _bulk(app, workspace)
        _legacy_review(path, identifier)
        await _search(app, database, "refresh", **changes)
        listing = await app.get_workspace_listing(
            GetWorkspaceListingRequest(
                workspace_record_identifier=workspace, listing_identifier=LISTING
            )
        )
        assert listing.review_state == ReviewState.STALE
        assert listing.scalar_comparison_available
        assert len(listing.scalar_field_changes) == 1
        change = listing.scalar_field_changes[0]
        assert change.field == field
        assert change.previous_value == previous
        assert change.current_value == current
        batch = await app.create_review_batch(
            CreateReviewBatchRequest(workspace_record_identifier=workspace)
        )
        assert batch.items[0].scalar_field_changes == listing.scalar_field_changes
        assert batch.items[0].scalar_comparison_available


@pytest.mark.anyio
@pytest.mark.parametrize("missing_boundary", [False, True])
async def test_unverifiable_legacy_baseline_is_conservative(
    tmp_path: Path, missing_boundary: bool
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        app, workspace = await _app(database, path)
        identifier = await _bulk(app, workspace)
        _legacy_review(path, identifier, invalid_hash=not missing_boundary)
        if missing_boundary:
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "DELETE FROM review_mutation_requests WHERE action = ?",
                    ("record_workspace_bulk_review",),
                )
        listing = await app.get_workspace_listing(
            GetWorkspaceListingRequest(
                workspace_record_identifier=workspace, listing_identifier=LISTING
            )
        )
        assert listing.review_state == ReviewState.STALE
        assert not listing.scalar_comparison_available
        assert listing.scalar_field_changes == ()


@pytest.mark.anyio
@pytest.mark.parametrize("claimed", [False, True])
async def test_individual_reviews_save_scalar_snapshot(tmp_path: Path, claimed: bool) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        app, workspace = await _app(database, path)
        listing = await app.get_workspace_listing(
            GetWorkspaceListingRequest(
                workspace_record_identifier=workspace, listing_identifier=LISTING
            )
        )
        acquired = (
            await app.acquire_review_batch(
                AcquireReviewBatchRequest(
                    request_identifier="claim",
                    owner_identifier="reviewer",
                    workspace_record_identifier=workspace,
                )
            )
            if claimed
            else None
        )
        claim_token = None
        if acquired is not None:
            assert acquired.lease is not None
            claim_token = acquired.lease.claim_token
        result = await app.record_listing_reviews(
            RecordListingReviewsRequest(
                request_identifier="review",
                workspace_record_identifier=workspace,
                batch_record_identifier=None
                if acquired is None
                else acquired.batch.record_identifier,
                claim_token=claim_token,
                claim_owner_identifier="reviewer" if claimed else None,
                reviews=(
                    ListingReviewInput(
                        listing_identifier=LISTING,
                        projection_revision=listing.projection_revision,
                        disposition=ReviewDisposition.REJECTED,
                    ),
                ),
            )
        )
        assert result.records[0].scalar_fields_snapshot is not None
        assert result.records[0].scalar_fields_snapshot.price == {
            "amount_decimal": "20",
            "currency": "USD",
        }
        stored = await database.latest_listing_reviews(
            workspace_record_identifier=workspace, listing_identifiers=(LISTING,)
        )
        assert stored[LISTING].scalar_fields_snapshot == result.records[0].scalar_fields_snapshot


@pytest.mark.anyio
async def test_legacy_batch_snapshot_normalizes_without_bulk_boundary(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        app, workspace = await _app(database, path)
        batch = await app.create_review_batch(
            CreateReviewBatchRequest(workspace_record_identifier=workspace)
        )
        result = await app.record_listing_reviews(
            RecordListingReviewsRequest(
                request_identifier="review",
                workspace_record_identifier=workspace,
                batch_record_identifier=batch.record_identifier,
                reviews=(
                    ListingReviewInput(
                        listing_identifier=LISTING,
                        projection_revision=batch.items[0].projection.projection_revision,
                        disposition=ReviewDisposition.REJECTED,
                    ),
                ),
            )
        )
        identifier = result.records[0].record_identifier
        _legacy_review(path, identifier)
        with sqlite3.connect(path) as connection:
            review = json.loads(
                connection.execute(
                    "SELECT value_json FROM records WHERE object_id = ?", (identifier,)
                ).fetchone()[0]
            )
            frozen = json.loads(
                connection.execute(
                    "SELECT value_json FROM records WHERE object_id = ?",
                    (batch.record_identifier,),
                ).fetchone()[0]
            )
            old_projection = frozen["items"][0]["projection"]
            old_projection["projection_revision"] = review["projection_revision"]
            old_projection["price"]["value"] = {"amount_decimal": "20.00", "currency": "USD"}
            connection.execute(
                "UPDATE records SET value_json = ? WHERE object_id = ?",
                (json.dumps(frozen), batch.record_identifier),
            )
        assert await database.listing_review_scalar_boundaries((identifier,)) == {}
        await _search(app, database, "refresh", currency=None, formatted="$20")
        listing = await app.get_workspace_listing(
            GetWorkspaceListingRequest(
                workspace_record_identifier=workspace, listing_identifier=LISTING
            )
        )
        assert listing.review_state == ReviewState.CURRENT
        assert listing.scalar_comparison_available
        assert listing.scalar_field_changes == ()
