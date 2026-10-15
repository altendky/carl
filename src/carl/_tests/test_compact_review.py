"""Compact review responses preserve durable review and claim information."""

import hashlib
import json
from pathlib import Path
from typing import cast

import pytest
from mcp import Client

from carl.core.compact_review import (
    CompactReviewBatchAcquisition,
    compact_review_acquisition,
    compact_workspace_listing,
)
from carl.core.components import Component, ComponentId
from carl.core.composed_projection import (
    ComposedField,
    ComposedListingPage,
    ComposedListingProjection,
    ComposedStatus,
    GetComposedListingRequest,
    ListComposedSearchRequest,
    ListingStatus,
    ProjectionEvidence,
    ProjectionRevision,
    ProjectionSourceKind,
    ScalarFieldChange,
    ScalarFieldName,
    ScalarFieldValues,
)
from carl.core.models import CodeProvenance, JsonValue, NamedOutput, RecordDraft
from carl.core.review_workspace import (
    AcquireReviewBatchRequest,
    CreateReviewWorkspaceRequest,
    ListingReviewInput,
    ListingReviewRecord,
    ProjectionRevisionComponent,
    RecordListingReviewsRequest,
    ReviewBatch,
    ReviewBatchAcquisition,
    ReviewBatchItem,
    ReviewClaimLease,
    ReviewState,
    WorkspaceListingReview,
    review_mutation_request_sha256,
)
from carl.io.sqlite import Database
from carl.mcp_server import build_server
from carl.review import ReviewApplication


def _projection(identifier: str, seller: JsonValue) -> ComposedListingProjection:
    evidence = ProjectionEvidence(
        evidence_record_identifier=f"observation-{identifier}",
        acquisition_completion_sequence=1,
        observation_completion_sequence=1,
        source_kind=ProjectionSourceKind.ITEM_PAGE,
    )
    revision = ProjectionRevision(
        recipe_version=2,
        status_sha256="a" * 64,
        scalar_fields_sha256="b" * 64,
        preview_image_sha256="c" * 64,
        gallery_sha256="d" * 64,
        analyses_sha256="e" * 64,
        search_membership_sha256="f" * 64,
        aggregate_sha256="0" * 64,
    )
    return ComposedListingProjection(
        listing_identifier=identifier,
        canonical_source_url=f"https://www.facebook.com/marketplace/item/{identifier}/",
        as_of_completion_sequence=1,
        projection_revision=revision,
        status=ComposedStatus(
            value=ListingStatus.AVAILABLE, raw_flags={"is_live": True}, evidence=evidence
        ),
        title=ComposedField(value="Telescope", evidence=evidence),
        price=None,
        location=None,
        description=None,
        seller=ComposedField(value=seller, evidence=evidence),
        preview_image=None,
        gallery=None,
        analyses=(),
        analyses_truncated=False,
        search_membership=None,
    )


def _prior(projection: ComposedListingProjection, seller: JsonValue) -> ListingReviewRecord:
    return ListingReviewRecord(
        record_identifier=f"prior-{projection.listing_identifier}",
        workspace_record_identifier="workspace",
        batch_record_identifier="prior-batch",
        listing_identifier=projection.listing_identifier,
        projection_revision=projection.projection_revision,
        inspected=True,
        disposition=None,
        recorded_at_utc="2026-10-13T00:00:00+00:00",
        scalar_fields_snapshot=ScalarFieldValues(title="Telescope", seller=seller),
    )


def _acquisition(items: tuple[ReviewBatchItem, ...]) -> ReviewBatchAcquisition:
    return ReviewBatchAcquisition(
        batch=ReviewBatch(
            record_identifier="batch",
            workspace_record_identifier="workspace",
            created_at_utc="2026-10-13T00:00:00+00:00",
            items=items,
            next_cursor="continuation",
            scanned_page_count=2,
        ),
        lease=ReviewClaimLease(
            claim_token="claim",
            owner_identifier="review-agent",
            workspace_record_identifier="workspace",
            batch_record_identifier="batch",
            acquired_at_utc_ns=1,
            lease_expires_at_utc_ns=601,
            listing_identifiers=tuple(item.projection.listing_identifier for item in items),
        ),
    )


def test_compact_batch_deduplicates_sellers_and_preserves_review_identity() -> None:
    seller: JsonValue = {"profile": {"name": "Seller", "biography": "A" * 50_000}}
    items = tuple(
        ReviewBatchItem(
            projection=(projection := _projection(str(123 + index), seller)),
            review_state=ReviewState.CURRENT,
            prior_review=_prior(projection, seller),
            changed_components=(),
            scalar_comparison_available=True,
        )
        for index in range(20)
    )
    acquisition = _acquisition(items)
    full_before = acquisition.model_dump_json()
    compact = compact_review_acquisition(acquisition)

    assert len(compact.sellers) == 1
    seller_reference = next(iter(compact.sellers))
    assert compact.sellers[seller_reference] == seller
    assert compact.lease == acquisition.lease
    assert compact.batch.record_identifier == acquisition.batch.record_identifier
    assert compact.batch.next_cursor == acquisition.batch.next_cursor
    assert compact.batch.scanned_page_count == acquisition.batch.scanned_page_count
    for original, reduced in zip(items, compact.batch.items, strict=True):
        assert reduced.projection.seller_reference == seller_reference
        assert reduced.projection.projection_revision == original.projection.projection_revision
        assert reduced.projection.projection_revision_sha256 == "0" * 64
        assert reduced.prior_review is not None
        assert original.prior_review is not None
        assert reduced.prior_review.scalar_fields_snapshot is None
        assert reduced.prior_review.record_identifier == original.prior_review.record_identifier
        assert reduced.review_state is original.review_state
    assert len(compact.model_dump_json()) < len(full_before) // 10
    assert acquisition.model_dump_json() == full_before


def test_compact_listing_interns_seller_changes_and_omits_prior_snapshot() -> None:
    previous: JsonValue = {"id": "seller", "name": "Previous"}
    current: JsonValue = {"name": "Current", "id": "seller"}
    projection = _projection("123", current)
    prior = _prior(projection, previous)
    changes = (
        ScalarFieldChange(
            field=ScalarFieldName.SELLER, previous_value=previous, current_value=current
        ),
        ScalarFieldChange(field=ScalarFieldName.PRICE, previous_value=500, current_value=600),
    )
    listing = WorkspaceListingReview.model_validate(
        {
            **projection.model_dump(mode="python"),
            "review_state": ReviewState.STALE,
            "prior_review": prior,
            "changed_components": (ProjectionRevisionComponent.SCALAR_FIELDS,),
            "scalar_field_changes": changes,
            "scalar_comparison_available": True,
        }
    )
    before = listing.model_dump_json()
    compact = compact_workspace_listing(listing)
    reduced = compact.listing

    assert len(compact.sellers) == 2
    seller_change = reduced.scalar_field_changes[0]
    assert isinstance(seller_change.previous_value, dict)
    assert isinstance(seller_change.current_value, dict)
    previous_reference = cast(dict[str, JsonValue], seller_change.previous_value)[
        "seller_reference"
    ]
    current_reference = cast(dict[str, JsonValue], seller_change.current_value)["seller_reference"]
    assert isinstance(previous_reference, str)
    assert isinstance(current_reference, str)
    assert compact.sellers[previous_reference] == previous
    assert compact.sellers[current_reference] == current
    assert reduced.projection.seller_reference == current_reference
    assert reduced.scalar_field_changes[1] == changes[1]
    assert reduced.changed_components == listing.changed_components
    assert reduced.scalar_comparison_available
    assert reduced.prior_review is not None
    assert reduced.prior_review.scalar_fields_snapshot is None
    assert reduced.projection.projection_revision == projection.projection_revision
    assert listing.model_dump_json() == before


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="unknown",
        package_version="test",
        python_implementation="CPython",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


def test_default_compact_flag_preserves_existing_acquisition_replay_hash() -> None:
    request = AcquireReviewBatchRequest(
        workspace_record_identifier="workspace",
        request_identifier="existing-request",
        owner_identifier="owner",
    )
    legacy_request = request.model_dump(mode="json", exclude={"request_identifier", "compact"})
    legacy_content = {
        "action": "acquire_batch",
        "request_schema_version": 1,
        "request": legacy_request,
    }
    legacy_hash = hashlib.sha256(
        json.dumps(
            legacy_content,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()
    assert review_mutation_request_sha256("acquire_batch", request) == legacy_hash
    assert (
        review_mutation_request_sha256(
            "acquire_batch", request.model_copy(update={"compact": True})
        )
        != legacy_hash
    )


@pytest.mark.anyio
async def test_compact_acquisition_replays_and_retains_full_review_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seller: JsonValue = {"id": "seller", "profile": {"name": "Seller"}}
    projection = _projection("123", seller)

    async def list_composed_search(
        _application: ReviewApplication, _request: ListComposedSearchRequest
    ) -> ComposedListingPage:
        return ComposedListingPage(
            as_of_completion_sequence=1,
            selected_search_run_record_identifier="search-run",
            included_ancestry_run_count=1,
            older_ancestry_truncated=False,
            examined_candidate_listing_count=1,
            candidate_examination_limit_reached=False,
            listings=(projection,),
            next_cursor=None,
        )

    async def code_provenance() -> CodeProvenance:
        return _provenance()

    async def get_composed_listing(
        _application: ReviewApplication, _request: GetComposedListingRequest
    ) -> ComposedListingProjection:
        return projection

    monkeypatch.setattr(ReviewApplication, "list_composed_search", list_composed_search)
    monkeypatch.setattr(ReviewApplication, "get_composed_listing", get_composed_listing)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.publish_records_operation(
            component=Component(ComponentId(("test", "search")), 1, lambda: None),
            operation_identifier="search-operation",
            records=(
                RecordDraft(
                    identifier="search-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value={},
                ),
            ),
            inputs=(),
            outputs=(NamedOutput(name=("search_run",), object_identifier="search-run"),),
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-10-13T00:00:00+00:00",
            ended_at_utc="2026-10-13T00:00:01+00:00",
            duration_ns=1,
            result={"state": "completed"},
        )
        application = ReviewApplication(
            database=database, repository_root=tmp_path, code_provenance=code_provenance
        )
        workspace = await application.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Telescopes", search_run_record_identifier="search-run"
            )
        )
        request = AcquireReviewBatchRequest(
            workspace_record_identifier=workspace.record_identifier,
            request_identifier="compact-batch",
            owner_identifier="review-agent",
            compact=True,
        )
        acquired = await application.acquire_review_batch(request)
        assert isinstance(acquired, CompactReviewBatchAcquisition)
        assert await application.acquire_review_batch(request) == acquired
        assert acquired.lease is not None
        stored = await application.get_review_batch(acquired.batch.record_identifier)
        assert stored.items[0].projection == projection
        assert stored.items[0].projection.seller is not None
        assert stored.items[0].projection.seller.value == seller

        async with Client(build_server(application)) as client:
            replay = await client.call_tool(
                "acquire_review_batch", {"request": request.model_dump(mode="json")}
            )
            assert not replay.is_error
            assert replay.structured_content == acquired.model_dump(mode="json")
            listing = await client.call_tool(
                "get_workspace_listing",
                {
                    "request": {
                        "workspace_record_identifier": workspace.record_identifier,
                        "listing_identifier": "123",
                        "compact": True,
                        "maximum_gallery_images": 0,
                        "maximum_analyses": 0,
                    }
                },
            )
            assert not listing.is_error
            assert listing.structured_content is not None
            assert len(listing.structured_content["sellers"]) == 1
            assert (
                listing.structured_content["listing"]["projection"]["seller_reference"]
                in listing.structured_content["sellers"]
            )
            full_listing = await client.call_tool(
                "get_workspace_listing",
                {
                    "request": {
                        "workspace_record_identifier": workspace.record_identifier,
                        "listing_identifier": "123",
                    }
                },
            )
            assert not full_listing.is_error
            assert full_listing.structured_content is not None
            assert full_listing.structured_content["listing_identifier"] == "123"
            assert full_listing.structured_content["seller"]["value"] == seller

        recorded = await application.record_listing_reviews(
            RecordListingReviewsRequest(
                request_identifier="compact-review",
                workspace_record_identifier=workspace.record_identifier,
                batch_record_identifier=acquired.batch.record_identifier,
                claim_token=acquired.lease.claim_token,
                claim_owner_identifier="review-agent",
                reviews=(
                    ListingReviewInput(
                        listing_identifier="123",
                        projection_revision=acquired.batch.items[0].projection.projection_revision,
                    ),
                ),
            )
        )
        assert recorded.records[0].scalar_fields_snapshot is not None
        assert recorded.records[0].scalar_fields_snapshot.seller == seller
