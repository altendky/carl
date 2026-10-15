"""Batch indexes expose one revision hash; frozen detail supplies review evidence."""

# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest
from mcp import Client
from pydantic import ValidationError

from carl._tests.test_compact_review import _acquisition, _projection, _provenance
from carl._tests.test_marketplace_listing import _publish
from carl._tests.test_selection_analysis_freshness import _workspace
from carl.core.composed_projection import ProjectionRevision
from carl.core.json import encode_json
from carl.core.models import CodeProvenance, RecordDraft
from carl.core.review_evidence import InternedReviewBatchItem, rehydrate_review_batch_item
from carl.core.review_response_pages import (
    GetReviewBatchItemRequest,
    GetReviewBatchPageRequest,
    paged_review_acquisition,
    review_batch_page,
)
from carl.core.review_workspace import (
    REVIEW_BATCH_KIND,
    REVIEW_WORKSPACE_KIND,
    ListingReviewInput,
    RecordListingReviewsRequest,
    ReviewBatchAcquisition,
    ReviewBatchItem,
    ReviewDisposition,
    ReviewState,
    ReviewWorkspace,
)
from carl.io.sqlite import Database
from carl.mcp_server import build_server
from carl.review import ReviewApplication


def _normal_acquisition(count: int) -> ReviewBatchAcquisition:
    return _acquisition(
        tuple(
            ReviewBatchItem(
                projection=_projection(str(1_000_000_000_000 + index), {"name": "Seller"}),
                review_state=ReviewState.UNREVIEWED,
                prior_review=None,
                changed_components=(),
            )
            for index in range(count)
        )
    )


@pytest.mark.parametrize("count", [1, 10, 100])
def test_batch_summary_revision_contains_only_aggregate_hash_and_never_changes_frozen_batch(
    count: int,
) -> None:
    acquisition = _normal_acquisition(count)
    before = acquisition.model_dump_json()
    first = paged_review_acquisition(acquisition)
    pages = [first.batch]
    cursor = first.batch.next_cursor
    while cursor is not None:
        page = review_batch_page(acquisition.batch, cursor=cursor, limit=10)
        pages.append(page)
        cursor = page.next_cursor
    summaries = [item for page in pages for item in page.items]
    assert len(summaries) == count
    for original, summary in zip(acquisition.batch.items, summaries, strict=True):
        assert summary.projection.projection_revision.model_dump(mode="json") == {
            "aggregate_sha256": original.projection.projection_revision.aggregate_sha256,
        }
    for page in pages:
        encoded = page.model_dump_json()
        for field in ProjectionRevision.model_fields:
            if field != "aggregate_sha256":
                assert f'"{field}"' not in encoded
        assert page.response_bytes <= page.response_byte_budget == 9_000
    assert first.lease == acquisition.lease
    assert acquisition.model_dump_json() == before


def test_ten_normal_items_fit_one_summary_page_with_all_metadata_and_claim() -> None:
    acquisition = _normal_acquisition(10)
    page = review_batch_page(acquisition.batch, limit=10)
    response = paged_review_acquisition(acquisition, limit=10)
    assert page.returned_items == response.batch.returned_items == 10
    assert page.next_cursor is response.batch.next_cursor is None
    assert not page.has_more
    assert response.lease is not None
    assert len(response.lease.listing_identifiers) == 10
    assert page.response_bytes == len(page.model_dump_json().encode("utf-8"))
    assert response.response_bytes == len(response.model_dump_json().encode("utf-8"))
    assert page.response_bytes <= response.response_bytes <= 9_000


@pytest.mark.anyio
async def test_frozen_item_retains_component_revisions_for_recording_a_review(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _normal_acquisition(1).batch
    original = source.items[0]
    workspace = _workspace(())

    async def get_workspace(_app: ReviewApplication, identifier: str) -> ReviewWorkspace:
        assert identifier == workspace.record_identifier
        return workspace

    async def provenance() -> CodeProvenance:
        return _provenance()

    async def forbidden_current_projection(*_args: object, **_kwargs: object) -> None:
        raise AssertionError(
            "Issued batch review must use frozen evidence, independent of current listings"
        )

    monkeypatch.setattr(ReviewApplication, "get_review_workspace", get_workspace)
    monkeypatch.setattr(ReviewApplication, "get_composed_listing", forbidden_current_projection)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                RecordDraft(
                    identifier=workspace.record_identifier,
                    kind=REVIEW_WORKSPACE_KIND,
                    schema_version=1,
                    value=workspace.model_dump(mode="json"),
                ),
                RecordDraft(
                    identifier=source.record_identifier,
                    kind=REVIEW_BATCH_KIND,
                    schema_version=1,
                    value=source.model_dump(mode="json"),
                ),
            ),
        )
        application = ReviewApplication(database, tmp_path, code_provenance=provenance)
        page = await application.get_review_batch_page(
            GetReviewBatchPageRequest(batch_record_identifier=source.record_identifier)
        )
        assert set(page.items[0].projection.projection_revision.model_dump()) == {
            "aggregate_sha256"
        }
        with pytest.raises(ValidationError):
            _ = ListingReviewInput.model_validate_json(
                encode_json(
                    {
                        "listing_identifier": original.projection.listing_identifier,
                        "projection_revision": page.items[
                            0
                        ].projection.projection_revision.model_dump(mode="json"),
                    }
                )
            )
        request = GetReviewBatchItemRequest(
            batch_record_identifier=source.record_identifier,
            listing_identifier=original.projection.listing_identifier,
        )
        full = await application.get_review_batch_item(request)
        assert full == original
        assert set(full.projection.projection_revision.model_dump()) == set(
            ProjectionRevision.model_fields
        )
        async with Client(build_server(application)) as client:
            result = await client.call_tool(
                "get_review_batch_item", {"request": request.model_dump(mode="json")}
            )
            assert not result.is_error
            assert result.structured_content is not None
            hydrated = rehydrate_review_batch_item(
                InternedReviewBatchItem.model_validate_json(encode_json(result.structured_content))
            )
        assert hydrated == original
        recorded = await application.record_listing_reviews(
            RecordListingReviewsRequest(
                request_identifier="review-from-frozen-detail",
                workspace_record_identifier=workspace.record_identifier,
                batch_record_identifier=source.record_identifier,
                reviews=(
                    ListingReviewInput(
                        listing_identifier=hydrated.projection.listing_identifier,
                        projection_revision=hydrated.projection.projection_revision,
                        disposition=ReviewDisposition.PROMISING,
                    ),
                ),
            )
        )
        assert recorded.records[0].projection_revision == original.projection.projection_revision
        assert await application.get_review_batch(source.record_identifier) == source
