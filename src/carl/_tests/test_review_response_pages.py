"""Bounded batch summaries preserve every claimed item and exact review revisions."""

# pyright: reportPrivateUsage=false

import base64
import json

import pytest
from pydantic import ValidationError

from carl._tests.test_compact_review import _acquisition, _prior, _projection
from carl.core.composed_projection import ComposedField, ScalarFieldChange, ScalarFieldName
from carl.core.review_errors import ReviewInputError
from carl.core.review_response_pages import (
    REVIEW_RESPONSE_PAGE_BYTE_BUDGET,
    GetReviewBatchPageRequest,
    paged_review_acquisition,
    review_batch_page,
    workspace_batch_cursor,
    workspace_batch_cursor_record_identifier,
)
from carl.core.review_workspace import (
    ListingReviewInput,
    ProjectionRevisionComponent,
    ReviewBatch,
    ReviewBatchAcquisition,
    ReviewBatchItem,
    ReviewDisposition,
    ReviewState,
)


def _large_acquisition(count: int = 100) -> ReviewBatchAcquisition:
    seller = {"profile": "DO_NOT_EMIT_SELLER_PROFILE" + "S" * 50_000}
    description = "DO_NOT_EMIT_DESCRIPTION" + "D" * 50_000
    items: list[ReviewBatchItem] = []
    for index in range(count):
        projection = _projection(str(1_000_000_000_000 + index), seller)
        assert projection.seller is not None
        evidence = projection.seller.evidence
        projection = projection.model_copy(
            update={
                "title": ComposedField(value='星\\"\n' * 1_000, evidence=evidence),
                "description": ComposedField(value=description, evidence=evidence),
                "price": ComposedField(value={"unbounded": "P" * 30_000}, evidence=evidence),
                "condition": ComposedField(value="Used", evidence=evidence),
                "shipping": ComposedField(value="Pickup", evidence=evidence),
                "last_sale": ComposedField(value={"date": "2026-10-16"}, evidence=evidence),
            }
        )
        prior = _prior(projection, seller).model_copy(
            update={"note": "DO_NOT_EMIT_PRIOR_NOTE" + "N" * 3_000}
        )
        items.append(
            ReviewBatchItem(
                projection=projection,
                review_state=ReviewState.STALE,
                prior_review=prior,
                changed_components=(ProjectionRevisionComponent.SCALAR_FIELDS,),
                scalar_field_changes=(
                    ScalarFieldChange(
                        field=ScalarFieldName.DESCRIPTION,
                        previous_value="old " + description,
                        current_value=description,
                    ),
                ),
                scalar_comparison_available=True,
            )
        )
    return _acquisition(tuple(items))


def _wire_size(value: object, text: str) -> int:
    return len(
        json.dumps(
            {"content": [{"type": "text", "text": text}], "structuredContent": value},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def test_acquisition_first_page_preserves_full_claim_and_stored_evidence() -> None:
    source = _large_acquisition()
    response = paged_review_acquisition(source)
    assert response.lease is source.lease
    assert response.lease is not None
    assert len(response.lease.listing_identifiers) == 100
    assert response.batch.record_identifier == source.batch.record_identifier
    assert response.batch.workspace_next_cursor == "batch:batch"
    assert source.batch.next_cursor == "continuation"
    assert response.batch.next_cursor is not None
    assert response.has_more and response.batch.has_more
    assert response.batch.total_items == 100
    assert 1 <= response.batch.returned_items <= 5
    assert response.response_bytes == len(response.model_dump_json().encode("utf-8"))
    assert (
        response.response_bytes <= response.response_byte_budget == REVIEW_RESPONSE_PAGE_BYTE_BUDGET
    )
    assert response.batch.response_bytes == len(response.batch.model_dump_json().encode("utf-8"))
    encoded = response.model_dump_json()
    assert _wire_size(response.model_dump(mode="json"), encoded) < 25_000
    for marker in (
        "DO_NOT_EMIT_SELLER_PROFILE",
        "DO_NOT_EMIT_DESCRIPTION",
        "DO_NOT_EMIT_PRIOR_NOTE",
    ):
        assert marker not in encoded
    assert len(source.batch.items) == 100
    assert source.batch.items[0].projection.description is not None
    assert len(str(source.batch.items[0].projection.description.value)) > 50_000
    assert paged_review_acquisition(source).model_dump_json() == encoded


def test_all_hundred_items_are_reachable_once_with_limit_independent_cursors() -> None:
    source = _large_acquisition()
    response = paged_review_acquisition(source)
    seen = list(response.batch.items)
    cursor = response.batch.next_cursor
    limits = (1, 10, 3, 7)
    page_index = 0
    while cursor is not None:
        limit = limits[page_index % len(limits)]
        page = review_batch_page(source.batch, cursor=cursor, limit=limit)
        assert page.offset == len(seen)
        assert 1 <= page.returned_items <= limit
        assert page.response_bytes <= page.response_byte_budget == REVIEW_RESPONSE_PAGE_BYTE_BUDGET
        assert page.response_bytes == len(page.model_dump_json().encode("utf-8"))
        assert page.has_more == (page.next_cursor is not None)
        assert page.status_counts == {"available": 100}
        assert page.review_state_counts == {"stale": 100}
        assert _wire_size(page.model_dump(mode="json"), page.model_dump_json()) < 50_000
        seen.extend(page.items)
        cursor = page.next_cursor
        page_index += 1
        assert page_index <= 100
    assert [item.projection.listing_identifier for item in seen] == [
        item.projection.listing_identifier for item in source.batch.items
    ]
    for original, summary in zip(source.batch.items, seen, strict=True):
        assert summary.projection.projection_revision == original.projection.projection_revision
        assert summary.projection.seller_available and summary.projection.description_available
        assert summary.projection.truncated_fields == (ScalarFieldName.TITLE, ScalarFieldName.PRICE)
        assert summary.prior_note_available
        assert summary.scalar_fields_changed == (ScalarFieldName.DESCRIPTION,)
        # The exact revision needed to record the review is present on every page.
        _ = ListingReviewInput(
            listing_identifier=summary.projection.listing_identifier,
            projection_revision=summary.projection.projection_revision,
            disposition=ReviewDisposition.REJECTED,
        )


def test_page_byte_budget_can_return_fewer_items_than_the_requested_limit() -> None:
    batch = _large_acquisition(10).batch
    page = review_batch_page(batch, limit=10, byte_budget=4_000)
    assert 1 <= len(page.items) < 10
    assert page.response_bytes <= 4_000
    assert page.next_cursor is not None
    following = review_batch_page(batch, cursor=page.next_cursor, limit=10, byte_budget=4_000)
    assert following.offset == len(page.items)
    assert (
        following.items[0].projection.listing_identifier
        == batch.items[len(page.items)].projection.listing_identifier
    )


def test_smallest_budget_keeps_review_identity_when_all_previews_must_be_omitted() -> None:
    batch = _large_acquisition(2).batch
    page = review_batch_page(batch, byte_budget=2_048)
    assert page.response_bytes <= 2_048
    assert len(page.items) == 1
    projection = page.items[0].projection
    assert projection.title is None
    assert projection.condition is None
    assert projection.shipping is None
    assert projection.last_sale is None
    assert ScalarFieldName.TITLE in projection.truncated_fields
    assert ScalarFieldName.CONDITION in projection.truncated_fields
    assert projection.projection_revision == batch.items[0].projection.projection_revision
    assert page.next_cursor is not None
    following = review_batch_page(batch, cursor=page.next_cursor, byte_budget=2_048)
    assert (
        following.items[0].projection.listing_identifier
        == batch.items[1].projection.listing_identifier
    )
    assert not following.has_more


def test_large_workspace_cursor_is_replaced_by_durable_batch_reference_on_every_page() -> None:
    source = _large_acquisition(100)
    raw_cursor = (
        base64.urlsafe_b64encode(
            json.dumps(
                {"runs": [f"retained-run-{index:03d}-" + "a" * 36 for index in range(100)]}
            ).encode()
        )
        .decode()
        .rstrip("=")
    )
    assert len(raw_cursor) > 6_000
    batch = source.batch.model_copy(update={"next_cursor": raw_cursor})
    response = paged_review_acquisition(source.model_copy(update={"batch": batch}), limit=10)
    assert response.lease is source.lease
    assert response.batch.workspace_next_cursor == "batch:batch"
    assert (
        workspace_batch_cursor_record_identifier(response.batch.workspace_next_cursor)
        == batch.record_identifier
    )
    assert raw_cursor not in response.model_dump_json()
    assert batch.next_cursor == raw_cursor
    seen = list(response.batch.items)
    cursor = response.batch.next_cursor
    minimum_returned = len(seen)
    while cursor is not None:
        page = review_batch_page(batch, cursor=cursor, limit=10)
        assert page.offset == len(seen)
        assert page.workspace_next_cursor == "batch:batch"
        assert page.response_bytes <= REVIEW_RESPONSE_PAGE_BYTE_BUDGET
        if page.next_cursor is not None:
            minimum_returned = min(minimum_returned, len(page.items))
        seen.extend(page.items)
        cursor = page.next_cursor
    assert minimum_returned >= 4
    assert [item.projection.listing_identifier for item in seen] == [
        item.projection.listing_identifier for item in batch.items
    ]
    # References survive JSON storage/reload and do not depend on a server cache.
    restored = ReviewBatch.model_validate_json(batch.model_dump_json())
    assert workspace_batch_cursor(restored) == response.batch.workspace_next_cursor
    assert restored.next_cursor == raw_cursor


@pytest.mark.parametrize("cursor", [None, "legacy-base64-cursor", "continuation"])
def test_workspace_cursor_parser_leaves_legacy_cursors_unchanged(cursor: str | None) -> None:
    assert workspace_batch_cursor_record_identifier(cursor) is None


@pytest.mark.parametrize(
    "cursor",
    [
        "batch:",
        "batch:invalid whitespace",
        "batch:\ninvalid",
        "batch:invalid\x7f",
        "batch:" + "a" * 513,
    ],
)
def test_malformed_workspace_batch_references_raise_domain_errors(cursor: str) -> None:
    with pytest.raises(ReviewInputError, match="cursor reference is malformed"):
        _ = workspace_batch_cursor_record_identifier(cursor)


@pytest.mark.parametrize("cursor", ["", "invalid cursor", "%%%", "YQ", "A" * 2_049])
def test_malformed_batch_cursors_are_rejected(cursor: str) -> None:
    with pytest.raises(ValueError, match="cursor is malformed"):
        _ = review_batch_page(_large_acquisition(2).batch, cursor=cursor)


def test_cursors_cannot_be_reused_for_another_batch_or_changed_revision() -> None:
    batch = _large_acquisition(2).batch
    cursor = review_batch_page(batch, limit=1).next_cursor
    assert cursor is not None
    with pytest.raises(ValueError, match="different batch or revision"):
        _ = review_batch_page(
            batch.model_copy(update={"record_identifier": "other-batch"}), cursor=cursor
        )
    changed_projection = batch.items[1].projection.model_copy(
        update={
            "projection_revision": batch.items[1].projection.projection_revision.model_copy(
                update={"scalar_fields_sha256": "1" * 64}
            )
        }
    )
    changed = batch.model_copy(
        update={
            "items": (
                batch.items[0],
                batch.items[1].model_copy(update={"projection": changed_projection}),
            )
        }
    )
    with pytest.raises(ValueError, match="different batch or revision"):
        _ = review_batch_page(changed, cursor=cursor)
    value = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
    value["offset"] = 3
    beyond = base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")
    with pytest.raises(ValueError, match="beyond the batch"):
        _ = review_batch_page(batch, cursor=beyond)


def test_empty_batches_have_complete_metadata_and_no_claim() -> None:
    batch = ReviewBatch(
        record_identifier="empty",
        workspace_record_identifier="workspace",
        created_at_utc="2026-10-16T00:00:00+00:00",
        items=(),
        next_cursor=None,
        scanned_page_count=1,
    )
    response = paged_review_acquisition(ReviewBatchAcquisition(batch=batch, lease=None))
    assert response.lease is None
    assert response.batch.total_items == response.batch.returned_items == 0
    assert response.batch.items == ()
    assert not response.has_more
    assert response.batch.next_cursor is None
    assert response.batch.status_counts == response.batch.review_state_counts == {}
    assert response.response_bytes == len(response.model_dump_json().encode("utf-8"))


@pytest.mark.parametrize("limit", [0, 11])
def test_request_and_helper_enforce_page_item_limits(limit: int) -> None:
    with pytest.raises(ValidationError):
        _ = GetReviewBatchPageRequest(batch_record_identifier="batch", limit=limit)
    with pytest.raises(ValueError, match="between one and ten"):
        _ = review_batch_page(_large_acquisition(1).batch, limit=limit)
