"""Byte-bounded views of immutable review batches without changing their claims."""

from __future__ import annotations

import base64
import hashlib
import json
from collections import Counter
from collections.abc import Callable
from typing import Literal

from pydantic import Field, model_validator

from carl.core.composed_projection import ListingStatus, ScalarFieldName
from carl.core.listing_identity import LISTING_IDENTIFIER_PATTERN
from carl.core.models import JsonValue, StrictModel
from carl.core.review_errors import ReviewInputError
from carl.core.review_workspace import (
    ProjectionRevisionComponent,
    ReviewBatch,
    ReviewBatchAcquisition,
    ReviewBatchItem,
    ReviewClaimLease,
    ReviewDisposition,
    ReviewState,
)
from carl.core.travel import TravelEstimate

REVIEW_RESPONSE_PAGE_BYTE_BUDGET = 9_000
REVIEW_RESPONSE_PAGE_DEFAULT_LIMIT = 5
REVIEW_RESPONSE_PAGE_MAXIMUM_LIMIT = 10
WORKSPACE_BATCH_CURSOR_PREFIX = "batch:"


class GetReviewBatchPageRequest(StrictModel):
    batch_record_identifier: str = Field(min_length=1)
    limit: int = Field(default=REVIEW_RESPONSE_PAGE_DEFAULT_LIMIT, ge=1, le=10)
    cursor: str | None = Field(default=None, max_length=2_048)


class GetReviewBatchItemRequest(StrictModel):
    batch_record_identifier: str = Field(min_length=1)
    listing_identifier: str = Field(pattern=LISTING_IDENTIFIER_PATTERN)


class ProjectionRevisionSummary(StrictModel):
    """Stable identity for browsing; full revision components are on the frozen item."""

    aggregate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ReviewBatchProjectionSummary(StrictModel):
    listing_identifier: str
    canonical_source_url: str
    as_of_completion_sequence: int = Field(ge=0)
    projection_revision: ProjectionRevisionSummary
    status: ListingStatus
    title: JsonValue
    price: JsonValue
    location: JsonValue
    condition: JsonValue
    shipping: JsonValue
    last_sale: JsonValue
    description_available: bool
    seller_available: bool
    referenced_image_count: int | None
    saved_image_count: int | None
    analysis_available: bool
    warning_count: int = Field(ge=0)
    truncated_fields: tuple[ScalarFieldName, ...] = ()
    route_estimate: TravelEstimate | None = None


class ReviewBatchItemSummary(StrictModel):
    projection: ReviewBatchProjectionSummary
    review_state: ReviewState
    prior_review_record_identifier: str | None
    prior_disposition: ReviewDisposition | None
    prior_note_available: bool
    changed_components: tuple[ProjectionRevisionComponent, ...]
    scalar_fields_changed: tuple[ScalarFieldName, ...]
    scalar_comparison_available: bool


class ReviewBatchPage(StrictModel):
    record_identifier: str
    workspace_record_identifier: str
    created_at_utc: str
    total_items: int = Field(ge=0, le=100)
    offset: int = Field(ge=0, le=100)
    returned_items: int = Field(ge=0, le=10)
    items: tuple[ReviewBatchItemSummary, ...] = Field(max_length=10)
    has_more: bool
    next_cursor: str | None
    workspace_next_cursor: str | None = Field(
        description="Durable continuation for a later workspace batch; pass as acquire_review_batch.cursor."
    )
    scanned_page_count: int = Field(ge=1)
    status_counts: dict[ListingStatus, int]
    review_state_counts: dict[ReviewState, int]
    response_bytes: int = Field(default=0, ge=0)
    response_byte_budget: int = Field(default=REVIEW_RESPONSE_PAGE_BYTE_BUDGET, ge=2_048)

    @model_validator(mode="after")
    def validate_page(self) -> ReviewBatchPage:
        if (
            self.returned_items != len(self.items)
            or self.offset + len(self.items) > self.total_items
        ):
            raise ValueError("Review batch page counts do not match its items")
        if (self.next_cursor is not None) != (self.offset + len(self.items) < self.total_items):
            raise ValueError("Review batch page cursor does not match its remaining items")
        if self.has_more != (self.next_cursor is not None):
            raise ValueError("Review batch page continuation metadata disagrees")
        return self


class PagedReviewBatchAcquisition(StrictModel):
    batch: ReviewBatchPage
    lease: ReviewClaimLease | None
    has_more: bool
    response_bytes: int = Field(default=0, ge=0)
    response_byte_budget: int = Field(default=REVIEW_RESPONSE_PAGE_BYTE_BUDGET, ge=2_048)


class _ReviewBatchCursor(StrictModel):
    version: Literal[1] = 1
    batch_record_identifier: str
    batch_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    offset: int = Field(ge=0, le=100)


def workspace_batch_cursor(batch: ReviewBatch) -> str | None:
    """Reference the immutable batch's retained workspace continuation."""

    return (
        None
        if batch.next_cursor is None
        else f"{WORKSPACE_BATCH_CURSOR_PREFIX}{batch.record_identifier}"
    )


def workspace_batch_cursor_record_identifier(cursor: str | None) -> str | None:
    """Recognize durable references while leaving legacy raw cursors untouched."""

    if cursor is None or not cursor.startswith(WORKSPACE_BATCH_CURSOR_PREFIX):
        return None
    identifier = cursor.removeprefix(WORKSPACE_BATCH_CURSOR_PREFIX)
    if (
        not identifier
        or len(identifier) > 512
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in identifier
        )
    ):
        raise ReviewInputError("Workspace batch cursor reference is malformed")
    return identifier


def _encoded_size(value: StrictModel) -> int:
    return len(value.model_dump_json().encode("utf-8"))


def _measure_page(page: ReviewBatchPage) -> ReviewBatchPage:
    for _ in range(8):
        size = _encoded_size(page)
        if page.response_bytes == size:
            return page
        page = page.model_copy(update={"response_bytes": size})
    raise ValueError("Review batch response size did not converge")


def _acquisition_response(
    page: ReviewBatchPage, lease: ReviewClaimLease | None, byte_budget: int
) -> PagedReviewBatchAcquisition:
    response = PagedReviewBatchAcquisition(
        batch=page, lease=lease, has_more=page.has_more, response_byte_budget=byte_budget
    )
    for _ in range(8):
        size = _encoded_size(response)
        if response.response_bytes == size:
            return response
        response = response.model_copy(update={"response_bytes": size})
    raise ValueError("Review batch acquisition response size did not converge")


def _batch_identity(batch: ReviewBatch) -> str:
    identity = [
        [
            item.projection.listing_identifier,
            item.projection.projection_revision.model_dump(mode="json"),
        ]
        for item in batch.items
    ]
    return hashlib.sha256(
        json.dumps(identity, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
    ).hexdigest()


def _cursor(batch: ReviewBatch, identity: str, offset: int) -> str:
    value = _ReviewBatchCursor(
        batch_record_identifier=batch.record_identifier, batch_sha256=identity, offset=offset
    )
    return base64.urlsafe_b64encode(value.model_dump_json().encode("utf-8")).decode().rstrip("=")


def _offset(batch: ReviewBatch, identity: str, cursor: str | None) -> int:
    if cursor is None:
        return 0
    if not cursor or len(cursor) > 2_048:
        raise ValueError("Review batch cursor is malformed")
    try:
        content = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        decoded = _ReviewBatchCursor.model_validate_json(content)
    except ValueError as error:
        raise ValueError("Review batch cursor is malformed") from error
    if (
        decoded.batch_record_identifier != batch.record_identifier
        or decoded.batch_sha256 != identity
    ):
        raise ValueError("Review batch cursor belongs to a different batch or revision")
    if decoded.offset > len(batch.items):
        raise ValueError("Review batch cursor lies beyond the batch")
    return decoded.offset


def _bounded_value(value: JsonValue, byte_limit: int) -> tuple[JsonValue, bool]:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) <= byte_limit:
        return value, False
    if isinstance(value, str):
        # Encode the candidate as JSON as well: quotes/control characters may
        # occupy several bytes on the wire despite having short UTF-8 text.
        shortened = value[:byte_limit]
        while len(json.dumps(shortened + "…", ensure_ascii=False).encode("utf-8")) > byte_limit:
            shortened = shortened[: max(0, len(shortened) - max(1, len(shortened) // 8))]
        return shortened + "…", True
    return None, True


def _item_summary(item: ReviewBatchItem, *, minimal: bool = False) -> ReviewBatchItemSummary:
    projection = item.projection
    values: dict[str, JsonValue] = {}
    truncated: list[ScalarFieldName] = []
    for name, limit in (
        (ScalarFieldName.TITLE, 256),
        (ScalarFieldName.PRICE, 192),
        (ScalarFieldName.LOCATION, 128),
        (ScalarFieldName.CONDITION, 128),
        (ScalarFieldName.SHIPPING, 128),
        (ScalarFieldName.LAST_SALE, 256),
    ):
        field = getattr(projection, name.value)
        value = None if field is None else field.value
        bounded, changed = (None, value is not None) if minimal else _bounded_value(value, limit)
        values[name.value] = bounded
        if changed:
            truncated.append(name)
    gallery = projection.gallery
    prior = item.prior_review
    return ReviewBatchItemSummary(
        projection=ReviewBatchProjectionSummary(
            listing_identifier=projection.listing_identifier,
            canonical_source_url=projection.canonical_source_url,
            as_of_completion_sequence=projection.as_of_completion_sequence,
            projection_revision=ProjectionRevisionSummary(
                aggregate_sha256=projection.projection_revision.aggregate_sha256
            ),
            status=projection.status.value,
            title=values["title"],
            price=values["price"],
            location=values["location"],
            condition=values["condition"],
            shipping=values["shipping"],
            last_sale=values["last_sale"],
            description_available=projection.description is not None,
            seller_available=projection.seller is not None,
            referenced_image_count=None if gallery is None else gallery.referenced_image_count,
            saved_image_count=None if gallery is None else gallery.saved_image_count,
            analysis_available=bool(projection.analyses) or projection.analyses_truncated,
            warning_count=len(projection.warnings),
            route_estimate=projection.route_estimate,
            truncated_fields=tuple(truncated),
        ),
        review_state=item.review_state,
        prior_review_record_identifier=None if prior is None else prior.record_identifier,
        prior_disposition=None if prior is None else prior.disposition,
        prior_note_available=prior is not None and prior.note is not None,
        changed_components=item.changed_components,
        scalar_fields_changed=tuple(
            dict.fromkeys(change.field for change in item.scalar_field_changes)
        ),
        scalar_comparison_available=item.scalar_comparison_available,
    )


def _build_page(
    batch: ReviewBatch,
    *,
    limit: int,
    cursor: str | None,
    byte_budget: int,
    response_size: Callable[[ReviewBatchPage], int],
) -> ReviewBatchPage:
    if not 1 <= limit <= REVIEW_RESPONSE_PAGE_MAXIMUM_LIMIT:
        raise ValueError("Review batch page limit must be between one and ten")
    if byte_budget < 2_048:
        raise ValueError("Review batch page byte budget must be at least 2048")
    identity = _batch_identity(batch)
    offset = _offset(batch, identity, cursor)

    def page(items: tuple[ReviewBatchItemSummary, ...]) -> ReviewBatchPage:
        next_offset = offset + len(items)
        return _measure_page(
            ReviewBatchPage(
                record_identifier=batch.record_identifier,
                workspace_record_identifier=batch.workspace_record_identifier,
                created_at_utc=batch.created_at_utc,
                total_items=len(batch.items),
                offset=offset,
                returned_items=len(items),
                items=items,
                has_more=next_offset < len(batch.items),
                next_cursor=_cursor(batch, identity, next_offset)
                if next_offset < len(batch.items)
                else None,
                workspace_next_cursor=workspace_batch_cursor(batch),
                scanned_page_count=batch.scanned_page_count,
                status_counts=dict(Counter(item.projection.status.value for item in batch.items)),
                review_state_counts=dict(Counter(item.review_state for item in batch.items)),
                response_byte_budget=byte_budget,
            )
        )

    items: tuple[ReviewBatchItemSummary, ...] = ()
    for item in batch.items[offset : offset + limit]:
        candidate = _item_summary(item)
        if response_size(page((*items, candidate))) > byte_budget:
            if items:
                break
            candidate = _item_summary(item, minimal=True)
            if response_size(page((candidate,))) > byte_budget:
                raise ValueError(
                    "Review batch item identity and claim metadata exceed the response byte budget"
                )
        items = (*items, candidate)
    result = page(items)
    if response_size(result) > byte_budget:
        raise ValueError("Review batch metadata exceeds the response byte budget")
    return result


def review_batch_page(
    batch: ReviewBatch,
    *,
    limit: int = REVIEW_RESPONSE_PAGE_DEFAULT_LIMIT,
    cursor: str | None = None,
    byte_budget: int = REVIEW_RESPONSE_PAGE_BYTE_BUDGET,
) -> ReviewBatchPage:
    """Page original batch indices; changing the limit never skips an item."""

    return _build_page(
        batch, limit=limit, cursor=cursor, byte_budget=byte_budget, response_size=_encoded_size
    )


def paged_review_acquisition(
    acquisition: ReviewBatchAcquisition,
    *,
    limit: int = REVIEW_RESPONSE_PAGE_DEFAULT_LIMIT,
    byte_budget: int = REVIEW_RESPONSE_PAGE_BYTE_BUDGET,
) -> PagedReviewBatchAcquisition:
    """Return the first page while preserving the exact lease for the full batch."""

    page = _build_page(
        acquisition.batch,
        limit=limit,
        cursor=None,
        byte_budget=byte_budget,
        response_size=lambda page: _encoded_size(
            _acquisition_response(page, acquisition.lease, byte_budget)
        ),
    )
    return _acquisition_response(page, acquisition.lease, byte_budget)
