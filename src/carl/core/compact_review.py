"""Response-only compact review views; stored evidence and claim semantics stay intact."""

import hashlib
import json
from typing import cast

from pydantic import ConfigDict, Field, RootModel

from carl.core.composed_projection import (
    ComposedListingProjection,
    ProjectionRevision,
    ScalarFieldChange,
    ScalarFieldName,
)
from carl.core.ebay import EbayListingState
from carl.core.marketplace_search import Marketplace
from carl.core.models import JsonValue, StrictModel
from carl.core.review_response_pages import PagedReviewBatchAcquisition
from carl.core.review_workspace import (
    ListingReviewRecord,
    ProjectionRevisionComponent,
    ReviewBatchAcquisition,
    ReviewBatchItem,
    ReviewClaimLease,
    ReviewStalenessPolicy,
    ReviewState,
    ReviewWorkspace,
    WorkspaceListingReview,
    WorkspaceListingSummary,
    WorkspaceProductGuideBinding,
)
from carl.core.work import WorkState


class CompactWorkspaceSearchTrack(StrictModel):
    """Track discovery fields without acquisition or provenance identifiers."""

    track_identifier: str = Field(min_length=1)
    query: str = Field(min_length=1)
    marketplace: Marketplace | None
    listing_state: EbayListingState | None
    enabled: bool
    search_specification_version: int = Field(ge=1)
    latest_refresh_work_state: WorkState | None


class CompactReviewWorkspace(StrictModel):
    record_identifier: str = Field(min_length=1)
    name: str = Field(min_length=1)
    search_run_record_identifier: str = Field(min_length=1)
    product_guide_record_identifier: str | None
    staleness_policy: ReviewStalenessPolicy
    created_at_utc: str
    archived: bool
    search_tracks: tuple[CompactWorkspaceSearchTrack, ...]
    product_guide_bindings: tuple[WorkspaceProductGuideBinding, ...]
    default_product_guide_binding_identifier: str | None


def compact_review_workspace(workspace: ReviewWorkspace) -> CompactReviewWorkspace:
    """Keep track identities and versions for exact workspace drill-down."""

    return CompactReviewWorkspace.model_validate(
        {
            **workspace.model_dump(mode="python", exclude={"search_tracks"}),
            "search_tracks": tuple(
                CompactWorkspaceSearchTrack(
                    track_identifier=track.track_identifier,
                    query=track.query,
                    marketplace=track.marketplace,
                    listing_state=track.listing_state,
                    enabled=track.enabled,
                    search_specification_version=track.search_specification_version,
                    latest_refresh_work_state=track.latest_refresh_work_state,
                )
                for track in workspace.search_tracks
            ),
        }
    )


def analysis_claude_details(value: JsonValue, *, include_details: bool = False) -> JsonValue:
    """Keep invocation and usage diagnostics behind the explicit details option."""

    if include_details or not isinstance(value, dict):
        return value
    return {
        key: item
        for key, item in cast(dict[str, JsonValue], value).items()
        if key not in {"argv", "output_metadata", "modelUsage"}
    }


class CompactReviewProjection(WorkspaceListingSummary):
    as_of_completion_sequence: int = Field(ge=0)
    projection_revision: ProjectionRevision
    description: JsonValue
    seller_reference: str | None


class CompactReviewItem(StrictModel):
    projection: CompactReviewProjection
    review_state: ReviewState
    prior_review: ListingReviewRecord | None
    changed_components: tuple[ProjectionRevisionComponent, ...]
    scalar_field_changes: tuple[ScalarFieldChange, ...]
    scalar_comparison_available: bool


class CompactWorkspaceListingReview(StrictModel):
    listing: CompactReviewItem
    sellers: dict[str, JsonValue]


class CompactReviewBatch(StrictModel):
    record_identifier: str
    workspace_record_identifier: str
    created_at_utc: str
    items: tuple[CompactReviewItem, ...] = Field(max_length=100)
    next_cursor: str | None
    scanned_page_count: int


class CompactReviewBatchAcquisition(StrictModel):
    batch: CompactReviewBatch
    lease: ReviewClaimLease | None
    sellers: dict[str, JsonValue]


class WorkspaceListingResponse(RootModel[WorkspaceListingReview | CompactWorkspaceListingReview]):
    """Keep both response variants as top-level objects on the MCP wire."""

    model_config = ConfigDict(json_schema_extra={"type": "object"})


class ReviewBatchAcquisitionResponse(
    RootModel[ReviewBatchAcquisition | PagedReviewBatchAcquisition]
):
    model_config = ConfigDict(json_schema_extra={"type": "object"})


def _seller_reference(value: JsonValue, sellers: dict[str, JsonValue]) -> str | None:
    if value is None:
        return None
    # Seller payloads need not contain a stable marketplace profile identifier.
    reference = hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    sellers.setdefault(reference, value)
    return reference


def _compact_item(
    projection: ComposedListingProjection,
    review: ReviewBatchItem | WorkspaceListingReview,
    sellers: dict[str, JsonValue],
) -> CompactReviewItem:
    def value(name: str) -> JsonValue:
        field = getattr(projection, name)
        return None if field is None else field.value

    changes = tuple(
        change.model_copy(
            update={
                "previous_value": (
                    None
                    if change.previous_value is None
                    else {"seller_reference": _seller_reference(change.previous_value, sellers)}
                ),
                "current_value": (
                    None
                    if change.current_value is None
                    else {"seller_reference": _seller_reference(change.current_value, sellers)}
                ),
            }
        )
        if change.field is ScalarFieldName.SELLER
        else change
        for change in review.scalar_field_changes
    )
    gallery = projection.gallery
    compact_projection = CompactReviewProjection(
        listing_identifier=projection.listing_identifier,
        canonical_source_url=projection.canonical_source_url,
        as_of_completion_sequence=projection.as_of_completion_sequence,
        projection_revision=projection.projection_revision,
        projection_revision_sha256=projection.projection_revision.aggregate_sha256,
        status=projection.status.value,
        title=value("title"),
        price=value("price"),
        location=value("location"),
        condition=value("condition"),
        shipping=value("shipping"),
        last_sale=value("last_sale"),
        description=value("description"),
        seller_reference=_seller_reference(value("seller"), sellers),
        preview_image_url=(
            None
            if projection.preview_image is None
            else projection.preview_image.descriptor.original_url
        ),
        description_available=projection.description is not None,
        seller_available=projection.seller is not None,
        referenced_image_count=None if gallery is None else gallery.referenced_image_count,
        saved_image_count=None if gallery is None else gallery.saved_image_count,
        analysis_available=bool(projection.analyses) or projection.analyses_truncated,
        warnings=projection.warnings,
    )
    return CompactReviewItem(
        projection=compact_projection,
        review_state=review.review_state,
        prior_review=(
            None
            if review.prior_review is None
            else review.prior_review.model_copy(update={"scalar_fields_snapshot": None})
        ),
        changed_components=review.changed_components,
        scalar_field_changes=changes,
        scalar_comparison_available=review.scalar_comparison_available,
    )


def compact_workspace_listing(listing: WorkspaceListingReview) -> CompactWorkspaceListingReview:
    sellers: dict[str, JsonValue] = {}
    item = _compact_item(listing, listing, sellers)
    return CompactWorkspaceListingReview(listing=item, sellers=sellers)


def compact_review_acquisition(
    acquisition: ReviewBatchAcquisition,
) -> CompactReviewBatchAcquisition:
    sellers: dict[str, JsonValue] = {}
    batch = acquisition.batch
    items = tuple(_compact_item(item.projection, item, sellers) for item in batch.items)
    return CompactReviewBatchAcquisition(
        batch=CompactReviewBatch(
            record_identifier=batch.record_identifier,
            workspace_record_identifier=batch.workspace_record_identifier,
            created_at_utc=batch.created_at_utc,
            items=items,
            next_cursor=batch.next_cursor,
            scanned_page_count=batch.scanned_page_count,
        ),
        lease=acquisition.lease,
        sellers=sellers,
    )
