"""Persistent agent review workspaces, batches, worksets, and snapshots."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from carl.core.activity import WorkActivity
from carl.core.components import Component, ComponentId, Registry
from carl.core.composed_projection import (
    ComposedListingFilters,
    ComposedListingProjection,
    ListingStatus,
    ProjectionRevision,
)
from carl.core.facebook_search import SearchTraversalPolicy, SearchTraversalStrategy
from carl.core.facebook_work import CreateSearchRequest
from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel
from carl.core.work import WorkState

CREATE_REVIEW_WORKSPACE = ComponentId(("carl", "review", "create", "workspace"))
RENAME_REVIEW_WORKSPACE = ComponentId(("carl", "review", "rename", "workspace"))
SET_REVIEW_WORKSPACE_ARCHIVED = ComponentId(("carl", "review", "set", "workspace_archived"))
ADD_WORKSPACE_PRODUCT_GUIDE = ComponentId(("carl", "review", "add", "workspace_product_guide"))
UPDATE_WORKSPACE_PRODUCT_GUIDE = ComponentId(
    ("carl", "review", "update", "workspace_product_guide")
)
SET_WORKSPACE_DEFAULT_PRODUCT_GUIDE = ComponentId(
    ("carl", "review", "set", "workspace_default_product_guide")
)
SET_PRODUCT_GUIDE_IDENTITY_RETIRED = ComponentId(
    ("carl", "review", "set", "product_guide_identity_retired")
)
CREATE_REVIEW_BATCH = ComponentId(("carl", "review", "create", "batch"))
RECORD_LISTING_REVIEWS = ComponentId(("carl", "review", "record", "listing_reviews"))
RECORD_WORKSPACE_BULK_REVIEW = ComponentId(("carl", "review", "record", "workspace_bulk_review"))
CREATE_REVIEW_WORKSET = ComponentId(("carl", "review", "create", "workset"))
UPDATE_REVIEW_WORKSET = ComponentId(("carl", "review", "update", "workset"))
CREATE_SELECTION_SNAPSHOT = ComponentId(("carl", "review", "create", "selection_snapshot"))
SET_WORKSPACE_SEARCH_TRACK_ENABLED = ComponentId(
    ("carl", "review", "set", "workspace_search_track_enabled")
)
ACQUIRE_REVIEW_BATCH = ComponentId(("carl", "review", "acquire", "batch"))
RENEW_REVIEW_CLAIM = ComponentId(("carl", "review", "renew", "claim"))
RELEASE_REVIEW_CLAIM = ComponentId(("carl", "review", "release", "claim"))

REVIEW_WORKSPACE_KIND = ("carl", "review", "workspace")
REVIEW_BATCH_KIND = ("carl", "review", "batch")
LISTING_REVIEW_KIND = ("carl", "review", "listing_state")
REVIEW_WORKSET_KIND = ("carl", "review", "workset")
SELECTION_SNAPSHOT_KIND = ("carl", "review", "selection_snapshot")
WORKSPACE_SEARCH_TRACK_STATE_KIND = ("carl", "review", "workspace_search_track_state")
REVIEW_WORKSPACE_IDENTITY_STATE_KIND = ("carl", "review", "workspace_identity_state")
WORKSPACE_PRODUCT_GUIDE_BINDING_KIND = ("carl", "review", "workspace_product_guide_binding")
WORKSPACE_DEFAULT_PRODUCT_GUIDE_STATE_KIND = (
    "carl",
    "review",
    "workspace_default_product_guide_state",
)
PRODUCT_GUIDE_IDENTITY_STATE_KIND = (
    "carl",
    "review",
    "product_guide_identity_state",
)
MAXIMUM_WORKSET_LISTINGS = 10_000


def _component_marker(*_args: object, **_kwargs: object) -> None:
    """Identify local mutations in provenance; applications perform the actual writes."""


def build_review_workspace_component_registry() -> Registry:
    return Registry(
        tuple(
            Component(identifier, 1, _component_marker)
            for identifier in (
                CREATE_REVIEW_WORKSPACE,
                RENAME_REVIEW_WORKSPACE,
                SET_REVIEW_WORKSPACE_ARCHIVED,
                ADD_WORKSPACE_PRODUCT_GUIDE,
                UPDATE_WORKSPACE_PRODUCT_GUIDE,
                SET_WORKSPACE_DEFAULT_PRODUCT_GUIDE,
                SET_PRODUCT_GUIDE_IDENTITY_RETIRED,
                CREATE_REVIEW_BATCH,
                RECORD_LISTING_REVIEWS,
                RECORD_WORKSPACE_BULK_REVIEW,
                CREATE_REVIEW_WORKSET,
                UPDATE_REVIEW_WORKSET,
                CREATE_SELECTION_SNAPSHOT,
                SET_WORKSPACE_SEARCH_TRACK_ENABLED,
                ACQUIRE_REVIEW_BATCH,
                RENEW_REVIEW_CLAIM,
                RELEASE_REVIEW_CLAIM,
            )
        )
    )


class ProjectionRevisionComponent(JsonStringEnumeration):
    STATUS = "status"
    SCALAR_FIELDS = "scalar_fields"
    PREVIEW_IMAGE = "preview_image"
    GALLERY = "gallery"
    ANALYSES = "analyses"
    SEARCH_MEMBERSHIP = "search_membership"


class ReviewDisposition(JsonStringEnumeration):
    PROMISING = "promising"
    REJECTED = "rejected"
    WAITING_FOR_DATA = "waiting_for_data"
    DEFERRED = "deferred"


class ReviewState(JsonStringEnumeration):
    UNREVIEWED = "unreviewed"
    CURRENT = "current"
    STALE = "stale"


class ReviewStalenessPolicy(StrictModel):
    components: Sequence[ProjectionRevisionComponent] = (
        ProjectionRevisionComponent.STATUS,
        ProjectionRevisionComponent.SCALAR_FIELDS,
        ProjectionRevisionComponent.PREVIEW_IMAGE,
        ProjectionRevisionComponent.GALLERY,
        ProjectionRevisionComponent.ANALYSES,
    )

    @field_validator("components")
    @classmethod
    def validate_components(
        cls, value: Sequence[ProjectionRevisionComponent]
    ) -> Sequence[ProjectionRevisionComponent]:
        if not value or len(set(value)) != len(value):
            raise ValueError("Staleness components must be nonempty and unique")
        return value


class CreateReviewWorkspaceRequest(StrictModel):
    name: str = Field(min_length=1, max_length=200)
    search_run_record_identifier: str = Field(min_length=1)
    product_guide_record_identifier: str | None = Field(default=None, min_length=1)
    staleness_policy: ReviewStalenessPolicy = ReviewStalenessPolicy()

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Workspace name must be trimmed")
        return value


class ReviewWorkspace(StrictModel):
    record_identifier: str = Field(min_length=1)
    name: str = Field(min_length=1)
    search_run_record_identifier: str = Field(min_length=1)
    product_guide_record_identifier: str | None
    staleness_policy: ReviewStalenessPolicy
    created_at_utc: str
    archived: bool = False
    search_tracks: tuple[WorkspaceSearchTrack, ...] = Field(default=(), max_length=20)
    product_guide_bindings: tuple[WorkspaceProductGuideBinding, ...] = Field(
        default=(), max_length=20
    )
    default_product_guide_binding_identifier: str | None = Field(default=None, min_length=1)


class RenameReviewWorkspaceRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    name: str = Field(min_length=1, max_length=200)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Workspace name must be trimmed")
        return value


class SetReviewWorkspaceArchivedRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    archived: bool


class ReviewWorkspaceIdentityStateRecord(StrictModel):
    record_identifier: str = Field(min_length=1)
    workspace_record_identifier: str = Field(min_length=1)
    name: str | None = Field(default=None, min_length=1, max_length=200)
    archived: bool | None = None
    recorded_at_utc: str

    @model_validator(mode="after")
    def validate_change(self) -> ReviewWorkspaceIdentityStateRecord:
        if self.name is None and self.archived is None:
            raise ValueError("A workspace identity state must change at least one field")
        return self


class WorkspaceProductGuideVersionPolicy(JsonStringEnumeration):
    PINNED = "pinned"
    FOLLOW_LATEST = "follow_latest"


class WorkspaceProductGuideBinding(StrictModel):
    binding_identifier: str = Field(min_length=1)
    workspace_record_identifier: str = Field(min_length=1)
    alias: str = Field(min_length=1, max_length=100)
    product_guide_identity: tuple[str, ...] = Field(min_length=1)
    version_policy: WorkspaceProductGuideVersionPolicy
    pinned_product_guide_record_identifier: str | None = Field(default=None, min_length=1)
    resolved_product_guide_record_identifier: str = Field(min_length=1)
    resolved_product_guide_version: int = Field(ge=1)
    enabled: bool = True
    is_default: bool = False


class WorkspaceProductGuideBindingRecord(StrictModel):
    record_identifier: str = Field(min_length=1)
    binding_identifier: str = Field(min_length=1)
    workspace_record_identifier: str = Field(min_length=1)
    alias: str = Field(min_length=1, max_length=100)
    product_guide_identity: tuple[str, ...] = Field(min_length=1)
    version_policy: WorkspaceProductGuideVersionPolicy
    pinned_product_guide_record_identifier: str | None = Field(default=None, min_length=1)
    enabled: bool = True
    recorded_at_utc: str

    @model_validator(mode="after")
    def validate_policy(self) -> WorkspaceProductGuideBindingRecord:
        if (
            self.version_policy is WorkspaceProductGuideVersionPolicy.PINNED
            and self.pinned_product_guide_record_identifier is None
        ):
            raise ValueError("A pinned workspace guide binding requires an exact guide record")
        if (
            self.version_policy is WorkspaceProductGuideVersionPolicy.FOLLOW_LATEST
            and self.pinned_product_guide_record_identifier is not None
        ):
            raise ValueError("A follow-latest workspace guide binding cannot pin a guide record")
        return self


class WorkspaceDefaultProductGuideStateRecord(StrictModel):
    record_identifier: str = Field(min_length=1)
    workspace_record_identifier: str = Field(min_length=1)
    binding_identifier: str | None = Field(default=None, min_length=1)
    recorded_at_utc: str


class AddWorkspaceProductGuideRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    product_guide_record_identifier: str = Field(min_length=1)
    alias: str = Field(min_length=1, max_length=100)
    version_policy: WorkspaceProductGuideVersionPolicy = (
        WorkspaceProductGuideVersionPolicy.FOLLOW_LATEST
    )
    make_default: bool = False

    @field_validator("alias")
    @classmethod
    def validate_alias(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Workspace product-guide aliases must be trimmed")
        return value


class UpdateWorkspaceProductGuideBindingRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    binding_identifier: str = Field(min_length=1)
    alias: str | None = Field(default=None, min_length=1, max_length=100)
    version_policy: WorkspaceProductGuideVersionPolicy | None = None
    product_guide_record_identifier: str | None = Field(default=None, min_length=1)
    enabled: bool | None = None

    @field_validator("alias")
    @classmethod
    def validate_alias(cls, value: str | None) -> str | None:
        if value is not None and value != value.strip():
            raise ValueError("Workspace product-guide aliases must be trimmed")
        return value

    @model_validator(mode="after")
    def validate_change(self) -> UpdateWorkspaceProductGuideBindingRequest:
        if (
            self.alias is None
            and self.version_policy is None
            and self.product_guide_record_identifier is None
            and self.enabled is None
        ):
            raise ValueError("A workspace product-guide update must change at least one field")
        return self


class SetWorkspaceDefaultProductGuideRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    binding_identifier: str | None = Field(default=None, min_length=1)


class WorkspaceSearchTrack(StrictModel):
    track_identifier: str = Field(min_length=1)
    query: str = Field(min_length=1)
    creation_work_identifier: str | None = Field(default=None, min_length=1)
    creation_work_state: WorkState | None = None
    origin_search_run_record_identifier: str | None = Field(default=None, min_length=1)
    current_search_run_record_identifier: str | None = Field(default=None, min_length=1)
    latest_refresh_work_identifier: str | None = Field(default=None, min_length=1)
    latest_refresh_work_state: WorkState | None = None
    enabled: bool = True


class SetWorkspaceSearchTrackEnabledRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    track_identifier: str = Field(min_length=1)
    enabled: bool


class WorkspaceSearchTrackStateRecord(StrictModel):
    record_identifier: str = Field(min_length=1)
    workspace_record_identifier: str = Field(min_length=1)
    track_identifier: str = Field(min_length=1)
    enabled: bool
    recorded_at_utc: str


class CreateWorkspaceSearchRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    search: CreateSearchRequest


class CreateWorkspaceSearchResult(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    track_identifier: str = Field(min_length=1)
    work_identifier: str = Field(min_length=1)
    created: bool
    state: WorkState


class RetryWorkspaceSearchTrackRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    track_identifier: str = Field(min_length=1)


class RetryWorkspaceSearchTrackResult(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    track_identifier: str = Field(min_length=1)
    work_identifier: str = Field(min_length=1)
    previous_attempt_count: int = Field(ge=1)
    state: WorkState


class RequestWorkspaceRefreshRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    track_identifier: str | None = Field(default=None, min_length=1)
    traversal: SearchTraversalPolicy | None = None
    traversal_strategy: SearchTraversalStrategy | None = None
    maximum_items: int | None = Field(default=None, ge=1)
    maximum_images: int | None = Field(default=None, ge=1)
    proton_route: str = Field(default="carl", min_length=1)
    decodo_route: str = Field(default="carl", min_length=1)


class RequestWorkspaceRefreshResult(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    track_identifier: str = Field(min_length=1)
    work_identifier: str = Field(min_length=1)
    created: bool
    state: WorkState
    base_search_run_record_identifier: str = Field(min_length=1)


class ListWorkspaceListingsRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    filters: ComposedListingFilters = ComposedListingFilters()
    maximum_search_runs: int = Field(default=100, ge=1, le=100)
    maximum_gallery_images_per_listing: int = Field(
        default=0,
        ge=0,
        le=10,
        deprecated=True,
        description="Accepted for compatibility; compact rows return gallery counts only.",
    )
    maximum_analyses_per_listing: int = Field(
        default=1,
        ge=0,
        le=5,
        deprecated=True,
        description="Accepted for compatibility; compact rows return analysis presence only.",
    )
    maximum_observations_per_listing: int = Field(default=100, ge=100, le=100)
    maximum_candidate_listings_examined: int = Field(default=2_500, ge=1, le=10_000)
    page_size: int = Field(default=25, ge=1, le=100)
    cursor: str | None = None


class WorkspaceListingSummary(StrictModel):
    """Compact workspace index entry; use get_workspace_listing for evidence."""

    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    canonical_source_url: str = Field(min_length=1)
    status: ListingStatus
    title: JsonValue
    price: JsonValue
    location: JsonValue
    preview_image_url: str | None
    description_available: bool
    seller_available: bool
    referenced_image_count: int | None = Field(default=None, ge=0)
    saved_image_count: int | None = Field(default=None, ge=0)
    analysis_available: bool
    projection_revision_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    warnings: tuple[str, ...] = Field(default=(), max_length=20)


class WorkspaceListingPage(StrictModel):
    as_of_completion_sequence: int = Field(ge=0)
    selected_search_run_record_identifier: str = Field(min_length=1)
    included_ancestry_run_count: int = Field(ge=1)
    older_ancestry_truncated: bool
    examined_candidate_listing_count: int = Field(ge=0, le=10_000)
    candidate_examination_limit_reached: bool
    listings: tuple[WorkspaceListingSummary, ...] = Field(max_length=100)
    next_cursor: str | None


class GetWorkspaceListingRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    maximum_search_runs: int = Field(default=100, ge=1, le=100)
    maximum_gallery_images: int = Field(default=20, ge=0, le=100)
    maximum_analyses: int = Field(default=10, ge=0, le=20)
    maximum_observations: int = Field(default=100, ge=100, le=100)


class ListingReviewRecord(StrictModel):
    record_identifier: str = Field(min_length=1)
    workspace_record_identifier: str = Field(min_length=1)
    batch_record_identifier: str | None = Field(default=None, min_length=1)
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    projection_revision: ProjectionRevision
    inspected: bool
    disposition: ReviewDisposition | None
    note: str | None = Field(default=None, max_length=4_000)
    recorded_at_utc: str

    @field_validator("note")
    @classmethod
    def validate_note(cls, value: str | None) -> str | None:
        if value is not None and (not value or value != value.strip()):
            raise ValueError("Review notes must be nonempty and trimmed")
        return value


class ListingReviewInput(StrictModel):
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    projection_revision: ProjectionRevision
    inspected: bool = True
    disposition: ReviewDisposition | None = None
    note: str | None = Field(default=None, max_length=4_000)

    @field_validator("note")
    @classmethod
    def validate_note(cls, value: str | None) -> str | None:
        if value is not None and (not value or value != value.strip()):
            raise ValueError("Review notes must be nonempty and trimmed")
        return value

    @model_validator(mode="after")
    def validate_inspection(self) -> ListingReviewInput:
        if not self.inspected:
            raise ValueError("Only listings actually inspected should be recorded")
        return self


class RecordListingReviewsRequest(StrictModel):
    request_identifier: str = Field(min_length=1, max_length=200)
    workspace_record_identifier: str = Field(min_length=1)
    batch_record_identifier: str | None = Field(default=None, min_length=1)
    claim_token: str | None = Field(default=None, min_length=1)
    claim_owner_identifier: str | None = Field(default=None, min_length=1)
    reviews: Sequence[ListingReviewInput] = Field(min_length=1, max_length=100)

    @field_validator("request_identifier")
    @classmethod
    def validate_request_identifier(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("The request identifier must be trimmed")
        return value

    @field_validator("reviews")
    @classmethod
    def validate_unique_listings(
        cls, value: Sequence[ListingReviewInput]
    ) -> Sequence[ListingReviewInput]:
        identifiers = [review.listing_identifier for review in value]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("A review request may mention each listing only once")
        return value

    @model_validator(mode="after")
    def validate_claim_fence(self) -> RecordListingReviewsRequest:
        if (self.claim_token is None) != (self.claim_owner_identifier is None):
            raise ValueError("Claim token and owner must be provided together")
        if self.claim_token is not None and self.batch_record_identifier is None:
            raise ValueError("Claim credentials require a review batch")
        return self


class RecordListingReviewsResult(StrictModel):
    records: tuple[ListingReviewRecord, ...] = Field(max_length=100)


class WorkspaceBulkReviewSelection(StrictModel):
    kind: Literal["workspace"] = "workspace"


class WorksetBulkReviewSelection(StrictModel):
    kind: Literal["workset"] = "workset"
    workset_identifier: str = Field(min_length=1)


class SelectionSnapshotBulkReviewSelection(StrictModel):
    kind: Literal["selection_snapshot"] = "selection_snapshot"
    selection_snapshot_record_identifier: str = Field(min_length=1)


BulkReviewSelection = Annotated[
    WorkspaceBulkReviewSelection
    | WorksetBulkReviewSelection
    | SelectionSnapshotBulkReviewSelection,
    Field(discriminator="kind"),
]


class RecordWorkspaceBulkReviewRequest(StrictModel):
    request_identifier: str = Field(min_length=1, max_length=200)
    workspace_record_identifier: str = Field(min_length=1)
    selection: BulkReviewSelection = WorkspaceBulkReviewSelection()
    statuses: Sequence[ListingStatus] = Field(
        default=(ListingStatus.AVAILABLE,),
        min_length=1,
        max_length=5,
        json_schema_extra={"uniqueItems": True},
    )
    include_review_states: Sequence[ReviewState] = Field(
        default=(ReviewState.UNREVIEWED,),
        min_length=1,
        max_length=3,
        json_schema_extra={"uniqueItems": True},
    )
    disposition: ReviewDisposition
    note: str | None = Field(default=None, max_length=4_000)
    exclude_listing_identifiers: Sequence[Annotated[str, Field(pattern=r"^[0-9]+$")]] = Field(
        default=(), max_length=10_000, json_schema_extra={"uniqueItems": True}
    )
    maximum_candidate_listings_examined: int = Field(default=10_000, ge=1, le=10_000)

    @field_validator("request_identifier")
    @classmethod
    def validate_request_identifier(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("The request identifier must be trimmed")
        return value

    @field_validator("note")
    @classmethod
    def validate_note(cls, value: str | None) -> str | None:
        if value is not None and (not value or value != value.strip()):
            raise ValueError("Review notes must be nonempty and trimmed")
        return value

    @field_validator("statuses", "include_review_states")
    @classmethod
    def validate_unique_filters(cls, value: Sequence[object]) -> Sequence[object]:
        if len(set(value)) != len(value):
            raise ValueError("Bulk-review filters must be unique")
        return value

    @field_validator("exclude_listing_identifiers")
    @classmethod
    def validate_excluded_listing_identifiers(cls, value: Sequence[str]) -> Sequence[str]:
        if any(not identifier.isascii() or not identifier.isdecimal() for identifier in value):
            raise ValueError("Excluded listing identifiers must be decimal strings")
        if len(set(value)) != len(value):
            raise ValueError("Excluded listing identifiers must be unique")
        return value


class RecordWorkspaceBulkReviewResult(StrictModel):
    operation_identifier: str = Field(min_length=1)
    workspace_record_identifier: str = Field(min_length=1)
    selection_kind: str = Field(min_length=1)
    as_of_completion_sequence: int = Field(ge=0)
    candidate_listings_examined: int = Field(ge=0, le=10_000)
    selection_member_count: int = Field(ge=0, le=10_000)
    explicitly_excluded_count: int = Field(ge=0, le=10_000)
    status_excluded_count: int = Field(ge=0, le=10_000)
    review_state_excluded_count: int = Field(ge=0, le=10_000)
    recorded_count: int = Field(ge=0, le=10_000)
    recorded_at_utc: str


class ReviewBatchItem(StrictModel):
    projection: ComposedListingProjection
    review_state: ReviewState
    prior_review: ListingReviewRecord | None
    changed_components: tuple[ProjectionRevisionComponent, ...]


class CreateReviewBatchRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    statuses: Sequence[ListingStatus] = Field(
        default=(ListingStatus.AVAILABLE,), min_length=1, max_length=5
    )
    include_review_states: Sequence[ReviewState] = Field(
        default=(ReviewState.UNREVIEWED, ReviewState.STALE), min_length=1, max_length=3
    )
    page_size: int = Field(default=25, ge=1, le=100)
    cursor: str | None = None
    maximum_scan_pages: int = Field(default=10, ge=1, le=100)

    @field_validator("statuses", "include_review_states")
    @classmethod
    def validate_unique_values(cls, value: Sequence[object]) -> Sequence[object]:
        if len(set(value)) != len(value):
            raise ValueError("Batch filters must be unique")
        return value


class AcquireReviewBatchRequest(CreateReviewBatchRequest):
    request_identifier: str = Field(min_length=1, max_length=200)
    owner_identifier: str = Field(min_length=1, max_length=200)
    lease_duration_seconds: int = Field(default=600, ge=30, le=3_600)

    @field_validator("request_identifier", "owner_identifier")
    @classmethod
    def validate_claim_identifiers(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Claim identifiers must be trimmed")
        return value


class ReviewBatch(StrictModel):
    record_identifier: str = Field(min_length=1)
    workspace_record_identifier: str = Field(min_length=1)
    created_at_utc: str
    items: tuple[ReviewBatchItem, ...] = Field(max_length=100)
    next_cursor: str | None
    scanned_page_count: int = Field(ge=1)


class ReviewClaimLease(StrictModel):
    claim_token: str = Field(min_length=1)
    owner_identifier: str = Field(min_length=1)
    workspace_record_identifier: str = Field(min_length=1)
    batch_record_identifier: str = Field(min_length=1)
    acquired_at_utc_ns: int = Field(ge=0)
    lease_expires_at_utc_ns: int = Field(ge=0)
    listing_identifiers: tuple[str, ...] = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def validate_lease(self) -> ReviewClaimLease:
        if self.lease_expires_at_utc_ns <= self.acquired_at_utc_ns:
            raise ValueError("A review claim must expire after it is acquired")
        if any(
            not identifier.isascii() or not identifier.isdecimal()
            for identifier in self.listing_identifiers
        ):
            raise ValueError("Claimed listing identifiers must be decimal strings")
        if len(set(self.listing_identifiers)) != len(self.listing_identifiers):
            raise ValueError("Claimed listing identifiers must be unique")
        return self


class ReviewBatchAcquisition(StrictModel):
    batch: ReviewBatch
    lease: ReviewClaimLease | None


class RenewReviewClaimRequest(StrictModel):
    request_identifier: str = Field(min_length=1, max_length=200)
    claim_token: str = Field(min_length=1)
    owner_identifier: str = Field(min_length=1, max_length=200)
    lease_duration_seconds: int = Field(default=600, ge=30, le=3_600)

    @field_validator("request_identifier", "owner_identifier")
    @classmethod
    def validate_identifiers(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Claim identifiers must be trimmed")
        return value


class ReleaseReviewClaimRequest(StrictModel):
    request_identifier: str = Field(min_length=1, max_length=200)
    claim_token: str = Field(min_length=1)
    owner_identifier: str = Field(min_length=1, max_length=200)

    @field_validator("request_identifier", "owner_identifier")
    @classmethod
    def validate_identifiers(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Claim identifiers must be trimmed")
        return value


class ReleaseReviewClaimResult(StrictModel):
    claim_token: str = Field(min_length=1)
    released_listing_count: int = Field(ge=1, le=100)
    released_at_utc_ns: int = Field(ge=0)


class CreateReviewWorksetRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    name: str = Field(min_length=1, max_length=200)
    listing_identifiers: Sequence[str] = Field(default=(), max_length=MAXIMUM_WORKSET_LISTINGS)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Workset name must be trimmed")
        return value

    @field_validator("listing_identifiers")
    @classmethod
    def validate_listing_identifiers(cls, value: Sequence[str]) -> Sequence[str]:
        if any(not identifier.isascii() or not identifier.isdecimal() for identifier in value):
            raise ValueError("Workset listing identifiers must be decimal strings")
        if len(set(value)) != len(value):
            raise ValueError("Workset listing identifiers must be unique")
        return value


class UpdateReviewWorksetRequest(StrictModel):
    workset_identifier: str = Field(min_length=1)
    expected_version: int = Field(ge=1)
    add_listing_identifiers: Sequence[str] = Field(default=(), max_length=MAXIMUM_WORKSET_LISTINGS)
    remove_listing_identifiers: Sequence[str] = Field(
        default=(), max_length=MAXIMUM_WORKSET_LISTINGS
    )

    @model_validator(mode="after")
    def validate_changes(self) -> UpdateReviewWorksetRequest:
        additions = tuple(self.add_listing_identifiers)
        removals = tuple(self.remove_listing_identifiers)
        if not additions and not removals:
            raise ValueError("A workset update must add or remove at least one listing")
        for values in (additions, removals):
            if any(not value.isascii() or not value.isdecimal() for value in values):
                raise ValueError("Workset listing identifiers must be decimal strings")
            if len(set(values)) != len(values):
                raise ValueError("Workset changes must not contain duplicates")
        if set(additions) & set(removals):
            raise ValueError("A listing cannot be both added and removed")
        return self


class ReviewWorkset(StrictModel):
    record_identifier: str = Field(min_length=1)
    workset_identifier: str = Field(min_length=1)
    workspace_record_identifier: str = Field(min_length=1)
    name: str = Field(min_length=1)
    version: int = Field(ge=1)
    listing_identifiers: tuple[str, ...] = Field(max_length=MAXIMUM_WORKSET_LISTINGS)
    created_at_utc: str


class ReviewWorksetConflict(StrictModel):
    workset_identifier: str
    expected_version: int
    current_version: int
    current_record_identifier: str


class ListingIdsSelection(StrictModel):
    kind: Literal["listing_ids"] = "listing_ids"
    listing_identifiers: Sequence[str] = Field(min_length=1, max_length=100)

    @field_validator("listing_identifiers")
    @classmethod
    def validate_listing_identifiers(cls, value: Sequence[str]) -> Sequence[str]:
        if any(not identifier.isascii() or not identifier.isdecimal() for identifier in value):
            raise ValueError("Selection listing identifiers must be decimal strings")
        if len(set(value)) != len(value):
            raise ValueError("Selection listing identifiers must be unique")
        return value


class WorksetSelection(StrictModel):
    kind: Literal["workset"] = "workset"
    workset_identifier: str = Field(min_length=1)


class ReviewBatchSelection(StrictModel):
    kind: Literal["review_batch"] = "review_batch"
    review_batch_record_identifier: str = Field(min_length=1)


SelectionSource = Annotated[
    ListingIdsSelection | WorksetSelection | ReviewBatchSelection,
    Field(discriminator="kind"),
]


class CreateSelectionSnapshotRequest(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    selection: SelectionSource


class SelectionSnapshotItem(StrictModel):
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    projection_revision: ProjectionRevision


class SelectionSnapshot(StrictModel):
    record_identifier: str = Field(min_length=1)
    workspace_record_identifier: str = Field(min_length=1)
    created_at_utc: str
    source: SelectionSource
    items: tuple[SelectionSnapshotItem, ...] = Field(min_length=1, max_length=100)


class ReviewBatchSummary(StrictModel):
    record_identifier: str = Field(min_length=1)
    created_at_utc: str
    item_count: int = Field(ge=0, le=100)
    next_cursor: str | None


class ReviewWorksetSummary(StrictModel):
    record_identifier: str = Field(min_length=1)
    workset_identifier: str = Field(min_length=1)
    name: str = Field(min_length=1)
    version: int = Field(ge=1)
    member_count: int = Field(ge=0, le=MAXIMUM_WORKSET_LISTINGS)


class SelectionSnapshotSummary(StrictModel):
    record_identifier: str = Field(min_length=1)
    created_at_utc: str
    source_kind: str = Field(min_length=1)
    item_count: int = Field(ge=1, le=100)


class ReviewClaimSummary(StrictModel):
    batch_record_identifier: str = Field(min_length=1)
    owner_identifier: str = Field(min_length=1)
    lease_expires_at_utc_ns: int = Field(ge=0)
    claimed_listing_count: int = Field(ge=1, le=100)


class ReviewWorkspaceActivity(StrictModel):
    workspace: ReviewWorkspace
    recent_batches: tuple[ReviewBatchSummary, ...] = Field(max_length=100)
    recent_reviews: tuple[ListingReviewRecord, ...] = Field(max_length=500)
    current_worksets: tuple[ReviewWorksetSummary, ...] = Field(max_length=1_000)
    recent_selection_snapshots: tuple[SelectionSnapshotSummary, ...] = Field(max_length=100)
    active_claims: tuple[ReviewClaimSummary, ...] = Field(default=(), max_length=100)


class WorkspaceWorkStatus(StrictModel):
    workspace_record_identifier: str = Field(min_length=1)
    captured_at_utc_ns: int = Field(ge=0)
    queued_count: int = Field(ge=0)
    in_progress_count: int = Field(ge=0)
    terminal_failure_count: int = Field(ge=0)
    active_work: tuple[WorkActivity, ...] = Field(max_length=100)
    active_work_truncated: bool
    failed_work: tuple[WorkActivity, ...] = Field(max_length=100)
    failed_work_truncated: bool
    idle: bool
    successful: bool


class WorkspaceWorkWaitResult(StrictModel):
    status: WorkspaceWorkStatus
    timed_out: bool
    waited_seconds: float = Field(ge=0)


def review_mutation_request_sha256(action: str, request: StrictModel) -> str:
    """Hash one validated caller intent independently of generated mutation results."""

    content: dict[str, JsonValue] = {
        "action": action,
        "request_schema_version": 1,
        "request": request.model_dump(mode="json", exclude={"request_identifier"}),
    }
    encoded = json.dumps(
        content,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def changed_projection_components(
    previous: ProjectionRevision, current: ProjectionRevision
) -> tuple[ProjectionRevisionComponent, ...]:
    return tuple(
        component
        for component, attribute in (
            (ProjectionRevisionComponent.STATUS, "status_sha256"),
            (ProjectionRevisionComponent.SCALAR_FIELDS, "scalar_fields_sha256"),
            (ProjectionRevisionComponent.PREVIEW_IMAGE, "preview_image_sha256"),
            (ProjectionRevisionComponent.GALLERY, "gallery_sha256"),
            (ProjectionRevisionComponent.ANALYSES, "analyses_sha256"),
            (ProjectionRevisionComponent.SEARCH_MEMBERSHIP, "search_membership_sha256"),
        )
        if getattr(previous, attribute) != getattr(current, attribute)
    )


def classify_review_state(
    *,
    current_revision: ProjectionRevision,
    previous_review: ListingReviewRecord | None,
    policy: ReviewStalenessPolicy,
) -> tuple[ReviewState, tuple[ProjectionRevisionComponent, ...]]:
    if previous_review is None:
        return ReviewState.UNREVIEWED, ()
    if not previous_review.inspected:
        return ReviewState.UNREVIEWED, ()
    if previous_review.projection_revision.recipe_version != current_revision.recipe_version:
        return ReviewState.STALE, tuple(policy.components)
    changed = changed_projection_components(previous_review.projection_revision, current_revision)
    relevant = tuple(component for component in changed if component in policy.components)
    return (ReviewState.STALE if relevant else ReviewState.CURRENT), relevant
