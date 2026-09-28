"""Thin official-SDK MCP boundary over Carl's review application."""

from __future__ import annotations

import base64
import sys
import traceback
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Annotated, Any
from uuid import uuid4

import anyio
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.context import Context
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ImageContent, ToolAnnotations
from pydantic import Field, field_validator

from carl.core.activity import ActivitySnapshot
from carl.core.analysis_batch import (
    SelectionAnalysesPreview,
    SelectionAnalysesRequest,
    SelectionAnalysesRequestResult,
)
from carl.core.composed_projection import (
    ComposedListingPage,
    ComposedListingProjection,
    GetComposedListingRequest,
    ListComposedSearchRequest,
)
from carl.core.facebook_images import RetryImageFailuresRequest, RetryImageFailuresResult
from carl.core.facebook_refresh import (
    SearchRefreshRequest,
    SearchRefreshRequestResult,
    SearchRunListingsPage,
    SearchRunPage,
)
from carl.core.facebook_work import CreateSearchRequest, CreateSearchResult
from carl.core.models import StrictModel
from carl.core.review import (
    AnalysisReport,
    CreateProductGuideRequest,
    ProductGuideConflict,
    ProductGuideDetails,
    ProductGuideMutationResult,
    ProductGuideSummary,
    ProvenanceObject,
    ReviseProductGuideRequest,
    ServerInfo,
    SetProductGuideIdentityRetiredRequest,
    WorkStatus,
)
from carl.core.review_workspace import (
    AcquireReviewBatchRequest,
    AddWorkspaceProductGuideRequest,
    CreateReviewWorksetRequest,
    CreateReviewWorkspaceRequest,
    CreateSelectionSnapshotRequest,
    CreateWorkspaceSearchRequest,
    CreateWorkspaceSearchResult,
    GetWorkspaceListingRequest,
    ListingReviewInput,
    ListWorkspaceListingsRequest,
    RecordListingReviewsRequest,
    RecordListingReviewsResult,
    ReleaseReviewClaimRequest,
    ReleaseReviewClaimResult,
    RenameReviewWorkspaceRequest,
    RenewReviewClaimRequest,
    RequestWorkspaceRefreshRequest,
    RequestWorkspaceRefreshResult,
    RetryWorkspaceSearchTrackRequest,
    RetryWorkspaceSearchTrackResult,
    ReviewBatch,
    ReviewBatchAcquisition,
    ReviewClaimLease,
    ReviewWorkset,
    ReviewWorksetConflict,
    ReviewWorkspace,
    ReviewWorkspaceActivity,
    SelectionSnapshot,
    SetReviewWorkspaceArchivedRequest,
    SetWorkspaceDefaultProductGuideRequest,
    SetWorkspaceSearchTrackEnabledRequest,
    UpdateReviewWorksetRequest,
    UpdateWorkspaceProductGuideBindingRequest,
    WorkspaceProductGuideBinding,
    WorkspaceSearchTrack,
    WorkspaceWorkStatus,
    WorkspaceWorkWaitResult,
)
from carl.io.claude import ClaudeCli
from carl.io.provenance import collect_code_provenance_async, source_tree_sha256_async
from carl.io.sqlite import Database
from carl.review import IncompleteGalleryError, ReviewApplication, ReviewInputError

_READ_ONLY = ToolAnnotations(
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
    open_world_hint=False,
)
_LOCAL_MUTATION = ToolAnnotations(
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=False,
    open_world_hint=False,
)
_IDEMPOTENT_LOCAL_MUTATION = ToolAnnotations(
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=True,
    open_world_hint=False,
)
_OPEN_WORLD_MUTATION = ToolAnnotations(
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=False,
    open_world_hint=True,
)


class RecordClaimedListingReviewsRequest(StrictModel):
    """MCP review write requiring the coordinated claim-backed workflow."""

    request_identifier: str = Field(min_length=1, max_length=200)
    workspace_record_identifier: str = Field(min_length=1)
    batch_record_identifier: str = Field(min_length=1)
    claim_token: str = Field(min_length=1)
    claim_owner_identifier: str = Field(min_length=1, max_length=200)
    reviews: Sequence[ListingReviewInput] = Field(min_length=1, max_length=100)

    @field_validator("request_identifier", "claim_owner_identifier")
    @classmethod
    def validate_identifiers(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Request and owner identifiers must be trimmed")
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


_INSTRUCTIONS = """Use Carl to collect and review retained Marketplace evidence.
Diagnose with get_server_info. Discover queued, active, and recent work and every local process with
this database open using get_activity_snapshot, then pass an exact full identifier to
get_work_status for compact progress. Request full details only when raw payload, checkpoint result,
error, or operation history is needed. list_search_runs includes the producing attempt's start and
completion timestamps plus whether the run originated as a fresh search or refresh. get_provenance
does not contain a search run's consolidated listing membership; page through the exact retained IDs
with get_search_run_listings. get_provenance
omits sibling output edges by default while reporting their count; request a bounded number only when
needed. Queue a new bounded search with create_search, or refresh an exact list_search_runs record,
then poll the returned work. Refresh reuses the latest usable retained item page for each exact listing
ID and spends Decodo only on listings without one.
Transient search and image transport/session failures retry with exponential backoff; a failed
shared Proton transport is replaced before another attempt. Use retry_image_failures with an exact search
run or refresh identifier to requeue older terminal image work after checking or correcting its
failure cause. The retry is one bounded database transaction and upgrades selected image work to
schema v2 so pre-fix schema-v1 workers cannot claim it; current workers still accept legacy v1 work.
MCP servers do not run workers. Keep `carl work` or `carl monitor --work` running while queued work
should progress. Review with list_composed_search or get_composed_listing, fetching full reports or
images only when needed. These read only retained database evidence, make no network requests, and
compose a fixed as-of snapshot on demand. list_composed_search defaults to available listings only;
override filters.statuses explicitly for pending, sold, unavailable, or unknown listings. Older
completed analyses are reused under the initial permissive policy and labeled assumed. Search
cards supply fallback scalar details and a separately labeled preview_image when item-page evidence
is absent. Price is the exception to item-page precedence: the newest actual item-page or search-card
price observation wins. A refresh updates price only for listings it actually observes; omission from
a later search neither refreshes nor clears an older price. Inspect the selected price evidence for
its source and observation time. A preview is not a complete gallery, and card prices may lack
currency. Search membership absence is not treated as proof that a listing is gone. Select an exact product-guide
record before analysis, and preserve record identifiers in conclusions. When creating a guide,
pass only its identity suffix; Carl adds the carl/product_guide namespace. Retire an unused identity
with set_product_guide_identity_retired after disabling all of its workspace bindings. Exact retired
versions remain readable, and include_retired=true reveals them in list_product_guides.
For multi-step agent review, create a review workspace from one search run and optional guide. That
run becomes its first search track. Add another phrase with create_workspace_search; after it
completes, refresh that track with request_workspace_refresh before requesting analysis. Refreshes
advance the selected track without replacing the workspace. Use list_workspace_listings for the
available-only deduplicated union of every track and its refresh ancestry; search absence alone does
not mark an older listing unavailable, and use get_workspace_listing for exact drill-down in the
same scope. Use set_workspace_search_track_enabled to remove a track from
or restore it to the active union without deleting its retained history.
Search acquisition is limited to one active job per proxy route. If an initial track search
exhausts a transient transport or session failure, get_workspace_work_status reports it under
failed_work and successful=false; call retry_workspace_search_track with that stable track ID to
give the same track a fresh retry budget. Retried legacy tracks are upgraded with the current
search-route scheduler scope. A transport failure invalidates the shared Proton transport and
backs off every queued search on that route before a replacement transport is used.
Rename a workspace with rename_review_workspace. Archive or restore it with
set_review_workspace_archived; ordinary list_review_workspaces calls hide archived workspaces, while
include_archived=true reveals them and exact workspace operations remain available.
Attach multiple guides with add_workspace_product_guide. A binding is either pinned to one exact
record or follows the latest version of one guide identity; list_workspace_product_guides reports
the exact version currently resolved. Use update_workspace_product_guide_binding to rename,
enable, disable, pin, or advance it, and set_workspace_default_product_guide for the ordinary
workspace view. Analysis requests may override that default with either
product_guide_binding_identifier or an exact product_guide_record_identifier; every preview and
queued result reports both the binding and exact resolved guide record. Use
missing_for_selected_guide to reuse completed analysis under that exact guide across retained
observations. Preview reports new, reused, and excluded counts before work is queued.
After refresh work completes, use acquire_review_batch with its default unreviewed and stale states
to obtain the ordinary review set: newly discovered listings have no prior review, while listings
whose review-relevant projection components changed are stale. Unobserved listings are not classified
as changed merely because a search missed them. When several agents may review concurrently, use
acquire_review_batch with a stable, caller-generated
request_identifier and owner_identifier. It atomically issues a durable batch and claims only listings
not held by another active agent. Replay the same request identifier and unchanged request to recover
the exact response after interruption. Renew longer work with renew_review_claim, pass the claim token
and owner when recording reviews, using a new stable request identifier for that mutation, and release
unfinished members with release_review_claim. Successful review recording releases only the submitted
members; replay its unchanged request identifier after a lost response. MCP review writes require an
active claim-backed batch; uncoordinated batch issuance and direct review writes are not exposed.
Batches distinguish issued work from inspected work. Use
get_review_workspace_activity to rediscover recent batches, reviews, worksets, snapshots, and active
claim summaries; claim tokens are intentionally omitted, so retain the acquisition response or replay
its request. Reuse overlapping static worksets for agent-created groupings, and freeze an exact bounded
group as a selection snapshot before a later bulk operation. Projection revisions report whether a
prior review is current or stale under the workspace's component policy; response size limits and
unrelated database activity do not change those revisions.
Preview analysis with preview_selection_analyses, then queue the unchanged intent with
request_selection_analyses. The workspace supplies the exact refreshed search run and product guide;
the selection may be the whole workspace, a claimed review batch, a workset, a frozen selection
snapshot, or an explicit bounded ID list. Available status is the default. A workspace based only on
an initial search must be replaced by one based on its completed refresh before analysis.
Preview and request scan at most maximum_candidate_listings_examined workspace members (2,500 by
default) through a status-only indexed path. Check candidate_examination_limit_reached; increase the
bound deliberately when an exact whole-workspace plan is required.
Poll the returned work identifier with get_work_status when detailed diagnostics are needed. Prefer
get_workspace_work_status to check all root work requested by a workspace, or
wait_for_workspace_work for a short progress-reporting wait until it is idle. It defaults to 30
seconds, accepts at most 300 seconds, and returns the still-active status at timeout so callers can
poll again. Idle only means that no work is active; inspect successful and
terminal_failure_count to distinguish successful completion from settled failures. Direct
observation-level analysis requests are not exposed through MCP.
"""


async def _expected[T](operation: Callable[[], Awaitable[T]]) -> T:
    try:
        return await operation()
    except IncompleteGalleryError as error:
        if error.gallery_absence_reason is not None:
            message = (
                f"The selected observation has no usable gallery ({error.gallery_absence_reason}). "
                "Set allow_incomplete_gallery=true to request text-only analysis."
            )
            raise ToolError(message) from error
        positions = ", ".join(str(value) for value in error.unavailable_gallery_orders)
        message = (
            f"The selected observation has unavailable gallery positions ({positions}). "
            "Set allow_incomplete_gallery=true to analyze the retained subset."
        )
        raise ToolError(message) from error
    except ReviewInputError as error:
        raise ToolError(str(error)) from error
    except KeyError as error:
        identifier = error.args[0] if error.args else "requested object"
        raise ToolError(f"Carl object was not found: {identifier}") from error
    except Exception as error:
        failure_identifier = str(uuid4())
        print(
            f"Carl MCP tool failure {failure_identifier} ({type(error).__name__})",
            file=sys.stderr,
            flush=True,
        )
        traceback.print_exception(error, file=sys.stderr)
        sys.stderr.flush()
        raise ToolError(
            f"Carl encountered an internal error ({failure_identifier}); see server stderr"
        ) from error


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    name: str
    operation: tuple[str, ...]
    function: Callable[..., Any]
    description: str
    annotations: ToolAnnotations
    structured_output: bool = True


def _guide_result(
    value: ProductGuideDetails | ProductGuideConflict,
) -> ProductGuideMutationResult:
    if isinstance(value, ProductGuideConflict):
        return ProductGuideMutationResult(conflict=value)
    return ProductGuideMutationResult(product_guide=value)


async def _ready_operation[T](
    before_tool_call: Callable[[], Awaitable[None]],
    operation: Callable[[], Awaitable[T]],
) -> T:
    await before_tool_call()
    return await operation()


async def _already_ready() -> None:
    return None


def tool_definitions(
    application: ReviewApplication,
    *,
    before_tool_call: Callable[[], Awaitable[None]] = _already_ready,
) -> tuple[ToolDefinition, ...]:
    """Construct the immutable, duplicate-checked Carl MCP tool registry."""

    async def expected[T](operation: Callable[[], Awaitable[T]]) -> T:
        return await _expected(lambda: _ready_operation(before_tool_call, operation))

    async def get_server_info() -> ServerInfo:
        """Identify this live Carl process, its source, database, and capabilities."""

        return await expected(application.get_server_info)

    async def get_activity_snapshot(
        recent_window_minutes: Annotated[int, Field(ge=1, le=7 * 24 * 60)] = 60,
        maximum_rows: Annotated[int, Field(ge=1, le=100)] = 20,
    ) -> ActivitySnapshot:
        """Discover active work, recent outcomes, and network activity in one bounded snapshot."""

        return await expected(
            lambda: application.get_activity_snapshot(recent_window_minutes, maximum_rows)
        )

    async def get_composed_listing(
        request: GetComposedListingRequest,
    ) -> ComposedListingProjection:
        """Compose one listing from retained evidence at a fixed database boundary."""

        return await expected(lambda: application.get_composed_listing(request))

    async def list_composed_search(
        request: ListComposedSearchRequest,
    ) -> ComposedListingPage:
        """Page through an on-demand search projection, available-only by default."""

        return await expected(lambda: application.list_composed_search(request))

    async def create_review_workspace(
        request: CreateReviewWorkspaceRequest,
    ) -> ReviewWorkspace:
        """Create a durable review context for one search run and optional product guide."""

        return await expected(lambda: application.create_review_workspace(request))

    async def list_review_workspaces(
        include_archived: bool = False,
    ) -> tuple[ReviewWorkspace, ...]:
        """List active workspaces by default, optionally including archived workspaces."""

        return await expected(
            lambda: application.list_review_workspaces(include_archived=include_archived)
        )

    async def rename_review_workspace(
        workspace_record_identifier: str,
        name: str,
    ) -> ReviewWorkspace:
        """Rename a workspace without changing its stable identity or retained history."""

        request = RenameReviewWorkspaceRequest(
            workspace_record_identifier=workspace_record_identifier,
            name=name,
        )
        return await expected(lambda: application.rename_review_workspace(request))

    async def set_review_workspace_archived(
        workspace_record_identifier: str,
        archived: bool,
    ) -> ReviewWorkspace:
        """Archive or restore a workspace; archived workspaces remain exactly addressable."""

        request = SetReviewWorkspaceArchivedRequest(
            workspace_record_identifier=workspace_record_identifier,
            archived=archived,
        )
        return await expected(lambda: application.set_review_workspace_archived(request))

    async def add_workspace_product_guide(
        request: AddWorkspaceProductGuideRequest,
    ) -> WorkspaceProductGuideBinding:
        """Attach a pinned or follow-latest product guide to a review workspace."""

        return await expected(lambda: application.add_workspace_product_guide(request))

    async def update_workspace_product_guide_binding(
        request: UpdateWorkspaceProductGuideBindingRequest,
    ) -> WorkspaceProductGuideBinding:
        """Rename, enable, disable, pin, or advance one workspace guide binding."""

        return await expected(lambda: application.update_workspace_product_guide_binding(request))

    async def set_workspace_default_product_guide(
        request: SetWorkspaceDefaultProductGuideRequest,
    ) -> ReviewWorkspace:
        """Choose or clear the guide binding used by default for workspace operations."""

        return await expected(lambda: application.set_workspace_default_product_guide(request))

    async def list_workspace_product_guides(
        workspace_record_identifier: str,
        include_disabled: bool = False,
    ) -> tuple[WorkspaceProductGuideBinding, ...]:
        """List enabled workspace guide bindings, optionally including disabled bindings."""

        return await expected(
            lambda: application.list_workspace_product_guides(
                workspace_record_identifier,
                include_disabled=include_disabled,
            )
        )

    async def list_workspace_listings(
        request: ListWorkspaceListingsRequest,
    ) -> ComposedListingPage:
        """Page through the available-only deduplicated union of workspace search tracks."""

        return await expected(lambda: application.list_workspace_listings(request))

    async def get_workspace_listing(
        request: GetWorkspaceListingRequest,
    ) -> ComposedListingProjection:
        """Compose one exact listing using a workspace's active tracks and product guide."""

        return await expected(lambda: application.get_workspace_listing(request))

    async def create_workspace_search(
        request: CreateWorkspaceSearchRequest,
    ) -> CreateWorkspaceSearchResult:
        """Queue a new search phrase as another search track in a workspace."""

        return await expected(lambda: application.create_workspace_search(request))

    async def retry_workspace_search_track(
        request: RetryWorkspaceSearchTrackRequest,
    ) -> RetryWorkspaceSearchTrackResult:
        """Retry a workspace track creation that exhausted a transient network failure."""

        return await expected(lambda: application.retry_workspace_search_track(request))

    async def request_workspace_refresh(
        request: RequestWorkspaceRefreshRequest,
    ) -> RequestWorkspaceRefreshResult:
        """Queue a refresh of one workspace search track, defaulting when only one exists."""

        return await expected(lambda: application.request_workspace_refresh(request))

    async def set_workspace_search_track_enabled(
        request: SetWorkspaceSearchTrackEnabledRequest,
    ) -> WorkspaceSearchTrack:
        """Enable or disable a workspace search track while retaining its history."""

        return await expected(lambda: application.set_workspace_search_track_enabled(request))

    async def get_review_workspace_activity(
        workspace_record_identifier: str,
        maximum_recent_batches: Annotated[int, Field(ge=0, le=100)] = 20,
        maximum_recent_reviews: Annotated[int, Field(ge=0, le=500)] = 100,
        maximum_recent_selection_snapshots: Annotated[int, Field(ge=0, le=100)] = 20,
    ) -> ReviewWorkspaceActivity:
        """Rediscover recent batches, reviews, worksets, and snapshots in one workspace."""

        return await expected(
            lambda: application.get_review_workspace_activity(
                workspace_record_identifier,
                maximum_recent_batches=maximum_recent_batches,
                maximum_recent_reviews=maximum_recent_reviews,
                maximum_recent_selection_snapshots=(maximum_recent_selection_snapshots),
            )
        )

    async def get_workspace_work_status(
        workspace_record_identifier: str,
        maximum_active_work: Annotated[int, Field(ge=1, le=100)] = 20,
        maximum_failed_work: Annotated[int, Field(ge=1, le=100)] = 20,
    ) -> WorkspaceWorkStatus:
        """Check active and failed root work; successful is false if any request failed."""

        return await expected(
            lambda: application.get_workspace_work_status(
                workspace_record_identifier,
                maximum_active_work=maximum_active_work,
                maximum_failed_work=maximum_failed_work,
            )
        )

    async def wait_for_workspace_work(
        workspace_record_identifier: str,
        context: Context,
        timeout_seconds: Annotated[float, Field(ge=0, le=300)] = 30,
        maximum_active_work: Annotated[int, Field(ge=1, le=100)] = 20,
        maximum_failed_work: Annotated[int, Field(ge=1, le=100)] = 20,
    ) -> WorkspaceWorkWaitResult:
        """Wait up to five minutes for workspace work, reporting progress every five seconds."""

        async def report_progress(status: WorkspaceWorkStatus, waited_seconds: float) -> None:
            await context.report_progress(
                waited_seconds,
                timeout_seconds,
                (f"{status.queued_count} queued, {status.in_progress_count} in progress"),
            )

        return await expected(
            lambda: application.wait_for_workspace_work(
                workspace_record_identifier,
                timeout_seconds=timeout_seconds,
                maximum_active_work=maximum_active_work,
                maximum_failed_work=maximum_failed_work,
                progress=report_progress,
            )
        )

    async def acquire_review_batch(
        request: AcquireReviewBatchRequest,
    ) -> ReviewBatchAcquisition:
        """Idempotently issue a batch and claim its listings for one concurrent agent."""

        return await expected(lambda: application.acquire_review_batch(request))

    async def renew_review_claim(request: RenewReviewClaimRequest) -> ReviewClaimLease:
        """Idempotently extend an active review claim owned by this agent."""

        return await expected(lambda: application.renew_review_claim(request))

    async def release_review_claim(
        request: ReleaseReviewClaimRequest,
    ) -> ReleaseReviewClaimResult:
        """Idempotently release every unfinished member of one active review claim."""

        return await expected(lambda: application.release_review_claim(request))

    async def get_review_batch(record_identifier: str) -> ReviewBatch:
        """Resume one exact durable review batch."""

        return await expected(lambda: application.get_review_batch(record_identifier))

    async def record_listing_reviews(
        request: RecordClaimedListingReviewsRequest,
    ) -> RecordListingReviewsResult:
        """Idempotently record inspected state through an active claimed review batch."""

        application_request = RecordListingReviewsRequest.model_validate(request.model_dump())
        return await expected(lambda: application.record_listing_reviews(application_request))

    async def create_review_workset(
        request: CreateReviewWorksetRequest,
    ) -> ReviewWorkset | ReviewWorksetConflict:
        """Create a reusable static listing group; worksets may overlap."""

        return await expected(lambda: application.create_review_workset(request))

    async def update_review_workset(
        request: UpdateReviewWorksetRequest,
    ) -> ReviewWorkset | ReviewWorksetConflict:
        """Add or remove members using optimistic workset version control."""

        return await expected(lambda: application.update_review_workset(request))

    async def list_review_worksets(
        workspace_record_identifier: str,
    ) -> tuple[ReviewWorkset, ...]:
        """List the current versions of a workspace's reusable static groups."""

        return await expected(lambda: application.list_review_worksets(workspace_record_identifier))

    async def create_selection_snapshot(
        request: CreateSelectionSnapshotRequest,
    ) -> SelectionSnapshot:
        """Freeze exact listing identities and projection revisions for a later operation."""

        return await expected(lambda: application.create_selection_snapshot(request))

    async def get_selection_snapshot(record_identifier: str) -> SelectionSnapshot:
        """Get one exact frozen selection and its listing revisions."""

        return await expected(lambda: application.get_selection_snapshot(record_identifier))

    async def list_search_runs(query: str | None = None, limit: int = 50) -> SearchRunPage:
        """List recent retained searches so an exact run can be selected for refresh."""

        return await expected(lambda: application.list_search_runs(query, limit))

    async def get_search_run_listings(
        search_run_record_identifier: str,
        offset: Annotated[int, Field(ge=0)] = 0,
        limit: Annotated[int, Field(ge=1, le=500)] = 100,
    ) -> SearchRunListingsPage:
        """Page through the exact listing IDs returned by one retained search run."""

        return await expected(
            lambda: application.get_search_run_listings(search_run_record_identifier, offset, limit)
        )

    async def create_search(request: CreateSearchRequest) -> CreateSearchResult:
        """Queue one new bounded Marketplace search for the explicit worker pool."""

        return await expected(lambda: application.create_search(request))

    async def request_search_refresh(
        request: SearchRefreshRequest,
    ) -> SearchRefreshRequestResult:
        """Queue a durable search, detail-page, and missing-image refresh workflow."""

        return await expected(lambda: application.request_search_refresh(request))

    async def preview_selection_analyses(
        request: SelectionAnalysesRequest,
    ) -> SelectionAnalysesPreview:
        """Preview analysis counts for a workspace, batch, workset, snapshot, or ID list."""

        return await expected(lambda: application.preview_selection_analyses(request))

    async def request_selection_analyses(
        request: SelectionAnalysesRequest,
    ) -> SelectionAnalysesRequestResult:
        """Queue analysis for a workspace-native selection using its exact guide and search."""

        return await expected(lambda: application.request_selection_analyses(request))

    async def get_listing_analysis(record_identifier: str) -> AnalysisReport:
        """Get one complete retained analysis report by its exact record identifier."""

        return await expected(lambda: application.get_listing_analysis(record_identifier))

    async def get_provenance(
        object_identifier: str,
        maximum_output_edges: Annotated[int | None, Field(ge=0, le=100)] = 0,
    ) -> ProvenanceObject:
        """Get provenance with no outputs by default, a bounded number, or all with null."""

        return await expected(
            lambda: application.get_provenance(object_identifier, maximum_output_edges)
        )

    async def get_listing_image(artifact_identifier: str) -> ImageContent:
        """Return one exact validated listing image artifact for multimodal inspection."""

        image = await expected(lambda: application.get_image(artifact_identifier))
        return ImageContent(
            data=base64.b64encode(image.content).decode("ascii"),
            mime_type=image.media_type,
            _meta={
                "carl/artifactIdentifier": image.artifact_identifier,
                "carl/sha256": image.sha256,
            },
        )

    async def list_product_guides(
        include_retired: bool = False,
    ) -> tuple[ProductGuideSummary, ...]:
        """List guide versions available for analysis, optionally including retired identities."""

        return await expected(
            lambda: application.list_product_guides(include_retired=include_retired)
        )

    async def get_product_guide(record_identifier: str) -> ProductGuideDetails:
        """Get an exact product-guide version and its retained text."""

        return await expected(lambda: application.get_product_guide(record_identifier))

    async def create_product_guide(
        request: CreateProductGuideRequest,
    ) -> ProductGuideMutationResult:
        """Create a guide; pass only the identity suffix because Carl adds carl/product_guide."""

        return _guide_result(await expected(lambda: application.create_product_guide(request)))

    async def revise_product_guide(
        request: ReviseProductGuideRequest,
    ) -> ProductGuideMutationResult:
        """Create a new guide version if the expected base is still current."""

        return _guide_result(await expected(lambda: application.revise_product_guide(request)))

    async def set_product_guide_identity_retired(
        request: SetProductGuideIdentityRetiredRequest,
    ) -> ProductGuideSummary:
        """Retire or restore a whole guide identity while preserving every exact version."""

        return await expected(lambda: application.set_product_guide_identity_retired(request))

    async def retry_image_failures(
        request: RetryImageFailuresRequest,
    ) -> RetryImageFailuresResult:
        """Requeue terminal image work linked to one exact search run or refresh."""

        return await expected(lambda: application.retry_image_failures(request))

    async def get_work_status(
        work_identifier: str,
        include_details: bool = False,
    ) -> WorkStatus:
        """Get compact live progress; include details only for raw durable diagnostic fields."""

        return await expected(lambda: application.get_work_status(work_identifier, include_details))

    definitions = (
        ToolDefinition(
            "get_server_info",
            ("carl", "mcp", "get_server_info"),
            get_server_info,
            get_server_info.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "get_activity_snapshot",
            ("carl", "activity", "get_snapshot"),
            get_activity_snapshot,
            get_activity_snapshot.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "get_composed_listing",
            ("carl", "review", "get_composed_listing"),
            get_composed_listing,
            get_composed_listing.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "list_composed_search",
            ("carl", "review", "list_composed_search"),
            list_composed_search,
            list_composed_search.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "create_review_workspace",
            ("carl", "review", "create_workspace"),
            create_review_workspace,
            create_review_workspace.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "list_review_workspaces",
            ("carl", "review", "list_workspaces"),
            list_review_workspaces,
            list_review_workspaces.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "rename_review_workspace",
            ("carl", "review", "rename_workspace"),
            rename_review_workspace,
            rename_review_workspace.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "set_review_workspace_archived",
            ("carl", "review", "set_workspace_archived"),
            set_review_workspace_archived,
            set_review_workspace_archived.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "add_workspace_product_guide",
            ("carl", "review", "add_workspace_product_guide"),
            add_workspace_product_guide,
            add_workspace_product_guide.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "update_workspace_product_guide_binding",
            ("carl", "review", "update_workspace_product_guide_binding"),
            update_workspace_product_guide_binding,
            update_workspace_product_guide_binding.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "set_workspace_default_product_guide",
            ("carl", "review", "set_workspace_default_product_guide"),
            set_workspace_default_product_guide,
            set_workspace_default_product_guide.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "list_workspace_product_guides",
            ("carl", "review", "list_workspace_product_guides"),
            list_workspace_product_guides,
            list_workspace_product_guides.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "list_workspace_listings",
            ("carl", "review", "list_workspace_listings"),
            list_workspace_listings,
            list_workspace_listings.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "get_workspace_listing",
            ("carl", "review", "get_workspace_listing"),
            get_workspace_listing,
            get_workspace_listing.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "create_workspace_search",
            ("carl", "review", "create_workspace_search"),
            create_workspace_search,
            create_workspace_search.__doc__ or "",
            _OPEN_WORLD_MUTATION,
        ),
        ToolDefinition(
            "retry_workspace_search_track",
            ("carl", "review", "retry_workspace_search_track"),
            retry_workspace_search_track,
            retry_workspace_search_track.__doc__ or "",
            _OPEN_WORLD_MUTATION,
        ),
        ToolDefinition(
            "request_workspace_refresh",
            ("carl", "review", "request_workspace_refresh"),
            request_workspace_refresh,
            request_workspace_refresh.__doc__ or "",
            _OPEN_WORLD_MUTATION,
        ),
        ToolDefinition(
            "set_workspace_search_track_enabled",
            ("carl", "review", "set_workspace_search_track_enabled"),
            set_workspace_search_track_enabled,
            set_workspace_search_track_enabled.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "get_review_workspace_activity",
            ("carl", "review", "get_workspace_activity"),
            get_review_workspace_activity,
            get_review_workspace_activity.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "get_workspace_work_status",
            ("carl", "review", "get_workspace_work_status"),
            get_workspace_work_status,
            get_workspace_work_status.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "wait_for_workspace_work",
            ("carl", "review", "wait_for_workspace_work"),
            wait_for_workspace_work,
            wait_for_workspace_work.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "acquire_review_batch",
            ("carl", "review", "acquire_batch"),
            acquire_review_batch,
            acquire_review_batch.__doc__ or "",
            _IDEMPOTENT_LOCAL_MUTATION,
        ),
        ToolDefinition(
            "renew_review_claim",
            ("carl", "review", "renew_claim"),
            renew_review_claim,
            renew_review_claim.__doc__ or "",
            _IDEMPOTENT_LOCAL_MUTATION,
        ),
        ToolDefinition(
            "release_review_claim",
            ("carl", "review", "release_claim"),
            release_review_claim,
            release_review_claim.__doc__ or "",
            _IDEMPOTENT_LOCAL_MUTATION,
        ),
        ToolDefinition(
            "get_review_batch",
            ("carl", "review", "get_batch"),
            get_review_batch,
            get_review_batch.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "record_listing_reviews",
            ("carl", "review", "record_listing_reviews"),
            record_listing_reviews,
            record_listing_reviews.__doc__ or "",
            _IDEMPOTENT_LOCAL_MUTATION,
        ),
        ToolDefinition(
            "create_review_workset",
            ("carl", "review", "create_workset"),
            create_review_workset,
            create_review_workset.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "update_review_workset",
            ("carl", "review", "update_workset"),
            update_review_workset,
            update_review_workset.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "list_review_worksets",
            ("carl", "review", "list_worksets"),
            list_review_worksets,
            list_review_worksets.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "create_selection_snapshot",
            ("carl", "review", "create_selection_snapshot"),
            create_selection_snapshot,
            create_selection_snapshot.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "get_selection_snapshot",
            ("carl", "review", "get_selection_snapshot"),
            get_selection_snapshot,
            get_selection_snapshot.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "list_search_runs",
            ("carl", "facebook", "list_search_runs"),
            list_search_runs,
            list_search_runs.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "get_search_run_listings",
            ("carl", "facebook", "get_search_run_listings"),
            get_search_run_listings,
            get_search_run_listings.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "create_search",
            ("carl", "facebook", "create_search"),
            create_search,
            create_search.__doc__ or "",
            _OPEN_WORLD_MUTATION,
        ),
        ToolDefinition(
            "request_search_refresh",
            ("carl", "facebook", "request_search_refresh"),
            request_search_refresh,
            request_search_refresh.__doc__ or "",
            _OPEN_WORLD_MUTATION,
        ),
        ToolDefinition(
            "preview_selection_analyses",
            ("carl", "review", "preview_selection_analyses"),
            preview_selection_analyses,
            preview_selection_analyses.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "request_selection_analyses",
            ("carl", "review", "request_selection_analyses"),
            request_selection_analyses,
            request_selection_analyses.__doc__ or "",
            _OPEN_WORLD_MUTATION,
        ),
        ToolDefinition(
            "get_listing_analysis",
            ("carl", "review", "get_listing_analysis"),
            get_listing_analysis,
            get_listing_analysis.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "get_provenance",
            ("carl", "review", "get_provenance"),
            get_provenance,
            get_provenance.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "get_listing_image",
            ("carl", "review", "get_listing_image"),
            get_listing_image,
            get_listing_image.__doc__ or "",
            _READ_ONLY,
            structured_output=False,
        ),
        ToolDefinition(
            "list_product_guides",
            ("carl", "review", "list_product_guides"),
            list_product_guides,
            list_product_guides.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "get_product_guide",
            ("carl", "review", "get_product_guide"),
            get_product_guide,
            get_product_guide.__doc__ or "",
            _READ_ONLY,
        ),
        ToolDefinition(
            "create_product_guide",
            ("carl", "review", "create_product_guide"),
            create_product_guide,
            create_product_guide.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "revise_product_guide",
            ("carl", "review", "revise_product_guide"),
            revise_product_guide,
            revise_product_guide.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "set_product_guide_identity_retired",
            ("carl", "review", "set_product_guide_identity_retired"),
            set_product_guide_identity_retired,
            set_product_guide_identity_retired.__doc__ or "",
            _LOCAL_MUTATION,
        ),
        ToolDefinition(
            "retry_image_failures",
            ("carl", "facebook", "retry_image_failures"),
            retry_image_failures,
            retry_image_failures.__doc__ or "",
            _OPEN_WORLD_MUTATION,
        ),
        ToolDefinition(
            "get_work_status",
            ("carl", "review", "get_work_status"),
            get_work_status,
            get_work_status.__doc__ or "",
            _READ_ONLY,
        ),
    )
    if len({definition.name for definition in definitions}) != len(definitions):
        raise ValueError("Duplicate MCP tool name")
    if len({definition.operation for definition in definitions}) != len(definitions):
        raise ValueError("Duplicate MCP operation identity")
    return definitions


def build_server(
    application: ReviewApplication,
    *,
    before_tool_call: Callable[[], Awaitable[None]] = _already_ready,
) -> MCPServer:
    """Build a transport-independent Carl MCP server."""

    definitions = tool_definitions(application, before_tool_call=before_tool_call)
    package_version = version("carl")
    source_version = application.server_source_tree_sha256
    advertised_version = (
        package_version
        if source_version is None
        else f"{package_version}+source.{source_version[:12]}"
    )
    server = MCPServer(
        name="carl",
        description="Review retained Carl evidence and request provenance-linked analysis",
        instructions=_INSTRUCTIONS,
        version=advertised_version,
    )
    for definition in definitions:
        server.add_tool(
            definition.function,
            name=definition.name,
            description=definition.description,
            annotations=definition.annotations,
            meta={
                "carl/operation": list(definition.operation),
            },
            structured_output=definition.structured_output,
        )
    return server


@dataclass(frozen=True, slots=True)
class McpRuntime:
    application: ReviewApplication
    server: MCPServer


class _DatabaseReadiness:
    """Gate tool execution while allowing the MCP transport to start immediately."""

    def __init__(self) -> None:
        self._ready: anyio.Event = anyio.Event()
        self._error: Exception | None = None
        self._shutting_down = False

    def begin_shutdown(self) -> None:
        self._shutting_down = True

    async def prepare(self, database: Database) -> None:
        try:
            await database.migrate()
            await database.validate_schema()
        except Exception as error:
            if self._shutting_down:
                return
            self._error = error
            print("Carl MCP database preparation failed", file=sys.stderr, flush=True)
            traceback.print_exception(error, file=sys.stderr)
            sys.stderr.flush()
        finally:
            self._ready.set()

    async def wait(self) -> None:
        await self._ready.wait()
        if self._error is not None:
            raise RuntimeError("Carl MCP database preparation failed") from self._error


@asynccontextmanager
async def managed_mcp_runtime(
    database_path: Path,
    repository_root: Path,
) -> AsyncGenerator[McpRuntime]:
    """Own the review application without implicitly running queue workers."""

    code_provenance = await collect_code_provenance_async(repository_root)
    source_sha256 = await source_tree_sha256_async(repository_root)
    readiness = _DatabaseReadiness()
    async with (
        Database.managed(database_path, prepare_schema=False) as database,
        anyio.create_task_group() as task_group,
    ):
        task_group.start_soon(readiness.prepare, database)
        application = ReviewApplication(
            database=database,
            repository_root=repository_root,
            claude=ClaudeCli(),
            server_code_provenance=code_provenance,
            server_source_tree_sha256=source_sha256,
        )
        try:
            yield McpRuntime(
                application=application,
                server=build_server(application, before_tool_call=readiness.wait),
            )
        finally:
            readiness.begin_shutdown()
            task_group.cancel_scope.cancel()


async def serve_stdio(database_path: Path, repository_root: Path) -> None:
    """Serve Carl over stdio while preserving Trio-compatible async ownership."""

    try:
        async with managed_mcp_runtime(database_path, repository_root) as runtime:
            await runtime.server.run_stdio_async()
    except Exception as error:
        print("Carl MCP server terminated unexpectedly", file=sys.stderr, flush=True)
        traceback.print_exception(error, file=sys.stderr)
        sys.stderr.flush()
        raise
