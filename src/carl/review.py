"""Application operations for reviewing and extending retained Carl evidence."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from time import perf_counter_ns, time_ns
from typing import Literal, cast
from uuid import uuid4

import anyio

from carl.core.activity import ActivitySnapshot, work_error_kind
from carl.core.analysis_batch import (
    REQUEST_MISSING_ANALYSES_WORK_KIND,
    ListingAnalysisBatchSelection,
    MissingListingAnalysesPreview,
    RequestMissingAnalysesPayload,
    RequestMissingListingAnalysesRequest,
    RequestMissingListingAnalysesResult,
    SelectionAnalysesPreview,
    SelectionAnalysesRequest,
    SelectionAnalysesRequestResult,
    SelectionSnapshotAnalysisSelection,
    WorkspaceAnalysisSelection,
    request_missing_analyses_work,
    select_listing_analysis_batch,
)
from carl.core.components import ComponentId
from carl.core.composed_projection import (
    ComposedGallery,
    ComposedGalleryImage,
    ComposedListingCursor,
    ComposedListingFilters,
    ComposedListingPage,
    ComposedListingProjection,
    GalleryCandidate,
    GetComposedListingRequest,
    ListComposedSearchRequest,
    ListingObservationCandidate,
    ListingStatus,
    ProjectionAnalysisDescriptor,
    ProjectionGalleryImageDescriptor,
    SavedImageProjectionCandidate,
    ScalarFieldValues,
    SearchAncestrySelection,
    SearchCardCandidate,
    SearchMembershipOccurrenceCandidate,
    SearchMembershipProjection,
    SearchRunCandidate,
    StatusObservationCandidate,
    bounded_refresh_ancestry,
    canonical_facebook_listing_url,
    compose_search_membership,
    composed_listing_cursor_scope_sha256,
    composed_listing_matches_filters,
    decode_composed_listing_cursor,
    encode_composed_listing_cursor,
    evidence_recency_key,
    normalize_scalar_fields,
    projection_revision,
    projection_scalar_fields,
    scalar_field_changes,
    scalar_fields_sha256,
    search_card_field_value,
    select_analyses,
    select_composed_field,
    select_gallery,
    select_search_card_preview,
    select_status,
    status_candidate_from_item_observation,
    truncate_analysis_selection,
    truncate_composed_gallery,
    validate_composed_listing_cursor_scope,
)
from carl.core.ebay import (
    COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_EBAY_SEARCH_WORK_KIND,
    EBAY_SEARCH_FAILURE_KINDS,
    CollectEbaySearchPayload,
    EbayListingState,
    EbaySearchRequest,
    EbaySearchResponseClassification,
    collect_ebay_search_work,
)
from carl.core.ebay_items import (
    CollectEbayItemPayload,
    ExtractEbayItemPayload,
    collect_ebay_item_work,
    extract_ebay_item_work,
)
from carl.core.ebay_price import ebay_price_value, normalize_ebay_scalar_fields
from carl.core.ebay_refresh import (
    REFRESH_EBAY_SEARCH_WORK_KIND,
    RefreshEbaySearchPayload,
    refresh_ebay_search_work,
    refresh_ebay_search_work_constraints,
)
from carl.core.ebay_search_support import (
    UnsupportedEbaySearchMode,
    require_supported_ebay_search_acquisition,
)
from carl.core.facebook_images import (
    GalleryImageReference,
    ImageFailureSourceKind,
    RetryImageFailuresRequest,
    RetryImageFailuresResult,
    gallery_references,
    image_session_work_constraint,
)
from carl.core.facebook_listing import (
    RequestFacebookListingDetailsPayload,
    request_facebook_listing_details_work,
)
from carl.core.facebook_refresh import (
    RefreshSearchPayload,
    SearchRefreshRequest,
    SearchRefreshRequestResult,
    SearchRunListingsPage,
    SearchRunPage,
    SearchRunSummary,
    refresh_search_work,
)
from carl.core.facebook_work import (
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    CollectSearchPayload,
    CreateSearchRequest,
    CreateSearchResult,
    collect_search_work,
    is_transient_search_failure,
)
from carl.core.item_analysis import (
    AnalysisImageSelection,
    AnalyzeItemPayload,
    UnavailableAnalysisImageSelection,
    analyze_item_work,
    listing_analysis_evidence_set,
)
from carl.core.json import encode_json
from carl.core.marketplace_listing import (
    GetMarketplaceListingRequest,
    MarketplaceListingDetails,
    MarketplaceListingImage,
    RequestEbayListingDetailsRequest,
    RequestListingDetailsResult,
)
from carl.core.marketplace_search import (
    ADD_MARKETPLACE_SEARCH_TARGET,
    CREATE_MARKETPLACE_SEARCH,
    MARKETPLACE_SEARCH_EXECUTION_KIND,
    MARKETPLACE_SEARCH_KIND,
    MARKETPLACE_SEARCH_TARGET_KIND,
    MARKETPLACE_SEARCH_TARGET_STATE_KIND,
    RUN_MARKETPLACE_SEARCH,
    SET_MARKETPLACE_SEARCH_TARGET_ENABLED,
    AddMarketplaceSearchTargetRequest,
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    FacebookSearchTargetSpecification,
    ListMarketplaceSearchResultsRequest,
    Marketplace,
    MarketplaceSearch,
    MarketplaceSearchExecution,
    MarketplaceSearchExecutionRecord,
    MarketplaceSearchExecutionWarning,
    MarketplaceSearchRecord,
    MarketplaceSearchResult,
    MarketplaceSearchResultOccurrence,
    MarketplaceSearchResultsCursor,
    MarketplaceSearchResultsPage,
    MarketplaceSearchTarget,
    MarketplaceSearchTargetRecord,
    MarketplaceSearchTargetStateRecord,
    RunMarketplaceSearchRequest,
    SearchTargetSpecification,
    SetMarketplaceSearchTargetEnabledRequest,
    build_marketplace_search_component_registry,
    decode_marketplace_search_results_cursor,
    encode_marketplace_search_results_cursor,
)
from carl.core.models import CodeProvenance, JsonValue, NamedInput, NamedOutput, RecordDraft
from carl.core.network_defaults import DEFAULT_DATACENTER_NETWORK_PATH
from carl.core.pipeline import (
    PipelineOptions,
    RequestSearchPipelineRequest,
    SearchPipelineRequestResult,
    SearchPipelineStatus,
)
from carl.core.refresh_recovery import (
    RetryItemFailuresRequest,
    RetryItemFailuresResult,
    refresh_failure_summary,
)
from carl.core.review import (
    AnalysisBatchProgress,
    AnalysisDescriptor,
    AnalysisReport,
    CandidateFilters,
    CandidatePage,
    CreateProductGuideRequest,
    GalleryImageDescriptor,
    ListCandidatesRequest,
    ListingDossier,
    ProductGuideConflict,
    ProductGuideDetails,
    ProductGuideIdentityStateRecord,
    ProductGuideSummary,
    ProvenanceObject,
    RequestAnalysisRequest,
    RequestAnalysisResult,
    ReviseProductGuideRequest,
    SearchRefreshProgress,
    ServerCapability,
    ServerInfo,
    SetProductGuideIdentityRetiredRequest,
    WorkFailureReasonCount,
    WorkFailureSummary,
    WorkOperationSummary,
    WorkRuntimeStatus,
    WorkStatus,
    WorkStatusDetails,
    candidate_page,
    listing_analysis_history,
    search_refresh_phase,
    work_group_progress,
)
from carl.core.review_errors import IncompleteGalleryError, ReviewInputError
from carl.core.review_workspace import (
    ACQUIRE_REVIEW_BATCH,
    ADD_WORKSPACE_PRODUCT_GUIDE,
    CREATE_REVIEW_BATCH,
    CREATE_REVIEW_WORKSET,
    CREATE_REVIEW_WORKSPACE,
    CREATE_SELECTION_SNAPSHOT,
    LISTING_REVIEW_KIND,
    MAXIMUM_WORKSET_LISTINGS,
    PRODUCT_GUIDE_IDENTITY_STATE_KIND,
    RECORD_LISTING_REVIEWS,
    RECORD_WORKSPACE_BULK_REVIEW,
    RELEASE_REVIEW_CLAIM,
    RENAME_REVIEW_WORKSPACE,
    RENEW_REVIEW_CLAIM,
    REVIEW_BATCH_KIND,
    REVIEW_WORKSET_KIND,
    REVIEW_WORKSPACE_IDENTITY_STATE_KIND,
    REVIEW_WORKSPACE_KIND,
    REVISE_WORKSPACE_SEARCH_TRACK,
    SELECTION_SNAPSHOT_KIND,
    SET_PRODUCT_GUIDE_IDENTITY_RETIRED,
    SET_REVIEW_WORKSPACE_ARCHIVED,
    SET_WORKSPACE_DEFAULT_PRODUCT_GUIDE,
    SET_WORKSPACE_SEARCH_TRACK_ENABLED,
    UPDATE_REVIEW_WORKSET,
    UPDATE_WORKSPACE_PRODUCT_GUIDE,
    WORKSPACE_DEFAULT_PRODUCT_GUIDE_STATE_KIND,
    WORKSPACE_PRODUCT_GUIDE_BINDING_KIND,
    WORKSPACE_SEARCH_TRACK_SPECIFICATION_KIND,
    WORKSPACE_SEARCH_TRACK_STATE_KIND,
    AcquireReviewBatchRequest,
    AddWorkspaceProductGuideRequest,
    CreateReviewBatchRequest,
    CreateReviewWorksetRequest,
    CreateReviewWorkspaceRequest,
    CreateSelectionSnapshotRequest,
    CreateWorkspaceSearchRequest,
    CreateWorkspaceSearchResult,
    GetWorkspaceListingRequest,
    ListingIdsSelection,
    ListingReviewRecord,
    ListWorkspaceListingsRequest,
    RecordListingReviewsRequest,
    RecordListingReviewsResult,
    RecordWorkspaceBulkReviewRequest,
    RecordWorkspaceBulkReviewResult,
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
    ReviewBatchItem,
    ReviewBatchSelection,
    ReviewBatchSummary,
    ReviewClaimLease,
    ReviewClaimSummary,
    ReviewWorkset,
    ReviewWorksetConflict,
    ReviewWorksetSummary,
    ReviewWorkspace,
    ReviewWorkspaceActivity,
    ReviewWorkspaceIdentityStateRecord,
    ReviseWorkspaceSearchTrackRequest,
    SelectionSnapshot,
    SelectionSnapshotBulkReviewSelection,
    SelectionSnapshotItem,
    SelectionSnapshotSummary,
    SetReviewWorkspaceArchivedRequest,
    SetWorkspaceDefaultProductGuideRequest,
    SetWorkspaceSearchTrackEnabledRequest,
    UpdateReviewWorksetRequest,
    UpdateWorkspaceProductGuideBindingRequest,
    WorksetBulkReviewSelection,
    WorksetSelection,
    WorkspaceDefaultProductGuideStateRecord,
    WorkspaceListingPage,
    WorkspaceListingReview,
    WorkspaceListingSummary,
    WorkspaceProductGuideBinding,
    WorkspaceProductGuideBindingRecord,
    WorkspaceProductGuideVersionPolicy,
    WorkspaceSearchTrack,
    WorkspaceSearchTrackSpecificationRecord,
    WorkspaceSearchTrackStateRecord,
    WorkspaceWorkStatus,
    WorkspaceWorkWaitResult,
    build_review_workspace_component_registry,
    classify_review_state,
    review_mutation_request_sha256,
)
from carl.core.work import WorkEventKind, WorkRequester, WorkState
from carl.core.workspace_search_results import (
    ListWorkspaceSearchResultsRequest,
    WorkspaceSearchResult,
    WorkspaceSearchResultOccurrence,
    WorkspaceSearchResultsCursor,
    WorkspaceSearchResultsPage,
    decode_workspace_search_results_cursor,
    encode_workspace_search_results_cursor,
    workspace_search_results_scope,
)
from carl.ebay_analysis_workers import request_ebay_listing_analysis
from carl.ebay_refresh_workers import ebay_refresh_child_work_states
from carl.ebay_retry import retry_ebay_image_failures
from carl.facebook_analysis_workers import (
    AUTHOR_PRODUCT_GUIDE,
    PLAN_FACEBOOK_LISTING_ANALYSIS_EVIDENCE,
    build_analysis_component_registry,
)
from carl.io.claude import ClaudeCli
from carl.io.configuration import ConfigurationFailure, load_configuration
from carl.io.paths import user_directories
from carl.io.processes import database_process_activity
from carl.io.provenance import (
    collect_code_provenance_async,
    process_invocation,
    source_tree_sha256_async,
)
from carl.io.sqlite import Database
from carl.marketplace_listing import ebay_observations, read_ebay_listing
from carl.marketplace_projection import (
    ebay_analysis_descriptor,
    ebay_candidate_sources,
    expand_search_runs,
    get_marketplace_composed_listing,
    list_marketplace_composed_search,
    marketplace_bulk_review_projections,
    run_listing_identifiers,
    scope_contains_ebay,
)
from carl.refresh_recovery import retry_refresh_item_failures


@dataclass(frozen=True, slots=True)
class ResolvedImage:
    artifact_identifier: str
    media_type: str
    sha256: str
    content: bytes


def _identifier() -> str:
    return str(uuid4())


def _utc_text(utc_ns: int) -> str:
    return datetime.fromtimestamp(utc_ns / 1_000_000_000, tz=UTC).isoformat()


def _nonnegative_integer(value: JsonValue) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _projection_analysis_descriptor(value: AnalysisDescriptor) -> ProjectionAnalysisDescriptor:
    return ProjectionAnalysisDescriptor(
        analysis_record_identifier=value.analysis_record_identifier,
        evidence_set_record_identifier=value.evidence_set_record_identifier,
        listing_observation_record_identifier=value.listing_observation_record_identifier,
        product_guide_record_identifier=value.product_guide_record_identifier,
        completion_sequence=value.completion_sequence,
        completed_at_utc=value.completed_at_utc,
        state=value.state,
        warnings=value.warnings,
        model=value.model,
    )


def _optional_string(source: dict[str, JsonValue], name: str) -> str | None:
    value = source.get(name)
    return value if isinstance(value, str) else None


def _optional_positive_integer(source: dict[str, JsonValue], name: str) -> int | None:
    value = source.get(name)
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


@dataclass(frozen=True, slots=True)
class _MarketplaceSearchResultCandidate:
    marketplace: Marketplace
    external_identifier: str
    canonical_url: str
    title: str | None
    displayed_price: str | None
    location: str | None
    shipping_text: str | None
    condition: str | None
    preview_image_url: str | None
    promoted: bool | None
    occurrence: MarketplaceSearchResultOccurrence


def _nested_text(value: JsonValue) -> str | None:
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        return None
    mapping = cast(dict[str, JsonValue], value)
    for name in ("text", "display_name", "label"):
        candidate = mapping.get(name)
        if isinstance(candidate, str):
            return candidate
    return None


def _facebook_displayed_price(original: dict[str, JsonValue]) -> str | None:
    price = search_card_field_value("price", original)
    if not isinstance(price, dict):
        return None
    normalized = cast(dict[str, JsonValue], price)
    formatted = normalized.get("formatted_amount")
    if isinstance(formatted, str):
        return formatted
    amount = normalized.get("amount_decimal")
    currency = normalized.get("currency")
    if not isinstance(amount, str):
        return None
    return f"{amount} {currency}" if isinstance(currency, str) else amount


def _facebook_preview_image(original: dict[str, JsonValue]) -> str | None:
    photo = original.get("primary_listing_photo")
    if not isinstance(photo, dict):
        return None
    image = cast(dict[str, JsonValue], photo).get("image")
    if not isinstance(image, dict):
        return None
    uri = cast(dict[str, JsonValue], image).get("uri")
    return uri if isinstance(uri, str) else None


def _ebay_listing_state(value: JsonValue) -> EbayListingState | None:
    request = value.get("request") if isinstance(value, dict) else None
    if not isinstance(request, dict):
        return None
    state = request.get("listing_state", "active")
    return state if state in ("active", "sold", "completed") else None


def _require_ebay_search_acquisition(request: EbaySearchRequest) -> None:
    try:
        require_supported_ebay_search_acquisition(request)
    except UnsupportedEbaySearchMode as error:
        raise ReviewInputError(str(error)) from error


def _newest_value[T](
    candidates: tuple[_MarketplaceSearchResultCandidate, ...],
    getter: Callable[[_MarketplaceSearchResultCandidate], T | None],
) -> T | None:
    return next(
        (value for candidate in candidates if (value := getter(candidate)) is not None), None
    )


@dataclass(frozen=True, slots=True)
class ReviewApplication:
    database: Database
    repository_root: Path
    claude: ClaudeCli = field(default_factory=ClaudeCli)
    new_identifier: Callable[[], str] = _identifier
    utc_now_ns: Callable[[], int] = time_ns
    monotonic_ns: Callable[[], int] = perf_counter_ns
    code_provenance: Callable[[], Awaitable[CodeProvenance]] | None = None
    server_instance_identifier: str = field(default_factory=_identifier)
    server_started_at_utc_ns: int = field(default_factory=time_ns)
    server_code_provenance: CodeProvenance | None = None
    server_source_tree_sha256: str | None = None

    async def _code_provenance(self) -> CodeProvenance:
        if self.code_provenance is not None:
            return await self.code_provenance()
        return await collect_code_provenance_async(self.repository_root)

    async def get_server_info(self) -> ServerInfo:
        """Describe the exact running server instance and its review capabilities."""

        code_provenance = self.server_code_provenance or await self._code_provenance()
        source_tree_sha256 = self.server_source_tree_sha256 or await source_tree_sha256_async(
            self.repository_root
        )
        return ServerInfo(
            instance_identifier=self.server_instance_identifier,
            started_at_utc_ns=self.server_started_at_utc_ns,
            process_identifier=os.getpid(),
            repository_root=str(self.repository_root.resolve()),
            database_path=str(self.database.path.resolve()),
            code_provenance=code_provenance,
            source_tree_sha256=source_tree_sha256,
            capabilities=(
                ServerCapability(identity=("carl", "mcp", "instructions"), version=53),
                ServerCapability(identity=("carl", "mcp", "tool_contracts"), version=39),
                ServerCapability(identity=("carl", "marketplace", "search_pipeline"), version=1),
                ServerCapability(identity=("carl", "activity", "snapshot"), version=3),
                ServerCapability(identity=("carl", "facebook", "search_refresh"), version=3),
                ServerCapability(identity=("carl", "marketplace", "create_search"), version=1),
                ServerCapability(identity=("carl", "marketplace", "search_group"), version=2),
                ServerCapability(
                    identity=("carl", "marketplace", "search_results_projection"), version=4
                ),
                ServerCapability(identity=("carl", "ebay", "collect_search_work"), version=9),
                ServerCapability(identity=("carl", "marketplace", "listing_details"), version=2),
                ServerCapability(identity=("carl", "ebay", "item_images"), version=1),
                ServerCapability(identity=("carl", "marketplace", "search_refresh"), version=2),
                ServerCapability(identity=("carl", "marketplace", "item_failure_retry"), version=1),
                ServerCapability(identity=("carl", "work", "connectivity_outage_pause"), version=1),
                ServerCapability(identity=("carl", "marketplace", "listing_analysis"), version=1),
                ServerCapability(
                    identity=("carl", "marketplace", "image_failure_retry"), version=1
                ),
                ServerCapability(identity=("carl", "facebook", "search_run_summary"), version=1),
                ServerCapability(identity=("carl", "facebook", "search_run_membership"), version=1),
                ServerCapability(
                    identity=("carl", "facebook", "search_transport_retry"), version=5
                ),
                ServerCapability(
                    identity=("carl", "facebook", "search_refresh", "child_progress"),
                    version=1,
                ),
                ServerCapability(identity=("carl", "work", "cross_process_constraints"), version=1),
                ServerCapability(identity=("carl", "work", "explicit_runtime"), version=1),
                ServerCapability(identity=("carl", "facebook", "image_failure_retry"), version=2),
                ServerCapability(identity=("carl", "facebook", "collect_image_work"), version=3),
                ServerCapability(identity=("carl", "facebook", "analysis_batch"), version=6),
                ServerCapability(
                    identity=("carl", "facebook", "analysis_timeout_retry"), version=1
                ),
                ServerCapability(
                    identity=("carl", "facebook", "listing_analysis_history"), version=1
                ),
                ServerCapability(identity=("carl", "facebook", "listing_availability"), version=1),
                ServerCapability(identity=("carl", "review", "provenance_summary"), version=1),
                ServerCapability(identity=("carl", "review", "composed_projection"), version=9),
                ServerCapability(identity=("carl", "review", "workspace"), version=10),
                ServerCapability(identity=("carl", "review", "workspace_bulk_review"), version=4),
                ServerCapability(
                    identity=("carl", "review", "workspace_product_guides"), version=1
                ),
                ServerCapability(identity=("carl", "review", "product_guides"), version=1),
                ServerCapability(identity=("carl", "review", "workspace_search_tracks"), version=8),
                ServerCapability(identity=("carl", "review", "claims"), version=2),
                ServerCapability(identity=("carl", "review", "selection_snapshot"), version=2),
                ServerCapability(identity=("carl", "review", "selection_analysis"), version=5),
                ServerCapability(identity=("carl", "review", "workspace_work"), version=4),
                ServerCapability(identity=("carl", "work", "runtime_status"), version=2),
            ),
        )

    async def get_activity_snapshot(
        self,
        recent_window_minutes: int = 60,
        maximum_rows: int = 20,
    ) -> ActivitySnapshot:
        """Return work, failed connection attempts, and global connectivity pause/probe state."""

        if not 1 <= recent_window_minutes <= 7 * 24 * 60:
            raise ReviewInputError("Activity recent-window minutes must be between 1 and 10080")
        if not 1 <= maximum_rows <= 100:
            raise ReviewInputError("Activity maximum rows must be between 1 and 100")
        snapshot = await self.database.activity_snapshot(
            captured_at_utc_ns=self.utc_now_ns(),
            recent_window_ns=recent_window_minutes * 60 * 1_000_000_000,
            maximum_rows=maximum_rows,
        )
        processes = await anyio.to_thread.run_sync(
            database_process_activity,
            self.database.path,
            self.server_source_tree_sha256,
            abandon_on_cancel=True,
        )
        return snapshot.model_copy(update={"database_processes": processes})

    async def list_candidates(self, request: ListCandidatesRequest) -> CandidatePage:
        return candidate_page(await self.database.facebook_review_candidate_sources(), request)

    async def _projection_ancestry(
        self,
        search_run_record_identifier: str,
        *,
        as_of_completion_sequence: int,
        maximum_runs: int,
    ) -> SearchAncestrySelection:
        runs_by_identifier: dict[str, SearchRunCandidate] = {}
        current_identifier = search_run_record_identifier
        for _ in range(maximum_runs):
            if current_identifier in runs_by_identifier:
                break
            try:
                run = await self.database.facebook_projection_search_run(
                    current_identifier,
                    as_of_completion_sequence=as_of_completion_sequence,
                )
            except KeyError:
                if not runs_by_identifier:
                    raise
                break
            runs_by_identifier[run.record_identifier] = run
            if run.refresh_source_run_record_identifier is None:
                break
            current_identifier = run.refresh_source_run_record_identifier
        return bounded_refresh_ancestry(
            selected_search_run_record_identifier=search_run_record_identifier,
            runs_by_identifier=runs_by_identifier,
            maximum_runs=maximum_runs,
        )

    async def _projection_search_scope(
        self,
        primary_search_run_record_identifier: str,
        additional_search_run_record_identifiers: tuple[str, ...],
        *,
        as_of_completion_sequence: int,
        maximum_runs: int,
    ) -> SearchAncestrySelection:
        selected_identifiers = (
            primary_search_run_record_identifier,
            *additional_search_run_record_identifiers,
        )
        if len(selected_identifiers) > maximum_runs:
            raise ReviewInputError(
                "Maximum search runs must be at least the number of selected search tracks"
            )
        runs_per_track, tracks_with_extra_run = divmod(maximum_runs, len(selected_identifiers))
        selected_ancestries = tuple(
            [
                await self._projection_ancestry(
                    identifier,
                    as_of_completion_sequence=as_of_completion_sequence,
                    maximum_runs=(runs_per_track + (1 if index < tracks_with_extra_run else 0)),
                )
                for index, identifier in enumerate(selected_identifiers)
            ]
        )
        primary_ancestry = selected_ancestries[0]
        runs: list[SearchRunCandidate] = []
        seen: set[str] = set()
        maximum_depth = max(len(ancestry.runs) for ancestry in selected_ancestries)
        for depth in range(maximum_depth):
            for ancestry in selected_ancestries:
                if depth >= len(ancestry.runs):
                    continue
                run = ancestry.runs[depth]
                if run.record_identifier not in seen:
                    seen.add(run.record_identifier)
                    runs.append(run)
        multiple_lineages = len(selected_ancestries) > 1
        warnings = tuple(
            dict.fromkeys(
                (
                    *(warning for ancestry in selected_ancestries for warning in ancestry.warnings),
                    *(("multiple_search_lineages",) if multiple_lineages else ()),
                )
            )
        )
        return SearchAncestrySelection(
            runs=tuple(runs),
            lineage_root_search_run_record_identifier=(
                None
                if multiple_lineages
                else primary_ancestry.lineage_root_search_run_record_identifier
            ),
            older_ancestry_truncated=(
                any(ancestry.older_ancestry_truncated for ancestry in selected_ancestries)
            ),
            warnings=warnings,
        )

    async def _projection_analyses_by_listing(
        self,
        listing_identifiers: tuple[str, ...],
        *,
        product_guide_record_identifier: str | None,
        maximum_per_listing: int,
        as_of_completion_sequence: int,
    ) -> dict[str, tuple[ProjectionAnalysisDescriptor, ...]]:
        descriptors = await self.database.facebook_projection_analysis_descriptors(
            listing_identifiers,
            product_guide_record_identifier=product_guide_record_identifier,
            maximum_per_listing=maximum_per_listing,
            as_of_completion_sequence=as_of_completion_sequence,
        )
        by_listing: dict[str, list[ProjectionAnalysisDescriptor]] = {}
        for listing_identifier, descriptor in descriptors:
            by_listing.setdefault(listing_identifier, []).append(
                _projection_analysis_descriptor(descriptor)
            )
        return {
            listing_identifier: tuple(values) for listing_identifier, values in by_listing.items()
        }

    @staticmethod
    def _bounded_observations_by_listing(
        observations: tuple[ListingObservationCandidate, ...],
        *,
        maximum_per_listing: int,
    ) -> tuple[dict[str, tuple[ListingObservationCandidate, ...]], frozenset[str]]:
        grouped: dict[str, list[ListingObservationCandidate]] = {}
        for observation in observations:
            grouped.setdefault(observation.listing_identifier, []).append(observation)
        truncated = frozenset(
            listing_identifier
            for listing_identifier, values in grouped.items()
            if len(values) > maximum_per_listing
        )
        return (
            {
                listing_identifier: tuple(values[-maximum_per_listing:])
                for listing_identifier, values in grouped.items()
            },
            truncated,
        )

    @staticmethod
    def _bounded_search_cards_by_listing(
        search_cards: tuple[SearchCardCandidate, ...],
        *,
        maximum_per_listing: int,
    ) -> tuple[dict[str, tuple[SearchCardCandidate, ...]], frozenset[str]]:
        grouped: dict[str, list[SearchCardCandidate]] = {}
        for search_card in search_cards:
            grouped.setdefault(search_card.listing_identifier, []).append(search_card)
        truncated = frozenset(
            listing_identifier
            for listing_identifier, values in grouped.items()
            if len(values) > maximum_per_listing
        )
        return (
            {
                listing_identifier: tuple(values[-maximum_per_listing:])
                for listing_identifier, values in grouped.items()
            },
            truncated,
        )

    async def _projection_galleries(
        self,
        observations_by_listing: dict[str, tuple[ListingObservationCandidate, ...]],
        *,
        as_of_completion_sequence: int,
    ) -> dict[str, ComposedGallery | None]:
        references_by_observation: dict[str, tuple[GalleryImageReference, ...]] = {}
        reference_counts_by_observation: dict[str, int] = {}
        observation_by_identifier: dict[str, ListingObservationCandidate] = {}
        for observations in observations_by_listing.values():
            eligible: list[
                tuple[ListingObservationCandidate, str, tuple[GalleryImageReference, ...]]
            ] = []
            for observation in observations:
                observation_identifier = observation.evidence.observation_record_identifier
                if observation_identifier is None:
                    continue
                references = gallery_references(
                    observation_identifier=observation_identifier,
                    observation=observation.observation,
                )
                if references:
                    eligible.append((observation, observation_identifier, references))
            if not eligible:
                continue
            observation, observation_identifier, references = max(
                eligible,
                key=lambda candidate: evidence_recency_key(candidate[0].evidence),
            )
            reference_counts_by_observation[observation_identifier] = len(references)
            references_by_observation[observation_identifier] = references[:100]
            observation_by_identifier[observation_identifier] = observation
        if not references_by_observation:
            return {listing_identifier: None for listing_identifier in observations_by_listing}

        reference_identifiers = (
            await self.database.facebook_projection_gallery_reference_identifiers(
                tuple(references_by_observation),
                as_of_completion_sequence=as_of_completion_sequence,
            )
        )
        selected_references = tuple(
            reference
            for references in references_by_observation.values()
            for reference in references
        )
        exact_reference_identifiers = tuple(
            identifier
            for reference in selected_references
            if (identifier := reference_identifiers.get(reference)) is not None
        )
        direct_results = await self.database.facebook_projection_saved_image_results_for_references(
            exact_reference_identifiers,
            as_of_completion_sequence=as_of_completion_sequence,
        )
        rendition_results = (
            await self.database.facebook_projection_saved_image_results_for_renditions(
                tuple(
                    (reference.photo_id, reference.original_url)
                    for reference in selected_references
                ),
                as_of_completion_sequence=as_of_completion_sequence,
            )
        )
        saved_by_reference: dict[str, list[SavedImageProjectionCandidate]] = {}
        for result in direct_results:
            reference_identifier = result.image_reference_record_identifier
            if reference_identifier is not None:
                saved_by_reference.setdefault(reference_identifier, []).append(result)
        saved_by_rendition: dict[tuple[str | None, str], list[SavedImageProjectionCandidate]] = {}
        for result in rendition_results:
            saved_by_rendition.setdefault(
                (result.source_photo_identifier, result.original_url), []
            ).append(result)

        candidates_by_listing: dict[str, list[GalleryCandidate]] = {}
        for observation_identifier, references in references_by_observation.items():
            observation = observation_by_identifier[observation_identifier]
            images: list[ComposedGalleryImage] = []
            for reference in references:
                reference_identifier = reference_identifiers.get(reference)
                saved_options = (
                    []
                    if reference_identifier is None
                    else list(saved_by_reference.get(reference_identifier, ()))
                )
                saved_options.extend(
                    saved_by_rendition.get((reference.photo_id, reference.original_url), ())
                )
                saved_by_identifier = {
                    candidate.result_record_identifier: candidate for candidate in saved_options
                }
                saved = (
                    None
                    if not saved_by_identifier
                    else max(
                        saved_by_identifier.values(),
                        key=lambda candidate: evidence_recency_key(candidate.evidence),
                    )
                )
                result_identifier = None if saved is None else saved.result_record_identifier
                result = {} if saved is None else saved.result
                images.append(
                    ComposedGalleryImage(
                        descriptor=ProjectionGalleryImageDescriptor(
                            gallery_order=reference.gallery_order,
                            gallery_reference_record_identifier=reference_identifier,
                            original_url=reference.original_url,
                            photo_identifier=reference.photo_id,
                            declared_width=reference.declared_width,
                            declared_height=reference.declared_height,
                            image_result_record_identifier=result_identifier,
                            image_artifact_identifier=_optional_string(
                                result, "image_artifact_identifier"
                            ),
                            download_state="not_yet_collected" if saved is None else "saved",
                            sha256=_optional_string(result, "sha256"),
                            media_type=_optional_string(result, "mime_type"),
                            width=_optional_positive_integer(result, "width"),
                            height=_optional_positive_integer(result, "height"),
                        ),
                        evidence=None if saved is None else saved.evidence,
                    )
                )
            candidates_by_listing.setdefault(observation.listing_identifier, []).append(
                GalleryCandidate(
                    listing_identifier=observation.listing_identifier,
                    images=tuple(images),
                    referenced_image_count=reference_counts_by_observation[observation_identifier],
                    reference_set_truncated=(
                        len(references) < reference_counts_by_observation[observation_identifier]
                    ),
                    reference_set_evidence=observation.evidence,
                )
            )
        return {
            listing_identifier: select_gallery(
                candidates_by_listing.get(listing_identifier, ()),
                as_of_completion_sequence=as_of_completion_sequence,
                maximum_images=100,
            )
            for listing_identifier in observations_by_listing
        }

    @staticmethod
    def _compose_listing_projection(
        listing_identifier: str,
        *,
        observations: tuple[ListingObservationCandidate, ...],
        search_cards: tuple[SearchCardCandidate, ...],
        search_statuses: tuple[StatusObservationCandidate, ...],
        analyses: tuple[ProjectionAnalysisDescriptor, ...],
        as_of_completion_sequence: int,
        product_guide_record_identifier: str | None,
        maximum_analyses: int,
        maximum_gallery_images: int,
        gallery: ComposedGallery | None = None,
        membership: SearchMembershipProjection | None = None,
        warnings: tuple[str, ...] = (),
    ) -> tuple[ComposedListingProjection, bool]:
        status_candidates = (
            tuple(
                candidate
                for observation in observations
                if (candidate := status_candidate_from_item_observation(observation)) is not None
            )
            + search_statuses
        )
        full_analysis_selection = select_analyses(
            analyses,
            product_guide_record_identifier=product_guide_record_identifier,
            maximum_analyses=20,
        )
        analysis_selection = truncate_analysis_selection(
            full_analysis_selection,
            maximum_analyses=maximum_analyses,
        )
        status = select_status(
            status_candidates,
            as_of_completion_sequence=as_of_completion_sequence,
        )
        title = select_composed_field(
            "title", observations, search_cards, as_of_completion_sequence=as_of_completion_sequence
        )
        price = select_composed_field(
            "price", observations, search_cards, as_of_completion_sequence=as_of_completion_sequence
        )
        location = select_composed_field(
            "location_text",
            observations,
            search_cards,
            as_of_completion_sequence=as_of_completion_sequence,
        )
        description = select_composed_field(
            "description",
            observations,
            search_cards,
            as_of_completion_sequence=as_of_completion_sequence,
        )
        seller = select_composed_field(
            "seller",
            observations,
            search_cards,
            as_of_completion_sequence=as_of_completion_sequence,
        )
        preview_image = select_search_card_preview(
            search_cards,
            as_of_completion_sequence=as_of_completion_sequence,
        )
        projection = ComposedListingProjection(
            listing_identifier=listing_identifier,
            canonical_source_url=canonical_facebook_listing_url(listing_identifier),
            as_of_completion_sequence=as_of_completion_sequence,
            projection_revision=projection_revision(
                listing_identifier=listing_identifier,
                status=status,
                title=title,
                price=price,
                location=location,
                description=description,
                seller=seller,
                preview_image=preview_image,
                gallery=gallery,
                analyses=full_analysis_selection.analyses,
                analyses_truncated=full_analysis_selection.analyses_truncated,
                search_membership=membership,
            ),
            status=status,
            title=title,
            price=price,
            location=location,
            description=description,
            seller=seller,
            preview_image=preview_image,
            gallery=truncate_composed_gallery(
                gallery,
                maximum_images=maximum_gallery_images,
            ),
            analyses=analysis_selection.analyses,
            analyses_truncated=analysis_selection.analyses_truncated,
            search_membership=membership,
            warnings=warnings,
        )
        return projection, full_analysis_selection.matching_analysis_present

    async def get_composed_listing(
        self, request: GetComposedListingRequest
    ) -> ComposedListingProjection:
        roots = tuple(
            identifier
            for identifier in (
                request.search_run_record_identifier,
                *request.additional_search_run_record_identifiers,
            )
            if identifier is not None
        )
        if request.listing_identifier.startswith("ebay:") or await scope_contains_ebay(
            self.database, roots
        ):
            return await get_marketplace_composed_listing(self, request)
        if request.product_guide_record_identifier is not None:
            await self._active_product_guide(request.product_guide_record_identifier)
        as_of = await self.database.current_completion_boundary()
        observation_candidates = await self.database.facebook_projection_item_observations(
            (request.listing_identifier,),
            as_of_completion_sequence=as_of,
            maximum_per_listing=request.maximum_observations + 1,
        )
        observations_by_listing, truncated_observations = self._bounded_observations_by_listing(
            observation_candidates,
            maximum_per_listing=request.maximum_observations,
        )
        observations = observations_by_listing.get(request.listing_identifier, ())
        search_card_candidates = await self.database.facebook_projection_search_cards(
            (request.listing_identifier,),
            as_of_completion_sequence=as_of,
            maximum_per_listing=request.maximum_observations + 1,
        )
        search_cards_by_listing, truncated_search_cards = self._bounded_search_cards_by_listing(
            search_card_candidates,
            maximum_per_listing=request.maximum_observations,
        )
        search_cards = search_cards_by_listing.get(request.listing_identifier, ())
        search_statuses = await self.database.facebook_projection_search_occurrences(
            (request.listing_identifier,),
            as_of_completion_sequence=as_of,
        )
        analyses_by_listing = await self._projection_analyses_by_listing(
            (request.listing_identifier,),
            product_guide_record_identifier=request.product_guide_record_identifier,
            maximum_per_listing=21,
            as_of_completion_sequence=as_of,
        )
        ancestry = None
        membership_occurrences: tuple[SearchMembershipOccurrenceCandidate, ...] = ()
        if request.search_run_record_identifier is not None:
            ancestry = await self._projection_search_scope(
                request.search_run_record_identifier,
                tuple(request.additional_search_run_record_identifiers),
                as_of_completion_sequence=as_of,
                maximum_runs=request.maximum_ancestry_runs,
            )
            membership_occurrences = await self.database.facebook_projection_membership_occurrences(
                (request.listing_identifier,),
                tuple(
                    (run.record_identifier, run.internal_search_run_identifier)
                    for run in ancestry.runs
                ),
                as_of_completion_sequence=as_of,
            )
        membership = (
            None
            if ancestry is None
            else compose_search_membership(
                listing_identifier=request.listing_identifier,
                ancestry=ancestry,
                occurrences=membership_occurrences,
            )
        )
        if not observations and not search_cards and not search_statuses and membership is None:
            raise KeyError(request.listing_identifier)
        galleries = await self._projection_galleries(
            {request.listing_identifier: observations},
            as_of_completion_sequence=as_of,
        )
        projection, _ = self._compose_listing_projection(
            request.listing_identifier,
            observations=observations,
            search_cards=search_cards,
            search_statuses=search_statuses,
            analyses=analyses_by_listing.get(request.listing_identifier, ()),
            as_of_completion_sequence=as_of,
            product_guide_record_identifier=request.product_guide_record_identifier,
            maximum_analyses=request.maximum_analyses,
            maximum_gallery_images=request.maximum_gallery_images,
            gallery=galleries.get(request.listing_identifier),
            membership=membership,
            warnings=tuple(
                warning
                for condition, warning in (
                    (
                        request.listing_identifier in truncated_observations,
                        "item_observation_history_truncated",
                    ),
                    (
                        request.listing_identifier in truncated_search_cards,
                        "search_card_history_truncated",
                    ),
                )
                if condition
            ),
        )
        return projection

    async def list_composed_search(self, request: ListComposedSearchRequest) -> ComposedListingPage:
        if await scope_contains_ebay(
            self.database,
            (
                request.search_run_record_identifier,
                *request.additional_search_run_record_identifiers,
            ),
        ):
            return await list_marketplace_composed_search(self, request)
        try:
            cursor = (
                None if request.cursor is None else decode_composed_listing_cursor(request.cursor)
            )
            if cursor is not None:
                validate_composed_listing_cursor_scope(cursor, request)
        except ValueError as error:
            raise ReviewInputError(str(error)) from error
        if request.filters.product_guide_record_identifier is not None:
            await self.database.require_product_guide_record(
                request.filters.product_guide_record_identifier
            )
        as_of = (
            await self.database.current_completion_boundary()
            if cursor is None
            else cursor.as_of_completion_sequence
        )
        ancestry = await self._projection_search_scope(
            request.search_run_record_identifier,
            tuple(request.additional_search_run_record_identifiers),
            as_of_completion_sequence=as_of,
            maximum_runs=request.maximum_ancestry_runs,
        )
        membership_candidates_with_lookahead = (
            await self.database.facebook_projection_membership_candidates(
                tuple(
                    (run.record_identifier, run.internal_search_run_identifier)
                    for run in ancestry.runs
                ),
                as_of_completion_sequence=as_of,
                maximum_listings=request.maximum_candidate_listings_examined + 1,
                after_position=(
                    None
                    if cursor is None
                    else (
                        cursor.after_membership_completion_sequence,
                        cursor.after_listing_identifier,
                    )
                ),
            )
        )
        candidate_lookahead_present = (
            len(membership_candidates_with_lookahead) > request.maximum_candidate_listings_examined
        )
        membership_candidates = membership_candidates_with_lookahead[
            : request.maximum_candidate_listings_examined
        ]
        selected: list[ComposedListingProjection] = []
        selected_observations: dict[str, tuple[ListingObservationCandidate, ...]] = {}
        selected_search_cards: dict[str, tuple[SearchCardCandidate, ...]] = {}
        selected_statuses: dict[str, tuple[StatusObservationCandidate, ...]] = {}
        selected_analyses: dict[str, tuple[ProjectionAnalysisDescriptor, ...]] = {}
        selected_warnings: dict[str, tuple[str, ...]] = {}
        examined = 0
        last_examined = None
        evidence_chunk_size = 100
        for chunk_start in range(0, len(membership_candidates), evidence_chunk_size):
            chunk = membership_candidates[chunk_start : chunk_start + evidence_chunk_size]
            listing_identifiers = tuple(
                candidate.candidate.listing_identifier for candidate in chunk
            )
            observations = await self.database.facebook_projection_item_observations(
                listing_identifiers,
                as_of_completion_sequence=as_of,
                maximum_per_listing=request.maximum_observations_per_listing + 1,
            )
            search_statuses = await self.database.facebook_projection_search_occurrences(
                listing_identifiers,
                as_of_completion_sequence=as_of,
            )
            search_cards = await self.database.facebook_projection_search_cards(
                listing_identifiers,
                as_of_completion_sequence=as_of,
                maximum_per_listing=request.maximum_observations_per_listing + 1,
            )
            frozen_observations, truncated_observations = self._bounded_observations_by_listing(
                observations,
                maximum_per_listing=request.maximum_observations_per_listing,
            )
            frozen_search_cards, truncated_search_cards = self._bounded_search_cards_by_listing(
                search_cards,
                maximum_per_listing=request.maximum_observations_per_listing,
            )
            analyses_by_listing = await self._projection_analyses_by_listing(
                listing_identifiers,
                product_guide_record_identifier=(request.filters.product_guide_record_identifier),
                maximum_per_listing=21,
                as_of_completion_sequence=as_of,
            )
            statuses_by_listing: dict[str, list[StatusObservationCandidate]] = {}
            for status in search_statuses:
                statuses_by_listing.setdefault(status.listing_identifier, []).append(status)

            for membership_candidate in chunk:
                listing_identifier = membership_candidate.candidate.listing_identifier
                examined += 1
                last_examined = membership_candidate.candidate
                listing_observations = frozen_observations.get(listing_identifier, ())
                listing_search_cards = frozen_search_cards.get(listing_identifier, ())
                listing_statuses = tuple(statuses_by_listing.get(listing_identifier, ()))
                listing_analyses = analyses_by_listing.get(listing_identifier, ())
                listing_warnings = tuple(
                    warning
                    for condition, warning in (
                        (
                            listing_identifier in truncated_observations,
                            "item_observation_history_truncated",
                        ),
                        (
                            listing_identifier in truncated_search_cards,
                            "search_card_history_truncated",
                        ),
                    )
                    if condition
                )
                projection, matching_analysis_present = self._compose_listing_projection(
                    listing_identifier,
                    observations=listing_observations,
                    search_cards=listing_search_cards,
                    search_statuses=listing_statuses,
                    analyses=listing_analyses,
                    as_of_completion_sequence=as_of,
                    product_guide_record_identifier=(
                        request.filters.product_guide_record_identifier
                    ),
                    maximum_analyses=request.maximum_analyses_per_listing,
                    maximum_gallery_images=0,
                    warnings=listing_warnings,
                )
                if composed_listing_matches_filters(
                    projection,
                    request.filters,
                    matching_analysis_present=matching_analysis_present,
                ):
                    selected.append(projection)
                    selected_observations[listing_identifier] = listing_observations
                    selected_search_cards[listing_identifier] = listing_search_cards
                    selected_statuses[listing_identifier] = listing_statuses
                    selected_analyses[listing_identifier] = listing_analyses
                    selected_warnings[listing_identifier] = listing_warnings
                    if len(selected) >= request.page_size:
                        break
            if len(selected) >= request.page_size:
                break

        selected_identifiers = tuple(item.listing_identifier for item in selected)
        membership_occurrences = await self.database.facebook_projection_membership_occurrences(
            selected_identifiers,
            tuple(
                (run.record_identifier, run.internal_search_run_identifier) for run in ancestry.runs
            ),
            as_of_completion_sequence=as_of,
        )
        galleries = await self._projection_galleries(
            selected_observations,
            as_of_completion_sequence=as_of,
        )
        finalized: list[ComposedListingProjection] = []
        for provisional in selected:
            listing_identifier = provisional.listing_identifier
            projection, _ = self._compose_listing_projection(
                listing_identifier,
                observations=selected_observations[listing_identifier],
                search_cards=selected_search_cards[listing_identifier],
                search_statuses=selected_statuses[listing_identifier],
                analyses=selected_analyses[listing_identifier],
                as_of_completion_sequence=as_of,
                product_guide_record_identifier=(request.filters.product_guide_record_identifier),
                maximum_analyses=request.maximum_analyses_per_listing,
                maximum_gallery_images=request.maximum_gallery_images_per_listing,
                gallery=galleries.get(listing_identifier),
                membership=compose_search_membership(
                    listing_identifier=listing_identifier,
                    ancestry=ancestry,
                    occurrences=membership_occurrences,
                ),
                warnings=selected_warnings[listing_identifier],
            )
            finalized.append(projection)
        final_listings = tuple(finalized)
        more_candidates_may_exist = last_examined is not None and (
            examined < len(membership_candidates) or candidate_lookahead_present
        )
        next_cursor = (
            None
            if not more_candidates_may_exist or last_examined is None
            else encode_composed_listing_cursor(
                ComposedListingCursor(
                    as_of_completion_sequence=as_of,
                    scope_sha256=composed_listing_cursor_scope_sha256(request),
                    after_membership_completion_sequence=(
                        last_examined.search_run_completion_sequence
                    ),
                    after_listing_identifier=last_examined.listing_identifier,
                )
            )
        )
        return ComposedListingPage(
            as_of_completion_sequence=as_of,
            selected_search_run_record_identifier=request.search_run_record_identifier,
            included_ancestry_run_count=len(ancestry.runs),
            older_ancestry_truncated=ancestry.older_ancestry_truncated,
            examined_candidate_listing_count=examined,
            candidate_examination_limit_reached=(
                examined == request.maximum_candidate_listings_examined
                and candidate_lookahead_present
            ),
            listings=final_listings,
            next_cursor=next_cursor,
        )

    async def list_workspace_listings(
        self, request: ListWorkspaceListingsRequest
    ) -> WorkspaceListingPage:
        """Return a compact index over completed search tracks in a workspace."""

        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        current_runs = tuple(
            track.current_search_run_record_identifier
            for track in workspace.search_tracks
            if track.enabled and track.current_search_run_record_identifier is not None
        )
        if not current_runs and not workspace.search_tracks:
            current_runs = (workspace.search_run_record_identifier,)
        if not current_runs:
            raise ReviewInputError("The workspace has no completed search track")
        requested_guide = request.filters.product_guide_record_identifier
        if (
            requested_guide is not None
            and requested_guide != workspace.product_guide_record_identifier
        ):
            raise ReviewInputError("Workspace listing filters cannot select a different guide")
        filters = request.filters.model_copy(
            update={"product_guide_record_identifier": workspace.product_guide_record_identifier}
        )
        page = await self.list_composed_search(
            ListComposedSearchRequest(
                search_run_record_identifier=current_runs[0],
                additional_search_run_record_identifiers=current_runs[1:],
                filters=filters,
                maximum_ancestry_runs=request.maximum_search_runs,
                maximum_gallery_images_per_listing=0,
                maximum_analyses_per_listing=0,
                maximum_observations_per_listing=(request.maximum_observations_per_listing),
                maximum_candidate_listings_examined=(request.maximum_candidate_listings_examined),
                page_size=request.page_size,
                cursor=request.cursor,
            )
        )
        return WorkspaceListingPage(
            as_of_completion_sequence=page.as_of_completion_sequence,
            selected_search_run_record_identifier=(page.selected_search_run_record_identifier),
            included_ancestry_run_count=page.included_ancestry_run_count,
            older_ancestry_truncated=page.older_ancestry_truncated,
            examined_candidate_listing_count=page.examined_candidate_listing_count,
            candidate_examination_limit_reached=(page.candidate_examination_limit_reached),
            listings=tuple(
                WorkspaceListingSummary(
                    listing_identifier=listing.listing_identifier,
                    canonical_source_url=listing.canonical_source_url,
                    status=listing.status.value,
                    title=None if listing.title is None else listing.title.value,
                    price=None if listing.price is None else listing.price.value,
                    location=None if listing.location is None else listing.location.value,
                    condition=None if listing.condition is None else listing.condition.value,
                    shipping=None if listing.shipping is None else listing.shipping.value,
                    last_sale=None if listing.last_sale is None else listing.last_sale.value,
                    preview_image_url=(
                        None
                        if listing.preview_image is None
                        else listing.preview_image.descriptor.original_url
                    ),
                    description_available=listing.description is not None,
                    seller_available=listing.seller is not None,
                    referenced_image_count=(
                        None if listing.gallery is None else listing.gallery.referenced_image_count
                    ),
                    saved_image_count=(
                        None if listing.gallery is None else listing.gallery.saved_image_count
                    ),
                    analysis_available=(bool(listing.analyses) or listing.analyses_truncated),
                    projection_revision_sha256=(listing.projection_revision.aggregate_sha256),
                    warnings=listing.warnings,
                )
                for listing in page.listings
            ),
            next_cursor=page.next_cursor,
        )

    async def list_workspace_search_results(
        self, request: ListWorkspaceSearchResultsRequest
    ) -> WorkspaceSearchResultsPage:
        """Read one track's latest retained cards, without composing or acquiring listings."""

        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        track = next(
            (
                item
                for item in workspace.search_tracks
                if item.track_identifier == request.track_identifier
            ),
            None,
        )
        if track is None:
            raise ReviewInputError("The search track does not belong to this workspace")
        scope = workspace_search_results_scope(request)
        cursor = None
        if request.cursor is not None:
            try:
                cursor = decode_workspace_search_results_cursor(request.cursor)
            except ValueError as error:
                raise ReviewInputError(str(error)) from error
            if cursor.scope_sha256 != scope:
                raise ReviewInputError(
                    "Workspace search-results cursor belongs to another track or filter"
                )
        now = await self.database.current_completion_boundary()
        as_of = now if cursor is None else cursor.as_of_completion_sequence
        object_boundary = (
            await self.database.current_object_boundary()
            if cursor is None
            else cursor.maximum_object_rowid
        )
        if as_of > now or object_boundary > await self.database.current_object_boundary():
            raise ReviewInputError(
                "Workspace search-results cursor boundary is beyond retained evidence"
            )
        warnings = []
        if track.creation_work_state in (
            WorkState.PENDING,
            WorkState.LEASED,
            WorkState.TERMINAL_FAILURE,
        ):
            warnings.append("track_creation_not_completed")
        if track.latest_refresh_work_state in (
            WorkState.PENDING,
            WorkState.LEASED,
            WorkState.TERMINAL_FAILURE,
        ):
            warnings.append("latest_track_refresh_not_completed")
        current = track.current_search_run_record_identifier
        source_run = current if cursor is None else cursor.source_search_run_record_identifier
        if source_run is None:
            return WorkspaceSearchResultsPage(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=track.track_identifier,
                source_search_run_record_identifier=None,
                as_of_completion_sequence=as_of,
                total_distinct_results=0,
                collection_succeeded=False,
                results=(),
                next_cursor=None,
                warnings=tuple(warnings or ["track_has_no_completed_search"]),
            )
        if cursor is not None and source_run != current:
            runs = (
                ()
                if current is None
                else await expand_search_runs(
                    self.database,
                    (current,),
                    as_of_completion_sequence=now,
                )
            )
            if source_run not in runs:
                raise ReviewInputError(
                    "Workspace search-results cursor source is outside this track"
                )
        kind, _, run_value = await self.database.get_record(source_run)
        if kind not in (("carl", "facebook", "search_run"), ("carl", "ebay", "search_run")):
            raise ReviewInputError("The track does not reference a source search run")
        marketplace = Marketplace(kind[1])
        classification = None
        stopping_reason = None
        succeeded = None
        if marketplace is Marketplace.EBAY and isinstance(run_value, dict):
            stopping_reason = _optional_string(run_value, "stopping_reason")
            pages = run_value.get("pages")
            if isinstance(pages, list) and pages and isinstance(pages[-1], dict):
                value = pages[-1].get("response_classification")
                if isinstance(value, dict):
                    classification = EbaySearchResponseClassification.model_validate_json(
                        encode_json(value)
                    )
            if classification is not None:
                succeeded = (
                    classification.kind not in EBAY_SEARCH_FAILURE_KINDS
                    and stopping_reason != "invalid_next_page"
                )
            elif stopping_reason in {
                "challenge",
                "unrecognized_response",
                "error_page",
                "http_error",
                "invalid_next_page",
            }:
                succeeded = False
            if succeeded is False:
                warnings.append("source_search_collection_failed")
        records = await self.database.search_listing_occurrences_for_runs(
            marketplace.value,
            (source_run,),
            as_of_completion_sequence=as_of,
            maximum_object_rowid=object_boundary,
        )
        grouped: dict[str, list[tuple[str, dict[str, JsonValue]]]] = {}
        for identifier, value in records:
            if not isinstance(value, dict):
                continue
            item = value.get(
                "item_identifier" if marketplace is Marketplace.EBAY else "listing_identifier"
            )
            if isinstance(item, str):
                grouped.setdefault(item, []).append((identifier, value))
        results: list[WorkspaceSearchResult] = []
        for external, occurrences in grouped.items():
            occurrences.sort(
                key=lambda pair: (
                    _nonnegative_integer(pair[1].get("page_ordinal")),
                    _nonnegative_integer(pair[1].get("position", pair[1].get("edge_index"))),
                    pair[0],
                ),
                reverse=True,
            )
            newest = occurrences[0][1]
            if marketplace is Marketplace.EBAY:
                title = _optional_string(newest, "title")
                displayed = _optional_string(newest, "displayed_price")
                state = newest.get("listing_state", "active")
                location = None
                price = ebay_price_value(displayed, currency=newest.get("currency"))
                condition = _optional_string(newest, "condition")
                shipping = _optional_string(newest, "shipping_text")
                preview = _optional_string(newest, "image_url")
                listing_identifier = f"ebay:{external}"
                canonical = f"https://www.ebay.com/itm/{external}"
            else:
                original = newest.get("original")
                if not isinstance(original, dict):
                    continue
                title = _nested_text(search_card_field_value("title", original))
                displayed = _facebook_displayed_price(original)
                state = (
                    "sold"
                    if original.get("is_sold") is True
                    else "active"
                    if original.get("is_live") is True
                    else None
                )
                location = search_card_field_value("location_text", original)
                price = search_card_field_value("price", original)
                condition = _nested_text(original.get("condition")) or _nested_text(
                    original.get("listing_condition")
                )
                shipping = None
                preview = _facebook_preview_image(original)
                listing_identifier = external
                canonical = canonical_facebook_listing_url(external)
            if request.listing_state == "completed":
                if state not in ("sold", "completed"):
                    continue
            elif request.listing_state is not None and request.listing_state != state:
                continue
            folded = (title or "").casefold()
            if any(
                word.casefold() not in folded for word in request.required_title_keywords
            ) or any(word.casefold() in folded for word in request.excluded_title_keywords):
                continue
            projected_occurrences = tuple(
                WorkspaceSearchResultOccurrence(
                    occurrence_record_identifier=identifier,
                    acquisition_record_identifier=_optional_string(
                        value, "acquisition_record_identifier"
                    ),
                    page_ordinal=_nonnegative_integer(value.get("page_ordinal")),
                    position=_nonnegative_integer(value.get("position", value.get("edge_index"))),
                    listing_state=value.get("listing_state")
                    if marketplace is Marketplace.EBAY
                    else state,
                    sold_price=value.get("sold_price"),
                    sold_price_value=ebay_price_value(value.get("sold_price")),
                    sold_date=date.fromisoformat(raw)
                    if isinstance(raw := value.get("sold_date"), str)
                    else None,
                    sold_date_text=value.get("sold_date_text"),
                    sold_price_status=value.get("sold_price_status"),
                )
                for identifier, value in occurrences
            )
            sale = next(
                (item for item in projected_occurrences if item.listing_state == "sold"), None
            )
            results.append(
                WorkspaceSearchResult(
                    marketplace=marketplace,
                    listing_identifier=listing_identifier,
                    external_identifier=external,
                    canonical_url=canonical,
                    title=title,
                    price=price if state == "active" else None,
                    displayed_price=displayed,
                    location=location,
                    condition=condition,
                    shipping_text=shipping,
                    preview_image_url=preview,
                    listing_state=state,
                    last_sale=None if sale is None else sale.model_dump(mode="json"),
                    occurrences=projected_occurrences,
                )
            )
        results.sort(key=lambda result: result.listing_identifier)
        total = len(results)
        if cursor is not None:
            results = [
                item
                for item in results
                if item.listing_identifier > cursor.after_listing_identifier
            ]
        selected = tuple(results[: request.page_size])
        next_cursor = None
        if len(results) > len(selected):
            next_cursor = encode_workspace_search_results_cursor(
                WorkspaceSearchResultsCursor(
                    scope_sha256=scope,
                    source_search_run_record_identifier=source_run,
                    as_of_completion_sequence=as_of,
                    maximum_object_rowid=object_boundary,
                    after_listing_identifier=selected[-1].listing_identifier,
                )
            )
        return WorkspaceSearchResultsPage(
            workspace_record_identifier=workspace.record_identifier,
            track_identifier=track.track_identifier,
            source_search_run_record_identifier=source_run,
            as_of_completion_sequence=as_of,
            total_distinct_results=total,
            collection_succeeded=succeeded,
            response_classification=classification,
            stopping_reason=stopping_reason,
            results=selected,
            next_cursor=next_cursor,
            warnings=tuple(warnings),
        )

    async def get_workspace_listing(
        self, request: GetWorkspaceListingRequest
    ) -> WorkspaceListingReview:
        """Compose one exact listing within a workspace's active search-track scope."""

        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        current_runs = self._workspace_current_search_runs(workspace)
        if not current_runs:
            raise ReviewInputError("The workspace has no completed active search track")
        projection = await self.get_composed_listing(
            GetComposedListingRequest(
                listing_identifier=request.listing_identifier,
                search_run_record_identifier=current_runs[0],
                additional_search_run_record_identifiers=current_runs[1:],
                product_guide_record_identifier=workspace.product_guide_record_identifier,
                maximum_ancestry_runs=request.maximum_search_runs,
                maximum_gallery_images=request.maximum_gallery_images,
                maximum_analyses=request.maximum_analyses,
                maximum_observations=request.maximum_observations,
            )
        )
        prior_by_listing = await self._latest_listing_reviews(
            workspace.record_identifier, (projection.listing_identifier,)
        )
        prior = prior_by_listing.get(projection.listing_identifier)
        baselines = await self._review_scalar_baselines(tuple(prior_by_listing.values()))
        baseline = None if prior is None else baselines.get(prior.record_identifier)
        state, changed = classify_review_state(
            current_revision=projection.projection_revision,
            previous_review=prior,
            policy=workspace.staleness_policy,
            previous_scalar_fields=baseline,
        )
        return WorkspaceListingReview.model_validate(
            {
                **projection.model_dump(mode="python"),
                "review_state": state,
                "prior_review": prior,
                "changed_components": changed,
                "scalar_field_changes": (
                    ()
                    if baseline is None
                    else scalar_field_changes(baseline, projection_scalar_fields(projection))
                ),
                "scalar_comparison_available": baseline is not None,
            }
        )

    async def list_search_runs(self, query: str | None = None, limit: int = 50) -> SearchRunPage:
        if query is not None and (not query or query != query.strip()):
            raise ReviewInputError("Search query filter must be nonempty and trimmed")
        if not 1 <= limit <= 200:
            raise ReviewInputError("Search-run limit must be between 1 and 200")
        summaries: list[SearchRunSummary] = []
        for search_run in await self.database.facebook_search_run_records(query=query, limit=limit):
            record_identifier = search_run.record_identifier
            value = search_run.value
            if not isinstance(value, dict):
                raise ValueError("Stored Facebook search run is malformed")
            request = value.get("request")
            traversal = value.get("traversal")
            strategy = value.get("traversal_strategy")
            if not isinstance(request, dict) or not isinstance(traversal, dict):
                raise ValueError("Stored Facebook search run is incomplete")
            location = request.get("location")
            radius = request.get("radius")
            price = request.get("price")
            identifiers = traversal.get("unique_listing_identifiers")
            if (
                not isinstance(location, dict)
                or not isinstance(radius, dict)
                or not isinstance(identifiers, list)
            ):
                raise ValueError("Stored Facebook search run fields are malformed")
            summaries.append(
                SearchRunSummary(
                    record_identifier=record_identifier,
                    completion_sequence=search_run.completion_sequence,
                    started_at_utc=search_run.started_at_utc,
                    ended_at_utc=search_run.ended_at_utc,
                    origin=search_run.origin,
                    refresh_source_run_record_identifier=(
                        search_run.refresh_source_run_record_identifier
                    ),
                    query=str(request["query"]),
                    facebook_location_identifier=str(location["identifier"]),
                    radius_value=int(radius["value"]),
                    radius_unit=str(radius["unit"]),
                    minimum_price=(price.get("minimum") if isinstance(price, dict) else None),
                    maximum_price=(price.get("maximum") if isinstance(price, dict) else None),
                    traversal_strategy=strategy,
                    unique_listings=len(identifiers),
                    stopping_reason=(
                        str(traversal["stopping_reason"])
                        if traversal.get("stopping_reason") is not None
                        else None
                    ),
                )
            )
        for run in await self.database.ebay_search_run_records(query=query, limit=limit):
            value = run.value
            if not isinstance(value, dict):
                raise ValueError("Stored eBay search run is malformed")
            stored_request = value.get("request")
            if not isinstance(stored_request, dict):
                raise ValueError("Stored eBay search request is malformed")
            summaries.append(
                SearchRunSummary(
                    marketplace="ebay",
                    listing_state=EbaySearchRequest.model_validate(stored_request).listing_state,
                    record_identifier=run.record_identifier,
                    completion_sequence=run.completion_sequence,
                    started_at_utc=run.started_at_utc,
                    ended_at_utc=run.ended_at_utc,
                    origin=run.origin,
                    refresh_source_run_record_identifier=run.refresh_source_run_record_identifier,
                    query=str(stored_request["query"]),
                    facebook_location_identifier=None,
                    radius_value=None,
                    radius_unit=None,
                    minimum_price=None,
                    maximum_price=None,
                    traversal_strategy=None,
                    unique_listings=int(value.get("listing_count", 0)),
                    stopping_reason=str(value["stopping_reason"])
                    if value.get("stopping_reason") is not None
                    else None,
                )
            )
        summaries.sort(
            key=lambda summary: (
                summary.started_at_utc,
                summary.completion_sequence,
                summary.record_identifier,
            ),
            reverse=True,
        )
        return SearchRunPage(search_runs=tuple(summaries[:limit]))

    async def _publish_local_records(
        self,
        *,
        component_identifier: ComponentId,
        records: tuple[RecordDraft, ...],
        inputs: tuple[NamedInput, ...],
        outputs: tuple[NamedOutput, ...],
        result: JsonValue,
        workspace_track_guard: tuple[str, str, int] | None = None,
    ) -> None:
        started_utc_ns = self.utc_now_ns()
        started_monotonic_ns = self.monotonic_ns()
        provenance = await self._code_provenance()
        ended_utc_ns = self.utc_now_ns()
        await self.database.publish_records_operation(
            component=build_review_workspace_component_registry().require(component_identifier),
            operation_identifier=self.new_identifier(),
            records=records,
            inputs=inputs,
            outputs=outputs,
            provenance=provenance,
            invocation=process_invocation(),
            started_at_utc=_utc_text(started_utc_ns),
            ended_at_utc=_utc_text(ended_utc_ns),
            duration_ns=max(0, self.monotonic_ns() - started_monotonic_ns),
            result=result,
            workspace_track_guard=workspace_track_guard,
        )

    async def _publish_marketplace_search_records(
        self,
        *,
        component_identifier: ComponentId,
        records: tuple[RecordDraft, ...],
        inputs: tuple[NamedInput, ...],
        outputs: tuple[NamedOutput, ...],
        result: JsonValue,
        marketplace_target_guard: MarketplaceSearchTargetRecord | None = None,
    ) -> None:
        started_utc_ns = self.utc_now_ns()
        started_monotonic_ns = self.monotonic_ns()
        provenance = await self._code_provenance()
        ended_utc_ns = self.utc_now_ns()
        try:
            await self.database.publish_records_operation(
                component=build_marketplace_search_component_registry().require(
                    component_identifier
                ),
                operation_identifier=self.new_identifier(),
                records=records,
                inputs=inputs,
                outputs=outputs,
                provenance=provenance,
                invocation=process_invocation(),
                started_at_utc=_utc_text(started_utc_ns),
                ended_at_utc=_utc_text(ended_utc_ns),
                duration_ns=max(0, self.monotonic_ns() - started_monotonic_ns),
                result=result,
                marketplace_target_guard=marketplace_target_guard,
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error

    async def create_review_workspace(
        self, request: CreateReviewWorkspaceRequest
    ) -> ReviewWorkspace:
        kind, _, _ = await self.database.get_record(request.search_run_record_identifier)
        if kind not in (
            ("carl", "facebook", "search_run"),
            ("carl", "ebay", "search_run"),
            MARKETPLACE_SEARCH_KIND,
        ):
            raise ReviewInputError("The workspace source is not a search run")
        if request.product_guide_record_identifier is not None:
            await self.database.require_product_guide_record(
                request.product_guide_record_identifier
            )
        record_identifier = self.new_identifier()
        workspace = ReviewWorkspace(
            record_identifier=record_identifier,
            name=request.name,
            search_run_record_identifier=request.search_run_record_identifier,
            product_guide_record_identifier=request.product_guide_record_identifier,
            staleness_policy=request.staleness_policy,
            created_at_utc=_utc_text(self.utc_now_ns()),
        )
        inputs = [
            NamedInput(
                name=("search_run",),
                object_identifier=request.search_run_record_identifier,
            )
        ]
        if request.product_guide_record_identifier is not None:
            inputs.append(
                NamedInput(
                    name=("product_guide",),
                    object_identifier=request.product_guide_record_identifier,
                )
            )
        await self._publish_local_records(
            component_identifier=CREATE_REVIEW_WORKSPACE,
            records=(
                RecordDraft(
                    identifier=record_identifier,
                    kind=REVIEW_WORKSPACE_KIND,
                    schema_version=1,
                    value=workspace.model_dump(mode="json"),
                ),
            ),
            inputs=tuple(inputs),
            outputs=(NamedOutput(name=("workspace",), object_identifier=record_identifier),),
            result={"state": "completed"},
        )
        return await self.get_review_workspace(record_identifier)

    async def _stored_review_workspace(self, record_identifier: str) -> ReviewWorkspace:
        kind, schema_version, value = await self.database.get_record(record_identifier)
        if kind != REVIEW_WORKSPACE_KIND or schema_version != 1:
            raise ReviewInputError("The record is not a supported review workspace")
        return ReviewWorkspace.model_validate_json(encode_json(value))

    async def _workspace_identity(self, workspace: ReviewWorkspace) -> ReviewWorkspace:
        for _, value in await self.database.records_by_kind(REVIEW_WORKSPACE_IDENTITY_STATE_KIND):
            state = ReviewWorkspaceIdentityStateRecord.model_validate_json(encode_json(value))
            if state.workspace_record_identifier == workspace.record_identifier:
                update: dict[str, object] = {}
                if state.name is not None:
                    update["name"] = state.name
                if state.archived is not None:
                    update["archived"] = state.archived
                workspace = workspace.model_copy(update=update)
        return workspace

    @staticmethod
    def _legacy_workspace_product_guide_binding_identifier(
        workspace_record_identifier: str,
    ) -> str:
        return f"legacy:{workspace_record_identifier}"

    async def _workspace_product_guide_bindings(
        self, workspace: ReviewWorkspace
    ) -> tuple[tuple[WorkspaceProductGuideBinding, ...], str | None]:
        guides = await self.database.product_guide_summaries()
        guides_by_record = {guide.record_identifier: guide for guide in guides}
        latest_by_identity: dict[tuple[str, ...], ProductGuideSummary] = {}
        for guide in guides:
            current = latest_by_identity.get(guide.identity)
            if current is None or guide.version > current.version:
                latest_by_identity[guide.identity] = guide
        records: dict[str, WorkspaceProductGuideBindingRecord] = {}
        default_binding_identifier: str | None = None
        explicit_default_state = False
        if workspace.product_guide_record_identifier is not None:
            guide = guides_by_record.get(workspace.product_guide_record_identifier)
            if guide is None:
                raise KeyError(workspace.product_guide_record_identifier)
            binding_identifier = self._legacy_workspace_product_guide_binding_identifier(
                workspace.record_identifier
            )
            records[binding_identifier] = WorkspaceProductGuideBindingRecord(
                record_identifier=binding_identifier,
                binding_identifier=binding_identifier,
                workspace_record_identifier=workspace.record_identifier,
                alias=guide.display_name or guide.identity[-1],
                product_guide_identity=guide.identity,
                version_policy=WorkspaceProductGuideVersionPolicy.PINNED,
                pinned_product_guide_record_identifier=guide.record_identifier,
                enabled=True,
                recorded_at_utc=workspace.created_at_utc,
            )
            default_binding_identifier = binding_identifier
        for _, value in await self.database.records_by_kind(WORKSPACE_PRODUCT_GUIDE_BINDING_KIND):
            record = WorkspaceProductGuideBindingRecord.model_validate_json(encode_json(value))
            if record.workspace_record_identifier == workspace.record_identifier:
                records[record.binding_identifier] = record
        for _, value in await self.database.records_by_kind(
            WORKSPACE_DEFAULT_PRODUCT_GUIDE_STATE_KIND
        ):
            state = WorkspaceDefaultProductGuideStateRecord.model_validate_json(encode_json(value))
            if state.workspace_record_identifier == workspace.record_identifier:
                explicit_default_state = True
                default_binding_identifier = state.binding_identifier

        resolved: list[WorkspaceProductGuideBinding] = []
        for record in records.values():
            if record.version_policy is WorkspaceProductGuideVersionPolicy.PINNED:
                assert record.pinned_product_guide_record_identifier is not None
                guide = guides_by_record.get(record.pinned_product_guide_record_identifier)
            else:
                guide = latest_by_identity.get(record.product_guide_identity)
            if guide is None:
                raise ReviewInputError(
                    f"Workspace product-guide binding {record.binding_identifier} cannot resolve"
                )
            resolved.append(
                WorkspaceProductGuideBinding(
                    binding_identifier=record.binding_identifier,
                    workspace_record_identifier=workspace.record_identifier,
                    alias=record.alias,
                    product_guide_identity=record.product_guide_identity,
                    version_policy=record.version_policy,
                    pinned_product_guide_record_identifier=(
                        record.pinned_product_guide_record_identifier
                    ),
                    resolved_product_guide_record_identifier=guide.record_identifier,
                    resolved_product_guide_version=guide.version,
                    enabled=record.enabled,
                    is_default=False,
                )
            )
        enabled_identifiers = {
            binding.binding_identifier for binding in resolved if binding.enabled
        }
        if default_binding_identifier not in enabled_identifiers:
            default_binding_identifier = None
        if (
            not explicit_default_state
            and default_binding_identifier is None
            and len(enabled_identifiers) == 1
        ):
            default_binding_identifier = next(iter(enabled_identifiers))
        return (
            tuple(
                binding.model_copy(
                    update={
                        "is_default": (binding.binding_identifier == default_binding_identifier)
                    }
                )
                for binding in resolved
            ),
            default_binding_identifier,
        )

    @staticmethod
    def _search_query(value: JsonValue) -> str:
        request = value.get("request") if isinstance(value, dict) else None
        query = request.get("query") if isinstance(request, dict) else None
        return query if isinstance(query, str) and query else "unknown"

    @staticmethod
    def _retained_track_specification(
        marketplace: Marketplace, value: JsonValue
    ) -> SearchTargetSpecification | None:
        """Recover complete legacy intent when present, without breaking partial old records."""

        if not isinstance(value, dict):
            return None
        try:
            if marketplace is Marketplace.EBAY:
                return EbaySearchTargetSpecification(
                    search=EbaySearchRequest.model_validate(value.get("request"))
                )
            traversal = value.get("traversal")
            if isinstance(traversal, dict) and "policy" in traversal:
                traversal = traversal["policy"]
            routing = value.get("routing")
            intent = {
                "request": value.get("request"),
                "traversal": traversal,
            }
            if isinstance(routing, list) and routing:
                intent["network_path"] = routing
            if value.get("traversal_strategy") is not None:
                intent["traversal_strategy"] = value["traversal_strategy"]
            return FacebookSearchTargetSpecification(
                search=CreateSearchRequest.model_validate(intent)
            )
        except ValueError:
            return None

    async def _workspace_search_tracks(
        self, workspace: ReviewWorkspace
    ) -> tuple[WorkspaceSearchTrack, ...]:
        initial_kind, _, initial_value = await self.database.get_record(
            workspace.search_run_record_identifier
        )
        initial_refresh_identifier = (
            await self.database.facebook_search_run_refresh_work_identifier(
                workspace.search_run_record_identifier
            )
        )
        tracks: dict[str, WorkspaceSearchTrack] = {
            workspace.search_run_record_identifier: WorkspaceSearchTrack(
                track_identifier=workspace.search_run_record_identifier,
                query=self._search_query(initial_value),
                marketplace=(
                    Marketplace(initial_kind[1])
                    if initial_kind
                    in (("carl", "facebook", "search_run"), ("carl", "ebay", "search_run"))
                    else None
                ),
                listing_state=_ebay_listing_state(initial_value)
                if initial_kind == ("carl", "ebay", "search_run")
                else None,
                creation_work_identifier=None,
                creation_work_state=None,
                origin_search_run_record_identifier=workspace.search_run_record_identifier,
                current_search_run_record_identifier=workspace.search_run_record_identifier,
                latest_refresh_work_identifier=initial_refresh_identifier,
                latest_refresh_work_state=(
                    WorkState.COMPLETED if initial_refresh_identifier is not None else None
                ),
            )
        }
        if initial_kind == MARKETPLACE_SEARCH_KIND:
            search = await self.get_marketplace_search(workspace.search_run_record_identifier)
            tracks = {}
            for target in search.targets:
                successful = tuple(
                    execution
                    for execution in target.executions
                    if execution.state is WorkState.COMPLETED
                    and execution.collection_succeeded is not False
                    and execution.search_run_record_identifier is not None
                )
                current = successful[-1] if successful else None
                latest = target.executions[-1] if target.executions else None
                run_identifier = None if current is None else current.search_run_record_identifier
                refresh_identifier = (
                    None
                    if run_identifier is None
                    else await self.database.facebook_search_run_refresh_work_identifier(
                        run_identifier
                    )
                )
                specification = target.specification.search
                query = (
                    specification.query
                    if isinstance(specification, EbaySearchRequest)
                    else specification.request.query
                )
                tracks[target.record_identifier] = WorkspaceSearchTrack(
                    track_identifier=target.record_identifier,
                    query=query,
                    marketplace=target.specification.marketplace,
                    listing_state=specification.listing_state
                    if isinstance(specification, EbaySearchRequest)
                    else None,
                    creation_work_identifier=None if latest is None else latest.work_identifier,
                    creation_work_state=None if latest is None else latest.state,
                    origin_search_run_record_identifier=run_identifier,
                    current_search_run_record_identifier=run_identifier,
                    latest_refresh_work_identifier=refresh_identifier,
                    latest_refresh_work_state=(
                        WorkState.COMPLETED if refresh_identifier is not None else None
                    ),
                    enabled=target.enabled,
                    search_specification=target.specification,
                )
        creation_identifiers = await self.database.requested_work_identifiers(
            requester_kind=("carl", "mcp", "create_workspace_search"),
            requester_identifier=workspace.record_identifier,
        )
        creation_work = [
            await self.database.work(identifier) for identifier in creation_identifiers
        ]
        for work in sorted(
            creation_work,
            key=lambda value: (int(value["created_at_utc_ns"]), str(value["identifier"])),
        ):
            identifier = str(work["identifier"])
            result = work.get("result")
            run_identifier = (
                result.get("search_run_record_identifier") if isinstance(result, dict) else None
            )
            if not isinstance(run_identifier, str):
                run_identifier = None
            tracks[identifier] = WorkspaceSearchTrack(
                track_identifier=identifier,
                query=self._search_query(work.get("payload")),
                marketplace=(
                    Marketplace.EBAY
                    if work["kind"] == list(COLLECT_EBAY_SEARCH_WORK_KIND)
                    else Marketplace.FACEBOOK
                ),
                listing_state=_ebay_listing_state(work.get("payload"))
                if work["kind"] == list(COLLECT_EBAY_SEARCH_WORK_KIND)
                else None,
                creation_work_identifier=identifier,
                creation_work_state=WorkState(str(work["state"])),
                origin_search_run_record_identifier=run_identifier,
                current_search_run_record_identifier=run_identifier,
                latest_refresh_work_identifier=None,
                latest_refresh_work_state=None,
                search_specification=self._retained_track_specification(
                    Marketplace.EBAY
                    if work["kind"] == list(COLLECT_EBAY_SEARCH_WORK_KIND)
                    else Marketplace.FACEBOOK,
                    work.get("payload"),
                ),
            )
        revised_creation_edges = await self.database.requested_work_edges(
            requester_kind=("carl", "mcp", "create_workspace_search_revision"),
            requester_identifier=workspace.record_identifier,
        )
        for edge in revised_creation_edges:
            context = edge.get("context")
            track_identifier = (
                context.get("track_identifier") if isinstance(context, dict) else None
            )
            if not isinstance(track_identifier, str) or track_identifier not in tracks:
                raise RuntimeError("Revised initial search lost its workspace track")
            work = await self.database.work(str(edge["work_identifier"]))
            track = tracks[track_identifier]
            result = work.get("result")
            run = result.get("search_run_record_identifier") if isinstance(result, dict) else None
            successful_run = run if isinstance(run, str) and work["state"] == "completed" else None
            tracks[track_identifier] = track.model_copy(
                update={
                    "creation_work_identifier": work["identifier"],
                    "creation_work_state": WorkState(str(work["state"])),
                    "origin_search_run_record_identifier": track.origin_search_run_record_identifier
                    or successful_run,
                    "current_search_run_record_identifier": successful_run
                    or track.current_search_run_record_identifier,
                }
            )
        refresh_edges = await self.database.requested_work_edges(
            requester_kind=("carl", "mcp", "request_workspace_refresh"),
            requester_identifier=workspace.record_identifier,
        )
        refresh_work_by_identifier = {
            identifier: await self.database.work(identifier)
            for identifier in {str(edge["work_identifier"]) for edge in refresh_edges}
        }
        refresh_items = tuple(
            (
                refresh_work_by_identifier[str(edge["work_identifier"])],
                edge.get("context"),
            )
            for edge in refresh_edges
        )
        for work, context in sorted(
            refresh_items,
            key=lambda value: (
                int(value[0]["created_at_utc_ns"]),
                str(value[0]["identifier"]),
            ),
        ):
            identifier = str(work["identifier"])
            track_identifier = (
                context.get("track_identifier") if isinstance(context, dict) else None
            )
            if not isinstance(track_identifier, str) or track_identifier not in tracks:
                continue
            track = tracks[track_identifier]
            payload = work.get("payload")
            base_identifier = (
                payload.get("base_search_run_record_identifier")
                if isinstance(payload, dict)
                else None
            )
            if base_identifier != track.current_search_run_record_identifier:
                continue
            result = work.get("result")
            refreshed_identifier = (
                result.get("refreshed_search_run_record_identifier")
                if isinstance(result, dict)
                else None
            )
            state = WorkState(str(work["state"]))
            tracks[track_identifier] = track.model_copy(
                update={
                    "current_search_run_record_identifier": (
                        refreshed_identifier
                        if state is WorkState.COMPLETED and isinstance(refreshed_identifier, str)
                        else track.current_search_run_record_identifier
                    ),
                    "latest_refresh_work_identifier": identifier,
                    "latest_refresh_work_state": state,
                    "latest_refresh_search_specification_version": (
                        context.get("search_specification_version", 1)
                        if isinstance(context, dict)
                        else 1
                    ),
                }
            )
        for _, value in await self.database.records_by_kind(WORKSPACE_SEARCH_TRACK_STATE_KIND):
            state = WorkspaceSearchTrackStateRecord.model_validate_json(encode_json(value))
            if (
                state.workspace_record_identifier == workspace.record_identifier
                and state.track_identifier in tracks
            ):
                tracks[state.track_identifier] = tracks[state.track_identifier].model_copy(
                    update={"enabled": state.enabled}
                )
        if initial_kind in (("carl", "facebook", "search_run"), ("carl", "ebay", "search_run")):
            identifier = workspace.search_run_record_identifier
            tracks[identifier] = tracks[identifier].model_copy(
                update={
                    "search_specification": self._retained_track_specification(
                        Marketplace(initial_kind[1]), initial_value
                    )
                }
            )
        for _, value in await self.database.workspace_search_track_specifications(
            workspace.record_identifier
        ):
            revision = WorkspaceSearchTrackSpecificationRecord.model_validate(value)
            if revision.track_identifier not in tracks:
                raise RuntimeError("Workspace track revision lost its track")
            track = tracks[revision.track_identifier]
            specification = revision.search
            query = (
                specification.search.query
                if isinstance(specification, EbaySearchTargetSpecification)
                else specification.search.request.query
            )
            tracks[revision.track_identifier] = track.model_copy(
                update={
                    "search_specification": specification,
                    "search_specification_version": revision.version,
                    "search_specification_record_identifier": revision.record_identifier,
                    "query": query,
                    "listing_state": (
                        specification.search.listing_state
                        if isinstance(specification, EbaySearchTargetSpecification)
                        else None
                    ),
                }
            )
        return tuple(tracks.values())

    async def get_review_workspace(self, record_identifier: str) -> ReviewWorkspace:
        stored = await self._stored_review_workspace(record_identifier)
        workspace = await self._workspace_identity(stored)
        (
            guide_bindings,
            default_guide_binding_identifier,
        ) = await self._workspace_product_guide_bindings(stored)
        default_guide_record_identifier = next(
            (
                binding.resolved_product_guide_record_identifier
                for binding in guide_bindings
                if binding.is_default
            ),
            None,
        )
        return workspace.model_copy(
            update={
                "product_guide_record_identifier": default_guide_record_identifier,
                "product_guide_bindings": guide_bindings,
                "default_product_guide_binding_identifier": (default_guide_binding_identifier),
                "search_tracks": await self._workspace_search_tracks(workspace),
            }
        )

    async def _record_workspace_identity(
        self,
        workspace: ReviewWorkspace,
        *,
        name: str | None = None,
        archived: bool | None = None,
        component_identifier: ComponentId,
    ) -> ReviewWorkspace:
        record_identifier = self.new_identifier()
        state = ReviewWorkspaceIdentityStateRecord(
            record_identifier=record_identifier,
            workspace_record_identifier=workspace.record_identifier,
            name=name,
            archived=archived,
            recorded_at_utc=_utc_text(self.utc_now_ns()),
        )
        await self._publish_local_records(
            component_identifier=component_identifier,
            records=(
                RecordDraft(
                    identifier=record_identifier,
                    kind=REVIEW_WORKSPACE_IDENTITY_STATE_KIND,
                    schema_version=1,
                    value=state.model_dump(mode="json", exclude_none=True),
                ),
            ),
            inputs=(
                NamedInput(name=("workspace",), object_identifier=workspace.record_identifier),
            ),
            outputs=(NamedOutput(name=("identity_state",), object_identifier=record_identifier),),
            result=state.model_dump(mode="json", exclude_none=True),
        )
        return await self.get_review_workspace(workspace.record_identifier)

    async def rename_review_workspace(
        self, request: RenameReviewWorkspaceRequest
    ) -> ReviewWorkspace:
        """Rename a workspace while retaining its stable identity and history."""

        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        if workspace.name == request.name:
            return workspace
        return await self._record_workspace_identity(
            workspace,
            name=request.name,
            component_identifier=RENAME_REVIEW_WORKSPACE,
        )

    async def set_review_workspace_archived(
        self, request: SetReviewWorkspaceArchivedRequest
    ) -> ReviewWorkspace:
        """Archive or restore a workspace without deleting retained review history."""

        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        if workspace.archived == request.archived:
            return workspace
        return await self._record_workspace_identity(
            workspace,
            archived=request.archived,
            component_identifier=SET_REVIEW_WORKSPACE_ARCHIVED,
        )

    @staticmethod
    def _workspace_product_guide_binding(
        workspace: ReviewWorkspace, binding_identifier: str
    ) -> WorkspaceProductGuideBinding:
        binding = next(
            (
                candidate
                for candidate in workspace.product_guide_bindings
                if candidate.binding_identifier == binding_identifier
            ),
            None,
        )
        if binding is None:
            raise ReviewInputError("The product-guide binding is not part of this workspace")
        return binding

    async def add_workspace_product_guide(
        self, request: AddWorkspaceProductGuideRequest
    ) -> WorkspaceProductGuideBinding:
        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        if len(workspace.product_guide_bindings) >= 20:
            raise ReviewInputError("A workspace may have at most 20 product-guide bindings")
        if any(
            binding.alias.casefold() == request.alias.casefold()
            for binding in workspace.product_guide_bindings
        ):
            raise ReviewInputError("Workspace product-guide aliases must be unique")
        guide = await self._active_product_guide(request.product_guide_record_identifier)
        binding_identifier = self.new_identifier()
        recorded_at_utc = _utc_text(self.utc_now_ns())
        binding_record = WorkspaceProductGuideBindingRecord(
            record_identifier=binding_identifier,
            binding_identifier=binding_identifier,
            workspace_record_identifier=workspace.record_identifier,
            alias=request.alias,
            product_guide_identity=guide.identity,
            version_policy=request.version_policy,
            pinned_product_guide_record_identifier=(
                guide.record_identifier
                if request.version_policy is WorkspaceProductGuideVersionPolicy.PINNED
                else None
            ),
            enabled=True,
            recorded_at_utc=recorded_at_utc,
        )
        records = [
            RecordDraft(
                identifier=binding_identifier,
                kind=WORKSPACE_PRODUCT_GUIDE_BINDING_KIND,
                schema_version=1,
                value=binding_record.model_dump(mode="json"),
            )
        ]
        outputs = [
            NamedOutput(name=("product_guide_binding",), object_identifier=binding_identifier)
        ]
        make_default = (
            request.make_default or workspace.default_product_guide_binding_identifier is None
        )
        if make_default:
            default_state_identifier = self.new_identifier()
            default_state = WorkspaceDefaultProductGuideStateRecord(
                record_identifier=default_state_identifier,
                workspace_record_identifier=workspace.record_identifier,
                binding_identifier=binding_identifier,
                recorded_at_utc=recorded_at_utc,
            )
            records.append(
                RecordDraft(
                    identifier=default_state_identifier,
                    kind=WORKSPACE_DEFAULT_PRODUCT_GUIDE_STATE_KIND,
                    schema_version=1,
                    value=default_state.model_dump(mode="json"),
                )
            )
            outputs.append(
                NamedOutput(
                    name=("default_product_guide_state",),
                    object_identifier=default_state_identifier,
                )
            )
        await self._publish_local_records(
            component_identifier=ADD_WORKSPACE_PRODUCT_GUIDE,
            records=tuple(records),
            inputs=(
                NamedInput(name=("workspace",), object_identifier=workspace.record_identifier),
                NamedInput(name=("product_guide",), object_identifier=guide.record_identifier),
            ),
            outputs=tuple(outputs),
            result={
                "state": "completed",
                "binding_identifier": binding_identifier,
                "made_default": make_default,
            },
        )
        updated = await self.get_review_workspace(workspace.record_identifier)
        return self._workspace_product_guide_binding(updated, binding_identifier)

    async def update_workspace_product_guide_binding(
        self, request: UpdateWorkspaceProductGuideBindingRequest
    ) -> WorkspaceProductGuideBinding:
        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        current = self._workspace_product_guide_binding(workspace, request.binding_identifier)
        alias = request.alias or current.alias
        if any(
            binding.binding_identifier != current.binding_identifier
            and binding.alias.casefold() == alias.casefold()
            for binding in workspace.product_guide_bindings
        ):
            raise ReviewInputError("Workspace product-guide aliases must be unique")
        requested_guide = (
            await self._active_product_guide(request.product_guide_record_identifier)
            if request.product_guide_record_identifier is not None
            else None
        )
        if request.enabled is True and requested_guide is None:
            await self._active_product_guide(current.resolved_product_guide_record_identifier)
        if (
            requested_guide is not None
            and requested_guide.identity != current.product_guide_identity
        ):
            raise ReviewInputError(
                "Add a new binding instead of changing a binding's product-guide identity"
            )
        policy = request.version_policy or current.version_policy
        pinned_identifier = (
            (
                requested_guide.record_identifier
                if requested_guide is not None
                else current.resolved_product_guide_record_identifier
            )
            if policy is WorkspaceProductGuideVersionPolicy.PINNED
            else None
        )
        record_identifier = self.new_identifier()
        record = WorkspaceProductGuideBindingRecord(
            record_identifier=record_identifier,
            binding_identifier=current.binding_identifier,
            workspace_record_identifier=workspace.record_identifier,
            alias=alias,
            product_guide_identity=current.product_guide_identity,
            version_policy=policy,
            pinned_product_guide_record_identifier=pinned_identifier,
            enabled=current.enabled if request.enabled is None else request.enabled,
            recorded_at_utc=_utc_text(self.utc_now_ns()),
        )
        input_guide_identifier = (
            requested_guide.record_identifier
            if requested_guide is not None
            else current.resolved_product_guide_record_identifier
        )
        await self._publish_local_records(
            component_identifier=UPDATE_WORKSPACE_PRODUCT_GUIDE,
            records=(
                RecordDraft(
                    identifier=record_identifier,
                    kind=WORKSPACE_PRODUCT_GUIDE_BINDING_KIND,
                    schema_version=1,
                    value=record.model_dump(mode="json"),
                ),
            ),
            inputs=(
                NamedInput(name=("workspace",), object_identifier=workspace.record_identifier),
                NamedInput(name=("product_guide",), object_identifier=input_guide_identifier),
            ),
            outputs=(
                NamedOutput(
                    name=("product_guide_binding_state",),
                    object_identifier=record_identifier,
                ),
            ),
            result={"state": "completed", "binding_identifier": current.binding_identifier},
        )
        updated = await self.get_review_workspace(workspace.record_identifier)
        return self._workspace_product_guide_binding(updated, current.binding_identifier)

    async def set_workspace_default_product_guide(
        self, request: SetWorkspaceDefaultProductGuideRequest
    ) -> ReviewWorkspace:
        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        if request.binding_identifier is not None:
            binding = self._workspace_product_guide_binding(workspace, request.binding_identifier)
            if not binding.enabled:
                raise ReviewInputError(
                    "Enable a workspace product-guide binding before making it the default"
                )
        if workspace.default_product_guide_binding_identifier == request.binding_identifier:
            return workspace
        record_identifier = self.new_identifier()
        state = WorkspaceDefaultProductGuideStateRecord(
            record_identifier=record_identifier,
            workspace_record_identifier=workspace.record_identifier,
            binding_identifier=request.binding_identifier,
            recorded_at_utc=_utc_text(self.utc_now_ns()),
        )
        await self._publish_local_records(
            component_identifier=SET_WORKSPACE_DEFAULT_PRODUCT_GUIDE,
            records=(
                RecordDraft(
                    identifier=record_identifier,
                    kind=WORKSPACE_DEFAULT_PRODUCT_GUIDE_STATE_KIND,
                    schema_version=1,
                    value=state.model_dump(mode="json"),
                ),
            ),
            inputs=(
                NamedInput(name=("workspace",), object_identifier=workspace.record_identifier),
            ),
            outputs=(
                NamedOutput(
                    name=("default_product_guide_state",),
                    object_identifier=record_identifier,
                ),
            ),
            result={
                "state": "completed",
                "binding_identifier": request.binding_identifier,
            },
        )
        return await self.get_review_workspace(workspace.record_identifier)

    async def list_workspace_product_guides(
        self,
        workspace_record_identifier: str,
        *,
        include_disabled: bool = False,
    ) -> tuple[WorkspaceProductGuideBinding, ...]:
        workspace = await self.get_review_workspace(workspace_record_identifier)
        return tuple(
            binding
            for binding in workspace.product_guide_bindings
            if include_disabled or binding.enabled
        )

    async def list_workspace_search_track_versions(
        self, workspace_record_identifier: str, track_identifier: str
    ) -> tuple[WorkspaceSearchTrackSpecificationRecord, ...]:
        workspace = await self.get_review_workspace(workspace_record_identifier)
        track = next(
            (item for item in workspace.search_tracks if item.track_identifier == track_identifier),
            None,
        )
        if track is None:
            raise ReviewInputError("The search track is not part of this workspace")
        stored = await self.database.workspace_search_track_specifications(
            workspace_record_identifier, track_identifier
        )
        if stored:
            return tuple(
                WorkspaceSearchTrackSpecificationRecord.model_validate(value) for _, value in stored
            )
        if track.search_specification is None:
            raise ReviewInputError(
                "The legacy track lacks a complete retained search specification"
            )
        return (
            WorkspaceSearchTrackSpecificationRecord(
                workspace_record_identifier=workspace_record_identifier,
                track_identifier=track_identifier,
                version=1,
                search=track.search_specification,
                recorded_at_utc=workspace.created_at_utc,
            ),
        )

    async def revise_workspace_search_track(
        self, request: ReviseWorkspaceSearchTrackRequest
    ) -> WorkspaceSearchTrack:
        """Change future search intent without changing track identity or retained evidence."""

        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        track = next(
            (
                item
                for item in workspace.search_tracks
                if item.track_identifier == request.track_identifier
            ),
            None,
        )
        if track is None:
            raise ReviewInputError("The search track is not part of this workspace")
        if track.search_specification_version != request.expected_version:
            raise ReviewInputError(
                f"Search track version conflict: expected {request.expected_version}, "
                f"current {track.search_specification_version}; read the current track before revising"
            )
        search = (
            FacebookSearchTargetSpecification(search=request.search)
            if isinstance(request.search, CreateSearchRequest)
            else request.search
        )
        if search.marketplace != track.marketplace:
            raise ReviewInputError(
                "A search track's marketplace cannot change; create another track"
            )
        if isinstance(search, EbaySearchTargetSpecification):
            _require_ebay_search_acquisition(search.search)
        else:
            CollectSearchPayload(
                request=search.search.request,
                traversal=search.search.traversal,
                traversal_strategy=search.search.traversal_strategy,
                routing=search.search.requested_network_path,
            )
        if track.search_specification is None:
            raise ReviewInputError(
                "The legacy track lacks a complete retained search specification"
            )
        if track.search_specification == search:
            return track
        records = []
        previous_identifier = track.search_specification_record_identifier
        if previous_identifier is None:
            previous_identifier = self.new_identifier()
            original = WorkspaceSearchTrackSpecificationRecord(
                record_identifier=previous_identifier,
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=track.track_identifier,
                version=1,
                search=track.search_specification,
                recorded_at_utc=workspace.created_at_utc,
            )
            records.append(
                RecordDraft(
                    identifier=previous_identifier,
                    kind=WORKSPACE_SEARCH_TRACK_SPECIFICATION_KIND,
                    schema_version=1,
                    value=original.model_dump(mode="json"),
                )
            )
        identifier = self.new_identifier()
        revision = WorkspaceSearchTrackSpecificationRecord(
            record_identifier=identifier,
            workspace_record_identifier=workspace.record_identifier,
            track_identifier=track.track_identifier,
            version=request.expected_version + 1,
            search=search,
            previous_record_identifier=previous_identifier,
            recorded_at_utc=_utc_text(self.utc_now_ns()),
        )
        records.append(
            RecordDraft(
                identifier=identifier,
                kind=WORKSPACE_SEARCH_TRACK_SPECIFICATION_KIND,
                schema_version=1,
                value=revision.model_dump(mode="json"),
            )
        )
        inputs = (NamedInput(name=("workspace",), object_identifier=workspace.record_identifier),)
        if track.search_specification_record_identifier is not None:
            inputs += (
                NamedInput(
                    name=("previous_specification",),
                    object_identifier=track.search_specification_record_identifier,
                ),
            )
        try:
            await self._publish_local_records(
                component_identifier=REVISE_WORKSPACE_SEARCH_TRACK,
                records=tuple(records),
                inputs=inputs,
                outputs=tuple(
                    NamedOutput(
                        name=("search_specification", str(index)),
                        object_identifier=record.identifier,
                    )
                    for index, record in enumerate(records)
                ),
                result={
                    "state": "completed",
                    "track_identifier": track.track_identifier,
                    "version": revision.version,
                },
                workspace_track_guard=(
                    workspace.record_identifier,
                    track.track_identifier,
                    request.expected_version,
                ),
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error
        return track.model_copy(
            update={
                "search_specification": search,
                "search_specification_version": revision.version,
                "search_specification_record_identifier": identifier,
                "query": search.search.query
                if isinstance(search, EbaySearchTargetSpecification)
                else search.search.request.query,
                "listing_state": search.search.listing_state
                if isinstance(search, EbaySearchTargetSpecification)
                else None,
            }
        )

    async def set_workspace_search_track_enabled(
        self, request: SetWorkspaceSearchTrackEnabledRequest
    ) -> WorkspaceSearchTrack:
        """Enable or disable one workspace search track without deleting its history."""

        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        track = next(
            (
                candidate
                for candidate in workspace.search_tracks
                if candidate.track_identifier == request.track_identifier
            ),
            None,
        )
        if track is None:
            raise ReviewInputError("The search track is not part of this workspace")
        if track.enabled == request.enabled:
            return track
        record_identifier = self.new_identifier()
        state = WorkspaceSearchTrackStateRecord(
            record_identifier=record_identifier,
            workspace_record_identifier=workspace.record_identifier,
            track_identifier=track.track_identifier,
            enabled=request.enabled,
            recorded_at_utc=_utc_text(self.utc_now_ns()),
        )
        await self._publish_local_records(
            component_identifier=SET_WORKSPACE_SEARCH_TRACK_ENABLED,
            records=(
                RecordDraft(
                    identifier=record_identifier,
                    kind=WORKSPACE_SEARCH_TRACK_STATE_KIND,
                    schema_version=1,
                    value=state.model_dump(mode="json"),
                ),
            ),
            inputs=(
                NamedInput(name=("workspace",), object_identifier=workspace.record_identifier),
            ),
            outputs=(NamedOutput(name=("track_state",), object_identifier=record_identifier),),
            result={"state": "completed", "enabled": request.enabled},
        )
        return track.model_copy(update={"enabled": request.enabled})

    async def list_review_workspaces(
        self, *, include_archived: bool = False
    ) -> tuple[ReviewWorkspace, ...]:
        stored = tuple(
            ReviewWorkspace.model_validate_json(encode_json(value))
            for _, value in await self.database.records_by_kind(REVIEW_WORKSPACE_KIND)
        )
        workspaces = tuple(
            [await self.get_review_workspace(workspace.record_identifier) for workspace in stored]
        )
        return tuple(
            workspace for workspace in workspaces if include_archived or not workspace.archived
        )

    @staticmethod
    def _workspace_current_search_runs(workspace: ReviewWorkspace) -> tuple[str, ...]:
        current_runs = tuple(
            track.current_search_run_record_identifier
            for track in workspace.search_tracks
            if track.enabled and track.current_search_run_record_identifier is not None
        )
        return (
            current_runs if workspace.search_tracks else (workspace.search_run_record_identifier,)
        )

    async def get_review_workspace_activity(
        self,
        workspace_record_identifier: str,
        *,
        maximum_recent_batches: int = 20,
        maximum_recent_reviews: int = 100,
        maximum_recent_selection_snapshots: int = 20,
    ) -> ReviewWorkspaceActivity:
        """Return bounded, rediscoverable review state for one workspace."""

        if not 0 <= maximum_recent_batches <= 100:
            raise ReviewInputError("Recent batch limit must be between zero and 100")
        if not 0 <= maximum_recent_reviews <= 500:
            raise ReviewInputError("Recent review limit must be between zero and 500")
        if not 0 <= maximum_recent_selection_snapshots <= 100:
            raise ReviewInputError("Recent snapshot limit must be between zero and 100")
        workspace = await self.get_review_workspace(workspace_record_identifier)
        batches = tuple(
            ReviewBatch.model_validate_json(encode_json(value))
            for _, value in await self.database.records_by_kind(REVIEW_BATCH_KIND)
        )
        reviews = tuple(
            ListingReviewRecord.model_validate_json(encode_json(value))
            for _, value in await self.database.records_by_kind(LISTING_REVIEW_KIND)
        )
        snapshots = tuple(
            SelectionSnapshot.model_validate_json(encode_json(value))
            for _, value in await self.database.records_by_kind(SELECTION_SNAPSHOT_KIND)
        )
        current_worksets = tuple(
            sorted(
                (
                    ReviewWorksetSummary(
                        record_identifier=workset.record_identifier,
                        workset_identifier=workset.workset_identifier,
                        name=workset.name,
                        version=workset.version,
                        member_count=len(workset.listing_identifiers),
                    )
                    for workset in (await self._current_review_worksets()).values()
                    if workset.workspace_record_identifier == workspace.record_identifier
                ),
                key=lambda workset: (workset.name.casefold(), workset.workset_identifier),
            )
        )
        matching_batches = tuple(
            batch
            for batch in batches
            if batch.workspace_record_identifier == workspace.record_identifier
        )
        matching_reviews = tuple(
            review
            for review in reviews
            if review.workspace_record_identifier == workspace.record_identifier
        )
        matching_snapshots = tuple(
            snapshot
            for snapshot in snapshots
            if snapshot.workspace_record_identifier == workspace.record_identifier
        )
        active_claims = await self.database.active_review_claims(
            workspace_record_identifier=workspace.record_identifier,
            now_utc_ns=self.utc_now_ns(),
        )
        return ReviewWorkspaceActivity(
            workspace=workspace,
            recent_batches=tuple(
                ReviewBatchSummary(
                    record_identifier=batch.record_identifier,
                    created_at_utc=batch.created_at_utc,
                    item_count=len(batch.items),
                    next_cursor=batch.next_cursor,
                )
                for batch in reversed(
                    matching_batches[-maximum_recent_batches:] if maximum_recent_batches else ()
                )
            ),
            recent_reviews=tuple(
                reversed(
                    matching_reviews[-maximum_recent_reviews:] if maximum_recent_reviews else ()
                )
            ),
            current_worksets=current_worksets,
            recent_selection_snapshots=tuple(
                SelectionSnapshotSummary(
                    record_identifier=snapshot.record_identifier,
                    created_at_utc=snapshot.created_at_utc,
                    source_kind=snapshot.source.kind,
                    item_count=len(snapshot.items),
                )
                for snapshot in reversed(
                    matching_snapshots[-maximum_recent_selection_snapshots:]
                    if maximum_recent_selection_snapshots
                    else ()
                )
            ),
            active_claims=tuple(
                ReviewClaimSummary(
                    batch_record_identifier=claim.batch_record_identifier,
                    owner_identifier=claim.owner_identifier,
                    lease_expires_at_utc_ns=claim.lease_expires_at_utc_ns,
                    claimed_listing_count=len(claim.listing_identifiers),
                )
                for claim in active_claims[:100]
            ),
        )

    async def _workspace_work_status(
        self,
        workspace_record_identifier: str,
        *,
        maximum_active_work: int,
        maximum_failed_work: int,
    ) -> WorkspaceWorkStatus:
        (
            queued_count,
            in_progress_count,
            terminal_failure_count,
            active_work,
            failed_work,
        ) = await self.database.workspace_active_work(
            workspace_record_identifier=workspace_record_identifier,
            maximum_rows=maximum_active_work,
            maximum_failure_rows=maximum_failed_work,
        )
        idle = queued_count == 0 and in_progress_count == 0
        partial_failures = await self.database.workspace_partial_failure_count(
            workspace_record_identifier
        )
        return WorkspaceWorkStatus(
            workspace_record_identifier=workspace_record_identifier,
            captured_at_utc_ns=self.utc_now_ns(),
            queued_count=queued_count,
            in_progress_count=in_progress_count,
            terminal_failure_count=terminal_failure_count,
            completed_with_failures_count=partial_failures,
            active_work=active_work,
            active_work_truncated=queued_count + in_progress_count > len(active_work),
            failed_work=failed_work,
            failed_work_truncated=terminal_failure_count + partial_failures > len(failed_work),
            idle=idle,
            successful=idle and terminal_failure_count == 0 and partial_failures == 0,
        )

    async def get_workspace_work_status(
        self,
        workspace_record_identifier: str,
        *,
        maximum_active_work: int = 20,
        maximum_failed_work: int = 20,
    ) -> WorkspaceWorkStatus:
        """Return queued and in-progress root work requested by one workspace."""

        if not 1 <= maximum_active_work <= 100:
            raise ReviewInputError("Active work limit must be between one and 100")
        if not 1 <= maximum_failed_work <= 100:
            raise ReviewInputError("Failed work limit must be between one and 100")
        await self.get_review_workspace(workspace_record_identifier)
        return await self._workspace_work_status(
            workspace_record_identifier,
            maximum_active_work=maximum_active_work,
            maximum_failed_work=maximum_failed_work,
        )

    async def wait_for_workspace_work(
        self,
        workspace_record_identifier: str,
        *,
        timeout_seconds: float | None = None,
        maximum_active_work: int = 20,
        maximum_failed_work: int = 20,
        progress: Callable[[WorkspaceWorkStatus, float], Awaitable[None]] | None = None,
    ) -> WorkspaceWorkWaitResult:
        """Wait briefly until a workspace is idle, reporting bounded progress when supplied."""

        effective_timeout_seconds = 30.0 if timeout_seconds is None else timeout_seconds
        if not 0 <= effective_timeout_seconds <= 300:
            raise ReviewInputError("Timeout must be between zero and 300 seconds")
        if not 1 <= maximum_active_work <= 100:
            raise ReviewInputError("Active work limit must be between one and 100")
        if not 1 <= maximum_failed_work <= 100:
            raise ReviewInputError("Failed work limit must be between one and 100")
        await self.get_review_workspace(workspace_record_identifier)
        started = anyio.current_time()
        deadline = started + effective_timeout_seconds
        next_progress_at = started
        while True:
            status = await self._workspace_work_status(
                workspace_record_identifier,
                maximum_active_work=maximum_active_work,
                maximum_failed_work=maximum_failed_work,
            )
            now = anyio.current_time()
            if status.idle:
                return WorkspaceWorkWaitResult(
                    status=status,
                    timed_out=False,
                    waited_seconds=max(0.0, now - started),
                )
            if now >= deadline:
                return WorkspaceWorkWaitResult(
                    status=status,
                    timed_out=True,
                    waited_seconds=max(0.0, now - started),
                )
            if progress is not None and now >= next_progress_at:
                await progress(status, max(0.0, now - started))
                next_progress_at = now + 5.0
            await anyio.sleep(min(0.5, deadline - now))

    async def _latest_listing_reviews(
        self, workspace_record_identifier: str, listing_identifiers: tuple[str, ...]
    ) -> dict[str, ListingReviewRecord]:
        return await self.database.latest_listing_reviews(
            workspace_record_identifier=workspace_record_identifier,
            listing_identifiers=listing_identifiers,
        )

    @staticmethod
    def _historical_scalar_fields(
        observations: Sequence[ListingObservationCandidate],
        cards: Sequence[SearchCardCandidate],
        *,
        boundary: int,
        inherit_currency: bool,
    ) -> ScalarFieldValues:
        values: dict[str, JsonValue] = {}
        for name in ("title", "price", "location", "description", "seller"):
            selected = select_composed_field(
                "location_text" if name == "location" else name,
                observations,
                cards,
                as_of_completion_sequence=boundary,
                inherit_price_currency=inherit_currency,
            )
            values[name] = None if selected is None else selected.value
        return ScalarFieldValues.model_validate(values)

    async def _review_scalar_baselines(
        self,
        reviews: tuple[ListingReviewRecord, ...],
        *,
        cache: dict[str, ScalarFieldValues | None] | None = None,
        batches: dict[str, ReviewBatch] | None = None,
    ) -> dict[str, ScalarFieldValues | None]:
        """Recover exact reviewed values; never infer an old review from current evidence."""

        cache = {} if cache is None else cache
        batches = {} if batches is None else batches
        missing = [review for review in reviews if review.record_identifier not in cache]
        boundaries = await self.database.listing_review_scalar_boundaries(
            tuple(
                review.record_identifier
                for review in missing
                if review.scalar_fields_snapshot is None and review.batch_record_identifier is None
            )
        )
        historical: dict[int, list[ListingReviewRecord]] = {}
        for review in missing:
            cache[review.record_identifier] = None
            if review.scalar_fields_snapshot is not None:
                cache[review.record_identifier] = normalize_scalar_fields(
                    normalize_ebay_scalar_fields(review.scalar_fields_snapshot)
                    if review.listing_identifier.startswith("ebay:")
                    else review.scalar_fields_snapshot
                )
                continue
            if review.batch_record_identifier is not None:
                identifier = review.batch_record_identifier
                if identifier not in batches:
                    batches[identifier] = await self.get_review_batch(identifier)
                old = next(
                    (
                        item.projection
                        for item in batches[identifier].items
                        if item.projection.listing_identifier == review.listing_identifier
                        and item.projection.projection_revision == review.projection_revision
                    ),
                    None,
                )
                if old is not None:
                    old_fields = projection_scalar_fields(old)
                    cache[review.record_identifier] = normalize_scalar_fields(
                        normalize_ebay_scalar_fields(old_fields)
                        if review.listing_identifier.startswith("ebay:")
                        else old_fields
                    )
                    if (
                        review.projection_revision.recipe_version == 1
                        and review.listing_identifier.isdecimal()
                        and old.price is not None
                        and isinstance(old.price.value, dict)
                        and not old.price.value.get("currency")
                    ):
                        historical.setdefault(old.as_of_completion_sequence, []).append(review)
                continue
            boundary = boundaries.get(review.record_identifier)
            if boundary is not None and review.listing_identifier.isdecimal():
                historical.setdefault(boundary, []).append(review)
        for boundary, group in historical.items():
            for start in range(0, len(group), 100):
                chunk = group[start : start + 100]
                identifiers = tuple(review.listing_identifier for review in chunk)
                observations = await self.database.facebook_projection_item_observations(
                    identifiers, as_of_completion_sequence=boundary, maximum_per_listing=100
                )
                cards = await self.database.facebook_projection_search_cards(
                    identifiers, as_of_completion_sequence=boundary, maximum_per_listing=100
                )
                observations_by_listing: dict[str, list[ListingObservationCandidate]] = {}
                cards_by_listing: dict[str, list[SearchCardCandidate]] = {}
                for observation in observations:
                    observations_by_listing.setdefault(observation.listing_identifier, []).append(
                        observation
                    )
                for card in cards:
                    cards_by_listing.setdefault(card.listing_identifier, []).append(card)
                for review in chunk:
                    selected_observations = observations_by_listing.get(
                        review.listing_identifier, ()
                    )
                    selected_cards = cards_by_listing.get(review.listing_identifier, ())
                    raw = self._historical_scalar_fields(
                        selected_observations,
                        selected_cards,
                        boundary=boundary,
                        inherit_currency=False,
                    )
                    # A matching legacy hash proves reconstruction used the values actually reviewed.
                    if (
                        scalar_fields_sha256(
                            raw, normalize=review.projection_revision.recipe_version != 1
                        )
                        == review.projection_revision.scalar_fields_sha256
                    ):
                        cache[review.record_identifier] = normalize_scalar_fields(
                            self._historical_scalar_fields(
                                selected_observations,
                                selected_cards,
                                boundary=boundary,
                                inherit_currency=True,
                            )
                        )
        return cache

    async def _build_review_batch_candidate(
        self,
        request: CreateReviewBatchRequest,
        *,
        created_at_utc_ns: int,
    ) -> ReviewBatch:
        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        current_runs = self._workspace_current_search_runs(workspace)
        if not current_runs:
            raise ReviewInputError("The workspace has no completed active search track")
        selected: list[ReviewBatchItem] = []
        scalar_cache: dict[str, ScalarFieldValues | None] = {}
        batch_cache: dict[str, ReviewBatch] = {}
        cursor = request.cursor
        scanned_pages = 0
        while len(selected) < request.page_size and scanned_pages < request.maximum_scan_pages:
            page = await self.list_composed_search(
                ListComposedSearchRequest(
                    search_run_record_identifier=current_runs[0],
                    additional_search_run_record_identifiers=current_runs[1:],
                    filters=ComposedListingFilters(
                        statuses=request.statuses,
                        product_guide_record_identifier=(workspace.product_guide_record_identifier),
                    ),
                    maximum_gallery_images_per_listing=10,
                    maximum_analyses_per_listing=5,
                    page_size=request.page_size - len(selected),
                    cursor=cursor,
                )
            )
            scanned_pages += 1
            identifiers = tuple(item.listing_identifier for item in page.listings)
            prior_by_listing = await self._latest_listing_reviews(
                workspace.record_identifier, identifiers
            )
            baselines = await self._review_scalar_baselines(
                tuple(prior_by_listing.values()), cache=scalar_cache, batches=batch_cache
            )
            claimed_identifiers = {
                listing_identifier
                for claim in await self.database.active_review_claims(
                    workspace_record_identifier=workspace.record_identifier,
                    now_utc_ns=created_at_utc_ns,
                    listing_identifiers=identifiers,
                )
                for listing_identifier in claim.listing_identifiers
            }
            for projection in page.listings:
                if projection.listing_identifier in claimed_identifiers:
                    continue
                prior = prior_by_listing.get(projection.listing_identifier)
                baseline = None if prior is None else baselines.get(prior.record_identifier)
                state, changed = classify_review_state(
                    current_revision=projection.projection_revision,
                    previous_review=prior,
                    policy=workspace.staleness_policy,
                    previous_scalar_fields=baseline,
                )
                if state in request.include_review_states:
                    selected.append(
                        ReviewBatchItem(
                            projection=projection,
                            review_state=state,
                            prior_review=prior,
                            changed_components=changed,
                            scalar_field_changes=(
                                ()
                                if baseline is None
                                else scalar_field_changes(
                                    baseline, projection_scalar_fields(projection)
                                )
                            ),
                            scalar_comparison_available=baseline is not None,
                        )
                    )
            cursor = page.next_cursor
            if cursor is None:
                break
        record_identifier = self.new_identifier()
        batch = ReviewBatch(
            record_identifier=record_identifier,
            workspace_record_identifier=workspace.record_identifier,
            created_at_utc=_utc_text(created_at_utc_ns),
            items=tuple(selected),
            next_cursor=cursor,
            scanned_page_count=scanned_pages,
        )
        return batch

    async def create_review_batch(self, request: CreateReviewBatchRequest) -> ReviewBatch:
        batch = await self._build_review_batch_candidate(
            request, created_at_utc_ns=self.utc_now_ns()
        )
        await self._publish_local_records(
            component_identifier=CREATE_REVIEW_BATCH,
            records=(
                RecordDraft(
                    identifier=batch.record_identifier,
                    kind=REVIEW_BATCH_KIND,
                    schema_version=1,
                    value=batch.model_dump(mode="json"),
                ),
            ),
            inputs=(
                NamedInput(
                    name=("workspace",),
                    object_identifier=batch.workspace_record_identifier,
                ),
            ),
            outputs=(
                NamedOutput(name=("review_batch",), object_identifier=batch.record_identifier),
            ),
            result={"state": "completed", "item_count": len(batch.items)},
        )
        return batch

    async def acquire_review_batch(
        self, request: AcquireReviewBatchRequest
    ) -> ReviewBatchAcquisition:
        """Atomically issue a batch and claim its listings for one review agent."""

        action = "acquire_batch"
        request_sha256 = review_mutation_request_sha256(action, request)
        try:
            replay = await self.database.review_mutation_replay(
                action=action,
                request_identifier=request.request_identifier,
                request_sha256=request_sha256,
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error
        if replay is not None:
            return ReviewBatchAcquisition.model_validate_json(encode_json(replay))

        started_utc_ns = self.utc_now_ns()
        started_monotonic_ns = self.monotonic_ns()
        candidate = await self._build_review_batch_candidate(
            request, created_at_utc_ns=started_utc_ns
        )
        provenance = await self._code_provenance()
        ended_utc_ns = self.utc_now_ns()
        try:
            return await self.database.publish_acquired_review_batch(
                candidate_batch=candidate,
                claim_token=self.new_identifier(),
                owner_identifier=request.owner_identifier,
                utc_now_ns=self.utc_now_ns,
                lease_duration_ns=request.lease_duration_seconds * 1_000_000_000,
                action=action,
                request_identifier=request.request_identifier,
                request_sha256=request_sha256,
                component=build_review_workspace_component_registry().require(ACQUIRE_REVIEW_BATCH),
                operation_identifier=self.new_identifier(),
                provenance=provenance,
                invocation=process_invocation(),
                started_at_utc=_utc_text(started_utc_ns),
                ended_at_utc=_utc_text(ended_utc_ns),
                duration_ns=max(0, self.monotonic_ns() - started_monotonic_ns),
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error

    async def renew_review_claim(self, request: RenewReviewClaimRequest) -> ReviewClaimLease:
        """Extend one active claim without changing its ownership or membership."""

        action = "renew_claim"
        request_sha256 = review_mutation_request_sha256(action, request)
        try:
            replay = await self.database.review_mutation_replay(
                action=action,
                request_identifier=request.request_identifier,
                request_sha256=request_sha256,
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error
        if replay is not None:
            return ReviewClaimLease.model_validate_json(encode_json(replay))
        started_utc_ns = self.utc_now_ns()
        started_monotonic_ns = self.monotonic_ns()
        provenance = await self._code_provenance()
        ended_utc_ns = self.utc_now_ns()
        try:
            lease = await self.database.renew_review_claim(
                claim_token=request.claim_token,
                owner_identifier=request.owner_identifier,
                utc_now_ns=self.utc_now_ns,
                lease_duration_ns=request.lease_duration_seconds * 1_000_000_000,
                action=action,
                request_identifier=request.request_identifier,
                request_sha256=request_sha256,
                component=build_review_workspace_component_registry().require(RENEW_REVIEW_CLAIM),
                operation_identifier=self.new_identifier(),
                provenance=provenance,
                invocation=process_invocation(),
                started_at_utc=_utc_text(started_utc_ns),
                ended_at_utc=_utc_text(ended_utc_ns),
                duration_ns=max(0, self.monotonic_ns() - started_monotonic_ns),
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error
        if lease is None:
            raise ReviewInputError("The review claim is missing, expired, or owned elsewhere")
        return lease

    async def release_review_claim(
        self, request: ReleaseReviewClaimRequest
    ) -> ReleaseReviewClaimResult:
        """Release every remaining listing held by one active claim."""

        action = "release_claim"
        request_sha256 = review_mutation_request_sha256(action, request)
        try:
            replay = await self.database.review_mutation_replay(
                action=action,
                request_identifier=request.request_identifier,
                request_sha256=request_sha256,
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error
        if replay is not None:
            return ReleaseReviewClaimResult.model_validate_json(encode_json(replay))
        started_utc_ns = self.utc_now_ns()
        started_monotonic_ns = self.monotonic_ns()
        provenance = await self._code_provenance()
        ended_utc_ns = self.utc_now_ns()
        try:
            result = await self.database.release_review_claim(
                claim_token=request.claim_token,
                owner_identifier=request.owner_identifier,
                utc_now_ns=self.utc_now_ns,
                action=action,
                request_identifier=request.request_identifier,
                request_sha256=request_sha256,
                component=build_review_workspace_component_registry().require(RELEASE_REVIEW_CLAIM),
                operation_identifier=self.new_identifier(),
                provenance=provenance,
                invocation=process_invocation(),
                started_at_utc=_utc_text(started_utc_ns),
                ended_at_utc=_utc_text(ended_utc_ns),
                duration_ns=max(0, self.monotonic_ns() - started_monotonic_ns),
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error
        if result is None:
            raise ReviewInputError("The review claim is missing, expired, or owned elsewhere")
        return result

    async def get_review_batch(self, record_identifier: str) -> ReviewBatch:
        kind, schema_version, value = await self.database.get_record(record_identifier)
        if kind != REVIEW_BATCH_KIND or schema_version != 1:
            raise ReviewInputError("The record is not a supported review batch")
        return ReviewBatch.model_validate_json(encode_json(value))

    async def record_listing_reviews(
        self, request: RecordListingReviewsRequest
    ) -> RecordListingReviewsResult:
        action = "record_reviews"
        request_sha256 = review_mutation_request_sha256(action, request)
        try:
            replay = await self.database.review_mutation_replay(
                action=action,
                request_identifier=request.request_identifier,
                request_sha256=request_sha256,
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error
        if replay is not None:
            return RecordListingReviewsResult.model_validate_json(encode_json(replay))

        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        batch = (
            None
            if request.batch_record_identifier is None
            else await self.get_review_batch(request.batch_record_identifier)
        )
        if batch is not None and batch.workspace_record_identifier != workspace.record_identifier:
            raise ReviewInputError("The review batch belongs to a different workspace")
        reviewed_scalars = (
            {}
            if batch is None
            else {
                item.projection.listing_identifier: projection_scalar_fields(item.projection)
                for item in batch.items
            }
        )
        batch_revisions = (
            {}
            if batch is None
            else {
                item.projection.listing_identifier: item.projection.projection_revision
                for item in batch.items
            }
        )
        for review in request.reviews:
            if batch is not None:
                expected = batch_revisions.get(review.listing_identifier)
                if expected is None:
                    raise ReviewInputError("A reviewed listing is not in the review batch")
                if expected != review.projection_revision:
                    raise ReviewInputError("A review revision does not match the review batch")
            else:
                current_runs = self._workspace_current_search_runs(workspace)
                if not current_runs:
                    raise ReviewInputError("The workspace has no completed search track")
                current = await self.get_composed_listing(
                    GetComposedListingRequest(
                        listing_identifier=review.listing_identifier,
                        search_run_record_identifier=current_runs[0],
                        additional_search_run_record_identifiers=current_runs[1:],
                        product_guide_record_identifier=(workspace.product_guide_record_identifier),
                        maximum_gallery_images=0,
                        maximum_analyses=0,
                    )
                )
                if current.projection_revision != review.projection_revision:
                    raise ReviewInputError("A review revision is no longer current")
                reviewed_scalars[review.listing_identifier] = projection_scalar_fields(current)
        started_utc_ns = self.utc_now_ns()
        started_monotonic_ns = self.monotonic_ns()
        recorded_at = _utc_text(started_utc_ns)
        records = tuple(
            ListingReviewRecord(
                record_identifier=self.new_identifier(),
                workspace_record_identifier=workspace.record_identifier,
                batch_record_identifier=request.batch_record_identifier,
                listing_identifier=review.listing_identifier,
                projection_revision=review.projection_revision,
                inspected=review.inspected,
                disposition=review.disposition,
                note=review.note,
                recorded_at_utc=recorded_at,
                scalar_fields_snapshot=(
                    reviewed_scalars[review.listing_identifier]
                    if review.projection_revision.recipe_version == 2
                    else None
                ),
            )
            for review in request.reviews
        )
        provenance = await self._code_provenance()
        ended_utc_ns = self.utc_now_ns()
        try:
            published = await self.database.publish_listing_review_records(
                action=action,
                request_identifier=request.request_identifier,
                request_sha256=request_sha256,
                workspace_record_identifier=workspace.record_identifier,
                batch_record_identifier=request.batch_record_identifier,
                claim_token=request.claim_token,
                claim_owner_identifier=request.claim_owner_identifier,
                utc_now_ns=self.utc_now_ns,
                component=build_review_workspace_component_registry().require(
                    RECORD_LISTING_REVIEWS
                ),
                operation_identifier=self.new_identifier(),
                records=tuple(
                    RecordDraft(
                        identifier=record.record_identifier,
                        kind=LISTING_REVIEW_KIND,
                        schema_version=1,
                        value=record.model_dump(mode="json"),
                    )
                    for record in records
                ),
                outputs=tuple(
                    NamedOutput(
                        name=("listing_review", str(index)),
                        object_identifier=record.record_identifier,
                    )
                    for index, record in enumerate(records)
                ),
                provenance=provenance,
                invocation=process_invocation(),
                started_at_utc=_utc_text(started_utc_ns),
                ended_at_utc=_utc_text(ended_utc_ns),
                duration_ns=max(0, self.monotonic_ns() - started_monotonic_ns),
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error
        if published is None:
            raise ReviewInputError(
                "An active review claim blocks these listings, or the supplied claim was lost"
            )
        return published

    async def _workspace_bulk_review_projections(
        self,
        workspace: ReviewWorkspace,
        *,
        maximum_candidate_listings_examined: int,
        selected_listing_identifiers: tuple[str, ...] | None = None,
    ) -> tuple[int, int, tuple[ComposedListingProjection, ...]]:
        """Compose one bounded workspace snapshot without per-page repeated evidence queries."""

        async with self.database._connections.reader():
            return await self._workspace_bulk_review_projections_at_snapshot(
                workspace,
                maximum_candidate_listings_examined=maximum_candidate_listings_examined,
                selected_listing_identifiers=selected_listing_identifiers,
            )

    async def _workspace_bulk_review_projections_at_snapshot(
        self,
        workspace: ReviewWorkspace,
        *,
        maximum_candidate_listings_examined: int,
        selected_listing_identifiers: tuple[str, ...] | None = None,
    ) -> tuple[int, int, tuple[ComposedListingProjection, ...]]:
        """Share a stable read transaction across source-specific batch composition."""

        current_runs = self._workspace_current_search_runs(workspace)
        if not current_runs:
            raise ReviewInputError("The workspace has no completed active search track")
        if await scope_contains_ebay(self.database, current_runs):
            return await self._marketplace_workspace_projections(
                workspace,
                maximum_candidate_listings_examined=maximum_candidate_listings_examined,
                selected_listing_identifiers=selected_listing_identifiers,
            )
        return await self._facebook_workspace_bulk_review_projections(
            workspace,
            current_runs=current_runs,
            as_of=await self.database.current_completion_boundary(),
            maximum_candidate_listings_examined=maximum_candidate_listings_examined,
            selected_listing_identifiers=selected_listing_identifiers,
        )

    async def _facebook_workspace_bulk_review_projections(
        self,
        workspace: ReviewWorkspace,
        *,
        current_runs: tuple[str, ...],
        as_of: int,
        maximum_candidate_listings_examined: int,
        selected_listing_identifiers: tuple[str, ...] | None = None,
        require_complete_ancestry: bool = True,
    ) -> tuple[int, int, tuple[ComposedListingProjection, ...]]:
        """Batch Facebook review evidence once, including within mixed workspaces."""

        ancestry = await self._projection_search_scope(
            current_runs[0],
            current_runs[1:],
            as_of_completion_sequence=as_of,
            maximum_runs=100,
        )
        if require_complete_ancestry and ancestry.older_ancestry_truncated:
            raise ReviewInputError(
                "Bulk review requires complete workspace search ancestry within 100 runs"
            )
        included_runs = tuple(
            (run.record_identifier, run.internal_search_run_identifier) for run in ancestry.runs
        )
        if selected_listing_identifiers is None:
            membership_with_lookahead = (
                await self.database.facebook_projection_membership_candidates(
                    included_runs,
                    as_of_completion_sequence=as_of,
                    maximum_listings=maximum_candidate_listings_examined + 1,
                )
            )
            if len(membership_with_lookahead) > maximum_candidate_listings_examined:
                raise ReviewInputError(
                    "Bulk review candidate limit reached; no reviews were recorded"
                )
            listing_identifiers = tuple(
                candidate.candidate.listing_identifier for candidate in membership_with_lookahead
            )
            candidate_listings_examined = len(listing_identifiers)
        else:
            listing_identifiers = tuple(dict.fromkeys(selected_listing_identifiers))
            if len(listing_identifiers) > maximum_candidate_listings_examined:
                raise ReviewInputError(
                    "Bulk review selection exceeds the candidate limit; no reviews were recorded"
                )
            candidate_listings_examined = len(listing_identifiers)
        if not listing_identifiers:
            return as_of, 0, ()

        membership_occurrences: list[SearchMembershipOccurrenceCandidate] = []
        chunk_size = 100
        for start in range(0, len(listing_identifiers), chunk_size):
            membership_occurrences.extend(
                await self.database.facebook_projection_membership_occurrences(
                    listing_identifiers[start : start + chunk_size],
                    included_runs,
                    as_of_completion_sequence=as_of,
                )
            )
        membership_by_listing: dict[str, list[SearchMembershipOccurrenceCandidate]] = {}
        for occurrence in membership_occurrences:
            membership_by_listing.setdefault(occurrence.listing_identifier, []).append(occurrence)
        listing_identifiers = tuple(
            identifier for identifier in listing_identifiers if identifier in membership_by_listing
        )
        if not listing_identifiers:
            return as_of, candidate_listings_examined, ()

        observations = await self.database.facebook_projection_item_observations(
            listing_identifiers,
            as_of_completion_sequence=as_of,
            maximum_per_listing=101,
        )
        search_statuses = await self.database.facebook_projection_search_occurrences(
            listing_identifiers,
            as_of_completion_sequence=as_of,
        )
        search_cards = await self.database.facebook_projection_search_cards(
            listing_identifiers,
            as_of_completion_sequence=as_of,
            maximum_per_listing=101,
        )
        frozen_observations, truncated_observations = self._bounded_observations_by_listing(
            observations,
            maximum_per_listing=100,
        )
        frozen_search_cards, truncated_search_cards = self._bounded_search_cards_by_listing(
            search_cards,
            maximum_per_listing=100,
        )
        analyses_by_listing = await self._projection_analyses_by_listing(
            listing_identifiers,
            product_guide_record_identifier=workspace.product_guide_record_identifier,
            maximum_per_listing=21,
            as_of_completion_sequence=as_of,
        )
        statuses_by_listing: dict[str, list[StatusObservationCandidate]] = {}
        for status in search_statuses:
            statuses_by_listing.setdefault(status.listing_identifier, []).append(status)

        galleries: dict[str, ComposedGallery | None] = {}
        for start in range(0, len(listing_identifiers), chunk_size):
            chunk = listing_identifiers[start : start + chunk_size]
            galleries.update(
                await self._projection_galleries(
                    {identifier: frozen_observations.get(identifier, ()) for identifier in chunk},
                    as_of_completion_sequence=as_of,
                )
            )

        projections: list[ComposedListingProjection] = []
        for listing_identifier in listing_identifiers:
            warnings = tuple(
                warning
                for condition, warning in (
                    (
                        listing_identifier in truncated_observations,
                        "item_observation_history_truncated",
                    ),
                    (
                        listing_identifier in truncated_search_cards,
                        "search_card_history_truncated",
                    ),
                )
                if condition
            )
            projection, _ = self._compose_listing_projection(
                listing_identifier,
                observations=frozen_observations.get(listing_identifier, ()),
                search_cards=frozen_search_cards.get(listing_identifier, ()),
                search_statuses=tuple(statuses_by_listing.get(listing_identifier, ())),
                analyses=analyses_by_listing.get(listing_identifier, ()),
                as_of_completion_sequence=as_of,
                product_guide_record_identifier=workspace.product_guide_record_identifier,
                maximum_analyses=0,
                maximum_gallery_images=0,
                gallery=galleries.get(listing_identifier),
                membership=compose_search_membership(
                    listing_identifier=listing_identifier,
                    ancestry=ancestry,
                    occurrences=membership_by_listing.get(listing_identifier, ()),
                ),
                warnings=warnings,
            )
            projections.append(projection)
        return as_of, candidate_listings_examined, tuple(projections)

    async def record_workspace_bulk_review(
        self, request: RecordWorkspaceBulkReviewRequest
    ) -> RecordWorkspaceBulkReviewResult:
        """Record one server-snapshotted disposition over a bounded workspace selection."""

        action = "record_workspace_bulk_review"
        request_sha256 = review_mutation_request_sha256(action, request)
        try:
            replay = await self.database.review_mutation_replay(
                action=action,
                request_identifier=request.request_identifier,
                request_sha256=request_sha256,
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error
        if replay is not None:
            return RecordWorkspaceBulkReviewResult.model_validate_json(encode_json(replay))

        started_at_utc_ns = self.utc_now_ns()
        started_monotonic_ns = self.monotonic_ns()
        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        source_input: NamedInput | None = None
        selected_identifiers: tuple[str, ...] | None
        if isinstance(request.selection, WorksetBulkReviewSelection):
            workset = (await self._current_review_worksets()).get(
                request.selection.workset_identifier
            )
            if workset is None:
                raise KeyError(request.selection.workset_identifier)
            if workset.workspace_record_identifier != workspace.record_identifier:
                raise ReviewInputError("The review workset belongs to a different workspace")
            source_input = NamedInput(
                name=("workset",), object_identifier=workset.record_identifier
            )
            selected_identifiers = workset.listing_identifiers
        elif isinstance(request.selection, SelectionSnapshotBulkReviewSelection):
            snapshot = await self.get_selection_snapshot(
                request.selection.selection_snapshot_record_identifier
            )
            if snapshot.workspace_record_identifier != workspace.record_identifier:
                raise ReviewInputError("The selection snapshot belongs to a different workspace")
            source_input = NamedInput(
                name=("selection_snapshot",), object_identifier=snapshot.record_identifier
            )
            selected_identifiers = tuple(item.listing_identifier for item in snapshot.items)
        else:
            selected_identifiers = None

        as_of, examined, projections = await self._workspace_bulk_review_projections(
            workspace,
            maximum_candidate_listings_examined=(request.maximum_candidate_listings_examined),
            selected_listing_identifiers=selected_identifiers,
        )
        projections_by_identifier = {
            projection.listing_identifier: projection for projection in projections
        }
        excluded_identifiers = set(request.exclude_listing_identifiers)
        unknown_exclusions = excluded_identifiers - projections_by_identifier.keys()
        if unknown_exclusions:
            raise ReviewInputError(
                f"{len(unknown_exclusions)} excluded listings are outside the selected workspace scope"
            )
        if selected_identifiers is not None:
            missing = set(selected_identifiers) - projections_by_identifier.keys()
            if missing:
                raise ReviewInputError(
                    f"{len(missing)} selected listings are outside the active workspace scope"
                )
            selected_projections = tuple(
                projection
                for projection in projections
                if projection.listing_identifier in selected_identifiers
            )
        else:
            selected_projections = projections

        prior_by_listing = await self.database.latest_listing_reviews(
            workspace_record_identifier=workspace.record_identifier,
            listing_identifiers=tuple(
                projection.listing_identifier for projection in selected_projections
            ),
        )
        baselines = await self._review_scalar_baselines(tuple(prior_by_listing.values()))
        explicitly_excluded_count = 0
        status_excluded_count = 0
        review_state_excluded_count = 0
        selected: list[tuple[ComposedListingProjection, ListingReviewRecord | None]] = []
        for projection in selected_projections:
            if projection.listing_identifier in excluded_identifiers:
                explicitly_excluded_count += 1
                continue
            if projection.status.value not in request.statuses:
                status_excluded_count += 1
                continue
            prior = prior_by_listing.get(projection.listing_identifier)
            baseline = None if prior is None else baselines.get(prior.record_identifier)
            review_state, _ = classify_review_state(
                current_revision=projection.projection_revision,
                previous_review=prior,
                policy=workspace.staleness_policy,
                previous_scalar_fields=baseline,
            )
            if review_state not in request.include_review_states:
                review_state_excluded_count += 1
                continue
            selected.append((projection, prior))

        operation_identifier = self.new_identifier()
        recorded_at_utc_ns = self.utc_now_ns()
        recorded_at_utc = _utc_text(recorded_at_utc_ns)
        review_records = tuple(
            ListingReviewRecord(
                record_identifier=self.new_identifier(),
                workspace_record_identifier=workspace.record_identifier,
                batch_record_identifier=None,
                listing_identifier=projection.listing_identifier,
                projection_revision=projection.projection_revision,
                inspected=True,
                disposition=request.disposition,
                note=request.note,
                recorded_at_utc=recorded_at_utc,
                scalar_fields_snapshot=projection_scalar_fields(projection),
            )
            for projection, _ in selected
        )
        result = RecordWorkspaceBulkReviewResult(
            operation_identifier=operation_identifier,
            workspace_record_identifier=workspace.record_identifier,
            selection_kind=request.selection.kind,
            as_of_completion_sequence=as_of,
            candidate_listings_examined=examined,
            selection_member_count=len(selected_projections),
            explicitly_excluded_count=explicitly_excluded_count,
            status_excluded_count=status_excluded_count,
            review_state_excluded_count=review_state_excluded_count,
            recorded_count=len(review_records),
            recorded_at_utc=recorded_at_utc,
        )
        provenance = await self._code_provenance()
        ended_at_utc_ns = self.utc_now_ns()
        try:
            published = await self.database.publish_workspace_bulk_review_records(
                request_identifier=request.request_identifier,
                request_sha256=request_sha256,
                workspace_record_identifier=workspace.record_identifier,
                expected_prior_review_identifiers={
                    projection.listing_identifier: (
                        None if prior is None else prior.record_identifier
                    )
                    for projection, prior in selected
                },
                component=build_review_workspace_component_registry().require(
                    RECORD_WORKSPACE_BULK_REVIEW
                ),
                operation_identifier=operation_identifier,
                records=tuple(
                    RecordDraft(
                        identifier=record.record_identifier,
                        kind=LISTING_REVIEW_KIND,
                        schema_version=1,
                        value=record.model_dump(mode="json"),
                    )
                    for record in review_records
                ),
                source_input=source_input,
                response=result,
                provenance=provenance,
                invocation=process_invocation(),
                started_at_utc=_utc_text(started_at_utc_ns),
                ended_at_utc=_utc_text(ended_at_utc_ns),
                duration_ns=max(0, self.monotonic_ns() - started_monotonic_ns),
                utc_now_ns=self.utc_now_ns,
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error
        if published is None:
            raise ReviewInputError(
                "A selected listing was claimed or reviewed concurrently; no reviews were recorded"
            )
        return published

    async def _current_review_worksets(self) -> dict[str, ReviewWorkset]:
        current: dict[str, ReviewWorkset] = {}
        for _, value in await self.database.records_by_kind(REVIEW_WORKSET_KIND):
            workset = ReviewWorkset.model_validate_json(encode_json(value))
            previous = current.get(workset.workset_identifier)
            if previous is None or workset.version > previous.version:
                current[workset.workset_identifier] = workset
        return current

    async def create_review_workset(
        self, request: CreateReviewWorksetRequest
    ) -> ReviewWorkset | ReviewWorksetConflict:
        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        workset_identifier = self.new_identifier()
        record_identifier = self.new_identifier()
        started_utc_ns = self.utc_now_ns()
        started_monotonic_ns = self.monotonic_ns()
        provenance = await self._code_provenance()
        ended_utc_ns = self.utc_now_ns()
        result = await self.database.publish_review_workset_revision(
            workset_identifier=workset_identifier,
            workspace_record_identifier=workspace.record_identifier,
            name=request.name,
            expected_version=None,
            listing_identifiers=tuple(request.listing_identifiers),
            component=build_review_workspace_component_registry().require(CREATE_REVIEW_WORKSET),
            operation_identifier=self.new_identifier(),
            record_identifier=record_identifier,
            provenance=provenance,
            invocation=process_invocation(),
            started_at_utc=_utc_text(started_utc_ns),
            ended_at_utc=_utc_text(ended_utc_ns),
            duration_ns=max(0, self.monotonic_ns() - started_monotonic_ns),
        )
        if isinstance(result, ReviewWorksetConflict):
            return result
        return ReviewWorkset(
            record_identifier=record_identifier,
            workset_identifier=workset_identifier,
            workspace_record_identifier=workspace.record_identifier,
            name=request.name,
            version=result[0],
            listing_identifiers=tuple(request.listing_identifiers),
            created_at_utc=_utc_text(ended_utc_ns),
        )

    async def update_review_workset(
        self, request: UpdateReviewWorksetRequest
    ) -> ReviewWorkset | ReviewWorksetConflict:
        current = (await self._current_review_worksets()).get(request.workset_identifier)
        if current is None:
            raise KeyError(request.workset_identifier)
        members = list(current.listing_identifiers)
        member_set = set(members)
        for identifier in request.remove_listing_identifiers:
            member_set.discard(identifier)
        for identifier in request.add_listing_identifiers:
            member_set.add(identifier)
        updated_members = tuple(
            identifier for identifier in members if identifier in member_set
        ) + tuple(
            identifier
            for identifier in request.add_listing_identifiers
            if identifier not in members
        )
        if len(updated_members) > MAXIMUM_WORKSET_LISTINGS:
            raise ReviewInputError("Workset membership exceeds the supported limit")
        record_identifier = self.new_identifier()
        started_utc_ns = self.utc_now_ns()
        started_monotonic_ns = self.monotonic_ns()
        provenance = await self._code_provenance()
        ended_utc_ns = self.utc_now_ns()
        result = await self.database.publish_review_workset_revision(
            workset_identifier=current.workset_identifier,
            workspace_record_identifier=current.workspace_record_identifier,
            name=current.name,
            expected_version=request.expected_version,
            listing_identifiers=updated_members,
            component=build_review_workspace_component_registry().require(UPDATE_REVIEW_WORKSET),
            operation_identifier=self.new_identifier(),
            record_identifier=record_identifier,
            provenance=provenance,
            invocation=process_invocation(),
            started_at_utc=_utc_text(started_utc_ns),
            ended_at_utc=_utc_text(ended_utc_ns),
            duration_ns=max(0, self.monotonic_ns() - started_monotonic_ns),
        )
        if isinstance(result, ReviewWorksetConflict):
            return result
        return ReviewWorkset(
            record_identifier=record_identifier,
            workset_identifier=current.workset_identifier,
            workspace_record_identifier=current.workspace_record_identifier,
            name=current.name,
            version=result[0],
            listing_identifiers=updated_members,
            created_at_utc=_utc_text(ended_utc_ns),
        )

    async def list_review_worksets(
        self, workspace_record_identifier: str
    ) -> tuple[ReviewWorkset, ...]:
        await self.get_review_workspace(workspace_record_identifier)
        return tuple(
            sorted(
                (
                    workset
                    for workset in (await self._current_review_worksets()).values()
                    if workset.workspace_record_identifier == workspace_record_identifier
                ),
                key=lambda workset: (workset.name.casefold(), workset.workset_identifier),
            )
        )

    async def create_selection_snapshot(
        self, request: CreateSelectionSnapshotRequest
    ) -> SelectionSnapshot:
        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        source_input: NamedInput | None = None
        if isinstance(request.selection, ReviewBatchSelection):
            batch = await self.get_review_batch(request.selection.review_batch_record_identifier)
            if batch.workspace_record_identifier != workspace.record_identifier:
                raise ReviewInputError("The review batch belongs to a different workspace")
            source_input = NamedInput(
                name=("review_batch",), object_identifier=batch.record_identifier
            )
            items = tuple(
                SelectionSnapshotItem(
                    listing_identifier=item.projection.listing_identifier,
                    projection_revision=item.projection.projection_revision,
                )
                for item in batch.items
            )
        else:
            if isinstance(request.selection, WorksetSelection):
                workset = (await self._current_review_worksets()).get(
                    request.selection.workset_identifier
                )
                if workset is None:
                    raise KeyError(request.selection.workset_identifier)
                if workset.workspace_record_identifier != workspace.record_identifier:
                    raise ReviewInputError("The workset belongs to a different workspace")
                source_input = NamedInput(
                    name=("workset",), object_identifier=workset.record_identifier
                )
                identifiers = workset.listing_identifiers
            else:
                assert isinstance(request.selection, ListingIdsSelection)
                identifiers = tuple(dict.fromkeys(request.selection.listing_identifiers))
            if len(identifiers) > 100:
                raise ReviewInputError("Selection snapshots are limited to 100 listings")
            current_runs = self._workspace_current_search_runs(workspace)
            if not current_runs:
                raise ReviewInputError("The workspace has no completed search track")
            projections = tuple(
                [
                    await self.get_composed_listing(
                        GetComposedListingRequest(
                            listing_identifier=identifier,
                            search_run_record_identifier=current_runs[0],
                            additional_search_run_record_identifiers=current_runs[1:],
                            product_guide_record_identifier=(
                                workspace.product_guide_record_identifier
                            ),
                            maximum_gallery_images=0,
                            maximum_analyses=0,
                        )
                    )
                    for identifier in identifiers
                ]
            )
            items = tuple(
                SelectionSnapshotItem(
                    listing_identifier=projection.listing_identifier,
                    projection_revision=projection.projection_revision,
                )
                for projection in projections
            )
        if not items:
            raise ReviewInputError("A selection snapshot cannot be empty")
        record_identifier = self.new_identifier()
        snapshot = SelectionSnapshot(
            record_identifier=record_identifier,
            workspace_record_identifier=workspace.record_identifier,
            created_at_utc=_utc_text(self.utc_now_ns()),
            source=request.selection,
            items=items,
        )
        await self._publish_local_records(
            component_identifier=CREATE_SELECTION_SNAPSHOT,
            records=(
                RecordDraft(
                    identifier=record_identifier,
                    kind=SELECTION_SNAPSHOT_KIND,
                    schema_version=1,
                    value=snapshot.model_dump(mode="json"),
                ),
            ),
            inputs=(
                NamedInput(name=("workspace",), object_identifier=workspace.record_identifier),
                *((source_input,) if source_input is not None else ()),
            ),
            outputs=(
                NamedOutput(name=("selection_snapshot",), object_identifier=record_identifier),
            ),
            result={"state": "completed", "item_count": len(items)},
        )
        return snapshot

    async def get_selection_snapshot(self, record_identifier: str) -> SelectionSnapshot:
        kind, schema_version, value = await self.database.get_record(record_identifier)
        if kind != SELECTION_SNAPSHOT_KIND or schema_version != 1:
            raise ReviewInputError("The record is not a supported selection snapshot")
        return SelectionSnapshot.model_validate_json(encode_json(value))

    async def get_search_run_listings(
        self, search_run_record_identifier: str, offset: int = 0, limit: int = 100
    ) -> SearchRunListingsPage:
        """Page through the exact listing identifiers retained by one immutable search run."""

        if offset < 0:
            raise ReviewInputError("Search-run listing offset must not be negative")
        if not 1 <= limit <= 500:
            raise ReviewInputError("Search-run listing limit must be between 1 and 500")
        identifiers = await run_listing_identifiers(self.database, search_run_record_identifier)
        selected = tuple(identifiers[offset : offset + limit])
        next_offset = offset + len(selected)
        return SearchRunListingsPage(
            search_run_record_identifier=search_run_record_identifier,
            total_listing_identifiers=len(identifiers),
            offset=offset,
            listing_identifiers=selected,
            next_offset=next_offset if next_offset < len(identifiers) else None,
        )

    async def _require_marketplace_search_record(
        self, record_identifier: str
    ) -> MarketplaceSearchRecord:
        kind, schema_version, value = await self.database.get_record(record_identifier)
        if kind != MARKETPLACE_SEARCH_KIND or schema_version != 1:
            raise ReviewInputError("The record is not a supported marketplace search")
        return MarketplaceSearchRecord.model_validate_json(encode_json(value))

    async def _marketplace_search_target_records(
        self, search_record_identifier: str
    ) -> tuple[MarketplaceSearchTargetRecord, ...]:
        records = tuple(
            MarketplaceSearchTargetRecord.model_validate_json(encode_json(value))
            for _, value in await self.database.records_by_kind(MARKETPLACE_SEARCH_TARGET_KIND)
        )
        return tuple(
            record
            for record in records
            if record.search_record_identifier == search_record_identifier
        )

    async def get_marketplace_search(self, record_identifier: str) -> MarketplaceSearch:
        """Return one search group with every target and retained execution."""

        search = await self._require_marketplace_search_record(record_identifier)
        target_records = await self._marketplace_search_target_records(record_identifier)
        enabled_by_target = {target.record_identifier: True for target in target_records}
        for _, value in await self.database.records_by_kind(MARKETPLACE_SEARCH_TARGET_STATE_KIND):
            state = MarketplaceSearchTargetStateRecord.model_validate_json(encode_json(value))
            if state.search_record_identifier == record_identifier:
                enabled_by_target[state.target_record_identifier] = state.enabled

        execution_records = tuple(
            MarketplaceSearchExecutionRecord.model_validate_json(encode_json(value))
            for _, value in await self.database.records_by_kind(MARKETPLACE_SEARCH_EXECUTION_KIND)
        )
        executions_by_target: dict[str, list[MarketplaceSearchExecution]] = {}
        marketplaces_by_target = {
            target.record_identifier: target.specification.marketplace for target in target_records
        }
        historical_runs: list[str] = []
        for execution in execution_records:
            if execution.search_record_identifier != record_identifier:
                continue
            work = await self.database.work(execution.work_identifier)
            result = work.get("result")
            run_identifier: str | None = None
            classification: EbaySearchResponseClassification | None = None
            stopping_reason: str | None = None
            collection_succeeded: bool | None = None
            if isinstance(result, dict):
                result_mapping = cast(dict[str, JsonValue], result)
                candidate = result_mapping.get("search_run_record_identifier")
                if isinstance(candidate, str):
                    run_identifier = candidate
                if marketplaces_by_target[execution.target_record_identifier] is Marketplace.EBAY:
                    raw_classification = result_mapping.get("response_classification")
                    if isinstance(raw_classification, dict):
                        classification = EbaySearchResponseClassification.model_validate_json(
                            encode_json(raw_classification)
                        )
                    raw_stopping_reason = result_mapping.get("stopping_reason")
                    if isinstance(raw_stopping_reason, str):
                        stopping_reason = raw_stopping_reason
                    # Interpret legacy completed-but-unrecognized runs without
                    # rewriting immutable work results or issuing new requests.
                    if classification is not None:
                        collection_succeeded = (
                            classification.kind not in EBAY_SEARCH_FAILURE_KINDS
                            and stopping_reason != "invalid_next_page"
                        )
                    elif stopping_reason in {
                        "challenge",
                        "unrecognized_response",
                        "error_page",
                        "http_error",
                        "invalid_next_page",
                    }:
                        collection_succeeded = False
            if run_identifier is not None and run_identifier not in historical_runs:
                historical_runs.append(run_identifier)
            executions_by_target.setdefault(execution.target_record_identifier, []).append(
                MarketplaceSearchExecution(
                    record_identifier=execution.record_identifier,
                    work_identifier=execution.work_identifier,
                    state=WorkState(str(work["state"])),
                    requested_at_utc=execution.requested_at_utc,
                    search_run_record_identifier=run_identifier,
                    response_classification=classification,
                    stopping_reason=stopping_reason,
                    collection_succeeded=collection_succeeded,
                )
            )
        return MarketplaceSearch(
            record_identifier=search.record_identifier,
            created_at_utc=search.created_at_utc,
            targets=tuple(
                MarketplaceSearchTarget(
                    record_identifier=target.record_identifier,
                    specification=target.specification,
                    enabled=enabled_by_target[target.record_identifier],
                    created_at_utc=target.created_at_utc,
                    executions=tuple(executions_by_target.get(target.record_identifier, ())),
                )
                for target in target_records
            ),
            historical_search_run_record_identifiers=tuple(historical_runs),
        )

    async def list_marketplace_search_results(
        self, request: ListMarketplaceSearchResultsRequest
    ) -> MarketplaceSearchResultsPage:
        """Project every completed source run in a search group into stable generic cards."""

        cursor: MarketplaceSearchResultsCursor | None = None
        if request.cursor is not None:
            try:
                cursor = decode_marketplace_search_results_cursor(request.cursor)
            except ValueError as error:
                raise ReviewInputError(str(error)) from error
            if cursor.search_record_identifier != request.search_record_identifier:
                raise ReviewInputError("Search-results cursor belongs to another search group")
        as_of = (
            await self.database.current_completion_boundary()
            if cursor is None
            else cursor.as_of_completion_sequence
        )
        search = await self.get_marketplace_search(request.search_record_identifier)
        warnings: list[MarketplaceSearchExecutionWarning] = []
        for target in search.targets:
            for execution in target.executions:
                reason: Literal["collection_failure", "work_failure", "collection_pending"]
                if execution.collection_succeeded is False:
                    reason = "collection_failure"
                elif execution.state is WorkState.TERMINAL_FAILURE:
                    reason = "work_failure"
                elif execution.state is not WorkState.COMPLETED:
                    reason = "collection_pending"
                else:
                    continue
                warnings.append(
                    MarketplaceSearchExecutionWarning(
                        target_record_identifier=target.record_identifier,
                        reason=reason,
                        execution=execution,
                    )
                )

        run_contexts: list[
            tuple[
                Marketplace,
                str,
                str,
                str,
                int,
            ]
        ] = []
        for target in search.targets:
            for execution in target.executions:
                if execution.state is not WorkState.COMPLETED:
                    continue
                work = await self.database.work(execution.work_identifier)
                completion_sequence = work.get("latest_event_sequence")
                if (
                    work.get("latest_event_kind") != "completed"
                    or not isinstance(completion_sequence, int)
                    or isinstance(completion_sequence, bool)
                    or completion_sequence > as_of
                ):
                    continue
                result = work.get("result")
                source_run = (
                    cast(dict[str, JsonValue], result).get("search_run_record_identifier")
                    if isinstance(result, dict)
                    else None
                )
                if not isinstance(source_run, str):
                    continue
                run_contexts.append(
                    (
                        target.specification.marketplace,
                        target.record_identifier,
                        execution.record_identifier,
                        source_run,
                        completion_sequence,
                    )
                )

        facebook_contexts: dict[str, list[tuple[str, str, str, int]]] = {}
        ebay_contexts: dict[str, list[tuple[str, str, int]]] = {}
        for (
            marketplace,
            target_identifier,
            execution_identifier,
            source_run,
            sequence,
        ) in run_contexts:
            kind, _, run_value = await self.database.get_record(source_run)
            expected_kind = ("carl", marketplace.value, "search_run")
            if kind != expected_kind or not isinstance(run_value, dict):
                raise ValueError(
                    f"Search execution references an invalid {marketplace.value} search run"
                )
            if marketplace is Marketplace.FACEBOOK:
                internal_identifier = cast(dict[str, JsonValue], run_value).get(
                    "search_run_identifier"
                )
                if not isinstance(internal_identifier, str):
                    raise ValueError("Stored Facebook search run has no internal identifier")
                facebook_contexts.setdefault(internal_identifier, []).append(
                    (target_identifier, execution_identifier, source_run, sequence)
                )
            else:
                ebay_contexts.setdefault(source_run, []).append(
                    (target_identifier, execution_identifier, sequence)
                )

        candidates: list[_MarketplaceSearchResultCandidate] = []
        facebook_records: list[tuple[str, JsonValue]] = []
        facebook_run_identifiers = tuple(
            dict.fromkeys(
                context[2] for contexts in facebook_contexts.values() for context in contexts
            )
        )
        for start in range(0, len(facebook_run_identifiers), 100):
            facebook_records.extend(
                await self.database.search_listing_occurrences_for_runs(
                    "facebook",
                    facebook_run_identifiers[start : start + 100],
                    as_of_completion_sequence=as_of,
                )
            )
        for occurrence_identifier, raw_value in facebook_records:
            if not isinstance(raw_value, dict):
                continue
            value = cast(dict[str, JsonValue], raw_value)
            internal_run = value.get("search_run_identifier")
            contexts = (
                facebook_contexts.get(internal_run) if isinstance(internal_run, str) else None
            )
            listing_identifier = value.get("listing_identifier")
            original = value.get("original")
            if (
                not contexts
                or not isinstance(listing_identifier, str)
                or not isinstance(original, dict)
            ):
                continue
            original_mapping = cast(dict[str, JsonValue], original)
            title_value = search_card_field_value("title", original_mapping)
            location_value = search_card_field_value("location_text", original_mapping)
            acquisition = value.get("acquisition_record_identifier")
            page_ordinal = _nonnegative_integer(value.get("page_ordinal"))
            edge_index = _nonnegative_integer(value.get("edge_index"))
            for target_identifier, execution_identifier, source_run, sequence in contexts:
                candidates.append(
                    _MarketplaceSearchResultCandidate(
                        marketplace=Marketplace.FACEBOOK,
                        external_identifier=listing_identifier,
                        canonical_url=(
                            f"https://www.facebook.com/marketplace/item/{listing_identifier}/"
                        ),
                        title=title_value if isinstance(title_value, str) else None,
                        displayed_price=_facebook_displayed_price(original_mapping),
                        location=location_value if isinstance(location_value, str) else None,
                        shipping_text=None,
                        condition=(
                            _nested_text(original_mapping.get("condition"))
                            or _nested_text(original_mapping.get("listing_condition"))
                        ),
                        preview_image_url=_facebook_preview_image(original_mapping),
                        promoted=None,
                        occurrence=MarketplaceSearchResultOccurrence(
                            occurrence_record_identifier=occurrence_identifier,
                            source_search_run_record_identifier=source_run,
                            target_record_identifier=target_identifier,
                            execution_record_identifier=execution_identifier,
                            acquisition_record_identifier=(
                                acquisition if isinstance(acquisition, str) else None
                            ),
                            completion_sequence=sequence,
                            page_ordinal=page_ordinal,
                            position=edge_index,
                        ),
                    )
                )

        ebay_records: list[tuple[str, JsonValue]] = []
        ebay_run_identifiers = tuple(ebay_contexts)
        for start in range(0, len(ebay_run_identifiers), 100):
            ebay_records.extend(
                await self.database.search_listing_occurrences_for_runs(
                    "ebay",
                    ebay_run_identifiers[start : start + 100],
                    as_of_completion_sequence=as_of,
                )
            )
        for occurrence_identifier, raw_value in ebay_records:
            if not isinstance(raw_value, dict):
                continue
            value = cast(dict[str, JsonValue], raw_value)
            source_run = value.get("search_run_record_identifier")
            contexts = ebay_contexts.get(source_run) if isinstance(source_run, str) else None
            listing_identifier = value.get("item_identifier")
            canonical_url = value.get("canonical_url")
            if (
                not contexts
                or not isinstance(source_run, str)
                or not isinstance(listing_identifier, str)
                or not isinstance(canonical_url, str)
            ):
                continue
            acquisition = value.get("acquisition_record_identifier")
            position = _nonnegative_integer(value.get("position"))
            for target_identifier, execution_identifier, sequence in contexts:
                candidates.append(
                    _MarketplaceSearchResultCandidate(
                        marketplace=Marketplace.EBAY,
                        external_identifier=listing_identifier,
                        canonical_url=canonical_url,
                        title=_optional_string(value, "title"),
                        displayed_price=_optional_string(value, "displayed_price"),
                        location=None,
                        shipping_text=_optional_string(value, "shipping_text"),
                        condition=_optional_string(value, "condition"),
                        preview_image_url=_optional_string(value, "image_url"),
                        promoted=(
                            promoted
                            if isinstance((promoted := value.get("promoted")), bool)
                            else None
                        ),
                        occurrence=MarketplaceSearchResultOccurrence(
                            occurrence_record_identifier=occurrence_identifier,
                            source_search_run_record_identifier=source_run,
                            target_record_identifier=target_identifier,
                            execution_record_identifier=execution_identifier,
                            acquisition_record_identifier=(
                                acquisition if isinstance(acquisition, str) else None
                            ),
                            completion_sequence=sequence,
                            page_ordinal=_nonnegative_integer(value.get("page_ordinal")),
                            position=position,
                            listing_state=value.get("listing_state", "active"),
                            displayed_price=value.get("displayed_price"),
                            sold_price=value.get("sold_price"),
                            sold_date=(
                                date.fromisoformat(raw_date)
                                if isinstance((raw_date := value.get("sold_date")), str)
                                else None
                            ),
                            sold_date_text=value.get("sold_date_text"),
                            sold_price_status=value.get("sold_price_status"),
                        ),
                    )
                )

        grouped: dict[tuple[Marketplace, str], list[_MarketplaceSearchResultCandidate]] = {}
        for candidate in candidates:
            grouped.setdefault((candidate.marketplace, candidate.external_identifier), []).append(
                candidate
            )
        projected: list[MarketplaceSearchResult] = []
        for group in grouped.values():
            newest = tuple(
                sorted(
                    group,
                    key=lambda candidate: (
                        candidate.occurrence.completion_sequence,
                        candidate.occurrence.page_ordinal,
                        candidate.occurrence.position,
                        candidate.occurrence.occurrence_record_identifier,
                        candidate.occurrence.execution_record_identifier,
                    ),
                    reverse=True,
                )
            )
            representative = newest[0]
            sale = next(
                (
                    candidate.occurrence
                    for candidate in newest
                    if candidate.occurrence.listing_state == "sold"
                ),
                None,
            )
            projected.append(
                MarketplaceSearchResult(
                    marketplace=representative.marketplace,
                    external_identifier=representative.external_identifier,
                    canonical_url=representative.canonical_url,
                    title=_newest_value(newest, lambda candidate: candidate.title),
                    displayed_price=_newest_value(
                        newest, lambda candidate: candidate.displayed_price
                    ),
                    location=_newest_value(newest, lambda candidate: candidate.location),
                    shipping_text=_newest_value(newest, lambda candidate: candidate.shipping_text),
                    condition=_newest_value(newest, lambda candidate: candidate.condition),
                    preview_image_url=_newest_value(
                        newest, lambda candidate: candidate.preview_image_url
                    ),
                    promoted=_newest_value(newest, lambda candidate: candidate.promoted),
                    listing_state=representative.occurrence.listing_state,
                    sold_price=sale.sold_price if sale else None,
                    sold_date=sale.sold_date if sale else None,
                    sold_date_text=sale.sold_date_text if sale else None,
                    sold_price_status=sale.sold_price_status if sale else None,
                    sold_occurrence_record_identifier=(
                        sale.occurrence_record_identifier if sale else None
                    ),
                    occurrences=tuple(candidate.occurrence for candidate in newest),
                )
            )

        projected.sort(
            key=lambda result: (
                -result.occurrences[0].completion_sequence,
                result.marketplace.value,
                result.external_identifier,
            )
        )
        total = len(projected)
        if cursor is not None and cursor.after_completion_sequence is not None:
            assert cursor.after_marketplace is not None
            assert cursor.after_external_identifier is not None
            after = (
                -cursor.after_completion_sequence,
                cursor.after_marketplace.value,
                cursor.after_external_identifier,
            )
            projected = [
                result
                for result in projected
                if (
                    -result.occurrences[0].completion_sequence,
                    result.marketplace.value,
                    result.external_identifier,
                )
                > after
            ]
        selected = tuple(projected[: request.page_size])
        next_cursor: str | None = None
        if len(projected) > len(selected):
            last = selected[-1]
            next_cursor = encode_marketplace_search_results_cursor(
                MarketplaceSearchResultsCursor(
                    search_record_identifier=request.search_record_identifier,
                    as_of_completion_sequence=as_of,
                    after_completion_sequence=last.occurrences[0].completion_sequence,
                    after_marketplace=last.marketplace,
                    after_external_identifier=last.external_identifier,
                )
            )
        return MarketplaceSearchResultsPage(
            search_record_identifier=request.search_record_identifier,
            as_of_completion_sequence=as_of,
            total_distinct_results=total,
            results=selected,
            next_cursor=next_cursor,
            execution_warnings=tuple(warnings),
        )

    async def _request_marketplace_target_execution(
        self,
        *,
        search_record_identifier: str,
        target: MarketplaceSearchTargetRecord,
    ) -> None:
        requester_kind = ("carl", "marketplace", "search_target")
        specification = target.specification
        if isinstance(specification, FacebookSearchTargetSpecification):
            queued = await self._create_search(
                specification.search,
                requester_kind=requester_kind,
                requester_identifier=target.record_identifier,
            )
        else:
            queued = await self._create_ebay_search(
                specification.search,
                requester_kind=requester_kind,
                requester_identifier=target.record_identifier,
            )

        execution = MarketplaceSearchExecutionRecord(
            record_identifier=self.new_identifier(),
            search_record_identifier=search_record_identifier,
            target_record_identifier=target.record_identifier,
            work_identifier=queued.work_identifier,
            requested_at_utc=_utc_text(self.utc_now_ns()),
        )
        await self._publish_marketplace_search_records(
            component_identifier=RUN_MARKETPLACE_SEARCH,
            records=(
                RecordDraft(
                    identifier=execution.record_identifier,
                    kind=MARKETPLACE_SEARCH_EXECUTION_KIND,
                    schema_version=1,
                    value=execution.model_dump(mode="json"),
                ),
            ),
            inputs=(
                NamedInput(name=("search",), object_identifier=search_record_identifier),
                NamedInput(name=("target",), object_identifier=target.record_identifier),
            ),
            outputs=(
                NamedOutput(
                    name=("execution",),
                    object_identifier=execution.record_identifier,
                ),
            ),
            result={
                "state": "completed",
                "work_identifier": queued.work_identifier,
                "work_created": queued.created,
                "work_state": queued.state,
            },
        )

    async def create_marketplace_search(
        self, request: CreateMarketplaceSearchRequest
    ) -> MarketplaceSearch:
        """Create a search group and queue each initial target once."""

        for specification in request.targets:
            if isinstance(specification, EbaySearchTargetSpecification):
                _require_ebay_search_acquisition(specification.search)
        search_identifier = self.new_identifier()
        created_at_utc = _utc_text(self.utc_now_ns())
        search = MarketplaceSearchRecord(
            record_identifier=search_identifier,
            created_at_utc=created_at_utc,
        )
        targets = tuple(
            MarketplaceSearchTargetRecord(
                record_identifier=self.new_identifier(),
                search_record_identifier=search_identifier,
                specification=specification,
                created_at_utc=created_at_utc,
            )
            for specification in request.targets
        )
        records = (
            RecordDraft(
                identifier=search_identifier,
                kind=MARKETPLACE_SEARCH_KIND,
                schema_version=1,
                value=search.model_dump(mode="json"),
            ),
            *(
                RecordDraft(
                    identifier=target.record_identifier,
                    kind=MARKETPLACE_SEARCH_TARGET_KIND,
                    schema_version=1,
                    value=target.model_dump(mode="json"),
                )
                for target in targets
            ),
        )
        await self._publish_marketplace_search_records(
            component_identifier=CREATE_MARKETPLACE_SEARCH,
            records=records,
            inputs=(),
            outputs=(
                NamedOutput(name=("search",), object_identifier=search_identifier),
                *(
                    NamedOutput(
                        name=("target", str(index)),
                        object_identifier=target.record_identifier,
                    )
                    for index, target in enumerate(targets)
                ),
            ),
            result={"state": "completed", "target_count": len(targets)},
        )
        for target in targets:
            await self._request_marketplace_target_execution(
                search_record_identifier=search_identifier,
                target=target,
            )
        return await self.get_marketplace_search(search_identifier)

    async def add_marketplace_search_target(
        self, request: AddMarketplaceSearchTargetRequest
    ) -> MarketplaceSearch:
        """Add and initially execute one target without changing earlier targets or runs."""

        if isinstance(request.target, EbaySearchTargetSpecification):
            _require_ebay_search_acquisition(request.target.search)
        _ = await self._require_marketplace_search_record(request.search_record_identifier)
        target = MarketplaceSearchTargetRecord(
            record_identifier=self.new_identifier(),
            search_record_identifier=request.search_record_identifier,
            specification=request.target,
            created_at_utc=_utc_text(self.utc_now_ns()),
        )
        await self._publish_marketplace_search_records(
            component_identifier=ADD_MARKETPLACE_SEARCH_TARGET,
            records=(
                RecordDraft(
                    identifier=target.record_identifier,
                    kind=MARKETPLACE_SEARCH_TARGET_KIND,
                    schema_version=1,
                    value=target.model_dump(mode="json"),
                ),
            ),
            inputs=(
                NamedInput(
                    name=("search",),
                    object_identifier=request.search_record_identifier,
                ),
            ),
            outputs=(NamedOutput(name=("target",), object_identifier=target.record_identifier),),
            result={"state": "completed"},
            marketplace_target_guard=target,
        )
        await self._request_marketplace_target_execution(
            search_record_identifier=request.search_record_identifier,
            target=target,
        )
        return await self.get_marketplace_search(request.search_record_identifier)

    async def set_marketplace_search_target_enabled(
        self, request: SetMarketplaceSearchTargetEnabledRequest
    ) -> MarketplaceSearch:
        """Change future scheduling eligibility without removing retained executions."""

        search = await self.get_marketplace_search(request.search_record_identifier)
        target = next(
            (
                candidate
                for candidate in search.targets
                if candidate.record_identifier == request.target_record_identifier
            ),
            None,
        )
        if target is None:
            raise ReviewInputError("The target does not belong to the marketplace search")
        if target.enabled == request.enabled:
            return search
        state = MarketplaceSearchTargetStateRecord(
            record_identifier=self.new_identifier(),
            search_record_identifier=request.search_record_identifier,
            target_record_identifier=request.target_record_identifier,
            enabled=request.enabled,
            recorded_at_utc=_utc_text(self.utc_now_ns()),
        )
        await self._publish_marketplace_search_records(
            component_identifier=SET_MARKETPLACE_SEARCH_TARGET_ENABLED,
            records=(
                RecordDraft(
                    identifier=state.record_identifier,
                    kind=MARKETPLACE_SEARCH_TARGET_STATE_KIND,
                    schema_version=1,
                    value=state.model_dump(mode="json"),
                ),
            ),
            inputs=(
                NamedInput(
                    name=("search",),
                    object_identifier=request.search_record_identifier,
                ),
                NamedInput(
                    name=("target",),
                    object_identifier=request.target_record_identifier,
                ),
            ),
            outputs=(
                NamedOutput(name=("target_state",), object_identifier=state.record_identifier),
            ),
            result={"state": "completed", "enabled": request.enabled},
        )
        return await self.get_marketplace_search(request.search_record_identifier)

    async def run_marketplace_search(
        self, request: RunMarketplaceSearchRequest
    ) -> MarketplaceSearch:
        """Queue every currently enabled target while retaining all earlier executions."""

        search = await self.get_marketplace_search(request.search_record_identifier)
        enabled_identifiers = {
            target.record_identifier for target in search.targets if target.enabled
        }
        if not enabled_identifiers:
            raise ReviewInputError("The marketplace search has no enabled targets")
        records = await self._marketplace_search_target_records(request.search_record_identifier)
        for target in records:
            if target.record_identifier in enabled_identifiers and isinstance(
                target.specification, EbaySearchTargetSpecification
            ):
                _require_ebay_search_acquisition(target.specification.search)
        for target in records:
            if target.record_identifier in enabled_identifiers:
                await self._request_marketplace_target_execution(
                    search_record_identifier=request.search_record_identifier,
                    target=target,
                )
        return await self.get_marketplace_search(request.search_record_identifier)

    async def create_search(self, request: CreateSearchRequest) -> CreateSearchResult:
        """Queue one new bounded Facebook Marketplace search for an explicit worker pool."""

        return await self._create_search(
            request,
            requester_kind=("carl", "mcp", "create_search"),
            requester_identifier=None,
        )

    async def create_ebay_search(self, request: EbaySearchRequest) -> CreateSearchResult:
        """Queue one new bounded eBay search for an explicit worker pool."""

        return await self._create_ebay_search(
            request,
            requester_kind=("carl", "mcp", "create_search"),
            requester_identifier=None,
        )

    async def _create_ebay_search(
        self,
        request: EbaySearchRequest,
        *,
        requester_kind: tuple[str, ...],
        requester_identifier: str | None,
        requester_context: dict[str, JsonValue] | None = None,
    ) -> CreateSearchResult:
        _require_ebay_search_acquisition(request)
        payload = CollectEbaySearchPayload(request=request)
        requested_identifier = self.new_identifier()
        enqueued = await self.database.enqueue_work(
            collect_ebay_search_work(
                identifier=requested_identifier,
                payload=payload,
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier=self.new_identifier(),
                kind=requester_kind,
                identifier=requester_identifier or requested_identifier,
                context={
                    "marketplace": "ebay",
                    "request": request.model_dump(mode="json"),
                    **(requester_context or {}),
                },
            ),
            event_identifier=self.new_identifier(),
            enqueued_at_utc_ns=self.utc_now_ns(),
        )
        return CreateSearchResult(
            work_identifier=enqueued.work_item_identifier,
            created=enqueued.created,
            state=(await self.database.work_state(enqueued.work_item_identifier)).value,
        )

    async def _create_search(
        self,
        request: CreateSearchRequest,
        *,
        requester_kind: tuple[str, ...],
        requester_identifier: str | None,
        requester_context: dict[str, JsonValue] | None = None,
    ) -> CreateSearchResult:
        payload = CollectSearchPayload(
            request=request.request,
            traversal=request.traversal,
            traversal_strategy=request.traversal_strategy,
            routing=request.requested_network_path,
        )
        requested_identifier = self.new_identifier()
        enqueued = await self.database.enqueue_work(
            collect_search_work(
                identifier=requested_identifier,
                payload=payload,
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier=self.new_identifier(),
                kind=requester_kind,
                identifier=requester_identifier or requested_identifier,
                context={"request": request.model_dump(mode="json"), **(requester_context or {})},
            ),
            event_identifier=self.new_identifier(),
            enqueued_at_utc_ns=self.utc_now_ns(),
        )
        return CreateSearchResult(
            work_identifier=enqueued.work_item_identifier,
            created=enqueued.created,
            state=(await self.database.work_state(enqueued.work_item_identifier)).value,
        )

    async def create_workspace_search(
        self, request: CreateWorkspaceSearchRequest
    ) -> CreateWorkspaceSearchResult:
        """Queue a new search phrase as another durable track in one workspace."""

        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        search = request.search
        if isinstance(search, EbaySearchTargetSpecification):
            result = await self._create_ebay_search(
                search.search,
                requester_kind=("carl", "mcp", "create_workspace_search"),
                requester_identifier=workspace.record_identifier,
            )
        else:
            result = await self._create_search(
                search.search if isinstance(search, FacebookSearchTargetSpecification) else search,
                requester_kind=("carl", "mcp", "create_workspace_search"),
                requester_identifier=workspace.record_identifier,
            )
        return CreateWorkspaceSearchResult(
            workspace_record_identifier=workspace.record_identifier,
            track_identifier=result.work_identifier,
            work_identifier=result.work_identifier,
            created=result.created,
            state=WorkState(result.state),
        )

    async def retry_workspace_search_track(
        self, request: RetryWorkspaceSearchTrackRequest
    ) -> RetryWorkspaceSearchTrackResult:
        """Retry one workspace track whose initial search exhausted a transient failure."""

        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        tracks = {track.track_identifier: track for track in workspace.search_tracks}
        track = tracks.get(request.track_identifier)
        if track is None:
            raise ReviewInputError("The search track does not belong to this workspace")
        if track.creation_work_identifier is None:
            raise ReviewInputError("The workspace's original search run has no creation to retry")
        work = await self.database.work(track.creation_work_identifier)
        if tuple(work["kind"]) == COLLECT_EBAY_SEARCH_WORK_KIND:
            original_payload = CollectEbaySearchPayload.model_validate_json(
                encode_json(work["payload"])
            )
            current_search = (
                track.search_specification.search
                if track.search_specification_version > 1
                and isinstance(track.search_specification, EbaySearchTargetSpecification)
                else original_payload.request
            )
            _require_ebay_search_acquisition(current_search)
        if WorkState(str(work["state"])) is not WorkState.TERMINAL_FAILURE:
            raise ReviewInputError("The search track creation is not in terminal failure")
        if (
            track.search_specification_version > 1
            and track.search_specification is not None
            and self._retained_track_specification(
                track.search_specification.marketplace, work["payload"]
            )
            != track.search_specification
        ):
            if track.current_search_run_record_identifier is not None:
                raise ReviewInputError(
                    "The track's search specification was revised; use request_workspace_refresh "
                    "to acquire the current version and preserve its retained baseline"
                )
            # No usable baseline exists yet. Acquire the revised initial intent
            # under its stable requester, never rewrite the old failed payload.
            specification = track.search_specification
            context: dict[str, JsonValue] = {
                "track_identifier": track.track_identifier,
                "search_specification_version": track.search_specification_version,
                "search_specification_record_identifier": track.search_specification_record_identifier,
            }
            if isinstance(specification, EbaySearchTargetSpecification):
                if request.acquisition_stack is not None:
                    raise ReviewInputError(
                        "Revise the search specification to change its acquisition stack"
                    )
                result = await self._create_ebay_search(
                    specification.search,
                    requester_kind=("carl", "mcp", "create_workspace_search_revision"),
                    requester_identifier=workspace.record_identifier,
                    requester_context=context,
                )
            else:
                if request.acquisition_stack is not None:
                    raise ReviewInputError("acquisition_stack applies only to eBay search retries")
                result = await self._create_search(
                    specification.search,
                    requester_kind=("carl", "mcp", "create_workspace_search_revision"),
                    requester_identifier=workspace.record_identifier,
                    requester_context=context,
                )
            return RetryWorkspaceSearchTrackResult(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=track.track_identifier,
                work_identifier=result.work_identifier,
                previous_attempt_count=int(work["attempt"]),
                state=WorkState(result.state),
            )
        error = work.get("error")
        if tuple(work["kind"]) == COLLECT_EBAY_SEARCH_WORK_KIND:
            if not isinstance(error, dict) or error.get("kind") not in {
                "ebay_search_response_failure",
                "ebay_search_acquisition_failure",
            }:
                raise ReviewInputError("The eBay search failed permanently and cannot be retried")
            if request.acquisition_stack is not None:
                try:
                    configured = load_configuration(user_directories().configuration_file)
                    stack = configured.configuration.require_acquisition_stack(
                        request.acquisition_stack
                    )
                    transport = configured.configuration.require_http_transport(
                        stack.http_transport
                    )
                    route = configured.configuration.require_route(stack.network_path)
                except KeyError as missing:
                    raise ReviewInputError(
                        "The requested acquisition stack is not configured"
                    ) from missing
                except ConfigurationFailure as configuration_error:
                    raise ReviewInputError(
                        "Cannot validate acquisition stack: " + configuration_error.code
                    ) from configuration_error
                if transport.implementation != "wreq" or route.provider.value != "decodo":
                    raise ReviewInputError("eBay search requires a configured Decodo/wreq stack")
            try:
                previous_attempt = await self.database.retry_terminal_ebay_search_work(
                    work_item_identifier=track.creation_work_identifier,
                    retried_at_utc_ns=self.utc_now_ns(),
                    event_identifier=self.new_identifier(),
                    reason={
                        "kind": "operator_workspace_search_track_retry",
                        "workspace_record_identifier": workspace.record_identifier,
                        "track_identifier": track.track_identifier,
                    },
                    payload_schema_version=COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
                    acquisition_stack=request.acquisition_stack,
                )
            except ValueError as retry_error:
                raise ReviewInputError(str(retry_error)) from retry_error
            return RetryWorkspaceSearchTrackResult(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=track.track_identifier,
                work_identifier=track.creation_work_identifier,
                previous_attempt_count=previous_attempt,
                state=WorkState.PENDING,
            )
        if request.acquisition_stack is not None:
            raise ReviewInputError("acquisition_stack applies only to eBay search retries")
        if not is_transient_search_failure(error):
            raise ReviewInputError("The search track failed permanently and cannot be retried")
        previous_attempt = await self.database.retry_terminal_collect_search_work(
            work_item_identifier=track.creation_work_identifier,
            retried_at_utc_ns=self.utc_now_ns(),
            event_identifier=self.new_identifier(),
            reason={
                "kind": "operator_workspace_search_track_retry",
                "workspace_record_identifier": workspace.record_identifier,
                "track_identifier": track.track_identifier,
            },
            payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
        )
        return RetryWorkspaceSearchTrackResult(
            workspace_record_identifier=workspace.record_identifier,
            track_identifier=track.track_identifier,
            work_identifier=track.creation_work_identifier,
            previous_attempt_count=previous_attempt,
            state=WorkState.PENDING,
        )

    async def request_search_refresh(
        self, request: SearchRefreshRequest
    ) -> SearchRefreshRequestResult:
        return await self._request_search_refresh(
            request,
            requester_kind=("carl", "mcp", "request_search_refresh"),
            requester_identifier=request.base_search_run_record_identifier,
            requester_context={"request": request.model_dump(mode="json")},
        )

    async def _request_search_refresh(
        self,
        request: SearchRefreshRequest,
        *,
        requester_kind: tuple[str, ...],
        requester_identifier: str,
        requester_context: JsonValue,
        search_specification: SearchTargetSpecification | None = None,
    ) -> SearchRefreshRequestResult:
        kind, _, base = await self.database.get_record(request.base_search_run_record_identifier)
        if kind == ("carl", "ebay", "search_run") and isinstance(base, dict):
            return await self._request_ebay_search_refresh(
                request,
                base=base,
                requester_kind=requester_kind,
                requester_identifier=requester_identifier,
                requester_context=requester_context,
                search_specification=search_specification,
            )
        if kind != ("carl", "facebook", "search_run") or not isinstance(base, dict):
            raise ReviewInputError("Refresh input is not a supported retained search-run record")
        if request.acquisition_stack is not None or request.maximum_pages is not None:
            raise ReviewInputError("acquisition_stack and maximum_pages apply only to eBay")
        stored_request = base.get("request")
        stored_traversal = base.get("traversal")
        stored_strategy = base.get("traversal_strategy")
        if isinstance(search_specification, FacebookSearchTargetSpecification):
            intent = search_specification.search
            stored_request = intent.request.model_dump(mode="json")
            stored_traversal = {"policy": intent.traversal.model_dump(mode="json")}
            stored_strategy = intent.traversal_strategy.model_dump(mode="json")
        if not isinstance(stored_traversal, dict):
            raise ReviewInputError("The retained search has no traversal policy")
        policy = stored_traversal.get("policy")
        try:
            base_payload = CollectSearchPayload.model_validate_json(
                encode_json(
                    {
                        "request": stored_request,
                        "traversal": (
                            request.traversal.model_dump(mode="json")
                            if request.traversal is not None
                            else policy
                        ),
                        "traversal_strategy": (
                            request.traversal_strategy.model_dump(mode="json")
                            if request.traversal_strategy is not None
                            else stored_strategy
                        ),
                        "routing": list(
                            request.requested_search_network_path
                            or (
                                search_specification.search.requested_network_path
                                if isinstance(
                                    search_specification, FacebookSearchTargetSpecification
                                )
                                else DEFAULT_DATACENTER_NETWORK_PATH
                            )
                        ),
                    }
                )
            )
        except ValueError as error:
            raise ReviewInputError(f"The retained search cannot be refreshed: {error}") from error
        refresh_identifier = self.new_identifier()
        payload = RefreshSearchPayload(
            base_search_run_record_identifier=request.base_search_run_record_identifier,
            search_work_identifier=self.new_identifier(),
            search=base_payload,
            maximum_items=request.maximum_items,
            maximum_images=request.maximum_images,
            item_routing=("decodo", "personal", request.decodo_route),
            image_routing=request.requested_image_network_path,
        )
        enqueued = await self.database.enqueue_work(
            refresh_search_work(
                identifier=refresh_identifier,
                payload=payload,
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier=self.new_identifier(),
                kind=requester_kind,
                identifier=requester_identifier,
                context=requester_context,
            ),
            event_identifier=self.new_identifier(),
            enqueued_at_utc_ns=self.utc_now_ns(),
        )
        return SearchRefreshRequestResult(
            work_identifier=enqueued.work_item_identifier,
            created=enqueued.created,
            state=(await self.database.work_state(enqueued.work_item_identifier)).value,
            base_search_run_record_identifier=request.base_search_run_record_identifier,
        )

    async def _request_ebay_search_refresh(
        self,
        request: SearchRefreshRequest,
        *,
        base: dict[str, JsonValue],
        requester_kind: tuple[str, ...],
        requester_identifier: str,
        requester_context: JsonValue,
        search_specification: SearchTargetSpecification | None = None,
    ) -> SearchRefreshRequestResult:
        if request.traversal is not None or request.traversal_strategy is not None:
            raise ReviewInputError("Facebook traversal overrides do not apply to eBay")
        if (
            request.proton_route is not None
            or request.search_network_path is not None
            or request.image_network_path != DEFAULT_DATACENTER_NETWORK_PATH
            or request.decodo_route != "carl"
        ):
            raise ReviewInputError(
                "eBay uses its acquisition stack and the carl image route; route overrides "
                "are not supported. Select acquisition_stack for item/search transport."
            )
        stored = (
            search_specification.search.model_dump(mode="json")
            if isinstance(search_specification, EbaySearchTargetSpecification)
            else base.get("request")
        )
        if not isinstance(stored, dict):
            raise ReviewInputError("The retained eBay search has no usable request")
        overrides: dict[str, JsonValue] = dict(stored)
        if request.acquisition_stack is not None:
            overrides["stack_identifier"] = request.acquisition_stack
        if request.maximum_pages is not None:
            overrides["maximum_pages"] = request.maximum_pages
        search = EbaySearchRequest.model_validate_json(encode_json(overrides))
        _require_ebay_search_acquisition(search)
        payload = RefreshEbaySearchPayload(
            base_search_run_record_identifier=request.base_search_run_record_identifier,
            search_work_identifier=self.new_identifier(),
            search=search,
            maximum_items=request.maximum_items,
            maximum_images=request.maximum_images,
        )
        for constraint in refresh_ebay_search_work_constraints():
            await self.database.register_constraint(
                constraint, registered_at_utc_ns=self.utc_now_ns()
            )
        enqueued = await self.database.enqueue_work(
            refresh_ebay_search_work(
                identifier=self.new_identifier(), payload=payload, not_before_utc_ns=0
            ),
            WorkRequester(
                request_identifier=self.new_identifier(),
                kind=requester_kind,
                identifier=requester_identifier,
                context=requester_context,
            ),
            event_identifier=self.new_identifier(),
            enqueued_at_utc_ns=self.utc_now_ns(),
        )
        return SearchRefreshRequestResult(
            work_identifier=enqueued.work_item_identifier,
            created=enqueued.created,
            state=(await self.database.work_state(enqueued.work_item_identifier)).value,
            base_search_run_record_identifier=request.base_search_run_record_identifier,
        )

    async def request_workspace_refresh(
        self, request: RequestWorkspaceRefreshRequest
    ) -> RequestWorkspaceRefreshResult:
        """Refresh one workspace search track and attribute the coordinator to the workspace."""

        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        tracks = tuple(
            track
            for track in workspace.search_tracks
            if track.enabled and track.current_search_run_record_identifier is not None
        )
        if request.track_identifier is None:
            if len(tracks) != 1:
                raise ReviewInputError(
                    "Select track_identifier when a workspace has more than one search track"
                )
            track = tracks[0]
        else:
            track = next(
                (
                    candidate
                    for candidate in workspace.search_tracks
                    if candidate.track_identifier == request.track_identifier
                ),
                None,
            )
            if track is None:
                raise ReviewInputError("The search track is not part of this workspace")
            if not track.enabled:
                raise ReviewInputError("Enable the workspace search track before refreshing it")
            if track.current_search_run_record_identifier is None:
                raise ReviewInputError("Wait for the search track's initial search to complete")
        assert track.current_search_run_record_identifier is not None
        if (
            track.latest_refresh_work_state in (WorkState.PENDING, WorkState.LEASED)
            and track.latest_refresh_search_specification_version is not None
            and track.latest_refresh_search_specification_version
            != track.search_specification_version
        ):
            raise ReviewInputError(
                "Wait for the active refresh to finish before refreshing the revised search version"
            )
        refresh_request = SearchRefreshRequest(
            base_search_run_record_identifier=track.current_search_run_record_identifier,
            traversal=request.traversal,
            traversal_strategy=request.traversal_strategy,
            maximum_items=request.maximum_items,
            maximum_images=request.maximum_images,
            proton_route=request.proton_route,
            search_network_path=request.search_network_path,
            image_network_path=request.image_network_path,
            decodo_route=request.decodo_route,
            acquisition_stack=request.acquisition_stack,
            maximum_pages=request.maximum_pages,
        )
        result = await self._request_search_refresh(
            refresh_request,
            requester_kind=("carl", "mcp", "request_workspace_refresh"),
            requester_identifier=workspace.record_identifier,
            requester_context={
                "track_identifier": track.track_identifier,
                "search_specification_version": track.search_specification_version,
                "search_specification_record_identifier": track.search_specification_record_identifier,
                "request": request.model_dump(mode="json"),
            },
            search_specification=(
                track.search_specification
                if track.search_specification_version > 1
                or (
                    track.creation_work_identifier is not None
                    and isinstance(track.search_specification, FacebookSearchTargetSpecification)
                    and track.search_specification.search.proton_route is None
                )
                else None
            ),
        )
        return RequestWorkspaceRefreshResult(
            workspace_record_identifier=workspace.record_identifier,
            track_identifier=track.track_identifier,
            work_identifier=result.work_identifier,
            created=result.created,
            state=WorkState(result.state),
            base_search_run_record_identifier=result.base_search_run_record_identifier,
        )

    def _selected_workspace_product_guide(
        self,
        workspace: ReviewWorkspace,
        requested_binding_identifier: str | None,
        requested_guide_record_identifier: str | None = None,
    ) -> tuple[str, str]:
        enabled = tuple(binding for binding in workspace.product_guide_bindings if binding.enabled)
        if not enabled and workspace.product_guide_record_identifier is not None:
            return (
                self._legacy_workspace_product_guide_binding_identifier(
                    workspace.record_identifier
                ),
                workspace.product_guide_record_identifier,
            )
        if requested_binding_identifier is not None:
            binding = self._workspace_product_guide_binding(workspace, requested_binding_identifier)
            if not binding.enabled:
                raise ReviewInputError("Enable the workspace product-guide binding before using it")
        elif requested_guide_record_identifier is not None:
            matching = tuple(
                candidate
                for candidate in enabled
                if candidate.resolved_product_guide_record_identifier
                == requested_guide_record_identifier
            )
            if not matching:
                raise ReviewInputError(
                    "The exact product-guide record is not resolved by an enabled workspace binding"
                )
            if len(matching) > 1:
                default_match = next(
                    (candidate for candidate in matching if candidate.is_default), None
                )
                if default_match is None:
                    raise ReviewInputError(
                        "Several workspace bindings resolve this guide; select one binding identifier"
                    )
                binding = default_match
            else:
                binding = next(iter(matching))
        else:
            binding = next((candidate for candidate in enabled if candidate.is_default), None)
            if binding is None and len(enabled) == 1:
                binding = enabled[0]
            if binding is None:
                if not enabled:
                    raise ReviewInputError(
                        "Add and enable a workspace product guide before analysis"
                    )
                raise ReviewInputError(
                    "Select product_guide_binding_identifier when a workspace has multiple enabled "
                    + "guides and no default"
                )
        return (
            binding.binding_identifier,
            binding.resolved_product_guide_record_identifier,
        )

    async def _selection_analysis_request(
        self, request: SelectionAnalysesRequest
    ) -> tuple[
        RequestMissingListingAnalysesRequest,
        frozenset[str],
        frozenset[str],
        ReviewWorkspace,
        str,
        int,
        bool,
    ]:
        workspace = await self.get_review_workspace(request.workspace_record_identifier)
        guide_binding_identifier, guide_record_identifier = self._selected_workspace_product_guide(
            workspace,
            request.product_guide_binding_identifier,
            request.product_guide_record_identifier,
        )
        completed_tracks = tuple(
            track
            for track in workspace.search_tracks
            if track.enabled and track.current_search_run_record_identifier is not None
        )
        if not completed_tracks:
            legacy_refresh_identifier = (
                await self.database.facebook_search_run_refresh_work_identifier(
                    workspace.search_run_record_identifier
                )
            )
            completed_tracks = (
                WorkspaceSearchTrack(
                    track_identifier=workspace.search_run_record_identifier,
                    query="unknown",
                    creation_work_identifier=None,
                    creation_work_state=None,
                    origin_search_run_record_identifier=(workspace.search_run_record_identifier),
                    current_search_run_record_identifier=(workspace.search_run_record_identifier),
                    latest_refresh_work_identifier=legacy_refresh_identifier,
                    latest_refresh_work_state=(
                        WorkState.COMPLETED if legacy_refresh_identifier is not None else None
                    ),
                ),
            )
        unrefreshed_tracks = tuple(
            track.track_identifier
            for track in completed_tracks
            if track.latest_refresh_work_identifier is None
            or track.latest_refresh_work_state is not WorkState.COMPLETED
        )
        if unrefreshed_tracks:
            raise ReviewInputError(
                "Refresh every completed workspace search track before analysis: "
                + ", ".join(unrefreshed_tracks)
            )
        if not completed_tracks:
            raise ReviewInputError("The workspace has no completed search track")
        refresh_work_identifier = completed_tracks[0].latest_refresh_work_identifier
        if refresh_work_identifier is None:
            raise RuntimeError(
                "A completed workspace search track lost its refresh work identifier"
            )

        selection = request.selection
        selected_listing_identifiers: tuple[str, ...] | None
        selection_snapshot_record_identifier: str | None = None
        if isinstance(selection, WorkspaceAnalysisSelection):
            selected_listing_identifiers = None
        elif isinstance(selection, ListingIdsSelection):
            selected_listing_identifiers = tuple(selection.listing_identifiers)
        elif isinstance(selection, ReviewBatchSelection):
            batch = await self.get_review_batch(selection.review_batch_record_identifier)
            if batch.workspace_record_identifier != workspace.record_identifier:
                raise ReviewInputError("The review batch belongs to a different workspace")
            selected_listing_identifiers = tuple(
                item.projection.listing_identifier for item in batch.items
            )
        elif isinstance(selection, WorksetSelection):
            workset = (await self._current_review_worksets()).get(selection.workset_identifier)
            if workset is None:
                raise KeyError(selection.workset_identifier)
            if workset.workspace_record_identifier != workspace.record_identifier:
                raise ReviewInputError("The review workset belongs to a different workspace")
            selected_listing_identifiers = workset.listing_identifiers
        else:
            assert isinstance(selection, SelectionSnapshotAnalysisSelection)
            snapshot = await self.get_selection_snapshot(
                selection.selection_snapshot_record_identifier
            )
            if snapshot.workspace_record_identifier != workspace.record_identifier:
                raise ReviewInputError("The selection snapshot belongs to a different workspace")
            selection_snapshot_record_identifier = snapshot.record_identifier
            selected_listing_identifiers = tuple(item.listing_identifier for item in snapshot.items)

        (
            status_matching_listing_identifiers,
            candidate_listings_examined,
            candidate_examination_limit_reached,
        ) = await self._workspace_analysis_candidates(
            workspace,
            statuses=frozenset(request.statuses),
            maximum_candidate_listings_examined=(request.maximum_candidate_listings_examined),
        )
        status_matching = frozenset(status_matching_listing_identifiers)
        selected_listing_identifiers = (
            tuple(status_matching_listing_identifiers)
            if selected_listing_identifiers is None
            else tuple(
                identifier
                for identifier in selected_listing_identifiers
                if identifier in status_matching
            )
        )
        low_level_request = RequestMissingListingAnalysesRequest(
            search_refresh_work_identifier=refresh_work_identifier,
            product_guide_record_identifier=guide_record_identifier,
            selection_snapshot_record_identifier=selection_snapshot_record_identifier,
            selection_policy=request.selection_policy,
            filters=CandidateFilters(),
            maximum_items=request.maximum_items,
            allow_incomplete_gallery=request.allow_incomplete_gallery,
        )
        return (
            low_level_request,
            frozenset(selected_listing_identifiers),
            status_matching,
            workspace,
            guide_binding_identifier,
            candidate_listings_examined,
            candidate_examination_limit_reached,
        )

    async def _marketplace_workspace_projections(
        self,
        workspace: ReviewWorkspace,
        *,
        maximum_candidate_listings_examined: int,
        selected_listing_identifiers: tuple[str, ...] | None = None,
    ) -> tuple[int, int, tuple[ComposedListingProjection, ...]]:
        try:
            return await marketplace_bulk_review_projections(
                self,
                workspace,
                maximum_candidate_listings_examined=maximum_candidate_listings_examined,
                selected_listing_identifiers=selected_listing_identifiers,
            )
        except ValueError as error:
            raise ReviewInputError(str(error)) from error

    async def _workspace_analysis_candidates(
        self,
        workspace: ReviewWorkspace,
        *,
        statuses: frozenset[ListingStatus],
        maximum_candidate_listings_examined: int,
    ) -> tuple[tuple[str, ...], int, bool]:
        """Select status-matching workspace IDs without composing rich listing views."""

        current_runs = self._workspace_current_search_runs(workspace)
        if await scope_contains_ebay(self.database, current_runs):
            selected: list[str] = []
            examined = 0
            cursor = None
            while True:
                page = await self.list_composed_search(
                    ListComposedSearchRequest(
                        search_run_record_identifier=current_runs[0],
                        additional_search_run_record_identifiers=current_runs[1:],
                        filters=ComposedListingFilters(statuses=tuple(ListingStatus)),
                        page_size=min(100, maximum_candidate_listings_examined),
                        maximum_candidate_listings_examined=maximum_candidate_listings_examined,
                        maximum_analyses_per_listing=0,
                        cursor=cursor,
                    )
                )
                if page.older_ancestry_truncated:
                    raise ReviewInputError("Workspace analysis requires complete search ancestry")
                remaining = maximum_candidate_listings_examined - examined
                bounded = page.listings[:remaining]
                selected.extend(
                    item.listing_identifier for item in bounded if item.status.value in statuses
                )
                examined += len(bounded)
                if len(page.listings) > remaining or (
                    examined == maximum_candidate_listings_examined and page.next_cursor is not None
                ):
                    return tuple(selected), examined, True
                cursor = page.next_cursor
                if cursor is None:
                    return tuple(selected), examined, False
        as_of = await self.database.current_completion_boundary()
        ancestry = await self._projection_search_scope(
            current_runs[0],
            current_runs[1:],
            as_of_completion_sequence=as_of,
            maximum_runs=100,
        )
        candidates_with_lookahead = await self.database.facebook_projection_membership_candidates(
            tuple(
                (run.record_identifier, run.internal_search_run_identifier) for run in ancestry.runs
            ),
            as_of_completion_sequence=as_of,
            maximum_listings=maximum_candidate_listings_examined + 1,
        )
        limit_reached = len(candidates_with_lookahead) > maximum_candidate_listings_examined
        candidates = candidates_with_lookahead[:maximum_candidate_listings_examined]
        listing_identifiers = tuple(
            candidate.candidate.listing_identifier for candidate in candidates
        )
        matching: list[str] = []
        chunk_size = 500
        for start in range(0, len(listing_identifiers), chunk_size):
            chunk = listing_identifiers[start : start + chunk_size]
            observations = await self.database.facebook_projection_item_observations(
                chunk,
                as_of_completion_sequence=as_of,
                maximum_per_listing=101,
            )
            bounded_observations, _ = self._bounded_observations_by_listing(
                observations,
                maximum_per_listing=100,
            )
            status_candidates: dict[str, list[StatusObservationCandidate]] = {}
            for listing_identifier, listing_observations in bounded_observations.items():
                for observation in listing_observations:
                    candidate = status_candidate_from_item_observation(observation)
                    if candidate is not None:
                        status_candidates.setdefault(listing_identifier, []).append(candidate)
            for candidate in await self.database.facebook_projection_search_occurrences(
                chunk,
                as_of_completion_sequence=as_of,
            ):
                status_candidates.setdefault(candidate.listing_identifier, []).append(candidate)
            matching.extend(
                listing_identifier
                for listing_identifier in chunk
                if select_status(
                    tuple(status_candidates.get(listing_identifier, ())),
                    as_of_completion_sequence=as_of,
                ).value
                in statuses
            )
        return tuple(matching), len(candidates), limit_reached

    async def _plan_missing_listing_analyses(
        self,
        request: RequestMissingListingAnalysesRequest,
        *,
        restrict_listing_identifiers: frozenset[str] | None = None,
        source_listing_identifiers: frozenset[str] | None = None,
    ) -> ListingAnalysisBatchSelection:
        await self._active_product_guide(request.product_guide_record_identifier)
        refresh = await self.database.work(request.search_refresh_work_identifier)
        if tuple(refresh["kind"]) not in {
            ("carl", "facebook", "work", "refresh_search"),
            REFRESH_EBAY_SEARCH_WORK_KIND,
        }:
            raise ReviewInputError("Analysis batch source is not a search-refresh work item")
        if refresh["state"] != WorkState.COMPLETED.value:
            raise ReviewInputError(
                "Wait for the search refresh to complete before requesting analysis"
            )
        refresh_payload = refresh.get("payload")
        refresh_result = refresh.get("result")
        if not isinstance(refresh_payload, dict) or not isinstance(refresh_result, dict):
            raise ReviewInputError("Search refresh has no usable retained result")
        base_identifier = refresh_payload.get("base_search_run_record_identifier")
        refreshed_identifier = refresh_result.get("refreshed_search_run_record_identifier")
        if not isinstance(base_identifier, str) or not isinstance(refreshed_identifier, str):
            raise ReviewInputError("Search refresh does not identify its source searches")

        async def listing_identifiers(record_identifier: str) -> tuple[str, ...]:
            return await run_listing_identifiers(self.database, record_identifier)

        if source_listing_identifiers is None:
            refreshed_listings = await listing_identifiers(refreshed_identifier)
            base_listings = await listing_identifiers(base_identifier)
            listing_ids = tuple(dict.fromkeys((*refreshed_listings, *base_listings)))
            refresh_maximum_items = refresh_payload.get("maximum_items")
            if isinstance(refresh_maximum_items, int):
                listing_ids = listing_ids[:refresh_maximum_items]
            included_listing_ids = frozenset(listing_ids)
        else:
            included_listing_ids = source_listing_identifiers
        if request.selection_snapshot_record_identifier is not None:
            snapshot = await self.get_selection_snapshot(
                request.selection_snapshot_record_identifier
            )
            workspace = await self.get_review_workspace(snapshot.workspace_record_identifier)
            enabled_guide_records = {
                binding.resolved_product_guide_record_identifier
                for binding in workspace.product_guide_bindings
                if binding.enabled
            }
            if (
                enabled_guide_records
                and request.product_guide_record_identifier not in enabled_guide_records
            ):
                raise ReviewInputError(
                    "The selection snapshot product guide is not enabled in its workspace"
                )
            snapshot_listing_ids = frozenset(item.listing_identifier for item in snapshot.items)
            included_listing_ids &= snapshot_listing_ids
        if restrict_listing_identifiers is not None:
            included_listing_ids &= restrict_listing_identifiers
        facebook_ids = tuple(
            identifier for identifier in included_listing_ids if not identifier.startswith("ebay:")
        )
        ebay_ids = tuple(
            identifier for identifier in included_listing_ids if identifier.startswith("ebay:")
        )
        sources = (
            tuple(
                source
                for source in await self.database.facebook_review_candidate_sources(facebook_ids)
                if source.listing_identifier in included_listing_ids
            )
            if facebook_ids
            else ()
        )
        if ebay_ids:
            sources += await ebay_candidate_sources(self.database, ebay_ids)

        return select_listing_analysis_batch(
            sources,
            filters=request.filters,
            selection_policy=request.selection_policy,
            product_guide_record_identifier=request.product_guide_record_identifier,
            maximum_items=request.maximum_items,
        )

    async def preview_selection_analyses(
        self, request: SelectionAnalysesRequest
    ) -> SelectionAnalysesPreview:
        (
            low_level_request,
            restrict_listing_identifiers,
            source_listing_identifiers,
            workspace,
            guide_binding_identifier,
            candidate_listings_examined,
            candidate_examination_limit_reached,
        ) = await self._selection_analysis_request(request)
        plan = await self._plan_missing_listing_analyses(
            low_level_request,
            restrict_listing_identifiers=restrict_listing_identifiers,
            source_listing_identifiers=source_listing_identifiers,
        )
        source_search_runs = self._workspace_current_search_runs(workspace)
        source_refresh_work = tuple(
            track.latest_refresh_work_identifier
            for track in workspace.search_tracks
            if track.enabled
            and track.current_search_run_record_identifier is not None
            and track.latest_refresh_work_identifier is not None
        ) or (low_level_request.search_refresh_work_identifier,)
        return SelectionAnalysesPreview(
            workspace_record_identifier=workspace.record_identifier,
            product_guide_binding_identifier=guide_binding_identifier,
            source_search_run_record_identifier=source_search_runs[0],
            source_search_run_record_identifiers=source_search_runs,
            source_search_refresh_work_identifier=(
                low_level_request.search_refresh_work_identifier
            ),
            source_search_refresh_work_identifiers=source_refresh_work,
            product_guide_record_identifier=low_level_request.product_guide_record_identifier,
            selection_kind=request.selection.kind,
            source_as_of_completion_sequence=plan.source_as_of_completion_sequence,
            selection_policy=request.selection_policy,
            candidate_listings_examined=candidate_listings_examined,
            candidate_examination_limit_reached=(candidate_examination_limit_reached),
            matching_latest_observations=len(plan.matching_observation_record_identifiers),
            excluded_by_selection_policy=(len(plan.policy_excluded_observation_record_identifiers)),
            eligible_latest_observations=len(plan.eligible_observation_record_identifiers),
            selected_observations=len(plan.selected_observation_record_identifiers),
            new_analysis_count=len(plan.selected_observation_record_identifiers),
            reused_analysis_count=len(plan.reusable_observation_record_identifiers),
            excluded_observation_count=len(plan.excluded_observation_record_identifiers),
        )

    async def request_selection_analyses(
        self, request: SelectionAnalysesRequest
    ) -> SelectionAnalysesRequestResult:
        (
            low_level_request,
            restrict_listing_identifiers,
            source_listing_identifiers,
            workspace,
            guide_binding_identifier,
            candidate_listings_examined,
            candidate_examination_limit_reached,
        ) = await self._selection_analysis_request(request)
        plan = await self._plan_missing_listing_analyses(
            low_level_request,
            restrict_listing_identifiers=restrict_listing_identifiers,
            source_listing_identifiers=source_listing_identifiers,
        )
        source_search_runs = self._workspace_current_search_runs(workspace)
        source_refresh_work = tuple(
            track.latest_refresh_work_identifier
            for track in workspace.search_tracks
            if track.enabled
            and track.current_search_run_record_identifier is not None
            and track.latest_refresh_work_identifier is not None
        ) or (low_level_request.search_refresh_work_identifier,)
        payload = RequestMissingAnalysesPayload(
            request_identifier="pending",
            source_search_refresh_work_identifier=(
                low_level_request.search_refresh_work_identifier
            ),
            source_search_refresh_work_identifiers=source_refresh_work,
            source_as_of_completion_sequence=plan.source_as_of_completion_sequence,
            listing_observation_record_identifiers=(plan.selected_observation_record_identifiers),
            product_guide_record_identifier=low_level_request.product_guide_record_identifier,
            selection_snapshot_record_identifier=(
                low_level_request.selection_snapshot_record_identifier
            ),
            selection_policy=request.selection_policy,
            allow_incomplete_gallery=request.allow_incomplete_gallery,
        )
        payload = payload.model_copy(
            update={
                "request_identifier": review_mutation_request_sha256(
                    "request_selection_analyses", payload
                )
            }
        )
        enqueued = await self.database.enqueue_work(
            request_missing_analyses_work(identifier=self.new_identifier(), payload=payload),
            WorkRequester(
                request_identifier=self.new_identifier(),
                kind=("carl", "mcp", "request_selection_analyses"),
                identifier=workspace.record_identifier,
                context={"request": request.model_dump(mode="json")},
            ),
            event_identifier=self.new_identifier(),
            enqueued_at_utc_ns=self.utc_now_ns(),
        )
        return SelectionAnalysesRequestResult(
            work_identifier=enqueued.work_item_identifier,
            created=enqueued.created,
            workspace_record_identifier=workspace.record_identifier,
            product_guide_binding_identifier=guide_binding_identifier,
            source_search_run_record_identifier=source_search_runs[0],
            source_search_run_record_identifiers=source_search_runs,
            source_search_refresh_work_identifier=(
                low_level_request.search_refresh_work_identifier
            ),
            source_search_refresh_work_identifiers=source_refresh_work,
            product_guide_record_identifier=low_level_request.product_guide_record_identifier,
            selection_kind=request.selection.kind,
            candidate_listings_examined=candidate_listings_examined,
            candidate_examination_limit_reached=(candidate_examination_limit_reached),
            selected_observation_count=len(plan.selected_observation_record_identifiers),
            source_as_of_completion_sequence=plan.source_as_of_completion_sequence,
        )

    async def preview_missing_listing_analyses(
        self, request: RequestMissingListingAnalysesRequest
    ) -> MissingListingAnalysesPreview:
        plan = await self._plan_missing_listing_analyses(request)
        return MissingListingAnalysesPreview(
            source_search_refresh_work_identifier=request.search_refresh_work_identifier,
            source_as_of_completion_sequence=plan.source_as_of_completion_sequence,
            selection_policy=request.selection_policy,
            matching_latest_observations=len(plan.matching_observation_record_identifiers),
            excluded_by_selection_policy=(
                len(plan.matching_observation_record_identifiers)
                - len(plan.eligible_observation_record_identifiers)
            ),
            eligible_latest_observations=len(plan.eligible_observation_record_identifiers),
            selected_observations=len(plan.selected_observation_record_identifiers),
        )

    async def request_missing_listing_analyses(
        self, request: RequestMissingListingAnalysesRequest
    ) -> RequestMissingListingAnalysesResult:
        plan = await self._plan_missing_listing_analyses(request)

        request_identifier = self.new_identifier()
        work_identifier = self.new_identifier()
        payload = RequestMissingAnalysesPayload(
            request_identifier=request_identifier,
            source_search_refresh_work_identifier=request.search_refresh_work_identifier,
            source_as_of_completion_sequence=plan.source_as_of_completion_sequence,
            listing_observation_record_identifiers=(plan.selected_observation_record_identifiers),
            product_guide_record_identifier=request.product_guide_record_identifier,
            selection_snapshot_record_identifier=(request.selection_snapshot_record_identifier),
            selection_policy=request.selection_policy,
            allow_incomplete_gallery=request.allow_incomplete_gallery,
        )
        enqueued = await self.database.enqueue_work(
            request_missing_analyses_work(identifier=work_identifier, payload=payload),
            WorkRequester(
                request_identifier=self.new_identifier(),
                kind=("carl", "mcp", "request_missing_listing_analyses"),
                identifier=request.search_refresh_work_identifier,
                context={"request": request.model_dump(mode="json")},
            ),
            event_identifier=self.new_identifier(),
            enqueued_at_utc_ns=self.utc_now_ns(),
        )
        return RequestMissingListingAnalysesResult(
            work_identifier=enqueued.work_item_identifier,
            created=enqueued.created,
            selected_observation_count=len(plan.selected_observation_record_identifiers),
            source_search_refresh_work_identifier=request.search_refresh_work_identifier,
            source_as_of_completion_sequence=plan.source_as_of_completion_sequence,
        )

    async def _retired_product_guide_identities(self) -> frozenset[tuple[str, ...]]:
        states: dict[tuple[str, ...], bool] = {}
        for _, value in await self.database.records_by_kind(PRODUCT_GUIDE_IDENTITY_STATE_KIND):
            state = ProductGuideIdentityStateRecord.model_validate_json(encode_json(value))
            states[state.product_guide_identity] = state.retired
        return frozenset(identity for identity, retired in states.items() if retired)

    @staticmethod
    def _product_guide_summary(guide: ProductGuideSummary, *, retired: bool) -> ProductGuideSummary:
        return ProductGuideSummary(
            record_identifier=guide.record_identifier,
            identity=guide.identity,
            display_name=guide.display_name,
            version=guide.version,
            previous_record_identifier=guide.previous_record_identifier,
            retired=retired,
        )

    async def list_product_guides(
        self, *, include_retired: bool = False
    ) -> tuple[ProductGuideSummary, ...]:
        retired = await self._retired_product_guide_identities()
        return tuple(
            self._product_guide_summary(guide, retired=guide.identity in retired)
            for guide in await self.database.product_guide_summaries()
            if include_retired or guide.identity not in retired
        )

    async def get_product_guide(self, record_identifier: str) -> ProductGuideDetails:
        guide = await self.database.product_guide(record_identifier)
        retired = guide.identity in await self._retired_product_guide_identities()
        return guide.model_copy(update={"retired": retired})

    async def _active_product_guide(self, record_identifier: str) -> ProductGuideDetails:
        guide = await self.get_product_guide(record_identifier)
        if guide.retired:
            raise ReviewInputError(
                "Restore the retired product-guide identity before using it for new work"
            )
        return guide

    async def set_product_guide_identity_retired(
        self, request: SetProductGuideIdentityRetiredRequest
    ) -> ProductGuideSummary:
        guide = await self.get_product_guide(request.product_guide_record_identifier)
        if guide.retired == request.retired:
            return self._product_guide_summary(guide, retired=guide.retired)
        if request.retired:
            for workspace in await self.list_review_workspaces(include_archived=True):
                if any(
                    binding.enabled and binding.product_guide_identity == guide.identity
                    for binding in workspace.product_guide_bindings
                ):
                    raise ReviewInputError(
                        "Disable this product-guide identity in every workspace before retiring it"
                    )
        record_identifier = self.new_identifier()
        state = ProductGuideIdentityStateRecord(
            record_identifier=record_identifier,
            product_guide_identity=guide.identity,
            retired=request.retired,
            recorded_at_utc=_utc_text(self.utc_now_ns()),
        )
        await self._publish_local_records(
            component_identifier=SET_PRODUCT_GUIDE_IDENTITY_RETIRED,
            records=(
                RecordDraft(
                    identifier=record_identifier,
                    kind=PRODUCT_GUIDE_IDENTITY_STATE_KIND,
                    schema_version=1,
                    value=state.model_dump(mode="json"),
                ),
            ),
            inputs=(
                NamedInput(
                    name=("product_guide",),
                    object_identifier=guide.record_identifier,
                ),
            ),
            outputs=(
                NamedOutput(
                    name=("product_guide_identity_state",),
                    object_identifier=record_identifier,
                ),
            ),
            result={
                "state": "completed",
                "product_guide_identity": list(guide.identity),
                "retired": request.retired,
            },
        )
        return self._product_guide_summary(guide, retired=request.retired)

    async def _author_product_guide(
        self,
        *,
        identity: tuple[str, ...],
        display_name: str,
        text: str,
        expected_base_record_identifier: str | None,
    ) -> ProductGuideDetails | ProductGuideConflict:
        started_utc_ns = self.utc_now_ns()
        started_monotonic_ns = self.monotonic_ns()
        provenance = await self._code_provenance()
        ended_utc_ns = self.utc_now_ns()
        return await self.database.author_product_guide(
            identity=identity,
            display_name=display_name,
            text=text,
            expected_base_record_identifier=expected_base_record_identifier,
            component=build_analysis_component_registry().require(AUTHOR_PRODUCT_GUIDE),
            operation_identifier=self.new_identifier(),
            record_identifier=self.new_identifier(),
            text_identifier=self.new_identifier(),
            provenance=provenance,
            invocation=process_invocation(),
            started_at_utc=_utc_text(started_utc_ns),
            ended_at_utc=_utc_text(ended_utc_ns),
            duration_ns=max(0, self.monotonic_ns() - started_monotonic_ns),
        )

    async def create_product_guide(
        self, request: CreateProductGuideRequest
    ) -> ProductGuideDetails | ProductGuideConflict:
        return await self._author_product_guide(
            identity=request.full_identity,
            display_name=request.display_name,
            text=request.text,
            expected_base_record_identifier=None,
        )

    async def retry_item_failures(
        self, request: RetryItemFailuresRequest
    ) -> RetryItemFailuresResult:
        """Retry linked transient item failures and resume a refresh without repeating its search."""
        return await retry_refresh_item_failures(
            self.database, request, utc_ns=self.utc_now_ns(), new_identifier=self.new_identifier
        )

    async def retry_image_failures(
        self, request: RetryImageFailuresRequest
    ) -> RetryImageFailuresResult:
        """Requeue terminal image work selected by a source-dispatched run or refresh."""

        source_kind: ImageFailureSourceKind | None = None
        try:
            source_work = await self.database.work(request.source_identifier)
        except KeyError:
            source_work = None
        if source_work is not None:
            if tuple(source_work.get("kind", ())) == REFRESH_EBAY_SEARCH_WORK_KIND:
                return await self._retry_ebay_images(request, ImageFailureSourceKind.SEARCH_REFRESH)
            if tuple(source_work.get("kind", ())) != (
                "carl",
                "facebook",
                "work",
                "refresh_search",
            ):
                raise ReviewInputError("The source work item is not a search refresh")
            source_kind = ImageFailureSourceKind.SEARCH_REFRESH
        else:
            try:
                record_kind, _, _ = await self.database.get_record(request.source_identifier)
            except KeyError:
                raise ReviewInputError("The retry source was not found") from None
            if record_kind == ("carl", "ebay", "search_run"):
                return await self._retry_ebay_images(request, ImageFailureSourceKind.SEARCH_RUN)
            if record_kind != ("carl", "facebook", "search_run"):
                raise ReviewInputError("The retry source record is not a search run")
            source_kind = ImageFailureSourceKind.SEARCH_RUN

        await self.database.register_constraint(
            image_session_work_constraint(),
            registered_at_utc_ns=self.utc_now_ns(),
        )
        matched, retried = await self.database.retry_terminal_facebook_image_work_for_source(
            source_identifier=request.source_identifier,
            maximum_items=request.maximum_items,
            retried_at_utc_ns=self.utc_now_ns(),
            retry_batch_identifier=self.new_identifier(),
            reason={
                "kind": "operator_image_retry",
                "source_kind": source_kind.value,
                "source_identifier": request.source_identifier,
            },
        )
        return RetryImageFailuresResult(
            source_identifier=request.source_identifier,
            source_kind=source_kind,
            matched_terminal_failures=matched,
            retried=len(retried),
            remaining_terminal_failures=matched - len(retried),
            retried_work_identifier_sample=tuple(retried[:20]),
        )

    async def _retry_ebay_images(
        self, request: RetryImageFailuresRequest, source_kind: ImageFailureSourceKind
    ) -> RetryImageFailuresResult:
        matched, identifiers = await retry_ebay_image_failures(
            self.database,
            request.source_identifier,
            request.maximum_items,
            self.utc_now_ns(),
            self.new_identifier,
        )
        return RetryImageFailuresResult(
            source_identifier=request.source_identifier,
            source_kind=source_kind,
            matched_terminal_failures=matched,
            retried=len(identifiers),
            remaining_terminal_failures=matched - len(identifiers),
            retried_work_identifier_sample=identifiers[:20],
        )

    async def revise_product_guide(
        self, request: ReviseProductGuideRequest
    ) -> ProductGuideDetails | ProductGuideConflict:
        base = await self._active_product_guide(request.expected_base_record_identifier)
        return await self._author_product_guide(
            identity=base.identity,
            display_name=request.display_name,
            text=request.text,
            expected_base_record_identifier=request.expected_base_record_identifier,
        )

    async def get_listing_dossier(self, listing_identifier: str) -> ListingDossier:
        if not listing_identifier.isdecimal():
            raise ReviewInputError("Facebook listing identifiers are decimal strings")
        sources = tuple(
            source
            for source in await self.database.facebook_review_candidate_sources(
                (listing_identifier,)
            )
            if source.listing_identifier == listing_identifier
        )
        if not sources:
            raise KeyError(listing_identifier)
        selected = max(
            sources,
            key=lambda source: (
                source.acquisition_completion_sequence,
                source.observation_completion_sequence,
                source.observation_record_identifier,
            ),
        )
        references = gallery_references(
            observation_identifier=selected.observation_record_identifier,
            observation=selected.observation,
        )
        reference_identifiers = await self.database.facebook_gallery_reference_identifiers(
            (selected.observation_record_identifier,)
        )
        selected_reference_identifiers = tuple(
            identifier
            for reference in references
            if (identifier := reference_identifiers.get(reference)) is not None
        )
        saved_by_reference = await self.database.resolved_facebook_image_results_by_reference(
            selected_reference_identifiers
        )
        saved_by_rendition: dict[tuple[str | None, str], tuple[str, dict[str, JsonValue]]] = {}
        for (
            result_identifier,
            value,
        ) in await self.database.saved_facebook_image_results_for_renditions(
            tuple((reference.photo_id, reference.original_url) for reference in references)
        ):
            photo_identifier = value.get("source_photo_id")
            original_url = value.get("original_url")
            if (photo_identifier is None or isinstance(photo_identifier, str)) and isinstance(
                original_url, str
            ):
                saved_by_rendition[(photo_identifier, original_url)] = (
                    result_identifier,
                    value,
                )
        gallery: list[GalleryImageDescriptor] = []
        for reference in references:
            reference_identifier = reference_identifiers.get(reference)
            saved = (
                None
                if reference_identifier is None
                else saved_by_reference.get(reference_identifier)
            ) or saved_by_rendition.get((reference.photo_id, reference.original_url))
            result_identifier, result = (None, {}) if saved is None else saved

            def optional_string(name: str, source: dict[str, JsonValue] = result) -> str | None:
                value = source.get(name)
                return value if isinstance(value, str) else None

            def optional_integer(name: str, source: dict[str, JsonValue] = result) -> int | None:
                value = source.get(name)
                return value if isinstance(value, int) and not isinstance(value, bool) else None

            gallery.append(
                GalleryImageDescriptor(
                    gallery_order=reference.gallery_order,
                    gallery_reference_record_identifier=reference_identifier,
                    original_url=reference.original_url,
                    photo_identifier=reference.photo_id,
                    declared_width=reference.declared_width,
                    declared_height=reference.declared_height,
                    image_result_record_identifier=result_identifier,
                    image_artifact_identifier=optional_string("image_artifact_identifier"),
                    download_state="not_yet_collected" if saved is None else "saved",
                    sha256=optional_string("sha256"),
                    media_type=optional_string("mime_type"),
                    width=optional_integer("width"),
                    height=optional_integer("height"),
                )
            )
        observation = selected.observation
        fields = observation.get("fields") if isinstance(observation, dict) else None
        if not isinstance(fields, dict):
            raise ValueError("Listing observation fields are malformed")
        older = sorted(
            (
                source
                for source in sources
                if source.observation_record_identifier != selected.observation_record_identifier
            ),
            key=lambda source: (
                -source.acquisition_completion_sequence,
                -source.observation_completion_sequence,
                source.observation_record_identifier,
            ),
        )
        return ListingDossier(
            listing_identifier=listing_identifier,
            selected_observation_record_identifier=selected.observation_record_identifier,
            selected_acquisition_record_identifier=selected.acquisition_record_identifier,
            availability=selected.availability,
            acquisition_completion_sequence=selected.acquisition_completion_sequence,
            observation_completion_sequence=selected.observation_completion_sequence,
            fields=fields,
            gallery=tuple(sorted(gallery, key=lambda image: image.gallery_order)),
            analyses=listing_analysis_history(sources),
            older_observation_record_identifiers=tuple(
                source.observation_record_identifier for source in older
            ),
        )

    async def get_listing_analysis(self, record_identifier: str) -> AnalysisReport:
        kind, _, value = await self.database.get_record(record_identifier)
        if kind not in {
            ("carl", "facebook", "item_analysis"),
            ("carl", "ebay", "item_analysis"),
        } or not isinstance(value, dict):
            raise ValueError("Item analysis record is malformed")
        descriptor = (
            await ebay_analysis_descriptor(self.database, record_identifier)
            if kind == ("carl", "ebay", "item_analysis")
            else await self.database.facebook_analysis_descriptor(record_identifier)
        )
        text = value.get("analysis_text")
        if text is not None and not isinstance(text, str):
            raise ValueError("Item analysis text is malformed")
        return AnalysisReport(
            descriptor=descriptor,
            text=text,
            limit_observations=value.get("limit_observations"),
            claude=value.get("claude"),
        )

    async def get_provenance(
        self, object_identifier: str, maximum_output_edges: int | None = 0
    ) -> ProvenanceObject:
        if maximum_output_edges is not None and not 0 <= maximum_output_edges <= 100:
            raise ReviewInputError("Maximum provenance output edges must be between 0 and 100")
        operation_identifier, inputs, outputs = await self.database.object_operation_relations(
            object_identifier,
            maximum_outputs=maximum_output_edges,
        )
        output_edge_count = await self.database.operation_output_count(operation_identifier)
        return ProvenanceObject(
            object_identifier=object_identifier,
            producing_operation=await self.database.operation(operation_identifier),
            inputs=[input_value.model_dump(mode="json") for input_value in inputs],
            output_edge_count=output_edge_count,
            output_edges_truncated=len(outputs) < output_edge_count,
            outputs=[output.model_dump(mode="json") for output in outputs],
        )

    async def request_search_pipeline(
        self, request: RequestSearchPipelineRequest
    ) -> SearchPipelineRequestResult:
        from carl.pipeline import request_search_pipeline

        return await request_search_pipeline(self, request)

    async def get_search_pipeline(self, work_identifier: str) -> SearchPipelineStatus:
        from carl.pipeline import get_search_pipeline

        return await get_search_pipeline(self, work_identifier)

    async def pipeline_analysis_reuse(
        self, observation_identifier: str, options: PipelineOptions
    ) -> bool:
        from carl.core.analysis_batch import ListingAnalysisSelectionPolicy

        if options.selection_policy is ListingAnalysisSelectionPolicy.MISSING_FOR_CURRENT_EVIDENCE:
            return False
        kind, _, value = await self.database.get_record(observation_identifier)
        if not isinstance(value, dict):
            raise ReviewInputError("Pipeline analysis input is not an observation")
        if kind == ("carl", "facebook", "listing_observation"):
            marketplace = "facebook"
            external_identifier = value.get("listing_id")
        elif kind == ("carl", "ebay", "listing_observation"):
            marketplace = "ebay"
            external_identifier = value.get("item_identifier")
        else:
            raise ReviewInputError("Pipeline analysis input is not a marketplace observation")
        if not isinstance(external_identifier, str):
            raise ReviewInputError("Pipeline observation has no exact listing identity")
        return await self.database.pipeline_completed_analysis_exists(
            marketplace=marketplace,
            external_identifier=external_identifier,
            product_guide_record_identifier=(
                options.product_guide_record_identifier
                if options.selection_policy
                is ListingAnalysisSelectionPolicy.MISSING_FOR_SELECTED_GUIDE
                else None
            ),
        )

    async def request_listing_details(
        self,
        request: RequestEbayListingDetailsRequest,
        *,
        requester_kind: tuple[str, ...] = ("carl", "marketplace", "request_listing_details"),
        requester_identifier: str | None = None,
    ) -> RequestListingDetailsResult:
        if request.marketplace is Marketplace.FACEBOOK:
            reused = not request.refresh and bool(
                await self.database.successful_facebook_item_page_results(
                    (request.external_identifier,)
                )
            )
            definition = request_facebook_listing_details_work(
                identifier=self.new_identifier(),
                payload=RequestFacebookListingDetailsPayload(
                    listing_identifier=request.external_identifier,
                    maximum_images=request.maximum_images,
                    refresh=request.refresh,
                    item_routing=("decodo", "personal", request.decodo_route),
                    image_routing=request.requested_image_network_path,
                ),
            )
            enqueued = await self.database.enqueue_work(
                definition,
                WorkRequester(
                    request_identifier=self.new_identifier(),
                    kind=requester_kind,
                    identifier=requester_identifier or definition.identifier,
                    context=request.model_dump(mode="json"),
                ),
                event_identifier=self.new_identifier(),
                enqueued_at_utc_ns=self.utc_now_ns(),
            )
            return RequestListingDetailsResult(
                marketplace=Marketplace.FACEBOOK,
                external_identifier=request.external_identifier,
                work_identifier=enqueued.work_item_identifier,
                created=enqueued.created,
                state=(await self.database.work_state(enqueued.work_item_identifier)).value,
                reused_item_page=reused,
            )
        item_request = request.item_request()
        usable = tuple(
            (identifier, value)
            for identifier, value in await ebay_observations(
                self.database, request.external_identifier
            )
            if value.get("classification") == "detail"
            and isinstance(value.get("request"), dict)
            and value["request"].get("stack_identifier") == request.stack_identifier
            and isinstance(value.get("acquisition_record_identifier"), str)
        )
        reused = bool(usable) and not request.refresh
        if reused:
            definition = extract_ebay_item_work(
                identifier=self.new_identifier(),
                payload=ExtractEbayItemPayload(
                    request=item_request,
                    acquisition_record_identifier=usable[-1][1]["acquisition_record_identifier"],
                ),
            )
        else:
            definition = collect_ebay_item_work(
                identifier=self.new_identifier(),
                payload=CollectEbayItemPayload(request=item_request),
            )
        enqueued = await self.database.enqueue_work(
            definition,
            WorkRequester(
                request_identifier=self.new_identifier(),
                kind=requester_kind,
                identifier=requester_identifier or definition.identifier,
                context=request.model_dump(mode="json"),
            ),
            event_identifier=self.new_identifier(),
            enqueued_at_utc_ns=self.utc_now_ns(),
        )
        return RequestListingDetailsResult(
            marketplace=Marketplace.EBAY,
            external_identifier=request.external_identifier,
            work_identifier=enqueued.work_item_identifier,
            created=enqueued.created,
            state=(await self.database.work_state(enqueued.work_item_identifier)).value,
            reused_item_page=reused,
        )

    async def get_listing_details(
        self, request: GetMarketplaceListingRequest
    ) -> MarketplaceListingDetails:
        if request.marketplace is Marketplace.EBAY:
            return await read_ebay_listing(self.database, request)
        try:
            projection = await self.get_composed_listing(
                GetComposedListingRequest(
                    listing_identifier=request.external_identifier,
                    maximum_gallery_images=request.maximum_images,
                )
            )
        except KeyError:
            return MarketplaceListingDetails(
                marketplace=Marketplace.FACEBOOK,
                external_identifier=request.external_identifier,
                canonical_url=canonical_facebook_listing_url(request.external_identifier),
                classification="not_collected",
            )
        title = projection.title.value if projection.title is not None else None
        description = projection.description.value if projection.description is not None else None
        price = projection.price.value if projection.price is not None else None
        displayed_price = price if isinstance(price, str) else None
        currency = None
        if isinstance(price, dict):
            amount = price.get("formatted_amount") or price.get("amount_decimal")
            currency = price.get("currency")
            displayed_price = str(amount) if isinstance(amount, (str, int, float)) else None
        images = (
            tuple(
                MarketplaceListingImage(
                    gallery_order=image.descriptor.gallery_order,
                    url=image.descriptor.original_url,
                    state=image.descriptor.download_state,
                    reference_record_identifier=image.descriptor.gallery_reference_record_identifier,
                    result_record_identifier=image.descriptor.image_result_record_identifier,
                    artifact_identifier=image.descriptor.image_artifact_identifier,
                    width=image.descriptor.width,
                    height=image.descriptor.height,
                )
                for image in projection.gallery.images
            )
            if projection.gallery is not None
            else ()
        )
        evidence = (
            projection.description.evidence
            if projection.description is not None
            else projection.title.evidence
            if projection.title is not None
            else None
        )
        return MarketplaceListingDetails(
            marketplace=Marketplace.FACEBOOK,
            external_identifier=request.external_identifier,
            canonical_url=projection.canonical_source_url,
            classification=projection.status.value.value,
            title=title if isinstance(title, str) else None,
            displayed_price=displayed_price,
            currency=currency if isinstance(currency, str) else None,
            description=description if isinstance(description, str) else None,
            description_state="retained" if description else "not_available",
            observation_record_identifier=evidence.observation_record_identifier
            if evidence
            else None,
            acquisition_record_identifier=evidence.acquisition_record_identifier
            if evidence
            else None,
            images=images,
            referenced_image_count=projection.gallery.referenced_image_count
            if projection.gallery
            else 0,
            images_truncated=projection.gallery.images_truncated if projection.gallery else False,
        )

    async def get_image(self, artifact_identifier: str) -> ResolvedImage:
        image_results = await self.database.saved_facebook_image_results()
        trusted_facebook = any(
            value.get("image_artifact_identifier") == artifact_identifier
            for _, value in image_results
        )
        trusted_ebay = any(
            isinstance(value, dict)
            and value.get("state") == "saved"
            and value.get("image_artifact_identifier") == artifact_identifier
            for _, value in await self.database.records_by_kind(("carl", "ebay", "image_result"))
        )
        if not trusted_facebook and not trusted_ebay:
            raise ReviewInputError("Artifact is not a saved listing image")
        metadata, content = await self.database.get_artifact(artifact_identifier)
        media_type = metadata.get("media_type")
        sha256 = metadata.get("sha256")
        if not isinstance(media_type, str) or not media_type.startswith("image/"):
            raise ValueError("Saved image artifact has an invalid media type")
        if not isinstance(sha256, str):
            raise ValueError("Saved image artifact has no content digest")
        return ResolvedImage(
            artifact_identifier=artifact_identifier,
            media_type=media_type,
            sha256=sha256,
            content=content,
        )

    async def request_listing_analysis(
        self,
        request: RequestAnalysisRequest,
        *,
        requester_kind: tuple[str, ...] = ("carl", "mcp", "request_listing_analysis"),
        requester_identifier: str | None = None,
        requester_context: JsonValue | None = None,
    ) -> RequestAnalysisResult:
        kind, _, observation = await self.database.get_record(
            request.listing_observation_record_identifier
        )
        if kind == ("carl", "ebay", "listing_observation"):
            return await request_ebay_listing_analysis(
                self,
                request,
                requester_kind=requester_kind,
                requester_identifier=requester_identifier,
                requester_context=requester_context,
            )
        if kind != ("carl", "facebook", "listing_observation") or not isinstance(observation, dict):
            raise ReviewInputError("Analysis input is not a listing observation")
        classification = observation.get("response_classification")
        if not isinstance(classification, dict) or classification.get("kind") != "full_listing":
            raise ReviewInputError("Analysis requires a full-listing observation")
        await self._active_product_guide(request.product_guide_record_identifier)
        references = gallery_references(
            observation_identifier=request.listing_observation_record_identifier,
            observation=observation,
        )
        reference_identifiers = await self.database.facebook_gallery_reference_identifiers(
            (request.listing_observation_record_identifier,)
        )
        selected_reference_identifiers = tuple(
            identifier
            for reference in references
            if (identifier := reference_identifiers.get(reference)) is not None
        )
        saved_by_reference = await self.database.resolved_facebook_image_results_by_reference(
            selected_reference_identifiers
        )
        saved_by_rendition: dict[tuple[str | None, str], tuple[str, dict[str, JsonValue]]] = {}
        for (
            result_identifier,
            value,
        ) in await self.database.saved_facebook_image_results_for_renditions(
            tuple((reference.photo_id, reference.original_url) for reference in references)
        ):
            photo_identifier = value.get("source_photo_id")
            original_url = value.get("original_url")
            if (photo_identifier is None or isinstance(photo_identifier, str)) and isinstance(
                original_url, str
            ):
                saved_by_rendition[(photo_identifier, original_url)] = (
                    result_identifier,
                    value,
                )
        included: list[AnalysisImageSelection] = []
        unavailable: list[UnavailableAnalysisImageSelection] = []
        gallery_absence_reason = (
            "listing_observation_has_no_gallery_references" if not references else None
        )
        for reference in references:
            reference_identifier = reference_identifiers.get(reference)
            if reference_identifier is None:
                raise ReviewInputError(
                    "Listing gallery references have not been retained; run collect-images first"
                )
            saved = saved_by_reference.get(reference_identifier) or saved_by_rendition.get(
                (reference.photo_id, reference.original_url)
            )
            if saved is None:
                unavailable.append(
                    UnavailableAnalysisImageSelection(
                        gallery_image_reference_record_identifier=reference_identifier,
                        gallery_order=reference.gallery_order,
                        reason="no_saved_usable_image",
                    )
                )
            else:
                included.append(
                    AnalysisImageSelection(
                        gallery_image_reference_record_identifier=reference_identifier,
                        image_result_record_identifier=saved[0],
                    )
                )
        if (unavailable or gallery_absence_reason is not None) and not (
            request.allow_incomplete_gallery
        ):
            raise IncompleteGalleryError(
                tuple(image.gallery_order for image in unavailable),
                gallery_absence_reason,
            )
        evidence = listing_analysis_evidence_set(
            listing_observation_record_identifier=request.listing_observation_record_identifier,
            gallery_images=tuple(included),
            unavailable_gallery_images=tuple(unavailable),
            gallery_absence_reason=gallery_absence_reason,
        )
        evidence_sets = await self.database.facebook_listing_analysis_evidence_sets(
            (request.listing_observation_record_identifier,)
        )
        evidence_identifier = evidence_sets.get(evidence)
        if evidence_identifier is None:
            evidence_identifier = self.new_identifier()
            operation_identifier = self.new_identifier()
            started_utc_ns = self.utc_now_ns()
            started_monotonic_ns = self.monotonic_ns()
            provenance = await self._code_provenance()
            with anyio.CancelScope(shield=True):
                await self.database.begin_operation(
                    operation_id=operation_identifier,
                    component=build_analysis_component_registry().require(
                        PLAN_FACEBOOK_LISTING_ANALYSIS_EVIDENCE
                    ),
                    provenance=provenance,
                    invocation=process_invocation(),
                    configuration={
                        "selection_policy": {
                            "listing_observation": "explicit_record_identifier",
                            "gallery_images": "all_saved_or_source_asset_reused_images",
                            "allow_incomplete_gallery": request.allow_incomplete_gallery,
                        }
                    },
                    started_at_utc=_utc_text(started_utc_ns),
                    inputs=(
                        (
                            ("listing_observation",),
                            request.listing_observation_record_identifier,
                        ),
                        *(
                            (
                                ("gallery_image_reference", f"{index:08d}"),
                                selection.gallery_image_reference_record_identifier,
                            )
                            for index, selection in enumerate((*included, *unavailable))
                        ),
                        *(
                            (
                                ("image_result", f"{index:08d}"),
                                selection.image_result_record_identifier,
                            )
                            for index, selection in enumerate(included)
                        ),
                    ),
                )
                await self.database.complete_operation(
                    operation_id=operation_identifier,
                    records=(
                        RecordDraft(
                            identifier=evidence_identifier,
                            kind=("carl", "facebook", "listing_analysis_evidence"),
                            schema_version=2,
                            value={
                                "unavailable_gallery_images": [
                                    image.model_dump(mode="json") for image in unavailable
                                ],
                                "gallery_absence_reason": gallery_absence_reason,
                            },
                        ),
                    ),
                    artifacts=(),
                    outputs=(
                        NamedOutput(
                            name=("listing_analysis_evidence",),
                            object_identifier=evidence_identifier,
                        ),
                    ),
                    result={"state": "completed"},
                    ended_at_utc=_utc_text(self.utc_now_ns()),
                    duration_ns=max(0, self.monotonic_ns() - started_monotonic_ns),
                )
        payload = AnalyzeItemPayload(
            evidence_set_record_identifier=evidence_identifier,
            product_guide_record_identifier=request.product_guide_record_identifier,
            maximum_turns=max(8, len(included) + 5),
        )
        existing_work_identifier = await self.database.facebook_item_analysis_work_identifier(
            payload
        )
        work_requester = WorkRequester(
            request_identifier=self.new_identifier(),
            kind=requester_kind,
            identifier=(
                requester_identifier
                if requester_identifier is not None
                else request.listing_observation_record_identifier
            ),
            context=(
                requester_context
                if requester_context is not None
                else {"request": request.model_dump(mode="json")}
            ),
        )
        if existing_work_identifier is not None:
            await self.database.attach_work_request(
                existing_work_identifier,
                work_requester,
                event_identifier=self.new_identifier(),
                requested_at_utc_ns=self.utc_now_ns(),
            )
            return RequestAnalysisResult(
                work_identifier=existing_work_identifier,
                created=False,
                evidence_set_record_identifier=evidence_identifier,
                included_gallery_count=len(included),
                unavailable_gallery_orders=tuple(image.gallery_order for image in unavailable),
                gallery_absence_reason=gallery_absence_reason,
            )
        enqueued = await self.database.enqueue_work(
            analyze_item_work(identifier=self.new_identifier(), payload=payload),
            work_requester,
            event_identifier=self.new_identifier(),
            enqueued_at_utc_ns=self.utc_now_ns(),
        )
        return RequestAnalysisResult(
            work_identifier=enqueued.work_item_identifier,
            created=enqueued.created,
            evidence_set_record_identifier=evidence_identifier,
            included_gallery_count=len(included),
            unavailable_gallery_orders=tuple(image.gallery_order for image in unavailable),
            gallery_absence_reason=gallery_absence_reason,
        )

    async def get_work_status(
        self,
        work_identifier: str,
        include_details: bool = False,
    ) -> WorkStatus:
        work = await self.database.work(work_identifier)
        operations_value = work.get("operations")
        operations = operations_value if isinstance(operations_value, list) else []
        result = work.get("result")
        result_mapping = result if isinstance(result, dict) else {}
        checkpoint_stage_value = result_mapping.get("stage")
        checkpoint_stage = (
            checkpoint_stage_value if isinstance(checkpoint_stage_value, str) else None
        )
        search_progress = None
        refresh_summary = None
        outcome = None
        analysis_batch_progress = None
        kind = tuple(str(part) for part in work["kind"])
        state = WorkState(str(work["state"]))
        error_kind = work_error_kind(work.get("error"))
        if state is WorkState.TERMINAL_FAILURE and error_kind is None:
            error_kind = "unknown"
        if kind in {("carl", "facebook", "work", "refresh_search"), REFRESH_EBAY_SEARCH_WORK_KIND}:
            refreshed_identifier_value = result_mapping.get(
                "refreshed_search_run_record_identifier"
            )
            refreshed_identifier = (
                refreshed_identifier_value if isinstance(refreshed_identifier_value, str) else None
            )
            if state in {WorkState.COMPLETED, WorkState.TERMINAL_FAILURE}:
                if (
                    "selected_unique_listings" in result_mapping
                    or "item_failures" in result_mapping
                ):
                    refresh_summary = refresh_failure_summary(result_mapping)
                    outcome = refresh_summary.severity
                elif result_mapping.get("state") == "completed_with_failures":
                    outcome = "partial_failure"
                else:
                    outcome = "success"
                if state is WorkState.TERMINAL_FAILURE:
                    outcome = "failed"
            child_states = (
                await ebay_refresh_child_work_states(self.database, work_identifier)
                if kind == REFRESH_EBAY_SEARCH_WORK_KIND
                else await self.database.search_refresh_child_work_states(
                    refresh_work_identifier=work_identifier,
                    refreshed_search_run_record_identifier=refreshed_identifier,
                )
            )
            item_pages = work_group_progress(child_states["item_pages"])
            item_extractions = work_group_progress(child_states["item_extractions"])
            images = work_group_progress(child_states["images"])
            image_extractions = work_group_progress(child_states["image_extractions"])
            search_progress = SearchRefreshProgress(
                checkpoint_stage=checkpoint_stage,
                active_phase=search_refresh_phase(
                    state=state,
                    checkpoint_stage=checkpoint_stage,
                    item_pages=item_pages,
                    item_extractions=item_extractions,
                    images=images,
                    image_extractions=image_extractions,
                ),
                item_pages=item_pages,
                item_extractions=item_extractions,
                images=images,
                image_extractions=image_extractions,
                descriptions=(
                    work_group_progress(child_states["descriptions"])
                    if "descriptions" in child_states
                    else None
                ),
            )
        if kind == REQUEST_MISSING_ANALYSES_WORK_KIND:
            payload_value = work.get("payload")
            payload_mapping = payload_value if isinstance(payload_value, dict) else {}
            selected_value = payload_mapping.get("listing_observation_record_identifiers")
            selected = selected_value if isinstance(selected_value, list) else []
            checkpoint_children_value = result_mapping.get("analysis_work_identifiers")
            checkpoint_children = (
                tuple(item for item in checkpoint_children_value if isinstance(item, str))
                if isinstance(checkpoint_children_value, list)
                else ()
            )
            observed_edges = await self.database.requested_work_edges(
                requester_kind=("carl", "facebook", "analysis_batch"),
                requester_identifier=work_identifier,
            )
            observed_children = tuple(
                dict.fromkeys(str(edge["work_identifier"]) for edge in observed_edges)
            )
            children = tuple(dict.fromkeys((*checkpoint_children, *observed_children)))
            checkpoint_processed = _nonnegative_integer(
                result_mapping.get("next_observation_index")
            )
            processed_observation_identifiers = set(selected[:checkpoint_processed])
            for edge in observed_edges:
                context = edge.get("context")
                if not isinstance(context, dict):
                    continue
                observation_identifier = context.get("listing_observation_record_identifier")
                if isinstance(observation_identifier, str):
                    processed_observation_identifiers.add(observation_identifier)
            processed_observations = sum(
                identifier in processed_observation_identifiers for identifier in selected
            )
            child_summaries = await self.database.work_summaries(children)
            child_states = tuple(WorkState(str(summary["state"])) for summary in child_summaries)
            active_identifiers = tuple(
                str(summary["identifier"])
                for summary in child_summaries
                if WorkState(str(summary["state"])) in {WorkState.PENDING, WorkState.LEASED}
            )
            failed = tuple(
                summary
                for summary in child_summaries
                if WorkState(str(summary["state"])) is WorkState.TERMINAL_FAILURE
            )
            failure_counts: dict[str, int] = {}
            for summary in failed:
                failure_kind = work_error_kind(summary.get("error")) or "unknown"
                failure_counts[failure_kind] = failure_counts.get(failure_kind, 0) + 1
            recent_failures = sorted(
                failed,
                key=lambda summary: int(summary["latest_event_sequence"]),
                reverse=True,
            )[:10]
            analysis_batch_progress = AnalysisBatchProgress(
                selected_observations=len(selected),
                processed_observations=processed_observations,
                remaining_observations=max(0, len(selected) - processed_observations),
                checkpoint_processed_observations=checkpoint_processed,
                observed_request_edges=len(observed_edges),
                newly_created_work=_nonnegative_integer(result_mapping.get("newly_created_work")),
                reused_work=_nonnegative_integer(result_mapping.get("reused_work")),
                skipped_observations=(
                    len(result_mapping.get("skipped", []))
                    if isinstance(result_mapping.get("skipped"), list)
                    else 0
                ),
                analyses=work_group_progress(child_states),
                active_analysis_work_count=len(active_identifiers),
                active_analysis_work_identifiers=active_identifiers[-10:],
                active_analysis_work_identifiers_truncated=len(active_identifiers) > 10,
                failure_reason_counts=tuple(
                    WorkFailureReasonCount(kind=failure_kind, count=count)
                    for failure_kind, count in sorted(failure_counts.items())
                ),
                recent_terminal_failures=tuple(
                    WorkFailureSummary(
                        work_identifier=str(summary["identifier"]),
                        kind=work_error_kind(summary.get("error")) or "unknown",
                    )
                    for summary in recent_failures
                ),
            )
        observed_at_utc_ns = self.utc_now_ns()
        lease_expires_value = work.get("lease_expires_at_utc_ns")
        lease_expires_at_utc_ns = (
            lease_expires_value if isinstance(lease_expires_value, int) else None
        )
        latest_event_at_utc_ns = int(work["latest_event_at_utc_ns"])
        last_lease_activity_value = work.get("last_lease_activity_at_utc_ns")
        last_lease_activity_at_utc_ns = (
            last_lease_activity_value if isinstance(last_lease_activity_value, int) else None
        )
        return WorkStatus(
            identifier=work_identifier,
            kind=kind,
            payload_schema_version=int(work["payload_schema_version"]),
            state=state,
            attempt=int(work["attempt"]),
            stage=checkpoint_stage,
            error_kind=error_kind,
            operation_count=len(operations),
            runtime=WorkRuntimeStatus(
                observed_at_utc_ns=observed_at_utc_ns,
                created_at_utc_ns=int(work["created_at_utc_ns"]),
                eligible_at_utc_ns=int(work["eligible_at_utc_ns"]),
                worker_identifier=(
                    str(work["worker_identifier"])
                    if work.get("worker_identifier") is not None
                    else None
                ),
                lease_expires_at_utc_ns=lease_expires_at_utc_ns,
                lease_remaining_ns=(
                    None
                    if lease_expires_at_utc_ns is None
                    else lease_expires_at_utc_ns - observed_at_utc_ns
                ),
                latest_event_sequence=int(work["latest_event_sequence"]),
                latest_event_kind=WorkEventKind(str(work["latest_event_kind"])),
                latest_event_at_utc_ns=latest_event_at_utc_ns,
                latest_event_age_ns=max(0, observed_at_utc_ns - latest_event_at_utc_ns),
                last_lease_activity_at_utc_ns=last_lease_activity_at_utc_ns,
                lease_activity_age_ns=(
                    None
                    if last_lease_activity_at_utc_ns is None
                    else max(0, observed_at_utc_ns - last_lease_activity_at_utc_ns)
                ),
            ),
            search_refresh_progress=search_progress,
            outcome=outcome,
            refresh_failure_summary=refresh_summary,
            analysis_batch_progress=analysis_batch_progress,
            details=(
                WorkStatusDetails(
                    payload=work["payload"],
                    result=result,
                    error=work.get("error"),
                    recent_operations=tuple(
                        WorkOperationSummary.model_validate(operation)
                        for operation in operations[-5:]
                    ),
                )
                if include_details
                else None
            ),
        )
