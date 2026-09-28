"""Pure request and work models for durable listing-analysis batches."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from carl.core.composed_projection import ListingStatus
from carl.core.models import JsonStringEnumeration, StrictModel
from carl.core.review import (
    CandidateAnalysisFilter,
    CandidateFilters,
    CandidateSource,
    ListCandidatesRequest,
    candidate_page,
)
from carl.core.review_workspace import ListingIdsSelection, ReviewBatchSelection, WorksetSelection
from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    WorkDefinition,
)

REQUEST_MISSING_ANALYSES_WORK_KIND = (
    "carl",
    "facebook",
    "work",
    "request_missing_listing_analyses",
)
LEGACY_REQUEST_MISSING_ANALYSES_PAYLOAD_SCHEMA_VERSION = 1
PREVIOUS_REQUEST_MISSING_ANALYSES_PAYLOAD_SCHEMA_VERSION = 2
REQUEST_MISSING_ANALYSES_PAYLOAD_SCHEMA_VERSION = 3
REQUEST_MISSING_ANALYSES_MAXIMUM_ACTIVE = 10


class ListingAnalysisSelectionPolicy(JsonStringEnumeration):
    MISSING_FOR_CURRENT_EVIDENCE = "missing_for_current_evidence"
    MISSING_FOR_SELECTED_GUIDE = "missing_for_selected_guide"
    NEVER_ANALYZED_LISTING = "never_analyzed_listing"


def apply_listing_analysis_selection_policy(
    sources: tuple[CandidateSource, ...],
    policy: ListingAnalysisSelectionPolicy,
    *,
    product_guide_record_identifier: str | None = None,
) -> tuple[CandidateSource, ...]:
    """Apply listing-history policy before selecting each listing's latest observation."""

    if policy is ListingAnalysisSelectionPolicy.MISSING_FOR_CURRENT_EVIDENCE:
        return sources
    if policy is ListingAnalysisSelectionPolicy.MISSING_FOR_SELECTED_GUIDE:
        if product_guide_record_identifier is None:
            raise ValueError("The selected-guide policy requires an exact product guide")
        analyzed_listing_identifiers = {
            source.listing_identifier
            for source in sources
            if any(
                analysis.product_guide_record_identifier == product_guide_record_identifier
                for analysis in source.analyses
            )
        }
        return tuple(
            source
            for source in sources
            if source.listing_identifier not in analyzed_listing_identifiers
        )
    analyzed_listing_identifiers = {
        source.listing_identifier for source in sources if source.analyses
    }
    return tuple(
        source
        for source in sources
        if source.listing_identifier not in analyzed_listing_identifiers
    )


class ListingAnalysisBatchSelection(StrictModel):
    source_as_of_completion_sequence: int = Field(ge=0)
    matching_observation_record_identifiers: tuple[str, ...]
    eligible_observation_record_identifiers: tuple[str, ...]
    selected_observation_record_identifiers: tuple[str, ...]
    reusable_observation_record_identifiers: tuple[str, ...] = ()
    policy_excluded_observation_record_identifiers: tuple[str, ...] = ()
    excluded_observation_record_identifiers: tuple[str, ...] = ()


def select_listing_analysis_batch(
    sources: tuple[CandidateSource, ...],
    *,
    filters: CandidateFilters,
    selection_policy: ListingAnalysisSelectionPolicy,
    product_guide_record_identifier: str | None = None,
    maximum_items: int | None,
) -> ListingAnalysisBatchSelection:
    """Select a stable analysis batch without performing I/O."""

    def matching_observations(
        candidate_sources: tuple[CandidateSource, ...],
    ) -> tuple[int, tuple[str, ...]]:
        selected: list[str] = []
        cursor: str | None = None
        as_of = 0
        while True:
            page = candidate_page(
                candidate_sources,
                ListCandidatesRequest(filters=filters, page_size=100, cursor=cursor),
            )
            as_of = page.as_of_completion_sequence
            selected.extend(
                candidate.observation_record_identifier for candidate in page.candidates
            )
            if page.next_cursor is None:
                return as_of, tuple(selected)
            cursor = page.next_cursor

    as_of, matching = matching_observations(sources)
    if selection_policy is ListingAnalysisSelectionPolicy.MISSING_FOR_CURRENT_EVIDENCE:
        eligible = matching
        reusable: tuple[str, ...] = ()
        policy_excluded: tuple[str, ...] = ()
    else:
        _, eligible = matching_observations(
            apply_listing_analysis_selection_policy(
                sources,
                selection_policy,
                product_guide_record_identifier=product_guide_record_identifier,
            )
        )
        eligible_set = set(eligible)
        reusable = (
            tuple(identifier for identifier in matching if identifier not in eligible_set)
            if selection_policy is ListingAnalysisSelectionPolicy.MISSING_FOR_SELECTED_GUIDE
            else ()
        )
        policy_excluded = (
            ()
            if selection_policy is ListingAnalysisSelectionPolicy.MISSING_FOR_SELECTED_GUIDE
            else tuple(identifier for identifier in matching if identifier not in eligible_set)
        )
    selected = eligible if maximum_items is None else eligible[:maximum_items]
    selected_set = set(selected)
    reusable_set = set(reusable)
    return ListingAnalysisBatchSelection(
        source_as_of_completion_sequence=as_of,
        matching_observation_record_identifiers=matching,
        eligible_observation_record_identifiers=eligible,
        selected_observation_record_identifiers=selected,
        reusable_observation_record_identifiers=reusable,
        policy_excluded_observation_record_identifiers=policy_excluded,
        excluded_observation_record_identifiers=tuple(
            identifier
            for identifier in matching
            if identifier not in selected_set and identifier not in reusable_set
        ),
    )


def request_missing_analyses_work_constraints() -> tuple[ConcurrencyConstraint, ...]:
    """Bound concurrent batch planning while analysis workers remain independently bounded."""

    return (
        ConcurrencyConstraint(
            identifier=("carl", "facebook", "analysis_batch", "work_concurrency", "v2"),
            subject_kind=SchedulingSubjectKind.WORK_ITEM,
            scope=SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND,
                identity=REQUEST_MISSING_ANALYSES_WORK_KIND,
            ),
            maximum_active=REQUEST_MISSING_ANALYSES_MAXIMUM_ACTIVE,
        ),
    )


def legacy_request_missing_analyses_constraint_identifiers() -> tuple[tuple[str, ...], ...]:
    return (("carl", "facebook", "analysis_batch", "work_concurrency", "v1"),)


class RequestMissingListingAnalysesRequest(StrictModel):
    """Select and durably request listing analyses after a completed refresh."""

    search_refresh_work_identifier: str = Field(min_length=1)
    product_guide_record_identifier: str = Field(min_length=1)
    selection_snapshot_record_identifier: str | None = Field(default=None, min_length=1)
    selection_policy: ListingAnalysisSelectionPolicy = Field(
        default=ListingAnalysisSelectionPolicy.MISSING_FOR_CURRENT_EVIDENCE,
        description=(
            "Use never_analyzed_listing to exclude a listing ID when any earlier observation "
            "has a completed analysis; missing_for_current_evidence may analyze fresher evidence."
        ),
    )
    filters: CandidateFilters = Field(
        default_factory=CandidateFilters,
        description=(
            "Optional candidate filters; leave filters.analysis as any because selection_policy "
            "defines the batch's analysis-history behavior."
        ),
    )
    maximum_items: int | None = Field(default=None, ge=1)
    allow_incomplete_gallery: bool = False

    @model_validator(mode="after")
    def validate_analysis_selection(self) -> RequestMissingListingAnalysesRequest:
        if self.filters.analysis is not CandidateAnalysisFilter.ANY:
            raise ValueError("Batch selection determines missing compatible analysis itself")
        if self.filters.product_guide_record_identifier is not None:
            raise ValueError("Select the batch product guide with its dedicated field")
        return self


class RequestMissingAnalysesPayload(StrictModel):
    request_identifier: str = Field(min_length=1)
    source_search_refresh_work_identifier: str = Field(min_length=1)
    source_search_refresh_work_identifiers: tuple[str, ...] = ()
    source_as_of_completion_sequence: int = Field(ge=0)
    listing_observation_record_identifiers: tuple[str, ...]
    product_guide_record_identifier: str = Field(min_length=1)
    selection_snapshot_record_identifier: str | None = Field(default=None, min_length=1)
    selection_policy: ListingAnalysisSelectionPolicy = (
        ListingAnalysisSelectionPolicy.MISSING_FOR_CURRENT_EVIDENCE
    )
    allow_incomplete_gallery: bool


class RequestMissingListingAnalysesResult(StrictModel):
    work_identifier: str
    created: bool
    selected_observation_count: int = Field(ge=0)
    source_search_refresh_work_identifier: str
    source_as_of_completion_sequence: int = Field(ge=0)


class MissingListingAnalysesPreview(StrictModel):
    source_search_refresh_work_identifier: str
    source_as_of_completion_sequence: int = Field(ge=0)
    selection_policy: ListingAnalysisSelectionPolicy
    matching_latest_observations: int = Field(ge=0)
    excluded_by_selection_policy: int = Field(ge=0)
    eligible_latest_observations: int = Field(ge=0)
    selected_observations: int = Field(ge=0)


class WorkspaceAnalysisSelection(StrictModel):
    kind: Literal["workspace"] = "workspace"


class SelectionSnapshotAnalysisSelection(StrictModel):
    kind: Literal["selection_snapshot"] = "selection_snapshot"
    selection_snapshot_record_identifier: str = Field(min_length=1)


AnalysisSelectionSource = Annotated[
    WorkspaceAnalysisSelection
    | ListingIdsSelection
    | WorksetSelection
    | ReviewBatchSelection
    | SelectionSnapshotAnalysisSelection,
    Field(discriminator="kind"),
]


class SelectionAnalysesRequest(StrictModel):
    """Plan analyses from one workspace-native durable or explicit selection."""

    workspace_record_identifier: str = Field(min_length=1)
    product_guide_binding_identifier: str | None = Field(default=None, min_length=1)
    product_guide_record_identifier: str | None = Field(default=None, min_length=1)
    selection: AnalysisSelectionSource = WorkspaceAnalysisSelection()
    statuses: Sequence[ListingStatus] = Field(
        default=(ListingStatus.AVAILABLE,), min_length=1, max_length=5
    )
    selection_policy: ListingAnalysisSelectionPolicy = Field(
        default=ListingAnalysisSelectionPolicy.MISSING_FOR_CURRENT_EVIDENCE,
        description=(
            "Use missing_for_selected_guide to reuse a completed analysis under the exact "
            "selected guide across retained observations. Use never_analyzed_listing to exclude "
            "a listing ID when any earlier observation has a completed analysis; "
            "missing_for_current_evidence may analyze fresher evidence."
        ),
    )
    maximum_items: int | None = Field(default=None, ge=1)
    maximum_candidate_listings_examined: int = Field(default=2_500, ge=1, le=10_000)
    allow_incomplete_gallery: bool = False

    @model_validator(mode="after")
    def validate_guide_selection(self) -> SelectionAnalysesRequest:
        if (
            self.product_guide_binding_identifier is not None
            and self.product_guide_record_identifier is not None
        ):
            raise ValueError(
                "Select a product guide by binding identifier or exact record identifier, not both"
            )
        return self

    @field_validator("statuses")
    @classmethod
    def validate_unique_statuses(cls, value: Sequence[ListingStatus]) -> Sequence[ListingStatus]:
        if len(set(value)) != len(value):
            raise ValueError("Analysis status filters must be unique")
        return value


class SelectionAnalysesPreview(StrictModel):
    workspace_record_identifier: str
    product_guide_binding_identifier: str
    source_search_run_record_identifier: str
    source_search_run_record_identifiers: tuple[str, ...]
    source_search_refresh_work_identifier: str
    source_search_refresh_work_identifiers: tuple[str, ...]
    product_guide_record_identifier: str
    selection_kind: str
    source_as_of_completion_sequence: int = Field(ge=0)
    selection_policy: ListingAnalysisSelectionPolicy
    candidate_listings_examined: int = Field(ge=0, le=10_000)
    candidate_examination_limit_reached: bool
    matching_latest_observations: int = Field(ge=0)
    excluded_by_selection_policy: int = Field(ge=0)
    eligible_latest_observations: int = Field(ge=0)
    selected_observations: int = Field(ge=0)
    new_analysis_count: int = Field(ge=0)
    reused_analysis_count: int = Field(ge=0)
    excluded_observation_count: int = Field(ge=0)


class SelectionAnalysesRequestResult(StrictModel):
    work_identifier: str
    created: bool
    workspace_record_identifier: str
    product_guide_binding_identifier: str
    source_search_run_record_identifier: str
    source_search_run_record_identifiers: tuple[str, ...]
    source_search_refresh_work_identifier: str
    source_search_refresh_work_identifiers: tuple[str, ...]
    product_guide_record_identifier: str
    selection_kind: str
    candidate_listings_examined: int = Field(ge=0, le=10_000)
    candidate_examination_limit_reached: bool
    selected_observation_count: int = Field(ge=0)
    source_as_of_completion_sequence: int = Field(ge=0)


def request_missing_analyses_work(
    *, identifier: str, payload: RequestMissingAnalysesPayload
) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=REQUEST_MISSING_ANALYSES_WORK_KIND,
        payload_schema_version=REQUEST_MISSING_ANALYSES_PAYLOAD_SCHEMA_VERSION,
        payload=payload.model_dump(mode="json"),
        deduplication_identity=(
            "facebook_marketplace",
            "request_missing_listing_analyses",
            payload.request_identifier,
        ),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND,
                identity=REQUEST_MISSING_ANALYSES_WORK_KIND,
            ),
        ),
    )
