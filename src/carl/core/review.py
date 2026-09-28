"""Pure models and selection rules for interactive review of retained evidence."""

from __future__ import annotations

import base64
from collections.abc import Sequence
from decimal import Decimal, InvalidOperation

from pydantic import Field, field_validator, model_validator

from carl.core.json import decode_json, encode_json
from carl.core.models import (
    CodeProvenance,
    JsonStringEnumeration,
    JsonValue,
    StrictModel,
)
from carl.core.work import WorkEventKind, WorkState


class CandidateAvailability(JsonStringEnumeration):
    FULL_LISTING = "full_listing"
    PENDING = "pending"
    SOLD = "sold"
    LISTING_UNAVAILABLE = "listing_unavailable"


class CandidateAnalysisFilter(JsonStringEnumeration):
    ANY = "any"
    PRESENT = "present"
    ABSENT = "absent"


class CandidateCursor(StrictModel):
    """Opaque traversal position within an immutable completion-sequence boundary."""

    as_of_completion_sequence: int = Field(ge=0)
    after_acquisition_completion_sequence: int | None = Field(default=None, ge=0)
    after_listing_identifier: str | None = None

    @model_validator(mode="after")
    def validate_position(self) -> CandidateCursor:
        if (self.after_acquisition_completion_sequence is None) != (
            self.after_listing_identifier is None
        ):
            raise ValueError("Candidate cursor position must be entirely present or absent")
        return self


def encode_candidate_cursor(cursor: CandidateCursor) -> str:
    """Serialize a validated cursor as an opaque URL-safe boundary value."""

    content = encode_json(cursor.model_dump(mode="json")).encode("utf-8")
    return base64.urlsafe_b64encode(content).decode("ascii").rstrip("=")


def decode_candidate_cursor(value: str) -> CandidateCursor:
    """Validate an opaque candidate cursor without accepting ambiguous encodings."""

    if not value or any(character.isspace() for character in value):
        raise ValueError("Candidate cursor must be nonempty URL-safe base64")
    try:
        raw = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
        decoded = decode_json(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise ValueError("Candidate cursor is malformed") from error
    return CandidateCursor.model_validate(decoded)


class CandidateFilters(StrictModel):
    availabilities: Sequence[CandidateAvailability] = (CandidateAvailability.FULL_LISTING,)
    analysis: CandidateAnalysisFilter = CandidateAnalysisFilter.ANY
    product_guide_record_identifier: str | None = None
    currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    minimum_price: Decimal | None = Field(default=None, ge=0)
    maximum_price: Decimal | None = Field(default=None, ge=0)
    text: str | None = None

    @field_validator("minimum_price", "maximum_price", mode="before")
    @classmethod
    def parse_json_price(cls, value: object) -> object:
        """Convert the JSON scalar forms advertised by the generated schema."""

        if value is None or isinstance(value, (Decimal, bool)):
            return value
        if isinstance(value, str) and (not value or value != value.strip()):
            raise ValueError("Candidate price must be a nonempty trimmed decimal")
        if isinstance(value, (str, int, float)):
            try:
                parsed = Decimal(str(value))
            except InvalidOperation as error:
                raise ValueError("Candidate price must be a decimal value") from error
            if not parsed.is_finite():
                raise ValueError("Candidate price must be finite")
            return parsed
        return value

    @field_validator("availabilities")
    @classmethod
    def validate_availabilities(
        cls, value: Sequence[CandidateAvailability]
    ) -> Sequence[CandidateAvailability]:
        if not value or len(set(value)) != len(value):
            raise ValueError("Candidate availability filters must be nonempty and unique")
        return value

    @field_validator("text")
    @classmethod
    def validate_text(cls, value: str | None) -> str | None:
        if value is not None and (not value or value != value.strip()):
            raise ValueError("Candidate text filter must be nonempty and trimmed")
        return value

    @model_validator(mode="after")
    def validate_price_range(self) -> CandidateFilters:
        if (
            self.minimum_price is not None
            and self.maximum_price is not None
            and self.maximum_price < self.minimum_price
        ):
            raise ValueError("Candidate maximum price is below its minimum")
        if (self.minimum_price is not None or self.maximum_price is not None) and (
            self.currency is None
        ):
            raise ValueError("Candidate price bounds require a currency")
        return self


class ListCandidatesRequest(StrictModel):
    filters: CandidateFilters = CandidateFilters()
    page_size: int = Field(default=25, ge=1, le=100)
    cursor: str | None = None


class AnalysisDescriptor(StrictModel):
    analysis_record_identifier: str
    evidence_set_record_identifier: str
    listing_observation_record_identifier: str
    product_guide_record_identifier: str
    completion_sequence: int = Field(ge=0)
    completed_at_utc: str | None
    state: str
    warnings: tuple[str, ...]
    model: str | None


class CandidateSource(StrictModel):
    listing_identifier: str
    observation_record_identifier: str
    acquisition_record_identifier: str
    availability: CandidateAvailability
    acquisition_completion_sequence: int = Field(ge=0)
    observation_completion_sequence: int = Field(ge=0)
    observation: JsonValue
    analyses: tuple[AnalysisDescriptor, ...] = ()


class CandidateSummary(StrictModel):
    listing_identifier: str
    observation_record_identifier: str
    acquisition_record_identifier: str
    availability: CandidateAvailability
    acquisition_completion_sequence: int = Field(ge=0)
    observation_completion_sequence: int = Field(ge=0)
    title: JsonValue
    price: JsonValue
    location: JsonValue
    published_at: JsonValue
    completed_analysis_count: int = Field(ge=0)
    product_guide_record_identifiers: tuple[str, ...]
    analyses: tuple[AnalysisDescriptor, ...]


class CandidatePage(StrictModel):
    as_of_completion_sequence: int = Field(ge=0)
    candidates: tuple[CandidateSummary, ...]
    next_cursor: str | None


def _field_value(observation: JsonValue, name: str) -> JsonValue:
    if not isinstance(observation, dict):
        return None
    fields = observation.get("fields")
    if not isinstance(fields, dict):
        return None
    field = fields.get(name)
    if not isinstance(field, dict):
        return None
    evidence = field.get("evidence")
    if not isinstance(evidence, list):
        return None
    for item in evidence:
        if isinstance(item, dict) and item.get("state") == "present":
            if "normalized" in item:
                return item["normalized"]
            if "original" in item:
                return item["original"]
    return None


def candidate_availability(
    observation: JsonValue, response_classification: str
) -> CandidateAvailability:
    """Separate Marketplace sale state from the page-response classification."""

    classified = CandidateAvailability(response_classification)
    if classified is not CandidateAvailability.FULL_LISTING:
        return classified
    if _field_value(observation, "availability_sold") is True:
        return CandidateAvailability.SOLD
    if _field_value(observation, "availability_pending") is True:
        return CandidateAvailability.PENDING
    return classified


def listing_analysis_history(sources: Sequence[CandidateSource]) -> tuple[AnalysisDescriptor, ...]:
    """Combine completed analyses across a listing's retained observations."""

    by_identifier: dict[str, AnalysisDescriptor] = {}
    for source in sources:
        for analysis in source.analyses:
            previous = by_identifier.setdefault(analysis.analysis_record_identifier, analysis)
            if previous != analysis:
                raise ValueError("Conflicting descriptors exist for one analysis record")
    return tuple(
        sorted(
            by_identifier.values(),
            key=lambda analysis: (
                -analysis.completion_sequence,
                analysis.analysis_record_identifier,
            ),
        )
    )


def _matches_price(source: CandidateSource, filters: CandidateFilters) -> bool:
    if filters.currency is None:
        return True
    price = _field_value(source.observation, "price")
    if not isinstance(price, dict) or price.get("currency") != filters.currency:
        return False
    raw_amount = price.get("amount_decimal")
    try:
        amount = Decimal(raw_amount) if isinstance(raw_amount, str) else None
    except InvalidOperation:
        return False
    if amount is None or not amount.is_finite():
        return False
    if filters.minimum_price is not None and amount < filters.minimum_price:
        return False
    return filters.maximum_price is None or amount <= filters.maximum_price


def _matches_candidate(source: CandidateSource, filters: CandidateFilters) -> bool:
    if source.availability not in filters.availabilities:
        return False
    if filters.analysis is CandidateAnalysisFilter.PRESENT and not source.analyses:
        return False
    if filters.analysis is CandidateAnalysisFilter.ABSENT and source.analyses:
        return False
    if filters.product_guide_record_identifier is not None and not any(
        analysis.product_guide_record_identifier == filters.product_guide_record_identifier
        for analysis in source.analyses
    ):
        return False
    if not _matches_price(source, filters):
        return False
    if filters.text is not None:
        needle = filters.text.casefold()
        values = (
            _field_value(source.observation, "title"),
            _field_value(source.observation, "description"),
        )
        if not any(isinstance(value, str) and needle in value.casefold() for value in values):
            return False
    return True


def _summary(source: CandidateSource) -> CandidateSummary:
    return CandidateSummary(
        listing_identifier=source.listing_identifier,
        observation_record_identifier=source.observation_record_identifier,
        acquisition_record_identifier=source.acquisition_record_identifier,
        availability=source.availability,
        acquisition_completion_sequence=source.acquisition_completion_sequence,
        observation_completion_sequence=source.observation_completion_sequence,
        title=_field_value(source.observation, "title"),
        price=_field_value(source.observation, "price"),
        location=_field_value(source.observation, "location_text"),
        published_at=_field_value(source.observation, "published_at"),
        completed_analysis_count=len(source.analyses),
        product_guide_record_identifiers=tuple(
            dict.fromkeys(analysis.product_guide_record_identifier for analysis in source.analyses)
        ),
        analyses=source.analyses,
    )


def candidate_page(
    sources: tuple[CandidateSource, ...], request: ListCandidatesRequest
) -> CandidatePage:
    """Filter and paginate latest observations at one durable snapshot boundary."""

    cursor = decode_candidate_cursor(request.cursor) if request.cursor is not None else None
    maximum_sequence = max(
        (source.observation_completion_sequence for source in sources), default=0
    )
    as_of = cursor.as_of_completion_sequence if cursor is not None else maximum_sequence
    eligible_sources = tuple(
        source for source in sources if source.observation_completion_sequence <= as_of
    )
    latest_by_listing: dict[str, CandidateSource] = {}
    sources_by_listing: dict[str, list[CandidateSource]] = {}
    for source in eligible_sources:
        sources_by_listing.setdefault(source.listing_identifier, []).append(source)
        previous = latest_by_listing.get(source.listing_identifier)
        if previous is None or (
            source.acquisition_completion_sequence,
            source.observation_completion_sequence,
            source.observation_record_identifier,
        ) > (
            previous.acquisition_completion_sequence,
            previous.observation_completion_sequence,
            previous.observation_record_identifier,
        ):
            latest_by_listing[source.listing_identifier] = source
    latest_by_listing = {
        listing_identifier: source.model_copy(
            update={"analyses": listing_analysis_history(sources_by_listing[listing_identifier])}
        )
        for listing_identifier, source in latest_by_listing.items()
    }
    ordered = sorted(
        (
            source
            for source in latest_by_listing.values()
            if _matches_candidate(source, request.filters)
        ),
        key=lambda source: (-source.acquisition_completion_sequence, source.listing_identifier),
    )
    if cursor is not None and cursor.after_acquisition_completion_sequence is not None:
        position = (
            -cursor.after_acquisition_completion_sequence,
            cursor.after_listing_identifier or "",
        )
        ordered = [
            source
            for source in ordered
            if (-source.acquisition_completion_sequence, source.listing_identifier) > position
        ]
    selected = ordered[: request.page_size]
    next_cursor = None
    if len(ordered) > len(selected) and selected:
        last = selected[-1]
        next_cursor = encode_candidate_cursor(
            CandidateCursor(
                as_of_completion_sequence=as_of,
                after_acquisition_completion_sequence=(last.acquisition_completion_sequence),
                after_listing_identifier=last.listing_identifier,
            )
        )
    return CandidatePage(
        as_of_completion_sequence=as_of,
        candidates=tuple(_summary(source) for source in selected),
        next_cursor=next_cursor,
    )


class ProductGuideSummary(StrictModel):
    record_identifier: str
    identity: tuple[str, ...]
    display_name: str | None
    version: int = Field(ge=1)
    previous_record_identifier: str | None
    retired: bool = False


class ProductGuideDetails(ProductGuideSummary):
    text: str
    text_artifact_identifier: str


def authored_product_guide(
    *, identity: tuple[str, ...], display_name: str, version: int
) -> dict[str, JsonValue]:
    """Construct the normalized record value produced by guide authoring."""

    return {
        "identity": list(identity),
        "display_name": display_name,
        "version": version,
    }


class CreateProductGuideRequest(StrictModel):
    identity: Sequence[str] = Field(
        min_length=1,
        description=(
            "Caller-owned identity suffix only; Carl supplies the leading carl/product_guide "
            "namespace. An already-prefixed value is normalized for compatibility."
        ),
    )
    display_name: str = Field(min_length=1)
    text: str = Field(min_length=1)

    @field_validator("identity", mode="before")
    @classmethod
    def normalize_identity(cls, value: object) -> object:
        if not isinstance(value, (list, tuple)):
            return value
        parts = list(value)
        while parts[:2] == ["carl", "product_guide"]:
            parts = parts[2:]
        if not parts:
            raise ValueError("Product guide identity requires a suffix after Carl's namespace")
        if any(
            not isinstance(part, str) or not part or part != part.strip()
            for part in parts
        ):
            raise ValueError("Product guide identity parts must be nonempty and trimmed")
        return parts

    @field_validator("display_name", "text")
    @classmethod
    def validate_trimmed(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Product guide text values must be trimmed")
        return value

    @property
    def full_identity(self) -> tuple[str, ...]:
        return ("carl", "product_guide", *self.identity)


class ReviseProductGuideRequest(StrictModel):
    expected_base_record_identifier: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    text: str = Field(min_length=1)

    @field_validator("display_name", "text")
    @classmethod
    def validate_trimmed(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Product guide text values must be trimmed")
        return value


class SetProductGuideIdentityRetiredRequest(StrictModel):
    product_guide_record_identifier: str = Field(
        min_length=1,
        description="Any exact version belonging to the guide identity to retire or restore.",
    )
    retired: bool = True


class ProductGuideIdentityStateRecord(StrictModel):
    record_identifier: str = Field(min_length=1)
    product_guide_identity: tuple[str, ...] = Field(min_length=1)
    retired: bool
    recorded_at_utc: str


class ProductGuideConflict(StrictModel):
    identity: tuple[str, ...]
    expected_base_record_identifier: str | None
    current_record_identifier: str
    current_version: int = Field(ge=1)


class ProductGuideMutationResult(StrictModel):
    product_guide: ProductGuideDetails | None = None
    conflict: ProductGuideConflict | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> ProductGuideMutationResult:
        if (self.product_guide is None) == (self.conflict is None):
            raise ValueError("A product-guide mutation must have exactly one outcome")
        return self


class ProvenanceObject(StrictModel):
    object_identifier: str
    producing_operation: JsonValue
    inputs: JsonValue
    output_edge_count: int = Field(ge=0)
    output_edges_truncated: bool
    outputs: JsonValue


class GalleryImageDescriptor(StrictModel):
    gallery_order: int = Field(ge=0)
    gallery_reference_record_identifier: str | None
    original_url: str
    photo_identifier: str | None
    declared_width: int | None
    declared_height: int | None
    image_result_record_identifier: str | None
    image_artifact_identifier: str | None
    download_state: str
    sha256: str | None
    media_type: str | None
    width: int | None
    height: int | None


class ListingDossier(StrictModel):
    listing_identifier: str
    selected_observation_record_identifier: str
    selected_acquisition_record_identifier: str
    availability: CandidateAvailability
    acquisition_completion_sequence: int = Field(ge=0)
    observation_completion_sequence: int = Field(ge=0)
    fields: JsonValue
    gallery: tuple[GalleryImageDescriptor, ...]
    analyses: tuple[AnalysisDescriptor, ...]
    older_observation_record_identifiers: tuple[str, ...]


class AnalysisReport(StrictModel):
    descriptor: AnalysisDescriptor
    text: str | None
    limit_observations: JsonValue
    claude: JsonValue


class RequestAnalysisRequest(StrictModel):
    listing_observation_record_identifier: str = Field(min_length=1)
    product_guide_record_identifier: str = Field(min_length=1)
    allow_incomplete_gallery: bool = False


class RequestAnalysisResult(StrictModel):
    work_identifier: str
    created: bool
    evidence_set_record_identifier: str
    included_gallery_count: int = Field(ge=0)
    unavailable_gallery_orders: tuple[int, ...]
    gallery_absence_reason: str | None


class WorkOperationSummary(StrictModel):
    operation_identifier: str
    attempt: int = Field(ge=1)


class WorkGroupProgress(StrictModel):
    total: int = Field(ge=0)
    pending: int = Field(ge=0)
    leased: int = Field(ge=0)
    completed: int = Field(ge=0)
    terminal_failure: int = Field(ge=0)


class SearchRefreshPhase(JsonStringEnumeration):
    SEARCH = "search"
    STARTING_ITEM_COLLECTION = "starting_item_collection"
    COLLECTING_ITEM_PAGES = "collecting_item_pages"
    EXTRACTING_ITEM_PAGES = "extracting_item_pages"
    STARTING_IMAGE_COLLECTION = "starting_image_collection"
    COLLECTING_IMAGES = "collecting_images"
    EXTRACTING_IMAGES = "extracting_images"
    COMPLETED = "completed"
    FAILED = "failed"


class SearchRefreshProgress(StrictModel):
    checkpoint_stage: str | None
    active_phase: SearchRefreshPhase
    item_pages: WorkGroupProgress
    item_extractions: WorkGroupProgress
    images: WorkGroupProgress
    image_extractions: WorkGroupProgress


class WorkFailureReasonCount(StrictModel):
    kind: str = Field(min_length=1)
    count: int = Field(ge=1)


class WorkFailureSummary(StrictModel):
    work_identifier: str = Field(min_length=1)
    kind: str = Field(min_length=1)


class AnalysisBatchProgress(StrictModel):
    selected_observations: int = Field(ge=0)
    processed_observations: int = Field(ge=0)
    remaining_observations: int = Field(ge=0)
    checkpoint_processed_observations: int = Field(ge=0)
    observed_request_edges: int = Field(ge=0)
    newly_created_work: int = Field(ge=0)
    reused_work: int = Field(ge=0)
    skipped_observations: int = Field(ge=0)
    analyses: WorkGroupProgress
    active_analysis_work_count: int = Field(ge=0)
    active_analysis_work_identifiers: tuple[str, ...] = Field(max_length=10)
    active_analysis_work_identifiers_truncated: bool
    failure_reason_counts: tuple[WorkFailureReasonCount, ...]
    recent_terminal_failures: tuple[WorkFailureSummary, ...] = Field(max_length=10)


class WorkRuntimeStatus(StrictModel):
    observed_at_utc_ns: int = Field(ge=0)
    created_at_utc_ns: int = Field(ge=0)
    eligible_at_utc_ns: int = Field(ge=0)
    worker_identifier: str | None
    lease_expires_at_utc_ns: int | None = Field(default=None, ge=0)
    lease_remaining_ns: int | None = None
    latest_event_sequence: int = Field(ge=1)
    latest_event_kind: WorkEventKind
    latest_event_at_utc_ns: int = Field(ge=0)
    latest_event_age_ns: int = Field(ge=0)
    last_lease_activity_at_utc_ns: int | None = Field(default=None, ge=0)
    lease_activity_age_ns: int | None = Field(default=None, ge=0)


class WorkStatusDetails(StrictModel):
    payload: JsonValue
    result: JsonValue | None
    error: JsonValue | None
    recent_operations: tuple[WorkOperationSummary, ...]


class WorkStatus(StrictModel):
    identifier: str
    kind: tuple[str, ...]
    payload_schema_version: int = Field(ge=1)
    state: WorkState
    attempt: int = Field(ge=0)
    stage: str | None
    error_kind: str | None
    operation_count: int = Field(ge=0)
    runtime: WorkRuntimeStatus
    search_refresh_progress: SearchRefreshProgress | None = None
    analysis_batch_progress: AnalysisBatchProgress | None = None
    details: WorkStatusDetails | None = None


class ServerCapability(StrictModel):
    identity: tuple[str, ...]
    version: int = Field(ge=1)


class ServerInfo(StrictModel):
    instance_identifier: str
    started_at_utc_ns: int
    process_identifier: int = Field(ge=1)
    repository_root: str
    database_path: str
    code_provenance: CodeProvenance
    source_tree_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    capabilities: tuple[ServerCapability, ...]


def work_group_progress(states: tuple[WorkState, ...]) -> WorkGroupProgress:
    """Summarize child work without losing the durable state distinctions."""

    counts = {state: states.count(state) for state in WorkState}
    return WorkGroupProgress(
        total=len(states),
        pending=counts[WorkState.PENDING],
        leased=counts[WorkState.LEASED],
        completed=counts[WorkState.COMPLETED],
        terminal_failure=counts[WorkState.TERMINAL_FAILURE],
    )


def search_refresh_phase(
    *,
    state: WorkState,
    checkpoint_stage: str | None,
    item_pages: WorkGroupProgress,
    item_extractions: WorkGroupProgress,
    images: WorkGroupProgress,
    image_extractions: WorkGroupProgress,
) -> SearchRefreshPhase:
    """Describe active refresh execution separately from its last checkpoint."""

    if state is WorkState.COMPLETED:
        return SearchRefreshPhase.COMPLETED
    if state is WorkState.TERMINAL_FAILURE:
        return SearchRefreshPhase.FAILED
    if checkpoint_stage in {None, "collecting_search"}:
        return SearchRefreshPhase.SEARCH
    if checkpoint_stage in {"search_complete", "collecting_items", "extracting_items"}:
        if item_pages.total == 0:
            return SearchRefreshPhase.STARTING_ITEM_COLLECTION
        if item_pages.pending + item_pages.leased > 0:
            return SearchRefreshPhase.COLLECTING_ITEM_PAGES
        if item_extractions.pending + item_extractions.leased > 0:
            return SearchRefreshPhase.EXTRACTING_ITEM_PAGES
        return SearchRefreshPhase.STARTING_IMAGE_COLLECTION
    if image_extractions.pending + image_extractions.leased > 0:
        return SearchRefreshPhase.EXTRACTING_IMAGES
    if images.total > 0 or checkpoint_stage in {
        "items_complete",
        "extracting_resumed_images",
        "collecting_images",
        "extracting_images",
    }:
        return SearchRefreshPhase.COLLECTING_IMAGES
    return SearchRefreshPhase.SEARCH
