"""Immutable source-neutral intents and work contracts for listing processing."""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema

from carl.core.analysis_batch import ListingAnalysisSelectionPolicy
from carl.core.components import Component, ComponentId, Registry
from carl.core.composed_projection import ListingStatus
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    Marketplace,
    SearchTargetSpecification,
)
from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel
from carl.core.network_defaults import (
    DEFAULT_DATACENTER_NETWORK_PATH,
    AcquisitionNetworkPath,
    LegacyProtonRoute,
    populate_legacy_proton_paths,
    validate_legacy_proton_paths,
)
from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScope,
    SchedulingScopeKind,
    WorkDefinition,
    WorkState,
)

SEARCH_PIPELINE_INTENT_KIND = ("carl", "marketplace", "search_pipeline_intent")
SEARCH_PIPELINE_WORK_KIND = ("carl", "marketplace", "work", "search_pipeline")
LISTING_PIPELINE_WORK_KIND = ("carl", "marketplace", "work", "listing_pipeline")
REQUEST_SEARCH_PIPELINE_PAYLOAD_SCHEMA_VERSION = 1
PIPELINE_LISTING_PAYLOAD_SCHEMA_VERSION = 1
REQUEST_SEARCH_PIPELINE = ComponentId(("carl", "marketplace", "request", "search_pipeline"))
RUN_SEARCH_PIPELINE = ComponentId(("carl", "marketplace", "run", "search_pipeline"))
RUN_LISTING_PIPELINE = ComponentId(("carl", "marketplace", "run", "listing_pipeline"))


def pipeline_collection_failed(value: JsonValue | None) -> bool:
    """Recognize retained failed collections, including legacy completed work."""

    if not isinstance(value, dict):
        return False
    classification = value.get("response_classification")
    kind = classification.get("kind") if isinstance(classification, dict) else None
    return (
        kind in ("challenge", "error_page", "http_error", "unrecognized")
        or value.get("state")
        in ("response_failed", "terminal_failure", "acquisition_failed", "completed_with_failures")
        or value.get("stopping_reason")
        in ("invalid_next_page", "unrecognized_response", "challenge", "error_page", "http_error")
    )


class PipelineStage(JsonStringEnumeration):
    DETAILS = "details"
    IMAGES = "images"
    ANALYSIS = "analysis"


class PipelineOptions(StrictModel):
    """Explicit processing stages, evidence reuse, and request-wide spending bounds."""

    stop_after: PipelineStage = PipelineStage.ANALYSIS
    product_guide_record_identifier: str | None = Field(default=None, min_length=1)
    maximum_items: int = Field(default=100, ge=1, le=1_000)
    maximum_images_per_listing: int = Field(default=20, ge=0, le=50)
    maximum_images: int = Field(
        default=2_000,
        ge=0,
        le=50_000,
        description="Global image reservation budget shared by every selected listing and source.",
    )
    maximum_analyses: int = Field(default=100, ge=0, le=1_000)
    maximum_inflight_listings: int = Field(default=25, ge=1, le=100)
    maximum_candidate_listings_examined: int = Field(default=10_000, ge=1, le=100_000)
    title_contains: str | None = Field(default=None, min_length=1, max_length=500)
    exclude_title_keywords: tuple[str, ...] = Field(default=(), max_length=100)
    statuses: tuple[ListingStatus, ...] = Field(
        default=(ListingStatus.AVAILABLE,), min_length=1, max_length=5
    )
    selection_policy: ListingAnalysisSelectionPolicy = (
        ListingAnalysisSelectionPolicy.MISSING_FOR_CURRENT_EVIDENCE
    )
    allow_incomplete_gallery: bool = False
    refresh_details: bool = False
    ebay_stack_identifier: str = Field(
        default="ebay_anonymous", pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"
    )
    facebook_decodo_route: str = Field(default="carl", min_length=1)
    facebook_image_network_path: AcquisitionNetworkPath = Field(
        default=DEFAULT_DATACENTER_NETWORK_PATH,
        description="Facebook gallery route; defaults directly to Decodo datacenter.",
    )
    facebook_image_route: SkipJsonSchema[LegacyProtonRoute | None] = Field(
        default=None, exclude=True
    )

    @model_validator(mode="before")
    @classmethod
    def populate_legacy_route(cls, value: object) -> object:
        return populate_legacy_proton_paths(
            value,
            legacy_field="facebook_image_route",
            network_fields=("facebook_image_network_path",),
        )

    @model_validator(mode="after")
    def validate_legacy_route(self) -> PipelineOptions:
        validate_legacy_proton_paths(self.facebook_image_route, self.facebook_image_network_path)
        return self

    @property
    def requested_facebook_image_network_path(self) -> tuple[str, ...]:
        return self.facebook_image_network_path

    @field_validator("exclude_title_keywords", "statuses", mode="before")
    @classmethod
    def parse_json_sequences(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def validate_analysis_guide(self) -> PipelineOptions:
        if (
            self.stop_after is PipelineStage.ANALYSIS
            and self.product_guide_record_identifier is None
        ):
            raise ValueError(
                "Analysis processing requires an exact product guide record identifier"
            )
        return self

    @field_validator(
        "product_guide_record_identifier",
        "title_contains",
        "facebook_decodo_route",
    )
    @classmethod
    def validate_trimmed_values(cls, value: str | None) -> str | None:
        if value is not None and value != value.strip():
            raise ValueError("Pipeline identifiers and title filters must be trimmed")
        return value

    @field_validator("exclude_title_keywords")
    @classmethod
    def validate_keywords(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not keyword or keyword != keyword.strip() for keyword in value):
            raise ValueError("Excluded title keywords must be nonempty and trimmed")
        if len({keyword.casefold() for keyword in value}) != len(value):
            raise ValueError("Excluded title keywords must be unique ignoring case")
        return value

    @field_validator("statuses")
    @classmethod
    def validate_statuses(cls, value: tuple[ListingStatus, ...]) -> tuple[ListingStatus, ...]:
        if len(set(value)) != len(value):
            raise ValueError("Pipeline status filters must be unique")
        return value


class NewSearchPipelineSource(StrictModel):
    kind: Literal["new_search"] = "new_search"
    targets: tuple[SearchTargetSpecification, ...] = Field(min_length=1, max_length=20)

    @field_validator("targets", mode="before")
    @classmethod
    def parse_json_targets(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("targets")
    @classmethod
    def validate_targets(
        cls, value: tuple[SearchTargetSpecification, ...]
    ) -> tuple[SearchTargetSpecification, ...]:
        CreateMarketplaceSearchRequest(targets=value)
        return value


class SearchPipelineSource(StrictModel):
    kind: Literal["search"] = "search"
    search_record_identifier: str = Field(min_length=1)


class WorkspacePipelineSource(StrictModel):
    kind: Literal["workspace"] = "workspace"
    workspace_record_identifier: str = Field(min_length=1)
    track_identifiers: tuple[str, ...] | None = Field(default=None, min_length=1, max_length=20)

    @field_validator("track_identifiers", mode="before")
    @classmethod
    def parse_json_tracks(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("track_identifiers")
    @classmethod
    def validate_tracks(cls, value: tuple[str, ...] | None) -> tuple[str, ...] | None:
        if value is not None:
            _validate_identifiers(value)
        return value


class SearchWorkPipelineSource(StrictModel):
    kind: Literal["search_work"] = "search_work"
    search_work_identifiers: tuple[str, ...] = Field(min_length=1, max_length=100)

    @field_validator("search_work_identifiers", mode="before")
    @classmethod
    def parse_json_identifiers(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("search_work_identifiers")
    @classmethod
    def validate_work_identifiers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_identifiers(value)


type SearchPipelineSourceSpecification = Annotated[
    NewSearchPipelineSource
    | SearchPipelineSource
    | WorkspacePipelineSource
    | SearchWorkPipelineSource,
    Field(discriminator="kind"),
]


def _validate_identifiers(value: tuple[str, ...]) -> tuple[str, ...]:
    if any(not identifier or identifier != identifier.strip() for identifier in value):
        raise ValueError("Pipeline scope identifiers must be nonempty and trimmed")
    if len(set(value)) != len(value):
        raise ValueError("Pipeline scope identifiers must be unique")
    return value


class RequestSearchPipelineRequest(StrictModel):
    request_identifier: str = Field(min_length=1, max_length=200)
    source: SearchPipelineSourceSpecification
    options: PipelineOptions

    @field_validator("request_identifier")
    @classmethod
    def validate_request_identifier(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Pipeline request identifier must be trimmed")
        return value


class RequestSearchPipelinePayload(StrictModel):
    request_identifier: str = Field(min_length=1, max_length=200)
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    intent_record_identifier: str = Field(min_length=1)
    search_work_identifiers: tuple[str, ...]
    search_run_record_identifiers: tuple[str, ...] = ()
    search_record_identifier: str | None = Field(default=None, min_length=1)
    workspace_record_identifier: str | None = Field(default=None, min_length=1)
    options: PipelineOptions


class PipelineListingPayload(StrictModel):
    root_work_identifier: str = Field(min_length=1)
    marketplace: Marketplace
    external_identifier: str = Field(min_length=1)
    occurrence_record_identifier: str = Field(min_length=1)
    maximum_images: int = Field(ge=0, le=50)
    analysis_authorized: bool
    options: PipelineOptions

    @model_validator(mode="after")
    def validate_allocations(self) -> PipelineListingPayload:
        if self.maximum_images > self.options.maximum_images_per_listing:
            raise ValueError("Listing image allocation exceeds the per-listing budget")
        if self.maximum_images > self.options.maximum_images:
            raise ValueError("Listing image allocation exceeds the global image budget")
        if self.analysis_authorized and (
            self.options.stop_after is not PipelineStage.ANALYSIS
            or self.options.maximum_analyses == 0
        ):
            raise ValueError(
                "Listing analysis must be authorized by the processing stage and budget"
            )
        return self


class SearchPipelineRequestResult(StrictModel):
    work_identifier: str
    created: bool
    search_record_identifier: str | None = None
    workspace_record_identifier: str | None = None
    search_work_identifiers: tuple[str, ...] = ()
    search_run_record_identifiers: tuple[str, ...] = ()
    product_guide_record_identifier: str | None = None


class PipelineListingProgress(StrictModel):
    work_identifier: str
    marketplace: Marketplace
    external_identifier: str
    stage: str
    state: WorkState
    observation_record_identifier: str | None = None
    analysis_work_identifier: str | None = None
    reason: JsonValue = None


class SearchPipelineStatus(StrictModel):
    work_identifier: str
    state: WorkState
    options: PipelineOptions
    search_work_identifiers: tuple[str, ...] = ()
    search_run_record_identifiers: tuple[str, ...] = ()
    searches_pending: int = Field(default=0, ge=0)
    searches_completed: int = Field(default=0, ge=0)
    searches_failed: int = Field(default=0, ge=0)
    candidates_examined: int = Field(default=0, ge=0)
    selected_listings: int = Field(default=0, ge=0)
    items_completed: int = Field(default=0, ge=0)
    galleries_completed: int = Field(default=0, ge=0)
    analyses_completed: int = Field(default=0, ge=0)
    analyses_reused: int = Field(default=0, ge=0)
    skipped_listings: int = Field(default=0, ge=0)
    failed_listings: int = Field(default=0, ge=0)
    image_budget_reserved: int = Field(default=0, ge=0)
    analysis_budget_reserved: int = Field(default=0, ge=0)
    successful: bool | None = None
    budget_exhausted: bool = False
    listing_progress: tuple[PipelineListingProgress, ...] = Field(default=(), max_length=100)


def pipeline_request_sha256(request: RequestSearchPipelineRequest) -> str:
    """Hash validated caller intent without mutable scope or guide resolution."""

    value = json.dumps(request.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _work_definition(
    *, identifier: str, kind: tuple[str, ...], payload: StrictModel, not_before_utc_ns: int
) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=kind,
        payload_schema_version=1,
        payload=payload.model_dump(mode="json"),
        deduplication_identity=(
            *kind,
            hashlib.sha256(payload.model_dump_json().encode()).hexdigest(),
        ),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
        ),
    )


def search_pipeline_work(
    *, identifier: str, payload: RequestSearchPipelinePayload, not_before_utc_ns: int = 0
) -> WorkDefinition:
    return _work_definition(
        identifier=identifier,
        kind=SEARCH_PIPELINE_WORK_KIND,
        payload=payload,
        not_before_utc_ns=not_before_utc_ns,
    )


def listing_pipeline_work(
    *, identifier: str, payload: PipelineListingPayload, not_before_utc_ns: int = 0
) -> WorkDefinition:
    return _work_definition(
        identifier=identifier,
        kind=LISTING_PIPELINE_WORK_KIND,
        payload=payload,
        not_before_utc_ns=not_before_utc_ns,
    )


def pipeline_work_constraints() -> tuple[ConcurrencyConstraint, ...]:
    return tuple(
        ConcurrencyConstraint(
            identifier=(*kind, "work_concurrency", "v1"),
            scope=SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
            maximum_active=maximum,
        )
        for kind, maximum in ((SEARCH_PIPELINE_WORK_KIND, 4), (LISTING_PIPELINE_WORK_KIND, 10))
    )


def _pipeline_component_marker() -> None:
    """Identify pipeline intent and coordinator operations in provenance."""


def build_pipeline_component_registry() -> Registry:
    return Registry(
        tuple(
            Component(identifier, 1, _pipeline_component_marker)
            for identifier in (REQUEST_SEARCH_PIPELINE, RUN_SEARCH_PIPELINE, RUN_LISTING_PIPELINE)
        )
    )
