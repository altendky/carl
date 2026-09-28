"""Pure models and selection rules for bounded composed listing projections."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import cast

from pydantic import Field, field_validator, model_validator

from carl.core.facebook_search import SearchStoppingReason
from carl.core.json import decode_json, encode_json
from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel


class ListingStatus(JsonStringEnumeration):
    AVAILABLE = "available"
    PENDING = "pending"
    SOLD = "sold"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class ProjectionSourceKind(JsonStringEnumeration):
    SEARCH_CARD = "search_card"
    PDP_PROBE = "pdp_probe"
    ITEM_PAGE = "item_page"
    IMAGE_DOWNLOAD = "image_download"


class ProjectionEvidence(StrictModel):
    """Repository ordering and drill-down identity for one selected value."""

    evidence_record_identifier: str = Field(min_length=1)
    observation_record_identifier: str | None = Field(default=None, min_length=1)
    acquisition_record_identifier: str | None = Field(default=None, min_length=1)
    producing_operation_identifier: str | None = Field(default=None, min_length=1)
    acquisition_completion_sequence: int = Field(ge=0)
    observation_completion_sequence: int = Field(ge=0)
    observed_at_utc: str | None = None
    completed_at_utc: str | None = None
    source_kind: ProjectionSourceKind
    warnings: tuple[str, ...] = Field(default=(), max_length=20)


class ComposedField(StrictModel):
    value: JsonValue
    evidence: ProjectionEvidence


class ComposedStatus(StrictModel):
    value: ListingStatus
    raw_flags: JsonValue
    evidence: ProjectionEvidence | None

    @model_validator(mode="after")
    def validate_unknown_without_evidence(self) -> ComposedStatus:
        if self.evidence is None and self.value is not ListingStatus.UNKNOWN:
            raise ValueError("A status without evidence must be unknown")
        return self


class ProjectionGalleryImageDescriptor(StrictModel):
    gallery_order: int = Field(ge=0)
    gallery_reference_record_identifier: str | None
    original_url: str
    photo_identifier: str | None
    declared_width: int | None = Field(default=None, gt=0)
    declared_height: int | None = Field(default=None, gt=0)
    image_result_record_identifier: str | None
    image_artifact_identifier: str | None
    download_state: str
    sha256: str | None
    media_type: str | None
    width: int | None = Field(default=None, gt=0)
    height: int | None = Field(default=None, gt=0)


class ComposedGalleryImage(StrictModel):
    descriptor: ProjectionGalleryImageDescriptor
    evidence: ProjectionEvidence | None


class ComposedGallery(StrictModel):
    referenced_image_count: int = Field(ge=0)
    saved_image_count: int = Field(ge=0)
    all_referenced_images_saved: bool
    images: tuple[ComposedGalleryImage, ...] = Field(max_length=100)
    images_truncated: bool
    reference_set_truncated: bool
    reference_set_evidence: ProjectionEvidence

    @model_validator(mode="after")
    def validate_counts(self) -> ComposedGallery:
        if self.saved_image_count > self.referenced_image_count:
            raise ValueError("Saved gallery count exceeds its reference count")
        if len(self.images) > self.referenced_image_count:
            raise ValueError("Returned gallery images exceed the reference count")
        if self.images_truncated != (
            self.reference_set_truncated or len(self.images) < self.referenced_image_count
        ):
            raise ValueError("Gallery truncation does not match returned descriptors")
        if self.all_referenced_images_saved != (
            not self.reference_set_truncated
            and self.saved_image_count == self.referenced_image_count
        ):
            raise ValueError("Gallery completeness does not match its saved count")
        return self


class ComposedPreviewImage(StrictModel):
    descriptor: ProjectionGalleryImageDescriptor
    evidence: ProjectionEvidence


class AnalysisApplicability(JsonStringEnumeration):
    EXACT = "exact"
    ASSUMED = "assumed"
    STALE = "stale"
    INDETERMINATE = "indeterminate"


class ProjectionAnalysisDescriptor(StrictModel):
    analysis_record_identifier: str = Field(min_length=1)
    evidence_set_record_identifier: str = Field(min_length=1)
    listing_observation_record_identifier: str = Field(min_length=1)
    product_guide_record_identifier: str = Field(min_length=1)
    completion_sequence: int = Field(ge=0)
    completed_at_utc: str | None
    state: str
    warnings: tuple[str, ...] = Field(default=(), max_length=20)
    model: str | None


class ComposedAnalysis(StrictModel):
    descriptor: ProjectionAnalysisDescriptor
    applicability: AnalysisApplicability
    applicability_reasons: tuple[str, ...] = Field(max_length=10)


class AnalysisPresenceFilter(JsonStringEnumeration):
    ANY = "any"
    PRESENT = "present"
    ABSENT = "absent"


class SearchComparisonCoverage(JsonStringEnumeration):
    COMPLETE = "complete"
    BOUNDED = "bounded"
    INCOMPLETE = "incomplete"
    UNKNOWN = "unknown"


class SearchMembershipProjection(StrictModel):
    lineage_root_search_run_record_identifier: str | None
    selected_search_run_record_identifier: str = Field(min_length=1)
    oldest_included_search_run_record_identifier: str = Field(min_length=1)
    included_ancestry_run_count: int = Field(ge=1)
    older_ancestry_truncated: bool
    first_seen_search_run_record_identifier: str = Field(min_length=1)
    last_seen_search_run_record_identifier: str = Field(min_length=1)
    first_seen_at_utc: str | None
    last_seen_at_utc: str | None
    seen_in_selected_run: bool
    seen_run_count: int = Field(ge=1)
    selected_run_stopping_reason: str | None
    comparison_coverage: SearchComparisonCoverage
    absence_comparison_valid: bool
    warnings: tuple[str, ...] = Field(default=(), max_length=20)

    @model_validator(mode="after")
    def validate_absence_comparison(self) -> SearchMembershipProjection:
        if self.absence_comparison_valid != (
            self.comparison_coverage is SearchComparisonCoverage.COMPLETE
        ):
            raise ValueError("Absence validity must be derived from complete coverage")
        return self


class ProjectionRevision(StrictModel):
    recipe_version: int = Field(default=1, ge=1)
    status_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    scalar_fields_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    preview_image_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    gallery_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    analyses_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    search_membership_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    aggregate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ComposedListingProjection(StrictModel):
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    canonical_source_url: str = Field(min_length=1)
    as_of_completion_sequence: int = Field(ge=0)
    projection_revision: ProjectionRevision
    status: ComposedStatus
    title: ComposedField | None
    price: ComposedField | None
    location: ComposedField | None
    description: ComposedField | None
    seller: ComposedField | None
    preview_image: ComposedPreviewImage | None
    gallery: ComposedGallery | None
    analyses: tuple[ComposedAnalysis, ...] = Field(max_length=20)
    analyses_truncated: bool
    search_membership: SearchMembershipProjection | None
    warnings: tuple[str, ...] = Field(default=(), max_length=20)

    @model_validator(mode="after")
    def validate_canonical_url(self) -> ComposedListingProjection:
        if self.canonical_source_url != canonical_facebook_listing_url(self.listing_identifier):
            raise ValueError("Listing source URL is not canonical")
        return self


class ComposedListingFilters(StrictModel):
    statuses: Sequence[ListingStatus] = Field(
        default=(ListingStatus.AVAILABLE,), min_length=1, max_length=5
    )
    product_guide_record_identifier: str | None = Field(default=None, min_length=1)
    analysis: AnalysisPresenceFilter = AnalysisPresenceFilter.ANY

    @field_validator("statuses")
    @classmethod
    def validate_unique_statuses(cls, value: Sequence[ListingStatus]) -> Sequence[ListingStatus]:
        if len(set(value)) != len(value):
            raise ValueError("Listing status filters must be unique")
        return value


class GetComposedListingRequest(StrictModel):
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    search_run_record_identifier: str | None = Field(default=None, min_length=1)
    additional_search_run_record_identifiers: Sequence[str] = Field(default=(), max_length=20)
    product_guide_record_identifier: str | None = Field(default=None, min_length=1)
    maximum_ancestry_runs: int = Field(default=100, ge=1, le=100)
    maximum_gallery_images: int = Field(default=20, ge=0, le=100)
    maximum_analyses: int = Field(default=10, ge=0, le=20)
    maximum_observations: int = Field(default=100, ge=100, le=100)

    @model_validator(mode="after")
    def validate_search_runs(self) -> GetComposedListingRequest:
        identifiers = tuple(self.additional_search_run_record_identifiers)
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("Additional search runs must be unique")
        if self.search_run_record_identifier in identifiers:
            raise ValueError("The primary search run cannot also be an additional search run")
        if self.search_run_record_identifier is None and identifiers:
            raise ValueError("Additional search runs require a primary search run")
        return self


class ListComposedSearchRequest(StrictModel):
    search_run_record_identifier: str = Field(min_length=1)
    additional_search_run_record_identifiers: Sequence[str] = Field(default=(), max_length=20)
    filters: ComposedListingFilters = ComposedListingFilters()
    maximum_ancestry_runs: int = Field(default=100, ge=1, le=100)
    maximum_gallery_images_per_listing: int = Field(default=0, ge=0, le=10)
    maximum_analyses_per_listing: int = Field(default=1, ge=0, le=5)
    maximum_observations_per_listing: int = Field(default=100, ge=100, le=100)
    maximum_candidate_listings_examined: int = Field(default=2_500, ge=1, le=10_000)
    page_size: int = Field(default=25, ge=1, le=100)
    cursor: str | None = None

    @model_validator(mode="after")
    def validate_search_runs(self) -> ListComposedSearchRequest:
        identifiers = tuple(self.additional_search_run_record_identifiers)
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("Additional search runs must be unique")
        if self.search_run_record_identifier in identifiers:
            raise ValueError("The primary search run cannot also be an additional search run")
        return self


class ComposedListingPage(StrictModel):
    as_of_completion_sequence: int = Field(ge=0)
    selected_search_run_record_identifier: str = Field(min_length=1)
    included_ancestry_run_count: int = Field(ge=1)
    older_ancestry_truncated: bool
    examined_candidate_listing_count: int = Field(ge=0, le=10_000)
    candidate_examination_limit_reached: bool
    listings: tuple[ComposedListingProjection, ...] = Field(max_length=100)
    next_cursor: str | None


class ListingObservationCandidate(StrictModel):
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    response_classification: str
    observation: JsonValue
    evidence: ProjectionEvidence


class SearchCardCandidate(StrictModel):
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    original: dict[str, JsonValue]
    evidence: ProjectionEvidence


class StatusObservationCandidate(StrictModel):
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    value: ListingStatus
    raw_flags: JsonValue
    evidence: ProjectionEvidence


class GalleryCandidate(StrictModel):
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    images: tuple[ComposedGalleryImage, ...] = Field(max_length=100)
    referenced_image_count: int = Field(ge=0)
    reference_set_truncated: bool
    reference_set_evidence: ProjectionEvidence

    @model_validator(mode="after")
    def validate_reference_count(self) -> GalleryCandidate:
        if self.referenced_image_count < len(self.images):
            raise ValueError("Gallery candidate has more images than references")
        if self.reference_set_truncated != (len(self.images) < self.referenced_image_count):
            raise ValueError("Gallery candidate truncation does not match its reference count")
        return self


class SavedImageProjectionCandidate(StrictModel):
    result_record_identifier: str = Field(min_length=1)
    image_reference_record_identifier: str | None = Field(default=None, min_length=1)
    source_photo_identifier: str | None = Field(default=None, min_length=1)
    original_url: str = Field(min_length=1)
    result: dict[str, JsonValue]
    evidence: ProjectionEvidence


class AnalysisSelection(StrictModel):
    analyses: tuple[ComposedAnalysis, ...] = Field(max_length=20)
    matching_analysis_present: bool
    analyses_truncated: bool


def _canonical_sha256(value: JsonValue) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _semantic_image_descriptor(
    descriptor: ProjectionGalleryImageDescriptor,
) -> dict[str, JsonValue]:
    """Prefer stable photo/content identity over expiring delivery URLs."""

    if descriptor.sha256 is not None:
        identity: dict[str, JsonValue] = {"sha256": descriptor.sha256}
    elif descriptor.photo_identifier is not None:
        identity = {"photo_identifier": descriptor.photo_identifier}
    else:
        identity = {"original_url": descriptor.original_url}
    return {
        "gallery_order": descriptor.gallery_order,
        "identity": identity,
        "declared_width": descriptor.declared_width,
        "declared_height": descriptor.declared_height,
        "download_state": descriptor.download_state,
        "media_type": descriptor.media_type,
        "width": descriptor.width,
        "height": descriptor.height,
    }


def projection_revision(
    *,
    listing_identifier: str,
    status: ComposedStatus,
    title: ComposedField | None,
    price: ComposedField | None,
    location: ComposedField | None,
    description: ComposedField | None,
    seller: ComposedField | None,
    preview_image: ComposedPreviewImage | None,
    gallery: ComposedGallery | None,
    analyses: Sequence[ComposedAnalysis],
    analyses_truncated: bool,
    search_membership: SearchMembershipProjection | None,
) -> ProjectionRevision:
    """Hash semantic components without transport limits or evidence-record churn."""

    status_sha256 = _canonical_sha256({"value": status.value.value, "raw_flags": status.raw_flags})
    scalar_fields_sha256 = _canonical_sha256(
        {
            "title": None if title is None else title.value,
            "price": None if price is None else price.value,
            "location": None if location is None else location.value,
            "description": None if description is None else description.value,
            "seller": None if seller is None else seller.value,
        }
    )
    preview_image_sha256 = _canonical_sha256(
        None if preview_image is None else _semantic_image_descriptor(preview_image.descriptor)
    )
    gallery_sha256 = _canonical_sha256(
        None
        if gallery is None
        else {
            "referenced_image_count": gallery.referenced_image_count,
            "saved_image_count": gallery.saved_image_count,
            "all_referenced_images_saved": gallery.all_referenced_images_saved,
            "images": [_semantic_image_descriptor(image.descriptor) for image in gallery.images],
            "reference_set_truncated": gallery.reference_set_truncated,
        }
    )
    analyses_sha256 = _canonical_sha256(
        {
            "analyses": [analysis.model_dump(mode="json") for analysis in analyses],
            "truncated": analyses_truncated,
        }
    )
    search_membership_sha256 = _canonical_sha256(
        None if search_membership is None else search_membership.model_dump(mode="json")
    )
    component_hashes: dict[str, JsonValue] = {
        "recipe_version": 1,
        "listing_identifier": listing_identifier,
        "status_sha256": status_sha256,
        "scalar_fields_sha256": scalar_fields_sha256,
        "preview_image_sha256": preview_image_sha256,
        "gallery_sha256": gallery_sha256,
        "analyses_sha256": analyses_sha256,
        "search_membership_sha256": search_membership_sha256,
    }
    return ProjectionRevision(
        status_sha256=status_sha256,
        scalar_fields_sha256=scalar_fields_sha256,
        preview_image_sha256=preview_image_sha256,
        gallery_sha256=gallery_sha256,
        analyses_sha256=analyses_sha256,
        search_membership_sha256=search_membership_sha256,
        aggregate_sha256=_canonical_sha256(component_hashes),
    )


class SearchRunCandidate(StrictModel):
    record_identifier: str = Field(min_length=1)
    internal_search_run_identifier: str = Field(min_length=1)
    completion_sequence: int = Field(ge=0)
    started_at_utc: str | None
    completed_at_utc: str | None
    refresh_source_run_record_identifier: str | None = Field(default=None, min_length=1)
    stopping_reason: SearchStoppingReason | None


class SearchMembershipOccurrenceCandidate(StrictModel):
    occurrence_record_identifier: str = Field(min_length=1)
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    search_run_record_identifier: str = Field(min_length=1)
    search_run_completion_sequence: int = Field(ge=0)
    acquisition_completion_sequence: int = Field(ge=0)
    observed_at_utc: str | None


class SearchAncestrySelection(StrictModel):
    runs: tuple[SearchRunCandidate, ...] = Field(min_length=1, max_length=100)
    lineage_root_search_run_record_identifier: str | None
    older_ancestry_truncated: bool
    warnings: tuple[str, ...] = Field(default=(), max_length=20)


class ComposedListingCursor(StrictModel):
    as_of_completion_sequence: int = Field(ge=0)
    scope_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    after_membership_completion_sequence: int = Field(ge=0)
    after_listing_identifier: str = Field(pattern=r"^[0-9]+$")


def canonical_facebook_listing_url(listing_identifier: str) -> str:
    if not listing_identifier.isascii() or not listing_identifier.isdecimal():
        raise ValueError("Facebook listing identifiers must be numeric strings")
    return f"https://www.facebook.com/marketplace/item/{listing_identifier}/"


def evidence_recency_key(evidence: ProjectionEvidence) -> tuple[int, int, str]:
    return (
        evidence.acquisition_completion_sequence,
        evidence.observation_completion_sequence,
        evidence.evidence_record_identifier,
    )


def normalize_listing_status(
    *,
    response_classification: str | None = None,
    is_sold: bool | None = None,
    is_pending: bool | None = None,
    is_live: bool | None = None,
) -> ListingStatus | None:
    """Normalize explicit source facts without treating absence as a status."""

    if response_classification == "listing_unavailable":
        return ListingStatus.UNAVAILABLE
    if is_sold is True:
        return ListingStatus.SOLD
    if is_pending is True:
        return ListingStatus.PENDING
    if is_live is True:
        return ListingStatus.AVAILABLE
    if any(value is not None for value in (is_sold, is_pending, is_live)):
        return ListingStatus.UNKNOWN
    return None


def _observation_field(observation: JsonValue, name: str) -> tuple[bool, JsonValue]:
    if not isinstance(observation, dict):
        return False, None
    observation_mapping = cast(dict[str, object], observation)
    fields = observation_mapping.get("fields")
    field = cast(dict[str, object], fields).get(name) if isinstance(fields, dict) else None
    if not isinstance(field, dict):
        return False, None
    field_mapping = cast(dict[str, object], field)
    if field_mapping.get("state") != "present":
        return False, None
    evidence = field_mapping.get("evidence")
    if not isinstance(evidence, list):
        return False, None
    values: list[JsonValue] = []
    for item in cast(list[object], evidence):
        if not isinstance(item, dict):
            continue
        item_mapping = cast(dict[str, object], item)
        if item_mapping.get("state") != "present":
            continue
        if item_mapping.get("normalized") is not None:
            values.append(item_mapping["normalized"])
        elif "original" in item_mapping:
            values.append(item_mapping["original"])
    if not values or any(value != values[0] for value in values[1:]):
        return False, None
    return True, values[0]


def status_candidate_from_item_observation(
    candidate: ListingObservationCandidate,
) -> StatusObservationCandidate | None:
    present_sold, sold = _observation_field(candidate.observation, "availability_sold")
    present_pending, pending = _observation_field(candidate.observation, "availability_pending")
    present_live, live = _observation_field(candidate.observation, "availability_live")
    flags = {
        "is_sold": sold if present_sold and isinstance(sold, bool) else None,
        "is_pending": pending if present_pending and isinstance(pending, bool) else None,
        "is_live": live if present_live and isinstance(live, bool) else None,
    }
    value = normalize_listing_status(
        response_classification=candidate.response_classification,
        is_sold=flags["is_sold"],
        is_pending=flags["is_pending"],
        is_live=flags["is_live"],
    )
    if value is None:
        return None
    return StatusObservationCandidate(
        listing_identifier=candidate.listing_identifier,
        value=value,
        raw_flags=flags,
        evidence=candidate.evidence,
    )


def status_candidate_from_search_occurrence(
    *,
    listing_identifier: str,
    original: JsonValue,
    evidence: ProjectionEvidence,
) -> StatusObservationCandidate | None:
    if not isinstance(original, dict):
        return None
    original_mapping = cast(dict[str, object], original)
    flags = {
        name: value if isinstance(value, bool) else None
        for name in ("is_sold", "is_pending", "is_live")
        if (value := original_mapping.get(name)) is not None
    }
    value = normalize_listing_status(
        is_sold=flags.get("is_sold"),
        is_pending=flags.get("is_pending"),
        is_live=flags.get("is_live"),
    )
    if value is None:
        return None
    return StatusObservationCandidate(
        listing_identifier=listing_identifier,
        value=value,
        raw_flags=flags,
        evidence=evidence,
    )


def select_status(
    candidates: Sequence[StatusObservationCandidate],
    *,
    as_of_completion_sequence: int,
) -> ComposedStatus:
    eligible = tuple(
        candidate
        for candidate in candidates
        if candidate.evidence.observation_completion_sequence <= as_of_completion_sequence
    )
    if not eligible:
        return ComposedStatus(value=ListingStatus.UNKNOWN, raw_flags=None, evidence=None)
    selected = max(eligible, key=lambda candidate: evidence_recency_key(candidate.evidence))
    return ComposedStatus(
        value=selected.value,
        raw_flags=selected.raw_flags,
        evidence=selected.evidence,
    )


def select_field(
    name: str,
    candidates: Sequence[ListingObservationCandidate],
    *,
    as_of_completion_sequence: int,
) -> ComposedField | None:
    eligible: list[tuple[ListingObservationCandidate, JsonValue]] = []
    for candidate in candidates:
        if candidate.evidence.observation_completion_sequence > as_of_completion_sequence:
            continue
        present, value = _observation_field(candidate.observation, name)
        if present:
            eligible.append((candidate, value))
    if not eligible:
        return None
    candidate, value = max(eligible, key=lambda item: evidence_recency_key(item[0].evidence))
    return ComposedField(value=value, evidence=candidate.evidence)


def _search_card_text(value: JsonValue) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        mapping = cast(dict[str, JsonValue], value)
        if isinstance(mapping.get("text"), str):
            return cast(str, mapping["text"])
    return None


def _search_card_location(value: JsonValue) -> str | None:
    text = _search_card_text(value)
    if text is not None:
        return text
    if not isinstance(value, dict):
        return None
    mapping = cast(dict[str, JsonValue], value)
    reverse_geocode = mapping.get("reverse_geocode")
    if not isinstance(reverse_geocode, dict):
        return None
    reverse_geocode_mapping = cast(dict[str, JsonValue], reverse_geocode)
    city_page = reverse_geocode_mapping.get("city_page")
    if isinstance(city_page, dict):
        city_page_mapping = cast(dict[str, JsonValue], city_page)
        if isinstance(city_page_mapping.get("display_name"), str):
            return cast(str, city_page_mapping["display_name"])
    parts = tuple(
        part
        for name in ("city", "state")
        if isinstance((part := reverse_geocode_mapping.get(name)), str) and part
    )
    return ", ".join(parts) or None


def _search_card_price(value: JsonValue) -> JsonValue | None:
    if not isinstance(value, dict):
        return None
    mapping = cast(dict[str, JsonValue], value)
    amount = mapping.get("amount")
    if isinstance(amount, bool) or not isinstance(amount, (int, float, str)):
        return None
    try:
        decimal = Decimal(str(amount))
    except (InvalidOperation, ValueError):
        return None
    if not decimal.is_finite():
        return None
    normalized: dict[str, JsonValue] = {"amount_decimal": str(decimal)}
    currency = mapping.get("currency")
    if isinstance(currency, str):
        normalized["currency"] = currency
    formatted_amount = mapping.get("formatted_amount")
    if isinstance(formatted_amount, str):
        normalized["formatted_amount"] = formatted_amount
    return normalized


def search_card_field_value(name: str, original: Mapping[str, JsonValue]) -> JsonValue | None:
    """Return a normalized usable detail value from a retained search card."""

    if name == "title":
        for key in ("marketplace_listing_title", "custom_title"):
            value = original.get(key)
            if isinstance(value, str):
                return value
        return None
    if name == "price":
        return _search_card_price(original.get("listing_price"))
    if name == "location_text":
        for key in ("location_text", "location"):
            if (value := _search_card_location(original.get(key))) is not None:
                return value
        return None
    if name == "description":
        return _search_card_text(original.get("redacted_description"))
    if name == "seller":
        seller = original.get("marketplace_listing_seller")
        return seller if seller is not None else None
    raise ValueError(f"Unsupported composed search-card field: {name}")


def select_composed_field(
    name: str,
    observations: Sequence[ListingObservationCandidate],
    search_cards: Sequence[SearchCardCandidate],
    *,
    as_of_completion_sequence: int,
) -> ComposedField | None:
    """Select one usable scalar value under the field's source policy.

    Item-page evidence remains authoritative for descriptive fields. Price is
    different: either an item page or a search card can directly observe it,
    so the newest actual price observation wins. A missing value supplies no
    candidate and therefore cannot erase an older observed value.
    """

    item_field = select_field(
        name,
        observations,
        as_of_completion_sequence=as_of_completion_sequence,
    )
    if name != "price" and item_field is not None:
        return item_field
    candidates = list(
        ComposedField(value=value, evidence=card.evidence)
        for card in search_cards
        if card.evidence.observation_completion_sequence <= as_of_completion_sequence
        and (value := search_card_field_value(name, card.original)) is not None
    )
    if item_field is not None:
        candidates.append(item_field)
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: evidence_recency_key(candidate.evidence))


def select_search_card_preview(
    search_cards: Sequence[SearchCardCandidate],
    *,
    as_of_completion_sequence: int,
) -> ComposedPreviewImage | None:
    eligible: list[tuple[SearchCardCandidate, Mapping[str, JsonValue], str]] = []
    for card in search_cards:
        if card.evidence.observation_completion_sequence > as_of_completion_sequence:
            continue
        photo = card.original.get("primary_listing_photo")
        if not isinstance(photo, dict):
            continue
        photo_mapping = cast(dict[str, JsonValue], photo)
        image = photo_mapping.get("image")
        if not isinstance(image, dict):
            continue
        image_mapping = cast(dict[str, JsonValue], image)
        uri = image_mapping.get("uri")
        if isinstance(uri, str):
            eligible.append((card, image_mapping, uri))
    if not eligible:
        return None
    card, image, uri = max(eligible, key=lambda item: evidence_recency_key(item[0].evidence))
    photo = card.original["primary_listing_photo"]
    assert isinstance(photo, dict)
    photo_mapping = cast(dict[str, JsonValue], photo)

    def positive_integer(value: object) -> int | None:
        return (
            value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None
        )

    photo_identifier = photo_mapping.get("id")
    return ComposedPreviewImage(
        descriptor=ProjectionGalleryImageDescriptor(
            gallery_order=0,
            gallery_reference_record_identifier=None,
            original_url=uri,
            photo_identifier=photo_identifier if isinstance(photo_identifier, str) else None,
            declared_width=positive_integer(image.get("width")),
            declared_height=positive_integer(image.get("height")),
            image_result_record_identifier=None,
            image_artifact_identifier=None,
            download_state="not_yet_collected",
            sha256=None,
            media_type=None,
            width=None,
            height=None,
        ),
        evidence=card.evidence,
    )


def select_gallery(
    candidates: Sequence[GalleryCandidate],
    *,
    as_of_completion_sequence: int,
    maximum_images: int,
) -> ComposedGallery | None:
    if not 0 <= maximum_images <= 100:
        raise ValueError("Maximum gallery images must be between zero and 100")
    eligible = tuple(
        candidate
        for candidate in candidates
        if candidate.images
        and candidate.reference_set_evidence.observation_completion_sequence
        <= as_of_completion_sequence
    )
    if not eligible:
        return None
    selected = max(eligible, key=lambda item: evidence_recency_key(item.reference_set_evidence))
    ordered = tuple(sorted(selected.images, key=lambda image: image.descriptor.gallery_order))
    saved_count = sum(image.descriptor.download_state == "saved" for image in ordered)
    returned = ordered[:maximum_images]
    return ComposedGallery(
        referenced_image_count=selected.referenced_image_count,
        saved_image_count=saved_count,
        all_referenced_images_saved=(
            not selected.reference_set_truncated and saved_count == selected.referenced_image_count
        ),
        images=returned,
        images_truncated=(
            selected.reference_set_truncated or len(returned) < selected.referenced_image_count
        ),
        reference_set_truncated=selected.reference_set_truncated,
        reference_set_evidence=selected.reference_set_evidence,
    )


def truncate_composed_gallery(
    gallery: ComposedGallery | None, *, maximum_images: int
) -> ComposedGallery | None:
    """Bound a full gallery for transport without changing its semantic revision."""

    if not 0 <= maximum_images <= 100:
        raise ValueError("Maximum gallery images must be between zero and 100")
    if gallery is None:
        return None
    images = gallery.images[:maximum_images]
    return gallery.model_copy(
        update={
            "images": images,
            "images_truncated": (
                gallery.reference_set_truncated or len(images) < gallery.referenced_image_count
            ),
        }
    )


def truncate_analysis_selection(
    selection: AnalysisSelection, *, maximum_analyses: int
) -> AnalysisSelection:
    """Bound analyses for transport while preserving full-selection presence metadata."""

    if not 0 <= maximum_analyses <= 20:
        raise ValueError("Maximum analyses must be between zero and 20")
    analyses = selection.analyses[:maximum_analyses]
    return selection.model_copy(
        update={
            "analyses": analyses,
            "analyses_truncated": (
                selection.analyses_truncated or len(analyses) < len(selection.analyses)
            ),
        }
    )


def select_analyses(
    descriptors: Sequence[ProjectionAnalysisDescriptor],
    *,
    product_guide_record_identifier: str | None,
    maximum_analyses: int,
) -> AnalysisSelection:
    if not 0 <= maximum_analyses <= 20:
        raise ValueError("Maximum analyses must be between zero and 20")
    eligible = tuple(
        descriptor
        for descriptor in descriptors
        if descriptor.state == "completed"
        and (
            product_guide_record_identifier is None
            or descriptor.product_guide_record_identifier == product_guide_record_identifier
        )
    )
    by_guide: dict[str, ProjectionAnalysisDescriptor] = {}
    for descriptor in eligible:
        previous = by_guide.get(descriptor.product_guide_record_identifier)
        if previous is None or (
            descriptor.completion_sequence,
            descriptor.analysis_record_identifier,
        ) > (previous.completion_sequence, previous.analysis_record_identifier):
            by_guide[descriptor.product_guide_record_identifier] = descriptor
    ordered = tuple(
        sorted(
            by_guide.values(),
            key=lambda item: (-item.completion_sequence, item.analysis_record_identifier),
        )
    )
    selected = ordered[:maximum_analyses]
    return AnalysisSelection(
        analyses=tuple(
            ComposedAnalysis(
                descriptor=descriptor,
                applicability=AnalysisApplicability.ASSUMED,
                applicability_reasons=("permissive_initial_reuse_policy",),
            )
            for descriptor in selected
        ),
        matching_analysis_present=bool(ordered),
        analyses_truncated=len(selected) < len(ordered),
    )


_COMPLETE_STOPPING_REASONS = {
    SearchStoppingReason.NO_NEXT_PAGE,
    SearchStoppingReason.PRICE_PARTITIONS_EXHAUSTED,
}
_INCOMPLETE_STOPPING_REASONS = {
    SearchStoppingReason.MISSING_CURSOR,
    SearchStoppingReason.REPEATED_CURSOR,
    SearchStoppingReason.TRANSFERRED_BYTES_UNAVAILABLE,
}


def classify_search_comparison_coverage(
    stopping_reason: SearchStoppingReason | None,
    *,
    successfully_completed: bool | None = True,
) -> SearchComparisonCoverage:
    if successfully_completed is False:
        return SearchComparisonCoverage.INCOMPLETE
    if successfully_completed is None or stopping_reason is None:
        return SearchComparisonCoverage.UNKNOWN
    if stopping_reason in _COMPLETE_STOPPING_REASONS:
        return SearchComparisonCoverage.COMPLETE
    if stopping_reason in _INCOMPLETE_STOPPING_REASONS:
        return SearchComparisonCoverage.INCOMPLETE
    return SearchComparisonCoverage.BOUNDED


def bounded_refresh_ancestry(
    *,
    selected_search_run_record_identifier: str,
    runs_by_identifier: Mapping[str, SearchRunCandidate],
    maximum_runs: int,
) -> SearchAncestrySelection:
    if not 1 <= maximum_runs <= 100:
        raise ValueError("Maximum ancestry runs must be between one and 100")
    selected = runs_by_identifier.get(selected_search_run_record_identifier)
    if selected is None:
        raise KeyError(selected_search_run_record_identifier)
    runs: list[SearchRunCandidate] = []
    seen: set[str] = set()
    warnings: list[str] = []
    current = selected
    truncated = False
    while True:
        if current.record_identifier in seen:
            warnings.append("refresh_ancestry_cycle")
            truncated = True
            break
        seen.add(current.record_identifier)
        runs.append(current)
        parent_identifier = current.refresh_source_run_record_identifier
        if parent_identifier is None:
            break
        if len(runs) >= maximum_runs:
            truncated = True
            break
        parent = runs_by_identifier.get(parent_identifier)
        if parent is None:
            warnings.append("refresh_ancestry_parent_unavailable")
            truncated = True
            break
        current = parent
    return SearchAncestrySelection(
        runs=tuple(runs),
        lineage_root_search_run_record_identifier=(
            runs[-1].record_identifier if not truncated else None
        ),
        older_ancestry_truncated=truncated,
        warnings=tuple(warnings),
    )


def compose_search_membership(
    *,
    listing_identifier: str,
    ancestry: SearchAncestrySelection,
    occurrences: Sequence[SearchMembershipOccurrenceCandidate],
) -> SearchMembershipProjection | None:
    run_by_identifier = {run.record_identifier: run for run in ancestry.runs}
    selected_run = ancestry.runs[0]
    relevant = tuple(
        occurrence
        for occurrence in occurrences
        if occurrence.listing_identifier == listing_identifier
        and occurrence.search_run_record_identifier in run_by_identifier
    )
    if not relevant:
        return None
    occurrences_by_run: dict[str, list[SearchMembershipOccurrenceCandidate]] = {}
    for occurrence in relevant:
        occurrences_by_run.setdefault(occurrence.search_run_record_identifier, []).append(
            occurrence
        )
    seen_runs = tuple(
        sorted(
            (run_by_identifier[identifier] for identifier in occurrences_by_run),
            key=lambda run: (run.completion_sequence, run.record_identifier),
        )
    )
    first_run = seen_runs[0]
    last_run = seen_runs[-1]

    def occurrence_time(run_identifier: str, *, latest: bool) -> str | None:
        values = tuple(
            occurrence
            for occurrence in occurrences_by_run[run_identifier]
            if occurrence.observed_at_utc is not None
        )
        if not values:
            return None
        selected = (max if latest else min)(
            values,
            key=lambda occurrence: (
                occurrence.acquisition_completion_sequence,
                occurrence.occurrence_record_identifier,
            ),
        )
        return selected.observed_at_utc

    coverage = classify_search_comparison_coverage(selected_run.stopping_reason)
    return SearchMembershipProjection(
        lineage_root_search_run_record_identifier=(
            ancestry.lineage_root_search_run_record_identifier
        ),
        selected_search_run_record_identifier=selected_run.record_identifier,
        oldest_included_search_run_record_identifier=ancestry.runs[-1].record_identifier,
        included_ancestry_run_count=len(ancestry.runs),
        older_ancestry_truncated=ancestry.older_ancestry_truncated,
        first_seen_search_run_record_identifier=first_run.record_identifier,
        last_seen_search_run_record_identifier=last_run.record_identifier,
        first_seen_at_utc=occurrence_time(first_run.record_identifier, latest=False),
        last_seen_at_utc=occurrence_time(last_run.record_identifier, latest=True),
        seen_in_selected_run=selected_run.record_identifier in occurrences_by_run,
        seen_run_count=len(seen_runs),
        selected_run_stopping_reason=(
            None if selected_run.stopping_reason is None else selected_run.stopping_reason.value
        ),
        comparison_coverage=coverage,
        absence_comparison_valid=coverage is SearchComparisonCoverage.COMPLETE,
        warnings=ancestry.warnings,
    )


def composed_listing_matches_filters(
    projection: ComposedListingProjection,
    filters: ComposedListingFilters,
    *,
    matching_analysis_present: bool,
) -> bool:
    if projection.status.value not in filters.statuses:
        return False
    if filters.analysis is AnalysisPresenceFilter.PRESENT:
        return matching_analysis_present
    if filters.analysis is AnalysisPresenceFilter.ABSENT:
        return not matching_analysis_present
    return True


def composed_listing_cursor_scope_sha256(request: ListComposedSearchRequest) -> str:
    """Bind a cursor to every request setting that affects traversal semantics."""

    content: JsonValue = {
        "search_run_record_identifier": request.search_run_record_identifier,
        "filters": {
            "statuses": sorted(status.value for status in request.filters.statuses),
            "product_guide_record_identifier": (request.filters.product_guide_record_identifier),
            "analysis": request.filters.analysis.value,
        },
        "maximum_ancestry_runs": request.maximum_ancestry_runs,
        "maximum_gallery_images_per_listing": request.maximum_gallery_images_per_listing,
        "maximum_analyses_per_listing": request.maximum_analyses_per_listing,
        "maximum_observations_per_listing": request.maximum_observations_per_listing,
        "maximum_candidate_listings_examined": request.maximum_candidate_listings_examined,
    }
    return hashlib.sha256(encode_json(content).encode("utf-8")).hexdigest()


def encode_composed_listing_cursor(cursor: ComposedListingCursor) -> str:
    content = encode_json(cursor.model_dump(mode="json")).encode("utf-8")
    return base64.urlsafe_b64encode(content).decode("ascii").rstrip("=")


def decode_composed_listing_cursor(value: str) -> ComposedListingCursor:
    if not value or any(character.isspace() for character in value):
        raise ValueError("Composed-listing cursor must be nonempty URL-safe base64")
    try:
        raw = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
        decoded = decode_json(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise ValueError("Composed-listing cursor is malformed") from error
    return ComposedListingCursor.model_validate(decoded)


def validate_composed_listing_cursor_scope(
    cursor: ComposedListingCursor,
    request: ListComposedSearchRequest,
) -> None:
    if cursor.scope_sha256 != composed_listing_cursor_scope_sha256(request):
        raise ValueError("Composed-listing cursor does not match the request scope")
