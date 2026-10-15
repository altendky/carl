"""Lossless response-only references for repeated projection evidence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from typing import Any, cast

from pydantic import BaseModel, Field

from carl.core.composed_projection import (
    ComposedAnalysis,
    ListingStatus,
    ProjectionEvidence,
    ProjectionGalleryImageDescriptor,
    ProjectionRevision,
    ScalarFieldChange,
    SearchMembershipProjection,
)
from carl.core.listing_identity import LISTING_IDENTIFIER_PATTERN
from carl.core.models import JsonValue, StrictModel
from carl.core.review_workspace import (
    ListingReviewRecord,
    ProjectionRevisionComponent,
    ReviewBatchItem,
    ReviewState,
)


class ProjectionEvidenceReference(StrictModel):
    evidence_reference: str = Field(pattern=r"^[0-9a-f]{64}$")


class ReferencedComposedField(StrictModel):
    value: JsonValue
    evidence: ProjectionEvidenceReference


class ReferencedComposedStatus(StrictModel):
    value: ListingStatus
    raw_flags: JsonValue
    evidence: ProjectionEvidenceReference | None


class ReferencedGalleryImage(StrictModel):
    descriptor: ProjectionGalleryImageDescriptor
    evidence: ProjectionEvidenceReference | None


class ReferencedGallery(StrictModel):
    referenced_image_count: int = Field(ge=0)
    saved_image_count: int = Field(ge=0)
    all_referenced_images_saved: bool
    images: tuple[ReferencedGalleryImage, ...] = Field(max_length=100)
    images_truncated: bool
    reference_set_truncated: bool
    reference_set_evidence: ProjectionEvidenceReference


class ReferencedPreviewImage(StrictModel):
    descriptor: ProjectionGalleryImageDescriptor
    evidence: ProjectionEvidenceReference


class ReferencedListingProjection(StrictModel):
    listing_identifier: str = Field(pattern=LISTING_IDENTIFIER_PATTERN)
    canonical_source_url: str = Field(min_length=1)
    as_of_completion_sequence: int = Field(ge=0)
    projection_revision: ProjectionRevision
    status: ReferencedComposedStatus
    title: ReferencedComposedField | None
    price: ReferencedComposedField | None
    location: ReferencedComposedField | None
    description: ReferencedComposedField | None
    seller: ReferencedComposedField | None
    last_sale: ReferencedComposedField | None = None
    condition: ReferencedComposedField | None = None
    shipping: ReferencedComposedField | None = None
    preview_image: ReferencedPreviewImage | None
    gallery: ReferencedGallery | None
    analyses: tuple[ComposedAnalysis, ...] = Field(max_length=20)
    analyses_truncated: bool
    search_membership: SearchMembershipProjection | None
    warnings: tuple[str, ...] = Field(default=(), max_length=20)


class InternedReviewBatchItem(StrictModel):
    """Every original item field, with typed evidence references and a shared table."""

    projection: ReferencedListingProjection
    review_state: ReviewState
    prior_review: ListingReviewRecord | None
    changed_components: tuple[ProjectionRevisionComponent, ...]
    scalar_field_changes: tuple[ScalarFieldChange, ...] = ()
    scalar_comparison_available: bool = False
    evidence_table: dict[str, ProjectionEvidence]


def _evidence_key(evidence: ProjectionEvidence) -> str:
    value = json.dumps(
        evidence.model_dump(mode="json"),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _model_values(
    value: Any, path: tuple[str | int, ...] = ()
) -> Iterator[tuple[tuple[str | int, ...], BaseModel]]:
    """Visit typed model fields, keeping arbitrary JSON values opaque.

    A seller profile or scalar snapshot may contain keys resembling an evidence
    object or reference. Those keys do not make that value projection evidence.
    """

    if isinstance(value, BaseModel):
        yield path, value
        for name, field in type(value).model_fields.items():
            if cast(Any, field.annotation) is not JsonValue and field.annotation is not Any:
                yield from _model_values(getattr(value, name), (*path, name))
    elif isinstance(value, (tuple, list)):
        for index, item in enumerate(cast(tuple[Any, ...] | list[Any], value)):
            yield from _model_values(item, (*path, index))


def _replace_path(value: Any, path: tuple[str | int, ...], replacement: Any) -> Any:
    if not path:
        return replacement
    head, *tail = path
    if isinstance(value, dict):
        mapping = cast(dict[str | int, Any], value)
        return {**mapping, head: _replace_path(mapping[head], tuple(tail), replacement)}
    if isinstance(value, tuple):
        return tuple(
            _replace_path(item, tuple(tail), replacement) if index == head else item
            for index, item in enumerate(cast(tuple[Any, ...], value))
        )
    if isinstance(value, list):
        return [
            _replace_path(item, tuple(tail), replacement) if index == head else item
            for index, item in enumerate(cast(list[Any], value))
        ]
    raise ValueError("Projection evidence path does not identify a container")


def intern_review_batch_item(item: ReviewBatchItem) -> InternedReviewBatchItem:
    """Intern exact evidence values without changing revisions or stored records."""

    value = item.model_dump(mode="python")
    evidence_table: dict[str, ProjectionEvidence] = {}
    for path, candidate in _model_values(item):
        if not isinstance(candidate, ProjectionEvidence):
            continue
        reference = _evidence_key(candidate)
        if reference in evidence_table and evidence_table[reference] != candidate:
            raise ValueError("Projection evidence content hash collision")
        evidence_table[reference] = candidate
        value = _replace_path(value, path, {"evidence_reference": reference})
    return InternedReviewBatchItem.model_validate(
        {**value, "evidence_table": dict(sorted(evidence_table.items()))}
    )


def rehydrate_review_batch_item(item: InternedReviewBatchItem) -> ReviewBatchItem:
    """Restore and validate the exact full item from a response or JSON round trip."""

    for reference, evidence in item.evidence_table.items():
        if reference != _evidence_key(evidence):
            raise ValueError("Projection evidence table key does not match its content")
    value = item.model_dump(mode="python", exclude={"evidence_table"})
    for path, candidate in _model_values(item):
        if not isinstance(candidate, ProjectionEvidenceReference):
            continue
        evidence = item.evidence_table.get(candidate.evidence_reference)
        if evidence is None:
            raise ValueError("Projection evidence reference is missing from the evidence table")
        value = _replace_path(value, path, evidence.model_dump(mode="python"))
    return ReviewBatchItem.model_validate(value)
