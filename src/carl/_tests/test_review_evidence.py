"""Evidence interning preserves frozen review data and arbitrary scalar JSON."""

from copy import deepcopy

import pytest
from mcp.types import CallToolResult, TextContent
from pydantic import BaseModel

from carl.core.composed_projection import (
    AnalysisApplicability,
    ComposedAnalysis,
    ComposedField,
    ComposedGallery,
    ComposedGalleryImage,
    ComposedListingProjection,
    ComposedPreviewImage,
    ComposedStatus,
    ListingStatus,
    ProjectionAnalysisDescriptor,
    ProjectionEvidence,
    ProjectionGalleryImageDescriptor,
    ProjectionRevision,
    ProjectionSourceKind,
    ScalarFieldChange,
    ScalarFieldName,
    ScalarFieldValues,
    SearchComparisonCoverage,
    SearchMembershipProjection,
)
from carl.core.models import JsonValue
from carl.core.review_evidence import (
    InternedReviewBatchItem,
    intern_review_batch_item,
    rehydrate_review_batch_item,
)
from carl.core.review_workspace import (
    ListingReviewRecord,
    ProjectionRevisionComponent,
    ReviewBatchItem,
    ReviewDisposition,
    ReviewState,
)


def _evidence(index: int, kind: ProjectionSourceKind) -> ProjectionEvidence:
    return ProjectionEvidence(
        evidence_record_identifier=f"evidence-00000000-0000-0000-0000-{index:012d}",
        observation_record_identifier=f"observation-00000000-0000-0000-0000-{index:012d}",
        acquisition_record_identifier=f"acquisition-00000000-0000-0000-0000-{index:012d}",
        producing_operation_identifier=f"operation-00000000-0000-0000-0000-{index:012d}",
        acquisition_completion_sequence=index,
        observation_completion_sequence=index,
        observed_at_utc="2026-10-15T00:00:00+00:00",
        completed_at_utc="2026-10-15T00:00:01+00:00",
        source_kind=kind,
        warnings=("A retained acquisition warning with exact original detail " * 3,),
    )


def _descriptor(index: int) -> ProjectionGalleryImageDescriptor:
    return ProjectionGalleryImageDescriptor(
        gallery_order=index,
        gallery_reference_record_identifier=f"reference-{index}",
        original_url=f"https://example.test/image-{index}.jpg",
        photo_identifier=f"photo-{index}",
        declared_width=1_200,
        declared_height=900,
        image_result_record_identifier=f"image-result-{index}",
        image_artifact_identifier=f"artifact-{index}",
        download_state="saved",
        sha256="a" * 64,
        media_type="image/jpeg",
        width=1_200,
        height=900,
    )


def _item() -> ReviewBatchItem:
    item_evidence = _evidence(1, ProjectionSourceKind.ITEM_PAGE)
    search_evidence = _evidence(2, ProjectionSourceKind.SEARCH_CARD)
    image_evidence = _evidence(3, ProjectionSourceKind.IMAGE_DOWNLOAD)
    revision = ProjectionRevision(
        recipe_version=2,
        status_sha256="a" * 64,
        scalar_fields_sha256="b" * 64,
        preview_image_sha256="c" * 64,
        gallery_sha256="d" * 64,
        analyses_sha256="e" * 64,
        search_membership_sha256="f" * 64,
        aggregate_sha256="0" * 64,
    )
    opaque_profile: JsonValue = {
        "name": "Seller 🔭",
        "custom": {
            "evidence": item_evidence.model_dump(mode="json"),
            "evidence_reference": "0" * 64,
            "nested": [{"unrecognized_profile_field": {"reference": "keep me"}}],
        },
    }
    projection = ComposedListingProjection(
        listing_identifier="123",
        canonical_source_url="https://www.facebook.com/marketplace/item/123/",
        as_of_completion_sequence=12,
        projection_revision=revision,
        status=ComposedStatus(
            value=ListingStatus.AVAILABLE,
            raw_flags={"is_live": True, "custom": {"evidence_reference": "0" * 64}},
            evidence=search_evidence,
        ),
        title=ComposedField(value="Telescope", evidence=item_evidence),
        price=ComposedField(
            value={"amount_decimal": "500", "currency": "USD"}, evidence=search_evidence
        ),
        location=ComposedField(
            value={"text": "Approximate city", "custom": [1, None]}, evidence=search_evidence
        ),
        description=ComposedField(value="Retained full description", evidence=item_evidence),
        seller=ComposedField(value=opaque_profile, evidence=item_evidence),
        last_sale=ComposedField(value={"sold_price": "$450"}, evidence=search_evidence),
        condition=ComposedField(value="Used", evidence=item_evidence),
        shipping=ComposedField(
            value={"text": "Local pickup", "custom": {}}, evidence=search_evidence
        ),
        preview_image=ComposedPreviewImage(descriptor=_descriptor(0), evidence=search_evidence),
        gallery=ComposedGallery(
            referenced_image_count=9,
            saved_image_count=8,
            all_referenced_images_saved=False,
            images=tuple(
                ComposedGalleryImage(
                    descriptor=_descriptor(index), evidence=image_evidence if index < 8 else None
                )
                for index in range(9)
            ),
            images_truncated=False,
            reference_set_truncated=False,
            reference_set_evidence=item_evidence,
        ),
        analyses=(
            ComposedAnalysis(
                descriptor=ProjectionAnalysisDescriptor(
                    analysis_record_identifier="analysis",
                    evidence_set_record_identifier="analysis-evidence",
                    listing_observation_record_identifier="observation",
                    product_guide_record_identifier="guide",
                    completion_sequence=9,
                    completed_at_utc="2026-10-15T00:00:09+00:00",
                    state="completed",
                    warnings=("Exact analysis warning",),
                    model="retained-model",
                ),
                applicability=AnalysisApplicability.STALE,
                applicability_reasons=("Exact retained reason",),
            ),
        ),
        analyses_truncated=True,
        search_membership=SearchMembershipProjection(
            lineage_root_search_run_record_identifier="root-run",
            selected_search_run_record_identifier="current-run",
            oldest_included_search_run_record_identifier="old-run",
            included_ancestry_run_count=3,
            older_ancestry_truncated=False,
            first_seen_search_run_record_identifier="old-run",
            last_seen_search_run_record_identifier="current-run",
            first_seen_at_utc="2026-10-13T00:00:00+00:00",
            last_seen_at_utc="2026-10-15T00:00:00+00:00",
            seen_in_selected_run=True,
            seen_run_count=3,
            selected_run_stopping_reason="complete",
            comparison_coverage=SearchComparisonCoverage.COMPLETE,
            absence_comparison_valid=True,
            warnings=("Membership warning",),
        ),
        warnings=("Projection warning",),
    )
    return ReviewBatchItem(
        projection=projection,
        review_state=ReviewState.STALE,
        prior_review=ListingReviewRecord(
            record_identifier="prior-review",
            workspace_record_identifier="workspace",
            batch_record_identifier="previous-batch",
            listing_identifier="123",
            projection_revision=revision,
            inspected=True,
            disposition=ReviewDisposition.WAITING_FOR_DATA,
            note="Original review note",
            recorded_at_utc="2026-10-14T00:00:00+00:00",
            scalar_fields_snapshot=ScalarFieldValues(
                seller=opaque_profile, price={"amount_decimal": "450"}
            ),
        ),
        changed_components=(
            ProjectionRevisionComponent.SCALAR_FIELDS,
            ProjectionRevisionComponent.ANALYSES,
        ),
        scalar_field_changes=(
            ScalarFieldChange(
                field=ScalarFieldName.SELLER,
                previous_value={"custom": {"evidence_reference": "0" * 64}},
                current_value=opaque_profile,
            ),
        ),
        scalar_comparison_available=True,
    )


def test_evidence_interning_is_lossless_for_all_projection_locations_and_scalar_json() -> None:
    original = _item()
    before = deepcopy(original.model_dump(mode="python"))
    interned = intern_review_batch_item(original)
    assert len(interned.evidence_table) == 3
    assert rehydrate_review_batch_item(interned) == original
    wire_roundtrip = InternedReviewBatchItem.model_validate_json(interned.model_dump_json())
    assert rehydrate_review_batch_item(wire_roundtrip) == original
    assert original.model_dump(mode="python") == before
    assert interned.projection.projection_revision == original.projection.projection_revision
    assert interned.projection.analyses == original.projection.analyses
    assert interned.projection.search_membership == original.projection.search_membership
    assert interned.prior_review == original.prior_review
    assert interned.scalar_field_changes == original.scalar_field_changes
    assert interned.projection.seller is not None and original.projection.seller is not None
    assert interned.projection.seller.value == original.projection.seller.value
    assert interned.projection.gallery is not None
    assert interned.projection.gallery.images[-1].evidence is None
    shared = interned.projection.gallery.reference_set_evidence.evidence_reference
    assert interned.projection.title is not None
    assert interned.projection.title.evidence.evidence_reference == shared
    assert interned.projection.preview_image is not None
    assert interned.projection.status.evidence is not None
    assert interned.projection.preview_image.evidence == interned.projection.status.evidence


def test_evidence_keys_are_content_stable_and_keep_distinct_metadata_for_one_observation() -> None:
    original = _item()
    first = intern_review_batch_item(original)
    assert (
        first.model_dump_json()
        == intern_review_batch_item(original.model_copy(deep=True)).model_dump_json()
    )
    assert original.projection.title is not None
    changed_evidence = original.projection.title.evidence.model_copy(
        update={"warnings": ("Different warning",)}
    )
    changed = original.model_copy(
        update={
            "projection": original.projection.model_copy(
                update={
                    "title": original.projection.title.model_copy(
                        update={"evidence": changed_evidence}
                    )
                }
            )
        }
    )
    second = intern_review_batch_item(changed)
    assert len(second.evidence_table) == 4
    assert set(first.evidence_table) < set(second.evidence_table)
    assert rehydrate_review_batch_item(second) == changed


def test_missing_or_mismatched_evidence_table_entries_are_rejected() -> None:
    interned = intern_review_batch_item(_item())
    reference = next(iter(interned.evidence_table))
    incomplete = interned.model_copy(
        update={
            "evidence_table": {
                key: evidence
                for key, evidence in interned.evidence_table.items()
                if key != reference
            }
        }
    )
    with pytest.raises(ValueError, match="missing"):
        _ = rehydrate_review_batch_item(incomplete)
    mismatched = interned.model_copy(
        update={
            "evidence_table": {
                **interned.evidence_table,
                reference: interned.evidence_table[reference].model_copy(
                    update={"completed_at_utc": "changed"}
                ),
            }
        }
    )
    with pytest.raises(ValueError, match="does not match"):
        _ = rehydrate_review_batch_item(mismatched)


def _wire_bytes(item: BaseModel) -> int:
    response = CallToolResult(
        content=[TextContent(type="text", text=item.model_dump_json())],
        structured_content=item.model_dump(mode="json"),
    )
    return len(response.model_dump_json(by_alias=True).encode())


def test_repeated_evidence_compacts_the_complete_mcp_wire_representation() -> None:
    original = _item()
    interned = intern_review_batch_item(original)
    assert _wire_bytes(interned) < _wire_bytes(original) * 0.7


def test_projection_without_evidence_roundtrips_without_synthetic_table_entries() -> None:
    original = _item()
    projection = original.projection.model_copy(
        update={
            "status": ComposedStatus(value=ListingStatus.UNKNOWN, raw_flags=None, evidence=None),
            "title": None,
            "price": None,
            "location": None,
            "description": None,
            "seller": None,
            "last_sale": None,
            "condition": None,
            "shipping": None,
            "preview_image": None,
            "gallery": None,
        }
    )
    minimal = original.model_copy(update={"projection": projection})
    interned = intern_review_batch_item(minimal)
    assert interned.evidence_table == {}
    assert rehydrate_review_batch_item(interned) == minimal
