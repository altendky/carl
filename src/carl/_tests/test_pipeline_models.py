"""Pipeline intent validation and durable work identity boundaries."""

import pytest
from pydantic import ValidationError

from carl.core.marketplace_search import Marketplace
from carl.core.pipeline import (
    LISTING_PIPELINE_WORK_KIND,
    SEARCH_PIPELINE_WORK_KIND,
    PipelineListingPayload,
    PipelineOptions,
    PipelineStage,
    RequestSearchPipelinePayload,
    RequestSearchPipelineRequest,
    SearchPipelineSource,
    SearchWorkPipelineSource,
    WorkspacePipelineSource,
    listing_pipeline_work,
    pipeline_request_sha256,
    pipeline_work_constraints,
    search_pipeline_work,
)


def test_analysis_requires_an_exact_guide_but_earlier_stages_do_not() -> None:
    with pytest.raises(ValidationError, match="exact product guide"):
        PipelineOptions()
    with pytest.raises(ValidationError):
        PipelineOptions(product_guide_record_identifier="")
    assert PipelineOptions(stop_after=PipelineStage.DETAILS).product_guide_record_identifier is None
    assert PipelineOptions(stop_after=PipelineStage.IMAGES).product_guide_record_identifier is None
    assert (
        PipelineOptions(product_guide_record_identifier="guide-v1").stop_after
        is PipelineStage.ANALYSIS
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("maximum_items", 0),
        ("maximum_items", 1_001),
        ("maximum_images_per_listing", 51),
        ("maximum_images", 50_001),
        ("maximum_analyses", 1_001),
        ("maximum_inflight_listings", 101),
        ("maximum_candidate_listings_examined", 100_001),
        ("title_contains", " untrimmed"),
        ("exclude_title_keywords", ("Zeiss", "zeiss")),
        ("exclude_title_keywords", ("",)),
        ("statuses", ("available", "available")),
        ("product_guide_record_identifier", " guide"),
    ),
)
def test_processing_intent_rejects_unbounded_or_ambiguous_values(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        PipelineOptions.model_validate({"stop_after": "details", field: value})


def test_scope_and_request_identifiers_cannot_repeat_or_hide_whitespace() -> None:
    for identifiers in (("search-1", "search-1"), (" search-1",), ("",)):
        with pytest.raises(ValidationError):
            SearchWorkPipelineSource(search_work_identifiers=identifiers)
        with pytest.raises(ValidationError):
            WorkspacePipelineSource(
                workspace_record_identifier="workspace", track_identifiers=identifiers
            )
    with pytest.raises(ValidationError):
        RequestSearchPipelineRequest(
            request_identifier=" request ",
            source=SearchPipelineSource(search_record_identifier="search"),
            options=PipelineOptions(stop_after=PipelineStage.DETAILS),
        )


def test_intent_hash_freezes_budgets_and_explicit_source() -> None:
    request = RequestSearchPipelineRequest.model_validate_json(
        '{"request_identifier":"intent-1","source":{"kind":"search","search_record_identifier":"search-1"},'
        '"options":{"stop_after":"details","maximum_images":0}}'
    )
    roundtrip = RequestSearchPipelineRequest.model_validate_json(request.model_dump_json())
    assert pipeline_request_sha256(roundtrip) == pipeline_request_sha256(request)
    changed = request.model_copy(
        update={"options": request.options.model_copy(update={"refresh_details": True})}
    )
    assert pipeline_request_sha256(changed) != pipeline_request_sha256(request)
    changed_scope = request.model_copy(
        update={"source": SearchPipelineSource(search_record_identifier="search-2")}
    )
    assert pipeline_request_sha256(changed_scope) != pipeline_request_sha256(request)


def test_allocations_cannot_bypass_processing_budgets() -> None:
    options = PipelineOptions(
        stop_after=PipelineStage.IMAGES, maximum_images_per_listing=5, maximum_images=3
    )
    parameters = {
        "root_work_identifier": "root",
        "marketplace": Marketplace.EBAY,
        "external_identifier": "123456789",
        "occurrence_record_identifier": "card",
        "options": options,
    }
    with pytest.raises(ValidationError, match="global image budget"):
        PipelineListingPayload.model_validate(
            {**parameters, "maximum_images": 4, "analysis_authorized": False}
        )
    with pytest.raises(ValidationError, match="per-listing budget"):
        PipelineListingPayload.model_validate(
            {**parameters, "maximum_images": 6, "analysis_authorized": False}
        )
    with pytest.raises(ValidationError, match="analysis must be authorized"):
        PipelineListingPayload.model_validate(
            {**parameters, "maximum_images": 0, "analysis_authorized": True}
        )


def test_work_identities_preserve_marketplace_root_and_allocations() -> None:
    options = PipelineOptions(stop_after=PipelineStage.IMAGES, maximum_images=20)
    root_payload = RequestSearchPipelinePayload(
        request_identifier="intent-1",
        request_sha256="a" * 64,
        intent_record_identifier="intent-record",
        search_work_identifiers=("search-work",),
        options=options,
    )
    first = search_pipeline_work(identifier="root-1", payload=root_payload)
    retry = search_pipeline_work(identifier="root-2", payload=root_payload)
    assert first.deduplication_identity == retry.deduplication_identity
    assert first.kind == SEARCH_PIPELINE_WORK_KIND
    listing = PipelineListingPayload(
        root_work_identifier="root-1",
        marketplace=Marketplace.EBAY,
        external_identifier="123456789",
        occurrence_record_identifier="card",
        maximum_images=20,
        analysis_authorized=False,
        options=options,
    )
    ebay = listing_pipeline_work(identifier="listing-1", payload=listing)
    facebook = listing_pipeline_work(
        identifier="listing-2",
        payload=listing.model_copy(update={"marketplace": Marketplace.FACEBOOK}),
    )
    other_root = listing_pipeline_work(
        identifier="listing-3",
        payload=listing.model_copy(update={"root_work_identifier": "root-2"}),
    )
    assert ebay.kind == LISTING_PIPELINE_WORK_KIND
    assert (
        len(
            {
                ebay.deduplication_identity,
                facebook.deduplication_identity,
                other_root.deduplication_identity,
            }
        )
        == 3
    )
    assert {
        constraint.scope.identity: constraint.maximum_active
        for constraint in pipeline_work_constraints()
    } == {
        SEARCH_PIPELINE_WORK_KIND: 4,
        LISTING_PIPELINE_WORK_KIND: 10,
    }
