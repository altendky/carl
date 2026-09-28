"""Pure composed-projection selection and contract tests."""

from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from carl.core.composed_projection import (
    AnalysisApplicability,
    AnalysisPresenceFilter,
    ComposedField,
    ComposedGalleryImage,
    ComposedListingCursor,
    ComposedListingFilters,
    ComposedListingProjection,
    ComposedPreviewImage,
    ComposedStatus,
    GalleryCandidate,
    ListComposedSearchRequest,
    ListingObservationCandidate,
    ListingStatus,
    ProjectionAnalysisDescriptor,
    ProjectionEvidence,
    ProjectionGalleryImageDescriptor,
    ProjectionSourceKind,
    SearchAncestrySelection,
    SearchCardCandidate,
    SearchComparisonCoverage,
    SearchMembershipOccurrenceCandidate,
    SearchRunCandidate,
    StatusObservationCandidate,
    bounded_refresh_ancestry,
    canonical_facebook_listing_url,
    classify_search_comparison_coverage,
    compose_search_membership,
    composed_listing_cursor_scope_sha256,
    composed_listing_matches_filters,
    decode_composed_listing_cursor,
    encode_composed_listing_cursor,
    normalize_listing_status,
    projection_revision,
    select_analyses,
    select_composed_field,
    select_field,
    select_gallery,
    select_search_card_preview,
    select_status,
    status_candidate_from_item_observation,
    status_candidate_from_search_occurrence,
    validate_composed_listing_cursor_scope,
)
from carl.core.facebook_search import SearchStoppingReason
from carl.core.models import JsonValue
from carl.io.sqlite import Database
from carl.review import ReviewApplication


def _evidence(
    identifier: str,
    acquisition_sequence: int,
    observation_sequence: int | None = None,
    *,
    kind: ProjectionSourceKind = ProjectionSourceKind.ITEM_PAGE,
) -> ProjectionEvidence:
    return ProjectionEvidence(
        evidence_record_identifier=identifier,
        observation_record_identifier=f"observation-{identifier}",
        acquisition_record_identifier=f"acquisition-{identifier}",
        producing_operation_identifier=f"operation-{identifier}",
        acquisition_completion_sequence=acquisition_sequence,
        observation_completion_sequence=(
            acquisition_sequence if observation_sequence is None else observation_sequence
        ),
        observed_at_utc=f"2026-09-{acquisition_sequence:02d}T00:00:00+00:00",
        completed_at_utc=f"2026-09-{acquisition_sequence:02d}T00:01:00+00:00",
        source_kind=kind,
    )


def _field(value: object, *, state: str = "present") -> dict[str, object]:
    return {
        "state": state,
        "evidence": [{"state": state, "normalized": value}],
    }


def _observation(
    identifier: str,
    sequence: int,
    fields: dict[str, object],
) -> ListingObservationCandidate:
    return ListingObservationCandidate(
        listing_identifier="123",
        response_classification="full_listing",
        observation={"fields": fields},
        evidence=_evidence(identifier, sequence),
    )


@pytest.mark.parametrize(
    ("sold", "pending", "live", "expected"),
    [
        (True, True, True, ListingStatus.SOLD),
        (True, False, False, ListingStatus.SOLD),
        (False, True, True, ListingStatus.PENDING),
        (False, False, True, ListingStatus.AVAILABLE),
        (False, False, False, ListingStatus.UNKNOWN),
        (None, None, None, None),
    ],
)
def test_status_normalization_precedence(
    sold: bool | None,
    pending: bool | None,
    live: bool | None,
    expected: ListingStatus | None,
) -> None:
    assert normalize_listing_status(is_sold=sold, is_pending=pending, is_live=live) is expected


def test_explicit_unavailable_status_wins() -> None:
    assert (
        normalize_listing_status(
            response_classification="listing_unavailable",
            is_sold=False,
            is_pending=False,
            is_live=True,
        )
        is ListingStatus.UNAVAILABLE
    )


def test_status_helpers_keep_absence_distinct_from_explicit_false_flags() -> None:
    evidence = _evidence("search", 1, kind=ProjectionSourceKind.SEARCH_CARD)
    assert (
        status_candidate_from_search_occurrence(
            listing_identifier="123", original={"title": "item"}, evidence=evidence
        )
        is None
    )
    candidate = status_candidate_from_search_occurrence(
        listing_identifier="123",
        original={"is_sold": False, "is_pending": False, "is_live": False},
        evidence=evidence,
    )
    assert candidate is not None
    assert candidate.value is ListingStatus.UNKNOWN

    item = _observation(
        "item",
        2,
        {
            "availability_sold": _field(False),
            "availability_pending": _field(False),
            "availability_live": _field(True),
        },
    )
    item_status = status_candidate_from_item_observation(item)
    assert item_status is not None
    assert item_status.value is ListingStatus.AVAILABLE


def test_new_source_without_status_does_not_erase_older_status() -> None:
    status = StatusObservationCandidate(
        listing_identifier="123",
        value=ListingStatus.AVAILABLE,
        raw_flags={"is_live": True},
        evidence=_evidence("old", 1),
    )
    newer_without_status = _observation("new", 2, {})

    assert status_candidate_from_item_observation(newer_without_status) is None
    assert select_status((status,), as_of_completion_sequence=2).value is ListingStatus.AVAILABLE
    assert select_status((status,), as_of_completion_sequence=0).value is ListingStatus.UNKNOWN


def test_status_recency_uses_acquisition_then_observation_then_identifier() -> None:
    candidates = (
        StatusObservationCandidate(
            listing_identifier="123",
            value=ListingStatus.AVAILABLE,
            raw_flags={},
            evidence=_evidence("z", 4, 4),
        ),
        StatusObservationCandidate(
            listing_identifier="123",
            value=ListingStatus.PENDING,
            raw_flags={},
            evidence=_evidence("a", 5, 1),
        ),
        StatusObservationCandidate(
            listing_identifier="123",
            value=ListingStatus.SOLD,
            raw_flags={},
            evidence=_evidence("b", 5, 1),
        ),
    )
    assert select_status(candidates, as_of_completion_sequence=5).value is ListingStatus.SOLD


def test_fields_compose_independently_and_unusable_new_values_do_not_erase() -> None:
    old = _observation(
        "old",
        1,
        {"title": _field("old title"), "description": _field("useful description")},
    )
    new = _observation(
        "new",
        2,
        {"title": _field("new title"), "description": _field(None, state="failed")},
    )

    title = select_field("title", (old, new), as_of_completion_sequence=2)
    description = select_field("description", (old, new), as_of_completion_sequence=2)
    assert title is not None and title.value == "new title"
    assert description is not None and description.value == "useful description"
    assert description.evidence.evidence_record_identifier == "old"


def _search_card(
    identifier: str,
    sequence: int,
    original: dict[str, JsonValue],
) -> SearchCardCandidate:
    return SearchCardCandidate(
        listing_identifier="123",
        original=original,
        evidence=_evidence(identifier, sequence, kind=ProjectionSourceKind.SEARCH_CARD),
    )


def test_newer_search_card_price_supersedes_older_item_price() -> None:
    card = _search_card(
        "card",
        3,
        {
            "marketplace_listing_title": "Card title",
            "listing_price": {"amount": "125.00", "formatted_amount": "$125"},
            "location": {
                "reverse_geocode": {
                    "city": "Example City",
                    "state": "PA",
                    "city_page": {"display_name": "Example City, Pennsylvania"},
                }
            },
            "marketplace_listing_seller": {"id": "seller-1", "name": "Seller"},
        },
    )
    item = _observation(
        "item",
        1,
        {
            "title": _field("Detailed title"),
            "price": _field({"amount_decimal": "150.00", "currency": "USD"}),
        },
    )

    title = select_composed_field("title", (item,), (card,), as_of_completion_sequence=3)
    price = select_composed_field("price", (item,), (card,), as_of_completion_sequence=3)
    location = select_composed_field("location_text", (item,), (card,), as_of_completion_sequence=3)
    seller = select_composed_field("seller", (item,), (card,), as_of_completion_sequence=3)

    assert title is not None and title.value == "Detailed title"
    assert title.evidence.source_kind is ProjectionSourceKind.ITEM_PAGE
    assert price is not None and price.value == {
        "amount_decimal": "125.00",
        "formatted_amount": "$125",
    }
    assert price.evidence.source_kind is ProjectionSourceKind.SEARCH_CARD
    assert location is not None and location.value == "Example City, Pennsylvania"
    assert seller is not None and seller.value == {"id": "seller-1", "name": "Seller"}


def test_newer_item_price_supersedes_older_search_card_price() -> None:
    card = _search_card(
        "card",
        1,
        {"listing_price": {"amount": "125.00", "formatted_amount": "$125"}},
    )
    item = _observation(
        "item",
        3,
        {"price": _field({"amount_decimal": "110.00", "currency": "USD"})},
    )

    price = select_composed_field("price", (item,), (card,), as_of_completion_sequence=3)

    assert price is not None
    assert price.value == {"amount_decimal": "110.00", "currency": "USD"}
    assert price.evidence.source_kind is ProjectionSourceKind.ITEM_PAGE


def test_missing_newer_search_card_price_does_not_clear_last_observed_price() -> None:
    observed = _search_card(
        "observed-card",
        1,
        {"listing_price": {"amount": "125.00", "formatted_amount": "$125"}},
    )
    later_without_price = _search_card("later-card", 3, {"is_live": True})

    price = select_composed_field(
        "price",
        (),
        (observed, later_without_price),
        as_of_completion_sequence=5,
    )

    assert price is not None
    assert price.value == {"amount_decimal": "125.00", "formatted_amount": "$125"}
    assert price.evidence.evidence_record_identifier == "observed-card"


def test_later_search_absence_does_not_make_an_observed_price_newer() -> None:
    observed = _search_card(
        "observed-card",
        1,
        {"listing_price": {"amount": "125.00", "formatted_amount": "$125"}},
    )

    price = select_composed_field("price", (), (observed,), as_of_completion_sequence=50)

    assert price is not None
    assert price.evidence.evidence_record_identifier == "observed-card"
    assert price.evidence.observation_completion_sequence == 1


def test_search_card_custom_title_and_preview_are_tolerant_of_sparse_cards() -> None:
    older = _search_card(
        "older-card",
        1,
        {
            "custom_title": "Fallback title",
            "primary_listing_photo": {
                "id": "photo-1",
                "image": {"uri": "https://example.invalid/preview.jpg"},
            },
        },
    )
    newer_without_values = _search_card("newer-card", 2, {"is_live": True})

    title = select_composed_field(
        "title", (), (older, newer_without_values), as_of_completion_sequence=2
    )
    preview = select_search_card_preview((older, newer_without_values), as_of_completion_sequence=2)

    assert title is not None and title.value == "Fallback title"
    assert preview is not None
    assert preview.descriptor.original_url == "https://example.invalid/preview.jpg"
    assert preview.descriptor.photo_identifier == "photo-1"
    assert preview.evidence.evidence_record_identifier == "older-card"


def _image(order: int, *, saved: bool) -> ComposedGalleryImage:
    return ComposedGalleryImage(
        descriptor=ProjectionGalleryImageDescriptor(
            gallery_order=order,
            gallery_reference_record_identifier=f"reference-{order}",
            original_url=f"https://example.invalid/{order}",
            photo_identifier=f"photo-{order}",
            declared_width=100,
            declared_height=100,
            image_result_record_identifier=f"result-{order}" if saved else None,
            image_artifact_identifier=f"artifact-{order}" if saved else None,
            download_state="saved" if saved else "not_yet_collected",
            sha256="a" * 64 if saved else None,
            media_type="image/jpeg" if saved else None,
            width=100 if saved else None,
            height=100 if saved else None,
        ),
        evidence=(
            _evidence(f"result-{order}", order, kind=ProjectionSourceKind.IMAGE_DOWNLOAD)
            if saved
            else None
        ),
    )


def test_gallery_selects_one_reference_set_and_bounds_descriptors() -> None:
    old = GalleryCandidate(
        listing_identifier="123",
        images=(_image(0, saved=True),),
        referenced_image_count=1,
        reference_set_truncated=False,
        reference_set_evidence=_evidence("old-gallery", 1),
    )
    new = GalleryCandidate(
        listing_identifier="123",
        images=(_image(1, saved=False), _image(0, saved=True)),
        referenced_image_count=2,
        reference_set_truncated=False,
        reference_set_evidence=_evidence("new-gallery", 2),
    )
    gallery = select_gallery((old, new), as_of_completion_sequence=2, maximum_images=1)
    assert gallery is not None
    assert gallery.reference_set_evidence.evidence_record_identifier == "new-gallery"
    assert gallery.referenced_image_count == 2
    assert gallery.saved_image_count == 1
    assert gallery.images[0].descriptor.gallery_order == 0
    assert gallery.images_truncated is True


def test_gallery_discloses_a_truncated_retained_reference_set() -> None:
    candidate = GalleryCandidate(
        listing_identifier="123",
        images=tuple(_image(order, saved=True) for order in range(100)),
        referenced_image_count=101,
        reference_set_truncated=True,
        reference_set_evidence=_evidence("large-gallery", 1),
    )

    gallery = select_gallery((candidate,), as_of_completion_sequence=1, maximum_images=100)

    assert gallery is not None
    assert gallery.referenced_image_count == 101
    assert gallery.saved_image_count == 100
    assert gallery.reference_set_truncated
    assert gallery.images_truncated
    assert not gallery.all_referenced_images_saved


def _analysis(identifier: str, guide: str, sequence: int) -> ProjectionAnalysisDescriptor:
    return ProjectionAnalysisDescriptor(
        analysis_record_identifier=identifier,
        evidence_set_record_identifier=f"evidence-{identifier}",
        listing_observation_record_identifier=f"observation-{identifier}",
        product_guide_record_identifier=guide,
        completion_sequence=sequence,
        completed_at_utc="2026-09-26T00:00:00+00:00",
        state="completed",
        model="claude-sonnet-5",
    )


def test_analysis_selection_uses_newest_per_guide_and_marks_assumed() -> None:
    selection = select_analyses(
        (
            _analysis("old", "guide-a", 1),
            _analysis("new", "guide-a", 3),
            _analysis("b", "guide-b", 2),
        ),
        product_guide_record_identifier=None,
        maximum_analyses=1,
    )
    assert selection.matching_analysis_present is True
    assert selection.analyses_truncated is True
    assert selection.analyses[0].descriptor.analysis_record_identifier == "new"
    assert selection.analyses[0].applicability is AnalysisApplicability.ASSUMED

    zero = select_analyses(
        (_analysis("old", "guide-a", 1),),
        product_guide_record_identifier="guide-a",
        maximum_analyses=0,
    )
    assert zero.matching_analysis_present is True
    assert zero.analyses == ()
    assert zero.analyses_truncated is True


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        (SearchStoppingReason.NO_NEXT_PAGE, SearchComparisonCoverage.COMPLETE),
        (SearchStoppingReason.PRICE_PARTITIONS_EXHAUSTED, SearchComparisonCoverage.COMPLETE),
        (SearchStoppingReason.MAXIMUM_RESULTS, SearchComparisonCoverage.BOUNDED),
        (SearchStoppingReason.MISSING_CURSOR, SearchComparisonCoverage.INCOMPLETE),
        (SearchStoppingReason.REPEATED_CURSOR, SearchComparisonCoverage.INCOMPLETE),
        (SearchStoppingReason.TRANSFERRED_BYTES_UNAVAILABLE, SearchComparisonCoverage.INCOMPLETE),
        (None, SearchComparisonCoverage.UNKNOWN),
    ],
)
def test_search_coverage_is_derived(
    reason: SearchStoppingReason | None, expected: SearchComparisonCoverage
) -> None:
    assert classify_search_comparison_coverage(reason) is expected
    assert (
        classify_search_comparison_coverage(reason, successfully_completed=False)
        is SearchComparisonCoverage.INCOMPLETE
    )


def _run(
    identifier: str,
    sequence: int,
    parent: str | None,
    *,
    reason: SearchStoppingReason = SearchStoppingReason.NO_NEXT_PAGE,
) -> SearchRunCandidate:
    return SearchRunCandidate(
        record_identifier=identifier,
        internal_search_run_identifier=f"internal-{identifier}",
        completion_sequence=sequence,
        started_at_utc=f"2026-09-{sequence:02d}T00:00:00+00:00",
        completed_at_utc=f"2026-09-{sequence:02d}T00:01:00+00:00",
        refresh_source_run_record_identifier=parent,
        stopping_reason=reason,
    )


def _occurrence(listing: str, run: SearchRunCandidate) -> SearchMembershipOccurrenceCandidate:
    return SearchMembershipOccurrenceCandidate(
        occurrence_record_identifier=f"occurrence-{listing}-{run.record_identifier}",
        listing_identifier=listing,
        search_run_record_identifier=run.record_identifier,
        search_run_completion_sequence=run.completion_sequence,
        acquisition_completion_sequence=run.completion_sequence,
        observed_at_utc=run.completed_at_utc,
    )


def test_bounded_ancestry_and_membership_keep_exact_run_facts() -> None:
    root = _run("root", 1, None)
    middle = _run("middle", 2, "root")
    selected = _run("selected", 3, "middle")
    ancestry = bounded_refresh_ancestry(
        selected_search_run_record_identifier="selected",
        runs_by_identifier={run.record_identifier: run for run in (root, middle, selected)},
        maximum_runs=2,
    )
    assert tuple(run.record_identifier for run in ancestry.runs) == ("selected", "middle")
    assert ancestry.older_ancestry_truncated is True
    assert ancestry.lineage_root_search_run_record_identifier is None

    membership = compose_search_membership(
        listing_identifier="123",
        ancestry=ancestry,
        occurrences=(_occurrence("123", middle),),
    )
    assert membership is not None
    assert membership.seen_in_selected_run is False
    assert membership.first_seen_search_run_record_identifier == "middle"
    assert membership.last_seen_search_run_record_identifier == "middle"
    assert membership.absence_comparison_valid is True


def test_request_json_arrays_and_cursor_scope_are_strict() -> None:
    request = ListComposedSearchRequest.model_validate(
        {
            "search_run_record_identifier": "run",
            "filters": {
                "statuses": ["available", "pending"],
                "analysis": "present",
            },
            "page_size": 10,
        }
    )
    assert isinstance(request.filters.statuses, Sequence)
    assert request.filters.analysis is AnalysisPresenceFilter.PRESENT
    scope = composed_listing_cursor_scope_sha256(request)
    cursor = ComposedListingCursor(
        as_of_completion_sequence=4,
        scope_sha256=scope,
        after_membership_completion_sequence=3,
        after_listing_identifier="123",
    )
    decoded = decode_composed_listing_cursor(encode_composed_listing_cursor(cursor))
    assert decoded == cursor
    validate_composed_listing_cursor_scope(decoded, request)
    reordered_statuses = request.model_copy(
        update={
            "filters": ComposedListingFilters(
                statuses=(ListingStatus.PENDING, ListingStatus.AVAILABLE),
                analysis=AnalysisPresenceFilter.PRESENT,
            )
        }
    )
    assert composed_listing_cursor_scope_sha256(reordered_statuses) == scope
    validate_composed_listing_cursor_scope(decoded, request.model_copy(update={"page_size": 11}))
    with pytest.raises(ValueError, match="request scope"):
        validate_composed_listing_cursor_scope(
            decoded, request.model_copy(update={"maximum_observations_per_listing": 11})
        )

    with pytest.raises(ValidationError):
        _ = ComposedListingFilters.model_validate({"statuses": []})
    with pytest.raises(ValidationError):
        _ = ComposedListingFilters.model_validate({"statuses": ["available", "available"]})


def test_analysis_presence_filter_does_not_depend_on_bounded_output() -> None:
    status = ComposedStatus(
        value=ListingStatus.AVAILABLE,
        raw_flags={"is_live": True},
        evidence=_evidence("status", 1),
    )
    projection = ComposedListingProjection(
        listing_identifier="123",
        canonical_source_url=canonical_facebook_listing_url("123"),
        as_of_completion_sequence=1,
        projection_revision=projection_revision(
            listing_identifier="123",
            status=status,
            title=None,
            price=None,
            location=None,
            description=None,
            seller=None,
            preview_image=None,
            gallery=None,
            analyses=(),
            analyses_truncated=True,
            search_membership=None,
        ),
        status=status,
        title=None,
        price=None,
        location=None,
        description=None,
        seller=None,
        preview_image=None,
        gallery=None,
        analyses=(),
        analyses_truncated=True,
        search_membership=None,
    )
    filters = ComposedListingFilters(analysis=AnalysisPresenceFilter.PRESENT)

    assert composed_listing_matches_filters(
        projection,
        filters,
        matching_analysis_present=True,
    )


def test_projection_revision_ignores_evidence_record_churn_but_detects_value_changes() -> None:
    def revision(title: str, evidence_identifier: str):
        status = ComposedStatus(
            value=ListingStatus.AVAILABLE,
            raw_flags={"is_live": True},
            evidence=_evidence(evidence_identifier, 1),
        )
        return projection_revision(
            listing_identifier="123",
            status=status,
            title=ComposedField(value=title, evidence=_evidence(evidence_identifier, 1)),
            price=None,
            location=None,
            description=None,
            seller=None,
            preview_image=None,
            gallery=None,
            analyses=(),
            analyses_truncated=False,
            search_membership=None,
        )

    first = revision("Small fridge", "first-fetch")
    identical_refetch = revision("Small fridge", "second-fetch")
    changed_refetch = revision("Small fridge with freezer", "third-fetch")

    assert identical_refetch == first
    assert changed_refetch.scalar_fields_sha256 != first.scalar_fields_sha256
    assert changed_refetch.status_sha256 == first.status_sha256

    with pytest.raises(ValidationError):
        ListComposedSearchRequest(
            search_run_record_identifier="search-run",
            maximum_observations_per_listing=25,
        )


def test_projection_revision_ignores_url_churn_for_a_stable_photo_identifier() -> None:
    status = ComposedStatus(
        value=ListingStatus.AVAILABLE,
        raw_flags={"is_live": True},
        evidence=_evidence("status", 1),
    )

    def revision(url: str):
        return projection_revision(
            listing_identifier="123",
            status=status,
            title=None,
            price=None,
            location=None,
            description=None,
            seller=None,
            preview_image=ComposedPreviewImage(
                descriptor=ProjectionGalleryImageDescriptor(
                    gallery_order=0,
                    gallery_reference_record_identifier=None,
                    original_url=url,
                    photo_identifier="stable-photo",
                    declared_width=320,
                    declared_height=240,
                    image_result_record_identifier=None,
                    image_artifact_identifier=None,
                    download_state="not_yet_collected",
                    sha256=None,
                    media_type=None,
                    width=None,
                    height=None,
                ),
                evidence=_evidence("preview", 1),
            ),
            gallery=None,
            analyses=(),
            analyses_truncated=False,
            search_membership=None,
        )

    assert revision("https://cdn.example/first") == revision("https://cdn.example/second")


@pytest.mark.anyio
async def test_composed_search_queries_the_union_of_independent_lineages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = SearchRunCandidate(
        record_identifier="first-current",
        internal_search_run_identifier="first-internal",
        completion_sequence=10,
        started_at_utc=None,
        completed_at_utc=None,
        refresh_source_run_record_identifier=None,
        stopping_reason=SearchStoppingReason.NO_NEXT_PAGE,
    )
    second = SearchRunCandidate(
        record_identifier="second-current",
        internal_search_run_identifier="second-internal",
        completion_sequence=9,
        started_at_utc=None,
        completed_at_utc=None,
        refresh_source_run_record_identifier=None,
        stopping_reason=SearchStoppingReason.NO_NEXT_PAGE,
    )
    scope = SearchAncestrySelection(
        runs=(first, second),
        lineage_root_search_run_record_identifier=None,
        older_ancestry_truncated=False,
        warnings=("multiple_search_lineages",),
    )
    observed_runs: list[Sequence[tuple[str, str]]] = []

    class ScopeDatabase:
        async def current_completion_boundary(self) -> int:
            return 10

        async def facebook_projection_membership_candidates(
            self,
            included_search_runs: Sequence[tuple[str, str]],
            **_kwargs: Any,
        ) -> tuple[()]:
            observed_runs.append(included_search_runs)
            return ()

        async def facebook_projection_membership_occurrences(
            self,
            _listing_identifiers: Sequence[str],
            _included_search_runs: Sequence[tuple[str, str]],
            **_kwargs: Any,
        ) -> tuple[()]:
            return ()

    async def projection_scope(
        _application: ReviewApplication,
        primary_search_run_record_identifier: str,
        additional_search_run_record_identifiers: tuple[str, ...],
        **_kwargs: Any,
    ) -> SearchAncestrySelection:
        assert primary_search_run_record_identifier == "first-current"
        assert additional_search_run_record_identifiers == ("second-current",)
        return scope

    monkeypatch.setattr(ReviewApplication, "_projection_search_scope", projection_scope)
    application = ReviewApplication(
        cast(Database, cast(object, ScopeDatabase())),
        tmp_path,
    )
    page = await application.list_composed_search(
        ListComposedSearchRequest(
            search_run_record_identifier="first-current",
            additional_search_run_record_identifiers=("second-current",),
        )
    )

    assert observed_runs == [
        (("first-current", "first-internal"), ("second-current", "second-internal"))
    ]
    assert page.included_ancestry_run_count == 2
    assert page.listings == ()
