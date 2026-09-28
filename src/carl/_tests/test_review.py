"""Sans-I/O candidate review rules and durable guide authoring."""

from collections.abc import Iterator, Sequence
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from carl.core.components import Component, ComponentId
from carl.core.models import CodeProvenance, NamedOutput, RecordDraft
from carl.core.review import (
    AnalysisDescriptor,
    CandidateAnalysisFilter,
    CandidateAvailability,
    CandidateFilters,
    CandidateSource,
    CreateProductGuideRequest,
    ListCandidatesRequest,
    ProductGuideConflict,
    RequestAnalysisRequest,
    ReviseProductGuideRequest,
    SearchRefreshPhase,
    SetProductGuideIdentityRetiredRequest,
    WorkGroupProgress,
    candidate_availability,
    candidate_page,
    search_refresh_phase,
)
from carl.core.work import WorkState
from carl.io.claude import ClaudeCli
from carl.io.sqlite import Database
from carl.review import IncompleteGalleryError, ReviewApplication, ReviewInputError


def _field(value: object) -> dict[str, object]:
    return {
        "state": "present",
        "evidence": [{"state": "present", "normalized": value}],
    }


def _source(
    listing_identifier: str,
    completion_sequence: int,
    *,
    title: str,
    price: str = "100",
    availability: CandidateAvailability = CandidateAvailability.FULL_LISTING,
    analyses: tuple[AnalysisDescriptor, ...] = (),
) -> CandidateSource:
    return CandidateSource(
        listing_identifier=listing_identifier,
        observation_record_identifier=f"observation-{listing_identifier}-{completion_sequence}",
        acquisition_record_identifier=f"acquisition-{listing_identifier}-{completion_sequence}",
        availability=availability,
        acquisition_completion_sequence=completion_sequence,
        observation_completion_sequence=completion_sequence,
        observation={
            "listing_id": listing_identifier,
            "response_classification": {"kind": "full_listing"},
            "images": [],
            "fields": {
                "title": _field(title),
                "description": _field("Includes tripod"),
                "price": _field({"currency": "USD", "amount_decimal": price}),
                "location_text": _field("Example City, PA"),
            },
        },
        analyses=analyses,
    )


def _analysis(identifier: str, observation_identifier: str, sequence: int) -> AnalysisDescriptor:
    return AnalysisDescriptor(
        analysis_record_identifier=identifier,
        evidence_set_record_identifier=f"evidence-{identifier}",
        listing_observation_record_identifier=observation_identifier,
        product_guide_record_identifier="guide",
        completion_sequence=sequence,
        completed_at_utc="2026-09-26T00:00:00+00:00",
        state="completed",
        warnings=(),
        model="claude-sonnet-5",
    )


def test_candidate_pagination_is_stable_and_uses_latest_observation() -> None:
    original = (
        _source("1", 1, title="old telescope"),
        _source("1", 4, title="new telescope"),
        _source("2", 3, title="telescope mount", price="50"),
        _source("3", 2, title="tool chest", price="200"),
    )
    request = ListCandidatesRequest(
        filters=CandidateFilters(
            analysis=CandidateAnalysisFilter.ANY,
            currency="USD",
            maximum_price=Decimal("150"),
            text="telescope",
        ),
        page_size=1,
    )
    first = candidate_page(original, request)
    assert first.as_of_completion_sequence == 4
    assert tuple(item.listing_identifier for item in first.candidates) == ("1",)
    assert first.candidates[0].title == "new telescope"
    assert first.next_cursor is not None

    later = (
        *original,
        _source("4", 5, title="newly collected telescope", price="25"),
    )
    second = candidate_page(later, request.model_copy(update={"cursor": first.next_cursor}))
    assert second.as_of_completion_sequence == 4
    assert tuple(item.listing_identifier for item in second.candidates) == ("2",)
    assert second.next_cursor is None


def test_candidate_history_follows_listing_across_observations() -> None:
    old = _source("1", 1, title="old title")
    analysis = _analysis("analysis-old", old.observation_record_identifier, 3)
    old = old.model_copy(update={"analyses": (analysis,)})
    latest = _source("1", 4, title="latest title")

    present = candidate_page(
        (old, latest),
        ListCandidatesRequest(
            filters=CandidateFilters(
                analysis=CandidateAnalysisFilter.PRESENT,
                product_guide_record_identifier="guide",
            )
        ),
    )
    assert len(present.candidates) == 1
    summary = present.candidates[0]
    assert summary.observation_record_identifier == latest.observation_record_identifier
    assert summary.title == "latest title"
    assert summary.completed_analysis_count == 1
    assert summary.analyses == (analysis,)
    assert (
        candidate_page(
            (old, latest),
            ListCandidatesRequest(
                filters=CandidateFilters(analysis=CandidateAnalysisFilter.ABSENT)
            ),
        ).candidates
        == ()
    )


def test_candidate_availability_uses_normalized_sale_state() -> None:
    sold = {"fields": {"availability_sold": _field(True)}}
    pending = {"fields": {"availability_pending": _field(True)}}
    both = {
        "fields": {
            "availability_sold": _field(True),
            "availability_pending": _field(True),
        }
    }
    assert candidate_availability(sold, "full_listing") is CandidateAvailability.SOLD
    assert candidate_availability(pending, "full_listing") is CandidateAvailability.PENDING
    assert candidate_availability(both, "full_listing") is CandidateAvailability.SOLD
    assert candidate_availability({}, "full_listing") is CandidateAvailability.FULL_LISTING
    assert candidate_availability(sold, "listing_unavailable") is (
        CandidateAvailability.LISTING_UNAVAILABLE
    )

    sources = (
        _source("1", 1, title="available"),
        _source("2", 2, title="pending", availability=CandidateAvailability.PENDING),
        _source("3", 3, title="sold", availability=CandidateAvailability.SOLD),
    )
    assert tuple(
        item.listing_identifier
        for item in candidate_page(sources, ListCandidatesRequest()).candidates
    ) == ("1",)
    visible_sale_states = candidate_page(
        sources,
        ListCandidatesRequest(
            filters=CandidateFilters(
                availabilities=(CandidateAvailability.PENDING, CandidateAvailability.SOLD)
            )
        ),
    )
    assert tuple(item.listing_identifier for item in visible_sale_states.candidates) == ("3", "2")


@pytest.mark.parametrize("checkpoint_stage", ["search_complete", "collecting_items"])
def test_search_refresh_phase_distinguishes_checkpoint_from_child_progress(
    checkpoint_stage: str,
) -> None:
    item_pages = WorkGroupProgress(
        total=874,
        pending=713,
        leased=4,
        completed=157,
        terminal_failure=0,
    )
    empty = WorkGroupProgress(
        total=0,
        pending=0,
        leased=0,
        completed=0,
        terminal_failure=0,
    )

    phase = search_refresh_phase(
        state=WorkState.LEASED,
        checkpoint_stage=checkpoint_stage,
        item_pages=item_pages,
        item_extractions=empty,
        images=empty,
        image_extractions=empty,
    )

    assert phase is SearchRefreshPhase.COLLECTING_ITEM_PAGES


@pytest.mark.parametrize("value", ["20", 20, 20.5, Decimal("20")])
def test_candidate_price_filters_accept_json_scalar_forms(value: object) -> None:
    filters = CandidateFilters.model_validate({"currency": "USD", "minimum_price": value})

    assert filters.minimum_price == Decimal(str(value))


@pytest.mark.parametrize("value", [True, " 20", "NaN", float("inf"), object()])
def test_candidate_price_filters_reject_invalid_scalar_forms(value: object) -> None:
    with pytest.raises(ValidationError):
        CandidateFilters.model_validate({"currency": "USD", "minimum_price": value})


def test_candidate_filters_accept_decoded_json_availabilities() -> None:
    availabilities = ["full_listing", "listing_unavailable"]
    filters = CandidateFilters.model_validate({"availabilities": availabilities})
    availabilities.append("changed_by_caller")

    assert tuple(filters.availabilities) == (
        CandidateAvailability.FULL_LISTING,
        CandidateAvailability.LISTING_UNAVAILABLE,
    )


@pytest.mark.anyio
async def test_dossier_includes_analyses_from_older_observations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old = _source("1", 1, title="old title")
    analysis = _analysis("analysis-old", old.observation_record_identifier, 3)
    old = old.model_copy(update={"analyses": (analysis,)})
    latest = _source("1", 4, title="latest title")

    async def sources(
        _database: Database, listing_identifiers: Sequence[str] | None = None
    ) -> tuple[CandidateSource, ...]:
        assert listing_identifiers == ("1",)
        return (old, latest)

    monkeypatch.setattr(Database, "facebook_review_candidate_sources", sources)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        dossier = await ReviewApplication(database, tmp_path).get_listing_dossier("1")

    assert dossier.selected_observation_record_identifier == latest.observation_record_identifier
    assert dossier.fields["title"] == _field("latest title")
    assert dossier.analyses == (analysis,)
    assert dossier.analyses[0].listing_observation_record_identifier == (
        old.observation_record_identifier
    )


@pytest.mark.anyio
async def test_search_run_listing_membership_is_stably_paginated(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.begin_operation(
            operation_id="search-operation",
            component=Component(ComponentId(("test", "search_run")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-26T00:00:00+00:00",
        )
        await database.complete_operation(
            operation_id="search-operation",
            records=(
                RecordDraft(
                    identifier="search-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value={
                        "traversal": {
                            "unique_listing_identifiers": ["3", "1", "2"],
                        }
                    },
                ),
            ),
            artifacts=(),
            outputs=(NamedOutput(name=("search_run",), object_identifier="search-run"),),
            result={},
            ended_at_utc="2026-09-26T00:00:01+00:00",
            duration_ns=1,
        )
        application = ReviewApplication(database, tmp_path)
        first = await application.get_search_run_listings("search-run", offset=1, limit=1)
        second = await application.get_search_run_listings(
            "search-run", offset=first.next_offset or 0, limit=1
        )

    assert first.total_listing_identifiers == 3
    assert first.listing_identifiers == ("1",)
    assert first.next_offset == 2
    assert second.listing_identifiers == ("2",)
    assert second.next_offset is None


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash="a" * 40,
        worktree_state="clean",
        package_version="test",
        python_implementation="CPython",
        python_version="3.14",
        dependencies=(),
        lockfile_sha256=None,
    )


async def _async_provenance() -> CodeProvenance:
    return _provenance()


def _identifiers() -> Iterator[str]:
    index = 0
    while True:
        index += 1
        yield f"identifier-{index}"


def test_create_product_guide_request_accepts_decoded_json_identity() -> None:
    identity = [
        "carl",
        "product_guide",
        "carl",
        "product_guide",
        "compact_beverage_refrigerator",
    ]
    request = CreateProductGuideRequest.model_validate(
        {
            "identity": identity,
            "display_name": "Compact beverage refrigerators",
            "text": "Identify the unit and assess its condition.",
        }
    )
    identity.append("mutated_by_caller")

    assert tuple(request.identity) == ("compact_beverage_refrigerator",)


@pytest.mark.anyio
async def test_provenance_defaults_to_counting_without_loading_sibling_outputs(
    tmp_path: Path,
) -> None:
    records = tuple(
        RecordDraft(
            identifier=f"output-{index:03d}",
            kind=("test", "provenance_output"),
            schema_version=1,
            value={"index": index},
        )
        for index in range(250)
    )
    outputs = tuple(
        NamedOutput(name=("output",), object_identifier=record.identifier) for record in records
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.begin_operation(
            operation_id="many-output-operation",
            component=Component(ComponentId(("test", "many_outputs")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-26T00:00:00+00:00",
        )
        await database.complete_operation(
            operation_id="many-output-operation",
            records=records,
            artifacts=(),
            outputs=outputs,
            result={},
            ended_at_utc="2026-09-26T00:00:01+00:00",
            duration_ns=1,
        )
        application = ReviewApplication(database=database, repository_root=tmp_path)

        summary = await application.get_provenance("output-000")
        bounded = await application.get_provenance("output-000", maximum_output_edges=3)
        complete = await application.get_provenance("output-000", maximum_output_edges=None)

    assert summary.output_edge_count == 250
    assert summary.output_edges_truncated
    assert summary.outputs == []
    assert bounded.output_edge_count == 250
    assert bounded.output_edges_truncated
    assert len(bounded.outputs) == 3
    assert complete.output_edge_count == 250
    assert not complete.output_edges_truncated
    assert len(complete.outputs) == 250


@pytest.mark.anyio
async def test_product_guide_versions_use_optimistic_concurrency(tmp_path: Path) -> None:
    identifiers = _identifiers()
    claude = tmp_path / "claude"
    claude.write_text("#!/bin/sh\nprintf 'test-version\\n'\n")
    claude.chmod(0o700)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        application = ReviewApplication(
            database=database,
            repository_root=tmp_path,
            claude=ClaudeCli(executable=str(claude)),
            new_identifier=lambda: next(identifiers),
            utc_now_ns=lambda: 1_000_000_000,
            monotonic_ns=lambda: 2_000_000_000,
            code_provenance=_async_provenance,
        )
        first = await application.create_product_guide(
            CreateProductGuideRequest(
                identity=("telescope",),
                display_name="Telescopes",
                text="Identify optical design and aperture.",
            )
        )
        assert not isinstance(first, ProductGuideConflict)
        assert first.identity == ("carl", "product_guide", "telescope")
        assert first.version == 1

        duplicate = await application.create_product_guide(
            CreateProductGuideRequest(
                identity=("telescope",),
                display_name="Telescopes",
                text="Different text",
            )
        )
        assert isinstance(duplicate, ProductGuideConflict)
        assert duplicate.current_record_identifier == first.record_identifier

        second = await application.revise_product_guide(
            ReviseProductGuideRequest(
                expected_base_record_identifier=first.record_identifier,
                display_name="Telescopes",
                text="Identify optical design, aperture, and mount.",
            )
        )
        assert not isinstance(second, ProductGuideConflict)
        assert second.version == 2
        assert second.previous_record_identifier == first.record_identifier

        stale = await application.revise_product_guide(
            ReviseProductGuideRequest(
                expected_base_record_identifier=first.record_identifier,
                display_name="Telescopes",
                text="Stale revision",
            )
        )
        assert isinstance(stale, ProductGuideConflict)
        assert stale.current_record_identifier == second.record_identifier
        assert tuple(
            guide.record_identifier for guide in await application.list_product_guides()
        ) == (
            first.record_identifier,
            second.record_identifier,
        )
        retired = await application.set_product_guide_identity_retired(
            SetProductGuideIdentityRetiredRequest(
                product_guide_record_identifier=first.record_identifier,
            )
        )
        assert retired.retired
        assert await application.list_product_guides() == ()
        assert all(
            guide.retired
            for guide in await application.list_product_guides(include_retired=True)
        )
        assert (await application.get_product_guide(second.record_identifier)).retired
        with pytest.raises(ReviewInputError, match="Restore the retired"):
            await application.revise_product_guide(
                ReviseProductGuideRequest(
                    expected_base_record_identifier=second.record_identifier,
                    display_name="Telescopes",
                    text="Cannot revise while retired.",
                )
            )
        restored = await application.set_product_guide_identity_retired(
            SetProductGuideIdentityRetiredRequest(
                product_guide_record_identifier=second.record_identifier,
                retired=False,
            )
        )
        assert not restored.retired
        assert len(await application.list_product_guides()) == 2

        await database.begin_operation(
            operation_id="seed-observation-operation",
            component=Component(ComponentId(("test", "seed_observation")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-23T00:00:00+00:00",
        )
        await database.complete_operation(
            operation_id="seed-observation-operation",
            records=(
                RecordDraft(
                    identifier="observation-without-images",
                    kind=("carl", "facebook", "listing_observation"),
                    schema_version=1,
                    value={
                        "listing_id": "123",
                        "response_classification": {"kind": "full_listing"},
                        "images": [],
                        "fields": {},
                    },
                ),
            ),
            artifacts=(),
            outputs=(
                NamedOutput(
                    name=("listing_observation",),
                    object_identifier="observation-without-images",
                ),
            ),
            result={},
            ended_at_utc="2026-09-23T00:00:01+00:00",
            duration_ns=1,
        )
        request = RequestAnalysisRequest(
            listing_observation_record_identifier="observation-without-images",
            product_guide_record_identifier=second.record_identifier,
        )
        with pytest.raises(IncompleteGalleryError) as caught:
            await application.request_listing_analysis(request)
        assert caught.value.gallery_absence_reason == (
            "listing_observation_has_no_gallery_references"
        )

        requested = await application.request_listing_analysis(
            request.model_copy(update={"allow_incomplete_gallery": True})
        )
        assert requested.included_gallery_count == 0
        assert requested.gallery_absence_reason == ("listing_observation_has_no_gallery_references")
        assert requested.created
        _, _, evidence = await database.get_record(requested.evidence_set_record_identifier)
        assert evidence["gallery_absence_reason"] == (
            "listing_observation_has_no_gallery_references"
        )
        reused = await application.request_listing_analysis(
            request.model_copy(update={"allow_incomplete_gallery": True}),
            requester_kind=("carl", "facebook", "analysis_batch"),
            requester_identifier="batch",
            requester_context={"source": "test"},
        )
        assert not reused.created
        assert reused.work_identifier == requested.work_identifier
        assert await database.requested_work_identifiers(
            requester_kind=("carl", "facebook", "analysis_batch"),
            requester_identifier="batch",
        ) == (requested.work_identifier,)
