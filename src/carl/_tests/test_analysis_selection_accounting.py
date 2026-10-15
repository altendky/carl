"""Selection exclusions survive durable analysis-batch continuation and restart."""

# pyright: reportPrivateUsage=false

from pathlib import Path
from typing import cast

import pytest

from carl._tests.test_analysis_batch import _candidate_source
from carl._tests.test_ebay_item_workers import _enqueue, _identifiers, _mapping, _run_next
from carl._tests.test_explicit_analysis_selection import _application
from carl._tests.test_marketplace_listing import _publish, _record
from carl._tests.test_selection_analysis_freshness import _track, _workspace
from carl.analysis_batch_workers import (
    AnalysisBatchWorkerDependencies,
    build_analysis_batch_worker_registry,
)
from carl.core.analysis_batch import (
    REQUEST_MISSING_ANALYSES_WORK_KIND,
    AnalysisSelectionSkip,
    ListingAnalysisSelectionPolicy,
    RequestMissingAnalysesPayload,
    RequestMissingListingAnalysesRequest,
    SelectionAnalysesRequest,
    request_missing_analyses_work,
)
from carl.core.composed_projection import (
    ComposedListingProjection,
    GetComposedListingRequest,
    ListingStatus,
)
from carl.core.json import decode_json
from carl.core.models import JsonValue, RecordDraft
from carl.core.review import (
    CandidateAvailability,
    CandidateSource,
    RequestAnalysisRequest,
    RequestAnalysisResult,
)
from carl.core.review_errors import ReviewInputError
from carl.core.review_workspace import ListingIdsSelection
from carl.io.sqlite import Database
from carl.review import ReviewApplication


class _RejectingApplication:
    def __init__(self) -> None:
        self.requests: list[str] = []

    async def request_listing_analysis(
        self,
        request: RequestAnalysisRequest,
        *,
        requester_kind: tuple[str, ...],
        requester_identifier: str | None,
        requester_context: JsonValue | None,
    ) -> RequestAnalysisResult:
        assert requester_kind == ("carl", "facebook", "analysis_batch")
        assert requester_identifier == "batch"
        assert isinstance(requester_context, dict)
        self.requests.append(request.listing_observation_record_identifier)
        # The message deliberately contains no listing ID. The batch must attach
        # the selected listing identity independently of the exception wording.
        raise ReviewInputError("Retained gallery is incomplete")


async def _sources(database: Database, observations: tuple[str, ...]) -> None:
    await _publish(
        database,
        tuple(
            RecordDraft(identifier=identifier, kind=("test", "source"), schema_version=1, value={})
            for identifier in ("guide", *observations)
        ),
    )


def _planning_skip() -> AnalysisSelectionSkip:
    return AnalysisSelectionSkip(
        listing_identifier="999",
        kind="outside_workspace",
        message="The listing is outside the active workspace.",
    )


@pytest.mark.anyio
async def test_preplanned_skips_survive_checkpoint_restart_without_duplication(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    identifiers = _identifiers()
    application = _RejectingApplication()
    payload = RequestMissingAnalysesPayload(
        request_identifier="request",
        source_search_refresh_work_identifier="refresh",
        source_search_refresh_work_identifiers=("refresh", "refresh-second"),
        source_as_of_completion_sequence=10,
        listing_observation_record_identifiers=("observation-123", "observation-456"),
        product_guide_record_identifier="guide",
        allow_incomplete_gallery=False,
        selection_skipped=(_planning_skip(),),
        listing_identifiers_by_observation={
            "observation-123": "123",
            "observation-456": "456",
        },
    )
    async with Database.managed(path, initialize=True) as database:
        await _sources(database, payload.listing_observation_record_identifiers)
        _ = await _enqueue(
            database,
            request_missing_analyses_work(identifier="batch", payload=payload),
            identifiers,
        )
        registry = build_analysis_batch_worker_registry(
            AnalysisBatchWorkerDependencies(database, application)
        )
        first = await _run_next(database, registry, identifiers, REQUEST_MISSING_ANALYSES_WORK_KIND)
        assert first["state"] == "pending"
        checkpoint = _mapping(first["result"])
        assert checkpoint["next_observation_index"] == 1
        assert checkpoint["skipped"] == [
            _planning_skip().model_dump(mode="json"),
            {
                "listing_observation_record_identifier": "observation-123",
                "listing_identifier": "123",
                "kind": "ReviewInputError",
                "message": "Retained gallery is incomplete",
            },
        ]

    # Opening a new Database and rebuilding the registry exercises the retained
    # result, with no in-memory checkpoint or worker state carried forward.
    async with Database.managed(path) as database:
        registry = build_analysis_batch_worker_registry(
            AnalysisBatchWorkerDependencies(database, application)
        )
        second = await _run_next(
            database, registry, identifiers, REQUEST_MISSING_ANALYSES_WORK_KIND
        )
        assert second["state"] == "completed"
        result = _mapping(second["result"])
        skip_value = result["skipped"]
        assert isinstance(skip_value, list)
        skips = cast(list[JsonValue], skip_value)
        assert len(skips) == 3
        assert skips[:2] == checkpoint["skipped"]
        assert _mapping(skips[2])["listing_identifier"] == "456"
        assert _mapping(skips[2])["kind"] == "ReviewInputError"
        assert result["next_observation_index"] == 2
        assert application.requests == ["observation-123", "observation-456"]
        status = await ReviewApplication(database, tmp_path).get_work_status("batch")
        assert status.analysis_batch_progress is not None
        assert status.analysis_batch_progress.selected_observations == 2
        assert status.analysis_batch_progress.processed_observations == 2
        assert status.analysis_batch_progress.remaining_observations == 0
        assert status.analysis_batch_progress.skipped_observations == 3


@pytest.mark.anyio
async def test_empty_selected_batch_keeps_all_planning_exclusions(tmp_path: Path) -> None:
    identifiers = _identifiers()
    application = _RejectingApplication()
    payload = RequestMissingAnalysesPayload(
        request_identifier="request",
        source_search_refresh_work_identifier="refresh",
        source_as_of_completion_sequence=10,
        listing_observation_record_identifiers=(),
        product_guide_record_identifier="guide",
        allow_incomplete_gallery=False,
        selection_skipped=(_planning_skip(),),
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _sources(database, ())
        _ = await _enqueue(
            database,
            request_missing_analyses_work(identifier="batch", payload=payload),
            identifiers,
        )
        registry = build_analysis_batch_worker_registry(
            AnalysisBatchWorkerDependencies(database, application)
        )
        completed = await _run_next(
            database, registry, identifiers, REQUEST_MISSING_ANALYSES_WORK_KIND
        )
        assert completed["state"] == "completed"
        assert _mapping(completed["result"])["skipped"] == [
            _planning_skip().model_dump(mode="json")
        ]
        assert application.requests == []


@pytest.mark.anyio
@pytest.mark.parametrize("schema_version", [1, 2, 3, 4])
async def test_retained_payload_versions_remain_executable(
    tmp_path: Path, schema_version: int
) -> None:
    identifiers = _identifiers()
    payload = RequestMissingAnalysesPayload(
        request_identifier="request",
        source_search_refresh_work_identifier="refresh",
        source_as_of_completion_sequence=10,
        listing_observation_record_identifiers=(),
        product_guide_record_identifier="guide",
        allow_incomplete_gallery=False,
    )
    retained = payload.model_dump(mode="json")
    if schema_version < 4:
        retained.pop("selection_skipped")
        retained.pop("listing_identifiers_by_observation")
    if schema_version < 3:
        retained.pop("source_search_refresh_work_identifiers")
    if schema_version == 1:
        retained.pop("selection_policy")
    definition = request_missing_analyses_work(identifier="batch", payload=payload).model_copy(
        update={"payload_schema_version": schema_version, "payload": retained}
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _sources(database, ())
        _ = await _enqueue(database, definition, identifiers)
        registry = build_analysis_batch_worker_registry(
            AnalysisBatchWorkerDependencies(database, _RejectingApplication())
        )
        completed = await _run_next(
            database, registry, identifiers, REQUEST_MISSING_ANALYSES_WORK_KIND
        )
        assert completed["state"] == "completed"
        assert _mapping(completed["result"])["skipped"] == []


@pytest.mark.anyio
async def test_exact_ebay_status_uses_global_item_evidence_without_reapplying_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    item = "256123456789"
    original = ReviewApplication.get_composed_listing
    requests: list[GetComposedListingRequest] = []

    async def get_listing(
        app: ReviewApplication, request: GetComposedListingRequest
    ) -> ComposedListingProjection:
        assert request.search_run_record_identifier is None
        assert request.additional_search_run_record_identifiers == ()
        assert request.maximum_gallery_images == request.maximum_analyses == 0
        requests.append(request)
        return await original(app, request)

    monkeypatch.setattr(ReviewApplication, "get_composed_listing", get_listing)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                _record(
                    "detail",
                    "listing_observation",
                    item_identifier=item,
                    classification="detail",
                    title="Telescope",
                    gallery_urls=[],
                ),
            ),
        )
        app = ReviewApplication(database, tmp_path)
        statuses = await app._selected_workspace_analysis_statuses(
            _workspace((_track("ebay", "already-checked-complete-ancestry"),)), (f"ebay:{item}",)
        )
        assert statuses == {f"ebay:{item}": ListingStatus.AVAILABLE}
        assert len(requests) == 1


@pytest.mark.anyio
async def test_all_outside_ids_are_identified_when_selection_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = await _application(database, tmp_path, monkeypatch, ("123",), ("123",))
        with pytest.raises(ReviewInputError) as captured:
            _ = await app.request_selection_analyses(
                SelectionAnalysesRequest(
                    workspace_record_identifier="workspace",
                    selection=ListingIdsSelection(listing_identifiers=("999", "888")),
                )
            )
        _, separator, exclusions = str(captured.value).partition("skipped=")
        assert separator
        assert decode_json(exclusions) == [
            {"listing_identifier": "999", "kind": "outside_workspace"},
            {"listing_identifier": "888", "kind": "outside_workspace"},
        ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "policy,availability,expected",
    [
        (
            ListingAnalysisSelectionPolicy.NEVER_ANALYZED_LISTING,
            CandidateAvailability.FULL_LISTING,
            "selection_policy_excluded",
        ),
        (
            ListingAnalysisSelectionPolicy.MISSING_FOR_SELECTED_GUIDE,
            CandidateAvailability.FULL_LISTING,
            "analysis_reused",
        ),
        (
            ListingAnalysisSelectionPolicy.MISSING_FOR_CURRENT_EVIDENCE,
            CandidateAvailability.LISTING_UNAVAILABLE,
            "availability_excluded",
        ),
    ],
)
async def test_planning_exclusions_report_listing_and_policy_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    policy: ListingAnalysisSelectionPolicy,
    availability: CandidateAvailability,
    expected: str,
) -> None:
    original_work = Database.work

    async def active_guide(_app: ReviewApplication, _identifier: str) -> None:
        return None

    async def work(database: Database, identifier: str) -> dict[str, JsonValue]:
        if identifier == "refresh":
            return {
                "kind": ["carl", "facebook", "work", "refresh_search"],
                "state": "completed",
                "payload": {"base_search_run_record_identifier": "base"},
                "result": {"refreshed_search_run_record_identifier": "fresh"},
            }
        return await original_work(database, identifier)

    async def sources(
        _database: Database, identifiers: tuple[str, ...]
    ) -> tuple[CandidateSource, ...]:
        assert identifiers == ("123",)
        return (
            _candidate_source("123", "observation", analyzed=True).model_copy(
                update={"availability": availability}
            ),
        )

    monkeypatch.setattr(ReviewApplication, "_active_product_guide", active_guide)
    monkeypatch.setattr(Database, "work", work)
    monkeypatch.setattr(Database, "facebook_review_candidate_sources", sources)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        plan = await ReviewApplication(database, tmp_path)._plan_missing_listing_analyses(
            RequestMissingListingAnalysesRequest(
                search_refresh_work_identifier="refresh",
                product_guide_record_identifier="guide",
                selection_policy=policy,
                report_listing_skips=True,
            ),
            restrict_listing_identifiers=frozenset({"123"}),
            source_listing_identifiers=frozenset({"123"}),
        )
        assert plan.selected_observation_record_identifiers == ()
        assert len(plan.skipped) == 1
        assert plan.skipped[0].listing_identifier == "123"
        assert plan.skipped[0].listing_observation_record_identifier == "observation"
        assert plan.skipped[0].kind == expected
