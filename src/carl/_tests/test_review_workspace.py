"""Review workspace state and optimistic grouping tests."""

import sqlite3
from pathlib import Path

import anyio
import pytest
from pydantic import ValidationError

from carl.core.analysis_batch import (
    ListingAnalysisBatchSelection,
    RequestMissingListingAnalysesRequest,
    SelectionAnalysesRequest,
)
from carl.core.components import Component, ComponentId
from carl.core.composed_projection import (
    ComposedGallery,
    ComposedListingPage,
    ComposedListingProjection,
    ComposedStatus,
    ListComposedSearchRequest,
    ListingStatus,
    ProjectionEvidence,
    ProjectionRevision,
    ProjectionSourceKind,
    SearchAncestrySelection,
    SearchMembershipOccurrenceCandidate,
    SearchRunCandidate,
    canonical_facebook_listing_url,
)
from carl.core.facebook_work import COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION, COLLECT_SEARCH_WORK_KIND
from carl.core.json import encode_json
from carl.core.models import CodeProvenance, NamedOutput, RecordDraft
from carl.core.review import (
    CandidateAvailability,
    CreateProductGuideRequest,
    ProductGuideConflict,
    ReviseProductGuideRequest,
    SetProductGuideIdentityRetiredRequest,
)
from carl.core.review_workspace import (
    ACQUIRE_REVIEW_BATCH,
    CREATE_REVIEW_WORKSET,
    LISTING_REVIEW_KIND,
    RECORD_WORKSPACE_BULK_REVIEW,
    REVIEW_WORKSPACE_IDENTITY_STATE_KIND,
    REVIEW_WORKSPACE_KIND,
    AcquireReviewBatchRequest,
    AddWorkspaceProductGuideRequest,
    CreateReviewBatchRequest,
    CreateReviewWorksetRequest,
    CreateReviewWorkspaceRequest,
    CreateSelectionSnapshotRequest,
    CreateWorkspaceSearchRequest,
    ListingReviewInput,
    ListingReviewRecord,
    ListWorkspaceListingsRequest,
    ProjectionRevisionComponent,
    RecordListingReviewsRequest,
    RecordWorkspaceBulkReviewRequest,
    RecordWorkspaceBulkReviewResult,
    ReleaseReviewClaimRequest,
    RenameReviewWorkspaceRequest,
    RenewReviewClaimRequest,
    RequestWorkspaceRefreshRequest,
    RetryWorkspaceSearchTrackRequest,
    ReviewBatch,
    ReviewBatchItem,
    ReviewBatchSelection,
    ReviewDisposition,
    ReviewStalenessPolicy,
    ReviewState,
    ReviewWorksetConflict,
    ReviewWorkspace,
    SelectionSnapshotBulkReviewSelection,
    SetReviewWorkspaceArchivedRequest,
    SetWorkspaceDefaultProductGuideRequest,
    SetWorkspaceSearchTrackEnabledRequest,
    UpdateReviewWorksetRequest,
    UpdateWorkspaceProductGuideBindingRequest,
    WorksetBulkReviewSelection,
    WorkspaceProductGuideBinding,
    WorkspaceProductGuideVersionPolicy,
    WorkspaceWorkStatus,
    build_review_workspace_component_registry,
    changed_projection_components,
    classify_review_state,
)
from carl.core.work import WorkCapability, WorkState
from carl.io.claude import ClaudeCli
from carl.io.sqlite import Database
from carl.review import ReviewApplication, ReviewInputError


def _revision(*, scalar: str = "b", gallery: str = "d") -> ProjectionRevision:
    return ProjectionRevision(
        status_sha256="a" * 64,
        scalar_fields_sha256=scalar * 64,
        preview_image_sha256="c" * 64,
        gallery_sha256=gallery * 64,
        analyses_sha256="e" * 64,
        search_membership_sha256="f" * 64,
        aggregate_sha256="0" * 64,
    )


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="unknown",
        package_version="test",
        python_implementation="CPython",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


def _search_run_value(query: str) -> dict[str, object]:
    return {
        "request": {
            "query": query,
            "location": {
                "kind": "facebook_location",
                "identifier": "123",
                "label": None,
            },
            "radius": {"value": 60, "unit": "miles"},
            "price": {"currency": "USD", "minimum": None, "maximum": "600"},
            "exact_match": False,
        },
        "traversal_strategy": {"kind": "cursor"},
        "traversal": {
            "policy": {
                "maximum_pages": 10,
                "maximum_results": None,
                "maximum_elapsed_duration_ns": None,
                "maximum_transferred_bytes": None,
                "maximum_decoded_body_bytes": None,
                "maximum_consecutive_pages_without_new_listings": None,
                "requested_page_size": None,
            },
            "unique_listing_identifiers": ["1"],
        },
    }


@pytest.mark.anyio
async def test_workspace_search_and_refresh_are_tracks_and_workspace_work(
    tmp_path: Path,
) -> None:
    component = Component(ComponentId(("test", "search")), 1, lambda: None)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.publish_records_operation(
            component=component,
            operation_identifier="search-operation",
            records=(
                RecordDraft(
                    identifier="search-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value=_search_run_value("telescope"),
                ),
            ),
            inputs=(),
            outputs=(NamedOutput(name=("search_run",), object_identifier="search-run"),),
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-09-27T00:00:00+00:00",
            ended_at_utc="2026-09-27T00:00:01+00:00",
            duration_ns=1,
            result={"state": "completed"},
        )

        async def code_provenance() -> CodeProvenance:
            return _provenance()

        application = ReviewApplication(database, tmp_path, code_provenance=code_provenance)
        workspace = await application.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Telescopes", search_run_record_identifier="search-run"
            )
        )
        assert [(track.track_identifier, track.query) for track in workspace.search_tracks] == [
            ("search-run", "telescope")
        ]
        renamed = await application.rename_review_workspace(
            RenameReviewWorkspaceRequest(
                workspace_record_identifier=workspace.record_identifier,
                name="Astronomy",
            )
        )
        archived = await application.set_review_workspace_archived(
            SetReviewWorkspaceArchivedRequest(
                workspace_record_identifier=workspace.record_identifier,
                archived=True,
            )
        )
        assert await application.list_review_workspaces() == ()
        listed_archived = await application.list_review_workspaces(include_archived=True)
        restored = await application.set_review_workspace_archived(
            SetReviewWorkspaceArchivedRequest(
                workspace_record_identifier=workspace.record_identifier,
                archived=False,
            )
        )
        identity_states = await database.records_by_kind(REVIEW_WORKSPACE_IDENTITY_STATE_KIND)

        added = await application.create_workspace_search(
            CreateWorkspaceSearchRequest.model_validate_json(
                encode_json(
                    {
                        "workspace_record_identifier": workspace.record_identifier,
                        "search": {
                            "request": {
                                "query": "astronomical telescope",
                                "location": {
                                    "kind": "facebook_location",
                                    "identifier": "123",
                                    "label": "Example City, PA",
                                },
                                "radius": {"value": 60, "unit": "miles"},
                            },
                            "traversal": {"maximum_pages": 10},
                        },
                    }
                )
            )
        )
        refreshed = await application.request_workspace_refresh(
            RequestWorkspaceRefreshRequest(workspace_record_identifier=workspace.record_identifier)
        )
        updated = await application.get_review_workspace(workspace.record_identifier)
        status = await application.get_workspace_work_status(workspace.record_identifier)
        disabled = await application.set_workspace_search_track_enabled(
            SetWorkspaceSearchTrackEnabledRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier="search-run",
                enabled=False,
            )
        )
        disabled_workspace = await application.get_review_workspace(workspace.record_identifier)

    assert added.track_identifier == added.work_identifier
    assert renamed.name == "Astronomy"
    assert not renamed.archived
    assert archived.archived
    assert listed_archived == (archived,)
    assert restored.name == "Astronomy"
    assert not restored.archived
    assert [
        ({"name": value.get("name")} if "name" in value else {"archived": value["archived"]})
        for _, value in identity_states
        if isinstance(value, dict)
    ] == [{"name": "Astronomy"}, {"archived": True}, {"archived": False}]
    assert refreshed.track_identifier == "search-run"
    assert refreshed.base_search_run_record_identifier == "search-run"
    assert [(track.query, track.creation_work_state) for track in updated.search_tracks] == [
        ("telescope", None),
        ("astronomical telescope", WorkState.PENDING),
    ]
    assert updated.search_tracks[0].latest_refresh_work_identifier == refreshed.work_identifier
    assert updated.search_tracks[0].latest_refresh_work_state is WorkState.PENDING
    assert status.queued_count == 2
    assert {work.identifier for work in status.active_work} == {
        added.work_identifier,
        refreshed.work_identifier,
    }
    assert not disabled.enabled
    assert not disabled_workspace.search_tracks[0].enabled


@pytest.mark.anyio
async def test_failed_workspace_search_track_is_visible_and_retryable(tmp_path: Path) -> None:
    component = Component(ComponentId(("test", "search")), 1, lambda: None)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.publish_records_operation(
            component=component,
            operation_identifier="initial-search-operation",
            records=(
                RecordDraft(
                    identifier="initial-search-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value=_search_run_value("telescope"),
                ),
            ),
            inputs=(),
            outputs=(NamedOutput(name=("search_run",), object_identifier="initial-search-run"),),
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-09-28T00:00:00+00:00",
            ended_at_utc="2026-09-28T00:00:01+00:00",
            duration_ns=1,
            result={"state": "completed"},
        )
        application = ReviewApplication(database, tmp_path)
        workspace = await application.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Astronomy", search_run_record_identifier="initial-search-run"
            )
        )
        added = await application.create_workspace_search(
            CreateWorkspaceSearchRequest.model_validate_json(
                encode_json(
                    {
                        "workspace_record_identifier": workspace.record_identifier,
                        "search": {
                            "request": {
                                "query": "binoculars",
                                "location": {
                                    "kind": "facebook_location",
                                    "identifier": "123",
                                },
                                "radius": {"value": 60, "unit": "miles"},
                            },
                            "traversal": {"maximum_pages": 10},
                        },
                    }
                )
            )
        )
        now_ns = 2 * 10**18
        with sqlite3.connect(tmp_path / "carl.sqlite3") as connection:
            connection.execute(
                """
                DELETE FROM work_scopes
                WHERE work_item_id = ? AND scope_kind = 'network_path'
                  AND scope_identity_json = ?
                """,
                (
                    added.work_identifier,
                    '["search_acquisition","proton","personal","carl"]',
                ),
            )
        claim = await database.claim_work(
            supported_capabilities=(
                WorkCapability(
                    kind=COLLECT_SEARCH_WORK_KIND,
                    payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
                ),
            ),
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=100,
            utc_now_ns=lambda: now_ns,
            event_identifier="claimed",
            eligible_identifiers=(added.work_identifier,),
        )
        assert claim.lease is not None
        await database.begin_leased_operation(
            work_item_identifier=added.work_identifier,
            lease_token="lease",
            worker_identifier="worker",
            lease_duration_ns=100,
            utc_now_ns=lambda: now_ns + 1,
            event_identifier="operation-bound",
            operation_id="failed-search-operation",
            component=component,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-28T00:00:02+00:00",
        )
        await database.terminally_fail_leased_operation(
            work_item_identifier=added.work_identifier,
            lease_token="lease",
            worker_identifier="worker",
            utc_now_ns=lambda: now_ns + 2,
            event_identifier="failed",
            operation_id="failed-search-operation",
            error={
                "kind": "search_acquisition_failure",
                "stopping_condition": "transport_failure",
                "exception_type": "RemoteProtocolError",
                "decision": "retry_exhausted",
            },
            result={"state": "acquisition_failed"},
            ended_at_utc="2026-09-28T00:00:03+00:00",
            duration_ns=1,
        )

        failed_status = await application.get_workspace_work_status(workspace.record_identifier)
        retried = await application.retry_workspace_search_track(
            RetryWorkspaceSearchTrackRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=added.track_identifier,
            )
        )
        retried_work = await database.work(added.work_identifier)
        active_status = await application.get_workspace_work_status(workspace.record_identifier)
        with sqlite3.connect(tmp_path / "carl.sqlite3") as connection:
            restored_search_route_scopes = connection.execute(
                """
                SELECT count(*) FROM work_scopes
                WHERE work_item_id = ? AND scope_kind = 'network_path'
                  AND scope_identity_json = ?
                """,
                (
                    added.work_identifier,
                    '["search_acquisition","proton","personal","carl"]',
                ),
            ).fetchone()

    assert failed_status.idle
    assert not failed_status.successful
    assert failed_status.terminal_failure_count == 1
    assert [work.identifier for work in failed_status.failed_work] == [added.work_identifier]
    assert retried.track_identifier == added.track_identifier
    assert retried.work_identifier == added.work_identifier
    assert retried.previous_attempt_count == 1
    assert retried_work["payload_schema_version"] == COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION
    assert retried_work["payload"]["retry_attempt_offset"] == 1
    assert restored_search_route_scopes == (1,)
    assert active_status.queued_count == 1
    assert active_status.terminal_failure_count == 0
    assert not active_status.idle
    assert not active_status.successful


@pytest.mark.anyio
async def test_workspace_product_guide_bindings_pin_follow_and_select_defaults(
    tmp_path: Path,
) -> None:
    claude = tmp_path / "claude"
    claude.write_text("#!/bin/sh\nprintf 'test-version\\n'\n")
    claude.chmod(0o700)
    component = Component(ComponentId(("test", "search")), 1, lambda: None)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.publish_records_operation(
            component=component,
            operation_identifier="search-operation",
            records=(
                RecordDraft(
                    identifier="search-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value=_search_run_value("astronomy"),
                ),
            ),
            inputs=(),
            outputs=(NamedOutput(name=("search_run",), object_identifier="search-run"),),
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-09-27T00:00:00+00:00",
            ended_at_utc="2026-09-27T00:00:01+00:00",
            duration_ns=1,
            result={"state": "completed"},
        )

        async def code_provenance() -> CodeProvenance:
            return _provenance()

        application = ReviewApplication(
            database,
            tmp_path,
            claude=ClaudeCli(executable=str(claude)),
            code_provenance=code_provenance,
        )
        first = await application.create_product_guide(
            CreateProductGuideRequest(
                identity=("astronomy_gear",),
                display_name="Astronomy gear",
                text="Assess telescopes.",
            )
        )
        assert not isinstance(first, ProductGuideConflict)
        workspace = await application.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Astronomy",
                search_run_record_identifier="search-run",
                product_guide_record_identifier=first.record_identifier,
            )
        )
        with pytest.raises(ReviewInputError, match="Disable this product-guide identity"):
            await application.set_product_guide_identity_retired(
                SetProductGuideIdentityRetiredRequest(
                    product_guide_record_identifier=first.record_identifier,
                )
            )
        second = await application.revise_product_guide(
            ReviseProductGuideRequest(
                expected_base_record_identifier=first.record_identifier,
                display_name="Astronomy gear",
                text="Assess telescopes, binoculars, and eyepieces.",
            )
        )
        assert not isinstance(second, ProductGuideConflict)
        pinned = workspace.product_guide_bindings[0]
        followed = await application.add_workspace_product_guide(
            AddWorkspaceProductGuideRequest(
                workspace_record_identifier=workspace.record_identifier,
                product_guide_record_identifier=first.record_identifier,
                alias="astronomy_gear",
                version_policy=WorkspaceProductGuideVersionPolicy.FOLLOW_LATEST,
                make_default=True,
            )
        )
        revised_workspace = await application.get_review_workspace(workspace.record_identifier)
        selected_by_record = application._selected_workspace_product_guide(
            revised_workspace, None, second.record_identifier
        )
        renamed_and_pinned = await application.update_workspace_product_guide_binding(
            UpdateWorkspaceProductGuideBindingRequest(
                workspace_record_identifier=workspace.record_identifier,
                binding_identifier=followed.binding_identifier,
                alias="astronomy_reference",
                version_policy=WorkspaceProductGuideVersionPolicy.PINNED,
                product_guide_record_identifier=first.record_identifier,
            )
        )
        cleared_default = await application.set_workspace_default_product_guide(
            SetWorkspaceDefaultProductGuideRequest(
                workspace_record_identifier=workspace.record_identifier,
                binding_identifier=None,
            )
        )
        with pytest.raises(ReviewInputError, match="multiple enabled guides"):
            application._selected_workspace_product_guide(cleared_default, None)
        assert application._selected_workspace_product_guide(
            cleared_default, followed.binding_identifier
        ) == (followed.binding_identifier, first.record_identifier)
        defaulted = await application.set_workspace_default_product_guide(
            SetWorkspaceDefaultProductGuideRequest(
                workspace_record_identifier=workspace.record_identifier,
                binding_identifier=pinned.binding_identifier,
            )
        )
        disabled = await application.update_workspace_product_guide_binding(
            UpdateWorkspaceProductGuideBindingRequest(
                workspace_record_identifier=workspace.record_identifier,
                binding_identifier=followed.binding_identifier,
                enabled=False,
            )
        )
        enabled_bindings = await application.list_workspace_product_guides(
            workspace.record_identifier
        )
        all_bindings = await application.list_workspace_product_guides(
            workspace.record_identifier, include_disabled=True
        )

    assert pinned.version_policy is WorkspaceProductGuideVersionPolicy.PINNED
    assert pinned.resolved_product_guide_record_identifier == first.record_identifier
    assert followed.version_policy is WorkspaceProductGuideVersionPolicy.FOLLOW_LATEST
    assert followed.resolved_product_guide_record_identifier == second.record_identifier
    assert selected_by_record == (followed.binding_identifier, second.record_identifier)
    assert revised_workspace.product_guide_record_identifier == second.record_identifier
    assert renamed_and_pinned.alias == "astronomy_reference"
    assert renamed_and_pinned.resolved_product_guide_record_identifier == first.record_identifier
    assert defaulted.product_guide_record_identifier == first.record_identifier
    assert not disabled.enabled
    assert enabled_bindings == (pinned,)
    assert len(all_bindings) == 2


def test_selection_analysis_rejects_two_guide_selectors() -> None:
    with pytest.raises(ValidationError, match="not both"):
        SelectionAnalysesRequest(
            workspace_record_identifier="workspace",
            product_guide_binding_identifier="binding",
            product_guide_record_identifier="guide",
        )


def test_review_state_uses_only_policy_components() -> None:
    previous = ListingReviewRecord(
        record_identifier="review",
        workspace_record_identifier="workspace",
        batch_record_identifier=None,
        listing_identifier="123",
        projection_revision=_revision(),
        inspected=True,
        disposition=ReviewDisposition.PROMISING,
        note=None,
        recorded_at_utc="2026-09-27T00:00:00+00:00",
    )
    current = _revision(scalar="1", gallery="2")

    assert changed_projection_components(previous.projection_revision, current) == (
        ProjectionRevisionComponent.SCALAR_FIELDS,
        ProjectionRevisionComponent.GALLERY,
    )
    state, changed = classify_review_state(
        current_revision=current,
        previous_review=previous,
        policy=ReviewStalenessPolicy(components=(ProjectionRevisionComponent.SCALAR_FIELDS,)),
    )
    assert state is ReviewState.STALE
    assert changed == (ProjectionRevisionComponent.SCALAR_FIELDS,)

    recipe_change = current.model_copy(update={"recipe_version": 2})
    state, changed = classify_review_state(
        current_revision=recipe_change,
        previous_review=previous,
        policy=ReviewStalenessPolicy(components=(ProjectionRevisionComponent.SCALAR_FIELDS,)),
    )
    assert state is ReviewState.STALE
    assert changed == (ProjectionRevisionComponent.SCALAR_FIELDS,)

    with pytest.raises(ValidationError, match="actually inspected"):
        ListingReviewInput(
            listing_identifier="123",
            projection_revision=current,
            inspected=False,
        )


@pytest.mark.anyio
async def test_workspace_analysis_preview_resolves_claimed_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    projection = ComposedListingProjection(
        listing_identifier="123",
        canonical_source_url=canonical_facebook_listing_url("123"),
        as_of_completion_sequence=10,
        projection_revision=_revision(),
        status=ComposedStatus(
            value=ListingStatus.AVAILABLE,
            raw_flags={"is_live": True},
            evidence=ProjectionEvidence(
                evidence_record_identifier="status",
                acquisition_completion_sequence=10,
                observation_completion_sequence=10,
                source_kind=ProjectionSourceKind.ITEM_PAGE,
            ),
        ),
        title=None,
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
    workspace = ReviewWorkspace(
        record_identifier="workspace",
        name="Fridges",
        search_run_record_identifier="refreshed-search-run",
        product_guide_record_identifier="default-guide",
        staleness_policy=ReviewStalenessPolicy(),
        created_at_utc="2026-09-27T00:00:00+00:00",
        product_guide_bindings=(
            WorkspaceProductGuideBinding(
                binding_identifier="selected-binding",
                workspace_record_identifier="workspace",
                alias="selected",
                product_guide_identity=("carl", "product_guide", "selected"),
                version_policy=WorkspaceProductGuideVersionPolicy.FOLLOW_LATEST,
                pinned_product_guide_record_identifier=None,
                resolved_product_guide_record_identifier="guide",
                resolved_product_guide_version=2,
                enabled=True,
                is_default=False,
            ),
        ),
    )
    batch = ReviewBatch(
        record_identifier="batch",
        workspace_record_identifier="workspace",
        created_at_utc="2026-09-27T00:00:01+00:00",
        items=(
            ReviewBatchItem(
                projection=projection,
                review_state=ReviewState.UNREVIEWED,
                prior_review=None,
                changed_components=(),
            ),
        ),
        next_cursor=None,
        scanned_page_count=1,
    )
    observed_requests: list[RequestMissingListingAnalysesRequest] = []
    observed_restrictions: list[frozenset[str] | None] = []
    wait_progress: list[tuple[int, int, float]] = []

    async def record_wait_progress(status: WorkspaceWorkStatus, waited: float) -> None:
        wait_progress.append((status.queued_count, status.in_progress_count, waited))

    async def get_workspace(
        _application: ReviewApplication, record_identifier: str
    ) -> ReviewWorkspace:
        assert record_identifier in {"workspace", "idle-workspace"}
        return workspace.model_copy(update={"record_identifier": record_identifier})

    async def get_batch(_application: ReviewApplication, record_identifier: str) -> ReviewBatch:
        assert record_identifier == "batch"
        return batch

    async def refresh_identifier(_database: Database, record_identifier: str) -> str:
        assert record_identifier == "refreshed-search-run"
        return "refresh-work"

    async def workspace_analysis_candidates(
        _application: ReviewApplication,
        _workspace: ReviewWorkspace,
        *,
        statuses: frozenset[ListingStatus],
        maximum_candidate_listings_examined: int,
    ) -> tuple[tuple[str, ...], int, bool]:
        assert statuses == frozenset({ListingStatus.AVAILABLE})
        assert maximum_candidate_listings_examined == 2_500
        return ("123",), 1, False

    async def plan(
        _application: ReviewApplication,
        request: RequestMissingListingAnalysesRequest,
        *,
        restrict_listing_identifiers: frozenset[str] | None = None,
        source_listing_identifiers: frozenset[str] | None = None,
    ) -> ListingAnalysisBatchSelection:
        observed_requests.append(request)
        observed_restrictions.append(restrict_listing_identifiers)
        assert source_listing_identifiers == frozenset({"123"})
        return ListingAnalysisBatchSelection(
            source_as_of_completion_sequence=10,
            matching_observation_record_identifiers=("observation",),
            eligible_observation_record_identifiers=("observation",),
            selected_observation_record_identifiers=("observation",),
            reusable_observation_record_identifiers=(),
            excluded_observation_record_identifiers=(),
        )

    monkeypatch.setattr(ReviewApplication, "get_review_workspace", get_workspace)
    monkeypatch.setattr(ReviewApplication, "get_review_batch", get_batch)
    monkeypatch.setattr(
        ReviewApplication,
        "_workspace_analysis_candidates",
        workspace_analysis_candidates,
    )
    monkeypatch.setattr(
        Database,
        "facebook_search_run_refresh_work_identifier",
        refresh_identifier,
    )
    monkeypatch.setattr(ReviewApplication, "_plan_missing_listing_analyses", plan)

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        application = ReviewApplication(database, tmp_path)
        request = SelectionAnalysesRequest(
            workspace_record_identifier="workspace",
            product_guide_binding_identifier="selected-binding",
            selection=ReviewBatchSelection(review_batch_record_identifier="batch"),
        )
        preview = await application.preview_selection_analyses(request)
        first_request = await application.request_selection_analyses(request)
        replayed_request = await application.request_selection_analyses(request)
        workspace_work = await application.get_workspace_work_status("workspace")
        timed_out_wait = await application.wait_for_workspace_work("workspace", timeout_seconds=0)

        async def queued_status(
            _application: ReviewApplication, record_identifier: str
        ) -> WorkspaceWorkStatus:
            assert record_identifier == "workspace"
            return workspace_work

        # Test progress delivery and timeout, not whether SQLite finishes within 10 ms.
        # The real queued status and zero-timeout read are verified above.
        with monkeypatch.context() as wait_patch:
            wait_patch.setattr(ReviewApplication, "get_workspace_work_status", queued_status)
            progress_wait = await application.wait_for_workspace_work(
                "workspace",
                timeout_seconds=0.01,
                progress=record_wait_progress,
            )
        idle_wait = await application.wait_for_workspace_work("idle-workspace")

    low_level_request = observed_requests[0]
    assert tuple(low_level_request.filters.availabilities) == (CandidateAvailability.FULL_LISTING,)
    assert observed_restrictions == [frozenset({"123"})] * 3
    assert preview.workspace_record_identifier == "workspace"
    assert preview.product_guide_binding_identifier == "selected-binding"
    assert preview.source_search_refresh_work_identifier == "refresh-work"
    assert preview.product_guide_record_identifier == "guide"
    assert preview.selection_kind == "review_batch"
    assert preview.candidate_listings_examined == 1
    assert not preview.candidate_examination_limit_reached
    assert preview.selected_observations == 1
    assert preview.new_analysis_count == 1
    assert preview.reused_analysis_count == 0
    assert preview.excluded_observation_count == 0
    assert first_request.created
    assert not replayed_request.created
    assert replayed_request.work_identifier == first_request.work_identifier
    assert first_request.selected_observation_count == 1
    assert first_request.candidate_listings_examined == 1
    assert not first_request.candidate_examination_limit_reached
    assert first_request.product_guide_binding_identifier == "selected-binding"
    assert first_request.product_guide_record_identifier == "guide"
    assert workspace_work.queued_count == 1
    assert workspace_work.in_progress_count == 0
    assert not workspace_work.idle
    assert [work.identifier for work in workspace_work.active_work] == [
        first_request.work_identifier
    ]
    assert timed_out_wait.timed_out
    assert timed_out_wait.status.queued_count == 1
    assert progress_wait.timed_out
    assert wait_progress and wait_progress[0][:2] == (1, 0)
    assert not idle_wait.timed_out
    assert idle_wait.status.idle


@pytest.mark.anyio
async def test_concurrent_database_claims_never_overlap(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    component = Component(ComponentId(("test", "workspace")), 1, lambda: None)
    projection = ComposedListingProjection(
        listing_identifier="123",
        canonical_source_url=canonical_facebook_listing_url("123"),
        as_of_completion_sequence=1,
        projection_revision=_revision(),
        status=ComposedStatus(
            value=ListingStatus.UNKNOWN,
            raw_flags={},
            evidence=None,
        ),
        title=None,
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
    item = ReviewBatchItem(
        projection=projection,
        review_state=ReviewState.UNREVIEWED,
        prior_review=None,
        changed_components=(),
    )
    async with Database.managed(path, initialize=True) as database:
        await database.publish_records_operation(
            component=component,
            operation_identifier="workspace-operation",
            records=(
                RecordDraft(
                    identifier="workspace",
                    kind=REVIEW_WORKSPACE_KIND,
                    schema_version=1,
                    value={"test": True},
                ),
            ),
            inputs=(),
            outputs=(NamedOutput(name=("workspace",), object_identifier="workspace"),),
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-09-27T00:00:00+00:00",
            ended_at_utc="2026-09-27T00:00:01+00:00",
            duration_ns=1,
            result={"state": "completed"},
        )

    results = []

    async def acquire(database: Database, suffix: str) -> None:
        result = await database.publish_acquired_review_batch(
            candidate_batch=ReviewBatch(
                record_identifier=f"batch-{suffix}",
                workspace_record_identifier="workspace",
                created_at_utc="2026-09-27T00:00:02+00:00",
                items=(item,),
                next_cursor=None,
                scanned_page_count=1,
            ),
            claim_token=f"token-{suffix}",
            owner_identifier=f"agent-{suffix}",
            utc_now_ns=lambda: 10,
            lease_duration_ns=10,
            action="acquire_batch",
            request_identifier=f"request-{suffix}",
            request_sha256=suffix * 64,
            component=build_review_workspace_component_registry().require(ACQUIRE_REVIEW_BATCH),
            operation_identifier=f"operation-{suffix}",
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-09-27T00:00:02+00:00",
            ended_at_utc="2026-09-27T00:00:03+00:00",
            duration_ns=1,
        )
        results.append(result)

    async with Database.managed(path) as database, anyio.create_task_group() as task_group:
        task_group.start_soon(acquire, database, "a")
        task_group.start_soon(acquire, database, "b")

    assert sorted(len(result.batch.items) for result in results) == [0, 1]
    assert sum(result.lease is not None for result in results) == 1
    async with Database.managed(path) as database:
        assert (
            len(
                await database.active_review_claims(
                    workspace_record_identifier="workspace", now_utc_ns=19
                )
            )
            == 1
        )
        assert not await database.active_review_claims(
            workspace_record_identifier="workspace", now_utc_ns=20
        )


@pytest.mark.anyio
async def test_workset_update_has_atomic_optimistic_concurrency(tmp_path: Path) -> None:
    component = Component(ComponentId(("test", "workspace")), 1, lambda: None)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.publish_records_operation(
            component=component,
            operation_identifier="workspace-operation",
            records=(
                RecordDraft(
                    identifier="workspace",
                    kind=REVIEW_WORKSPACE_KIND,
                    schema_version=1,
                    value={"test": True},
                ),
            ),
            inputs=(),
            outputs=(NamedOutput(name=("workspace",), object_identifier="workspace"),),
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-09-27T00:00:00+00:00",
            ended_at_utc="2026-09-27T00:00:01+00:00",
            duration_ns=1,
            result={"state": "completed"},
        )
        registry = build_review_workspace_component_registry()
        created = await database.publish_review_workset_revision(
            workset_identifier="workset",
            workspace_record_identifier="workspace",
            name="Shortlist",
            expected_version=None,
            listing_identifiers=("1", "2"),
            component=registry.require(CREATE_REVIEW_WORKSET),
            operation_identifier="create-operation",
            record_identifier="workset-v1",
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-09-27T00:00:02+00:00",
            ended_at_utc="2026-09-27T00:00:03+00:00",
            duration_ns=1,
        )
        assert created == (1, "workset-v1")
        updated = await database.publish_review_workset_revision(
            workset_identifier="workset",
            workspace_record_identifier="workspace",
            name="Shortlist",
            expected_version=1,
            listing_identifiers=("1", "3"),
            component=registry.require(CREATE_REVIEW_WORKSET),
            operation_identifier="update-operation",
            record_identifier="workset-v2",
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-09-27T00:00:04+00:00",
            ended_at_utc="2026-09-27T00:00:05+00:00",
            duration_ns=1,
        )
        assert updated == (2, "workset-v2")
        stale = await database.publish_review_workset_revision(
            workset_identifier="workset",
            workspace_record_identifier="workspace",
            name="Shortlist",
            expected_version=1,
            listing_identifiers=("4",),
            component=registry.require(CREATE_REVIEW_WORKSET),
            operation_identifier="stale-operation",
            record_identifier="workset-stale",
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-09-27T00:00:06+00:00",
            ended_at_utc="2026-09-27T00:00:07+00:00",
            duration_ns=1,
        )
        assert isinstance(stale, ReviewWorksetConflict)
        assert stale.current_version == 2
        with pytest.raises(ValueError, match="exceeds"):
            await database.publish_review_workset_revision(
                workset_identifier="too-large",
                workspace_record_identifier="workspace",
                name="Too large",
                expected_version=None,
                listing_identifiers=tuple(str(index) for index in range(10_001)),
                component=registry.require(CREATE_REVIEW_WORKSET),
                operation_identifier="too-large-operation",
                record_identifier="too-large-v1",
                provenance=_provenance(),
                invocation={},
                started_at_utc="2026-09-27T00:00:08+00:00",
                ended_at_utc="2026-09-27T00:00:09+00:00",
                duration_ns=1,
            )


@pytest.mark.anyio
async def test_application_persists_review_workflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    projection = ComposedListingProjection(
        listing_identifier="123",
        canonical_source_url=canonical_facebook_listing_url("123"),
        as_of_completion_sequence=1,
        projection_revision=_revision(),
        status=ComposedStatus(
            value=ListingStatus.AVAILABLE,
            raw_flags={"is_live": True},
            evidence=ProjectionEvidence(
                evidence_record_identifier="search-card",
                acquisition_completion_sequence=1,
                observation_completion_sequence=1,
                source_kind=ProjectionSourceKind.SEARCH_CARD,
            ),
        ),
        title=None,
        price=None,
        location=None,
        description=None,
        seller=None,
        preview_image=None,
        gallery=ComposedGallery(
            referenced_image_count=3,
            saved_image_count=2,
            all_referenced_images_saved=False,
            images=(),
            images_truncated=True,
            reference_set_truncated=False,
            reference_set_evidence=ProjectionEvidence(
                evidence_record_identifier="gallery",
                acquisition_completion_sequence=1,
                observation_completion_sequence=1,
                source_kind=ProjectionSourceKind.ITEM_PAGE,
            ),
        ),
        analyses=(),
        analyses_truncated=True,
        search_membership=None,
    )
    second_projection = projection.model_copy(
        update={
            "listing_identifier": "456",
            "canonical_source_url": canonical_facebook_listing_url("456"),
            "projection_revision": _revision(scalar="1"),
        }
    )

    async def list_composed_search(
        _application: ReviewApplication, request: ListComposedSearchRequest
    ) -> ComposedListingPage:
        return ComposedListingPage(
            as_of_completion_sequence=1,
            selected_search_run_record_identifier="search-run",
            included_ancestry_run_count=1,
            older_ancestry_truncated=False,
            examined_candidate_listing_count=2,
            candidate_examination_limit_reached=False,
            listings=(projection, second_projection),
            next_cursor=None,
        )

    monkeypatch.setattr(ReviewApplication, "list_composed_search", list_composed_search)
    component = Component(ComponentId(("test", "search")), 1, lambda: None)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.publish_records_operation(
            component=component,
            operation_identifier="search-operation",
            records=(
                RecordDraft(
                    identifier="search-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value={},
                ),
            ),
            inputs=(),
            outputs=(NamedOutput(name=("search_run",), object_identifier="search-run"),),
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-09-27T00:00:00+00:00",
            ended_at_utc="2026-09-27T00:00:01+00:00",
            duration_ns=1,
            result={"state": "completed"},
        )

        async def code_provenance() -> CodeProvenance:
            return _provenance()

        application = ReviewApplication(
            database=database,
            repository_root=tmp_path,
            code_provenance=code_provenance,
        )
        workspace = await application.create_review_workspace(
            CreateReviewWorkspaceRequest(name="Fridges", search_run_record_identifier="search-run")
        )
        listing_page = await application.list_workspace_listings(
            ListWorkspaceListingsRequest(
                workspace_record_identifier=workspace.record_identifier,
                page_size=2,
            )
        )
        assert [listing.listing_identifier for listing in listing_page.listings] == ["123", "456"]
        assert listing_page.listings[0].status is ListingStatus.AVAILABLE
        assert listing_page.listings[0].projection_revision_sha256 == "0" * 64
        assert listing_page.listings[0].title is None
        assert listing_page.listings[0].analysis_available
        assert listing_page.listings[0].referenced_image_count == 3
        assert listing_page.listings[0].saved_image_count == 2
        assert "raw_flags" not in listing_page.listings[0].model_dump(mode="json")
        assert "evidence" not in listing_page.listings[0].model_dump(mode="json")
        acquisition_request = AcquireReviewBatchRequest(
            workspace_record_identifier=workspace.record_identifier,
            request_identifier="agent-a-batch-1",
            owner_identifier="agent-a",
        )
        acquisition = await application.acquire_review_batch(acquisition_request)
        assert await application.acquire_review_batch(acquisition_request) == acquisition
        assert acquisition.lease is not None
        batch = acquisition.batch
        assert [item.projection.listing_identifier for item in batch.items] == ["123", "456"]
        assert batch.items[0].review_state is ReviewState.UNREVIEWED

        with pytest.raises(ReviewInputError, match="different request"):
            await application.acquire_review_batch(
                acquisition_request.model_copy(update={"owner_identifier": "agent-b"})
            )

        competing = await application.acquire_review_batch(
            AcquireReviewBatchRequest(
                workspace_record_identifier=workspace.record_identifier,
                request_identifier="agent-b-batch-1",
                owner_identifier="agent-b",
            )
        )
        assert competing.batch.items == ()
        assert competing.lease is None

        renewal_request = RenewReviewClaimRequest(
            request_identifier="agent-a-renew-1",
            claim_token=acquisition.lease.claim_token,
            owner_identifier="agent-a",
        )
        renewed = await application.renew_review_claim(renewal_request)
        assert await application.renew_review_claim(renewal_request) == renewed
        assert renewed.lease_expires_at_utc_ns > acquisition.lease.lease_expires_at_utc_ns
        with pytest.raises(ReviewInputError, match="owned elsewhere"):
            await application.renew_review_claim(
                RenewReviewClaimRequest(
                    request_identifier="agent-b-renew-1",
                    claim_token=acquisition.lease.claim_token,
                    owner_identifier="agent-b",
                )
            )
        activity = await application.get_review_workspace_activity(workspace.record_identifier)
        assert activity.active_claims[0].owner_identifier == "agent-a"

        release_request = ReleaseReviewClaimRequest(
            request_identifier="agent-a-release-1",
            claim_token=acquisition.lease.claim_token,
            owner_identifier="agent-a",
        )
        released = await application.release_review_claim(release_request)
        assert await application.release_review_claim(release_request) == released
        assert released.released_listing_count == 2

        with pytest.raises(ReviewInputError, match="supplied claim was lost"):
            await application.record_listing_reviews(
                RecordListingReviewsRequest(
                    request_identifier="agent-a-review-after-release",
                    workspace_record_identifier=workspace.record_identifier,
                    batch_record_identifier=batch.record_identifier,
                    reviews=(
                        ListingReviewInput(
                            listing_identifier="123",
                            projection_revision=projection.projection_revision,
                        ),
                    ),
                )
            )

        acquisition = await application.acquire_review_batch(
            AcquireReviewBatchRequest(
                workspace_record_identifier=workspace.record_identifier,
                request_identifier="agent-a-batch-2",
                owner_identifier="agent-a",
            )
        )
        assert acquisition.lease is not None
        batch = acquisition.batch

        review_request = RecordListingReviewsRequest(
            request_identifier="agent-a-review-1",
            workspace_record_identifier=workspace.record_identifier,
            batch_record_identifier=batch.record_identifier,
            claim_token=acquisition.lease.claim_token,
            claim_owner_identifier="agent-a",
            reviews=(
                ListingReviewInput(
                    listing_identifier="123",
                    projection_revision=projection.projection_revision,
                    disposition=ReviewDisposition.PROMISING,
                    note="Compare dimensions.",
                ),
            ),
        )
        recorded = await application.record_listing_reviews(review_request)
        assert await application.record_listing_reviews(review_request) == recorded
        assert recorded.records[0].inspected
        partial_activity = await application.get_review_workspace_activity(
            workspace.record_identifier
        )
        assert partial_activity.active_claims[0].claimed_listing_count == 1
        partial_renewal = await application.renew_review_claim(
            RenewReviewClaimRequest(
                request_identifier="agent-a-renew-partial",
                claim_token=acquisition.lease.claim_token,
                owner_identifier="agent-a",
            )
        )
        assert partial_renewal.listing_identifiers == ("456",)

        second_review_request = RecordListingReviewsRequest(
            request_identifier="agent-a-review-2",
            workspace_record_identifier=workspace.record_identifier,
            batch_record_identifier=batch.record_identifier,
            claim_token=acquisition.lease.claim_token,
            claim_owner_identifier="agent-a",
            reviews=(
                ListingReviewInput(
                    listing_identifier="456",
                    projection_revision=second_projection.projection_revision,
                    disposition=ReviewDisposition.PROMISING,
                ),
            ),
        )
        second_recorded = await application.record_listing_reviews(second_review_request)
        assert await application.record_listing_reviews(second_review_request) == second_recorded
        assert not (
            await application.get_review_workspace_activity(workspace.record_identifier)
        ).active_claims

        next_batch = await application.create_review_batch(
            CreateReviewBatchRequest(workspace_record_identifier=workspace.record_identifier)
        )
        assert next_batch.items == ()

        workset = await application.create_review_workset(
            CreateReviewWorksetRequest(
                workspace_record_identifier=workspace.record_identifier,
                name="Shortlist",
                listing_identifiers=("123",),
            )
        )
        assert not isinstance(workset, ReviewWorksetConflict)
        revised = await application.update_review_workset(
            UpdateReviewWorksetRequest(
                workset_identifier=workset.workset_identifier,
                expected_version=1,
                add_listing_identifiers=("456",),
            )
        )
        assert not isinstance(revised, ReviewWorksetConflict)
        assert revised.listing_identifiers == ("123", "456")

        snapshot = await application.create_selection_snapshot(
            CreateSelectionSnapshotRequest(
                workspace_record_identifier=workspace.record_identifier,
                selection=ReviewBatchSelection(
                    review_batch_record_identifier=batch.record_identifier
                ),
            )
        )
        assert snapshot.items[0].projection_revision == projection.projection_revision
        assert await application.get_selection_snapshot(snapshot.record_identifier) == snapshot
        activity = await application.get_review_workspace_activity(workspace.record_identifier)
        assert activity.recent_batches[0].record_identifier == next_batch.record_identifier
        assert activity.recent_reviews[0].disposition is ReviewDisposition.PROMISING
        assert activity.current_worksets[0].member_count == 2
        assert (
            activity.recent_selection_snapshots[0].record_identifier == snapshot.record_identifier
        )

        single_agent_workspace = await application.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Single-agent review",
                search_run_record_identifier="search-run",
            )
        )
        unclaimed_batch = await application.create_review_batch(
            CreateReviewBatchRequest(
                workspace_record_identifier=single_agent_workspace.record_identifier
            )
        )
        unclaimed_review = await application.record_listing_reviews(
            RecordListingReviewsRequest(
                request_identifier="single-agent-review-1",
                workspace_record_identifier=single_agent_workspace.record_identifier,
                batch_record_identifier=unclaimed_batch.record_identifier,
                reviews=(
                    ListingReviewInput(
                        listing_identifier="123",
                        projection_revision=projection.projection_revision,
                    ),
                ),
            )
        )
        assert unclaimed_review.records[0].listing_identifier == "123"


@pytest.mark.anyio
async def test_workspace_bulk_review_preserves_prior_reviews_and_is_atomic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def projection(identifier: str, scalar: str) -> ComposedListingProjection:
        return ComposedListingProjection(
            listing_identifier=identifier,
            canonical_source_url=canonical_facebook_listing_url(identifier),
            as_of_completion_sequence=7,
            projection_revision=_revision(scalar=scalar),
            status=ComposedStatus(
                value=ListingStatus.AVAILABLE,
                raw_flags={"is_live": True},
                evidence=ProjectionEvidence(
                    evidence_record_identifier=f"search-card-{identifier}",
                    acquisition_completion_sequence=7,
                    observation_completion_sequence=7,
                    source_kind=ProjectionSourceKind.SEARCH_CARD,
                ),
            ),
            title=None,
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

    projections = (
        projection("123", "1"),
        projection("456", "2"),
        projection("789", "3"),
    )

    async def list_composed_search(
        _application: ReviewApplication, request: ListComposedSearchRequest
    ) -> ComposedListingPage:
        return ComposedListingPage(
            as_of_completion_sequence=7,
            selected_search_run_record_identifier=request.search_run_record_identifier,
            included_ancestry_run_count=1,
            older_ancestry_truncated=False,
            examined_candidate_listing_count=3,
            candidate_examination_limit_reached=False,
            listings=projections,
            next_cursor=None,
        )

    async def bulk_projections(
        _application: ReviewApplication,
        _workspace: ReviewWorkspace,
        *,
        maximum_candidate_listings_examined: int,
        selected_listing_identifiers: tuple[str, ...] | None = None,
    ) -> tuple[int, int, tuple[ComposedListingProjection, ...]]:
        assert maximum_candidate_listings_examined == 10_000
        selected = (
            projections
            if selected_listing_identifiers is None
            else tuple(
                projection
                for projection in projections
                if projection.listing_identifier in selected_listing_identifiers
            )
        )
        return 7, len(selected), selected

    monkeypatch.setattr(ReviewApplication, "list_composed_search", list_composed_search)
    monkeypatch.setattr(ReviewApplication, "_workspace_bulk_review_projections", bulk_projections)
    component = Component(ComponentId(("test", "search")), 1, lambda: None)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.publish_records_operation(
            component=component,
            operation_identifier="search-operation",
            records=(
                RecordDraft(
                    identifier="search-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value={},
                ),
            ),
            inputs=(),
            outputs=(NamedOutput(name=("search_run",), object_identifier="search-run"),),
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-09-28T00:00:00+00:00",
            ended_at_utc="2026-09-28T00:00:01+00:00",
            duration_ns=1,
            result={"state": "completed"},
        )

        async def code_provenance() -> CodeProvenance:
            return _provenance()

        application = ReviewApplication(
            database=database,
            repository_root=tmp_path,
            code_provenance=code_provenance,
        )
        workspace = await application.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Bulk review", search_run_record_identifier="search-run"
            )
        )
        acquisition = await application.acquire_review_batch(
            AcquireReviewBatchRequest(
                workspace_record_identifier=workspace.record_identifier,
                request_identifier="explicit-review-batch",
                owner_identifier="agent-a",
            )
        )
        assert acquisition.lease is not None
        await application.record_listing_reviews(
            RecordListingReviewsRequest(
                request_identifier="explicit-review",
                workspace_record_identifier=workspace.record_identifier,
                batch_record_identifier=acquisition.batch.record_identifier,
                claim_token=acquisition.lease.claim_token,
                claim_owner_identifier="agent-a",
                reviews=(
                    ListingReviewInput(
                        listing_identifier="123",
                        projection_revision=projections[0].projection_revision,
                        disposition=ReviewDisposition.PROMISING,
                        note="Keep this one.",
                    ),
                ),
            )
        )
        await application.release_review_claim(
            ReleaseReviewClaimRequest(
                request_identifier="release-explicit-review-batch",
                claim_token=acquisition.lease.claim_token,
                owner_identifier="agent-a",
            )
        )

        request = RecordWorkspaceBulkReviewRequest(
            request_identifier="reject-unreviewed",
            workspace_record_identifier=workspace.record_identifier,
            disposition=ReviewDisposition.REJECTED,
            note="Not of interest, first-pass triage 2026-09-28.",
            exclude_listing_identifiers=("789",),
        )
        result = await application.record_workspace_bulk_review(request)
        assert await application.record_workspace_bulk_review(request) == result
        assert result.as_of_completion_sequence == 7
        assert result.candidate_listings_examined == 3
        assert result.selection_member_count == 3
        assert result.explicitly_excluded_count == 1
        assert result.status_excluded_count == 0
        assert result.review_state_excluded_count == 1
        assert result.recorded_count == 1

        latest = await database.latest_listing_reviews(
            workspace_record_identifier=workspace.record_identifier,
            listing_identifiers=("123", "456", "789"),
        )
        assert latest["123"].disposition is ReviewDisposition.PROMISING
        assert latest["456"].disposition is ReviewDisposition.REJECTED
        assert "789" not in latest
        changed_revision = projections[1].projection_revision.model_copy(
            update={"scalar_fields_sha256": "9" * 64}
        )
        assert (
            classify_review_state(
                current_revision=changed_revision,
                previous_review=latest["456"],
                policy=workspace.staleness_policy,
            )[0]
            is ReviewState.STALE
        )

        with pytest.raises(ReviewInputError, match="different request"):
            await application.record_workspace_bulk_review(
                request.model_copy(update={"note": "Changed request content."})
            )
        with pytest.raises(ReviewInputError, match="excluded listings are outside"):
            await application.record_workspace_bulk_review(
                request.model_copy(
                    update={
                        "request_identifier": "mistyped-exclusion",
                        "exclude_listing_identifiers": ("999",),
                    }
                )
            )

        claimed = await application.acquire_review_batch(
            AcquireReviewBatchRequest(
                workspace_record_identifier=workspace.record_identifier,
                request_identifier="claim-remaining",
                owner_identifier="agent-b",
            )
        )
        assert claimed.lease is not None
        assert [item.projection.listing_identifier for item in claimed.batch.items] == ["789"]
        with pytest.raises(ReviewInputError, match="claimed or reviewed concurrently"):
            await application.record_workspace_bulk_review(
                RecordWorkspaceBulkReviewRequest(
                    request_identifier="reject-claimed",
                    workspace_record_identifier=workspace.record_identifier,
                    disposition=ReviewDisposition.REJECTED,
                )
            )
        assert "789" not in await database.latest_listing_reviews(
            workspace_record_identifier=workspace.record_identifier,
            listing_identifiers=("789",),
        )
        await application.release_review_claim(
            ReleaseReviewClaimRequest(
                request_identifier="release-remaining",
                claim_token=claimed.lease.claim_token,
                owner_identifier="agent-b",
            )
        )

        workset = await application.create_review_workset(
            CreateReviewWorksetRequest(
                workspace_record_identifier=workspace.record_identifier,
                name="Remaining",
                listing_identifiers=("789",),
            )
        )
        assert not isinstance(workset, ReviewWorksetConflict)
        workset_result = await application.record_workspace_bulk_review(
            RecordWorkspaceBulkReviewRequest(
                request_identifier="reject-workset",
                workspace_record_identifier=workspace.record_identifier,
                selection=WorksetBulkReviewSelection(workset_identifier=workset.workset_identifier),
                disposition=ReviewDisposition.REJECTED,
            )
        )
        assert workset_result.selection_kind == "workset"
        assert workset_result.recorded_count == 1

        snapshot = await application.create_selection_snapshot(
            CreateSelectionSnapshotRequest(
                workspace_record_identifier=workspace.record_identifier,
                selection=ReviewBatchSelection(
                    review_batch_record_identifier=acquisition.batch.record_identifier
                ),
            )
        )
        snapshot_result = await application.record_workspace_bulk_review(
            RecordWorkspaceBulkReviewRequest(
                request_identifier="defer-snapshot",
                workspace_record_identifier=workspace.record_identifier,
                selection=SelectionSnapshotBulkReviewSelection(
                    selection_snapshot_record_identifier=snapshot.record_identifier
                ),
                include_review_states=(ReviewState.CURRENT,),
                disposition=ReviewDisposition.DEFERRED,
                exclude_listing_identifiers=("123", "456"),
            )
        )
        assert snapshot_result.selection_kind == "selection_snapshot"
        assert snapshot_result.recorded_count == 1


@pytest.mark.anyio
async def test_workset_bulk_projection_does_not_scan_the_whole_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    membership_scan_called = False

    async def source_record(
        _database: Database, identifier: str
    ) -> tuple[tuple[str, ...], int, object]:
        assert identifier == "search-run"
        return ("carl", "facebook", "search_run"), 1, {}

    async def current_completion_boundary(_database: Database) -> int:
        return 7

    async def projection_search_scope(
        _application: ReviewApplication,
        primary_search_run_record_identifier: str,
        additional_search_run_record_identifiers: tuple[str, ...],
        *,
        as_of_completion_sequence: int,
        maximum_runs: int,
    ) -> SearchAncestrySelection:
        assert primary_search_run_record_identifier == "search-run"
        assert additional_search_run_record_identifiers == ()
        assert as_of_completion_sequence == 7
        assert maximum_runs == 100
        return SearchAncestrySelection(
            runs=(
                SearchRunCandidate(
                    record_identifier="search-run",
                    internal_search_run_identifier="internal-run",
                    completion_sequence=7,
                    started_at_utc=None,
                    completed_at_utc=None,
                    refresh_source_run_record_identifier=None,
                    stopping_reason=None,
                ),
            ),
            lineage_root_search_run_record_identifier="search-run",
            older_ancestry_truncated=False,
        )

    async def membership_candidates(*_args: object, **_kwargs: object) -> tuple[object, ...]:
        nonlocal membership_scan_called
        membership_scan_called = True
        return ()

    async def membership_occurrences(
        _database: Database,
        listing_identifiers: tuple[str, ...],
        included_search_runs: tuple[tuple[str, str], ...],
        *,
        as_of_completion_sequence: int,
    ) -> tuple[SearchMembershipOccurrenceCandidate, ...]:
        assert listing_identifiers == ("123",)
        assert included_search_runs == (("search-run", "internal-run"),)
        assert as_of_completion_sequence == 7
        return (
            SearchMembershipOccurrenceCandidate(
                occurrence_record_identifier="occurrence",
                listing_identifier="123",
                search_run_record_identifier="search-run",
                search_run_completion_sequence=7,
                acquisition_completion_sequence=7,
                observed_at_utc=None,
            ),
        )

    async def no_projection_rows(*_args: object, **_kwargs: object) -> tuple[object, ...]:
        return ()

    monkeypatch.setattr(Database, "get_record", source_record)
    monkeypatch.setattr(Database, "current_completion_boundary", current_completion_boundary)
    monkeypatch.setattr(ReviewApplication, "_projection_search_scope", projection_search_scope)
    monkeypatch.setattr(
        Database, "facebook_projection_membership_candidates", membership_candidates
    )
    monkeypatch.setattr(
        Database, "facebook_projection_membership_occurrences", membership_occurrences
    )
    monkeypatch.setattr(Database, "facebook_projection_item_observations", no_projection_rows)
    monkeypatch.setattr(Database, "facebook_projection_search_occurrences", no_projection_rows)
    monkeypatch.setattr(Database, "facebook_projection_search_cards", no_projection_rows)
    monkeypatch.setattr(Database, "facebook_projection_analysis_descriptors", no_projection_rows)

    workspace = ReviewWorkspace(
        record_identifier="workspace",
        name="Large workspace",
        search_run_record_identifier="search-run",
        product_guide_record_identifier=None,
        staleness_policy=ReviewStalenessPolicy(),
        created_at_utc="2026-09-28T00:00:00+00:00",
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        application = ReviewApplication(database=database, repository_root=tmp_path)
        as_of, examined, projections = await application._workspace_bulk_review_projections(
            workspace,
            maximum_candidate_listings_examined=10_000,
            selected_listing_identifiers=("123",),
        )

    assert as_of == 7
    assert examined == 1
    assert [projection.listing_identifier for projection in projections] == ["123"]
    assert not membership_scan_called


@pytest.mark.anyio
async def test_workspace_bulk_review_publishes_thousands_in_one_bounded_write(
    tmp_path: Path,
) -> None:
    workspace_identifier = "workspace"
    listing_identifiers = tuple(str(1_000_000 + index) for index in range(3_600))
    recorded_at = "2026-09-28T12:00:00+00:00"
    reviews = tuple(
        ListingReviewRecord(
            record_identifier=f"review-{listing_identifier}",
            workspace_record_identifier=workspace_identifier,
            batch_record_identifier=None,
            listing_identifier=listing_identifier,
            projection_revision=_revision(),
            inspected=True,
            disposition=ReviewDisposition.REJECTED,
            note="Not of interest, first-pass triage 2026-09-28.",
            recorded_at_utc=recorded_at,
        )
        for listing_identifier in listing_identifiers
    )
    response = RecordWorkspaceBulkReviewResult(
        operation_identifier="bulk-operation",
        workspace_record_identifier=workspace_identifier,
        selection_kind="workspace",
        as_of_completion_sequence=1,
        candidate_listings_examined=len(reviews),
        selection_member_count=len(reviews),
        explicitly_excluded_count=0,
        status_excluded_count=0,
        review_state_excluded_count=0,
        recorded_count=len(reviews),
        recorded_at_utc=recorded_at,
    )
    component = Component(ComponentId(("test", "workspace")), 1, lambda: None)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.publish_records_operation(
            component=component,
            operation_identifier="workspace-operation",
            records=(
                RecordDraft(
                    identifier=workspace_identifier,
                    kind=REVIEW_WORKSPACE_KIND,
                    schema_version=1,
                    value={"test": True},
                ),
            ),
            inputs=(),
            outputs=(NamedOutput(name=("workspace",), object_identifier=workspace_identifier),),
            provenance=_provenance(),
            invocation={},
            started_at_utc=recorded_at,
            ended_at_utc=recorded_at,
            duration_ns=1,
            result={"state": "completed"},
        )
        with anyio.fail_after(10):
            published = await database.publish_workspace_bulk_review_records(
                request_identifier="bulk-3,600",
                request_sha256="a" * 64,
                workspace_record_identifier=workspace_identifier,
                expected_prior_review_identifiers={
                    listing_identifier: None for listing_identifier in listing_identifiers
                },
                component=build_review_workspace_component_registry().require(
                    RECORD_WORKSPACE_BULK_REVIEW
                ),
                operation_identifier=response.operation_identifier,
                records=tuple(
                    RecordDraft(
                        identifier=review.record_identifier,
                        kind=LISTING_REVIEW_KIND,
                        schema_version=1,
                        value=review.model_dump(mode="json"),
                    )
                    for review in reviews
                ),
                source_input=None,
                response=response,
                provenance=_provenance(),
                invocation={},
                started_at_utc=recorded_at,
                ended_at_utc=recorded_at,
                duration_ns=1,
                utc_now_ns=lambda: 1,
            )
        assert published == response
        assert len(
            await database.latest_listing_reviews(
                workspace_record_identifier=workspace_identifier,
                listing_identifiers=listing_identifiers,
            )
        ) == len(reviews)
        assert (
            await database.latest_listing_reviews(
                workspace_record_identifier=workspace_identifier,
                listing_identifiers=(),
            )
            == {}
        )
        conflicting_review = reviews[0].model_copy(
            update={"record_identifier": "conflicting-review"}
        )
        conflict = await database.publish_workspace_bulk_review_records(
            request_identifier="stale-plan",
            request_sha256="b" * 64,
            workspace_record_identifier=workspace_identifier,
            expected_prior_review_identifiers={reviews[0].listing_identifier: None},
            component=build_review_workspace_component_registry().require(
                RECORD_WORKSPACE_BULK_REVIEW
            ),
            operation_identifier="conflicting-operation",
            records=(
                RecordDraft(
                    identifier=conflicting_review.record_identifier,
                    kind=LISTING_REVIEW_KIND,
                    schema_version=1,
                    value=conflicting_review.model_dump(mode="json"),
                ),
            ),
            source_input=None,
            response=response.model_copy(
                update={
                    "operation_identifier": "conflicting-operation",
                    "candidate_listings_examined": 1,
                    "selection_member_count": 1,
                    "recorded_count": 1,
                }
            ),
            provenance=_provenance(),
            invocation={},
            started_at_utc=recorded_at,
            ended_at_utc=recorded_at,
            duration_ns=1,
            utc_now_ns=lambda: 2,
        )
        assert conflict is None
        with pytest.raises(KeyError):
            await database.get_record(conflicting_review.record_identifier)
