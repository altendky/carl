"""Selection freshness uses retained track membership and bounded provenance."""

from collections.abc import Sequence
from itertools import pairwise
from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest
from mcp import Client

from carl._tests.mcp_response_helpers import complete_tool_result
from carl._tests.test_marketplace_listing import _publish, _record
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_review_workspace import _search_run_value
from carl.core.analysis_batch import (
    ListingAnalysisBatchSelection,
    RequestMissingListingAnalysesRequest,
    SelectionAnalysesRequest,
)
from carl.core.composed_projection import ListingStatus
from carl.core.models import RecordDraft
from carl.core.review_errors import ReviewInputError
from carl.core.review_workspace import (
    REVIEW_WORKSPACE_KIND,
    ListingIdsSelection,
    ReviewStalenessPolicy,
    ReviewWorkspace,
    WorkspaceProductGuideBinding,
    WorkspaceProductGuideVersionPolicy,
    WorkspaceSearchTrack,
)
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkDefinition,
    WorkRequester,
    WorkState,
)
from carl.io.sqlite import Database
from carl.mcp_server import build_server
from carl.review import ReviewApplication

FACEBOOK_ID = "1001137228964859"
EBAY_ITEM_ID = "256123456789"
EBAY_ID = f"ebay:{EBAY_ITEM_ID}"


def _track(
    identifier: str,
    run: str,
    *,
    failed: bool = False,
    enabled: bool = True,
) -> WorkspaceSearchTrack:
    return WorkspaceSearchTrack(
        track_identifier=identifier,
        query=f"query for {identifier}",
        current_search_run_record_identifier=run,
        latest_refresh_work_identifier=f"refresh-{identifier}",
        latest_refresh_work_state=WorkState.TERMINAL_FAILURE if failed else WorkState.COMPLETED,
        enabled=enabled,
    )


def _workspace(tracks: tuple[WorkspaceSearchTrack, ...]) -> ReviewWorkspace:
    return ReviewWorkspace(
        record_identifier="workspace",
        name="Mixed freshness",
        search_run_record_identifier="facebook-run",
        product_guide_record_identifier="guide",
        staleness_policy=ReviewStalenessPolicy(),
        created_at_utc="2026-10-13T00:00:00+00:00",
        search_tracks=tracks,
        product_guide_bindings=(
            WorkspaceProductGuideBinding(
                binding_identifier="binding",
                workspace_record_identifier="workspace",
                alias="Optics",
                product_guide_identity=("optics",),
                version_policy=WorkspaceProductGuideVersionPolicy.PINNED,
                pinned_product_guide_record_identifier="guide",
                resolved_product_guide_record_identifier="guide",
                resolved_product_guide_version=1,
                is_default=True,
            ),
        ),
    )


def _facebook_run(identifier: str, members: Sequence[str]) -> RecordDraft:
    value = _search_run_value(identifier)
    value["traversal"] = {"unique_listing_identifiers": list(members)}
    return RecordDraft(
        identifier=identifier,
        kind=("carl", "facebook", "search_run"),
        schema_version=1,
        value=value,
    )


async def _retained_runs(database: Database) -> None:
    await _publish(
        database,
        (
            _facebook_run("facebook-run", (FACEBOOK_ID,)),
            _facebook_run("overlap-run", (FACEBOOK_ID,)),
            _record("ebay-run", "search_run", request={"query": "telescope"}, listing_count=1),
            _record(
                "ebay-card",
                "search_listing_occurrence",
                search_run_record_identifier="ebay-run",
                item_identifier=EBAY_ITEM_ID,
            ),
        ),
    )


async def _completed_refresh(
    database: Database, identifier: str, base: str, refreshed: str
) -> None:
    kind = ("carl", "facebook", "work", "refresh_search")
    _ = await database.enqueue_work(
        WorkDefinition(
            identifier=identifier,
            kind=kind,
            payload_schema_version=1,
            payload={"base_search_run_record_identifier": base},
            deduplication_identity=(identifier,),
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
            ),
        ),
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier="workspace", context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    await _complete(
        database,
        identifier,
        result={"refreshed_search_run_record_identifier": refreshed},
    )


def _application(
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    workspace: ReviewWorkspace,
) -> tuple[ReviewApplication, list[tuple[str, ...]]]:
    observed_candidate_tracks: list[tuple[str, ...]] = []
    original_work = Database.work

    async def get_workspace(_app: ReviewApplication, record_identifier: str) -> ReviewWorkspace:
        assert record_identifier == workspace.record_identifier
        return workspace

    async def candidates(
        _app: ReviewApplication,
        scoped_workspace: ReviewWorkspace,
        *,
        statuses: frozenset[ListingStatus],
        maximum_candidate_listings_examined: int,
    ) -> tuple[tuple[str, ...], int, bool]:
        assert statuses == frozenset({ListingStatus.AVAILABLE})
        assert maximum_candidate_listings_examined == 2_500
        observed_candidate_tracks.append(
            tuple(track.track_identifier for track in scoped_workspace.search_tracks)
        )
        return (FACEBOOK_ID, EBAY_ID), 2, False

    async def plan(
        _app: ReviewApplication,
        request: RequestMissingListingAnalysesRequest,
        *,
        restrict_listing_identifiers: frozenset[str] | None = None,
        source_listing_identifiers: frozenset[str] | None = None,
    ) -> ListingAnalysisBatchSelection:
        assert restrict_listing_identifiers is not None
        assert source_listing_identifiers == restrict_listing_identifiers
        assert request.product_guide_record_identifier == "guide"
        observations = tuple(
            f"observation-{identifier}" for identifier in sorted(restrict_listing_identifiers)
        )
        return ListingAnalysisBatchSelection(
            source_as_of_completion_sequence=await database.current_completion_boundary(),
            matching_observation_record_identifiers=observations,
            eligible_observation_record_identifiers=observations,
            selected_observation_record_identifiers=observations,
        )

    async def work(_database: Database, identifier: str) -> dict[str, object]:
        if identifier in {
            track.latest_refresh_work_identifier
            for track in workspace.search_tracks
            if track.latest_refresh_work_state == WorkState.TERMINAL_FAILURE
        }:
            return {
                "state": "terminal_failure",
                "error": {"kind": "refresh_search_collection_failed"},
                "result": {
                    "stage": "search_failed",
                    "search_work": {
                        "identifier": f"search-{identifier}",
                        "error": {
                            "kind": "ebay_search_response_failed",
                            "response_classification": {
                                "kind": "error_page",
                                "evidence": ["http_status_403"],
                            },
                        },
                    },
                },
            }
        return await original_work(_database, identifier)

    monkeypatch.setattr(ReviewApplication, "get_review_workspace", get_workspace)
    monkeypatch.setattr(ReviewApplication, "_workspace_analysis_candidates", candidates)

    async def selected_statuses(
        _app: ReviewApplication, scoped_workspace: ReviewWorkspace, identifiers: tuple[str, ...]
    ) -> dict[str, ListingStatus]:
        observed_candidate_tracks.append(
            tuple(track.track_identifier for track in scoped_workspace.search_tracks)
        )
        return dict.fromkeys(identifiers, ListingStatus.AVAILABLE)

    monkeypatch.setattr(
        ReviewApplication, "_selected_workspace_analysis_statuses", selected_statuses
    )
    monkeypatch.setattr(ReviewApplication, "_plan_missing_listing_analyses", plan)
    monkeypatch.setattr(Database, "work", work)
    return ReviewApplication(database, tmp_path), observed_candidate_tracks


@pytest.mark.anyio
async def test_explicit_selection_ignores_unrelated_failure_and_narrows_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retained_runs(database)
        app, candidate_tracks = _application(
            database,
            tmp_path,
            monkeypatch,
            _workspace(
                (_track("facebook", "facebook-run"), _track("ebay", "ebay-run", failed=True))
            ),
        )
        request = SelectionAnalysesRequest(
            workspace_record_identifier="workspace",
            selection=ListingIdsSelection(listing_identifiers=(FACEBOOK_ID,)),
        )
        preview = await app.preview_selection_analyses(request)
        requested = await app.request_selection_analyses(request)
        assert preview.source_search_run_record_identifiers == ("facebook-run",)
        assert requested.source_search_run_record_identifiers == ("facebook-run",)
        assert requested.source_search_refresh_work_identifiers == ("refresh-facebook",)
        assert candidate_tracks == [("facebook",), ("facebook",)]
        payload = (await database.work(requested.work_identifier))["payload"]
        assert payload["source_search_refresh_work_identifiers"] == ["refresh-facebook"]
        assert payload["listing_observation_record_identifiers"] == [f"observation-{FACEBOOK_ID}"]


@pytest.mark.anyio
@pytest.mark.parametrize("whole_workspace", [False, True])
async def test_failed_selected_track_reports_refresh_and_cause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, whole_workspace: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retained_runs(database)
        app, candidate_tracks = _application(
            database,
            tmp_path,
            monkeypatch,
            _workspace(
                (_track("facebook", "facebook-run"), _track("ebay", "ebay-run", failed=True))
            ),
        )
        request = (
            SelectionAnalysesRequest(workspace_record_identifier="workspace")
            if whole_workspace
            else SelectionAnalysesRequest(
                workspace_record_identifier="workspace",
                selection=ListingIdsSelection(listing_identifiers=(EBAY_ID,)),
            )
        )
        with pytest.raises(ReviewInputError) as captured:
            _ = await app.request_selection_analyses(request)
        message = str(captured.value)
        assert "refresh-ebay" in message
        assert "query for ebay" in message
        assert "403" in message
        assert "http_status_403" in message
        assert "error_page" in message
        assert "search_failed" in message
        assert candidate_tracks == []


@pytest.mark.anyio
@pytest.mark.parametrize("overlap_failed", [False, True])
async def test_all_selected_memberships_are_required_and_disabled_tracks_are_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overlap_failed: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retained_runs(database)
        app, candidate_tracks = _application(
            database,
            tmp_path,
            monkeypatch,
            _workspace(
                (
                    _track("facebook", "facebook-run"),
                    _track("overlap", "overlap-run", failed=overlap_failed),
                    _track("disabled", "facebook-run", failed=True, enabled=False),
                )
            ),
        )
        request = SelectionAnalysesRequest(
            workspace_record_identifier="workspace",
            selection=ListingIdsSelection(listing_identifiers=(FACEBOOK_ID,)),
        )
        if overlap_failed:
            with pytest.raises(ReviewInputError, match="refresh-overlap") as captured:
                _ = await app.request_selection_analyses(request)
            assert "refresh-disabled" not in str(captured.value)
            assert candidate_tracks == []
        else:
            requested = await app.request_selection_analyses(request)
            assert requested.source_search_run_record_identifiers == ("facebook-run", "overlap-run")
            assert requested.source_search_refresh_work_identifiers == (
                "refresh-facebook",
                "refresh-overlap",
            )
            assert candidate_tracks == [("facebook", "overlap")]


@pytest.mark.anyio
async def test_selection_in_older_search_ancestry_keeps_its_track(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retained_runs(database)
        await _publish(database, (_facebook_run("facebook-new-run", ()),))
        await _completed_refresh(database, "refresh-facebook", "facebook-run", "facebook-new-run")
        app, candidate_tracks = _application(
            database,
            tmp_path,
            monkeypatch,
            _workspace(
                (_track("facebook", "facebook-new-run"), _track("ebay", "ebay-run", failed=True))
            ),
        )
        requested = await app.request_selection_analyses(
            SelectionAnalysesRequest(
                workspace_record_identifier="workspace",
                selection=ListingIdsSelection(listing_identifiers=(FACEBOOK_ID,)),
            )
        )
        assert requested.source_search_run_record_identifiers == ("facebook-new-run",)
        assert candidate_tracks == [("facebook",)]


@pytest.mark.anyio
async def test_unrelated_track_with_over_one_hundred_ancestor_runs_does_not_block_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retained_runs(database)
        run_identifiers = tuple(f"unrelated-{index}" for index in range(102))
        await _publish(
            database,
            tuple(_facebook_run(identifier, ("999",)) for identifier in run_identifiers),
        )
        for index, (base, refreshed) in enumerate(pairwise(run_identifiers)):
            await _completed_refresh(database, f"unrelated-refresh-{index}", base, refreshed)
        app, candidate_tracks = _application(
            database,
            tmp_path,
            monkeypatch,
            _workspace(
                (
                    _track("facebook", "facebook-run"),
                    _track("long-unrelated", run_identifiers[-1], failed=True),
                )
            ),
        )
        requested = await app.request_selection_analyses(
            SelectionAnalysesRequest(
                workspace_record_identifier="workspace",
                selection=ListingIdsSelection(listing_identifiers=(FACEBOOK_ID,)),
            )
        )
        assert requested.source_search_run_record_identifiers == ("facebook-run",)
        assert requested.source_search_refresh_work_identifiers == ("refresh-facebook",)
        assert candidate_tracks == [("facebook",)]


@pytest.mark.anyio
async def test_selection_only_in_disabled_track_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retained_runs(database)
        app, candidate_tracks = _application(
            database,
            tmp_path,
            monkeypatch,
            _workspace(
                (
                    _track("facebook", "facebook-run"),
                    _track("disabled-ebay", "ebay-run", enabled=False),
                )
            ),
        )
        with pytest.raises(ReviewInputError, match="no listings in active workspace tracks"):
            _ = await app.request_selection_analyses(
                SelectionAnalysesRequest(
                    workspace_record_identifier="workspace",
                    selection=ListingIdsSelection(listing_identifiers=(EBAY_ID,)),
                )
            )
        assert candidate_tracks == []


@pytest.mark.anyio
async def test_mcp_lists_workspace_with_more_than_twenty_tracks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tracks = tuple(
        _track(f"track-{index}", "facebook-run").model_copy(
            update={"search_specification_version": index % 3 + 1}
        )
        for index in range(25)
    )
    workspace = _workspace(tracks)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="workspace",
                    kind=REVIEW_WORKSPACE_KIND,
                    schema_version=1,
                    value=workspace.model_dump(mode="json"),
                ),
            ),
        )
        app, _ = _application(database, tmp_path, monkeypatch, workspace)
        async with Client(build_server(app)) as client:
            result = await client.call_tool("list_review_workspaces")
            result = await complete_tool_result(client, result)
        assert not result.is_error
        assert result.structured_content is not None
        returned = result.structured_content["result"]
        assert len(returned) == 1
        assert len(returned[0]["search_tracks"]) == 25
