"""Bulk refresh preserves independent track outcomes and active-work identity."""

from contextlib import AsyncExitStack
from pathlib import Path

import anyio
import pytest

from carl._tests.test_marketplace_listing import _record
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_workspace_track_revisions import (
    _application,
    _facebook_run,
    _facebook_search,
)
from carl.core.ebay import EbaySearchRequest
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    FacebookSearchTargetSpecification,
)
from carl.core.review_workspace import (
    CreateReviewWorkspaceRequest,
    CreateWorkspaceSearchRequest,
    RequestWorkspaceRefreshAllRequest,
    RequestWorkspaceRefreshAllResult,
    RequestWorkspaceRefreshRequest,
    RequestWorkspaceRefreshResult,
    ReviewWorkspace,
    ReviseWorkspaceSearchTrackRequest,
    SetWorkspaceSearchTrackEnabledRequest,
    WorkspaceSearchTrack,
)
from carl.io.sqlite import Database
from carl.review import ReviewApplication, ReviewInputError


async def _workspace(app: ReviewApplication, database: Database) -> ReviewWorkspace:
    search = _facebook_search()
    ebay = EbaySearchRequest(query="croquet")
    group = await app.create_marketplace_search(
        CreateMarketplaceSearchRequest(
            targets=(
                FacebookSearchTargetSpecification(search=search),
                EbaySearchTargetSpecification(search=ebay),
            )
        )
    )
    await _complete(
        database,
        group.targets[0].executions[0].work_identifier,
        records=(_facebook_run("fb-run", search),),
        result={"search_run_record_identifier": "fb-run"},
    )
    await _complete(
        database,
        group.targets[1].executions[0].work_identifier,
        records=(
            _record(
                "ebay-run",
                "search_run",
                request=ebay.model_dump(mode="json"),
                listing_occurrence_record_identifiers=[],
                listing_count=0,
            ),
        ),
        result={"search_run_record_identifier": "ebay-run", "state": "completed"},
    )
    return await app.create_review_workspace(
        CreateReviewWorkspaceRequest(
            name="Croquet", search_run_record_identifier=group.record_identifier
        )
    )


@pytest.mark.anyio
@pytest.mark.parametrize("separate_connections", (False, True))
async def test_bulk_mixed_tracks_and_concurrent_retries(
    tmp_path: Path, separate_connections: bool
) -> None:
    async with AsyncExitStack() as stack:
        database = await stack.enter_async_context(
            Database.managed(tmp_path / "carl.sqlite3", initialize=True)
        )
        app = await _application(database, tmp_path)
        workspace = await _workspace(app, database)
        second_database = (
            await stack.enter_async_context(Database.managed(database.path))
            if separate_connections
            else database
        )
        second_app = await _application(second_database, tmp_path)
        request = RequestWorkspaceRefreshAllRequest(
            workspace_record_identifier=workspace.record_identifier
        )
        with pytest.raises(ReviewInputError, match="Select track_identifier"):
            await app.request_workspace_refresh(
                RequestWorkspaceRefreshRequest(
                    workspace_record_identifier=workspace.record_identifier
                )
            )
        results: list[RequestWorkspaceRefreshAllResult] = []

        async def refresh(application: ReviewApplication) -> None:
            results.append(await application.request_workspace_refresh_all(request))

        async with anyio.create_task_group() as group:
            group.start_soon(refresh, app)
            group.start_soon(refresh, second_app)
        assert len(results) == 2
        assert set(results[0].work_identifiers) == set(results[1].work_identifiers)
        assert len(results[0].work_identifiers) == 2
        assert sum(track.created is True for result in results for track in result.tracks) == 2
        assert {track.status for result in results for track in result.tracks} == {
            "queued",
            "active",
        }
        assert {
            tuple((await database.work(identifier))["kind"])
            for identifier in results[0].work_identifiers
        } == {
            ("carl", "facebook", "work", "refresh_search"),
            ("carl", "ebay", "work", "refresh_search"),
        }
        retry = await app.request_workspace_refresh_all(request)
        assert all(track.status == "active" and track.created is False for track in retry.tracks)
        assert set(retry.work_identifiers) == set(results[0].work_identifiers)


@pytest.mark.anyio
async def test_bulk_skips_disabled_and_uninitialized_tracks(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = await _application(database, tmp_path)
        workspace = await _workspace(app, database)
        disabled = workspace.search_tracks[0].track_identifier
        await app.set_workspace_search_track_enabled(
            SetWorkspaceSearchTrackEnabledRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=disabled,
                enabled=False,
            )
        )
        pending = await app.create_workspace_search(
            CreateWorkspaceSearchRequest(
                workspace_record_identifier=workspace.record_identifier,
                search=_facebook_search("99"),
            )
        )
        result = await app.request_workspace_refresh_all(
            RequestWorkspaceRefreshAllRequest(
                workspace_record_identifier=workspace.record_identifier
            )
        )
        tracks = {track.track_identifier: track for track in result.tracks}
        assert tracks[disabled].status == "skipped" and tracks[disabled].reason == "disabled"
        assert tracks[pending.track_identifier].status == "skipped"
        assert tracks[pending.track_identifier].reason == "initial_search_incomplete"
        assert len(result.work_identifiers) == 1


@pytest.mark.anyio
async def test_bulk_reuses_active_different_options_and_old_spec(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = await _application(database, tmp_path)
        workspace = await _workspace(app, database)
        fb_track = next(
            track for track in workspace.search_tracks if track.marketplace == "facebook"
        )
        active = await app.request_workspace_refresh(
            RequestWorkspaceRefreshRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=fb_track.track_identifier,
                maximum_items=7,
            )
        )
        await app.revise_workspace_search_track(
            ReviseWorkspaceSearchTrackRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=fb_track.track_identifier,
                expected_version=1,
                search=_facebook_search("300"),
            )
        )
        result = await app.request_workspace_refresh_all(
            RequestWorkspaceRefreshAllRequest(
                workspace_record_identifier=workspace.record_identifier
            )
        )
        outcome = next(
            track for track in result.tracks if track.track_identifier == fb_track.track_identifier
        )
        assert outcome.status == "active" and outcome.created is False
        assert outcome.work_identifier == active.work_identifier
        assert outcome.base_search_run_record_identifier == "fb-run"
        assert (await database.work(active.work_identifier))["payload"]["maximum_items"] == 7


@pytest.mark.anyio
@pytest.mark.parametrize("failure_index", (0, 1))
async def test_bulk_failure_rolls_back_only_its_track(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_index: int
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = await _application(database, tmp_path)
        workspace = await _workspace(app, database)
        original = ReviewApplication._request_workspace_track_refresh
        failing = workspace.search_tracks[failure_index].track_identifier

        async def fail_after_enqueue(
            self: ReviewApplication,
            workspace: ReviewWorkspace,
            track: WorkspaceSearchTrack,
            request: RequestWorkspaceRefreshRequest,
        ) -> RequestWorkspaceRefreshResult:
            result = await original(self, workspace, track, request)
            if track.track_identifier == failing:
                raise ValueError("track-specific failure")
            return result

        monkeypatch.setattr(
            ReviewApplication, "_request_workspace_track_refresh", fail_after_enqueue
        )
        result = await app.request_workspace_refresh_all(
            RequestWorkspaceRefreshAllRequest(
                workspace_record_identifier=workspace.record_identifier
            )
        )
        assert result.tracks[failure_index].status == "failed"
        assert result.tracks[failure_index].reason == "track-specific failure"
        assert result.tracks[1 - failure_index].status == "queued"
        assert len(result.work_identifiers) == 1
        assert (
            await database.active_workspace_track_refresh(workspace.record_identifier, failing)
            is None
        )
        assert (
            len(
                await database.requested_work_edges(
                    requester_kind=("carl", "mcp", "request_workspace_refresh"),
                    requester_identifier=workspace.record_identifier,
                )
            )
            == 1
        )
        monkeypatch.setattr(ReviewApplication, "_request_workspace_track_refresh", original)
        retry = await app.request_workspace_refresh_all(
            RequestWorkspaceRefreshAllRequest(
                workspace_record_identifier=workspace.record_identifier
            )
        )
        assert retry.tracks[failure_index].status == "queued"
        assert retry.tracks[1 - failure_index].status == "active"
        assert (
            retry.tracks[1 - failure_index].work_identifier
            == result.tracks[1 - failure_index].work_identifier
        )


@pytest.mark.anyio
async def test_bulk_rereads_base_after_refresh_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = await _application(database, tmp_path)
        workspace = await _workspace(app, database)
        fb_track = next(
            track for track in workspace.search_tracks if track.marketplace == "facebook"
        )
        active = await app.request_workspace_refresh(
            RequestWorkspaceRefreshRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=fb_track.track_identifier,
            )
        )
        original = ReviewApplication.get_review_workspace

        async def complete_after_snapshot(
            self: ReviewApplication, identifier: str
        ) -> ReviewWorkspace:
            stale = await original(self, identifier)
            await _complete(
                database,
                active.work_identifier,
                records=(_facebook_run("refreshed-run", _facebook_search()),),
                result={"refreshed_search_run_record_identifier": "refreshed-run"},
            )
            return stale

        monkeypatch.setattr(ReviewApplication, "get_review_workspace", complete_after_snapshot)
        result = await app.request_workspace_refresh_all(
            RequestWorkspaceRefreshAllRequest(
                workspace_record_identifier=workspace.record_identifier
            )
        )
        outcome = next(
            track for track in result.tracks if track.track_identifier == fb_track.track_identifier
        )
        assert outcome.status == "queued"
        assert outcome.work_identifier != active.work_identifier
        assert outcome.base_search_run_record_identifier == "refreshed-run"


@pytest.mark.anyio
async def test_bulk_rereads_enabled_state_and_handles_no_eligible_tracks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = await _application(database, tmp_path)
        workspace = await _workspace(app, database)
        original = ReviewApplication.get_review_workspace
        stale = await original(app, workspace.record_identifier)
        for track in stale.search_tracks:
            await app.set_workspace_search_track_enabled(
                SetWorkspaceSearchTrackEnabledRequest(
                    workspace_record_identifier=workspace.record_identifier,
                    track_identifier=track.track_identifier,
                    enabled=False,
                )
            )

        async def stale_snapshot(self: ReviewApplication, identifier: str) -> ReviewWorkspace:
            return stale

        monkeypatch.setattr(ReviewApplication, "get_review_workspace", stale_snapshot)
        result = await app.request_workspace_refresh_all(
            RequestWorkspaceRefreshAllRequest(
                workspace_record_identifier=workspace.record_identifier
            )
        )
        assert len(result.tracks) == 2 and result.work_identifiers == ()
        assert all(
            track.status == "skipped" and track.reason == "disabled" for track in result.tracks
        )


@pytest.mark.anyio
async def test_bulk_cancellation_propagates_and_rolls_back_enqueue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = await _application(database, tmp_path)
        workspace = await _workspace(app, database)
        original = ReviewApplication._request_workspace_track_refresh
        scope = anyio.CancelScope()

        async def cancel_after_enqueue(
            self: ReviewApplication,
            workspace: ReviewWorkspace,
            track: WorkspaceSearchTrack,
            request: RequestWorkspaceRefreshRequest,
        ) -> RequestWorkspaceRefreshResult:
            result = await original(self, workspace, track, request)
            scope.cancel()
            await anyio.lowlevel.checkpoint()
            return result

        monkeypatch.setattr(
            ReviewApplication, "_request_workspace_track_refresh", cancel_after_enqueue
        )
        with scope:
            await app.request_workspace_refresh_all(
                RequestWorkspaceRefreshAllRequest(
                    workspace_record_identifier=workspace.record_identifier
                )
            )
            pytest.fail("Bulk refresh swallowed cancellation")
        assert scope.cancelled_caught
        assert (
            await database.requested_work_edges(
                requester_kind=("carl", "mcp", "request_workspace_refresh"),
                requester_identifier=workspace.record_identifier,
            )
            == ()
        )
