"""Workspace discovery stays small independently of workspace and track growth."""

import json
from pathlib import Path

import pytest
from mcp import Client

from carl.core.review_errors import ReviewInputError
from carl.core.review_workspace import ReviewStalenessPolicy, ReviewWorkspace, WorkspaceSearchTrack
from carl.core.work import WorkState
from carl.core.workspace_summaries import (
    ListReviewWorkspaceSummariesRequest,
    ReviewWorkspaceSummariesPage,
    ReviewWorkspaceSummary,
    build_workspace_summary_page,
    decode_workspace_summaries_cursor,
    review_workspace_summary,
)
from carl.io.sqlite import Database
from carl.mcp_server import build_server
from carl.review import ReviewApplication


def _summary(
    index: int, *, archived: bool = False, name: str = "Astronomy"
) -> ReviewWorkspaceSummary:
    return ReviewWorkspaceSummary(
        record_identifier=f"workspace-{index:04d}",
        name=name,
        search_run_record_identifier=f"search-{index}",
        product_guide_record_identifier="guide",
        created_at_utc="2026-10-15T00:00:00+00:00",
        archived=archived,
        track_count=200,
        enabled_track_count=100,
        completed_track_count=197,
        failed_refresh_track_count=3,
        product_guide_binding_count=1,
        default_product_guide_binding_identifier="binding",
    )


def _assert_size(page: ReviewWorkspaceSummariesPage) -> None:
    assert page.response_bytes == len(page.model_dump_json().encode())
    assert page.response_bytes <= page.response_byte_budget <= 9_000
    assert page.has_more == (page.next_cursor is not None)


def test_workspace_summary_has_counts_without_unbounded_track_payloads() -> None:
    workspace = ReviewWorkspace(
        record_identifier="workspace",
        name="Astronomy",
        search_run_record_identifier="search",
        product_guide_record_identifier=None,
        staleness_policy=ReviewStalenessPolicy(),
        created_at_utc="2026-10-15T00:00:00+00:00",
        search_tracks=tuple(
            WorkspaceSearchTrack(
                track_identifier=f"track-{index}",
                query="large retained query " * 100,
                enabled=index % 2 == 0,
                current_search_run_record_identifier=None if index < 3 else f"run-{index}",
                latest_refresh_work_state=WorkState.TERMINAL_FAILURE
                if index < 3
                else WorkState.COMPLETED,
            )
            for index in range(200)
        ),
    )
    before = workspace.model_dump_json()
    summary = review_workspace_summary(workspace)
    assert summary.track_count == 200
    assert summary.enabled_track_count == 100
    assert summary.completed_track_count == 197
    assert summary.failed_refresh_track_count == 3
    assert summary.record_identifier == workspace.record_identifier
    assert "search_tracks" not in summary.model_dump()
    assert "large retained query" not in summary.model_dump_json()
    assert len(summary.model_dump_json()) < 1_000
    assert workspace.model_dump_json() == before


def test_summary_byte_pagination_preserves_ids_with_changed_page_size_and_budget() -> None:
    workspaces = tuple(_summary(index, name="🔭" * 200) for index in range(35))
    cursor = None
    found: list[str] = []
    pages = 0
    while True:
        request = ListReviewWorkspaceSummariesRequest(
            page_size=100 if pages % 2 == 0 else 2,
            response_byte_budget=3_000 if pages % 2 == 0 else 2_500,
            cursor=cursor,
        )
        page = build_workspace_summary_page(tuple(reversed(workspaces)), request)
        _assert_size(page)
        assert page.workspaces
        found.extend(workspace.record_identifier for workspace in page.workspaces)
        pages += 1
        if not page.has_more:
            break
        decoded = decode_workspace_summaries_cursor(
            request.model_copy(update={"cursor": page.next_cursor})
        )
        assert decoded is not None
        assert decoded.after_workspace_identifier == page.workspaces[-1].record_identifier
        cursor = page.next_cursor
    assert pages > 1
    assert found == [workspace.record_identifier for workspace in workspaces]


def test_summary_pagination_respects_archive_scope_and_external_prefix() -> None:
    workspaces = tuple(_summary(index, archived=index == 1) for index in range(7))
    request = ListReviewWorkspaceSummariesRequest(page_size=2)
    first = build_workspace_summary_page(workspaces[:3], request, more_available=True)
    _assert_size(first)
    assert [workspace.record_identifier for workspace in first.workspaces] == [
        "workspace-0000",
        "workspace-0002",
    ]
    assert first.has_more
    second_request = request.model_copy(update={"cursor": first.next_cursor})
    second = build_workspace_summary_page(workspaces, second_request)
    assert [workspace.record_identifier for workspace in second.workspaces] == [
        "workspace-0003",
        "workspace-0004",
    ]
    with pytest.raises(ReviewInputError, match="archive scope"):
        _ = build_workspace_summary_page(
            workspaces, second_request.model_copy(update={"include_archived": True})
        )
    archived = build_workspace_summary_page(
        workspaces, ListReviewWorkspaceSummariesRequest(include_archived=True)
    )
    assert len(archived.workspaces) == 7
    assert not archived.has_more
    _assert_size(archived)


def test_empty_summary_page_and_invalid_cursor_are_bounded() -> None:
    request = ListReviewWorkspaceSummariesRequest()
    page = build_workspace_summary_page((), request)
    assert page.workspaces == ()
    assert not page.has_more
    _assert_size(page)
    with pytest.raises(ReviewInputError, match="Invalid workspace summary cursor"):
        _ = build_workspace_summary_page((), request.model_copy(update={"cursor": "invalid!"}))
    with pytest.raises(ReviewInputError, match="exceeds"):
        _ = build_workspace_summary_page((_summary(0, name="🔭" * 10_000),), request)


@pytest.mark.anyio
async def test_workspace_summary_mcp_page_has_size_and_continuation_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    summaries = tuple(_summary(index, name='A "quoted" workspace \\ 🔭') for index in range(100))

    async def list_summaries(
        _application: ReviewApplication, request: ListReviewWorkspaceSummariesRequest
    ) -> ReviewWorkspaceSummariesPage:
        return build_workspace_summary_page(summaries, request)

    monkeypatch.setattr(ReviewApplication, "list_review_workspace_summaries", list_summaries)
    async with Database.managed(tmp_path / "summaries.sqlite3", initialize=True) as database:
        application = ReviewApplication(database, tmp_path)
        async with Client(build_server(application)) as client:
            first = await client.call_tool(
                "list_review_workspace_summaries", {"request": {"page_size": 3}}
            )
            assert not first.is_error
            assert first.structured_content is not None
            page = ReviewWorkspaceSummariesPage.model_validate_json(
                json.dumps(first.structured_content)
            )
            _assert_size(page)
            assert first.structured_content == page.model_dump(mode="json")
            assert len(page.workspaces) == 3
            assert page.has_more
            second = await client.call_tool(
                "list_review_workspace_summaries",
                {"request": {"cursor": page.next_cursor, "page_size": 1}},
            )
            assert not second.is_error
            assert second.structured_content is not None
            assert (
                second.structured_content["workspaces"][0]["record_identifier"] == "workspace-0003"
            )
            bounded = await client.call_tool(
                "list_review_workspace_summaries", {"request": {"page_size": 100}}
            )
            assert not bounded.is_error
            assert bounded.structured_content is not None
            bounded_page = ReviewWorkspaceSummariesPage.model_validate_json(
                json.dumps(bounded.structured_content)
            )
            assert bounded_page.workspaces
            assert bounded_page.has_more
            _assert_size(bounded_page)
            assert len(bounded.model_dump_json().encode()) < 25_000


@pytest.mark.anyio
async def test_application_summary_scan_pages_past_archived_workspaces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspaces = tuple(
        ReviewWorkspace(
            record_identifier=f"workspace-{index:04d}",
            name="Astronomy",
            search_run_record_identifier="search",
            product_guide_record_identifier=None,
            created_at_utc="2026-10-15T00:00:00+00:00",
            staleness_policy=ReviewStalenessPolicy(),
            archived=index < 25 or index % 2 == 0,
        )
        for index in range(40)
    )
    queried_after: list[str | None] = []

    async def record_identifiers(
        _database: Database,
        _kind: tuple[str, ...],
        *,
        after_identifier: str | None,
        limit: int,
    ) -> tuple[str, ...]:
        queried_after.append(after_identifier)
        return tuple(
            workspace.record_identifier
            for workspace in workspaces
            if after_identifier is None or workspace.record_identifier > after_identifier
        )[:limit]

    async def get_workspace(
        _application: ReviewApplication, record_identifier: str
    ) -> ReviewWorkspace:
        return next(
            workspace
            for workspace in workspaces
            if workspace.record_identifier == record_identifier
        )

    monkeypatch.setattr(Database, "record_identifiers_by_kind_page", record_identifiers)
    monkeypatch.setattr(ReviewApplication, "get_review_workspace", get_workspace)
    async with Database.managed(tmp_path / "scan.sqlite3", initialize=True) as database:
        application = ReviewApplication(database, tmp_path)
        found: list[str] = []
        cursor = None
        while True:
            page = await application.list_review_workspace_summaries(
                ListReviewWorkspaceSummariesRequest(
                    page_size=2, cursor=cursor, response_byte_budget=1_500
                )
            )
            _assert_size(page)
            assert 1 <= len(page.workspaces) <= 2
            found.extend(workspace.record_identifier for workspace in page.workspaces)
            if not page.has_more:
                break
            cursor = page.next_cursor
        assert found == [
            workspace.record_identifier for workspace in workspaces if not workspace.archived
        ]
        assert "workspace-0019" in queried_after
