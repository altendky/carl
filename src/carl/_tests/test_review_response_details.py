"""Routine responses omit bulky acquisition recipes and prompt-bearing argv."""

from pathlib import Path
from typing import cast

import pytest
from mcp import Client

from carl.core.compact_review import (
    CompactReviewWorkspace,
    analysis_claude_details,
    compact_review_workspace,
)
from carl.core.components import Component, ComponentId
from carl.core.ebay import EbaySearchRequest
from carl.core.marketplace_search import EbaySearchTargetSpecification, Marketplace
from carl.core.models import CodeProvenance, JsonValue, RecordDraft
from carl.core.review import AnalysisDescriptor
from carl.core.review_workspace import (
    REVIEW_WORKSPACE_KIND,
    ReviewStalenessPolicy,
    ReviewWorkspace,
    WorkspaceSearchTrack,
)
from carl.core.work import WorkState
from carl.io.sqlite import Database
from carl.mcp_server import build_server
from carl.review import ReviewApplication


def _workspace(identifier: str, *, archived: bool = False) -> ReviewWorkspace:
    return ReviewWorkspace(
        record_identifier=identifier,
        name="Telescopes",
        search_run_record_identifier="search-run",
        product_guide_record_identifier=None,
        staleness_policy=ReviewStalenessPolicy(),
        created_at_utc="2026-10-15T00:00:00+00:00",
        archived=archived,
        search_tracks=tuple(
            WorkspaceSearchTrack(
                track_identifier=f"track-{index}",
                query=f"telescope {index}",
                marketplace=Marketplace.EBAY,
                origin_search_run_record_identifier=f"origin-{index}",
                current_search_run_record_identifier=f"current-{index}",
                latest_refresh_work_identifier=f"refresh-{index}",
                latest_refresh_work_state=WorkState.COMPLETED,
                latest_refresh_search_specification_version=2,
                enabled=index % 2 == 0,
                search_specification=EbaySearchTargetSpecification(
                    search=EbaySearchRequest(query=f"telescope {index}")
                ),
                search_specification_version=3,
                search_specification_record_identifier=f"specification-{index}-3",
            )
            for index in range(30)
        ),
    )


def test_compact_workspaces_omit_specifications_but_keep_track_scope_and_versions() -> None:
    workspace = _workspace("workspace")
    before = workspace.model_dump_json()
    compact = compact_review_workspace(workspace)
    assert len(compact.search_tracks) == 30
    expected = workspace.model_dump(
        mode="json", exclude={"search_tracks": {"__all__": {"search_specification"}}}
    )
    assert compact.model_dump(mode="json") == expected
    assert all("search_specification" not in track for track in expected["search_tracks"])
    assert workspace.model_dump_json() == before
    assert compact_review_workspace(workspace) == compact


def test_analysis_details_omit_argv_without_changing_retained_metadata() -> None:
    metadata: JsonValue = {
        "argv": ["claude", "-p", "full retained prompt " * 10_000],
        "version": "2.0",
        "tool_calls": [{"name": "WebSearch", "input": {"query": "telescope"}}],
        "external_research_provenance": "agent_reported_urls",
    }
    ordinary = analysis_claude_details(metadata)
    assert isinstance(ordinary, dict)
    assert "argv" not in ordinary
    assert ordinary["version"] == "2.0"
    assert ordinary["tool_calls"] == metadata["tool_calls"]
    assert analysis_claude_details(metadata, include_details=True) == metadata
    assert "argv" in metadata
    assert analysis_claude_details(None) is None


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


@pytest.mark.anyio
async def test_workspace_listing_compact_mode_keeps_default_and_archive_behavior(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspaces = (_workspace("active"), _workspace("archived", archived=True))

    async def get_workspace(
        _application: ReviewApplication, record_identifier: str
    ) -> ReviewWorkspace:
        return next(item for item in workspaces if item.record_identifier == record_identifier)

    monkeypatch.setattr(ReviewApplication, "get_review_workspace", get_workspace)
    async with Database.managed(tmp_path / "workspaces.sqlite3", initialize=True) as database:
        await database.publish_records_operation(
            component=Component(ComponentId(("test", "workspaces")), 1, lambda: None),
            operation_identifier="workspace-operation",
            records=tuple(
                RecordDraft(
                    identifier=item.record_identifier,
                    kind=REVIEW_WORKSPACE_KIND,
                    schema_version=1,
                    value=item.model_dump(mode="json"),
                )
                for item in workspaces
            ),
            inputs=(),
            outputs=(),
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-10-15T00:00:00+00:00",
            ended_at_utc="2026-10-15T00:00:01+00:00",
            duration_ns=1,
            result={"state": "completed"},
        )
        application = ReviewApplication(database, tmp_path)
        assert await application.list_review_workspaces() == (workspaces[0],)
        compact = await application.list_review_workspaces(compact=True)
        assert len(compact) == 1
        assert isinstance(compact[0], CompactReviewWorkspace)
        assert compact[0] == compact_review_workspace(workspaces[0])
        all_compact = await application.list_review_workspaces(include_archived=True, compact=True)
        assert {item.record_identifier for item in all_compact} == {"active", "archived"}

        async with Client(build_server(application)) as client:
            full_wire = await client.call_tool("list_review_workspaces")
            assert not full_wire.is_error
            assert full_wire.structured_content == {
                "result": [workspaces[0].model_dump(mode="json")]
            }
            compact_wire = await client.call_tool("list_review_workspaces", {"compact": True})
            assert not compact_wire.is_error
            assert compact_wire.structured_content == {
                "result": [compact[0].model_dump(mode="json")]
            }
            archived_wire = await client.call_tool(
                "list_review_workspaces", {"compact": True, "include_archived": True}
            )
            assert not archived_wire.is_error
            assert archived_wire.structured_content is not None
            result = cast(list[dict[str, JsonValue]], archived_wire.structured_content["result"])
            assert {item["record_identifier"] for item in result} == {"active", "archived"}


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["completed", "failed"])
async def test_analysis_report_details_are_opt_in_and_retained_record_is_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    descriptor = AnalysisDescriptor(
        analysis_record_identifier="analysis",
        evidence_set_record_identifier="evidence",
        listing_observation_record_identifier="observation",
        product_guide_record_identifier="guide",
        completion_sequence=1,
        completed_at_utc="2026-10-15T00:00:01+00:00",
        state=state,
        warnings=(),
        model="test-model",
    )

    async def analysis_descriptor(
        _database: Database, _record_identifier: str
    ) -> AnalysisDescriptor:
        return descriptor

    monkeypatch.setattr(Database, "facebook_analysis_descriptor", analysis_descriptor)
    claude: JsonValue = {
        "argv": ["claude", "-p", "full retained prompt " * 10_000],
        "version": "2.0",
        "tool_calls": [],
    }
    value: JsonValue = {
        "analysis_text": "Exact retained report",
        "limit_observations": [],
        "claude": claude,
    }
    async with Database.managed(tmp_path / "analysis.sqlite3", initialize=True) as database:
        await database.publish_records_operation(
            component=Component(ComponentId(("test", "analysis")), 1, lambda: None),
            operation_identifier="analysis-operation",
            records=(
                RecordDraft(
                    identifier="analysis",
                    kind=("carl", "facebook", "item_analysis"),
                    schema_version=5,
                    value=value,
                ),
            ),
            inputs=(),
            outputs=(),
            provenance=_provenance(),
            invocation={},
            started_at_utc="2026-10-15T00:00:00+00:00",
            ended_at_utc="2026-10-15T00:00:01+00:00",
            duration_ns=1,
            result={"state": "completed"},
        )
        application = ReviewApplication(database, tmp_path)
        ordinary = await application.get_listing_analysis("analysis")
        detailed = await application.get_listing_analysis("analysis", include_details=True)
        assert ordinary.descriptor == detailed.descriptor == descriptor
        assert ordinary.text == detailed.text == "Exact retained report"
        assert ordinary.claude == analysis_claude_details(claude)
        assert detailed.claude == claude
        assert len(ordinary.model_dump_json()) < len(detailed.model_dump_json()) // 100
        assert (await database.get_record("analysis"))[2] == value

        async with Client(build_server(application)) as client:
            report = await client.call_tool(
                "get_listing_analysis", {"record_identifier": "analysis"}
            )
            assert not report.is_error
            assert report.structured_content == ordinary.model_dump(mode="json")
            details = await client.call_tool(
                "get_listing_analysis", {"record_identifier": "analysis", "include_details": True}
            )
            assert not details.is_error
            assert details.structured_content == detailed.model_dump(mode="json")
