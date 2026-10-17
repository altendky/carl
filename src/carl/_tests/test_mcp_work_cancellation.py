"""Public cancellation stops durable queues without requiring a running worker."""

# Share the offline fixtures used by existing workspace and scheduler tests.
# pyright: reportPrivateUsage=false

import json
from pathlib import Path

import pytest
from mcp import Client

from carl._tests.mcp_response_helpers import complete_tool_result, tool_response_page
from carl._tests.test_review_workspace import _provenance, _search_run_value
from carl._tests.test_work import _capabilities, _definition
from carl.core.components import Component, ComponentId
from carl.core.facebook_refresh import REFRESH_SEARCH_WORK_KIND
from carl.core.models import CodeProvenance, NamedOutput, RecordDraft
from carl.core.review_workspace import CreateReviewWorkspaceRequest, RequestWorkspaceRefreshRequest
from carl.core.work import WorkCapability, WorkRequester, WorkState
from carl.core.work_cancellation import WorkCancellationResult
from carl.io.sqlite import Database
from carl.mcp_server import build_server
from carl.review import ReviewApplication


async def _workspace(application: ReviewApplication) -> str:
    await application.database.publish_records_operation(
        component=Component(ComponentId(("test", "search")), 1, lambda: None),
        operation_identifier="search-operation",
        records=(
            RecordDraft.model_validate(
                {
                    "identifier": "search-run",
                    "kind": ("carl", "facebook", "search_run"),
                    "schema_version": 1,
                    "value": _search_run_value("telescope"),
                }
            ),
        ),
        inputs=(),
        outputs=(NamedOutput(name=("search_run",), object_identifier="search-run"),),
        provenance=_provenance(),
        invocation={},
        started_at_utc="2026-10-17T00:00:00+00:00",
        ended_at_utc="2026-10-17T00:00:01+00:00",
        duration_ns=1,
        result={},
    )
    workspace = await application.create_review_workspace(
        CreateReviewWorkspaceRequest(name="Astronomy", search_run_record_identifier="search-run")
    )
    return workspace.record_identifier


@pytest.mark.anyio
async def test_workspace_mcp_cancels_78_pending_without_workers_and_fresh_refresh_is_new(
    tmp_path: Path,
) -> None:
    async def provenance() -> CodeProvenance:
        return _provenance()

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        application = ReviewApplication(database, tmp_path, code_provenance=provenance)
        workspace_id = await _workspace(application)
        request = RequestWorkspaceRefreshRequest(workspace_record_identifier=workspace_id)
        refresh = await application.request_workspace_refresh(request)
        root_ids = [refresh.work_identifier, *(f"root-{i}" for i in range(15))]
        expected_ids = [*root_ids, *(f"child-{i}" for i in range(62))]
        for index, identifier in enumerate([*expected_ids[1:], "unrelated"]):
            parent = workspace_id if identifier.startswith("root-") else root_ids[index % 16]
            if identifier == "unrelated":
                parent = "another-workspace"
            await database.enqueue_work(
                _definition().model_copy(
                    update={
                        "identifier": identifier,
                        "deduplication_identity": (identifier,),
                    }
                ),
                WorkRequester(
                    request_identifier=f"request-{identifier}",
                    kind=("test", "owner"),
                    identifier=parent,
                    context={},
                ),
                event_identifier=f"enqueue-{identifier}",
                enqueued_at_utc_ns=index,
            )
        async with Client(build_server(application)) as client:
            response = await client.call_tool(
                "cancel_workspace_work",
                {
                    "workspace_record_identifier": workspace_id,
                    "reason": "User said stop all work",
                },
            )
            assert not response.is_error
            result = WorkCancellationResult.model_validate_json(
                json.dumps(response.structured_content)
            )
            assert result.cancelled_pending == 78
            assert result.cancelling_leased == result.already_terminal == 0
            assert set(result.cancelled_pending_identifiers) == set(expected_ids)
            status_response = await client.call_tool(
                "get_workspace_work_status",
                {
                    "workspace_record_identifier": workspace_id,
                },
            )
            status = status_response.structured_content
            assert status is not None
            assert status["queued_count"] == status["in_progress_count"] == 0
            assert status["cancelled_count"] == 16
            assert status["idle"] and not status["successful"]
            assert status["failed_work"] == [] and status["terminal_failure_count"] == 0
            work_response = await client.call_tool(
                "get_work_status",
                {
                    "work_identifier": refresh.work_identifier,
                },
            )
            work = work_response.structured_content
            assert work is not None and work["state"] == work["outcome"] == "cancelled"
            assert work["error_kind"] is None and work["refresh_failure_summary"] is None
            assert work["search_refresh_progress"]["active_phase"] == "cancelled"
            assert work["cancellation"]["reason"] == "User said stop all work"
            repeated = await client.call_tool(
                "cancel_work",
                {
                    "work_identifier": refresh.work_identifier,
                    "reason": "Repeat stop",
                },
            )
            repeat = WorkCancellationResult.model_validate_json(
                json.dumps(repeated.structured_content)
            )
            assert repeat.cancelled_pending == repeat.cancelling_leased == 0
            assert repeat.already_terminal > 0
        assert await database.work_state("unrelated") is WorkState.PENDING
        # No cancelled root or descendant can be claimed when the worker starts.
        capabilities = (
            *_capabilities(),
            WorkCapability(
                kind=REFRESH_SEARCH_WORK_KIND,
                payload_schema_version=1,
            ),
        )
        claim = await database.claim_work(
            supported_capabilities=capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=100,
            utc_now_ns=lambda: 10**18,
            event_identifier="claim",
        )
        assert claim.lease is not None and claim.lease.work_item_identifier == "unrelated"
        second = await database.claim_work(
            supported_capabilities=capabilities,
            worker_identifier="worker",
            lease_token="lease-2",
            lease_duration_ns=100,
            utc_now_ns=lambda: 10**18,
            event_identifier="claim-2",
        )
        assert second.lease is None
        fresh = await application.request_workspace_refresh(request)
        assert fresh.created and fresh.work_identifier != refresh.work_identifier
        assert await database.work_state(fresh.work_identifier) is WorkState.PENDING


@pytest.mark.anyio
async def test_cancellation_confirmation_validation_and_lossless_response_paging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        async with Client(build_server(app)) as client:
            refused = await client.call_tool("cancel_all_work", {"reason": "Stop"})
            assert refused.is_error
            blank = await client.call_tool("cancel_all_work", {"reason": "  ", "confirm": True})
            assert blank.is_error
            identifiers = tuple(f"work-{i:06d}-" + "x" * 50 for i in range(1200))

            async def cancel(
                _app: ReviewApplication, reason: str, confirm: bool = False
            ) -> WorkCancellationResult:
                assert reason == "Stop" and confirm
                return WorkCancellationResult(
                    cancelled_pending=len(identifiers),
                    cancelling_leased=0,
                    already_terminal=0,
                    cancelled_pending_identifiers=identifiers,
                    cancelling_leased_identifiers=(),
                    already_terminal_identifiers=(),
                )

            monkeypatch.setattr(ReviewApplication, "cancel_all_work", cancel)
            result = await client.call_tool("cancel_all_work", {"reason": "Stop", "confirm": True})
            assert tool_response_page(result) is not None
            complete = await complete_tool_result(client, result)
            assert (
                WorkCancellationResult.model_validate_json(
                    json.dumps(complete.structured_content)
                ).cancelled_pending_identifiers
                == identifiers
            )
