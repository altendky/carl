"""MCP registration and an in-process protocol smoke test."""

import json
import sys
from pathlib import Path
from typing import Any

import anyio
import pytest
from mcp import Client, StdioServerParameters
from mcp.types import TextContent

from carl.core.composed_projection import (
    ComposedListingPage,
    ComposedListingProjection,
    ComposedStatus,
    GetComposedListingRequest,
    ListComposedSearchRequest,
    ListingStatus,
    ProjectionEvidence,
    ProjectionSourceKind,
    canonical_facebook_listing_url,
    projection_revision,
)
from carl.core.facebook_images import (
    ImageFailureSourceKind,
    RetryImageFailuresRequest,
    RetryImageFailuresResult,
)
from carl.io.sqlite import Database
from carl.mcp_server import build_server, managed_mcp_runtime, tool_definitions
from carl.review import ReviewApplication

EXPECTED_TOOLS = {
    "acquire_review_batch",
    "add_workspace_product_guide",
    "create_review_workspace",
    "create_review_workset",
    "create_selection_snapshot",
    "create_product_guide",
    "create_search",
    "create_workspace_search",
    "get_activity_snapshot",
    "get_composed_listing",
    "get_listing_analysis",
    "get_listing_image",
    "get_product_guide",
    "get_review_batch",
    "get_review_workspace_activity",
    "get_workspace_work_status",
    "get_workspace_listing",
    "get_server_info",
    "get_search_run_listings",
    "get_provenance",
    "get_work_status",
    "list_composed_search",
    "list_search_runs",
    "list_product_guides",
    "list_review_worksets",
    "list_review_workspaces",
    "list_workspace_listings",
    "list_workspace_product_guides",
    "preview_selection_analyses",
    "request_selection_analyses",
    "request_search_refresh",
    "request_workspace_refresh",
    "rename_review_workspace",
    "set_review_workspace_archived",
    "set_product_guide_identity_retired",
    "set_workspace_search_track_enabled",
    "set_workspace_default_product_guide",
    "record_listing_reviews",
    "release_review_claim",
    "retry_image_failures",
    "retry_workspace_search_track",
    "revise_product_guide",
    "renew_review_claim",
    "update_review_workset",
    "update_workspace_product_guide_binding",
    "wait_for_workspace_work",
    "get_selection_snapshot",
}


def _contract_documents(tools: list[Any]) -> list[dict[str, Any]]:
    return [
        tool.model_dump(mode="json", by_alias=True, exclude_none=True)
        for tool in sorted(tools, key=lambda value: value.name)
    ]


def _expected_contract_documents() -> list[dict[str, Any]]:
    path = Path(__file__).with_name("mcp_tool_contracts.json")
    return json.loads(path.read_text())


def _assert_analysis_batch_contracts(tools_by_name: dict[str, Any]) -> None:
    preview_schema = dict(tools_by_name["preview_selection_analyses"].input_schema)
    request_schema = dict(tools_by_name["request_selection_analyses"].input_schema)
    preview_schema.pop("title")
    request_schema.pop("title")
    assert request_schema == preview_schema
    request_model = request_schema["$defs"]["SelectionAnalysesRequest"]
    selection = request_model["properties"]["selection_policy"]
    assert selection["$ref"].endswith("/ListingAnalysisSelectionPolicy")
    assert request_schema["$defs"]["ListingAnalysisSelectionPolicy"]["enum"] == [
        "missing_for_current_evidence",
        "missing_for_selected_guide",
        "never_analyzed_listing",
    ]


@pytest.mark.anyio
async def test_composed_projection_tools_accept_real_json_and_return_structured_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    evidence = ProjectionEvidence(
        evidence_record_identifier="status-evidence",
        acquisition_completion_sequence=1,
        observation_completion_sequence=1,
        source_kind=ProjectionSourceKind.SEARCH_CARD,
    )
    status = ComposedStatus(
        value=ListingStatus.AVAILABLE,
        raw_flags={"is_live": True},
        evidence=evidence,
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
            analyses_truncated=False,
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
        analyses_truncated=False,
        search_membership=None,
    )
    observed_requests: list[ListComposedSearchRequest] = []

    async def get_composed_listing(
        _application: ReviewApplication, request: GetComposedListingRequest
    ) -> ComposedListingProjection:
        assert request.listing_identifier == "123"
        return projection

    async def list_composed_search(
        _application: ReviewApplication, request: ListComposedSearchRequest
    ) -> ComposedListingPage:
        observed_requests.append(request)
        return ComposedListingPage(
            as_of_completion_sequence=1,
            selected_search_run_record_identifier=request.search_run_record_identifier,
            included_ancestry_run_count=1,
            older_ancestry_truncated=False,
            examined_candidate_listing_count=1,
            candidate_examination_limit_reached=False,
            listings=(projection,),
            next_cursor=None,
        )

    monkeypatch.setattr(ReviewApplication, "get_composed_listing", get_composed_listing)
    monkeypatch.setattr(ReviewApplication, "list_composed_search", list_composed_search)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        application = ReviewApplication(database=database, repository_root=tmp_path)
        async with Client(build_server(application)) as client:
            exact = await client.call_tool(
                "get_composed_listing", {"request": {"listing_identifier": "123"}}
            )
            page = await client.call_tool(
                "list_composed_search",
                {
                    "request": {
                        "search_run_record_identifier": "search-run",
                        "filters": {"statuses": ["pending", "available"]},
                    }
                },
            )

    assert not exact.is_error
    assert exact.structured_content is not None
    assert exact.structured_content["listing_identifier"] == "123"
    assert not page.is_error
    assert page.structured_content is not None
    assert page.structured_content["listings"][0]["status"]["value"] == "available"
    assert tuple(observed_requests[0].filters.statuses) == (
        ListingStatus.PENDING,
        ListingStatus.AVAILABLE,
    )


@pytest.mark.anyio
async def test_mcp_tool_discovery_and_empty_guide_listing(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        application = ReviewApplication(
            database=database,
            repository_root=tmp_path,
            server_source_tree_sha256="a" * 64,
        )
        definitions = tool_definitions(application)
        assert {definition.name for definition in definitions} == EXPECTED_TOOLS
        assert len({definition.operation for definition in definitions}) == len(definitions)
        definitions_by_name = {definition.name: definition for definition in definitions}
        for name in (
            "acquire_review_batch",
            "record_listing_reviews",
            "renew_review_claim",
            "release_review_claim",
        ):
            assert definitions_by_name[name].annotations.idempotent_hint
        refresh = next(
            definition for definition in definitions if definition.name == "request_search_refresh"
        )
        assert refresh.annotations.open_world_hint

        async with Client(build_server(application)) as client:
            called_tools: set[str] = set()

            async def call_json(name: str, arguments: dict[str, Any] | None = None) -> Any:
                called_tools.add(name)
                decoded_arguments = json.loads(json.dumps(arguments or {}))
                return await client.call_tool(name, decoded_arguments)

            assert client.server_info is not None
            assert client.server_info.version.endswith("+source.aaaaaaaaaaaa")
            listing = await client.list_tools()
            assert {tool.name for tool in listing.tools} == EXPECTED_TOOLS
            assert _contract_documents(listing.tools) == _expected_contract_documents()
            tools_by_name = {tool.name: tool for tool in listing.tools}
            activity_properties = tools_by_name["get_activity_snapshot"].input_schema["properties"]
            assert activity_properties["recent_window_minutes"] == {
                "default": 60,
                "maximum": 10080,
                "minimum": 1,
                "title": "Recent Window Minutes",
                "type": "integer",
            }
            assert activity_properties["maximum_rows"] == {
                "default": 20,
                "maximum": 100,
                "minimum": 1,
                "title": "Maximum Rows",
                "type": "integer",
            }
            work_status_properties = tools_by_name["get_work_status"].input_schema["properties"]
            assert work_status_properties["include_details"] == {
                "default": False,
                "title": "Include Details",
                "type": "boolean",
            }
            record_reviews_schema = tools_by_name["record_listing_reviews"].input_schema
            record_reviews_request = record_reviews_schema["$defs"][
                "RecordClaimedListingReviewsRequest"
            ]
            assert {
                "request_identifier",
                "workspace_record_identifier",
                "batch_record_identifier",
                "claim_token",
                "claim_owner_identifier",
                "reviews",
            } <= set(record_reviews_request["required"])
            for name, tool in tools_by_name.items():
                definition = definitions_by_name[name]
                assert tool.meta == {
                    "carl/operation": list(definition.operation),
                }
            _assert_analysis_batch_contracts(tools_by_name)
            server_info = await call_json("get_server_info")
            assert not server_info.is_error
            assert server_info.structured_content is not None
            assert server_info.structured_content["repository_root"] == str(tmp_path)
            assert server_info.structured_content["database_path"] == str(tmp_path / "carl.sqlite3")
            assert server_info.structured_content["source_tree_sha256"] == "a" * 64
            assert {
                tuple(capability["identity"])
                for capability in server_info.structured_content["capabilities"]
            } >= {
                ("carl", "mcp", "instructions"),
                ("carl", "mcp", "tool_contracts"),
                ("carl", "activity", "snapshot"),
                ("carl", "facebook", "create_search"),
                ("carl", "facebook", "search_run_summary"),
                ("carl", "facebook", "search_run_membership"),
                ("carl", "facebook", "search_refresh", "child_progress"),
                ("carl", "facebook", "search_transport_retry"),
                ("carl", "facebook", "image_failure_retry"),
                ("carl", "facebook", "analysis_batch"),
                ("carl", "facebook", "analysis_timeout_retry"),
                ("carl", "facebook", "listing_analysis_history"),
                ("carl", "facebook", "listing_availability"),
                ("carl", "review", "provenance_summary"),
                ("carl", "review", "composed_projection"),
                ("carl", "review", "workspace"),
                ("carl", "review", "workspace_product_guides"),
                ("carl", "review", "product_guides"),
                ("carl", "review", "claims"),
                ("carl", "review", "selection_snapshot"),
                ("carl", "review", "selection_analysis"),
                ("carl", "work", "explicit_runtime"),
            }
            capability_versions = {
                tuple(capability["identity"]): capability["version"]
                for capability in server_info.structured_content["capabilities"]
            }
            assert capability_versions[("carl", "activity", "snapshot")] == 2
            assert capability_versions[("carl", "facebook", "image_failure_retry")] == 2
            assert capability_versions[("carl", "facebook", "collect_image_work")] == 2
            assert capability_versions[("carl", "facebook", "create_search")] == 1
            assert capability_versions[("carl", "facebook", "search_transport_retry")] == 3
            assert capability_versions[("carl", "facebook", "search_run_summary")] == 1
            assert capability_versions[("carl", "facebook", "search_run_membership")] == 1
            assert capability_versions[("carl", "facebook", "analysis_batch")] == 6
            assert capability_versions[("carl", "review", "provenance_summary")] == 1
            assert capability_versions[("carl", "mcp", "instructions")] == 32
            assert capability_versions[("carl", "facebook", "analysis_timeout_retry")] == 1
            assert capability_versions[("carl", "mcp", "tool_contracts")] == 21
            assert capability_versions[("carl", "review", "workspace_work")] == 3
            assert capability_versions[("carl", "review", "composed_projection")] == 4
            assert capability_versions[("carl", "review", "workspace")] == 5
            assert capability_versions[("carl", "review", "workspace_product_guides")] == 1
            assert capability_versions[("carl", "review", "product_guides")] == 1
            assert capability_versions[("carl", "review", "workspace_search_tracks")] == 3
            assert capability_versions[("carl", "review", "claims")] == 1
            assert capability_versions[("carl", "review", "selection_snapshot")] == 1
            assert capability_versions[("carl", "review", "selection_analysis")] == 4
            result = await call_json("list_product_guides")
            assert not result.is_error
            assert result.structured_content is not None
            created_guide = await call_json(
                "create_product_guide",
                {
                    "request": {
                        "identity": ["carl", "product_guide", "mcp_test"],
                        "display_name": "MCP test guide",
                        "text": "Exercise the decoded JSON request path.",
                    }
                },
            )
            assert not created_guide.is_error
            assert created_guide.structured_content is not None
            assert created_guide.structured_content["product_guide"]["identity"] == [
                "carl",
                "product_guide",
                "mcp_test",
            ]
            guide_record_identifier = created_guide.structured_content["product_guide"][
                "record_identifier"
            ]
            fetched_guide = await call_json(
                "get_product_guide", {"record_identifier": guide_record_identifier}
            )
            assert not fetched_guide.is_error
            revised_guide = await call_json(
                "revise_product_guide",
                {
                    "request": {
                        "expected_base_record_identifier": guide_record_identifier,
                        "display_name": "Revised MCP test guide",
                        "text": "Exercise another decoded JSON request path.",
                    }
                },
            )
            assert not revised_guide.is_error
            assert revised_guide.structured_content is not None
            revised_guide_identifier = revised_guide.structured_content["product_guide"][
                "record_identifier"
            ]
            retired_guide = await call_json(
                "set_product_guide_identity_retired",
                {
                    "request": {
                        "product_guide_record_identifier": revised_guide_identifier,
                        "retired": True,
                    }
                },
            )
            assert not retired_guide.is_error
            assert retired_guide.structured_content is not None
            assert retired_guide.structured_content["retired"] is True
            assert (await call_json("list_product_guides")).structured_content == {"result": []}
            retired_listing = await call_json("list_product_guides", {"include_retired": True})
            assert retired_listing.structured_content is not None
            assert len(retired_listing.structured_content["result"]) == 2
            provenance = await call_json(
                "get_provenance", {"object_identifier": guide_record_identifier}
            )
            assert not provenance.is_error
            assert provenance.structured_content is not None
            assert provenance.structured_content["output_edge_count"] == 2
            assert provenance.structured_content["output_edges_truncated"]
            assert provenance.structured_content["outputs"] == []
            bounded_provenance = await call_json(
                "get_provenance",
                {
                    "object_identifier": guide_record_identifier,
                    "maximum_output_edges": 1,
                },
            )
            assert not bounded_provenance.is_error
            assert bounded_provenance.structured_content is not None
            assert len(bounded_provenance.structured_content["outputs"]) == 1
            assert bounded_provenance.structured_content["output_edges_truncated"]
            full_provenance = await call_json(
                "get_provenance",
                {
                    "object_identifier": guide_record_identifier,
                    "maximum_output_edges": None,
                },
            )
            assert not full_provenance.is_error
            assert full_provenance.structured_content is not None
            assert len(full_provenance.structured_content["outputs"]) == 2
            assert not full_provenance.structured_content["output_edges_truncated"]
            activity = await call_json("get_activity_snapshot")
            assert not activity.is_error
            assert activity.structured_content is not None
            assert activity.structured_content["active_work"] == []
            assert activity.structured_content["recent_window_ns"] == 60 * 60 * 1_000_000_000
            assert any(
                process["current_process"]
                for process in activity.structured_content["database_processes"]
            )
            invalid_activity = await call_json("get_activity_snapshot", {"maximum_rows": 0})
            assert invalid_activity.is_error
            assert isinstance(invalid_activity.content[0], TextContent)
            assert "greater than or equal to 1" in invalid_activity.content[0].text
            searches = await call_json("list_search_runs")
            assert not searches.is_error
            assert searches.structured_content == {"search_runs": []}
            missing_search_membership = await call_json(
                "get_search_run_listings",
                {
                    "search_run_record_identifier": "missing-search-run",
                    "offset": 0,
                    "limit": 100,
                },
            )
            assert missing_search_membership.is_error
            created_search = await call_json(
                "create_search",
                {
                    "request": {
                        "request": {
                            "query": "beverage cooler",
                            "location": {
                                "kind": "facebook_location",
                                "identifier": "123",
                                "label": "Example City, PA",
                            },
                            "radius": {"value": 30, "unit": "miles"},
                            "price": {
                                "currency": "USD",
                                "minimum": "0",
                                "maximum": 150,
                            },
                        },
                        "traversal": {"maximum_results": 600, "maximum_pages": 200},
                        "traversal_strategy": {
                            "kind": "overlapping_price_partitions",
                            "width": "10",
                            "overlap": "0",
                            "order": "balanced",
                        },
                    }
                },
            )
            assert not created_search.is_error
            assert created_search.structured_content is not None
            assert created_search.structured_content["created"]
            assert created_search.structured_content["state"] == "pending"
            created_search_work = await database.work(
                created_search.structured_content["work_identifier"]
            )
            assert created_search_work["payload"]["routing"] == [
                "proton",
                "personal",
                "carl",
            ]
            assert created_search_work["payload"]["request"]["price"] == {
                "currency": "USD",
                "minimum": "0",
                "maximum": "150",
            }
            work_status = await call_json(
                "get_work_status",
                {
                    "work_identifier": created_search.structured_content["work_identifier"],
                    "include_details": True,
                },
            )
            assert not work_status.is_error
            missing_workspace_claim = await call_json(
                "acquire_review_batch",
                {
                    "request": {
                        "workspace_record_identifier": "missing-workspace",
                        "request_identifier": "mcp-acquire-1",
                        "owner_identifier": "mcp-agent",
                    }
                },
            )
            assert missing_workspace_claim.is_error
            assert isinstance(missing_workspace_claim.content[0], TextContent)
            assert "not found" in missing_workspace_claim.content[0].text

            for tool_name, request in (
                (
                    "renew_review_claim",
                    {
                        "request_identifier": "mcp-renew-1",
                        "claim_token": "missing-claim",
                        "owner_identifier": "mcp-agent",
                    },
                ),
                (
                    "release_review_claim",
                    {
                        "request_identifier": "mcp-release-1",
                        "claim_token": "missing-claim",
                        "owner_identifier": "mcp-agent",
                    },
                ),
            ):
                missing_claim = await call_json(tool_name, {"request": request})
                assert missing_claim.is_error
                assert isinstance(missing_claim.content[0], TextContent)
                assert "missing, expired, or owned elsewhere" in missing_claim.content[0].text

            missing_composed_listing = await call_json(
                "get_composed_listing",
                {
                    "request": {
                        "listing_identifier": "123456789",
                        "maximum_gallery_images": 0,
                        "maximum_analyses": 0,
                    }
                },
            )
            assert missing_composed_listing.is_error
            assert isinstance(missing_composed_listing.content[0], TextContent)
            assert "not found" in missing_composed_listing.content[0].text

            missing_composed_search = await call_json(
                "list_composed_search",
                {
                    "request": {
                        "search_run_record_identifier": "missing-search-run",
                        "filters": {
                            "statuses": ["available", "pending"],
                            "analysis": "any",
                        },
                        "page_size": 10,
                    }
                },
            )
            assert missing_composed_search.is_error
            assert isinstance(missing_composed_search.content[0], TextContent)
            assert "not found" in missing_composed_search.content[0].text
            assert "instance of" not in missing_composed_search.content[0].text

            workspaces = await call_json("list_review_workspaces")
            assert not workspaces.is_error
            assert workspaces.structured_content == {"result": []}
            archived_workspaces = await call_json(
                "list_review_workspaces", {"include_archived": True}
            )
            assert not archived_workspaces.is_error
            assert archived_workspaces.structured_content == {"result": []}
            revision_json = {
                "recipe_version": 1,
                "status_sha256": "a" * 64,
                "scalar_fields_sha256": "b" * 64,
                "preview_image_sha256": "c" * 64,
                "gallery_sha256": "d" * 64,
                "analyses_sha256": "e" * 64,
                "search_membership_sha256": "f" * 64,
                "aggregate_sha256": "0" * 64,
            }
            workspace_calls = (
                await call_json(
                    "create_review_workspace",
                    {
                        "request": {
                            "name": "Fridge review",
                            "search_run_record_identifier": "missing-search-run",
                            "staleness_policy": {"components": ["scalar_fields", "gallery"]},
                        }
                    },
                ),
                await call_json(
                    "record_listing_reviews",
                    {
                        "request": {
                            "request_identifier": "mcp-review-1",
                            "workspace_record_identifier": "missing-workspace",
                            "batch_record_identifier": "missing-batch",
                            "claim_token": "missing-claim",
                            "claim_owner_identifier": "mcp-agent",
                            "reviews": [
                                {
                                    "listing_identifier": "123",
                                    "projection_revision": revision_json,
                                    "inspected": True,
                                    "disposition": "promising",
                                    "note": "Worth comparing.",
                                }
                            ],
                        }
                    },
                ),
                await call_json(
                    "create_review_workset",
                    {
                        "request": {
                            "workspace_record_identifier": "missing-workspace",
                            "name": "Shortlist",
                            "listing_identifiers": ["123", "456"],
                        }
                    },
                ),
                await call_json(
                    "update_review_workset",
                    {
                        "request": {
                            "workset_identifier": "missing-workset",
                            "expected_version": 1,
                            "add_listing_identifiers": ["789"],
                        }
                    },
                ),
                await call_json(
                    "create_selection_snapshot",
                    {
                        "request": {
                            "workspace_record_identifier": "missing-workspace",
                            "selection": {
                                "kind": "listing_ids",
                                "listing_identifiers": ["123"],
                            },
                        }
                    },
                ),
                await call_json(
                    "create_workspace_search",
                    {
                        "request": {
                            "workspace_record_identifier": "missing-workspace",
                            "search": {
                                "request": {
                                    "query": "mini fridge",
                                    "location": {
                                        "kind": "facebook_location",
                                        "identifier": "123",
                                        "label": "Example City, PA",
                                    },
                                    "radius": {"value": 30, "unit": "miles"},
                                },
                                "traversal": {
                                    "maximum_results": 100,
                                    "maximum_pages": 20,
                                },
                            },
                        }
                    },
                ),
                await call_json(
                    "retry_workspace_search_track",
                    {
                        "request": {
                            "workspace_record_identifier": "missing-workspace",
                            "track_identifier": "missing-track",
                        }
                    },
                ),
                await call_json(
                    "request_workspace_refresh",
                    {
                        "request": {
                            "workspace_record_identifier": "missing-workspace",
                            "track_identifier": "missing-track",
                        }
                    },
                ),
                await call_json(
                    "set_workspace_search_track_enabled",
                    {
                        "request": {
                            "workspace_record_identifier": "missing-workspace",
                            "track_identifier": "missing-track",
                            "enabled": False,
                        }
                    },
                ),
                await call_json(
                    "rename_review_workspace",
                    {
                        "workspace_record_identifier": "missing-workspace",
                        "name": "Astronomy",
                    },
                ),
                await call_json(
                    "set_review_workspace_archived",
                    {
                        "workspace_record_identifier": "missing-workspace",
                        "archived": True,
                    },
                ),
                await call_json(
                    "add_workspace_product_guide",
                    {
                        "request": {
                            "workspace_record_identifier": "missing-workspace",
                            "product_guide_record_identifier": "missing-guide",
                            "alias": "astronomy_gear",
                            "version_policy": "follow_latest",
                            "make_default": True,
                        }
                    },
                ),
                await call_json(
                    "update_workspace_product_guide_binding",
                    {
                        "request": {
                            "workspace_record_identifier": "missing-workspace",
                            "binding_identifier": "missing-binding",
                            "version_policy": "pinned",
                            "product_guide_record_identifier": "missing-guide",
                            "enabled": True,
                        }
                    },
                ),
                await call_json(
                    "set_workspace_default_product_guide",
                    {
                        "request": {
                            "workspace_record_identifier": "missing-workspace",
                            "binding_identifier": "missing-binding",
                        }
                    },
                ),
            )
            for failed_call in workspace_calls:
                assert failed_call.is_error
                assert isinstance(failed_call.content[0], TextContent)
                assert "instance of" not in failed_call.content[0].text

            for name, arguments in (
                ("get_review_batch", {"record_identifier": "missing-batch"}),
                (
                    "get_review_workspace_activity",
                    {
                        "workspace_record_identifier": "missing-workspace",
                        "maximum_recent_batches": 5,
                        "maximum_recent_reviews": 10,
                        "maximum_recent_selection_snapshots": 5,
                    },
                ),
                (
                    "get_workspace_work_status",
                    {
                        "workspace_record_identifier": "missing-workspace",
                        "maximum_active_work": 5,
                    },
                ),
                (
                    "get_workspace_listing",
                    {
                        "request": {
                            "workspace_record_identifier": "missing-workspace",
                            "listing_identifier": "123",
                            "maximum_gallery_images": 5,
                        }
                    },
                ),
                (
                    "list_workspace_listings",
                    {
                        "request": {
                            "workspace_record_identifier": "missing-workspace",
                            "filters": {"statuses": ["available", "pending"]},
                            "page_size": 10,
                        }
                    },
                ),
                (
                    "wait_for_workspace_work",
                    {
                        "workspace_record_identifier": "missing-workspace",
                        "timeout_seconds": 0,
                        "maximum_active_work": 5,
                    },
                ),
                (
                    "list_review_worksets",
                    {"workspace_record_identifier": "missing-workspace"},
                ),
                (
                    "list_workspace_product_guides",
                    {
                        "workspace_record_identifier": "missing-workspace",
                        "include_disabled": True,
                    },
                ),
                (
                    "get_selection_snapshot",
                    {"record_identifier": "missing-selection-snapshot"},
                ),
            ):
                missing_review_object = await call_json(name, arguments)
                assert missing_review_object.is_error
                assert isinstance(missing_review_object.content[0], TextContent)

            selected_guide = await call_json(
                "preview_selection_analyses",
                {
                    "request": {
                        "workspace_record_identifier": "missing-workspace",
                        "product_guide_record_identifier": "missing-guide",
                        "selection": {
                            "kind": "listing_ids",
                            "listing_identifiers": ["123", "456"],
                        },
                        "statuses": ["available", "pending"],
                        "selection_policy": "missing_for_selected_guide",
                    }
                },
            )
            assert selected_guide.is_error
            assert isinstance(selected_guide.content[0], TextContent)
            assert "not found" in selected_guide.content[0].text
            assert "instance of" not in selected_guide.content[0].text

            traversal_order = await call_json(
                "request_search_refresh",
                {
                    "request": {
                        "base_search_run_record_identifier": "missing-search",
                        "traversal_strategy": {
                            "kind": "overlapping_price_partitions",
                            "width": "10",
                            "overlap": "2",
                            "order": "ascending",
                        },
                    }
                },
            )
            assert traversal_order.is_error
            assert isinstance(traversal_order.content[0], TextContent)
            assert "not found" in traversal_order.content[0].text
            assert "instance of" not in traversal_order.content[0].text

            image_retry = await call_json(
                "retry_image_failures",
                {
                    "request": {
                        "source_identifier": "missing-search-or-refresh",
                        "maximum_items": 10,
                    }
                },
            )
            assert image_retry.is_error
            assert isinstance(image_retry.content[0], TextContent)
            assert "not found" in image_retry.content[0].text
            assert "instance of" not in image_retry.content[0].text

            missing_analysis = await call_json(
                "get_listing_analysis", {"record_identifier": "missing-analysis"}
            )
            missing_image = await call_json(
                "get_listing_image", {"artifact_identifier": "missing-image"}
            )
            requested_batch = await call_json(
                "request_selection_analyses",
                {
                    "request": {
                        "workspace_record_identifier": "missing-workspace",
                        "product_guide_binding_identifier": "missing-binding",
                        "selection": {"kind": "workspace"},
                        "selection_policy": "never_analyzed_listing",
                        "maximum_items": 5,
                        "allow_incomplete_gallery": True,
                    }
                },
            )
            for domain_error in (
                missing_analysis,
                missing_image,
                requested_batch,
            ):
                assert domain_error.is_error
                assert isinstance(domain_error.content[0], TextContent)
                assert "instance of" not in domain_error.content[0].text

            assert called_tools == EXPECTED_TOOLS


@pytest.mark.anyio
async def test_carl_mcp_stdio_process_smoke(tmp_path: Path) -> None:
    database_path = tmp_path / "carl.sqlite3"
    async with Database.managed(database_path, initialize=True):
        pass
    parameters = StdioServerParameters(
        command=sys.executable,
        args=["-m", "carl.cli", "mcp", "--database", str(database_path)],
        cwd=Path(__file__).resolve().parents[3],
    )
    async with Client(parameters) as client:
        assert client.server_info is not None
        assert "+source." in client.server_info.version
        listing = await client.list_tools()
        assert {tool.name for tool in listing.tools} == EXPECTED_TOOLS
        assert _contract_documents(listing.tools) == _expected_contract_documents()
        _assert_analysis_batch_contracts({tool.name: tool for tool in listing.tools})


@pytest.mark.anyio
async def test_carl_mcp_stdio_process_exits_when_client_input_closes(tmp_path: Path) -> None:
    database_path = tmp_path / "carl.sqlite3"
    async with Database.managed(database_path, initialize=True):
        pass
    process = await anyio.open_process(
        [
            sys.executable,
            "-m",
            "carl.cli",
            "mcp",
            "--database",
            str(database_path),
        ],
        cwd=Path(__file__).resolve().parents[3],
    )
    assert process.stdin is not None
    await process.stdin.aclose()
    try:
        with anyio.fail_after(10):
            assert await process.wait() == 0
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.anyio
async def test_mcp_runtime_does_not_start_queue_workers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    database_path = tmp_path / "carl.sqlite3"
    async with Database.managed(database_path, initialize=True):
        pass
    claim_attempted = anyio.Event()

    async def unexpected_claim(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        claim_attempted.set()
        raise AssertionError("MCP runtime started an implicit queue worker")

    monkeypatch.setattr(Database, "claim_work", unexpected_claim)
    async with managed_mcp_runtime(database_path, Path(__file__).resolve().parents[3]):
        await anyio.sleep(0.1)

    assert not claim_attempted.is_set()


@pytest.mark.anyio
async def test_mcp_runtime_is_available_while_database_migration_runs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    database_path = tmp_path / "carl.sqlite3"
    async with Database.managed(database_path, initialize=True):
        pass
    migration_started = anyio.Event()
    release_migration = anyio.Event()
    migration_completed = anyio.Event()
    migrate = Database.migrate

    async def slow_migration(self: Database) -> None:
        migration_started.set()
        await release_migration.wait()
        await migrate(self)
        migration_completed.set()

    monkeypatch.setattr(Database, "migrate", slow_migration)
    with anyio.fail_after(2):
        async with managed_mcp_runtime(
            database_path, Path(__file__).resolve().parents[3]
        ) as runtime:
            await migration_started.wait()
            assert runtime.server is not None
            assert not migration_completed.is_set()
            release_migration.set()
            await migration_completed.wait()


@pytest.mark.anyio
async def test_concurrent_tool_call_survives_slow_failing_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    slow_started = anyio.Event()
    release_slow = anyio.Event()

    async def controlled_retry(
        self: ReviewApplication, request: RetryImageFailuresRequest
    ) -> RetryImageFailuresResult:
        del self
        if request.source_identifier == "slow-refresh":
            slow_started.set()
            await release_slow.wait()
            raise RuntimeError("simulated concurrent tool failure")
        return RetryImageFailuresResult(
            source_identifier=request.source_identifier,
            source_kind=ImageFailureSourceKind.SEARCH_REFRESH,
            matched_terminal_failures=0,
            retried=0,
            remaining_terminal_failures=0,
            retried_work_identifier_sample=(),
        )

    monkeypatch.setattr(ReviewApplication, "retry_image_failures", controlled_retry)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        application = ReviewApplication(
            database=database,
            repository_root=tmp_path,
            server_source_tree_sha256="a" * 64,
        )
        slow_results: list[Any] = []
        async with Client(build_server(application)) as client:

            async def call_slow() -> None:
                slow_results.append(
                    await client.call_tool(
                        "retry_image_failures",
                        {"request": {"source_identifier": "slow-refresh"}},
                    )
                )

            async with anyio.create_task_group() as group:
                group.start_soon(call_slow)
                await slow_started.wait()
                with anyio.fail_after(2):
                    concurrent = await client.call_tool(
                        "retry_image_failures",
                        {"request": {"source_identifier": "fast-refresh"}},
                    )
                assert not concurrent.is_error
                release_slow.set()

            assert len(slow_results) == 1
            assert slow_results[0].is_error
            assert isinstance(slow_results[0].content[0], TextContent)
            assert "internal error" in slow_results[0].content[0].text
            with anyio.fail_after(2):
                info = await client.call_tool("get_server_info")
            assert not info.is_error

    stderr = capsys.readouterr().err
    assert "Carl MCP tool failure" in stderr
    assert "simulated concurrent tool failure" in stderr
