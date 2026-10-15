"""Exact MCP result budgets and lossless continuation across mutation side effects."""

import hashlib
import json
from typing import Any

import anyio
import pytest
from mcp.types import CallToolResult, ImageContent, TextContent

from carl.core.models import JsonValue, StrictModel
from carl.core.tool_response_pages import ToolResponsePage
from carl.io.mcp_response_budget import (
    BudgetedMCPServer,
    ResponseCapacityError,
    ToolResponseStore,
    page_result,
    paging_output_schema,
    response_bytes,
    response_json,
)


class _Payload(StrictModel):
    value: JsonValue
    lease: dict[str, JsonValue] | None = None


def _page(result: CallToolResult) -> ToolResponsePage:
    page = ToolResponsePage.model_validate(result.structured_content)
    assert isinstance(result.content[0], TextContent)
    assert json.loads(result.content[0].text) == result.structured_content
    assert result.meta is not None
    assert result.meta["carl/response"]["response_bytes"] == response_bytes(result)
    assert result.meta["carl/response"]["has_more"] == page.has_more
    return page


@pytest.mark.parametrize("value", ['🛰️é\n"\\' * 20_000, {"nested": [[{"report": "z" * 150_000}]]}])
def test_huge_unicode_and_nested_strings_reassemble_exactly(value: JsonValue) -> None:
    store = ToolResponseStore()
    original = CallToolResult(
        content=[TextContent(type="text", text=json.dumps(value, ensure_ascii=False))],
        structured_content={"value": value},
    )
    result = store.bound("get_analysis", original)
    assert isinstance(result, CallToolResult)
    chunks: list[str] = []
    while True:
        assert response_bytes(result) <= store.target_bytes < store.hard_bytes < 50_000
        page = _page(result)
        assert page.offset == sum(map(len, chunks))
        chunks.append(page.json_fragment)
        if page.next_cursor is None:
            break
        result = page_result(store.get_page(page.next_cursor))
    rebuilt = "".join(chunks)
    assert rebuilt == response_json(original)
    assert len(rebuilt.encode()) == page.total_json_bytes
    assert hashlib.sha256(rebuilt.encode()).hexdigest() == page.sha256


def test_binary_image_content_is_budgeted_and_recoverable() -> None:
    store = ToolResponseStore()
    original = CallToolResult(
        content=[ImageContent(type="image", data="A" * 100_000, mime_type="image/png")]
    )
    result = store.bound("get_image", original)
    assert isinstance(result, CallToolResult)
    page = _page(result)
    chunks = [page.json_fragment]
    while page.next_cursor:
        page = store.get_page(page.next_cursor)
        assert response_bytes(page_result(page)) <= store.target_bytes
        chunks.append(page.json_fragment)
    assert json.loads("".join(chunks))["content"][0]["data"] == "A" * 100_000


def test_unread_results_are_pinned_and_finished_results_can_be_evicted() -> None:
    store = ToolResponseStore(soft_max_bytes=1, soft_max_entries=1)
    result = store.bound(
        "mutation", CallToolResult(content=[TextContent(type="text", text="x" * 50_000)])
    )
    assert isinstance(result, CallToolResult)
    page = _page(result)
    with pytest.raises(ValueError, match="Only the final"):
        store.get_page(page.cursor, acknowledge=True)
    first_cursor = page.cursor
    assert store.retained_bytes > store.soft_max_bytes
    with pytest.raises(ResponseCapacityError), store.admit():
        pytest.fail("An unread response must prevent admission")
    assert store.get_page(first_cursor).json_fragment == page.json_fragment
    while page.next_cursor:
        page = store.get_page(page.next_cursor)
    # Delivery of the last page is not an acknowledgement: it may be lost.
    with pytest.raises(ResponseCapacityError), store.admit():
        pytest.fail("Unacknowledged final page must remain replayable")
    assert store.get_page(page.cursor).json_fragment == page.json_fragment
    assert store.get_page(page.cursor, acknowledge=True).acknowledged
    with store.admit():
        assert store.retained_bytes == 0
    with pytest.raises(ValueError, match="no longer available"):
        store.get_page(first_cursor)


def test_cursors_cannot_be_modified_to_skip_unread_pages() -> None:
    store = ToolResponseStore()
    result = store.bound(
        "read", CallToolResult(content=[TextContent(type="text", text="x" * 50_000)])
    )
    assert isinstance(result, CallToolResult)
    page = _page(result)
    identifier, _, signature = page.cursor.split(".")
    with pytest.raises(ValueError, match="Invalid"):
        store.get_page(f"{identifier}.40000.{signature}")


def test_schema_definitions_are_scoped_when_both_models_define_json_value() -> None:
    original = _Payload.model_json_schema()
    original_definitions = dict(original.get("$defs", {}))
    schema = paging_output_schema(original)
    assert schema["type"] == "object"
    assert len(schema["anyOf"]) == 2
    assert all(schema["$defs"][key] == value for key, value in original_definitions.items())
    page_keys = set(schema.get("$defs", {})) - set(original_definitions)
    assert all(key.startswith("CarlResponsePage_") for key in page_keys)

    def check_refs(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "$ref" and child.startswith("#/$defs/"):
                    assert child.removeprefix("#/$defs/") in schema["$defs"]
                check_refs(child)
        elif isinstance(value, list):
            for child in value:
                check_refs(child)

    check_refs(schema)
    assert original.get("$defs", {}) == original_definitions


@pytest.mark.anyio
async def test_server_preserves_normal_shape_and_reports_typed_continuation() -> None:
    class Page(StrictModel):
        items: tuple[str, ...]
        has_more: bool

    async def normal() -> Page:
        return Page(items=("small",), has_more=True)

    server = BudgetedMCPServer(name="test", version="one")
    server.add_tool(normal)
    result = await server.call_tool("normal", {})
    assert isinstance(result, CallToolResult)
    assert result.structured_content == {"items": ["small"], "has_more": True}
    assert result.meta is not None
    assert result.meta["carl/response"] == {
        "response_bytes": response_bytes(result),
        "has_more": True,
    }
    assert result.meta["io.modelcontextprotocol/serverInfo"] == {"name": "test", "version": "one"}
    assert response_bytes(result) < 24_000
    advertised = await server.list_tools()
    assert advertised[0].output_schema is not None
    assert len(advertised[0].output_schema["anyOf"]) == 2


@pytest.mark.anyio
async def test_mutation_response_and_claim_token_recover_without_repeating_mutation() -> None:
    store = ToolResponseStore(soft_max_bytes=1, soft_max_entries=1)
    server = BudgetedMCPServer(name="test", response_store=store)
    invocations = 0

    async def mutate() -> _Payload:
        nonlocal invocations
        invocations += 1
        return _Payload(
            value="huge" * 40_000,
            lease={
                "claim_token": "claim-secret",
                "owner_identifier": "owner",
                "listing_identifiers": ["123"],
            },
        )

    async def get_tool_response_page(cursor: str, acknowledge: bool = False) -> ToolResponsePage:
        return store.get_page(cursor, acknowledge=acknowledge)

    server.add_tool(mutate)
    server.add_tool(get_tool_response_page)
    result = await server.call_tool("mutate", {})
    assert isinstance(result, CallToolResult)
    page = _page(result)
    assert page.critical_metadata["lease"]["claim_token"] == "claim-secret"
    assert "listing_identifiers" not in page.critical_metadata["lease"]
    blocked = await server.call_tool("mutate", {})
    assert isinstance(blocked, CallToolResult) and blocked.is_error
    assert invocations == 1
    chunks = [page.json_fragment]
    while page.next_cursor:
        result = await server.call_tool("get_tool_response_page", {"cursor": page.next_cursor})
        assert isinstance(result, CallToolResult) and not result.is_error
        assert response_bytes(result) <= store.target_bytes
        page = _page(result)
        chunks.append(page.json_fragment)
    recovered = json.loads("".join(chunks))
    assert recovered["structuredContent"]["lease"]["claim_token"] == "claim-secret"
    assert recovered["structuredContent"]["value"] == "huge" * 40_000
    assert invocations == 1
    ack = await server.call_tool(
        "get_tool_response_page", {"cursor": page.cursor, "acknowledge": True}
    )
    assert isinstance(ack, CallToolResult)
    assert _page(ack).acknowledged
    subsequent = await server.call_tool("mutate", {})
    assert isinstance(subsequent, CallToolResult) and not subsequent.is_error
    assert invocations == 2


@pytest.mark.anyio
async def test_admission_reserves_capacity_before_a_concurrent_mutation_starts() -> None:
    server = BudgetedMCPServer(name="test", response_store=ToolResponseStore(soft_max_entries=1))
    started, finish = anyio.Event(), anyio.Event()
    invocations = 0

    async def mutate() -> _Payload:
        nonlocal invocations
        invocations += 1
        started.set()
        await finish.wait()
        return _Payload(value="complete")

    server.add_tool(mutate)

    async def first() -> None:
        result = await server.call_tool("mutate", {})
        assert isinstance(result, CallToolResult) and not result.is_error

    async with anyio.create_task_group() as group:
        group.start_soon(first)
        await started.wait()
        second = await server.call_tool("mutate", {})
        assert isinstance(second, CallToolResult) and second.is_error
        assert invocations == 1
        finish.set()


@pytest.mark.anyio
@pytest.mark.parametrize("continuation_key", ["next_cursor", "next_offset"])
@pytest.mark.parametrize(("has_more", "expected"), [(None, True), (False, False), (True, True)])
async def test_native_next_cursor_metadata_with_explicit_boolean_precedence(
    has_more: bool | None, expected: bool, continuation_key: str
) -> None:
    async def native_page() -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "items": [],
            continuation_key: "next" if continuation_key == "next_cursor" else 25,
        }
        if has_more is not None:
            result["has_more"] = has_more
        return result

    server = BudgetedMCPServer(name="test")
    server.add_tool(native_page)
    result = await server.call_tool("native_page", {})
    assert isinstance(result, CallToolResult) and result.meta is not None
    assert result.meta["carl/response"]["has_more"] is expected


@pytest.mark.anyio
async def test_pending_response_does_not_block_lease_maintenance() -> None:
    store = ToolResponseStore(soft_max_entries=1)
    server = BudgetedMCPServer(name="test", response_store=store)
    _ = store.bound("prior", CallToolResult(content=[TextContent(type="text", text="x" * 100_000)]))

    async def maintain() -> _Payload:
        return _Payload(value="maintained")

    for name in ("renew_review_claim", "release_review_claim"):
        server.add_tool(maintain, name=name)
        result = await server.call_tool(name, {})
        assert isinstance(result, CallToolResult) and not result.is_error
        assert response_bytes(result) < 24_000
