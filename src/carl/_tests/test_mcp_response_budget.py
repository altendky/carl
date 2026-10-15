"""Exact MCP result budgets and lossless continuation across mutation side effects."""

import hashlib
import json
from typing import Any

import anyio
import pytest
from mcp import Client
from mcp.types import Annotations, CallToolResult, ContentBlock, ImageContent, TextContent

from carl._tests.mcp_response_helpers import tool_response_page
from carl.core.models import JsonValue, StrictModel
from carl.core.tool_response_pages import (
    ToolResponseAcknowledgement,
    ToolResponsePage,
    ToolResponseReadResult,
)
from carl.io.mcp_response_budget import (
    BudgetedMCPServer,
    ResponseCapacityError,
    ToolResponseStore,
    canonical_payload,
    page_result,
    paging_output_schema,
    response_bytes,
    response_json,
)


class _Payload(StrictModel):
    value: JsonValue
    lease: dict[str, JsonValue] | None = None


def _page(result: CallToolResult) -> ToolResponsePage:
    page = tool_response_page(result)
    assert page is not None
    assert isinstance(result.content[0], TextContent)
    assert page.model_dump(mode="json", exclude={"json_fragment"}) == result.structured_content
    assert "json_fragment" not in result.structured_content
    assert result.meta is not None
    assert result.meta["carl/response"]["response_bytes"] == response_bytes(result)
    assert result.meta["carl/response"]["has_more"] == page.has_more
    return page


def _stored_page(store: ToolResponseStore, cursor: str) -> ToolResponsePage:
    page = store.get_page(cursor)
    assert isinstance(page, ToolResponsePage)
    return page


@pytest.mark.parametrize("value", ['🛰️é\n"\\' * 20_000, {"nested": [[{"report": "z" * 150_000}]]}])
def test_huge_unicode_and_nested_strings_reassemble_exactly(value: JsonValue) -> None:
    store = ToolResponseStore()
    original = CallToolResult(
        content=[
            TextContent(
                type="text", text=json.dumps({"value": value}, ensure_ascii=False, indent=2)
            )
        ],
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
        result = page_result(_stored_page(store, page.next_cursor))
    rebuilt = "".join(chunks)
    assert page.payload_format == "structured_json"
    assert rebuilt == json.dumps({"value": value}, ensure_ascii=False, separators=(",", ":"))
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
        page = _stored_page(store, page.next_cursor)
        assert response_bytes(page_result(page)) <= store.target_bytes
        chunks.append(page.json_fragment)
    assert json.loads("".join(chunks))["content"][0]["data"] == "A" * 100_000


def test_pages_transmit_one_compact_payload_with_bounded_manifest_overhead() -> None:
    payload = {"value": "single-copy-" + "x" * 80_000}
    compact = json.dumps(payload, separators=(",", ":"))
    original = CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload, indent=2))],
        structured_content=payload,
    )
    store = ToolResponseStore()
    result = store.bound("report", original)
    assert isinstance(result, CallToolResult)
    wire_bytes, page_count, fragments = 0, 0, []
    while True:
        page = _page(result)
        wire_bytes += response_bytes(result)
        page_count += 1
        fragments.append(page.json_fragment)
        assert page.payload_format == "structured_json"
        assert page.total_json_bytes == len(compact.encode())
        assert isinstance(result.content[0], TextContent)
        assert "\n" not in result.content[0].text
        if page.next_cursor is None:
            break
        result = page_result(_stored_page(store, page.next_cursor))
    assert "".join(fragments) == compact
    assert sum(len(fragment.encode()) for fragment in fragments) == len(compact.encode())
    assert wire_bytes <= len(compact.encode()) + 6_000 * page_count
    assert wire_bytes < response_bytes(original)


@pytest.mark.parametrize("binary", [False, True])
def test_genuine_additional_content_preserves_complete_mcp_semantics(binary: bool) -> None:
    contents: list[ContentBlock] = [
        TextContent(type="text", text="Actual explanation: " + "text" * 15_000)
    ]
    original = CallToolResult(
        content=[*contents, ImageContent(type="image", data="A" * 20_000, mime_type="image/png")]
        if binary
        else contents,
        structured_content={"summary": "separate"},
        is_error=True,
    )
    store = ToolResponseStore()
    result = store.bound("mixed", original)
    assert isinstance(result, CallToolResult)
    chunks: list[str] = []
    while True:
        page = _page(result)
        assert page.payload_format == "mcp_result_json"
        assert page.original_is_error
        chunks.append(page.json_fragment)
        if page.next_cursor is None:
            break
        result = page_result(_stored_page(store, page.next_cursor))
    assert CallToolResult.model_validate_json("".join(chunks)) == original


@pytest.mark.parametrize("text", ['{"value":1}', '{"value":1.0}'])
def test_json_text_with_different_boolean_value_is_not_discarded(text: str) -> None:
    original = CallToolResult(
        content=[TextContent(type="text", text=text)], structured_content={"value": True}
    )
    encoded, payload_format = canonical_payload(original)
    assert payload_format == "mcp_result_json"
    assert CallToolResult.model_validate_json(encoded) == original


@pytest.mark.parametrize("annotation", ["annotations", "meta"])
def test_annotated_json_text_is_not_discarded(annotation: str) -> None:
    original = CallToolResult(
        content=[
            TextContent(
                type="text",
                text='{"value":"same"}',
                annotations=Annotations(audience=["assistant"])
                if annotation == "annotations"
                else None,
                _meta={"render": "special"} if annotation == "meta" else None,
            )
        ],
        structured_content={"value": "same"},
    )
    encoded, payload_format = canonical_payload(original)
    assert payload_format == "mcp_result_json"
    assert CallToolResult.model_validate_json(encoded) == original


def test_sdk_wrapped_sequence_uses_one_compact_structured_representation() -> None:
    values = [{"value": "first"}, {"value": "second"}]
    original = CallToolResult(
        content=[TextContent(type="text", text=json.dumps(value, indent=2)) for value in values],
        structured_content={"result": values},
    )
    encoded, payload_format = canonical_payload(original)
    assert payload_format == "structured_json"
    assert encoded == json.dumps({"result": values}, separators=(",", ":"))


@pytest.mark.anyio
async def test_public_mcp_text_only_client_recovers_payload_and_receives_tiny_ack() -> None:
    store = ToolResponseStore()
    server = BudgetedMCPServer(name="text-only-test", response_store=store)
    original = {"report": "large" * 30_000, "count": 30_000}

    async def report() -> _Payload:
        return _Payload(value=original)

    async def get_tool_response_page(
        cursor: str, acknowledge: bool = False
    ) -> ToolResponseReadResult:
        return ToolResponseReadResult(store.get_page(cursor, acknowledge=acknowledge))

    server.add_tool(report)
    server.add_tool(get_tool_response_page)
    fragments: list[str] = []
    async with Client(server) as client:
        result = await client.call_tool("report", {})
        while True:
            assert not result.is_error and response_bytes(result) <= store.target_bytes
            # Simulate a client that displays only TextContent, not structuredContent.
            text_only = result.model_copy(update={"structured_content": None})
            page = tool_response_page(text_only)
            assert page is not None
            assert page.payload_format == "structured_json"
            fragments.append(page.json_fragment)
            if not page.has_more:
                break
            result = await client.call_tool("get_tool_response_page", {"cursor": page.next_cursor})
        assert json.loads("".join(fragments)) == {"value": original, "lease": None}
        acknowledgement = await client.call_tool(
            "get_tool_response_page", {"cursor": page.cursor, "acknowledge": True}
        )
        assert not acknowledgement.is_error
        assert response_bytes(acknowledgement) < 1_000
        assert isinstance(acknowledgement.content[0], TextContent)
        receipt = ToolResponseAcknowledgement.model_validate_json(acknowledgement.content[0].text)
        assert receipt.acknowledged and receipt.sha256 == page.sha256
        assert "json_fragment" not in response_json(acknowledgement)


def test_unread_results_are_pinned_and_finished_results_can_be_evicted() -> None:
    store = ToolResponseStore(soft_max_bytes=1, soft_max_entries=1)
    result = store.bound(
        "mutation", CallToolResult(content=[TextContent(type="text", text="x" * 50_000)])
    )
    assert isinstance(result, CallToolResult)
    page = _page(result)
    with pytest.raises(ValueError, match="final response page"):
        store.get_page(page.cursor, acknowledge=True)
    first_cursor = page.cursor
    assert store.retained_bytes > store.soft_max_bytes
    with pytest.raises(ResponseCapacityError), store.admit():
        pytest.fail("An unread response must prevent admission")
    assert _stored_page(store, first_cursor).json_fragment == page.json_fragment
    while page.next_cursor:
        page = _stored_page(store, page.next_cursor)
    # Delivery of the last page is not an acknowledgement: it may be lost.
    with pytest.raises(ResponseCapacityError), store.admit():
        pytest.fail("Unacknowledged final page must remain replayable")
    assert _stored_page(store, page.cursor).json_fragment == page.json_fragment
    acknowledgement = store.get_page(page.cursor, acknowledge=True)
    assert isinstance(acknowledgement, ToolResponseAcknowledgement) and acknowledgement.acknowledged
    assert response_bytes(page_result(acknowledgement)) < 1_000
    assert "json_fragment" not in page_result(acknowledgement).model_dump_json()
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

    async def get_tool_response_page(
        cursor: str, acknowledge: bool = False
    ) -> ToolResponseReadResult:
        return ToolResponseReadResult(store.get_page(cursor, acknowledge=acknowledge))

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
    assert page.payload_format == "structured_json"
    assert recovered["lease"]["claim_token"] == "claim-secret"
    assert recovered["value"] == "huge" * 40_000
    assert invocations == 1
    ack = await server.call_tool(
        "get_tool_response_page", {"cursor": page.cursor, "acknowledge": True}
    )
    assert isinstance(ack, CallToolResult)
    assert ToolResponseAcknowledgement.model_validate(ack.structured_content).acknowledged
    assert response_bytes(ack) < 1_000
    assert "json_fragment" not in response_json(ack)
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
