"""Consume response continuations through the same public protocol as clients."""

import hashlib
import json

from mcp import Client
from mcp.types import CallToolResult, TextContent

from carl.core.tool_response_pages import ToolResponseAcknowledgement, ToolResponsePage


def visible_tool_value(result: CallToolResult) -> object:
    """Match Claude Code: structured content takes precedence without text fallback."""
    if result.structured_content is not None:
        return result.structured_content
    if not result.content or not isinstance(result.content[0], TextContent):
        return None
    try:
        return json.loads(result.content[0].text)
    except ValueError:
        return None


def tool_response_page(result: CallToolResult) -> ToolResponsePage | None:
    value = visible_tool_value(result)
    if not isinstance(value, dict) or value.get("kind") != "carl_tool_response_page":
        return None
    return ToolResponsePage.model_validate(value)


async def complete_tool_result(client: Client, result: CallToolResult) -> CallToolResult:
    page = tool_response_page(result)
    if page is None:
        return result
    fragments: list[str] = []
    while True:
        assert len(result.model_dump_json(by_alias=True).encode()) < 25_000
        page = tool_response_page(result)
        assert page is not None
        assert page.offset == sum(map(len, fragments))
        fragments.append(page.json_fragment)
        if not page.has_more:
            break
        result = await client.call_tool("get_tool_response_page", {"cursor": page.next_cursor})
        assert not result.is_error
    original_json = "".join(fragments)
    assert len(original_json.encode()) == page.total_json_bytes
    assert hashlib.sha256(original_json.encode()).hexdigest() == page.sha256
    acknowledgement = await client.call_tool(
        "get_tool_response_page", {"cursor": page.cursor, "acknowledge": True}
    )
    assert not acknowledgement.is_error
    assert acknowledgement.content and isinstance(acknowledgement.content[0], TextContent)
    receipt = ToolResponseAcknowledgement.model_validate(visible_tool_value(acknowledgement))
    assert receipt.sha256 == page.sha256 and receipt.acknowledged
    assert "json_fragment" not in acknowledgement.content[0].text
    if page.payload_format == "structured_json":
        return CallToolResult(
            content=[TextContent(type="text", text=original_json)],
            structured_content=json.loads(original_json),
            is_error=page.original_is_error,
        )
    return CallToolResult.model_validate_json(original_json)
