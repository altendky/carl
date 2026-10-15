"""Consume response continuations through the same public protocol as clients."""

import hashlib

from mcp import Client
from mcp.types import CallToolResult

from carl.core.tool_response_pages import ToolResponsePage


async def complete_tool_result(client: Client, result: CallToolResult) -> CallToolResult:
    if (result.structured_content or {}).get("kind") != "carl_tool_response_page":
        return result
    fragments: list[str] = []
    while True:
        assert len(result.model_dump_json(by_alias=True).encode()) < 25_000
        page = ToolResponsePage.model_validate(result.structured_content)
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
    return CallToolResult.model_validate_json(original_json)
