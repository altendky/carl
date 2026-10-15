"""Lossless continuation for an MCP response that exceeds the wire budget."""

from typing import Literal

from pydantic import Field

from carl.core.models import JsonValue, StrictModel


class ToolResponsePage(StrictModel):
    kind: Literal["carl_tool_response_page"] = "carl_tool_response_page"
    tool_name: str
    cursor: str
    total_json_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    offset: int = Field(ge=0, description="Unicode character offset in the original response JSON")
    json_fragment: str
    next_cursor: str | None
    has_more: bool
    original_is_error: bool
    acknowledged: bool = False
    critical_metadata: dict[str, JsonValue] = Field(default_factory=dict)
    recovery_instructions: str = (
        "Call get_tool_response_page with next_cursor until has_more is false. "
        "Concatenate json_fragment strings in offset order, then parse the complete JSON "
        "as the original MCP CallToolResult. Do not repeat the original tool: it already ran. "
        "After retaining the final fragment, call get_tool_response_page with that page's cursor "
        "and acknowledge=true to allow cache cleanup. "
        "Unread pages remain available for this server process; retrieve them before restarting it."
    )
