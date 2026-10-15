"""Lossless continuation for an MCP response that exceeds the wire budget."""

from typing import Literal

from pydantic import ConfigDict, Field, RootModel

from carl.core.models import JsonValue, StrictModel


class ToolResponsePageManifest(StrictModel):
    """Common metadata for a complete structured response page."""

    kind: Literal["carl_tool_response_page"] = "carl_tool_response_page"
    tool_name: str
    cursor: str
    payload_format: Literal["structured_json", "mcp_result_json"]
    encoding: Literal["utf-8"] = "utf-8"
    fragment_location: Literal["structuredContent.json_fragment"] = (
        "structuredContent.json_fragment"
    )
    total_json_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    offset: int = Field(ge=0, description="Unicode character offset in the original response JSON")
    next_cursor: str | None
    has_more: bool
    original_is_error: bool
    critical_metadata: dict[str, JsonValue] = Field(default_factory=dict)
    recovery_instructions: str = (
        "Read this full page from structuredContent, including json_fragment; content text is "
        "only a notice. Call get_tool_response_page with next_cursor until has_more is false. "
        "Concatenate json_fragment strings in offset order and parse the compact JSON. "
        "For structured_json it is the original structured payload; original_is_error retains "
        "the tool error flag. For mcp_result_json it is the complete original MCP result. "
        "Do not repeat the original tool: it already ran. After retaining the final fragment, "
        "call get_tool_response_page with that page's cursor and acknowledge=true for a small "
        "receipt. Retrieve unread pages before restarting this server process."
    )


class ToolResponsePage(ToolResponsePageManifest):
    json_fragment: str


class ToolResponseAcknowledgement(StrictModel):
    kind: Literal["carl_tool_response_acknowledgement"] = "carl_tool_response_acknowledgement"
    tool_name: str
    cursor: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    acknowledged: Literal[True] = True
    has_more: Literal[False] = False


class ToolResponseReadResult(RootModel[ToolResponsePage | ToolResponseAcknowledgement]):
    model_config = ConfigDict(json_schema_extra={"type": "object"})
