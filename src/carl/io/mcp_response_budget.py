"""Bound MCP tool responses after SDK conversion without discarding mutation results.

Cache limits are admission thresholds, not hard memory bounds. An in-flight
result can exceed them, because it is already materialized and must remain
recoverable. Unacknowledged results never expire or get evicted. Once a reader
acknowledges the final page, that result becomes eligible for LRU eviction.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from collections import OrderedDict
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from typing import Any, Literal

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.context import Context
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import (
    SERVER_INFO_META_KEY,
    CallToolResult,
    Implementation,
    InputRequiredResult,
    TextContent,
    Tool,
)

from carl.core.models import JsonValue
from carl.core.tool_response_pages import (
    ToolResponseAcknowledgement,
    ToolResponsePage,
    ToolResponseReadResult,
)

TARGET_RESPONSE_BYTES = 24_000
HARD_RESPONSE_BYTES = 40_000
RESPONSE_PAGE_TOOL = "get_tool_response_page"
_RESPONSE_META = "carl/response"


def response_json(result: CallToolResult | InputRequiredResult) -> str:
    """Measure the entire MCP result, including text, structured data and metadata."""
    return result.model_dump_json(by_alias=True, exclude_none=True)


def response_bytes(result: CallToolResult | InputRequiredResult) -> int:
    return len(response_json(result).encode("utf-8"))


def _annotated[T: CallToolResult | InputRequiredResult](result: T, *, has_more: bool) -> T:
    meta = dict(result.meta or {})
    size = 0
    for _ in range(8):
        meta[_RESPONSE_META] = {"response_bytes": size, "has_more": has_more}
        annotated = result.model_copy(update={"meta": dict(meta)})
        measured = response_bytes(annotated)
        if measured == size:
            return annotated
        size = measured
    raise AssertionError("Response byte count did not stabilize")


def page_result(
    page: ToolResponsePage | ToolResponseAcknowledgement,
    *,
    result_meta: dict[str, JsonValue] | None = None,
) -> CallToolResult:
    """Keep the complete page once in structuredContent, with a small text notice."""
    notice = (
        "Response page is in structuredContent. Follow its recovery_instructions."
        if isinstance(page, ToolResponsePage)
        else "Response acknowledged; receipt is in structuredContent."
    )
    return _annotated(
        CallToolResult(
            content=[TextContent(type="text", text=notice)],
            structured_content=page.model_dump(mode="json"),
            _meta=result_meta,
        ),
        has_more=page.has_more,
    )


def _duplicate_structured_text(result: CallToolResult) -> bool:
    """Recognize SDK JSON mirrors, preserving genuine text and binary content."""
    if result.structured_content is None:
        return False
    if not result.content:
        return True
    if not all(isinstance(content, TextContent) for content in result.content):
        return False
    if any(
        set(content.model_dump(mode="json", by_alias=True, exclude_none=True)) - {"type", "text"}
        for content in result.content
    ):
        return False
    try:
        values = [
            json.loads(content.text)
            for content in result.content
            if isinstance(content, TextContent)
        ]
        structured = result.model_dump(mode="json", by_alias=True)["structuredContent"]

        def equal(left: JsonValue, right: JsonValue) -> bool:
            # Python equality equates True and 1; their JSON representations
            # are different and must not be treated as mirrored content.
            return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(
                right, sort_keys=True, allow_nan=False
            )

        return (len(values) == 1 and equal(values[0], structured)) or (
            isinstance(structured, dict)
            and set(structured) == {"result"}
            and equal(structured["result"], values)
        )
    except (ValueError, RecursionError, TypeError, OverflowError):
        return False


def _compact_json(value: JsonValue) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _compact_normal(
    result: CallToolResult | InputRequiredResult,
) -> CallToolResult | InputRequiredResult:
    if not isinstance(result, CallToolResult) or not _duplicate_structured_text(result):
        return result
    return result.model_copy(
        update={"content": [TextContent(type="text", text="Tool result is in structuredContent.")]}
    )


def canonical_payload(
    result: CallToolResult | InputRequiredResult,
) -> tuple[str, Literal["structured_json", "mcp_result_json"]]:
    """Store one structured representation or preserve genuine MCP content whole."""
    if isinstance(result, CallToolResult) and _duplicate_structured_text(result):
        metadata = result.meta or {}
        if not (set(metadata) - {SERVER_INFO_META_KEY, _RESPONSE_META}):
            structured = result.model_dump(mode="json", by_alias=True)["structuredContent"]
            return _compact_json(structured), "structured_json"
    return response_json(result), "mcp_result_json"


def _critical_metadata(result: CallToolResult | InputRequiredResult) -> dict[str, JsonValue]:
    if not isinstance(result, CallToolResult):
        return {}
    value = result.structured_content
    if not isinstance(value, dict):
        return {}
    keys = (
        "claim_token",
        "owner_identifier",
        "workspace_record_identifier",
        "batch_record_identifier",
        "request_identifier",
        "work_identifier",
        "record_identifier",
        "acquired_at_utc_ns",
        "lease_expires_at_utc_ns",
    )

    def selected(source: dict[str, JsonValue]) -> dict[str, JsonValue]:
        return {
            key: source[key]
            for key in keys
            if isinstance(source.get(key), (str, int, bool))
            and len(json.dumps(source[key], ensure_ascii=False).encode()) <= 512
        }

    output = selected(value)
    lease = value.get("lease")
    if isinstance(lease, dict):
        output["lease"] = selected(lease)
    # Keep this manifest bounded independently of arbitrary original strings.
    return output if len(json.dumps(output, ensure_ascii=False).encode()) <= 6_000 else {}


@dataclass(slots=True)
class _CachedResponse:
    tool_name: str
    text: str
    size_bytes: int
    sha256: str
    is_error: bool
    critical_metadata: dict[str, JsonValue]
    payload_format: Literal["structured_json", "mcp_result_json"]
    complete: bool = False
    final_page_offsets: set[int] | None = None


class ResponseCapacityError(ValueError):
    """Admission failed before the application tool ran."""


class ToolResponseStore:
    """Immutable JSON snapshots with pinned unread entries and signed cursors."""

    def __init__(
        self,
        *,
        target_bytes: int = TARGET_RESPONSE_BYTES,
        hard_bytes: int = HARD_RESPONSE_BYTES,
        soft_max_bytes: int = 64_000_000,
        soft_max_entries: int = 32,
    ) -> None:
        if not 12_000 <= target_bytes <= hard_bytes < 50_000:
            raise ValueError("Response budget must satisfy 12000 <= target <= hard < 50000")
        if soft_max_bytes < 1 or soft_max_entries < 1:
            raise ValueError("Response cache admission thresholds must be positive")
        self.target_bytes: int = target_bytes
        self.hard_bytes: int = hard_bytes
        self.soft_max_bytes: int = soft_max_bytes
        self.soft_max_entries: int = soft_max_entries
        self._entries: OrderedDict[str, _CachedResponse] = OrderedDict()
        self._reservations: int = 0
        self._cursor_key: bytes = secrets.token_bytes(32)
        self.result_meta: dict[str, JsonValue] = {}

    def _cursor(self, identifier: str, offset: int) -> str:
        position = f"{identifier}.{offset}"
        signature = hmac.new(self._cursor_key, position.encode(), hashlib.sha256).hexdigest()[:32]
        return f"{position}.{signature}"

    def _decode_cursor(self, cursor: str) -> tuple[str, int]:
        try:
            identifier, raw_offset, _signature = cursor.split(".")
            offset = int(raw_offset)
        except (ValueError, AttributeError):
            raise ValueError("Invalid tool-response cursor") from None
        if offset < 0 or not hmac.compare_digest(self._cursor(identifier, offset), cursor):
            raise ValueError("Invalid tool-response cursor")
        return identifier, offset

    @property
    def retained_bytes(self) -> int:
        return sum(entry.size_bytes for entry in self._entries.values())

    @property
    def pending_cursors(self) -> tuple[str, ...]:
        return tuple(
            self._cursor(identifier, 0)
            for identifier, entry in self._entries.items()
            if not entry.complete
        )

    @contextmanager
    def admit(self) -> Iterator[None]:
        """Reserve an entry before any await or application side effect."""
        while (
            len(self._entries) + self._reservations >= self.soft_max_entries
            or self.retained_bytes >= self.soft_max_bytes
        ):
            evictable = next((key for key, entry in self._entries.items() if entry.complete), None)
            if evictable is None:
                raise ResponseCapacityError(
                    "Tool was not run: retrieve and acknowledge the pending tool-response pages before requesting more application work."
                )
            del self._entries[evictable]
        self._reservations += 1
        try:
            yield
        finally:
            self._reservations -= 1

    def bound(
        self, tool_name: str, result: CallToolResult | InputRequiredResult
    ) -> CallToolResult | InputRequiredResult:
        if self.result_meta:
            result = result.model_copy(update={"meta": {**self.result_meta, **(result.meta or {})}})
        # Retain the SDK mirror for canonical detection before replacing its
        # redundant text with a notice in the wire result.
        original = result
        result = _compact_normal(result)
        structured = result.structured_content if isinstance(result, CallToolResult) else None
        has_more = (
            structured["has_more"]
            if isinstance(structured, dict) and isinstance(structured.get("has_more"), bool)
            else isinstance(structured, dict)
            and (
                structured.get("next_cursor") is not None
                or structured.get("next_offset") is not None
            )
        )
        normal = _annotated(result, has_more=has_more)
        if response_bytes(normal) <= self.target_bytes:
            return normal
        text, payload_format = canonical_payload(original)
        identifier = secrets.token_urlsafe(24)
        self._entries[identifier] = _CachedResponse(
            tool_name=tool_name,
            text=text,
            size_bytes=len(text.encode()),
            sha256=hashlib.sha256(text.encode()).hexdigest(),
            is_error=result.is_error if isinstance(result, CallToolResult) else False,
            critical_metadata=_critical_metadata(result),
            payload_format=payload_format,
        )
        return page_result(self.get_page(self._cursor(identifier, 0)), result_meta=self.result_meta)

    def get_page(
        self, cursor: str, *, acknowledge: bool = False
    ) -> ToolResponsePage | ToolResponseAcknowledgement:
        identifier, offset = self._decode_cursor(cursor)
        entry = self._entries.get(identifier)
        if entry is None:
            raise ValueError("Tool-response cursor is no longer available in this server process")
        if offset > len(entry.text):
            raise ValueError("Invalid tool-response cursor offset")
        if acknowledge:
            if offset not in (entry.final_page_offsets or set()):
                raise ValueError(
                    "Only a previously retrieved final response page may be acknowledged"
                )
            entry.complete = True
            self._entries.move_to_end(identifier)
            return ToolResponseAcknowledgement(
                tool_name=entry.tool_name, cursor=cursor, sha256=entry.sha256
            )

        def candidate(count: int) -> ToolResponsePage:
            end = offset + count
            has_more = end < len(entry.text)
            return ToolResponsePage(
                tool_name=entry.tool_name,
                cursor=cursor,
                payload_format=entry.payload_format,
                total_json_bytes=entry.size_bytes,
                sha256=entry.sha256,
                offset=offset,
                json_fragment=entry.text[offset:end],
                next_cursor=self._cursor(identifier, end) if has_more else None,
                has_more=has_more,
                original_is_error=entry.is_error,
                critical_metadata=entry.critical_metadata if offset == 0 else {},
            )

        lower, upper = 0, min(len(entry.text) - offset, self.target_bytes)
        while lower < upper:
            midpoint = (lower + upper + 1) // 2
            if (
                response_bytes(page_result(candidate(midpoint), result_meta=self.result_meta))
                <= self.target_bytes
            ):
                lower = midpoint
            else:
                upper = midpoint - 1
        page = candidate(lower)
        if lower == 0 and page.has_more:
            raise ValueError("Response-page manifest exceeds the configured wire budget")
        if response_bytes(page_result(page, result_meta=self.result_meta)) >= self.hard_bytes:
            raise AssertionError("Response-page exceeded its hard wire budget")
        if not page.has_more:
            if entry.final_page_offsets is None:
                entry.final_page_offsets = set()
            entry.final_page_offsets.add(offset)
        self._entries.move_to_end(identifier)
        return page


def paging_output_schema(original: dict[str, Any]) -> dict[str, Any]:
    """Advertise continuation without changing the original runtime validator."""
    original = dict(original)
    definitions = original.pop("$defs", {})
    page = ToolResponsePage.model_json_schema()
    page_definitions = page.pop("$defs", {})
    renamed: dict[str, str] = {}
    for key in page_definitions:
        name = f"CarlResponsePage_{key}"
        while name in definitions or name in renamed.values():
            name += "_"
        renamed[key] = name

    def rewrite(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: (
                    f"#/$defs/{renamed[item.removeprefix('#/$defs/')]}"
                    if key == "$ref"
                    and isinstance(item, str)
                    and item.startswith("#/$defs/")
                    and item.removeprefix("#/$defs/") in renamed
                    else rewrite(item)
                )
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [rewrite(item) for item in value]
        return value

    page = rewrite(page)
    page_definitions = {renamed[key]: rewrite(value) for key, value in page_definitions.items()}
    schema: dict[str, Any] = {"type": "object", "anyOf": [original, page]}
    if definitions or page_definitions:
        schema["$defs"] = {**definitions, **page_definitions}
    return schema


class BudgetedMCPServer(MCPServer[Any]):
    """Apply the same bound to protocol calls and direct SDK .call_tool callers."""

    def __init__(
        self, *args: Any, response_store: ToolResponseStore | None = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.response_store: ToolResponseStore = response_store or ToolResponseStore()
        self.response_store.result_meta = {
            SERVER_INFO_META_KEY: Implementation(
                name=self.name,
                version=self.version,
                title=self.title,
                description=self.description,
                website_url=self.website_url,
                icons=self.icons,
            ).model_dump(mode="json", by_alias=True, exclude_none=True)
        }

    async def list_tools(self) -> list[Tool]:
        tools = await super().list_tools()
        return [
            tool.model_copy(update={"output_schema": paging_output_schema(tool.output_schema)})
            if tool.output_schema is not None and tool.name != RESPONSE_PAGE_TOOL
            else tool
            for tool in tools
        ]

    async def call_tool(
        self, name: str, arguments: dict[str, Any], context: Context[Any, Any] | None = None
    ) -> CallToolResult | InputRequiredResult:
        if name == RESPONSE_PAGE_TOOL:
            try:
                result = await super().call_tool(name, arguments, context)
            except ToolError as error:
                result = CallToolResult(
                    content=[TextContent(type="text", text=str(error))], is_error=True
                )
            if isinstance(result, CallToolResult) and not result.is_error:
                page = ToolResponseReadResult.model_validate(result.structured_content).root
                if not isinstance(page, (ToolResponsePage, ToolResponseAcknowledgement)):
                    raise ToolError(
                        "Continuation endpoint did not return a fragment or acknowledgement"
                    )
                return page_result(
                    page,
                    result_meta=self.response_store.result_meta,
                )
            return self.response_store.bound(name, result)
        try:
            with (
                nullcontext()
                if name in {"renew_review_claim", "release_review_claim"}
                else self.response_store.admit()
            ):
                try:
                    result = await super().call_tool(name, arguments, context)
                except ToolError as error:
                    result = CallToolResult(
                        content=[TextContent(type="text", text=str(error))], is_error=True
                    )
                return self.response_store.bound(name, result)
        except ResponseCapacityError as error:
            return _annotated(
                CallToolResult(
                    content=[TextContent(type="text", text=str(error))],
                    is_error=True,
                    structured_content={
                        "pending_response_cursors": self.response_store.pending_cursors[:3]
                    },
                    _meta=self.response_store.result_meta,
                ),
                has_more=False,
            )
