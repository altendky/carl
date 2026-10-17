"""Bounded transport results for durable work cancellation."""

from pydantic import Field

from carl.core.models import StrictModel


class WorkCancellationSummary(StrictModel):
    reason: str
    caller: str
    requested_at_utc_ns: int = Field(ge=0)


class WorkCancellationResult(StrictModel):
    """Large ID lists continue through the standard get_tool_response_page tool."""

    cancelled_pending: int = Field(ge=0)
    cancelling_leased: int = Field(ge=0)
    already_terminal: int = Field(ge=0)
    cancelled_pending_identifiers: tuple[str, ...]
    cancelling_leased_identifiers: tuple[str, ...]
    already_terminal_identifiers: tuple[str, ...]
