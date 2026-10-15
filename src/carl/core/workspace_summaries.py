"""Byte-bounded workspace discovery without search specifications or track payloads."""

from __future__ import annotations

import base64
import binascii
from collections.abc import Sequence

from pydantic import Field, ValidationError, model_validator

from carl.core.models import StrictModel
from carl.core.review_errors import ReviewInputError
from carl.core.review_workspace import ReviewWorkspace
from carl.core.work import WorkState

WORKSPACE_SUMMARY_RESPONSE_BYTE_BUDGET = 9_000


class ListReviewWorkspaceSummariesRequest(StrictModel):
    include_archived: bool = False
    page_size: int = Field(default=20, ge=1, le=100)
    cursor: str | None = Field(default=None, min_length=1, max_length=4_000)
    response_byte_budget: int = Field(
        default=WORKSPACE_SUMMARY_RESPONSE_BYTE_BUDGET,
        ge=1_500,
        le=WORKSPACE_SUMMARY_RESPONSE_BYTE_BUDGET,
    )


class ReviewWorkspaceSummary(StrictModel):
    record_identifier: str = Field(min_length=1)
    name: str = Field(min_length=1)
    search_run_record_identifier: str = Field(min_length=1)
    product_guide_record_identifier: str | None
    created_at_utc: str
    archived: bool
    track_count: int = Field(ge=0)
    enabled_track_count: int = Field(ge=0)
    completed_track_count: int = Field(ge=0)
    failed_refresh_track_count: int = Field(ge=0)
    product_guide_binding_count: int = Field(ge=0)
    default_product_guide_binding_identifier: str | None


class ReviewWorkspaceSummariesPage(StrictModel):
    workspaces: tuple[ReviewWorkspaceSummary, ...] = Field(max_length=100)
    next_cursor: str | None
    has_more: bool
    response_bytes: int = Field(ge=0)
    response_byte_budget: int = Field(ge=1_500, le=WORKSPACE_SUMMARY_RESPONSE_BYTE_BUDGET)

    @model_validator(mode="after")
    def validate_page(self) -> ReviewWorkspaceSummariesPage:
        if self.has_more != (self.next_cursor is not None):
            raise ValueError("Workspace summary continuation does not match has_more")
        if self.response_bytes > self.response_byte_budget:
            raise ValueError("Workspace summary response exceeds its byte budget")
        return self


class WorkspaceSummariesCursor(StrictModel):
    include_archived: bool
    after_workspace_identifier: str = Field(min_length=1)


def decode_workspace_summaries_cursor(
    request: ListReviewWorkspaceSummariesRequest,
) -> WorkspaceSummariesCursor | None:
    """Bind continuation to archive scope while permitting smaller pages or budgets."""

    if request.cursor is None:
        return None
    try:
        encoded = request.cursor.encode("ascii")
        raw = base64.b64decode(encoded + b"=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
        cursor = WorkspaceSummariesCursor.model_validate_json(raw)
    except (ValueError, UnicodeError, binascii.Error, ValidationError) as error:
        raise ReviewInputError("Invalid workspace summary cursor") from error
    if cursor.include_archived != request.include_archived:
        raise ReviewInputError("Workspace summary cursor archive scope does not match the request")
    return cursor


def review_workspace_summary(workspace: ReviewWorkspace) -> ReviewWorkspaceSummary:
    tracks = workspace.search_tracks
    return ReviewWorkspaceSummary(
        record_identifier=workspace.record_identifier,
        name=workspace.name,
        search_run_record_identifier=workspace.search_run_record_identifier,
        product_guide_record_identifier=workspace.product_guide_record_identifier,
        created_at_utc=workspace.created_at_utc,
        archived=workspace.archived,
        track_count=len(tracks),
        enabled_track_count=sum(track.enabled for track in tracks),
        completed_track_count=sum(
            track.current_search_run_record_identifier is not None for track in tracks
        ),
        failed_refresh_track_count=sum(
            track.latest_refresh_work_state is WorkState.TERMINAL_FAILURE for track in tracks
        ),
        product_guide_binding_count=len(workspace.product_guide_bindings),
        default_product_guide_binding_identifier=workspace.default_product_guide_binding_identifier,
    )


def _measured_page(page: ReviewWorkspaceSummariesPage) -> ReviewWorkspaceSummariesPage:
    # Including the decimal byte count changes its own size. This converges once
    # the count has the right number of digits, including at power-of-ten edges.
    for _ in range(10):
        response_bytes = len(page.model_dump_json().encode())
        if response_bytes == page.response_bytes:
            return page
        page = page.model_copy(update={"response_bytes": response_bytes})
    raise RuntimeError("Workspace summary response size did not converge")


def build_workspace_summary_page(
    workspaces: Sequence[ReviewWorkspace | ReviewWorkspaceSummary],
    request: ListReviewWorkspaceSummariesRequest,
    *,
    more_available: bool = False,
) -> ReviewWorkspaceSummariesPage:
    """Page a full collection or a sorted eligible prefix with an external continuation.

    Callers fetching a prefix should provide at least one eligible row and set
    more_available when additional eligible workspace identities remain. No track
    specifications or nested per-track arrays enter the response.
    """

    cursor = decode_workspace_summaries_cursor(request)
    eligible = sorted(
        (
            review_workspace_summary(workspace)
            if isinstance(workspace, ReviewWorkspace)
            else workspace
            for workspace in workspaces
            if (request.include_archived or not workspace.archived)
            and (cursor is None or workspace.record_identifier > cursor.after_workspace_identifier)
        ),
        key=lambda workspace: workspace.record_identifier,
    )
    if len({workspace.record_identifier for workspace in eligible}) != len(eligible):
        raise ValueError("Workspace summary identities must be unique")
    if more_available and not eligible:
        raise ValueError("A workspace summary prefix requires an eligible row for continuation")

    def page(count: int) -> ReviewWorkspaceSummariesPage:
        selected = tuple(eligible[:count])
        has_more = more_available or count < len(eligible)
        next_cursor = None
        if has_more and selected:
            next_cursor = (
                base64.urlsafe_b64encode(
                    WorkspaceSummariesCursor(
                        include_archived=request.include_archived,
                        after_workspace_identifier=selected[-1].record_identifier,
                    )
                    .model_dump_json()
                    .encode()
                )
                .decode()
                .rstrip("=")
            )
        return _measured_page(
            ReviewWorkspaceSummariesPage(
                workspaces=selected,
                next_cursor=next_cursor,
                has_more=has_more,
                response_bytes=0,
                response_byte_budget=request.response_byte_budget,
            )
        )

    if not eligible:
        return page(0)
    result = None
    for count in range(1, min(request.page_size, len(eligible)) + 1):
        candidate = page(count)
        if candidate.response_bytes > request.response_byte_budget:
            break
        result = candidate
    if result is None:
        raise ReviewInputError("One workspace summary exceeds the requested response byte budget")
    return result
