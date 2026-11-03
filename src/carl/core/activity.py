"""Pure activity-monitoring values and work labels."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field

from carl.core.models import JsonValue, StrictModel
from carl.core.scheduler_activity import SchedulerActivity
from carl.core.work import NetworkActivityState, WorkState

type ActivityLabelPart = Annotated[str, Field(max_length=64)]
type DatabaseFileKind = Literal["database", "wal", "shared_memory"]


class WorkKindActivity(StrictModel):
    kind: tuple[ActivityLabelPart, ...] = Field(max_length=8)
    pending: int
    leased: int
    completed: int
    terminal_failure: int
    recent_completed: int
    recent_terminal_failure: int
    cancelling: int = 0
    cancelled: int = 0
    recent_cancelled: int = 0
    labels_truncated: bool = False


class WorkActivity(StrictModel):
    identifier: str
    kind: tuple[str, ...]
    state: WorkState
    attempt: int
    created_at_utc_ns: int
    eligible_at_utc_ns: int
    lease_expires_at_utc_ns: int | None
    worker_identifier: str | None
    subject: str | None
    stage: str | None
    error_kind: str | None
    terminal_at_utc_ns: int | None = None


class BoundedWorkActivity(WorkActivity):
    """Activity-monitor labels without changing explicit workspace progress."""

    identifier: str = Field(max_length=160)
    kind: tuple[ActivityLabelPart, ...] = Field(max_length=8)
    worker_identifier: str | None = Field(max_length=160)
    subject: str | None = Field(max_length=160)
    stage: str | None = Field(max_length=160)
    error_kind: str | None = Field(max_length=160)
    identity_truncated: bool = False
    labels_truncated: bool = False


class NetworkActivityCounts(StrictModel):
    pending: int
    admitted: int
    completed: int
    skipped: int
    failed: int
    cancelled: int
    recent_completed: int
    recent_skipped: int
    recent_failed: int
    recent_cancelled: int


class NetworkPathActivity(StrictModel):
    path: tuple[ActivityLabelPart, ...] = Field(max_length=8)
    pending: int
    admitted: int
    completed: int
    skipped: int
    failed: int
    cancelled: int
    recent_completed: int
    recent_skipped: int
    recent_failed: int
    recent_cancelled: int
    labels_truncated: bool = False


class ActiveNetworkActivity(StrictModel):
    identifier: str = Field(max_length=160)
    kind: tuple[ActivityLabelPart, ...] = Field(max_length=8)
    path: tuple[ActivityLabelPart, ...] = Field(max_length=8)
    state: NetworkActivityState
    session_identifier: str = Field(max_length=160)
    ordinal: int
    attempt: int
    created_at_utc_ns: int
    eligible_at_utc_ns: int
    admitted_at_utc_ns: int | None
    permit_expires_at_utc_ns: int | None
    identity_truncated: bool = False
    labels_truncated: bool = False


class DatabaseProcessActivity(StrictModel):
    process_identifier: int
    command: str = Field(max_length=96)
    working_directory: str | None = Field(max_length=160)
    database_files: tuple[DatabaseFileKind, ...] = Field(max_length=3)
    current_process: bool
    source_tree_sha256: str | None = Field(max_length=64)


class DatabaseProcessSnapshot(StrictModel):
    processes: tuple[DatabaseProcessActivity, ...] = Field(max_length=8)
    total_count: int = Field(ge=0)


class NetworkConnectivityActivity(StrictModel):
    """Global network-work pause and its latest direct connectivity probe."""

    paused: bool = False
    reason: str | None = None
    probe_in_progress: bool = False
    next_probe_at_utc_ns: int | None = None
    last_probe_at_utc_ns: int | None = None
    last_probe_result: JsonValue = None


class ActivitySnapshot(StrictModel):
    captured_at_utc_ns: int
    recent_window_ns: int
    work_kinds: tuple[WorkKindActivity, ...] = Field(max_length=32)
    active_work: tuple[BoundedWorkActivity, ...] = Field(max_length=100)
    recent_terminal_work: tuple[BoundedWorkActivity, ...] = Field(max_length=100)
    network: NetworkActivityCounts
    network_paths: tuple[NetworkPathActivity, ...] = Field(max_length=16)
    active_network: tuple[ActiveNetworkActivity, ...] = Field(max_length=100)
    database_processes: tuple[DatabaseProcessActivity, ...] = Field(default=(), max_length=8)
    database_process_count: int | None = Field(default=None, ge=0)
    process_omitted_count: int = Field(default=0, ge=0)
    work_kind_groups_omitted: int | None = Field(default=None, ge=0)
    work_kind_groups_truncated: bool = False
    network_path_groups_omitted: int | None = Field(default=None, ge=0)
    network_path_groups_truncated: bool = False
    active_work_omitted: int | None = Field(default=None, ge=0)
    recent_terminal_work_omitted: int | None = Field(default=None, ge=0)
    active_network_omitted: int | None = Field(default=None, ge=0)
    connectivity: NetworkConnectivityActivity = Field(default_factory=NetworkConnectivityActivity)
    scheduler: SchedulerActivity | None = None


def _mapping(value: JsonValue | None) -> dict[str, JsonValue] | None:
    return value if isinstance(value, dict) else None


def work_subject(kind: tuple[str, ...], payload: JsonValue) -> str | None:
    """Return a concise source-aware label without changing retained work data."""

    value = _mapping(payload)
    if value is None or not kind:
        return None
    operation = kind[-1]
    if operation == "collect_search":
        request = _mapping(value.get("request"))
        query = request.get("query") if request is not None else None
        return query if isinstance(query, str) else None
    if operation == "refresh_search":
        search = _mapping(value.get("search"))
        request = _mapping(search.get("request")) if search is not None else None
        query = request.get("query") if request is not None else None
        return query if isinstance(query, str) else None
    if operation in {"collect_item", "extract_item"}:
        listing_identifier = value.get("listing_id")
        if isinstance(listing_identifier, str):
            return f"listing {listing_identifier}"
    if operation in {"collect_image", "extract_image"}:
        reference = _mapping(value.get("reference"))
        listing_identifier = reference.get("listing_id") if reference is not None else None
        gallery_order = reference.get("gallery_order") if reference is not None else None
        if isinstance(listing_identifier, str):
            suffix = f" image {gallery_order}" if isinstance(gallery_order, int) else " image"
            return f"listing {listing_identifier}{suffix}"
    if operation == "analyze_item":
        model = value.get("model")
        effort = value.get("effort")
        if isinstance(model, str):
            return f"{model} {effort}" if isinstance(effort, str) else model
    return None


def work_stage(result: JsonValue | None) -> str | None:
    value = _mapping(result)
    stage = value.get("stage") if value is not None else None
    return stage if isinstance(stage, str) else None


def work_error_kind(error: JsonValue | None) -> str | None:
    value = _mapping(error)
    kind = value.get("kind") if value is not None else None
    return kind if isinstance(kind, str) else None
