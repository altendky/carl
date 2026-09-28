"""Pure activity-monitoring values and work labels."""

from __future__ import annotations

from carl.core.models import JsonValue, StrictModel
from carl.core.work import NetworkActivityState, WorkState


class WorkKindActivity(StrictModel):
    kind: tuple[str, ...]
    pending: int
    leased: int
    completed: int
    terminal_failure: int
    recent_completed: int
    recent_terminal_failure: int


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
    path: tuple[str, ...]
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


class ActiveNetworkActivity(StrictModel):
    identifier: str
    kind: tuple[str, ...]
    path: tuple[str, ...]
    state: NetworkActivityState
    session_identifier: str
    ordinal: int
    attempt: int
    created_at_utc_ns: int
    eligible_at_utc_ns: int
    admitted_at_utc_ns: int | None
    permit_expires_at_utc_ns: int | None


class DatabaseProcessActivity(StrictModel):
    process_identifier: int
    command: str
    working_directory: str | None
    database_files: tuple[str, ...]
    current_process: bool
    source_tree_sha256: str | None


class ActivitySnapshot(StrictModel):
    captured_at_utc_ns: int
    recent_window_ns: int
    work_kinds: tuple[WorkKindActivity, ...]
    active_work: tuple[WorkActivity, ...]
    recent_terminal_work: tuple[WorkActivity, ...]
    network: NetworkActivityCounts
    network_paths: tuple[NetworkPathActivity, ...]
    active_network: tuple[ActiveNetworkActivity, ...]
    database_processes: tuple[DatabaseProcessActivity, ...] = ()


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
