"""Rich rendering for Carl's durable activity snapshot."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from rich.console import Group, RenderableType
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from carl.core.activity import ActivitySnapshot, WorkActivity
from carl.core.json import encode_json
from carl.core.work import NetworkActivityState, WorkState


def _duration(value_ns: int) -> str:
    seconds = max(0, value_ns) / 1_000_000_000
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


def _kind(parts: tuple[str, ...]) -> Text:
    return Text(encode_json(list(parts)))


def _identifier(value: str) -> str:
    return value if len(value) <= 16 else f"{value[:12]}…"


def _work_status(work: WorkActivity, now_utc_ns: int) -> Text:
    if work.state is WorkState.LEASED:
        if work.lease_expires_at_utc_ns is None:
            return Text("leased", style="yellow")
        remaining = work.lease_expires_at_utc_ns - now_utc_ns
        if remaining < 0:
            return Text(f"stale {_duration(-remaining)}", style="bold red")
        return Text(f"leased {_duration(remaining)}", style="cyan")
    delay = work.eligible_at_utc_ns - now_utc_ns
    if delay > 0:
        return Text(f"held {_duration(delay)}", style="yellow")
    return Text("ready", style="green")


def _network_status(
    state: NetworkActivityState,
    *,
    eligible_at_utc_ns: int,
    permit_expires_at_utc_ns: int | None,
    now_utc_ns: int,
) -> Text:
    if state is NetworkActivityState.ADMITTED:
        if permit_expires_at_utc_ns is None:
            return Text("admitted", style="yellow")
        remaining = permit_expires_at_utc_ns - now_utc_ns
        if remaining < 0:
            return Text(f"stale {_duration(-remaining)}", style="bold red")
        return Text(f"admitted {_duration(remaining)}", style="cyan")
    delay = eligible_at_utc_ns - now_utc_ns
    if delay > 0:
        return Text(f"held {_duration(delay)}", style="yellow")
    return Text("ready", style="green")


def _work_summary(snapshot: ActivitySnapshot) -> Table:
    pending = sum(item.pending for item in snapshot.work_kinds)
    leased = sum(item.leased for item in snapshot.work_kinds)
    completed = sum(item.completed for item in snapshot.work_kinds)
    failed = sum(item.terminal_failure for item in snapshot.work_kinds)
    recent_completed = sum(item.recent_completed for item in snapshot.work_kinds)
    recent_failed = sum(item.recent_terminal_failure for item in snapshot.work_kinds)
    table = Table.grid(expand=True, padding=(0, 1))
    for _ in range(6):
        table.add_column(justify="center")
    table.add_row(
        f"[bold]{pending}[/]\npending",
        f"[bold cyan]{leased}[/]\nactive leases",
        f"[bold green]{recent_completed}[/]\nrecently completed",
        f"[bold red]{recent_failed}[/]\nrecently failed",
        f"[bold]{completed}[/]\nall completed",
        f"[bold]{failed}[/]\nall failed",
    )
    return table


def _work_kinds(snapshot: ActivitySnapshot) -> Table:
    table = Table(title="Durable work by kind", expand=True)
    table.add_column("Kind", overflow="fold")
    table.add_column("Pending", justify="right")
    table.add_column("Active", justify="right")
    table.add_column("Recent done", justify="right")
    table.add_column("Recent failed", justify="right")
    table.add_column("All done", justify="right")
    table.add_column("All failed", justify="right")
    for item in snapshot.work_kinds:
        if not (
            item.pending or item.leased or item.recent_completed or item.recent_terminal_failure
        ):
            continue
        table.add_row(
            _kind(item.kind),
            str(item.pending),
            str(item.leased),
            str(item.recent_completed),
            str(item.recent_terminal_failure),
            str(item.completed),
            str(item.terminal_failure),
        )
    return table


def _active_work(snapshot: ActivitySnapshot) -> Table:
    table = Table(title="Active and queued work", expand=True)
    table.add_column("ID", no_wrap=True)
    table.add_column("State", no_wrap=True)
    table.add_column("Age", justify="right", no_wrap=True)
    table.add_column("Attempt", justify="right")
    table.add_column("Worker", no_wrap=True)
    table.add_column("Kind", overflow="fold")
    table.add_column("Subject", overflow="fold")
    table.add_column("Phase", overflow="fold")
    for work in snapshot.active_work:
        table.add_row(
            _identifier(work.identifier),
            _work_status(work, snapshot.captured_at_utc_ns),
            _duration(snapshot.captured_at_utc_ns - work.created_at_utc_ns),
            str(work.attempt),
            Text(_identifier(work.worker_identifier)) if work.worker_identifier else "",
            _kind(work.kind),
            Text(work.subject or ""),
            Text(work.stage or ""),
        )
    if not snapshot.active_work:
        table.add_row("", "idle", "", "", "", "", "", "")
    return table


def _network_paths(snapshot: ActivitySnapshot) -> Table:
    table = Table(title="Network-layer usage", expand=True)
    table.add_column("Ordered path", overflow="fold")
    table.add_column("Pending", justify="right")
    table.add_column("Admitted", justify="right")
    table.add_column("Recent done", justify="right")
    table.add_column("Recent failed", justify="right")
    table.add_column("All done", justify="right")
    table.add_column("All failed", justify="right")
    network = snapshot.network
    table.add_row(
        Text("All recorded paths", style="bold"),
        str(network.pending),
        str(network.admitted),
        str(network.recent_completed),
        str(network.recent_failed + network.recent_cancelled),
        str(network.completed),
        str(network.failed + network.cancelled),
    )
    for item in snapshot.network_paths:
        recent_failed = item.recent_failed + item.recent_cancelled
        all_failed = item.failed + item.cancelled
        table.add_row(
            _kind(item.path),
            str(item.pending),
            str(item.admitted),
            str(item.recent_completed),
            str(recent_failed),
            str(item.completed),
            str(all_failed),
        )
    if not snapshot.network_paths:
        table.add_row("none recorded", "0", "0", "0", "0", "0", "0")
    return table


def _active_network(snapshot: ActivitySnapshot) -> Table:
    table = Table(title="Current network activity", expand=True)
    table.add_column("ID", no_wrap=True)
    table.add_column("State", no_wrap=True)
    table.add_column("Age", justify="right", no_wrap=True)
    table.add_column("Path", overflow="fold")
    table.add_column("Kind", overflow="fold")
    table.add_column("Sequence", justify="right")
    table.add_column("Session", no_wrap=True)
    for activity in snapshot.active_network:
        table.add_row(
            _identifier(activity.identifier),
            _network_status(
                activity.state,
                eligible_at_utc_ns=activity.eligible_at_utc_ns,
                permit_expires_at_utc_ns=activity.permit_expires_at_utc_ns,
                now_utc_ns=snapshot.captured_at_utc_ns,
            ),
            _duration(snapshot.captured_at_utc_ns - activity.created_at_utc_ns),
            _kind(activity.path),
            _kind(activity.kind),
            f"{activity.ordinal}.{activity.attempt}",
            _identifier(activity.session_identifier),
        )
    if not snapshot.active_network:
        table.add_row("", "idle", "", "", "", "", "")
    return table


def _database_processes(snapshot: ActivitySnapshot) -> Table:
    table = Table(title="Processes using this database", expand=True)
    table.add_column("PID", justify="right", no_wrap=True)
    table.add_column("Command", no_wrap=True)
    table.add_column("Current", no_wrap=True)
    table.add_column("Open files", overflow="fold")
    table.add_column("Working directory", overflow="fold")
    table.add_column("Source", no_wrap=True)
    for process in snapshot.database_processes:
        table.add_row(
            str(process.process_identifier),
            process.command,
            "yes" if process.current_process else "no",
            ", ".join(process.database_files),
            process.working_directory or "unknown",
            process.source_tree_sha256[:12] if process.source_tree_sha256 else "unknown",
        )
    if not snapshot.database_processes:
        table.add_row("", "none visible", "", "", "", "")
    return table


def _recent_work(snapshot: ActivitySnapshot) -> Table:
    table = Table(title="Recent terminal work", expand=True)
    table.add_column("Ago", justify="right", no_wrap=True)
    table.add_column("State", no_wrap=True)
    table.add_column("ID", no_wrap=True)
    table.add_column("Kind", overflow="fold")
    table.add_column("Subject", overflow="fold")
    table.add_column("Result", overflow="fold")
    for work in snapshot.recent_terminal_work:
        terminal_at = work.terminal_at_utc_ns or snapshot.captured_at_utc_ns
        state = (
            Text("completed", style="green")
            if work.state is WorkState.COMPLETED
            else Text("failed", style="red")
        )
        table.add_row(
            _duration(snapshot.captured_at_utc_ns - terminal_at),
            state,
            _identifier(work.identifier),
            _kind(work.kind),
            Text(work.subject or ""),
            Text(work.error_kind or work.stage or ""),
        )
    return table


def activity_dashboard(snapshot: ActivitySnapshot, database: Path) -> RenderableType:
    """Render a snapshot without performing I/O or retaining another state copy."""

    observed = datetime.fromtimestamp(
        snapshot.captured_at_utc_ns / 1_000_000_000, tz=UTC
    ).isoformat(timespec="seconds")
    recent = _duration(snapshot.recent_window_ns)
    heading = Text()
    heading.append("Carl activity", style="bold")
    heading.append(f"  {observed}  •  recent window {recent}\n")
    heading.append(str(database))
    if snapshot.connectivity.paused:
        _ = heading.append("\nNetwork work paused", style="bold red")
        if snapshot.connectivity.probe_in_progress:
            _ = heading.append(" — checking connectivity", style="yellow")
        else:
            _ = heading.append(" — internet connectivity probe failed", style="red")
            next_probe = snapshot.connectivity.next_probe_at_utc_ns
            if next_probe is not None:
                _ = heading.append(
                    f"; next probe in {_duration(next_probe - snapshot.captured_at_utc_ns)}"
                )
    return Group(
        Panel(heading),
        Panel(_work_summary(snapshot)),
        _database_processes(snapshot),
        _network_paths(snapshot),
        _active_network(snapshot),
        _work_kinds(snapshot),
        _active_work(snapshot),
        _recent_work(snapshot),
    )
