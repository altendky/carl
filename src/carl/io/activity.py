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
from carl.core.scheduler_activity import SchedulerWorkStatus
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


def _work_status(
    work: WorkActivity, now_utc_ns: int, scheduler: SchedulerWorkStatus | None = None
) -> Text:
    if work.state is WorkState.CANCELLING:
        return Text("cancelling", style="yellow")
    if work.state is WorkState.CANCELLED:
        return Text("cancelled", style="yellow")
    if work.state is WorkState.LEASED:
        if scheduler is not None and scheduler.stage_state == "pending":
            reason = scheduler.observed_blockers[0] if scheduler.observed_blockers else "pending"
            return Text(f"waiting {reason.replace('_', ' ')}", style="yellow")
        if work.lease_expires_at_utc_ns is None:
            return Text("leased", style="yellow")
        remaining = work.lease_expires_at_utc_ns - now_utc_ns
        if remaining < 0:
            return Text(f"lease expired {_duration(-remaining)}", style="yellow")
        return Text(f"logical lease {_duration(remaining)}", style="cyan")
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
    cancelling = sum(item.cancelling for item in snapshot.work_kinds)
    cancelled = sum(item.cancelled for item in snapshot.work_kinds)
    if snapshot.scheduler is not None and snapshot.scheduler.work_counts.complete:
        totals = snapshot.scheduler.work_counts
        pending, leased, completed, failed = (
            totals.pending,
            totals.leased,
            totals.completed,
            totals.terminal_failure,
        )
        cancelling, cancelled = totals.cancelling, totals.cancelled
    table = Table.grid(expand=True, padding=(0, 1))
    for _ in range(8):
        table.add_column(justify="center")
    table.add_row(
        f"[bold]{pending}[/]\npending",
        f"[bold cyan]{leased}[/]\nleased work",
        f"[bold green]{recent_completed}[/]\nrecently completed",
        f"[bold red]{recent_failed}[/]\nrecently failed",
        f"[bold]{completed}[/]\nall completed",
        f"[bold]{failed}[/]\nall failed",
        f"[bold yellow]{cancelling}[/]\ncancelling",
        f"[bold yellow]{cancelled}[/]\nall cancelled",
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
    table.add_column("Cancelling", justify="right")
    table.add_column("Recent cancelled", justify="right")
    table.add_column("All cancelled", justify="right")
    for item in snapshot.work_kinds:
        if not (
            item.pending
            or item.leased
            or item.cancelling
            or item.recent_completed
            or item.recent_terminal_failure
            or item.recent_cancelled
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
            str(item.cancelling),
            str(item.recent_cancelled),
            str(item.cancelled),
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
        scheduler = (
            next(
                (
                    sample
                    for sample in snapshot.scheduler.samples
                    if sample.work_identifier == work.identifier
                ),
                None,
            )
            if snapshot.scheduler is not None
            else None
        )
        table.add_row(
            Text(_identifier(work.identifier)),
            _work_status(work, snapshot.captured_at_utc_ns, scheduler),
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
    table.add_column("Recent cancelled", justify="right")
    table.add_column("All cancelled", justify="right")
    network = snapshot.network
    table.add_row(
        Text("All recorded paths", style="bold"),
        str(network.pending),
        str(network.admitted),
        str(network.recent_completed),
        str(network.recent_failed),
        str(network.completed),
        str(network.failed),
        str(network.recent_cancelled),
        str(network.cancelled),
    )
    for item in snapshot.network_paths:
        table.add_row(
            _kind(item.path),
            str(item.pending),
            str(item.admitted),
            str(item.recent_completed),
            str(item.recent_failed),
            str(item.completed),
            str(item.failed),
            str(item.recent_cancelled),
            str(item.cancelled),
        )
    if not snapshot.network_paths:
        table.add_row("none recorded", "0", "0", "0", "0", "0", "0", "0", "0")
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
    table.add_column("Process / role", no_wrap=True)
    table.add_column("Current", no_wrap=True)
    table.add_column("Open files", overflow="fold")
    table.add_column("Source", no_wrap=True)
    for process in snapshot.database_processes:
        table.add_row(
            str(process.process_identifier),
            Text(process.command),
            "yes" if process.current_process else "no",
            ", ".join(process.database_files),
            process.source_tree_sha256[:12] if process.source_tree_sha256 else "unknown",
        )
    if not snapshot.database_processes:
        table.add_row("", "none visible", "", "", "")
    if snapshot.process_omitted_count:
        table.add_row("", f"{snapshot.process_omitted_count} more omitted", "", "", "")
    return table


def _scheduler_resources(snapshot: ActivitySnapshot) -> Table:
    table = Table(title="Resource reservations — database and selected host scope", expand=True)
    table.add_column("Scope")
    table.add_column("Resource")
    table.add_column("Capacity", justify="right")
    table.add_column("Held", justify="right")
    table.add_column("Expired, still held", justify="right")
    table.add_column("Oldest hold", justify="right")
    scheduler = snapshot.scheduler
    if scheduler is None:
        table.add_row("unknown", "not observed", "unknown", "", "", "")
        return table
    for resource in scheduler.resources:
        table.add_row(
            Text(scheduler.scope_handle),
            resource.name,
            "unknown" if resource.capacity is None else str(resource.capacity),
            str(resource.held_units)
            if resource.complete
            else f"{resource.held_units} recorded (partial)",
            str(resource.expired_held_units),
            "unknown"
            if resource.oldest_held_age_ns is None
            else _duration(resource.oldest_held_age_ns),
        )
    if not scheduler.resources:
        table.add_row(Text(scheduler.scope_handle), "unconfigured", "unknown", "", "", "")
    return table


def _scheduler_phases(snapshot: ActivitySnapshot) -> Table:
    table = Table(title="Database phase sample — blockers checked for selected host", expand=True)
    table.add_column("Work")
    table.add_column("Logical state")
    table.add_column("Phase / state")
    table.add_column("Recorded wait / observation")
    table.add_column("Observed blockers")
    table.add_column("Ready age", justify="right")
    table.add_column("Execution age (UTC estimate)", justify="right")
    table.add_column("Wait age / source")
    if snapshot.scheduler is None:
        table.add_row("", "unknown", "", "", "not observed", "", "", "")
        return table
    for sample in snapshot.scheduler.samples:
        blockers = ", ".join(reason.replace("_", " ") for reason in sample.observed_blockers)
        if not sample.checks_complete:
            blockers += " (partial checks)"
        age = "unknown" if sample.wait_age_ns is None else _duration(sample.wait_age_ns)
        recorded = (
            sample.recorded_wait_kind.value if sample.recorded_wait_kind is not None else "none"
        )
        if sample.recorded_observation_reason is not None:
            recorded += f" / {sample.recorded_observation_reason.replace('_', ' ')}"
            if sample.recorded_observation_at_utc_ns is not None:
                recorded += f" ({_duration(sample.observed_at_utc_ns - sample.recorded_observation_at_utc_ns)} ago)"
        table.add_row(
            Text(_identifier(sample.work_identifier or sample.work_handle)),
            sample.logical_state.value,
            f"{sample.phase_family} / {sample.stage_state}",
            recorded,
            Text(blockers or "unknown"),
            "unknown"
            if sample.phase_ready_age_ns is None
            else _duration(sample.phase_ready_age_ns),
            "unknown"
            if sample.execution_age_ns is None or sample.execution_age_method == "unavailable"
            else _duration(sample.execution_age_ns),
            f"{age} / {sample.wait_observation_source}",
        )
    if not snapshot.scheduler.samples:
        table.add_row("", "no sampled phases", "", "", "", "", "", "")
    return table


def _scheduler_timings(snapshot: ActivitySnapshot) -> Table:
    table = Table(title="Selected-host scheduler timing coverage", expand=True)
    table.add_column("Metric")
    table.add_column("Count", justify="right")
    table.add_column("Total", justify="right")
    table.add_column("Maximum", justify="right")
    table.add_column("Method")
    table.add_column("Censored", justify="right")
    if snapshot.scheduler is not None:
        for timing in snapshot.scheduler.timings:
            table.add_row(
                timing.metric.replace("_", " "),
                str(timing.count),
                _duration(timing.total_ns) if timing.method != "unavailable" else "unknown",
                _duration(timing.maximum_ns) if timing.maximum_ns is not None else "unknown",
                timing.method.replace("_", " "),
                str(timing.censored_count),
            )
    if snapshot.scheduler is None or not snapshot.scheduler.timings:
        table.add_row("not observed", "", "unknown", "unknown", "unavailable", "")
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
        if work.state is WorkState.COMPLETED:
            state = Text("completed", style="green")
        elif work.state is WorkState.CANCELLED:
            state = Text("cancelled", style="yellow")
        else:
            state = Text("failed", style="red")
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
    heading.append("\nLogical leases are work ownership; held units remain charged during cleanup.")
    if snapshot.work_kind_groups_truncated or snapshot.network_path_groups_truncated:
        heading.append(
            "\nKind/path groups are truncated; shown group totals are partial.", style="yellow"
        )
    if snapshot.scheduler is not None:
        coverage = snapshot.scheduler.coverage
        stages = snapshot.scheduler.stage_counts
        heading.append(
            f"\nDatabase stages: {stages.running} recorded running, {stages.pending} pending, "
            f"{stages.progress_waiting} waiting for progress"
            + (" (partial counts)" if not stages.complete else "")
        )
        heading.append(
            f"\nScheduler coverage: {coverage.method}; {coverage.examined_rows} rows examined"
            + (" (truncated)" if coverage.truncated else "")
        )
        throughput = snapshot.scheduler.throughput
        if throughput is not None:
            heading.append(
                f"\nRecent database outcomes: {throughput.logical_completed} completed, "
                f"{throughput.logical_failed} failed, {throughput.logical_cancelled} cancelled; "
                f"{throughput.admissions} selected-host phase admissions"
                + (" (partial window)" if not throughput.complete else "")
            )
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
        Panel(_work_summary(snapshot), title="Retained logical work — database"),
        _scheduler_resources(snapshot),
        _scheduler_phases(snapshot),
        _scheduler_timings(snapshot),
        _database_processes(snapshot),
        _network_paths(snapshot),
        _active_network(snapshot),
        _work_kinds(snapshot),
        _active_work(snapshot),
        _recent_work(snapshot),
    )
