"""Cancelled work remains visible without being presented as a failure."""

from pathlib import Path

import pytest
from rich.console import Console, RenderableType

from carl.core.activity import (
    ActivitySnapshot,
    NetworkActivityCounts,
    WorkActivity,
    WorkKindActivity,
)
from carl.core.work import WorkState
from carl.io.activity import activity_dashboard


def _render(renderable: RenderableType) -> str:
    console = Console(record=True, width=300, color_system=None)
    console.print(renderable)
    return console.export_text()


def _work(state: WorkState) -> WorkActivity:
    return WorkActivity(
        identifier="cancelled-refresh",
        kind=("carl", "test", "refresh"),
        state=state,
        attempt=1,
        created_at_utc_ns=0,
        eligible_at_utc_ns=0,
        lease_expires_at_utc_ns=5_000,
        worker_identifier="worker" if state is WorkState.CANCELLING else None,
        subject="astronomy",
        stage="collecting_images",
        error_kind=None,
        terminal_at_utc_ns=1_000 if state is WorkState.CANCELLED else None,
    )


@pytest.mark.parametrize("state", (WorkState.CANCELLING, WorkState.CANCELLED))
def test_activity_dashboard_distinguishes_cancellation(state: WorkState) -> None:
    snapshot = ActivitySnapshot(
        captured_at_utc_ns=2_000,
        recent_window_ns=2_000,
        work_kinds=(
            WorkKindActivity(
                kind=("carl", "test", "refresh"),
                pending=0,
                leased=0,
                completed=0,
                terminal_failure=0,
                recent_completed=0,
                recent_terminal_failure=0,
                cancelling=int(state is WorkState.CANCELLING),
                cancelled=int(state is WorkState.CANCELLED),
                recent_cancelled=int(state is WorkState.CANCELLED),
            ),
        ),
        active_work=(_work(state),) if state is WorkState.CANCELLING else (),
        recent_terminal_work=(_work(state),) if state is WorkState.CANCELLED else (),
        network=NetworkActivityCounts(
            pending=0,
            admitted=0,
            completed=0,
            skipped=0,
            failed=3,
            cancelled=11,
            recent_completed=0,
            recent_skipped=0,
            recent_failed=3,
            recent_cancelled=11,
        ),
        network_paths=(),
        active_network=(),
    )

    rendered = _render(activity_dashboard(snapshot, Path("carl.sqlite3")))
    assert state.value in rendered
    assert "astronomy" in rendered
    assert '["carl","test","refresh"]' in rendered
    # Cancelled network requests have their own columns, rather than increasing
    # either of the failure counts shown to the operator.
    network_row = next(line for line in rendered.splitlines() if "All recorded paths" in line)
    assert network_row.split() == [
        "│",
        "All",
        "recorded",
        "paths",
        "│",
        "0",
        "│",
        "0",
        "│",
        "0",
        "│",
        "3",
        "│",
        "0",
        "│",
        "3",
        "│",
        "11",
        "│",
        "11",
        "│",
    ]
