"""A productive finite pass does not inherit its empty tail's idle backoff."""

# Synthetic discovery leaves the production dispatcher and execution tasks intact.
# pyright: reportPrivateUsage=false

from collections.abc import Awaitable, Callable
from typing import cast

import anyio
import pytest

from carl._tests.test_stage_handoff_reservations import _candidate, _Harness
from carl.core.fair_scheduling import (
    FairDispatchCursor,
    FairDispatchHorizon,
    FairDispatchLaneHorizon,
    FairDispatchPage,
    FairLane,
    SchedulingServiceClass,
)
from carl.core.resources import ResourceRequest
from carl.core.work_dispatch import DispatchCandidate
from carl.io.stage_dispatcher import StageDispatcher

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _cursor(now: int) -> FairDispatchCursor:
    return FairDispatchCursor(
        horizon=FairDispatchHorizon(
            scope="test-host",
            captured_at_utc_ns=now,
            workspace_rowid_high_water=1,
            lanes=tuple(
                FairDispatchLaneHorizon(lane=lane, sequence=0, interactive_streak=0)
                for lane in FairLane
            ),
        ),
        lane=FairLane.HTTP,
        service_class=SchedulingServiceClass.BACKGROUND,
        workspace_sequence=0,
        workspace_key="workspace",
        root_sequence=0,
        root_request_identifier="root",
    )


class _PassHarness(_Harness):
    def __init__(self, first: tuple[DispatchCandidate, ...], *, multiple_pages: bool) -> None:
        super().__init__((first, ()))
        self.first: tuple[DispatchCandidate, ...] = first
        self.multiple_pages: bool = multiple_pages
        self.now: int = 1_000
        self.discovery: list[tuple[int, FairDispatchCursor | None]] = []
        self.dispatcher.poll_seconds = 0.25

    def clock(self) -> int:
        return self.now

    async def fair_dispatch_candidates(
        self, **arguments: object
    ) -> FairDispatchPage[DispatchCandidate]:
        # Wait for actual receiver readiness and for earlier synthetic phases to
        # settle, rather than depending on a guessed number of checkpoints.
        await anyio.wait_all_tasks_blocked()
        now = cast(int, arguments["now_utc_ns"])
        after = cast(FairDispatchCursor | None, arguments["after"])
        self.discovery.append((now, after))
        first = self.page_reads == 0
        self.page_reads += 1
        self.now += 1
        more = first and self.multiple_pages
        return FairDispatchPage(
            items=self.first if first else (),
            next_cursor=_cursor(now) if more else None,
            has_more=more,
        )


def _maintenance(
    monkeypatch: pytest.MonkeyPatch, progress: Callable[[StageDispatcher], Awaitable[bool]]
) -> None:
    async def recover(_dispatcher: StageDispatcher) -> None:
        pass

    monkeypatch.setattr(StageDispatcher, "_recover", recover)
    monkeypatch.setattr(StageDispatcher, "_progress", progress)


async def _quiet_progress(_dispatcher: StageDispatcher) -> bool:
    return False


async def _two_backoffs(
    monkeypatch: pytest.MonkeyPatch, harness: _PassHarness
) -> list[tuple[float, int]]:
    reached = anyio.Event()
    sleeps: list[tuple[float, int]] = []
    occupied = {name: units for name, units in harness.physical.items() if units}
    monkeypatch.setattr(harness, "utc_now_ns", harness.clock)

    async def sleep(seconds: float) -> None:
        sleeps.append((seconds, harness.page_reads))
        # Allow a second quiet pass, proving activity does not leak past the
        # first completed pass. Block at its backoff so cancellation is bounded.
        if len(sleeps) == 1:
            await anyio.lowlevel.checkpoint()
        else:
            reached.set()
            await anyio.Event().wait()

    monkeypatch.setattr(anyio, "sleep", sleep)
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(harness.dispatcher.run)
            await reached.wait()
            tasks.cancel_scope.cancel()
    assert harness.dispatcher._pending_admissions == {}
    assert harness.dispatcher._queued == set()
    assert {name: units for name, units in harness.physical.items() if units} == occupied
    return sleeps


@pytest.mark.parametrize("finish_phase", (True, False))
async def test_earlier_handoff_skips_empty_tail_backoff_then_quiet_passes_sleep(
    monkeypatch: pytest.MonkeyPatch, finish_phase: bool
) -> None:
    _maintenance(monkeypatch, _quiet_progress)
    work = _candidate("http", ResourceRequest(name="http"))
    harness = _PassHarness((work,), multiple_pages=True)
    if finish_phase:
        harness.finish_execution.set()
    sleeps = await _two_backoffs(monkeypatch, harness)
    assert harness.executed == ["http"]
    assert sleeps == [(0.25, 3), (0.25, 4)]
    assert [now for now, _ in harness.discovery] == [1_000, 1_000, 1_002, 1_003]
    assert [after is None for _, after in harness.discovery] == [True, False, True, True]
    # Both the completed-phase and cancelled-active-phase paths clear handoff
    # ownership; restarting discovery never resumes the prior finite horizon.
    assert harness.dispatcher._after is None
    assert harness.dispatcher._pass_time is None


async def test_earlier_progress_skips_empty_tail_backoff_without_leaking_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _PassHarness((), multiple_pages=True)

    async def progress(_dispatcher: StageDispatcher) -> bool:
        return harness.page_reads == 0

    _maintenance(monkeypatch, progress)
    assert await _two_backoffs(monkeypatch, harness) == [(0.25, 3), (0.25, 4)]
    assert harness.executed == []


@pytest.mark.parametrize("blocked", (False, True))
async def test_unproductive_finite_pass_still_backs_off(
    monkeypatch: pytest.MonkeyPatch, blocked: bool
) -> None:
    _maintenance(monkeypatch, _quiet_progress)
    first = (_candidate("cpu", ResourceRequest(name="cpu")),) if blocked else ()
    harness = _PassHarness(first, multiple_pages=True)
    if blocked:
        harness.physical["cpu"] = harness.resources.capacities["cpu"]
    # The occupied permit belongs to an external execution, so no local runner
    # may reclaim it when this dispatcher is cancelled.
    occupied = dict(harness.physical)
    sleeps = await _two_backoffs(monkeypatch, harness)
    assert sleeps == [(0.25, 2), (0.25, 3)]
    assert harness.executed == []
    assert harness.physical == occupied
