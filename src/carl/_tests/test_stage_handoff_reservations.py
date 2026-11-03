"""Pending admission cannot let one resource monopolize the execution runners."""

# Small barriers isolate dispatcher handoff from the database admission authority.
# pyright: reportPrivateUsage=false

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import cast

import anyio
import pytest

from carl._tests.test_worker import _provenance
from carl.core.fair_scheduling import FairDispatchPage
from carl.core.resources import ResourceRequest
from carl.core.scheduler_activity import SchedulerWaitObservation
from carl.core.work import WorkCapability, WorkState
from carl.core.work_dispatch import DispatchCandidate
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.sqlite import LeaseLostError
from carl.io.stage_dispatcher import StageDispatcher, _PendingAdmission
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import StageManager

pytestmark = pytest.mark.anyio
CAPABILITY = WorkCapability(kind=("test", "handoff"), payload_schema_version=1)


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _candidate(identifier: str, *resources: ResourceRequest) -> DispatchCandidate:
    return DispatchCandidate(
        work_item_identifier=identifier,
        capability=CAPABILITY,
        state=WorkState.PENDING,
        attempt=0,
        priority=0,
        eligible_at_utc_ns=0,
        created_at_utc_ns=0,
        stage_identifier=f"stage-{identifier}",
        stage_kind=("test", "stage"),
        stage_resources=resources,
    )


class _Harness:
    def __init__(self, pages: tuple[tuple[DispatchCandidate, ...], ...]) -> None:
        self.pages = pages
        self.stages = {
            candidate.stage_identifier: SimpleNamespace(
                identifier=candidate.stage_identifier,
                work_item_identifier=candidate.work_item_identifier,
                kind=candidate.stage_kind,
                resources=candidate.stage_resources,
                resident_session_identifier=None,
            )
            for page in pages
            for candidate in page
        }
        self.resources = SimpleNamespace(
            capacities={"cpu": 4, "http": 25, "analysis": 10}, scope="test-host"
        )
        self.database = self
        self.allow_cpu = anyio.Event()
        self.finish_execution = anyio.Event()
        self.entered: list[str] = []
        self.executed: list[str] = []
        self.events: dict[str, anyio.Event] = {}
        self.physical: dict[str, int] = {}
        self.offered: list[str] = []
        self.observations: list[SchedulerWaitObservation] = []
        self.page_reads = 0
        self.occupancy_reads = 0
        self.third_page = anyio.Event()
        self.occupancy_hook: Callable[[], Awaitable[None]] | None = None
        self.mode = "execute"
        self.block_heavy_only = False
        self.resident_owned = False
        self.resident_submissions = 0
        self.owned_submissions = 0
        self.adapter = SimpleNamespace(
            capability=CAPABILITY,
            component=object(),
            initial_resources=(ResourceRequest(name="cpu"),),
            initial_stage=self.initial_stage,
        )
        self.adapters = (self.adapter,)
        self.owned_work_identifiers: tuple[str, ...] = ()
        self.dispatcher = StageDispatcher(
            cast(StageManager, cast(object, self)),
            cast(StageHandlerRegistry, cast(object, self)),
            cast(ResidentSessionPool, cast(object, self)),
            _provenance(),
            {},
            self.submit_owned,
            lambda _kind: self.mode == "owned",
            poll_seconds=0,
        )

    def event(self, name: str) -> anyio.Event:
        return self.events.setdefault(name, anyio.Event())

    @staticmethod
    def utc_now_ns() -> int:
        return 1_000

    @staticmethod
    def new_identifier() -> str:
        return "new-stage"

    async def fair_dispatch_candidates(
        self, **arguments: object
    ) -> FairDispatchPage[DispatchCandidate]:
        # Discovery starts after idle receivers have reached their channel wait,
        # not after an arbitrary number of scheduler checkpoints.
        await anyio.wait_all_tasks_blocked()
        index = self.page_reads % len(self.pages)
        self.page_reads += 1
        if self.page_reads >= 3:
            self.third_page.set()
        excluded = cast(tuple[str, ...], arguments["excluded_identifiers"])
        return cast(
            FairDispatchPage[DispatchCandidate],
            cast(
                object,
                SimpleNamespace(
                    items=tuple(
                        candidate
                        for candidate in self.pages[index]
                        if candidate.work_item_identifier not in excluded
                    ),
                    next_cursor=None,
                    has_more=index + 1 < len(self.pages),
                ),
            ),
        )

    async def offer_fair_dispatch_candidates(self, **arguments: object) -> None:
        self.offered.extend(cast(tuple[str, ...], arguments["work_identifiers"]))

    async def resource_occupancy(self, _scope: str) -> dict[str, int]:
        before = dict(self.physical)
        self.occupancy_reads += 1
        if self.occupancy_hook is not None:
            await self.occupancy_hook()
        return before

    async def observe_scheduler_waits(self, **arguments: object) -> None:
        self.observations.extend(
            cast(tuple[SchedulerWaitObservation, ...], arguments["observations"])
        )
        await anyio.lowlevel.checkpoint()

    def require_adapter(self, _capability: WorkCapability) -> SimpleNamespace:
        return self.adapter

    async def stage(self, identifier: str) -> SimpleNamespace:
        return self.stages[identifier]

    async def work_dispatch_snapshot(self, _identifier: str) -> SimpleNamespace:
        return SimpleNamespace(definition=CAPABILITY)

    def initial_stage(self, *_arguments: object) -> SimpleNamespace:
        if self.mode == "invalid_initial":
            raise ValueError("invalid initial payload")
        return next(iter(self.stages.values()))

    async def try_register_initial_stage(
        self, *_arguments: object, **_keywords: object
    ) -> SimpleNamespace:
        return SimpleNamespace(stage_identifier=None)

    def owns(self, _identifier: str) -> bool:
        return self.resident_owned

    async def submit(self, *_arguments: object, **_keywords: object) -> None:
        self.resident_submissions += 1

    async def submit_owned(self, _identifier: str) -> bool:
        self.owned_submissions += 1
        self.event("owned-submitted").set()
        return False

    @asynccontextmanager
    async def admit(self, identifier: str, **_arguments: object) -> AsyncIterator[SimpleNamespace]:
        stage = self.stages[identifier]
        work = str(stage.work_item_identifier)
        self.entered.append(work)
        self.event(f"entered-{work}").set()
        if any(
            request.name == "cpu" and (not self.block_heavy_only or request.units > 1)
            for request in stage.resources
        ):
            await self.allow_cpu.wait()
        if self.mode == "admission_error":
            raise ValueError("admission failed")
        if self.mode == "ownership_lost":
            raise LeaseLostError("ownership lost")
        if self.mode == "declined":
            yield SimpleNamespace(execution=None)
            return
        for request in stage.resources:
            self.physical[request.name] = self.physical.get(request.name, 0) + request.units
        try:
            yield SimpleNamespace(execution=stage)
        finally:
            for request in stage.resources:
                self.physical[request.name] -= request.units

    async def execute(self, stage: SimpleNamespace) -> None:
        work = str(stage.work_item_identifier)
        # Physical admission replaces the hint before a potentially long handler.
        assert work not in self.dispatcher._pending_admissions
        self.executed.append(work)
        self.event(f"executed-{work}").set()
        await self.finish_execution.wait()


async def _noop(_dispatcher: StageDispatcher) -> None:
    pass


def _without_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(StageDispatcher, "_recover", _noop)
    monkeypatch.setattr(StageDispatcher, "_progress", _noop)


async def test_slow_cpu_admission_leaves_runners_for_http_on_later_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cpu = tuple(_candidate(f"cpu-{index}", ResourceRequest(name="cpu")) for index in range(80))
    http = _candidate("http", ResourceRequest(name="http"))
    metadata = _candidate("metadata")
    harness = _Harness((cpu[:32], cpu[32:64], cpu[64:], (http, metadata)))
    _without_recovery(monkeypatch)

    with anyio.fail_after(10):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(harness.dispatcher.run)
            await harness.event("executed-http").wait()
            await harness.event("executed-metadata").wait()
            assert len([work for work in harness.entered if work.startswith("cpu-")]) == 4
            assert not harness.allow_cpu.is_set()
            assert harness.physical.get("cpu", 0) == 0
            assert "http" in harness.offered
            tasks.cancel_scope.cancel()
    assert harness.dispatcher._pending_admissions == {}
    assert harness.dispatcher._queued == set()


async def test_pending_hint_survives_admission_finishing_during_occupancy_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _candidate("first", ResourceRequest(name="cpu"))
    second = _candidate("second", ResourceRequest(name="cpu"))
    harness = _Harness(((first,), (second,), ()))
    harness.resources.capacities["cpu"] = 1
    _without_recovery(monkeypatch)

    async def commit_during_read() -> None:
        if harness.occupancy_reads == 2:
            await harness.event("entered-first").wait()
            harness.allow_cpu.set()
            await harness.event("executed-first").wait()

    harness.occupancy_hook = commit_during_read
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(harness.dispatcher.run)
            await harness.third_page.wait()
            assert harness.entered == ["first"]
            assert harness.dispatcher._queued == {"first"}
            assert harness.physical["cpu"] == 1
            tasks.cancel_scope.cancel()


async def test_owned_handoff_is_released_without_a_dispatcher_offer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(((_candidate("owned", ResourceRequest(name="cpu")),),))
    harness.mode = "owned"
    _without_recovery(monkeypatch)
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(harness.dispatcher.run)
            await harness.event("owned-submitted").wait()
            assert harness.offered == []
            tasks.cancel_scope.cancel()
    assert harness.dispatcher._pending_admissions == {}


async def test_one_weighted_cpu_handoff_can_drain_without_monopolizing_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    heavy = tuple(
        _candidate(f"heavy-{index}", ResourceRequest(name="cpu", units=4)) for index in range(80)
    )
    harness = _Harness(
        (heavy[:32], heavy[32:64], heavy[64:], (_candidate("http", ResourceRequest(name="http")),))
    )
    harness.physical["cpu"] = 4
    _without_recovery(monkeypatch)
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(harness.dispatcher.run)
            await harness.event("executed-http").wait()
            assert len([work for work in harness.entered if work.startswith("heavy-")]) == 1
            assert not harness.allow_cpu.is_set()
            tasks.cancel_scope.cancel()


async def test_fitting_weighted_admissions_fill_the_cpu_budget_in_parallel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _candidate("first", ResourceRequest(name="cpu", units=2))
    second = _candidate("second", ResourceRequest(name="cpu", units=2))
    http = _candidate("http", ResourceRequest(name="http"))
    harness = _Harness(((first,), (second,), (http,)))
    _without_recovery(monkeypatch)
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(harness.dispatcher.run)
            await harness.event("executed-http").wait()
            assert set(harness.entered) == {"first", "second", "http"}
            assert not harness.allow_cpu.is_set()
            assert harness.physical.get("cpu", 0) == 0
            tasks.cancel_scope.cancel()


async def test_weighted_cpu_handoff_still_respects_other_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mixed = _candidate("mixed", ResourceRequest(name="cpu", units=4), ResourceRequest(name="http"))
    harness = _Harness(((mixed,), (), ()))
    harness.physical.update(cpu=4, http=25)
    _without_recovery(monkeypatch)
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(harness.dispatcher.run)
            await harness.third_page.wait()
            assert harness.entered == []
            assert any("http" in observation.resource_names for observation in harness.observations)
            tasks.cancel_scope.cancel()


async def test_waiting_weighted_probe_does_not_hide_spare_cpu_from_small_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    heavy = _candidate("heavy", ResourceRequest(name="cpu", units=4))
    small = _candidate("small", ResourceRequest(name="cpu"))
    harness = _Harness(((heavy,), (small,)))
    harness.physical["cpu"] = 3
    harness.block_heavy_only = True
    _without_recovery(monkeypatch)
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(harness.dispatcher.run)
            await harness.event("executed-small").wait()
            assert "heavy" in harness.entered
            assert not harness.allow_cpu.is_set()
            assert harness.physical["cpu"] == 4
            tasks.cancel_scope.cancel()


async def test_waiting_mixed_cpu_probe_does_not_reserve_http_on_later_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    heavy = _candidate(
        "heavy", ResourceRequest(name="cpu", units=4), ResourceRequest(name="http", units=25)
    )
    http = _candidate("http", ResourceRequest(name="http"))
    harness = _Harness(((heavy,), (), (http,)))
    harness.physical["cpu"] = 3
    _without_recovery(monkeypatch)
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(harness.dispatcher.run)
            await harness.event("executed-http").wait()
            assert "heavy" in harness.entered
            assert not harness.allow_cpu.is_set()
            assert harness.physical["cpu"] == 3
            assert harness.physical["http"] == 1
            tasks.cancel_scope.cancel()


async def test_unavailable_receiver_does_not_leave_a_pending_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _candidate("first", ResourceRequest(name="cpu"))
    second = _candidate("second", ResourceRequest(name="cpu"))
    harness = _Harness(((first, second), (), ()))
    harness.dispatcher.execution_count = 1
    _without_recovery(monkeypatch)
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(harness.dispatcher.run)
            await harness.event("entered-first").wait()
            await harness.third_page.wait()
            assert set(harness.dispatcher._pending_admissions) == {"first"}
            assert harness.dispatcher._queued == {"first"}
            assert any(
                observation.work_identifier == "second" and observation.reason == "execution_runner"
                for observation in harness.observations
            )
            tasks.cancel_scope.cancel()


@pytest.mark.parametrize(
    "mode",
    (
        "declined",
        "ownership_lost",
        "admission_error",
        "owned",
        "resident_owned",
        "resident_submit",
        "metadata_stale",
        "initial_declined",
        "invalid_initial",
    ),
)
async def test_handoff_hint_is_released_on_decline_error_or_early_return(mode: str) -> None:
    candidate = _candidate("work", ResourceRequest(name="cpu"))
    harness = _Harness(((candidate,),))
    harness.mode = mode
    harness.allow_cpu.set()
    if mode in {"resident_owned", "resident_submit"}:
        harness.stages[candidate.stage_identifier].resident_session_identifier = "resident"
        harness.resident_owned = mode == "resident_owned"
    if mode in {"initial_declined", "invalid_initial"}:
        candidate = candidate.model_copy(update={"stage_identifier": None})
    harness.dispatcher._queued.add("work")
    harness.dispatcher._pending_admissions["work"] = _PendingAdmission(
        candidate.stage_resources, candidate.stage_resources
    )
    send, receive = anyio.create_memory_object_stream[DispatchCandidate](1)
    async with send, receive:
        send.send_nowait(candidate)
        await send.aclose()
        if mode == "admission_error":
            with pytest.raises(ValueError, match="admission failed"):
                await harness.dispatcher._execute(receive)
        else:
            await harness.dispatcher._execute(receive, metadata_lane=mode == "metadata_stale")
    assert harness.dispatcher._pending_admissions == {}
    assert harness.dispatcher._queued == set()
    assert harness.resident_submissions == int(mode == "resident_submit")
    assert harness.owned_submissions == int(mode == "owned")
