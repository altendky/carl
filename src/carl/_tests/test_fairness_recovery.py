"""Fair turns survive runtime stop, cancellation and occupied physical guards."""

# Share only the bounded provider-free production runtime fixture.
# pyright: reportPrivateUsage=false

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from time import time_ns
from uuid import uuid4

import anyio
import httpx
import pytest

from carl import dataset_setup_stages, work_runtime
from carl._tests.test_dataset_setup_stages import _Engine
from carl._tests.test_production_fairness import _context, _Harness
from carl.core.components import Component
from carl.core.dataset_setup import DatasetSetupState
from carl.core.dataset_workflows import DatasetWorkflowBinding, DatasetWorkflowDefinition
from carl.core.fair_scheduling import SchedulingServiceClass
from carl.core.resources import ResourceRequest
from carl.core.stages import StageDefinition
from carl.core.travel import TravelCoordinate
from carl.core.travel_data import TravelDataSetupRequest, travel_data_work
from carl.core.work import WorkRequester
from carl.io.dataset_setup import DatasetSetupEngine, prepare_dataset_setup
from carl.io.dataset_workflow_files import dataset_workflow_gate
from carl.io.dataset_workflow_owner import DatasetWorkflowService
from carl.io.paths import CarlDirectories
from carl.io.sqlite import Database
from carl.io.stages import stage_manager
from carl.io.travel_data import TravelDataManager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _drain_target(database: Database) -> str | None:
    async with database._connections.reader() as connection:
        row = await (
            await connection.execute(
                "SELECT json_extract(intent_json,'$.identifier') FROM fair_cpu_drains WHERE scope=?",
                (stage_manager(database).resources.scope,),
            )
        ).fetchone()
    return None if row is None or row[0] is None else str(row[0])


async def _wait_drain(database: Database, target: str) -> None:
    with anyio.fail_after(15):
        while await _drain_target(database) != target:
            await anyio.sleep(0.01)


async def _wait_offer(database: Database, identifier: str) -> None:
    with anyio.fail_after(15):
        while True:
            async with database._connections.reader() as connection:
                row = await (
                    await connection.execute(
                        "SELECT 1 FROM fair_offers WHERE work_item_id=? LIMIT 1", (identifier,)
                    )
                ).fetchone()
            if row is not None:
                return
            await anyio.sleep(0.01)


@asynccontextmanager
async def _cpu_holders(database: Database) -> AsyncGenerator[tuple[anyio.Event, ...]]:
    releases = tuple(anyio.Event() for _ in range(3))
    entered = tuple(anyio.Event() for _ in range(3))

    async def hold(index: int) -> None:
        async with stage_manager(database).resources.admit(ResourceRequest(name="cpu")):
            entered[index].set()
            await releases[index].wait()

    async with anyio.create_task_group() as tasks:
        try:
            for index in range(3):
                _ = tasks.start_soon(hold, index)
            with anyio.fail_after(10):
                for event in entered:
                    await event.wait()
            yield releases
        finally:
            for release in releases:
                release.set()


@pytest.mark.parametrize("cancel_target", (False, True))
async def test_cpu_drain_lends_no_partial_cpu_but_other_lane_and_cancellation_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancel_target: bool
) -> None:
    harness = _Harness()
    harness.install(monkeypatch, tmp_path)
    async with Database.managed(
        tmp_path / "carl.sqlite3",
        initialize=True,
        resource_capacities={"cpu": 4, "http": 1},
    ) as database:
        _ = await harness.enqueue(
            database, "large", _context("large", "large"), units=4, register=True
        )
        harness.clock.advance(31)
        small = harness.entered["small"] = anyio.Event()
        large = harness.entered["large"] = anyio.Event()
        http = harness.entered["http"] = anyio.Event()
        harness.gates["large"] = anyio.Event()
        harness.gates["small"] = anyio.Event()
        async with (
            _cpu_holders(database) as releases,
            work_runtime.managed_worker_pool(database, tmp_path, worker_count=4),
        ):
            await _wait_drain(database, "large")
            _ = await harness.enqueue(database, "small", _context("small", "small"), register=True)
            _ = await harness.enqueue(
                database, "http", _context("http", "http"), resource="http", register=True
            )
            with anyio.fail_after(15):
                await http.wait()
            await _wait_offer(database, "small")
            assert not small.is_set() and not large.is_set()
            occupancy = await database.resource_occupancy(stage_manager(database).resources.scope)
            assert occupancy.get("cpu") == 3  # No partial reservation for the four-unit target.
            releases[0].set()
            if cancel_target:
                _ = await database.cancel_work(
                    "large",
                    reason="fixture cancel",
                    caller="test",
                    requested_at_utc_ns=harness.clock(),
                )
                with anyio.fail_after(15):
                    await small.wait()
                assert not large.is_set()
                assert await _drain_target(database) is None
            else:
                # Even a second free CPU unit is not lent while the live aged
                # contender is waiting for its whole four-unit allocation.
                await _wait_offer(database, "small")
                assert not small.is_set()
                for release in releases:
                    release.set()
                with anyio.fail_after(15):
                    await large.wait()
                assert not small.is_set()
                harness.gates["large"].set()
                with anyio.fail_after(15):
                    await small.wait()


async def test_expired_held_guard_does_not_trap_free_cpu_behind_impossible_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness()
    harness.install(monkeypatch, tmp_path)
    path = tmp_path / "carl.sqlite3"
    inherited: int | None = None
    try:
        async with Database.managed(
            path, initialize=True, resource_capacities={"cpu": 4}
        ) as database:
            resources = stage_manager(database).resources
            token = str(uuid4())
            async with resources.execution_guard(token, owner="uncertain-child") as guard:
                lease = await database.try_acquire_resource_permit(
                    scope=resources.scope,
                    token=token,
                    owner="uncertain-child",
                    requests=(ResourceRequest(name="cpu"),),
                    capacities=resources.capacities,
                    now_utc_ns=time_ns() - 2_000_000_000,
                    lease_duration_ns=1_000_000_000,
                )
                assert lease is not None and guard.descriptor is not None
                inherited = os.dup(guard.descriptor)
            _ = await harness.enqueue(
                database, "large", _context("large", "large"), units=4, register=True
            )
            harness.clock.advance(31)
            small = harness.entered["small"] = anyio.Event()
            large = harness.entered["large"] = anyio.Event()
            harness.gates["small"] = anyio.Event()
            harness.gates["large"] = anyio.Event()
            _ = await harness.enqueue(database, "small", _context("small", "small"), register=True)
            async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=2):
                with anyio.fail_after(15):
                    await small.wait()
                assert not large.is_set()
                assert await _drain_target(database) is None
                occupancy = await database.resource_occupancy(resources.scope)
                assert occupancy["cpu"] == 2  # Live small phase plus the unknown guard.
            assert (await database.resource_occupancy(resources.scope))["cpu"] == 1

        # Restart cannot reclaim a lock merely because its database lease is
        # expired. A later physical exit allows recovery and the whole target.
        async with Database.managed(path, resource_capacities={"cpu": 4}) as database:
            resources = stage_manager(database).resources
            await resources._recover()
            assert (await database.resource_occupancy(resources.scope))["cpu"] == 1
            os.close(inherited)
            inherited = None
            await resources._recover()
            harness.clock.advance(6)
            harness.gates["small"].set()
            async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=2):
                with anyio.fail_after(15):
                    await large.wait()
                assert (await database.resource_occupancy(resources.scope))["cpu"] == 4
    finally:
        if inherited is not None:
            os.close(inherited)


async def test_runtime_restart_preserves_the_next_background_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness(pause_after=3)
    harness.install(monkeypatch, tmp_path)
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True, resource_capacities={"cpu": 1}) as database:
        for index in range(8):
            _ = await harness.enqueue(
                database,
                f"interactive-{index}",
                _context("interactive", "interactive-root", SchedulingServiceClass.INTERACTIVE),
            )
        for index in range(2):
            _ = await harness.enqueue(
                database, f"background-{index}", _context("background", "background-root")
            )
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            with anyio.fail_after(15):
                await harness.paused.wait()
            assert all(identifier.startswith("interactive") for identifier in harness.order)
        # The third executor stopped after admission. Its cancelled execution
        # cannot reset the persisted class turn when another runtime opens.
        occupancy = await database.resource_occupancy(stage_manager(database).resources.scope)
        assert occupancy.get("cpu", 0) == 0

    harness.pause_after = 4
    harness.paused = anyio.Event()
    async with (
        Database.managed(path, resource_capacities={"cpu": 1}) as database,
        work_runtime.managed_worker_pool(database, tmp_path, worker_count=1),
    ):
        with anyio.fail_after(15):
            await harness.paused.wait()
        assert harness.order[3].startswith("background")


async def test_foreground_owned_dataset_cannot_offer_a_phantom_cpu_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness()
    harness.install(monkeypatch, tmp_path)
    directories = CarlDirectories(
        config=tmp_path / "config",
        data=tmp_path,
        cache=tmp_path / "cache",
        state=tmp_path / "state",
        runtime=tmp_path / "runtime",
    )
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        engine = _Engine(
            database,
            directories,
            httpx.MockTransport(lambda _: pytest.fail("Blocked workflow must not download")),
        )

        def injected_engine(
            _directories: CarlDirectories, *, manager: TravelDataManager
        ) -> DatasetSetupEngine:
            assert isinstance(manager, TravelDataManager)
            return engine

        monkeypatch.setattr(work_runtime, "DatasetSetupEngine", injected_engine)
        original_prepare = prepare_dataset_setup

        async def frozen_native_phase(
            paths: CarlDirectories,
            request: TravelDataSetupRequest,
            *,
            binding: DatasetWorkflowBinding | None = None,
        ) -> DatasetSetupState:
            state = await original_prepare(
                paths,
                request,
                binding=binding,
                origin=TravelCoordinate(latitude=40, longitude=-75),
            )
            # The genuine CPU prepare publishes a durable four-thread phase.
            # Prior download/partition details are irrelevant: the foreign
            # physical owner must prevent this fixture engine from running.
            return state.model_copy(update={"phase": "osrm_build"})

        monkeypatch.setattr(dataset_setup_stages, "prepare_dataset_setup", frozen_native_phase)
        owner = _context("A-dataset", "dataset-root")
        work = travel_data_work(
            TravelDataSetupRequest(
                request_identifier="blocked-dataset", provider="osrm", threads=4, activate=False
            )
        )
        _ = await database.enqueue_work(
            work,
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("test", "dataset"),
                identifier=owner.root_request_identifier,
                context={},
                scheduling=owner,
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=harness.clock(),
        )
        declined = anyio.Event()
        original_submit = DatasetWorkflowService.submit

        async def observe_submit(
            service: DatasetWorkflowService,
            stage: StageDefinition,
            *,
            component: Component,
            definition: DatasetWorkflowDefinition,
            resume: bool = False,
        ) -> bool:
            result = await original_submit(
                service, stage, component=component, definition=definition, resume=resume
            )
            if stage.work_item_identifier == work.identifier:
                assert not result
                declined.set()
            return result

        monkeypatch.setattr(DatasetWorkflowService, "submit", observe_submit)
        with dataset_workflow_gate(directories):
            async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=2):
                with anyio.fail_after(15):
                    await declined.wait()
                admissions = await database.fair_admissions(
                    stage_manager(database).resources.scope, limit=10
                )
                assert len(admissions) == 1
                assert admissions[0].work_item_identifier == work.identifier
                assert admissions[0].lane.value == "cpu"  # Initial preparation is ordinary work.
                async with _cpu_holders(database):
                    harness.clock.advance(31)
                    declined = anyio.Event()
                    entered = harness.entered["unrelated"] = anyio.Event()
                    harness.gates["unrelated"] = anyio.Event()
                    _ = await harness.enqueue(
                        database, "unrelated", _context("Z-other", "other-root"), register=True
                    )
                    with anyio.fail_after(15):
                        await declined.wait()
                        await entered.wait()
                    async with database._connections.reader() as connection:
                        offers = await (
                            await connection.execute(
                                "SELECT lane FROM fair_offers WHERE work_item_id=?",
                                (work.identifier,),
                            )
                        ).fetchall()
                        running = await (
                            await connection.execute(
                                "SELECT count(*) FROM work_stages WHERE work_item_id=? AND state='running'",
                                (work.identifier,),
                            )
                        ).fetchone()
                    assert offers == [] and running == (0,)
                    assert await _drain_target(database) is None
                    assert engine.phases == []
                    assert (
                        await database.resource_occupancy(stage_manager(database).resources.scope)
                    )["cpu"] == 4  # Only the three holders and unrelated live executor.
