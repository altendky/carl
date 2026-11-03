"""Runtime phase timing uses monotonic elapsed time, independently of UTC age."""

from dataclasses import replace
from pathlib import Path
from time import time_ns

import pytest

from carl._tests.test_stage_execution import _COMPONENT, _PROVENANCE, _Clock, _seed
from carl.core.worker import CompletedWork
from carl.io.sqlite import Database
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.mark.parametrize("clock_reversed", (False, True))
async def test_phase_runtime_and_running_age_have_distinct_clock_coverage(
    tmp_path: Path, clock_reversed: bool
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        stage = await _seed(database)
        wall = _Clock(time_ns() + 100_000_000)
        monotonic = _Clock(100)
        manager = replace(stage_manager(database), utc_now_ns=wall, monotonic_now_ns=monotonic)
        async with manager.admit(
            stage.identifier, component=_COMPONENT, provenance=_PROVENANCE, invocation={}
        ) as admission:
            assert admission.execution is not None
            wall.now += 5_000_000_000
            monotonic.now += -1 if clock_reversed else 42_000_000
            running = await database.work_scheduler_status(
                stage.work_item_identifier,
                coordination_scope=manager.resources.scope,
                now_utc_ns=wall.now,
            )
            assert running.stage_state == "running"
            assert running.execution_age_ns == 5_000_000_000
            assert running.execution_age_method == "utc_estimate"
            assert running.observed_blockers == ("running",)
            assert not running.blocked_resources
            await admission.execution.apply(CompletedWork(result={"saved": True}))
        snapshot = await database.scheduler_activity(
            coordination_scope=manager.resources.scope,
            now_utc_ns=wall.now,
        )
        assert all(resource.held_units == 0 for resource in snapshot.resources)
        (phase,) = tuple(item for item in snapshot.timings if item.metric == "phase_runtime")
        if clock_reversed:
            assert phase.count == 0 and phase.censored_count == 1
            assert phase.method == "unavailable"
        else:
            assert phase.method == "monotonic"
            assert phase.count == 1 and phase.total_ns == phase.maximum_ns == 42_000_000
            assert sum(phase.histogram_counts) == 1
