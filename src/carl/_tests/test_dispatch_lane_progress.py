"""Real production dispatch notices newly ready HTTP work during a CPU backlog."""

# Synthetic phases replace providers; discovery, admission and execution stay real.
# pyright: reportPrivateUsage=false

from dataclasses import dataclass
from pathlib import Path

import anyio
import pytest

from carl import work_runtime
from carl._tests.test_production_fairness import _context, _Harness, _Phase
from carl.core.stages import StageAttemptContext
from carl.core.worker import CompletedWork
from carl.io.sqlite import Database
from carl.io.stage_dispatcher import StageDispatcher
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@dataclass
class _SmallPages(StageDispatcher):
    page_size: int = 4


async def test_http_arriving_mid_pass_runs_before_cpu_backlog_drains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness()
    harness.install(monkeypatch, tmp_path)
    monkeypatch.setattr(work_runtime, "StageDispatcher", _SmallPages)
    owner = _context("shared-workspace", "shared-root")
    cpu_count = 48
    http_entered = anyio.Event()
    published_http = False
    original_phase = harness.phase

    async with Database.managed(
        tmp_path / "carl.sqlite3",
        initialize=True,
        resource_capacities={"cpu": 1, "http": 1, "analysis": 1},
    ) as database:

        async def phase(payload: _Phase, context: StageAttemptContext) -> CompletedWork:
            nonlocal published_http
            if not published_http:
                published_http = True
                # This enqueue occurs after the first pass captured its clock.
                # It must become visible in a fresh pass while CPU work remains.
                await harness.enqueue(database, "late-http", owner, resource="http", register=True)
                harness.clock.advance(2)
            outcome = await original_phase(payload, context)
            if payload.work_identifier == "late-http":
                http_entered.set()
            return outcome

        monkeypatch.setattr(harness, "phase", phase)
        for ordinal in range(cpu_count):
            await harness.enqueue(database, f"cpu-{ordinal:03d}", owner, register=True)
        scope = stage_manager(database).resources.scope
        await database.enable_fair_scheduling(scope, harness.clock())
        async with database._connections.writer() as connection:
            # Lane counters are independent; a busy historical CPU lane can
            # legitimately be far ahead of the HTTP lane in the same workspace.
            for table in ("fair_lanes", "fair_workspace_service", "fair_root_service"):
                await connection.execute(
                    f"UPDATE {table} SET sequence=CASE lane WHEN 'cpu' THEN 1000 "
                    "WHEN 'http' THEN 10 ELSE sequence END WHERE scope=?",
                    (scope,),
                )
        async with work_runtime.managed_worker_pool(database, tmp_path):
            with anyio.fail_after(20):
                await http_entered.wait()
            assert harness.order.index("late-http") < cpu_count, harness.order
            assert any(f"cpu-{ordinal:03d}" not in harness.order for ordinal in range(cpu_count))
