"""Shipped whole-work ceilings yield to guarded stage resource budgets."""

# Inspect only the immutable retirement ledger; runtime checks use actual admission.
# pyright: reportPrivateUsage=false

from contextlib import AsyncExitStack
from datetime import UTC, datetime
from pathlib import Path
from time import time_ns

import pytest

from carl._tests.test_worker import _provenance
from carl.core.benchmark import BenchmarkConfig, BenchmarkScenario
from carl.core.components import Component, ComponentId
from carl.core.item_analysis import ANALYZE_ITEM_WORK_KIND, analysis_work_constraints
from carl.core.marketplace_images import (
    MARKETPLACE_IMAGE_SCOPE,
    marketplace_image_network_constraint,
)
from carl.core.resources import ResourceRequest
from carl.core.stages import StageDefinition, StageWaitKind, WaitWork
from carl.core.work import (
    NetworkActivityState,
    SchedulingScope,
    SchedulingScopeKind,
    WorkDefinition,
    WorkRequester,
)
from carl.core.worker import CompletedWork
from carl.io.network_activity import network_activity_definition
from carl.io.resource_permits import run_cpu
from carl.io.scheduler_benchmark import run_scheduler_benchmark
from carl.io.sqlite import Database
from carl.io.stages import stage_manager
from carl.work_runtime import prepare_worker_constraints

_COMPONENT = Component(ComponentId(("test", "capacity_retirement")), 1, lambda: None)


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _stage(database: Database, identifier: str, resource: str) -> StageDefinition:
    _ = await database.enqueue_work(
        WorkDefinition(
            identifier=identifier,
            kind=ANALYZE_ITEM_WORK_KIND,
            payload_schema_version=4,
            payload={"synthetic_admission_only": True},
            deduplication_identity=(identifier,),
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(
                    kind=SchedulingScopeKind.WORK_KIND, identity=ANALYZE_ITEM_WORK_KIND
                ),
            ),
        ),
        WorkRequester(
            request_identifier=f"request-{identifier}",
            kind=("test",),
            identifier=identifier,
            context={},
        ),
        event_identifier=f"enqueue-{identifier}",
        enqueued_at_utc_ns=time_ns(),
    )
    stage = StageDefinition(
        identifier=f"stage-{identifier}",
        work_item_identifier=identifier,
        operation_identifier=f"operation-{identifier}",
        kind=("test", "admission_only"),
        payload=None,
        resources=(ResourceRequest(name=resource),),
    )
    await database.register_stage(stage, created_at_utc_ns=time_ns())
    return stage


@pytest.mark.anyio
async def test_actual_managed_analysis_pressure_uses_configured_eleven_slots(
    tmp_path: Path,
) -> None:
    # Before retirement this exact fixture plateaued at ten real supervised
    # children with one free analysis slot and two free CPU units. Verify mode
    # does not release the children until all eleven have physically started.
    report = await run_scheduler_benchmark(
        BenchmarkConfig(
            scenario=BenchmarkScenario.ANALYSIS_PRESSURE,
            items=11,
            image_width=8,
            image_height=8,
            analysis_delay_ms=0,
            timeout_seconds=40,
            capacities={"cpu": 2, "http": 2, "analysis": 11},
        ),
        repository_root=Path(__file__).resolve().parents[3],
        work_parent=tmp_path,
    )
    result = report.iterations[0]
    assert result.completed == result.analysis_invocations == 11
    assert result.failed == result.cancelled == 0
    assert result.capture_invocations == 0
    assert result.sampled_peak_held_units["analysis"] == 11
    assert "real_supervised_analysis_capacity" in result.checks
    assert "physical_cleanup" in result.checks
    assert tuple(tmp_path.iterdir()) == ()


@pytest.mark.anyio
async def test_persisted_builtin_cap_retires_and_does_not_block_offline_cpu(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3",
        initialize=True,
        resource_capacities={"cpu": 2, "analysis": 11},
    ) as database:
        builtin = analysis_work_constraints()[0]
        await database.register_constraint(builtin, registered_at_utc_ns=1)
        await prepare_worker_constraints(database, tmp_path)
        await prepare_worker_constraints(database, tmp_path)
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    """SELECT c.definition_json,r.reason FROM scheduling_constraints c
                    JOIN scheduling_constraint_retirements r ON r.identifier_parts_json=c.identifier_parts_json"""
                )
            ).fetchall()
        assert len(rows) == 1
        assert builtin.model_dump_json() == rows[0][0]
        manager = stage_manager(database)
        initial = await _stage(database, "offline", "cpu")
        continuation = initial.model_copy(update={"identifier": "offline-resumed"})
        async with manager.admit(
            initial.identifier, component=_COMPONENT, provenance=_provenance(), invocation={}
        ) as prepared:
            assert prepared.execution is not None
            assert prepared.execution.lease.work_lease.attempt == 1
            await prepared.execution.apply(
                WaitWork(
                    continuation=continuation,
                    wait_kind=StageWaitKind.TIME,
                    result={"synthetic": "prepared"},
                )
            )
        async with AsyncExitStack() as held:
            for index in range(10):
                stage = await _stage(database, f"analysis-{index}", "analysis")
                admission = await held.enter_async_context(
                    manager.admit(
                        stage.identifier,
                        component=_COMPONENT,
                        provenance=_provenance(),
                        invocation={},
                    )
                )
                assert admission.execution is not None
            assert await database.resource_occupancy(manager.resources.scope) == {"analysis": 10}
            async with manager.admit(
                continuation.identifier,
                component=_COMPONENT,
                provenance=_provenance(),
                invocation={},
            ) as admission:
                assert admission.execution is not None
                assert admission.execution.lease.work_lease.attempt == 1
                assert (
                    admission.execution.lease.definition.operation_identifier
                    == initial.operation_identifier
                )
                assert await run_cpu(lambda: "offline-progress") == "offline-progress"
                assert await database.resource_occupancy(manager.resources.scope) == {
                    "analysis": 10,
                    "cpu": 1,
                }
                await admission.execution.apply(CompletedWork(result={"synthetic": "completed"}))
        assert await database.resource_occupancy(manager.resources.scope) == {}


@pytest.mark.anyio
async def test_custom_same_scope_cap_remains_authoritative(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3",
        initialize=True,
        resource_capacities={"cpu": 2, "analysis": 2},
    ) as database:
        builtin = analysis_work_constraints()[0]
        custom = builtin.model_copy(
            update={"identifier": ("user", "analysis", "serial"), "maximum_active": 1}
        )
        await database.register_constraint(builtin, registered_at_utc_ns=1)
        await database.register_constraint(custom, registered_at_utc_ns=1)
        await prepare_worker_constraints(database, tmp_path)
        manager = stage_manager(database)
        first = await _stage(database, "first", "analysis")
        second = await _stage(database, "second", "cpu")
        async with manager.admit(
            first.identifier, component=_COMPONENT, provenance=_provenance(), invocation={}
        ) as active:
            assert active.execution is not None
            async with manager.admit(
                second.identifier, component=_COMPONENT, provenance=_provenance(), invocation={}
            ) as declined:
                assert declined.execution is None
                assert declined.result.wait_kind == StageWaitKind.OWNERSHIP
            assert await database.resource_occupancy(manager.resources.scope) == {"analysis": 1}
            await active.execution.apply(CompletedWork(result={"synthetic": "completed"}))
        async with manager.admit(
            second.identifier, component=_COMPONENT, provenance=_provenance(), invocation={}
        ) as admitted:
            assert admitted.execution is not None
            await admitted.execution.apply(CompletedWork(result={"synthetic": "completed"}))
        assert await database.resource_occupancy(manager.resources.scope) == {}


@pytest.mark.anyio
@pytest.mark.parametrize("custom_cap", (False, True))
async def test_startup_retires_shared_network_ceiling_but_preserves_custom_policy(
    tmp_path: Path, custom_cap: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        builtin = marketplace_image_network_constraint()
        await database.register_constraint(builtin, registered_at_utc_ns=1)
        if custom_cap:
            await database.register_constraint(
                builtin.model_copy(
                    update={"identifier": ("user", "image", "serial"), "maximum_active": 1}
                ),
                registered_at_utc_ns=1,
            )
        # No image preparation is run, so this isolates capacity retirement
        # from the deliberately retained source-specific pacing policies.
        await prepare_worker_constraints(database, tmp_path)
        await database.begin_operation(
            operation_id="network-operation",
            component=_COMPONENT,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        admitted: list[str] = []
        for index in range(26):
            identifier = f"network-{index}"
            definition = network_activity_definition(
                identifier=identifier,
                kind=("test", "image"),
                operation_identifier="network-operation",
                network_session_identifier=f"session-{index}",
                network_path=("test", "offline"),
            )
            definition = definition.model_copy(
                update={"scopes": (*definition.scopes, MARKETPLACE_IMAGE_SCOPE)}
            )
            _ = await database.create_network_activity(
                definition,
                created_at_utc_ns=10,
                event_identifier=f"create-{index}",
                sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
            )
            admission = await database.try_admit_network_activity(
                network_activity_identifier=identifier,
                admission_token=f"token-{index}",
                permit_duration_ns=100,
                now_utc_ns=10,
                event_identifier=f"admit-{index}",
            )
            if custom_cap and index > 0:
                assert admission.admission is None
                assert admission.next_eligible_at_utc_ns == 110
            else:
                assert admission.admission is not None
                admitted.append(identifier)
        assert len(admitted) == (1 if custom_cap else 26)
        async with database._connections.reader() as connection:
            assert await (
                await connection.execute("SELECT count(*) FROM rate_starts")
            ).fetchall() == [(0,)]
        for identifier in admitted:
            index = identifier.removeprefix("network-")
            await database.finish_network_activity(
                network_activity_identifier=identifier,
                admission_token=f"token-{index}",
                state=NetworkActivityState.COMPLETED,
                ended_at_utc_ns=11,
                result={},
                event_identifier=f"finish-{index}",
            )
        async with database._connections.reader() as connection:
            assert await (
                await connection.execute(
                    "SELECT count(*) FROM network_activities WHERE state='admitted'"
                )
            ).fetchall() == [(0,)]
        await database.complete_operation(
            operation_id="network-operation",
            records=(),
            artifacts=(),
            outputs=(),
            result={"synthetic": "completed"},
            ended_at_utc=datetime.now(UTC).isoformat(),
            duration_ns=0,
        )
        manager = stage_manager(database)
        assert await database.resource_occupancy(manager.resources.scope) == {}
