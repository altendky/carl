"""Root resource owners outlive callers without inheriting their execution permits."""

# Inspect context isolation and durable fences at their actual ownership boundary.
# pyright: reportPrivateUsage=false

import os
from pathlib import Path
from uuid import uuid4

import anyio
import pytest

import carl.io.resource_permits as resources
from carl._tests.test_stage_execution import _admit, _seed
from carl.core.resources import ResourceRequest
from carl.core.worker import CompletedWork
from carl.io.resource_permits import (
    ResourceLeaseLost,
    ResourceManager,
    configured_capacities,
    current_cpu_guard_descriptor,
    current_resource_manager,
    resource_operation,
)
from carl.io.sqlite import Database
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _manager() -> ResourceManager:
    manager = current_resource_manager()
    assert manager is not None
    return manager


def test_routing_engine_capacity_supports_existing_environment_and_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CARL_RESOURCE_ROUTING_ENGINES", raising=False)
    assert configured_capacities()["routing_engines"] == 1
    monkeypatch.setenv("CARL_RESOURCE_ROUTING_ENGINES", "3")
    assert configured_capacities()["routing_engines"] == 3
    assert configured_capacities({"routing_engines": 2})["routing_engines"] == 2


async def test_services_are_keyed_per_manager_and_nested_scopes_restore_outer_manager(
    tmp_path: Path,
) -> None:
    closed: list[str] = []
    async with Database.managed(tmp_path / "outer.sqlite", initialize=True):
        outer = _manager()
        service = outer.get_or_create_service("native", object)
        assert outer.get_or_create_service("native", object) is service

        async def close_outer() -> None:
            assert current_resource_manager() is outer
            closed.append("outer")

        outer.register_closer("native", close_outer)
        outer.register_closer("native", close_outer)
        async with Database.managed(tmp_path / "inner.sqlite", initialize=True):
            inner = _manager()
            assert inner is not outer
            assert inner.get_or_create_service("native", object) is not service

            async def close_inner() -> None:
                assert current_resource_manager() is inner
                closed.append("inner")

            inner.register_closer("native", close_inner)
            bound = anyio.Event()

            async def outer_task() -> None:
                assert current_resource_manager() is outer
                bound.set()

            outer.start_background(outer_task)
            await bound.wait()
        assert current_resource_manager() is outer
        assert closed == ["inner"]
    assert closed == ["inner", "outer"]
    assert current_resource_manager() is None
    with pytest.raises(RuntimeError, match="active manager lifecycle"):
        outer.get_or_create_service("late", object)
    with pytest.raises(RuntimeError, match="active manager lifecycle"):
        outer.register_closer("late", close_outer)
    with pytest.raises(RuntimeError, match="active manager lifecycle"):
        outer.start_background(close_outer)


async def test_shutdown_closes_services_with_database_open_before_stopping_background_tasks(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    started = anyio.Event()
    async with Database.managed(tmp_path / "carl.sqlite", initialize=True) as database:
        manager = _manager()

        async def owner() -> None:
            assert current_resource_manager() is manager
            # These scopes belong wholly to the root task, including cleanup.
            async with anyio.create_task_group():
                started.set()
                try:
                    await anyio.sleep_forever()
                finally:
                    events.append("background_stopped")

        async def close() -> None:
            assert current_resource_manager() is manager
            await database.validate_schema()
            await anyio.sleep(0)
            assert events == []
            events.append("closed")
            with pytest.raises(RuntimeError, match="active manager lifecycle"):
                manager.start_background(owner)

        manager.register_closer("owner", close)
        manager.start_background(owner)
        await started.wait()
    assert events == ["closed", "background_stopped"]


async def test_body_exception_stays_bare_and_preserves_identity(tmp_path: Path) -> None:
    failure = ValueError("body failure")
    closed = False
    with pytest.raises(ValueError) as raised:
        async with Database.managed(tmp_path / "carl.sqlite", initialize=True):

            async def close() -> None:
                nonlocal closed
                await anyio.sleep(0)
                closed = True

            _manager().register_closer("owner", close)
            raise failure
    assert raised.value is failure and closed


async def test_cleanup_errors_do_not_skip_other_closers_or_hide_body_failure(
    tmp_path: Path,
) -> None:
    body = ValueError("body")
    cleanup = OSError("cleanup")
    closed: list[str] = []
    with pytest.raises(ExceptionGroup) as raised:
        async with Database.managed(tmp_path / "carl.sqlite", initialize=True):

            async def first() -> None:
                closed.append("first")

            async def second() -> None:
                closed.append("second")
                raise cleanup

            _manager().register_closer("first", first)
            _manager().register_closer("second", second)
            raise body
    assert raised.value.exceptions == (body, cleanup)
    assert closed == ["second", "first"]


async def test_cancellation_still_awaits_service_cleanup_without_exception_group(
    tmp_path: Path,
) -> None:
    closed = False
    with anyio.CancelScope() as scope:
        async with Database.managed(tmp_path / "carl.sqlite", initialize=True):

            async def close() -> None:
                nonlocal closed
                await anyio.sleep(0.01)
                closed = True

            _manager().register_closer("owner", close)
            scope.cancel()
            await anyio.sleep_forever()
    assert scope.cancel_called and closed


async def test_root_background_failure_closes_services_and_preserves_original_error(
    tmp_path: Path,
) -> None:
    failure = ValueError("background failure")
    closed = False
    with pytest.raises(ExceptionGroup) as raised:
        async with Database.managed(tmp_path / "carl.sqlite", initialize=True):

            async def close() -> None:
                nonlocal closed
                closed = True

            async def owner() -> None:
                raise failure

            _manager().register_closer("owner", close)
            _manager().start_background(owner)
            await anyio.sleep_forever()
    assert raised.value.exceptions == (failure,)
    assert closed


async def test_background_owner_clears_inherited_stage_permits_before_reusing_cpu(
    tmp_path: Path,
) -> None:
    started, resume, finished = anyio.Event(), anyio.Event(), anyio.Event()
    async with Database.managed(
        tmp_path / "carl.sqlite", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        manager = _manager()
        stage = await _seed(database)

        @resource_operation("cpu")
        async def new_cpu_operation() -> None:
            descriptor = await current_cpu_guard_descriptor()
            assert os.fstat(descriptor).st_ino > 0

        async def owner() -> None:
            assert current_resource_manager() is manager
            assert resources._CURRENT_PERMITS.get() == ()
            started.set()
            await resume.wait()
            with pytest.raises(ResourceLeaseLost, match="active task CPU borrow"):
                await current_cpu_guard_descriptor()
            await new_cpu_operation()
            finished.set()

        @resource_operation("cpu")
        async def start_from_stage() -> None:
            assert resources._CURRENT_PERMITS.get()
            assert await current_cpu_guard_descriptor() >= 0
            manager.start_background(owner)
            await started.wait()

        async with _admit(stage_manager(database), stage) as admission:
            assert admission.execution is not None
            await start_from_stage()
            await admission.execution.finish(CompletedWork(result={}))
        resume.set()
        with anyio.fail_after(2):
            await finished.wait()


async def test_cpu_guard_requires_actual_task_borrow_and_live_ownership(tmp_path: Path) -> None:
    with pytest.raises(ResourceLeaseLost, match="active task CPU borrow"):
        await current_cpu_guard_descriptor()
    async with Database.managed(tmp_path / "carl.sqlite", initialize=True) as database:
        manager = _manager()
        with pytest.raises(ResourceLeaseLost, match="expired during execution"):
            async with manager.admit(ResourceRequest(name="cpu")) as permit:
                with pytest.raises(ResourceLeaseLost, match="active task CPU borrow"):
                    await current_cpu_guard_descriptor()
                async with resources._borrow_resource(manager, "cpu", include_reserved=True):
                    assert await current_cpu_guard_descriptor() == permit.guard_descriptor

                    async def child() -> None:
                        with pytest.raises(ResourceLeaseLost, match="active task CPU borrow"):
                            await current_cpu_guard_descriptor()

                    async with anyio.create_task_group() as tasks:
                        tasks.start_soon(child)
                    assert await database.release_resource_permit(
                        token=permit.lease.token, owner=permit.lease.owner
                    )
                    with pytest.raises(ResourceLeaseLost, match="expired or changed"):
                        await current_cpu_guard_descriptor()


async def test_cpu_guard_rejects_a_stage_guard_after_sealing_begins(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite", initialize=True) as database:
        stage = await _seed(database)
        async with _admit(stage_manager(database), stage) as admission:
            execution = admission.execution
            assert execution is not None

            @resource_operation("cpu")
            async def cpu() -> None:
                assert await current_cpu_guard_descriptor() >= 0
                assert not execution.guard.seal()  # An active borrower keeps capacity occupied.
                with pytest.raises(ResourceLeaseLost, match="closing or sealed"):
                    await current_cpu_guard_descriptor()

            await cpu()


async def test_manually_constructed_manager_keeps_ordinary_admission_compatibility(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite", initialize=True) as database:
        root = _manager()
        manual = ResourceManager(
            database=database,
            capacities=root.capacities,
            scope=root.scope,
            guard_directory=root.guard_directory,
            owner=str(uuid4()),
        )
        with pytest.raises(RuntimeError, match="active manager lifecycle"):
            manual.get_or_create_service("native", object)
        async with manual.admit(ResourceRequest(name="routing_engines")) as permit:
            assert await database.validate_resource_permit(
                token=permit.lease.token, owner=manual.owner, now_utc_ns=0
            )
        assert not await database.validate_resource_permit(
            token=permit.lease.token, owner=manual.owner, now_utc_ns=0
        )
