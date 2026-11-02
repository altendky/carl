"""Stage reservations remain bounded when inherited by adapter child tasks."""

# Explicit guard failures and ownership inspection exercise private I/O boundaries.
# pyright: reportPrivateUsage=false

import fcntl
import os
import sys
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from time import time_ns
from typing import override
from uuid import uuid4

import anyio
import httpx
import pytest

from carl.core.http import RequestPlan
from carl.core.resources import ResourceLease, ResourceRequest
from carl.io.httpx import DirectHttpxAcquirer
from carl.io.resource_permits import (
    ExecutionGuard,
    ResourceLeaseLost,
    ResourceManager,
    current_resource_manager,
    guarded_process_arguments,
    resource_operation,
    run_cpu,
)
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


def _manager() -> ResourceManager:
    manager = current_resource_manager()
    assert manager is not None
    manager.poll_seconds = 0.01
    return manager


@asynccontextmanager
async def _reservation(
    database: Database,
    *requests: ResourceRequest,
    now_utc_ns: int | None = None,
    duration_ns: int = 60_000_000_000,
) -> AsyncGenerator[tuple[ResourceManager, ResourceLease, ExecutionGuard]]:
    manager = _manager()
    token = str(uuid4())
    owner = "stage-worker"
    async with manager.execution_guard(token, owner=owner) as guard:
        lease = await database.try_acquire_resource_permit(
            scope=manager.scope,
            token=token,
            owner=owner,
            requests=requests,
            capacities=manager.capacities,
            now_utc_ns=time_ns() if now_utc_ns is None else now_utc_ns,
            lease_duration_ns=duration_ns,
        )
        assert lease is not None
        yield manager, lease, guard


async def test_adopted_cpu_fanout_cannot_exceed_reserved_units(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"cpu": 2}
    ) as database:
        lock = threading.Lock()
        release = threading.Event()
        started = 0
        active = 0
        maximum = 0

        def cpu() -> None:
            nonlocal started, active, maximum
            with lock:
                started += 1
                active += 1
                maximum = max(maximum, active)
            try:
                assert release.wait(5), "test did not release CPU threads"
            finally:
                with lock:
                    active -= 1

        async def invoke() -> None:
            await run_cpu(cpu)

        async with _reservation(database, ResourceRequest(name="cpu", units=2)) as (
            manager,
            lease,
            guard,
        ):
            with anyio.fail_after(4):
                async with manager.adopt(lease, guard), anyio.create_task_group() as tasks:
                    try:
                        for _ in range(7):
                            _ = tasks.start_soon(invoke)
                        while started < 2:
                            await anyio.sleep(0.01)
                        await anyio.sleep(0.05)
                        assert started == 2
                        assert maximum == 2
                    finally:
                        release.set()
            assert started == 7
            assert maximum == 2


@pytest.mark.parametrize("resource", ["http", "analysis"])
async def test_adopted_decorated_fanout_shares_reserved_units(
    tmp_path: Path, resource: str
) -> None:
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={resource: 2}
    ) as database:
        active = 0
        maximum = 0
        completed = 0

        @resource_operation(resource)
        async def operation() -> None:
            nonlocal active, maximum, completed
            active += 1
            maximum = max(maximum, active)
            try:
                await anyio.sleep(0.03)
            finally:
                active -= 1
            completed += 1

        async def invoke() -> None:
            await operation()

        async with _reservation(database, ResourceRequest(name=resource, units=2)) as (
            manager,
            lease,
            guard,
        ):
            with anyio.fail_after(3):
                async with manager.adopt(lease, guard), anyio.create_task_group() as tasks:
                    for _ in range(9):
                        _ = tasks.start_soon(invoke)
            assert maximum == 2
            assert completed == 9


class _BytesStream(httpx.AsyncByteStream):
    @override
    async def __aiter__(self):
        yield b"offline response"


async def test_http_adapter_children_borrow_stage_reservation_without_readmitting(
    tmp_path: Path,
) -> None:
    active = 0
    maximum = 0
    sent = 0
    release = anyio.Event()

    async def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum, sent
        active += 1
        sent += 1
        maximum = max(maximum, active)
        try:
            await release.wait()
            return httpx.Response(200, stream=_BytesStream())
        finally:
            active -= 1

    acquirer = DirectHttpxAcquirer(transport=httpx.MockTransport(respond))

    async def acquire() -> None:
        _ = await acquirer.acquire(RequestPlan(url="https://offline.test/"), lambda: str(uuid4()))

    async with (
        Database.managed(
            tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"http": 2}
        ) as database,
        _reservation(database, ResourceRequest(name="http", units=2)) as (
            manager,
            lease,
            guard,
        ),
    ):
        with anyio.fail_after(4):
            async with manager.adopt(lease, guard), anyio.create_task_group() as tasks:
                try:
                    for _ in range(5):
                        _ = tasks.start_soon(acquire)
                    while sent < 2:
                        await anyio.sleep(0.01)
                    await anyio.sleep(0.05)
                    assert sent == 2
                    assert maximum == 2
                    async with database._connections.reader() as connection:
                        cursor = await connection.execute(
                            "SELECT count(*) FROM resource_permits WHERE state = 'held'"
                        )
                        assert await cursor.fetchall() == [(1,)]
                finally:
                    release.set()
        assert sent == 5
        assert maximum == 2


async def test_same_task_nested_adapter_calls_reenter_one_reserved_unit(tmp_path: Path) -> None:
    calls: list[str] = []

    @resource_operation("http")
    async def inner() -> None:
        calls.append("inner")

    @resource_operation("http")
    async def outer() -> None:
        calls.append("outer")
        await inner()

    async with (
        Database.managed(
            tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"http": 1}
        ) as database,
        _reservation(database, ResourceRequest(name="http")) as (manager, lease, guard),
    ):
        with anyio.fail_after(2):
            async with manager.adopt(lease, guard):
                await outer()
        assert calls == ["outer", "inner"]


@pytest.mark.parametrize("identity", ["manager", "token", "owner", "scope"])
async def test_adoption_rejects_mismatched_guard_or_lease(tmp_path: Path, identity: str) -> None:
    async with (
        Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database,
        _reservation(database, ResourceRequest(name="cpu")) as (manager, lease, guard),
    ):
        if identity == "manager":
            manager = replace(manager)
        else:
            lease = lease.model_copy(update={identity: "wrong"})
        with pytest.raises(ValueError, match="must match"):
            async with manager.adopt(lease, guard):
                pytest.fail("mismatched reservation was adopted")


async def test_adoption_rejects_sealed_and_closed_guards(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        async with _reservation(database, ResourceRequest(name="cpu")) as (manager, lease, guard):
            assert guard.seal()
            with pytest.raises(ResourceLeaseLost, match="sealed"):
                async with manager.adopt(lease, guard):
                    pytest.fail("sealed guard was adopted")
        assert guard.closed
        with pytest.raises(ResourceLeaseLost, match="sealed"):
            async with manager.adopt(lease, guard):
                pytest.fail("closed guard was adopted")


async def test_concurrent_adopters_cannot_multiply_one_reservation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with (
        Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database,
        _reservation(database, ResourceRequest(name="cpu")) as (manager, lease, guard),
    ):
        validating = anyio.Event()
        release_validation = anyio.Event()
        finished = anyio.Event()
        validate = database.validate_resource_permit

        async def delayed_validation(*, token: str, owner: str, now_utc_ns: int) -> bool:
            validating.set()
            await release_validation.wait()
            return await validate(token=token, owner=owner, now_utc_ns=now_utc_ns)

        monkeypatch.setattr(database, "validate_resource_permit", delayed_validation)

        async def first_adopter() -> None:
            async with manager.adopt(lease, guard):
                pass
            finished.set()

        with anyio.fail_after(2):
            async with anyio.create_task_group() as tasks:
                try:
                    _ = tasks.start_soon(first_adopter)
                    await validating.wait()
                    # Ownership must be reserved before the validation await;
                    # otherwise both adopters could create independent semaphores.
                    with pytest.raises(ValueError, match="one active resource adopter"):
                        async with manager.adopt(lease, guard):
                            pytest.fail("concurrent adopter multiplied the reserved CPU unit")
                finally:
                    release_validation.set()
                await finished.wait()
        assert not guard.adopted
        async with manager.adopt(lease, guard):
            assert guard.adopted
        assert not guard.adopted


async def test_ended_adoption_keeps_active_children_bounded_and_rejects_queued_children(
    tmp_path: Path,
) -> None:
    started = threading.Event()
    release = threading.Event()
    queued_started = threading.Event()
    first_finished = anyio.Event()
    queued_finished = anyio.Event()

    def cpu() -> None:
        started.set()
        assert release.wait(5), "test did not release CPU thread"

    async def first() -> None:
        await run_cpu(cpu)
        first_finished.set()

    async def queued() -> None:
        with pytest.raises(ResourceLeaseLost, match="adoption ended"):
            await run_cpu(queued_started.set)
        queued_finished.set()

    async with (
        Database.managed(
            tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"cpu": 1}
        ) as database,
        _reservation(database, ResourceRequest(name="cpu")) as (manager, lease, guard),
    ):
        with anyio.fail_after(4):
            async with anyio.create_task_group() as tasks:
                try:
                    async with manager.adopt(lease, guard):
                        _ = tasks.start_soon(first)
                        while not started.is_set():
                            await anyio.sleep(0.01)
                        _ = tasks.start_soon(queued)
                        await anyio.sleep(0.05)
                        assert not queued_started.is_set()
                    # A fresh adopter must not create another local semaphore
                    # while a child from the first adoption still consumes a unit.
                    with pytest.raises(ValueError, match="one active resource adopter"):
                        async with manager.adopt(lease, guard):
                            pytest.fail("sequential adopter bypassed a surviving CPU borrower")
                finally:
                    release.set()
                await first_finished.wait()
                await queued_finished.wait()
        assert not queued_started.is_set()
        assert guard.active_borrowers == 0
        async with manager.adopt(lease, guard):
            await run_cpu(lambda: None)


async def test_adoption_rejects_an_expired_lease_before_cpu_execution(tmp_path: Path) -> None:
    ran = threading.Event()
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        async with _reservation(database, ResourceRequest(name="cpu"), now_utc_ns=1) as (
            manager,
            lease,
            guard,
        ):
            with pytest.raises(ResourceLeaseLost, match="expired"):
                async with manager.adopt(lease, guard):
                    await run_cpu(ran.set)
        assert not ran.is_set()


async def test_expired_adopted_context_cannot_start_an_adapter_operation(tmp_path: Path) -> None:
    ran = threading.Event()
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        async with (
            _reservation(database, ResourceRequest(name="cpu")) as (manager, lease, guard),
            manager.adopt(lease, guard),
        ):
            assert await database.release_resource_permit(token=lease.token, owner=lease.owner)
            with pytest.raises(ResourceLeaseLost, match="no longer owns"):
                await run_cpu(ran.set)
        assert not ran.is_set()


async def test_cancelling_adopted_operation_retains_cpu_until_thread_finishes(
    tmp_path: Path,
) -> None:
    started = threading.Event()
    release = threading.Event()
    finished = anyio.Event()
    scope = anyio.CancelScope()

    def cpu() -> None:
        started.set()
        assert release.wait(5), "test did not release CPU thread"

    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        async with _reservation(database, ResourceRequest(name="cpu")) as (manager, lease, guard):

            async def operation() -> None:
                with scope:
                    async with manager.adopt(lease, guard):
                        await run_cpu(cpu)
                finished.set()

            async with anyio.create_task_group() as tasks:
                try:
                    _ = tasks.start_soon(operation)
                    with anyio.fail_after(2):
                        while not started.is_set():
                            await anyio.sleep(0.01)
                    scope.cancel()
                    await anyio.sleep(0.05)
                    assert not finished.is_set()
                    with anyio.move_on_after(0.05) as blocked:
                        async with manager.admit(ResourceRequest(name="cpu")):
                            pytest.fail("cancelled adopted CPU execution released capacity early")
                    assert blocked.cancel_called
                finally:
                    release.set()
                with anyio.fail_after(2):
                    await finished.wait()
        with anyio.fail_after(2):
            async with manager.admit(ResourceRequest(name="cpu")):
                pass


async def test_nested_cpu_context_keeps_outer_analysis_guard_for_subprocess(tmp_path: Path) -> None:
    async with (
        Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database,
        _reservation(database, ResourceRequest(name="analysis")) as (
            manager,
            analysis,
            analysis_guard,
        ),
        manager.adopt(analysis, analysis_guard),
        _reservation(database, ResourceRequest(name="cpu")) as (_, cpu, cpu_guard),
        manager.adopt(cpu, cpu_guard),
    ):
        argv, descriptors = guarded_process_arguments((sys.executable, "-V"), 10)
        assert descriptors == (analysis_guard.descriptor,)
        assert descriptors != (cpu_guard.descriptor,)
        assert "carl.io.analysis_supervisor" in argv


@pytest.mark.parametrize("phase", ["acquire", "seal"])
async def test_execution_guard_closes_new_descriptor_when_flock_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True):
        manager = _manager()
        descriptor = os.open(tmp_path / "guard", os.O_CREAT | os.O_RDWR, 0o600)

        def guard_file(self: ResourceManager, token: str) -> int:
            del self, token
            return descriptor

        def fail_flock(descriptor: int, operation: int) -> None:
            del descriptor, operation
            raise OSError("synthetic flock failure")

        with monkeypatch.context() as patch:
            patch.setattr(ResourceManager, "_guard", guard_file)
            patch.setattr(fcntl, "flock", fail_flock)
            with pytest.raises(OSError, match="synthetic"):
                if phase == "acquire":
                    async with manager.execution_guard(str(uuid4()), owner="worker"):
                        pytest.fail("failed physical guard was yielded")
                else:
                    original = os.open(tmp_path / "original-guard", os.O_CREAT | os.O_RDWR, 0o600)
                    guard = ExecutionGuard(manager, str(uuid4()), "worker", original)
                    _ = guard.seal()
        with pytest.raises(OSError):
            _ = os.fstat(descriptor)
