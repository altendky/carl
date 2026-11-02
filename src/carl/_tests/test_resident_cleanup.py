"""Resident cleanup preserves AnyIO scope order and physical accounting."""

# Deliberate cleanup failure retains a guard; fake-client teardown releases it here.
# pyright: reportPrivateUsage=false

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from time import time_ns

import anyio
import pytest

from carl._tests.test_resident_sessions import (
    _COMPONENT,
    _PROVENANCE,
    _initial,
    _next,
    _Payload,
    _Session,
)
from carl._tests.test_resident_storage import _scalar
from carl.core.residents import ResidentState
from carl.core.resources import ResourceRequest
from carl.core.stages import StageAttemptContext, WaitWork
from carl.core.worker import CompletedWork
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.resource_permits import ResourceLeaseLost
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio
_CAPACITIES = {"http": 1, "cpu": 1, "resident_sessions": 1, "resident_state_bytes": 8}


@pytest.mark.parametrize("shielded", (False, True))
async def test_cancelled_provider_task_group_with_awaited_physical_cleanup(
    tmp_path: Path,
    shielded: bool,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities=_CAPACITIES
    ) as database:
        offset = [0]
        manager = replace(
            stage_manager(database),
            lease_duration_ns=300_000_000,
            renewal_interval_ns=25_000_000,
            utc_now_ns=lambda: time_ns() + offset[0],
        )
        pool = ResidentSessionPool(
            manager, StageHandlerRegistry((), ()), _PROVENANCE, {}, state_bytes=8, poll_seconds=0.01
        )
        session = _Session()
        running = anyio.Event()
        watcher_done = anyio.Event()
        identifiers: list[str] = []

        async def watcher() -> None:
            try:
                await anyio.sleep_forever()
            finally:
                with anyio.CancelScope(shield=True):
                    await anyio.lowlevel.checkpoint()
                    watcher_done.set()

        @asynccontextmanager
        async def factory(identifier: str) -> AsyncIterator[_Session]:
            identifiers.append(identifier)
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(watcher)
                try:
                    yield session
                finally:
                    if shielded:
                        # Shield inside the provider's own scope, before its TG exits.
                        with anyio.CancelScope(shield=True):
                            await anyio.sleep(0.01)
                            session.closed = True
                    else:
                        await anyio.lowlevel.checkpoint()
                        session.closed = True
                    tasks.cancel_scope.cancel()

        async def phase(
            payload: _Payload, context: StageAttemptContext
        ) -> WaitWork | CompletedWork:
            identifier = context.resident_session_identifier
            assert identifier is not None
            if payload.step == 0:
                _ = await pool.get_or_open(identifier, factory)
                return _next(context, step=1, resources=(ResourceRequest(name="cpu"),))
            running.set()
            await anyio.sleep_forever()
            raise AssertionError("Unreachable resident phase")

        pool.registry = StageHandlerRegistry(
            (), (TypedStageHandler(("test", "resident", "phase"), 1, _Payload, phase),)
        )
        initial = await _initial(database, resources=(ResourceRequest(name="http"),))
        async with pool.managed():
            assert await pool.submit(initial, component=_COMPONENT)
            with anyio.fail_after(3):
                await running.wait()
            _ = await database.cancel_work(
                initial.work_item_identifier,
                reason="test",
                caller="test",
                requested_at_utc_ns=time_ns(),
            )
            with anyio.fail_after(3):
                await pool.wait_idle()
            assert watcher_done.is_set()
            assert session.closed is shielded
            assert (await database.work(initial.work_item_identifier))["state"] == "cancelled"
            resident = await database.resident_session(identifiers[0])
            assert resident is not None and resident.active_stage_token is None
            assert all(isinstance(error, str) for error in pool.errors.values())
            if shielded:
                assert resident.state is ResidentState.CLOSED, pool.errors
                assert pool.active_count == 0
                async with manager.resources.admit(
                    ResourceRequest(name="http"),
                    ResourceRequest(name="cpu"),
                    ResourceRequest(name="resident_sessions"),
                    ResourceRequest(name="resident_state_bytes", units=8),
                ):
                    pass
            else:
                assert resident.state is ResidentState.QUARANTINED and pool.active_count == 1
                assert (
                    await _scalar(
                        database, "SELECT count(*) FROM resource_permits WHERE state='held'"
                    )
                    == 1
                )
                # Even after expiry, a retained resident guard prevents recovery.
                offset[0] = 10_000_000_000
                await pool.recover()
                current = await database.resident_session(identifiers[0])
                assert current is not None and current.state is ResidentState.QUARANTINED
                queued = await _initial(database, resources=(ResourceRequest(name="http"),))
                assert not await pool.submit(queued, component=_COMPONENT)
                assert pool.active_count == 1
        if not shielded:
            # This fake owns no physical client after its TG/watchers exited.
            # Explicit disposal avoids leaking the intentionally retained test FD.
            session.closed = True
            actor = pool._quarantined.pop(identifiers[0])
            actor.handle = None
            assert await database.finish_resident_session(
                actor.lease, state=ResidentState.CLOSED, now_utc_ns=manager.utc_now_ns()
            )
            os.close(actor.descriptor)


async def test_resident_lease_expiry_closes_provider_without_resetting_attempt(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities=_CAPACITIES
    ) as database:
        offset = [0]
        manager = replace(
            stage_manager(database),
            lease_duration_ns=300_000_000,
            renewal_interval_ns=25_000_000,
            utc_now_ns=lambda: time_ns() + offset[0],
        )
        pool = ResidentSessionPool(
            manager, StageHandlerRegistry((), ()), _PROVENANCE, {}, state_bytes=8, poll_seconds=0.01
        )
        session = _Session()
        running = anyio.Event()

        @asynccontextmanager
        async def factory(_identifier: str) -> AsyncIterator[_Session]:
            async with anyio.create_task_group() as tasks:
                try:
                    yield session
                finally:
                    with anyio.CancelScope(shield=True):
                        await anyio.lowlevel.checkpoint()
                        session.closed = True
                    tasks.cancel_scope.cancel()

        async def phase(
            payload: _Payload, context: StageAttemptContext
        ) -> WaitWork | CompletedWork:
            identifier = context.resident_session_identifier
            assert identifier is not None
            if payload.step == 0:
                _ = await pool.get_or_open(identifier, factory)
                return _next(context, step=1, resources=(ResourceRequest(name="cpu"),))
            running.set()
            await anyio.sleep_forever()
            raise AssertionError("Unreachable resident phase")

        pool.registry = StageHandlerRegistry(
            (), (TypedStageHandler(("test", "resident", "phase"), 1, _Payload, phase),)
        )
        initial = await _initial(database, resources=(ResourceRequest(name="http"),))
        async with pool.managed():
            assert await pool.submit(initial, component=_COMPONENT)
            with anyio.fail_after(3):
                await running.wait()
            offset[0] = 10_000_000_000
            with anyio.fail_after(3):
                await pool.wait_idle()
            assert session.closed and pool.active_count == 0
            work = await database.work(initial.work_item_identifier)
            assert work["attempt"] == 1 and work["state"] == "leased"
            assert len(work["operations"]) == 1
            assert "ResourceLeaseLost" in pool.errors.values()
            assert (
                await _scalar(database, "SELECT count(*) FROM resource_permits WHERE state='held'")
                == 0
            )


async def test_flock_failure_closes_unadmitted_descriptor_and_guard_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities=_CAPACITIES
    ) as database:
        manager = stage_manager(database)
        pool = ResidentSessionPool(
            manager, StageHandlerRegistry((), ()), _PROVENANCE, {}, state_bytes=8
        )
        initial = await _initial(database, resources=())
        async with pool.managed():
            await pool.recover()
            guards = set(manager.resources.guard_directory.glob("*.lock"))
            opened: list[int] = []
            manager_type = type(manager.resources)
            original_guard = manager_type._guard

            def record_guard(resources, token: str) -> int:
                descriptor = original_guard(resources, token)
                opened.append(descriptor)
                return descriptor

            monkeypatch.setattr(manager_type, "_guard", record_guard)

            def fail_flock(*_args: object) -> None:
                raise OSError("offline lock failure")

            monkeypatch.setattr("carl.io.resident_sessions.fcntl.flock", fail_flock)
            for _ in range(3):
                with pytest.raises(OSError, match="offline lock failure"):
                    await pool.submit(initial, component=_COMPONENT)
                with pytest.raises(OSError):
                    os.fstat(opened[-1])
            assert len(opened) == 3
            assert set(manager.resources.guard_directory.glob("*.lock")) == guards
            assert pool.active_count == 0


async def test_failed_stage_binding_rolls_back_ledger_and_unlinks_unadmitted_guards(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities=_CAPACITIES
    ) as database:
        manager = stage_manager(database)
        pool = ResidentSessionPool(
            manager, StageHandlerRegistry((), ()), _PROVENANCE, {}, state_bytes=8
        )
        initial = await _initial(database, resources=())
        await database.register_stage(initial, created_at_utc_ns=time_ns())
        async with pool.managed():
            guards = set(manager.resources.guard_directory.glob("*.lock"))
            for _ in range(3):
                with pytest.raises(ResourceLeaseLost, match="transfer resident ownership"):
                    await pool.submit(initial, component=_COMPONENT)
            assert pool.active_count == 0
            assert set(manager.resources.guard_directory.glob("*.lock")) == guards
            assert await _scalar(database, "SELECT count(*) FROM resident_sessions") == 0
            assert await _scalar(database, "SELECT count(*) FROM resource_permits") == 0
            assert (await database.work(initial.work_item_identifier))["attempt"] == 0
