"""Renewable resource admission shared by processes using one evidence database.

The ledger is authoritative for admission. A local flock guards physical execution
after lease expiry: a paused process must not silently turn one slot into two.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import shutil
import signal
import socket
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from time import time_ns
from typing import Protocol
from uuid import uuid4

import anyio
from anyio import sleep as resource_sleep

from carl.core.resources import ResourceLease, ResourceRequest


class ResourceDatabase(Protocol):
    """Resource ledger port; SQLite owns its schema and transactional semantics."""

    path: Path

    def transaction(self) -> AbstractAsyncContextManager[None]: ...

    async def try_acquire_resource_permit(
        self,
        *,
        scope: str,
        token: str,
        owner: str,
        requests: Sequence[ResourceRequest],
        capacities: dict[str, int],
        now_utc_ns: int,
        lease_duration_ns: int,
    ) -> ResourceLease | None: ...

    async def renew_resource_permit(
        self,
        *,
        token: str,
        owner: str,
        now_utc_ns: int,
        lease_duration_ns: int,
    ) -> ResourceLease | None: ...

    async def release_resource_permit(self, *, token: str, owner: str) -> bool: ...

    async def validate_resource_permit(
        self,
        *,
        token: str,
        owner: str,
        now_utc_ns: int,
    ) -> bool: ...

    async def reclaim_expired_resource_permit(
        self,
        *,
        token: str,
        owner: str,
        now_utc_ns: int,
    ) -> bool: ...

    async def expired_resource_permits(
        self,
        *,
        scope: str,
        now_utc_ns: int,
    ) -> tuple[ResourceLease, ...]: ...


DEFAULT_RESOURCE_CAPACITIES = {"http": 25, "cpu": 4, "analysis": 10}
_CURRENT_MANAGER: ContextVar[ResourceManager | None] = ContextVar("resource_manager", default=None)
_CURRENT_PERMITS: ContextVar[tuple[ResourcePermit, ...]] = ContextVar(
    "resource_permits", default=()
)


class ResourceLeaseLost(RuntimeError):
    """Execution no longer owns a current resource lease."""


def configured_capacities(overrides: Mapping[str, int] | None = None) -> dict[str, int]:
    capacities = dict(DEFAULT_RESOURCE_CAPACITIES)
    for name in capacities:
        value = os.environ.get(f"CARL_RESOURCE_{name.upper()}")
        if value is not None:
            capacities[name] = int(value)
    if overrides is not None:
        capacities.update(overrides)
    if any(not name or type(value) is not int or value < 1 for name, value in capacities.items()):
        raise ValueError("Resource capacities must be positive integers with nonempty names")
    return capacities


@dataclass(slots=True)
class ResourcePermit:
    manager: ResourceManager
    lease: ResourceLease
    guard_descriptor: int
    release_on_exit: bool = True

    @asynccontextmanager
    async def publishing(self) -> AsyncIterator[None]:
        """Fence evidence publication in the same transaction as its writes."""

        async with self.manager.database.transaction():
            if not await self.manager.database.validate_resource_permit(
                token=self.lease.token, owner=self.lease.owner, now_utc_ns=time_ns()
            ):
                raise ResourceLeaseLost("Cannot publish through an expired resource permit")
            yield
            if not await self.manager.database.validate_resource_permit(
                token=self.lease.token, owner=self.lease.owner, now_utc_ns=time_ns()
            ):
                raise ResourceLeaseLost("Resource permit expired before publication committed")


@dataclass(slots=True)
class ResourceManager:
    database: ResourceDatabase
    capacities: dict[str, int]
    scope: str
    guard_directory: Path
    owner: str
    lease_duration_ns: int = 60_000_000_000
    poll_seconds: float = 0.1

    def _guard(self, token: str) -> int:
        # Tokens are generated UUIDs, never supplied paths. Never unlink these
        # files: replacing a locked inode would create a second independent lock.
        if len(token) != 36 or any(c not in "0123456789abcdef-" for c in token):
            raise ValueError("Invalid resource guard token")
        self.guard_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        return os.open(
            self.guard_directory / f"{token}.lock", os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600
        )

    async def _recover(self) -> None:
        expired = await self.database.expired_resource_permits(
            scope=self.scope, now_utc_ns=time_ns()
        )
        for lease in expired:
            descriptor = self._guard(lease.token)
            try:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                await self.database.reclaim_expired_resource_permit(
                    token=lease.token, owner=lease.owner, now_utc_ns=time_ns()
                )
            finally:
                os.close(descriptor)

    async def _renew(self, permit: ResourcePermit) -> None:
        while True:
            await resource_sleep(self.lease_duration_ns / 3_000_000_000)
            lease = await self.database.renew_resource_permit(
                token=permit.lease.token,
                owner=permit.lease.owner,
                now_utc_ns=time_ns(),
                lease_duration_ns=self.lease_duration_ns,
            )
            if lease is None:
                raise ResourceLeaseLost("Resource permit expired or ownership changed")
            permit.lease = lease

    @asynccontextmanager
    async def admit(self, *requests: ResourceRequest) -> AsyncIterator[ResourcePermit]:
        """Reserve every requested budget together; hold no SQLite lock while waiting."""

        token = str(uuid4())
        descriptor = self._guard(token)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lease: ResourceLease | None = None
        permit: ResourcePermit | None = None
        try:
            while lease is None:
                await self._recover()
                # Receive ownership even if cancellation arrives during commit.
                with anyio.CancelScope(shield=True):
                    lease = await self.database.try_acquire_resource_permit(
                        scope=self.scope,
                        token=token,
                        owner=self.owner,
                        requests=requests,
                        capacities=self.capacities,
                        now_utc_ns=time_ns(),
                        lease_duration_ns=self.lease_duration_ns,
                    )
                if lease is None:
                    await resource_sleep(self.poll_seconds)
            permit = ResourcePermit(self, lease, descriptor)
            context_token = _CURRENT_PERMITS.set((*_CURRENT_PERMITS.get(), permit))
            try:
                body_error: BaseException | None = None
                async with anyio.create_task_group() as tasks:
                    tasks.start_soon(self._renew, permit)
                    try:
                        await anyio.lowlevel.checkpoint()
                        yield permit
                        if not await self.database.validate_resource_permit(
                            token=token, owner=self.owner, now_utc_ns=time_ns()
                        ):
                            raise ResourceLeaseLost("Resource permit expired during execution")
                    except BaseException as error:
                        body_error = error
                    finally:
                        tasks.cancel_scope.cancel()
                if body_error is not None:
                    raise body_error
            finally:
                _CURRENT_PERMITS.reset(context_token)
        finally:
            # Drop our reference first, then prove no inherited subprocess still
            # owns the original lock description. Even an unexpectedly killed
            # supervisor must not release a surviving child's reservation.
            os.close(descriptor)
            cleanup_descriptor: int | None = None
            try:
                if lease is not None and (permit is None or permit.release_on_exit):
                    cleanup_descriptor = self._guard(token)
                    try:
                        fcntl.flock(cleanup_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        pass  # Recover only after the remaining executor exits.
                    else:
                        with anyio.fail_after(40, shield=True):
                            await self.database.release_resource_permit(
                                token=token, owner=self.owner
                            )
            finally:
                if cleanup_descriptor is not None:
                    os.close(cleanup_descriptor)


@asynccontextmanager
async def resource_manager_context(
    database: ResourceDatabase, *, capacities: Mapping[str, int] | None = None
) -> AsyncIterator[ResourceManager]:
    """Bind root worker/CLI/MCP operations to their database's local budgets."""

    host = os.environ.get("CARL_RESOURCE_HOST", socket.gethostname())
    if not host.strip():
        raise ValueError("Resource host identity must be nonempty")
    scope = f"host:{host}"
    scope_hash = hashlib.sha256(scope.encode()).hexdigest()[:24]
    database_path = database.path.resolve()
    manager = ResourceManager(
        database=database,
        capacities=configured_capacities(capacities),
        scope=scope,
        guard_directory=database_path.parent
        / f"{database_path.name}.resource-permits"
        / scope_hash,
        owner=str(uuid4()),
    )
    context_token = _CURRENT_MANAGER.set(manager)
    try:
        yield manager
    finally:
        _CURRENT_MANAGER.reset(context_token)


def current_resource_manager() -> ResourceManager | None:
    return _CURRENT_MANAGER.get()


def resource_operation[**P, T](
    name: str,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Admit an adapter operation when invoked inside an evidence database scope."""

    def decorate(function: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @wraps(function)
        async def admitted(*args: P.args, **kwargs: P.kwargs) -> T:
            manager = _CURRENT_MANAGER.get()
            if manager is None:
                return await function(*args, **kwargs)
            async with manager.admit(ResourceRequest(name=name)):
                return await function(*args, **kwargs)

        return admitted

    return decorate


async def run_cpu[T](function: Callable[..., T], *args: object) -> T:
    """Keep capacity until the physical thread finishes, including cancellation."""

    manager = _CURRENT_MANAGER.get()
    if manager is None:
        return await anyio.to_thread.run_sync(function, *args, abandon_on_cancel=False)
    async with manager.admit(ResourceRequest(name="cpu")):
        return await anyio.to_thread.run_sync(function, *args, abandon_on_cancel=False)


def guarded_process_arguments(
    argv: tuple[str, ...], timeout_seconds: int, *, directory: Path | None = None
) -> tuple[tuple[str, ...], tuple[int, ...]]:
    """Retain analysis capacity in an owned supervisor if Carl itself dies."""

    permits = _CURRENT_PERMITS.get()
    if not permits:
        return argv, ()
    permit = permits[-1]
    if not any(request.name == "analysis" for request in permit.lease.allocations):
        return argv, ()
    executable = argv[0]
    if os.sep in executable and not Path(executable).is_absolute():
        executable = str((directory or Path.cwd()) / executable)
    if shutil.which(executable) is None:
        raise FileNotFoundError("Analysis executable is unavailable")
    descriptor = permit.guard_descriptor
    return (
        (
            sys.executable,
            "-m",
            "carl.io.analysis_supervisor",
            str(descriptor),
            str(timeout_seconds),
            *argv,
        ),
        (descriptor,),
    )


async def terminate_guarded_process(process: anyio.abc.Process, *, guarded: bool = False) -> None:
    """Kill the owned process group and reap before the permit can be released."""

    if process.returncode is None:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        with anyio.move_on_after(5, shield=True):
            await process.wait()
        if process.returncode is None:
            if guarded:
                # The supervisor still owns an execution guard. Leave the ledger
                # occupied until its independent deadline kills and reaps the child.
                for permit in _CURRENT_PERMITS.get():
                    if any(request.name == "analysis" for request in permit.lease.allocations):
                        permit.release_on_exit = False
                raise TimeoutError("Analysis supervisor has not completed child cleanup")
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
    await process.aclose()
