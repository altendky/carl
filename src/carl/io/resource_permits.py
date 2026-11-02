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
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from time import time_ns
from typing import Any, Protocol
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


DEFAULT_RESOURCE_CAPACITIES = {
    "http": 25,
    "cpu": 4,
    "analysis": 10,
    "resident_sessions": 32,
    "resident_state_bytes": 128 * 1024 * 1024,
}
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
    local_limiters: dict[str, anyio.Semaphore] = field(default_factory=dict)
    borrowers: dict[tuple[int, str], int] = field(default_factory=dict)
    execution_guard: ExecutionGuard | None = None
    closed: bool = False

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
class ExecutionGuard:
    """Own an execution token before atomic admission and through physical cleanup."""

    manager: ResourceManager
    token: str
    owner: str
    descriptor: int | None
    sealed: bool = False
    closed: bool = False
    closing: bool = False
    active_borrowers: int = 0
    adopted: bool = False
    discard_unadmitted: bool = False
    _idle: anyio.Event = field(default_factory=anyio.Event)

    def __post_init__(self) -> None:
        self._idle.set()

    def begin_borrow(self) -> None:
        if self.closing or self.sealed or self.closed:
            raise ResourceLeaseLost("Cannot execute after its stage guard began closing")
        if self.active_borrowers == 0:
            self._idle = anyio.Event()
        self.active_borrowers += 1

    def end_borrow(self) -> None:
        self.active_borrowers -= 1
        if self.active_borrowers == 0:
            self._idle.set()

    async def wait_idle(self) -> None:
        """Stop new local operations and retain capacity through actual cleanup."""

        self.closing = True
        with anyio.CancelScope(shield=True):
            await self._idle.wait()

    def seal(self) -> bool:
        """Stop launching children and prove all inherited executors have exited."""

        if self.closed:
            raise ResourceLeaseLost("Execution guard is closed")
        self.closing = True
        if self.active_borrowers:
            return False
        if self.sealed:
            return True
        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None
        descriptor = self.manager._guard(self.token)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            return False
        except BaseException:
            os.close(descriptor)
            raise
        self.descriptor = descriptor
        self.sealed = True
        return True

    async def aclose(self) -> None:
        if self.closed:
            return
        try:
            await self.wait_idle()
            if self.seal():
                with anyio.fail_after(40, shield=True):
                    await self.manager.database.release_resource_permit(
                        token=self.token, owner=self.owner
                    )
        finally:
            if self.descriptor is not None:
                os.close(self.descriptor)
                self.descriptor = None
            self.closed = True
            if self.discard_unadmitted and self.sealed:
                # No execution/token was committed or handed to an adapter.
                # Blocked nonblocking polls must not consume an inode each time.
                with suppress(FileNotFoundError):
                    (self.manager.guard_directory / f"{self.token}.lock").unlink()


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
        # Tokens are generated UUIDs, never supplied paths. Keep admitted-token
        # files: replacing a locked inode creates a second independent lock.
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
    async def execution_guard(self, token: str, *, owner: str) -> AsyncIterator[ExecutionGuard]:
        """Acquire the physical guard before a caller's atomic nonblocking admission."""

        descriptor = self._guard(token)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(descriptor)
            raise
        guard = ExecutionGuard(self, token, owner, descriptor)
        try:
            yield guard
        finally:
            await guard.aclose()

    @asynccontextmanager
    async def adopt(
        self, lease: ResourceLease, guard: ExecutionGuard
    ) -> AsyncIterator[ResourcePermit]:
        """Use an atomically admitted lease; its stage owner renews and finalizes it."""

        if (
            guard.manager is not self
            or lease.token != guard.token
            or lease.owner != guard.owner
            or lease.scope != self.scope
        ):
            raise ValueError("Adopted resource lease must match its execution guard and scope")
        if guard.sealed or guard.closing or guard.closed or guard.descriptor is None:
            raise ResourceLeaseLost("Cannot execute through a sealed execution guard")
        if guard.adopted or guard.active_borrowers:
            raise ValueError("An execution guard can have only one active resource adopter")
        guard.adopted = True
        try:
            if not await self.database.validate_resource_permit(
                token=lease.token, owner=lease.owner, now_utc_ns=time_ns()
            ):
                raise ResourceLeaseLost("Cannot adopt an expired resource permit")
            permit = ResourcePermit(self, lease, guard.descriptor, execution_guard=guard)
            context_token = _CURRENT_PERMITS.set((*_CURRENT_PERMITS.get(), permit))
            try:
                yield permit
            finally:
                permit.closed = True
                _CURRENT_PERMITS.reset(context_token)
        finally:
            guard.adopted = False

    @asynccontextmanager
    async def admit(self, *requests: ResourceRequest) -> AsyncIterator[ResourcePermit]:
        """Reserve every requested budget together; hold no SQLite lock while waiting."""

        token = str(uuid4())
        descriptor = self._guard(token)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(descriptor)
            raise
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


def _current_permit(
    manager: ResourceManager, name: str, *, include_reserved: bool = False
) -> ResourcePermit | None:
    borrower = (anyio.get_current_task().id, name)
    for permit in reversed(_CURRENT_PERMITS.get()):
        if (
            permit.manager is manager
            and any(request.name == name for request in permit.lease.allocations)
            and (
                include_reserved
                or permit.execution_guard is not None
                or borrower in permit.borrowers
            )
        ):
            return permit
    return None


@asynccontextmanager
async def _borrow_resource(
    manager: ResourceManager, name: str, *, include_reserved: bool = False
) -> AsyncIterator[bool]:
    permit = _current_permit(manager, name, include_reserved=include_reserved)
    if permit is None:
        yield False
        return
    if permit.closed:
        raise ResourceLeaseLost("Cannot execute after resource adoption ended")
    if permit.execution_guard is not None and (
        permit.execution_guard.sealed or permit.execution_guard.closed
    ):
        raise ResourceLeaseLost("Cannot execute after its stage guard was sealed")
    borrower = (anyio.get_current_task().id, name)
    if borrower in permit.borrowers:
        guard = permit.execution_guard
        if guard is not None:
            guard.begin_borrow()
        permit.borrowers[borrower] += 1
        try:
            if not await manager.database.validate_resource_permit(
                token=permit.lease.token, owner=permit.lease.owner, now_utc_ns=time_ns()
            ):
                raise ResourceLeaseLost("The current operation no longer owns its resource")
            yield True
        finally:
            permit.borrowers[borrower] -= 1
            if guard is not None:
                guard.end_borrow()
        return
    # Context variables are inherited by child tasks. Serialize their physical
    # operations within this reservation instead of letting each reuse one unit.
    limiter = permit.local_limiters.get(name)
    if limiter is None:
        units = next(request.units for request in permit.lease.allocations if request.name == name)
        limiter = anyio.Semaphore(units)
        permit.local_limiters[name] = limiter
    async with limiter:
        if permit.closed:
            raise ResourceLeaseLost("Cannot execute after resource adoption ended")
        guard = permit.execution_guard
        if guard is not None:
            guard.begin_borrow()
        permit.borrowers[borrower] = 1
        try:
            if not await manager.database.validate_resource_permit(
                token=permit.lease.token, owner=permit.lease.owner, now_utc_ns=time_ns()
            ):
                raise ResourceLeaseLost("The current operation no longer owns its resource")
            yield True
        finally:
            del permit.borrowers[borrower]
            if guard is not None:
                guard.end_borrow()


def resource_operation[**P, T](
    name: str,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Coroutine[Any, Any, T]]]:
    """Admit an adapter operation when invoked inside an evidence database scope."""

    def decorate(function: Callable[P, Awaitable[T]]) -> Callable[P, Coroutine[Any, Any, T]]:
        @wraps(function)
        async def admitted(*args: P.args, **kwargs: P.kwargs) -> T:
            manager = _CURRENT_MANAGER.get()
            if manager is None:
                return await function(*args, **kwargs)
            async with _borrow_resource(manager, name) as borrowed:
                if borrowed:
                    return await function(*args, **kwargs)
            async with (
                manager.admit(ResourceRequest(name=name)),
                _borrow_resource(manager, name, include_reserved=True),
            ):
                return await function(*args, **kwargs)

        return admitted

    return decorate


@resource_operation("cpu")
async def run_cpu[T](function: Callable[..., T], *args: object) -> T:
    """Keep capacity until the physical thread finishes, including cancellation."""

    return await anyio.to_thread.run_sync(function, *args, abandon_on_cancel=False)


def guarded_process_arguments(
    argv: tuple[str, ...], timeout_seconds: int, *, directory: Path | None = None
) -> tuple[tuple[str, ...], tuple[int, ...]]:
    """Retain analysis capacity in an owned supervisor if Carl itself dies."""

    manager = _CURRENT_MANAGER.get()
    permit = None if manager is None else _current_permit(manager, "analysis")
    if permit is None:
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
