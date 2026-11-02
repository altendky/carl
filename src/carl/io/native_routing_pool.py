"""Root-owned warm routing engines, reserved independently from active CPU work."""

import fcntl
import os
import tempfile
import threading
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from time import monotonic, time_ns
from typing import BinaryIO, Literal, Protocol
from uuid import uuid4

import anyio

from carl.core.native_routing import (
    MAX_NATIVE_DESTINATIONS,
    NativeRoutingDataset,
    NativeRoutingMetric,
)
from carl.core.resources import ResourceLease, ResourceRequest
from carl.core.travel import TravelCoordinate
from carl.io.native_routing_protocol import (
    NativeRoutingBusy,
    NativeRoutingCleanupError,
    NativeRoutingProcess,
    NativeRoutingProtocolError,
)
from carl.io.resource_permits import (
    ResourceLeaseLost,
    ResourceManager,
    ResourcePermit,
    current_cpu_guard_descriptor,
    current_resource_manager,
    resource_operation,
)

DEFAULT_BACKEND_MODULE: str | None = None
DEFAULT_IDLE_TIMEOUT_SECONDS = 30.0
_SERVICE = "native_routing_pool"
_UNMANAGED_LOCK = threading.Lock()
_UNMANAGED_QUARANTINE: list[tuple["NativeProcess | None", BinaryIO, BinaryIO]] = []


class NativeProcess(Protocol):
    @property
    def pid(self) -> int: ...

    @property
    def worker_pid(self) -> int | None: ...

    async def query(
        self,
        origin: TravelCoordinate,
        destinations: tuple[TravelCoordinate, ...],
        *,
        maximum_snap_distance_meters: float,
        cpu_guard_fd: int,
        timeout_seconds: float = 60,
    ) -> tuple[NativeRoutingMetric, ...]: ...

    async def aclose(self) -> None: ...

    def is_closed(self) -> bool: ...


class NativeRoutingBorrowExpired(ResourceLeaseLost):
    """An idle owner retired before this borrow dispatched any native work."""


@dataclass(slots=True)
class _Query:
    origin: TravelCoordinate
    destinations: tuple[TravelCoordinate, ...]
    maximum_snap_distance_meters: float
    descriptor: int
    timeout_seconds: float
    done: anyio.Event = field(default_factory=anyio.Event)
    scope: anyio.CancelScope | None = None
    cancelled: bool = False
    result: tuple[NativeRoutingMetric, ...] | None = None
    failure: Literal["timeout", "unavailable", "cancelled"] | None = None


@dataclass(slots=True)
class _Engine:
    dataset: NativeRoutingDataset
    ready: anyio.Event = field(default_factory=anyio.Event)
    done: anyio.Event = field(default_factory=anyio.Event)
    wake: anyio.Event = field(default_factory=anyio.Event)
    admitted: bool = False
    closing: bool = False
    stop: bool = False
    quarantined: bool = False
    quarantine_descriptor: int | None = None
    token: str | None = None
    borrower: "NativeBorrow | None" = None
    command: _Query | None = None
    process: NativeProcess | None = None
    unknown_process: bool = False
    idle_since: float = field(default_factory=monotonic)
    failure: str | None = None


@dataclass(slots=True)
class NativeBorrow:
    """Exclusive engine use; closing this handle returns a warm engine to its owner."""

    pool: "NativeRoutingPool"
    _engine: _Engine
    _closed: bool = False
    _querying: bool = False

    @property
    def pid(self) -> int | None:
        return None if self._engine.process is None else self._engine.process.pid

    @property
    def worker_pid(self) -> int | None:
        return None if self._engine.process is None else self._engine.process.worker_pid

    def _require_active(self) -> None:
        if current_resource_manager() is not self.pool.manager:
            raise ResourceLeaseLost("Native routing borrow belongs to another resource manager")
        if self._closed or self.pool._closing:
            raise ResourceLeaseLost("Native routing borrow is no longer active")
        if self._engine.process is not None and self._engine.process.is_closed():
            self._engine.stop = self._engine.closing = True
            self._engine.wake.set()
        if self._engine.borrower is not self or self._engine.closing or self._engine.done.is_set():
            raise NativeRoutingBorrowExpired("Native routing owner expired before dispatch")

    async def __aenter__(self) -> "NativeBorrow":
        self._require_active()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        command = self._engine.command
        if command is not None:
            command.cancelled = True
            if command.scope is not None:
                command.scope.cancel()
            with anyio.CancelScope(shield=True):
                await command.done.wait()
        if self._engine.borrower is self:
            self._engine.borrower = None
            self._engine.wake.set()

    async def query(
        self,
        origin: TravelCoordinate,
        destinations: tuple[TravelCoordinate, ...],
        *,
        maximum_snap_distance_meters: float,
        timeout_seconds: float = 60,
    ) -> tuple[NativeRoutingMetric, ...]:
        self._require_active()
        if not 1 <= len(destinations) <= MAX_NATIVE_DESTINATIONS:
            raise ValueError("Native routing queries require one bounded batch")
        if self._querying:
            raise NativeRoutingBusy
        self._querying = True
        try:
            return await self._query(
                origin,
                destinations,
                maximum_snap_distance_meters=maximum_snap_distance_meters,
                timeout_seconds=timeout_seconds,
            )
        finally:
            self._querying = False

    @resource_operation("cpu")
    async def _query(
        self,
        origin: TravelCoordinate,
        destinations: tuple[TravelCoordinate, ...],
        *,
        maximum_snap_distance_meters: float,
        timeout_seconds: float,
    ) -> tuple[NativeRoutingMetric, ...]:
        self._require_active()
        token = self._engine.token
        if token is None or not await self.pool.manager.database.validate_resource_permit(
            token=token, owner=self.pool.manager.owner, now_utc_ns=time_ns()
        ):
            self._engine.stop = self._engine.closing = True
            self._engine.wake.set()
            raise NativeRoutingBorrowExpired("Native routing residency expired before dispatch")
        current_descriptor = await current_cpu_guard_descriptor()
        # The root idle timer may have retired this owner during either DB read.
        self._require_active()
        descriptor = os.dup(current_descriptor)
        command = _Query(
            origin, destinations, maximum_snap_distance_meters, descriptor, timeout_seconds
        )
        if self._engine.command is not None:
            os.close(descriptor)
            raise NativeRoutingBusy
        self._engine.command = command
        self._engine.wake.set()
        try:
            await command.done.wait()
        except BaseException:
            command.cancelled = True
            if command.scope is not None:
                command.scope.cancel()
            # The root owner and physical child retain this CPU guard until done.
            with anyio.fail_after(
                self.pool.startup_timeout_seconds
                + timeout_seconds
                + self.pool.cleanup_timeout_seconds
                + 5,
                shield=True,
            ):
                await command.done.wait()
            raise
        if command.failure == "timeout":
            raise TimeoutError("Native routing query timed out")
        if command.failure == "cancelled":
            raise ResourceLeaseLost("Native routing owner stopped its query")
        if command.failure is not None or command.result is None:
            raise NativeRoutingProtocolError
        return command.result


class NativeRoutingPool:
    """At most the configured number of root actors, with no task per waiter."""

    def __init__(
        self,
        manager: ResourceManager,
        *,
        backend_module: str | None = None,
        idle_timeout_seconds: float | None = None,
        startup_timeout_seconds: float = 10,
        cleanup_timeout_seconds: float = 10,
        poll_seconds: float = 0.05,
        process_factory: Callable[..., Awaitable[NativeProcess]] | None = None,
    ) -> None:
        self.manager = manager
        self.backend_module = DEFAULT_BACKEND_MODULE if backend_module is None else backend_module
        self.idle_timeout_seconds = (
            DEFAULT_IDLE_TIMEOUT_SECONDS if idle_timeout_seconds is None else idle_timeout_seconds
        )
        self.startup_timeout_seconds = startup_timeout_seconds
        self.cleanup_timeout_seconds = cleanup_timeout_seconds
        self.poll_seconds = poll_seconds
        if (
            min(
                self.idle_timeout_seconds,
                startup_timeout_seconds,
                cleanup_timeout_seconds,
                poll_seconds,
            )
            <= 0
        ):
            raise ValueError("Native routing lifecycle timeouts must be positive")
        self._process_factory = process_factory
        self._engines: list[_Engine] = []
        self._closing = False
        manager.register_closer(f"{_SERVICE}:{id(self)}", self.aclose)

    @property
    def owner_count(self) -> int:
        return len(self._engines)

    @property
    def quarantined_count(self) -> int:
        return sum(engine.quarantined for engine in self._engines)

    async def try_borrow(self, dataset: NativeRoutingDataset) -> NativeBorrow | None:
        if current_resource_manager() is not self.manager or self._closing:
            raise ResourceLeaseLost("Native routing pool belongs to another or closing runtime")
        for engine in self._engines:
            if engine.process is not None and engine.process.is_closed():
                engine.stop = engine.closing = True
                engine.wake.set()
            if (
                engine.dataset == dataset
                and engine.admitted
                and not engine.closing
                and engine.borrower is None
            ):
                borrow = NativeBorrow(self, engine)
                engine.borrower = borrow
                return borrow
        capacity = self.manager.capacities.get("routing_engines", 0)
        if len(self._engines) >= capacity:
            idle = next(
                (
                    engine
                    for engine in self._engines
                    if engine.admitted and engine.borrower is None and not engine.closing
                ),
                None,
            )
            if idle is not None:
                idle.stop = idle.closing = True
                idle.wake.set()
            return None
        engine = _Engine(dataset, idle_since=monotonic())
        self._engines.append(engine)
        try:
            self.manager.start_background(self._own, engine)
        except BaseException:
            self._engines.remove(engine)
            raise
        try:
            await engine.ready.wait()  # One database attempt, never a capacity wait.
        except BaseException:
            if engine.borrower is None:
                engine.stop = engine.closing = True
                engine.wake.set()
            raise
        if engine.failure is not None:
            # Permanent admission/configuration failures are not capacity waits.
            # Retain only the sanitized public error, not a database traceback.
            raise NativeRoutingProtocolError
        if (
            not engine.admitted
            or engine.closing
            or engine.done.is_set()
            or engine.borrower is not None
        ):
            return None
        borrow = NativeBorrow(self, engine)
        engine.borrower = borrow
        return borrow

    @asynccontextmanager
    async def borrow(self, dataset: NativeRoutingDataset) -> AsyncGenerator[NativeBorrow]:
        while (borrow := await self.try_borrow(dataset)) is None:
            await anyio.sleep(self.poll_seconds)
        async with borrow:
            yield borrow

    async def _renew(self, engine: _Engine, permit: ResourcePermit) -> None:
        while True:
            await anyio.sleep(self.manager.lease_duration_ns / 3_000_000_000)
            if engine.closing:
                return
            lease = await self.manager.database.renew_resource_permit(
                token=permit.lease.token,
                owner=permit.lease.owner,
                now_utc_ns=time_ns(),
                lease_duration_ns=self.manager.lease_duration_ns,
            )
            if engine.closing:
                return
            if lease is None:
                engine.failure = "ResourceLeaseLost"
                engine.stop = engine.closing = True
                if engine.command is not None and engine.command.scope is not None:
                    engine.command.scope.cancel()
                engine.wake.set()
                return
            permit.lease = lease

    async def _run_query(self, engine: _Engine, resident_descriptor: int, command: _Query) -> None:
        try:
            with anyio.CancelScope() as scope:
                command.scope = scope
                if command.cancelled:
                    scope.cancel()
                if engine.process is None:
                    factory = self._process_factory or NativeRoutingProcess.start
                    engine.process = await factory(
                        dataset=engine.dataset,
                        resident_guard_fd=resident_descriptor,
                        backend_module=self.backend_module,
                        startup_timeout_seconds=self.startup_timeout_seconds,
                        idle_timeout_seconds=self.idle_timeout_seconds,
                    )
                command.result = await engine.process.query(
                    command.origin,
                    command.destinations,
                    maximum_snap_distance_meters=command.maximum_snap_distance_meters,
                    cpu_guard_fd=command.descriptor,
                    timeout_seconds=command.timeout_seconds,
                )
                if engine.process.is_closed():
                    engine.stop = engine.closing = True
            if scope.cancel_called:
                command.failure = "cancelled"
                engine.stop = True
        except TimeoutError:
            command.failure = "timeout"
            engine.stop = True
        except NativeRoutingCleanupError as error:
            if error.process is not None:
                engine.process = error.process
            else:
                engine.unknown_process = True
            command.failure = "unavailable"
            engine.stop = True
        except BaseException as error:
            command.failure = (
                "cancelled" if isinstance(error, anyio.get_cancelled_exc_class()) else "unavailable"
            )
            engine.stop = True
        finally:
            # Never wake a caller whose stage may seal while our FD still pins it.
            os.close(command.descriptor)
            command.scope = None
            engine.command = None
            engine.idle_since = monotonic()
            command.done.set()

    async def _dispose(self, engine: _Engine) -> bool:
        process = engine.process
        if process is None:
            return not engine.unknown_process
        try:
            with anyio.fail_after(self.cleanup_timeout_seconds, shield=True):
                await process.aclose()
        except BaseException:
            if not process.is_closed():
                engine.quarantined = True
                return False
        return process.is_closed()

    async def _own(self, engine: _Engine) -> None:
        token, descriptor = str(uuid4()), None
        engine.token = token
        lease: ResourceLease | None = None
        try:
            await self.manager._recover()
            # Every lifetime scope is entered and exited in this root owner task.
            async with self.manager.execution_guard(token, owner=self.manager.owner) as guard:
                descriptor = guard.descriptor
                assert descriptor is not None
                guard.discard_unadmitted = True
                with anyio.CancelScope(shield=True):
                    lease = await self.manager.database.try_acquire_resource_permit(
                        scope=self.manager.scope,
                        token=token,
                        owner=self.manager.owner,
                        requests=(ResourceRequest(name="routing_engines"),),
                        capacities=self.manager.capacities,
                        now_utc_ns=time_ns(),
                        lease_duration_ns=self.manager.lease_duration_ns,
                    )
                if lease is None:
                    engine.ready.set()
                    return
                guard.discard_unadmitted = False
                async with (
                    self.manager.adopt(lease, guard) as permit,
                    anyio.create_task_group() as tasks,
                ):
                    _ = tasks.start_soon(self._renew, engine, permit)
                    engine.admitted = True
                    engine.ready.set()
                    try:
                        while not engine.stop:
                            command = engine.command
                            if command is not None:
                                await self._run_query(engine, descriptor, command)
                                continue
                            remaining = self.idle_timeout_seconds - (
                                monotonic() - engine.idle_since
                            )
                            if remaining <= 0:
                                break
                            wake = engine.wake
                            with anyio.move_on_after(remaining):
                                await wake.wait()
                            if wake is engine.wake:
                                engine.wake = anyio.Event()
                    finally:
                        engine.closing = True
                        with anyio.move_on_after(self.cleanup_timeout_seconds, shield=True):
                            while not await self._dispose(engine):
                                engine.quarantined = True
                                await anyio.sleep(self.poll_seconds)
                        if engine.unknown_process or (
                            engine.process is not None and not engine.process.is_closed()
                        ):
                            engine.quarantined = True
                            engine.quarantine_descriptor = os.dup(descriptor)
                        else:
                            engine.quarantined = False
                        tasks.cancel_scope.cancel()
        except BaseException as error:
            engine.failure = type(error).__name__
        finally:
            engine.closing = True
            if engine.command is not None:
                command = engine.command
                os.close(command.descriptor)
                command.failure = "unavailable"
                engine.command = None
                command.done.set()
            engine.ready.set()
            if not engine.quarantined and engine in self._engines:
                self._engines.remove(engine)
            engine.done.set()

    async def aclose(self) -> None:
        self._closing = True
        engines = tuple(self._engines)
        for engine in engines:
            engine.stop = engine.closing = True
            command = engine.command
            if command is not None:
                command.cancelled = True
                if command.scope is not None:
                    command.scope.cancel()
            engine.wake.set()
        try:
            with anyio.fail_after(self.cleanup_timeout_seconds + 1, shield=True):
                for engine in engines:
                    await engine.done.wait()
        except TimeoutError:
            raise NativeRoutingCleanupError from None
        if any(engine.quarantined for engine in engines):
            for engine in engines:
                if engine.quarantined and engine.process is not None and engine.process.is_closed():
                    if engine.quarantine_descriptor is not None:
                        os.close(engine.quarantine_descriptor)
                        engine.quarantine_descriptor = None
                    if engine.token is not None:
                        async with self.manager.execution_guard(
                            engine.token, owner=self.manager.owner
                        ):
                            pass
                    engine.quarantined = False
                    if engine in self._engines:
                        self._engines.remove(engine)
            if any(engine.quarantined for engine in engines):
                raise NativeRoutingCleanupError


def native_routing_pool(manager: ResourceManager) -> NativeRoutingPool:
    return manager.get_or_create_service(_SERVICE, lambda: NativeRoutingPool(manager))


async def close_unmanaged_native_routing() -> None:
    """Retry physical cleanup of the single fail-closed standalone owner."""
    for process, resident, cpu in tuple(_UNMANAGED_QUARANTINE):
        if process is None:
            raise NativeRoutingCleanupError
        try:
            with anyio.fail_after(10, shield=True):
                await process.aclose()
        except BaseException:
            if not process.is_closed():
                raise
        if not process.is_closed():
            raise NativeRoutingCleanupError
        resident.close()
        cpu.close()
        _UNMANAGED_QUARANTINE.remove((process, resident, cpu))
        _UNMANAGED_LOCK.release()


async def ephemeral_native_query(
    dataset: NativeRoutingDataset,
    origin: TravelCoordinate,
    destinations: tuple[TravelCoordinate, ...],
    *,
    maximum_snap_distance_meters: float,
    timeout_seconds: float = 60,
) -> tuple[NativeRoutingMetric, ...]:
    """Compatibility outside Database.managed: one ephemeral supervised child.

    Production managed runtimes use the shared durable pool. Standalone callers
    retain the same process/FD cleanup guarantees and a process-local limit of one.
    """
    if current_resource_manager() is not None:
        raise ResourceLeaseLost("Managed routing must use its durable owner pool")
    while not _UNMANAGED_LOCK.acquire(blocking=False):
        await anyio.sleep(0.05)
    resident: BinaryIO | None = None
    cpu: BinaryIO | None = None
    process: NativeProcess | None = None
    failed = False
    startup_uncertain = False
    try:
        # These files may transfer to fail-closed quarantine instead of closing.
        resident = tempfile.TemporaryFile()  # noqa: SIM115
        cpu = tempfile.TemporaryFile()  # noqa: SIM115
        fcntl.flock(resident.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(cpu.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        process = await NativeRoutingProcess.start(
            dataset=dataset,
            resident_guard_fd=resident.fileno(),
            backend_module=DEFAULT_BACKEND_MODULE,
            idle_timeout_seconds=DEFAULT_IDLE_TIMEOUT_SECONDS,
        )
        return await process.query(
            origin,
            destinations,
            maximum_snap_distance_meters=maximum_snap_distance_meters,
            cpu_guard_fd=cpu.fileno(),
            timeout_seconds=timeout_seconds,
        )
    except NativeRoutingCleanupError as error:
        process = error.process
        startup_uncertain = True
        failed = True
        raise
    except BaseException:
        failed = True
        raise
    finally:
        if process is None and startup_uncertain:
            assert resident is not None and cpu is not None
            _UNMANAGED_QUARANTINE.append((None, resident, cpu))
        if process is not None:
            assert resident is not None and cpu is not None
            try:
                with anyio.fail_after(10, shield=True):
                    await process.aclose()
            except BaseException:
                if not process.is_closed():
                    _UNMANAGED_QUARANTINE.append((process, resident, cpu))
                    if not failed:
                        raise NativeRoutingCleanupError from None
            if not process.is_closed():
                if not _UNMANAGED_QUARANTINE:
                    _UNMANAGED_QUARANTINE.append((process, resident, cpu))
                if not failed:
                    raise NativeRoutingCleanupError
        if (process is None and not startup_uncertain) or (
            process is not None and process.is_closed()
        ):
            if resident is not None:
                resident.close()
            if cpu is not None:
                cpu.close()
            _UNMANAGED_LOCK.release()
