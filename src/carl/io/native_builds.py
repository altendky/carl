"""Weighted, fenced native commands with immutable completion replay."""

import fcntl
import hashlib
import json
import math
import os
import stat
from dataclasses import dataclass, field
from pathlib import Path

import anyio

from carl.core.native_build import NativeBuildResult, NativeBuildSpec
from carl.io.native_build_protocol import NativeBuildCleanupError, NativeBuildProcess
from carl.io.native_build_receipts import (
    NativeBuildReceipt,
    NativeBuildReceiptError,
    open_receipt_directory,
    read_build_receipt,
    verify_build_logs,
)
from carl.io.native_build_receipts import (
    NativeBuildUncertain as NativeBuildUncertain,
)
from carl.io.resource_permits import (
    ResourceManager,
    cpu_reservation,
    invalidate_cpu_reservation,
    run_cpu,
    validate_cpu_reservation,
)


class NativeBuildBusy(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Native build phase is already owned")


@dataclass(frozen=True, slots=True)
class BuildCommandCapture:
    result: NativeBuildResult
    receipt: NativeBuildReceipt
    replayed: bool


@dataclass(slots=True)
class _Quarantine:
    manager: ResourceManager
    process: NativeBuildProcess | None
    descriptors: tuple[int, ...]
    owners: "_Owners"


_QUARANTINE: list[_Quarantine] = []


def _private_guard(descriptor: int) -> None:
    if type(descriptor) is not int or descriptor < 0:
        raise NativeBuildReceiptError
    info = os.fstat(descriptor)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise NativeBuildReceiptError


def _require_workflow_lock(descriptor: int) -> None:
    _private_guard(descriptor)
    # A separate open-file description is essential: flock on the caller's
    # existing description succeeds even when a different task is using it.
    independent = os.open(f"/proc/self/fd/{descriptor}", os.O_RDONLY | os.O_CLOEXEC)
    try:
        try:
            fcntl.flock(independent, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise NativeBuildReceiptError from None
            return
        raise NativeBuildReceiptError
    finally:
        os.close(independent)


def _workflow_path(descriptor: int) -> Path:
    path = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
    if not path.is_absolute():
        raise NativeBuildReceiptError
    parent = open_receipt_directory(path.parent)
    try:
        named = os.open(
            path.name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
        )
        try:
            actual, expected = os.fstat(named), os.fstat(descriptor)
            _private_guard(named)
            if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
                raise NativeBuildReceiptError
        finally:
            os.close(named)
    finally:
        os.close(parent)
    return path


def _phase_lock(spec: NativeBuildSpec, workflow: int) -> int:
    path = _workflow_path(workflow)
    directory = open_receipt_directory(path.parent / ".native-build-phase-locks", create=True)
    name = (
        hashlib.sha256(
            json.dumps(
                (spec.recipe_identifier, spec.phase_identifier), separators=(",", ":")
            ).encode()
        ).hexdigest()
        + ".lock"
    )
    try:
        descriptor = os.open(
            name,
            os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            0o600,
            dir_fd=directory,
        )
        try:
            _private_guard(descriptor)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise NativeBuildBusy from None
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise
    finally:
        os.close(directory)


def _known_closed(process: NativeBuildProcess | None) -> bool:
    try:
        return process is not None and process.is_closed()
    except Exception:
        return False


async def _close_quarantine(manager: ResourceManager) -> None:
    """Release retained guards only after both command and supervisor are gone."""
    failed = False
    for owner in tuple(_QUARANTINE):
        if owner.manager is not manager:
            continue
        if owner.process is None:
            failed = True
            continue
        try:
            with anyio.fail_after(10, shield=True):
                await owner.process.aclose()
        except BaseException:
            if not _known_closed(owner.process):
                failed = True
                continue
        if not _known_closed(owner.process):
            failed = True
            continue
        for descriptor in owner.descriptors:
            os.close(descriptor)
        _QUARANTINE.remove(owner)
    if failed:
        raise NativeBuildCleanupError


def _duplicate(descriptors: tuple[int, ...]) -> tuple[int, ...]:
    copies: list[int] = []
    try:
        for descriptor in descriptors:
            copies.append(os.dup(descriptor))
    except BaseException:
        for descriptor in copies:
            os.close(descriptor)
        raise
    return tuple(copies)


@dataclass(slots=True)
class _Owners:
    manager: ResourceManager
    lock: anyio.Lock = field(default_factory=anyio.Lock)
    wake: anyio.Event = field(default_factory=anyio.Event)
    done: anyio.Event = field(default_factory=anyio.Event)
    stopping: bool = False

    async def reconcile(self) -> None:
        try:
            while not self.stopping:
                pending = False
                async with self.lock:
                    for owner in tuple(_QUARANTINE):
                        if self.stopping:
                            break
                        if owner.manager is not self.manager:
                            continue
                        pending = True
                        process = owner.process
                        if not _known_closed(process) or process is None:
                            continue
                        # Polling never initiates slow cleanup of an unproven
                        # subtree. Once proven, close its remaining local handles.
                        try:
                            with anyio.fail_after(1, shield=True):
                                await process.aclose()
                        except BaseException:
                            if not _known_closed(process):
                                continue
                        if not _known_closed(process):
                            continue
                        for descriptor in owner.descriptors:
                            os.close(descriptor)
                        _QUARANTINE.remove(owner)
                wake = self.wake
                if pending:
                    with anyio.move_on_after(0.1):
                        await wake.wait()
                else:
                    await wake.wait()
                if self.wake is wake:
                    self.wake = anyio.Event()
        finally:
            self.done.set()

    async def aclose(self) -> None:
        self.stopping = True
        self.wake.set()
        with anyio.fail_after(2, shield=True):
            await self.done.wait()
        async with self.lock:
            await _close_quarantine(self.manager)


async def close_native_build_quarantine(manager: ResourceManager) -> None:
    """Retry retained owners on this event loop, including after manager exit.

    Descriptor cleanup leaves durable resource recovery to its normal guards
    and expiry checks. Passive background polling only observes physical proof.
    """
    retained = next((owner for owner in _QUARANTINE if owner.manager is manager), None)
    if retained is None:
        return
    # The owning root task may already have stopped. Reuse its existing lock;
    # never create services or background work through a closed manager.
    async with retained.owners.lock:
        await _close_quarantine(manager)


def _register_owner_service(manager: ResourceManager) -> _Owners:
    def create() -> _Owners:
        owners = _Owners(manager)
        manager.register_closer("native_build_quarantine", owners.aclose)
        manager.start_background(owners.reconcile)
        return owners

    return manager.get_or_create_service("native_build_quarantine", create)


async def run_native_build_command(
    spec: NativeBuildSpec,
    *,
    workflow_lock_fd: int,
    startup_timeout_seconds: float = 10,
    cleanup_timeout_seconds: float = 10,
) -> BuildCommandCapture:
    """Run one command or replay its physically completed matching execution.

    The caller retains ownership of its global workflow lock. A fresh phase
    lock additionally excludes concurrent calls sharing that same workflow FD.
    An incomplete or incompatible receipt requires controller reconciliation.
    """
    manager = validate_cpu_reservation(spec.threads)
    if any(
        isinstance(value, bool) or not math.isfinite(value) or not 0 < value <= 60
        for value in (startup_timeout_seconds, cleanup_timeout_seconds)
    ):
        raise ValueError("Native build lifecycle timeouts must be finite and within 60 seconds")
    owners = _register_owner_service(manager)
    _require_workflow_lock(workflow_lock_fd)
    phase = _phase_lock(spec, workflow_lock_fd)
    try:
        os.close(open_receipt_directory(spec.receipt_directory, create=True))
        receipt = read_build_receipt(spec)
        if receipt is not None:
            await run_cpu(verify_build_logs, spec, receipt)
            assert receipt.result is not None
            return BuildCommandCapture(receipt.result, receipt, True)
        async with cpu_reservation(units=spec.threads) as cpu_fd:
            process: NativeBuildProcess | None = None
            uncertain = False
            # Keep duplicates before entering native code; cleanup uncertainty
            # must retain reservations even if dup later becomes impossible.
            descriptors = _duplicate((cpu_fd, workflow_lock_fd, phase))
            try:
                try:
                    process = await NativeBuildProcess.start(
                        spec,
                        guard_descriptors=descriptors,
                        startup_timeout_seconds=startup_timeout_seconds,
                    )
                    result = await process.wait()
                except NativeBuildCleanupError as error:
                    if error.process is not None:
                        process = error.process
                    uncertain = True
                    raise
                finally:
                    if process is not None:
                        try:
                            with anyio.fail_after(cleanup_timeout_seconds, shield=True):
                                await process.aclose()
                        except BaseException:
                            if not process.is_closed():
                                uncertain = True
                        uncertain = not process.is_closed()
                    if uncertain:
                        invalidate_cpu_reservation()
                        _QUARANTINE.append(_Quarantine(manager, process, descriptors, owners))
                        descriptors = ()
                        owners.wake.set()
                        raise NativeBuildCleanupError(process)
                receipt = read_build_receipt(spec)
                if receipt is None or receipt.result != result:
                    raise NativeBuildReceiptError
                await run_cpu(verify_build_logs, spec, receipt)
                return BuildCommandCapture(result, receipt, False)
            finally:
                for descriptor in descriptors:
                    os.close(descriptor)
    finally:
        os.close(phase)
