"""Bounded Unix messages and the parent side of an owned warm routing process."""

import array
import json
import math
import os
import select
import signal
import socket
import stat
import subprocess
import sys
from collections.abc import Sequence
from contextlib import suppress
from typing import NoReturn, cast
from uuid import uuid4

import anyio

from carl.core.json import encode_json
from carl.core.models import JsonValue
from carl.core.native_routing import (
    MAX_NATIVE_DESTINATIONS,
    NativeRoutingDataset,
    NativeRoutingMetric,
    NativeRoutingQuery,
)
from carl.core.travel import TravelCoordinate

MAX_PACKET_BYTES = 65_536
MAX_PACKET_FDS = 4
_REASONS = frozenset(
    {
        "osrm_dataset_missing",
        "osrm_invalid_response",
        "location_outside_coverage",
        "no_route",
        "osrm_unavailable",
        "osrm_timeout",
    }
)


class NativeRoutingProtocolError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Native routing protocol failed")


class NativeRoutingBusy(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Native routing engine is busy")


class NativeRoutingCleanupError(RuntimeError):
    def __init__(self, process: "NativeRoutingProcess | None" = None) -> None:
        super().__init__("Native routing physical cleanup is unconfirmed")
        self.process = process


def close_fds(descriptors: Sequence[int]) -> None:
    for descriptor in descriptors:
        with suppress(OSError):
            os.close(descriptor)


def require_guard_fd(descriptor: int) -> None:
    if type(descriptor) is not int or descriptor < 0:
        raise NativeRoutingProtocolError
    try:
        info = os.fstat(descriptor)
    except OSError:
        raise NativeRoutingProtocolError from None
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
        raise NativeRoutingProtocolError


def _unique_object(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise NativeRoutingProtocolError
        result[key] = value
    return result


def _reject_constant(_value: str) -> NoReturn:
    raise NativeRoutingProtocolError


def send_packet(
    connection: socket.socket, message: dict[str, JsonValue], descriptors: tuple[int, ...] = ()
) -> None:
    if len(descriptors) > MAX_PACKET_FDS:
        raise NativeRoutingProtocolError
    try:
        content = json.dumps(message, allow_nan=False, separators=(",", ":")).encode()
    except (ValueError, TypeError, RecursionError):
        raise NativeRoutingProtocolError from None
    if not content or len(content) > MAX_PACKET_BYTES:
        raise NativeRoutingProtocolError
    ancillary = (
        [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", descriptors))]
        if descriptors
        else []
    )
    if connection.sendmsg((content,), ancillary) != len(content):
        raise NativeRoutingProtocolError


def receive_packet(connection: socket.socket) -> tuple[dict[str, JsonValue], tuple[int, ...]]:
    content, ancillary, flags, _ = connection.recvmsg(
        MAX_PACKET_BYTES,
        socket.CMSG_SPACE(MAX_PACKET_FDS * array.array("i").itemsize),
        socket.MSG_CMSG_CLOEXEC,
    )
    descriptors: list[int] = []
    try:
        unknown_ancillary = False
        for level, kind, data in ancillary:
            if level != socket.SOL_SOCKET or kind != socket.SCM_RIGHTS:
                unknown_ancillary = True
            else:
                values = array.array("i")
                values.frombytes(data[: len(data) - len(data) % values.itemsize])
                descriptors.extend(values)
        if not content:
            raise EOFError
        if unknown_ancillary or flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
            raise NativeRoutingProtocolError
        value: object = json.loads(
            content,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
        if not isinstance(value, dict):
            raise NativeRoutingProtocolError
        return cast(dict[str, JsonValue], value), tuple(descriptors)
    except (ValueError, UnicodeDecodeError, RecursionError):
        close_fds(descriptors)
        raise NativeRoutingProtocolError from None
    except BaseException:
        close_fds(descriptors)
        raise


def require_keys(message: dict[str, JsonValue], keys: set[str], *, kind: str) -> None:
    if set(message) != keys or message.get("kind") != kind:
        raise NativeRoutingProtocolError


def decode_query(message: dict[str, JsonValue]) -> NativeRoutingQuery:
    require_keys(
        message,
        {"kind", "request_identifier", "origin", "destinations", "maximum_snap_distance_meters"},
        kind="query",
    )
    value = dict(message)
    del value["kind"]
    return NativeRoutingQuery.model_validate_json(encode_json(value))


def decode_metrics(value: object, count: int) -> tuple[NativeRoutingMetric, ...]:
    if not isinstance(value, list) or len(value) != count:
        raise NativeRoutingProtocolError
    metrics: list[NativeRoutingMetric] = []
    for item in value:
        if isinstance(item, str) and item in _REASONS:
            metrics.append(item)
        elif (
            isinstance(item, list | tuple)
            and len(item) == 2
            and all(_valid_number(number) for number in item)
        ):
            metrics.append((float(item[0]), float(item[1])))
        else:
            raise NativeRoutingProtocolError
    return tuple(metrics)


def _valid_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        return False


async def _send(
    connection: socket.socket, message: dict[str, JsonValue], descriptors: tuple[int, ...] = ()
) -> None:
    while True:
        try:
            send_packet(connection, message, descriptors)
            return
        except BlockingIOError:
            await anyio.wait_writable(connection)


async def _receive(connection: socket.socket) -> dict[str, JsonValue]:
    while True:
        try:
            message, descriptors = receive_packet(connection)
            if descriptors:
                close_fds(descriptors)
                raise NativeRoutingProtocolError
            return message
        except BlockingIOError:
            await anyio.wait_readable(connection)


class NativeRoutingProcess:
    """One explicit supervisor and native child; no opaque shared process pool."""

    def __init__(self, process: anyio.abc.Process, connection: socket.socket) -> None:
        self._process = process
        self._connection = connection
        self._lock = anyio.Lock()
        self._worker_pid: int | None = None
        self._worker_pidfd: int | None = None
        self._closed = False

    @property
    def pid(self) -> int:
        return self._process.pid

    @property
    def worker_pid(self) -> int | None:
        return self._worker_pid

    def is_closed(self) -> bool:
        if self._closed:
            return True
        if self._process.returncode == 0 and self._worker_pidfd is None:
            # A normal supervisor exit is emitted only after its child has
            # been killed and reaped, including startup before READY.
            self._closed = True
        if (
            self._process.returncode is not None
            and self._worker_pidfd is not None
            and select.select((self._worker_pidfd,), (), (), 0)[0]
        ):
            self._closed = True
        return self._closed

    @classmethod
    async def start(
        cls,
        *,
        dataset: NativeRoutingDataset,
        resident_guard_fd: int,
        backend_module: str | None = None,
        startup_timeout_seconds: float = 10,
        idle_timeout_seconds: float = 30,
    ) -> "NativeRoutingProcess":
        if (
            sys.platform != "linux"
            or type(startup_timeout_seconds) not in (float, int)
            or type(idle_timeout_seconds) not in (float, int)
            or not 0 < startup_timeout_seconds <= 60
            or not 0 < idle_timeout_seconds <= 300
        ):
            raise ValueError("Native routing requires Linux and bounded process deadlines")
        require_guard_fd(resident_guard_fd)
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        parent.setblocking(False)
        owner: NativeRoutingProcess | None = None
        try:
            # Cancellation may arrive after the OS spawn but before its await
            # returns. Keep ownership construction in the same shield.
            with anyio.CancelScope(shield=True):
                process = await anyio.open_process(
                    (
                        sys.executable,
                        "-m",
                        "carl.io.native_routing_supervisor",
                        str(child.fileno()),
                        str(resident_guard_fd),
                        str(os.getpid()),
                        str(idle_timeout_seconds),
                    ),
                    pass_fds=(child.fileno(), resident_guard_fd),
                    start_new_session=True,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                owner = cls(process, parent)
        except BaseException:
            parent.close()
            raise
        finally:
            child.close()
        if owner is None:
            parent.close()
            raise NativeRoutingProtocolError
        request = str(uuid4())
        try:
            with anyio.fail_after(startup_timeout_seconds):
                await _send(
                    parent,
                    {
                        "kind": "open",
                        "request_identifier": request,
                        "dataset": dataset.model_dump(mode="json"),
                        "backend_module": backend_module,
                    },
                )
                message = await _receive(parent)
                require_keys(message, {"kind", "request_identifier", "worker_pid"}, kind="ready")
                if (
                    message["request_identifier"] != request
                    or type(message["worker_pid"]) is not int
                    or message["worker_pid"] <= 0
                ):
                    raise NativeRoutingProtocolError
                owner._worker_pid = message["worker_pid"]
                owner._worker_pidfd = os.pidfd_open(owner._worker_pid)
            return owner
        except BaseException:
            with anyio.CancelScope(shield=True):
                await owner.aclose()
            raise

    async def query(
        self,
        origin: TravelCoordinate,
        destinations: Sequence[TravelCoordinate],
        *,
        maximum_snap_distance_meters: float,
        cpu_guard_fd: int,
        timeout_seconds: float = 60,
    ) -> tuple[NativeRoutingMetric, ...]:
        if type(timeout_seconds) not in (float, int) or not 0 < timeout_seconds <= 300:
            raise ValueError("Native routing query deadline must be bounded")
        if not 1 <= len(destinations) <= MAX_NATIVE_DESTINATIONS:
            raise ValueError("Native routing requires between one and 64 destinations")
        query = NativeRoutingQuery(
            request_identifier=str(uuid4()),
            origin=origin,
            destinations=tuple(destinations),
            maximum_snap_distance_meters=maximum_snap_distance_meters,
        )
        require_guard_fd(cpu_guard_fd)
        try:
            self._lock.acquire_nowait()
        except anyio.WouldBlock:
            raise NativeRoutingBusy from None
        try:
            if self.is_closed():
                raise NativeRoutingProtocolError
            message = query.model_dump(mode="json")
            message["kind"] = "query"
            message["timeout_seconds"] = timeout_seconds
            with anyio.fail_after(timeout_seconds + 5):
                await _send(self._connection, message, (cpu_guard_fd,))
                reply = await _receive(self._connection)
            if reply.get("request_identifier") != query.request_identifier:
                raise NativeRoutingProtocolError
            if reply.get("kind") == "closed":
                require_keys(reply, {"kind", "request_identifier", "reason"}, kind="closed")
                if reply["reason"] not in {"osrm_timeout", "osrm_unavailable"}:
                    raise NativeRoutingProtocolError
                await self.aclose()
                return (reply["reason"],) * len(query.destinations)
            require_keys(reply, {"kind", "request_identifier", "metrics"}, kind="result")
            return decode_metrics(reply["metrics"], len(query.destinations))
        except BaseException:
            with anyio.CancelScope(shield=True):
                await self.aclose()
            raise
        finally:
            self._lock.release()

    async def aclose(self) -> None:
        if not self._closed:
            with suppress(OSError):
                self._connection.shutdown(socket.SHUT_RDWR)
            if self._process.returncode is None and self._worker_pid is not None:
                with suppress(ProcessLookupError):
                    self._process.send_signal(signal.SIGTERM)
            try:
                with anyio.fail_after(5, shield=True):
                    await self._process.wait()
                    if self._worker_pidfd is not None:
                        await anyio.wait_readable(self._worker_pidfd)
                    elif self._process.returncode != 0:
                        raise NativeRoutingCleanupError(self)
            except TimeoutError:
                raise NativeRoutingCleanupError(self) from None
            self._closed = True
        self._connection.close()
        if self._worker_pidfd is not None:
            os.close(self._worker_pidfd)
            self._worker_pidfd = None
        await self._process.aclose()
