"""Real offline processes exercise warm engines and physical guard ownership."""

# Tests inspect the private socket only to inject invalid peer messages.
# pyright: reportPrivateUsage=false

import fcntl
import os
import signal
import socket
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import anyio
import pytest

import carl.io.native_routing_protocol as protocol
from carl.core.native_routing import NativeRoutingDataset, NativeRoutingQuery
from carl.core.travel import TravelCoordinate
from carl.io.native_routing_protocol import (
    MAX_PACKET_BYTES,
    NativeRoutingCleanupError,
    NativeRoutingProcess,
    NativeRoutingProtocolError,
    close_fds,
    decode_metrics,
    receive_packet,
    send_packet,
)

_BACKEND = "carl._tests.native_routing_backend"
_ORIGIN = TravelCoordinate(latitude=40, longitude=-77)
_DESTINATIONS = (TravelCoordinate(latitude=41, longitude=-76),)


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _guard(path: Path) -> int:
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return descriptor


def _available(path: Path) -> bool:
    descriptor = os.open(path, os.O_RDWR)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False
    finally:
        os.close(descriptor)


async def _start(root: Path, *, idle: float = 30) -> NativeRoutingProcess:
    resident = _guard(root / "resident.lock")
    try:
        return await NativeRoutingProcess.start(
            dataset=NativeRoutingDataset(
                dataset_path=root / "roads.osrm", dataset_identifier="fixture"
            ),
            resident_guard_fd=resident,
            backend_module=_BACKEND,
            idle_timeout_seconds=idle,
        )
    finally:
        os.close(resident)


async def _query(process: NativeRoutingProcess, root: Path, *, timeout: float = 2):
    cpu = _guard(root / "cpu.lock")
    try:
        return await process.query(
            _ORIGIN,
            _DESTINATIONS,
            maximum_snap_distance_meters=5000,
            cpu_guard_fd=cpu,
            timeout_seconds=timeout,
        )
    finally:
        os.close(cpu)


async def _wait_file(path: Path) -> None:
    with anyio.fail_after(5):
        while not path.exists():
            await anyio.sleep(0.01)


def test_bounded_packets_reject_invalid_json_and_close_transferred_fds(tmp_path: Path) -> None:
    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    guard = _guard(tmp_path / "guard")
    try:
        send_packet(sender, {"kind": "example"}, (guard,))
        message, descriptors = receive_packet(receiver)
        assert message == {"kind": "example"} and len(descriptors) == 1
        assert not os.get_inheritable(descriptors[0])
        close_fds(descriptors)
        with pytest.raises(NativeRoutingProtocolError):
            send_packet(sender, {"oversized": "x" * MAX_PACKET_BYTES})
        before = len(list(Path("/proc/self/fd").iterdir()))
        import array

        _ = sender.sendmsg(
            (b'{"invalid":',), ((socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", (guard,))),)
        )
        with pytest.raises(NativeRoutingProtocolError):
            receive_packet(receiver)
        assert len(list(Path("/proc/self/fd").iterdir())) == before
        _ = sender.send(b'{"duplicate":1,"duplicate":2}')
        with pytest.raises(NativeRoutingProtocolError):
            receive_packet(receiver)
    finally:
        sender.close()
        receiver.close()
        os.close(guard)


def test_query_and_result_bounds_are_strict() -> None:
    with pytest.raises(ValueError):
        NativeRoutingQuery(
            request_identifier="query",
            origin=_ORIGIN,
            destinations=_DESTINATIONS * 65,
            maximum_snap_distance_meters=5000,
        )
    for invalid in (
        [[True, 1]],
        [[-1, 0]],
        [[float("nan"), 0]],
        [[10**1000, 0]],
        ["private native exception"],
    ):
        with pytest.raises(NativeRoutingProtocolError):
            decode_metrics(invalid, 1)


def test_unknown_ancillary_before_rights_still_closes_later_fds(tmp_path: Path) -> None:
    import array

    guard = _guard(tmp_path / "guard")
    received = os.dup(guard)

    class Peer:
        def recvmsg(self, *_args: object):
            return (
                b'{"kind":"message"}',
                [
                    (socket.SOL_SOCKET, socket.SCM_CREDENTIALS, b"unknown"),
                    (socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", (received,)).tobytes()),
                ],
                0,
                None,
            )

    try:
        with pytest.raises(NativeRoutingProtocolError):
            receive_packet(cast(socket.socket, cast(object, Peer())))
        with pytest.raises(OSError):
            _ = os.fstat(received)
    finally:
        os.close(guard)


@pytest.mark.anyio
async def test_warm_worker_reuses_engine_and_releases_query_guard_before_reply(
    tmp_path: Path,
) -> None:
    process = await _start(tmp_path)
    try:
        first = await _query(process, tmp_path)
        second = await _query(process, tmp_path)
        worker_pid = process.worker_pid
        assert worker_pid is not None
        assert first == ((float(worker_pid), 1.0),)
        assert second == ((float(worker_pid), 2.0),)
        assert _available(tmp_path / "cpu.lock")
        assert not _available(tmp_path / "resident.lock")
        assert not process.is_closed()
    finally:
        await process.aclose()
    assert process.is_closed() and _available(tmp_path / "resident.lock")


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ("query", "initialize"))
async def test_supervisor_deadline_kills_and_reaps_stalled_native_work(
    tmp_path: Path, phase: str
) -> None:
    _ = (tmp_path / f"block-{phase}").write_text("block")
    process = await _start(tmp_path)
    try:
        assert await _query(process, tmp_path, timeout=0.1) == ("osrm_timeout",)
        assert process.is_closed()
        assert _available(tmp_path / "cpu.lock") and _available(tmp_path / "resident.lock")
    finally:
        await process.aclose()


@pytest.mark.anyio
async def test_idle_eviction_disposes_mapped_engine_and_residency(tmp_path: Path) -> None:
    process = await _start(tmp_path, idle=0.15)
    try:
        _ = await _query(process, tmp_path)
        with anyio.fail_after(5):
            while not process.is_closed():
                await anyio.sleep(0.01)
        assert _available(tmp_path / "resident.lock")
    finally:
        await process.aclose()


@pytest.mark.anyio
async def test_query_cancellation_waits_for_physical_cleanup(tmp_path: Path) -> None:
    _ = (tmp_path / "block-query").write_text("block")
    process = await _start(tmp_path)
    try:
        with anyio.move_on_after(0.2) as cancelled:
            _ = await _query(process, tmp_path)
            pytest.fail("Stalled query should be cancelled")
        assert cancelled.cancel_called
        assert process.is_closed()
        assert _available(tmp_path / "cpu.lock") and _available(tmp_path / "resident.lock")
    finally:
        await process.aclose()


@pytest.mark.anyio
async def test_supervisor_death_cannot_leave_native_worker_or_guards_alive(tmp_path: Path) -> None:
    _ = (tmp_path / "block-query").write_text("block")
    process = await _start(tmp_path)

    async def kill_supervisor() -> None:
        await _wait_file(tmp_path / "query-started")
        assert not _available(tmp_path / "cpu.lock")
        os.kill(process.pid, signal.SIGKILL)

    try:
        async with anyio.create_task_group() as group:
            group.start_soon(kill_supervisor)
            with pytest.raises((EOFError, OSError, NativeRoutingProtocolError)):
                _ = await _query(process, tmp_path)
        assert process.is_closed()
        assert _available(tmp_path / "cpu.lock") and _available(tmp_path / "resident.lock")
    finally:
        await process.aclose()


@pytest.mark.anyio
async def test_invalid_multiple_query_fds_are_closed_without_executing_backend(
    tmp_path: Path,
) -> None:
    process = await _start(tmp_path)
    first = _guard(tmp_path / "first.lock")
    second = _guard(tmp_path / "second.lock")
    try:
        send_packet(process._connection, {"kind": "query"}, (first, second))
    finally:
        os.close(first)
        os.close(second)
    try:
        with anyio.fail_after(5):
            while not process.is_closed():
                await anyio.sleep(0.01)
        assert not (tmp_path / "initialize-started").exists()
        assert _available(tmp_path / "first.lock") and _available(tmp_path / "second.lock")
        assert _available(tmp_path / "resident.lock")
    finally:
        await process.aclose()


@pytest.mark.anyio
async def test_cancel_after_spawn_cannot_lose_supervisor_handle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = anyio.open_process
    spawned: list[anyio.abc.Process] = []

    async def delayed_spawn(command: Sequence[str], **kwargs: Any) -> anyio.abc.Process:
        process = await original(command, **kwargs)
        spawned.append(process)
        await anyio.sleep(0.1)
        return process

    monkeypatch.setattr(anyio, "open_process", delayed_spawn)
    resident = _guard(tmp_path / "resident.lock")
    try:
        with anyio.move_on_after(0.02) as cancelled:
            process = await NativeRoutingProcess.start(
                dataset=NativeRoutingDataset(
                    dataset_path=tmp_path / "roads.osrm", dataset_identifier="fixture"
                ),
                resident_guard_fd=resident,
                backend_module=_BACKEND,
            )
            await process.aclose()
            pytest.fail("Startup cancellation should propagate after bounded cleanup")
        assert cancelled.cancel_called
        assert len(spawned) == 1 and spawned[0].returncode is not None
    finally:
        os.close(resident)
    assert _available(tmp_path / "resident.lock")


@pytest.mark.anyio
async def test_failed_startup_cleanup_exposes_owned_process_for_quarantine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_close = NativeRoutingProcess.aclose

    async def failed_receive(_connection: socket.socket) -> dict[str, Any]:
        raise NativeRoutingProtocolError

    async def unconfirmed_close(self: NativeRoutingProcess) -> None:
        raise NativeRoutingCleanupError(self)

    monkeypatch.setattr(protocol, "_receive", failed_receive)
    monkeypatch.setattr(NativeRoutingProcess, "aclose", unconfirmed_close)
    resident = _guard(tmp_path / "resident.lock")
    try:
        with pytest.raises(NativeRoutingCleanupError) as failure:
            await NativeRoutingProcess.start(
                dataset=NativeRoutingDataset(
                    dataset_path=tmp_path / "roads.osrm", dataset_identifier="fixture"
                ),
                resident_guard_fd=resident,
                backend_module=_BACKEND,
            )
    finally:
        os.close(resident)
    owned = failure.value.process
    assert owned is not None and not owned.is_closed()
    assert not _available(tmp_path / "resident.lock")
    await original_close(owned)
    assert owned.is_closed() and _available(tmp_path / "resident.lock")
