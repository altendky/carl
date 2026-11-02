"""Own one bounded native command, its ordinary descendants, logs and receipts.

The private supervisor is a Linux subreaper. While alive it kills and reaps the
command's process group, including ordinary forked wrappers. The exec gate's
parent-death signal protects the direct command if this supervisor itself dies;
it does not promise protection for arbitrary escaped descendant sessions.
"""

import ctypes
import hashlib
import os
import selectors
import signal
import socket
import subprocess
import sys
from contextlib import suppress
from pathlib import Path
from time import monotonic, sleep

from carl.core.json import encode_json
from carl.core.native_build import NativeBuildResult, NativeBuildSpec
from carl.io.native_build_receipts import (
    open_receipt_directory,
    write_complete_receipt,
    write_start_receipt,
)
from carl.io.native_routing_protocol import close_fds, receive_packet, require_guard_fd, send_packet

_CHUNK = 65_536
_TAIL = 65_536


class _Log:
    def __init__(self, directory: int, name: str, limit: int):
        self.descriptor = os.open(
            name,
            os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory,
        )
        self.limit, self.size = limit, 0
        self.head, self.tail = bytearray(), bytearray()

    def retain(self, chunk: bytes, remaining: int) -> bool:
        self.tail.extend(chunk)
        if len(self.tail) > _TAIL:
            del self.tail[:-_TAIL]
        available = min(self.limit - self.size, remaining)
        view = memoryview(chunk)[:available]
        offset = 0
        while offset < len(view):
            written = os.write(self.descriptor, view[offset:])
            if written <= 0:
                raise OSError("Native build log write failed")
            if len(self.head) < 4096:
                self.head.extend(view[offset : offset + min(written, 4096 - len(self.head))])
            offset += written
            self.size += written
        return len(chunk) > available

    def finalize(self, *, retain_tail: bool) -> str:
        if retain_tail:
            count = min(len(self.tail), max(0, self.size - 4096))
            if count:
                view = memoryview(self.tail)[-count:]
                offset = 0
                while offset < count:
                    written = os.pwrite(self.descriptor, view[offset:], self.size - count + offset)
                    if written <= 0:
                        raise OSError("Native build tail write failed")
                    offset += written
        os.fsync(self.descriptor)
        os.lseek(self.descriptor, 0, os.SEEK_SET)
        checksum = hashlib.sha256()
        observed = 0
        while chunk := os.read(self.descriptor, _CHUNK):
            observed += len(chunk)
            if observed > self.size:
                raise OSError("Native build log size changed")
            checksum.update(chunk)
        if observed != self.size:
            raise OSError("Native build log size changed")
        return checksum.hexdigest()

    def close(self) -> None:
        os.close(self.descriptor)


def _subreaper() -> None:
    if sys.platform != "linux" or ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise RuntimeError("Native build descendant ownership unavailable")
    # SIG_IGN can survive exec and auto-reap children. The initial group signal
    # relies on our direct leader retaining its PID until we explicitly wait.
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


def _cleanup(child: subprocess.Popen[bytes]) -> int:
    # A group signal is safe only while the leader is still unreaped. Once it
    # has been reaped, an escaped descendant no longer pins the old group ID.
    if child.returncode is None:
        with suppress(ProcessLookupError):
            os.killpg(child.pid, signal.SIGKILL)
    returncode = child.wait()
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                _terminate_adopted_children()
                sleep(0.01)
        except ChildProcessError:
            return returncode
        except InterruptedError:
            continue


def _proc_bytes(path: Path) -> bytes:
    with path.open("rb") as stream:
        content = stream.read(65_537)
    if len(content) > 65_536:
        raise RuntimeError("Native build child ownership metadata exceeds its bound")
    return content


def _terminate_adopted_children() -> None:
    owner = os.getpid()
    children = _proc_bytes(Path(f"/proc/self/task/{owner}/children"))
    for value in children.split():
        pid = int(value)
        try:
            descriptor = os.pidfd_open(pid)
        except ProcessLookupError:
            continue
        try:
            try:
                status = _proc_bytes(Path(f"/proc/{pid}/status"))
            except FileNotFoundError:
                continue
            parent = next(
                (
                    line.partition(b":")[2].strip()
                    for line in status.splitlines()
                    if line.startswith(b"PPid:")
                ),
                None,
            )
            # Open the pidfd first, then independently confirm this is still
            # our adopted child. No numeric signal follows a PID-reuse race.
            if parent is not None and int(parent) == owner:
                with suppress(ProcessLookupError):
                    signal.pidfd_send_signal(descriptor, signal.SIGKILL)
        finally:
            os.close(descriptor)


def main() -> int:
    connection: socket.socket | None = None
    guards: tuple[int, ...] = ()
    child: subprocess.Popen[bytes] | None = None
    child_pidfd: int | None = None
    directory: int | None = None
    logs: dict[str, _Log] = {}
    spec: NativeBuildSpec | None = None
    selector = selectors.DefaultSelector()
    wake_read, wake_write = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
    started_at: int | None = None
    reason = "supervisor_failure"
    returncode: int | None = None
    output_truncated = False

    def stop(_signum: int, _frame: object) -> None:
        with suppress(OSError):
            os.write(wake_write, b"x")

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    selector.register(wake_read, selectors.EVENT_READ, "stop")
    try:
        control = int(sys.argv[1])
        guards = tuple(int(value) for value in sys.argv[2].split(","))
        if (
            os.getppid() != int(sys.argv[3])
            or not 2 <= len(guards) <= 16
            or len(set(guards)) != len(guards)
        ):
            raise RuntimeError("Native build ownership unavailable")
        for descriptor in guards:
            require_guard_fd(descriptor)
            os.set_inheritable(descriptor, False)
        _subreaper()
        connection = socket.socket(fileno=control)
        connection.setblocking(False)
        selector.register(connection, selectors.EVENT_READ, "parent")
        deadline = monotonic() + 10
        while True:
            ready = selector.select(max(0, deadline - monotonic()))
            if not ready or any(key.data == "stop" for key, _ in ready):
                return 0
            if any(key.data == "parent" for key, _ in ready):
                message, descriptors = receive_packet(connection)
                close_fds(descriptors)
                if descriptors:
                    raise RuntimeError("Unexpected native build descriptors")
                spec = NativeBuildSpec.model_validate_json(encode_json(message))
                break
        directory = open_receipt_directory(spec.receipt_directory, create=False)
        logs["stdout"] = _Log(directory, "stdout.log", spec.stdout_limit_bytes)
        logs["stderr"] = _Log(directory, "stderr.log", spec.stderr_limit_bytes)
        receipt = write_start_receipt(spec)
        started_at = receipt.started_at_utc_ns
        deadline = monotonic() + spec.timeout_seconds
        try:
            child = subprocess.Popen(
                (
                    sys.executable,
                    "-m",
                    "carl.io.native_build_exec",
                    str(os.getpid()),
                    ",".join(str(value) for value in guards),
                    *spec.argv,
                ),
                cwd=spec.directory,
                env={**os.environ, "OSMIUM_POOL_THREADS": str(spec.threads)},
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                pass_fds=guards,
                start_new_session=True,
            )
        except Exception:
            reason = "launch_failure"
        if child is not None:
            child_pidfd = os.pidfd_open(child.pid)
            selector.register(child_pidfd, selectors.EVENT_READ, "child")
            assert child.stdout is not None and child.stderr is not None
            for name, stream in (("stdout", child.stdout), ("stderr", child.stderr)):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)
            send_packet(
                connection,
                {
                    "kind": "started",
                    "execution_identifier": spec.execution_identifier,
                    "command_pid": child.pid,
                },
            )
            active = {"stdout", "stderr"}
            reason = "completed"
            while active or child is not None:
                # Check on every turn, including continuously readable pipes.
                if child is not None and monotonic() >= deadline:
                    reason = "timeout"
                    returncode = _cleanup(child)
                    child = None
                    selector.unregister(child_pidfd)
                ready = selector.select(
                    0.1 if child is None else min(0.1, max(0, deadline - monotonic()))
                )
                if any(key.data in {"parent", "stop"} for key, _ in ready):
                    reason = "cancelled"
                    if child is not None:
                        returncode = _cleanup(child)
                        child = None
                        selector.unregister(child_pidfd)
                elif child is not None and any(key.data == "child" for key, _ in ready):
                    returncode = _cleanup(child)
                    child = None
                    selector.unregister(child_pidfd)
                    if returncode:
                        reason = "exit_nonzero"
                for key, _ in ready:
                    if key.data not in active:
                        continue
                    chunk = os.read(key.fd, _CHUNK)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        active.remove(key.data)
                        continue
                    log = logs[key.data]
                    limited = log.retain(
                        chunk, spec.combined_limit_bytes - sum(item.size for item in logs.values())
                    )
                    output_truncated |= limited
                    if limited and spec.on_output_limit == "terminate":
                        reason = "output_limit"
                        if child is not None:
                            returncode = _cleanup(child)
                            child = None
                            selector.unregister(child_pidfd)
                    if child is not None and monotonic() >= deadline:
                        reason = "timeout"
                        returncode = _cleanup(child)
                        child = None
                        selector.unregister(child_pidfd)
        checksums = {
            name: log.finalize(retain_tail=spec.on_output_limit == "truncate" and output_truncated)
            for name, log in logs.items()
        }
        os.fsync(directory)
        result = NativeBuildResult(
            execution_identifier=spec.execution_identifier,
            reason=reason,
            returncode=returncode,
            stdout_head=logs["stdout"].head.decode("utf-8", errors="replace"),
            stdout_bytes=logs["stdout"].size,
            stderr_bytes=logs["stderr"].size,
            stdout_sha256=checksums["stdout"],
            stderr_sha256=checksums["stderr"],
            output_truncated=output_truncated,
        )
        _ = write_complete_receipt(spec, result, started_at_utc_ns=started_at)
        for log in logs.values():
            log.close()
        logs.clear()
        os.close(directory)
        directory = None
        close_fds(guards)
        guards = ()
        with suppress(OSError):
            send_packet(connection, {"kind": "done", "result": result.model_dump(mode="json")})
        return 0
    except Exception as error:
        if child is not None:
            _cleanup(child)
            child = None
        if connection is not None:
            with suppress(OSError, RuntimeError):
                send_packet(
                    connection, {"kind": "failed", "failure_type": type(error).__name__[:128]}
                )
        return 0
    finally:
        if child is not None:
            _cleanup(child)
        for log in logs.values():
            log.close()
        if directory is not None:
            os.close(directory)
        if child_pidfd is not None:
            os.close(child_pidfd)
        close_fds(guards)
        if connection is not None:
            connection.close()
        selector.close()
        os.close(wake_read)
        os.close(wake_write)


if __name__ == "__main__":
    raise SystemExit(main())
