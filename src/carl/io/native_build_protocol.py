"""Parent ownership of a fresh guarded native-build supervisor."""

import os
import socket
import subprocess
import sys
from contextlib import suppress

import anyio

from carl.core.json import encode_json
from carl.core.native_build import NativeBuildResult, NativeBuildSpec
from carl.io.native_routing_protocol import close_fds, receive_packet, require_guard_fd, send_packet


class NativeBuildProtocolError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Native build supervisor protocol failed")


class NativeBuildCleanupError(RuntimeError):
    def __init__(self, process: "NativeBuildProcess | None" = None) -> None:
        super().__init__("Native build physical cleanup is unconfirmed")
        self.process = process


async def _send(connection: socket.socket, value: dict[str, object]) -> None:
    while True:
        try:
            send_packet(connection, value)
            return
        except BlockingIOError:
            await anyio.wait_writable(connection)


async def _receive(connection: socket.socket) -> dict[str, object]:
    while True:
        try:
            value, descriptors = receive_packet(connection)
            close_fds(descriptors)
            if descriptors:
                raise NativeBuildProtocolError
            return value
        except BlockingIOError:
            await anyio.wait_readable(connection)


class NativeBuildProcess:
    """The owner is retained on unconfirmed cleanup, including failed startup.

    Normal completion proves all subreaped descendants exited. Abrupt supervisor
    death can prove only the direct command PID protected by the exec gate;
    this owner therefore stays quarantined after any abnormal supervisor exit.
    Arbitrary escaped descendants are outside that fail-stop guarantee.
    """

    def __init__(
        self,
        process: anyio.abc.Process,
        connection: socket.socket,
        spec: NativeBuildSpec,
        owned_guards: tuple[int, ...],
    ):
        self._process, self._connection, self.spec = process, connection, spec
        self._owned_guards = owned_guards
        self._child_pid: int | None = None
        self._child_pidfd: int | None = None
        self._child_exit_proven = False
        self._closed = False
        self._result: NativeBuildResult | None = None

    @property
    def pid(self) -> int:
        return self._process.pid

    @property
    def command_pid(self) -> int | None:
        return self._child_pid

    def is_closed(self) -> bool:
        if self._closed:
            return True
        if self._process.returncode == 0:
            self._closed = True
        return self._closed

    @classmethod
    async def start(
        cls,
        spec: NativeBuildSpec,
        *,
        guard_descriptors: tuple[int, ...],
        startup_timeout_seconds: float = 10,
    ) -> "NativeBuildProcess":
        if (
            sys.platform != "linux"
            or not 2 <= len(guard_descriptors) <= 16
            or len(set(guard_descriptors)) != len(guard_descriptors)
        ):
            raise ValueError(
                "Native builds require Linux and distinct CPU/workflow guard descriptors"
            )
        if (
            type(startup_timeout_seconds) not in (float, int)
            or not 0 < startup_timeout_seconds <= 60
        ):
            raise ValueError("Native build startup deadline must be bounded")
        spec = NativeBuildSpec.model_validate_json(spec.model_dump_json())
        for descriptor in guard_descriptors:
            require_guard_fd(descriptor)
        copied: list[int] = []
        try:
            for descriptor in guard_descriptors:
                copied.append(os.dup(descriptor))
        except BaseException:
            close_fds(copied)
            raise
        owned_guards = tuple(copied)
        parent: socket.socket | None = None
        child: socket.socket | None = None
        try:
            parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            parent.setblocking(False)
        except BaseException:
            close_fds(owned_guards)
            if parent is not None:
                parent.close()
            if child is not None:
                child.close()
            raise
        owner: NativeBuildProcess | None = None
        try:
            with anyio.CancelScope(shield=True):
                process = await anyio.open_process(
                    (
                        sys.executable,
                        "-m",
                        "carl.io.native_build_supervisor",
                        str(child.fileno()),
                        ",".join(str(value) for value in guard_descriptors),
                        str(os.getpid()),
                    ),
                    pass_fds=(child.fileno(), *guard_descriptors),
                    start_new_session=True,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                owner = cls(process, parent, spec, owned_guards)
        except BaseException:
            close_fds(owned_guards)
            parent.close()
            raise
        finally:
            child.close()
        if owner is None:
            close_fds(owned_guards)
            parent.close()
            raise NativeBuildProtocolError
        try:
            with anyio.fail_after(startup_timeout_seconds):
                await _send(parent, spec.model_dump(mode="json"))
                message = await _receive(parent)
                if set(message) == {"kind", "result"} and message.get("kind") == "done":
                    result = NativeBuildResult.model_validate_json(encode_json(message["result"]))
                    if (
                        result.execution_identifier != spec.execution_identifier
                        or result.reason != "launch_failure"
                    ):
                        raise NativeBuildProtocolError
                    await owner._process.wait()
                    if owner._process.returncode != 0:
                        raise NativeBuildProtocolError
                    owner._closed, owner._result = True, result
                    await owner.aclose()
                    return owner
                if (
                    set(message) != {"kind", "execution_identifier", "command_pid"}
                    or message.get("kind") != "started"
                    or message["execution_identifier"] != spec.execution_identifier
                    or type(message["command_pid"]) is not int
                    or message["command_pid"] <= 0
                ):
                    raise NativeBuildProtocolError
                owner._child_pid = message["command_pid"]
                try:
                    owner._child_pidfd = os.pidfd_open(owner._child_pid)
                except ProcessLookupError:
                    owner._child_exit_proven = True
            return owner
        except BaseException:
            with anyio.CancelScope(shield=True):
                await owner.aclose()
            raise

    async def wait(self) -> NativeBuildResult:
        if self._result is not None:
            return self._result
        try:
            with anyio.fail_after(self.spec.timeout_seconds + 10):
                message = await _receive(self._connection)
            if set(message) != {"kind", "result"} or message.get("kind") != "done":
                raise NativeBuildProtocolError
            result = NativeBuildResult.model_validate_json(encode_json(message["result"]))
            if result.execution_identifier != self.spec.execution_identifier:
                raise NativeBuildProtocolError
            # DONE is sent only after guard close, durable receipt, and native
            # subtree cleanup. Reap the supervisor before returning to caller.
            with anyio.fail_after(5, shield=True):
                await self._process.wait()
            if self._process.returncode != 0:
                raise NativeBuildProtocolError
            self._closed, self._result = True, result
            await self.aclose()
            return result
        except BaseException:
            with anyio.CancelScope(shield=True):
                await self.aclose()
            raise

    async def aclose(self) -> None:
        if not self.is_closed():
            with suppress(OSError):
                self._connection.shutdown(socket.SHUT_RDWR)
            # EOF is the independent supervisor's cancellation channel. A
            # redundant SIGTERM can race its successful interpreter shutdown
            # after a final message and manufacture an unconfirmed exit.
            try:
                with anyio.fail_after(5, shield=True):
                    await self._process.wait()
                    if self._process.returncode != 0:
                        # The direct PID cannot certify forked descendants
                        # after the subreaper died. Keep guards quarantined.
                        raise NativeBuildCleanupError(self)
            except TimeoutError:
                raise NativeBuildCleanupError(self) from None
            self._closed = True
        self._connection.close()
        close_fds(self._owned_guards)
        self._owned_guards = ()
        if self._child_pidfd is not None:
            os.close(self._child_pidfd)
            self._child_pidfd = None
        await self._process.aclose()
