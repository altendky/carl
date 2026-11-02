"""Independent deadlines and physical ownership for one warm native worker."""

import os
import re
import selectors
import signal
import socket
import subprocess
import sys
from contextlib import suppress
from time import monotonic

from carl.core.json import encode_json
from carl.core.native_routing import NativeRoutingDataset
from carl.io.native_routing_protocol import (
    NativeRoutingProtocolError,
    close_fds,
    decode_metrics,
    decode_query,
    receive_packet,
    require_guard_fd,
    require_keys,
    send_packet,
)


class _Stop(Exception):
    pass


def _kill_and_reap(child: subprocess.Popen[bytes] | None) -> None:
    if child is not None:
        with suppress(ProcessLookupError):
            os.killpg(child.pid, signal.SIGKILL)
        _ = child.wait()


def main() -> int:
    parent: socket.socket | None = None
    worker: socket.socket | None = None
    child: subprocess.Popen[bytes] | None = None
    resident = -1
    cpu: int | None = None
    selector = selectors.DefaultSelector()
    wake_read, wake_write = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)

    def stop(_signum: int, _frame: object) -> None:
        with suppress(OSError):
            _ = os.write(wake_write, b"x")

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    selector.register(wake_read, selectors.EVENT_READ, "stop")

    def events(deadline: float):
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError
        ready = selector.select(remaining)
        if not ready:
            raise TimeoutError
        if any(key.data == "stop" for key, _ in ready):
            raise _Stop
        return ready

    def child_reply(deadline: float):
        if parent is None or worker is None:
            raise NativeRoutingProtocolError
        while True:
            for key, _ in events(deadline):
                if key.data == "parent":
                    # A second request while active is invalid; EOF means the
                    # root owner disappeared. Both require physical cleanup.
                    _, descriptors = receive_packet(parent)
                    close_fds(descriptors)
                    raise NativeRoutingProtocolError
                if key.data == "worker":
                    message, descriptors = receive_packet(worker)
                    close_fds(descriptors)
                    if descriptors:
                        raise NativeRoutingProtocolError
                    return message

    try:
        descriptor, resident, expected_parent = (int(item) for item in sys.argv[1:4])
        idle_seconds = float(sys.argv[4])
        if (
            sys.platform != "linux"
            or os.getppid() != expected_parent
            or not 0 < idle_seconds <= 300
        ):
            raise NativeRoutingProtocolError
        require_guard_fd(resident)
        os.set_inheritable(resident, False)
        parent = socket.socket(fileno=descriptor)
        parent.setblocking(False)
        selector.register(parent, selectors.EVENT_READ, "parent")
        initial_deadline = monotonic() + 10
        while True:
            if any(key.data == "parent" for key, _ in events(initial_deadline)):
                message, descriptors = receive_packet(parent)
                close_fds(descriptors)
                if descriptors:
                    raise NativeRoutingProtocolError
                break
        require_keys(
            message, {"kind", "request_identifier", "dataset", "backend_module"}, kind="open"
        )
        if (
            not isinstance(message["request_identifier"], str)
            or not 1 <= len(message["request_identifier"]) <= 100
        ):
            raise NativeRoutingProtocolError
        _ = NativeRoutingDataset.model_validate_json(encode_json(message["dataset"]))
        backend_module = message["backend_module"]
        if backend_module is not None and (
            not isinstance(backend_module, str)
            or len(backend_module) > 200
            or re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*", backend_module) is None
        ):
            raise NativeRoutingProtocolError
        worker, native = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        try:
            child = subprocess.Popen(
                (
                    sys.executable,
                    "-m",
                    "carl.io.native_routing_worker",
                    str(native.fileno()),
                    str(resident),
                    str(os.getpid()),
                ),
                pass_fds=(native.fileno(), resident),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        finally:
            native.close()
        worker.setblocking(False)
        selector.register(worker, selectors.EVENT_READ, "worker")
        send_packet(worker, message)
        ready = child_reply(monotonic() + 10)
        require_keys(ready, {"kind", "request_identifier", "worker_pid"}, kind="ready")
        if (
            ready["request_identifier"] != message["request_identifier"]
            or ready["worker_pid"] != child.pid
        ):
            raise NativeRoutingProtocolError
        send_packet(parent, ready)
        last_request = None
        while True:
            deadline = monotonic() + idle_seconds
            while True:
                available = events(deadline)
                if any(key.data == "worker" for key, _ in available):
                    raise NativeRoutingProtocolError
                if any(key.data == "parent" for key, _ in available):
                    message, descriptors = receive_packet(parent)
                    break
            if len(descriptors) != 1:
                close_fds(descriptors)
                raise NativeRoutingProtocolError
            cpu = descriptors[0]
            require_guard_fd(cpu)
            timeout = message.pop("timeout_seconds", None)
            if type(timeout) not in (float, int) or not 0 < timeout <= 300:
                raise NativeRoutingProtocolError
            query = decode_query(message)
            if query.request_identifier == last_request:
                raise NativeRoutingProtocolError
            last_request = query.request_identifier
            request_deadline = monotonic() + timeout
            send_packet(worker, message, (cpu,))
            try:
                adopted = child_reply(request_deadline)
                require_keys(adopted, {"kind", "request_identifier"}, kind="adopted")
                if adopted["request_identifier"] != query.request_identifier:
                    raise NativeRoutingProtocolError
                reply = child_reply(request_deadline)
                require_keys(reply, {"kind", "request_identifier", "metrics"}, kind="result")
                if reply["request_identifier"] != query.request_identifier:
                    raise NativeRoutingProtocolError
                _ = decode_metrics(reply["metrics"], len(query.destinations))
            except (TimeoutError, NativeRoutingProtocolError, EOFError, OSError) as error:
                _kill_and_reap(child)
                child = None
                os.close(cpu)
                cpu = None
                with suppress(OSError, NativeRoutingProtocolError):
                    send_packet(
                        parent,
                        {
                            "kind": "closed",
                            "request_identifier": query.request_identifier,
                            "reason": "osrm_timeout"
                            if isinstance(error, TimeoutError)
                            else "osrm_unavailable",
                        },
                    )
                return 0
            os.close(cpu)
            cpu = None
            # The supervisor's copy must be gone before the parent can seal its
            # stage guard. The worker already closed its copy before replying.
            send_packet(parent, reply)
    except Exception:
        return 0
    finally:
        _kill_and_reap(child)
        if cpu is not None:
            os.close(cpu)
        if worker is not None:
            worker.close()
        if parent is not None:
            parent.close()
        if resident >= 0:
            os.close(resident)
        selector.close()
        os.close(wake_read)
        os.close(wake_write)


if __name__ == "__main__":
    raise SystemExit(main())
