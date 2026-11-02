"""Native engine child: arm parent death before importing any routing backend."""

import ctypes
import importlib
import os
import signal
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from carl.core.native_routing import (
        NativeRoutingDataset,
        NativeRoutingMetric,
        NativeRoutingQuery,
    )


def arm_parent_death(expected_parent: int) -> None:
    if sys.platform != "linux" or os.getppid() != expected_parent:
        raise RuntimeError("Native routing parent ownership unavailable")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != expected_parent:
        raise RuntimeError("Native routing parent ownership unavailable")


def _quiet_output() -> None:
    descriptor = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(descriptor, 1)
        os.dup2(descriptor, 2)
    finally:
        os.close(descriptor)


class Backend(Protocol):
    def query(self, query: "NativeRoutingQuery") -> tuple["NativeRoutingMetric", ...]: ...


@dataclass(frozen=True, slots=True)
class _Settings:
    dataset_path: Path
    dataset_identifier: str
    maximum_snap_distance_meters: float
    use_mmap: bool


class _OsrmBackend:
    def __init__(self, dataset: "NativeRoutingDataset") -> None:
        # This import and engine construction happen only with a CPU guard held.
        from carl.io.osrm import OsrmRouter

        self.dataset = dataset
        self.router = OsrmRouter(
            _Settings(dataset.dataset_path, dataset.dataset_identifier, 5000, dataset.use_mmap)
        )

    def query(self, query: "NativeRoutingQuery") -> tuple["NativeRoutingMetric", ...]:
        self.router.configuration = _Settings(
            self.dataset.dataset_path,
            self.dataset.dataset_identifier,
            query.maximum_snap_distance_meters,
            self.dataset.use_mmap,
        )
        return self.router._table(query.origin, query.destinations)


def main() -> int:
    connection: socket.socket | None = None
    resident = -1
    try:
        descriptor, resident, expected_parent = (int(item) for item in sys.argv[1:4])
        arm_parent_death(expected_parent)
        _quiet_output()
        # No native imports may move above the parent-death fence.
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

        require_guard_fd(resident)
        os.set_inheritable(resident, False)
        connection = socket.socket(fileno=descriptor)
        message, descriptors = receive_packet(connection)
        close_fds(descriptors)
        if descriptors:
            raise NativeRoutingProtocolError
        require_keys(
            message, {"kind", "request_identifier", "dataset", "backend_module"}, kind="open"
        )
        dataset = NativeRoutingDataset.model_validate_json(encode_json(message["dataset"]))
        backend_module = message["backend_module"]
        backend: Backend | None = None
        send_packet(
            connection,
            {
                "kind": "ready",
                "request_identifier": message["request_identifier"],
                "worker_pid": os.getpid(),
            },
        )
        last_request = None
        while True:
            message, descriptors = receive_packet(connection)
            if len(descriptors) != 1:
                close_fds(descriptors)
                raise NativeRoutingProtocolError
            cpu = descriptors[0]
            try:
                require_guard_fd(cpu)
                query = decode_query(message)
                if query.request_identifier == last_request:
                    raise NativeRoutingProtocolError
                last_request = query.request_identifier
                send_packet(
                    connection, {"kind": "adopted", "request_identifier": query.request_identifier}
                )
                try:
                    if backend is None:
                        if backend_module is None:
                            backend = _OsrmBackend(dataset)
                        else:
                            module = importlib.import_module(backend_module)
                            backend = module.create_backend(dataset)
                    if backend is None:
                        raise RuntimeError("Native routing backend unavailable")
                    result = decode_metrics(list(backend.query(query)), len(query.destinations))
                except Exception:
                    result = ("osrm_unavailable",) * len(query.destinations)
            finally:
                # A done reply allows immediate parent stage finalization, so
                # the physical child must relinquish its CPU FD first.
                os.close(cpu)
            send_packet(
                connection,
                {
                    "kind": "result",
                    "request_identifier": query.request_identifier,
                    "metrics": result,
                },
            )
    except Exception:
        return 1
    finally:
        if connection is not None:
            connection.close()
        if resident >= 0:
            os.close(resident)


if __name__ == "__main__":
    raise SystemExit(main())
