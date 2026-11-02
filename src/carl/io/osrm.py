"""Local, memory-mapped OSRM tables, with explicit map coverage checks."""

import importlib
import math
import os
from collections.abc import Callable, Iterator, Sequence
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, Protocol

import anyio

from carl.core.native_routing import NativeRoutingDataset
from carl.core.travel import TravelCoordinate
from carl.io.native_routing_pool import (
    NativeRoutingBorrowExpired,
    close_unmanaged_native_routing,
    ephemeral_native_query,
    native_routing_pool,
)
from carl.io.resource_permits import current_resource_manager, run_cpu

_BATCH_SIZE = 64
_QUERY_TIMEOUT_SECONDS = 60


@contextmanager
def _quiet_native_output() -> Iterator[None]:
    """Protect AnyIO's stdout protocol from C++ logging to native descriptors."""
    with ExitStack() as stack:
        null = os.open(os.devnull, os.O_WRONLY)
        stack.callback(os.close, null)
        stdout = os.dup(1)
        stack.callback(os.close, stdout)
        stderr = os.dup(2)
        stack.callback(os.close, stderr)
        try:
            os.dup2(null, 1)
            os.dup2(null, 2)
            yield
        finally:
            os.dup2(stdout, 1)
            os.dup2(stderr, 2)


class OsrmSettings(Protocol):
    @property
    def dataset_path(self) -> Path: ...

    @property
    def dataset_identifier(self) -> str: ...

    @property
    def maximum_snap_distance_meters(self) -> float: ...

    @property
    def use_mmap(self) -> bool: ...


def _bindings() -> Any:
    # Lazy import keeps reading configuration and ORS-only deployments lightweight.
    return importlib.import_module("osrm")


def _metric(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value) if math.isfinite(value) and value >= 0 else None


def _array(value: Any, size: int) -> list[Any]:
    """Accept native binding Array wrappers without accepting JSON objects as rows."""
    if isinstance(value, str | bytes | dict) or len(value) != size:
        raise ValueError("Invalid OSRM response shape")
    return [value[index] for index in range(size)]


class OsrmRouter:
    """Use an owned native engine whose warm residency is separate from CPU work.

    Managed runtimes share a bounded pool across router instances. Outside a
    managed database, each call uses a serial ephemeral supervised process.
    Injected test bindings run in non-abandoning CPU-admitted threads.

    Dataset files must be immutable while mapped. Publish a rebuilt dataset under
    a new prefix and identifier, then reload configuration. The identifier also
    lets callers invalidate their route caches when road data changes.
    """

    def __init__(
        self,
        configuration: OsrmSettings,
        *,
        bindings_loader: Callable[[], Any] = _bindings,
    ) -> None:
        self.configuration = configuration
        self.bindings_loader = bindings_loader
        self._native_process = bindings_loader is _bindings
        self._lock = anyio.Lock()
        self._engine: Any = None
        self._module: Any = None
        self._engine_identity: tuple[Path, str, bool] | None = None

    def _load_engine(self) -> None:
        configuration = self.configuration
        identity = (
            configuration.dataset_path,
            configuration.dataset_identifier,
            configuration.use_mmap,
        )
        if self._engine_identity == identity:
            return
        module = self.bindings_loader()
        engine = module.OSRM(
            storage_config=str(configuration.dataset_path),
            algorithm="MLD",
            use_shared_memory=False,
            use_mmap=configuration.use_mmap,
            max_locations_distance_table=_BATCH_SIZE + 1,
        )
        self._engine = engine
        self._module = module
        self._engine_identity = identity

    def _table(
        self, origin: TravelCoordinate, destinations: Sequence[TravelCoordinate]
    ) -> tuple[tuple[float, float] | str, ...]:
        try:
            if (
                self.bindings_loader is _bindings
                and not self.configuration.dataset_path.with_suffix(".osrm.mldgr").is_file()
            ):
                return ("osrm_dataset_missing",) * len(destinations)
            self._load_engine()
            coordinates = [
                (coordinate.longitude, coordinate.latitude)
                for coordinate in (origin, *destinations)
            ]
            parameters = self._module.TableParameters(
                coordinates=coordinates,
                sources=[0],
                destinations=list(range(1, len(coordinates))),
                annotations=["distance", "duration"],
                generate_hints=False,
            )
            result = self._engine.Table(parameters)
            code = result["code"]
            if code != "Ok":
                reason = (
                    "location_outside_coverage"
                    if code == "NoSegment"
                    else "no_route"
                    if code == "NoTable"
                    else "osrm_invalid_response"
                )
                return (reason,) * len(destinations)
            count = len(destinations)
            sources = _array(result["sources"], 1)
            snapped_destinations = _array(result["destinations"], count)
            distances = _array(_array(result["distances"], 1)[0], count)
            durations = _array(_array(result["durations"], 1)[0], count)
            origin_snap = _metric(sources[0]["distance"])
            if origin_snap is None:
                return ("osrm_invalid_response",) * count
            maximum_snap = self.configuration.maximum_snap_distance_meters
            if origin_snap > maximum_snap:
                return ("location_outside_coverage",) * count
            estimates: list[tuple[float, float] | str] = []
            for waypoint, distance, duration in zip(
                snapped_destinations, distances, durations, strict=True
            ):
                snap = _metric(waypoint["distance"])
                if snap is None:
                    estimates.append("osrm_invalid_response")
                elif snap > maximum_snap:
                    estimates.append("location_outside_coverage")
                elif distance is None and duration is None:
                    estimates.append("no_route")
                else:
                    meters, seconds = _metric(distance), _metric(duration)
                    estimates.append(
                        (meters, seconds)
                        if meters is not None and seconds is not None
                        else "osrm_invalid_response"
                    )
            return tuple(estimates)
        except (KeyError, IndexError, TypeError, ValueError, OverflowError):
            # Never expose dataset paths, coordinates or native exception messages.
            return ("osrm_invalid_response",) * len(destinations)
        except RuntimeError as error:
            # Native bindings throw RuntimeError(code + " - " + message) for
            # non-Ok statuses. Read only allowlisted codes; discard the message.
            reason = {
                "NoSegment": "location_outside_coverage",
                "NoTable": "no_route",
            }.get(str(error).partition(" - ")[0], "osrm_unavailable")
            return (reason,) * len(destinations)
        except Exception:
            return ("osrm_unavailable",) * len(destinations)

    async def estimates(
        self, origin: TravelCoordinate, destinations: Sequence[TravelCoordinate]
    ) -> tuple[tuple[float, float] | str, ...]:
        if not destinations:
            return ()
        results: list[tuple[float, float] | str] = []
        async with self._lock:
            for start in range(0, len(destinations), _BATCH_SIZE):
                batch = tuple(destinations[start : start + _BATCH_SIZE])
                if self._native_process:
                    configuration = self.configuration
                    dataset = NativeRoutingDataset(
                        dataset_path=configuration.dataset_path,
                        dataset_identifier=configuration.dataset_identifier,
                        use_mmap=configuration.use_mmap,
                    )
                    try:
                        with anyio.fail_after(_QUERY_TIMEOUT_SECONDS):
                            manager = current_resource_manager()
                            if manager is None:
                                estimates = await ephemeral_native_query(
                                    dataset,
                                    origin,
                                    batch,
                                    maximum_snap_distance_meters=configuration.maximum_snap_distance_meters,
                                    timeout_seconds=_QUERY_TIMEOUT_SECONDS,
                                )
                            else:
                                while True:
                                    try:
                                        async with native_routing_pool(manager).borrow(
                                            dataset
                                        ) as borrow:
                                            estimates = await borrow.query(
                                                origin,
                                                batch,
                                                maximum_snap_distance_meters=configuration.maximum_snap_distance_meters,
                                                timeout_seconds=_QUERY_TIMEOUT_SECONDS,
                                            )
                                        break
                                    except NativeRoutingBorrowExpired:
                                        # The CPU decorator has unwound before another residency wait.
                                        continue
                    except TimeoutError:
                        estimates = ("osrm_timeout",) * len(batch)
                    except (OSError, RuntimeError):
                        estimates = ("osrm_unavailable",) * len(batch)
                else:
                    estimates = await run_cpu(self._table, origin, batch)
                results.extend(estimates)
        return tuple(results)

    async def aclose(self) -> None:
        """Managed engines belong to the root pool; standalone calls close eagerly."""
        async with self._lock:
            if current_resource_manager() is None:
                await close_unmanaged_native_routing()
            self._engine = self._module = None
            self._engine_identity = None
