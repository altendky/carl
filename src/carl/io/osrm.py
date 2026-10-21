"""Local, memory-mapped OSRM tables, with explicit map coverage checks."""

import importlib
import math
import os
from collections.abc import Callable, Iterator, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import anyio
import anyio.to_process

from carl.core.travel import TravelCoordinate

_BATCH_SIZE = 64
_QUERY_TIMEOUT_SECONDS = 60
type _SettingsIdentity = tuple[str, str, float, bool]
type _Coordinate = tuple[float, float]


@dataclass(frozen=True)
class _WorkerSettings:
    dataset_path: Path
    dataset_identifier: str
    maximum_snap_distance_meters: float
    use_mmap: bool


_native_router: "OsrmRouter | None" = None
_native_identity: _SettingsIdentity | None = None


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


def _native_table(
    settings: _SettingsIdentity, origin: _Coordinate, destinations: tuple[_Coordinate, ...]
) -> tuple[tuple[float, float] | str, ...]:
    """Picklable subprocess entry point; retain only one mapped dataset per child."""
    global _native_router, _native_identity
    with _quiet_native_output():
        if _native_router is None or _native_identity != settings:
            _native_router = OsrmRouter(
                _WorkerSettings(Path(settings[0]), settings[1], settings[2], settings[3])
            )
            _native_identity = settings
        return _native_router._table(
            TravelCoordinate(latitude=origin[0], longitude=origin[1]),
            tuple(TravelCoordinate(latitude=lat, longitude=lon) for lat, lon in destinations),
        )


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
    """Keep a warm engine in an AnyIO subprocess, isolated from the main GIL.

    OSRM 26.10 native bindings retain the GIL during construction and queries.
    Threads would therefore stall the main event loop despite the offload. AnyIO
    owns the process pool, cleans it up on shutdown, and kills a busy child when
    its caller is cancelled. Injected test engines use threads instead.

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
        self._limiter = anyio.CapacityLimiter(1)
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
                batch = destinations[start : start + _BATCH_SIZE]
                if self._native_process:
                    configuration = self.configuration
                    try:
                        with anyio.fail_after(_QUERY_TIMEOUT_SECONDS):
                            estimates = await anyio.to_process.run_sync(
                                _native_table,
                                (
                                    str(configuration.dataset_path),
                                    configuration.dataset_identifier,
                                    configuration.maximum_snap_distance_meters,
                                    configuration.use_mmap,
                                ),
                                (origin.latitude, origin.longitude),
                                tuple((item.latitude, item.longitude) for item in batch),
                                cancellable=True,
                                limiter=self._limiter,
                            )
                    except TimeoutError:
                        estimates = ("osrm_timeout",) * len(batch)
                    except (anyio.BrokenWorkerProcess, OSError, RuntimeError):
                        estimates = ("osrm_unavailable",) * len(batch)
                else:
                    estimates = await anyio.to_thread.run_sync(self._table, origin, batch)
                results.extend(estimates)
        return tuple(results)
