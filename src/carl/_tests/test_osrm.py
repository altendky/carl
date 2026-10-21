"""The local router preserves native metrics and rejects excessive map snapping."""

import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio
import anyio.to_process
import pytest

from carl.core.travel import TravelCoordinate
from carl.io.osrm import OsrmRouter, _quiet_native_output


@dataclass
class Settings:
    dataset_path: Path
    dataset_identifier: str = "fixture-v1"
    maximum_snap_distance_meters: float = 5000
    use_mmap: bool = True


class Array:
    """OSRM returns wrappers with indexing rather than Python list subclasses."""

    def __init__(self, values: list[Any]) -> None:
        self.values = values

    def __len__(self) -> int:
        return len(self.values)

    def __getitem__(self, index: int) -> Any:
        return self.values[index]


def _response(count: int) -> dict[str, Any]:
    return {
        "code": "Ok",
        "sources": Array([{"distance": 2.0}]),
        "destinations": Array([{"distance": 3.0} for _ in range(count)]),
        "distances": Array([Array([1000.0] * count)]),
        "durations": Array([Array([60.0] * count)]),
    }


class Bindings:
    def __init__(self) -> None:
        self.engines: list[dict[str, Any]] = []
        self.tables: list[dict[str, Any]] = []
        self.response: Any = None
        self.thread_ids: list[int] = []

    def OSRM(self, **kwargs: Any) -> "Bindings":
        self.thread_ids.append(threading.get_ident())
        self.engines.append(kwargs)
        return self

    def TableParameters(self, **kwargs: Any) -> dict[str, Any]:
        return kwargs

    def Table(self, parameters: dict[str, Any]) -> Any:
        self.thread_ids.append(threading.get_ident())
        self.tables.append(parameters)
        return (
            _response(len(parameters["destinations"])) if self.response is None else self.response
        )


ORIGIN = TravelCoordinate(latitude=10, longitude=20)
DESTINATION = TravelCoordinate(latitude=11, longitude=21)


def test_native_output_is_suppressed_and_protocol_descriptors_restored(
    capfd: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(RuntimeError), _quiet_native_output():
        _ = os.write(1, b"native stdout must not corrupt protocol\n")
        _ = os.write(2, b"native stderr\n")
        raise RuntimeError("restore also on errors")
    assert capfd.readouterr() == ("", "")
    _ = os.write(1, b"protocol restored\n")
    _ = os.write(2, b"stderr restored\n")
    assert capfd.readouterr() == ("protocol restored\n", "stderr restored\n")


@pytest.mark.anyio
async def test_local_tables_are_warm_bounded_mapped_and_offloaded(tmp_path: Path) -> None:
    settings = Settings(tmp_path / "roads.osrm")
    bindings = Bindings()
    router = OsrmRouter(settings, bindings_loader=lambda: bindings)
    assert await router.estimates(ORIGIN, ()) == ()
    assert not bindings.engines
    results = await router.estimates(ORIGIN, [DESTINATION] * 130)
    assert results == ((1000.0, 60.0),) * 130
    assert len(bindings.engines) == 1
    assert bindings.engines[0] == {
        "storage_config": str(settings.dataset_path),
        "algorithm": "MLD",
        "use_shared_memory": False,
        "use_mmap": True,
        "max_locations_distance_table": 65,
    }
    assert [len(table["destinations"]) for table in bindings.tables] == [64, 64, 2]
    table = bindings.tables[-1]
    assert table["coordinates"] == [(20.0, 10.0), (21.0, 11.0), (21.0, 11.0)]
    assert table["sources"] == [0]
    assert table["destinations"] == [1, 2]
    assert table["annotations"] == ["distance", "duration"]
    assert table["generate_hints"] is False
    assert "fallback_speed" not in table
    assert set(bindings.thread_ids).isdisjoint({threading.get_ident()})
    assert await router.estimates(ORIGIN, [DESTINATION]) == ((1000.0, 60.0),)
    assert len(bindings.engines) == 1
    settings.dataset_identifier = "fixture-v2"
    _ = await router.estimates(ORIGIN, [DESTINATION])
    assert len(bindings.engines) == 2


@pytest.mark.anyio
async def test_stalled_native_query_is_bounded_and_explicit_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def stalled(*args: Any, **kwargs: Any) -> Any:
        await anyio.sleep_forever()

    monkeypatch.setattr("carl.io.osrm._QUERY_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr("anyio.to_process.run_sync", stalled)
    router = OsrmRouter(Settings(tmp_path / "roads.osrm"))
    assert await router.estimates(ORIGIN, [DESTINATION]) == ("osrm_timeout",)


@pytest.mark.anyio
async def test_snap_guard_rejects_only_outside_destinations_and_all_if_origin_outside(
    tmp_path: Path,
) -> None:
    bindings = Bindings()
    response = _response(3)
    response["destinations"] = Array([{"distance": 5}, {"distance": 5001}, {"distance": 5000}])
    bindings.response = response
    router = OsrmRouter(Settings(tmp_path / "roads.osrm"), bindings_loader=lambda: bindings)
    assert await router.estimates(ORIGIN, [DESTINATION] * 3) == (
        (1000.0, 60.0),
        "location_outside_coverage",
        (1000.0, 60.0),
    )
    response["sources"] = Array([{"distance": 5001}])
    assert await router.estimates(ORIGIN, [DESTINATION] * 3) == ("location_outside_coverage",) * 3


@pytest.mark.anyio
async def test_null_routes_have_no_crow_flies_fallback(tmp_path: Path) -> None:
    bindings = Bindings()
    response = _response(2)
    response["distances"] = Array([Array([0, None])])
    response["durations"] = Array([Array([0, None])])
    bindings.response = response
    router = OsrmRouter(Settings(tmp_path / "roads.osrm"), bindings_loader=lambda: bindings)
    assert await router.estimates(ORIGIN, [DESTINATION] * 2) == ((0.0, 0.0), "no_route")


@pytest.mark.anyio
async def test_partial_null_metric_is_invalid_instead_of_no_route(tmp_path: Path) -> None:
    bindings = Bindings()
    bindings.response = _response(1)
    bindings.response["distances"] = Array([Array([None])])
    router = OsrmRouter(Settings(tmp_path / "fixture.osrm"), bindings_loader=lambda: bindings)
    assert await router.estimates(ORIGIN, [DESTINATION]) == ("osrm_invalid_response",)


@pytest.mark.parametrize(
    ("code", "reason"),
    [
        ("NoSegment", "location_outside_coverage"),
        ("NoTable", "no_route"),
        ("InvalidValue", "osrm_invalid_response"),
    ],
)
@pytest.mark.anyio
async def test_provider_error_codes_are_sanitized(tmp_path: Path, code: str, reason: str) -> None:
    bindings = Bindings()
    bindings.response = {"code": code, "message": "sensitive paths and location"}
    router = OsrmRouter(Settings(tmp_path / "roads.osrm"), bindings_loader=lambda: bindings)
    assert await router.estimates(ORIGIN, [DESTINATION]) == (reason,)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("distances", [[True]]),
        ("distances", [[float("nan")]]),
        ("distances", [[-1]]),
        ("distances", [["123"]]),
        ("distances", [{"x": 1}]),
        ("distances", ["x"]),
        ("durations", [[]]),
        ("sources", [{"distance": None}]),
        ("destinations", [{"distance": False}]),
    ],
)
@pytest.mark.anyio
async def test_malformed_response_is_unknown(tmp_path: Path, field: str, value: Any) -> None:
    bindings = Bindings()
    response = _response(1)
    response[field] = value
    bindings.response = response
    router = OsrmRouter(Settings(tmp_path / "roads.osrm"), bindings_loader=lambda: bindings)
    assert await router.estimates(ORIGIN, [DESTINATION]) == ("osrm_invalid_response",)


@pytest.mark.anyio
async def test_missing_bindings_or_dataset_is_sanitized(tmp_path: Path) -> None:
    def fail() -> Any:
        raise RuntimeError("/private/path to missing dataset")

    router = OsrmRouter(Settings(tmp_path / "roads.osrm"), bindings_loader=fail)
    assert await router.estimates(ORIGIN, [DESTINATION]) == ("osrm_unavailable",)


@pytest.mark.parametrize(
    ("message", "reason"),
    [
        ("NoSegment - sensitive coordinate", "location_outside_coverage"),
        ("NoTable - sensitive location", "no_route"),
        ("Missing dataset /private/path", "osrm_unavailable"),
        ("NoSegment sensitive path", "osrm_unavailable"),
    ],
)
@pytest.mark.anyio
async def test_native_runtime_error_codes_are_sanitized(
    tmp_path: Path, message: str, reason: str
) -> None:
    class Failure(Bindings):
        def Table(self, parameters: dict[str, Any]) -> Any:
            raise RuntimeError(message)

    router = OsrmRouter(Settings(tmp_path / "roads.osrm"), bindings_loader=Failure)
    assert await router.estimates(ORIGIN, [DESTINATION]) == (reason,)


@pytest.mark.anyio
async def test_native_process_uses_bounded_cancellable_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []

    async def process(function: Any, *args: Any, **kwargs: Any) -> Any:
        calls.append({"arguments": args, **kwargs})
        return ((1.0, 2.0),) * len(args[2])

    monkeypatch.setattr(anyio.to_process, "run_sync", process)
    router = OsrmRouter(Settings(tmp_path / "roads.osrm"))
    assert await router.estimates(ORIGIN, [DESTINATION] * 65) == ((1.0, 2.0),) * 65
    assert len(calls) == 2
    assert calls[0]["cancellable"] is True
    assert calls[0]["limiter"].total_tokens == 1
    assert calls[0]["limiter"] is calls[1]["limiter"]
    assert calls[0]["arguments"][0] == (
        str(tmp_path / "roads.osrm"),
        "fixture-v1",
        5000,
        True,
    )
    assert calls[0]["arguments"][1] == (10.0, 20.0)


@pytest.mark.anyio
async def test_native_process_failure_is_sanitized_and_cancellation_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def broken(*args: Any, **kwargs: Any) -> Any:
        raise anyio.BrokenWorkerProcess("sensitive details")

    monkeypatch.setattr(anyio.to_process, "run_sync", broken)
    router = OsrmRouter(Settings(tmp_path / "roads.osrm"))
    assert await router.estimates(ORIGIN, [DESTINATION]) == ("osrm_unavailable",)

    # Trio's Cancelled constructor is intentionally private; request cancellation
    # through its public scope rather than manufacturing the exception.
    async def wait(*args: Any, **kwargs: Any) -> Any:
        await anyio.sleep_forever()

    monkeypatch.setattr(anyio.to_process, "run_sync", wait)
    with anyio.move_on_after(0.01) as scope:
        _ = await router.estimates(ORIGIN, [DESTINATION])
        pytest.fail("Cancellation was swallowed")
    assert scope.cancel_called


@pytest.mark.anyio
async def test_parallel_callers_share_one_serial_engine(tmp_path: Path) -> None:
    class DetectParallel(Bindings):
        active = 0
        maximum = 0

        def Table(self, parameters: dict[str, Any]) -> Any:
            self.active += 1
            self.maximum = max(self.maximum, self.active)
            time.sleep(0.01)
            result = super().Table(parameters)
            self.active -= 1
            return result

    bindings = DetectParallel()
    router = OsrmRouter(Settings(tmp_path / "roads.osrm"), bindings_loader=lambda: bindings)

    async def estimate() -> None:
        assert await router.estimates(ORIGIN, [DESTINATION]) == ((1000.0, 60.0),)

    async with anyio.create_task_group() as group:
        for _ in range(8):
            group.start_soon(estimate)
    assert bindings.maximum == 1
    assert len(bindings.engines) == 1


@pytest.mark.anyio
async def test_native_mld_bindings_on_tiny_local_road_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise the installed wrapper types and MLD distance annotations end to end."""
    osrm = pytest.importorskip("osrm")
    package_path = Path(osrm.__file__).parent
    source = tmp_path / "roads.osm"
    _ = source.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<osm version="0.6" generator="carl-test">'
        '<node id="1" lat="43.73" lon="7.41"/>'
        '<node id="2" lat="43.73" lon="7.42"/>'
        '<node id="3" lat="43.74" lon="7.42"/>'
        '<way id="10"><nd ref="1"/><nd ref="2"/><nd ref="3"/>'
        '<tag k="highway" v="residential"/><tag k="maxspeed" v="50"/>'
        "</way></osm>"
    )
    prefix = source.with_suffix(".osrm")

    def build() -> None:
        commands = [
            ["extract", str(source), "-p", str(package_path / "share/profiles/car.lua")],
            ["partition", str(prefix)],
            ["customize", str(prefix)],
        ]
        for arguments in commands:
            _ = subprocess.run(
                [sys.executable, "-m", "osrm", *arguments, "--threads", "1"],
                check=True,
                capture_output=True,
                timeout=30,
            )

    await anyio.to_thread.run_sync(build)

    def parent_engine_forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Native graph loaded in main process")

    monkeypatch.setattr(osrm, "OSRM", parent_engine_forbidden)
    router = OsrmRouter(Settings(prefix, maximum_snap_distance_meters=100))
    heartbeat_ticks = 0
    results: list[tuple[float, float] | str] = []

    async def heartbeat() -> None:
        nonlocal heartbeat_ticks
        while True:
            heartbeat_ticks += 1
            await anyio.sleep(0.001)

    async with anyio.create_task_group() as group:
        group.start_soon(heartbeat)
        results.extend(
            await router.estimates(
                TravelCoordinate(latitude=43.73, longitude=7.41),
                [
                    TravelCoordinate(latitude=43.74, longitude=7.42),
                    TravelCoordinate(latitude=44.74, longitude=7.42),
                ],
            )
        )
        group.cancel_scope.cancel()
    assert heartbeat_ticks > 1
    first = results[0]
    assert isinstance(first, tuple)
    assert 1500 < first[0] < 2500
    assert first[1] > 0
    assert results[1] == "location_outside_coverage"
