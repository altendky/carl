"""Routing uses road metrics, bounded lookups, private caches and explicit failures."""

import json
import sqlite3
import stat
from pathlib import Path

import httpx
import pytest

from carl._tests.test_configuration import _write_configuration
from carl.core.travel import (
    TravelCoordinate,
    TravelDestination,
    TravelFilters,
    travel_matches_filters,
)
from carl.io.paths import CarlDirectories
from carl.io.routing import ConfiguredTravelService, RoutingCache


def _configured(
    tmp_path: Path, *, label_origin: bool = False, geocode_budget: int = 20
) -> tuple[CarlDirectories, Path]:
    directories, path = _write_configuration(tmp_path)
    settings = f'\n[openrouteservice]\ncredential_id="test"\nmaximum_geocodes_per_batch={geocode_budget}\ncountry_code="US"\n'
    settings += (
        'origin_label="Example City, ZZ"\n'
        if label_origin
        else "[openrouteservice.origin]\nlatitude=10.0\nlongitude=20.0\n"
    )
    with path.open("a") as config:
        _ = config.write(settings)
    (directories.config / "private").mkdir(mode=0o700)
    directories.openrouteservice_credential_directory.mkdir(mode=0o700)
    credential = directories.openrouteservice_credential_directory / "test.api-key"
    _ = credential.write_text("test-secret-not-a-real-key")
    credential.chmod(0o600)
    return directories, path


def _destination(
    latitude: float = 11.0, *, label: str = "Example destination"
) -> TravelDestination:
    return TravelDestination(
        coordinate=TravelCoordinate(latitude=latitude, longitude=21.0),
        label=label,
        precision="source_approximate",
    )


class Clock:
    value = 1_000_000.0

    def now(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.value += seconds


@pytest.mark.anyio
async def test_matrix_batches_deduplicates_caches_and_scopes_origin(tmp_path: Path) -> None:
    directories, path = _configured(tmp_path)
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == "test-secret-not-a-real-key"
        assert "test-secret" not in str(request.url)
        payload = json.loads(request.content)
        assert payload["sources"] == ["0"]
        return httpx.Response(
            200,
            json={
                "distances": [[16093.44] * len(payload["destinations"])],
                "durations": [[900.0] * len(payload["destinations"])],
            },
        )

    clock = Clock()
    service = ConfiguredTravelService(
        directories=directories,
        transport=httpx.MockTransport(respond),
        now=clock.now,
        sleep=clock.sleep,
    )
    destinations = (_destination(), _destination(label="Same coordinate"), _destination(12.0))
    first = await service.estimate_many(destinations)
    assert len(requests) == 1
    assert json.loads(requests[0].content)["locations"] == [
        [20.0, 10.0],
        [21.0, 11.0],
        [21.0, 12.0],
    ]
    assert [item.status for item in first] == ["ready"] * 3
    assert first[1].destination_label == "Same coordinate"
    assert first[0].approximate
    assert travel_matches_filters(first[0], TravelFilters(maximum_driving_distance_miles=10))
    assert not travel_matches_filters(first[0], TravelFilters(maximum_driving_duration_minutes=14))
    assert await service.estimate_many(destinations) == first
    assert len(requests) == 1
    assert stat.S_IMODE(directories.routing_cache_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(directories.routing_cache_file.parent.stat().st_mode) == 0o700
    assert b"test-secret" not in directories.routing_cache_file.read_bytes()

    identity = await service.origin_identity()
    _ = path.write_text(path.read_text().replace("latitude=10.0", "latitude=9.0"))
    assert await service.origin_identity() != identity
    _ = await service.estimate_many(destinations)
    assert len(requests) == 2

    clock.value += 604_801
    _ = await service.estimate_many(destinations)
    assert len(requests) == 3


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ("none", "quota", "network", "malformed"))
async def test_provider_failures_stay_unknown_and_never_become_zero_distance(
    tmp_path: Path, kind: str
) -> None:
    directories, _ = _configured(tmp_path)

    def respond(request: httpx.Request) -> httpx.Response:
        if kind == "quota":
            return httpx.Response(429, json={"error": "ignored response body"})
        if kind == "network":
            raise httpx.ConnectError("must not be exposed", request=request)
        if kind == "malformed":
            return httpx.Response(200, text="not json")
        return httpx.Response(200, json={"distances": [[None]], "durations": [[None]]})

    service = ConfiguredTravelService(
        directories=directories, transport=httpx.MockTransport(respond)
    )
    estimate = (await service.estimate_many((_destination(),)))[0]
    assert estimate.status == "unavailable"
    assert estimate.unavailable_reason is not None
    assert estimate.distance_meters is None and estimate.duration_seconds is None
    assert travel_matches_filters(estimate, TravelFilters(maximum_driving_distance_miles=10))
    assert not travel_matches_filters(
        estimate, TravelFilters(maximum_driving_distance_miles=10, include_unknown=False)
    )
    assert "must not be exposed" not in estimate.model_dump_json()
    assert "ignored response body" not in estimate.model_dump_json()


def _geocode_feature(
    longitude: float, latitude: float, confidence: float = 1.0
) -> dict[str, object]:
    return {
        "geometry": {"coordinates": [longitude, latitude]},
        "properties": {"confidence": confidence},
    }


@pytest.mark.anyio
async def test_geocoding_is_bounded_cached_and_rejects_ambiguity(tmp_path: Path) -> None:
    directories, _ = _configured(tmp_path, geocode_budget=1)
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/geocode/search":
            assert request.url.params["boundary.country"] == "US"
            assert "api_key" not in request.url.params
            return httpx.Response(200, json={"features": [_geocode_feature(21.0, 11.0)]})
        return httpx.Response(200, json={"distances": [[1000.0]], "durations": [[100.0]]})

    clock = Clock()
    service = ConfiguredTravelService(
        directories=directories,
        transport=httpx.MockTransport(respond),
        now=clock.now,
        sleep=clock.sleep,
    )
    destinations = (
        TravelDestination(label="Example Town, ZZ"),
        TravelDestination(label="Different Town, ZZ"),
    )
    estimates = await service.estimate_many(destinations)
    assert estimates[0].status == "ready"
    assert estimates[0].location_precision == "geocoded_locality"
    assert estimates[1].unavailable_reason == "geocode_budget_exhausted"
    assert calls == ["/geocode/search", "/v2/matrix/driving-car"]
    _ = await service.estimate_many((destinations[0],))
    assert len(calls) == 2

    def ambiguous(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"features": [_geocode_feature(22.0, 12.0), _geocode_feature(23.0, 13.0)]}
        )

    service = ConfiguredTravelService(
        directories=directories,
        transport=httpx.MockTransport(ambiguous),
        now=clock.now,
        sleep=clock.sleep,
    )
    estimate = (await service.estimate_many((destinations[1],)))[0]
    assert estimate.unavailable_reason == "ambiguous_location"


@pytest.mark.anyio
async def test_conflicting_and_missing_locations_make_no_requests(tmp_path: Path) -> None:
    directories, _ = _configured(tmp_path)

    def forbidden(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Unexpected provider request")

    service = ConfiguredTravelService(
        directories=directories, transport=httpx.MockTransport(forbidden)
    )
    results = await service.estimate_many(
        (
            TravelDestination(
                label="Conflicting town", unavailable_reason="conflicting_coordinates"
            ),
            TravelDestination(),
        )
    )
    assert [result.unavailable_reason for result in results] == [
        "conflicting_coordinates",
        "location_unavailable",
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "matrix",
    ({"x": 1}, "x", [[1]], [True], [-1], [float("inf")]),
)
async def test_malformed_matrix_rows_are_explicit_unknowns(tmp_path: Path, matrix: object) -> None:
    directories, _ = _configured(tmp_path)

    def respond(request: httpx.Request) -> httpx.Response:
        # A raw response also exercises nonfinite provider numbers, which httpx's
        # JSON encoder deliberately refuses to generate.
        return httpx.Response(200, text=json.dumps({"distances": [matrix], "durations": [matrix]}))

    service = ConfiguredTravelService(
        directories=directories, transport=httpx.MockTransport(respond)
    )
    estimate = (await service.estimate_many((_destination(),)))[0]
    assert estimate.status == "unavailable"
    assert estimate.unavailable_reason == "provider_invalid_response"
    assert estimate.distance_meters is None and estimate.duration_seconds is None


@pytest.mark.anyio
async def test_resolved_origin_changes_cursor_identity_after_geocode_expiry(tmp_path: Path) -> None:
    directories, _ = _configured(tmp_path, label_origin=True)
    clock = Clock()
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"features": [_geocode_feature(20.0, 10.0 + calls)]})

    service = ConfiguredTravelService(
        directories=directories,
        transport=httpx.MockTransport(respond),
        now=clock.now,
        sleep=clock.sleep,
    )
    first = await service.origin_identity()
    assert await service.origin_identity() == first
    assert calls == 1
    clock.value += 30 * 86400 + 1
    assert await service.origin_identity() != first
    assert calls == 2


@pytest.mark.anyio
async def test_shorter_configured_ttl_does_not_reuse_long_lived_route(tmp_path: Path) -> None:
    directories, path = _configured(tmp_path)
    clock = Clock()
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"distances": [[1000 * calls]], "durations": [[100]]})

    service = ConfiguredTravelService(
        directories=directories,
        transport=httpx.MockTransport(respond),
        now=clock.now,
        sleep=clock.sleep,
    )
    identity = await service.origin_identity()
    assert (await service.estimate_many((_destination(),)))[0].distance_meters == 1000
    _ = path.write_text(
        path.read_text().replace(
            'credential_id="test"', 'credential_id="test"\ncache_ttl_seconds=60'
        )
    )
    assert await service.origin_identity() != identity
    assert (await service.estimate_many((_destination(),)))[0].distance_meters == 2000
    assert calls == 2


@pytest.mark.anyio
async def test_explicit_refresh_bypasses_cache_and_checks_frozen_origin(tmp_path: Path) -> None:
    directories, path = _configured(tmp_path)
    clock = Clock()
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"distances": [[1000 * calls]], "durations": [[100]]})

    service = ConfiguredTravelService(
        directories=directories,
        transport=httpx.MockTransport(respond),
        now=clock.now,
        sleep=clock.sleep,
    )
    origin = await service.origin_identity()
    first = await service.estimate_many((_destination(),))
    assert (await service.estimate_many((_destination(),))) == first
    refreshed = await service.refresh_many((_destination(),), expected_origin_identity=origin)
    assert refreshed[0].distance_meters == 2000
    assert calls == 2
    assert await service.estimate_many((_destination(),)) == refreshed
    _ = path.write_text(path.read_text().replace("latitude=10.0", "latitude=9.0"))
    changed = await service.refresh_many((_destination(),), expected_origin_identity=origin)
    assert changed[0].unavailable_reason == "origin_changed"
    assert calls == 2


@pytest.mark.anyio
async def test_cache_prunes_expired_rows_in_bounded_batches(tmp_path: Path) -> None:
    directories, _ = _configured(tmp_path)
    clock = Clock()
    cache = RoutingCache(directories, now=clock.now)
    for index in range(250):
        await cache.put(str(index), {}, clock.now() + 100)
    clock.value += 101
    await cache.put("fresh", {}, clock.now() + 100)
    with sqlite3.connect(directories.routing_cache_file) as connection:
        assert connection.execute("SELECT count(*) FROM estimates").fetchone()[0] == 151
    assert await cache.get("fresh", clock.now()) == {}
    assert await cache.get("249", clock.now()) is None
