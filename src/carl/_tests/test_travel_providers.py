"""Backend selection isolates caches and never requires hosted routing for OSRM."""

from collections.abc import Sequence
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from carl._tests.test_configuration import _write_configuration
from carl._tests.test_travel_service import Clock, _configured, _destination
from carl.core.configuration import OsrmConfiguration
from carl.core.travel import TravelCoordinate, TravelDestination
from carl.io.configuration import ConfigurationLoadFailure, load_configuration
from carl.io.routing import ConfiguredTravelService


def _local_settings(path: Path, *, geocoding: str = "disabled") -> str:
    return f'''
[travel]
provider = "osrm"
geocoding_provider = "{geocoding}"
origin = {{ latitude = 10.0, longitude = 20.0 }}
[osrm]
dataset_path = "{path.parent / "fixture.osrm"}"
dataset_identifier = "test-map-v1"
'''


class LocalRouter:
    def __init__(self, settings: OsrmConfiguration):
        self.settings = settings
        self.calls = 0

    async def estimates(
        self, origin: TravelCoordinate, destinations: Sequence[TravelCoordinate]
    ) -> tuple[tuple[float, float] | str, ...]:
        assert origin == TravelCoordinate(latitude=10, longitude=20)
        self.calls += 1
        return tuple((1000.0 * self.calls, 100.0) for _ in destinations)


def _offline(request: httpx.Request) -> httpx.Response:
    raise AssertionError("Coordinate-based OSRM routing must not access the network")


@pytest.mark.anyio
async def test_local_provider_works_without_api_key_and_scopes_cache_by_dataset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directories, path = _write_configuration(tmp_path)
    _ = path.write_text(path.read_text() + _local_settings(path))
    monkeypatch.setattr("carl.io.routing.OsrmRouter", LocalRouter)
    clock = Clock()
    service = ConfiguredTravelService(
        directories=directories,
        transport=httpx.MockTransport(_offline),
        now=clock.now,
        sleep=clock.sleep,
    )
    identity = await service.origin_identity()
    first = await service.estimate_many((_destination(), _destination()))
    assert [item.provider for item in first] == ["osrm", "osrm"]
    assert all(
        item.distance_meters == 1000 and item.dataset_identifier == "test-map-v1" for item in first
    )
    assert await service.estimate_many((_destination(), _destination())) == first
    refreshed = await service.refresh_many((_destination(),), expected_origin_identity=identity)
    assert refreshed[0].distance_meters == 2000
    _ = path.write_text(path.read_text().replace("test-map-v1", "test-map-v2"))
    assert await service.origin_identity() != identity
    changed = await service.estimate_many((_destination(),))
    assert changed[0].dataset_identifier == "test-map-v2"
    assert changed[0].distance_meters == 1000
    assert (await service.refresh_many((_destination(),), expected_origin_identity=identity))[
        0
    ].unavailable_reason == "origin_changed"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "geocoding,reason",
    [("disabled", "geocoding_disabled"), ("openrouteservice", "geocoding_not_configured")],
)
async def test_unknown_localities_do_not_disable_local_coordinate_routes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, geocoding: str, reason: str
) -> None:
    directories, path = _write_configuration(tmp_path)
    _ = path.write_text(path.read_text() + _local_settings(path, geocoding=geocoding))
    monkeypatch.setattr("carl.io.routing.OsrmRouter", LocalRouter)
    service = ConfiguredTravelService(
        directories=directories, transport=httpx.MockTransport(_offline)
    )
    results = await service.estimate_many(
        (TravelDestination(label="Example City, ZZ"), _destination())
    )
    assert results[0].provider == "osrm" and results[0].unavailable_reason == reason
    assert results[1].provider == "osrm" and results[1].status == "ready"


@pytest.mark.anyio
async def test_switching_backends_never_selects_another_providers_cached_metrics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directories, path = _configured(tmp_path)
    original = path.read_text()
    calls = []

    def hosted(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"distances": [[5000]], "durations": [[500]]})

    monkeypatch.setattr("carl.io.routing.OsrmRouter", LocalRouter)
    clock = Clock()
    service = ConfiguredTravelService(
        directories=directories,
        transport=httpx.MockTransport(hosted),
        now=clock.now,
        sleep=clock.sleep,
    )
    hosted_identity = await service.origin_identity()
    hosted_result = await service.estimate_many((_destination(),))
    assert hosted_result[0].provider == "openrouteservice"
    assert hosted_result[0].distance_meters == 5000
    _ = path.write_text(original + _local_settings(path))
    assert await service.origin_identity() != hosted_identity
    local_result = await service.estimate_many((_destination(),))
    assert local_result[0].provider == "osrm" and local_result[0].distance_meters == 1000
    _ = path.write_text(original)
    assert await service.origin_identity() == hosted_identity
    assert await service.estimate_many((_destination(),)) == hosted_result
    assert service.osrm_router is None
    assert len(calls) == 1


@pytest.mark.anyio
async def test_hosted_geocoding_can_feed_local_routes_without_hosted_matrix_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directories, path = _configured(tmp_path)
    _ = path.write_text(path.read_text() + _local_settings(path, geocoding="openrouteservice"))
    calls = []

    def geocode(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.method == "GET" and request.url.path == "/geocode/search"
        return httpx.Response(
            200,
            json={
                "features": [
                    {"geometry": {"coordinates": [21, 11]}, "properties": {"confidence": 1.0}}
                ]
            },
        )

    monkeypatch.setattr("carl.io.routing.OsrmRouter", LocalRouter)
    service = ConfiguredTravelService(
        directories=directories, transport=httpx.MockTransport(geocode)
    )
    result = (await service.estimate_many((TravelDestination(label="Example City, ZZ"),)))[0]
    assert result.status == "ready" and result.provider == "osrm"
    assert result.location_precision == "geocoded_locality"
    assert len(calls) == 1
    assert await service.estimate_many((TravelDestination(label="Example City, ZZ"),)) == (result,)
    assert len(calls) == 1


def test_local_backend_requires_absolute_versioned_dataset(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError, match="absolute"):
        OsrmConfiguration(dataset_path=Path("relative.osrm"), dataset_identifier="fixture")
    _, path = _write_configuration(tmp_path)
    _ = path.write_text(path.read_text() + '\n[travel]\nprovider="osrm"\n')
    with pytest.raises(ConfigurationLoadFailure, match="invalid_configuration"):
        load_configuration(path)


def _geonames_settings(path: Path, *, dataset_identifier: str = "test-geocoder-v1") -> str:
    return f'''
[geonames]
database_path = "{path.parent / "places.sqlite3"}"
dataset_identifier = "{dataset_identifier}"
'''


@pytest.mark.anyio
@pytest.mark.parametrize("geocode_budget", [0, 20])
async def test_local_text_origin_and_destinations_need_no_key_http_or_hosted_lookup_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, geocode_budget: int
) -> None:
    directories, path = _write_configuration(tmp_path)
    settings = _local_settings(path, geocoding="geonames").replace(
        "origin = { latitude = 10.0, longitude = 20.0 }",
        f'origin_label = "Home, PA"\ncountry_code = "US"\nmaximum_geocodes_per_batch = {geocode_budget}',
    )
    _ = path.write_text(path.read_text() + settings + _geonames_settings(path))
    lookups: list[tuple[str, str | None]] = []

    class Geocoder:
        def __init__(self, database_path: Path):
            assert database_path == path.parent / "places.sqlite3"

        async def geocode(self, label: str, country_code: str | None) -> TravelCoordinate:
            lookups.append((label, country_code))
            return TravelCoordinate(
                latitude=10 if label == "Home, PA" else 11,
                longitude=20 if label == "Home, PA" else 21,
            )

    monkeypatch.setattr("carl.io.routing.LocalGeonamesGeocoder", Geocoder)
    monkeypatch.setattr("carl.io.routing.OsrmRouter", LocalRouter)
    service = ConfiguredTravelService(
        directories=directories, transport=httpx.MockTransport(_offline)
    )
    identity = await service.origin_identity()
    destinations = tuple(TravelDestination(label=f"Locality {index}, PA") for index in range(25))
    first = await service.estimate_many(destinations)
    assert len(first) == 25
    assert all(item.status == "ready" and item.provider == "osrm" for item in first)
    assert all(item.location_precision == "geocoded_locality" for item in first)
    assert lookups == [("Home, PA", "US"), *((item.label, "US") for item in destinations)]
    assert await service.estimate_many(destinations) == first
    assert await service.origin_identity() == identity
    assert len(lookups) == 26


@pytest.mark.anyio
async def test_geonames_version_changes_regeocode_and_invalidate_retained_route_guards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directories, path = _write_configuration(tmp_path)
    _ = path.write_text(
        path.read_text() + _local_settings(path, geocoding="geonames") + _geonames_settings(path)
    )
    latitude = 11.0
    lookups: list[float] = []
    routed: list[TravelCoordinate] = []

    class Geocoder:
        def __init__(self, database_path: Path):
            assert database_path == path.parent / "places.sqlite3"

        async def geocode(self, label: str, country_code: str | None) -> TravelCoordinate:
            lookups.append(latitude)
            return TravelCoordinate(latitude=latitude, longitude=21)

    class CoordinateRouter(LocalRouter):
        async def estimates(
            self, origin: TravelCoordinate, destinations: Sequence[TravelCoordinate]
        ) -> tuple[tuple[float, float] | str, ...]:
            assert origin == TravelCoordinate(latitude=10, longitude=20)
            routed.extend(destinations)
            return tuple((item.latitude * 1000, 100) for item in destinations)

    monkeypatch.setattr("carl.io.routing.LocalGeonamesGeocoder", Geocoder)
    monkeypatch.setattr("carl.io.routing.OsrmRouter", CoordinateRouter)
    service = ConfiguredTravelService(
        directories=directories, transport=httpx.MockTransport(_offline)
    )
    destination = TravelDestination(label="Locality, PA")
    original_identity = await service.origin_identity()
    first = (await service.estimate_many((destination,)))[0]
    assert first.distance_meters == 11_000
    assert await service.estimate_many((destination,)) == (first,)
    assert lookups == [11]
    latitude = 12.0
    _ = path.write_text(path.read_text().replace("test-geocoder-v1", "test-geocoder-v2"))
    assert await service.origin_identity() != original_identity
    stale_request = await service.refresh_many(
        (destination,), expected_origin_identity=original_identity
    )
    assert stale_request[0].status == "unavailable"
    assert stale_request[0].unavailable_reason == "origin_changed"
    assert lookups == [11] and len(routed) == 1
    second = (await service.estimate_many((destination,)))[0]
    assert second.distance_meters == 12_000
    assert lookups == [11, 12]
    assert [item.latitude for item in routed] == [11, 12]
    assert await service.estimate_many((destination,)) == (second,)
    assert lookups == [11, 12]


@pytest.mark.anyio
async def test_hosted_and_local_geocoding_never_reuse_each_others_cached_coordinates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directories, path = _configured(tmp_path)
    original = path.read_text()
    hosted_settings = _local_settings(path, geocoding="openrouteservice")
    local_settings = _local_settings(path, geocoding="geonames") + _geonames_settings(path)
    _ = path.write_text(original + hosted_settings)
    requests: list[httpx.Request] = []
    local_lookups: list[str] = []

    def hosted(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == "/geocode/search" and request.method == "GET"
        return httpx.Response(
            200,
            json={
                "features": [
                    {"geometry": {"coordinates": [21, 11]}, "properties": {"confidence": 1}}
                ]
            },
        )

    class Geocoder:
        def __init__(self, database_path: Path):
            assert database_path == path.parent / "places.sqlite3"

        async def geocode(self, label: str, country_code: str | None) -> TravelCoordinate:
            local_lookups.append(label)
            return TravelCoordinate(latitude=12, longitude=21)

    class CoordinateRouter(LocalRouter):
        async def estimates(
            self, origin: TravelCoordinate, destinations: Sequence[TravelCoordinate]
        ) -> tuple[tuple[float, float] | str, ...]:
            return tuple((item.latitude * 1000, 100) for item in destinations)

    monkeypatch.setattr("carl.io.routing.LocalGeonamesGeocoder", Geocoder)
    monkeypatch.setattr("carl.io.routing.OsrmRouter", CoordinateRouter)
    service = ConfiguredTravelService(
        directories=directories, transport=httpx.MockTransport(hosted)
    )
    destination = TravelDestination(label="Locality, PA")
    hosted_identity = await service.origin_identity()
    first = (await service.estimate_many((destination,)))[0]
    assert first.distance_meters == 11_000
    _ = path.write_text(original + local_settings)
    assert await service.origin_identity() != hosted_identity
    second = (await service.estimate_many((destination,)))[0]
    assert second.distance_meters == 12_000
    assert len(requests) == 1 and local_lookups == ["Locality, PA"]
    _ = path.write_text(original + hosted_settings)
    assert await service.origin_identity() == hosted_identity
    assert await service.estimate_many((destination,)) == (first,)
    assert len(requests) == 1 and local_lookups == ["Locality, PA"]


@pytest.mark.anyio
@pytest.mark.parametrize("configured", [True, False])
async def test_missing_local_geocoder_reports_not_ready_without_blocking_known_coordinates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, configured: bool
) -> None:
    directories, path = _write_configuration(tmp_path)
    _ = path.write_text(
        path.read_text()
        + _local_settings(path, geocoding="geonames")
        + (_geonames_settings(path) if configured else "")
    )
    monkeypatch.setattr("carl.io.routing.OsrmRouter", LocalRouter)
    service = ConfiguredTravelService(
        directories=directories, transport=httpx.MockTransport(_offline)
    )
    unknown, known = await service.estimate_many(
        (TravelDestination(label="Locality, PA"), _destination())
    )
    assert unknown.status == "unavailable" and unknown.unavailable_reason == "geonames_not_ready"
    assert unknown.provider == "osrm"
    assert known.status == "ready" and known.distance_meters == 1000
    assert not (path.parent / "places.sqlite3").exists()
