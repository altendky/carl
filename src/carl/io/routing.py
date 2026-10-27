"""Cached road routing through ORS; credentials never enter URLs or cache rows."""

import hashlib
import json
import math
import os
import sqlite3
import stat
from collections.abc import Awaitable, Callable, Sequence
from contextlib import closing
from datetime import UTC, datetime
from time import time
from typing import Any

import anyio
import httpx

from carl.core.configuration import GeonamesConfiguration, OsrmConfiguration, TravelConfiguration
from carl.core.travel import TravelCoordinate, TravelDestination, TravelEstimate
from carl.io.configuration import (
    ConfigurationFailure,
    _require_private_directory,
    load_configuration,
    openrouteservice_api_key,
)
from carl.io.geonames import GeonamesFailure, LocalGeonamesGeocoder
from carl.io.osrm import OsrmRouter
from carl.io.paths import CarlDirectories

_API = "https://api.openrouteservice.org"
_VERSION = "ors-driving-car-v1"


def _identity(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class RoutingFailure(Exception):
    """Only a sanitized machine-readable reason crosses the provider boundary."""


class RoutingCache:
    def __init__(self, directories: CarlDirectories, *, now: Callable[[], float] = time):
        self.directories = directories
        self.now = now

    def _connect(self) -> sqlite3.Connection:
        path = self.directories.routing_cache_file
        self.directories.cache.mkdir(parents=True, exist_ok=True)
        _require_private_directory(path.parent, ConfigurationFailure)
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            info = path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) & 0o077
            ):
                raise ConfigurationFailure("routing_cache_permissions") from None
        else:
            os.close(fd)
        connection = sqlite3.connect(path, timeout=5)
        connection.execute(
            "CREATE TABLE IF NOT EXISTS estimates (key TEXT PRIMARY KEY, expires REAL NOT NULL, value TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS pacing (lane TEXT PRIMARY KEY, next_start REAL NOT NULL)"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS estimates_expiry ON estimates(expires)")
        return connection

    def _get(self, key: str, now: float) -> Any:
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                "SELECT value FROM estimates WHERE key=? AND expires>?", (key, now)
            ).fetchone()
        if row is None:
            return None
        try:
            return json.loads(row[0])
        except ValueError:
            return None

    async def get(self, key: str, now: float) -> Any:
        return await anyio.to_thread.run_sync(self._get, key, now)

    def _put(self, key: str, value: Any, expires: float) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "DELETE FROM estimates WHERE key IN (SELECT key FROM estimates WHERE expires<=? ORDER BY expires LIMIT 100)",
                (self.now(),),
            )
            connection.execute(
                "INSERT OR REPLACE INTO estimates VALUES (?,?,?)",
                (key, expires, json.dumps(value, allow_nan=False)),
            )

    async def put(self, key: str, value: Any, expires: float) -> None:
        await anyio.to_thread.run_sync(self._put, key, value, expires)

    def _reserve(self, lane: str, interval: float, now: float) -> float:
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT next_start FROM pacing WHERE lane=?", (lane,)
            ).fetchone()
            start = max(now, row[0] if row else now)
            connection.execute(
                "INSERT OR REPLACE INTO pacing VALUES (?,?)", (lane, start + interval)
            )
        return start - now

    async def reserve(self, lane: str, interval: float, now: float) -> float:
        return await anyio.to_thread.run_sync(self._reserve, lane, interval, now)


class ConfiguredTravelService:
    def __init__(
        self,
        *,
        directories: CarlDirectories,
        transport: httpx.AsyncBaseTransport | None = None,
        now: Callable[[], float] = time,
        sleep: Callable[[float], Awaitable[None]] = anyio.sleep,
    ):
        self.directories = directories
        self.transport = transport
        self.now = now
        self.sleep = sleep
        self.cache = RoutingCache(directories, now=now)
        self.lock = anyio.Lock()
        self.blocked_until = 0.0
        self.osrm_router: OsrmRouter | None = None
        self.osrm_router_identity: str | None = None

    async def _request(self, client: httpx.AsyncClient, lane: str, path: str, **kwargs: Any) -> Any:
        if self.now() < self.blocked_until:
            raise RoutingFailure("provider_cooldown")
        wait = await self.cache.reserve(lane, 0.65 if lane == "geocode" else 1.6, self.now())
        if wait > 0:
            await self.sleep(wait)
        try:
            key = openrouteservice_api_key(
                load_configuration(self.directories.configuration_file), self.directories
            )
        except ConfigurationFailure:
            raise RoutingFailure(
                "geocoding_not_configured"
                if lane == "geocode"
                else "routing_configuration_unavailable"
            ) from None
        try:
            response = await client.request(
                "GET" if lane == "geocode" else "POST",
                _API + path,
                headers={"Authorization": key.get_secret_value()},
                **kwargs,
            )
        except httpx.HTTPError:
            self.blocked_until = self.now() + 30
            raise RoutingFailure("provider_network_failure") from None
        if response.status_code != 200:
            if response.status_code in (401, 403, 429, 502, 503, 504):
                self.blocked_until = self.now() + 60
            raise RoutingFailure(f"provider_http_{response.status_code}")
        try:
            return response.json()
        except ValueError:
            raise RoutingFailure("provider_invalid_response") from None

    async def _geocode(
        self,
        client: httpx.AsyncClient,
        label: str,
        country: str | None,
        *,
        enabled: bool = True,
        provider: str = "openrouteservice",
        geonames: GeonamesConfiguration | None = None,
    ) -> TravelCoordinate:
        key = self._geocode_key(label, country, provider, geonames)
        cached = await self.cache.get(key, self.now())
        if isinstance(cached, dict):
            if isinstance(cached.get("reason"), str):
                raise RoutingFailure(cached["reason"])
            try:
                return TravelCoordinate.model_validate(cached)
            except ValueError:
                pass
        if not enabled:
            raise RoutingFailure("geocoding_disabled")
        if provider == "geonames":
            if geonames is None:
                raise RoutingFailure("geonames_not_ready")
            try:
                coordinate = await LocalGeonamesGeocoder(geonames.database_path).geocode(
                    label, country
                )
            except GeonamesFailure as error:
                raise RoutingFailure(str(error)) from None
            await self.cache.put(key, coordinate.model_dump(mode="json"), self.now() + 30 * 86400)
            return coordinate
        params: dict[str, str | int] = {
            "text": label,
            "size": 3,
            "layers": "locality,localadmin,postalcode,borough,neighbourhood",
        }
        if country is not None:
            params["boundary.country"] = country
        response = await self._request(client, "geocode", "/geocode/search", params=params)
        candidates: list[tuple[float, TravelCoordinate]] = []
        features = response.get("features", []) if isinstance(response, dict) else []
        if isinstance(features, list):
            for feature in features:
                try:
                    coordinates = feature["geometry"]["coordinates"]
                    confidence = feature["properties"]["confidence"]
                    if (
                        isinstance(confidence, bool)
                        or not isinstance(confidence, (int, float))
                        or not math.isfinite(confidence)
                    ):
                        continue
                    candidates.append(
                        (
                            float(confidence),
                            TravelCoordinate(latitude=coordinates[1], longitude=coordinates[0]),
                        )
                    )
                except (KeyError, IndexError, TypeError, ValueError):
                    continue
        candidates.sort(key=lambda value: value[0], reverse=True)
        unique: dict[str, tuple[float, TravelCoordinate]] = {}
        for candidate in candidates:
            unique.setdefault(candidate[1].model_dump_json(), candidate)
        candidates = list(unique.values())
        if not candidates:
            reason = "location_not_found"
        elif candidates[0][0] < 0.8 or (
            len(candidates) > 1 and candidates[0][0] - candidates[1][0] < 0.05
        ):
            reason = "ambiguous_location"
        else:
            coordinate = candidates[0][1]
            await self.cache.put(key, coordinate.model_dump(mode="json"), self.now() + 30 * 86400)
            return coordinate
        await self.cache.put(key, {"reason": reason}, self.now() + 86400)
        raise RoutingFailure(reason)

    @staticmethod
    def _geocode_key(
        label: str, country: str | None, provider: str, geonames: GeonamesConfiguration | None
    ) -> str:
        normalized = " ".join(label.split()).casefold()
        if provider != "geonames":
            return _identity(("geocode-v1", normalized, country))
        return _identity(
            (
                "geocode-geonames-v1",
                normalized,
                country,
                geonames.model_dump(mode="json") if geonames else None,
            )
        )

    async def _origin(
        self,
        client: httpx.AsyncClient,
        configuration: TravelConfiguration,
        geonames: GeonamesConfiguration | None = None,
    ) -> TravelCoordinate:
        if configuration.origin is not None:
            return configuration.origin
        if configuration.origin_label is None:
            raise RoutingFailure("origin_not_configured")
        return await self._geocode(
            client,
            configuration.origin_label,
            configuration.country_code,
            enabled=configuration.geocoding_provider != "disabled",
            provider=configuration.geocoding_provider,
            geonames=geonames,
        )

    async def origin_identity(self) -> str:
        async with self.lock:
            return await self._origin_identity()

    async def _origin_identity(self) -> str:
        backend_identity = None
        try:
            loaded = load_configuration(self.directories.configuration_file)
            configuration = loaded.configuration.travel_settings()
            if configuration is None:
                return "routing_not_configured"
            backend_identity = (
                configuration.model_dump(mode="json"),
                loaded.configuration.osrm.model_dump(mode="json")
                if configuration.provider == "osrm" and loaded.configuration.osrm is not None
                else None,
                loaded.configuration.geonames.model_dump(mode="json")
                if configuration.geocoding_provider == "geonames" and loaded.configuration.geonames
                else None,
            )
            origin = configuration.origin
            if origin is None:
                async with httpx.AsyncClient(
                    transport=self.transport,
                    timeout=15,
                    trust_env=False,
                    follow_redirects=False,
                ) as client:
                    origin = await self._origin(
                        client, configuration, loaded.configuration.geonames
                    )
            return self._resolved_origin_identity(
                configuration, origin, loaded.configuration.osrm, loaded.configuration.geonames
            )
        except (ConfigurationFailure, sqlite3.Error, OSError):
            return "routing_configuration_unavailable"
        except RoutingFailure as error:
            return _identity((_VERSION, backend_identity, "origin_unavailable", str(error)))

    @staticmethod
    def _resolved_origin_identity(
        configuration: TravelConfiguration,
        origin: TravelCoordinate,
        osrm: OsrmConfiguration | None = None,
        geonames: GeonamesConfiguration | None = None,
    ) -> str:
        return _identity(
            (
                _VERSION,
                configuration.origin_label,
                configuration.country_code,
                origin.model_dump(),
                configuration.cache_ttl_seconds,
                configuration.provider,
                configuration.geocoding_provider,
                osrm.model_dump(mode="json")
                if configuration.provider == "osrm" and osrm is not None
                else None,
                geonames.model_dump(mode="json")
                if configuration.geocoding_provider == "geonames" and geonames
                else None,
            )
        )

    async def estimate_many(
        self, destinations: Sequence[TravelDestination]
    ) -> tuple[TravelEstimate, ...]:
        return await self._estimate_many(destinations)

    async def refresh_many(
        self, destinations: Sequence[TravelDestination], *, expected_origin_identity: str
    ) -> tuple[TravelEstimate, ...]:
        """Refresh route metrics while retaining safe geocode caching and provider pacing."""
        return await self._estimate_many(
            destinations, refresh=True, expected_origin_identity=expected_origin_identity
        )

    async def _estimate_many(
        self,
        destinations: Sequence[TravelDestination],
        *,
        refresh: bool = False,
        expected_origin_identity: str | None = None,
    ) -> tuple[TravelEstimate, ...]:
        if not destinations:
            return ()
        async with self.lock:
            provider = "openrouteservice"
            dataset_identifier = None
            try:
                loaded = load_configuration(self.directories.configuration_file)
                configuration = loaded.configuration.travel_settings()
                if configuration is None:
                    raise RoutingFailure("routing_not_configured")
                provider = configuration.provider
                if provider != "osrm":
                    self.osrm_router = None
                    self.osrm_router_identity = None
                osrm = loaded.configuration.osrm if provider == "osrm" else None
                dataset_identifier = osrm.dataset_identifier if osrm is not None else None
                if configuration.origin is None and configuration.origin_label is None:
                    raise RoutingFailure("origin_not_configured")
                async with httpx.AsyncClient(
                    transport=self.transport,
                    timeout=15,
                    trust_env=False,
                    follow_redirects=False,
                ) as client:
                    origin = await self._origin(
                        client, configuration, loaded.configuration.geonames
                    )
                    if (
                        expected_origin_identity is not None
                        and self._resolved_origin_identity(
                            configuration, origin, osrm, loaded.configuration.geonames
                        )
                        != expected_origin_identity
                    ):
                        raise RoutingFailure("origin_changed")
                    return await self._estimates(
                        client,
                        configuration,
                        origin,
                        destinations,
                        refresh=refresh,
                        osrm=osrm,
                        geonames=loaded.configuration.geonames,
                    )
            except (ConfigurationFailure, sqlite3.Error, OSError):
                reason = "routing_configuration_unavailable"
            except RoutingFailure as error:
                reason = str(error)
            return tuple(
                self._unavailable(destination, reason).model_copy(
                    update={"provider": provider, "dataset_identifier": dataset_identifier}
                )
                for destination in destinations
            )

    @staticmethod
    def _unavailable(destination: TravelDestination, reason: str) -> TravelEstimate:
        return TravelEstimate(
            status="unavailable",
            destination_label=destination.label,
            location_precision=destination.precision,
            unavailable_reason=reason,
        )

    async def _estimates(
        self,
        client: httpx.AsyncClient,
        configuration: TravelConfiguration,
        origin: TravelCoordinate,
        destinations: Sequence[TravelDestination],
        *,
        refresh: bool = False,
        osrm: OsrmConfiguration | None = None,
        geonames: GeonamesConfiguration | None = None,
    ) -> tuple[TravelEstimate, ...]:
        results: list[TravelEstimate | None] = [None] * len(destinations)
        resolved: dict[int, TravelDestination] = {}
        pending: dict[str, tuple[TravelCoordinate, list[int]]] = {}
        geocodes = 0

        def unavailable(destination: TravelDestination, reason: str) -> TravelEstimate:
            return self._unavailable(destination, reason).model_copy(
                update={
                    "provider": configuration.provider,
                    "dataset_identifier": osrm.dataset_identifier if osrm is not None else None,
                }
            )

        for index, destination in enumerate(destinations):
            if destination.unavailable_reason == "conflicting_coordinates":
                results[index] = unavailable(destination, "conflicting_coordinates")
                continue
            coordinate = destination.coordinate
            if coordinate is None:
                if not destination.label:
                    results[index] = unavailable(
                        destination, destination.unavailable_reason or "location_unavailable"
                    )
                    continue
                # Cache hits do not consume this request's provider lookup allowance.
                geo_key = self._geocode_key(
                    destination.label,
                    configuration.country_code,
                    configuration.geocoding_provider,
                    geonames,
                )
                cached = await self.cache.get(geo_key, self.now())
                if (
                    configuration.geocoding_provider == "openrouteservice"
                    and cached is None
                    and geocodes >= configuration.maximum_geocodes_per_batch
                ):
                    results[index] = unavailable(destination, "geocode_budget_exhausted")
                    continue
                geocodes += cached is None
                try:
                    coordinate = await self._geocode(
                        client,
                        destination.label,
                        configuration.country_code,
                        enabled=configuration.geocoding_provider != "disabled",
                        provider=configuration.geocoding_provider,
                        geonames=geonames,
                    )
                except RoutingFailure as error:
                    results[index] = unavailable(destination, str(error))
                    continue
                destination = destination.model_copy(
                    update={"coordinate": coordinate, "precision": "geocoded_locality"}
                )
            resolved[index] = destination
            route_key = _identity(
                (
                    _VERSION,
                    origin.model_dump(),
                    coordinate.model_dump(),
                    configuration.cache_ttl_seconds,
                    configuration.provider,
                    osrm.model_dump(mode="json") if osrm is not None else None,
                )
            )
            cached = None if refresh else await self.cache.get(route_key, self.now())
            if isinstance(cached, dict):
                try:
                    cached_estimate = TravelEstimate.model_validate(
                        {
                            **cached,
                            "destination_label": destination.label,
                            "location_precision": destination.precision,
                        }
                    )
                    if cached_estimate.provider != configuration.provider:
                        raise ValueError("Cached route provider does not match")
                    results[index] = cached_estimate
                    continue
                except ValueError:
                    pass
            if route_key not in pending:
                pending[route_key] = (coordinate, [])
            pending[route_key][1].append(index)
        entries = list(pending.items())
        for offset in range(0, len(entries), 50):
            batch = entries[offset : offset + 50]
            payload = {
                "locations": [
                    [origin.longitude, origin.latitude],
                    *[[value[0].longitude, value[0].latitude] for _, value in batch],
                ],
                "sources": ["0"],
                "destinations": [str(i + 1) for i in range(len(batch))],
                "metrics": ["distance", "duration"],
            }
            reason = "provider_invalid_response"
            local_metrics: tuple[tuple[float, float] | str, ...] | None = None
            try:
                if osrm is not None:
                    router_identity = _identity(osrm.model_dump(mode="json"))
                    if self.osrm_router is None or self.osrm_router_identity != router_identity:
                        self.osrm_router = OsrmRouter(osrm)
                        self.osrm_router_identity = router_identity
                    local_metrics = await self.osrm_router.estimates(
                        origin, tuple(value[0] for _, value in batch)
                    )
                    if len(local_metrics) != len(batch):
                        raise RoutingFailure("provider_invalid_response")
                    distances = durations = None
                else:
                    response = await self._request(
                        client, "matrix", "/v2/matrix/driving-car", json=payload
                    )
                    if not isinstance(response, dict):
                        raise RoutingFailure("provider_invalid_response")
                    matrices = (response.get("distances"), response.get("durations"))
                    rows: list[list[Any]] = []
                    for matrix in matrices:
                        if (
                            not isinstance(matrix, list)
                            or len(matrix) != 1
                            or not isinstance(matrix[0], list)
                            or len(matrix[0]) != len(batch)
                        ):
                            raise RoutingFailure("provider_invalid_response")
                        rows.append(matrix[0])
                    distances, durations = rows
            except (KeyError, IndexError, TypeError):
                local_metrics = None
                distances = durations = None
                reason = "provider_invalid_response"
            except RoutingFailure as error:
                local_metrics = None
                distances = durations = None
                reason = str(error)
            for position, (route_key, (_, indices)) in enumerate(batch):
                distance = None if distances is None else distances[position]
                duration = None if durations is None else durations[position]
                item_reason = reason
                if local_metrics is not None:
                    local_metric = local_metrics[position]
                    if isinstance(local_metric, str):
                        item_reason = local_metric
                    else:
                        distance, duration = local_metric
                valid = all(
                    not isinstance(value, bool)
                    and isinstance(value, (int, float))
                    and math.isfinite(value)
                    and value >= 0
                    for value in (distance, duration)
                )
                value = TravelEstimate(
                    status="ready" if valid else "unavailable",
                    provider=configuration.provider,
                    dataset_identifier=osrm.dataset_identifier if osrm is not None else None,
                    distance_meters=distance if valid else None,
                    duration_seconds=duration if valid else None,
                    estimated_at_utc=datetime.fromtimestamp(self.now(), UTC).isoformat(),
                    expires_at_utc=datetime.fromtimestamp(
                        self.now() + (configuration.cache_ttl_seconds if valid else 60), UTC
                    ).isoformat(),
                    unavailable_reason=None
                    if valid
                    else "no_route"
                    if distances is not None and distance is None and duration is None
                    else "provider_invalid_response"
                    if distances is not None
                    else item_reason,
                )
                await self.cache.put(
                    route_key,
                    value.model_dump(mode="json"),
                    self.now() + (configuration.cache_ttl_seconds if valid else 60),
                )
                for index in indices:
                    destination = resolved[index]
                    results[index] = value.model_copy(
                        update={
                            "destination_label": destination.label,
                            "location_precision": destination.precision,
                        }
                    )
        return tuple(
            result
            if result is not None
            else unavailable(destinations[index], "location_unavailable")
            for index, result in enumerate(results)
        )
