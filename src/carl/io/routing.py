"""Configured direct travel calls share the same phase and physical query drivers."""

from collections.abc import Awaitable, Callable, Sequence
from time import time

import anyio
import httpx

from carl.core.travel import TravelDestination, TravelEstimate
from carl.io.geonames import LocalGeonamesGeocoder
from carl.io.osrm import OsrmRouter
from carl.io.paths import CarlDirectories
from carl.io.routing_cache import RoutingCache as RoutingCache
from carl.io.travel_queries import TravelQueries


class RoutingFailure(Exception):
    """Stable provider reasons retained for compatibility with existing callers."""


class ConfiguredTravelService:
    def __init__(
        self,
        *,
        directories: CarlDirectories,
        transport: httpx.AsyncBaseTransport | None = None,
        now: Callable[[], float] = time,
        sleep: Callable[[float], Awaitable[None]] = anyio.sleep,
    ) -> None:
        self.directories, self.transport, self.now, self.sleep = directories, transport, now, sleep
        self.lock = anyio.Lock()
        self.queries = TravelQueries(
            directories,
            transport=transport,
            now=now,
            sleep=sleep,
            # Resolve the factory at call time, preserving injected local routers.
            router_factory=lambda configuration: OsrmRouter(configuration),
            geocoder_factory=lambda database_path: LocalGeonamesGeocoder(database_path),
        )
        self.cache = self.queries.cache

    @property
    def osrm_router(self) -> OsrmRouter | None:
        return self.queries.osrm_router

    async def origin_identity(self) -> str:
        async with self.lock:
            result = await self.queries.run((), mode="origin_identity")
            return result.origin_identity or "routing_configuration_unavailable"

    async def estimate_many(
        self, destinations: Sequence[TravelDestination]
    ) -> tuple[TravelEstimate, ...]:
        if not destinations:
            return ()
        async with self.lock:
            return (await self.queries.run(destinations)).estimates

    async def refresh_many(
        self, destinations: Sequence[TravelDestination], *, expected_origin_identity: str
    ) -> tuple[TravelEstimate, ...]:
        if not destinations:
            return ()
        async with self.lock:
            return (
                await self.queries.run(
                    destinations, mode="refresh", expected_origin_identity=expected_origin_identity
                )
            ).estimates
