"""Shared physical travel queries and direct execution of the pure phase engine."""

import json
import sqlite3
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Literal
from urllib.parse import urlencode
from uuid import uuid4

import anyio
import httpx

from carl.core.configuration import OsrmConfiguration
from carl.core.content_encoding import ContentDecodingError, decode_content
from carl.core.http import RequestPlan, TimeoutPlan
from carl.core.models import Header
from carl.core.travel import TravelDestination
from carl.core.travel_phases import (
    CacheObserved,
    CacheRead,
    Complete,
    Geocode,
    GeocodeObserved,
    Matrix,
    MatrixObserved,
    TravelEvent,
    TravelMode,
    TravelSnapshot,
    TravelState,
    TravelStep,
    advance_travel,
    parse_hosted_geocode,
    parse_hosted_matrix,
    start_travel,
)
from carl.io.configuration import (
    ConfigurationFailure,
    load_configuration,
    openrouteservice_api_key_from_reference,
)
from carl.io.geonames import GeonamesFailure, LocalGeonamesGeocoder
from carl.io.httpx import (
    Acquisition,
    AcquisitionFailure,
    ClientHttpxAcquirer,
    completed_capture_after_close_failure,
)
from carl.io.native_routing_pool import NativeBorrow
from carl.io.osrm import OsrmRouter
from carl.io.paths import CarlDirectories
from carl.io.resource_permits import resource_operation, run_cpu
from carl.io.routing_cache import RoutingCache

MAX_TRAVEL_RESPONSE_BYTES = 1024 * 1024
_API = "https://api.openrouteservice.org"


@dataclass(frozen=True, slots=True)
class HostedRequest:
    action: Geocode | Matrix
    lane: Literal["geocode", "matrix"]
    plan: RequestPlan
    content: bytes | None


@dataclass(frozen=True, slots=True)
class HostedCapture:
    acquisition: Acquisition | None = None
    reason: str | None = None
    next_eligible_at: float | None = None


def hosted_request(action: Geocode | Matrix) -> HostedRequest:
    headers = (Header(name=b"accept", value=b"application/json"),)
    content = None
    if isinstance(action, Geocode):
        parameters: dict[str, str | int] = {
            "text": action.label,
            "size": 3,
            "layers": "locality,localadmin,postalcode,borough,neighbourhood",
        }
        if action.country is not None:
            parameters["boundary.country"] = action.country
        lane, method, url = "geocode", "GET", _API + "/geocode/search?" + urlencode(parameters)
    else:
        lane, method, url = "matrix", "POST", _API + "/v2/matrix/driving-car"
        content = json.dumps(
            {
                "locations": [
                    [action.origin.longitude, action.origin.latitude],
                    *[
                        [group.coordinate.longitude, group.coordinate.latitude]
                        for group in action.groups
                    ],
                ],
                "sources": ["0"],
                "destinations": [str(index + 1) for index in range(len(action.groups))],
                "metrics": ["distance", "duration"],
            },
            allow_nan=False,
            separators=(",", ":"),
        ).encode()
        headers += (Header(name=b"content-type", value=b"application/json"),)
    return HostedRequest(
        action,
        lane,
        RequestPlan(
            url=url,
            method=method,
            headers=headers,
            follow_redirects=False,
            max_redirects=0,
            max_body_bytes=MAX_TRAVEL_RESPONSE_BYTES,
            timeout=TimeoutPlan(
                connect_seconds=15, read_seconds=15, write_seconds=15, pool_seconds=15
            ),
        ),
        content,
    )


def acquisition_status(acquisition: Acquisition) -> int | None:
    hops = acquisition.record.get("hops")
    if not isinstance(hops, list) or not hops or not isinstance(hops[-1], dict):
        return None
    response = hops[-1].get("response")
    status = response.get("status_code") if isinstance(response, dict) else None
    return status if type(status) is int else None


def query_event(
    action: Geocode | Matrix, acquisition: Acquisition | None, reason: str | None = None
) -> GeocodeObserved | MatrixObserved:
    response: object = None
    if reason is None:
        if acquisition is None:
            reason = "provider_invalid_response"
        elif acquisition_status(acquisition) != 200:
            status = acquisition_status(acquisition)
            reason = f"provider_http_{status}" if status is not None else "provider_network_failure"
        elif len(acquisition.bodies) != 1:
            reason = "provider_invalid_response"
        else:
            body = acquisition.bodies[0]
            encodings = body.representation.get("content_encoding_headers", [])
            if not isinstance(encodings, list) or not all(
                isinstance(item, str) for item in encodings
            ):
                reason = "provider_invalid_response"
            else:
                try:
                    content = (
                        body.content
                        if body.representation.get("content_decoded") is True
                        else decode_content(
                            body.content, tuple(encodings), maximum_bytes=MAX_TRAVEL_RESPONSE_BYTES
                        )
                    )
                    response = json.loads(content)
                except ContentDecodingError:
                    reason = "provider_network_failure"
                except (UnicodeDecodeError, ValueError, RecursionError):
                    reason = "provider_invalid_response"
    if isinstance(action, Geocode):
        parsed = parse_hosted_geocode(response) if reason is None else None
        return GeocodeObserved(
            action_id=action.action_id,
            coordinate=parsed.coordinate if parsed is not None else None,
            reason=parsed.reason if parsed is not None else reason,
        )
    return MatrixObserved(
        action_id=action.action_id,
        metrics=parse_hosted_matrix(response, len(action.groups)) if reason is None else None,
        reason=reason,
    )


class TravelQueries:
    def __init__(
        self,
        directories: CarlDirectories,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        now: Callable[[], float] = time,
        sleep: Callable[[float], Awaitable[None]] = anyio.sleep,
        router_factory: Callable[[OsrmConfiguration], OsrmRouter] = OsrmRouter,
        geocoder_factory: Callable[[Path], LocalGeonamesGeocoder] = LocalGeonamesGeocoder,
    ) -> None:
        self.directories, self.transport, self.now, self.sleep = directories, transport, now, sleep
        self.cache = RoutingCache(directories, now=now)
        self.router_factory = router_factory
        self.geocoder_factory = geocoder_factory
        self.osrm_router: OsrmRouter | None = None
        self.osrm_configuration: OsrmConfiguration | None = None

    async def snapshot(self) -> TravelSnapshot:
        def read() -> TravelSnapshot:
            try:
                loaded = load_configuration(self.directories.configuration_file).configuration
                configuration = loaded.travel_settings()
                return TravelSnapshot(
                    configuration=configuration,
                    osrm=loaded.osrm
                    if configuration is not None and configuration.provider == "osrm"
                    else None,
                    geonames=loaded.geonames
                    if configuration is not None and configuration.geocoding_provider == "geonames"
                    else None,
                )
            except (ConfigurationFailure, OSError):
                return TravelSnapshot(unavailable_reason="routing_configuration_unavailable")

        snapshot = await run_cpu(read)
        if snapshot.configuration is None or snapshot.configuration.provider != "osrm":
            self.osrm_router = self.osrm_configuration = None
        return snapshot

    async def advance(
        self,
        state: TravelState,
        event: TravelEvent | None = None,
        *,
        observed_at_utc_ns: int | None = None,
    ) -> TravelStep:
        while True:
            step = await run_cpu(
                lambda state=state, event=event: advance_travel(
                    state,
                    event,
                    now_utc_ns=observed_at_utc_ns
                    if observed_at_utc_ns is not None
                    else int(self.now() * 1_000_000_000),
                )
            )
            for write in step.cache_writes:
                await self.cache.put(
                    write.key, write.value, write.expires_at_utc_ns / 1_000_000_000
                )
            if not isinstance(step.action, CacheRead):
                return step
            state = step.state
            event = CacheObserved(
                action_id=step.action.action_id,
                key=step.action.key,
                value=await self.cache.get(step.action.key, self.now()),
            )

    async def credential_reference(self) -> str | None:
        def read() -> str | None:
            try:
                configuration = load_configuration(
                    self.directories.configuration_file
                ).configuration
                return (
                    configuration.openrouteservice.credential_id
                    if configuration.openrouteservice is not None
                    else None
                )
            except (ConfigurationFailure, OSError):
                return None

        return await run_cpu(read)

    @resource_operation("http")
    async def capture(
        self,
        request: HostedRequest,
        *,
        new_identifier: Callable[[], str],
        action_identifier: str,
        credential_id: str | None,
        authorize: Callable[[], Awaitable[bool]] | None = None,
    ) -> HostedCapture:
        gate = await self.cache.try_gate(
            request.lane, action_identifier=action_identifier, now=self.now()
        )
        if not gate.granted:
            return (
                HostedCapture(reason="provider_cooldown")
                if gate.reason == "provider_cooldown"
                else HostedCapture(next_eligible_at=gate.next_eligible_at)
            )
        try:
            if credential_id is None:
                raise ConfigurationFailure("openrouteservice_not_configured")
            key = await anyio.to_thread.run_sync(
                openrouteservice_api_key_from_reference, credential_id, self.directories
            )
        except (ConfigurationFailure, OSError):
            return HostedCapture(
                reason="geocoding_not_configured"
                if request.lane == "geocode"
                else "routing_configuration_unavailable"
            )
        if authorize is not None and not await authorize():
            return HostedCapture(reason="provider_uncertain_dispatch")
        acquisition = None
        failure = None
        try:
            async with httpx.AsyncClient(
                headers={"Authorization": key.get_secret_value()},
                transport=self.transport,
                timeout=15,
                trust_env=False,
                follow_redirects=False,
            ) as client:
                acquirer = ClientHttpxAcquirer(
                    client=client,
                    expected_routing=("direct",),
                    routing_observation={"kind": "direct"},
                    authentication={"kind": "runtime_credential"},
                    protected_request_headers=frozenset(
                        {b"authorization", b"cookie", b"proxy-authorization"}
                    ),
                    protected_response_headers=frozenset({b"set-cookie"}),
                )
                try:
                    acquisition = (
                        await acquirer.capture(request.plan, new_identifier)
                        if request.content is None
                        else await acquirer.capture_json(
                            request.plan, request.content, new_identifier
                        )
                    )
                except AcquisitionFailure as error:
                    acquisition = completed_capture_after_close_failure(error, plan=request.plan)
                    if acquisition is None:
                        acquisition = Acquisition(error.result, error.bodies)
                        failure = (
                            "provider_network_failure"
                            if error.result.get("stopping_condition") == "transport_failure"
                            else "provider_invalid_response"
                        )
        except Exception as error:
            if acquisition is None:
                if not isinstance(error, httpx.HTTPError):
                    raise
                failure = "provider_network_failure"
            else:
                acquisition.record["client_cleanup_failure"] = {"type": type(error).__name__[:128]}
        status = acquisition_status(acquisition) if acquisition is not None else None
        try:
            if failure == "provider_network_failure":
                await self.cache.block_until(self.now() + 30)
            elif status in (401, 403, 429, 502, 503, 504):
                await self.cache.block_until(self.now() + 60)
        except (sqlite3.Error, OSError, ConfigurationFailure) as error:
            if acquisition is None:
                raise
            # Metadata maintenance after a completed request cannot erase the
            # response before the staged caller checkpoints it.
            acquisition.record["cooldown_cache_failure"] = {"type": type(error).__name__[:128]}
        return HostedCapture(acquisition=acquisition, reason=failure)

    async def local(
        self, state: TravelState, action: Geocode | Matrix, *, borrow: NativeBorrow | None = None
    ) -> GeocodeObserved | MatrixObserved:
        if isinstance(action, Geocode):
            configuration = state.snapshot.geonames
            if configuration is None:
                return GeocodeObserved(action_id=action.action_id, reason="geonames_not_ready")
            try:
                coordinate = await self.geocoder_factory(configuration.database_path).geocode(
                    action.label, action.country
                )
                return GeocodeObserved(action_id=action.action_id, coordinate=coordinate)
            except GeonamesFailure as error:
                return GeocodeObserved(action_id=action.action_id, reason=error.reason)
        configuration = state.snapshot.osrm
        if configuration is None:
            return MatrixObserved(action_id=action.action_id, reason="osrm_dataset_missing")
        destinations = tuple(group.coordinate for group in action.groups)
        if borrow is not None:
            metrics = await borrow.query(
                action.origin,
                destinations,
                maximum_snap_distance_meters=configuration.maximum_snap_distance_meters,
            )
        else:
            if self.osrm_router is None or self.osrm_configuration != configuration:
                self.osrm_router = self.router_factory(configuration)
                self.osrm_configuration = configuration
            metrics = await self.osrm_router.estimates(action.origin, destinations)
        return MatrixObserved(action_id=action.action_id, metrics=metrics)

    async def run(
        self,
        destinations: Sequence[TravelDestination],
        *,
        mode: TravelMode = "estimate",
        expected_origin_identity: str | None = None,
    ) -> Complete:
        snapshot = await self.snapshot()
        state = start_travel(
            snapshot, destinations, mode=mode, expected_origin_identity=expected_origin_identity
        )
        event = None
        try:
            while True:
                step = await self.advance(state, event)
                state, action, event = step.state, step.action, None
                if isinstance(action, Complete):
                    return action
                assert isinstance(action, (Geocode, Matrix))
                if action.backend == "openrouteservice":
                    request = await run_cpu(hosted_request, action)
                    capture = await self.capture(
                        request,
                        new_identifier=lambda: str(uuid4()),
                        action_identifier=f"direct:{id(state)}:{action.action_id}",
                        credential_id=await self.credential_reference(),
                    )
                    if capture.next_eligible_at is not None:
                        await self.sleep(max(0, capture.next_eligible_at - self.now()))
                        continue
                    event = await run_cpu(query_event, action, capture.acquisition, capture.reason)
                    if event.reason == "provider_network_failure":
                        await self.cache.block_until(self.now() + 30)
                else:
                    event = await self.local(state, action)
        except (ConfigurationFailure, sqlite3.Error, OSError):
            failed = start_travel(
                snapshot.model_copy(
                    update={"unavailable_reason": "routing_configuration_unavailable"}
                ),
                destinations,
                mode=mode,
                expected_origin_identity=expected_origin_identity,
            )
            final = await self.advance(failed)
            assert isinstance(final.action, Complete)
            return final.action
