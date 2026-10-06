"""Managed Mullvad WireGuard sessions exposed through local SOCKS5."""

import ipaddress
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from time import perf_counter_ns

import anyio
import httpx
from pydantic import Field

from carl.core.http import RequestPlan
from carl.core.json import decode_json
from carl.core.models import ConfigurationDocumentIdentity, Header, JsonValue, StrictModel
from carl.core.routing import (
    LocalSocks5Endpoint,
    MullvadHealthProbeObservation,
    MullvadRouteIdentity,
    MullvadSessionObservation,
    NetworkProvider,
    WireproxyIdentity,
)
from carl.io.httpx import (
    Acquisition,
    AcquisitionFailure,
    HttpAcquirer,
    IdentifierFactory,
    LocalSocks5HttpxAcquirer,
    RouteConfigurationFailure,
    close_httpx_client,
)
from carl.io.wireproxy import (
    ManagedWireproxyFailure,
    ManagedWireproxyProcess,
    WireproxyProcessManager,
    render_wireguard_configuration,
    wireguard_device_lock_identity,
)

_MULLVAD_EXIT_PROBE = "https://am.i.mullvad.net/json"


class MullvadWireproxySettings(StrictModel):
    route: MullvadRouteIdentity
    configuration: ConfigurationDocumentIdentity
    wireproxy: WireproxyIdentity
    wireproxy_path: Path = Field(exclude=True, repr=False)
    configuration_content: bytes = Field(exclude=True, repr=False)
    runtime_directory: Path = Field(exclude=True, repr=False)
    startup_timeout_seconds: float = Field(default=60.0, gt=0, le=300)
    shutdown_timeout_seconds: float = Field(default=10.0, gt=0, le=60)

    def safe_configuration(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")

    @property
    def device_lock_identity(self) -> bytes:
        return wireguard_device_lock_identity(self.configuration_content)


class ManagedMullvadTransportFailure(Exception):
    def __init__(
        self,
        code: str,
        *,
        exit_code: int | None = None,
        diagnostic: dict[str, JsonValue] | None = None,
    ):
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code
        self.diagnostic = diagnostic or {}


@dataclass(slots=True)
class ManagedMullvadSession:
    route: MullvadRouteIdentity
    configuration: ConfigurationDocumentIdentity
    process: ManagedWireproxyProcess
    observed_exit_ip: ipaddress.IPv4Address
    health_probe: MullvadHealthProbeObservation
    validated_at_utc: str

    @property
    def endpoint(self) -> LocalSocks5Endpoint:
        return self.process.endpoint

    def active_observation(self) -> dict[str, JsonValue]:
        return {
            "provider": self.route.provider.value,
            "route": self.route.model_dump(mode="json"),
            "configuration": self.configuration.as_json(),
            "endpoint": self.endpoint.model_dump(mode="json"),
            "wireproxy": self.process.wireproxy.model_dump(mode="json"),
            "process_argv": list(self.process.process_argv),
            "observed_exit_ip": str(self.observed_exit_ip),
            "health_test": "mullvad_exit_ip_probe_passed",
            "health_probe": self.health_probe.model_dump(mode="json", by_alias=True),
            "started_at_utc": self.process.started_at_utc,
            "ready_at_utc": self.validated_at_utc,
            "state": "ready",
        }

    def completed_observation(self) -> MullvadSessionObservation:
        if self.process.stopped_at_utc is None or self.process.shutdown_state is None:
            raise RuntimeError("Mullvad session has not completed cleanup")
        return MullvadSessionObservation(
            route=self.route,
            configuration=self.configuration,
            endpoint=self.endpoint,
            wireproxy=self.process.wireproxy,
            process_argv=self.process.process_argv,
            observed_exit_ip=self.observed_exit_ip,
            health_test="mullvad_exit_ip_probe_passed",
            health_probe=self.health_probe,
            archive_permission_policy="strict",
            started_at_utc=self.process.started_at_utc,
            ready_at_utc=self.validated_at_utc,
            stopped_at_utc=self.process.stopped_at_utc,
            shutdown_state=self.process.shutdown_state,
        )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _render_configuration(settings: MullvadWireproxySettings, port: int) -> bytes:
    try:
        return render_wireguard_configuration(
            settings.configuration_content,
            port=port,
            dns_override=("10.64.0.1",),
            mtu_override=1280,
        )
    except ValueError:
        raise RouteConfigurationFailure("invalid_mullvad_configuration") from None


async def _probe_exit_ip(
    endpoint: LocalSocks5Endpoint,
    *,
    expected_relay_hostname: str,
    timeout_seconds: float,
    transport: httpx.AsyncBaseTransport | None = None,
) -> tuple[ipaddress.IPv4Address, MullvadHealthProbeObservation]:
    started_at_utc = _utc_now()
    started = perf_counter_ns()
    client = httpx.AsyncClient(
        proxy=endpoint.url if transport is None else None,
        transport=transport,
        timeout=httpx.Timeout(connect=5, read=10, write=5, pool=5),
        trust_env=False,
        follow_redirects=False,
    )
    try:
        with anyio.fail_after(timeout_seconds):
            response = await client.get(
                _MULLVAD_EXIT_PROBE,
                headers={"Accept": "application/json", "Accept-Encoding": "identity"},
            )
        raw = response.content
        if len(raw) > 64 * 1024:
            raise ValueError("Mullvad health response exceeds limit")
        response.raise_for_status()
        text = raw.decode("utf-8")
        value = decode_json(text)
        if not isinstance(value, dict) or value.get("mullvad_exit_ip") is not True:
            raise ValueError("Mullvad endpoint did not confirm a Mullvad exit")
        observed_ip = ipaddress.IPv4Address(value.get("ip"))
        hostname = value.get("mullvad_exit_ip_hostname")
        if hostname is not None and hostname != expected_relay_hostname:
            raise ValueError("Mullvad exit hostname does not match selected relay")
        return observed_ip, MullvadHealthProbeObservation(
            url=_MULLVAD_EXIT_PROBE,
            method="GET",
            timeout_seconds=timeout_seconds,
            request_headers=tuple(Header(name=n, value=v) for n, v in response.request.headers.raw),
            started_at_utc=started_at_utc,
            ended_at_utc=_utc_now(),
            duration_ns=perf_counter_ns() - started,
            status_code=response.status_code,
            response_headers=tuple(Header(name=n, value=v) for n, v in response.headers.raw),
            response_body_utf8=text,
            response_body_bytes=len(raw),
            response_json=value,
            mullvad_exit_ip=True,
            observed_exit_hostname=hostname,
        )
    except (httpx.HTTPError, TimeoutError, UnicodeError, ValueError) as error:
        diagnostic: dict[str, JsonValue] = {"exception_type": type(error).__name__}
        if isinstance(error, httpx.HTTPStatusError):
            diagnostic["http_status"] = error.response.status_code
        raise ManagedMullvadTransportFailure(
            "mullvad_egress_probe_failed", diagnostic=diagnostic
        ) from None
    finally:
        await close_httpx_client(client, primary_error=sys.exception())


class MullvadWireproxyManager:
    def __init__(self, process_manager: WireproxyProcessManager | None = None):
        self.process_manager = process_manager or WireproxyProcessManager()

    @asynccontextmanager
    async def open(
        self, settings: MullvadWireproxySettings
    ) -> AsyncGenerator[ManagedMullvadSession]:
        try:
            device_lock_identity = settings.device_lock_identity
        except ValueError:
            raise RouteConfigurationFailure("invalid_mullvad_configuration") from None
        try:
            async with self.process_manager.open(
                provider=NetworkProvider.MULLVAD,
                wireproxy=settings.wireproxy,
                wireproxy_path=settings.wireproxy_path,
                runtime_directory=settings.runtime_directory,
                device_lock_identity=device_lock_identity,
                render_configuration=partial(_render_configuration, settings),
                startup_timeout_seconds=settings.startup_timeout_seconds,
                shutdown_timeout_seconds=settings.shutdown_timeout_seconds,
            ) as process:
                observed_ip, probe = await _probe_exit_ip(
                    process.endpoint,
                    expected_relay_hostname=settings.route.relay_hostname,
                    timeout_seconds=settings.startup_timeout_seconds,
                )
                yield ManagedMullvadSession(
                    route=settings.route,
                    configuration=settings.configuration,
                    process=process,
                    observed_exit_ip=observed_ip,
                    health_probe=probe,
                    validated_at_utc=_utc_now(),
                )
        except ManagedWireproxyFailure as error:
            raise ManagedMullvadTransportFailure(
                error.code, exit_code=error.exit_code, diagnostic=error.diagnostic
            ) from None


class ManagedMullvadHttpAcquirer:
    def __init__(self, *, manager: MullvadWireproxyManager, settings: MullvadWireproxySettings):
        self.manager = manager
        self.settings = settings

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        if plan.routing != self.settings.route.network_path:
            raise RouteConfigurationFailure("request_route_does_not_match_mullvad_route")
        acquisition: Acquisition | None = None
        failure: AcquisitionFailure | None = None
        session: ManagedMullvadSession | None = None
        try:
            async with self.manager.open(self.settings) as session:
                acquirer: HttpAcquirer = LocalSocks5HttpxAcquirer(
                    endpoint=session.endpoint,
                    expected_routing=self.settings.route.network_path,
                    routing_observation=session.active_observation(),
                )
                try:
                    acquisition = await acquirer.acquire(plan, new_identifier)
                except AcquisitionFailure as error:
                    failure = error
        except RouteConfigurationFailure:
            raise
        except ManagedMullvadTransportFailure as error:
            raise AcquisitionFailure(
                "Managed Mullvad transport failed",
                result={
                    "hops": failure.result.get("hops", []) if failure is not None else [],
                    "stopping_condition": "transport_failure",
                    "route_failure": {
                        "code": error.code,
                        "exit_code": error.exit_code,
                        "diagnostic": error.diagnostic,
                    },
                    "routing": {"configured": list(plan.routing), "observed": None},
                },
                bodies=failure.bodies if failure is not None else (),
            ) from None
        if session is None:
            raise AssertionError("Managed Mullvad session was not created")
        observation = session.completed_observation().as_json()
        if failure is not None:
            failure.result["routing"] = {"configured": list(plan.routing), "observed": observation}
            raise failure
        if acquisition is None:
            raise AssertionError("Mullvad acquisition produced no outcome")
        acquisition.record["routing"] = {"configured": list(plan.routing), "observed": observation}
        return acquisition
