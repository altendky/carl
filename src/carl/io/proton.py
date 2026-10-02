"""Managed Proton WireGuard sessions exposed through local SOCKS5."""

from __future__ import annotations

import ipaddress
import logging
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from time import perf_counter_ns
from types import TracebackType
from typing import Protocol, final

import anyio
import httpx
from pydantic import Field

from carl.core.http import RequestPlan
from carl.core.models import ConfigurationDocumentIdentity, Header, JsonValue, StrictModel
from carl.core.routing import (
    InternetProtocolVersion,
    LocalSocks5Endpoint,
    NetworkProvider,
    ProtonHealthProbeObservation,
    ProtonRouteIdentity,
    ProtonSessionObservation,
    WireproxyIdentity,
)
from carl.io.httpx import (
    Acquisition,
    AcquisitionFailure,
    HttpAcquirer,
    IdentifierFactory,
    LocalSocks5HttpxAcquirer,
    RouteConfigurationFailure,
)
from carl.io.wireproxy import (
    ManagedWireproxyFailure,
    ManagedWireproxyProcess,
    WireproxyProcessManager,
    render_wireguard_configuration,
    wireguard_device_lock_identity,
)
from carl.io.wreq import (
    LocalSocks5WreqAcquirer,
    WreqClientFactory,
    WreqTransportSettings,
)

_PROTON_EXIT_PROBE = "https://ip.me/"
_LOGGER = logging.getLogger(__name__)


class ProtonWireproxySettings(StrictModel):
    route: ProtonRouteIdentity
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


class ManagedProtonTransportFailure(Exception):
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
class ManagedProtonSession:
    route: ProtonRouteIdentity
    configuration: ConfigurationDocumentIdentity
    process: ManagedWireproxyProcess
    observed_exit_ip: ipaddress.IPv4Address | ipaddress.IPv6Address
    health_probe: ProtonHealthProbeObservation
    validated_at_utc: str

    @property
    def endpoint(self) -> LocalSocks5Endpoint:
        return self.process.endpoint

    def active_observation(self) -> dict[str, JsonValue]:
        return {
            "provider": self.route.provider.value,
            "route": self.route.model_dump(mode="json"),
            "configuration": self.configuration.as_json(),
            "endpoint": self.process.endpoint.model_dump(mode="json"),
            "wireproxy": self.process.wireproxy.model_dump(mode="json"),
            "process_argv": list(self.process.process_argv),
            "observed_exit_ip": str(self.observed_exit_ip),
            "health_probe": self.health_probe.model_dump(mode="json", by_alias=True),
            "started_at_utc": self.process.started_at_utc,
            "proxy_listening_at_utc": self.process.proxy_listening_at_utc,
            "exit_code": self.process.exit_code,
            "validated_at_utc": self.validated_at_utc,
            "state": "ready",
        }

    def completed_observation(self) -> dict[str, JsonValue]:
        if self.process.stopped_at_utc is None or self.process.shutdown_state is None:
            raise RuntimeError("Proton session has not completed cleanup")
        return ProtonSessionObservation(
            route=self.route,
            configuration=self.configuration,
            endpoint=self.process.endpoint,
            wireproxy=self.process.wireproxy,
            process_argv=self.process.process_argv,
            observed_exit_ip=self.observed_exit_ip,
            health_probe=self.health_probe,
            started_at_utc=self.process.started_at_utc,
            proxy_listening_at_utc=self.process.proxy_listening_at_utc,
            validated_at_utc=self.validated_at_utc,
            stopped_at_utc=self.process.stopped_at_utc,
            shutdown_state=self.process.shutdown_state,
            exit_code=self.process.exit_code,
        ).as_json()


class ProtonSession(Protocol):
    @property
    def endpoint(self) -> LocalSocks5Endpoint: ...

    def active_observation(self) -> dict[str, JsonValue]: ...

    def completed_observation(self) -> dict[str, JsonValue]: ...


class ProtonSessionManager(Protocol):
    def open(
        self, settings: ProtonWireproxySettings
    ) -> AbstractAsyncContextManager[ProtonSession]: ...


@dataclass(slots=True)
class SharedManagedProtonSession:
    """One caller's use of a worker-runtime-scoped Proton transport."""

    provider_session: ManagedProtonSession
    usage_started_at_utc: str
    usage_ended_at_utc: str | None = None
    transport_failed: bool = False

    @property
    def endpoint(self) -> LocalSocks5Endpoint:
        return self.provider_session.endpoint

    def active_observation(self) -> dict[str, JsonValue]:
        return {
            **self.provider_session.active_observation(),
            "transport_scope": "worker_runtime",
            "usage_started_at_utc": self.usage_started_at_utc,
        }

    def completed_observation(self) -> dict[str, JsonValue]:
        if self.usage_ended_at_utc is None:
            raise RuntimeError("Shared Proton transport usage has not ended")
        provider_observation = (
            self.provider_session.completed_observation()
            if self.provider_session.process.stopped_at_utc is not None
            else self.provider_session.active_observation()
        )
        return {
            **provider_observation,
            "transport_scope": "worker_runtime",
            "usage_started_at_utc": self.usage_started_at_utc,
            "state": "released",
            "usage_ended_at_utc": self.usage_ended_at_utc,
            "transport_state_at_release": "failed" if self.transport_failed else "ready",
        }


@dataclass(slots=True)
class _SharedProtonEntry:
    settings: ProtonWireproxySettings
    session: ManagedProtonSession
    stack: AsyncExitStack
    users: int = 0
    invalidated: bool = False
    closed: anyio.Event = dataclass_field(default_factory=anyio.Event)


class SharedProtonWireproxyManager:
    """Keep one validated WireProxy transport per device identity for this runtime."""

    def __init__(
        self,
        manager: ProtonWireproxyManager | None = None,
        *,
        watcher_task_group: anyio.abc.TaskGroup | None = None,
    ):
        self.manager: ProtonWireproxyManager = manager or ProtonWireproxyManager()
        self.watcher_task_group = watcher_task_group
        self._lock: anyio.Lock = anyio.Lock()
        self._running = False
        self._entries: dict[bytes, _SharedProtonEntry] = {}

    async def __aenter__(self) -> SharedProtonWireproxyManager:
        if self._running:
            raise RuntimeError("Shared Proton manager is already running")
        self._running = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        if not self._running:
            raise RuntimeError("Shared Proton manager is not running")
        self._running = False
        async with self._lock:
            entries = tuple(self._entries.values())
            self._entries.clear()
        with anyio.CancelScope(shield=True):
            for entry in reversed(entries):
                await self._close_entry(entry)

    async def _close_entry(self, entry: _SharedProtonEntry) -> None:
        try:
            await entry.stack.aclose()
        except Exception:
            _LOGGER.exception("Failed to close shared Proton transport")
        finally:
            entry.closed.set()

    async def _close_invalidated_entry(
        self, device_identity: bytes, entry: _SharedProtonEntry
    ) -> None:
        close = False
        async with self._lock:
            entry.invalidated = True
            if entry.users == 0 and self._entries.get(device_identity) is entry:
                del self._entries[device_identity]
                close = True
        if close:
            with anyio.CancelScope(shield=True):
                await self._close_entry(entry)

    async def _watch_entry(self, device_identity: bytes, entry: _SharedProtonEntry) -> None:
        child = entry.session.process.child
        if child is None:
            return
        try:
            await child.wait()
            entry.session.process.exit_code = child.returncode
            entry.session.process.exited.set()
            await self._close_invalidated_entry(device_identity, entry)
        except anyio.get_cancelled_exc_class():
            raise
        except Exception:
            _LOGGER.exception("Shared Proton transport watcher failed")
            await self._close_invalidated_entry(device_identity, entry)

    @asynccontextmanager
    async def open(
        self, settings: ProtonWireproxySettings
    ) -> AsyncGenerator[SharedManagedProtonSession]:
        try:
            device_identity = settings.device_lock_identity
        except ValueError:
            raise RouteConfigurationFailure("invalid_proton_configuration") from None
        selected_entry: _SharedProtonEntry | None = None
        while True:
            wait_for_close: anyio.Event | None = None
            close_exited_entry: _SharedProtonEntry | None = None
            async with self._lock:
                if not self._running:
                    raise RuntimeError("Shared Proton manager is not running")
                entry = self._entries.get(device_identity)
                if entry is not None and (
                    entry.session.process.exited.is_set()
                    or (
                        entry.session.process.child is not None
                        and entry.session.process.child.returncode is not None
                    )
                ):
                    entry.invalidated = True
                    if entry.users == 0:
                        del self._entries[device_identity]
                        close_exited_entry = entry
                        entry = None
                if entry is not None and entry.invalidated:
                    wait_for_close = entry.closed
                elif close_exited_entry is not None:
                    pass
                elif entry is None and close_exited_entry is None:
                    stack = AsyncExitStack()
                    _ = await stack.__aenter__()
                    try:
                        provider_session = await stack.enter_async_context(
                            self.manager.open(settings)
                        )
                    except BaseException:
                        await stack.aclose()
                        raise
                    entry = _SharedProtonEntry(
                        settings=settings,
                        session=provider_session,
                        stack=stack,
                        users=1,
                    )
                    self._entries[device_identity] = entry
                    if self.watcher_task_group is not None:
                        self.watcher_task_group.start_soon(
                            self._watch_entry, device_identity, entry
                        )
                    selected_entry = entry
                    break
                elif entry is not None and entry.settings != settings:
                    raise RouteConfigurationFailure("proton_device_configuration_conflict")
                elif entry is not None:
                    entry.users += 1
                    selected_entry = entry
                    break
            if close_exited_entry is not None:
                with anyio.CancelScope(shield=True):
                    await self._close_entry(close_exited_entry)
                continue
            if wait_for_close is not None:
                await wait_for_close.wait()
        if selected_entry is None:
            raise AssertionError("Shared Proton entry selection did not complete")
        entry = selected_entry
        usage = SharedManagedProtonSession(
            provider_session=entry.session,
            usage_started_at_utc=_utc_now(),
        )
        transport_failed = False
        try:
            yield usage
        except AcquisitionFailure as error:
            transport_failed = error.result.get("stopping_condition") == "transport_failure"
            usage.transport_failed = transport_failed
            raise
        finally:
            usage.usage_ended_at_utc = _utc_now()
            close = False
            async with self._lock:
                entry.users -= 1
                if transport_failed:
                    entry.invalidated = True
                if (
                    entry.invalidated
                    and entry.users == 0
                    and self._entries.get(device_identity) is entry
                ):
                    del self._entries[device_identity]
                    close = True
            if close:
                with anyio.CancelScope(shield=True):
                    await self._close_entry(entry)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _render_configuration(settings: ProtonWireproxySettings, port: int) -> bytes:
    try:
        return render_wireguard_configuration(
            settings.configuration_content,
            port=port,
            expected_peer_endpoint=settings.route.peer_endpoint,
            internet_protocol_version=settings.route.internet_protocol_version,
        )
    except ValueError:
        raise RouteConfigurationFailure("invalid_proton_configuration") from None


async def _probe_exit_ip(
    endpoint: LocalSocks5Endpoint,
    *,
    timeout_seconds: float,
    transport: httpx.AsyncBaseTransport | None = None,
) -> tuple[
    ipaddress.IPv4Address | ipaddress.IPv6Address,
    ProtonHealthProbeObservation,
]:
    timeout = httpx.Timeout(connect=5, read=10, write=5, pool=5)
    started_at_utc = _utc_now()
    started = perf_counter_ns()
    client = httpx.AsyncClient(
        proxy=endpoint.url if transport is None else None,
        transport=transport,
        timeout=timeout,
        trust_env=False,
        follow_redirects=False,
    )
    response: httpx.Response | None = None
    try:
        with anyio.fail_after(timeout_seconds):
            request = client.build_request(
                "GET",
                _PROTON_EXIT_PROBE,
                headers={"Accept": "text/plain", "Accept-Encoding": "identity"},
            )
            response = await client.send(request, stream=True, follow_redirects=False)
            content = bytearray()
            async for chunk in response.aiter_raw():
                content.extend(chunk)
                if len(content) > 4096:
                    raise ValueError("Proton health response exceeds limit")
            raw = bytes(content)
            response.raise_for_status()
            text = raw.decode("utf-8")
            observed_ip = ipaddress.ip_address(text.strip())
            probe = ProtonHealthProbeObservation(
                url=_PROTON_EXIT_PROBE,
                method="GET",
                timeout_seconds=timeout_seconds,
                request_headers=tuple(
                    Header(name=name, value=value) for name, value in request.headers.raw
                ),
                started_at_utc=started_at_utc,
                ended_at_utc=_utc_now(),
                duration_ns=perf_counter_ns() - started,
                status_code=response.status_code,
                response_headers=tuple(
                    Header(name=name, value=value) for name, value in response.headers.raw
                ),
                response_body_utf8=text,
                response_body_bytes=len(raw),
                observed_exit_ip=observed_ip,
                verification_scope="proxied_public_ip_observed",
            )
            return observed_ip, probe
    except (httpx.HTTPError, TimeoutError, UnicodeError, ValueError) as error:
        diagnostic: dict[str, JsonValue] = {"exception_type": type(error).__name__}
        if isinstance(error, httpx.HTTPStatusError):
            diagnostic["http_status"] = error.response.status_code
        raise ManagedProtonTransportFailure(
            "proton_egress_probe_failed", diagnostic=diagnostic
        ) from None
    finally:
        with anyio.CancelScope(shield=True):
            if response is not None:
                await response.aclose()
            await client.aclose()


class ProtonWireproxyManager:
    def __init__(self, process_manager: WireproxyProcessManager | None = None):
        self.process_manager = process_manager or WireproxyProcessManager()

    @asynccontextmanager
    async def open(self, settings: ProtonWireproxySettings) -> AsyncGenerator[ManagedProtonSession]:
        try:
            device_lock_identity = settings.device_lock_identity
        except ValueError:
            raise RouteConfigurationFailure("invalid_proton_configuration") from None
        try:
            async with self.process_manager.open(
                provider=NetworkProvider.PROTON,
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
                    timeout_seconds=settings.startup_timeout_seconds,
                )
                expected_version = {
                    InternetProtocolVersion.VERSION_4: 4,
                    InternetProtocolVersion.VERSION_6: 6,
                }.get(settings.route.internet_protocol_version)
                if expected_version is not None and observed_ip.version != expected_version:
                    raise ManagedProtonTransportFailure(
                        "proton_exit_ip_version_mismatch",
                        diagnostic={
                            "expected": settings.route.internet_protocol_version.value,
                            "observed": f"version_{observed_ip.version}",
                        },
                    )
                yield ManagedProtonSession(
                    route=settings.route,
                    configuration=settings.configuration,
                    process=process,
                    observed_exit_ip=observed_ip,
                    health_probe=probe,
                    validated_at_utc=_utc_now(),
                )
        except ManagedWireproxyFailure as error:
            raise ManagedProtonTransportFailure(
                error.code,
                exit_code=error.exit_code,
                diagnostic=error.diagnostic,
            ) from None


class ManagedProtonHttpAcquirer:
    def __init__(
        self,
        *,
        manager: ProtonWireproxyManager,
        settings: ProtonWireproxySettings,
    ):
        self.manager = manager
        self.settings = settings

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        if plan.routing != self.settings.route.network_path:
            raise RouteConfigurationFailure("request_route_does_not_match_proton_route")
        acquisition: Acquisition | None = None
        acquisition_failure: AcquisitionFailure | None = None
        session: ManagedProtonSession | None = None
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
                    acquisition_failure = error
        except RouteConfigurationFailure:
            raise
        except ManagedProtonTransportFailure as error:
            retained_bodies = (
                acquisition.bodies
                if acquisition is not None
                else acquisition_failure.bodies
                if acquisition_failure is not None
                else ()
            )
            raise AcquisitionFailure(
                "Managed Proton transport failed",
                result={
                    "hops": (
                        acquisition.record.get("hops", [])
                        if acquisition is not None
                        else acquisition_failure.result.get("hops", [])
                        if acquisition_failure is not None
                        else []
                    ),
                    "stopping_condition": "transport_failure",
                    "route_failure": {
                        "code": error.code,
                        "exit_code": error.exit_code,
                        "diagnostic": error.diagnostic,
                    },
                    "routing": {
                        "configured": list(plan.routing),
                        "observed": session.active_observation() if session is not None else None,
                    },
                },
                bodies=retained_bodies,
            ) from None
        if session is None:
            raise AssertionError("Managed Proton session was not created")
        observation = session.completed_observation()
        if acquisition_failure is not None:
            acquisition_failure.result["routing"] = {
                "configured": list(plan.routing),
                "observed": observation,
            }
            raise acquisition_failure
        if acquisition is None:
            raise AssertionError("Proton acquisition produced no outcome")
        acquisition.record["routing"] = {
            "configured": list(plan.routing),
            "observed": observation,
        }
        return acquisition


@final
class ManagedProtonWreqAcquirer:
    """Acquire one browser-profiled document through the configured Proton route."""

    def __init__(
        self,
        *,
        manager: ProtonSessionManager,
        settings: ProtonWireproxySettings,
        transport_settings: WreqTransportSettings | None = None,
        client_factory: WreqClientFactory | None = None,
    ):
        self.manager = manager
        self.settings = settings
        self.transport_settings = transport_settings or WreqTransportSettings()
        self.client_factory = client_factory

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        if plan.routing != self.settings.route.network_path:
            raise RouteConfigurationFailure("request_route_does_not_match_proton_route")
        session: ProtonSession | None = None
        try:
            async with self.manager.open(self.settings) as session:
                if self.client_factory is None:
                    acquirer = LocalSocks5WreqAcquirer(
                        endpoint=session.endpoint,
                        expected_routing=self.settings.route.network_path,
                        routing_observation=session.active_observation(),
                        settings=self.transport_settings,
                    )
                else:
                    acquirer = LocalSocks5WreqAcquirer(
                        endpoint=session.endpoint,
                        expected_routing=self.settings.route.network_path,
                        routing_observation=session.active_observation(),
                        settings=self.transport_settings,
                        client_factory=self.client_factory,
                    )
                acquisition = await acquirer.acquire(plan, new_identifier)
        except AcquisitionFailure as error:
            if session is not None:
                error.result["routing"] = {
                    "configured": list(plan.routing),
                    "observed": session.completed_observation(),
                }
            raise
        except ManagedProtonTransportFailure as error:
            raise AcquisitionFailure(
                "Managed Proton transport failed",
                result={
                    "hops": [],
                    "stopping_condition": "transport_failure",
                    "route_failure": {
                        "code": error.code,
                        "exit_code": error.exit_code,
                        "diagnostic": error.diagnostic,
                    },
                    "transport": self.transport_settings.safe_configuration(plan),
                    "routing": {
                        "configured": list(plan.routing),
                        "observed": None,
                    },
                },
            ) from None
        acquisition.record["routing"] = {
            "configured": list(plan.routing),
            "observed": session.completed_observation(),
        }
        return acquisition
