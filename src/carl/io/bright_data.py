"""Bright Data native-proxy sessions with sticky peer and cookie continuity."""

import secrets
import string
import sys
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic
from typing import Protocol

import anyio
import httpx
from pydantic import Field, SecretStr

from carl.core.http import RequestPlan
from carl.core.models import ConfigurationDocumentIdentity, JsonValue, StrictModel
from carl.core.routing import BrightDataRouteIdentity, BrightDataSessionObservation
from carl.io.cleanup import shielded_cleanup
from carl.io.httpx import (
    Acquisition,
    AcquisitionFailure,
    ClientHttpxAcquirer,
    IdentifierFactory,
    RouteConfigurationFailure,
    close_httpx_client,
)

_PROVIDER_SESSION_ALPHABET = string.ascii_letters + string.digits
_PROTECTED_REQUEST_HEADERS = frozenset({b"cookie", b"proxy-authorization"})
_PROTECTED_RESPONSE_HEADERS = frozenset(
    {
        b"set-cookie",
        b"proxy-authenticate",
        b"proxy-status",
        b"x-brd-error",
        b"x-brd-err-msg",
    }
)


class BrightDataProxySettings(StrictModel):
    route: BrightDataRouteIdentity
    configuration: ConfigurationDocumentIdentity
    max_idle_seconds: float = Field(default=240.0, gt=0, lt=300)

    def safe_configuration(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class BrightDataCredentials(StrictModel):
    """Secret proxy values that must never become ordinary provenance."""

    proxy_password: SecretStr = Field(exclude=True, repr=False)


class BrightDataCredentialSource(Protocol):
    def open(self) -> AbstractAsyncContextManager[BrightDataCredentials]: ...


@dataclass(frozen=True, slots=True)
class StaticBrightDataCredentialSource:
    """A small injection adapter; production configuration can replace it."""

    credentials: BrightDataCredentials = field(repr=False)

    @asynccontextmanager
    async def open(self) -> AsyncGenerator[BrightDataCredentials]:
        yield self.credentials


class BrightDataClientFactory(Protocol):
    def __call__(self, *, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient: ...


def _default_client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        http2=True,
        follow_redirects=False,
        trust_env=False,
        proxy=proxy,
        verify=verify,
    )


def _provider_session_identifier() -> str:
    return "".join(secrets.choice(_PROVIDER_SESSION_ALPHABET) for _ in range(20))


def _sticky_proxy(
    route: BrightDataRouteIdentity,
    credentials: BrightDataCredentials,
    provider_session_identifier: str,
) -> httpx.Proxy:
    if (
        not provider_session_identifier
        or not provider_session_identifier.isalnum()
        or not provider_session_identifier.isascii()
    ):
        raise RouteConfigurationFailure("invalid_bright_data_session_identifier")
    password = credentials.proxy_password.get_secret_value()
    username = route.proxy_username
    if not password or "-session-" in username or username.endswith("-const"):
        raise RouteConfigurationFailure("invalid_bright_data_credentials")
    sticky_username = f"{username}-session-{provider_session_identifier}-const"
    return httpx.Proxy(route.endpoint.url, auth=(sticky_username, password))


def _stored_header_value(headers: object, name: str) -> str | None:
    if not isinstance(headers, list):
        return None
    value = None
    for header in headers:
        if not isinstance(header, dict):
            continue
        if header.get("name_latin1", "").lower() != name:
            continue
        candidate = header.get("value_latin1")
        if isinstance(candidate, str):
            value = candidate
    return value


def _provider_result(record: dict[str, JsonValue]) -> dict[str, JsonValue] | None:
    if (
        record.get("stopping_condition") == "transport_failure"
        and record.get("exception_type") == "ProxyError"
    ):
        return {
            "provider": "bright_data",
            "category": "proxy_transport_or_authentication",
            "http_status": None,
            "error_code": None,
        }
    hops = record.get("hops")
    if not isinstance(hops, list) or not hops:
        return None
    final = hops[-1]
    if not isinstance(final, dict):
        return None
    response = final.get("response")
    if not isinstance(response, dict):
        return None
    status = response.get("status_code")
    headers = response.get("headers")
    error_code = _stored_header_value(headers, "x-brd-err-code")
    if error_code is None:
        return None
    if status == 407:
        category = "proxy_authentication_or_configuration"
    elif status == 429:
        category = "provider_limit"
    elif status == 502:
        category = "provider_peer_or_upstream_failure"
    else:
        category = "provider_reported_error"
    return {
        "provider": "bright_data",
        "category": category,
        "http_status": status,
        "error_code": error_code,
    }


@dataclass(slots=True)
class ManagedBrightDataSession:
    settings: BrightDataProxySettings
    client: httpx.AsyncClient
    session_record_identifier: str
    started_at_utc: str
    _last_activity: float
    _lock: anyio.Lock = field(default_factory=anyio.Lock)
    requests_started: int = 0
    requests_completed: int = 0
    ended_at_utc: str | None = None
    failed: bool = False

    def _observation(self, *, state: str) -> dict[str, JsonValue]:
        return {
            "provider": self.settings.route.provider.value,
            "route": self.settings.route.model_dump(mode="json"),
            "configuration": self.settings.configuration.as_json(),
            "session_record_identifier": self.session_record_identifier,
            "proxy_username_shape": "zone_username_with_sticky_session",
            "constant_peer": True,
            "provider_idle_expiry_seconds": 300,
            "configured_idle_guard_seconds": self.settings.max_idle_seconds,
            "tls_verification": "system_roots",
            "started_at_utc": self.started_at_utc,
            "requests_started": self.requests_started,
            "requests_completed": self.requests_completed,
            "state": state,
        }

    def active_observation(self) -> dict[str, JsonValue]:
        return self._observation(state="active")

    def completed_observation(self) -> BrightDataSessionObservation:
        if self.ended_at_utc is None:
            raise RuntimeError("Bright Data session has not completed cleanup")
        return BrightDataSessionObservation(
            route=self.settings.route,
            configuration=self.settings.configuration,
            session_record_identifier=self.session_record_identifier,
            proxy_username_shape="zone_username_with_sticky_session",
            constant_peer=True,
            provider_idle_expiry_seconds=300,
            configured_idle_guard_seconds=self.settings.max_idle_seconds,
            tls_verification="system_roots",
            started_at_utc=self.started_at_utc,
            ended_at_utc=self.ended_at_utc,
            requests_started=self.requests_started,
            requests_completed=self.requests_completed,
            failed=self.failed,
            shutdown_state="closed",
        )

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        if plan.routing != self.settings.route.network_path:
            raise RouteConfigurationFailure("request_route_does_not_match_bright_data_route")
        async with self._lock:
            if self.failed:
                raise RouteConfigurationFailure("bright_data_session_failed")
            if monotonic() - self._last_activity > self.settings.max_idle_seconds:
                raise RouteConfigurationFailure("bright_data_session_idle_expired")
            self.requests_started += 1
            acquirer = ClientHttpxAcquirer(
                client=self.client,
                expected_routing=self.settings.route.network_path,
                routing_observation=self.active_observation(),
                authentication={
                    "target": "anonymous_cookie_session",
                    "network": {
                        "kind": "proxy_basic",
                        "credential_reference": list(self.settings.route.credential_reference),
                    },
                },
                protected_request_headers=_PROTECTED_REQUEST_HEADERS,
                protected_response_headers=_PROTECTED_RESPONSE_HEADERS,
            )
            try:
                acquisition = await acquirer.acquire(plan, new_identifier)
            except AcquisitionFailure as error:
                self.failed = True
                self.requests_completed += 1
                self._last_activity = monotonic()
                error.result["routing"] = {
                    "configured": list(plan.routing),
                    "observed": self.active_observation(),
                }
                provider_result = _provider_result(error.result)
                if provider_result is not None:
                    error.result["network_provider_result"] = provider_result
                raise
            except BaseException:
                self.failed = True
                raise
            else:
                self.requests_completed += 1
                self._last_activity = monotonic()
                acquisition.record["routing"] = {
                    "configured": list(plan.routing),
                    "observed": self.active_observation(),
                }
                provider_result = _provider_result(acquisition.record)
                if provider_result is not None:
                    acquisition.record["network_provider_result"] = provider_result
                    acquisition.record["stopping_condition"] = "network_provider_failure"
                    acquisition.record["collection_completeness"] = "failed"
                    self.failed = True
                    raise AcquisitionFailure(
                        "Bright Data reported a provider failure",
                        result=acquisition.record,
                        bodies=acquisition.bodies,
                    )
                return acquisition


class BrightDataSessionManager:
    def __init__(
        self,
        *,
        client_factory: BrightDataClientFactory = _default_client_factory,
        provider_session_factory: Callable[[], str] = _provider_session_identifier,
    ):
        self.client_factory = client_factory
        self.provider_session_factory = provider_session_factory

    @asynccontextmanager
    async def open(
        self,
        *,
        settings: BrightDataProxySettings,
        credential_source: BrightDataCredentialSource,
        new_identifier: IdentifierFactory,
    ) -> AsyncGenerator[ManagedBrightDataSession]:
        stack = AsyncExitStack()
        client: httpx.AsyncClient | None = None
        session: ManagedBrightDataSession | None = None
        try:
            credentials = await stack.enter_async_context(credential_source.open())
            proxy = _sticky_proxy(
                settings.route,
                credentials,
                self.provider_session_factory(),
            )
            client = self.client_factory(proxy=proxy, verify=True)
            session = ManagedBrightDataSession(
                settings=settings,
                client=client,
                session_record_identifier=new_identifier(),
                started_at_utc=datetime.now(UTC).isoformat(),
                _last_activity=monotonic(),
            )
            yield session
        finally:
            primary_error = sys.exception()
            try:
                if client is not None:
                    await close_httpx_client(client, primary_error=primary_error)
            finally:
                if session is not None:
                    session.ended_at_utc = datetime.now(UTC).isoformat()
                async with shielded_cleanup(
                    "bright_data_credentials", primary_error=primary_error or sys.exception()
                ):
                    await stack.aclose()


class ManagedBrightDataHttpAcquirer:
    """Open a fresh managed provider session for one independent request."""

    def __init__(
        self,
        *,
        manager: BrightDataSessionManager,
        settings: BrightDataProxySettings,
        credential_source: BrightDataCredentialSource,
    ):
        self.manager = manager
        self.settings = settings
        self.credential_source = credential_source

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        acquisition: Acquisition | None = None
        failure: AcquisitionFailure | None = None
        session: ManagedBrightDataSession | None = None
        async with self.manager.open(
            settings=self.settings,
            credential_source=self.credential_source,
            new_identifier=new_identifier,
        ) as opened_session:
            session = opened_session
            try:
                acquisition = await opened_session.acquire(plan, new_identifier)
            except AcquisitionFailure as error:
                failure = error
        if session is None:
            raise AssertionError("Bright Data session was not created")
        observation = session.completed_observation().as_json()
        if failure is not None:
            failure.result["routing"] = {
                "configured": list(plan.routing),
                "observed": observation,
            }
            raise failure
        if acquisition is None:
            raise AssertionError("Bright Data acquisition produced no outcome")
        acquisition.record["routing"] = {
            "configured": list(plan.routing),
            "observed": observation,
        }
        return acquisition
