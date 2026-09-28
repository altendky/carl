"""Decodo native-proxy sessions with sticky peer and cookie continuity."""

import secrets
import string
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
from carl.core.routing import DecodoRouteIdentity, DecodoSessionObservation
from carl.io.httpx import (
    Acquisition,
    AcquisitionFailure,
    ClientHttpxAcquirer,
    IdentifierFactory,
    RouteConfigurationFailure,
)

_PROVIDER_SESSION_ALPHABET = string.ascii_letters + string.digits
_PROTECTED_REQUEST_HEADERS = frozenset({b"cookie", b"proxy-authorization"})
_PROTECTED_RESPONSE_HEADERS = frozenset(
    {
        b"set-cookie",
        b"proxy-authenticate",
        b"proxy-status",
    }
)


class DecodoProxySettings(StrictModel):
    route: DecodoRouteIdentity
    configuration: ConfigurationDocumentIdentity
    session_duration_minutes: int = Field(default=10, ge=1, le=1440)

    def safe_configuration(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class DecodoCredentials(StrictModel):
    """Secret proxy values that must never become ordinary provenance."""

    proxy_password: SecretStr = Field(exclude=True, repr=False)


class DecodoCredentialSource(Protocol):
    def open(self) -> AbstractAsyncContextManager[DecodoCredentials]: ...


@dataclass(frozen=True, slots=True)
class StaticDecodoCredentialSource:
    """A small injection adapter; production configuration can replace it."""

    credentials: DecodoCredentials = field(repr=False)

    @asynccontextmanager
    async def open(self) -> AsyncGenerator[DecodoCredentials]:
        yield self.credentials


class DecodoClientFactory(Protocol):
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
    route: DecodoRouteIdentity,
    credentials: DecodoCredentials,
    provider_session_identifier: str,
    session_duration_minutes: int,
) -> httpx.Proxy:
    if (
        not provider_session_identifier
        or not provider_session_identifier.isalnum()
        or not provider_session_identifier.isascii()
    ):
        raise RouteConfigurationFailure("invalid_decodo_session_identifier")
    password = credentials.proxy_password.get_secret_value()
    username = route.proxy_username
    if not password or any(
        part in username for part in ("-country-", "-session-", "-sessionduration-")
    ):
        raise RouteConfigurationFailure("invalid_decodo_credentials")
    sticky_username = (
        f"user-{username}-country-{route.country_code}-session-{provider_session_identifier}"
        f"-sessionduration-{session_duration_minutes}"
    )
    return httpx.Proxy(route.endpoint.url, auth=(sticky_username, password))


def _provider_result(record: dict[str, JsonValue]) -> dict[str, JsonValue] | None:
    if (
        record.get("stopping_condition") == "transport_failure"
        and record.get("exception_type") == "ProxyError"
    ):
        return {
            "provider": "decodo",
            "category": "proxy_transport_or_authentication",
            "http_status": None,
            "error_code": None,
        }
    return None


@dataclass(slots=True)
class ManagedDecodoSession:
    settings: DecodoProxySettings
    client: httpx.AsyncClient
    session_record_identifier: str
    started_at_utc: str
    _started_monotonic: float
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
            "proxy_username_shape": ("base_username_with_country_sticky_session_and_duration"),
            "sticky_peer_requested": True,
            "configured_session_duration_minutes": self.settings.session_duration_minutes,
            "tls_verification": "system_roots",
            "started_at_utc": self.started_at_utc,
            "requests_started": self.requests_started,
            "requests_completed": self.requests_completed,
            "state": state,
        }

    def active_observation(self) -> dict[str, JsonValue]:
        return self._observation(state="active")

    def completed_observation(self) -> DecodoSessionObservation:
        if self.ended_at_utc is None:
            raise RuntimeError("Decodo session has not completed cleanup")
        return DecodoSessionObservation(
            route=self.settings.route,
            configuration=self.settings.configuration,
            session_record_identifier=self.session_record_identifier,
            proxy_username_shape="base_username_with_country_sticky_session_and_duration",
            sticky_peer_requested=True,
            configured_session_duration_minutes=self.settings.session_duration_minutes,
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
            raise RouteConfigurationFailure("request_route_does_not_match_decodo_route")
        async with self._lock:
            if self.failed:
                raise RouteConfigurationFailure("decodo_session_failed")
            maximum_age_seconds = self.settings.session_duration_minutes * 60
            if monotonic() - self._started_monotonic >= maximum_age_seconds:
                raise RouteConfigurationFailure("decodo_session_duration_expired")
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
                        "Decodo reported a provider failure",
                        result=acquisition.record,
                        bodies=acquisition.bodies,
                    )
                return acquisition


class DecodoSessionManager:
    def __init__(
        self,
        *,
        client_factory: DecodoClientFactory = _default_client_factory,
        provider_session_factory: Callable[[], str] = _provider_session_identifier,
    ):
        self.client_factory = client_factory
        self.provider_session_factory = provider_session_factory

    @asynccontextmanager
    async def open(
        self,
        *,
        settings: DecodoProxySettings,
        credential_source: DecodoCredentialSource,
        new_identifier: IdentifierFactory,
    ) -> AsyncGenerator[ManagedDecodoSession]:
        stack = AsyncExitStack()
        client: httpx.AsyncClient | None = None
        session: ManagedDecodoSession | None = None
        try:
            credentials = await stack.enter_async_context(credential_source.open())
            proxy = _sticky_proxy(
                settings.route,
                credentials,
                self.provider_session_factory(),
                settings.session_duration_minutes,
            )
            client = self.client_factory(proxy=proxy, verify=True)
            session = ManagedDecodoSession(
                settings=settings,
                client=client,
                session_record_identifier=new_identifier(),
                started_at_utc=datetime.now(UTC).isoformat(),
                _started_monotonic=monotonic(),
            )
            yield session
        finally:
            with anyio.CancelScope(shield=True):
                try:
                    if client is not None:
                        client.cookies.clear()
                        await client.aclose()
                finally:
                    if session is not None:
                        session.ended_at_utc = datetime.now(UTC).isoformat()
                    await stack.aclose()


class ManagedDecodoHttpAcquirer:
    """Open a fresh managed provider session for one independent request."""

    def __init__(
        self,
        *,
        manager: DecodoSessionManager,
        settings: DecodoProxySettings,
        credential_source: DecodoCredentialSource,
    ):
        self.manager = manager
        self.settings = settings
        self.credential_source = credential_source

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        acquisition: Acquisition | None = None
        failure: AcquisitionFailure | None = None
        session: ManagedDecodoSession | None = None
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
            raise AssertionError("Decodo session was not created")
        observation = session.completed_observation().as_json()
        if failure is not None:
            failure.result["routing"] = {
                "configured": list(plan.routing),
                "observed": observation,
            }
            raise failure
        if acquisition is None:
            raise AssertionError("Decodo acquisition produced no outcome")
        acquisition.record["routing"] = {
            "configured": list(plan.routing),
            "observed": observation,
        }
        return acquisition
