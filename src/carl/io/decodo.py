"""Decodo native-proxy sessions with sticky peer and cookie continuity."""

import secrets
import string
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic
from typing import Protocol, final

import anyio
import httpx
import wreq
from pydantic import Field, SecretStr

from carl.core.http import FormField, RequestPlan
from carl.core.models import ConfigurationDocumentIdentity, JsonValue, StrictModel
from carl.core.routing import DecodoProduct, DecodoRouteIdentity, DecodoSessionObservation
from carl.io.httpx import (
    Acquisition,
    AcquisitionFailure,
    ClientHttpxAcquirer,
    IdentifierFactory,
    RouteConfigurationFailure,
)
from carl.io.wreq import (
    ProxyWreqAcquirer,
    WreqClientFactory,
    WreqTransportSettings,
)

_PROVIDER_SESSION_ALPHABET = string.ascii_letters + string.digits
_PROTECTED_REQUEST_HEADERS = frozenset({b"cookie", b"proxy-authorization", b"x-fb-lsd"})
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
    sticky_username, password = _sticky_proxy_credentials(
        route,
        credentials,
        provider_session_identifier,
        session_duration_minutes,
    )
    return httpx.Proxy(route.endpoint.url, auth=(sticky_username, password))


def _sticky_proxy_credentials(
    route: DecodoRouteIdentity,
    credentials: DecodoCredentials,
    provider_session_identifier: str,
    session_duration_minutes: int,
) -> tuple[str, str]:
    password = credentials.proxy_password.get_secret_value()
    username = route.proxy_username
    if not password or any(
        part in username for part in ("-country-", "-session-", "-sessionduration-")
    ):
        raise RouteConfigurationFailure("invalid_decodo_credentials")
    if route.product is DecodoProduct.DATACENTER_PROXY:
        if not 10000 <= route.endpoint.port <= 63000:
            raise RouteConfigurationFailure("invalid_decodo_datacenter_port")
        return f"user-{username}-country-{route.country_code}", password
    if (
        not provider_session_identifier
        or not provider_session_identifier.isalnum()
        or not provider_session_identifier.isascii()
    ):
        raise RouteConfigurationFailure("invalid_decodo_session_identifier")
    sticky_username = (
        f"user-{username}-country-{route.country_code}-session-{provider_session_identifier}"
        f"-sessionduration-{session_duration_minutes}"
    )
    return sticky_username, password


def _session_options(settings: DecodoProxySettings) -> dict[str, JsonValue]:
    if settings.route.product is DecodoProduct.DATACENTER_PROXY:
        return {
            "proxy_username_shape": "base_username_with_country",
            "sticky_peer_requested": settings.route.endpoint.port != 10000,
            "configured_session_duration_minutes": None,
        }
    return {
        "proxy_username_shape": "base_username_with_country_sticky_session_and_duration",
        "sticky_peer_requested": True,
        "configured_session_duration_minutes": settings.session_duration_minutes,
    }


def _provider_result(record: dict[str, JsonValue]) -> dict[str, JsonValue] | None:
    if record.get("stopping_condition") == "transport_failure" and record.get("exception_type") in {
        "ProxyError",
        "ProxyConnectionError",
    }:
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
    target_authentication: str = "anonymous_cookie_session"
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
            **_session_options(self.settings),
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
            proxy_username_shape=(
                "base_username_with_country"
                if self.settings.route.product is DecodoProduct.DATACENTER_PROXY
                else "base_username_with_country_sticky_session_and_duration"
            ),
            sticky_peer_requested=(
                self.settings.route.product is not DecodoProduct.DATACENTER_PROXY
                or self.settings.route.endpoint.port != 10000
            ),
            configured_session_duration_minutes=(
                None
                if self.settings.route.product is DecodoProduct.DATACENTER_PROXY
                else self.settings.session_duration_minutes
            ),
            tls_verification="system_roots",
            started_at_utc=self.started_at_utc,
            ended_at_utc=self.ended_at_utc,
            requests_started=self.requests_started,
            requests_completed=self.requests_completed,
            failed=self.failed,
            shutdown_state="closed",
        )

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        return await self._acquire(plan, new_identifier, form_fields=None)

    async def acquire_form(
        self,
        plan: RequestPlan,
        fields: tuple[FormField, ...],
        new_identifier: IdentifierFactory,
    ) -> Acquisition:
        if plan.method != "POST":
            raise ValueError("Form acquisition requires POST")
        return await self._acquire(plan, new_identifier, form_fields=fields)

    async def _acquire(
        self,
        plan: RequestPlan,
        new_identifier: IdentifierFactory,
        *,
        form_fields: tuple[FormField, ...] | None,
    ) -> Acquisition:
        if plan.routing != self.settings.route.network_path:
            raise RouteConfigurationFailure("request_route_does_not_match_decodo_route")
        async with self._lock:
            if self.failed:
                raise RouteConfigurationFailure("decodo_session_failed")
            maximum_age_seconds = self.settings.session_duration_minutes * 60
            if (
                self.settings.route.product is not DecodoProduct.DATACENTER_PROXY
                and monotonic() - self._started_monotonic >= maximum_age_seconds
            ):
                raise RouteConfigurationFailure("decodo_session_duration_expired")
            self.requests_started += 1
            acquirer = ClientHttpxAcquirer(
                client=self.client,
                expected_routing=self.settings.route.network_path,
                routing_observation=self.active_observation(),
                authentication={
                    "target": self.target_authentication,
                    "network": {
                        "kind": "proxy_basic",
                        "credential_reference": list(self.settings.route.credential_reference),
                    },
                },
                protected_request_headers=_PROTECTED_REQUEST_HEADERS,
                protected_response_headers=_PROTECTED_RESPONSE_HEADERS,
            )
            try:
                acquisition = (
                    await acquirer.acquire(plan, new_identifier)
                    if form_fields is None
                    else await acquirer.acquire_form(plan, form_fields, new_identifier)
                )
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
        target_authentication: str = "anonymous_cookie_session",
    ) -> AsyncGenerator[ManagedDecodoSession]:
        stack = AsyncExitStack()
        client: httpx.AsyncClient | None = None
        session: ManagedDecodoSession | None = None
        body_error: BaseException | None = None
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
                target_authentication=target_authentication,
            )
            yield session
        except BaseException as error:
            body_error = error
            if session is not None:
                session.failed = True
            raise
        finally:
            close_failure: AcquisitionFailure | None = None
            with anyio.CancelScope(shield=True):
                try:
                    if client is not None:
                        try:
                            client.cookies.clear()
                            await client.aclose()
                        except Exception as error:
                            if session is not None:
                                session.failed = True
                            close_failure = AcquisitionFailure(
                                "HTTP transport failed while closing a Decodo client",
                                result={
                                    "hops": [],
                                    "stopping_condition": "transport_failure",
                                    "exception_type": type(error).__name__,
                                    "failure_phase": "client_close",
                                },
                            )
                finally:
                    if session is not None:
                        session.ended_at_utc = datetime.now(UTC).isoformat()
                    await stack.aclose()
            if close_failure is not None:
                if body_error is None:
                    raise close_failure from None
                if isinstance(body_error, AcquisitionFailure):
                    body_error.result["cleanup_failure"] = close_failure.result
                else:
                    body_error.add_note(
                        "Decodo client cleanup also failed: "
                        f"{close_failure.result['exception_type']}"
                    )


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


def _wreq_session_observation(
    *,
    settings: DecodoProxySettings,
    transport_settings: WreqTransportSettings,
    session_record_identifier: str,
    started_at_utc: str,
    ended_at_utc: str | None,
    failed: bool,
    requests_started: int = 1,
    requests_completed: int | None = None,
    shared_session: bool = False,
) -> dict[str, JsonValue]:
    return {
        "provider": settings.route.provider.value,
        "route": settings.route.model_dump(mode="json"),
        "configuration": settings.configuration.as_json(),
        "session_record_identifier": session_record_identifier,
        **_session_options(settings),
        "tls_certificate_verification": transport_settings.tls_certificate_verification,
        "tls_trust_store": transport_settings.tls_trust_store,
        "started_at_utc": started_at_utc,
        "ended_at_utc": ended_at_utc,
        "requests_started": requests_started,
        "requests_completed": (1 if ended_at_utc is not None else 0)
        if requests_completed is None
        else requests_completed,
        "session_scope": "managed_session" if shared_session else "acquisition",
        "failed": failed,
        "state": "closed" if ended_at_utc is not None else "active",
    }


@dataclass(slots=True)
class ManagedDecodoWreqSession:
    """One sticky provider identity and cookie jar, without mid-session rotation."""

    settings: DecodoProxySettings
    transport_settings: WreqTransportSettings
    acquirer: ProxyWreqAcquirer
    session_record_identifier: str
    started_at_utc: str
    _started_monotonic: float
    requests_started: int = 0
    requests_completed: int = 0
    ended_at_utc: str | None = None
    failed: bool = False
    _lock: anyio.Lock = field(default_factory=anyio.Lock)

    def observation(self) -> dict[str, JsonValue]:
        return _wreq_session_observation(
            settings=self.settings,
            transport_settings=self.transport_settings,
            session_record_identifier=self.session_record_identifier,
            started_at_utc=self.started_at_utc,
            ended_at_utc=self.ended_at_utc,
            failed=self.failed,
            requests_started=self.requests_started,
            requests_completed=self.requests_completed,
            shared_session=True,
        )

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        if plan.routing != self.settings.route.network_path:
            raise RouteConfigurationFailure("request_route_does_not_match_decodo_route")
        async with self._lock:
            if self.ended_at_utc is not None or self.failed:
                raise RouteConfigurationFailure("decodo_session_not_available")
            if (
                self.settings.route.product is not DecodoProduct.DATACENTER_PROXY
                and monotonic() - self._started_monotonic
                >= self.settings.session_duration_minutes * 60
            ):
                self.failed = True
                raise RouteConfigurationFailure("decodo_session_duration_expired")
            self.requests_started += 1
            try:
                acquisition = await self.acquirer.acquire(plan, new_identifier)
            except AcquisitionFailure as error:
                self.failed = True
                self.requests_completed += 1
                error.result["routing"] = {
                    "configured": list(plan.routing),
                    "observed": self.observation(),
                }
                provider_result = _provider_result(error.result)
                if provider_result is not None:
                    error.result["network_provider_result"] = provider_result
                raise
            except BaseException:
                self.failed = True
                raise
            self.requests_completed += 1
            acquisition.record["routing"] = {
                "configured": list(plan.routing),
                "observed": self.observation(),
            }
            return acquisition


@final
class ManagedDecodoWreqAcquirer:
    """Use a fresh Decodo sticky session for one browser-profiled acquisition."""

    def __init__(
        self,
        *,
        settings: DecodoProxySettings,
        credential_source: DecodoCredentialSource,
        transport_settings: WreqTransportSettings | None = None,
        client_factory: WreqClientFactory | None = None,
        provider_session_factory: Callable[[], str] = _provider_session_identifier,
    ):
        self.settings = settings
        self.credential_source = credential_source
        self.transport_settings = transport_settings or WreqTransportSettings()
        self.client_factory = client_factory
        self.provider_session_factory = provider_session_factory

    @asynccontextmanager
    async def session(
        self, new_identifier: IdentifierFactory
    ) -> AsyncGenerator[ManagedDecodoWreqSession]:
        """Open a fresh session for one independent multi-page acquisition attempt."""
        session: ManagedDecodoWreqSession | None = None
        credential_stack = AsyncExitStack()
        try:
            credentials = await credential_stack.enter_async_context(self.credential_source.open())
            username, password = _sticky_proxy_credentials(
                self.settings.route,
                credentials,
                self.provider_session_factory(),
                self.settings.session_duration_minutes,
            )
            session_identifier = new_identifier()
            started_at_utc = datetime.now(UTC).isoformat()
            started_monotonic = monotonic()
            options = {} if self.client_factory is None else {"client_factory": self.client_factory}
            proxy_acquirer = ProxyWreqAcquirer(
                proxy_factory=lambda: wreq.Proxy.all(
                    self.settings.route.endpoint.url, username=username, password=password
                ),
                expected_routing=self.settings.route.network_path,
                routing_observation=_wreq_session_observation(
                    settings=self.settings,
                    transport_settings=self.transport_settings,
                    session_record_identifier=session_identifier,
                    started_at_utc=started_at_utc,
                    ended_at_utc=None,
                    failed=False,
                    requests_started=0,
                    requests_completed=0,
                    shared_session=True,
                ),
                authentication={
                    "target": "anonymous_cookie_session",
                    "network": {
                        "kind": "proxy_basic",
                        "credential_reference": list(self.settings.route.credential_reference),
                    },
                },
                settings=self.transport_settings,
                **options,
            )
            async with proxy_acquirer.session() as shared_acquirer:
                session = ManagedDecodoWreqSession(
                    settings=self.settings,
                    transport_settings=self.transport_settings,
                    acquirer=shared_acquirer,
                    session_record_identifier=session_identifier,
                    started_at_utc=started_at_utc,
                    _started_monotonic=started_monotonic,
                )
                yield session
        except BaseException:
            if session is not None:
                session.failed = True
            raise
        finally:
            with anyio.CancelScope(shield=True):
                try:
                    await credential_stack.aclose()
                finally:
                    if session is not None:
                        session.ended_at_utc = datetime.now(UTC).isoformat()

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        if plan.routing != self.settings.route.network_path:
            raise RouteConfigurationFailure("request_route_does_not_match_decodo_route")
        provider_session_identifier = self.provider_session_factory()
        session_record_identifier = new_identifier()
        started_at_utc = datetime.now(UTC).isoformat()
        acquisition: Acquisition | None = None
        failure: AcquisitionFailure | None = None
        async with self.credential_source.open() as credentials:
            username, password = _sticky_proxy_credentials(
                self.settings.route,
                credentials,
                provider_session_identifier,
                self.settings.session_duration_minutes,
            )
            active_observation = _wreq_session_observation(
                settings=self.settings,
                transport_settings=self.transport_settings,
                session_record_identifier=session_record_identifier,
                started_at_utc=started_at_utc,
                ended_at_utc=None,
                failed=False,
            )

            def proxy_factory() -> wreq.Proxy:
                return wreq.Proxy.all(
                    self.settings.route.endpoint.url,
                    username=username,
                    password=password,
                )

            authentication: dict[str, JsonValue] = {
                "target": "anonymous_cookie_session",
                "network": {
                    "kind": "proxy_basic",
                    "credential_reference": list(self.settings.route.credential_reference),
                },
            }
            if self.client_factory is None:
                acquirer = ProxyWreqAcquirer(
                    proxy_factory=proxy_factory,
                    expected_routing=self.settings.route.network_path,
                    routing_observation=active_observation,
                    authentication=authentication,
                    settings=self.transport_settings,
                )
            else:
                acquirer = ProxyWreqAcquirer(
                    proxy_factory=proxy_factory,
                    expected_routing=self.settings.route.network_path,
                    routing_observation=active_observation,
                    authentication=authentication,
                    settings=self.transport_settings,
                    client_factory=self.client_factory,
                )
            try:
                acquisition = await acquirer.acquire(plan, new_identifier)
            except AcquisitionFailure as error:
                failure = error
        ended_at_utc = datetime.now(UTC).isoformat()
        observation = _wreq_session_observation(
            settings=self.settings,
            transport_settings=self.transport_settings,
            session_record_identifier=session_record_identifier,
            started_at_utc=started_at_utc,
            ended_at_utc=ended_at_utc,
            failed=failure is not None,
        )
        if failure is not None:
            failure.result["routing"] = {
                "configured": list(plan.routing),
                "observed": observation,
            }
            provider_result = _provider_result(failure.result)
            if provider_result is not None:
                failure.result["network_provider_result"] = provider_result
            raise failure
        if acquisition is None:
            raise AssertionError("Decodo wreq acquisition produced no outcome")
        acquisition.record["routing"] = {
            "configured": list(plan.routing),
            "observed": observation,
        }
        return acquisition
