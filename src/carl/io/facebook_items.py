"""Coherent HTTP sessions for Facebook Marketplace item collection."""

from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Protocol

import anyio
import httpx

from carl.core.models import JsonValue
from carl.io.decodo import (
    DecodoCredentialSource,
    DecodoProxySettings,
    DecodoSessionManager,
    ManagedDecodoHttpAcquirer,
)
from carl.io.httpx import ClientHttpxAcquirer, HttpAcquirer
from carl.io.mullvad import (
    ManagedMullvadSession,
    ManagedMullvadTransportFailure,
    MullvadWireproxyManager,
    MullvadWireproxySettings,
)


class FacebookItemHttpSession(Protocol):
    identifier: str
    acquirer: HttpAcquirer

    def active_observation(self) -> dict[str, JsonValue]: ...

    def completed_observation(self) -> dict[str, JsonValue]: ...


@dataclass(slots=True)
class ManagedFacebookItemHttpSession:
    identifier: str
    acquirer: HttpAcquirer
    provider_session: ManagedMullvadSession

    def active_observation(self) -> dict[str, JsonValue]:
        return {
            "network_session_identifier": self.identifier,
            "provider_session": self.provider_session.active_observation(),
        }

    def completed_observation(self) -> dict[str, JsonValue]:
        return {
            "network_session_identifier": self.identifier,
            "provider_session": self.provider_session.completed_observation().as_json(),
        }


@dataclass(slots=True)
class DecodoFacebookItemHttpSession:
    identifier: str
    acquirer: HttpAcquirer

    def active_observation(self) -> dict[str, JsonValue]:
        return {
            "network_session_identifier": self.identifier,
            "provider_session_scope": "one_fresh_sticky_session_per_acquisition",
        }

    def completed_observation(self) -> dict[str, JsonValue]:
        return {
            **self.active_observation(),
            "state": "closed",
        }


class FacebookItemSessionFactory(Protocol):
    def __call__(self, identifier: str) -> AbstractAsyncContextManager[FacebookItemHttpSession]: ...


class FacebookItemSessionFailure(Exception):
    def __init__(
        self,
        code: str,
        *,
        provider: str,
        exit_code: int | None = None,
        diagnostic: dict[str, JsonValue] | None = None,
    ):
        super().__init__(code)
        self.code = code
        self.provider = provider
        self.exit_code = exit_code
        self.diagnostic = diagnostic or {}


class MullvadFacebookItemSessionFactory:
    def __init__(
        self,
        *,
        manager: MullvadWireproxyManager,
        settings: MullvadWireproxySettings,
    ):
        self.manager = manager
        self.settings = settings

    @asynccontextmanager
    async def __call__(self, identifier: str) -> AsyncGenerator[FacebookItemHttpSession]:
        try:
            async with self.manager.open(self.settings) as provider_session:
                client = httpx.AsyncClient(
                    proxy=provider_session.endpoint.url,
                    http2=True,
                    follow_redirects=False,
                    trust_env=False,
                )
                try:
                    yield ManagedFacebookItemHttpSession(
                        identifier=identifier,
                        acquirer=ClientHttpxAcquirer(
                            client=client,
                            expected_routing=self.settings.route.network_path,
                            routing_observation={
                                "network_session_identifier": identifier,
                                "provider_session": provider_session.active_observation(),
                            },
                            authentication="anonymous_guest_session",
                            protected_request_headers=frozenset(
                                {b"cookie", b"proxy-authorization"}
                            ),
                            protected_response_headers=frozenset({b"set-cookie"}),
                        ),
                        provider_session=provider_session,
                    )
                finally:
                    with anyio.CancelScope(shield=True):
                        client.cookies.clear()
                        await client.aclose()
        except ManagedMullvadTransportFailure as error:
            raise FacebookItemSessionFailure(
                error.code,
                provider="mullvad",
                exit_code=error.exit_code,
                diagnostic=error.diagnostic,
            ) from None


class DecodoFacebookItemSessionFactory:
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

    @asynccontextmanager
    async def __call__(self, identifier: str) -> AsyncGenerator[FacebookItemHttpSession]:
        yield DecodoFacebookItemHttpSession(
            identifier=identifier,
            acquirer=ManagedDecodoHttpAcquirer(
                manager=self.manager,
                settings=self.settings,
                credential_source=self.credential_source,
            ),
        )
