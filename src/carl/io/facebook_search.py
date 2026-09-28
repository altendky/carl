"""Coherent HTTP sessions for Facebook Marketplace search collection."""

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Protocol

import anyio
import httpx

from carl.core.models import JsonValue
from carl.io.httpx import AcquisitionFailure, ClientHttpxAcquirer, HttpFormAcquirer
from carl.io.proton import (
    ManagedProtonTransportFailure,
    ProtonSession,
    ProtonSessionManager,
    ProtonWireproxySettings,
)


class FacebookSearchHttpSession(Protocol):
    identifier: str
    acquirer: HttpFormAcquirer

    def active_observation(self) -> dict[str, JsonValue]: ...

    def completed_observation(self) -> dict[str, JsonValue]: ...


@dataclass(slots=True)
class ManagedFacebookSearchHttpSession:
    identifier: str
    acquirer: HttpFormAcquirer
    provider_session: ProtonSession

    def active_observation(self) -> dict[str, JsonValue]:
        return {
            "network_session_identifier": self.identifier,
            "provider_session": self.provider_session.active_observation(),
        }

    def completed_observation(self) -> dict[str, JsonValue]:
        return {
            "network_session_identifier": self.identifier,
            "provider_session": self.provider_session.completed_observation(),
        }


class FacebookSearchSessionFactory(Protocol):
    def __call__(
        self, identifier: str
    ) -> AbstractAsyncContextManager[FacebookSearchHttpSession]: ...


class FacebookSearchSessionFailure(Exception):
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


class ProtonFacebookSearchSessionFactory:
    def __init__(
        self,
        *,
        manager: ProtonSessionManager,
        settings: ProtonWireproxySettings,
    ):
        self.manager = manager
        self.settings = settings

    @asynccontextmanager
    async def __call__(self, identifier: str) -> AsyncIterator[FacebookSearchHttpSession]:
        provider_session: ProtonSession | None = None
        try:
            async with self.manager.open(self.settings) as provider_session:
                client = httpx.AsyncClient(
                    proxy=provider_session.endpoint.url,
                    http2=True,
                    follow_redirects=False,
                    trust_env=False,
                )
                try:
                    yield ManagedFacebookSearchHttpSession(
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
                                {b"cookie", b"proxy-authorization", b"x-fb-lsd"}
                            ),
                            protected_response_headers=frozenset({b"set-cookie"}),
                        ),
                        provider_session=provider_session,
                    )
                finally:
                    try:
                        with anyio.CancelScope(shield=True):
                            client.cookies.clear()
                            await client.aclose()
                    except Exception as error:
                        raise AcquisitionFailure(
                            "HTTP transport failed while closing a search client",
                            result={
                                "hops": [],
                                "stopping_condition": "transport_failure",
                                "exception_type": type(error).__name__,
                                "failure_phase": "client_close",
                            },
                        ) from error
        except AcquisitionFailure as error:
            if provider_session is not None:
                error.result["routing"] = {
                    "configured": list(self.settings.route.network_path),
                    "observed": {
                        "network_session_identifier": identifier,
                        "provider_session": provider_session.completed_observation(),
                    },
                }
            raise
        except ManagedProtonTransportFailure as error:
            raise FacebookSearchSessionFailure(
                error.code,
                provider="proton",
                exit_code=error.exit_code,
                diagnostic=error.diagnostic,
            ) from None
