"""One explicitly routed provider session for a bounded gallery-image batch."""

from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import final

import anyio
import httpx

from carl.core.models import JsonValue
from carl.io.decodo import (
    DecodoCredentialSource,
    DecodoProxySettings,
    DecodoSessionManager,
    ManagedDecodoSession,
)
from carl.io.httpx import AcquisitionFailure, ClientHttpxAcquirer, HttpAcquirer
from carl.io.proton import (
    ManagedProtonTransportFailure,
    ProtonSession,
    ProtonSessionManager,
    ProtonWireproxySettings,
)


@dataclass(slots=True)
class FacebookImageHttpSession:
    identifier: str
    acquirer: HttpAcquirer
    provider_session: ProtonSession | ManagedDecodoSession

    def active_observation(self) -> dict[str, JsonValue]:
        return {
            "network_session_identifier": self.identifier,
            "provider_session": self.provider_session.active_observation(),
        }

    def completed_observation(self) -> dict[str, JsonValue]:
        return {
            "network_session_identifier": self.identifier,
            "provider_session": (
                self.provider_session.completed_observation().as_json()
                if isinstance(self.provider_session, ManagedDecodoSession)
                else self.provider_session.completed_observation()
            ),
        }


class FacebookImageSessionFailure(Exception):
    def __init__(
        self,
        code: str,
        diagnostic: dict[str, JsonValue],
        exit_code: int | None = None,
    ):
        super().__init__(code)
        self.code = code
        self.diagnostic = diagnostic
        self.exit_code = exit_code


class ProtonFacebookImageSessionFactory:
    def __init__(self, *, manager: ProtonSessionManager, settings: ProtonWireproxySettings):
        self.manager = manager
        self.settings = settings

    @asynccontextmanager
    async def __call__(self, identifier: str) -> AsyncIterator[FacebookImageHttpSession]:
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
                    yield FacebookImageHttpSession(
                        identifier=identifier,
                        acquirer=ClientHttpxAcquirer(
                            client=client,
                            expected_routing=self.settings.route.network_path,
                            routing_observation={
                                "network_session_identifier": identifier,
                                "provider_session": provider_session.active_observation(),
                            },
                            authentication="anonymous",
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
            raise FacebookImageSessionFailure(
                error.code,
                error.diagnostic,
                exit_code=error.exit_code,
            ) from None


@final
class DecodoFacebookImageSessionFactory:
    """Use the managed Decodo client without opening a direct connection."""

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
    async def __call__(self, identifier: str) -> AsyncGenerator[FacebookImageHttpSession]:
        provider_session: ManagedDecodoSession | None = None
        try:
            async with self.manager.open(
                settings=self.settings,
                credential_source=self.credential_source,
                new_identifier=lambda: identifier,
                target_authentication="anonymous",
            ) as provider_session:
                yield FacebookImageHttpSession(
                    identifier=identifier,
                    acquirer=provider_session,
                    provider_session=provider_session,
                )
        except AcquisitionFailure as error:
            if provider_session is not None:
                error.result["routing"] = {
                    "configured": list(self.settings.route.network_path),
                    "observed": {
                        "network_session_identifier": identifier,
                        "provider_session": provider_session.completed_observation().as_json(),
                    },
                }
            raise
