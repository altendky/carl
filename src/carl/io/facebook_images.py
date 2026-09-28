"""One explicitly routed Proton session for a bounded gallery-image batch."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import anyio
import httpx

from carl.core.models import JsonValue
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
