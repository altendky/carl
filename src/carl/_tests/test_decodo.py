from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager

import anyio
import httpx
import pytest
from pydantic import SecretStr

from carl.core.http import RequestPlan
from carl.core.models import (
    ConfigurationDocumentIdentity,
    ConfigurationSchemaIdentity,
    Domain,
    Namespace,
)
from carl.core.routing import DecodoProduct, DecodoRouteIdentity, RemoteProxyEndpoint
from carl.io.decodo import (
    DecodoCredentials,
    DecodoProxySettings,
    DecodoSessionManager,
    ManagedDecodoHttpAcquirer,
    StaticDecodoCredentialSource,
)
from carl.io.httpx import AcquisitionFailure, RouteConfigurationFailure


class _Stream(httpx.AsyncByteStream):
    def __init__(self, content: bytes):
        self.content = content

    async def __aiter__(self):
        yield self.content


def _settings() -> DecodoProxySettings:
    return DecodoProxySettings(
        route=DecodoRouteIdentity(
            network_path=("decodo", "personal", "carl"),
            account_identifier="personal",
            proxy_username="example",
            credential_reference=("carl", "configuration", "decodo", "carl"),
            product=DecodoProduct.RESIDENTIAL_PROXY,
            endpoint=RemoteProxyEndpoint(host="gate.decodo.com", port=7000),
            country_code="us",
        ),
        configuration=ConfigurationDocumentIdentity(
            schema=ConfigurationSchemaIdentity(
                namespace=Namespace.CARL,
                domain=Domain.CONFIGURATION,
                version=1,
            ),
            document_sha256="d" * 64,
        ),
        session_duration_minutes=15,
    )


def _identifiers() -> Callable[[], str]:
    values = iter(f"identifier-{index}" for index in range(20))
    return lambda: next(values)


@pytest.mark.anyio
async def test_one_acquisition_uses_fresh_sticky_session_and_redacts_secrets() -> None:
    seen_proxy_auth: list[tuple[str | bytes, str | bytes] | None] = []

    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        assert str(proxy.url) == "http://gate.decodo.com:7000"
        seen_proxy_auth.append(proxy.auth)
        assert verify is True
        return httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    200,
                    headers={"Set-Cookie": "guest=secret-cookie; Path=/"},
                    stream=_Stream(b"listing"),
                )
            )
        )

    identifiers = _identifiers()
    acquirer = ManagedDecodoHttpAcquirer(
        manager=DecodoSessionManager(
            client_factory=client_factory,
            provider_session_factory=iter(("ProviderSessionOne", "ProviderSessionTwo")).__next__,
        ),
        settings=_settings(),
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("proxy-password-secret"))
        ),
    )
    plan = RequestPlan(
        url="https://www.facebook.com/marketplace/item/123/",
        routing=("decodo", "personal", "carl"),
    )

    first = await acquirer.acquire(plan, identifiers)
    second = await acquirer.acquire(plan, identifiers)

    assert seen_proxy_auth == [
        (
            "user-example-country-us-session-ProviderSessionOne-sessionduration-15",
            "proxy-password-secret",
        ),
        (
            "user-example-country-us-session-ProviderSessionTwo-sessionduration-15",
            "proxy-password-secret",
        ),
    ]
    retained = repr((first.record, second.record))
    assert "proxy-password-secret" not in retained
    assert "secret-cookie" not in retained
    assert "ProviderSessionOne" not in retained
    assert "ProviderSessionTwo" not in retained
    assert first.record["routing"]["observed"]["sticky_peer_requested"] is True
    assert first.record["routing"]["observed"]["configured_session_duration_minutes"] == 15


@pytest.mark.anyio
async def test_target_407_remains_target_evidence_and_redacts_proxy_headers() -> None:
    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    407,
                    headers={"Proxy-Authenticate": 'Basic realm="credential details"'},
                    stream=_Stream(b""),
                )
            )
        )

    identifiers = _identifiers()
    acquirer = ManagedDecodoHttpAcquirer(
        manager=DecodoSessionManager(
            client_factory=client_factory,
            provider_session_factory=lambda: "ProviderSession",
        ),
        settings=_settings(),
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("password"))
        ),
    )

    acquisition = await acquirer.acquire(
        RequestPlan(
            url="https://www.facebook.com/marketplace/item/123/",
            routing=("decodo", "personal", "carl"),
        ),
        identifiers,
    )

    assert acquisition.record["hops"][0]["response"]["status_code"] == 407
    assert "network_provider_result" not in acquisition.record
    assert "credential details" not in repr(acquisition.record)


@pytest.mark.anyio
async def test_proxy_transport_failure_is_safe_and_attributed_to_decodo() -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ProxyError("credential-bearing diagnostic", request=request)

    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(transport=httpx.MockTransport(fail))

    identifiers = _identifiers()
    acquirer = ManagedDecodoHttpAcquirer(
        manager=DecodoSessionManager(
            client_factory=client_factory,
            provider_session_factory=lambda: "ProviderSession",
        ),
        settings=_settings(),
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("password"))
        ),
    )

    with pytest.raises(AcquisitionFailure) as raised:
        await acquirer.acquire(
            RequestPlan(
                url="https://www.facebook.com/marketplace/item/123/",
                routing=("decodo", "personal", "carl"),
            ),
            identifiers,
        )

    assert raised.value.result["network_provider_result"]["category"] == (
        "proxy_transport_or_authentication"
    )
    assert "credential-bearing" not in repr(raised.value.result)


@pytest.mark.anyio
async def test_target_502_remains_target_evidence() -> None:
    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(502, stream=_Stream(b"target response"))
            )
        )

    identifiers = _identifiers()
    acquirer = ManagedDecodoHttpAcquirer(
        manager=DecodoSessionManager(
            client_factory=client_factory,
            provider_session_factory=lambda: "ProviderSession",
        ),
        settings=_settings(),
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("password"))
        ),
    )

    acquisition = await acquirer.acquire(
        RequestPlan(
            url="https://www.facebook.com/marketplace/item/123/",
            routing=("decodo", "personal", "carl"),
        ),
        identifiers,
    )

    assert acquisition.record["hops"][0]["response"]["status_code"] == 502
    assert "network_provider_result" not in acquisition.record


@pytest.mark.anyio
async def test_session_rejects_wrong_route_and_expired_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = 100.0
    monkeypatch.setattr("carl.io.decodo.monotonic", lambda: clock)

    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, stream=_Stream(b"")))
        )

    identifiers = _identifiers()
    async with DecodoSessionManager(
        client_factory=client_factory,
        provider_session_factory=lambda: "ProviderSession",
    ).open(
        settings=_settings(),
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("password"))
        ),
        new_identifier=identifiers,
    ) as session:
        with pytest.raises(RouteConfigurationFailure) as wrong_route:
            await session.acquire(
                RequestPlan(url="https://example.com/", routing=("direct",)), identifiers
            )
        assert wrong_route.value.code == "request_route_does_not_match_decodo_route"
        clock += 15 * 60
        with pytest.raises(RouteConfigurationFailure) as expired:
            await session.acquire(
                RequestPlan(
                    url="https://example.com/",
                    routing=("decodo", "personal", "carl"),
                ),
                identifiers,
            )
        assert expired.value.code == "decodo_session_duration_expired"


@pytest.mark.anyio
async def test_session_shields_client_and_credential_cleanup_from_cancellation() -> None:
    class CredentialSource:
        cleaned = False

        @asynccontextmanager
        async def open(self) -> AsyncGenerator[DecodoCredentials]:
            try:
                yield DecodoCredentials(proxy_password=SecretStr("password"))
            finally:
                await anyio.sleep(0)
                self.cleaned = True

    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, stream=_Stream(b"")))
        )

    source = CredentialSource()
    completed_session = None
    with anyio.CancelScope() as scope:
        async with DecodoSessionManager(
            client_factory=client_factory,
            provider_session_factory=lambda: "ProviderSession",
        ).open(
            settings=_settings(),
            credential_source=source,
            new_identifier=_identifiers(),
        ) as opened_session:
            completed_session = opened_session
            scope.cancel()
            await anyio.sleep_forever()

    assert source.cleaned is True
    assert completed_session is not None
    assert completed_session.ended_at_utc is not None
