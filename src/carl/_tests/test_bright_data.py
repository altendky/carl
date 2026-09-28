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
from carl.core.routing import (
    BrightDataProduct,
    BrightDataRouteIdentity,
    RemoteProxyEndpoint,
)
from carl.io.bright_data import (
    BrightDataCredentials,
    BrightDataProxySettings,
    BrightDataSessionManager,
    ManagedBrightDataHttpAcquirer,
    StaticBrightDataCredentialSource,
)
from carl.io.httpx import AcquisitionFailure, RouteConfigurationFailure


class _Stream(httpx.AsyncByteStream):
    def __init__(self, content: bytes):
        self.content = content

    async def __aiter__(self):
        yield self.content


def _route() -> BrightDataRouteIdentity:
    return BrightDataRouteIdentity(
        network_path=("bright_data", "personal", "marketplace_search"),
        account_identifier="personal",
        proxy_username="brd-customer-example-zone-marketplace_search",
        credential_reference=("one_password", "Test Vault", "Test Item", "password"),
        zone_identifier="marketplace_search",
        product=BrightDataProduct.RESIDENTIAL_PROXY,
        endpoint=RemoteProxyEndpoint(host="brd.superproxy.io", port=33335),
    )


def _settings(
    route: BrightDataRouteIdentity, *, max_idle_seconds: float = 240.0
) -> BrightDataProxySettings:
    return BrightDataProxySettings(
        route=route,
        configuration=ConfigurationDocumentIdentity(
            schema=ConfigurationSchemaIdentity(
                namespace=Namespace.CARL, domain=Domain.CONFIGURATION, version=1
            ),
            document_sha256="c" * 64,
        ),
        max_idle_seconds=max_idle_seconds,
    )


def _identifiers() -> Callable[[], str]:
    values = iter(f"identifier-{index}" for index in range(20))
    return lambda: next(values)


@pytest.mark.anyio
async def test_session_preserves_cookies_but_redacts_session_material() -> None:
    requests = 0
    secret_cookie = "guest-session-secret"

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if requests == 1:
            assert "cookie" not in request.headers
            return httpx.Response(
                200,
                headers={"Set-Cookie": f"guest={secret_cookie}; Path=/; Secure"},
                stream=_Stream(b"bootstrap"),
            )
        assert request.headers["cookie"] == f"guest={secret_cookie}"
        return httpx.Response(200, stream=_Stream(b"pagination"))

    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        assert isinstance(proxy, httpx.Proxy)
        assert str(proxy.url) == "http://brd.superproxy.io:33335"
        assert proxy.auth == (
            "brd-customer-example-zone-marketplace_search-session-ProviderSession123-const",
            "proxy-password-secret",
        )
        assert verify is True
        return httpx.AsyncClient(
            transport=httpx.MockTransport(handle),
            trust_env=False,
            follow_redirects=False,
        )

    route = _route()
    settings = _settings(route)
    credentials = BrightDataCredentials(
        proxy_password=SecretStr("proxy-password-secret"),
    )
    source = StaticBrightDataCredentialSource(credentials)
    manager = BrightDataSessionManager(
        client_factory=client_factory,
        provider_session_factory=lambda: "ProviderSession123",
    )
    identifiers = _identifiers()

    async with manager.open(
        settings=settings,
        credential_source=source,
        new_identifier=identifiers,
    ) as session:
        first = await session.acquire(
            RequestPlan(url="https://www.facebook.com/bootstrap", routing=route.network_path),
            identifiers,
        )
        second = await session.acquire(
            RequestPlan(url="https://www.facebook.com/page-two", routing=route.network_path),
            identifiers,
        )

    observation = session.completed_observation()
    retained = repr((first.record, second.record, observation.model_dump(mode="json")))
    assert secret_cookie not in retained
    assert "proxy-password-secret" not in retained
    assert "ProviderSession123" not in retained
    assert credentials.model_dump(mode="json") == {}
    assert settings.safe_configuration()["route"]["zone_identifier"] == "marketplace_search"
    assert observation.requests_started == 2
    assert observation.requests_completed == 2
    assert observation.configuration.document_sha256 == "c" * 64
    assert first.record["hops"][0]["response"]["headers"][0]["value"] == {
        "state": "redacted",
        "reason": "protected_session_or_credential_material",
        "bytes": len(f"guest={secret_cookie}; Path=/; Secure"),
    }
    assert second.record["hops"][0]["request"]["headers"][-1]["value"]["state"] == ("redacted")


@pytest.mark.anyio
async def test_provider_error_is_classified_without_retaining_message() -> None:
    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    407,
                    headers={
                        "x-brd-err-code": "bad_zone",
                        "x-brd-err-msg": "provider diagnostic with identifiers",
                        "Proxy-Status": 'error="more provider details"',
                    },
                    stream=_Stream(b""),
                )
            )
        )

    route = _route()
    manager = BrightDataSessionManager(
        client_factory=client_factory,
        provider_session_factory=lambda: "ProviderSession123",
    )
    identifiers = _identifiers()
    async with manager.open(
        settings=_settings(route),
        credential_source=StaticBrightDataCredentialSource(
            BrightDataCredentials(
                proxy_password=SecretStr("password"),
            )
        ),
        new_identifier=identifiers,
    ) as session:
        with pytest.raises(AcquisitionFailure) as raised:
            await session.acquire(
                RequestPlan(url="https://www.facebook.com/", routing=route.network_path),
                identifiers,
            )
        with pytest.raises(RouteConfigurationFailure) as poisoned:
            await session.acquire(
                RequestPlan(url="https://www.facebook.com/again", routing=route.network_path),
                identifiers,
            )

    assert poisoned.value.code == "bright_data_session_failed"
    assert raised.value.result["network_provider_result"] == {
        "provider": "bright_data",
        "category": "proxy_authentication_or_configuration",
        "http_status": 407,
        "error_code": "bad_zone",
    }
    assert raised.value.result["stopping_condition"] == "network_provider_failure"
    assert raised.value.bodies
    assert "provider diagnostic" not in repr(raised.value.result)
    assert "more provider details" not in repr(raised.value.result)


@pytest.mark.anyio
async def test_target_error_is_not_misclassified_as_provider_failure() -> None:
    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(502, stream=_Stream(b"facebook upstream error"))
            )
        )

    route = _route()
    identifiers = _identifiers()
    async with BrightDataSessionManager(
        client_factory=client_factory,
        provider_session_factory=lambda: "ProviderSession123",
    ).open(
        settings=_settings(route),
        credential_source=StaticBrightDataCredentialSource(
            BrightDataCredentials(proxy_password=SecretStr("password"))
        ),
        new_identifier=identifiers,
    ) as session:
        acquisition = await session.acquire(
            RequestPlan(url="https://www.facebook.com/", routing=route.network_path),
            identifiers,
        )

    assert acquisition.record["hops"][0]["response"]["status_code"] == 502
    assert "network_provider_result" not in acquisition.record


@pytest.mark.anyio
async def test_proxy_transport_failure_is_safe_and_poisons_session() -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ProxyError("credential-bearing diagnostic", request=request)

    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(transport=httpx.MockTransport(fail))

    route = _route()
    identifiers = _identifiers()
    async with BrightDataSessionManager(
        client_factory=client_factory,
        provider_session_factory=lambda: "ProviderSession123",
    ).open(
        settings=_settings(route),
        credential_source=StaticBrightDataCredentialSource(
            BrightDataCredentials(proxy_password=SecretStr("password"))
        ),
        new_identifier=identifiers,
    ) as session:
        with pytest.raises(AcquisitionFailure) as raised:
            await session.acquire(
                RequestPlan(url="https://www.facebook.com/", routing=route.network_path),
                identifiers,
            )

    assert raised.value.result["network_provider_result"]["category"] == (
        "proxy_transport_or_authentication"
    )
    assert "credential-bearing" not in repr(raised.value.result)


@pytest.mark.anyio
async def test_session_rejects_wrong_route_and_expired_idle_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = 100.0
    monkeypatch.setattr("carl.io.bright_data.monotonic", lambda: clock)

    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, stream=_Stream(b"")))
        )

    route = _route()
    manager = BrightDataSessionManager(
        client_factory=client_factory,
        provider_session_factory=lambda: "ProviderSession123",
    )
    identifiers = _identifiers()
    async with manager.open(
        settings=_settings(route, max_idle_seconds=10),
        credential_source=StaticBrightDataCredentialSource(
            BrightDataCredentials(
                proxy_password=SecretStr("password"),
            )
        ),
        new_identifier=identifiers,
    ) as session:
        with pytest.raises(RouteConfigurationFailure) as wrong_route:
            await session.acquire(
                RequestPlan(url="https://example.com/", routing=("direct",)),
                identifiers,
            )
        assert wrong_route.value.code == "request_route_does_not_match_bright_data_route"
        clock = 111.0
        with pytest.raises(RouteConfigurationFailure) as expired:
            await session.acquire(
                RequestPlan(url="https://example.com/", routing=route.network_path),
                identifiers,
            )
        assert expired.value.code == "bright_data_session_idle_expired"


@pytest.mark.anyio
async def test_managed_single_request_records_closed_session() -> None:
    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, stream=_Stream(b"complete"))
            )
        )

    route = _route()
    acquirer = ManagedBrightDataHttpAcquirer(
        manager=BrightDataSessionManager(
            client_factory=client_factory,
            provider_session_factory=lambda: "ProviderSession123",
        ),
        settings=_settings(route),
        credential_source=StaticBrightDataCredentialSource(
            BrightDataCredentials(
                proxy_password=SecretStr("password"),
            )
        ),
    )

    acquisition = await acquirer.acquire(
        RequestPlan(url="https://example.com/", routing=route.network_path),
        _identifiers(),
    )

    observed = acquisition.record["routing"]["observed"]
    assert observed["shutdown_state"] == "closed"
    assert observed["requests_started"] == 1
    assert observed["requests_completed"] == 1


@pytest.mark.anyio
async def test_session_shields_client_and_credential_cleanup_from_cancellation() -> None:
    class CredentialSource:
        cleaned = False

        @asynccontextmanager
        async def open(self) -> AsyncGenerator[BrightDataCredentials]:
            try:
                yield BrightDataCredentials(proxy_password=SecretStr("password"))
            finally:
                await anyio.sleep(0)
                self.cleaned = True

    def client_factory(*, proxy: httpx.Proxy, verify: object) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, stream=_Stream(b"")))
        )

    source = CredentialSource()
    route = _route()
    manager = BrightDataSessionManager(
        client_factory=client_factory,
        provider_session_factory=lambda: "ProviderSession123",
    )
    completed_session = None
    with anyio.CancelScope() as scope:
        async with manager.open(
            settings=_settings(route),
            credential_source=source,
            new_identifier=_identifiers(),
        ) as opened_session:
            completed_session = opened_session
            scope.cancel()
            await anyio.sleep_forever()

    assert source.cleaned is True
    assert completed_session is not None
    assert completed_session.ended_at_utc is not None
