"""Facebook session adapters retain Decodo routing, cookies, and safe evidence."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from itertools import count
from typing import override

import httpx
import pytest
from pydantic import SecretStr

from carl.core.http import FormField, RequestPlan
from carl.core.models import (
    ConfigurationDocumentIdentity,
    ConfigurationSchemaIdentity,
    Domain,
    Header,
    Namespace,
)
from carl.core.routing import DecodoProduct, DecodoRouteIdentity, RemoteProxyEndpoint
from carl.io.decodo import (
    DecodoCredentials,
    DecodoProxySettings,
    DecodoSessionManager,
    StaticDecodoCredentialSource,
)
from carl.io.facebook_images import DecodoFacebookImageSessionFactory
from carl.io.facebook_search import DecodoFacebookSearchSessionFactory
from carl.io.httpx import AcquisitionFailure, RouteConfigurationFailure

ROUTE = ("decodo", "personal", "datacenter")


class _Stream(httpx.AsyncByteStream):
    @override
    async def __aiter__(self):
        yield b"body"


def _settings() -> DecodoProxySettings:
    return DecodoProxySettings(
        route=DecodoRouteIdentity(
            network_path=ROUTE,
            account_identifier="personal",
            proxy_username="example",
            credential_reference=("carl", "configuration", "decodo", "datacenter"),
            product=DecodoProduct.DATACENTER_PROXY,
            endpoint=RemoteProxyEndpoint(host="dc.decodo.com", port=10001),
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
    )


def _credentials() -> StaticDecodoCredentialSource:
    return StaticDecodoCredentialSource(
        DecodoCredentials(proxy_password=SecretStr("proxy-password-secret"))
    )


@pytest.mark.anyio
async def test_search_bootstrap_and_forms_share_proxy_client_and_redact_secrets() -> None:
    requests: list[httpx.Request] = []
    clients: list[httpx.AsyncClient] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"Set-Cookie": "guest=cookie-secret; Path=/"},
            stream=_Stream(),
        )

    def client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        assert str(proxy.url) == "http://dc.decodo.com:10001"
        assert proxy.auth == ("user-example-country-us", "proxy-password-secret")
        assert verify is True
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        clients.append(client)
        return client

    factory = DecodoFacebookSearchSessionFactory(
        manager=DecodoSessionManager(client_factory=client_factory),
        settings=_settings(),
        credential_source=_credentials(),
    )
    identifiers = count()
    async with factory("search-session") as session:
        bootstrap = await session.acquirer.acquire(
            RequestPlan(url="https://www.facebook.com/marketplace/", routing=ROUTE),
            lambda: f"body-{next(identifiers)}",
        )
        form = await session.acquirer.acquire_form(
            RequestPlan(
                url="https://www.facebook.com/api/graphql/",
                method="POST",
                follow_redirects=False,
                headers=(Header(name=b"x-fb-lsd", value=b"lsd-secret"),),
                routing=ROUTE,
            ),
            (FormField(name="lsd", value=SecretStr("form-secret"), protected=True),),
            lambda: f"body-{next(identifiers)}",
        )
        assert session.active_observation()["network_session_identifier"] == "search-session"
        assert form.record["authentication"]["target"] == "anonymous_guest_session"

    assert len(clients) == 1
    assert clients[0].is_closed
    assert not clients[0].cookies
    assert requests[1].headers["cookie"] == "guest=cookie-secret"
    assert requests[1].headers["x-fb-lsd"] == "lsd-secret"
    assert requests[1].content == b"lsd=form-secret"
    retained = repr((bootstrap.record, form.record, session.completed_observation()))
    for secret in ("proxy-password-secret", "cookie-secret", "lsd-secret", "form-secret"):
        assert secret not in retained
    observation = session.completed_observation()["provider_session"]
    assert isinstance(observation, dict)
    assert observation["route"]["provider"] == "decodo"
    assert observation["requests_started"] == 2
    assert observation["requests_completed"] == 2
    assert observation["shutdown_state"] == "closed"
    assert not observation["failed"]


@pytest.mark.anyio
@pytest.mark.parametrize("search", (False, True))
async def test_proxy_failure_has_completed_safe_routing_and_client_cleanup(search: bool) -> None:
    clients: list[httpx.AsyncClient] = []

    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ProxyError("proxy-password-secret", request=request)

    def client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        del proxy, verify
        client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
        clients.append(client)
        return client

    manager = DecodoSessionManager(client_factory=client_factory)
    if search:
        factory = DecodoFacebookSearchSessionFactory(
            manager=manager, settings=_settings(), credential_source=_credentials()
        )
    else:
        factory = DecodoFacebookImageSessionFactory(
            manager=manager, settings=_settings(), credential_source=_credentials()
        )
    with pytest.raises(AcquisitionFailure) as raised:
        async with factory("failed-session") as session:
            _ = await session.acquirer.acquire(
                RequestPlan(url="https://www.facebook.com/marketplace/", routing=ROUTE),
                lambda: "body",
            )
    assert clients[0].is_closed
    assert not clients[0].cookies
    assert "proxy-password-secret" not in repr(raised.value.result)
    routing = raised.value.result["routing"]
    assert routing["configured"] == list(ROUTE)
    observation = routing["observed"]["provider_session"]
    assert observation["route"]["provider"] == "decodo"
    assert observation["shutdown_state"] == "closed"
    assert observation["failed"] is True
    assert observation["requests_started"] == observation["requests_completed"] == 1
    assert raised.value.result["network_provider_result"]["provider"] == "decodo"


@pytest.mark.anyio
async def test_image_session_uses_managed_proxy_and_completed_observation() -> None:
    clients: list[httpx.AsyncClient] = []

    def client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        assert str(proxy.url) == "http://dc.decodo.com:10001"
        assert verify is True
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, stream=_Stream()))
        )
        clients.append(client)
        return client

    factory = DecodoFacebookImageSessionFactory(
        manager=DecodoSessionManager(client_factory=client_factory),
        settings=_settings(),
        credential_source=_credentials(),
    )
    async with factory("image-session") as session:
        acquisition = await session.acquirer.acquire(
            RequestPlan(url="https://scontent.example.net/image.jpg", routing=ROUTE),
            lambda: "body",
        )
    assert clients[0].is_closed
    assert acquisition.record["routing"]["configured"] == list(ROUTE)
    observation = session.completed_observation()["provider_session"]
    assert isinstance(observation, dict)
    assert observation["requests_completed"] == 1
    assert observation["shutdown_state"] == "closed"


@pytest.mark.anyio
async def test_search_form_failure_retains_bootstrap_counts_and_closed_routing() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            raise httpx.ProxyError("proxy-password-secret", request=request)
        return httpx.Response(200, stream=_Stream())

    def client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(transport=httpx.MockTransport(respond))

    factory = DecodoFacebookSearchSessionFactory(
        manager=DecodoSessionManager(client_factory=client_factory),
        settings=_settings(),
        credential_source=_credentials(),
    )
    with pytest.raises(AcquisitionFailure) as raised:
        async with factory("failed-form-session") as session:
            _ = await session.acquirer.acquire(
                RequestPlan(url="https://www.facebook.com/marketplace/", routing=ROUTE),
                lambda: "body",
            )
            _ = await session.acquirer.acquire_form(
                RequestPlan(
                    url="https://www.facebook.com/api/graphql/",
                    method="POST",
                    follow_redirects=False,
                    routing=ROUTE,
                ),
                (FormField(name="lsd", value=SecretStr("form-secret"), protected=True),),
                lambda: "body",
            )
    assert "form-secret" not in repr(raised.value.result)
    assert "proxy-password-secret" not in repr(raised.value.result)
    observation = raised.value.result["routing"]["observed"]["provider_session"]
    assert observation["requests_started"] == observation["requests_completed"] == 2
    assert observation["failed"] is True
    assert observation["shutdown_state"] == "closed"


@pytest.mark.anyio
async def test_search_rejects_rotating_datacenter_before_credentials_or_client() -> None:
    settings = _settings()
    settings = settings.model_copy(
        update={
            "route": settings.route.model_copy(
                update={"endpoint": RemoteProxyEndpoint(host="dc.decodo.com", port=10000)}
            )
        }
    )

    class UnusedCredentials:
        @asynccontextmanager
        async def open(self) -> AsyncGenerator[DecodoCredentials]:
            raise AssertionError("Rotating search must not open credentials")
            yield DecodoCredentials(proxy_password=SecretStr("unused"))

    def unused_client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        del proxy, verify
        raise AssertionError("Rotating search must not create a client")

    factory = DecodoFacebookSearchSessionFactory(
        manager=DecodoSessionManager(client_factory=unused_client_factory),
        settings=settings,
        credential_source=UnusedCredentials(),
    )
    with pytest.raises(
        RouteConfigurationFailure, match="facebook_search_requires_static_decodo_datacenter_port"
    ):
        async with factory("rotating-search"):
            raise AssertionError("Rotating search must not yield a session")
