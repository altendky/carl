from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import asynccontextmanager
from typing import cast

import anyio
import httpx
import pytest
import wreq
from pydantic import SecretStr, ValidationError

from carl._tests.test_wreq import _Factory, _response
from carl._tests.test_wreq_reconnect import _broken_pipe, _CookieClient, _ReplacingFactory
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
    ManagedDecodoHttpAcquirer,
    ManagedDecodoWreqAcquirer,
    StaticDecodoCredentialSource,
    _provider_result,
    _sticky_proxy_credentials,
    select_decodo_session_settings,
)
from carl.io.httpx import AcquisitionFailure, RouteConfigurationFailure
from carl.io.wreq import WreqClientFactory


def test_proxy_quota_diagnostic_is_classified_without_retaining_secret_text() -> None:
    secret_error = httpx.ProxyError("407 Traffic limit reached; password=private")
    failure = AcquisitionFailure(
        "Transport failed",
        result={"stopping_condition": "transport_failure", "exception_type": "ProxyError"},
    )
    failure.__cause__ = secret_error
    result = _provider_result(failure.result, failure)
    assert result == {
        "provider": "decodo",
        "category": "proxy_quota_exhausted",
        "http_status": None,
        "error_code": None,
    }
    assert "private" not in repr(result)


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


def _pool_settings(product: DecodoProduct, *, range_pool: bool = False) -> DecodoProxySettings:
    settings = _settings()
    if range_pool:
        return settings.model_copy(
            update={
                "route": settings.route.model_copy(
                    update={
                        "product": product,
                        "endpoint": settings.route.endpoint.model_copy(update={"port": 11000}),
                    }
                ),
                "endpoint_port_end": 19000,
            }
        )
    return settings.model_copy(
        update={
            "route": settings.route.model_copy(update={"product": product}),
            "endpoint_ports": (10001, 10002, 10100),
        }
    )


def test_preselected_endpoint_is_frozen_and_original_pool_is_unchanged() -> None:
    settings = _pool_settings(DecodoProduct.RESIDENTIAL_PROXY)
    selected = select_decodo_session_settings(settings, lambda ports: ports[-1])
    assert selected.route.endpoint.port == 10100
    assert selected.endpoint_ports == ()
    assert settings.endpoint_ports == (10001, 10002, 10100)
    assert settings.route.endpoint.port == 7000

    def unexpected_choice(_ports: Sequence[int]) -> int:
        raise AssertionError("A preselected session must not rotate its endpoint")

    assert select_decodo_session_settings(selected, unexpected_choice) is selected
    with pytest.raises(RouteConfigurationFailure, match="invalid_decodo_selected_port"):
        _ = select_decodo_session_settings(settings, lambda _ports: 10004)


@pytest.mark.parametrize("product", tuple(DecodoProduct))
@pytest.mark.parametrize("take_last", (False, True))
def test_range_endpoints_are_inclusive_compact_and_frozen(
    product: DecodoProduct, take_last: bool
) -> None:
    settings = _pool_settings(product, range_pool=True)

    def choose(ports: Sequence[int]) -> int:
        assert isinstance(ports, range)
        assert len(ports) == 8001
        assert ports[0] == 11000
        assert ports[-1] == 19000
        return ports[-1] if take_last else ports[0]

    selected = select_decodo_session_settings(settings, choose)
    assert selected.route.endpoint.port == (19000 if take_last else 11000)
    assert selected.endpoint_ports == ()
    assert selected.endpoint_port_end is None
    assert select_decodo_session_settings(selected, choose) is selected
    assert settings.route.endpoint.port == 11000
    assert settings.endpoint_port_end == 19000


@pytest.mark.parametrize(
    ("start", "end", "ports"),
    (
        (11000, 0, ()),
        (11000, 65536, ()),
        (11000, 10999, ()),
        (9999, 19000, ()),
        (11000, 63001, ()),
        (11000, 19000, (11000, 19000)),
    ),
)
def test_settings_reject_invalid_or_ambiguous_ranges(
    start: int, end: int, ports: tuple[int, ...]
) -> None:
    settings = _pool_settings(DecodoProduct.DATACENTER_PROXY)
    value = settings.model_dump(mode="python")
    value["route"] = settings.route.model_copy(
        update={"endpoint": settings.route.endpoint.model_copy(update={"port": start})}
    )
    value["endpoint_ports"] = ports
    value["endpoint_port_end"] = end
    with pytest.raises(ValidationError):
        _ = DecodoProxySettings.model_validate(value)


@pytest.mark.anyio
@pytest.mark.parametrize("product", tuple(DecodoProduct))
@pytest.mark.parametrize("range_pool", (False, True))
async def test_httpx_pool_selects_once_per_session_and_records_actual_endpoint(
    product: DecodoProduct, range_pool: bool
) -> None:
    settings = _pool_settings(product, range_pool=range_pool)
    expected_ports = (11000, 19000) if range_pool else (10002, 10100)
    selected_ports = iter(expected_ports)
    choices: list[Sequence[int]] = []
    proxies: list[str] = []

    def choose(ports: Sequence[int]) -> int:
        choices.append(ports)
        return next(selected_ports)

    def client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        assert verify
        proxies.append(str(proxy.url))
        return httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, stream=_Stream(b"page"))
            )
        )

    manager = DecodoSessionManager(client_factory=client_factory, port_choice=choose)
    identifiers = _identifiers()
    source = StaticDecodoCredentialSource(DecodoCredentials(proxy_password=SecretStr("secret")))
    plan = RequestPlan(url="https://example.com/", routing=settings.route.network_path)
    for port in expected_ports:
        async with manager.open(
            settings=settings, credential_source=source, new_identifier=identifiers
        ) as session:
            for _ in range(2):
                acquisition = await session.acquire(plan, identifiers)
                assert (
                    acquisition.record["routing"]["observed"]["route"]["endpoint"]["port"] == port
                )
        assert session.completed_observation().route.endpoint.port == port
    assert choices == [range(11000, 19001) if range_pool else (10001, 10002, 10100)] * 2
    assert proxies == [f"http://gate.decodo.com:{port}" for port in expected_ports]


@pytest.mark.anyio
@pytest.mark.parametrize("product", tuple(DecodoProduct))
@pytest.mark.parametrize("shared", (False, True))
@pytest.mark.parametrize("range_pool", (False, True))
async def test_wreq_pool_selects_once_and_records_actual_endpoint(
    product: DecodoProduct, shared: bool, range_pool: bool
) -> None:
    settings = _pool_settings(product, range_pool=range_pool)
    expected_port = 19000 if range_pool else 10100
    choices: list[Sequence[int]] = []

    def choose(ports: Sequence[int]) -> int:
        choices.append(ports)
        return ports[-1]

    factory = _Factory([_response(), _response()])
    acquirer = ManagedDecodoWreqAcquirer(
        settings=settings,
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("secret"))
        ),
        client_factory=cast(WreqClientFactory, cast(object, factory)),
        port_choice=choose,
    )
    identifiers = _identifiers()
    plan = RequestPlan(url="https://www.ebay.com/", routing=settings.route.network_path)
    if shared:
        async with acquirer.session(identifiers) as session:
            for _ in range(2):
                acquisition = await session.acquire(plan, identifiers)
                assert (
                    acquisition.record["routing"]["observed"]["route"]["endpoint"]["port"]
                    == expected_port
                )
        assert session.observation()["route"]["endpoint"]["port"] == expected_port
    else:
        acquisition = await acquirer.acquire(plan, identifiers)
        assert (
            acquisition.record["routing"]["observed"]["route"]["endpoint"]["port"] == expected_port
        )
    assert choices == [range(11000, 19001) if range_pool else (10001, 10002, 10100)]
    assert f"gate.decodo.com:{expected_port}" in str(factory.proxy)
    assert settings.endpoint_ports == (() if range_pool else (10001, 10002, 10100))


@pytest.mark.anyio
@pytest.mark.parametrize("product", tuple(DecodoProduct))
async def test_range_session_reconnect_preserves_selected_port_provider_and_cookies(
    product: DecodoProduct,
) -> None:
    events: list[str] = []
    jar = wreq.Jar()
    first = _CookieClient([_response(), _broken_pipe()], jar=jar, events=events, label="first")
    second = _CookieClient(
        [_response(), _response()], jar=wreq.Jar(), events=events, label="second"
    )
    factory = _ReplacingFactory([first, second], events)
    choices: list[Sequence[int]] = []
    provider_sessions: list[str] = []

    def choose(ports: Sequence[int]) -> int:
        choices.append(ports)
        return ports[-1]

    def provider_session() -> str:
        provider_sessions.append("StableProviderSession")
        return provider_sessions[-1]

    settings = _pool_settings(product, range_pool=True)
    acquirer = ManagedDecodoWreqAcquirer(
        settings=settings,
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("proxy-secret"))
        ),
        client_factory=cast(WreqClientFactory, cast(object, factory)),
        port_choice=choose,
        provider_session_factory=provider_session,
    )
    plan = RequestPlan(
        url="https://www.ebay.com/",
        routing=settings.route.network_path,
        reconnect_on_broken_pipe=True,
    )
    identifiers = _identifiers()
    async with acquirer.session(identifiers) as session:
        acquisitions = [await session.acquire(plan, identifiers) for _ in range(3)]
    assert choices == [range(11000, 19001)]
    assert provider_sessions == ["StableProviderSession"]
    assert len(factory.proxies) == 2 and factory.proxies[0] is factory.proxies[1]
    assert "gate.decodo.com:19000" in str(factory.proxies[0])
    if product is not DecodoProduct.DATACENTER_PROXY:
        assert "-session-StableProviderSession-" in str(factory.proxies[0])
    assert events == ["open:1", "close:first", "open:2", "close:second"]
    assert all(
        call[2]["cookie_provider"] is jar for call in (*first.inner.calls, *second.inner.calls)
    )
    assert acquisitions[1].record["connection_recovery"]["recovered"] is True
    assert all(
        acquisition.record["routing"]["observed"]["route"]["endpoint"]["port"] == 19000
        for acquisition in acquisitions
    )
    assert session.observation()["route"]["endpoint"]["port"] == 19000


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
@pytest.mark.parametrize(
    ("diagnostic", "category"),
    [
        ("credential-bearing diagnostic", "proxy_transport_or_authentication"),
        ("407 Traffic limit reached; secret=private", "proxy_quota_exhausted"),
    ],
)
async def test_proxy_transport_failure_is_safe_and_attributed_to_decodo(
    diagnostic: str, category: str
) -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ProxyError(diagnostic, request=request)

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

    assert raised.value.result["network_provider_result"]["category"] == category
    assert "credential-bearing" not in repr(raised.value.result)
    assert "private" not in repr(raised.value.result)


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


def _datacenter_settings(port: int) -> DecodoProxySettings:
    settings = _settings()
    return settings.model_copy(
        update={
            "route": settings.route.model_copy(
                update={
                    "product": DecodoProduct.DATACENTER_PROXY,
                    "network_path": ("decodo", "personal", "datacenter"),
                    "endpoint": RemoteProxyEndpoint(host="dc.decodo.com", port=port),
                }
            )
        }
    )


@pytest.mark.anyio
@pytest.mark.parametrize("port", (10000, 10001, 63000))
async def test_datacenter_httpx_authentication_and_port_based_session_provenance(port: int) -> None:
    settings = _datacenter_settings(port)
    seen_auth: list[tuple[str | bytes, str | bytes] | None] = []

    def client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        assert str(proxy.url) == f"http://dc.decodo.com:{port}"
        assert verify is True
        seen_auth.append(proxy.auth)
        return httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, stream=_Stream(b"image"))
            )
        )

    acquirer = ManagedDecodoHttpAcquirer(
        manager=DecodoSessionManager(
            client_factory=client_factory,
            provider_session_factory=iter(("IgnoredSessionOne", "IgnoredSessionTwo")).__next__,
        ),
        settings=settings,
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("datacenter-secret"))
        ),
    )
    identifiers = _identifiers()
    plan = RequestPlan(url="https://example.com/image.jpg", routing=settings.route.network_path)
    acquisitions = [await acquirer.acquire(plan, identifiers) for _ in range(2)]

    assert seen_auth == [("user-example-country-us", "datacenter-secret")] * 2
    for acquisition in acquisitions:
        observed = acquisition.record["routing"]["observed"]
        assert observed["route"]["product"] == "datacenter_proxy"
        assert observed["route"]["endpoint"]["port"] == port
        assert observed["proxy_username_shape"] == "base_username_with_country"
        assert observed["sticky_peer_requested"] is (port != 10000)
        assert observed["configured_session_duration_minutes"] is None
        assert observed["requests_started"] == observed["requests_completed"] == 1
    retained = repr([acquisition.record for acquisition in acquisitions])
    assert "datacenter-secret" not in retained
    assert "IgnoredSession" not in retained


@pytest.mark.anyio
async def test_datacenter_httpx_session_does_not_apply_mobile_expiration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = 0.0
    monkeypatch.setattr("carl.io.decodo.monotonic", lambda: clock)
    settings = _datacenter_settings(10001)

    def client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, stream=_Stream(b"image"))
            )
        )

    identifiers = _identifiers()
    async with DecodoSessionManager(client_factory=client_factory).open(
        settings=settings,
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("password"))
        ),
        new_identifier=identifiers,
    ) as session:
        assert session.active_observation()["configured_session_duration_minutes"] is None
        clock = 86400.0
        _ = await session.acquire(
            RequestPlan(url="https://example.com/", routing=settings.route.network_path),
            identifiers,
        )
    assert session.completed_observation().configured_session_duration_minutes is None


@pytest.mark.anyio
@pytest.mark.parametrize("port", (10000, 10001))
@pytest.mark.parametrize("shared", (False, True))
async def test_datacenter_wreq_uses_port_based_auth_and_observations(
    port: int, shared: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = 0.0
    monkeypatch.setattr("carl.io.decodo.monotonic", lambda: clock)
    settings = _datacenter_settings(port)
    factory = _Factory([_response()])
    acquirer = ManagedDecodoWreqAcquirer(
        settings=settings,
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("datacenter-secret"))
        ),
        client_factory=cast(WreqClientFactory, cast(object, factory)),
        provider_session_factory=lambda: "IgnoredProviderSession",
    )
    plan = RequestPlan(url="https://www.ebay.com/", routing=settings.route.network_path)
    identifiers = _identifiers()
    if shared:
        async with acquirer.session(identifiers) as session:
            assert session.observation()["configured_session_duration_minutes"] is None
            clock = 86400.0
            acquisition = await session.acquire(plan, identifiers)
        observed = session.observation()
    else:
        acquisition = await acquirer.acquire(plan, identifiers)
        observed = acquisition.record["routing"]["observed"]

    assert factory.proxy is not None
    proxy = str(factory.proxy)
    assert f"dc.decodo.com:{port}" in proxy
    assert "user-example-country-us" in proxy
    assert "-session-" not in proxy
    assert "sessionduration" not in proxy
    assert factory.client.closed
    assert observed["route"]["endpoint"]["port"] == port
    assert observed["proxy_username_shape"] == "base_username_with_country"
    assert observed["sticky_peer_requested"] is (port != 10000)
    assert observed["configured_session_duration_minutes"] is None
    assert observed["state"] == "closed"
    assert observed["requests_started"] == observed["requests_completed"] == 1
    assert "datacenter-secret" not in repr(acquisition.record)
    assert "IgnoredProviderSession" not in repr(acquisition.record)


@pytest.mark.parametrize("port", (9999, 63001))
def test_datacenter_rejects_ports_outside_supported_pool(port: int) -> None:
    with pytest.raises(RouteConfigurationFailure, match="invalid_decodo_datacenter_port"):
        _ = _sticky_proxy_credentials(
            _datacenter_settings(port).route,
            DecodoCredentials(proxy_password=SecretStr("password")),
            "UnusedProviderSession",
            15,
        )


@pytest.mark.anyio
async def test_managed_decodo_form_shares_cookies_counters_and_redaction() -> None:
    settings = _datacenter_settings(10001)
    seen_requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen_requests.append(request)
        return httpx.Response(
            200,
            headers={"Set-Cookie": "guest=guest-secret; Path=/"},
            stream=_Stream(b"response"),
        )

    def client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(transport=httpx.MockTransport(respond))

    identifiers = _identifiers()
    async with DecodoSessionManager(client_factory=client_factory).open(
        settings=settings,
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("proxy-secret"))
        ),
        new_identifier=identifiers,
        target_authentication="anonymous_guest_session",
    ) as session:
        first = await session.acquire(
            RequestPlan(url="https://www.facebook.com/", routing=settings.route.network_path),
            identifiers,
        )
        second = await session.acquire_form(
            RequestPlan(
                method="POST",
                url="https://www.facebook.com/api/graphql/",
                routing=settings.route.network_path,
                follow_redirects=False,
                headers=(Header(name=b"X-FB-LSD", value=b"lsd-secret"),),
            ),
            (FormField(name="lsd", value=SecretStr("lsd-secret"), protected=True),),
            identifiers,
        )

    assert seen_requests[1].headers["cookie"] == "guest=guest-secret"
    assert seen_requests[1].headers["x-fb-lsd"] == "lsd-secret"
    assert seen_requests[1].content == b"lsd=lsd-secret"
    assert second.record["authentication"]["target"] == "anonymous_guest_session"
    assert second.record["authentication"]["network"]["kind"] == "proxy_basic"
    assert second.record["routing"]["observed"]["requests_started"] == 2
    assert second.record["routing"]["observed"]["requests_completed"] == 2
    completed = session.completed_observation()
    assert completed.requests_started == completed.requests_completed == 2
    retained = repr((first.record, second.record, completed.as_json()))
    for secret in ("lsd-secret", "guest-secret", "proxy-secret"):
        assert secret not in retained


@pytest.mark.anyio
async def test_managed_decodo_form_failure_uses_same_session_failure_guard() -> None:
    settings = _datacenter_settings(10001)

    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ProxyError("proxy-secret", request=request)

    def client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        del proxy, verify
        return httpx.AsyncClient(transport=httpx.MockTransport(fail))

    identifiers = _identifiers()
    async with DecodoSessionManager(client_factory=client_factory).open(
        settings=settings,
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("password"))
        ),
        new_identifier=identifiers,
    ) as session:
        with pytest.raises(AcquisitionFailure) as raised:
            _ = await session.acquire_form(
                RequestPlan(
                    method="POST",
                    url="https://example.com/",
                    routing=settings.route.network_path,
                    follow_redirects=False,
                ),
                (),
                identifiers,
            )
        assert session.failed
        assert session.requests_started == session.requests_completed == 1
        assert "proxy-secret" not in repr(raised.value.result)
        assert raised.value.result["network_provider_result"]["provider"] == "decodo"
        with pytest.raises(RouteConfigurationFailure, match="decodo_session_failed"):
            _ = await session.acquire(
                RequestPlan(url="https://example.com/", routing=settings.route.network_path),
                identifiers,
            )
    assert session.completed_observation().failed


@pytest.mark.anyio
@pytest.mark.parametrize("ending", ("normal", "body_error", "acquisition_failure", "cancel"))
async def test_decodo_client_close_failure_is_safe_and_preserves_body_exception(
    ending: str,
) -> None:
    credentials_cleaned = False
    clients_closed = 0

    class FailingCloseClient(httpx.AsyncClient):
        async def aclose(self) -> None:
            nonlocal clients_closed
            await super().aclose()
            clients_closed += 1
            raise OSError("proxy-secret-close-diagnostic")

    class CredentialSource:
        @asynccontextmanager
        async def open(self) -> AsyncGenerator[DecodoCredentials]:
            nonlocal credentials_cleaned
            try:
                yield DecodoCredentials(proxy_password=SecretStr("proxy-secret"))
            finally:
                await anyio.lowlevel.checkpoint()
                credentials_cleaned = True

    def client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        del proxy, verify
        return FailingCloseClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, stream=_Stream(b"response"))
            )
        )

    original_error = (
        AcquisitionFailure("original acquisition failure", result={"stopping_condition": "test"})
        if ending == "acquisition_failure"
        else RuntimeError("original body failure")
    )
    caught: Exception | None = None
    session = None
    with anyio.CancelScope() as scope:
        try:
            async with DecodoSessionManager(client_factory=client_factory).open(
                settings=_datacenter_settings(10001),
                credential_source=CredentialSource(),
                new_identifier=_identifiers(),
            ) as session:
                if ending == "cancel":
                    scope.cancel()
                    await anyio.sleep_forever()
                if ending != "normal":
                    raise original_error
        except Exception as error:
            caught = error

    assert clients_closed == 1
    assert credentials_cleaned
    assert session is not None
    completed = session.completed_observation()
    assert completed.failed
    assert completed.ended_at_utc
    assert "proxy-secret" not in repr(completed.as_json())
    if ending == "normal":
        assert isinstance(caught, AcquisitionFailure)
        assert caught.result == {
            "hops": [],
            "stopping_condition": "transport_failure",
            "exception_type": "OSError",
            "failure_phase": "client_close",
        }
        assert "proxy-secret" not in repr(caught.result)
    elif ending == "cancel":
        assert scope.cancelled_caught
        assert caught is None
    else:
        assert caught is not None
        assert caught is original_error
        if isinstance(caught, AcquisitionFailure):
            assert caught.result["stopping_condition"] == "test"
            assert caught.result["cleanup_failure"]["failure_phase"] == "client_close"
            assert "proxy-secret" not in repr(caught.result)
        else:
            assert caught.__notes__ == ["Decodo client cleanup also failed: OSError"]
