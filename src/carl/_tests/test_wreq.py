import json
from collections.abc import AsyncGenerator, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import get_ident
from typing import cast, final
from uuid import uuid4

import anyio
import pytest
import wreq
import wreq.blocking
import wreq.exceptions
from pydantic import SecretStr

from carl._tests.test_ebay_workers import _directories, _provenance
from carl.core.ebay import EbaySearchRequest, ebay_search_url
from carl.core.http import RequestPlan
from carl.core.models import (
    ConfigurationDocumentIdentity,
    ConfigurationSchemaIdentity,
    Domain,
    Header,
    Namespace,
)
from carl.core.routing import (
    DecodoProduct,
    DecodoRouteIdentity,
    LocalSocks5Endpoint,
    RemoteProxyEndpoint,
)
from carl.ebay import collect_configured_ebay_search
from carl.io.decodo import (
    DecodoCredentials,
    DecodoProxySettings,
    ManagedDecodoWreqAcquirer,
    StaticDecodoCredentialSource,
)
from carl.io.httpx import AcquisitionFailure, RouteConfigurationFailure
from carl.io.proton import (
    ManagedProtonWreqAcquirer,
    ProtonSessionManager,
    ProtonWireproxySettings,
)
from carl.io.sqlite import Database
from carl.io.wreq import (
    LocalSocks5WreqAcquirer,
    WreqClientFactory,
    WreqTransportSettings,
)


@dataclass(frozen=True)
class _Status:
    value: int

    def as_int(self) -> int:
        return self.value


@final
class _Stream:
    def __init__(self, chunks: tuple[object, ...]):
        self.chunks = chunks

    def __enter__(self) -> Iterator[object]:
        return iter(self.chunks)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback


@final
class _Response:
    def __init__(
        self,
        *,
        url: str,
        status: int,
        headers: tuple[tuple[bytes, bytes], ...],
        chunks: tuple[object, ...],
    ):
        self.url = url
        self.status = _Status(status)
        self.version = wreq.Version.HTTP_2
        self.headers = headers
        self.chunks = chunks
        self.closed = False
        self.close_error: Exception | None = None

    def stream(self) -> _Stream:
        return _Stream(self.chunks)

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


@final
class _Client:
    def __init__(
        self,
        outcomes: list[_Response | Exception],
        *,
        calls: list[tuple[int, str, dict[str, object]]],
    ):
        self.outcomes = outcomes
        self.calls = calls
        self.closed = False
        self.close_thread: int | None = None
        self.close_error: Exception | None = None

    def get(self, url: str, **kwargs: object) -> _Response:
        self.calls.append((get_ident(), url, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def close(self) -> None:
        self.closed = True
        self.close_thread = get_ident()
        if self.close_error is not None:
            raise self.close_error


@final
class _Factory:
    def __init__(self, outcomes: list[_Response | Exception]):
        self.calls: list[tuple[int, str, dict[str, object]]] = []
        self.client = _Client(outcomes, calls=self.calls)
        self.proxy: wreq.Proxy | None = None
        self.settings: WreqTransportSettings | None = None
        self.plan: RequestPlan | None = None
        self.thread: int | None = None

    def __call__(
        self,
        *,
        proxy: wreq.Proxy,
        settings: WreqTransportSettings,
        plan: RequestPlan,
    ) -> _Client:
        self.proxy = proxy
        self.settings = settings
        self.plan = plan
        self.thread = get_ident()
        return self.client


def _acquirer(factory: _Factory) -> LocalSocks5WreqAcquirer:
    return LocalSocks5WreqAcquirer(
        endpoint=LocalSocks5Endpoint(port=31080),
        expected_routing=("proton", "personal", "carl"),
        routing_observation={"state": "test", "observed_exit_ip": "203.0.113.7"},
        client_factory=cast(WreqClientFactory, cast(object, factory)),
    )


def _response(
    *,
    url: str = "https://www.ebay.com/sch/i.html?_nkw=oscilloscope",
    status: int = 200,
    headers: tuple[tuple[bytes, bytes], ...] = (
        (b"content-type", b"text/html; charset=utf-8"),
        (b"content-encoding", b"br"),
        (b"date", b"today"),
        (b"set-cookie", b"secret-session"),
    ),
    chunks: tuple[object, ...] = (b"<html>results</html>",),
) -> _Response:
    return _Response(url=url, status=status, headers=headers, chunks=chunks)


def _decodo_settings() -> DecodoProxySettings:
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


def test_real_wreq_accepts_list_header_order_without_network() -> None:
    headers = wreq.HeaderMap()
    headers.append("Referer", "https://example.invalid/")
    client = wreq.blocking.Client(https_only=True)
    try:
        # HTTPS-only rejects this URL locally after argument conversion. A tuple
        # for orig_headers instead raises TypeError before that builder check.
        with pytest.raises(wreq.exceptions.BuilderError):
            client.get("http://example.invalid/", headers=headers, orig_headers=["Referer"])
    finally:
        client.close()


@pytest.mark.anyio
async def test_profiled_acquisition_runs_in_thread_and_records_truthful_provenance() -> None:
    response = _response()
    factory = _Factory([response])
    main_thread = get_ident()
    plan = RequestPlan(
        url=response.url,
        headers=(Header(name=b"X-Test", value=b"value"),),
        routing=("proton", "personal", "carl"),
    )

    acquisition = await _acquirer(factory).acquire(plan, lambda: "body-1")

    assert factory.thread is not None and factory.thread != main_thread
    assert factory.thread == factory.calls[0][0] == factory.client.close_thread
    assert factory.proxy is not None
    assert "socks5h://127.0.0.1:31080/" in str(factory.proxy)
    assert response.closed is True
    assert factory.client.closed is True
    assert factory.calls[0][1] == response.url
    request_options = factory.calls[0][2]
    assert request_options["default_headers"] is True
    assert list(cast(wreq.HeaderMap, request_options["headers"])) == [(b"x-test", b"value")]
    assert request_options["orig_headers"] == ["X-Test"]

    transport = acquisition.record["transport"]
    assert transport["implementation"] == "wreq"
    assert transport["implementation_version"] == "0.12.3"
    assert transport["emulation_profile"] == "Chrome153"
    assert transport["execution_mode"] == "blocking_worker_thread"
    assert transport["effective_timeouts"]["total_seconds"] == 40.0
    request = acquisition.record["hops"][0]["request"]
    assert request["header_evidence_scope"] == "caller_supplied_only"
    assert request["profile_generated_headers"]["state"] == "not_observed"
    assert "secret-session" not in json.dumps(acquisition.record)
    assert acquisition.record["hops"][0]["response"]["headers"][-1]["value"]["state"] == (
        "redacted"
    )

    assert acquisition.bodies[0].content == b"<html>results</html>"
    representation = acquisition.bodies[0].representation
    assert representation["kind"] == "content_decoded_http_body"
    assert representation["exact_wire_bytes"] is False
    assert representation["content_encoding_headers"] == ["br"]
    assert representation["received_content_bytes"]["state"] == "unavailable"


@pytest.mark.anyio
async def test_manual_redirects_reuse_one_client_and_retain_each_body() -> None:
    first = _response(
        url="https://www.ebay.com/start",
        status=302,
        headers=((b"location", b"/final"), (b"set-cookie", b"redirect-secret")),
        chunks=(b"redirect",),
    )
    final = _response(
        url="https://www.ebay.com/final",
        headers=((b"content-type", b"text/html"),),
        chunks=(b"results",),
    )
    factory = _Factory([first, final])
    identifiers = iter(("body-1", "body-2"))

    acquisition = await _acquirer(factory).acquire(
        RequestPlan(
            url=first.url,
            routing=("proton", "personal", "carl"),
            max_redirects=2,
        ),
        lambda: next(identifiers),
    )

    assert [call[1] for call in factory.calls] == [first.url, final.url]
    assert acquisition.record["redirect_count"] == 1
    assert acquisition.record["effective_url"] == final.url
    assert [body.content for body in acquisition.bodies] == [b"redirect", b"results"]
    assert "redirect-secret" not in json.dumps(acquisition.record)


@pytest.mark.anyio
async def test_decoded_body_limit_is_a_retained_acquisition_failure() -> None:
    response = _response(chunks=(b"123", b"456"))
    factory = _Factory([response])

    with pytest.raises(AcquisitionFailure) as raised:
        _ = await _acquirer(factory).acquire(
            RequestPlan(
                url=response.url,
                routing=("proton", "personal", "carl"),
                max_body_bytes=5,
            ),
            lambda: "unused",
        )

    assert raised.value.result["stopping_condition"] == "body_size_limit"
    body = raised.value.result["hops"][0]["response"]["body"]
    assert body == {
        "state": "unavailable",
        "reason": "size_limit_exceeded",
        "measured_representation": "wreq_content_decoded",
    }
    assert raised.value.bodies == ()
    assert response.closed is True
    assert factory.client.closed is True


@pytest.mark.anyio
async def test_wreq_transport_failure_is_safe_and_attributed() -> None:
    factory = _Factory([wreq.exceptions.ConnectionError("secret diagnostic")])

    with pytest.raises(AcquisitionFailure) as raised:
        _ = await _acquirer(factory).acquire(
            RequestPlan(
                url="https://www.ebay.com/",
                routing=("proton", "personal", "carl"),
            ),
            lambda: "unused",
        )

    assert raised.value.result["stopping_condition"] == "transport_failure"
    assert raised.value.result["exception_type"] == "ConnectionError"
    assert raised.value.result["failed_request"] == {
        "method": "GET",
        "url": "https://www.ebay.com/",
    }
    assert "secret diagnostic" not in json.dumps(raised.value.result)


@pytest.mark.anyio
async def test_client_cleanup_failure_retains_completed_response_without_diagnostic() -> None:
    factory = _Factory([_response(url="https://www.ebay.com/")])
    factory.client.close_error = RuntimeError("secret cleanup diagnostic")

    with pytest.raises(AcquisitionFailure) as raised:
        _ = await _acquirer(factory).acquire(
            RequestPlan(
                url="https://www.ebay.com/",
                routing=("proton", "personal", "carl"),
            ),
            lambda: "body",
        )

    assert raised.value.result["stopping_condition"] == "transport_failure"
    assert raised.value.result["failure_phase"] == "client_close"
    assert raised.value.result["exception_type"] == "RuntimeError"
    assert raised.value.bodies[0].content == b"<html>results</html>"
    assert "secret cleanup diagnostic" not in json.dumps(raised.value.result)


@pytest.mark.anyio
async def test_response_cleanup_failure_retains_completed_response() -> None:
    response = _response()
    response.close_error = RuntimeError("secret response diagnostic")
    factory = _Factory([response])

    with pytest.raises(AcquisitionFailure) as raised:
        await _acquirer(factory).acquire(
            RequestPlan(url=response.url, routing=("proton", "personal", "carl")),
            lambda: "body",
        )

    assert raised.value.result["failure_phase"] == "response_close"
    assert raised.value.bodies[0].content == b"<html>results</html>"
    assert response.closed and factory.client.closed
    assert "secret response diagnostic" not in json.dumps(raised.value.result)


@pytest.mark.anyio
async def test_rejected_redirect_is_typed_failure_with_complete_evidence() -> None:
    response = _response(status=302, headers=((b"location", b"http://www.ebay.com/unsafe"),))
    factory = _Factory([response])

    with pytest.raises(AcquisitionFailure) as raised:
        await _acquirer(factory).acquire(
            RequestPlan(url=response.url, routing=("proton", "personal", "carl")),
            lambda: "body",
        )

    assert raised.value.result["stopping_condition"] == "invalid_redirect"
    assert raised.value.bodies[0].content == b"<html>results</html>"
    assert len(raised.value.result["hops"]) == 1
    assert response.closed and factory.client.closed
    assert len(factory.calls) == 1


@pytest.mark.anyio
async def test_proxy_diagnostics_redacted_in_headers_and_trailers() -> None:
    response = _response(
        headers=((b"Proxy-Status", b"secret header diagnostic"),),
        chunks=(b"results", ((b"proxy-status", b"secret trailer diagnostic"),)),
    )
    acquisition = await _acquirer(_Factory([response])).acquire(
        RequestPlan(url=response.url, routing=("proton", "personal", "carl")),
        lambda: "body",
    )

    serialized = json.dumps(acquisition.record)
    assert "secret header diagnostic" not in serialized
    assert "secret trailer diagnostic" not in serialized
    response_record = acquisition.record["hops"][0]["response"]
    assert response_record["headers"][0]["value"]["state"] == "redacted"
    assert response_record["trailers"][0]["value"]["state"] == "redacted"


@pytest.mark.anyio
async def test_wreq_acquirer_rejects_route_method_and_compression_mismatch() -> None:
    acquirer = _acquirer(_Factory([_response()]))

    with pytest.raises(RouteConfigurationFailure):
        _ = await acquirer.acquire(
            RequestPlan(url="https://www.ebay.com/", routing=("direct",)),
            lambda: "unused",
        )
    with pytest.raises(ValueError, match="GET only"):
        _ = await acquirer.acquire(
            RequestPlan(
                url="https://www.ebay.com/",
                method="POST",
                follow_redirects=False,
                routing=("proton", "personal", "carl"),
            ),
            lambda: "unused",
        )
    with pytest.raises(ValueError, match="compression defaults"):
        _ = await acquirer.acquire(
            RequestPlan(
                url="https://www.ebay.com/",
                routing=("proton", "personal", "carl"),
                compression=("gzip",),
            ),
            lambda: "unused",
        )


@final
class _Session:
    def __init__(self):
        self.endpoint = LocalSocks5Endpoint(port=31080)
        self.released = False

    def active_observation(self) -> dict[str, object]:
        return {"state": "ready", "observed_exit_ip": "203.0.113.7"}

    def completed_observation(self) -> dict[str, object]:
        if not self.released:
            raise RuntimeError("session has not been released")
        return {"state": "released", "observed_exit_ip": "203.0.113.7"}


@final
class _Manager:
    def __init__(self):
        self.session = _Session()

    @asynccontextmanager
    async def open(self, _settings: ProtonWireproxySettings) -> AsyncGenerator[_Session]:
        try:
            yield self.session
        finally:
            self.session.released = True


@dataclass(frozen=True)
class _Route:
    network_path: tuple[str, ...] = ("proton", "personal", "carl")


@dataclass(frozen=True)
class _ProtonSettings:
    route: _Route = _Route()


@pytest.mark.anyio
async def test_managed_proton_wreq_acquirer_records_released_route() -> None:
    settings = cast(ProtonWireproxySettings, cast(object, _ProtonSettings()))
    factory = _Factory([_response(url="https://www.ebay.com/")])
    manager = _Manager()
    acquirer = ManagedProtonWreqAcquirer(
        manager=cast(ProtonSessionManager, cast(object, manager)),
        settings=settings,
        client_factory=cast(WreqClientFactory, cast(object, factory)),
    )

    acquisition = await acquirer.acquire(
        RequestPlan(url="https://www.ebay.com/", routing=settings.route.network_path),
        lambda: "body",
    )

    assert manager.session.released is True
    assert acquisition.record["routing"]["observed"] == {
        "state": "released",
        "observed_exit_ip": "203.0.113.7",
    }

    with pytest.raises(RouteConfigurationFailure):
        _ = await acquirer.acquire(
            RequestPlan(url="https://www.ebay.com/", routing=("direct",)),
            lambda: "unused",
        )


@pytest.mark.anyio
async def test_managed_decodo_wreq_acquirer_uses_safe_sticky_proxy_session() -> None:
    factory = _Factory([_response()])
    identifiers = iter(("session-record", "body"))
    acquirer = ManagedDecodoWreqAcquirer(
        settings=_decodo_settings(),
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("proxy-password-secret"))
        ),
        client_factory=cast(WreqClientFactory, cast(object, factory)),
        provider_session_factory=lambda: "ProviderSession",
    )

    acquisition = await acquirer.acquire(
        RequestPlan(
            url="https://www.ebay.com/sch/i.html?_nkw=oscilloscope",
            routing=("decodo", "personal", "carl"),
        ),
        lambda: next(identifiers),
    )

    assert factory.proxy is not None
    proxy_description = str(factory.proxy)
    assert "gate.decodo.com:7000" in proxy_description
    assert "user-example-country-us-session-ProviderSession-sessionduration-15" in (
        proxy_description
    )
    assert "proxy-password-secret" in proxy_description
    retained = json.dumps(acquisition.record)
    assert "ProviderSession" not in retained
    assert "proxy-password-secret" not in retained
    assert acquisition.record["authentication"]["network"] == {
        "kind": "proxy_basic",
        "credential_reference": ["carl", "configuration", "decodo", "carl"],
    }
    observation = acquisition.record["routing"]["observed"]
    assert observation["state"] == "closed"
    assert observation["sticky_peer_requested"] is True
    assert observation["tls_trust_store"] == "wreq_default"


@pytest.mark.anyio
async def test_managed_decodo_wreq_proxy_failure_is_attributed_without_diagnostic() -> None:
    factory = _Factory([wreq.exceptions.ProxyConnectionError("secret proxy diagnostic")])
    identifiers = iter(("session-record", "unused"))
    acquirer = ManagedDecodoWreqAcquirer(
        settings=_decodo_settings(),
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("proxy-password-secret"))
        ),
        client_factory=cast(WreqClientFactory, cast(object, factory)),
        provider_session_factory=lambda: "ProviderSession",
    )

    with pytest.raises(AcquisitionFailure) as raised:
        _ = await acquirer.acquire(
            RequestPlan(
                url="https://www.ebay.com/",
                routing=("decodo", "personal", "carl"),
            ),
            lambda: next(identifiers),
        )

    assert raised.value.result["network_provider_result"] == {
        "provider": "decodo",
        "category": "proxy_transport_or_authentication",
        "http_status": None,
        "error_code": None,
    }
    retained = json.dumps(raised.value.result)
    assert "ProviderSession" not in retained
    assert "proxy-password-secret" not in retained
    assert "secret proxy diagnostic" not in retained


def test_wreq_transport_settings_are_secret_free_and_explicit() -> None:
    settings = WreqTransportSettings()
    safe = settings.safe_configuration(RequestPlan(url="https://www.ebay.com/"))

    assert safe["implementation_version"] == "0.12.3"
    assert safe["emulation_profile"] == "Chrome153"
    assert safe["emulation_platform"] == "profile_default"
    assert safe["profile_default_headers"] is True
    assert safe["tls_certificate_verification"] == "enabled"
    assert safe["tls_trust_store"] == "wreq_default"
    assert "proxy" not in json.dumps(safe).lower()


@pytest.mark.anyio
async def test_configured_search_shares_session_across_pages_not_attempts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = b'<li class="s-card"><a href="/itm/256123456789">Scope</a></li><a class="pagination__next" href="/sch/i.html?_pgn=2">Next</a>'
    second = b'<li class="s-card"><a href="/itm/256987654321">Another scope</a></li>'
    clients: list[_Client] = []
    proxies: list[str] = []

    def client_factory(
        *, proxy: wreq.Proxy, settings: WreqTransportSettings, plan: RequestPlan
    ) -> _Client:
        del settings, plan
        proxies.append(str(proxy))
        client = _Client(
            [
                _response(url=ebay_search_url("scope"), chunks=(first,)),
                _response(url=ebay_search_url("scope", page_number=2), chunks=(second,)),
            ],
            calls=[],
        )
        clients.append(client)
        return client

    credentials = StaticDecodoCredentialSource(
        DecodoCredentials(proxy_password=SecretStr("secret"))
    )
    provider_sessions = iter(("FirstProviderSession", "SecondProviderSession"))
    acquirer = ManagedDecodoWreqAcquirer(
        settings=_decodo_settings(),
        credential_source=credentials,
        client_factory=cast(WreqClientFactory, cast(object, client_factory)),
        provider_session_factory=lambda: next(provider_sessions),
    )
    monkeypatch.setattr("carl.ebay.load_configuration", lambda _: None)
    monkeypatch.setattr(
        "carl.ebay.decodo_wreq_stack_settings",
        lambda *_: (_decodo_settings(), credentials, WreqTransportSettings()),
    )
    monkeypatch.setattr("carl.ebay.ManagedDecodoWreqAcquirer", lambda **_: acquirer)
    monkeypatch.setattr("carl.ebay.collect_code_provenance_async", lambda _: _provenance())
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        for _ in range(2):
            result = await collect_configured_ebay_search(
                database,
                directories=_directories(tmp_path),
                request=EbaySearchRequest(query="scope", maximum_pages=2),
                new_identifier=lambda: str(uuid4()),
            )
            assert result["state"] == "completed" and result["page_count"] == 2
            assert result["listing_count"] == 2
        acquisitions = await database.records_by_kind(("carl", "http", "acquisition"))
        observations = [value["routing"]["observed"] for _, value in acquisitions]
        session_ids = [observation["session_record_identifier"] for observation in observations]
        assert session_ids[0] == session_ids[1] != session_ids[2] == session_ids[3]
        assert [observation["requests_completed"] for observation in observations] == [1, 2, 1, 2]
        assert all(observation["state"] == "active" for observation in observations)
        assert all(
            value["transport"]["effective_timeouts"]["pool_seconds"]["reason"]
            == "one_client_per_managed_session"
            for _, value in acquisitions
        )
        assert "FirstProviderSession" not in json.dumps(acquisitions)
        assert "SecondProviderSession" not in json.dumps(acquisitions)
    assert len(clients) == 2 and all(client.closed and len(client.calls) == 2 for client in clients)
    assert "session-FirstProviderSession-" in proxies[0]
    assert "session-SecondProviderSession-" in proxies[1]


@pytest.mark.anyio
@pytest.mark.parametrize("ending", ("normal", "exception", "cancel", "transport_failure"))
async def test_shared_decodo_session_cleanup_and_cookie_client_lifetime(ending: str) -> None:
    factory = _Factory([_response(), wreq.exceptions.ProxyConnectionError("secret diagnostic")])
    released = False

    @asynccontextmanager
    async def credentials():
        nonlocal released
        try:
            yield DecodoCredentials(proxy_password=SecretStr("password"))
        finally:
            await anyio.lowlevel.checkpoint()
            released = True

    class CredentialSource:
        def open(self):
            return credentials()

    acquirer = ManagedDecodoWreqAcquirer(
        settings=_decodo_settings(),
        credential_source=CredentialSource(),
        client_factory=cast(WreqClientFactory, cast(object, factory)),
    )
    plan = RequestPlan(url="https://www.ebay.com/", routing=("decodo", "personal", "carl"))
    main_thread = get_ident()
    session = None
    with anyio.CancelScope() as scope:
        try:
            async with acquirer.session(lambda: str(uuid4())) as session:
                acquisition = await session.acquire(plan, lambda: str(uuid4()))
                assert not factory.client.closed
                assert acquisition.record["routing"]["observed"]["requests_completed"] == 1
                if ending == "exception":
                    raise RuntimeError("outer failure")
                if ending == "cancel":
                    scope.cancel()
                    await anyio.lowlevel.checkpoint()
                if ending == "transport_failure":
                    await session.acquire(plan, lambda: str(uuid4()))
        except RuntimeError as error:
            assert ending == "exception" and str(error) == "outer failure"
        except AcquisitionFailure as error:
            assert ending == "transport_failure"
            assert "secret diagnostic" not in json.dumps(error.result)
    assert released and factory.client.closed
    assert factory.client.close_thread != main_thread
    assert session is not None
    assert session.observation()["state"] == "closed"
    with pytest.raises(RouteConfigurationFailure, match="decodo_session_not_available"):
        await session.acquire(plan, lambda: str(uuid4()))


@pytest.mark.anyio
@pytest.mark.parametrize("ending", ("normal", "body_error", "cancel"))
async def test_shared_wreq_close_failure_reports_safely_and_preserves_primary(
    ending: str, caplog: pytest.LogCaptureFixture
) -> None:
    factory = _Factory([_response()])
    factory.client.close_error = OSError("secret provider diagnostic")
    acquirer = ManagedDecodoWreqAcquirer(
        settings=_decodo_settings(),
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("secret"))
        ),
        client_factory=cast(WreqClientFactory, cast(object, factory)),
    )
    original = RuntimeError("original body error")

    async def run(scope: anyio.CancelScope) -> None:
        async with acquirer.session(lambda: str(uuid4())) as session:
            _ = await session.acquire(
                RequestPlan(url="https://www.ebay.com/", routing=("decodo", "personal", "carl")),
                lambda: str(uuid4()),
            )
            if ending == "cancel":
                scope.cancel()
                await anyio.sleep_forever()
            if ending == "body_error":
                raise original

    with anyio.CancelScope() as scope:
        if ending == "normal":
            with pytest.raises(AcquisitionFailure) as raised_close:
                await run(scope)
            assert raised_close.value.result["failure_phase"] == "client_close"
        elif ending == "body_error":
            with pytest.raises(RuntimeError) as raised_body:
                await run(scope)
            assert raised_body.value is original
            assert original.__notes__ == ["Carl cleanup failed during wreq_client_close: OSError"]
        else:
            await run(scope)
    assert scope.cancelled_caught is (ending == "cancel")
    assert factory.client.closed
    assert factory.client.close_thread != get_ident()
    assert "secret provider diagnostic" not in caplog.text
    assert "wreq_client_close: OSError" in caplog.text


@pytest.mark.anyio
async def test_shared_session_expiration_does_not_rotate_mid_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0
    monkeypatch.setattr("carl.io.decodo.monotonic", lambda: now)
    factory = _Factory([_response()])
    provider_calls: list[int] = []

    def provider_session() -> str:
        provider_calls.append(1)
        return "ProviderSession"

    acquirer = ManagedDecodoWreqAcquirer(
        settings=_decodo_settings(),
        credential_source=StaticDecodoCredentialSource(
            DecodoCredentials(proxy_password=SecretStr("password"))
        ),
        client_factory=cast(WreqClientFactory, cast(object, factory)),
        provider_session_factory=provider_session,
    )
    plan = RequestPlan(url="https://www.ebay.com/", routing=("decodo", "personal", "carl"))
    async with acquirer.session(lambda: str(uuid4())) as session:
        await session.acquire(plan, lambda: str(uuid4()))
        now = 15 * 60
        with pytest.raises(RouteConfigurationFailure, match="decodo_session_duration_expired"):
            await session.acquire(plan, lambda: str(uuid4()))
    assert len(factory.calls) == len(provider_calls) == 1
    assert factory.client.closed
