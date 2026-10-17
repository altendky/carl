import json
from collections.abc import Callable
from threading import Event
from typing import cast

import anyio
import pytest
import wreq
import wreq.exceptions

from carl._tests.test_wreq import _Client, _Response, _response, _sent_headers
from carl.core.http import RequestPlan
from carl.io.httpx import AcquisitionFailure, RouteConfigurationFailure
from carl.io.wreq import ProxyWreqAcquirer, WreqClientFactory, WreqTransportSettings

_ROUTE = ("diagnostic", "local")
_HOME = "https://www.ebay.com/"
_SEARCH = "https://www.ebay.com/sch/i.html?_nkw=scope"


def _broken_pipe() -> Exception:
    return wreq.exceptions.RequestError(
        'netty::Error(Io, Custom { kind: BrokenPipe, error: "secret-proxy-credential" })'
    )


class _CookieClient:
    def __init__(
        self,
        outcomes: list[_Response | Exception],
        *,
        jar: wreq.Jar | None,
        events: list[str],
        label: str,
        on_get: Callable[[], None] | None = None,
    ) -> None:
        self.inner = _Client(outcomes, calls=[])
        self.cookie_jar = jar
        self.events = events
        self.label = label
        self.on_get = on_get

    def get(self, url: str, **kwargs: object) -> _Response:
        if self.on_get is not None:
            self.on_get()
        return self.inner.get(url, **kwargs)

    def close(self) -> None:
        self.events.append(f"close:{self.label}")
        self.inner.close()


class _ReplacingFactory:
    def __init__(self, clients: list[_CookieClient | Exception], events: list[str]) -> None:
        self.clients = clients
        self.events = events
        self.proxies: list[wreq.Proxy] = []
        self.settings: list[WreqTransportSettings] = []

    def __call__(
        self, *, proxy: wreq.Proxy, settings: WreqTransportSettings, plan: RequestPlan
    ) -> _CookieClient:
        del plan
        self.proxies.append(proxy)
        self.settings.append(settings)
        self.events.append(f"open:{len(self.proxies)}")
        client = self.clients.pop(0)
        if isinstance(client, Exception):
            raise client
        return client


def _acquirer(factory: _ReplacingFactory) -> ProxyWreqAcquirer:
    return ProxyWreqAcquirer(
        proxy_factory=lambda: wreq.Proxy.all("http://localhost:31080"),
        expected_routing=_ROUTE,
        routing_observation={"scope": "synthetic"},
        client_factory=cast(WreqClientFactory, cast(object, factory)),
    )


def _plan(url: str = _SEARCH, **options: object) -> RequestPlan:
    return RequestPlan.model_validate(
        {"url": url, "routing": _ROUTE, "reconnect_on_broken_pipe": True, **options}
    )


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ("request", "response_body"))
async def test_reconnect_preserves_native_jar_proxy_hints_and_reuses_replacement(
    phase: str,
) -> None:
    events: list[str] = []
    jar = wreq.Jar()
    broken = _broken_pipe()
    failed_response = _response(
        url=_SEARCH,
        headers=(
            (b"accept-ch", b"sec-ch-ua-platform-version"),
            (b"set-cookie", b"secret-cookie-value"),
        ),
        chunks=(b"partial response", broken),
    )
    first = _CookieClient(
        [
            _response(url=_HOME, headers=((b"accept-ch", b"sec-ch-ua-model"),)),
            broken if phase == "request" else failed_response,
        ],
        jar=jar,
        events=events,
        label="first",
    )
    second = _CookieClient(
        [_response(url=_SEARCH), _response(url=_SEARCH + "&_pgn=2")],
        jar=wreq.Jar(),
        events=events,
        label="second",
    )
    factory = _ReplacingFactory([first, second], events)
    async with _acquirer(factory).session() as session:
        assert session.cookie_jar is None
        await session.acquire(_plan(_HOME), lambda: "home-body")
        assert session.cookie_jar is jar
        acquisition = await session.acquire(_plan(), lambda: "search-body")
        next_page = await session.acquire(_plan(_SEARCH + "&_pgn=2"), lambda: "next-body")
        assert session.cookie_jar is jar
    assert factory.proxies[0] is factory.proxies[1]
    assert factory.settings[0] is factory.settings[1]
    assert events == ["open:1", "close:first", "open:2", "close:second"]
    assert all(
        call[2]["cookie_provider"] is jar for call in (*first.inner.calls, *second.inner.calls)
    )
    retry_headers = _sent_headers(second.inner.calls[0])
    hint = b"sec-ch-ua-model" if phase == "request" else b"sec-ch-ua-platform-version"
    assert retry_headers[hint] == b'""'
    assert second.inner.calls[0][1] == _SEARCH
    assert len(acquisition.record["hops"]) == 1
    assert acquisition.record["redirect_count"] == 0
    assert acquisition.record["transport"]["effective_timeouts"]["pool_seconds"]["reason"] == (
        "one_active_client_per_managed_session"
    )
    recovery = acquisition.record["connection_recovery"]
    assert recovery["attempted"] is True and recovery["recovered"] is True
    assert recovery["first_failure"]["failure_phase"] == phase
    assert len(recovery["first_failure"]["hops"]) == (0 if phase == "request" else 1)
    assert [body.content for body in acquisition.bodies] == [b"<html>results</html>"]
    assert "connection_recovery" not in next_page.record
    serialized = json.dumps(acquisition.record)
    assert "secret-proxy-credential" not in serialized and "secret-cookie-value" not in serialized
    if phase == "response_body":
        assert failed_response.aborted and not failed_response.released


@pytest.mark.anyio
async def test_second_broken_pipe_stops_and_retains_both_failure_records() -> None:
    events: list[str] = []
    first = _CookieClient([_broken_pipe()], jar=wreq.Jar(), events=events, label="first")
    second = _CookieClient([_broken_pipe()], jar=wreq.Jar(), events=events, label="second")
    factory = _ReplacingFactory([first, second], events)
    async with _acquirer(factory).session() as session:
        with pytest.raises(AcquisitionFailure) as raised:
            await session.acquire(_plan(), lambda: "unused")
        with pytest.raises(RouteConfigurationFailure):
            await session.acquire(_plan(), lambda: "unused")
    result = raised.value.result
    assert len(factory.proxies) == 2
    assert result["failure_phase"] == "request" and result["hops"] == []
    assert result["transport_error"]["reason_codes"] == ["broken_pipe"]
    recovery = result["connection_recovery"]
    assert recovery["attempted"] is True and recovery["recovered"] is False
    assert recovery["first_failure"]["transport_error"]["reason_codes"] == ["broken_pipe"]
    assert "secret-proxy-credential" not in json.dumps(result)


@pytest.mark.anyio
async def test_reconnect_budget_resets_for_the_next_page_acquisition() -> None:
    events: list[str] = []
    first = _CookieClient([_broken_pipe()], jar=wreq.Jar(), events=events, label="first")
    second = _CookieClient(
        [_response(url=_SEARCH), _broken_pipe()], jar=wreq.Jar(), events=events, label="second"
    )
    third = _CookieClient(
        [_response(url=_SEARCH + "&_pgn=2")], jar=wreq.Jar(), events=events, label="third"
    )
    factory = _ReplacingFactory([first, second, third], events)
    async with _acquirer(factory).session() as session:
        first_page = await session.acquire(_plan(), lambda: "first-body")
        next_page = await session.acquire(_plan(_SEARCH + "&_pgn=2"), lambda: "second-body")
    assert first_page.record["connection_recovery"]["recovered"] is True
    assert next_page.record["connection_recovery"]["recovered"] is True
    assert len(factory.proxies) == 3
    assert all(proxy is factory.proxies[0] for proxy in factory.proxies)
    assert all(call[2]["cookie_provider"] is first.cookie_jar for call in third.inner.calls)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "case", ("disabled", "standalone", "no_jar", "other_error", "403_body", "response_close")
)
async def test_reconnect_does_not_expand_to_other_policies_or_failures(case: str) -> None:
    events: list[str] = []
    response = _response(url=_SEARCH, status=403 if case == "403_body" else 200)
    if case == "403_body":
        response.chunks = (_broken_pipe(),)
    elif case == "response_close":
        response.release_error = _broken_pipe()
    outcome = (
        response
        if case in {"403_body", "response_close"}
        else wreq.exceptions.RequestError("HTTP2 error")
        if case == "other_error"
        else _broken_pipe()
    )
    client = _CookieClient(
        [outcome], jar=None if case == "no_jar" else wreq.Jar(), events=events, label="first"
    )
    factory = _ReplacingFactory([client], events)
    acquirer = _acquirer(factory)
    plan = _plan(reconnect_on_broken_pipe=case != "disabled")
    with pytest.raises(AcquisitionFailure) as raised:
        if case == "standalone":
            await acquirer.acquire(plan, lambda: "unused")
        else:
            async with acquirer.session() as session:
                await session.acquire(plan, lambda: "unused")
    assert len(factory.proxies) == 1
    if case == "no_jar":
        assert (
            raised.value.result["connection_recovery"]["reason"] == "native_cookie_jar_unavailable"
        )
        assert raised.value.result["connection_recovery"]["attempted"] is False
    else:
        assert "connection_recovery" not in raised.value.result


@pytest.mark.anyio
async def test_completed_403_is_returned_without_reconnect() -> None:
    events: list[str] = []
    client = _CookieClient(
        [_response(url=_SEARCH, status=403)], jar=wreq.Jar(), events=events, label="first"
    )
    factory = _ReplacingFactory([client], events)
    async with _acquirer(factory).session() as session:
        result = await session.acquire(_plan(), lambda: "body")
    assert len(factory.proxies) == 1
    assert result.record["hops"][0]["response"]["status_code"] == 403
    assert "connection_recovery" not in result.record


@pytest.mark.anyio
@pytest.mark.parametrize("maximum_redirects", (1, 2))
async def test_resume_redirect_target_preserves_prefix_once_and_remaining_budget(
    maximum_redirects: int,
) -> None:
    events: list[str] = []
    start, middle, final = _HOME + "start", _HOME + "middle", _HOME + "final"
    first = _CookieClient(
        [
            _response(
                url=start,
                status=302,
                headers=((b"location", middle.encode()),),
                chunks=(b"prefix",),
            ),
            _broken_pipe(),
        ],
        jar=wreq.Jar(),
        events=events,
        label="first",
    )
    second = _CookieClient(
        [
            _response(
                url=middle,
                status=302,
                headers=((b"location", final.encode()),),
                chunks=(b"second redirect",),
            ),
            _response(url=final),
        ],
        jar=wreq.Jar(),
        events=events,
        label="second",
    )
    factory = _ReplacingFactory([first, second], events)
    identifiers = iter(("prefix-body", "middle-body", "final-body"))
    async with _acquirer(factory).session() as session:
        if maximum_redirects == 1:
            with pytest.raises(AcquisitionFailure) as raised:
                await session.acquire(
                    _plan(start, max_redirects=maximum_redirects), lambda: next(identifiers)
                )
            record, bodies = raised.value.result, raised.value.bodies
            assert record["stopping_condition"] == "redirect_limit"
            assert [call[1] for call in second.inner.calls] == [middle]
        else:
            acquired = await session.acquire(
                _plan(start, max_redirects=maximum_redirects), lambda: next(identifiers)
            )
            record, bodies = acquired.record, acquired.bodies
            assert record["redirect_count"] == 2
            assert record["effective_url"] == final
            assert [call[1] for call in second.inner.calls] == [middle, final]
    assert [hop["request"]["url"] for hop in record["hops"]][:2] == [start, middle]
    first_failure = record["connection_recovery"]["first_failure"]
    assert first_failure["redirect_prefix_hop_indexes"] == [0]
    assert first_failure["hops"] == []
    assert [body.content for body in bodies].count(b"prefix") == 1


@pytest.mark.anyio
async def test_reconnect_retains_visited_redirect_urls_for_loop_detection() -> None:
    events: list[str] = []
    middle = _HOME + "middle"
    first = _CookieClient(
        [
            _response(url=_HOME, status=302, headers=((b"location", middle.encode()),)),
            _broken_pipe(),
        ],
        jar=wreq.Jar(),
        events=events,
        label="first",
    )
    second = _CookieClient(
        [_response(url=middle, status=302, headers=((b"location", _HOME.encode()),))],
        jar=wreq.Jar(),
        events=events,
        label="second",
    )
    factory = _ReplacingFactory([first, second], events)
    async with _acquirer(factory).session() as session:
        with pytest.raises(AcquisitionFailure) as raised:
            await session.acquire(_plan(_HOME), lambda: "body")
    assert raised.value.result["stopping_condition"] == "redirect_loop"
    assert len(second.inner.calls) == 1
    assert raised.value.result["connection_recovery"]["first_failure"][
        "redirect_prefix_hop_indexes"
    ] == [0]


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ("client_close", "client_open"))
async def test_failed_replacement_stops_with_first_and_final_diagnostics(phase: str) -> None:
    events: list[str] = []
    client = _CookieClient([_broken_pipe()], jar=wreq.Jar(), events=events, label="first")
    if phase == "client_close":
        client.inner.close_error = RuntimeError("secret cleanup failure")
    factory = _ReplacingFactory(
        [client, wreq.exceptions.ConnectionError("secret open failure")], events
    )
    with pytest.raises(AcquisitionFailure) as raised:
        async with _acquirer(factory).session() as session:
            await session.acquire(_plan(), lambda: "unused")
    record = raised.value.result
    assert record["failure_phase"] == phase
    assert record["connection_recovery"]["recovered"] is False
    assert record["connection_recovery"]["first_failure"]["failure_phase"] == "request"
    assert len(factory.proxies) == (1 if phase == "client_close" else 2)
    assert "secret" not in json.dumps(record)


@pytest.mark.anyio
async def test_cancellation_during_retry_still_closes_replacement_client() -> None:
    events: list[str] = []
    started, release = Event(), Event()

    def wait_for_release() -> None:
        started.set()
        assert release.wait(5)

    first = _CookieClient([_broken_pipe()], jar=wreq.Jar(), events=events, label="first")
    second = _CookieClient(
        [_response(url=_SEARCH)],
        jar=wreq.Jar(),
        events=events,
        label="second",
        on_get=wait_for_release,
    )
    factory = _ReplacingFactory([first, second], events)

    async def acquire() -> None:
        async with _acquirer(factory).session() as session:
            await session.acquire(_plan(), lambda: "body")

    try:
        with anyio.fail_after(10):
            async with anyio.create_task_group() as group:
                group.start_soon(acquire)
                assert await anyio.to_thread.run_sync(started.wait, 5)
                group.cancel_scope.cancel()
                release.set()
    finally:
        release.set()
    assert first.inner.closed and second.inner.closed
    assert events == ["open:1", "close:first", "open:2", "close:second"]
