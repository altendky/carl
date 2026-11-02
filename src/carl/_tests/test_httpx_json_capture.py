"""JSON POST capture shares raw transport bounds without retaining runtime credentials."""

# Bounded streams are shared with the existing capture integration tests.
# pyright: reportPrivateUsage=false

import gzip
import json
from pathlib import Path
from uuid import uuid4

import anyio
import httpcore
import httpx
import pytest

from carl._tests.test_http_capture import _Stream
from carl.core.content_encoding import decoded_stored_body
from carl.core.http import RequestPlan
from carl.core.models import Header
from carl.core.resources import ResourceRequest
from carl.io import httpx as httpx_io
from carl.io.httpx import (
    MAX_JSON_REQUEST_BYTES,
    AcquisitionFailure,
    ClientHttpxAcquirer,
    DirectHttpxAcquirer,
    HttpJsonCaptureAcquirer,
    completed_capture_after_close_failure,
)
from carl.io.resource_permits import current_resource_manager
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
_CONTENT = b'{"locations":[[-77,40],[-76,41]],"metrics":["distance","duration"]}'


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _plan(**updates: object) -> RequestPlan:
    plan = RequestPlan(
        url="https://offline.test/matrix",
        method="POST",
        follow_redirects=False,
        headers=(Header(name=b"Content-Type", value=b"application/json"),),
    )
    return plan.model_copy(update=updates)


async def test_raw_json_post_preserves_prepared_bytes_and_redacts_client_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unexpected_cpu(*_args: object):
        pytest.fail("Raw JSON capture must not request CPU parsing or decoding")

    monkeypatch.setattr(httpx_io, "run_cpu", unexpected_cpu)
    compressed = gzip.compress(b"x" * 100_000)
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, headers={"Content-Encoding": "gzip"}, stream=_Stream(compressed))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
        headers={
            "Authorization": "Bearer runtime-api-secret",
            "Cookie": "session=runtime-cookie-secret",
        },
    ) as client:
        acquirer = ClientHttpxAcquirer(
            client=client,
            expected_routing=("direct",),
            routing_observation=None,
            authentication="api_key",
        )
        assert isinstance(acquirer, HttpJsonCaptureAcquirer)
        captured = await acquirer.capture_json(
            _plan(max_body_bytes=1000), _CONTENT, lambda: str(uuid4())
        )
    assert len(requests) == 1 and requests[0].method == "POST"
    assert requests[0].content == _CONTENT
    assert requests[0].headers["Authorization"] == "Bearer runtime-api-secret"
    assert requests[0].headers["Cookie"] == "session=runtime-cookie-secret"
    assert captured.bodies[0].content == compressed
    assert captured.bodies[0].representation["content_decoded"] is False
    assert captured.record["hops"][0]["request"]["body"] == {
        "kind": "application/json",
        "bytes": len(_CONTENT),
    }
    evidence = json.dumps(captured.record)
    assert "runtime-api-secret" not in evidence and "runtime-cookie-secret" not in evidence
    assert "locations" not in evidence


@pytest.mark.parametrize(
    "case", ("get", "missing_type", "wrong_type", "duplicate_type", "empty", "oversized")
)
async def test_invalid_json_capture_is_rejected_before_wire(case: str) -> None:
    count = 0

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal count
        count += 1
        raise AssertionError("Invalid prepared JSON must never reach the transport")

    plan = _plan()
    content = _CONTENT
    if case == "get":
        plan = plan.model_copy(update={"method": "GET"})
    elif case == "missing_type":
        plan = plan.model_copy(update={"headers": ()})
    elif case == "wrong_type":
        plan = plan.model_copy(
            update={"headers": (Header(name=b"Content-Type", value=b"text/plain"),)}
        )
    elif case == "duplicate_type":
        plan = plan.model_copy(update={"headers": plan.headers * 2})
    elif case == "empty":
        content = b""
    else:
        content = b"x" * (MAX_JSON_REQUEST_BYTES + 1)
    with pytest.raises(ValueError):
        await DirectHttpxAcquirer(httpx.MockTransport(respond)).capture_json(
            plan, content, lambda: str(uuid4())
        )
    assert count == 0


@pytest.mark.parametrize("failure", ("read", "close", "size", "broken_pipe"))
async def test_json_capture_failure_keeps_protected_evidence_and_never_replays_post(
    failure: str,
) -> None:
    calls = 0
    compressed = gzip.compress(b'{"distances":[[123]]}')

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if failure == "broken_pipe":
            raise httpcore.WriteError("offline broken pipe")
        return httpx.Response(
            200,
            headers={"Content-Encoding": "gzip"},
            stream=_Stream(
                compressed,
                fail_read=failure == "read",
                fail_close=failure == "close",
            ),
        )

    plan = _plan(max_body_bytes=1 if failure == "size" else 1000, reconnect_on_broken_pipe=True)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), headers={"Authorization": "failure-api-secret"}
    ) as client:
        acquirer = ClientHttpxAcquirer(
            client=client,
            expected_routing=("direct",),
            routing_observation=None,
            authentication="api_key",
        )
        with pytest.raises(AcquisitionFailure) as raised:
            await acquirer.capture_json(plan, _CONTENT, lambda: str(uuid4()))
    assert calls == 1
    error = raised.value
    assert "failure-api-secret" not in json.dumps(error.result)
    if failure == "close":
        assert error.result["failure_phase"] == "response_close"
        assert error.bodies[0].content == compressed
        completed = completed_capture_after_close_failure(error, plan=plan)
        assert completed is not None and completed.bodies[0].content == compressed
    else:
        assert error.bodies == ()
        assert error.result["stopping_condition"] == (
            "body_size_limit" if failure == "size" else "transport_failure"
        )


async def test_json_capture_waits_for_http_capacity_and_does_not_need_cpu(tmp_path: Path) -> None:
    requested = anyio.Event()
    started = anyio.Event()
    completed = anyio.Event()

    def respond(_request: httpx.Request) -> httpx.Response:
        requested.set()
        return httpx.Response(200, stream=_Stream(b"{}"))

    async def capture() -> None:
        started.set()
        await DirectHttpxAcquirer(httpx.MockTransport(respond)).capture_json(
            _plan(), _CONTENT, lambda: str(uuid4())
        )
        completed.set()

    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1, "http": 1}
    ):
        manager = current_resource_manager()
        assert manager is not None
        manager.poll_seconds = 0.01
        async with manager.admit(ResourceRequest(name="cpu")), anyio.create_task_group() as tasks:
            async with manager.admit(ResourceRequest(name="http")):
                tasks.start_soon(capture)
                await started.wait()
                await anyio.sleep(0.05)
                assert not requested.is_set()
            with anyio.fail_after(3):
                await completed.wait()
            assert requested.is_set()


@pytest.mark.parametrize("mode", ("capture_json", "capture", "acquire"))
@pytest.mark.parametrize("compressed", (False, True))
async def test_prebuffered_response_records_decoding_truthfully_without_decoding_twice(
    monkeypatch: pytest.MonkeyPatch, mode: str, compressed: bool
) -> None:
    async def unexpected_cpu(*_args: object):
        pytest.fail("Already decoded transport bytes must never be decoded again")

    monkeypatch.setattr(httpx_io, "run_cpu", unexpected_cpu)
    content = b'{"distances":[[123]],"durations":[[45]]}'

    def respond(_request: httpx.Request) -> httpx.Response:
        if compressed:
            return httpx.Response(
                200, headers={"Content-Encoding": "gzip"}, content=gzip.compress(content)
            )
        return httpx.Response(200, json=json.loads(content))

    acquirer = DirectHttpxAcquirer(httpx.MockTransport(respond))
    plan = _plan() if mode == "capture_json" else RequestPlan(url="https://offline.test/")
    if mode == "capture_json":
        acquired = await acquirer.capture_json(plan, _CONTENT, lambda: str(uuid4()))
    else:
        acquired = await (acquirer.capture if mode == "capture" else acquirer.acquire)(
            plan, lambda: str(uuid4())
        )
    retained = acquired.bodies[0]
    assert retained.content == content
    assert retained.representation["content_decoded"] is True
    assert retained.representation["kind"] == "content_decoded_http_body"
    assert retained.representation["content_encoding_headers"] == (["gzip"] if compressed else [])
    assert (
        decoded_stored_body(
            retained.content, retained.representation, ("gzip",) if compressed else ()
        )
        == content
    )
    if compressed:
        assert any(
            header.get("name_latin1", "").lower() == "content-encoding"
            and header.get("value_latin1") == "gzip"
            for header in acquired.record["hops"][0]["response"]["headers"]
        )


async def test_prebuffered_decoded_body_is_bounded_before_retention() -> None:
    content = gzip.compress(b"x" * 1000)
    assert len(content) < 64

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Content-Encoding": "gzip"}, content=content)

    with pytest.raises(AcquisitionFailure) as raised:
        await DirectHttpxAcquirer(httpx.MockTransport(respond)).capture_json(
            _plan(max_body_bytes=64), _CONTENT, lambda: str(uuid4())
        )
    assert raised.value.result["stopping_condition"] == "body_size_limit"
    assert raised.value.bodies == ()
    assert raised.value.result["hops"][0]["response"]["body"]["reason"] == "size_limit_exceeded"


async def test_prebuffered_complete_body_survives_eof_close_failure_with_decoding_metadata() -> (
    None
):
    content = b'{"distances":[[123]]}'

    class CloseFailureResponse(httpx.Response):
        async def aclose(self) -> None:
            await super().aclose()
            raise httpcore.ProtocolError("offline buffered close failure")

    def respond(_request: httpx.Request) -> httpx.Response:
        return CloseFailureResponse(
            200, headers={"Content-Encoding": "gzip"}, content=gzip.compress(content)
        )

    plan = _plan()
    with pytest.raises(AcquisitionFailure) as raised:
        await DirectHttpxAcquirer(httpx.MockTransport(respond)).capture_json(
            plan, _CONTENT, lambda: str(uuid4())
        )
    recovered = completed_capture_after_close_failure(raised.value, plan=plan)
    assert recovered is not None
    assert recovered.bodies[0].content == content
    assert recovered.bodies[0].representation["content_decoded"] is True
    assert recovered.bodies[0].representation["content_encoding_headers"] == ["gzip"]
