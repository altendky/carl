"""Transport capture stays bounded and releases HTTP before CPU admission."""

import gzip
import json
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import anyio
import httpcore
import httpx
import pytest
from pydantic import SecretStr

from carl.core.content_encoding import ContentDecodingError, decode_content, decoded_stored_body
from carl.core.http import FormField, RequestPlan
from carl.core.resources import ResourceRequest
from carl.io import httpx as httpx_io
from carl.io.http_checkpoints import checkpoint_acquisition
from carl.io.httpx import (
    AcquiredBody,
    Acquisition,
    AcquisitionFailure,
    ClientHttpxAcquirer,
    DirectHttpxAcquirer,
    IdentifierFactory,
    RouteConfigurationFailure,
    RoutedHttpAcquirer,
    completed_capture_after_close_failure,
)
from carl.io.resource_permits import current_resource_manager
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


class _Stream(httpx.AsyncByteStream):
    def __init__(self, content: bytes, *, fail_read: bool = False, fail_close: bool = False):
        self.content = content
        self.fail_read = fail_read
        self.fail_close = fail_close
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self.content
        if self.fail_read:
            raise httpcore.ProtocolError("offline incomplete transfer")

    async def aclose(self) -> None:
        self.closed = True
        if self.fail_close:
            raise httpcore.ProtocolError("offline close failure")


def _identifier() -> str:
    return str(uuid4())


async def test_capture_retains_undecoded_redirect_bodies_without_cpu_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unexpected_cpu(*_args: object) -> bytes:
        pytest.fail("Capture must not request CPU admission")

    monkeypatch.setattr(httpx_io, "run_cpu", unexpected_cpu)
    contents = (gzip.compress(b"redirect"), gzip.compress(b"terminal"))
    streams = [_Stream(content) for content in contents]

    def respond(request: httpx.Request) -> httpx.Response:
        first = request.url.path == "/first"
        return httpx.Response(
            302 if first else 200,
            headers={"Content-Encoding": "gzip", **({"Location": "/last"} if first else {})},
            stream=streams[0 if first else 1],
        )

    result = await DirectHttpxAcquirer(httpx.MockTransport(respond)).capture(
        RequestPlan(url="https://offline.test/first"), _identifier
    )
    assert [body.content for body in result.bodies] == list(contents)
    assert result.record["redirect_count"] == 1
    assert result.record["effective_url"] == "https://offline.test/last"
    for index, body in enumerate(result.bodies):
        assert body.representation["kind"] == "http_message_content"
        assert body.representation["content_decoded"] is False
        assert body.representation["http_hop_index"] == index
        assert decoded_stored_body(body.content, body.representation, ("gzip",)) == (
            b"redirect" if index == 0 else b"terminal"
        )
    assert all(stream.closed for stream in streams)


async def test_legacy_malformed_compressed_redirect_stops_before_next_request() -> None:
    requests: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return httpx.Response(
            302,
            headers={"Location": "/must-not-request", "Content-Encoding": "gzip"},
            stream=_Stream(b"invalid gzip"),
        )

    with pytest.raises(AcquisitionFailure) as raised:
        await DirectHttpxAcquirer(httpx.MockTransport(respond)).acquire(
            RequestPlan(url="https://offline.test/first"), _identifier
        )
    assert requests == ["https://offline.test/first"]
    assert raised.value.result["stopping_condition"] == "content_decoding_failure"
    assert raised.value.bodies == ()


async def test_capture_form_preserves_transport_values_and_redacts_evidence() -> None:
    seen: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200, headers={"Set-Cookie": "session=response-secret"}, stream=_Stream(b"body")
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        client.cookies.set("session", "request-secret")
        acquirer = ClientHttpxAcquirer(
            client=client,
            expected_routing=("provider",),
            routing_observation={"session": "safe-reference"},
            authentication="session",
            protected_request_headers=frozenset({b"cookie"}),
            protected_response_headers=frozenset({b"set-cookie"}),
        )
        result = await acquirer.capture_form(
            RequestPlan(
                url="https://offline.test/",
                method="POST",
                routing=("provider",),
                follow_redirects=False,
            ),
            (FormField(name="token", value=SecretStr("form-secret"), protected=True),),
            _identifier,
        )
    assert seen[0].content == b"token=form-secret"
    assert "request-secret" in seen[0].headers["Cookie"]
    evidence = json.dumps(result.record)
    for secret in ("form-secret", "request-secret", "response-secret"):
        assert secret not in evidence
    assert result.record["routing"] == {
        "configured": ["provider"],
        "observed": {"session": "safe-reference"},
    }


@pytest.mark.parametrize("capture", (False, True))
@pytest.mark.parametrize("aggregate", (False, True))
async def test_size_limit_preserves_completed_redirect_bodies(
    monkeypatch: pytest.MonkeyPatch,
    capture: bool,
    aggregate: bool,
) -> None:
    monkeypatch.setattr(httpx_io, "MAX_ACQUISITION_BODY_BYTES", 7 if aggregate else 100)
    streams: list[_Stream] = []

    def respond(request: httpx.Request) -> httpx.Response:
        first = request.url.path == "/first"
        stream = _Stream(b"1234" if first else b"5678")
        streams.append(stream)
        return httpx.Response(
            302 if first else 200, headers={"Location": "/last"} if first else {}, stream=stream
        )

    acquirer = DirectHttpxAcquirer(httpx.MockTransport(respond))
    with pytest.raises(AcquisitionFailure) as raised:
        await (acquirer.capture if capture else acquirer.acquire)(
            RequestPlan(url="https://offline.test/first", max_body_bytes=100 if aggregate else 3),
            _identifier,
        )
    failure = raised.value
    assert failure.result["stopping_condition"] == "body_size_limit"
    assert [body.content for body in failure.bodies] == ([b"1234"] if aggregate else [])
    metadata = failure.result["hops"][-1]["response"]["body"]
    assert metadata["reason"] == "size_limit_exceeded"
    assert metadata.get("limit_scope") == ("acquisition" if aggregate else None)
    assert all(stream.closed for stream in streams)


async def test_legacy_decoding_is_bounded_by_remaining_aggregate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(httpx_io, "MAX_ACQUISITION_BODY_BYTES", 70)

    def respond(request: httpx.Request) -> httpx.Response:
        first = request.url.path == "/first"
        return httpx.Response(
            302 if first else 200,
            headers={"Content-Encoding": "gzip", **({"Location": "/last"} if first else {})},
            stream=_Stream(gzip.compress(b"x" * 40)),
        )

    with pytest.raises(AcquisitionFailure) as raised:
        await DirectHttpxAcquirer(httpx.MockTransport(respond)).acquire(
            RequestPlan(url="https://offline.test/first"), _identifier
        )
    assert raised.value.result["stopping_condition"] == "content_decoding_failure"
    assert [body.content for body in raised.value.bodies] == [b"x" * 40]


@pytest.mark.parametrize("close_failure", (False, True))
async def test_capture_transport_failures_retain_previous_complete_bodies(
    close_failure: bool,
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        first = request.url.path == "/first"
        return httpx.Response(
            302 if first else 200,
            headers={"Location": "/last"} if first else {},
            stream=_Stream(
                b"body",
                fail_read=not first and not close_failure,
                fail_close=not first and close_failure,
            ),
        )

    with pytest.raises(AcquisitionFailure) as raised:
        await DirectHttpxAcquirer(httpx.MockTransport(respond)).capture(
            RequestPlan(url="https://offline.test/first"), _identifier
        )
    assert raised.value.result["stopping_condition"] == "transport_failure"
    assert [body.content for body in raised.value.bodies] == (
        [b"body", b"body"] if close_failure else [b"body"]
    )
    assert raised.value.result["exception_type"] == "ProtocolError"
    if not close_failure:
        assert raised.value.result["hops"][-1]["response"]["body"] == {
            "state": "unavailable",
            "reason": "incomplete_transfer",
        }
    else:
        assert raised.value.result["failure_phase"] == "response_close"


@pytest.mark.parametrize("capture", (False, True))
async def test_full_compressed_body_survives_close_failure_and_can_be_checkpointed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capture: bool,
) -> None:
    content = gzip.compress(b"complete paid response")

    async def unexpected_cpu(*_args: object) -> bytes:
        pytest.fail("A close failure must preserve raw bytes without CPU admission")

    monkeypatch.setattr(httpx_io, "run_cpu", unexpected_cpu)
    acquirer = DirectHttpxAcquirer(
        httpx.MockTransport(
            lambda _request: httpx.Response(
                200, headers={"Content-Encoding": "gzip"}, stream=_Stream(content, fail_close=True)
            )
        )
    )
    with pytest.raises(AcquisitionFailure) as raised:
        await (acquirer.capture if capture else acquirer.acquire)(
            RequestPlan(url="https://offline.test/"), _identifier
        )
    failure = raised.value
    assert failure.result["failure_phase"] == "response_close"
    assert len(failure.bodies) == 1 and failure.bodies[0].content == content
    assert failure.bodies[0].representation["kind"] == "http_message_content"
    assert failure.bodies[0].representation["content_decoded"] is False
    assert (
        failure.result["hops"][0]["response"]["body"]["artifact_id"] == failure.bodies[0].identifier
    )
    checkpoint = await checkpoint_acquisition(
        Acquisition(failure.result, failure.bodies),
        data_directory=tmp_path,
        new_identifier=_identifier,
    )
    assert (tmp_path / checkpoint.artifacts[0].locator).read_bytes() == content
    recovered = completed_capture_after_close_failure(
        failure, plan=RequestPlan(url="https://offline.test/")
    )
    assert recovered is not None and recovered.bodies[0].content == content
    assert recovered.record["effective_url"] == "https://offline.test/"
    assert recovered.record["stopping_condition"] == "terminal_response"
    assert recovered.record["session_cleanup_failure"]["failure_phase"] == "response_close"
    assert (
        decoded_stored_body(
            recovered.bodies[0].content, recovered.bodies[0].representation, ("gzip",)
        )
        == b"complete paid response"
    )
    assert failure.result["stopping_condition"] == "transport_failure"


@pytest.mark.parametrize(
    "invalid",
    (
        "missing_body",
        "unavailable_body",
        "wrong_artifact",
        "wrong_size",
        "read_failure",
        "size_failure",
        "followed_redirect",
        "invalid_phase",
    ),
)
def test_close_recovery_rejects_incomplete_or_unrelated_body_evidence(invalid: str) -> None:
    body = AcquiredBody(
        identifier="body",
        media_type=None,
        representation={"kind": "http_message_content", "content_decoded": False},
        content=b"raw",
    )
    response = {
        "status_code": 302 if invalid == "followed_redirect" else 200,
        "url": "https://offline.test/",
        "body": {
            "state": "unavailable" if invalid == "unavailable_body" else "available",
            "artifact_id": "other" if invalid == "wrong_artifact" else "body",
            "bytes": 4 if invalid == "wrong_size" else 3,
        },
    }
    error = AcquisitionFailure(
        "not retained",
        result={
            "hops": [{"response": response}],
            "failure_phase": "response_body"
            if invalid == "read_failure"
            else {}
            if invalid == "invalid_phase"
            else "response_close",
            "stopping_condition": "body_size_limit"
            if invalid == "size_failure"
            else "transport_failure",
        },
        bodies=() if invalid == "missing_body" else (body,),
    )
    assert (
        completed_capture_after_close_failure(error, plan=RequestPlan(url="https://offline.test/"))
        is None
    )


@pytest.mark.parametrize("size_limit", (False, True))
async def test_secondary_close_failure_preserves_partial_or_size_failure(size_limit: bool) -> None:
    acquirer = DirectHttpxAcquirer(
        httpx.MockTransport(
            lambda _request: httpx.Response(
                200, stream=_Stream(b"body", fail_read=not size_limit, fail_close=True)
            )
        )
    )
    with pytest.raises(AcquisitionFailure) as raised:
        await acquirer.capture(
            RequestPlan(url="https://offline.test/", max_body_bytes=2 if size_limit else 100),
            _identifier,
        )
    assert raised.value.bodies == ()
    assert raised.value.result["stopping_condition"] == (
        "body_size_limit" if size_limit else "transport_failure"
    )
    assert "failure_phase" not in raised.value.result
    assert (
        completed_capture_after_close_failure(
            raised.value, plan=RequestPlan(url="https://offline.test/")
        )
        is None
    )


async def test_cpu_wait_releases_http_and_capture_runs_with_cpu_exhausted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cpu_held = anyio.Event()
    release_cpu = anyio.Event()
    waiting_cpu = anyio.Event()
    legacy_done = anyio.Event()
    original_run_cpu = httpx_io.run_cpu

    async def observe_cpu(function, *args: object):
        waiting_cpu.set()
        return await original_run_cpu(function, *args)

    monkeypatch.setattr(httpx_io, "run_cpu", observe_cpu)
    acquirer = DirectHttpxAcquirer(
        httpx.MockTransport(lambda _request: httpx.Response(200, stream=_Stream(b"body")))
    )

    async def occupy_cpu() -> None:
        manager = current_resource_manager()
        assert manager is not None
        async with manager.admit(ResourceRequest(name="cpu")):
            cpu_held.set()
            await release_cpu.wait()

    async def legacy() -> None:
        result = await acquirer.acquire(RequestPlan(url="https://offline.test/"), _identifier)
        assert result.bodies[0].content == b"body"
        legacy_done.set()

    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"http": 1, "cpu": 1}
    ):
        with anyio.fail_after(5):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(occupy_cpu)
                await cpu_held.wait()
                tasks.start_soon(legacy)
                await waiting_cpu.wait()
                try:
                    result = await acquirer.capture(
                        RequestPlan(url="https://offline.test/another"), _identifier
                    )
                    assert result.bodies[0].content == b"body"
                    assert not legacy_done.is_set()
                finally:
                    release_cpu.set()
    assert legacy_done.is_set()


async def test_routed_capture_rejects_collectors_without_capture() -> None:
    class LegacyCollector:
        async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory):
            del plan, new_identifier
            pytest.fail("Capture must not fall back to decoded acquisition")

    routed = RoutedHttpAcquirer({("legacy",): LegacyCollector()})
    with pytest.raises(RouteConfigurationFailure) as raised:
        await routed.capture(
            RequestPlan(url="https://offline.test/", routing=("legacy",)), _identifier
        )
    assert raised.value.code == "network_route_does_not_support_capture"


@pytest.mark.parametrize("encoding", ("gzip", "deflate", "br", "zstd"))
async def test_explicit_decode_budget_rejects_expansion(encoding: str) -> None:
    import zlib

    import brotli
    import zstandard

    content = b"expansion" * 100
    encoded = {
        "gzip": gzip.compress,
        "deflate": zlib.compress,
        "br": brotli.compress,
        "zstd": zstandard.ZstdCompressor().compress,
    }[encoding](content)
    with pytest.raises(ContentDecodingError):
        decode_content(encoded, (encoding,), maximum_bytes=20)
    assert decode_content(encoded, (encoding,), maximum_bytes=len(content)) == content
