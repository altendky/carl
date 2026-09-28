import json

import httpcore
import httpx
import pytest
from pydantic import SecretStr, ValidationError

from carl.core.http import FormField, RequestPlan, TimeoutPlan
from carl.core.json import decode_json
from carl.core.models import Header
from carl.io.httpx import (
    AcquisitionFailure,
    ClientHttpxAcquirer,
    DirectHttpxAcquirer,
    RouteConfigurationFailure,
    RoutedHttpAcquirer,
)


class _AsyncBytesStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes):
        self.content = content

    async def __aiter__(self):
        yield self.content


class _ProtocolErrorOnCloseStream(_AsyncBytesStream):
    async def aclose(self) -> None:
        raise httpcore.ProtocolError("synthetic close failure")


def test_request_plan_has_typed_json_serialization() -> None:
    plan = RequestPlan(
        url="https://www.facebook.com/marketplace/item/123/",
        headers=(Header(name=b"X-Source-Bytes", value=b"opaque-\xff"),),
        timeout=TimeoutPlan(
            connect_seconds=1.0,
            read_seconds=2.0,
            write_seconds=3.0,
            pool_seconds=4.0,
        ),
        routing=("bright_data", "account-1"),
    )

    serialized = decode_json(plan.model_dump_json(by_alias=True))

    assert serialized["headers"] == [
        {"name_latin1": "X-Source-Bytes", "value_latin1": "opaque-\u00ff"}
    ]
    assert serialized["timeout"] == {
        "connect_seconds": 1.0,
        "read_seconds": 2.0,
        "write_seconds": 3.0,
        "pool_seconds": 4.0,
    }
    assert serialized["routing"] == ["bright_data", "account-1"]
    assert plan.as_json() == serialized
    assert RequestPlan.model_validate_json(plan.model_dump_json(by_alias=True)) == plan


@pytest.mark.parametrize(
    "change",
    [
        {"url": "https://user:secret@example.com/"},
        {"headers": (Header(name=b"Cookie", value=b"secret"),)},
        {"routing": ()},
    ],
)
def test_request_plan_rejects_unsafe_or_invalid_settings(change: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        RequestPlan.model_validate({"url": "https://example.com/", **change})


def test_timeout_plan_rejects_nonpositive_values() -> None:
    with pytest.raises(ValidationError):
        TimeoutPlan(connect_seconds=0.0)


@pytest.mark.anyio
async def test_form_post_executes_values_but_redacts_protected_evidence() -> None:
    received_content = b""

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal received_content
        received_content = request.content
        return httpx.Response(200, stream=_AsyncBytesStream(b"{}"))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
        follow_redirects=False,
    ) as client:
        acquirer = ClientHttpxAcquirer(
            client=client,
            expected_routing=("proton", "test"),
            routing_observation={"state": "test"},
            authentication="anonymous",
            protected_request_headers=frozenset({b"cookie", b"x-fb-lsd"}),
            protected_response_headers=frozenset({b"set-cookie"}),
        )
        result = await acquirer.acquire_form(
            RequestPlan(
                url="https://www.facebook.com/api/graphql/",
                method="POST",
                follow_redirects=False,
                routing=("proton", "test"),
            ),
            (
                FormField(name="doc_id", value=SecretStr("123"), protected=False),
                FormField(name="lsd", value=SecretStr("secret-token"), protected=True),
            ),
            lambda: "body",
        )

    assert received_content == b"doc_id=123&lsd=secret-token"
    serialized = json.dumps(result.record)
    assert "secret-token" not in serialized
    fields = result.record["hops"][0]["request"]["body"]["fields"]
    assert fields[0] == {"name": "doc_id", "value": "123"}
    assert fields[1]["name"] == "lsd"
    assert fields[1]["value"]["state"] == "redacted"


@pytest.mark.anyio
async def test_response_close_protocol_error_is_a_transport_failure() -> None:
    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_ProtocolErrorOnCloseStream(b"{}"))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=False
    ) as client:
        acquirer = ClientHttpxAcquirer(
            client=client,
            expected_routing=("direct", "test"),
            routing_observation={"state": "test"},
            authentication="anonymous",
        )
        with pytest.raises(AcquisitionFailure) as raised:
            await acquirer.acquire(
                RequestPlan(url="https://example.com/", routing=("direct", "test")),
                lambda: "body",
            )

    assert raised.value.result["stopping_condition"] == "transport_failure"
    assert raised.value.result["exception_type"] == "ProtocolError"
    assert raised.value.result["failed_request"] == {
        "method": "GET",
        "url": "https://example.com/",
    }


@pytest.mark.anyio
async def test_routed_acquirer_has_no_implicit_fallback() -> None:
    direct = DirectHttpxAcquirer(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, content=b"direct"))
    )
    acquirer = RoutedHttpAcquirer({("direct", "explicit-control"): direct})
    plan = RequestPlan(url="https://example.com/", routing=("mullvad", "missing"))

    with pytest.raises(RouteConfigurationFailure) as raised:
        await acquirer.acquire(plan, lambda: "artifact")

    assert raised.value.code == "unconfigured_network_route"
