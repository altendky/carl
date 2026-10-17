import errno
import json
import socket
from pathlib import Path
from typing import cast

import pytest
import wreq
import wreq.exceptions

from carl._tests.test_ebay_workers import _directories
from carl._tests.test_wreq import _acquirer, _Factory, _response
from carl.core.ebay import CollectEbaySearchPayload, EbaySearchRequest
from carl.core.http import RequestPlan
from carl.core.models import JsonValue
from carl.core.worker import AttemptContext, RetryWork, TerminalFailureWork
from carl.ebay_workers import EbaySearchWorkerDependencies, build_ebay_worker_registry
from carl.io.httpx import AcquisitionFailure
from carl.io.sqlite import Database
from carl.io.wreq import WreqClientFactory, _transport_error_diagnostics

_SECRETS = (
    "secret-proxy-user:secret-proxy-password",
    "secret-proxy-user%3Asecret-proxy-password",
    "c2VjcmV0LXByb3h5LXVzZXI6c2VjcmV0LXByb3h5LXBhc3N3b3Jk",
    "secret-cookie-value",
    "203.0.113.44",
    "private.example.invalid",
)
_PRIVATE_DETAIL = (
    "url https://secret-proxy-user:secret-proxy-password@private.example.invalid/ "
    + " ".join(_SECRETS[1:])
)


@pytest.mark.parametrize(
    ("message", "expected"),
    (
        ('ConnectError("dns error", "failed to lookup address information")', "dns_failure"),
        ('ConnectError("tcp connect error")', "tcp_connect_failure"),
        ("proxy connect error: unsuccessful tunnel", "proxy_connect_failure"),
        ("TLS handshake failed: certificate verify failed", "tls_failure"),
        ("hyper::Error(Http2, h2::Error)", "http2_failure"),
        (f"connection reset by peer (os error {errno.ECONNRESET})", "connection_reset"),
        ("deadline has elapsed", "timeout"),
        ("error sending request", "unknown"),
    ),
)
def test_native_request_details_are_fixed_codes_without_private_message_text(
    message: str, expected: str
) -> None:
    diagnostics = _transport_error_diagnostics(
        wreq.exceptions.RequestError(f"{message}; {_PRIVATE_DETAIL}")
    )
    assert expected in diagnostics["reason_codes"]
    assert diagnostics["exception_chain"] == (
        [{"exception_type": "RequestError", "message_os_error_numbers": [errno.ECONNRESET]}]
        if f"os error {errno.ECONNRESET}" in message
        else [{"exception_type": "RequestError"}]
    )
    assert diagnostics["native_messages_inspected"] is True
    assert "matches_are_inferences_not_proven_root_causes" in diagnostics["message_evidence_limit"]
    serialized = json.dumps(diagnostics)
    assert all(secret not in serialized for secret in _SECRETS)
    assert message not in serialized


@pytest.mark.parametrize(
    ("message", "expected"),
    (
        (
            "netty::Error(Http2, Error { kind: Reset(StreamId(1), PROTOCOL_ERROR, Remote) })",
            {"http2_failure", "http2_stream_reset"},
        ),
        (
            'netty::Error(Io, Custom { kind: BrokenPipe, error: "stream closed because of a broken pipe" })',
            {"broken_pipe"},
        ),
        (
            "netty::Error(Body, Error { kind: Reset(StreamId(1), PROTOCOL_ERROR, Library) })",
            {"http2_stream_reset"},
        ),
        (
            'code: SSL (1), reason: "CERTIFICATE_VERIFY_FAILED"',
            {"tls_failure", "certificate_verification_failure"},
        ),
        (
            "kind: ProxyConnect, source: Some(ProxyAuthRequired)",
            {"proxy_connect_failure", "proxy_authentication_required"},
        ),
    ),
)
def test_observed_native_debug_shapes_are_recognized_without_assuming_fixture_cause(
    message: str, expected: set[str]
) -> None:
    diagnostics = _transport_error_diagnostics(wreq.exceptions.RequestError(message))
    assert set(diagnostics["reason_codes"]) == expected
    assert {entry["source"] for entry in diagnostics["reason_evidence"]} == {
        "native_message_pattern"
    }
    assert message not in json.dumps(diagnostics)


def test_diagnostics_use_structured_sources_and_bound_exception_chains() -> None:
    error = wreq.exceptions.RequestError(_PRIVATE_DETAIL)
    error.__cause__ = ConnectionRefusedError(errno.ECONNREFUSED, _PRIVATE_DETAIL)
    diagnostics = _transport_error_diagnostics(error)
    assert diagnostics["reason_codes"] == ["connection_refused"]
    assert diagnostics["exception_chain"] == [
        {"exception_type": "RequestError"},
        {"exception_type": "ConnectionRefusedError", "errno": errno.ECONNREFUSED},
    ]
    assert {entry["source"] for entry in diagnostics["reason_evidence"]} == {
        "exception_type",
        "os_errno",
    }

    root = current = RuntimeError(_PRIVATE_DETAIL)
    for _ in range(12):
        current.__cause__ = RuntimeError(_PRIVATE_DETAIL)
        current = current.__cause__
    bounded = _transport_error_diagnostics(root)
    assert len(bounded["exception_chain"]) == 8
    assert bounded["chain_truncated"] is True
    current.__cause__ = root
    assert _transport_error_diagnostics(current)["chain_truncated"] is True
    assert all(secret not in json.dumps(diagnostics) for secret in _SECRETS)


def test_diagnostics_do_not_record_arbitrary_class_names_or_unvalidated_errno() -> None:
    private_type = type("secret_cookie_value", (Exception,), {})
    error = private_type(_PRIVATE_DETAIL)
    error.__cause__ = OSError(123456789, _PRIVATE_DETAIL)
    diagnostics = _transport_error_diagnostics(error)
    assert diagnostics["reason_codes"] == ["unknown"]
    assert diagnostics["exception_chain"] == [
        {"exception_type": "OtherError"},
        {"exception_type": "OSError"},
    ]
    assert "secret_cookie_value" not in json.dumps(diagnostics)
    assert _transport_error_diagnostics(socket.gaierror(-2, _PRIVATE_DETAIL))["reason_codes"] == [
        "dns_failure"
    ]


def test_diagnostics_do_not_infer_from_url_query_and_report_message_scan_limits() -> None:
    diagnostics = _transport_error_diagnostics(
        wreq.exceptions.RequestError(
            "error sending request for url (https://private.example.invalid/?error=connection-reset&more=timeout-error)"
        )
    )
    assert diagnostics["reason_codes"] == ["unknown"]
    long = _transport_error_diagnostics(
        wreq.exceptions.RequestError("x" * 17000 + " connection reset by peer")
    )
    assert long["message_scan_truncated"] is True
    assert long["reason_codes"] == ["unknown"]


def test_binding_omitted_source_message_is_reported_without_inventing_a_network_cause() -> None:
    diagnostics = _transport_error_diagnostics(
        wreq.exceptions.RequestError("is_request error: The Python interpreter is not available")
    )
    assert diagnostics["native_source_details"] == "omitted_by_binding"
    assert diagnostics["reason_codes"] == ["unknown"]
    assert diagnostics["reason_evidence"] == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "phase",
    ("request", "response_body", "response_close", "client_close", "client_open", "session_close"),
)
async def test_transport_error_details_retain_phase_without_private_values(phase: str) -> None:
    url = "https://www.ebay.com/"
    error = wreq.exceptions.RequestError(f"connection reset by peer; {_PRIVATE_DETAIL}")
    response = _response(url=url)
    factory = _Factory([error if phase == "request" else response])
    acquirer = _acquirer(factory).acquirer
    if phase == "response_body":
        response.chunks = (b"incomplete", error)
    elif phase == "response_close":
        response.release_error = error
    elif phase in {"client_close", "session_close"}:
        factory.client.close_error = error
    elif phase == "client_open":

        def fail_open(**_kwargs: object) -> object:
            raise error

        acquirer.client_factory = cast(WreqClientFactory, fail_open)
    plan = RequestPlan(url=url, routing=("proton", "personal", "carl"))
    with pytest.raises(AcquisitionFailure) as raised:
        if phase == "session_close":
            async with acquirer.session() as session:
                await session.acquire(plan, lambda: "body")
        else:
            await acquirer.acquire(plan, lambda: "body")
    result = raised.value.result
    assert result["stopping_condition"] == "transport_failure"
    assert result["failure_phase"] == ("client_close" if phase == "session_close" else phase)
    assert result["exception_type"] == "RequestError"
    assert result["transport_error"]["reason_codes"] == ["connection_reset"]
    if phase == "request":
        assert result["hops"] == []
    elif phase == "response_body":
        assert result["hops"][0]["response"]["body"]["reason"] == "incomplete_transfer"
        assert raised.value.bodies == ()
    serialized = json.dumps(result)
    assert all(secret not in serialized for secret in _SECRETS)
    assert all(secret not in str(raised.value) for secret in _SECRETS)


@pytest.mark.anyio
@pytest.mark.parametrize("attempt", (1, 3))
async def test_detailed_request_error_keeps_existing_worker_retry_budget(attempt: int) -> None:
    factory = _Factory([wreq.exceptions.RequestError(f"deadline has elapsed; {_PRIVATE_DETAIL}")])
    with pytest.raises(AcquisitionFailure) as raised:
        await _acquirer(factory).acquire(
            RequestPlan(url="https://www.ebay.com/", routing=("proton", "personal", "carl")),
            lambda: "unused",
        )
    failure = raised.value

    async def collect(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
        raise failure

    class CheckpointDatabase:
        async def publish_leased_operation_checkpoint(self, **_kwargs: object) -> None:
            pass

    registry = build_ebay_worker_registry(
        EbaySearchWorkerDependencies(
            cast(Database, cast(object, CheckpointDatabase())),
            _directories(Path("/unused")),
            lambda: "unused",
            collector=collect,
        )
    )
    outcome = await registry.handlers[0].execute(
        CollectEbaySearchPayload(request=EbaySearchRequest(query="scope")).as_json(),
        AttemptContext(
            work_item_identifier="work",
            lease_token="lease",
            worker_identifier="worker",
            attempt=attempt,
            operation_identifier="operation",
        ),
    )
    assert isinstance(outcome, RetryWork if attempt == 1 else TerminalFailureWork)
    reason = outcome.reason if isinstance(outcome, RetryWork) else outcome.error
    assert reason["maximum_attempts"] == 3
    assert "cooldown" not in reason
    if isinstance(outcome, RetryWork):
        assert outcome.delay_ns == 1_000_000_000
    assert outcome.result["acquisition"] == failure.result
    assert all(secret not in json.dumps(outcome.result) for secret in _SECRETS)
