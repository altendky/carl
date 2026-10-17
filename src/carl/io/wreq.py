"""Browser-profiled HTTP acquisition using wreq's blocking client."""

from __future__ import annotations

import errno
import re
import socket
import ssl
from collections.abc import AsyncGenerator, Iterable, Iterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from importlib.metadata import version
from time import perf_counter_ns
from typing import Literal, Protocol, cast, final
from urllib.parse import urlsplit

import anyio
import wreq
import wreq.blocking
import wreq.exceptions

from carl.core.http import RequestPlan, header_values, redirect_target
from carl.core.models import Header, JsonValue, StrictModel
from carl.core.routing import LocalSocks5Endpoint
from carl.io.cleanup import report_cleanup_failure
from carl.io.httpx import (
    AcquiredBody,
    Acquisition,
    AcquisitionFailure,
    IdentifierFactory,
    RouteConfigurationFailure,
)

_DEFAULT_COMPRESSION = ("gzip", "deflate", "br", "zstd")
_PROTECTED_REQUEST_HEADERS = frozenset({b"cookie", b"proxy-authorization", b"x-pl-config"})
_PROTECTED_RESPONSE_HEADERS = frozenset({b"set-cookie", b"proxy-authenticate", b"proxy-status"})
_WREQ_EXCEPTIONS = (
    wreq.exceptions.RustPanic,
    wreq.exceptions.TlsError,
    wreq.exceptions.ConnectionError,
    wreq.exceptions.ProxyConnectionError,
    wreq.exceptions.ConnectionResetError,
    wreq.exceptions.BodyError,
    wreq.exceptions.BuilderError,
    wreq.exceptions.DecodingError,
    wreq.exceptions.StatusError,
    wreq.exceptions.RequestError,
    wreq.exceptions.RedirectError,
    wreq.exceptions.UpgradeError,
    wreq.exceptions.WebSocketError,
    wreq.exceptions.TimeoutError,
)
_DIAGNOSTIC_EXCEPTION_TYPES = (
    *_WREQ_EXCEPTIONS,
    OSError,
    ConnectionError,
    ConnectionRefusedError,
    ConnectionResetError,
    ConnectionAbortedError,
    BrokenPipeError,
    TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    socket.gaierror,
    ssl.SSLError,
    ssl.SSLCertVerificationError,
)
_TYPE_REASON_CODES: dict[type[BaseException], str] = {
    wreq.exceptions.TlsError: "tls_failure",
    wreq.exceptions.ConnectionError: "connection_failure",
    wreq.exceptions.ProxyConnectionError: "proxy_connection_failure",
    wreq.exceptions.ConnectionResetError: "connection_reset",
    wreq.exceptions.TimeoutError: "timeout",
    wreq.exceptions.BodyError: "body_transfer_failure",
    wreq.exceptions.DecodingError: "content_decoding_failure",
    ConnectionRefusedError: "connection_refused",
    ConnectionResetError: "connection_reset",
    ConnectionAbortedError: "connection_aborted",
    BrokenPipeError: "broken_pipe",
    TimeoutError: "timeout",
    socket.gaierror: "dns_failure",
    ssl.SSLError: "tls_failure",
    ssl.SSLCertVerificationError: "tls_failure",
}
_ERRNO_REASON_CODES = {
    errno.ECONNREFUSED: "connection_refused",
    errno.ECONNRESET: "connection_reset",
    errno.ECONNABORTED: "connection_aborted",
    errno.ETIMEDOUT: "timeout",
    errno.EHOSTUNREACH: "host_unreachable",
    errno.ENETUNREACH: "network_unreachable",
    errno.EPIPE: "broken_pipe",
}
_MESSAGE_REASON_PATTERNS = (
    (
        "dns_failure",
        re.compile(
            r"\b(?:dns error|dns resolution failed|failed to lookup address information|name or service not known|temporary failure in name resolution)\b"
        ),
    ),
    ("tcp_connect_failure", re.compile(r"\btcp connect error\b")),
    (
        "proxy_connect_failure",
        re.compile(
            r"\b(?:proxy connect(?:ion)? (?:error|failed|failure)|unsuccessful tunnel|tunnel error|failed to establish (?:a )?(?:proxy )?tunnel|kind:\s*proxyconnect)\b"
        ),
    ),
    (
        "proxy_authentication_required",
        re.compile(r"\b(?:proxyauthrequired|proxy authentication required)\b"),
    ),
    (
        "tls_failure",
        re.compile(
            r"\b(?:tls handshake (?:error|failed|failure)|ssl handshake (?:error|failed|failure)|certificate verify failed|certificate verification failed|certificate_verify_failed|invalid peer certificate)\b|\bcode:\s*ssl\s*\("
        ),
    ),
    (
        "certificate_verification_failure",
        re.compile(
            r"\b(?:certificate verify failed|certificate verification failed|certificate_verify_failed|invalid peer certificate)\b"
        ),
    ),
    (
        "http2_failure",
        re.compile(
            r"\b(?:http2 (?:error|connection error|protocol error)|http/2 (?:error|connection error|protocol error)|h2::error|(?:netty|hyper)::error\(http2|goaway received|received goaway|stream error received)\b"
        ),
    ),
    (
        "http2_stream_reset",
        re.compile(r"\bkind:\s*reset\(streamid\("),
    ),
    ("connection_reset", re.compile(r"\bconnection reset(?: by peer)?\b")),
    ("connection_refused", re.compile(r"\bconnection refused\b")),
    ("connection_aborted", re.compile(r"\bconnection aborted\b")),
    ("broken_pipe", re.compile(r"\b(?:brokenpipe|broken pipe)\b")),
    ("connection_closed", re.compile(r"\bconnection closed before message completed\b")),
    ("host_unreachable", re.compile(r"\b(?:host is unreachable|no route to host)\b")),
    ("network_unreachable", re.compile(r"\bnetwork is unreachable\b")),
    (
        "timeout",
        re.compile(
            r"\b(?:operation timed out|connection timed out|request timed out|deadline (?:has )?(?:elapsed|exceeded)|timeout error)\b"
        ),
    ),
)
_MESSAGE_OS_ERROR = re.compile(r"\b(?:os error ([0-9]{1,4})|os \{\s*code:\s*([0-9]{1,4}))\b")
_MESSAGE_URL = re.compile(r"\b(?:https?|socks5h?)://\S+", re.IGNORECASE)


def _exception_label(error: BaseException) -> str:
    return type(error).__name__ if type(error) in _DIAGNOSTIC_EXCEPTION_TYPES else "OtherError"


def _transport_error_diagnostics(error: BaseException) -> dict[str, JsonValue]:
    """Retain only fixed labels and validated numbers from a bounded error chain.

    The binding offers exception classes and formatted native messages, not a
    structured Rust source chain. Message matches are therefore evidence hints,
    never definitive attribution to an endpoint, proxy, or remote server.
    """
    chain: list[JsonValue] = []
    evidence: list[JsonValue] = []
    reason_codes: set[str] = set()
    seen: set[int] = set()
    current: BaseException | None = error
    inspected_messages = False
    message_truncated = False
    binding_omitted_sources = False

    def record(reason: str, source: str, index: int) -> None:
        reason_codes.add(reason)
        evidence.append({"reason_code": reason, "source": source, "chain_index": index})

    while current is not None and len(chain) < 8 and id(current) not in seen:
        seen.add(id(current))
        index = len(chain)
        entry: dict[str, JsonValue] = {"exception_type": _exception_label(current)}
        reason = _TYPE_REASON_CODES.get(type(current))
        if reason is not None:
            record(reason, "exception_type", index)
        if (
            isinstance(current, OSError)
            and type(current.errno) is int
            and current.errno in errno.errorcode
        ):
            entry["errno"] = current.errno
            if (reason := _ERRNO_REASON_CODES.get(current.errno)) is not None:
                record(reason, "os_errno", index)
        # Inspect only native binding messages. Arbitrary application exception
        # text can describe URLs, cookies or cleanup failures rather than I/O.
        if type(current) in _WREQ_EXCEPTIONS:
            inspected_messages = True
            message = str(current)
            message_truncated = message_truncated or len(message) > 16384
            binding_omitted_sources = binding_omitted_sources or (
                re.fullmatch(
                    r"(?:is_[a-z_]+ error|error): The Python interpreter is not available",
                    message[:16384],
                )
                is not None
            )
            message = _MESSAGE_URL.sub("", message[:16384]).lower()
            for reason, pattern in _MESSAGE_REASON_PATTERNS:
                if pattern.search(message) is not None:
                    record(reason, "native_message_pattern", index)
            numbers: set[int] = set()
            for match in _MESSAGE_OS_ERROR.finditer(message):
                number = int(match[1] or match[2])
                if number in errno.errorcode:
                    numbers.add(number)
                if len(numbers) >= 4:
                    break
            if numbers:
                entry["message_os_error_numbers"] = sorted(numbers)
                for number in sorted(numbers):
                    if (reason := _ERRNO_REASON_CODES.get(number)) is not None:
                        record(reason, "native_message_os_error_number", index)
        chain.append(entry)
        current = current.__cause__ or (
            current.__context__ if not current.__suppress_context__ else None
        )
    return {
        "reason_codes": sorted(reason_codes) or ["unknown"],
        "reason_evidence": evidence,
        "exception_chain": chain,
        "chain_truncated": current is not None,
        "native_messages_inspected": inspected_messages,
        "native_source_details": "omitted_by_binding"
        if binding_omitted_sources
        else "formatted_message_only"
        if inspected_messages
        else "not_applicable",
        "message_scan_truncated": message_truncated,
        "message_evidence_limit": "fixed_patterns_only; raw_text_not_retained; matches_are_inferences_not_proven_root_causes",
    }


class WreqTransportSettings(StrictModel):
    """Nonsecret identity and behavior of the profiled HTTP engine."""

    implementation: Literal["wreq"] = "wreq"
    implementation_version: str = version("wreq")
    emulation_profile: Literal["Chrome153", "Chrome154"] = "Chrome154"
    emulation_platform: Literal["profile_default", "Linux"] = "Linux"
    profile_default_headers: Literal[True] = True
    execution_mode: Literal["blocking_worker_thread"] = "blocking_worker_thread"
    redirect_handling: Literal["carl_manual"] = "carl_manual"
    cookie_store: Literal[True] = True
    tls_certificate_verification: Literal["enabled"] = "enabled"
    tls_trust_store: Literal["wreq_default"] = "wreq_default"
    configured_http_versions: tuple[Literal["http2", "http1"], ...] = ("http2", "http1")
    automatic_content_decoding: Literal[True] = True

    def safe_configuration(
        self, plan: RequestPlan, *, shared_client: bool = False
    ) -> dict[str, JsonValue]:
        result = self.model_dump(mode="json")
        result["profile_default_headers"] = plan.profile_default_headers
        result["effective_timeouts"] = {
            "total_seconds": plan.timeout.connect_seconds + plan.timeout.read_seconds,
            "connect_seconds": plan.timeout.connect_seconds,
            "read_seconds": plan.timeout.read_seconds,
            "write_seconds": {
                "state": "not_applicable",
                "reason": "get_without_request_body",
            },
            "pool_seconds": {
                "state": "not_applicable",
                "reason": "one_active_client_per_managed_session"
                if shared_client and plan.reconnect_on_broken_pipe
                else "one_client_per_managed_session"
                if shared_client
                else "one_client_per_acquisition",
            },
        }
        result["compression"] = {
            "accepted": list(plan.compression),
            "header_source": "emulation_profile"
            if plan.profile_default_headers
            else "caller_supplied",
            "decoded_before_python_body_exposure": True,
        }
        return result


class _StatusCode(Protocol):
    def as_int(self) -> int: ...


class _WreqStream(Protocol):
    def __enter__(self) -> Iterator[object]: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None: ...


class _WreqResponse(Protocol):
    url: str
    status: _StatusCode
    version: wreq.Version
    headers: Iterable[tuple[bytes | memoryview, bytes | memoryview]]

    def stream(self) -> _WreqStream: ...

    def __enter__(self) -> _WreqResponse: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None: ...

    def close(self) -> None: ...


class _WreqClient(Protocol):
    def get(self, url: str, **kwargs: object) -> _WreqResponse: ...

    def close(self) -> None: ...


class WreqClientFactory(Protocol):
    def __call__(
        self,
        *,
        proxy: wreq.Proxy,
        settings: WreqTransportSettings,
        plan: RequestPlan,
    ) -> _WreqClient: ...


class WreqProxyFactory(Protocol):
    def __call__(self) -> wreq.Proxy: ...


def _default_client_factory(
    *,
    proxy: wreq.Proxy,
    settings: WreqTransportSettings,
    plan: RequestPlan,
) -> _WreqClient:
    total_seconds = plan.timeout.connect_seconds + plan.timeout.read_seconds
    profile = {
        "Chrome153": wreq.emulation.Profile.Chrome153,
        "Chrome154": wreq.emulation.Profile.Chrome154,
    }[settings.emulation_profile]
    emulation = (
        wreq.Emulation(profile=profile, platform=wreq.emulation.Platform.Linux)
        if settings.emulation_platform == "Linux"
        else wreq.Emulation(profile=profile)
    )
    client = wreq.blocking.Client(
        emulation=emulation,
        redirect=wreq.Policy.none(),
        raise_for_status=False,
        cookie_store=True,
        timeout=timedelta(seconds=total_seconds),
        connect_timeout=timedelta(seconds=plan.timeout.connect_seconds),
        read_timeout=timedelta(seconds=plan.timeout.read_seconds),
        https_only=True,
        tls_verify=True,
        proxies=[proxy],
    )
    return cast(_WreqClient, cast(object, client))


def _headers(raw: Iterable[tuple[bytes | memoryview, bytes | memoryview]]) -> tuple[Header, ...]:
    return tuple(Header(name=bytes(name), value=bytes(value)) for name, value in raw)


def _header_evidence(
    headers: tuple[Header, ...], protected_names: frozenset[bytes]
) -> list[dict[str, JsonValue]]:
    evidence: list[dict[str, JsonValue]] = []
    for header in headers:
        if header.name.lower() in protected_names:
            evidence.append(
                {
                    "name_latin1": header.name.decode("latin-1"),
                    "value": {
                        "state": "redacted",
                        "reason": "protected_session_or_credential_material",
                        "bytes": len(header.value),
                    },
                }
            )
        else:
            evidence.append(header.as_json())
    return evidence


def _content_type(headers: tuple[Header, ...]) -> str | None:
    values = header_values(headers, b"content-type")
    return values[-1].decode("latin-1") if values else None


def _http_version(value: wreq.Version) -> str:
    names = (
        (wreq.Version.HTTP_09, "HTTP/0.9"),
        (wreq.Version.HTTP_10, "HTTP/1.0"),
        (wreq.Version.HTTP_11, "HTTP/1.1"),
        (wreq.Version.HTTP_2, "HTTP/2"),
        (wreq.Version.HTTP_3, "HTTP/3"),
    )
    return next((name for candidate, name in names if value == candidate), str(value))


def _reason_phrase(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase
    except ValueError:
        return ""


def _supplied_headers(plan: RequestPlan) -> tuple[wreq.HeaderMap | None, tuple[str, ...]]:
    if not plan.headers:
        return None, ()
    header_map = wreq.HeaderMap()
    ordered_names: list[str] = []
    for header in plan.headers:
        name = header.name.decode("latin-1")
        header_map.append(name, header.value.decode("latin-1"))
        ordered_names.append(name)
    return header_map, tuple(ordered_names)


_CLIENT_HINT_TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9a-z-]{1,128}$")
_LINUX_CLIENT_HINTS = frozenset({"sec-ch-ua-model", "sec-ch-ua-platform-version"})
type _Origin = tuple[str, str, int]


def _origin(url: str) -> _Origin | None:
    try:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            return None
        return parsed.scheme, parsed.hostname.lower(), parsed.port or 443
    except ValueError:
        return None


class _ClientHints:
    """Response-negotiated hints for one client lifetime, isolated by origin.

    wreq exposes reduced user-agent versions rather than an actual full browser
    version. Only the observed Linux empty model/platform-version values are
    supported; a full-version hint is deliberately not fabricated.
    """

    def __init__(self) -> None:
        self.requested: dict[_Origin, frozenset[str]] = {}

    def observe(self, url: str, headers: tuple[Header, ...]) -> None:
        origin = _origin(url)
        values = header_values(headers, b"accept-ch")
        if origin is None or not values:
            return
        combined = b",".join(values).decode("latin-1").strip()
        if not combined:
            self.requested[origin] = frozenset()
            return
        names = tuple(name.strip().lower() for name in combined.split(","))
        if len(combined) > 16384 or any(
            _CLIENT_HINT_TOKEN.fullmatch(name) is None for name in names
        ):
            return
        self.requested[origin] = frozenset(names) & _LINUX_CLIENT_HINTS

    def headers(self, url: str, settings: WreqTransportSettings) -> tuple[Header, ...]:
        origin = _origin(url)
        if settings.emulation_platform != "Linux" or origin is None:
            return ()
        return tuple(
            Header(name=name.encode("ascii"), value=b'""')
            for name in sorted(self.requested.get(origin, ()))
        )


def _request_evidence(
    plan: RequestPlan,
    url: str,
    started_at_utc: str,
    settings: WreqTransportSettings,
    negotiated_headers: tuple[Header, ...] = (),
) -> dict[str, JsonValue]:
    return {
        "method": plan.method,
        "url": url,
        "headers": _header_evidence(
            (*plan.headers, *negotiated_headers), _PROTECTED_REQUEST_HEADERS
        ),
        "header_evidence_scope": "caller_supplied_and_negotiated_client_hints"
        if negotiated_headers
        else "caller_supplied_only",
        **(
            {
                "negotiated_client_hint_names": [
                    header.name.decode("ascii") for header in negotiated_headers
                ]
            }
            if negotiated_headers
            else {}
        ),
        **({"requested_header_order": list(plan.header_order)} if plan.header_order else {}),
        "profile_generated_headers": {
            "state": "not_observed" if plan.profile_default_headers else "disabled",
            "generator": {
                "implementation": "wreq",
                "profile": settings.emulation_profile,
                "platform": settings.emulation_platform,
                "default_headers": plan.profile_default_headers,
            },
        },
        "started_at_utc": started_at_utc,
        "ended_at_utc": datetime.now(UTC).isoformat(),
        "body": None,
    }


@dataclass(frozen=True, slots=True)
class _SyncHop:
    request: dict[str, JsonValue]
    response: dict[str, JsonValue]
    content: bytes | None = None
    media_type: str | None = None
    content_encoding_headers: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _SyncResult:
    hops: tuple[_SyncHop, ...]
    stopping_condition: str
    connection_recovery: _ConnectionRecovery | None = None


@final
class _SyncAcquisitionFailure(Exception):
    def __init__(
        self,
        message: str,
        *,
        hops: tuple[_SyncHop, ...],
        result: dict[str, JsonValue],
        completed_hops_before_request: int | None = None,
        connection_recovery: _ConnectionRecovery | None = None,
    ):
        super().__init__(message)
        self.hops = hops
        self.result = result
        self.completed_hops_before_request = completed_hops_before_request
        self.connection_recovery = connection_recovery


@dataclass(frozen=True, slots=True)
class _ConnectionRecovery:
    attempted: bool
    recovered: bool
    reason: str
    first_failure: _SyncAcquisitionFailure


def _response_metadata(
    response: _WreqResponse,
    *,
    duration_ns: int,
    headers: tuple[Header, ...],
    trailers: tuple[Header, ...] = (),
) -> dict[str, JsonValue]:
    status_code = response.status.as_int()
    metadata: dict[str, JsonValue] = {
        "status_code": status_code,
        "reason_phrase": _reason_phrase(status_code),
        "reason_phrase_source": "iana_status_registry",
        "url": response.url,
        "http_version": _http_version(response.version),
        "headers": _header_evidence(headers, _PROTECTED_RESPONSE_HEADERS),
        "server_date_headers": [
            value.decode("latin-1") for value in header_values(headers, b"date")
        ],
        "duration_ns": duration_ns,
    }
    if trailers:
        metadata["trailers"] = _header_evidence(trailers, _PROTECTED_RESPONSE_HEADERS)
    return metadata


def _close_response(response: _WreqResponse | None) -> None:
    if response is not None:
        response.close()


def _sync_acquire(
    *,
    plan: RequestPlan,
    proxy_factory: WreqProxyFactory,
    settings: WreqTransportSettings,
    client_factory: WreqClientFactory,
    client: _WreqClient | None = None,
    client_hints: _ClientHints | None = None,
    cookie_provider: wreq.Jar | None = None,
    visited_urls: frozenset[str] = frozenset(),
) -> _SyncResult:
    client_hints = client_hints if client_hints is not None else _ClientHints()
    owns_client = client is None
    if client is None:
        client = client_factory(proxy=proxy_factory(), settings=settings, plan=plan)
    hops: list[_SyncHop] = []
    current_url = plan.url
    seen_urls = set(visited_urls)
    failure: _SyncAcquisitionFailure | None = None
    result: _SyncResult | None = None
    try:
        while True:
            if current_url in seen_urls:
                raise _SyncAcquisitionFailure(
                    "Redirect loop",
                    hops=tuple(hops),
                    result={"stopping_condition": "redirect_loop"},
                )
            seen_urls.add(current_url)
            completed_hops_before_request = len(hops)
            header_map, ordered_header_names = _supplied_headers(plan)
            supplied_names = {header.name.lower() for header in plan.headers}
            negotiated_headers = tuple(
                header
                for header in client_hints.headers(current_url, settings)
                if header.name.lower() not in supplied_names
            )
            if negotiated_headers:
                header_map = header_map if header_map is not None else wreq.HeaderMap()
                for header in negotiated_headers:
                    header_map.append(header.name.decode("ascii"), header.value.decode("ascii"))
            request_options: dict[str, object] = {"default_headers": plan.profile_default_headers}
            if cookie_provider is not None:
                request_options["cookie_provider"] = cookie_provider
            if header_map is not None:
                request_options["headers"] = header_map
            effective_order = plan.header_order or ordered_header_names
            if effective_order:
                request_options["orig_headers"] = list(effective_order)
            started_at_utc = datetime.now(UTC).isoformat()
            started = perf_counter_ns()
            response: _WreqResponse | None = None
            response_headers: tuple[Header, ...] = ()
            failure_phase = "request"
            try:
                response = client.get(current_url, **request_options)
                response_headers = _headers(response.headers)
                client_hints.observe(response.url, response_headers)
                failure_phase = "response_body"
                body = bytearray()
                trailer_headers: list[Header] = []
                with response.stream() as stream:
                    for chunk in stream:
                        if isinstance(chunk, (bytes, memoryview)):
                            if len(body) + len(chunk) > plan.max_body_bytes:
                                metadata = _response_metadata(
                                    response,
                                    duration_ns=perf_counter_ns() - started,
                                    headers=response_headers,
                                )
                                metadata["body"] = {
                                    "state": "unavailable",
                                    "reason": "size_limit_exceeded",
                                    "measured_representation": "wreq_content_decoded",
                                }
                                hops.append(
                                    _SyncHop(
                                        request=_request_evidence(
                                            plan,
                                            current_url,
                                            started_at_utc,
                                            settings,
                                            negotiated_headers,
                                        ),
                                        response=metadata,
                                    )
                                )
                                raise _SyncAcquisitionFailure(
                                    "Response body exceeded configured limit",
                                    hops=tuple(hops),
                                    result={"stopping_condition": "body_size_limit"},
                                )
                            body.extend(chunk)
                        else:
                            trailer_headers.extend(
                                _headers(
                                    cast(
                                        Iterable[tuple[bytes | memoryview, bytes | memoryview]],
                                        chunk,
                                    )
                                )
                            )
            except _SyncAcquisitionFailure:
                with suppress(Exception):
                    _close_response(response)
                raise
            except _WREQ_EXCEPTIONS as error:
                if response is not None:
                    metadata = _response_metadata(
                        response,
                        duration_ns=perf_counter_ns() - started,
                        headers=response_headers,
                    )
                    metadata["body"] = {
                        "state": "unavailable",
                        "reason": "incomplete_transfer",
                    }
                    hops.append(
                        _SyncHop(
                            request=_request_evidence(
                                plan, current_url, started_at_utc, settings, negotiated_headers
                            ),
                            response=metadata,
                        )
                    )
                with suppress(Exception):
                    _close_response(response)
                raise _SyncAcquisitionFailure(
                    "HTTP transport failed before a complete body was received",
                    hops=tuple(hops),
                    result={
                        "stopping_condition": "transport_failure",
                        "exception_type": _exception_label(error),
                        "failure_phase": failure_phase,
                        "transport_error": _transport_error_diagnostics(error),
                        "failed_request": {"method": plan.method, "url": current_url},
                    },
                    completed_hops_before_request=completed_hops_before_request,
                ) from error
            duration_ns = perf_counter_ns() - started
            metadata = _response_metadata(
                response,
                duration_ns=duration_ns,
                headers=response_headers,
                trailers=tuple(trailer_headers),
            )
            content_encoding_headers = tuple(
                value.decode("latin-1")
                for value in header_values(response_headers, b"content-encoding")
            )
            hops.append(
                _SyncHop(
                    request=_request_evidence(
                        plan, current_url, started_at_utc, settings, negotiated_headers
                    ),
                    response=metadata,
                    content=bytes(body),
                    media_type=_content_type(response_headers),
                    content_encoding_headers=content_encoding_headers,
                )
            )
            try:
                target = redirect_target(
                    status_code=response.status.as_int(),
                    headers=response_headers,
                    response_url=response.url,
                )
                metadata["redirect_target"] = target
            except ValueError as error:
                with suppress(Exception):
                    _close_response(response)
                raise _SyncAcquisitionFailure(
                    "Response redirect rejected by request policy",
                    hops=tuple(hops),
                    result={"stopping_condition": "invalid_redirect"},
                ) from error
            try:
                # close() forbids connection reuse even after a complete read.
                # Context release returns the fully read connection to the pool.
                with response:
                    pass
            except Exception as error:
                with suppress(Exception):
                    _close_response(response)
                raise _SyncAcquisitionFailure(
                    "HTTP transport failed while closing a response",
                    hops=tuple(hops),
                    result={
                        "stopping_condition": "transport_failure",
                        "exception_type": _exception_label(error),
                        "failure_phase": "response_close",
                        "transport_error": _transport_error_diagnostics(error),
                        "failed_request": {"method": plan.method, "url": current_url},
                    },
                ) from error
            if target is None or not plan.follow_redirects:
                stopping_condition = (
                    "terminal_response" if target is None else "redirect_following_disabled"
                )
                result = _SyncResult(hops=tuple(hops), stopping_condition=stopping_condition)
                break
            if len(hops) > plan.max_redirects:
                raise _SyncAcquisitionFailure(
                    "Redirect limit exceeded",
                    hops=tuple(hops),
                    result={"stopping_condition": "redirect_limit"},
                )
            current_url = target
    except _SyncAcquisitionFailure as error:
        failure = error
    finally:
        try:
            if owns_client:
                client.close()
        except Exception as error:
            if failure is None:
                failure = _SyncAcquisitionFailure(
                    "HTTP transport failed while closing the wreq client",
                    hops=tuple(hops),
                    result={
                        "stopping_condition": "transport_failure",
                        "exception_type": _exception_label(error),
                        "failure_phase": "client_close",
                        "transport_error": _transport_error_diagnostics(error),
                    },
                )
    if failure is not None:
        raise failure
    if result is None:
        raise AssertionError("wreq acquisition ended without a result")
    return result


def _materialize_hops(
    hops: tuple[_SyncHop, ...],
    new_identifier: IdentifierFactory,
    settings: WreqTransportSettings,
) -> tuple[list[dict[str, JsonValue]], tuple[AcquiredBody, ...]]:
    evidence: list[dict[str, JsonValue]] = []
    bodies: list[AcquiredBody] = []
    for index, hop in enumerate(hops):
        response = dict(hop.response)
        if hop.content is not None:
            identifier = new_identifier()
            response["body"] = {
                "state": "available",
                "artifact_id": identifier,
                "bytes": len(hop.content),
                "received_content_bytes": {
                    "state": "unavailable",
                    "reason": "wreq_decodes_content_before_body_exposure",
                },
            }
            bodies.append(
                AcquiredBody(
                    identifier=identifier,
                    media_type=hop.media_type,
                    representation={
                        "kind": "content_decoded_http_body",
                        "http_hop_index": index,
                        "transfer_framing_removed": True,
                        "content_decoded": True,
                        "content_decoder": {
                            "implementation": settings.implementation,
                            "implementation_version": settings.implementation_version,
                        },
                        "exact_wire_bytes": False,
                        "content_encoding_headers": list(hop.content_encoding_headers),
                        "received_content_bytes": {
                            "state": "unavailable",
                            "reason": "wreq_decodes_content_before_body_exposure",
                        },
                        "storage_compression": {"kind": "none"},
                    },
                    content=hop.content,
                )
            )
        evidence.append({"request": hop.request, "response": response})
    return evidence, tuple(bodies)


def _materialize_recovery(
    recovery: _ConnectionRecovery,
    new_identifier: IdentifierFactory,
    settings: WreqTransportSettings,
) -> tuple[dict[str, JsonValue], tuple[AcquiredBody, ...]]:
    first = recovery.first_failure
    prefix_count = first.completed_hops_before_request or 0
    failed_hops, bodies = _materialize_hops(first.hops[prefix_count:], new_identifier, settings)
    return {
        "attempted": recovery.attempted,
        "recovered": recovery.recovered,
        "reason": recovery.reason,
        "first_failure": {
            **first.result,
            "redirect_prefix_hop_indexes": list(range(prefix_count)),
            "hops": failed_hops,
        },
    }, bodies


@final
class ProxyWreqAcquirer:
    """Acquire bounded GET documents through an explicitly constructed proxy."""

    def __init__(
        self,
        *,
        proxy_factory: WreqProxyFactory,
        expected_routing: tuple[str, ...],
        routing_observation: JsonValue,
        authentication: JsonValue = "anonymous",
        settings: WreqTransportSettings | None = None,
        client_factory: WreqClientFactory = _default_client_factory,
    ):
        self.proxy_factory = proxy_factory
        self.expected_routing = expected_routing
        self.routing_observation = routing_observation
        self.authentication = authentication
        self.settings = settings or WreqTransportSettings()
        self.client_factory = client_factory
        self._managed_session = False
        self._client: _WreqClient | None = None
        self._client_proxy: wreq.Proxy | None = None
        self._cookie_jar: wreq.Jar | None = None
        self._client_plan: RequestPlan | None = None
        self._client_hints = _ClientHints()
        self._closed = False
        self._failed = False
        self._lock = anyio.Lock()

    @property
    def cookie_jar(self) -> wreq.Jar | None:
        """Canonical native cookie provider, without reading cookie values."""
        return self._cookie_jar

    def _sync_open_session_client(self, plan: RequestPlan) -> None:
        if self._client_proxy is None:
            self._client_proxy = self.proxy_factory()
        self._client = self.client_factory(
            proxy=self._client_proxy, settings=self.settings, plan=plan
        )
        if self._cookie_jar is None:
            jar = getattr(self._client, "cookie_jar", None)
            if isinstance(jar, wreq.Jar):
                self._cookie_jar = jar
        self._client_plan = plan

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[ProxyWreqAcquirer]:
        """Own one lazy cookie-preserving client across a bounded series of requests."""
        session = ProxyWreqAcquirer(
            proxy_factory=self.proxy_factory,
            expected_routing=self.expected_routing,
            routing_observation=self.routing_observation,
            authentication=self.authentication,
            settings=self.settings,
            client_factory=self.client_factory,
        )
        session._managed_session = True
        primary_error: BaseException | None = None
        try:
            yield session
        except BaseException as error:
            primary_error = error
            raise
        finally:
            with anyio.CancelScope(shield=True):
                async with session._lock:
                    session._closed = True
                    client, session._client = session._client, None
                    if client is not None:
                        try:
                            await anyio.to_thread.run_sync(client.close, abandon_on_cancel=False)
                        except Exception as error:
                            report_cleanup_failure(primary_error, error, "wreq_client_close")
                            if primary_error is None:
                                raise AcquisitionFailure(
                                    "HTTP transport failed while closing the wreq session",
                                    result={
                                        "hops": [],
                                        "stopping_condition": "transport_failure",
                                        "failure_phase": "client_close",
                                        "exception_type": _exception_label(error),
                                        "transport_error": _transport_error_diagnostics(error),
                                    },
                                ) from None

    def _sync_session_acquire(self, plan: RequestPlan) -> _SyncResult:
        if self._managed_session and self._client is None:
            self._sync_open_session_client(plan)
        try:
            return _sync_acquire(
                plan=plan,
                proxy_factory=self.proxy_factory,
                settings=self.settings,
                client_factory=self.client_factory,
                client=self._client,
                client_hints=self._client_hints if self._managed_session else None,
                cookie_provider=self._cookie_jar,
            )
        except _SyncAcquisitionFailure as first:
            diagnostics = first.result.get("transport_error")
            eligible = (
                self._managed_session
                and plan.reconnect_on_broken_pipe
                and plan.method == "GET"
                and first.result.get("stopping_condition") == "transport_failure"
                and first.result.get("failure_phase") in {"request", "response_body"}
                and isinstance(diagnostics, dict)
                and "broken_pipe" in diagnostics.get("reason_codes", [])
                and first.completed_hops_before_request is not None
            )
            if not eligible:
                raise
            prefix_count = first.completed_hops_before_request
            assert prefix_count is not None
            if any(hop.response.get("status_code") == 403 for hop in first.hops[prefix_count:]):
                raise
            if self._cookie_jar is None:
                first.connection_recovery = _ConnectionRecovery(
                    False, False, "native_cookie_jar_unavailable", first
                )
                raise
            return self._sync_reconnect(plan, first, prefix_count)

    def _sync_reconnect(
        self, plan: RequestPlan, first: _SyncAcquisitionFailure, prefix_count: int
    ) -> _SyncResult:
        failed_request = first.result["failed_request"]
        assert isinstance(failed_request, dict) and isinstance(failed_request.get("url"), str)
        prefix = first.hops[:prefix_count]
        resume_plan = plan.model_copy(
            update={
                "url": failed_request["url"],
                "max_redirects": plan.max_redirects - prefix_count,
            }
        )
        recovery = _ConnectionRecovery(True, False, "reconnect_failed", first)
        phase = "client_close"
        try:
            assert self._client is not None
            self._client.close()
            self._client = None
            phase = "client_open"
            self._sync_open_session_client(resume_plan)
        except Exception as error:
            raise _SyncAcquisitionFailure(
                "HTTP transport failed while replacing a broken-pipe connection",
                hops=prefix,
                result={
                    "stopping_condition": "transport_failure",
                    "failure_phase": phase,
                    "exception_type": _exception_label(error),
                    "transport_error": _transport_error_diagnostics(error),
                },
                connection_recovery=recovery,
            ) from error
        try:
            resumed = _sync_acquire(
                plan=resume_plan,
                proxy_factory=self.proxy_factory,
                settings=self.settings,
                client_factory=self.client_factory,
                client=self._client,
                client_hints=self._client_hints,
                cookie_provider=self._cookie_jar,
                visited_urls=frozenset(cast(str, hop.request["url"]) for hop in prefix),
            )
        except _SyncAcquisitionFailure as final:
            raise _SyncAcquisitionFailure(
                str(final),
                hops=(*prefix, *final.hops),
                result=final.result,
                connection_recovery=recovery,
            ) from final
        except _WREQ_EXCEPTIONS as error:
            raise _SyncAcquisitionFailure(
                "HTTP transport failed while preparing the reconnect request",
                hops=prefix,
                result={
                    "stopping_condition": "transport_failure",
                    "failure_phase": "request_preparation",
                    "exception_type": _exception_label(error),
                    "transport_error": _transport_error_diagnostics(error),
                },
                connection_recovery=recovery,
            ) from error
        return _SyncResult(
            hops=(*prefix, *resumed.hops),
            stopping_condition=resumed.stopping_condition,
            connection_recovery=_ConnectionRecovery(True, True, "broken_pipe", first),
        )

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        async with self._lock:
            if self._closed or self._failed:
                raise RouteConfigurationFailure("wreq_session_not_available")
            if self._client_plan is not None and self._client_plan.timeout != plan.timeout:
                raise ValueError("A shared wreq client requires consistent request timeouts")
            try:
                return await self._acquire(plan, new_identifier)
            except BaseException:
                if self._managed_session:
                    self._failed = True
                raise

    async def _acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        if plan.routing != self.expected_routing:
            raise RouteConfigurationFailure("request_route_does_not_match_acquirer")
        if plan.method != "GET":
            raise ValueError("wreq acquisition currently supports GET only")
        if plan.compression != _DEFAULT_COMPRESSION:
            raise ValueError("wreq acquisition requires coherent profile compression defaults")
        try:
            sync_result = await anyio.to_thread.run_sync(
                lambda: self._sync_session_acquire(plan),
                abandon_on_cancel=False,
            )
        except _SyncAcquisitionFailure as error:
            hops, bodies = _materialize_hops(error.hops, new_identifier, self.settings)
            result = {"hops": hops, **error.result}
            if error.connection_recovery is not None:
                recovery, recovery_bodies = _materialize_recovery(
                    error.connection_recovery, new_identifier, self.settings
                )
                result["connection_recovery"] = recovery
                bodies = (*bodies, *recovery_bodies)
            result["transport"] = self.settings.safe_configuration(
                plan, shared_client=self._managed_session
            )
            result["routing"] = {
                "configured": list(plan.routing),
                "observed": self.routing_observation,
            }
            raise AcquisitionFailure(str(error), result=result, bodies=bodies) from error
        except _WREQ_EXCEPTIONS as error:
            raise AcquisitionFailure(
                "HTTP transport failed while opening the wreq client",
                result={
                    "hops": [],
                    "stopping_condition": "transport_failure",
                    "exception_type": _exception_label(error),
                    "failure_phase": "client_open",
                    "transport_error": _transport_error_diagnostics(error),
                    "transport": self.settings.safe_configuration(
                        plan, shared_client=self._managed_session
                    ),
                    "routing": {
                        "configured": list(plan.routing),
                        "observed": self.routing_observation,
                    },
                },
            ) from error

        hops, bodies = _materialize_hops(sync_result.hops, new_identifier, self.settings)
        recovery_record: dict[str, JsonValue] = {}
        if sync_result.connection_recovery is not None:
            recovery, recovery_bodies = _materialize_recovery(
                sync_result.connection_recovery, new_identifier, self.settings
            )
            recovery_record["connection_recovery"] = recovery
            bodies = (*bodies, *recovery_bodies)
        final_response = hops[-1]["response"]
        if not isinstance(final_response, dict):
            raise AssertionError("Response metadata must be an object")
        return Acquisition(
            record={
                **recovery_record,
                "request_plan": plan.as_json(protected_header_names=_PROTECTED_REQUEST_HEADERS),
                "authentication": self.authentication,
                "transport": self.settings.safe_configuration(
                    plan, shared_client=self._managed_session
                ),
                "routing": {
                    "configured": list(plan.routing),
                    "observed": self.routing_observation,
                },
                "hops": hops,
                "redirect_count": max(0, len(hops) - 1),
                "effective_url": final_response["url"],
                "stopping_condition": sync_result.stopping_condition,
                "collection_completeness": "complete",
            },
            bodies=bodies,
        )


@final
class LocalSocks5WreqAcquirer:
    """Acquire bounded anonymous GET documents through one local SOCKS5 route."""

    def __init__(
        self,
        *,
        endpoint: LocalSocks5Endpoint,
        expected_routing: tuple[str, ...],
        routing_observation: JsonValue,
        settings: WreqTransportSettings | None = None,
        client_factory: WreqClientFactory = _default_client_factory,
    ):
        self.acquirer = ProxyWreqAcquirer(
            proxy_factory=lambda: wreq.Proxy.all(endpoint.url),
            expected_routing=expected_routing,
            routing_observation=routing_observation,
            settings=settings,
            client_factory=client_factory,
        )

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        return await self.acquirer.acquire(plan, new_identifier)
