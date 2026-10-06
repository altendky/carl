"""Browser-profiled HTTP acquisition using wreq's blocking client."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Iterable, Iterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from importlib.metadata import version
from time import perf_counter_ns
from typing import Literal, Protocol, cast, final

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
_PROTECTED_REQUEST_HEADERS = frozenset({b"cookie", b"proxy-authorization"})
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


class WreqTransportSettings(StrictModel):
    """Nonsecret identity and behavior of the profiled HTTP engine."""

    implementation: Literal["wreq"] = "wreq"
    implementation_version: str = version("wreq")
    emulation_profile: Literal["Chrome153"] = "Chrome153"
    emulation_platform: Literal["profile_default"] = "profile_default"
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
                "reason": "one_client_per_managed_session"
                if shared_client
                else "one_client_per_acquisition",
            },
        }
        result["compression"] = {
            "accepted": list(plan.compression),
            "header_source": "emulation_profile",
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
    headers: Iterable[tuple[bytes, bytes]]

    def stream(self) -> _WreqStream: ...

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
    emulation = {"Chrome153": wreq.Emulation.Chrome153}[settings.emulation_profile]
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


def _headers(raw: Iterable[tuple[bytes, bytes]]) -> tuple[Header, ...]:
    return tuple(Header(name=name, value=value) for name, value in raw)


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


def _request_evidence(plan: RequestPlan, url: str, started_at_utc: str) -> dict[str, JsonValue]:
    return {
        "method": plan.method,
        "url": url,
        "headers": _header_evidence(plan.headers, _PROTECTED_REQUEST_HEADERS),
        "header_evidence_scope": "caller_supplied_only",
        "profile_generated_headers": {
            "state": "not_observed",
            "generator": {
                "implementation": "wreq",
                "profile": "Chrome153",
                "default_headers": True,
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


@final
class _SyncAcquisitionFailure(Exception):
    def __init__(
        self,
        message: str,
        *,
        hops: tuple[_SyncHop, ...],
        result: dict[str, JsonValue],
    ):
        super().__init__(message)
        self.hops = hops
        self.result = result


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
) -> _SyncResult:
    owns_client = client is None
    if client is None:
        client = client_factory(proxy=proxy_factory(), settings=settings, plan=plan)
    hops: list[_SyncHop] = []
    current_url = plan.url
    seen_urls: set[str] = set()
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
            header_map, ordered_header_names = _supplied_headers(plan)
            request_options: dict[str, object] = {"default_headers": True}
            if header_map is not None:
                request_options["headers"] = header_map
                request_options["orig_headers"] = list(ordered_header_names)
            started_at_utc = datetime.now(UTC).isoformat()
            started = perf_counter_ns()
            response: _WreqResponse | None = None
            response_headers: tuple[Header, ...] = ()
            try:
                response = client.get(current_url, **request_options)
                response_headers = _headers(response.headers)
                body = bytearray()
                trailer_headers: list[Header] = []
                with response.stream() as stream:
                    for chunk in stream:
                        if isinstance(chunk, bytes):
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
                                            plan, current_url, started_at_utc
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
                                _headers(cast(Iterable[tuple[bytes, bytes]], chunk))
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
                            request=_request_evidence(plan, current_url, started_at_utc),
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
                        "exception_type": type(error).__name__,
                        "failed_request": {"method": plan.method, "url": current_url},
                    },
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
                    request=_request_evidence(plan, current_url, started_at_utc),
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
                _close_response(response)
            except Exception as error:
                raise _SyncAcquisitionFailure(
                    "HTTP transport failed while closing a response",
                    hops=tuple(hops),
                    result={
                        "stopping_condition": "transport_failure",
                        "exception_type": type(error).__name__,
                        "failure_phase": "response_close",
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
                        "exception_type": type(error).__name__,
                        "failure_phase": "client_close",
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
        self._client_plan: RequestPlan | None = None
        self._closed = False
        self._failed = False
        self._lock = anyio.Lock()

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
                                    },
                                ) from None

    def _sync_session_acquire(self, plan: RequestPlan) -> _SyncResult:
        if self._managed_session and self._client is None:
            self._client = self.client_factory(
                proxy=self.proxy_factory(), settings=self.settings, plan=plan
            )
            self._client_plan = plan
        return _sync_acquire(
            plan=plan,
            proxy_factory=self.proxy_factory,
            settings=self.settings,
            client_factory=self.client_factory,
            client=self._client,
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
                    "exception_type": type(error).__name__,
                    "failure_phase": "client_open",
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
        final_response = hops[-1]["response"]
        if not isinstance(final_response, dict):
            raise AssertionError("Response metadata must be an object")
        return Acquisition(
            record={
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
