"""Bounded HTTP acquisition using HTTPX raw response streams."""

from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter_ns
from typing import Protocol
from urllib.parse import urlencode

import anyio
import httpcore
import httpx

from carl.core.content_encoding import ContentDecodingError, decode_content
from carl.core.http import FormField, RequestPlan, header_values, redirect_target
from carl.core.models import Header, JsonValue
from carl.core.routing import LocalSocks5Endpoint


@dataclass(frozen=True, slots=True)
class AcquiredBody:
    identifier: str
    media_type: str | None
    representation: dict[str, JsonValue]
    content: bytes


@dataclass(frozen=True, slots=True)
class Acquisition:
    record: dict[str, JsonValue]
    bodies: tuple[AcquiredBody, ...]


class IdentifierFactory(Protocol):
    def __call__(self) -> str: ...


class HttpAcquirer(Protocol):
    async def acquire(
        self, plan: RequestPlan, new_identifier: IdentifierFactory
    ) -> Acquisition: ...


class HttpFormAcquirer(HttpAcquirer, Protocol):
    async def acquire_form(
        self,
        plan: RequestPlan,
        fields: tuple[FormField, ...],
        new_identifier: IdentifierFactory,
    ) -> Acquisition: ...


class AcquisitionFailure(Exception):
    def __init__(
        self,
        message: str,
        *,
        result: dict[str, JsonValue],
        bodies: tuple[AcquiredBody, ...] = (),
    ):
        super().__init__(message)
        self.result = result
        self.bodies = bodies


class RouteConfigurationFailure(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _headers(raw: list[tuple[bytes, bytes]]) -> tuple[Header, ...]:
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


def _response_metadata(
    response: httpx.Response,
    duration_ns: int,
    *,
    protected_headers: frozenset[bytes],
) -> dict[str, JsonValue]:
    headers = _headers(response.headers.raw)
    return {
        "status_code": response.status_code,
        "reason_phrase": response.reason_phrase,
        "url": str(response.url),
        "http_version": response.http_version,
        "headers": _header_evidence(headers, protected_headers),
        "server_date_headers": [
            value.decode("latin-1") for value in header_values(headers, b"date")
        ],
        "duration_ns": duration_ns,
    }


class _HttpxAcquirer:
    def __init__(
        self,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        proxy: str | httpx.Proxy | None = None,
        client: httpx.AsyncClient | None = None,
        expected_routing: tuple[str, ...] | None = None,
        routing_observation: JsonValue = None,
        authentication: JsonValue = "anonymous",
        protected_request_headers: frozenset[bytes] = frozenset(),
        protected_response_headers: frozenset[bytes] = frozenset(),
    ):
        if client is not None and (transport is not None or proxy is not None):
            raise ValueError("A supplied HTTPX client owns its transport configuration")
        self.transport = transport
        self.proxy = proxy
        self.client = client
        self.expected_routing = expected_routing
        self.routing_observation = routing_observation
        self.authentication = authentication
        self.protected_request_headers = protected_request_headers
        self.protected_response_headers = protected_response_headers

    @asynccontextmanager
    async def _open_client(self, timeout: httpx.Timeout):
        if self.client is not None:
            yield self.client
            return
        client = httpx.AsyncClient(
            http2=True,
            follow_redirects=False,
            timeout=timeout,
            trust_env=False,
            transport=self.transport,
            proxy=self.proxy,
        )
        try:
            yield client
        finally:
            with anyio.CancelScope(shield=True):
                await client.aclose()

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        if self.expected_routing is not None and plan.routing != self.expected_routing:
            raise RouteConfigurationFailure("request_route_does_not_match_acquirer")
        try:
            return await self._acquire(plan, new_identifier, form_fields=())
        except AcquisitionFailure as error:
            error.result["routing"] = {
                "configured": list(plan.routing),
                "observed": self.routing_observation,
            }
            raise

    async def acquire_form(
        self,
        plan: RequestPlan,
        fields: tuple[FormField, ...],
        new_identifier: IdentifierFactory,
    ) -> Acquisition:
        if plan.method != "POST":
            raise ValueError("Form acquisition requires POST")
        if self.expected_routing is not None and plan.routing != self.expected_routing:
            raise RouteConfigurationFailure("request_route_does_not_match_acquirer")
        try:
            return await self._acquire(plan, new_identifier, form_fields=fields)
        except AcquisitionFailure as error:
            error.result["routing"] = {
                "configured": list(plan.routing),
                "observed": self.routing_observation,
            }
            raise

    async def _acquire(
        self,
        plan: RequestPlan,
        new_identifier: IdentifierFactory,
        *,
        form_fields: tuple[FormField, ...],
    ) -> Acquisition:
        timeout = httpx.Timeout(
            connect=plan.timeout.connect_seconds,
            read=plan.timeout.read_seconds,
            write=plan.timeout.write_seconds,
            pool=plan.timeout.pool_seconds,
        )
        supplied_headers = [(header.name, header.value) for header in plan.headers]
        if not any(name.lower() == b"accept-encoding" for name, _ in supplied_headers):
            supplied_headers.append(
                (b"Accept-Encoding", ", ".join(plan.compression).encode("ascii"))
            )
        request_content: bytes | None = None
        request_body_evidence: dict[str, JsonValue] | None = None
        if form_fields:
            if not any(name.lower() == b"content-type" for name, _ in supplied_headers):
                supplied_headers.append((b"Content-Type", b"application/x-www-form-urlencoded"))
            request_content = urlencode(
                [(field.name, field.value.get_secret_value()) for field in form_fields]
            ).encode("ascii")
            request_body_evidence = {
                "kind": "application/x-www-form-urlencoded",
                "fields": [field.evidence() for field in form_fields],
                "bytes": len(request_content),
            }
        hops: list[dict[str, JsonValue]] = []
        bodies: list[AcquiredBody] = []
        seen_urls: set[str] = set()
        current_url = plan.url

        async with self._open_client(timeout) as client:
            while True:
                if current_url in seen_urls:
                    raise AcquisitionFailure(
                        "Redirect loop",
                        result={"hops": hops, "stopping_condition": "redirect_loop"},
                        bodies=tuple(bodies),
                    )
                seen_urls.add(current_url)
                request = client.build_request(
                    plan.method,
                    current_url,
                    headers=supplied_headers,
                    content=request_content,
                    timeout=timeout,
                )
                actual_headers = _headers(request.headers.raw)
                started_at_utc = datetime.now(UTC).isoformat()
                started = perf_counter_ns()
                incomplete_response: dict[str, JsonValue] | None = None
                response: httpx.Response | None = None
                try:
                    response = await client.send(request, stream=True, follow_redirects=False)
                    incomplete_response = _response_metadata(
                        response,
                        0,
                        protected_headers=self.protected_response_headers,
                    )
                    body = bytearray()
                    async for chunk in response.aiter_raw():
                        if len(body) + len(chunk) > plan.max_body_bytes:
                            metadata = _response_metadata(
                                response,
                                perf_counter_ns() - started,
                                protected_headers=self.protected_response_headers,
                            )
                            metadata["body"] = {
                                "state": "unavailable",
                                "reason": "size_limit_exceeded",
                            }
                            hops.append(
                                {
                                    "request": {
                                        "method": plan.method,
                                        "url": str(request.url),
                                        "headers": _header_evidence(
                                            actual_headers, self.protected_request_headers
                                        ),
                                        "started_at_utc": started_at_utc,
                                        "ended_at_utc": datetime.now(UTC).isoformat(),
                                        "body": request_body_evidence,
                                    },
                                    "response": metadata,
                                }
                            )
                            raise AcquisitionFailure(
                                "Response body exceeded configured limit",
                                result={
                                    "hops": hops,
                                    "stopping_condition": "body_size_limit",
                                },
                                bodies=tuple(bodies),
                            )
                        body.extend(chunk)
                except AcquisitionFailure:
                    raise
                except (
                    httpx.HTTPError,
                    httpcore.NetworkError,
                    httpcore.ProtocolError,
                    httpcore.ProxyError,
                    httpcore.TimeoutException,
                ) as error:
                    if incomplete_response is not None:
                        incomplete_response["duration_ns"] = perf_counter_ns() - started
                        incomplete_response["body"] = {
                            "state": "unavailable",
                            "reason": "incomplete_transfer",
                        }
                        hops.append(
                            {
                                "request": {
                                    "method": plan.method,
                                    "url": str(request.url),
                                    "headers": _header_evidence(
                                        actual_headers, self.protected_request_headers
                                    ),
                                    "started_at_utc": started_at_utc,
                                    "ended_at_utc": datetime.now(UTC).isoformat(),
                                    "body": request_body_evidence,
                                },
                                "response": incomplete_response,
                            }
                        )
                    raise AcquisitionFailure(
                        "HTTP transport failed before a complete body was received",
                        result={
                            "hops": hops,
                            "stopping_condition": "transport_failure",
                            "exception_type": type(error).__name__,
                            "failed_request": {
                                "method": plan.method,
                                "url": str(request.url),
                            },
                        },
                        bodies=tuple(bodies),
                    ) from error
                finally:
                    if response is not None:
                        try:
                            with anyio.CancelScope(shield=True):
                                await response.aclose()
                        except Exception as error:
                            raise AcquisitionFailure(
                                "HTTP transport failed while closing a response",
                                result={
                                    "hops": hops,
                                    "stopping_condition": "transport_failure",
                                    "exception_type": type(error).__name__,
                                    "failure_phase": "response_close",
                                    "failed_request": {
                                        "method": plan.method,
                                        "url": str(request.url),
                                    },
                                },
                                bodies=tuple(bodies),
                            ) from error

                duration_ns = perf_counter_ns() - started
                if response is None:
                    raise AssertionError("HTTPX returned no response")
                response_headers = _headers(response.headers.raw)
                content_encoding_headers = [
                    value.decode("latin-1")
                    for value in header_values(response_headers, b"content-encoding")
                ]
                content_encodings = tuple(
                    part.strip()
                    for value in content_encoding_headers
                    for part in value.split(",")
                    if part.strip()
                )
                metadata = _response_metadata(
                    response,
                    duration_ns,
                    protected_headers=self.protected_response_headers,
                )
                target = redirect_target(
                    status_code=response.status_code,
                    headers=response_headers,
                    response_url=str(response.url),
                )
                metadata["redirect_target"] = target
                try:
                    decoded_body = await anyio.to_thread.run_sync(
                        decode_content,
                        bytes(body),
                        content_encodings,
                        abandon_on_cancel=True,
                    )
                except ContentDecodingError as error:
                    metadata["body"] = {
                        "state": "unavailable",
                        "reason": "content_decoding_failure",
                        "received_bytes": len(body),
                        "content_encoding_headers": content_encoding_headers,
                    }
                    hops.append(
                        {
                            "request": {
                                "method": plan.method,
                                "url": str(request.url),
                                "headers": _header_evidence(
                                    actual_headers, self.protected_request_headers
                                ),
                                "started_at_utc": started_at_utc,
                                "ended_at_utc": datetime.now(UTC).isoformat(),
                                "body": request_body_evidence,
                            },
                            "response": metadata,
                        }
                    )
                    raise AcquisitionFailure(
                        "Response content encoding could not be decoded",
                        result={
                            "hops": hops,
                            "stopping_condition": "content_decoding_failure",
                            "decoding_error": str(error),
                        },
                        bodies=tuple(bodies),
                    ) from error
                body_identifier = new_identifier()
                metadata["body"] = {
                    "state": "available",
                    "artifact_id": body_identifier,
                    "bytes": len(decoded_body),
                    "received_content_bytes": len(body),
                }
                hop_index = len(hops)
                hops.append(
                    {
                        "request": {
                            "method": plan.method,
                            "url": str(request.url),
                            "headers": _header_evidence(
                                actual_headers, self.protected_request_headers
                            ),
                            "started_at_utc": started_at_utc,
                            "ended_at_utc": datetime.now(UTC).isoformat(),
                            "body": request_body_evidence,
                        },
                        "response": metadata,
                    }
                )
                bodies.append(
                    AcquiredBody(
                        identifier=body_identifier,
                        media_type=_content_type(response_headers),
                        representation={
                            "kind": "content_decoded_http_body",
                            "http_hop_index": hop_index,
                            "transfer_framing_removed": True,
                            "content_decoded": True,
                            "exact_wire_bytes": False,
                            "content_encoding_headers": content_encoding_headers,
                            "storage_compression": {"kind": "none"},
                        },
                        content=decoded_body,
                    )
                )
                if target is None or not plan.follow_redirects:
                    stopping_condition = (
                        "terminal_response" if target is None else "redirect_following_disabled"
                    )
                    break
                if len(hops) > plan.max_redirects:
                    raise AcquisitionFailure(
                        "Redirect limit exceeded",
                        result={"hops": hops, "stopping_condition": "redirect_limit"},
                        bodies=tuple(bodies),
                    )
                current_url = target

        final_response = hops[-1]["response"]
        if not isinstance(final_response, dict):
            raise AssertionError("Response metadata must be an object")
        record: dict[str, JsonValue] = {
            "request_plan": plan.as_json(protected_header_names=self.protected_request_headers),
            "authentication": self.authentication,
            "routing": {
                "configured": list(plan.routing),
                "observed": self.routing_observation,
            },
            "hops": hops,
            "redirect_count": max(0, len(hops) - 1),
            "effective_url": final_response["url"],
            "stopping_condition": stopping_condition,
            "collection_completeness": "complete",
        }
        return Acquisition(record=record, bodies=tuple(bodies))


class DirectHttpxAcquirer(_HttpxAcquirer):
    def __init__(self, transport: httpx.AsyncBaseTransport | None = None):
        super().__init__(transport=transport)


class LocalSocks5HttpxAcquirer(_HttpxAcquirer):
    def __init__(
        self,
        *,
        endpoint: LocalSocks5Endpoint,
        expected_routing: tuple[str, ...],
        routing_observation: JsonValue,
    ):
        super().__init__(
            proxy=endpoint.url,
            expected_routing=expected_routing,
            routing_observation=routing_observation,
        )


class ClientHttpxAcquirer(_HttpxAcquirer):
    """Use a caller-owned client so cookies and connections span acquisitions."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        expected_routing: tuple[str, ...],
        routing_observation: JsonValue,
        authentication: JsonValue,
        protected_request_headers: frozenset[bytes] = frozenset(),
        protected_response_headers: frozenset[bytes] = frozenset(),
    ):
        super().__init__(
            client=client,
            expected_routing=expected_routing,
            routing_observation=routing_observation,
            authentication=authentication,
            protected_request_headers=protected_request_headers,
            protected_response_headers=protected_response_headers,
        )


class RoutedHttpAcquirer:
    def __init__(self, routes: Mapping[tuple[str, ...], HttpAcquirer]):
        if not routes:
            raise ValueError("At least one HTTP route is required")
        if any(not route or any(not part for part in route) for route in routes):
            raise ValueError("Route identity parts must be nonempty")
        self.routes = dict(routes)

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        acquirer = self.routes.get(plan.routing)
        if acquirer is None:
            raise RouteConfigurationFailure("unconfigured_network_route")
        return await acquirer.acquire(plan, new_identifier)
