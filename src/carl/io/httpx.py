"""Bounded HTTP acquisition using HTTPX raw response streams."""

import sys
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from time import perf_counter_ns
from typing import Protocol, runtime_checkable
from urllib.parse import urlencode

import httpcore
import httpx

from carl.core.content_encoding import ContentDecodingError, decode_content
from carl.core.http import FormField, RequestPlan, header_values, redirect_target
from carl.core.models import Header, JsonValue
from carl.core.routing import LocalSocks5Endpoint
from carl.io.cleanup import shielded_cleanup
from carl.io.resource_permits import resource_operation, run_cpu

MAX_ACQUISITION_BODY_BYTES = 100_000_000
MAX_JSON_REQUEST_BYTES = 1024 * 1024
_JSON_PROTECTED_REQUEST_HEADERS = frozenset({b"authorization", b"cookie", b"proxy-authorization"})


async def close_httpx_client(
    client: httpx.AsyncClient, *, primary_error: BaseException | None = None
) -> bool:
    """Clear session material and close the transport even if either step fails."""
    cookie_error: BaseException | None = None
    transport_closed = False
    try:
        async with shielded_cleanup("http_client_cookies", primary_error=primary_error):
            client.cookies.clear()
    except BaseException as error:
        cookie_error = error
        raise
    finally:
        async with shielded_cleanup(
            "http_client_close", primary_error=primary_error or cookie_error
        ):
            await client.aclose()
            transport_closed = True
    return transport_closed


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


class _CompletionTrackingStream(httpx.AsyncByteStream):
    """Distinguish EOF-close errors from failures before all bytes arrived."""

    def __init__(self, stream: httpx.AsyncByteStream):
        self.stream = stream
        self.complete = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self.stream:
            yield chunk
        self.complete = True

    async def aclose(self) -> None:
        await self.stream.aclose()


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


@runtime_checkable
class HttpCaptureAcquirer(HttpAcquirer, Protocol):
    """Capture transport-exposed bodies with their actual decoding representation."""

    async def capture(
        self, plan: RequestPlan, new_identifier: IdentifierFactory
    ) -> Acquisition: ...


class HttpFormCaptureAcquirer(HttpCaptureAcquirer, HttpFormAcquirer, Protocol):
    """Capture form responses while preserving the same protected request evidence."""

    async def capture_form(
        self,
        plan: RequestPlan,
        fields: tuple[FormField, ...],
        new_identifier: IdentifierFactory,
    ) -> Acquisition: ...


@runtime_checkable
class HttpJsonCaptureAcquirer(HttpCaptureAcquirer, Protocol):
    """Capture raw responses to already validated, CPU-prepared JSON request bytes."""

    async def capture_json(
        self, plan: RequestPlan, content: bytes, new_identifier: IdentifierFactory
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


def completed_capture_after_close_failure(
    error: AcquisitionFailure,
    *,
    plan: RequestPlan,
) -> Acquisition | None:
    """Recover a complete stage capture without changing legacy failure contracts."""

    if error.result.get("stopping_condition") != "transport_failure":
        return None
    phase = error.result.get("failure_phase")
    if not isinstance(phase, str) or phase not in {
        "response_close",
        "client_close",
        "session_close",
    }:
        return None
    hops = error.result.get("hops")
    if not isinstance(hops, list) or not hops or not isinstance(hops[-1], dict):
        return None
    response = hops[-1].get("response")
    if not isinstance(response, dict):
        return None
    metadata = response.get("body")
    if not isinstance(metadata, dict) or metadata.get("state") != "available":
        return None
    retained = next(
        (body for body in error.bodies if body.identifier == metadata.get("artifact_id")), None
    )
    if (
        retained is None
        or type(metadata.get("bytes")) is not int
        or metadata["bytes"] != len(retained.content)
    ):
        return None
    status = response.get("status_code")
    if type(status) is not int or (plan.follow_redirects and 300 <= status < 400):
        return None
    url = response.get("url")
    if not isinstance(url, str):
        request = hops[-1].get("request")
        url = request.get("url") if isinstance(request, dict) else None
    if not isinstance(url, str):
        return None
    record = dict(error.result)
    cleanup: dict[str, JsonValue] = {"type": type(error).__name__, "failure_phase": phase}
    exception_type = record.pop("exception_type", None)
    if isinstance(exception_type, str):
        cleanup["exception_type"] = exception_type[:256]
    record.pop("failure_phase", None)
    record.update(
        {
            "effective_url": url,
            "redirect_count": len(hops) - 1,
            "collection_completeness": "complete",
            "session_cleanup_failure": cleanup,
            "stopping_condition": "redirect_following_disabled"
            if 300 <= status < 400
            else "terminal_response",
        }
    )
    return Acquisition(record=record, bodies=error.bodies)


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
            await close_httpx_client(client, primary_error=sys.exception())

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        return await self._execute(plan, new_identifier, form_fields=(), decode=True)

    async def capture(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        return await self._execute(plan, new_identifier, form_fields=(), decode=False)

    async def acquire_form(
        self,
        plan: RequestPlan,
        fields: tuple[FormField, ...],
        new_identifier: IdentifierFactory,
    ) -> Acquisition:
        if plan.method != "POST":
            raise ValueError("Form acquisition requires POST")
        return await self._execute(plan, new_identifier, form_fields=fields, decode=True)

    async def capture_form(
        self,
        plan: RequestPlan,
        fields: tuple[FormField, ...],
        new_identifier: IdentifierFactory,
    ) -> Acquisition:
        if plan.method != "POST":
            raise ValueError("Form acquisition requires POST")
        return await self._execute(plan, new_identifier, form_fields=fields, decode=False)

    async def capture_json(
        self, plan: RequestPlan, content: bytes, new_identifier: IdentifierFactory
    ) -> Acquisition:
        """Send bounded serialized JSON without parsing or retaining its values."""

        content_types = header_values(plan.headers, b"content-type")
        if plan.method != "POST":
            raise ValueError("JSON capture requires POST")
        if (
            len(content_types) != 1
            or content_types[0].partition(b";")[0].strip().lower() != b"application/json"
        ):
            raise ValueError("JSON capture requires one application/json Content-Type")
        if not isinstance(content, bytes) or not 0 < len(content) <= MAX_JSON_REQUEST_BYTES:
            raise ValueError(
                "JSON capture requires between one byte and 1 MiB of serialized content"
            )
        return await self._execute(
            plan, new_identifier, form_fields=(), decode=False, json_content=content
        )

    async def _execute(
        self,
        plan: RequestPlan,
        new_identifier: IdentifierFactory,
        *,
        form_fields: tuple[FormField, ...],
        decode: bool,
        json_content: bytes | None = None,
    ) -> Acquisition:
        if self.expected_routing is not None and plan.routing != self.expected_routing:
            raise RouteConfigurationFailure("request_route_does_not_match_acquirer")
        try:
            return await self._acquire(
                plan,
                new_identifier,
                form_fields=form_fields,
                decode=decode,
                json_content=json_content,
            )
        except AcquisitionFailure as error:
            error.result["routing"] = {
                "configured": list(plan.routing),
                "observed": self.routing_observation,
            }
            raise

    @resource_operation("http")
    async def _capture_hop(
        self,
        client: httpx.AsyncClient,
        request: httpx.Request,
        *,
        plan: RequestPlan,
        request_body_evidence: dict[str, JsonValue] | None,
        hops: list[dict[str, JsonValue]],
        bodies: list[AcquiredBody],
        maximum_bytes: int,
        new_identifier: IdentifierFactory,
    ) -> tuple[httpx.Response, bytes, int, str, bool]:
        request_protection = self.protected_request_headers
        if (
            request_body_evidence is not None
            and request_body_evidence.get("kind") == "application/json"
        ):
            request_protection |= _JSON_PROTECTED_REQUEST_HEADERS
        actual_headers = _headers(request.headers.raw)
        started_at_utc = datetime.now(UTC).isoformat()
        started = perf_counter_ns()
        incomplete_response: dict[str, JsonValue] | None = None
        response: httpx.Response | None = None
        stream: _CompletionTrackingStream | None = None
        body = bytearray()
        predecoded = False
        buffered_complete = False

        def close_failure(error: Exception) -> AcquisitionFailure:
            if response is not None and (
                buffered_complete or (stream is not None and stream.complete)
            ):
                headers = _headers(response.headers.raw)
                encodings = [
                    value.decode("latin-1") for value in header_values(headers, b"content-encoding")
                ]
                identifier = new_identifier()
                metadata = _response_metadata(
                    response,
                    perf_counter_ns() - started,
                    protected_headers=self.protected_response_headers,
                )
                metadata["body"] = {
                    "state": "available",
                    "artifact_id": identifier,
                    "bytes": len(body),
                    "received_content_bytes": len(body),
                }
                bodies.append(
                    AcquiredBody(
                        identifier=identifier,
                        media_type=_content_type(headers),
                        representation={
                            "kind": "content_decoded_http_body"
                            if predecoded
                            else "http_message_content",
                            "http_hop_index": len(hops),
                            "transfer_framing_removed": True,
                            "content_decoded": predecoded,
                            "exact_wire_bytes": False,
                            "content_encoding_headers": encodings,
                            "storage_compression": {"kind": "none"},
                        },
                        content=bytes(body),
                    )
                )
                hops.append(
                    {
                        "request": {
                            "method": plan.method,
                            "url": str(request.url),
                            "headers": _header_evidence(actual_headers, request_protection),
                            "started_at_utc": started_at_utc,
                            "ended_at_utc": datetime.now(UTC).isoformat(),
                            "body": request_body_evidence,
                        },
                        "response": metadata,
                    }
                )
            return AcquisitionFailure(
                "HTTP transport failed while closing a response",
                result={
                    "hops": hops,
                    "stopping_condition": "transport_failure",
                    "exception_type": type(error).__name__,
                    "failure_phase": "response_close",
                    "failed_request": {"method": plan.method, "url": str(request.url)},
                },
                bodies=tuple(bodies),
            )

        try:
            response = await client.send(request, stream=True, follow_redirects=False)
            predecoded = response.is_stream_consumed
            if not predecoded:
                if not isinstance(response.stream, httpx.AsyncByteStream):
                    raise RuntimeError("HTTPX returned a synchronous response stream")
                stream = _CompletionTrackingStream(response.stream)
                response.stream = stream
            incomplete_response = _response_metadata(
                response,
                0,
                protected_headers=self.protected_response_headers,
            )

            async def chunks() -> AsyncIterator[bytes]:
                if predecoded:
                    # Custom transports may return an already-read Response.
                    # HTTPX has decoded its content; original encoding headers
                    # remain evidence, not instructions to decode these bytes.
                    yield response.content
                else:
                    async for chunk in response.aiter_raw():
                        yield chunk

            async for chunk in chunks():
                if len(body) + len(chunk) > maximum_bytes:
                    metadata = _response_metadata(
                        response,
                        perf_counter_ns() - started,
                        protected_headers=self.protected_response_headers,
                    )
                    metadata["body"] = {
                        "state": "unavailable",
                        "reason": "size_limit_exceeded",
                        **(
                            {"limit_scope": "acquisition"}
                            if maximum_bytes < plan.max_body_bytes
                            else {}
                        ),
                    }
                    hops.append(
                        {
                            "request": {
                                "method": plan.method,
                                "url": str(request.url),
                                "headers": _header_evidence(actual_headers, request_protection),
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
            buffered_complete = predecoded
        except AcquisitionFailure:
            raise
        except (
            httpx.HTTPError,
            httpcore.NetworkError,
            httpcore.ProtocolError,
            httpcore.ProxyError,
            httpcore.TimeoutException,
        ) as error:
            if buffered_complete or (stream is not None and stream.complete):
                raise close_failure(error) from error
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
                            "headers": _header_evidence(actual_headers, request_protection),
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
        except Exception as error:
            if buffered_complete or (stream is not None and stream.complete):
                raise close_failure(error) from error
            raise
        finally:
            if response is not None:
                try:
                    async with shielded_cleanup(
                        "http_response_close", primary_error=sys.exception()
                    ):
                        await response.aclose()
                except Exception as error:
                    raise close_failure(error) from error

        if response is None:
            raise AssertionError("HTTPX returned no response")
        return response, bytes(body), perf_counter_ns() - started, started_at_utc, predecoded

    async def _acquire(
        self,
        plan: RequestPlan,
        new_identifier: IdentifierFactory,
        *,
        form_fields: tuple[FormField, ...],
        decode: bool,
        json_content: bytes | None = None,
    ) -> Acquisition:
        timeout = httpx.Timeout(
            connect=plan.timeout.connect_seconds,
            read=plan.timeout.read_seconds,
            write=plan.timeout.write_seconds,
            pool=plan.timeout.pool_seconds,
        )
        request_protection = self.protected_request_headers
        if json_content is not None:
            request_protection |= _JSON_PROTECTED_REQUEST_HEADERS
        supplied_headers = [(header.name, header.value) for header in plan.headers]
        if not any(name.lower() == b"accept-encoding" for name, _ in supplied_headers):
            supplied_headers.append(
                (b"Accept-Encoding", ", ".join(plan.compression).encode("ascii"))
            )
        request_content: bytes | None = None
        request_body_evidence: dict[str, JsonValue] | None = None
        if json_content is not None:
            if form_fields:
                raise ValueError("JSON and form request bodies are mutually exclusive")
            request_content = json_content
            request_body_evidence = {"kind": "application/json", "bytes": len(json_content)}
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
        total_bytes = 0
        total_received_bytes = 0
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
                response, body, duration_ns, started_at_utc, predecoded = await self._capture_hop(
                    client,
                    request,
                    plan=plan,
                    request_body_evidence=request_body_evidence,
                    hops=hops,
                    bodies=bodies,
                    maximum_bytes=min(
                        plan.max_body_bytes,
                        MAX_ACQUISITION_BODY_BYTES - total_received_bytes,
                        MAX_ACQUISITION_BODY_BYTES - total_bytes,
                    ),
                    new_identifier=new_identifier,
                )
                total_received_bytes += len(body)
                actual_headers = _headers(request.headers.raw)
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
                    decoded_body = (
                        await run_cpu(
                            partial(
                                decode_content,
                                body,
                                content_encodings,
                                maximum_bytes=MAX_ACQUISITION_BODY_BYTES - total_bytes,
                            )
                        )
                        if decode and not predecoded
                        else body
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
                                "headers": _header_evidence(actual_headers, request_protection),
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
                total_bytes += len(decoded_body)
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
                            "headers": _header_evidence(actual_headers, request_protection),
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
                            "kind": "content_decoded_http_body"
                            if decode or predecoded
                            else "http_message_content",
                            "http_hop_index": hop_index,
                            "transfer_framing_removed": True,
                            "content_decoded": decode or predecoded,
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
            "request_plan": plan.as_json(protected_header_names=request_protection),
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

    def retained_bytes(self) -> int:
        from carl.io.resident_memory import httpx_retained_bytes

        return 0 if self.client is None else httpx_retained_bytes(self.client)

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

    async def capture(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        acquirer = self.routes.get(plan.routing)
        if acquirer is None:
            raise RouteConfigurationFailure("unconfigured_network_route")
        if not isinstance(acquirer, HttpCaptureAcquirer):
            raise RouteConfigurationFailure("network_route_does_not_support_capture")
        return await acquirer.capture(plan, new_identifier)
