"""Pure request construction for Facebook Marketplace searches."""

import json
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from urllib.parse import urlencode, urlsplit

from pydantic import SecretStr

from carl.core.facebook_search_protocol import AppliedSearchConfiguration
from carl.core.facebook_search_session import (
    ProtectedSearchSessionMaterial,
    pagination_variables,
)
from carl.core.facebook_work import (
    FacebookSearchRequest,
    SearchDistanceUnit,
    SearchFacebookLocation,
)
from carl.core.http import FormField, RequestPlan
from carl.core.models import Header

_GRAPHQL_URL = "https://www.facebook.com/api/graphql/"
_MARKETPLACE_URL = "https://www.facebook.com/marketplace/"
_ROUTE_DEFINITION_URL = "https://www.facebook.com/ajax/route-definition/"


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _facebook_radius_kilometers(request: FacebookSearchRequest) -> int:
    if request.radius.unit is SearchDistanceUnit.KILOMETERS:
        return request.radius.value
    return int(
        (Decimal(request.radius.value) * Decimal("1.609344")).to_integral_value(
            rounding=ROUND_HALF_UP
        )
    )


def facebook_search_url(request: FacebookSearchRequest) -> str:
    if not isinstance(request.location, SearchFacebookLocation):
        raise ValueError("Facebook search requires a resolved Facebook location identifier")
    if request.price is not None and request.price.currency != "USD":
        raise ValueError("The initial Facebook search adapter supports USD prices only")
    parameters: list[tuple[str, str]] = [("query", request.query)]
    if request.price is not None:
        if request.price.minimum is not None:
            parameters.append(("minPrice", _decimal_text(request.price.minimum)))
        if request.price.maximum is not None:
            parameters.append(("maxPrice", _decimal_text(request.price.maximum)))
    parameters.extend(
        (
            ("exact", "true" if request.exact_match else "false"),
            ("radius", str(_facebook_radius_kilometers(request))),
        )
    )
    return (
        "https://www.facebook.com/marketplace/"
        f"{request.location.identifier}/search?{urlencode(parameters)}"
    )


def search_bootstrap_plan(
    request: FacebookSearchRequest,
    *,
    routing: tuple[str, ...],
    navigation_headers: tuple[Header, ...],
) -> RequestPlan:
    return RequestPlan(
        url=facebook_search_url(request),
        headers=navigation_headers,
        routing=routing,
    )


def search_session_bootstrap_plan(
    *,
    routing: tuple[str, ...],
    navigation_headers: tuple[Header, ...],
) -> RequestPlan:
    return RequestPlan(
        url=_MARKETPLACE_URL,
        headers=navigation_headers,
        routing=routing,
    )


@dataclass(frozen=True, slots=True)
class SearchPaginationRequest:
    plan: RequestPlan
    form_fields: tuple[FormField, ...]


@dataclass(frozen=True, slots=True)
class SearchRouteDefinitionRequest:
    plan: RequestPlan
    form_fields: tuple[FormField, ...]


def _header_value(headers: tuple[Header, ...], name: bytes) -> bytes | None:
    lowered = name.lower()
    for header in reversed(headers):
        if header.name.lower() == lowered:
            return header.value
    return None


def _graphql_headers(
    navigation_headers: tuple[Header, ...],
    *,
    referer: str,
    lsd: SecretStr,
) -> tuple[Header, ...]:
    copied_names = (
        b"user-agent",
        b"accept-language",
        b"sec-ch-ua",
        b"sec-ch-ua-mobile",
        b"sec-ch-ua-platform",
    )
    headers = [
        Header(name=name, value=value)
        for name in copied_names
        if (value := _header_value(navigation_headers, name)) is not None
    ]
    headers.extend(
        (
            Header(name=b"Accept", value=b"*/*"),
            Header(name=b"Origin", value=b"https://www.facebook.com"),
            Header(name=b"Referer", value=referer.encode("ascii")),
            Header(name=b"Sec-Fetch-Dest", value=b"empty"),
            Header(name=b"Sec-Fetch-Mode", value=b"cors"),
            Header(name=b"Sec-Fetch-Site", value=b"same-origin"),
            Header(name=b"X-FB-LSD", value=lsd.get_secret_value().encode("utf-8")),
        )
    )
    return tuple(headers)


def _route_definition_headers(
    navigation_headers: tuple[Header, ...],
) -> tuple[Header, ...]:
    copied_names = (
        b"user-agent",
        b"accept-language",
        b"sec-ch-ua",
        b"sec-ch-ua-mobile",
        b"sec-ch-ua-platform",
    )
    headers = [
        Header(name=name, value=value)
        for name in copied_names
        if (value := _header_value(navigation_headers, name)) is not None
    ]
    headers.extend(
        (
            Header(name=b"Accept", value=b"*/*"),
            Header(name=b"Referer", value=_MARKETPLACE_URL.encode("ascii")),
            Header(name=b"Sec-Fetch-Dest", value=b"empty"),
            Header(name=b"Sec-Fetch-Mode", value=b"cors"),
            Header(name=b"Sec-Fetch-Site", value=b"same-origin"),
        )
    )
    return tuple(headers)


def _base36(value: int) -> str:
    if value < 1:
        raise ValueError("Pagination request numbers start at one")
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    result = ""
    while value:
        value, remainder = divmod(value, 36)
        result = alphabet[remainder] + result
    return result


def search_route_definition_request(
    *,
    bootstrap_plan: RequestPlan,
    navigation_headers: tuple[Header, ...],
    session_material: ProtectedSearchSessionMaterial,
    request_number: int,
) -> SearchRouteDefinitionRequest:
    parsed = urlsplit(bootstrap_plan.url)
    route_url = parsed.path
    if parsed.query:
        route_url = f"{route_url}?{parsed.query}"
    fields = [
        FormField(name="__aaid", value=SecretStr("0")),
        FormField(name="__user", value=SecretStr("0")),
        FormField(name="__a", value=SecretStr("1")),
        FormField(name="__req", value=SecretStr(_base36(request_number))),
        FormField(name="__comet_req", value=SecretStr("15")),
    ]
    fields.extend(
        FormField(name=name, value=SecretStr(value), protected=True)
        for name, value in session_material.reveal_graphql_form_fields()
    )
    fields.extend(
        (
            FormField(name="client_previous_actor_id", value=SecretStr("")),
            FormField(name="dpr", value=SecretStr("1")),
            FormField(name="route_url", value=SecretStr(route_url)),
            FormField(name="routing_namespace", value=SecretStr("fb_comet")),
            FormField(name="trace_policy", value=SecretStr("comet.marketplace.search")),
        )
    )
    return SearchRouteDefinitionRequest(
        plan=RequestPlan(
            url=_ROUTE_DEFINITION_URL,
            method="POST",
            headers=_route_definition_headers(navigation_headers),
            follow_redirects=False,
            routing=bootstrap_plan.routing,
        ),
        form_fields=tuple(fields),
    )


def search_pagination_request(
    *,
    bootstrap_plan: RequestPlan,
    navigation_headers: tuple[Header, ...],
    applied_configuration: AppliedSearchConfiguration,
    session_material: ProtectedSearchSessionMaterial,
    cursor: str,
    request_number: int,
    requested_page_size: int | None,
) -> SearchPaginationRequest:
    variables = pagination_variables(
        applied_configuration.complete_variables,
        cursor=cursor,
    )
    if requested_page_size is not None:
        if requested_page_size < 1:
            raise ValueError("Requested page size must be positive")
        variables["count"] = requested_page_size
    fields = [
        FormField(name="av", value=SecretStr("0")),
        FormField(name="__user", value=SecretStr("0")),
        FormField(name="__a", value=SecretStr("1")),
        FormField(name="__req", value=SecretStr(_base36(request_number))),
        FormField(name="__comet_req", value=SecretStr("15")),
    ]
    fields.extend(
        FormField(name=name, value=SecretStr(value), protected=True)
        for name, value in session_material.reveal_graphql_form_fields()
    )
    fields.extend(
        (
            FormField(name="fb_api_caller_class", value=SecretStr("RelayModern")),
            FormField(
                name="fb_api_req_friendly_name",
                value=SecretStr(applied_configuration.query_name),
            ),
            FormField(
                name="variables",
                value=SecretStr(
                    json.dumps(
                        variables,
                        ensure_ascii=True,
                        allow_nan=False,
                        separators=(",", ":"),
                    )
                ),
            ),
            FormField(name="server_timestamps", value=SecretStr("true")),
            FormField(
                name="doc_id",
                value=SecretStr(applied_configuration.query_identifier),
            ),
        )
    )
    return SearchPaginationRequest(
        plan=RequestPlan(
            url=_GRAPHQL_URL,
            method="POST",
            headers=_graphql_headers(
                navigation_headers,
                referer=bootstrap_plan.url,
                lsd=session_material.lsd,
            ),
            follow_redirects=False,
            routing=bootstrap_plan.routing,
        ),
        form_fields=tuple(fields),
    )
