from decimal import Decimal

import pytest
from pydantic import SecretStr

from carl.core.facebook_search_protocol import (
    AppliedSearchConfiguration,
    SearchSourceReference,
)
from carl.core.facebook_search_requests import (
    facebook_search_url,
    search_bootstrap_plan,
    search_pagination_request,
    search_route_definition_request,
    search_session_bootstrap_plan,
)
from carl.core.facebook_search_session import ProtectedSearchSessionMaterial
from carl.core.facebook_work import (
    FacebookSearchRequest,
    SearchDistanceUnit,
    SearchFacebookLocation,
    SearchPriceRange,
    SearchRadius,
    SearchTextLocation,
)
from carl.core.models import Header


def _request() -> FacebookSearchRequest:
    return FacebookSearchRequest(
        query="telescope mount",
        location=SearchFacebookLocation(identifier="123"),
        radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
        price=SearchPriceRange(
            currency="USD",
            minimum=Decimal("12.50"),
            maximum=Decimal("600"),
        ),
    )


def test_search_url_retains_requested_parameters_without_claiming_applied_filters() -> None:
    assert facebook_search_url(_request()) == (
        "https://www.facebook.com/marketplace/123/search?"
        "query=telescope+mount&minPrice=12.50&maxPrice=600&exact=false&radius=97"
    )


def test_bootstrap_plan_uses_configured_route_and_navigation_headers() -> None:
    headers = (Header(name=b"User-Agent", value=b"test-browser"),)

    plan = search_bootstrap_plan(
        _request(),
        routing=("proton", "personal", "carl"),
        navigation_headers=headers,
    )

    assert plan.method == "GET"
    assert plan.headers == headers
    assert plan.routing == ("proton", "personal", "carl")


def test_session_bootstrap_plan_visits_marketplace_root_on_same_route() -> None:
    headers = (Header(name=b"User-Agent", value=b"test-browser"),)

    plan = search_session_bootstrap_plan(
        routing=("proton", "personal", "carl"),
        navigation_headers=headers,
    )

    assert plan.url == "https://www.facebook.com/marketplace/"
    assert plan.method == "GET"
    assert plan.headers == headers
    assert plan.routing == ("proton", "personal", "carl")


def test_search_url_requires_resolved_facebook_location() -> None:
    request = _request().model_copy(
        update={"location": SearchTextLocation(text="Hagerstown, Maryland")}
    )

    with pytest.raises(ValueError, match="resolved Facebook location"):
        facebook_search_url(request)


def test_pagination_request_reuses_bootstrap_identity_and_replaces_cursor() -> None:
    headers = (
        Header(name=b"User-Agent", value=b"test-browser"),
        Header(name=b"Accept-Language", value=b"en-US"),
    )
    bootstrap = search_bootstrap_plan(
        _request(),
        routing=("proton", "personal", "carl"),
        navigation_headers=headers,
    )
    source = SearchSourceReference(
        acquisition_record_identifier="bootstrap-acquisition",
        block_index=1,
        json_path=("preloaders", 0),
    )
    configuration = AppliedSearchConfiguration(
        query_identifier="987654321",
        query_name="CometMarketplaceSearchContentContainerQuery",
        complete_variables={"count": 24, "cursor": None, "nested": {"retained": True}},
        browse_request_parameters=None,
        sources=(source,),
    )
    material = ProtectedSearchSessionMaterial(
        lsd=SecretStr("lsd-secret"),
        jazoest=SecretStr("jazoest-secret"),
        hsi=SecretStr("hsi-secret"),
        spin_revision=SecretStr("revision-secret"),
        spin_branch=SecretStr("branch-secret"),
        spin_timestamp=SecretStr("timestamp-secret"),
    )

    pagination = search_pagination_request(
        bootstrap_plan=bootstrap,
        navigation_headers=headers,
        applied_configuration=configuration,
        session_material=material,
        cursor="cursor-secret-but-retained",
        request_number=1,
        requested_page_size=30,
    )

    assert pagination.plan.method == "POST"
    assert pagination.plan.follow_redirects is False
    fields = {field.name: field.value.get_secret_value() for field in pagination.form_fields}
    assert fields["doc_id"] == "987654321"
    assert fields["lsd"] == "lsd-secret"
    assert fields["variables"] == (
        '{"count":30,"cursor":"cursor-secret-but-retained","nested":{"retained":true}}'
    )
    assert configuration.complete_variables["cursor"] is None
    safe_plan = pagination.plan.as_json(protected_header_names=frozenset({b"x-fb-lsd"}))
    assert "lsd-secret" not in str(safe_plan)


def test_route_definition_request_uses_relative_search_route_and_protects_session_fields() -> None:
    headers = (
        Header(name=b"User-Agent", value=b"test-browser"),
        Header(name=b"Accept-Language", value=b"en-US"),
    )
    bootstrap = search_bootstrap_plan(
        _request(),
        routing=("proton", "personal", "carl"),
        navigation_headers=headers,
    )
    material = ProtectedSearchSessionMaterial(
        lsd=SecretStr("lsd-secret"),
        jazoest=SecretStr("jazoest-secret"),
        hsi=SecretStr("hsi-secret"),
        spin_revision=SecretStr("revision-secret"),
        spin_branch=SecretStr("branch-secret"),
        spin_timestamp=SecretStr("timestamp-secret"),
    )

    request = search_route_definition_request(
        bootstrap_plan=bootstrap,
        navigation_headers=headers,
        session_material=material,
        request_number=1,
    )

    assert request.plan.url == "https://www.facebook.com/ajax/route-definition/"
    assert request.plan.method == "POST"
    assert request.plan.follow_redirects is False
    assert request.plan.routing == ("proton", "personal", "carl")
    assert {header.name.lower(): header.value for header in request.plan.headers}[b"referer"] == (
        b"https://www.facebook.com/marketplace/"
    )
    fields = {field.name: field for field in request.form_fields}
    assert fields["route_url"].value.get_secret_value() == (
        "/marketplace/123/search?"
        "query=telescope+mount&minPrice=12.50&maxPrice=600&exact=false&radius=97"
    )
    assert fields["routing_namespace"].value.get_secret_value() == "fb_comet"
    assert fields["lsd"].protected is True
    assert fields["__hsi"].protected is True
    assert "lsd-secret" not in str(fields["lsd"].evidence())
