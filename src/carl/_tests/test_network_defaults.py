"""Public route defaults are explicit, with truthful historical Proton compatibility."""

import pytest
from pydantic import ValidationError

from carl.core.facebook_refresh import SearchRefreshRequest
from carl.core.facebook_work import CreateSearchRequest
from carl.core.json import encode_json
from carl.core.marketplace_listing import RequestListingDetailsRequest
from carl.core.models import StrictModel
from carl.core.network_defaults import DEFAULT_DATACENTER_NETWORK_PATH
from carl.core.pipeline import PipelineOptions, PipelineStage
from carl.core.review_workspace import RequestWorkspaceRefreshRequest

_SEARCH = {
    "request": {
        "query": "telescope",
        "location": {"kind": "facebook_location", "identifier": "103966822975104"},
        "radius": {"value": 50, "unit": "miles"},
    },
    "traversal": {"maximum_pages": 1},
}
_CASES = (
    (CreateSearchRequest, _SEARCH, "network_path", "proton_route"),
    (
        SearchRefreshRequest,
        {"base_search_run_record_identifier": "run"},
        "image_network_path",
        "proton_route",
    ),
    (
        RequestWorkspaceRefreshRequest,
        {"workspace_record_identifier": "workspace"},
        "image_network_path",
        "proton_route",
    ),
    (
        RequestListingDetailsRequest,
        {"marketplace": "facebook", "external_identifier": "123456789012"},
        "image_network_path",
        "proton_route",
    ),
    (
        PipelineOptions,
        {"stop_after": "images"},
        "facebook_image_network_path",
        "facebook_image_route",
    ),
)


@pytest.mark.parametrize(("model", "inputs", "path_field", "legacy_field"), _CASES)
def test_defaults_select_datacenter_without_legacy_proton(
    model: type[StrictModel], inputs: dict[str, object], path_field: str, legacy_field: str
) -> None:
    request = model.model_validate_json(encode_json(inputs))
    assert getattr(request, path_field) == DEFAULT_DATACENTER_NETWORK_PATH
    assert getattr(request, legacy_field) is None
    assert legacy_field not in model.model_json_schema()["properties"]
    assert legacy_field not in request.model_dump(mode="json")
    assert model.model_validate_json(request.model_dump_json()) == request


@pytest.mark.parametrize(("model", "inputs", "path_field", "legacy_field"), _CASES)
def test_explicit_historical_proton_is_not_a_decodo_alias(
    model: type[StrictModel], inputs: dict[str, object], path_field: str, legacy_field: str
) -> None:
    request = model.model_validate_json(encode_json({**inputs, legacy_field: "carl"}))
    assert getattr(request, path_field) == ("proton", "personal", "carl")
    restored = model.model_validate_json(request.model_dump_json())
    assert getattr(restored, path_field) == ("proton", "personal", "carl")
    assert restored.model_dump(mode="json") == request.model_dump(mode="json")


@pytest.mark.parametrize(("model", "inputs", "path_field", "legacy_field"), _CASES)
def test_conflicting_explicit_routes_are_rejected(
    model: type[StrictModel], inputs: dict[str, object], path_field: str, legacy_field: str
) -> None:
    with pytest.raises(ValidationError, match="conflict"):
        model.model_validate_json(
            encode_json(
                {**inputs, legacy_field: "carl", path_field: list(DEFAULT_DATACENTER_NETWORK_PATH)}
            )
        )


@pytest.mark.parametrize(
    "path",
    (
        (),
        ("decodo", "personal"),
        ("decodo", "personal", ""),
        ("decodo", "personal", "datacenter "),
        ("other", "personal", "carl"),
    ),
)
def test_invalid_explicit_paths_are_rejected(path: tuple[str, ...]) -> None:
    with pytest.raises(ValidationError, match=r"Network paths|acquisition supports"):
        CreateSearchRequest.model_validate_json(
            encode_json({**_SEARCH, "network_path": list(path)})
        )


@pytest.mark.parametrize("model", (SearchRefreshRequest, RequestWorkspaceRefreshRequest))
def test_refresh_search_override_is_optional_and_independent(
    model: type[SearchRefreshRequest] | type[RequestWorkspaceRefreshRequest],
) -> None:
    inputs = (
        {"base_search_run_record_identifier": "run"}
        if model is SearchRefreshRequest
        else {"workspace_record_identifier": "workspace"}
    )
    request = model.model_validate_json(
        encode_json({**inputs, "image_network_path": ["proton", "personal", "images"]})
    )
    assert request.requested_search_network_path is None
    assert request.requested_image_network_path == ("proton", "personal", "images")
    legacy = model.model_validate_json(encode_json({**inputs, "proton_route": "carl"}))
    assert legacy.requested_search_network_path == ("proton", "personal", "carl")


def test_ebay_images_default_to_datacenter_and_reject_ignored_route_overrides() -> None:
    inputs = {"marketplace": "ebay", "external_identifier": "123456789012"}
    request = RequestListingDetailsRequest.model_validate_json(encode_json(inputs))
    assert request.requested_image_network_path == DEFAULT_DATACENTER_NETWORK_PATH
    for override in (
        {"image_network_path": ["proton", "personal", "carl"]},
        {"proton_route": "carl"},
        {"decodo_route": "other"},
    ):
        with pytest.raises(ValidationError, match="eBay"):
            RequestListingDetailsRequest.model_validate_json(encode_json({**inputs, **override}))


def test_explicit_neutral_proton_route_and_property_names() -> None:
    search = CreateSearchRequest.model_validate_json(
        encode_json({**_SEARCH, "network_path": ["proton", "personal", "other"]})
    )
    assert search.requested_network_path == ("proton", "personal", "other")
    assert search.proton_route is None
    pipeline = PipelineOptions(stop_after=PipelineStage.IMAGES)
    assert pipeline.requested_facebook_image_network_path == DEFAULT_DATACENTER_NETWORK_PATH
