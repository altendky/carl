"""Keep the canonical onboarding examples aligned with current request models."""

import re
import tomllib
from pathlib import Path

from carl.core.configuration import CarlConfiguration, DecodoRouteConfiguration
from carl.core.marketplace_search import CreateMarketplaceSearchRequest, Marketplace
from carl.core.network_defaults import DEFAULT_DATACENTER_NETWORK_PATH
from carl.core.routing import DecodoProduct
from carl.mcp_server import _INSTRUCTIONS

_GUIDE = Path(__file__).resolve().parents[3] / "docs/src/project/setup-and-usage.md"


def _example(language: str) -> str:
    examples = re.findall(rf"```{language}\n(.*?)\n```", _GUIDE.read_text(), re.DOTALL)
    assert len(examples) == 1
    return examples[0]


def test_documented_decodo_configuration_matches_runtime_defaults() -> None:
    # Users must supply verified binary metadata; validation is offline and does
    # not load a binary or resolve credentials. These are test-only substitutions.
    source = _example("toml").replace("REPLACE_WITH_WIREPROXY_VERSION", "test-version")
    source = source.replace("REPLACE_WITH_64_CHARACTER_LOWERCASE_SHA256", "a" * 64)
    configuration = CarlConfiguration.model_validate(tomllib.loads(source))
    datacenter = configuration.require_route(DEFAULT_DATACENTER_NETWORK_PATH)
    assert isinstance(datacenter, DecodoRouteConfiguration)
    assert datacenter.product is DecodoProduct.DATACENTER_PROXY
    stack = configuration.require_acquisition_stack("ebay_anonymous")
    assert stack.network_path == ("decodo", "personal", "carl")
    assert configuration.require_http_transport(stack.http_transport).implementation == "wreq"
    assert not configuration.route_overrides


def test_documented_search_request_is_bounded_and_supported() -> None:
    request = CreateMarketplaceSearchRequest.model_validate_json(_example("json"))
    assert len(request.targets) == 1
    target = request.targets[0]
    assert target.marketplace is Marketplace.EBAY
    assert target.search.maximum_pages == 2
    assert target.search.listing_state == "active"


def test_mcp_instructions_describe_execution_and_evidence_limits() -> None:
    assert "never runs workers implicitly" in _INSTRUCTIONS
    assert "same database and per-user configuration" in _INSTRUCTIONS
    assert "init does not generate" in _INSTRUCTIONS
    assert "different ages" in _INSTRUCTIONS
    assert "not typed extraction schemas" in _INSTRUCTIONS
    assert "closed-mode acquisitions remain unsupported" in _INSTRUCTIONS
