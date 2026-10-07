"""Semantic search-scope identities, independent of acquisition and traversal."""

import hashlib
import json
from decimal import Decimal

from carl.core.ebay import EbaySearchRequest
from carl.core.facebook_work import FacebookSearchRequest
from carl.core.models import JsonValue


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def search_scope_sha256(marketplace: str, request: JsonValue) -> str | None:
    """Identify the searched population, returning None for unknown legacy requests."""

    try:
        if marketplace == "facebook":
            parsed = FacebookSearchRequest.model_validate(request)
            value = parsed.model_dump(mode="json")
            location = value.get("location")
            if isinstance(location, dict):
                location.pop("label", None)
            # Decimal spelling is not a search-scope change (300 == 300.00).
            if parsed.price is not None:
                value["price"] = {
                    "currency": parsed.price.currency,
                    "minimum": _decimal_text(parsed.price.minimum),
                    "maximum": _decimal_text(parsed.price.maximum),
                }
        elif marketplace == "ebay":
            ebay = EbaySearchRequest.model_validate(request)
            value = {"query": ebay.query, "listing_state": ebay.listing_state}
        else:
            return None
    except ValueError:
        return None
    content = json.dumps(
        {"marketplace": marketplace, "request": value},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def search_scope_changed(
    marketplace: str, baseline_request: JsonValue, current_request: JsonValue
) -> bool:
    """Conservatively recognize known scope changes without breaking legacy records."""

    baseline = search_scope_sha256(marketplace, baseline_request)
    current = search_scope_sha256(marketplace, current_request)
    return baseline is not None and current is not None and baseline != current
