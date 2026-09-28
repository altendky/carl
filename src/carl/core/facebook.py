"""Pure Facebook Marketplace item-page extraction."""

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from urllib.parse import urlsplit

from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel

EXTRACTOR_COMPONENT = ("carl", "facebook", "extract", "embedded_json")


class FacebookItemResponseKind(JsonStringEnumeration):
    FULL_LISTING = "full_listing"
    LISTING_UNAVAILABLE = "listing_unavailable"
    LOGIN_PAGE = "login_page"
    GENERIC_ERROR_PAGE = "generic_error_page"
    BOT_CHALLENGE = "bot_challenge"
    MALFORMED_OR_INCOMPLETE = "malformed_or_incomplete"
    REQUESTED_ID_ABSENT = "requested_id_absent"


class FacebookItemResponseClassification(StrictModel):
    kind: FacebookItemResponseKind
    requested_listing_id: str
    effective_url: str
    evidence: tuple[str, ...]
    parseable_json_blocks: int
    malformed_json_blocks: int

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


def listing_id_from_url(url: str) -> str:
    parsed = urlsplit(url)
    match = re.fullmatch(r"/marketplace/item/([0-9]+)/?", parsed.path)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in {"www.facebook.com", "facebook.com"}
        or parsed.username
        or parsed.password
        or parsed.port not in {None, 443}
        or parsed.query
        or parsed.fragment
        or match is None
    ):
        raise ValueError("Expected a canonical HTTPS Facebook Marketplace item URL")
    return match[1]


def _strict_json(text: str) -> JsonValue:
    def invalid_constant(value: str) -> None:
        raise ValueError(f"Non-JSON number: {value}")

    value = json.loads(text, parse_constant=invalid_constant)
    if not isinstance(value, (dict, list)):
        raise ValueError("Embedded JSON must be an object or array")
    return value


class _ScriptParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.script_index = -1
        self.blocks: list[dict[str, JsonValue]] = []
        self._current: dict[str, JsonValue] | None = None
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "script":
            return
        self.script_index += 1
        attributes = {name: value for name, value in attrs}
        media_type = (attributes.get("type") or "").split(";", 1)[0].strip().lower()
        if media_type != "application/json":
            return
        self._current = {
            "block_index": len(self.blocks),
            "script_index": self.script_index,
            "attributes": [{"name": name, "value": value} for name, value in attrs],
            "html_position": list(self.getpos()),
        }
        self._parts = []

    def handle_data(self, data: str) -> None:
        if self._current is not None:
            self._parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "script" and self._current is not None:
            self._finish(closed=True)

    def close(self) -> None:
        super().close()
        if self._current is not None:
            self._finish(closed=False)

    def _finish(self, *, closed: bool) -> None:
        if self._current is None:
            return
        block = self._current
        raw = "".join(self._parts)
        block["closed"] = closed
        try:
            block["value"] = _strict_json(raw)
            block["parse_error"] = None
        except (ValueError, RecursionError) as error:
            block["value"] = None
            block["parse_error"] = {"type": type(error).__name__, "message": str(error)}
            block["unparsed_text"] = raw
        self.blocks.append(block)
        self._current = None
        self._parts = []


def parse_json_blocks(html: str) -> tuple[dict[str, JsonValue], ...]:
    parser = _ScriptParser()
    parser.feed(html)
    parser.close()
    return tuple(parser.blocks)


def classify_item_response(
    html: str, *, requested_listing_id: str, effective_url: str
) -> FacebookItemResponseClassification:
    """Classify a complete Facebook item response using content, not status alone."""

    parsed_url = urlsplit(effective_url)
    normalized_path = parsed_url.path.lower()
    folded = html.casefold()
    blocks = parse_json_blocks(html)
    parseable = tuple(block for block in blocks if block["value"] is not None)
    malformed_count = len(blocks) - len(parseable)

    def result(
        kind: FacebookItemResponseKind, *evidence: str
    ) -> FacebookItemResponseClassification:
        return FacebookItemResponseClassification(
            kind=kind,
            requested_listing_id=requested_listing_id,
            effective_url=effective_url,
            evidence=evidence,
            parseable_json_blocks=len(parseable),
            malformed_json_blocks=malformed_count,
        )

    if "/checkpoint/" in normalized_path:
        return result(FacebookItemResponseKind.BOT_CHALLENGE, "challenge_marker")
    if "/login" in normalized_path:
        return result(FacebookItemResponseKind.LOGIN_PAGE, "login_marker")

    marketplace_payload = False
    requested_object_with_title = False
    for block in parseable:
        value = block["value"]
        assert value is not None
        for obj, _path in _walk(value):
            if any(
                marker in obj
                for marker in (
                    "marketplace_listing_title",
                    "listing_price",
                    "listing_photos",
                    "marketplace_search",
                )
            ):
                marketplace_payload = True
            if (
                obj.get("id") == requested_listing_id
                and isinstance(obj.get("marketplace_listing_title"), str)
                and any(
                    marker in obj
                    for marker in ("listing_price", "listing_photos", "redacted_description")
                )
            ):
                requested_object_with_title = True

    if requested_object_with_title:
        return result(
            FacebookItemResponseKind.FULL_LISTING,
            "requested_listing_object",
            "marketplace_listing_title",
            "structured_listing_fields",
        )
    if any(marker in folded for marker in ('id="login_form"', 'name="login"', "login/?next=")):
        return result(FacebookItemResponseKind.LOGIN_PAGE, "login_marker")
    if any(
        marker in folded
        for marker in ('id="captcha"', "checkpointsubmitbutton", "security check required")
    ):
        return result(FacebookItemResponseKind.BOT_CHALLENGE, "challenge_marker")
    if any(
        marker in folded
        for marker in (
            "this listing is no longer available",
            "this item is no longer available",
            "listing may have been deleted",
        )
    ):
        return result(FacebookItemResponseKind.LISTING_UNAVAILABLE, "unavailable_marker")
    if any(
        marker in folded
        for marker in (
            "something went wrong",
            "facebook.com/error",
            "temporarily blocked",
        )
    ):
        return result(FacebookItemResponseKind.GENERIC_ERROR_PAGE, "error_marker")
    if marketplace_payload or "marketplacepdp" in folded:
        return result(
            FacebookItemResponseKind.REQUESTED_ID_ABSENT,
            "marketplace_payload",
            "requested_listing_object_absent",
        )
    return result(
        FacebookItemResponseKind.MALFORMED_OR_INCOMPLETE,
        "no_recognized_marketplace_item_payload",
    )


def _walk(value: JsonValue) -> tuple[tuple[dict[str, JsonValue], list[str | int]], ...]:
    found: list[tuple[dict[str, JsonValue], list[str | int]]] = []
    pending: list[tuple[JsonValue, list[str | int]]] = [(value, [])]
    while pending:
        node, path = pending.pop()
        if isinstance(node, dict):
            found.append((node, path))
            pending.extend((child, [*path, key]) for key, child in reversed(tuple(node.items())))
        elif isinstance(node, list):
            pending.extend(
                (child, [*path, index]) for index, child in reversed(tuple(enumerate(node)))
            )
    return tuple(found)


def _normalized_field(name: str, raw: JsonValue) -> JsonValue:
    if name in {"description", "location_text"}:
        if not isinstance(raw, dict) or not isinstance(raw.get("text"), str):
            raise ValueError("Expected a text object")
        return raw["text"]
    if name == "price":
        if not isinstance(raw, dict) or not isinstance(raw.get("currency"), str):
            raise ValueError("Expected price amount and currency")
        amount = Decimal(str(raw["amount"]))
        if not amount.is_finite():
            raise ValueError("Price must be finite")
        return {"amount_decimal": str(amount), "currency": raw["currency"]}
    if name in {"location", "item_location"}:
        if not isinstance(raw, dict):
            raise ValueError("Expected a coordinate object")
        return {"coordinates": raw, "precision": "approximate"}
    if name == "published_at":
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise ValueError("Expected a Unix timestamp")
        return {"unix_seconds": raw, "meaning": "source_listing_creation_time"}
    if name.startswith("availability_") and not isinstance(raw, bool):
        raise ValueError("Expected a boolean")
    if name == "title" and not isinstance(raw, str):
        raise ValueError("Expected a string")
    return raw


_FIELDS = {
    "title": "marketplace_listing_title",
    "description": "redacted_description",
    "price": "listing_price",
    "location_text": "location_text",
    "location": "location",
    "item_location": "item_location",
    "availability_sold": "is_sold",
    "availability_pending": "is_pending",
    "availability_live": "is_live",
    "inventory_count": "inventory_count",
    "published_at": "creation_time",
    "attributes": "attribute_data",
    "seller": "marketplace_listing_seller",
}


@dataclass(frozen=True, slots=True)
class Extraction:
    blocks: tuple[dict[str, JsonValue], ...]
    observation: dict[str, JsonValue]


def extract_listing(html: str, *, listing_id: str, acquisition_record_id: str) -> Extraction:
    blocks = parse_json_blocks(html)
    warnings: list[JsonValue] = []
    fields: dict[str, JsonValue] = {name: {"state": "missing", "evidence": []} for name in _FIELDS}
    images: list[JsonValue] = []
    fragments: list[JsonValue] = []

    for block in blocks:
        block_index = block["block_index"]
        if block["parse_error"] is not None or not block["closed"]:
            warnings.append({"kind": "embedded_json_parse_failure", "block_index": block_index})
        value = block["value"]
        if value is None:
            continue
        for obj, path in _walk(value):
            if obj.get("id") != listing_id or not any(
                key in obj
                for key in (
                    "marketplace_listing_title",
                    "listing_price",
                    "listing_photos",
                    "redacted_description",
                )
            ):
                continue
            source: dict[str, JsonValue] = {
                "acquisition_record_id": acquisition_record_id,
                "block_index": block_index,
                "json_path": path,
            }
            fragments.append(source)
            for name, key in _FIELDS.items():
                if key not in obj:
                    continue
                raw = obj[key]
                evidence: dict[str, JsonValue] = {
                    "source": {**source, "json_path": [*path, key]},
                    "original": raw,
                    "evidence_kind": (
                        "seller_claim"
                        if name in {"title", "description", "price", "attributes"}
                        else "source_reported"
                    ),
                    "confidence": None,
                    "state": "unavailable" if raw is None else "present",
                    "normalized": None,
                }
                if raw is not None:
                    try:
                        evidence["normalized"] = _normalized_field(name, raw)
                    except (InvalidOperation, KeyError, TypeError, ValueError) as error:
                        evidence["state"] = "failed"
                        warnings.append(
                            {
                                "kind": "normalization_failure",
                                "field": name,
                                "error_type": type(error).__name__,
                            }
                        )
                field = fields[name]
                assert isinstance(field, dict)
                field_evidence = field["evidence"]
                assert isinstance(field_evidence, list)
                field_evidence.append(evidence)

            photos = obj.get("listing_photos")
            if photos is None:
                continue
            if not isinstance(photos, list):
                warnings.append({"kind": "malformed_gallery"})
                continue
            for order, photo in enumerate(photos):
                image = photo.get("image") if isinstance(photo, dict) else None
                if not isinstance(image, dict) or not isinstance(image.get("uri"), str):
                    warnings.append({"kind": "malformed_gallery_image", "order": order})
                    continue
                images.append(
                    {
                        "original_url": image["uri"],
                        "role": "listing_gallery",
                        "gallery_order": order,
                        "photo_id": photo.get("id") if isinstance(photo, dict) else None,
                        "declared_dimensions": {
                            "width": image.get("width"),
                            "height": image.get("height"),
                        },
                        "source": {
                            **source,
                            "json_path": [*path, "listing_photos", order, "image"],
                        },
                        "download": {"state": "not_yet_collected"},
                    }
                )

    for name, field_value in fields.items():
        assert isinstance(field_value, dict)
        evidence = field_value["evidence"]
        assert isinstance(evidence, list)
        if not evidence:
            continue
        distinct = {
            json.dumps([item["state"], item["normalized"]], sort_keys=True, allow_nan=False)
            for item in evidence
            if isinstance(item, dict)
        }
        field_value["state"] = "conflicting" if len(distinct) > 1 else evidence[0]["state"]
        if len(distinct) > 1:
            warnings.append({"kind": "conflicting_fragment_values", "field": name})

    if not fragments:
        warnings.append(
            {
                "kind": "target_listing_not_found",
                "possible_causes": [
                    "preview_response",
                    "login_or_challenge_response",
                    "source_schema_change",
                ],
            }
        )

    observation: dict[str, JsonValue] = {
        "source": "facebook_marketplace",
        "listing_id": listing_id,
        "acquisition_record_id": acquisition_record_id,
        "scope": "item_page",
        "state": "extracted" if fragments else "unavailable",
        "completeness": "partial" if warnings else "mapped_fields_present",
        "warnings": warnings,
        "fragments": fragments,
        "fields": fields,
        "images": images,
        "gallery_completeness": "unknown",
        "interpretations": [],
    }
    return Extraction(blocks=blocks, observation=observation)
