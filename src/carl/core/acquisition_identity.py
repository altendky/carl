"""Conservative network-resource identity, independent of caller evidence IDs."""

import json
import re
from typing import cast
from urllib.parse import parse_qsl, urlsplit

from carl.core.models import JsonValue

_EBAY_IMAGE_PATH = re.compile(r"/images/g/[A-Za-z0-9_-]+/s-l[0-9]+\.(?:jpg|jpeg|png|webp|avif)$")
_FACEBOOK_IMAGE_PATH = re.compile(
    r"/v/t[0-9]+(?:\.[0-9]+)*-[0-9]+/[^/]+\.(?:jpg|jpeg|png|webp|avif)$"
)
# These select an edge or authorize a request, not the image transformation.
# Preserve stp, format, and every unrecognized parameter.
_FACEBOOK_VOLATILE_PARAMETERS = frozenset({"oh", "oe", "_nc_ht", "_nc_cat", "_nc_ohc", "_nc_oc"})


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def ebay_image_identity(url: str) -> tuple[str, ...]:
    """Share recognized identical renditions across approved eBay CDN hosts."""

    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname or ""
        if (
            parsed.scheme == "https"
            and (hostname == "ebayimg.com" or hostname.endswith(".ebayimg.com"))
            and parsed.port is None
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
            and _EBAY_IMAGE_PATH.fullmatch(parsed.path)
        ):
            return (
                "ebay",
                "image",
                "rendition",
                parsed.path,
                _json(
                    sorted(
                        parse_qsl(parsed.query, keep_blank_values=True), key=lambda pair: pair[0]
                    )
                ),
            )
    except ValueError:
        pass
    return ("ebay", "image", "exact_url", url)


def facebook_image_identity(url: str, photo_id: str | None = None) -> tuple[str, ...]:
    """Ignore only known CDN/signature differences; retain crop and transforms."""

    try:
        parsed = urlsplit(url)
        if (
            parsed.scheme == "https"
            and (parsed.hostname or "").endswith(".fbcdn.net")
            and parsed.port is None
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
            and _FACEBOOK_IMAGE_PATH.fullmatch(parsed.path)
        ):
            parameters = sorted(
                (
                    (key, value)
                    for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                    if key not in _FACEBOOK_VOLATILE_PARAMETERS
                ),
                key=lambda pair: pair[0],
            )
            return (
                "facebook",
                "image",
                "rendition",
                photo_id or "unknown_photo",
                parsed.path,
                _json(parameters),
            )
    except ValueError:
        pass
    return ("facebook", "image", "exact_url", photo_id or "unknown_photo", url)


def acquisition_resource_identity(
    kind: tuple[str, ...], payload: JsonValue
) -> tuple[str, ...] | None:
    """Identify fetches that must not overlap, retaining route/auth distinctions.

    Caller-bound work remains separate. The queue serializes each resource until
    the preceding work has committed its reusable evidence, including old jobs.
    """

    if not isinstance(payload, dict):
        return None
    payload = cast(dict[str, JsonValue], payload)
    request = payload.get("request")
    if kind == ("carl", "ebay", "collect", "item") and isinstance(request, dict):
        item = request.get("item_identifier")
        stack = request.get("stack_identifier", "ebay_anonymous")
        if isinstance(item, str) and isinstance(stack, str):
            return ("ebay", "item", item, stack)
    if kind == ("carl", "ebay", "collect", "description") and isinstance(request, dict):
        url, stack = payload.get("url"), request.get("stack_identifier", "ebay_anonymous")
        if isinstance(url, str) and isinstance(stack, str):
            return ("ebay", "description", url, stack)
    if kind == ("carl", "ebay", "collect", "image"):
        url = payload.get("url")
        if isinstance(url, str):
            return ebay_image_identity(url)
    if kind == ("carl", "facebook", "work", "collect_image"):
        reference, plan = payload.get("reference"), payload.get("request_plan")
        if isinstance(reference, dict) and isinstance(plan, dict):
            url, photo_id = reference.get("original_url"), reference.get("photo_id")
            if isinstance(url, str):
                return (
                    *facebook_image_identity(url, photo_id if isinstance(photo_id, str) else None),
                    _json({key: value for key, value in plan.items() if key != "url"}),
                )
    if kind == ("carl", "facebook", "work", "collect_item"):
        item, plan = payload.get("listing_id"), payload.get("request_plan")
        if isinstance(item, str) and isinstance(plan, dict):
            return ("facebook", "item", item, _json(plan))
    return None
