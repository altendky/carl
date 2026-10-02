"""Bounded, offline eBay item extraction and durable work definitions."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import cast, override
from urllib.parse import urljoin, urlsplit

from pydantic import Field, field_validator

from carl.core.ebay import ebay_item_identifier
from carl.core.http import RequestPlan
from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel
from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    WorkDefinition,
)

COLLECT_EBAY_ITEM_WORK_KIND = ("carl", "ebay", "collect", "item")
EXTRACT_EBAY_ITEM_WORK_KIND = ("carl", "ebay", "extract", "item")
COLLECT_EBAY_IMAGE_WORK_KIND = ("carl", "ebay", "collect", "image")
COLLECT_EBAY_DESCRIPTION_WORK_KIND = ("carl", "ebay", "collect", "description")
_ITEM_PATTERN = r"^[0-9]{9,15}$"
_SPACE = re.compile(r"\s+")
_VOID = frozenset(
    {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }
)
_CHALLENGE = (
    "pardon our interruption",
    "checking your browser before accessing",
    "we've detected unusual activity",
    "to keep ebay a safe place to buy and sell",
    "verify you are human",
)
_UNAVAILABLE = (
    "this listing has ended",
    "this listing sold on",
    "this listing was ended by the seller",
    "this item is no longer available",
    "this listing is no longer available",
    "we looked everywhere",
    "the listing you're looking for has ended",
)


def _allowed_url(value: str, domain: str) -> bool:
    try:
        parsed = urlsplit(value)
        hostname = (parsed.hostname or "").lower()
        return (
            parsed.scheme == "https"
            and (hostname == domain or hostname.endswith("." + domain))
            and parsed.username is None
            and parsed.password is None
            and parsed.port is None
            and not any(character.isspace() or ord(character) < 32 for character in value)
            and "\\" not in value
        )
    except ValueError:
        return False


def validate_ebay_image_url(value: str) -> str:
    if not _allowed_url(value, "ebayimg.com"):
        raise ValueError(
            "eBay images require credential-free HTTPS ebayimg.com URLs without a port"
        )
    return value


def validate_ebay_description_url(value: str) -> str:
    if not _allowed_url(value, "ebaydesc.com"):
        raise ValueError(
            "eBay descriptions require credential-free HTTPS ebaydesc.com URLs without a port"
        )
    return value


class EbayItemRequest(StrictModel):
    item_identifier: str = Field(pattern=_ITEM_PATTERN)
    stack_identifier: str = Field(
        default="ebay_anonymous", pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"
    )
    maximum_images: int = Field(default=20, ge=0, le=50)


class EbayItemResponseKind(JsonStringEnumeration):
    DETAIL = "detail"
    CHALLENGE = "challenge"
    UNAVAILABLE = "unavailable"
    UNRECOGNIZED = "unrecognized"
    MISMATCHED_ITEM = "mismatched_item"


class EbayItemExtraction(StrictModel):
    classification: EbayItemResponseKind
    item_identifier: str = Field(pattern=_ITEM_PATTERN)
    title: str | None = None
    displayed_price: str | None = None
    currency: str | None = None
    condition: str | None = None
    description: str | None = None
    description_url: str | None = None
    gallery_urls: tuple[str, ...] = Field(default=(), max_length=50)
    issues: tuple[JsonValue, ...] = ()

    @field_validator("description_url")
    @classmethod
    def validate_description(cls, value: str | None) -> str | None:
        return validate_ebay_description_url(value) if value is not None else None

    @field_validator("gallery_urls")
    @classmethod
    def validate_gallery(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(validate_ebay_image_url(url) for url in value)


class CollectEbayItemPayload(StrictModel):
    request: EbayItemRequest

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class ExtractEbayItemPayload(StrictModel):
    request: EbayItemRequest
    acquisition_record_identifier: str = Field(min_length=1)

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class CollectEbayImagePayload(StrictModel):
    item_identifier: str = Field(pattern=_ITEM_PATTERN)
    observation_record_identifier: str = Field(min_length=1)
    reference_record_identifier: str = Field(min_length=1)
    url: str

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        return validate_ebay_image_url(value)

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class CollectEbayDescriptionPayload(StrictModel):
    request: EbayItemRequest
    observation_record_identifier: str = Field(min_length=1)
    url: str

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        return validate_ebay_description_url(value)

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


def _work(
    *, identifier: str, payload: StrictModel, kind: tuple[str, ...], not_before_utc_ns: int
) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=kind,
        payload_schema_version=1,
        payload=payload.model_dump(mode="json"),
        deduplication_identity=(
            *kind,
            hashlib.sha256(payload.model_dump_json().encode()).hexdigest(),
        ),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
        ),
    )


def collect_ebay_item_work(
    *, identifier: str, payload: CollectEbayItemPayload, not_before_utc_ns: int = 0
) -> WorkDefinition:
    return _work(
        identifier=identifier,
        payload=payload,
        kind=COLLECT_EBAY_ITEM_WORK_KIND,
        not_before_utc_ns=not_before_utc_ns,
    )


def extract_ebay_item_work(
    *, identifier: str, payload: ExtractEbayItemPayload, not_before_utc_ns: int = 0
) -> WorkDefinition:
    return _work(
        identifier=identifier,
        payload=payload,
        kind=EXTRACT_EBAY_ITEM_WORK_KIND,
        not_before_utc_ns=not_before_utc_ns,
    )


def collect_ebay_image_work(
    *, identifier: str, payload: CollectEbayImagePayload, not_before_utc_ns: int = 0
) -> WorkDefinition:
    return _work(
        identifier=identifier,
        payload=payload,
        kind=COLLECT_EBAY_IMAGE_WORK_KIND,
        not_before_utc_ns=not_before_utc_ns,
    )


def collect_ebay_description_work(
    *, identifier: str, payload: CollectEbayDescriptionPayload, not_before_utc_ns: int = 0
) -> WorkDefinition:
    return _work(
        identifier=identifier,
        payload=payload,
        kind=COLLECT_EBAY_DESCRIPTION_WORK_KIND,
        not_before_utc_ns=not_before_utc_ns,
    )


def ebay_item_work_constraints() -> tuple[ConcurrencyConstraint, ...]:
    return tuple(
        ConcurrencyConstraint(
            identifier=(*kind, "concurrency"),
            subject_kind=SchedulingSubjectKind.WORK_ITEM,
            scope=SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
            maximum_active=maximum,
        )
        for kind, maximum in (
            (COLLECT_EBAY_ITEM_WORK_KIND, 1),
            (COLLECT_EBAY_DESCRIPTION_WORK_KIND, 1),
            (COLLECT_EBAY_IMAGE_WORK_KIND, 5),
            (EXTRACT_EBAY_ITEM_WORK_KIND, 5),
        )
    )


def ebay_item_plan(request: EbayItemRequest, *, network_path: tuple[str, ...]) -> RequestPlan:
    return RequestPlan(
        url=f"https://www.ebay.com/itm/{request.item_identifier}", routing=network_path
    )


def ebay_image_plan(
    payload: CollectEbayImagePayload, *, network_path: tuple[str, ...]
) -> RequestPlan:
    return RequestPlan(url=payload.url, routing=network_path, follow_redirects=False)


def ebay_description_plan(
    payload: CollectEbayDescriptionPayload, *, network_path: tuple[str, ...]
) -> RequestPlan:
    return RequestPlan(url=payload.url, routing=network_path, follow_redirects=False)


def _text(parts: list[str], *, limit: int = 60_000) -> str | None:
    return _SPACE.sub(" ", " ".join(parts)).strip()[:limit] or None


@dataclass(slots=True)
class _Frame:
    tag: str
    hidden: bool
    fields: frozenset[str] = frozenset()
    gallery: bool = False
    json_script: bool = False


@dataclass(slots=True)
class _ItemParser(HTMLParser):
    frames: list[_Frame] = field(default_factory=list)
    texts: dict[str, list[str]] = field(default_factory=dict)
    visible: list[str] = field(default_factory=list)
    canonicals: list[str] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)
    gallery: list[str] = field(default_factory=list)
    descriptions: list[str] = field(default_factory=list)
    json_blocks: list[str] = field(default_factory=list)
    json_parts: list[str] = field(default_factory=list)
    json_size: int = 0

    def __post_init__(self) -> None:
        HTMLParser.__init__(self, convert_charrefs=True)

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key.lower(): value or "" for key, value in attrs}
        classes = frozenset(attributes.get("class", "").split())
        parent_hidden = bool(self.frames and self.frames[-1].hidden)
        hidden = (
            parent_hidden
            or tag in {"script", "style", "template", "noscript"}
            or "hidden" in attributes
            or attributes.get("aria-hidden") == "true"
            or bool(
                re.search(
                    r"(?:display\s*:\s*none|visibility\s*:\s*hidden)",
                    attributes.get("style", ""),
                    re.I,
                )
            )
        )
        json_script = (
            tag == "script"
            and attributes.get("type", "").lower().split(";")[0].strip() == "application/ld+json"
            and not parent_hidden
        )
        fields: set[str] = set()
        if "x-item-title__mainTitle" in classes:
            fields.add("title")
        if "x-price-primary" in classes:
            fields.add("price")
        if "x-item-condition-text" in classes:
            fields.add("condition")
        if attributes.get("id") == "desc_div":
            fields.add("description")
        gallery = bool(self.frames and self.frames[-1].gallery) or any(
            token.startswith(
                ("ux-image-carousel", "ux-image-filmstrip", "ux-image-grid", "x-item-image")
            )
            or token == "image-gallery"
            for token in classes
        )
        if not parent_hidden:
            if tag == "link" and "canonical" in attributes.get("rel", "").lower().split():
                self.canonicals.append(attributes.get("href", ""))
            if tag == "meta":
                self.metadata[attributes.get("property", attributes.get("name", "")).lower()] = (
                    attributes.get("content", "")
                )
        if not hidden:
            if tag == "img" and gallery:
                self.gallery.append(
                    attributes.get("data-zoom-src")
                    or attributes.get("data-src")
                    or attributes.get("src", "")
                )
            if tag == "iframe" and attributes.get("id") == "desc_ifr":
                self.descriptions.append(attributes.get("src", ""))
        if tag not in _VOID:
            self.frames.append(_Frame(tag, hidden, frozenset(fields), gallery, json_script))
            if json_script:
                self.json_parts = []
                self.json_size = 0

    @override
    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if tag not in _VOID:
            self.handle_endtag(tag)

    @override
    def handle_endtag(self, tag: str) -> None:
        for index in range(len(self.frames) - 1, -1, -1):
            if self.frames[index].tag == tag:
                if self.frames[index].json_script and len(self.json_blocks) < 64:
                    self.json_blocks.append("".join(self.json_parts))
                del self.frames[index:]
                return

    @override
    def handle_data(self, data: str) -> None:
        if self.frames and self.frames[-1].json_script:
            self.json_size += len(data)
            if self.json_size <= 2_000_000:
                self.json_parts.append(data)
            return
        if self.frames and self.frames[-1].hidden:
            return
        if data.strip():
            self.visible.append(data)
            for name in {name for frame in self.frames for name in frame.fields}:
                self.texts.setdefault(name, []).append(data)


def _objects(value: JsonValue) -> Iterator[dict[str, JsonValue]]:
    pending = [value]
    examined = 0
    while pending and examined < 20_000:
        node = pending.pop()
        examined += 1
        if isinstance(node, dict):
            yield node
            pending.extend(reversed(tuple(node.values())))
        elif isinstance(node, list):
            pending.extend(reversed(node))


def _strings(value: JsonValue | None) -> tuple[str, ...]:
    pending = [value]
    strings: list[str] = []
    examined = 0
    while pending and examined < 20_000:
        node = pending.pop()
        examined += 1
        if isinstance(node, str):
            strings.append(node)
        elif isinstance(node, list):
            pending.extend(reversed(node))
        elif isinstance(node, dict):
            pending.append(node.get("url") or node.get("contentUrl") or node.get("@id"))
    return tuple(strings)


def _product_urls(product: dict[str, JsonValue]) -> tuple[str, ...]:
    urls = tuple(
        url for key in ("url", "@id", "mainEntityOfPage") for url in _strings(product.get(key))
    )
    offers = product.get("offers")
    offer_objects = (
        [offers] if isinstance(offers, dict) else offers if isinstance(offers, list) else []
    )
    urls += tuple(
        url
        for offer in offer_objects
        if isinstance(offer, dict)
        for url in _strings(offer.get("url"))
    )
    return tuple(url for url in urls if url)


def _product_identifiers(product: dict[str, JsonValue], effective_url: str) -> set[str]:
    return {
        identifier
        for url in _product_urls(product)
        if (identifier := ebay_item_identifier(urljoin(effective_url, url))) is not None
    }


def _reject_json_constant(value: str) -> JsonValue:
    raise ValueError(f"Non-finite JSON value: {value}")


def extract_ebay_item(
    html: str, *, item_identifier: str, effective_url: str, status_code: int = 200
) -> EbayItemExtraction:
    _ = EbayItemRequest(item_identifier=item_identifier)
    if len(html) > 20_000_000:
        return EbayItemExtraction(
            classification=EbayItemResponseKind.UNRECOGNIZED,
            item_identifier=item_identifier,
            issues=({"kind": "html_size_limit"},),
        )
    parser = _ItemParser()
    parser.feed(html)
    parser.close()
    visible = (_text(parser.visible, limit=2_000_000) or "").casefold()
    issues: list[JsonValue] = []

    def classified(kind: EbayItemResponseKind) -> EbayItemExtraction:
        return EbayItemExtraction(
            classification=kind, item_identifier=item_identifier, issues=tuple(issues)
        )

    if any(marker in visible for marker in _CHALLENGE) or "/captcha" in effective_url.casefold():
        return classified(EbayItemResponseKind.CHALLENGE)
    identifiers = {
        identifier
        for url in (effective_url, *(urljoin(effective_url, value) for value in parser.canonicals))
        if (identifier := ebay_item_identifier(url)) is not None
    }
    if identifiers - {item_identifier}:
        return classified(EbayItemResponseKind.MISMATCHED_ITEM)
    if status_code in {404, 410} or any(marker in visible for marker in _UNAVAILABLE):
        return classified(EbayItemResponseKind.UNAVAILABLE)
    if (
        status_code != 200
        or not _allowed_url(effective_url, "ebay.com")
        or item_identifier not in identifiers
    ):
        return classified(EbayItemResponseKind.UNRECOGNIZED)

    products: list[dict[str, JsonValue]] = []
    for index, block in enumerate(parser.json_blocks):
        try:
            decoded = cast(JsonValue, json.loads(block, parse_constant=_reject_json_constant))
        except (ValueError, RecursionError):
            issues.append({"kind": "malformed_json_ld", "block_index": index})
            continue
        products.extend(
            node for node in _objects(decoded) if "Product" in _strings(node.get("@type"))
        )
    matching = [
        product
        for product in products
        if _product_identifiers(product, effective_url) == {item_identifier}
    ]
    if (
        not matching
        and len(products) == 1
        and not _product_urls(products[0])
        and isinstance(products[0].get("name"), str)
    ):
        # An unlinked Product is accepted only when its title agrees with the bound page.
        dom_title = _text(parser.texts.get("title", [])) or parser.metadata.get("og:title")
        if dom_title == products[0].get("name"):
            matching = products
    if len(matching) > 1:
        issues.append({"kind": "multiple_matching_products"})
    product = matching[0] if matching else {}

    def scalar(name: str) -> str | None:
        value = product.get(name)
        return _text([value]) if isinstance(value, str) else None

    title = (
        scalar("name")
        or _text(parser.texts.get("title", []))
        or _text([parser.metadata.get("og:title", "")])
    )
    offers = product.get("offers")
    offer = (
        offers
        if isinstance(offers, dict)
        else next((child for child in offers if isinstance(child, dict)), {})
        if isinstance(offers, list)
        else {}
    )
    raw_price = offer.get("price")
    price = _text(parser.texts.get("price", []))
    if (
        price is None
        and isinstance(raw_price, (str, int, float))
        and not isinstance(raw_price, bool)
    ):
        price = str(raw_price)
    currency = offer.get("priceCurrency")
    condition = _text(parser.texts.get("condition", [])) or scalar("itemCondition")
    description = scalar("description") or _text(parser.texts.get("description", []))
    if description is not None:
        try:
            description = extract_ebay_description(description)
        except ValueError:
            description = None
            issues.append({"kind": "description_challenge"})
    urls = [*_strings(product.get("image")), *parser.gallery]
    if not urls and parser.metadata.get("og:image"):
        urls.append(parser.metadata["og:image"])
    gallery: list[str] = []
    for raw in urls:
        url = urljoin(effective_url, raw)
        if not raw or not _allowed_url(url, "ebayimg.com"):
            issues.append({"kind": "invalid_gallery_url"})
        elif url not in gallery:
            if len(gallery) < 50:
                gallery.append(url)
            else:
                issues.append({"kind": "gallery_limit"})
                break
    description_url = None
    for raw in parser.descriptions:
        url = urljoin(effective_url, raw)
        if raw and _allowed_url(url, "ebaydesc.com"):
            description_url = url
            break
        issues.append({"kind": "invalid_description_url"})
    if not title:
        return classified(EbayItemResponseKind.UNRECOGNIZED)
    return EbayItemExtraction(
        classification=EbayItemResponseKind.DETAIL,
        item_identifier=item_identifier,
        title=title,
        displayed_price=price,
        currency=currency if isinstance(currency, str) else None,
        condition=condition,
        description=description,
        description_url=description_url,
        gallery_urls=tuple(gallery),
        issues=tuple(issues),
    )


def extract_ebay_description(html: str) -> str:
    parser = _ItemParser()
    parser.feed(html)
    parser.close()
    visible = _text(parser.visible) or ""
    if any(marker in visible.casefold() for marker in _CHALLENGE):
        raise ValueError("eBay description response contains a challenge")
    return visible
