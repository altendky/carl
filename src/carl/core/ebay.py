"""Pure request construction and bounded eBay search-page extraction."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import date
from html.parser import HTMLParser
from typing import Literal, override
from urllib.parse import parse_qs, urlencode, urlsplit

from pydantic import Field, field_validator

from carl.core.http import RequestPlan
from carl.core.models import Header, JsonStringEnumeration, JsonValue, StrictModel
from carl.core.work import (
    ConcurrencyConstraint,
    ConstraintKind,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    WorkDefinition,
)

COLLECT_EBAY_SEARCH_WORK_KIND = ("carl", "ebay", "collect", "search")
LEGACY_COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION = 1
COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION = 2

type EbayListingState = Literal["active", "sold", "completed"]

_ITEM_IDENTIFIER = re.compile(r"^[0-9]{9,15}$")
_SPACE = re.compile(r"\s+")
_VOID_ELEMENTS = frozenset(
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
_CHALLENGE_MARKERS = (
    "pardon our interruption",
    "checking your browser before accessing",
    "to keep ebay a safe place to buy and sell",
    "we've detected unusual activity",
)
_EMPTY_MARKERS = (
    "0 results for",
    "no exact matches found",
    "we couldn't find any results",
    "please check your spelling or use different words",
)
_ERROR_MARKERS = (
    "error page | ebay",
    "something went wrong on our end",
)
_CONDITION_TEXT = frozenset(
    {
        "brand new",
        "certified - refurbished",
        "certified refurbished",
        "excellent - refurbished",
        "for parts or not working",
        "good - refurbished",
        "new",
        "new other",
        "new with defects",
        "open box",
        "parts only",
        "pre-owned",
        "seller refurbished",
        "used",
        "very good - refurbished",
    }
)


class EbaySearchResponseKind(JsonStringEnumeration):
    USABLE_RESULTS = "usable_results"
    EMPTY_RESULTS = "empty_results"
    CHALLENGE = "challenge"
    ERROR_PAGE = "error_page"
    HTTP_ERROR = "http_error"
    UNRECOGNIZED = "unrecognized"


EBAY_SEARCH_FAILURE_KINDS = frozenset(
    {
        EbaySearchResponseKind.CHALLENGE,
        EbaySearchResponseKind.ERROR_PAGE,
        EbaySearchResponseKind.HTTP_ERROR,
        EbaySearchResponseKind.UNRECOGNIZED,
    }
)


class EbaySearchRequest(StrictModel):
    query: str = Field(min_length=1, max_length=300)
    listing_state: EbayListingState = Field(
        default="active",
        description="Use active for new acquisitions. Sold/completed are retained historical modes only: Carl's anonymous acquisition does not support closed-listing sign-in/challenge gating.",
    )
    stack_identifier: str = Field(
        default="ebay_anonymous",
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$",
    )
    maximum_pages: int = Field(default=5, ge=1, le=20)

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("eBay search query must be trimmed")
        return value


class CollectEbaySearchPayload(StrictModel):
    request: EbaySearchRequest
    retry_attempt_offset: int = Field(default=0, ge=0)

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


def collect_ebay_search_work(
    *, identifier: str, payload: CollectEbaySearchPayload, not_before_utc_ns: int
) -> WorkDefinition:
    digest = hashlib.sha256(
        payload.model_dump_json(exclude={"retry_attempt_offset"}).encode("utf-8")
    ).hexdigest()
    return WorkDefinition(
        identifier=identifier,
        kind=COLLECT_EBAY_SEARCH_WORK_KIND,
        payload_schema_version=COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
        payload=payload.as_json(),
        deduplication_identity=("ebay", "search", digest),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND,
                identity=COLLECT_EBAY_SEARCH_WORK_KIND,
            ),
            SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_PATH,
                identity=("ebay", "search", payload.request.stack_identifier),
            ),
        ),
    )


def ebay_search_work_constraint() -> ConcurrencyConstraint:
    return ConcurrencyConstraint(
        kind=ConstraintKind.CONCURRENCY,
        identifier=("carl", "constraint", "ebay", "search", "serial"),
        subject_kind=SchedulingSubjectKind.WORK_ITEM,
        scope=SchedulingScope(
            kind=SchedulingScopeKind.WORK_KIND,
            identity=COLLECT_EBAY_SEARCH_WORK_KIND,
        ),
        maximum_active=1,
    )


class EbayListingOccurrence(StrictModel):
    item_identifier: str = Field(pattern=r"^[0-9]{9,15}$")
    position: int = Field(ge=1)
    canonical_url: str = Field(pattern=r"^https://www\.ebay\.com/itm/[0-9]{9,15}$")
    title: str | None = None
    displayed_price: str | None = None
    listing_state: EbayListingState | None = "active"
    sold_price: str | None = None
    sold_date: date | None = None
    sold_date_text: str | None = None
    sold_price_status: Literal["displayed", "best_offer_accepted", "unavailable"] | None = None
    shipping_text: str | None = None
    condition: str | None = None
    image_url: str | None = None
    promoted: bool


class EbaySearchResponseClassification(StrictModel):
    kind: EbaySearchResponseKind
    evidence: tuple[str, ...] = ()


class EbaySearchExtraction(StrictModel):
    response_classification: EbaySearchResponseClassification
    listing_occurrences: tuple[EbayListingOccurrence, ...]
    next_page_number: int | None = Field(default=None, ge=2)
    issues: tuple[JsonValue, ...] = ()


def ebay_search_url(
    query: str, *, page_number: int = 1, listing_state: EbayListingState = "active"
) -> str:
    if page_number < 1:
        raise ValueError("eBay search page number must be positive")
    request = EbaySearchRequest(query=query, listing_state=listing_state)
    parameters: dict[str, str | int] = {"_nkw": request.query}
    if request.listing_state == "sold":
        parameters.update(LH_Sold=1, LH_Complete=1)
    elif request.listing_state == "completed":
        parameters["LH_Complete"] = 1
    if page_number > 1:
        parameters["_pgn"] = page_number
    return "https://www.ebay.com/sch/i.html?" + urlencode(parameters)


def ebay_search_plan(
    request: EbaySearchRequest,
    *,
    network_path: tuple[str, ...],
    page_number: int = 1,
) -> RequestPlan:
    return RequestPlan(
        url=ebay_search_url(
            request.query, page_number=page_number, listing_state=request.listing_state
        ),
        headers=(
            (
                Header(
                    name=b"Referer",
                    value=ebay_search_url(
                        request.query,
                        page_number=page_number - 1,
                        listing_state=request.listing_state,
                    ).encode("ascii"),
                ),
            )
            if page_number > 1
            else ()
        ),
        routing=network_path,
    )


def ebay_item_identifier(url: str) -> str | None:
    try:
        parsed = urlsplit(url)
    except ValueError:
        return None
    hostname = (parsed.hostname or "").lower()
    if hostname and hostname != "ebay.com" and not hostname.endswith(".ebay.com"):
        return None
    parts = tuple(part for part in parsed.path.split("/") if part)
    try:
        item_index = parts.index("itm")
    except ValueError:
        item_index = -1
    if item_index >= 0:
        for part in parts[item_index + 1 :]:
            if _ITEM_IDENTIFIER.fullmatch(part):
                return part
    query = parse_qs(parsed.query)
    for name in ("item", "itemid"):
        values = query.get(name, ())
        if values and _ITEM_IDENTIFIER.fullmatch(values[0]):
            return values[0]
    return None


def _clean_text(parts: list[str]) -> str | None:
    value = _SPACE.sub(" ", " ".join(parts)).strip()
    return value or None


def _shipping_text(parts: list[str]) -> str | None:
    return next(
        (part for part in parts if "shipping" in part.lower() or "delivery" in part.lower()),
        None,
    )


def _condition_text(parts: list[str]) -> str | None:
    return next((part for part in parts if part.lower() in _CONDITION_TEXT), None)


def _class_tokens(attributes: dict[str, str]) -> frozenset[str]:
    return frozenset(attributes.get("class", "").split())


def _field_names(tokens: frozenset[str]) -> frozenset[str]:
    lowered = {token.lower() for token in tokens}
    fields: set[str] = set()
    if any(
        token in {"s-item__title", "s-card__title"}
        or token.endswith("__title")
        or token.endswith("-title")
        for token in lowered
    ):
        fields.add("title")
    if any("price" in token for token in lowered):
        fields.add("price")
    if any("shipping" in token or "logisticscost" in token for token in lowered):
        fields.add("shipping")
    if any(
        token in {"secondary_info", "s-item__subtitle", "s-card__condition"} or "condition" in token
        for token in lowered
    ):
        fields.add("condition")
    if any("sponsor" in token or "promoted" in token or "ad-badge" in token for token in lowered):
        fields.add("promoted")
    if any(
        "caption" in token
        or "endtime" in token
        or "title--tagblock" in token
        or "sold-date" in token
        for token in lowered
    ):
        fields.add("sale_metadata")
    return frozenset(fields)


@dataclass(slots=True)
class _Card:
    item_identifier: str | None = None
    source_url: str | None = None
    image_url: str | None = None
    image_alt: str | None = None
    title: list[str] = field(default_factory=list)
    price: list[str] = field(default_factory=list)
    shipping: list[str] = field(default_factory=list)
    condition: list[str] = field(default_factory=list)
    visible_text: list[str] = field(default_factory=list)
    sale_text: list[str] = field(default_factory=list)
    promoted: bool = False
    crossed_out_price: bool = False


@dataclass(frozen=True, slots=True)
class _Frame:
    tag: str
    fields: frozenset[str]
    starts_card: bool


class _EbaySearchParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.frames: list[_Frame] = []
        self.card: _Card | None = None
        self.cards: list[_Card] = []
        self.next_page_numbers: list[int] = []
        self.document_text: list[str] = []

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if any(frame.tag in {"script", "style", "template"} for frame in self.frames):
            if tag not in _VOID_ELEMENTS:
                self.frames.append(_Frame(tag=tag, fields=frozenset(), starts_card=False))
            return
        attributes = {name.lower(): value or "" for name, value in attrs}
        tokens = _class_tokens(attributes)
        if tag == "a" and attributes.get("aria-disabled", "").lower() != "true":
            pagination_marker = (
                "pagination__next" in {token.lower() for token in tokens}
                or "next page" in attributes.get("aria-label", "").lower()
                or "next" in attributes.get("rel", "").lower().split()
            )
            if pagination_marker and (href := attributes.get("href")):
                valid_target = False
                page_number = 0
                try:
                    parsed = urlsplit(href)
                    hostname = (parsed.hostname or "").lower()
                    page_values = parse_qs(parsed.query).get("_pgn", ())
                    page_number = int(page_values[0]) if page_values else 0
                    valid_target = (
                        not hostname or hostname == "ebay.com" or hostname.endswith(".ebay.com")
                    ) and parsed.path.startswith("/sch/")
                except (ValueError, IndexError):
                    pass
                if valid_target and page_number >= 2:
                    self.next_page_numbers.append(page_number)
        starts_card = (
            self.card is None and tag == "li" and bool(tokens.intersection({"s-item", "s-card"}))
        )
        if starts_card:
            self.card = _Card()
        card = self.card
        if tag == "a" and (href := attributes.get("href")):
            identifier = ebay_item_identifier(href)
            if identifier is not None and card is not None and card.item_identifier is None:
                card.item_identifier = identifier
                card.source_url = href
        if card is not None and tag == "img":
            source = next(
                (
                    attributes[name]
                    for name in ("data-src", "data-image-src", "src")
                    if attributes.get(name) and not attributes[name].startswith("data:")
                ),
                None,
            )
            if source is not None and card.image_url is None:
                card.image_url = source
            if (alt := attributes.get("alt")) and card.image_alt is None:
                card.image_alt = _SPACE.sub(" ", alt).strip() or None
        fields: frozenset[str] = _field_names(tokens) if card is not None else frozenset()
        if (
            card is not None
            and ("price" in fields or any("price" in frame.fields for frame in self.frames))
            and (
                tag in {"s", "del", "strike"}
                or any("strikethrough" in token or "strike-through" in token for token in tokens)
                or "line-through" in attributes.get("style", "").lower()
            )
        ):
            card.crossed_out_price = True
        if "promoted" in fields and card is not None:
            card.promoted = True
        if tag not in _VOID_ELEMENTS:
            self.frames.append(_Frame(tag=tag, fields=fields, starts_card=starts_card))

    @override
    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if tag not in _VOID_ELEMENTS:
            self.handle_endtag(tag)

    @override
    def handle_endtag(self, tag: str) -> None:
        matching_index = next(
            (
                index
                for index in range(len(self.frames) - 1, -1, -1)
                if self.frames[index].tag == tag
            ),
            None,
        )
        if matching_index is None:
            return
        removed = self.frames[matching_index:]
        del self.frames[matching_index:]
        if any(frame.starts_card for frame in removed) and self.card is not None:
            self.cards.append(self.card)
            self.card = None

    @override
    def handle_data(self, data: str) -> None:
        if any(frame.tag in {"script", "style", "template"} for frame in self.frames):
            return
        text = _SPACE.sub(" ", data).strip()
        if not text:
            return
        self.document_text.append(text)
        card = self.card
        if card is None:
            return
        card.visible_text.append(text)
        active_fields: set[str] = set()
        for frame in self.frames:
            active_fields.update(frame.fields)
        if "sale_metadata" in active_fields or (
            not active_fields.intersection({"title", "price", "shipping", "condition"})
            and not any(frame.tag == "a" for frame in self.frames)
        ):
            card.sale_text.append(text)
        for name in active_fields:
            if name == "promoted":
                card.promoted = True
            elif name == "title" and "sale_metadata" not in active_fields:
                card.title.append(text)
            elif name == "price":
                card.price.append(text)
            elif name == "shipping":
                card.shipping.append(text)
            elif name == "condition":
                card.condition.append(text)

    @override
    def close(self) -> None:
        super().close()
        if self.card is not None:
            self.cards.append(self.card)
            self.card = None


_MONTHS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
_MONTH_NUMBER = {
    spelling.lower(): number
    for number, month in enumerate(_MONTHS, 1)
    for spelling in (month, month[:3], *(("Sept",) if number == 9 else ()))
}
_MONTH_PATTERN = "(?:" + "|".join(sorted(_MONTH_NUMBER, key=len, reverse=True)) + ")"
_SOLD_DATE = re.compile(
    r"^Sold\s+(?P<text>" + _MONTH_PATTERN + r"\.?\s+\d{1,2}(?:,?\s+\d{4})?"
    r"|\d{1,2}\s+" + _MONTH_PATTERN + r"\.?(?:\s+\d{4})?)\b",
    re.IGNORECASE,
)
_ENDED_DATE = re.compile(_SOLD_DATE.pattern.replace("^Sold", "^Ended", 1), re.IGNORECASE)


def _sold_date(visible: str) -> tuple[str | None, date | None]:
    match = _SOLD_DATE.search(visible)
    if match is None:
        return None, None
    text = match["text"]
    parts = text.replace(",", "").replace(".", "").split()
    # Never infer a year from collection time: retain ambiguous source text instead.
    if len(parts) != 3:
        return text, None
    month, day, year = parts if not parts[0].isdigit() else (parts[1], parts[0], parts[2])
    try:
        return text, date(int(year), _MONTH_NUMBER[month.lower()], int(day))
    except (KeyError, ValueError):
        return text, None


def _occurrences(
    parser: _EbaySearchParser, listing_state: EbayListingState
) -> tuple[EbayListingOccurrence, ...]:
    result: list[EbayListingOccurrence] = []
    seen: set[str] = set()
    for card in parser.cards:
        identifier = card.item_identifier
        if identifier is None or identifier in seen:
            continue
        seen.add(identifier)
        visible = " ".join(card.visible_text).lower()
        date_text, sold_date = None, None
        for index, text in enumerate(card.sale_text):
            if text.lower().startswith("sold ") or text.lower() == "sold":
                date_text, sold_date = _sold_date(" ".join(card.sale_text[index:]))
                if date_text is not None:
                    break
        accepted_offer = "best offer accepted" in " ".join(card.sale_text).lower()
        sold = (
            date_text is not None
            or accepted_offer
            or any(text.strip().lower() == "sold" for text in card.sale_text)
        )
        ended = any(
            text.strip().lower() == "ended"
            or _ENDED_DATE.match(" ".join(card.sale_text[index:])) is not None
            for index, text in enumerate(card.sale_text)
        )
        displayed_price = _clean_text(card.price)
        title = _clean_text(card.title) or card.image_alt
        shipping = _clean_text(card.shipping) or _shipping_text(card.visible_text)
        condition = _clean_text(card.condition) or _condition_text(card.visible_text)
        result.append(
            EbayListingOccurrence(
                item_identifier=identifier,
                position=len(result) + 1,
                canonical_url=f"https://www.ebay.com/itm/{identifier}",
                title=title,
                displayed_price=displayed_price,
                listing_state=(
                    "sold"
                    if sold
                    else "completed"
                    if ended
                    else "active"
                    if listing_state == "active"
                    else None
                ),
                sold_price=(
                    displayed_price
                    if sold and not accepted_offer and not card.crossed_out_price
                    else None
                ),
                sold_date=sold_date,
                sold_date_text=date_text,
                sold_price_status=(
                    "best_offer_accepted"
                    if accepted_offer
                    else "displayed"
                    if sold and displayed_price and not card.crossed_out_price
                    else "unavailable"
                    if sold
                    else None
                ),
                shipping_text=shipping,
                condition=condition,
                image_url=card.image_url,
                promoted=(
                    card.promoted
                    or "sponsored" in visible
                    or "promoted" in visible
                    or "derosnops" in visible
                ),
            )
        )
    return tuple(result)


def extract_ebay_search(
    html: str,
    *,
    status_code: int | None = None,
    listing_state: EbayListingState = "active",
) -> EbaySearchExtraction:
    parser = _EbaySearchParser()
    parser.feed(html)
    parser.close()
    occurrences = _occurrences(parser, listing_state)
    document_text = " ".join(parser.document_text).lower()
    challenge_evidence = tuple(marker for marker in _CHALLENGE_MARKERS if marker in document_text)
    empty_evidence = tuple(marker for marker in _EMPTY_MARKERS if marker in document_text)
    error_evidence = tuple(marker for marker in _ERROR_MARKERS if marker in document_text)
    http_evidence = (f"http_status_{status_code}",) if status_code is not None else ()
    issues: tuple[JsonValue, ...] = ()
    if challenge_evidence:
        classification = EbaySearchResponseClassification(
            kind=EbaySearchResponseKind.CHALLENGE,
            evidence=challenge_evidence + http_evidence,
        )
        occurrences = ()
        next_page_number = None
    elif error_evidence or (status_code is not None and status_code != 200):
        classification = EbaySearchResponseClassification(
            kind=(
                EbaySearchResponseKind.ERROR_PAGE
                if error_evidence
                else EbaySearchResponseKind.HTTP_ERROR
            ),
            evidence=error_evidence + http_evidence,
        )
        occurrences = ()
        next_page_number = None
    elif occurrences:
        classification = EbaySearchResponseClassification(
            kind=EbaySearchResponseKind.USABLE_RESULTS,
            evidence=("ebay_item_links",),
        )
        next_page_number = min(parser.next_page_numbers, default=None)
        if listing_state in ("sold", "completed"):
            issues = tuple(
                {
                    "kind": "sold_result_without_sale_marker"
                    if listing_state == "sold"
                    else "completed_result_without_end_marker",
                    "item_identifier": occurrence.item_identifier,
                }
                for occurrence in occurrences
                if occurrence.listing_state is None
            )
    elif empty_evidence:
        classification = EbaySearchResponseClassification(
            kind=EbaySearchResponseKind.EMPTY_RESULTS,
            evidence=empty_evidence,
        )
        next_page_number = None
    else:
        classification = EbaySearchResponseClassification(
            kind=EbaySearchResponseKind.UNRECOGNIZED,
        )
        issues = ({"kind": "no_recognized_search_result_structure"},)
        next_page_number = None
    return EbaySearchExtraction(
        response_classification=classification,
        listing_occurrences=occurrences,
        next_page_number=next_page_number,
        issues=issues,
    )
