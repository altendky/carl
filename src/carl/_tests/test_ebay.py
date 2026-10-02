from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import cast, final

import pytest

from carl.core.ebay import (
    COLLECT_EBAY_SEARCH_WORK_KIND,
    CollectEbaySearchPayload,
    EbayListingState,
    EbaySearchRequest,
    EbaySearchResponseKind,
    collect_ebay_search_work,
    ebay_item_identifier,
    ebay_search_plan,
    ebay_search_url,
    ebay_search_work_constraint,
    extract_ebay_search,
)
from carl.core.facebook_refresh import SearchRefreshRequest
from carl.core.http import RequestPlan
from carl.core.models import CodeProvenance, Header, JsonValue
from carl.ebay import collect_ebay_search
from carl.io.httpx import AcquiredBody, Acquisition, AcquisitionFailure, IdentifierFactory
from carl.io.sqlite import Database
from carl.review import ReviewApplication, ReviewInputError


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="dirty",
        package_version="test",
        python_implementation="test",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


@final
class _Acquirer:
    def __init__(self, html: bytes, *, status_code: int = 200):
        self.html = html
        self.status_code = status_code
        self.plan: RequestPlan | None = None

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        self.plan = plan
        body_identifier = new_identifier()
        return Acquisition(
            record={
                "request_plan": plan.as_json(),
                "authentication": {
                    "target": "anonymous_cookie_session",
                    "network": {"kind": "proxy_basic"},
                },
                "transport": {
                    "implementation": "wreq",
                    "implementation_version": "test",
                    "emulation_profile": "Chrome153",
                },
                "routing": {
                    "configured": list(plan.routing),
                    "observed": {"provider": "decodo", "state": "closed"},
                },
                "hops": [
                    {
                        "request": {"method": "GET", "url": plan.url},
                        "response": {
                            "status_code": self.status_code,
                            "url": plan.url,
                            "headers": [
                                {
                                    "name_latin1": "Content-Type",
                                    "value_latin1": "text/html; charset=utf-8",
                                }
                            ],
                            "body": {
                                "state": "available",
                                "artifact_id": body_identifier,
                                "bytes": len(self.html),
                            },
                        },
                    }
                ],
                "effective_url": plan.url,
                "stopping_condition": "terminal_response",
                "collection_completeness": "complete",
            },
            bodies=(
                AcquiredBody(
                    identifier=body_identifier,
                    media_type="text/html; charset=utf-8",
                    representation={
                        "kind": "content_decoded_http_body",
                        "content_decoded": True,
                        "exact_wire_bytes": False,
                    },
                    content=self.html,
                ),
            ),
        )


@final
class _PagingAcquirer:
    def __init__(self, pages: tuple[bytes, ...]):
        self.pages = iter(pages)
        self.plans: list[RequestPlan] = []

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        self.plans.append(plan)
        return await _Acquirer(next(self.pages)).acquire(plan, new_identifier)


def _identifiers() -> Callable[[], str]:
    values = iter(f"identifier-{index}" for index in range(100))
    return lambda: next(values)


def test_ebay_search_request_and_url_are_explicit() -> None:
    request = EbaySearchRequest(query="bench oscilloscope")

    assert ebay_search_url(request.query) == (
        "https://www.ebay.com/sch/i.html?_nkw=bench+oscilloscope"
    )
    assert ebay_search_url(request.query, page_number=2) == (
        "https://www.ebay.com/sch/i.html?_nkw=bench+oscilloscope&_pgn=2"
    )
    assert ebay_search_plan(request, network_path=("decodo", "personal", "carl")).routing == (
        "decodo",
        "personal",
        "carl",
    )


@pytest.mark.parametrize("listing_state", ("active", "sold", "completed"))
@pytest.mark.parametrize("page_number", (1, 2, 3))
def test_pagination_referer_names_preceding_page(
    listing_state: EbayListingState, page_number: int
) -> None:
    request = EbaySearchRequest(query="32 mm Plössl & eyepiece", listing_state=listing_state)
    plan = ebay_search_plan(request, network_path=("test",), page_number=page_number)
    assert plan.headers == (
        (
            Header(
                name=b"Referer",
                value=ebay_search_url(
                    request.query, page_number=page_number - 1, listing_state=listing_state
                ).encode("ascii"),
            ),
        )
        if page_number > 1
        else ()
    )


def test_sold_search_request_filters_every_page_and_has_distinct_work() -> None:
    active = EbaySearchRequest(query="morpheus eyepiece")
    sold = EbaySearchRequest(query=active.query, listing_state="sold")
    assert active.listing_state == "active"
    for page in (1, 2, 5):
        url = ebay_search_plan(sold, network_path=("test",), page_number=page).url
        assert "LH_Sold=1&LH_Complete=1" in url
        assert "_pgn=" not in url if page == 1 else f"_pgn={page}" in url
    identities = tuple(
        collect_ebay_search_work(
            identifier="test",
            payload=CollectEbaySearchPayload(request=request),
            not_before_utc_ns=0,
        ).deduplication_identity
        for request in (active, sold)
    )
    assert identities[0] != identities[1]
    with pytest.raises(ValueError):
        EbaySearchRequest.model_validate({"query": "scope", "listing_state": "invalid"})


@pytest.mark.parametrize("card_class", ["s-item", "s-card"])
@pytest.mark.parametrize(
    "label,normalized",
    [
        ("Sold Sep 29, 2026", date(2026, 9, 29)),
        ("Sold 29 September 2026", date(2026, 9, 29)),
        ("Sold Sep 29", None),
        ("Sold Feb 30, 2026", None),
    ],
)
def test_sold_cards_preserve_price_and_source_date(
    card_class: str, label: str, normalized: date | None
) -> None:
    extraction = extract_ebay_search(
        f'''<li class="{card_class}"><a href="/itm/256123456789"></a>
        <div class="{card_class}__title">Morpheus</div><span>{label}</span>
        <span class="{card_class}__price">$180.00</span><span>Used</span><span>+$8 shipping</span></li>''',
        listing_state="sold",
    )
    (occurrence,) = extraction.listing_occurrences
    assert occurrence.listing_state == "sold"
    assert occurrence.sold_price == "$180.00"
    assert occurrence.sold_price_status == "displayed"
    assert occurrence.sold_date == normalized
    assert occurrence.sold_date_text == label.removeprefix("Sold ")
    assert occurrence.condition == "Used" and occurrence.shipping_text == "+$8 shipping"


@pytest.mark.parametrize(
    "price_markup,offer,status",
    [
        (" <s>$180.00</s>", "", "unavailable"),
        ("$180.00", "<span>Best offer accepted</span>", "best_offer_accepted"),
    ],
)
def test_hidden_offer_prices_are_not_known_sale_amounts(
    price_markup: str, offer: str, status: str
) -> None:
    (occurrence,) = extract_ebay_search(
        f"""<li class="s-card"><a href="/itm/256123456789"></a>
        <span>Sold Sep 29, 2026</span><span class="s-card__price">{price_markup}</span>{offer}</li>""",
        listing_state="sold",
    ).listing_occurrences
    assert occurrence.displayed_price == "$180.00"
    assert occurrence.sold_price is None and occurrence.sold_price_status == status


def test_sold_filter_does_not_turn_unmarked_related_cards_into_sales() -> None:
    extraction = extract_ebay_search(
        """<li class="s-card"><a href="/itm/256123456789"></a><div class="s-card__title">Sold as is telescope</div><span class="s-card__price">$100.00</span><span>4 sold</span></li>""",
        listing_state="sold",
    )
    (occurrence,) = extraction.listing_occurrences
    assert occurrence.listing_state is None and occurrence.sold_price is None
    assert occurrence.sold_date is None and occurrence.sold_date_text is None
    assert extraction.issues == (
        {"kind": "sold_result_without_sale_marker", "item_identifier": "256123456789"},
    )


@pytest.mark.parametrize(
    "title", ["Sold September 12, 2026 Rare Eyepiece Catalog", "Best offer accepted slogan mug"]
)
def test_sale_phrases_in_titles_are_not_sale_evidence(title: str) -> None:
    for title_markup in (
        f'<div class="s-card__title">{title}</div>',
        f'<a href="/itm/256123456789">{title}</a>',
    ):
        (occurrence,) = extract_ebay_search(
            f'<li class="s-card"><a href="/itm/256123456789"></a>{title_markup}<span class="s-card__price">$20.00</span></li>'
        ).listing_occurrences
        assert occurrence.listing_state == "active" and occurrence.sold_price is None
        assert occurrence.sold_date is None and occurrence.sold_price_status is None


@pytest.mark.anyio
async def test_offline_sold_collection_retains_card_evidence_and_rejects_new_refresh(
    tmp_path: Path,
) -> None:
    def card(item: str, price: str) -> bytes:
        return f'<li class="s-card"><a href="/itm/{item}"></a><span>Sold Sep 29, 2026</span><span class="s-card__price">{price}</span></li>'.encode()

    pages = (
        card("256123456789", "$180.00")
        + b'<a class="pagination__next" href="/sch/i.html?_pgn=2">Next</a>',
        card("256123456788", "$175.00"),
    )
    acquirer = _PagingAcquirer(pages)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        result = await collect_ebay_search(
            database,
            request=EbaySearchRequest(query="Morpheus", listing_state="sold", maximum_pages=2),
            network_path=("test",),
            acquirer=acquirer,
            new_identifier=_identifiers(),
            provenance=_provenance(),
        )
        assert all("LH_Sold=1&LH_Complete=1" in plan.url for plan in acquirer.plans)
        occurrences = await database.records_by_kind(("carl", "ebay", "search_listing_occurrence"))
        assert {value["sold_price"] for _, value in occurrences} == {"$180.00", "$175.00"}
        assert all(
            value["sold_date"] == "2026-09-29" and value["acquisition_record_identifier"]
            for _, value in occurrences
        )

        async def provenance() -> CodeProvenance:
            return _provenance()

        app = ReviewApplication(database, tmp_path, code_provenance=provenance)
        summaries = await app.list_search_runs(query="Morpheus")
        assert summaries.search_runs[0].listing_state == "sold"
        with pytest.raises(
            ReviewInputError, match="does not presently support eBay sold/completed"
        ):
            await app.request_search_refresh(
                SearchRefreshRequest(
                    base_search_run_record_identifier=str(result["search_run_record_identifier"])
                )
            )


def test_ebay_search_work_is_deduplicated_and_serialized_by_kind() -> None:
    payload = CollectEbaySearchPayload(request=EbaySearchRequest(query="bench oscilloscope"))

    first = collect_ebay_search_work(
        identifier="work-1",
        payload=payload,
        not_before_utc_ns=0,
    )
    second = collect_ebay_search_work(
        identifier="work-2",
        payload=payload,
        not_before_utc_ns=0,
    )
    constraint = ebay_search_work_constraint()

    assert first.kind == COLLECT_EBAY_SEARCH_WORK_KIND
    assert first.payload == payload.as_json()
    assert first.deduplication_identity == second.deduplication_identity
    assert constraint.scope.identity == COLLECT_EBAY_SEARCH_WORK_KIND
    assert constraint.maximum_active == 1


def test_ebay_item_identifier_accepts_canonical_and_slugged_urls() -> None:
    assert ebay_item_identifier("https://www.ebay.com/itm/256123456789") == "256123456789"
    assert (
        ebay_item_identifier("https://www.ebay.com/itm/Test-Instrument/256987654321?hash=item")
        == "256987654321"
    )
    assert ebay_item_identifier("https://example.com/itm/256123456789") is None


def test_extracts_old_and_new_search_card_shapes_without_tracking_urls() -> None:
    extraction = extract_ebay_search(
        """
        <html><body><ul>
          <li class="s-card">
            <a class="s-card__link"
               href="https://www.ebay.com/itm/Bench-Scope/256123456789?hash=tracking">
              <img data-src="https://i.ebayimg.com/images/one.jpg" alt="Bench Scope">
            </a>
            <div class="s-card__title"><span>Bench Scope</span></div>
            <span class="s-card__price">$49.95</span>
            <span class="s-card__shipping">Free shipping</span>
            <span class="s-card__condition">Used</span>
            <span aria-hidden="true">derosnopS</span>
          </li>
          <li class="s-item">
            <a href="https://www.ebay.com/itm/256987654321">
              <img class="s-item__image-img" src="https://i.ebayimg.com/images/two.jpg">
            </a>
            <div class="s-item__title">Portable Scope</div>
            <span class="s-item__price">$89.00</span>
            <span class="su-styled-text secondary large">+$8 shipping</span>
            <span class="su-styled-text secondary large">Open box</span>
          </li>
        </ul></body></html>
        """
    )

    assert extraction.response_classification.kind is EbaySearchResponseKind.USABLE_RESULTS
    assert [item.item_identifier for item in extraction.listing_occurrences] == [
        "256123456789",
        "256987654321",
    ]
    first, second = extraction.listing_occurrences
    assert first.canonical_url == "https://www.ebay.com/itm/256123456789"
    assert first.title == "Bench Scope"
    assert first.displayed_price == "$49.95"
    assert first.shipping_text == "Free shipping"
    assert first.condition == "Used"
    assert first.promoted is True
    assert second.title == "Portable Scope"
    assert second.shipping_text == "+$8 shipping"
    assert second.condition == "Open box"
    assert second.promoted is False


def test_extracts_valid_ebay_next_page_number() -> None:
    extraction = extract_ebay_search(
        """
        <html><body>
          <ul><li class="s-card">
            <a href="https://www.ebay.com/itm/256123456789"></a>
            <div class="s-card__title">Bench Scope</div>
          </li></ul>
          <a class="pagination__next" aria-label="Go to next page"
             href="https://www.ebay.com/sch/i.html?_nkw=scope&amp;_pgn=2">Next</a>
        </body></html>
        """
    )

    assert extraction.next_page_number == 2


def test_challenge_wins_over_incidental_item_links() -> None:
    extraction = extract_ebay_search(
        """
        <html><title>Pardon Our Interruption</title><body>
          <a href="https://www.ebay.com/itm/256123456789">unrelated link</a>
        </body></html>
        """
    )

    assert extraction.response_classification.kind is EbaySearchResponseKind.CHALLENGE
    assert extraction.listing_occurrences == ()


def test_distinguishes_empty_and_unrecognized_pages() -> None:
    empty = extract_ebay_search("<html><body>0 results for impossible query</body></html>")
    unknown = extract_ebay_search("<html><body>ordinary account page</body></html>")

    assert empty.response_classification.kind is EbaySearchResponseKind.EMPTY_RESULTS
    assert unknown.response_classification.kind is EbaySearchResponseKind.UNRECOGNIZED
    assert unknown.issues == ({"kind": "no_recognized_search_result_structure"},)


@pytest.mark.parametrize("status_code", [200, 403])
def test_classifies_retained_ebay_error_page(status_code: int) -> None:
    extraction = extract_ebay_search(
        """<html><head><title>Error Page | eBay</title></head><body>
        <h1>SORRY</h1><h2>Something went wrong on our end</h2>
        <p>Please go back and try again or go to eBay Homepage.</p>
        </body></html>""",
        status_code=status_code,
    )
    assert extraction.response_classification.kind is EbaySearchResponseKind.ERROR_PAGE
    assert extraction.response_classification.evidence == (
        "error page | ebay",
        "something went wrong on our end",
        f"http_status_{status_code}",
    )
    assert extraction.listing_occurrences == ()
    assert extraction.next_page_number is None


@pytest.mark.parametrize("status_code", [403, 429, 500])
def test_non_success_http_status_never_establishes_usable_or_empty_results(
    status_code: int,
) -> None:
    for html in (
        '<li class="s-card"><a href="https://www.ebay.com/itm/256123456789">Scope</a></li>',
        "<p>0 results for impossible query</p>",
    ):
        extraction = extract_ebay_search(html, status_code=status_code)
        assert extraction.response_classification.kind is EbaySearchResponseKind.HTTP_ERROR
        assert extraction.response_classification.evidence == (f"http_status_{status_code}",)
        assert extraction.listing_occurrences == ()


@pytest.mark.anyio
async def test_error_page_is_retained_but_collection_does_not_claim_success(tmp_path: Path) -> None:
    html = b"<title>Error Page | eBay</title><h1>Something went wrong on our end</h1>"
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        result = await collect_ebay_search(
            database,
            request=EbaySearchRequest(query="baader morpheus eyepiece"),
            network_path=("decodo", "personal", "carl"),
            acquirer=_Acquirer(html, status_code=403),
            new_identifier=_identifiers(),
            provenance=_provenance(),
        )
        assert result["state"] == "response_failed"
        assert result["stopping_reason"] == "error_page"
        assert result["listing_count"] == 0
        assert result["page_count"] == 1
        assert result["response_classification"]["kind"] == "error_page"
        runs = await database.records_by_kind(("carl", "ebay", "search_run"))
        assert len(runs) == 1
        assert runs[0][1]["stopping_reason"] == "error_page"
        acquisitions = await database.records_by_kind(("carl", "http", "acquisition"))
        assert len(acquisitions) == 1


def test_incidental_item_links_do_not_establish_search_results() -> None:
    extraction = extract_ebay_search(
        '<html><body>Account <a href="/itm/256123456789">Recently viewed</a></body></html>'
    )
    assert extraction.response_classification.kind is EbaySearchResponseKind.UNRECOGNIZED
    assert extraction.listing_occurrences == ()


def test_nonvisible_challenge_text_does_not_override_real_cards() -> None:
    extraction = extract_ebay_search(
        """
        <script>const message = 'pardon our interruption';</script>
        <style>/* verify you are human */</style>
        <template>pardon our interruption</template>
        <li class="s-card"><a href="/itm/256123456789"></a>
        <div class="s-card__title">Bench Scope<script>secret title</script></div></li>
        """
    )
    assert extraction.response_classification.kind is EbaySearchResponseKind.USABLE_RESULTS
    assert extraction.listing_occurrences[0].title == "Bench Scope"


def test_template_cards_and_pagination_are_not_visible_results() -> None:
    extraction = extract_ebay_search(
        """
        <template>
          <li class="s-card"><a href="/itm/256123456789">Template card</a></li>
          <a class="pagination__next" href="/sch/i.html?_pgn=2">Next</a>
        </template>
        <body>0 results for impossible query</body>
        """
    )
    assert extraction.response_classification.kind is EbaySearchResponseKind.EMPTY_RESULTS
    assert extraction.listing_occurrences == ()
    assert extraction.next_page_number is None


@pytest.mark.anyio
async def test_failed_acquisition_publishes_completed_redirect_bodies(tmp_path: Path) -> None:
    class FailingAcquirer:
        async def acquire(
            self, plan: RequestPlan, new_identifier: IdentifierFactory
        ) -> Acquisition:
            acquisition = await _Acquirer(b"retained redirect").acquire(plan, new_identifier)
            raise AcquisitionFailure(
                "redirect limit", result=acquisition.record, bodies=acquisition.bodies
            )

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        with pytest.raises(AcquisitionFailure) as raised:
            await collect_ebay_search(
                database,
                request=EbaySearchRequest(query="scope"),
                network_path=("decodo", "personal", "carl"),
                acquirer=FailingAcquirer(),
                new_identifier=_identifiers(),
                provenance=_provenance(),
            )
        operation = await database.operation("identifier-1")
        assert operation["state"] == "failed"
        body_identifier = raised.value.bodies[0].identifier
        _, content = await database.get_artifact(body_identifier)
        assert content == b"retained redirect"
        assert await database.operation_output_count("identifier-1") == 1


@pytest.mark.anyio
async def test_search_without_next_page_stops_after_one_page(
    tmp_path: Path,
) -> None:
    html = b"""
    <html><body><ul><li class="s-card">
      <a href="https://www.ebay.com/itm/256123456789"></a>
      <div class="s-card__title">Bench Scope</div>
      <span class="s-card__price">$49.95</span>
    </li></ul></body></html>
    """
    acquirer = _Acquirer(html)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        result = await collect_ebay_search(
            database,
            request=EbaySearchRequest(query="bench oscilloscope"),
            network_path=("decodo", "personal", "carl"),
            acquirer=acquirer,
            new_identifier=_identifiers(),
            provenance=_provenance(),
        )

        acquisitions = await database.records_by_kind(("carl", "http", "acquisition"))
        extractions = await database.records_by_kind(("carl", "ebay", "search_extraction"))
        occurrences = await database.records_by_kind(("carl", "ebay", "search_listing_occurrence"))
        search_runs = await database.records_by_kind(("carl", "ebay", "search_run"))
        extraction = extractions[0][1]
        assert isinstance(extraction, dict)
        extraction_value = cast(dict[str, JsonValue], extraction)
        acquisition_value = acquisitions[0][1]
        assert isinstance(acquisition_value, dict)
        typed_acquisition = cast(dict[str, JsonValue], acquisition_value)
        hops = typed_acquisition.get("hops")
        assert isinstance(hops, list) and hops and isinstance(hops[0], dict)
        response = cast(dict[str, JsonValue], hops[0]).get("response")
        assert isinstance(response, dict)
        body = cast(dict[str, JsonValue], response).get("body")
        assert isinstance(body, dict)
        body_identifier = cast(dict[str, JsonValue], body).get("artifact_id")
        assert isinstance(body_identifier, str)
        body_metadata, body_content = await database.get_artifact(body_identifier)
        decoded_metadata, decoded_content = await database.get_artifact(
            cast(str, extraction_value["decoded_body_artifact_identifier"])
        )

    assert acquirer.plan is not None
    assert acquirer.plan.routing == ("decodo", "personal", "carl")
    assert len(acquisitions) == len(extractions) == len(occurrences) == len(search_runs) == 1
    assert result["listing_count"] == 1
    assert result["response_classification"]["kind"] == "usable_results"
    assert occurrences[0][1]["item_identifier"] == "256123456789"
    assert search_runs[0][1]["stopping_reason"] == "no_next_page"
    assert search_runs[0][1]["page_count"] == 1
    assert body_metadata["representation"]["kind"] == "content_decoded_http_body"
    assert body_content == html
    assert decoded_metadata["representation"]["kind"] == "content_decoded_utf8_text"
    assert decoded_content == html


@pytest.mark.anyio
async def test_search_follows_next_page_until_explicit_maximum(tmp_path: Path) -> None:
    first = b"""
    <html><body><ul><li class="s-card">
      <a href="https://www.ebay.com/itm/256123456789"></a>
      <div class="s-card__title">Bench Scope</div>
    </li></ul>
    <a class="pagination__next"
       href="https://www.ebay.com/sch/i.html?_nkw=scope&amp;_pgn=2">Next</a>
    </body></html>
    """
    second = b"""
    <html><body><ul>
    <li class="s-card">
      <a href="https://www.ebay.com/itm/256123456789"></a>
      <div class="s-card__title">Repeated Bench Scope</div>
    </li>
    <li class="s-card">
      <a href="https://www.ebay.com/itm/256987654321"></a>
      <div class="s-card__title">Portable Scope</div>
    </li>
    </ul>
    <a aria-label="Go to next page"
       href="/sch/i.html?_nkw=scope&amp;_pgn=3">Next</a>
    </body></html>
    """
    acquirer = _PagingAcquirer((first, second))
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        result = await collect_ebay_search(
            database,
            request=EbaySearchRequest(query="scope", maximum_pages=2),
            network_path=("decodo", "personal", "carl"),
            acquirer=acquirer,
            new_identifier=_identifiers(),
            provenance=_provenance(),
        )

        acquisitions = await database.records_by_kind(("carl", "http", "acquisition"))
        extractions = await database.records_by_kind(("carl", "ebay", "search_extraction"))
        occurrences = await database.records_by_kind(("carl", "ebay", "search_listing_occurrence"))
        search_runs = await database.records_by_kind(("carl", "ebay", "search_run"))

    assert [plan.url for plan in acquirer.plans] == [
        "https://www.ebay.com/sch/i.html?_nkw=scope",
        "https://www.ebay.com/sch/i.html?_nkw=scope&_pgn=2",
    ]
    assert acquirer.plans[0].headers == ()
    assert acquirer.plans[1].headers == (
        Header(name=b"Referer", value=acquirer.plans[0].url.encode("ascii")),
    )
    assert len(acquisitions) == len(extractions) == 2
    assert len(occurrences) == 3
    assert len(search_runs) == 1
    assert result["page_count"] == 2
    assert result["listing_count"] == 2
    assert result["listing_occurrence_count"] == 3
    assert result["stopping_reason"] == "maximum_pages"
    run = search_runs[0][1]
    assert isinstance(run, dict)
    run_value = cast(dict[str, JsonValue], run)
    assert run_value["page_count"] == 2
    assert run_value["stopping_reason"] == "maximum_pages"
    pages = run_value["pages"]
    assert isinstance(pages, list)
    page_values = cast(list[JsonValue], pages)
    typed_pages = [
        cast(dict[str, JsonValue], page) for page in page_values if isinstance(page, dict)
    ]
    assert len(typed_pages) == len(page_values)
    assert [page["page_number"] for page in typed_pages] == [1, 2]
    assert [occurrence[1]["page_ordinal"] for occurrence in occurrences] == [1, 2, 2]


@pytest.mark.anyio
async def test_three_page_search_retains_each_preceding_page_referer(tmp_path: Path) -> None:
    html_pages = tuple(
        (
            f'<li class="s-card"><a href="/itm/25612345678{page}">Scope</a></li>'
            + (
                f'<a class="pagination__next" href="/sch/i.html?_pgn={page + 1}">Next</a>'
                if page < 3
                else ""
            )
        ).encode()
        for page in range(1, 4)
    )
    acquirer = _PagingAcquirer(html_pages)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        result = await collect_ebay_search(
            database,
            request=EbaySearchRequest(query="scope", maximum_pages=4),
            network_path=("test",),
            acquirer=acquirer,
            new_identifier=_identifiers(),
            provenance=_provenance(),
        )
        acquisitions = await database.records_by_kind(("carl", "http", "acquisition"))

    assert result["page_count"] == 3
    assert result["stopping_reason"] == "no_next_page"
    retained = sorted((value for _, value in acquisitions), key=lambda value: value["page_number"])
    assert retained[0]["request_plan"]["headers"] == []
    for index in (1, 2):
        expected = Header(name=b"Referer", value=acquirer.plans[index - 1].url.encode("ascii"))
        assert acquirer.plans[index].headers == (expected,)
        assert retained[index]["request_plan"]["headers"] == [expected.as_json()]
