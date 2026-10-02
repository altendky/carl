import json

import pytest
from pydantic import ValidationError

from carl.core.ebay_items import (
    COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    COLLECT_EBAY_IMAGE_WORK_KIND,
    COLLECT_EBAY_ITEM_WORK_KIND,
    EXTRACT_EBAY_ITEM_WORK_KIND,
    CollectEbayDescriptionPayload,
    CollectEbayImagePayload,
    CollectEbayItemPayload,
    EbayItemRequest,
    EbayItemResponseKind,
    ExtractEbayItemPayload,
    collect_ebay_description_work,
    collect_ebay_image_work,
    collect_ebay_item_work,
    ebay_description_plan,
    ebay_image_plan,
    ebay_item_plan,
    ebay_item_work_constraints,
    extract_ebay_description,
    extract_ebay_item,
    extract_ebay_item_work,
)

ITEM = "256123456789"
URL = f"https://www.ebay.com/itm/{ITEM}"
IMAGE = "https://i.ebayimg.com/images/g/example/s-l1600.jpg"
DESCRIPTION = "https://vi.vipr.ebaydesc.com/itmdesc/256123456789"


def test_extracts_identity_bound_json_ld_product_with_offers() -> None:
    product = {
        "@type": "Product",
        "url": URL,
        "name": "Bench Scope",
        "description": "<p>Works <b>well</b>.</p>",
        "image": [IMAGE, {"@type": "ImageObject", "contentUrl": IMAGE}],
        "offers": {"price": "49.95", "priceCurrency": "USD"},
    }
    html = '<script type="application/ld+json">' + json.dumps({"@graph": [product]}) + "</script>"

    result = extract_ebay_item(html, item_identifier=ITEM, effective_url=URL)

    assert result.classification is EbayItemResponseKind.DETAIL
    assert result.item_identifier == ITEM
    assert result.title == "Bench Scope"
    assert result.description == "Works well ."
    assert result.displayed_price == "49.95"
    assert result.currency == "USD"
    assert result.gallery_urls == (IMAGE,)


def test_targeted_dom_and_iframe_are_not_confused_with_recommendations() -> None:
    result = extract_ebay_item(
        f"""
        <link rel="canonical" href="{URL}">
        <h1 class="x-item-title__mainTitle"><span>Bench Scope</span></h1>
        <div class="x-price-primary"><span>$49.95</span></div>
        <div class="x-item-condition-text">Used</div>
        <div class="ux-image-carousel"><img src="{IMAGE}"></div>
        <img src="https://i.ebayimg.com/images/recommendation.jpg">
        <iframe id="desc_ifr" src="{DESCRIPTION}"></iframe>
        <template><h1 class="x-item-title__mainTitle">Fake title</h1></template>
        <div hidden>pardon our interruption</div>
        """,
        item_identifier=ITEM,
        effective_url=URL,
    )

    assert result.classification is EbayItemResponseKind.DETAIL
    assert result.title == "Bench Scope"
    assert result.condition == "Used"
    assert result.displayed_price == "$49.95"
    assert result.description is None
    assert result.description_url == DESCRIPTION
    assert result.gallery_urls == (IMAGE,)


def test_unrelated_products_and_arbitrary_sku_never_supply_detail() -> None:
    unrelated = {
        "@type": "Product",
        "url": "https://www.ebay.com/itm/256987654321",
        "sku": ITEM,
        "name": "Wrong",
        "image": IMAGE,
    }
    html = '<script type="application/ld+json">' + json.dumps(unrelated) + "</script>"
    assert (
        extract_ebay_item(html, item_identifier=ITEM, effective_url=URL).classification
        is EbayItemResponseKind.UNRECOGNIZED
    )


def test_foreign_product_with_same_title_does_not_supply_gallery() -> None:
    product = {
        "@type": "Product",
        "url": "https://example.com/scope",
        "name": "Scope",
        "image": IMAGE,
    }
    html = (
        '<script type="application/ld+json">'
        + json.dumps(product)
        + '</script><h1 class="x-item-title__mainTitle">Scope</h1>'
    )
    result = extract_ebay_item(html, item_identifier=ITEM, effective_url=URL)
    assert result.title == "Scope"
    assert result.gallery_urls == ()


def test_meta_fallback_is_bound_to_the_requested_page() -> None:
    result = extract_ebay_item(
        f'<meta property="og:title" content="Scope"><meta property="og:image" content="{IMAGE}">',
        item_identifier=ITEM,
        effective_url=URL,
    )
    assert result.classification is EbayItemResponseKind.DETAIL
    assert result.title == "Scope"
    assert result.gallery_urls == (IMAGE,)


@pytest.mark.parametrize(
    ("html", "effective_url", "status_code", "kind"),
    [
        ("Pardon our interruption", URL, 200, EbayItemResponseKind.CHALLENGE),
        (
            '<h1 class="x-item-title__mainTitle">Old listing</h1>This listing has ended',
            URL,
            200,
            EbayItemResponseKind.UNAVAILABLE,
        ),
        ("missing", URL, 404, EbayItemResponseKind.UNAVAILABLE),
        ("broken", URL, 500, EbayItemResponseKind.UNRECOGNIZED),
        (
            '<h1 class="x-item-title__mainTitle">Wrong</h1>',
            "https://www.ebay.com/itm/256987654321",
            200,
            EbayItemResponseKind.MISMATCHED_ITEM,
        ),
        (
            '<link rel="canonical" href="https://www.ebay.com/itm/256987654321"><meta property="og:title" content="Wrong">',
            URL,
            200,
            EbayItemResponseKind.MISMATCHED_ITEM,
        ),
        (
            '<meta property="og:title" content="Wrong">',
            "https://example.com/itm/256123456789",
            200,
            EbayItemResponseKind.UNRECOGNIZED,
        ),
    ],
)
def test_non_detail_classifications_do_not_expose_listing_fields(
    html: str, effective_url: str, status_code: int, kind: EbayItemResponseKind
) -> None:
    result = extract_ebay_item(
        html, item_identifier=ITEM, effective_url=effective_url, status_code=status_code
    )
    assert result.classification is kind
    assert result.title is None
    assert result.gallery_urls == ()


def test_malformed_json_ld_does_not_hide_usable_dom() -> None:
    result = extract_ebay_item(
        '<script type="application/ld+json">{bad</script><h1 class="x-item-title__mainTitle">Scope</h1>',
        item_identifier=ITEM,
        effective_url=URL,
    )
    assert result.classification is EbayItemResponseKind.DETAIL
    assert result.issues == ({"kind": "malformed_json_ld", "block_index": 0},)


def test_non_finite_json_price_is_not_projected() -> None:
    html = (
        '<script type="application/ld+json">'
        + '{"@type":"Product","url":"'
        + URL
        + '","name":"Scope","offers":{"price":Infinity}}</script><h1 class="x-item-title__mainTitle">Scope</h1>'
    )
    result = extract_ebay_item(html, item_identifier=ITEM, effective_url=URL)
    assert result.title == "Scope"
    assert result.displayed_price is None
    assert result.issues == ({"kind": "malformed_json_ld", "block_index": 0},)


def test_gallery_is_bounded_and_rejects_foreign_images() -> None:
    html = '<h1 class="x-item-title__mainTitle">Scope</h1><div class="ux-image-carousel">'
    html += '<img src="https://evil.com/image.jpg"><img src="https://i.ebayimg.com:443/image.jpg">'
    html += (
        "".join(f'<img src="https://i.ebayimg.com/images/{index}.jpg">' for index in range(60))
        + "</div>"
    )
    result = extract_ebay_item(html, item_identifier=ITEM, effective_url=URL)
    assert len(result.gallery_urls) == 50
    assert {issue["kind"] for issue in result.issues if isinstance(issue, dict)} == {
        "invalid_gallery_url",
        "gallery_limit",
    }


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.com/image.jpg",
        "http://i.ebayimg.com/image.jpg",
        "https://i.ebayimg.com:443/image.jpg",
        "https://user@i.ebayimg.com/image.jpg",
        "https://i.ebayimg.com.evil.com/image.jpg",
        "https://i.ebayimg.com\\@evil.com/image.jpg",
    ],
)
def test_image_payload_rejects_untrusted_url(url: str) -> None:
    with pytest.raises(ValidationError):
        CollectEbayImagePayload(
            item_identifier=ITEM,
            observation_record_identifier="observation",
            reference_record_identifier="reference",
            url=url,
        )


def test_description_is_plain_text_hidden_content_removed_and_bounded() -> None:
    assert (
        extract_ebay_description(
            "<p>Works &amp; tested</p><script>Fake</script><style>Fake</style><div hidden>Fake</div><template>Fake</template>"
        )
        == "Works & tested"
    )
    assert len(extract_ebay_description("x" * 70_000)) == 60_000


@pytest.mark.parametrize(
    "challenge",
    [
        "Pardon our interruption",
        "Checking your browser before accessing",
        "We've detected unusual activity",
        "Verify you are human",
    ],
)
def test_description_challenge_cannot_be_saved_as_seller_text(challenge: str) -> None:
    with pytest.raises(ValueError, match="challenge"):
        extract_ebay_description(f"<html><body><h1>{challenge}</h1></body></html>")


def test_hidden_description_challenge_does_not_reject_actual_seller_text() -> None:
    assert (
        extract_ebay_description("<div hidden>Pardon our interruption</div><p>Works</p>") == "Works"
    )


def test_inline_json_description_challenge_becomes_issue_not_exception() -> None:
    product = {
        "@type": "Product",
        "url": URL,
        "name": "Scope",
        "description": "Pardon our interruption",
    }
    result = extract_ebay_item(
        '<script type="application/ld+json">' + json.dumps(product) + "</script>",
        item_identifier=ITEM,
        effective_url=URL,
    )
    assert result.classification is EbayItemResponseKind.DETAIL
    assert result.title == "Scope"
    assert result.description is None
    assert result.issues == ({"kind": "description_challenge"},)


def test_work_definitions_and_restricted_plans() -> None:
    request = EbayItemRequest(item_identifier=ITEM)
    item = CollectEbayItemPayload(request=request)
    extraction = ExtractEbayItemPayload(
        request=request, acquisition_record_identifier="acquisition"
    )
    image = CollectEbayImagePayload(
        item_identifier=ITEM,
        observation_record_identifier="observation",
        reference_record_identifier="reference",
        url=IMAGE,
    )
    description = CollectEbayDescriptionPayload(
        request=request, observation_record_identifier="observation", url=DESCRIPTION
    )
    definitions = (
        collect_ebay_item_work(identifier="item", payload=item),
        extract_ebay_item_work(identifier="extraction", payload=extraction),
        collect_ebay_image_work(identifier="image", payload=image),
        collect_ebay_description_work(identifier="description", payload=description),
    )
    assert tuple(definition.kind for definition in definitions) == (
        COLLECT_EBAY_ITEM_WORK_KIND,
        EXTRACT_EBAY_ITEM_WORK_KIND,
        COLLECT_EBAY_IMAGE_WORK_KIND,
        COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    )
    assert (
        definitions[0].deduplication_identity
        == collect_ebay_item_work(identifier="other", payload=item).deduplication_identity
    )
    assert all(definition.payload_schema_version == 1 for definition in definitions)
    assert tuple(constraint.maximum_active for constraint in ebay_item_work_constraints()) == (
        1,
        1,
        5,
        5,
    )
    assert ebay_item_plan(request, network_path=("decodo",)).url == URL
    assert not ebay_image_plan(image, network_path=("decodo",)).follow_redirects
    assert not ebay_description_plan(description, network_path=("decodo",)).follow_redirects
    assert request.maximum_images == 20
    with pytest.raises(ValidationError):
        EbayItemRequest(item_identifier=ITEM, maximum_images=51)
