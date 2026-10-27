"""Only explicit, identity-bound item locations can become route destinations."""

import json

import pytest

from carl.core.ebay_items import EbayItemExtraction, EbayItemResponseKind, extract_ebay_item

ITEM = "256123456789"
URL = f"https://www.ebay.com/itm/{ITEM}"
LOCATION = {
    "@type": "PostalAddress",
    "addressLocality": "Example City",
    "addressRegion": "PA",
    "postalCode": "12345",
    "addressCountry": "United States",
}


def _extract(product: dict[str, object], dom: str = "") -> EbayItemExtraction:
    return extract_ebay_item(
        '<script type="application/ld+json">' + json.dumps(product) + "</script>" + dom,
        item_identifier=ITEM,
        effective_url=URL,
    )


@pytest.mark.parametrize("location_field", ["itemLocation", "availableAtOrFrom"])
def test_explicit_requested_item_place_has_locality_and_country(location_field: str) -> None:
    product: dict[str, object] = {"@type": "Product", "url": URL, "name": "Bench Scope"}
    if location_field == "itemLocation":
        product[location_field] = {"@type": "Place", "address": LOCATION}
    else:
        product["offers"] = {"@type": "Offer", location_field: {"address": LOCATION}}
    result = _extract(product)
    assert result.classification is EbayItemResponseKind.DETAIL
    assert result.location_text == "Example City, PA, 12345, United States"
    assert result.model_dump(mode="json")["location_text"] == result.location_text


def test_postal_location_can_be_used_without_inventing_a_city() -> None:
    result = _extract(
        {
            "@type": "Product",
            "url": URL,
            "name": "Scope",
            "itemLocation": {
                "postalCode": "A1A 1A1",
                "addressCountry": {"@type": "Country", "name": "Canada"},
            },
        }
    )
    assert result.location_text == "A1A 1A1, Canada"


def test_item_location_dom_module_excludes_hidden_content() -> None:
    result = _extract(
        {},
        f'<link rel="canonical" href="{URL}">'
        + """<h1 class="x-item-title__mainTitle">Scope</h1>
        <div class="ux-labels-values--itemLocation"><span>Item location:</span>
        <span>Example City, PA, United States</span>
        <span hidden>Wrong place</span></div>""",
    )
    assert result.location_text == "Example City, PA, United States"


def test_seller_and_buyer_delivery_addresses_are_never_item_locations() -> None:
    result = _extract(
        {
            "@type": "Product",
            "url": URL,
            "name": "Scope",
            "address": LOCATION,
            "seller": {"address": LOCATION},
            "offers": {
                "seller": {"address": LOCATION},
                "shippingDetails": {
                    "shippingDestination": {"address": LOCATION},
                },
                "availableDeliveryMethod": {"address": LOCATION},
            },
        },
        '<div class="seller-location">Example City</div>'
        + '<div class="ux-labels-values--delivery">Ship to: Example City</div>',
    )
    assert result.location_text is None


def test_unrelated_product_location_does_not_supply_requested_item_location() -> None:
    result = _extract(
        {
            "@type": "Product",
            "url": "https://www.ebay.com/itm/256987654321",
            "name": "Scope",
            "itemLocation": {"address": LOCATION},
        },
        '<h1 class="x-item-title__mainTitle">Scope</h1>',
    )
    assert result.classification is EbayItemResponseKind.DETAIL
    assert result.location_text is None


def test_conflicting_places_report_unknown_instead_of_selecting_one() -> None:
    result = _extract(
        {
            "@type": "Product",
            "url": URL,
            "name": "Scope",
            "itemLocation": "First Example City, PA",
            "offers": {
                "availableAtOrFrom": "Second Example City, PA",
            },
        }
    )
    assert result.location_text is None
    assert {"kind": "conflicting_item_locations"} in result.issues


def test_country_only_and_absent_location_remain_unknown() -> None:
    for location in (
        None,
        {"addressRegion": "PA", "addressCountry": "United States"},
        {"addressLocality": " ", "postalCode": 12345, "addressRegion": "PA"},
    ):
        result = _extract(
            {"@type": "Product", "url": URL, "name": "Scope", "itemLocation": location}
        )
        assert result.location_text is None


def test_mismatched_item_page_never_exposes_location() -> None:
    result = extract_ebay_item(
        '<h1 class="x-item-title__mainTitle">Scope</h1>'
        + '<div class="ux-labels-values--itemLocation">Located in: Example City, PA</div>',
        item_identifier=ITEM,
        effective_url="https://www.ebay.com/itm/256987654321",
    )
    assert result.classification is EbayItemResponseKind.MISMATCHED_ITEM
    assert result.location_text is None


def test_nested_or_excessive_location_lists_remain_bounded_and_unknown() -> None:
    for location in ([[[[[LOCATION]]]]], [LOCATION] * 21):
        result = _extract(
            {"@type": "Product", "url": URL, "name": "Scope", "itemLocation": location}
        )
        assert result.location_text is None
