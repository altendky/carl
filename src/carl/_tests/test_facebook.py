import gzip

import pytest

from carl.core.content_encoding import ContentDecodingError, decode_content
from carl.core.facebook import (
    FacebookItemResponseKind,
    classify_item_response,
    extract_listing,
)

HTML = """<!doctype html>
<script type="application/json" data-sjs>
{"id":"123","crawler_configuration":true}
</script>
<script type="application/json" data-content-len="1">
{"data":{"target":{"id":"123","__typename":"GroupCommerceProductItem",
"marketplace_listing_title":"A telescope",
"redacted_description":{"text":"Seller says it works"},
"listing_price":{"amount":"125.00","currency":"USD"},
"location_text":{"text":"Example City, Pennsylvania"},
"location":{"latitude":1.0,"longitude":2.0},
"creation_time":1700000000,"is_sold":false,"marketplace_listing_seller":null,
"attribute_data":[{"attribute_name":"Condition","value":"used_good","label":"Used - Good"}]}}}
</script>
<script type="application/json">
{"data":{"target":{"id":"123","listing_photos":[
{"id":"photo-1","image":{"uri":"https://example.invalid/one.jpg","width":720,"height":960}},
{"id":"photo-2","image":{"uri":"https://example.invalid/two.jpg","width":720,"height":960}}
]}}}
</script>
<script type="application/json">{"broken":</script>
"""


def test_extraction_preserves_blocks_provenance_and_missing_states() -> None:
    result = extract_listing(HTML, listing_id="123", acquisition_record_id="capture-1")

    assert len(result.blocks) == 4
    assert result.blocks[0]["value"] == {"id": "123", "crawler_configuration": True}
    assert result.blocks[3]["parse_error"] is not None
    assert result.observation["state"] == "extracted"
    assert result.observation["listing_id"] == "123"
    assert result.observation["fields"]["title"]["evidence"][0]["normalized"] == "A telescope"
    assert result.observation["fields"]["price"]["evidence"][0]["normalized"] == {
        "amount_decimal": "125.00",
        "currency": "USD",
    }
    assert (
        result.observation["fields"]["location"]["evidence"][0]["normalized"]["precision"]
        == "approximate"
    )
    assert result.observation["fields"]["seller"]["state"] == "unavailable"
    assert result.observation["fields"]["availability_pending"]["state"] == "missing"
    assert [image["gallery_order"] for image in result.observation["images"]] == [0, 1]
    assert all(
        image["download"]["state"] == "not_yet_collected" for image in result.observation["images"]
    )


def test_incomplete_or_unrecognized_response_is_explicitly_unavailable() -> None:
    result = extract_listing(
        '<html><script type="application/json">{"login":true}</script></html>',
        listing_id="123",
        acquisition_record_id="capture-1",
    )

    assert result.observation["state"] == "unavailable"
    assert {warning["kind"] for warning in result.observation["warnings"]} == {
        "target_listing_not_found"
    }


def test_item_response_classification_requires_requested_structured_listing() -> None:
    classification = classify_item_response(
        HTML + '<script>const captcha = "unused-client-code";</script>',
        requested_listing_id="123",
        effective_url="https://www.facebook.com/marketplace/item/123/",
    )

    assert classification.kind is FacebookItemResponseKind.FULL_LISTING
    assert classification.parseable_json_blocks == 3
    assert classification.malformed_json_blocks == 1


@pytest.mark.parametrize(
    ("html", "effective_url", "kind"),
    [
        (
            '<html><form id="login_form"></form></html>',
            "https://www.facebook.com/login/?next=item",
            FacebookItemResponseKind.LOGIN_PAGE,
        ),
        (
            '<script type="application/json">'
            '{"id":"999","marketplace_listing_title":"Other","listing_price":{}}'
            "</script>",
            "https://www.facebook.com/marketplace/item/123/",
            FacebookItemResponseKind.REQUESTED_ID_ABSENT,
        ),
        (
            '<script type="application/json">{"broken":</script>',
            "https://www.facebook.com/marketplace/item/123/",
            FacebookItemResponseKind.MALFORMED_OR_INCOMPLETE,
        ),
    ],
)
def test_item_response_classification_distinguishes_non_listing_responses(
    html: str, effective_url: str, kind: FacebookItemResponseKind
) -> None:
    classification = classify_item_response(
        html,
        requested_listing_id="123",
        effective_url=effective_url,
    )

    assert classification.kind is kind


def test_content_decoding_is_separate_from_raw_evidence() -> None:
    content = HTML.encode()

    assert decode_content(gzip.compress(content), ("gzip",)) == content


@pytest.mark.parametrize(
    ("content", "encoding"),
    [(b"not-gzip", "gzip"), (b"content", "unknown")],
)
def test_content_decoding_reports_invalid_content(content: bytes, encoding: str) -> None:
    with pytest.raises(ContentDecodingError):
        decode_content(content, (encoding,))
