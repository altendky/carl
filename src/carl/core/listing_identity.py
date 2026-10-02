"""Stable review identities without cross-marketplace numeric collisions.

Bare decimal IDs remain Facebook identities for compatibility with retained records.
eBay review identities are always explicitly qualified.
"""

import re

LISTING_IDENTIFIER_PATTERN = r"^(?:[0-9]+|ebay:[0-9]{9,15})$"


def is_listing_identifier(value: str) -> bool:
    return re.fullmatch(LISTING_IDENTIFIER_PATTERN, value) is not None


def canonical_listing_url(value: str) -> str:
    if not is_listing_identifier(value):
        raise ValueError("Listing identity must be a Facebook decimal ID or ebay:<item ID>")
    if value.startswith("ebay:"):
        return f"https://www.ebay.com/itm/{value.removeprefix('ebay:')}"
    return f"https://www.facebook.com/marketplace/item/{value}/"
