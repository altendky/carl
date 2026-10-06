"""Acquisition policy separate from historical eBay request deserialization."""

from carl.core.ebay import EbaySearchRequest

CLOSED_SEARCH_UNSUPPORTED_MESSAGE = (
    "Carl does not presently support eBay sold/completed searches: the configured eBay "
    "acquisition is anonymous, and closed-listing requests encounter eBay sign-in/challenge "
    "gating that this acquisition does not support. Use listing_state='active'. "
    "Retained sold/completed evidence remains readable."
)


class UnsupportedEbaySearchMode(ValueError):
    """A retained request is valid evidence but cannot initiate an acquisition."""


def require_supported_ebay_search_acquisition(request: EbaySearchRequest) -> None:
    if request.listing_state in {"sold", "completed"}:
        raise UnsupportedEbaySearchMode(CLOSED_SEARCH_UNSUPPORTED_MESSAGE)
