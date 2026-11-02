"""Acquisition policy separate from retained Facebook search deserialization."""

from carl.core.facebook_search import (
    OverlappingPricePartitionSearchTraversalStrategy,
    SearchTraversalStrategy,
)
from carl.core.facebook_work import FacebookSearchRequest

CURSOR_SEARCH_UNSUPPORTED_MESSAGE = (
    "Facebook cursor pagination is disabled. Use traversal_strategy.kind="
    "'overlapping_price_partitions' with an explicit bucket width and price.maximum. "
    "Revise existing cursor search tracks before refreshing or retrying them. "
    "Retained cursor search evidence remains readable."
)


class UnsupportedFacebookSearchMode(ValueError):
    """A retained search is valid evidence but cannot initiate acquisition."""


def require_supported_facebook_search_acquisition(
    request: FacebookSearchRequest, strategy: SearchTraversalStrategy
) -> None:
    if not isinstance(strategy, OverlappingPricePartitionSearchTraversalStrategy):
        raise UnsupportedFacebookSearchMode(CURSOR_SEARCH_UNSUPPORTED_MESSAGE)
    if request.price is None or request.price.maximum is None:
        raise UnsupportedFacebookSearchMode(
            "Facebook price-bucket searches require price.maximum; supply a bounded price range."
        )
