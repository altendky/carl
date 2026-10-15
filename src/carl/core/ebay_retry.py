"""Retry budget and delays shared by blocked eBay searches and item pages."""

EBAY_BLOCKED_MAXIMUM_ATTEMPTS = 5


def ebay_response_retry_delay_ns(attempt: int, *, blocked: bool) -> int:
    if blocked:
        return min(3_600_000_000_000, 600_000_000_000 * 2 ** min(attempt - 1, 3))
    return 2 ** (attempt - 1) * 1_000_000_000
