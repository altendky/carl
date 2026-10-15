"""Safe image failure categories derived from retained acquisition evidence."""

from datetime import datetime
from typing import cast

from carl.core.facebook_images import facebook_image_url_expiry_utc_ns
from carl.core.models import JsonValue


def image_acquisition_failure_kind(
    record: dict[str, JsonValue], *, observed_at_utc_ns: int | None = None
) -> str | None:
    """Distinguish explicit provider failures from target HTTP responses.

    A 403 or 429 alone says nothing about provider quota. Require provider
    attribution for quota failures, and retain only a category in compact status.
    """
    provider = record.get("network_provider_result")
    if isinstance(provider, dict):
        category = cast(dict[str, JsonValue], provider).get("category")
        if category in {"proxy_quota_exhausted", "quota_exhausted"}:
            return "proxy_quota_exhausted"
        if isinstance(category, str):
            return category
    if record.get("stopping_condition") == "transport_failure":
        exception = record.get("exception_type")
        return (
            "transport_timeout"
            if isinstance(exception, str) and "timeout" in exception.lower()
            else "transport_failure"
        )
    hops = record.get("hops")
    if isinstance(hops, list) and hops and isinstance(hops[-1], dict):
        hop = cast(dict[str, JsonValue], hops[-1])
        response = hop.get("response")
        status = (
            cast(dict[str, JsonValue], response).get("status_code")
            if isinstance(response, dict)
            else None
        )
        if isinstance(status, int) and status != 200:
            if status == 403:
                request = hop.get("request")
                request = cast(dict[str, JsonValue], request) if isinstance(request, dict) else {}
                url = request.get("url") or record.get("effective_url")
                expiry = facebook_image_url_expiry_utc_ns(url) if isinstance(url, str) else None
                requested_at = request.get("started_at_utc")
                if isinstance(requested_at, str):
                    try:
                        timestamp = datetime.fromisoformat(requested_at)
                        if timestamp.tzinfo is not None:
                            observed_at_utc_ns = int(timestamp.timestamp() * 1_000_000_000)
                    except (ValueError, OverflowError, OSError):
                        pass
                if (
                    expiry is not None
                    and observed_at_utc_ns is not None
                    and expiry <= observed_at_utc_ns
                ):
                    return "expired_url"
            return f"site_http_{status}"
    return None
