"""Safe image failure categories derived from retained acquisition evidence."""

from typing import cast

from carl.core.models import JsonValue


def image_acquisition_failure_kind(record: dict[str, JsonValue]) -> str | None:
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
        response = cast(dict[str, JsonValue], hops[-1]).get("response")
        status = (
            cast(dict[str, JsonValue], response).get("status_code")
            if isinstance(response, dict)
            else None
        )
        if isinstance(status, int) and status != 200:
            return f"site_http_{status}"
    return None
