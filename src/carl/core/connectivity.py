"""Connectivity outage classification, separate from target-site refusals."""

from carl.core.models import JsonValue
from carl.core.worker import CompletedWork, RetryWork, WorkOutcome


def network_work_kind(kind: tuple[str, ...]) -> bool:
    return kind[:3] == ("carl", "ebay", "collect") or kind in {
        ("carl", "facebook", "work", "collect_search"),
        ("carl", "facebook", "work", "collect_item"),
        ("carl", "facebook", "work", "collect_image"),
    }


def connectivity_failure(outcome: WorkOutcome) -> bool:
    """Transport failures warrant a probe; HTTP refusals and auth errors do not."""

    if isinstance(outcome, CompletedWork):
        return False

    def transport_failure(value: JsonValue) -> bool:
        if not isinstance(value, dict):
            return False
        if value.get("stopping_condition") == "transport_failure":
            return True
        if value.get("code") in {
            "proton_egress_probe_failed",
            "wireproxy_startup_timeout",
            "wireproxy_exited_during_startup",
        }:
            return True
        return any(transport_failure(child) for child in value.values())

    failure = outcome.reason if isinstance(outcome, RetryWork) else outcome.error
    return transport_failure(outcome.result) or transport_failure(failure)
