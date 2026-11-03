"""Identify retired marketplace timing policy without changing custom scheduling."""

from carl.core.ebay import COLLECT_EBAY_SEARCH_WORK_KIND
from carl.core.ebay_items import (
    COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    COLLECT_EBAY_IMAGE_WORK_KIND,
    COLLECT_EBAY_ITEM_WORK_KIND,
)
from carl.core.facebook_images import COLLECT_IMAGE_WORK_KIND, IMAGE_NETWORK_ACTIVITY_KIND
from carl.core.facebook_work import (
    COLLECT_ITEM_WORK_KIND,
    COLLECT_SEARCH_WORK_KIND,
    ITEM_PAGE_NETWORK_ACTIVITY_KIND,
    SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND,
    SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
)
from carl.core.models import JsonValue
from carl.core.work import (
    Constraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
)
from carl.core.worker import RetryWork

MARKETPLACE_ACQUISITION_WORK_KINDS = frozenset(
    (
        COLLECT_SEARCH_WORK_KIND,
        COLLECT_ITEM_WORK_KIND,
        COLLECT_IMAGE_WORK_KIND,
        COLLECT_EBAY_SEARCH_WORK_KIND,
        COLLECT_EBAY_ITEM_WORK_KIND,
        COLLECT_EBAY_DESCRIPTION_WORK_KIND,
        COLLECT_EBAY_IMAGE_WORK_KIND,
    )
)

_RETRY_REASONS: dict[tuple[str, ...], frozenset[str]] = {
    COLLECT_SEARCH_WORK_KIND: frozenset(
        (
            "network_session_failure",
            "search_acquisition_failure",
            "search_session_bootstrap_response_failure",
            "search_route_definition_response_failure",
            "search_route_definition_extraction_failure",
            "search_price_partition_extraction_failure",
            "search_pagination_rate_limited",
        )
    ),
    COLLECT_ITEM_WORK_KIND: frozenset(("acquisition_failure",)),
    COLLECT_IMAGE_WORK_KIND: frozenset(
        ("facebook_image_session_failure", "image_transport_failure")
    ),
    COLLECT_EBAY_SEARCH_WORK_KIND: frozenset(
        ("ebay_search_acquisition_failure", "ebay_search_response_failure")
    ),
    COLLECT_EBAY_ITEM_WORK_KIND: frozenset(
        ("ebay_transport_failure", "ebay_image_session_failure", "ebay_item_response_failure")
    ),
    COLLECT_EBAY_DESCRIPTION_WORK_KIND: frozenset(
        ("ebay_transport_failure", "ebay_image_session_failure", "ebay_item_response_failure")
    ),
    COLLECT_EBAY_IMAGE_WORK_KIND: frozenset(
        ("ebay_transport_failure", "ebay_image_session_failure", "ebay_item_response_failure")
    ),
}


def is_marketplace_retry_reason(kind: tuple[str, ...], reason: JsonValue) -> bool:
    """Recognize acquisition recovery; exclude outages, analysis and dependencies."""
    return (
        isinstance(reason, dict)
        and isinstance(reason.get("kind"), str)
        and reason["kind"] in _RETRY_REASONS.get(kind, frozenset())
    )


def immediate_marketplace_retry(kind: tuple[str, ...], outcome: RetryWork) -> RetryWork:
    """Normalize a retained pre-upgrade outcome when connectivity review replays it."""
    if not is_marketplace_retry_reason(kind, outcome.reason):
        return outcome
    assert isinstance(outcome.reason, dict)
    reason = {
        name: value
        for name, value in outcome.reason.items()
        if name not in {"cooldown", "facebook_search_cooldown"}
    }
    return outcome.model_copy(update={"delay_ns": 0, "reason": reason})


def _rate(
    identifier: tuple[str, ...], scope: SchedulingScope, starts: int, period: int
) -> SlidingWindowRateConstraint:
    return SlidingWindowRateConstraint(
        identifier=identifier,
        subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
        scope=scope,
        maximum_starts=starts,
        period_ns=period,
    )


def _hold(
    identifier: tuple[str, ...], kind: tuple[str, ...], minimum: int, maximum: int
) -> UniformHoldoffConstraint:
    return UniformHoldoffConstraint(
        identifier=identifier,
        subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
        scope=SchedulingScope(kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND, identity=kind),
        minimum_ns=minimum,
        maximum_ns=maximum,
    )


def legacy_marketplace_timing_constraints() -> tuple[Constraint, ...]:
    """Immutable, verified static definitions, including known earlier releases."""
    origin = SchedulingScope(
        kind=SchedulingScopeKind.REMOTE_ORIGIN, identity=("https", "www.facebook.com", "443")
    )
    image = SchedulingScope(
        kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND, identity=IMAGE_NETWORK_ACTIVITY_KIND
    )
    return (
        _rate(
            ("carl", "facebook", "network_activity_rate", "remote_origin"),
            origin,
            12,
            60_000_000_000,
        ),
        _rate(
            ("carl", "facebook", "network_activity_rate", "remote_origin", "v2"),
            origin,
            180,
            60_000_000_000,
        ),
        _rate(
            ("carl", "facebook", "network_activity_rate", "remote_origin", "second", "v2"),
            origin,
            3,
            1_000_000_000,
        ),
        _hold(
            ("carl", "facebook", "item", "network_activity_holdoff"),
            ITEM_PAGE_NETWORK_ACTIVITY_KIND,
            2_000_000_000,
            5_000_000_000,
        ),
        _hold(
            ("carl", "facebook", "item", "network_activity_holdoff", "v2"),
            ITEM_PAGE_NETWORK_ACTIVITY_KIND,
            0,
            100_000_000,
        ),
        *(
            _hold(
                ("carl", "facebook", "search", "network_activity_holdoff", *kind, "v2"),
                kind,
                0,
                100_000_000,
            )
            for kind in (
                SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
                SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND,
            )
        ),
        _rate(
            ("carl", "facebook", "image", "network_activity_rate", "all_cdns", "v2"),
            image,
            180,
            60_000_000_000,
        ),
        _rate(
            ("carl", "facebook", "image", "network_activity_rate", "all_cdns", "second", "v2"),
            image,
            3,
            1_000_000_000,
        ),
        _hold(
            ("carl", "facebook", "image", "network_activity_holdoff", "v2"),
            IMAGE_NETWORK_ACTIVITY_KIND,
            0,
            100_000_000,
        ),
    )


def is_legacy_marketplace_timing_constraint(constraint: Constraint) -> bool:
    """Match full policy definitions; names alone never retire a custom rule."""
    if constraint in legacy_marketplace_timing_constraints():
        return True
    if not isinstance(constraint, SlidingWindowRateConstraint):
        return False
    identifier = constraint.identifier
    if identifier[:5] == ("carl", "facebook", "network_path", "network_activity_rate", "v2"):
        route = identifier[5:]
        scope = (
            SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=route)
            if route
            else None
        )
        starts = 180
    elif identifier[:5] == ("carl", "facebook", "network_path", "network_activity_rate", "v3"):
        route = identifier[5:]
        scope = (
            SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                identity=("carl", "facebook", "http_requests", *route),
            )
            if route
            else None
        )
        starts = 180
    elif identifier[:4] == ("carl", "facebook", "search", "network_activity_rate"):
        route = identifier[4:]
        scope = (
            SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=route)
            if route
            else None
        )
        starts = 12
    else:
        return False
    if not route or any(not part for part in route) or scope is None:
        return False
    return constraint == _rate(identifier, scope, starts, 60_000_000_000)
