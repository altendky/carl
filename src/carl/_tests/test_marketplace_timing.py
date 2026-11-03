"""Marketplace timing retirement preserves explicit policy and retry accounting."""

import pytest

from carl.core.ebay import COLLECT_EBAY_SEARCH_WORK_KIND
from carl.core.facebook_images import image_network_constraints
from carl.core.facebook_work import (
    COLLECT_SEARCH_WORK_KIND,
    facebook_network_path_constraint,
    facebook_network_policy_constraints,
)
from carl.core.marketplace_timing import (
    immediate_marketplace_retry,
    is_legacy_marketplace_timing_constraint,
    legacy_marketplace_timing_constraints,
)
from carl.core.work import (
    Constraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
)
from carl.core.worker import RetryWork


@pytest.mark.parametrize("route", (("proton", "old-alias"), ("decodo", "pool")))
def test_retirement_recognizes_shipped_policy_factories(route: tuple[str, ...]) -> None:
    for constraint in (
        *facebook_network_policy_constraints(route),
        *image_network_constraints(route),
        *legacy_marketplace_timing_constraints(),
    ):
        assert is_legacy_marketplace_timing_constraint(constraint)
    old_route = facebook_network_path_constraint(route).model_copy(
        update={
            "identifier": (
                "carl",
                "facebook",
                "network_path",
                "network_activity_rate",
                "v2",
                *route,
            ),
            "scope": SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=route),
        }
    )
    assert is_legacy_marketplace_timing_constraint(old_route)


@pytest.mark.parametrize("constraint", legacy_marketplace_timing_constraints())
def test_customized_definitions_remain_active(constraint: Constraint) -> None:
    variants = (
        constraint.model_copy(update={"schema_version": 2}),
        constraint.model_copy(update={"subject_kind": SchedulingSubjectKind.WORK_ITEM}),
        constraint.model_copy(update={"identifier": ("user", *constraint.identifier)}),
        constraint.model_copy(
            update={"scope": SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=())}
        ),
    )
    for custom in variants:
        assert not is_legacy_marketplace_timing_constraint(custom)
    if isinstance(constraint, SlidingWindowRateConstraint):
        assert not is_legacy_marketplace_timing_constraint(
            constraint.model_copy(update={"maximum_starts": constraint.maximum_starts + 1})
        )
    elif isinstance(constraint, UniformHoldoffConstraint):
        assert not is_legacy_marketplace_timing_constraint(
            constraint.model_copy(update={"maximum_ns": constraint.maximum_ns + 1})
        )


@pytest.mark.parametrize(
    ("kind", "reason_kind"),
    (
        (COLLECT_SEARCH_WORK_KIND, "search_acquisition_failure"),
        (COLLECT_SEARCH_WORK_KIND, "search_price_partition_extraction_failure"),
        (COLLECT_EBAY_SEARCH_WORK_KIND, "ebay_search_response_failure"),
    ),
)
def test_retained_acquisition_retry_is_immediate_without_changing_evidence(
    kind: tuple[str, ...], reason_kind: str
) -> None:
    old = RetryWork(
        delay_ns=600_000_000_000,
        reason={
            "kind": reason_kind,
            "maximum_attempts": 5,
            "policy_attempt": 2,
            "cooldown": {"until_utc_ns": 600_000_000_001},
            "facebook_search_cooldown": {"until_utc_ns": 600_000_000_001},
        },
        result={"checkpoint": "unchanged", "charged_attempts": 2},
    )
    new = immediate_marketplace_retry(kind, old)
    assert new.delay_ns == 0 and old.delay_ns == 600_000_000_000
    assert new.reason == {"kind": reason_kind, "maximum_attempts": 5, "policy_attempt": 2}
    assert new.result == old.result
    assert old.reason != new.reason


@pytest.mark.parametrize("reason_kind", ("connectivity_outage", "waiting_for_child", "user_delay"))
def test_infrastructure_dependency_and_custom_waits_remain(reason_kind: str) -> None:
    old = RetryWork(delay_ns=30_000_000_000, reason={"kind": reason_kind}, result={})
    assert immediate_marketplace_retry(COLLECT_SEARCH_WORK_KIND, old) is old


def test_analysis_or_hosted_work_is_not_marketplace_acquisition() -> None:
    old = RetryWork(
        delay_ns=60_000_000_000, reason={"kind": "search_acquisition_failure"}, result={}
    )
    for kind in (("carl", "facebook", "work", "analyze_item"), ("carl", "travel", "estimate")):
        assert immediate_marketplace_retry(kind, old) is old
