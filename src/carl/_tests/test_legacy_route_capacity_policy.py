"""Retirement recognizes shipped route caps without consuming custom policy."""

import pytest

from carl.core.legacy_capacity import (
    FACEBOOK_ROUTE_CAPACITY_FAMILIES,
    is_legacy_facebook_route_capacity_constraint,
)
from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
)

_FAMILIES = ("route_concurrency", "effective_route_concurrency")


def _constraint(
    family: str,
    version: str,
    routing: tuple[str, ...],
    *,
    maximum: int = 1,
) -> ConcurrencyConstraint:
    scope_family = (
        "search_acquisition" if family == "route_concurrency" else "effective_search_acquisition"
    )
    return ConcurrencyConstraint(
        identifier=("carl", "facebook", "search", family, version, *routing),
        subject_kind=SchedulingSubjectKind.WORK_ITEM,
        scope=SchedulingScope(
            kind=SchedulingScopeKind.NETWORK_PATH, identity=(scope_family, *routing)
        ),
        maximum_active=maximum,
    )


def test_route_scan_families_are_exact_and_separate_from_request_pacing() -> None:
    assert FACEBOOK_ROUTE_CAPACITY_FAMILIES == (
        ("carl", "facebook", "search", "route_concurrency"),
        ("carl", "facebook", "search", "effective_route_concurrency"),
    )


@pytest.mark.parametrize("family", _FAMILIES)
@pytest.mark.parametrize("version", ("v1", "v2"))
@pytest.mark.parametrize(
    "routing",
    (
        ("proton", "personal", "removed_alias"),
        ("decodo", "personal", "datacenter"),
        ("custom-provider",),
        ("proton", "personal", 'unicode-été-"\\'),
    ),
)
def test_shipped_serial_versions_match_all_retained_route_names(
    family: str, version: str, routing: tuple[str, ...]
) -> None:
    assert is_legacy_facebook_route_capacity_constraint(_constraint(family, version, routing))


@pytest.mark.parametrize("family", _FAMILIES)
@pytest.mark.parametrize("routing", (("decodo",), ("decodo", "personal", "old_alias")))
def test_shipped_decodo_twenty_version_matches(family: str, routing: tuple[str, ...]) -> None:
    assert is_legacy_facebook_route_capacity_constraint(
        _constraint(family, "v3", routing, maximum=20)
    )


@pytest.mark.parametrize("family", _FAMILIES)
@pytest.mark.parametrize(
    ("version", "routing", "maximum"),
    (
        ("v1", ("proton",), 2),
        ("v2", ("decodo",), 20),
        ("v3", ("decodo",), 1),
        ("v3", ("proton",), 20),
        ("v3", ("Decodo",), 20),
        ("v4", ("decodo",), 20),
        ("v0", ("proton",), 1),
        ("v2", (), 1),
    ),
)
def test_unshipped_versions_limits_and_routes_remain_custom(
    family: str, version: str, routing: tuple[str, ...], maximum: int
) -> None:
    assert not is_legacy_facebook_route_capacity_constraint(
        _constraint(family, version, routing, maximum=maximum)
    )


@pytest.mark.parametrize("family", _FAMILIES)
@pytest.mark.parametrize(
    "variant", ("subject", "scope_kind", "scope_route", "scope_family", "schema", "identifier")
)
def test_full_definition_must_match_not_just_identifier(family: str, variant: str) -> None:
    original = _constraint(family, "v2", ("proton", "personal", "route"))
    match variant:
        case "subject":
            changed = original.model_copy(
                update={"subject_kind": SchedulingSubjectKind.NETWORK_ACTIVITY}
            )
        case "scope_kind":
            changed = original.model_copy(
                update={
                    "scope": original.scope.model_copy(
                        update={"kind": SchedulingScopeKind.WORK_KIND}
                    )
                }
            )
        case "scope_route":
            changed = original.model_copy(
                update={
                    "scope": original.scope.model_copy(
                        update={"identity": (*original.scope.identity[:-1], "different")}
                    )
                }
            )
        case "scope_family":
            changed = original.model_copy(
                update={
                    "scope": original.scope.model_copy(
                        update={"identity": ("other", *original.scope.identity[1:])}
                    )
                }
            )
        case "schema":
            changed = original.model_copy(update={"schema_version": 2})
        case "identifier":
            changed = original.model_copy(
                update={"identifier": ("custom", *original.identifier[1:])}
            )
        case _:
            raise AssertionError(variant)
    assert not is_legacy_facebook_route_capacity_constraint(changed)


@pytest.mark.parametrize("family", _FAMILIES)
def test_request_rates_and_holdoffs_never_match_route_capacity(family: str) -> None:
    original = _constraint(family, "v2", ("proton",))
    assert not is_legacy_facebook_route_capacity_constraint(
        SlidingWindowRateConstraint(
            identifier=original.identifier,
            scope=original.scope,
            maximum_starts=1,
            period_ns=1_000_000_000,
        )
    )
    assert not is_legacy_facebook_route_capacity_constraint(
        UniformHoldoffConstraint(
            identifier=original.identifier,
            scope=original.scope,
            minimum_ns=0,
            maximum_ns=100_000_000,
        )
    )
