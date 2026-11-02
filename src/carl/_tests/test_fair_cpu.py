"""Soft draining admits aged weighted CPU work without reclaiming live guards."""

from dataclasses import replace

import pytest

from carl.core.fair_cpu import (
    CPU_DRAIN_LEASE_NS,
    CPU_DRAIN_MINIMUM_AGE_NS,
    CPU_DRAIN_RETRY_NS,
    CpuDrainCandidate,
    choose_cpu_drain,
)

NOW = 100_000_000_000
TARGET = CpuDrainCandidate("build", "definition-1", 4, NOW - CPU_DRAIN_MINIMUM_AGE_NS)


def test_fragmented_cpu_drains_until_the_target_can_claim_all_units() -> None:
    decision = choose_cpu_drain(
        (TARGET,), current=None, now_utc_ns=NOW, capacity=4, occupied_units=4
    )
    intent = decision.intent
    assert intent is not None and intent.identifier == "build"
    for occupied in (3, 2, 1, 0):
        decision = choose_cpu_drain(
            (TARGET,), current=intent, now_utc_ns=NOW + 1, capacity=4, occupied_units=occupied
        )
        assert decision.intent is not None
        assert decision.intent.created_at_utc_ns == NOW
        assert decision.intent.expires_at_utc_ns == NOW + 1 + CPU_DRAIN_LEASE_NS
    # The writer clears the intent on successful admission. No drain is needed
    # for the next fresh contender when its complete allocation is already free.
    assert (
        choose_cpu_drain(
            (TARGET,), current=None, now_utc_ns=NOW, capacity=4, occupied_units=0
        ).intent
        is None
    )


def test_live_long_cpu_turns_renew_but_an_unoffered_or_changed_target_does_not() -> None:
    intent = choose_cpu_drain(
        (TARGET,), current=None, now_utc_ns=NOW, capacity=4, occupied_units=4
    ).intent
    assert intent is not None
    later = NOW
    for _ in range(100):
        later += CPU_DRAIN_LEASE_NS // 2
        decision = choose_cpu_drain(
            (TARGET,), current=intent, now_utc_ns=later, capacity=4, occupied_units=1
        )
        assert decision.intent is not None
        intent = decision.intent
    assert intent.created_at_utc_ns == NOW
    assert (
        choose_cpu_drain((), current=intent, now_utc_ns=later, capacity=4, occupied_units=1).intent
        is None
    )
    changed = replace(TARGET, signature="definition-2", ready_since_utc_ns=later)
    assert (
        choose_cpu_drain(
            (changed,), current=intent, now_utc_ns=later, capacity=4, occupied_units=1
        ).intent
        is None
    )


def test_unknown_held_guard_yields_spare_capacity_and_can_be_retried_after_proof() -> None:
    intent = choose_cpu_drain(
        (TARGET,), current=None, now_utc_ns=NOW, capacity=4, occupied_units=4
    ).intent
    decision = choose_cpu_drain(
        (TARGET,), current=intent, now_utc_ns=NOW, capacity=4, occupied_units=1, unconfirmed_units=1
    )
    assert decision.intent is None
    assert decision.retry_not_before_utc_ns == NOW + CPU_DRAIN_RETRY_NS
    assert (
        choose_cpu_drain(
            (TARGET,),
            current=None,
            now_utc_ns=NOW + 1,
            capacity=4,
            occupied_units=1,
            retry_not_before_utc_ns=decision.retry_not_before_utc_ns,
        ).intent
        is None
    )
    after_proof = choose_cpu_drain(
        (TARGET,),
        current=None,
        now_utc_ns=decision.retry_not_before_utc_ns,
        capacity=4,
        occupied_units=1,
        unconfirmed_units=0,
        retry_not_before_utc_ns=decision.retry_not_before_utc_ns,
    )
    assert after_proof.intent is not None


@pytest.mark.parametrize(("units", "occupied"), ((1, 3), (1, 4), (2, 2)))
def test_higher_ranked_contender_keeps_its_turn_before_a_later_heavy_drain(
    units: int, occupied: int
) -> None:
    first = replace(TARGET, identifier="first", units=units)
    assert (
        choose_cpu_drain(
            (first, TARGET), current=None, now_utc_ns=NOW, capacity=4, occupied_units=occupied
        ).intent
        is None
    )


def test_younger_fair_winner_is_not_preempted_by_an_aged_lower_ranked_offer() -> None:
    first = replace(TARGET, identifier="first", units=2, ready_since_utc_ns=NOW)
    assert (
        choose_cpu_drain(
            (first, TARGET), current=None, now_utc_ns=NOW, capacity=4, occupied_units=4
        ).intent
        is None
    )


def test_unfulfillable_unknown_guard_target_yields_to_a_possible_weighted_head() -> None:
    possible = replace(TARGET, identifier="possible", units=2)
    decision = choose_cpu_drain(
        (TARGET, possible),
        current=None,
        now_utc_ns=NOW,
        capacity=4,
        occupied_units=4,
        unconfirmed_units=1,
    )
    assert decision.intent is not None and decision.intent.identifier == "possible"
    expired = replace(decision.intent, expires_at_utc_ns=NOW)
    assert (
        choose_cpu_drain((), current=expired, now_utc_ns=NOW, capacity=4, occupied_units=4).intent
        is None
    )


@pytest.mark.parametrize(
    "kwargs",
    (
        {"capacity": 0},
        {"occupied_units": -1},
        {"unconfirmed_units": 5},
        {"now_utc_ns": -1},
        {"minimum_age_ns": -1},
    ),
)
def test_invalid_cpu_drain_inputs_are_rejected(kwargs: dict[str, int]) -> None:
    values = {"capacity": 4, "occupied_units": 4, "now_utc_ns": NOW, **kwargs}
    with pytest.raises(ValueError, match="Invalid bounded"):
        choose_cpu_drain((TARGET,), current=None, **values)
