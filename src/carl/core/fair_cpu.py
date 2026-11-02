"""Choose a soft CPU drain without reserving execution capacity."""

from collections.abc import Sequence
from dataclasses import dataclass, replace

CPU_DRAIN_LEASE_NS = 1_000_000_000
CPU_DRAIN_RETRY_NS = 5_000_000_000
CPU_DRAIN_MINIMUM_AGE_NS = 30_000_000_000


@dataclass(frozen=True, slots=True)
class CpuDrainCandidate:
    identifier: str
    signature: str
    units: int
    ready_since_utc_ns: int


@dataclass(frozen=True, slots=True)
class CpuDrainIntent:
    identifier: str
    signature: str
    units: int
    created_at_utc_ns: int
    expires_at_utc_ns: int


@dataclass(frozen=True, slots=True)
class CpuDrainDecision:
    intent: CpuDrainIntent | None
    retry_not_before_utc_ns: int = 0


def choose_cpu_drain(
    candidates: Sequence[CpuDrainCandidate],
    *,
    current: CpuDrainIntent | None,
    now_utc_ns: int,
    capacity: int,
    occupied_units: int,
    unconfirmed_units: int = 0,
    retry_not_before_utc_ns: int = 0,
    minimum_age_ns: int = CPU_DRAIN_MINIMUM_AGE_NS,
) -> CpuDrainDecision:
    """Return one fair contender whose full CPU allocation should be allowed to form.

    Callers provide at most 256 otherwise eligible, live offers in fair order.
    They persist this decision under the admission writer and clear it on a
    successful claim. An intent only stops lending newly free CPU units; it owns
    no permit and cannot preempt an executor or reclaim a physical guard.

    Live offers renew the short lease through healthy long executions. An
    expired held allocation whose guard remains occupied reduces known usable
    capacity: an impossible drain yields rather than idling the remaining units.
    """
    if (
        len(candidates) > 256
        or capacity < 1
        or occupied_units < 0
        or not 0 <= unconfirmed_units <= occupied_units
        or now_utc_ns < 0
        or minimum_age_ns < 0
    ):
        raise ValueError("Invalid bounded CPU drain inputs")
    known_capacity = max(0, capacity - unconfirmed_units)
    available = max(0, capacity - occupied_units)
    live = {candidate.identifier: candidate for candidate in candidates}
    if current is not None and current.expires_at_utc_ns > now_utc_ns:
        candidate = live.get(current.identifier)
        if (
            candidate is not None
            and candidate.signature == current.signature
            and candidate.units == current.units
            and 1 < candidate.units <= capacity
        ):
            if candidate.units > known_capacity:
                return CpuDrainDecision(None, now_utc_ns + CPU_DRAIN_RETRY_NS)
            return CpuDrainDecision(
                replace(
                    current,
                    expires_at_utc_ns=max(
                        current.expires_at_utc_ns, now_utc_ns + CPU_DRAIN_LEASE_NS
                    ),
                )
            )
    if now_utc_ns < retry_not_before_utc_ns:
        return CpuDrainDecision(None, retry_not_before_utc_ns)
    unfulfillable = False
    for candidate in candidates:
        if candidate.units < 1 or candidate.units > capacity:
            continue
        if candidate.units > known_capacity:
            unfulfillable = True
            continue
        if candidate.units == 1 or now_utc_ns - candidate.ready_since_utc_ns < minimum_age_ns:
            # A lower-ranked heavy offer cannot take the fair turn from a
            # higher-ranked small or not-yet-aged contender. Once that work
            # claims, shared service counters can give the heavy offer its turn.
            return CpuDrainDecision(None)
        if candidate.units <= available:
            # Already available allocations use ordinary atomic admission.
            return CpuDrainDecision(None)
        return CpuDrainDecision(
            CpuDrainIntent(
                candidate.identifier,
                candidate.signature,
                candidate.units,
                now_utc_ns,
                now_utc_ns + CPU_DRAIN_LEASE_NS,
            )
        )
    return CpuDrainDecision(None, now_utc_ns + CPU_DRAIN_RETRY_NS if unfulfillable else 0)
