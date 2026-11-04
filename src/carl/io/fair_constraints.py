"""Async compatibility APIs for the shared native constraint policy."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from carl.io import fair_constraints_native
from carl.io.fair_constraints_native import NetworkConstraintCandidate as NetworkConstraintCandidate
from carl.io.fair_constraints_native import WorkConstraintCandidate as WorkConstraintCandidate

if TYPE_CHECKING:
    from apsw import AsyncConnection, Connection


async def eligible_work(
    connection: AsyncConnection,
    candidates: tuple[WorkConstraintCandidate, ...],
    *,
    now_utc_ns: int,
) -> frozenset[str]:
    return await connection.async_run(
        fair_constraints_native.eligible_work,
        cast("Connection", cast(object, connection)),
        candidates,
        now_utc_ns=now_utc_ns,
    )


async def eligible_network(
    connection: AsyncConnection,
    candidates: tuple[NetworkConstraintCandidate, ...],
    *,
    now_utc_ns: int,
) -> frozenset[str]:
    return await connection.async_run(
        fair_constraints_native.eligible_network,
        cast("Connection", cast(object, connection)),
        candidates,
        now_utc_ns=now_utc_ns,
    )
