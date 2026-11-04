"""Async compatibility API for the shared native offer eligibility policy."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from carl.core.fair_scheduling import FairLane
from carl.io import fair_eligibility_native

if TYPE_CHECKING:
    from collections.abc import Mapping

    from apsw import AsyncConnection, Connection


async def eligible_offers(
    connection: AsyncConnection,
    *,
    scope: str,
    offers: tuple[tuple[str, str | None], ...],
    lane: FairLane,
    now_utc_ns: int,
    ebay_cooldowns: Mapping[str, int],
    facebook_cooldowns: Mapping[str, int],
) -> frozenset[str]:
    return await connection.async_run(
        fair_eligibility_native.eligible_offers,
        cast("Connection", cast(object, connection)),
        scope=scope,
        offers=offers,
        lane=lane,
        now_utc_ns=now_utc_ns,
        ebay_cooldowns=ebay_cooldowns,
        facebook_cooldowns=facebook_cooldowns,
    )
