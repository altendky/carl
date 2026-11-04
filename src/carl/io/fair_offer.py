"""Shared pure fair-offer identity, lane and serialization policy."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

from carl.core.fair_scheduling import FAIR_OFFER_NS, FairLane
from carl.core.json import encode_json
from carl.core.resources import ResourceRequest

if TYPE_CHECKING:
    from apsw import SQLiteValue


def lane_for(resources: tuple[ResourceRequest, ...], *, resident: bool = False) -> FairLane:
    if resident:
        return FairLane.RESIDENT
    names = {request.name for request in resources}
    return next(
        (lane for lane in (FairLane.ANALYSIS, FairLane.HTTP, FairLane.CPU) if lane.value in names),
        FairLane.METADATA,
    )


def initial_offer_signature(kind_parts_json: str, schema_version: int, attempt: int) -> str:
    return encode_json(["initial", kind_parts_json, schema_version, attempt])


@dataclass(frozen=True, slots=True)
class FairOffer:
    scope: str
    lane: FairLane
    work_identifier: str
    stage_identifier: str | None
    signature: str
    resources: tuple[ResourceRequest, ...]
    ready_since_utc_ns: int
    initial: bool = False


_OFFER_UPSERT = """INSERT INTO fair_offers VALUES (?,?,?,?,?,?,?,?)
    ON CONFLICT(scope,lane,work_item_id) DO UPDATE SET stage_id=excluded.stage_id,
    signature=excluded.signature,resources_json=excluded.resources_json,
    ready_since_utc_ns=excluded.ready_since_utc_ns,expires_at_utc_ns=excluded.expires_at_utc_ns"""


def _offer_values(offer: FairOffer, now: int) -> tuple[SQLiteValue, ...]:
    return (
        offer.scope,
        offer.lane.value,
        offer.work_identifier,
        offer.stage_identifier,
        ("initial:" if offer.initial else "")
        + hashlib.sha256(offer.signature.encode()).hexdigest(),
        encode_json([request.model_dump(mode="json") for request in offer.resources]),
        offer.ready_since_utc_ns,
        now + FAIR_OFFER_NS,
    )
