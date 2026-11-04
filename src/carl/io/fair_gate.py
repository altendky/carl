"""Synchronous fair arbitration within the caller's existing writer transaction."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, cast

from pydantic import TypeAdapter

from carl.core.fair_cpu import CpuDrainCandidate, CpuDrainIntent, choose_cpu_drain
from carl.core.fair_scheduling import FAIR_AGING_NS, FairLane
from carl.core.json import decode_json, encode_json
from carl.core.models import JsonValue
from carl.core.resources import ResourceRequest
from carl.io import fair_eligibility_native, fair_offer

if TYPE_CHECKING:
    from collections.abc import Mapping

    from apsw import Connection, SQLiteValue

_DRAIN_ADAPTER = TypeAdapter(CpuDrainIntent)


def _integer(value: object) -> int:
    assert isinstance(value, int)
    return value


@dataclass(frozen=True, slots=True)
class AdmissionFacts:
    occupied: dict[str, int]
    rows: tuple[tuple[SQLiteValue, ...], ...]


def clear_offer(connection: Connection, scope: str, lane: FairLane, work: str) -> None:
    connection.execute(
        "DELETE FROM fair_offers WHERE scope=? AND lane=? AND work_item_id=?",
        (scope, lane.value, work),
    )


def prepare_admission(
    connection: Connection,
    *,
    scope: str,
    lane: FairLane,
    work_identifier: str,
    capacities: dict[str, int],
    now: int,
) -> AdmissionFacts:
    """Arbitrate only live driver offers; inactive queue rows reserve no turn."""
    occupied_rows = (
        connection.execute(
            """SELECT allocation.name,sum(allocation.units) FROM resource_allocations allocation
            JOIN resource_permits permit ON permit.token=allocation.permit_token
            WHERE permit.scope=? AND permit.state='held' GROUP BY allocation.name""",
            (scope,),
        )
    ).fetchall()
    occupied = {str(row[0]): _integer(row[1]) for row in occupied_rows}
    rows = (
        connection.execute(
            """SELECT offer.work_item_id,offer.stage_id,offer.signature,offer.resources_json,
            offer.ready_since_utc_ns,owner.service_class,workspace.sequence,roots.sequence,
            owner.workspace_key,owner.root_request_identifier,f.ready_since_utc_ns,f.priority,
            f.available_at_utc_ns,f.created_at_utc_ns
        FROM fair_offers offer JOIN fair_ready f ON f.work_item_id=offer.work_item_id
        JOIN work_scheduling owner ON owner.work_item_id=offer.work_item_id
        JOIN fair_workspace_service workspace ON workspace.scope=offer.scope AND workspace.lane=offer.lane
            AND workspace.service_class=owner.service_class AND workspace.workspace_key=owner.workspace_key
        JOIN fair_root_service roots ON roots.scope=offer.scope AND roots.lane=offer.lane
            AND roots.root_request_identifier=owner.root_request_identifier
        JOIN fair_lanes lanes ON lanes.scope=offer.scope AND lanes.lane=offer.lane
        WHERE offer.scope=? AND offer.lane=? AND offer.expires_at_utc_ns>?
            AND f.active=1 AND f.lane=offer.lane AND offer.ready_since_utc_ns<=?
            AND (offer.stage_id=f.stage_id OR (offer.stage_id IS NULL
                AND (f.stage_id IS NULL OR offer.lane='resident')))
        ORDER BY CASE WHEN (owner.service_class='interactive' AND lanes.interactive_streak<3)
                OR (owner.service_class='background' AND lanes.interactive_streak>=3) THEN 0 ELSE 1 END,
            workspace.sequence,owner.workspace_key,roots.sequence,owner.root_request_identifier,
            CASE WHEN f.ready_since_utc_ns<=? THEN 0 ELSE 1 END,
            CASE WHEN f.ready_since_utc_ns<=? THEN f.ready_since_utc_ns ELSE 0 END,
            f.priority DESC,f.available_at_utc_ns,f.created_at_utc_ns,offer.work_item_id
        LIMIT 256""",
            (scope, lane.value, now, now, now - FAIR_AGING_NS, now - FAIR_AGING_NS),
        )
    ).fetchall()
    return AdmissionFacts(occupied, tuple(rows))


def finish_admission(
    connection: Connection,
    facts: AdmissionFacts,
    eligible_work: frozenset[str],
    *,
    scope: str,
    lane: FairLane,
    work_identifier: str,
    capacities: dict[str, int],
    now: int,
) -> bool:
    occupied, rows = facts.occupied, facts.rows
    candidates: list[tuple[str, str, tuple[ResourceRequest, ...], int]] = []
    for row in rows:
        identifier = str(row[0])
        requests = tuple(
            ResourceRequest.model_validate_json(encode_json(value))
            for value in cast(list[JsonValue], decode_json(str(row[3])))
        )
        if identifier not in eligible_work:
            clear_offer(connection, scope, lane, identifier)
            continue
        # A heavy CPU offer can drain CPU, but cannot stop unrelated budgets
        # while it is itself blocked on one of those other allocations.
        if any(
            occupied.get(request.name, 0) + request.units > capacities.get(request.name, 0)
            for request in requests
            if request.name != "cpu"
        ):
            continue
        candidates.append((identifier, str(row[2]), requests, _integer(row[4])))
    if lane is FairLane.CPU and capacities.get("cpu", 0) > 0:
        previous = (
            connection.execute(
                "SELECT intent_json,retry_not_before_utc_ns FROM fair_cpu_drains WHERE scope=?",
                (scope,),
            )
        ).fetchone()
        current = (
            None
            if previous is None or previous[0] is None
            else _DRAIN_ADAPTER.validate_json(str(previous[0]))
        )
        unconfirmed_row = (
            connection.execute(
                """SELECT coalesce(sum(a.units),0) FROM resource_allocations a JOIN resource_permits p ON p.token=a.permit_token
            WHERE p.scope=? AND p.state='held' AND p.expires_at_utc_ns<=? AND a.name='cpu'""",
                (scope, now),
            )
        ).fetchone()
        assert unconfirmed_row is not None
        decision = choose_cpu_drain(
            [
                CpuDrainCandidate(
                    identifier,
                    signature,
                    next((request.units for request in requests if request.name == "cpu"), 0),
                    ready,
                )
                for identifier, signature, requests, ready in candidates
            ],
            current=current,
            now_utc_ns=now,
            capacity=capacities["cpu"],
            occupied_units=occupied.get("cpu", 0),
            unconfirmed_units=_integer(unconfirmed_row[0]),
            retry_not_before_utc_ns=0 if previous is None else _integer(previous[1]),
        )
        connection.execute(
            "INSERT INTO fair_cpu_drains VALUES (?,?,?) ON CONFLICT(scope) DO UPDATE SET intent_json=excluded.intent_json,retry_not_before_utc_ns=excluded.retry_not_before_utc_ns",
            (
                scope,
                None
                if decision.intent is None
                else encode_json(cast(dict[str, JsonValue], asdict(decision.intent))),
                decision.retry_not_before_utc_ns,
            ),
        )
        if decision.intent is not None:
            return decision.intent.identifier == work_identifier
    for identifier, _, requests, _ in candidates:
        if all(
            occupied.get(request.name, 0) + request.units <= capacities.get(request.name, 0)
            for request in requests
        ):
            return identifier == work_identifier
    # Actual resource/network/work admission still explains this target's decline.
    return True


def gate(
    connection: Connection,
    *,
    scope: str,
    work_identifier: str,
    stage_identifier: str | None,
    signature: str,
    resources: tuple[ResourceRequest, ...],
    capacities: dict[str, int],
    now_utc_ns: int,
    resident: bool,
    ebay_cooldowns: Mapping[str, int],
    facebook_cooldowns: Mapping[str, int],
) -> tuple[bool, FairLane | None]:
    """Publish this live offer and arbitrate without leaving the writer thread."""
    if (
        connection.execute(
            "SELECT 1 FROM fair_scopes WHERE scope=? AND enabled=1", (scope,)
        ).fetchone()
        is None
    ):
        return True, None
    lane = fair_offer.lane_for(resources, resident=resident)
    row = connection.execute(
        "SELECT ready_since_utc_ns FROM fair_ready WHERE work_item_id=?", (work_identifier,)
    ).fetchone()
    ready = now_utc_ns if row is None else _integer(row[0])
    connection.execute(
        fair_offer._OFFER_UPSERT,  # pyright: ignore[reportPrivateUsage]
        fair_offer._offer_values(  # pyright: ignore[reportPrivateUsage]
            fair_offer.FairOffer(
                scope, lane, work_identifier, stage_identifier, signature, resources, ready
            ),
            now_utc_ns,
        ),
    )
    facts = prepare_admission(
        connection,
        scope=scope,
        lane=lane,
        work_identifier=work_identifier,
        capacities=capacities,
        now=now_utc_ns,
    )
    eligible = fair_eligibility_native.eligible_offers(
        connection,
        scope=scope,
        offers=tuple((str(row[0]), None if row[1] is None else str(row[1])) for row in facts.rows),
        lane=lane,
        now_utc_ns=now_utc_ns,
        ebay_cooldowns=ebay_cooldowns,
        facebook_cooldowns=facebook_cooldowns,
    )
    return finish_admission(
        connection,
        facts,
        eligible,
        scope=scope,
        lane=lane,
        work_identifier=work_identifier,
        capacities=capacities,
        now=now_utc_ns,
    ), lane
