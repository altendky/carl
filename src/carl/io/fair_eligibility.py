"""Read live offer eligibility in batches within the caller's writer transaction."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from carl.core.ebay import COLLECT_EBAY_SEARCH_WORK_KIND
from carl.core.facebook_work import COLLECT_SEARCH_WORK_KIND
from carl.core.fair_scheduling import FairLane
from carl.core.json import encode_json
from carl.core.stages import StageDefinition
from carl.io import fair_constraints, fair_scheduling

if TYPE_CHECKING:
    from apsw import AsyncConnection


@dataclass(frozen=True, slots=True)
class _Offer:
    work_identifier: str
    stage_identifier: str | None
    state: str
    lease_expires_at: int | None
    kind_json: str
    ebay_stack: str | None
    facebook_routing: str | None
    eligible_at: int
    schema_version: int
    attempt: int
    signature: str | None
    resources_json: str | None
    ready_stage: str | None
    initial_resources_json: str | None
    definition_json: str | None
    execution: int | None
    stage_created_at: int | None
    attempt_state: str | None
    pending_not_before: int | None
    active_attempt: bool
    dependency: bool
    acquisition_peer: bool
    resident_owner: bool

    def initial_signature(self) -> str:
        return (
            "initial:"
            + hashlib.sha256(
                fair_scheduling.initial_offer_signature(
                    self.kind_json, self.schema_version, self.attempt
                ).encode()
            ).hexdigest()
        )


async def _offers(
    connection: AsyncConnection,
    *,
    scope: str,
    offers: tuple[tuple[str, str | None], ...],
    lane: FairLane,
    now_utc_ns: int,
) -> tuple[_Offer, ...]:
    # Start at the bounded offered IDs. Correlated probes use primary keys or
    # partial live-row indexes, rather than joining retained stage/lease history.
    rows = await (
        await connection.execute(
            """SELECT w.id,json_extract(requested.value,'$[1]'),w.state,
                w.lease_expires_at_utc_ns,w.kind_parts_json,
                CASE WHEN w.kind_parts_json=? THEN json_extract(w.payload_json,'$.request.stack_identifier') END,
                CASE WHEN w.kind_parts_json=? THEN json_extract(w.payload_json,'$.routing') END,
                w.eligible_at_utc_ns,w.payload_schema_version,w.attempt,
                offer.signature,offer.resources_json,ready.stage_id,initial.resources_json,
                stage.definition_json,stage.execution,stage.created_at_utc_ns,attempt.state,
                CASE WHEN ?='resident' THEN (
                    SELECT json_extract(pending.definition_json,'$.not_before_utc_ns')
                    FROM work_stages pending WHERE pending.work_item_id=w.id
                        AND pending.state IN ('pending','running') AND pending.state='pending') END,
                EXISTS(SELECT 1 FROM stage_attempts active WHERE active.work_item_id=w.id
                    AND active.state IN ('pending','active')),
                EXISTS(SELECT 1 FROM stage_dependencies dependency
                    JOIN work_items parent ON parent.id=dependency.dependency_work_id
                    WHERE dependency.stage_id=stage.id
                        AND parent.state NOT IN ('completed','terminal_failure','cancelled')),
                CASE WHEN w.acquisition_resource_identity_json IS NOT NULL THEN EXISTS(
                    SELECT 1 FROM work_items peer INDEXED BY work_items_live_acquisition
                    WHERE peer.kind_parts_json=w.kind_parts_json
                        AND peer.acquisition_resource_identity_json=w.acquisition_resource_identity_json
                        AND peer.acquisition_resource_identity_json IS NOT NULL
                        AND peer.id!=w.id AND peer.state IN ('leased','cancelling')
                        AND (peer.lease_expires_at_utc_ns>? OR EXISTS(
                            SELECT 1 FROM stage_attempts active WHERE active.work_item_id=peer.id
                                AND active.state IN ('pending','active') AND active.state='active')))
                    ELSE 0 END,
                CASE WHEN ?='resident' THEN EXISTS(SELECT 1 FROM resident_sessions resident
                    WHERE resident.work_item_id=w.id
                        AND resident.state IN ('opening','ready','closing','quarantined')) ELSE 0 END
            FROM json_each(?) requested
            CROSS JOIN work_items w ON w.id=json_extract(requested.value,'$[0]')
            LEFT JOIN fair_ready ready ON ready.work_item_id=w.id
            LEFT JOIN fair_offers offer ON offer.work_item_id=ready.work_item_id
                AND offer.scope=? AND offer.lane=?
            LEFT JOIN fair_initial_resources initial ON initial.kind_parts_json=ready.kind_parts_json
                AND initial.payload_schema_version=ready.payload_schema_version
            LEFT JOIN work_stages stage ON stage.id=json_extract(requested.value,'$[1]')
                AND stage.work_item_id=w.id AND stage.state='pending'
            LEFT JOIN stage_attempts attempt ON attempt.operation_id=
                json_extract(stage.definition_json,'$.operation_identifier')""",
            (
                encode_json(list(COLLECT_EBAY_SEARCH_WORK_KIND)),
                encode_json(list(COLLECT_SEARCH_WORK_KIND)),
                lane.value,
                now_utc_ns,
                lane.value,
                encode_json([[work, stage] for work, stage in offers]),
                scope,
                lane.value,
            ),
        )
    ).fetchall()
    return tuple(
        _Offer(
            work_identifier=cast(str, row[0]),
            stage_identifier=cast(str | None, row[1]),
            state=cast(str, row[2]),
            lease_expires_at=cast(int | None, row[3]),
            kind_json=cast(str, row[4]),
            ebay_stack=cast(str | None, row[5]),
            facebook_routing=cast(str | None, row[6]),
            eligible_at=cast(int, row[7]),
            schema_version=cast(int, row[8]),
            attempt=cast(int, row[9]),
            signature=cast(str | None, row[10]),
            resources_json=cast(str | None, row[11]),
            ready_stage=cast(str | None, row[12]),
            initial_resources_json=cast(str | None, row[13]),
            definition_json=cast(str | None, row[14]),
            execution=cast(int | None, row[15]),
            stage_created_at=cast(int | None, row[16]),
            attempt_state=cast(str | None, row[17]),
            pending_not_before=cast(int | None, row[18]),
            active_attempt=bool(row[19]),
            dependency=bool(row[20]),
            acquisition_peer=bool(row[21]),
            resident_owner=bool(row[22]),
        )
        for row in rows
    )


def _cooldown_active(
    offer: _Offer,
    *,
    ebay_cooldowns: Mapping[str, int],
    facebook_cooldowns: Mapping[str, int],
    now_utc_ns: int,
) -> bool:
    if offer.kind_json == encode_json(list(COLLECT_EBAY_SEARCH_WORK_KIND)):
        return (
            ebay_cooldowns.get(
                "ebay_anonymous" if offer.ebay_stack is None else offer.ebay_stack, 0
            )
            > now_utc_ns
        )
    if offer.kind_json == encode_json(list(COLLECT_SEARCH_WORK_KIND)):
        return (
            facebook_cooldowns.get(
                "null" if offer.facebook_routing is None else offer.facebook_routing, 0
            )
            > now_utc_ns
        )
    return False


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
    """Return eligible work IDs without publishing offers or reserving anything."""
    if not offers:
        return frozenset()
    facts = await _offers(connection, scope=scope, offers=offers, lane=lane, now_utc_ns=now_utc_ns)
    candidates: list[tuple[_Offer, StageDefinition | None]] = []
    work_checks: list[fair_constraints.WorkConstraintCandidate] = []
    for offer in facts:
        if offer.state not in ("pending", "leased"):
            continue
        initial = offer.signature is not None and offer.signature.startswith("initial:")
        if initial and (
            offer.signature != offer.initial_signature()
            or offer.ready_stage is not None
            or offer.resources_json != offer.initial_resources_json
            or offer.state != "pending"
            or offer.eligible_at > now_utc_ns
        ):
            continue
        if lane is FairLane.RESIDENT:
            if offer.eligible_at > now_utc_ns or (
                offer.pending_not_before is not None and offer.pending_not_before > now_utc_ns
            ):
                continue
            if offer.stage_identifier is not None and (
                offer.definition_json is None
                or offer.signature != hashlib.sha256(offer.definition_json.encode()).hexdigest()
            ):
                continue
            if initial:
                work_checks.append(
                    fair_constraints.WorkConstraintCandidate(offer.work_identifier, True)
                )
            candidates.append((offer, None))
            continue
        if offer.stage_identifier is None:
            if (
                offer.state != "pending"
                or offer.eligible_at > now_utc_ns
                or offer.ready_stage is not None
                or offer.signature != offer.initial_signature()
            ):
                continue
            work_checks.append(
                fair_constraints.WorkConstraintCandidate(offer.work_identifier, True)
            )
            if not offer.active_attempt:
                candidates.append((offer, None))
            continue
        if (
            offer.state == "leased"
            and offer.lease_expires_at is not None
            and offer.lease_expires_at > now_utc_ns
        ) or offer.definition_json is None:
            continue
        definition = StageDefinition.model_validate_json(offer.definition_json)
        if (
            definition.not_before_utc_ns > now_utc_ns
            or offer.signature != hashlib.sha256(definition.model_dump_json().encode()).hexdigest()
            or offer.dependency
        ):
            continue
        work_checks.append(
            fair_constraints.WorkConstraintCandidate(
                offer.work_identifier, offer.attempt_state == "pending"
            )
        )
        candidates.append((offer, definition))

    work_available = await fair_constraints.eligible_work(
        connection, tuple(work_checks), now_utc_ns=now_utc_ns
    )
    checked_work = {candidate.work_identifier for candidate in work_checks}
    accepted: set[str] = set()
    network_checks: list[fair_constraints.NetworkConstraintCandidate] = []
    paused: bool | None = None
    for offer, definition in candidates:
        if offer.work_identifier in checked_work and offer.work_identifier not in work_available:
            continue
        if lane is FairLane.RESIDENT:
            if not offer.resident_owner and not _cooldown_active(
                offer,
                ebay_cooldowns=ebay_cooldowns,
                facebook_cooldowns=facebook_cooldowns,
                now_utc_ns=now_utc_ns,
            ):
                accepted.add(offer.work_identifier)
            continue
        if definition is None:
            accepted.add(offer.work_identifier)
            continue
        if offer.acquisition_peer:
            continue
        network = definition.network_activity is not None or any(
            request.name == "http" for request in definition.resources
        )
        if not network:
            accepted.add(offer.work_identifier)
            continue
        if paused is None:
            connectivity = await (
                await connection.execute(
                    "SELECT paused FROM network_connectivity WHERE singleton=1"
                )
            ).fetchone()
            paused = connectivity is not None and connectivity[0] == 1
        if paused or _cooldown_active(
            offer,
            ebay_cooldowns=ebay_cooldowns,
            facebook_cooldowns=facebook_cooldowns,
            now_utc_ns=now_utc_ns,
        ):
            continue
        template = definition.network_activity
        if template is None:
            accepted.add(offer.work_identifier)
        elif template.not_before_utc_ns <= now_utc_ns:
            assert offer.stage_identifier is not None
            assert offer.execution is not None and offer.stage_created_at is not None
            network_checks.append(
                fair_constraints.NetworkConstraintCandidate(
                    offer.work_identifier,
                    offer.stage_identifier,
                    offer.execution,
                    offer.stage_created_at,
                    template,
                )
            )
    accepted.update(
        await fair_constraints.eligible_network(
            connection, tuple(network_checks), now_utc_ns=now_utc_ns
        )
    )
    return frozenset(accepted)
