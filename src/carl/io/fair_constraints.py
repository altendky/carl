"""Read constraint eligibility for one bounded fair-admission transaction."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from carl.core.json import encode_json
from carl.core.work import (
    CONSTRAINT_ADAPTER,
    ConcurrencyConstraint,
    Constraint,
    NetworkActivityDefinition,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
)

if TYPE_CHECKING:
    from apsw import AsyncConnection, SQLiteValue

type _Scope = tuple[str, str]


@dataclass(frozen=True, slots=True)
class WorkConstraintCandidate:
    work_identifier: str
    check_rates: bool


@dataclass(frozen=True, slots=True)
class NetworkConstraintCandidate:
    work_identifier: str
    stage_identifier: str
    execution: int
    created_at_utc_ns: int
    definition: NetworkActivityDefinition


def _definitions(
    rows: list[tuple[SQLiteValue, ...]],
) -> tuple[dict[str, list[str]], dict[str, Constraint]]:
    by_work: dict[str, list[str]] = {}
    definitions: dict[str, Constraint] = {}
    for work, identifier, value in rows:
        key = str(identifier)
        by_work.setdefault(str(work), []).append(key)
        if key not in definitions:
            definitions[key] = CONSTRAINT_ADAPTER.validate_json(str(value))
    return by_work, definitions


def _scope(constraint: ConcurrencyConstraint) -> _Scope:
    return constraint.scope.kind.value, encode_json(list(constraint.scope.identity))


async def _concurrency_counts(
    connection: AsyncConnection,
    scopes: set[_Scope],
    *,
    network: bool,
    now_utc_ns: int,
) -> dict[_Scope, int]:
    if not scopes:
        return {}
    if network:
        source = """network_activities AS owner INDEXED BY network_activities_state
            CROSS JOIN network_activity_scopes AS scope
              ON scope.network_activity_id=owner.id"""
        live = "owner.state='admitted' AND owner.permit_expires_at_utc_ns>?"
    else:
        source = """work_items AS owner INDEXED BY work_items_expired
            CROSS JOIN work_scopes AS scope ON scope.work_item_id=owner.id"""
        live = "owner.state IN ('leased','cancelling') AND owner.lease_expires_at_utc_ns>?"
    # The source is a fixed internal choice. Read live owners first, then seek
    # their memberships; common scopes must not walk completed owner history.
    rows = await (
        await connection.execute(
            f"""SELECT scope.scope_kind,scope.scope_identity_json,count(*)
            FROM {source}
            WHERE {live} AND (scope.scope_kind,scope.scope_identity_json) IN (
                SELECT json_extract(value,'$[0]'),json_extract(value,'$[1]')
                FROM json_each(?)
            )
            GROUP BY scope.scope_kind,scope.scope_identity_json""",
            (now_utc_ns, encode_json([list(scope) for scope in sorted(scopes)])),
        )
    ).fetchall()
    return {(str(row[0]), str(row[1])): cast(int, row[2]) for row in rows}


async def _rate_counts(
    connection: AsyncConnection,
    constraints: dict[str, SlidingWindowRateConstraint],
    *,
    now_utc_ns: int,
) -> dict[str, int]:
    if not constraints:
        return {}
    # Reservations are keyed by immutable constraint identity, not scope. Two
    # rate policies on the same scope can have different windows and histories.
    selected = [
        [identifier, now_utc_ns - constraint.period_ns]
        for identifier, constraint in constraints.items()
    ]
    rows = await (
        await connection.execute(
            """SELECT json_extract(selected.value,'$[0]'),count(starts.reserved_at_utc_ns)
            FROM json_each(?) AS selected
            LEFT JOIN rate_starts AS starts INDEXED BY rate_starts_constraint_time
              ON starts.constraint_identifier_parts_json=json_extract(selected.value,'$[0]')
             AND starts.reserved_at_utc_ns>json_extract(selected.value,'$[1]')
            GROUP BY selected.key""",
            (encode_json(selected),),
        )
    ).fetchall()
    return {str(row[0]): cast(int, row[1]) for row in rows}


async def eligible_work(
    connection: AsyncConnection,
    candidates: tuple[WorkConstraintCandidate, ...],
    *,
    now_utc_ns: int,
) -> frozenset[str]:
    """Check work constraints without charging a logical attempt or rate start."""
    if not candidates:
        return frozenset()
    rows = await (
        await connection.execute(
            """SELECT DISTINCT scope.work_item_id,c.identifier_parts_json,c.definition_json
            FROM json_each(?) AS selected
            CROSS JOIN work_scopes AS scope ON scope.work_item_id=selected.value
            CROSS JOIN scheduling_constraints AS c INDEXED BY scheduling_constraints_scope
              ON c.subject_kind='work_item' AND c.scope_kind=scope.scope_kind
             AND c.scope_identity_json=scope.scope_identity_json
            WHERE NOT EXISTS (
                SELECT 1 FROM scheduling_constraint_retirements AS retired
                WHERE retired.identifier_parts_json=c.identifier_parts_json
            )""",
            (encode_json(list(dict.fromkeys(item.work_identifier for item in candidates))),),
        )
    ).fetchall()
    by_work, definitions = _definitions(rows)
    counts = await _concurrency_counts(
        connection,
        {
            _scope(value)
            for value in definitions.values()
            if isinstance(value, ConcurrencyConstraint)
        },
        network=False,
        now_utc_ns=now_utc_ns,
    )
    checked_rates = {
        encode_json(list(value.identifier)): value
        for candidate in candidates
        if candidate.check_rates
        for identifier in by_work.get(candidate.work_identifier, ())
        if isinstance((value := definitions[identifier]), SlidingWindowRateConstraint)
    }
    rates = await _rate_counts(connection, checked_rates, now_utc_ns=now_utc_ns)
    eligible = {candidate.work_identifier for candidate in candidates}
    for candidate in candidates:
        for identifier in by_work.get(candidate.work_identifier, ()):
            constraint = definitions[identifier]
            if isinstance(constraint, ConcurrencyConstraint):
                if counts.get(_scope(constraint), 0) >= constraint.maximum_active:
                    eligible.discard(candidate.work_identifier)
            elif (
                candidate.check_rates
                and isinstance(constraint, SlidingWindowRateConstraint)
                and rates[encode_json(list(constraint.identifier))] >= constraint.maximum_starts
            ):
                eligible.discard(candidate.work_identifier)
            # Work holdoff decisions already contributed to durable eligibility.
    return frozenset(eligible)


async def eligible_network(
    connection: AsyncConnection,
    candidates: tuple[NetworkConstraintCandidate, ...],
    *,
    now_utc_ns: int,
) -> frozenset[str]:
    """Check future network templates before creating or reserving an activity."""
    active = tuple(
        candidate
        for candidate in candidates
        if candidate.definition.not_before_utc_ns <= now_utc_ns
    )
    if not active:
        return frozenset()
    scopes = [
        [candidate.work_identifier, scope.kind.value, encode_json(list(scope.identity))]
        for candidate in active
        for scope in candidate.definition.scopes
    ]
    rows = await (
        await connection.execute(
            """SELECT DISTINCT json_extract(scope.value,'$[0]'),
                   c.identifier_parts_json,c.definition_json
            FROM json_each(?) AS scope
            CROSS JOIN scheduling_constraints AS c INDEXED BY scheduling_constraints_scope
              ON c.subject_kind='network_activity'
             AND c.scope_kind=json_extract(scope.value,'$[1]')
             AND c.scope_identity_json=json_extract(scope.value,'$[2]')
            WHERE NOT EXISTS (
                SELECT 1 FROM scheduling_constraint_retirements AS retired
                WHERE retired.identifier_parts_json=c.identifier_parts_json
            )""",
            (encode_json(scopes),),
        )
    ).fetchall()
    by_work, definitions = _definitions(rows)
    counts = await _concurrency_counts(
        connection,
        {
            _scope(value)
            for value in definitions.values()
            if isinstance(value, ConcurrencyConstraint)
        },
        network=True,
        now_utc_ns=now_utc_ns,
    )
    rates = await _rate_counts(
        connection,
        {
            encode_json(list(value.identifier)): value
            for value in definitions.values()
            if isinstance(value, SlidingWindowRateConstraint)
        },
        now_utc_ns=now_utc_ns,
    )
    eligible = {candidate.work_identifier for candidate in active}
    for candidate in active:
        for identifier in by_work.get(candidate.work_identifier, ()):
            constraint = definitions[identifier]
            if isinstance(constraint, UniformHoldoffConstraint):
                delay = constraint.minimum_ns + int(
                    hashlib.sha256(
                        f"{candidate.stage_identifier}:{candidate.execution + 1}:"
                        f"{constraint.minimum_ns}:{constraint.maximum_ns}".encode()
                    ).hexdigest(),
                    16,
                ) % (constraint.maximum_ns - constraint.minimum_ns + 1)
                if candidate.created_at_utc_ns + delay > now_utc_ns:
                    eligible.discard(candidate.work_identifier)
            elif isinstance(constraint, SlidingWindowRateConstraint):
                if rates[encode_json(list(constraint.identifier))] >= constraint.maximum_starts:
                    eligible.discard(candidate.work_identifier)
            elif counts.get(_scope(constraint), 0) >= constraint.maximum_active:
                eligible.discard(candidate.work_identifier)
    return frozenset(eligible)
