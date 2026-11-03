"""Bounded, audited retirement of attributable marketplace scheduling delays.

The private connection access is intentional: scalar read pages and their
compare-and-swap publication belong in this module, not the generic scheduler.
Unknown deadlines remain intact. Physical ownership and retry accounting never
change. Historical evidence and stage definitions remain immutable.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter_ns
from typing import TYPE_CHECKING

import anyio
from pydantic import ValidationError

from carl.core.components import Component, ComponentId
from carl.core.json import decode_json, encode_json
from carl.core.marketplace_timing import (
    MARKETPLACE_ACQUISITION_WORK_KINDS,
    is_legacy_marketplace_timing_constraint,
    is_marketplace_retry_reason,
)
from carl.core.models import CodeProvenance, JsonValue, NamedOutput, RecordDraft
from carl.core.stages import StageDefinition
from carl.core.work import CONSTRAINT_ADAPTER, Constraint, NetworkActivityDefinition, WorkEventKind

if TYPE_CHECKING:
    from apsw import AsyncConnection, SQLiteValue

    from carl.io.sqlite import Database

_PAGE = 128
_METADATA_BYTES = 65_536
_CAPTURE = ("carl", "ebay", "stage", "resident", "capture")
_PREPARE = ("carl", "ebay", "stage", "resident", "prepare")
_PROGRESS = ("carl", "ebay", "session_stage_progress")
_EBAY_SEARCH = ("carl", "ebay", "collect", "search")
_EBAY_ITEM = ("carl", "ebay", "collect", "item")
_SHARED = {
    "ebay_search_shared_cooldown": _EBAY_SEARCH,
    "shared_search_route_transport_backoff": ("carl", "facebook", "work", "collect_search"),
    "shared_search_route_session_backoff": ("carl", "facebook", "work", "collect_search"),
}


@dataclass(frozen=True)
class _Plan:
    category: str
    identifier: str
    previous: int
    eligible: int
    sequence: int
    attempt: int = 0
    definition: StageDefinition | None = None


def _integer(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _int(value: object) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    raise TypeError("Expected numeric scheduling metadata")


def _parts(value: object) -> tuple[str, ...]:
    return (
        tuple(value) if isinstance(value, list) and all(isinstance(p, str) for p in value) else ()
    )


async def _pages(
    database: Database, table: str, columns: str
) -> AsyncIterator[list[tuple[SQLiteValue, ...]]]:
    """Visit at most 128 examined metadata rows, including irrelevant history."""
    # Identifiers here are internal constants, never caller-provided SQL.
    async with database._connections.reader() as connection:
        row = await (await connection.execute(f"SELECT max(rowid) FROM {table}")).fetchone()
    through = 0 if row is None or row[0] is None else _int(row[0])
    after = 0
    while after < through:
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    f"SELECT rowid,{columns} FROM {table} WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT ?",
                    (after, through, _PAGE),
                )
            ).fetchall()
        if not rows:
            break
        after = _int(rows[-1][0])
        yield rows
        await anyio.lowlevel.checkpoint()


async def _constraint_pages(database: Database) -> AsyncIterator[list[tuple[SQLiteValue, ...]]]:
    lower, upper = '["carl","facebook",', '["carl","facebook"-'
    async with database._connections.reader() as connection:
        high = await (
            await connection.execute(
                "SELECT max(identifier_parts_json) FROM scheduling_constraints WHERE identifier_parts_json>=? AND identifier_parts_json<?",
                (lower, upper),
            )
        ).fetchone()
    if high is None or high[0] is None:
        return
    after, through = lower, str(high[0])
    while after < through:
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    """SELECT c.identifier_parts_json,
                    CASE WHEN length(CAST(c.definition_json AS BLOB))<=? THEN c.definition_json END,
                    r.identifier_parts_json IS NOT NULL FROM scheduling_constraints c
                    LEFT JOIN scheduling_constraint_retirements r ON r.identifier_parts_json=c.identifier_parts_json
                    WHERE c.identifier_parts_json>? AND c.identifier_parts_json<=?
                    ORDER BY c.identifier_parts_json LIMIT ?""",
                    (_METADATA_BYTES, after, through, _PAGE),
                )
            ).fetchall()
        if not rows:
            break
        after = str(rows[-1][0])
        yield rows
        await anyio.lowlevel.checkpoint()


async def _pending_pages(
    database: Database, table: str, columns: str, now: int
) -> AsyncIterator[list[tuple[SQLiteValue, ...]]]:
    """Seek only pending future deadlines; archived rows never enter the pass."""
    if table == "work_items":
        async for rows in _pending_work_pages(database, columns, now):
            yield rows
        return
    if table == "work_stages":
        index = "work_stages_ready"
        keys = "eligible_at_utc_ns,created_at_utc_ns,id"
        reverse = "eligible_at_utc_ns DESC,created_at_utc_ns DESC,id DESC"
        fixed = ""
        cursor_fields = (3, 4, 1)
    else:
        assert table == "network_activities"
        index = "network_activities_state"
        keys = "eligible_at_utc_ns,rowid"
        reverse = "eligible_at_utc_ns DESC,rowid DESC"
        fixed = "AND permit_expires_at_utc_ns IS NULL"
        cursor_fields = (4, 0)
    async with database._connections.reader() as connection:
        through = await (
            await connection.execute(
                f"SELECT {keys} FROM {table} INDEXED BY {index} WHERE state='pending' AND eligible_at_utc_ns>? {fixed} ORDER BY {reverse} LIMIT 1",
                (now,),
            )
        ).fetchone()
    if through is None:
        return
    after: tuple[SQLiteValue, ...] | None = None
    placeholders = ",".join("?" for _ in through)
    while True:
        frontier = "" if after is None else f"AND ({keys})>({placeholders})"
        bindings = (now, *(() if after is None else after), *through, _PAGE)
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    f"SELECT rowid,{columns} FROM {table} INDEXED BY {index} WHERE state='pending' AND eligible_at_utc_ns>? {fixed} {frontier} AND ({keys})<=({placeholders}) ORDER BY {keys} LIMIT ?",
                    bindings,
                )
            ).fetchall()
        if not rows:
            return
        after = tuple(rows[-1][position] for position in cursor_fields)
        yield rows
        await anyio.lowlevel.checkpoint()


async def _pending_work_pages(
    database: Database, columns: str, now: int
) -> AsyncIterator[list[tuple[SQLiteValue, ...]]]:
    # The existing index mixes ASC/DESC keys. Four disjoint seeks preserve that
    # exact ordering without sorting all future jobs or rescanning a tied prefix.
    order = "eligible_at_utc_ns,priority DESC,created_at_utc_ns,rowid"
    async with database._connections.reader() as connection:
        through = await (
            await connection.execute(
                "SELECT eligible_at_utc_ns,priority,created_at_utc_ns,rowid FROM work_items INDEXED BY work_items_claim WHERE state='pending' AND eligible_at_utc_ns>? ORDER BY eligible_at_utc_ns DESC,priority,created_at_utc_ns DESC,rowid DESC LIMIT 1",
                (now,),
            )
        ).fetchone()
    if through is None:
        return
    end = (_int(through[0]), -_int(through[1]), _int(through[2]), _int(through[3]))
    after: tuple[int, int, int, int] | None = None
    while True:
        if after is None:
            branches: tuple[tuple[str, tuple[int, ...]], ...] = (("", ()),)
        else:
            e, priority, created, rowid = after
            branches = tuple(
                reversed(
                    (
                        ("AND eligible_at_utc_ns>?", (e,)),
                        ("AND eligible_at_utc_ns=? AND priority<?", (e, priority)),
                        (
                            "AND eligible_at_utc_ns=? AND priority=? AND created_at_utc_ns>?",
                            (e, priority, created),
                        ),
                        (
                            "AND eligible_at_utc_ns=? AND priority=? AND created_at_utc_ns=? AND rowid>?",
                            (e, priority, created, rowid),
                        ),
                    )
                )
            )
        examined: list[tuple[SQLiteValue, ...]] = []
        async with database._connections.reader() as connection:
            for frontier, bindings in branches:
                remaining = _PAGE - len(examined)
                if not remaining:
                    break
                examined.extend(
                    await (
                        await connection.execute(
                            f"SELECT rowid,{columns},priority FROM work_items INDEXED BY work_items_claim WHERE state='pending' AND eligible_at_utc_ns>? AND eligible_at_utc_ns<=? {frontier} ORDER BY {order} LIMIT ?",
                            (now, through[0], *bindings, remaining),
                        )
                    ).fetchall()
                )

        def key(row: tuple[SQLiteValue, ...]) -> tuple[int, int, int, int]:
            return (_int(row[5]), -_int(row[7]), _int(row[6]), _int(row[0]))

        selected = sorted((row for row in examined if key(row) <= end), key=key)[:_PAGE]
        if not selected:
            return
        last = selected[-1]
        after = (_int(last[5]), _int(last[7]), _int(last[6]), _int(last[0]))
        yield [row[:-1] for row in selected]
        await anyio.lowlevel.checkpoint()


async def _audit(
    database: Database,
    *,
    category: str,
    provenance: CodeProvenance,
    invocation: JsonValue,
    new_identifier: Callable[[], str],
    now: int,
    changes: tuple[_Plan, ...] = (),
    constraints: tuple[Constraint, ...] = (),
) -> None:
    if not changes and not constraints:
        return
    operation = new_identifier()
    started = perf_counter_ns()
    with anyio.CancelScope(shield=True):
        await database.begin_operation(
            operation_id=operation,
            component=Component(
                ComponentId(("carl", "work", "retire_marketplace_timing")),
                1,
                retire_marketplace_timing,
            ),
            provenance=provenance,
            invocation=invocation,
            configuration={"policy_version": 1, "category": category, "page_limit": _PAGE},
            started_at_utc=datetime.fromtimestamp(now / 1_000_000_000, UTC).isoformat(),
        )
        try:
            async with database._connections.writer() as connection:
                if constraints:
                    await database.supersede_constraints(
                        retired_identifiers=(),
                        replacements=(),
                        retired_definitions=constraints,
                        operation_identifier=operation,
                        at_utc_ns=now,
                        reason="Remove shipped Facebook acquisition pacing",
                    )
                applied: list[JsonValue] = []
                for plan in changes:
                    if await _apply(database, connection, plan, now, new_identifier):
                        applied.append(
                            {
                                "identifier": plan.identifier,
                                "previous_eligible_at_utc_ns": plan.previous,
                                "eligible_at_utc_ns": plan.eligible,
                            }
                        )
                record = new_identifier()
                await database.complete_operation(
                    operation_id=operation,
                    records=(
                        RecordDraft(
                            identifier=record,
                            kind=("carl", "marketplace", "timing_retirement"),
                            schema_version=1,
                            value={
                                "category": category,
                                "changes": applied,
                                "retired_definitions": [
                                    c.model_dump(mode="json") for c in constraints
                                ],
                            },
                        ),
                    ),
                    artifacts=(),
                    outputs=(NamedOutput(name=("timing_retirement",), object_identifier=record),),
                    result={
                        "state": "prepared",
                        "changed": len(applied),
                        "constraints": len(constraints),
                    },
                    ended_at_utc=datetime.fromtimestamp(now / 1_000_000_000, UTC).isoformat(),
                    duration_ns=max(0, perf_counter_ns() - started),
                )
        except BaseException as error:
            await database.fail_operation(
                operation_id=operation,
                error={
                    "kind": "marketplace_timing_retirement_failure",
                    "type": type(error).__name__,
                },
                result={"state": "failed"},
                ended_at_utc=datetime.fromtimestamp(now / 1_000_000_000, UTC).isoformat(),
                duration_ns=max(0, perf_counter_ns() - started),
            )
            raise


async def _apply(
    database: Database,
    connection: AsyncConnection,
    plan: _Plan,
    now: int,
    new_identifier: Callable[[], str],
) -> bool:
    if plan.category == "work":
        await connection.execute(
            """UPDATE work_items SET eligible_at_utc_ns=?
            WHERE id=? AND state='pending' AND attempt=? AND eligible_at_utc_ns=?
              AND (SELECT max(sequence) FROM work_events WHERE work_item_id=work_items.id)=?""",
            (plan.eligible, plan.identifier, plan.attempt, plan.previous, plan.sequence),
        )
        if await connection.changes() != 1:
            return False
        await database._append_work_event(
            connection,
            event_identifier=new_identifier(),
            work_item_identifier=plan.identifier,
            event_kind=WorkEventKind.ENQUEUED,
            recorded_at_utc_ns=now,
            data={
                "reason": {"kind": "marketplace_timing_retired"},
                "previous_eligible_at_utc_ns": plan.previous,
                "eligible_at_utc_ns": plan.eligible,
            },
        )
        return True
    if plan.category == "network":
        await connection.execute(
            """UPDATE network_activities SET eligible_at_utc_ns=?
            WHERE id=? AND state='pending' AND eligible_at_utc_ns=?
              AND (SELECT max(sequence) FROM network_activity_events WHERE network_activity_id=network_activities.id)=?""",
            (plan.eligible, plan.identifier, plan.previous, plan.sequence),
        )
        return await connection.changes() == 1
    if plan.category == "cooldown":
        await connection.execute(
            "DELETE FROM scheduler_cooldowns WHERE work_id=? AND until_ns=?",
            (plan.identifier, plan.previous),
        )
        return await connection.changes() == 1
    assert plan.category == "stage" and plan.definition is not None
    definition = plan.definition
    row = await (
        await connection.execute(
            """SELECT 1 FROM work_stages s JOIN work_items w ON w.id=s.work_item_id
            JOIN stage_attempts a ON a.operation_id=s.operation_id
            WHERE s.id=? AND s.state='pending' AND s.definition_json=? AND s.eligible_at_utc_ns=?
              AND s.execution=? AND s.progress_waiting=0 AND s.wait_kind='time'
              AND a.state='active' AND w.state='leased' AND w.lease_expires_at_utc_ns<=?
              AND (SELECT max(sequence) FROM stage_events WHERE stage_id=s.id)=?""",
            (
                plan.identifier,
                definition.model_dump_json(),
                plan.previous,
                plan.attempt,
                now,
                plan.sequence,
            ),
        )
    ).fetchone()
    if row is None:
        return False
    replacement = definition.model_copy(
        update={"identifier": new_identifier(), "not_before_utc_ns": plan.eligible}
    )
    await connection.execute(
        "UPDATE work_stages SET state='cancelled' WHERE id=?", (plan.identifier,)
    )
    await database._register_stage(connection, replacement, now, continuation=True)
    await database._append_stage_event(
        connection,
        plan.identifier,
        "timing_retired",
        now,
        {
            "continuation": replacement.identifier,
            "previous_not_before_utc_ns": definition.not_before_utc_ns,
        },
    )
    return True


async def _work_plan(database: Database, row: tuple[SQLiteValue, ...], now: int) -> _Plan | None:
    _, identifier, kind_json, state, attempt, eligible, created = row
    if state != "pending" or _int(eligible) <= now:
        return None
    kind = _parts(decode_json(str(kind_json)))
    if kind not in MARKETPLACE_ACQUISITION_WORK_KINDS:
        return None
    async with database._connections.reader() as connection:
        last = await (
            await connection.execute(
                "SELECT max(sequence) FROM work_events WHERE work_item_id=?", (identifier,)
            )
        ).fetchone()
    through = 0 if last is None or last[0] is None else _int(last[0])
    baseline, effective, removable, after = _int(created), _int(created), False, 0
    while after < through:
        async with database._connections.reader() as connection:
            events = await (
                await connection.execute(
                    """SELECT e.sequence,e.event_kind,e.recorded_at_utc_ns,
                    CASE WHEN length(CAST(e.data_json AS BLOB))<=? THEN e.data_json END,
                    EXISTS(SELECT 1 FROM work_operations b WHERE b.work_item_id=e.work_item_id
                      AND b.attempt=json_extract(e.data_json,'$.attempt')
                      AND b.operation_id=json_extract(e.data_json,'$.operation_identifier'))
                    FROM work_events e WHERE e.work_item_id=? AND e.sequence>? AND e.sequence<=?
                    ORDER BY e.sequence LIMIT ?""",
                    (_METADATA_BYTES, identifier, after, through, _PAGE),
                )
            ).fetchall()
        if not events:
            return None
        for sequence, event_kind, recorded, raw, bound in events:
            after = _int(sequence)
            if raw is None:
                return None
            data = decode_json(str(raw))
            if not isinstance(data, dict):
                continue
            deadline = _integer(
                data.get("effective_eligible_at_utc_ns", data.get("eligible_at_utc_ns"))
            )
            if deadline is None:
                continue
            reason = data.get("reason")
            reason_kind = reason.get("kind") if isinstance(reason, dict) else None
            if (
                event_kind == "released"
                and bound
                and _integer(data.get("attempt")) == _int(attempt)
                and is_marketplace_retry_reason(kind, reason)
            ):
                # Its lease was acquired after the original explicit deadline.
                baseline, effective, removable = _int(recorded), deadline, True
            elif (
                event_kind == "enqueued"
                and isinstance(reason_kind, str)
                and _SHARED.get(reason_kind) == kind
            ):
                effective, removable = deadline, True
            elif (
                event_kind == "enqueued"
                and kind == _EBAY_SEARCH
                and _integer(data.get("shared_cooldown_until_utc_ns"))
            ):
                own = _integer(data.get("own_eligible_at_utc_ns"))
                if own is None:
                    return None
                baseline, effective, removable = own, deadline, True
            else:
                baseline, effective, removable = deadline, deadline, False
        await anyio.lowlevel.checkpoint()
    target = max(now, baseline)
    if removable and effective == eligible and target < _int(eligible):
        return _Plan("work", str(identifier), _int(eligible), target, through, _int(attempt))
    return None


async def _network_plan(database: Database, row: tuple[SQLiteValue, ...], now: int) -> _Plan | None:
    _, identifier, kind_json, state, eligible, created, operation = row
    kind = _parts(decode_json(str(kind_json)))
    if (
        state != "pending"
        or _int(eligible) <= now
        or kind[:3]
        not in (("carl", "facebook", "network_activity"), ("carl", "ebay", "network_activity"))
    ):
        return None
    async with database._connections.reader() as connection:
        event = await (
            await connection.execute(
                """SELECT CASE WHEN length(CAST(data_json AS BLOB))<=? THEN data_json END,
                (SELECT max(sequence) FROM network_activity_events WHERE network_activity_id=?)
                FROM network_activity_events WHERE network_activity_id=? ORDER BY sequence LIMIT 1""",
                (_METADATA_BYTES, identifier, identifier),
            )
        ).fetchone()
        if event is None or event[0] is None:
            return None
        data = decode_json(str(event[0]))
        if not isinstance(data, dict) or not isinstance(data.get("holdoff_decisions"), list):
            return None
        try:
            definition = NetworkActivityDefinition.model_validate_json(
                encode_json(data.get("definition"))
            )
        except ValidationError:
            return None
        if definition.identifier != identifier or definition.operation_identifier != operation:
            return None
        decisions = data["holdoff_decisions"]
        if len(decisions) > _PAGE:
            return None
        original = target = max(_int(created), definition.not_before_utc_ns)
        removed = False
        for decision in decisions:
            if not isinstance(decision, dict):
                return None
            identity, delay = (
                _parts(decision.get("constraint_identifier")),
                _integer(decision.get("sampled_delay_ns")),
            )
            if not identity or delay is None:
                return None
            original = max(original, _int(created) + delay)
            stored = await (
                await connection.execute(
                    "SELECT CASE WHEN length(CAST(definition_json AS BLOB))<=? THEN definition_json END FROM scheduling_constraints WHERE identifier_parts_json=?",
                    (_METADATA_BYTES, encode_json(list(identity))),
                )
            ).fetchone()
            known = False
            if stored is not None and stored[0] is not None:
                with suppress(ValidationError):
                    known = is_legacy_marketplace_timing_constraint(
                        CONSTRAINT_ADAPTER.validate_json(str(stored[0]))
                    )
            if known:
                removed = True
            else:
                target = max(target, _int(created) + delay)
    target = max(now, target)
    if removed and original == eligible and target < _int(eligible):
        return _Plan("network", str(identifier), _int(eligible), target, _int(event[1]))
    return None


async def _stage_plan(database: Database, row: tuple[SQLiteValue, ...], now: int) -> _Plan | None:
    _, identifier, state, eligible, created, execution, raw = row
    if state != "pending" or _int(eligible) <= now or raw is None:
        return None
    definition = StageDefinition.model_validate_json(str(raw))
    if definition.kind != _CAPTURE or definition.not_before_utc_ns != eligible:
        return None
    payload = definition.payload
    if not isinstance(payload, dict) or not isinstance(payload.get("progress_identifier"), str):
        return None
    async with database._connections.reader() as connection:
        proof = await (
            await connection.execute(
                """SELECT json_extract(r.value_json,'$.phase'),
                json_extract(r.value_json,'$.search_policy.maximum_holdoff_seconds'),
                json_extract(r.value_json,'$.item_policy.maximum_holdoff_seconds'),
                (SELECT max(sequence) FROM stage_events WHERE stage_id=s.id),w.kind_parts_json
                FROM work_stages s JOIN stage_inputs i ON i.stage_id=s.id AND i.name_parts_json='["progress"]'
                JOIN objects o ON o.id=i.object_id JOIN records r ON r.object_id=o.id
                JOIN operations p ON p.id=s.operation_id JOIN stage_attempts a ON a.operation_id=s.operation_id
                JOIN work_items w ON w.id=s.work_item_id
                WHERE s.id=? AND s.wait_kind='time' AND s.progress_waiting=0
                  AND o.id=? AND o.created_by_operation_id=s.operation_id
                  AND o.kind_parts_json=? AND r.schema_version=1 AND a.state='active'
                  AND w.state='leased' AND w.lease_expires_at_utc_ns<=?
                  AND json_extract(p.configuration_json,'$.stage.kind')=?""",
                (
                    identifier,
                    payload["progress_identifier"],
                    encode_json(list(_PROGRESS)),
                    now,
                    encode_json(list(_PREPARE)),
                ),
            )
        ).fetchone()
    if proof is None or proof[0] not in ("homepage", "page", "item"):
        return None
    expected_kind = _EBAY_ITEM if proof[0] == "item" else _EBAY_SEARCH
    if _parts(decode_json(str(proof[4]))) != expected_kind:
        return None
    maximum = proof[2] if proof[0] == "item" else proof[1]
    if type(maximum) not in (int, float) or not 0 < maximum <= 60:
        return None
    if _int(eligible) > _int(created) + _int(maximum * 1_000_000_000):
        return None
    return _Plan(
        "stage", str(identifier), _int(eligible), now, _int(proof[3]), _int(execution), definition
    )


async def retire_marketplace_timing(
    database: Database,
    *,
    provenance: CodeProvenance,
    invocation: JsonValue,
    new_identifier: Callable[[], str],
    now_utc_ns: int,
) -> None:
    """Retire known timing and pending attributed waits before worker dispatch.

    Every read/write batch is bounded; total startup work grows with retained
    metadata. Cancellation finishes the current audit transaction, then stops.
    Restart is idempotent. No streams, leases, counters, or old records are freed.
    """
    if now_utc_ns < 0:
        raise ValueError("Timing retirement time must be nonnegative")
    async for rows in _constraint_pages(database):
        candidates = []
        for _, raw, retired in rows:
            if raw is None or retired:
                continue
            try:
                constraint = CONSTRAINT_ADAPTER.validate_json(str(raw))
            except ValidationError:
                continue
            if is_legacy_marketplace_timing_constraint(constraint):
                candidates.append(constraint)
        await _audit(
            database,
            category="constraints",
            provenance=provenance,
            invocation=invocation,
            new_identifier=new_identifier,
            now=now_utc_ns,
            constraints=tuple(candidates),
        )
    for table, columns, planner in (
        (
            "work_items",
            "id,kind_parts_json,state,attempt,eligible_at_utc_ns,created_at_utc_ns",
            _work_plan,
        ),
        (
            "network_activities",
            "id,kind_parts_json,state,eligible_at_utc_ns,created_at_utc_ns,operation_id",
            _network_plan,
        ),
        (
            "work_stages",
            f"id,state,eligible_at_utc_ns,created_at_utc_ns,execution,CASE WHEN length(CAST(definition_json AS BLOB))<={_METADATA_BYTES} THEN definition_json END",
            _stage_plan,
        ),
    ):
        async for rows in _pending_pages(database, table, columns, now_utc_ns):
            plans = []
            for row in rows:
                plan = await planner(database, row, now_utc_ns)
                if plan is not None:
                    plans.append(plan)
            await _audit(
                database,
                category=table,
                provenance=provenance,
                invocation=invocation,
                new_identifier=new_identifier,
                now=now_utc_ns,
                changes=tuple(plans),
            )
    async for rows in _pages(database, "scheduler_cooldowns", "work_id,until_ns"):
        plans = []
        async with database._connections.reader() as connection:
            for _, identifier, deadline in rows:
                work = await (
                    await connection.execute(
                        "SELECT kind_parts_json FROM work_items WHERE id=?", (identifier,)
                    )
                ).fetchone()
                if (
                    work is not None
                    and _parts(decode_json(str(work[0]))) in MARKETPLACE_ACQUISITION_WORK_KINDS
                ):
                    plans.append(_Plan("cooldown", str(identifier), _int(deadline), now_utc_ns, 0))
        await _audit(
            database,
            category="cooldowns",
            provenance=provenance,
            invocation=invocation,
            new_identifier=new_identifier,
            now=now_utc_ns,
            changes=tuple(plans),
        )
