"""Synchronous bounded fair discovery, run in the caller's SQLite worker thread."""

from __future__ import annotations

from typing import TYPE_CHECKING

from carl.core.fair_scheduling import (
    FAIR_AGING_NS,
    FairDispatchCursor,
    FairDispatchHorizon,
    FairDispatchLaneHorizon,
    FairLane,
    SchedulingServiceClass,
)
from carl.core.json import encode_json

if TYPE_CHECKING:
    from apsw import Connection, SQLiteValue


def _integer(value: object) -> int:
    assert isinstance(value, int)
    return value


def discover(
    connection: Connection,
    *,
    scope: str,
    now: int,
    limit: int,
    capabilities: str,
    eligible: str | None,
    excluded: tuple[str, ...],
    after: FairDispatchCursor | None,
) -> tuple[tuple[str, ...], FairDispatchCursor | None, bool]:
    """Seek bounded heads within a finite pass despite concurrent admissions."""
    if after is None:
        snapshot = (
            connection.execute(
                """SELECT lane,sequence,interactive_streak,
                    coalesce((SELECT max(rowid) FROM fair_workspace_service),0)
                FROM fair_lanes WHERE scope=? ORDER BY lane LIMIT 6""",
                (scope,),
            )
        ).fetchall()
        horizon = FairDispatchHorizon(
            scope=scope,
            captured_at_utc_ns=now,
            workspace_rowid_high_water=0 if not snapshot else max(0, _integer(snapshot[0][3])),
            lanes=tuple(
                FairDispatchLaneHorizon(
                    lane=FairLane(str(row[0])),
                    sequence=_integer(row[1]),
                    interactive_streak=_integer(row[2]),
                )
                for row in snapshot
            ),
        )
    else:
        horizon = after.horizon
        if horizon.scope != scope or horizon.captured_at_utc_ns != now:
            raise ValueError("A dispatch cursor requires its original scope and frozen pass time")
    membership = """f.active=1 AND f.available_at_utc_ns<=?
        AND EXISTS(SELECT 1 FROM json_each(?) c
            WHERE json_extract(c.value,'$[0]')=f.kind_parts_json
              AND json_extract(c.value,'$[1]')=f.payload_schema_version)
        AND (? IS NULL OR f.work_item_id IN (SELECT value FROM json_each(?)))
        AND f.work_item_id NOT IN (SELECT value FROM json_each(?))"""
    parameters = (now, capabilities, eligible, eligible, encode_json(list(excluded)))
    preference = "CASE WHEN (w.service_class='interactive' AND l.interactive_streak<3) OR (w.service_class='background' AND l.interactive_streak>=3) THEN 0 ELSE 1 END"
    frontier = ""
    bindings: list[SQLiteValue] = [
        encode_json(
            [[lane.lane.value, lane.sequence, lane.interactive_streak] for lane in horizon.lanes]
        ),
        scope,
        horizon.workspace_rowid_high_water,
        *parameters,
    ]
    if after is not None:
        frontier = (
            f"AND ({preference},w.sequence,w.workspace_key,w.lane,w.service_class)>(?,?,?,?,?)"
        )
        bindings.extend(
            (
                after.preference,
                after.workspace_sequence,
                after.workspace_key,
                after.lane.value,
                after.service_class.value,
            )
        )
    bindings.append(limit + 1)
    rows = (
        connection.execute(
            f"""WITH lane_horizons AS MATERIALIZED (
            SELECT json_extract(value,'$[0]') AS lane,
                   json_extract(value,'$[1]') AS maximum_sequence,
                   json_extract(value,'$[2]') AS interactive_streak FROM json_each(?)
        ) SELECT w.lane,w.service_class,w.workspace_key,w.sequence,{preference}
        FROM fair_workspace_service w INDEXED BY fair_workspace_ready_turn
        JOIN lane_horizons l ON l.lane=w.lane
        WHERE w.scope=? AND w.rowid<=? AND w.sequence<=l.maximum_sequence
          AND w.ready_count>0 AND EXISTS(SELECT 1 FROM fair_ready f
            WHERE f.workspace_key=w.workspace_key AND f.service_class=w.service_class
              AND f.lane=w.lane AND {membership}) {frontier}
        ORDER BY {preference},w.sequence,w.workspace_key,w.lane,w.service_class LIMIT ?""",
            bindings,
        )
    ).fetchall()
    work_ids: list[str] = []
    used_roots: dict[tuple[str, str, str], list[str]] = {}
    last: FairDispatchCursor | None = None
    while len(work_ids) < limit:
        progress = False
        for row in rows[:limit]:
            lane, service, key = str(row[0]), str(row[1]), str(row[2])
            # Advance across an examined group even if its root disappears
            # between the group read and the exact root lookup.
            last = FairDispatchCursor(
                horizon=horizon,
                lane=FairLane(lane),
                service_class=SchedulingServiceClass(service),
                workspace_key=key,
                workspace_sequence=_integer(row[3]),
                root_sequence=0,
                root_request_identifier="",
                preference=_integer(row[4]),
            )
            used = used_roots.setdefault((lane, service, key), [])
            all_excluded = encode_json([*excluded, *work_ids])
            root_sql = f"""SELECT r.root_request_identifier,r.sequence FROM fair_root_service r INDEXED BY fair_root_ready_turn
                WHERE r.scope=? AND r.lane=? AND r.service_class=? AND r.workspace_key=?
                  AND r.ready_count>0
                  AND r.root_request_identifier NOT IN (SELECT value FROM json_each(?))
                  AND EXISTS(SELECT 1 FROM fair_ready f WHERE f.root_request_identifier=r.root_request_identifier
                    AND f.lane=r.lane AND {membership})
                ORDER BY r.sequence,r.root_request_identifier LIMIT 1"""
            root_parameters = (
                scope,
                lane,
                service,
                key,
                encode_json(used),
                now,
                capabilities,
                eligible,
                eligible,
                all_excluded,
            )
            root = (connection.execute(root_sql, root_parameters)).fetchone()
            if root is None and used:
                used.clear()
                root = (
                    connection.execute(
                        root_sql,
                        (
                            scope,
                            lane,
                            service,
                            key,
                            "[]",
                            now,
                            capabilities,
                            eligible,
                            eligible,
                            all_excluded,
                        ),
                    )
                ).fetchone()
            if root is None:
                continue
            root_id = str(root[0])
            used.append(root_id)
            # Oldest aged work wins over arbitrary numeric priority. Otherwise
            # preserve priority/eligibility/creation/ID inside this root.
            oldest = (
                connection.execute(
                    f"SELECT f.work_item_id FROM fair_ready f INDEXED BY fair_ready_age WHERE f.root_request_identifier=? AND f.lane=? AND {membership} AND f.ready_since_utc_ns<=? ORDER BY f.ready_since_utc_ns,f.work_item_id LIMIT 1",
                    (
                        root_id,
                        lane,
                        now,
                        capabilities,
                        eligible,
                        eligible,
                        all_excluded,
                        now - FAIR_AGING_NS,
                    ),
                )
            ).fetchone()
            selected = (
                oldest
                or (
                    connection.execute(
                        f"SELECT f.work_item_id FROM fair_ready f INDEXED BY fair_ready_priority WHERE f.root_request_identifier=? AND f.lane=? AND {membership} ORDER BY f.priority DESC,f.available_at_utc_ns,f.created_at_utc_ns,f.work_item_id LIMIT 1",
                        (root_id, lane, now, capabilities, eligible, eligible, all_excluded),
                    )
                ).fetchone()
            )
            if selected is None:
                continue
            work_ids.append(str(selected[0]))
            progress = True
            last = last.model_copy(
                update={
                    "root_sequence": _integer(root[1]),
                    "root_request_identifier": root_id,
                }
            )
            if len(work_ids) == limit:
                break
        if not progress:
            break
    more = len(rows) > limit or len(work_ids) == limit
    return tuple(work_ids), last if more else None, more
