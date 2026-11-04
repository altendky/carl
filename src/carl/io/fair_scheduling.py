"""Small durable fairness ledgers; callers retain the SQLite transaction boundary."""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from typing import TYPE_CHECKING, cast

from pydantic import TypeAdapter

from carl.core.fair_cpu import CpuDrainCandidate, CpuDrainIntent, choose_cpu_drain
from carl.core.fair_scheduling import (
    FAIR_AGING_NS,
    FAIR_INTERACTIVE_BURST,
    FAIR_OFFER_NS,
    FairAdmission,
    FairDispatchCursor,
    FairDispatchHorizon,
    FairDispatchLaneHorizon,
    FairLane,
    SchedulingContext,
    SchedulingServiceClass,
)
from carl.core.json import decode_json, encode_json
from carl.core.models import JsonValue
from carl.core.resources import ResourceRequest
from carl.core.work import WorkRequester

if TYPE_CHECKING:
    from apsw import AsyncConnection, SQLiteValue

LEGACY_CONTEXT = SchedulingContext(root_request_identifier="legacy")
_DRAIN_ADAPTER = TypeAdapter(CpuDrainIntent)


def workspace_key(context: SchedulingContext) -> str:
    return encode_json(
        ["workspace", context.workspace_identifier]
        if context.workspace_identifier is not None
        else ["root", context.root_request_identifier]
    )


def _refresh_sql(identifier: str) -> str:
    lane = """CASE WHEN s.id IS NULL THEN
        coalesce((SELECT CASE WHEN initial.resident=1 THEN 'resident' ELSE initial.lane END
            FROM fair_initial_resources initial WHERE initial.kind_parts_json=w.kind_parts_json
                AND initial.payload_schema_version=w.payload_schema_version),'cpu')
        WHEN json_extract(s.definition_json,'$.resident_session_identifier') IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM resident_sessions resident
                WHERE resident.id=json_extract(s.definition_json,'$.resident_session_identifier')
                  AND resident.state IN ('opening','ready','closing','quarantined')) THEN 'resident'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='analysis') THEN 'analysis'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='http') THEN 'http'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='cpu') THEN 'cpu'
        ELSE 'metadata' END"""
    return f"""
    INSERT INTO fair_ready(work_item_id,root_request_identifier,workspace_key,service_class,
        lane,stage_id,kind_parts_json,payload_schema_version,priority,available_at_utc_ns,
        ready_since_utc_ns,created_at_utc_ns,active,work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns)
    SELECT w.id,coalesce(owner.root_request_identifier,'["legacy"]'),
        coalesce(owner.workspace_key,'["legacy"]'),
        coalesce(owner.service_class,'background'),{lane},s.id,w.kind_parts_json,
        w.payload_schema_version,w.priority,max(w.eligible_at_utc_ns,coalesce(s.eligible_at_utc_ns,0)),
        max(w.created_at_utc_ns,w.eligible_at_utc_ns,
            coalesce(json_extract(s.definition_json,'$.not_before_utc_ns'),0)),w.created_at_utc_ns,
        CASE WHEN w.state IN ('pending','leased') AND
            ((w.state='pending' AND s.id IS NULL AND NOT EXISTS(
                SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active')))
             OR (s.state='pending' AND s.progress_waiting=0)) THEN 1 ELSE 0 END,w.eligible_at_utc_ns,
        CASE WHEN w.state IN ('leased','cancelling') AND s.id IS NULL AND NOT EXISTS(
            SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active'))
            THEN w.lease_expires_at_utc_ns END
    FROM work_items w LEFT JOIN work_scheduling owner ON owner.work_item_id=w.id
    LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running')
    WHERE w.id={identifier}
    ON CONFLICT(work_item_id) DO UPDATE SET
        root_request_identifier=excluded.root_request_identifier,
        workspace_key=excluded.workspace_key,service_class=excluded.service_class,
        lane=excluded.lane,stage_id=excluded.stage_id,kind_parts_json=excluded.kind_parts_json,
        payload_schema_version=excluded.payload_schema_version,priority=excluded.priority,
        available_at_utc_ns=excluded.available_at_utc_ns,
        work_eligible_at_utc_ns=excluded.work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns=excluded.legacy_lease_expires_at_utc_ns,
        ready_since_utc_ns=CASE WHEN
            (fair_ready.stage_id IS excluded.stage_id AND excluded.stage_id IS NOT NULL)
            OR EXISTS(SELECT 1 FROM stage_attempts a WHERE a.work_item_id=excluded.work_item_id
                AND a.state IN ('pending','active') AND a.state='active')
            THEN max(fair_ready.ready_since_utc_ns,excluded.ready_since_utc_ns)
            ELSE excluded.ready_since_utc_ns END,
        active=excluded.active;
    """


_schema = """
CREATE TABLE IF NOT EXISTS fair_initial_resources (
    kind_parts_json TEXT NOT NULL,payload_schema_version INTEGER NOT NULL,
    lane TEXT NOT NULL,resources_json TEXT NOT NULL,resident INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(kind_parts_json,payload_schema_version)
) STRICT;
CREATE TABLE IF NOT EXISTS fair_roots (
    root_request_identifier TEXT PRIMARY KEY,
    workspace_key TEXT NOT NULL,
    service_class TEXT NOT NULL CHECK(service_class IN ('interactive','background')),
    scheduling_json TEXT NOT NULL CHECK(json_valid(scheduling_json))
) STRICT;
CREATE TABLE IF NOT EXISTS work_scheduling (
    work_item_id TEXT PRIMARY KEY REFERENCES work_items(id),
    root_request_identifier TEXT NOT NULL REFERENCES fair_roots(root_request_identifier),
    workspace_key TEXT NOT NULL,
    service_class TEXT NOT NULL CHECK(service_class IN ('interactive','background')),
    scheduling_json TEXT NOT NULL CHECK(json_valid(scheduling_json))
) STRICT;
CREATE INDEX IF NOT EXISTS work_scheduling_root ON work_scheduling(root_request_identifier,work_item_id);
CREATE TABLE IF NOT EXISTS fair_unowned(work_item_id TEXT PRIMARY KEY REFERENCES work_items(id)) STRICT;
CREATE TABLE IF NOT EXISTS fair_scopes(scope TEXT PRIMARY KEY,enabled_at_utc_ns INTEGER NOT NULL,enabled INTEGER NOT NULL DEFAULT 0) STRICT;
CREATE TABLE IF NOT EXISTS fair_lanes (
    scope TEXT NOT NULL REFERENCES fair_scopes(scope), lane TEXT NOT NULL,
    sequence INTEGER NOT NULL DEFAULT 0,interactive_streak INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(scope,lane)
) STRICT;
CREATE TABLE IF NOT EXISTS fair_workspace_service (
    scope TEXT NOT NULL,lane TEXT NOT NULL,service_class TEXT NOT NULL,
    workspace_key TEXT NOT NULL,sequence INTEGER NOT NULL,
    ready_count INTEGER NOT NULL DEFAULT 0 CHECK(ready_count>=0),
    PRIMARY KEY(scope,lane,service_class,workspace_key)
) STRICT;
CREATE INDEX IF NOT EXISTS fair_workspace_turn ON fair_workspace_service(scope,lane,service_class,sequence,workspace_key);
CREATE INDEX IF NOT EXISTS fair_workspace_ready_turn ON fair_workspace_service(scope,lane,service_class,sequence,workspace_key) WHERE ready_count>0;
CREATE INDEX IF NOT EXISTS fair_workspace_count_group ON fair_workspace_service(workspace_key,service_class,lane);
CREATE TABLE IF NOT EXISTS fair_root_service (
    scope TEXT NOT NULL,lane TEXT NOT NULL,service_class TEXT NOT NULL,
    workspace_key TEXT NOT NULL,root_request_identifier TEXT NOT NULL,sequence INTEGER NOT NULL,
    ready_count INTEGER NOT NULL DEFAULT 0 CHECK(ready_count>=0),
    PRIMARY KEY(scope,lane,root_request_identifier)
) STRICT;
CREATE INDEX IF NOT EXISTS fair_root_turn ON fair_root_service(scope,lane,service_class,workspace_key,sequence,root_request_identifier);
CREATE INDEX IF NOT EXISTS fair_root_ready_turn ON fair_root_service(scope,lane,service_class,workspace_key,sequence,root_request_identifier) WHERE ready_count>0;
CREATE INDEX IF NOT EXISTS fair_root_count_group ON fair_root_service(root_request_identifier,lane);
CREATE TABLE IF NOT EXISTS fair_ready (
    work_item_id TEXT PRIMARY KEY REFERENCES work_items(id),root_request_identifier TEXT NOT NULL,
    workspace_key TEXT NOT NULL,service_class TEXT NOT NULL,lane TEXT NOT NULL,stage_id TEXT,
    kind_parts_json TEXT NOT NULL,payload_schema_version INTEGER NOT NULL,priority INTEGER NOT NULL,
    available_at_utc_ns INTEGER NOT NULL,ready_since_utc_ns INTEGER NOT NULL,
    created_at_utc_ns INTEGER NOT NULL,active INTEGER NOT NULL,
    work_eligible_at_utc_ns INTEGER NOT NULL DEFAULT 0,
    legacy_lease_expires_at_utc_ns INTEGER
) STRICT;
CREATE INDEX IF NOT EXISTS fair_ready_group ON fair_ready(root_request_identifier,lane,active,available_at_utc_ns,work_item_id);
CREATE INDEX IF NOT EXISTS fair_ready_workspace ON fair_ready(workspace_key,service_class,lane,active,available_at_utc_ns,root_request_identifier,work_item_id);
CREATE INDEX IF NOT EXISTS fair_ready_priority ON fair_ready(root_request_identifier,lane,active,priority DESC,available_at_utc_ns,created_at_utc_ns,work_item_id);
CREATE INDEX IF NOT EXISTS fair_ready_age ON fair_ready(root_request_identifier,lane,active,ready_since_utc_ns,work_item_id);
CREATE INDEX IF NOT EXISTS fair_ready_initial ON fair_ready(kind_parts_json,payload_schema_version,work_item_id) WHERE active=1 AND stage_id IS NULL;
CREATE INDEX IF NOT EXISTS fair_ready_dispatch ON fair_ready(priority DESC,work_eligible_at_utc_ns,created_at_utc_ns,work_item_id) WHERE active=1;
CREATE INDEX IF NOT EXISTS fair_ready_legacy_expiry ON fair_ready(legacy_lease_expires_at_utc_ns,work_item_id) WHERE legacy_lease_expires_at_utc_ns IS NOT NULL;
CREATE TABLE IF NOT EXISTS fair_offers (
    scope TEXT NOT NULL,lane TEXT NOT NULL,work_item_id TEXT NOT NULL REFERENCES work_items(id),
    stage_id TEXT,signature TEXT NOT NULL,resources_json TEXT NOT NULL CHECK(json_valid(resources_json)),
    ready_since_utc_ns INTEGER NOT NULL,expires_at_utc_ns INTEGER NOT NULL,
    PRIMARY KEY(scope,lane,work_item_id)
) STRICT;
CREATE INDEX IF NOT EXISTS fair_offers_live ON fair_offers(scope,lane,expires_at_utc_ns,work_item_id);
CREATE INDEX IF NOT EXISTS fair_offers_work ON fair_offers(work_item_id,stage_id,lane);
CREATE TABLE IF NOT EXISTS fair_admissions (
    token TEXT PRIMARY KEY,scope TEXT NOT NULL,lane TEXT NOT NULL,sequence INTEGER NOT NULL,
    work_item_id TEXT NOT NULL REFERENCES work_items(id),scheduling_json TEXT NOT NULL,
    admitted_at_utc_ns INTEGER NOT NULL,UNIQUE(scope,lane,sequence)
) STRICT;
CREATE TABLE IF NOT EXISTS fair_cpu_drains (
    scope TEXT PRIMARY KEY,intent_json TEXT,retry_not_before_utc_ns INTEGER NOT NULL DEFAULT 0
) STRICT;
INSERT OR IGNORE INTO fair_roots VALUES ('["legacy"]','["legacy"]','background',
    '{"root_request_identifier":"legacy","workspace_identifier":null,"service_class":"background"}');
"""

_REFRESH_TRIGGERS: dict[str, str] = {}
for _name, _table, _identity, _events in (
    ("work", "work_items", "NEW.id", ("INSERT", "UPDATE")),
    ("stage", "work_stages", "NEW.work_item_id", ("INSERT", "UPDATE")),
    ("attempt", "stage_attempts", "NEW.work_item_id", ("INSERT", "UPDATE")),
    ("owner", "work_scheduling", "NEW.work_item_id", ("INSERT",)),
    ("resident", "resident_sessions", "NEW.work_item_id", ("INSERT", "UPDATE")),
):
    for _event in _events:
        _trigger = f"fair_refresh_{_name}_{_event.lower()}"
        _REFRESH_TRIGGERS[_trigger] = (
            f"CREATE TRIGGER IF NOT EXISTS {_trigger} AFTER {_event} ON {_table} BEGIN {_refresh_sql(_identity)} END;\n"
        )
_schema += "".join(_REFRESH_TRIGGERS.values())
_schema += """
CREATE TRIGGER IF NOT EXISTS fair_ready_count_insert AFTER INSERT ON fair_ready
WHEN NEW.active=1 BEGIN
    UPDATE fair_workspace_service SET ready_count=ready_count+1
    WHERE workspace_key=NEW.workspace_key AND service_class=NEW.service_class AND lane=NEW.lane;
    UPDATE fair_root_service SET ready_count=ready_count+1
    WHERE root_request_identifier=NEW.root_request_identifier AND lane=NEW.lane;
END;
CREATE TRIGGER IF NOT EXISTS fair_ready_count_delete AFTER DELETE ON fair_ready
WHEN OLD.active=1 BEGIN
    UPDATE fair_workspace_service SET ready_count=ready_count-1
    WHERE workspace_key=OLD.workspace_key AND service_class=OLD.service_class AND lane=OLD.lane;
    UPDATE fair_root_service SET ready_count=ready_count-1
    WHERE root_request_identifier=OLD.root_request_identifier AND lane=OLD.lane;
END;
CREATE TRIGGER IF NOT EXISTS fair_ready_count_update AFTER UPDATE ON fair_ready
WHEN (OLD.active,OLD.workspace_key,OLD.service_class,OLD.root_request_identifier,OLD.lane)
    IS NOT (NEW.active,NEW.workspace_key,NEW.service_class,NEW.root_request_identifier,NEW.lane)
BEGIN
    UPDATE fair_workspace_service SET ready_count=ready_count-1
    WHERE OLD.active=1 AND workspace_key=OLD.workspace_key AND service_class=OLD.service_class AND lane=OLD.lane;
    UPDATE fair_root_service SET ready_count=ready_count-1
    WHERE OLD.active=1 AND root_request_identifier=OLD.root_request_identifier AND lane=OLD.lane;
    UPDATE fair_workspace_service SET ready_count=ready_count+1
    WHERE NEW.active=1 AND workspace_key=NEW.workspace_key AND service_class=NEW.service_class AND lane=NEW.lane;
    UPDATE fair_root_service SET ready_count=ready_count+1
    WHERE NEW.active=1 AND root_request_identifier=NEW.root_request_identifier AND lane=NEW.lane;
END;
CREATE TRIGGER IF NOT EXISTS fair_unowned_work AFTER INSERT ON work_items BEGIN
    INSERT OR IGNORE INTO fair_unowned VALUES (NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS fair_owned_work AFTER INSERT ON work_scheduling BEGIN
    DELETE FROM fair_unowned WHERE work_item_id=NEW.work_item_id;
END;
"""
FAIR_SCHEMA = _schema


async def migrate_logical_ready_age(connection: AsyncConnection) -> None:
    """Replace schema-23 refresh triggers and repair current stage ages atomically."""
    for name in _REFRESH_TRIGGERS:
        await connection.execute(f"DROP TRIGGER IF EXISTS {name}")
    await connection.execute("".join(_REFRESH_TRIGGERS.values()))
    # Drive from live stages, not retained stage/work history. Only undo the
    # identifiable stage-creation floor; preserve polling and explicit wake ages.
    # Progress continuations can have a wake exactly at stage creation in a
    # fixed clock, so retain their age even when that boundary is ambiguous.
    await connection.execute(
        """WITH ages AS MATERIALIZED (
            SELECT stage.work_item_id,stage.id,
                max(work.created_at_utc_ns,work.eligible_at_utc_ns,
                    coalesce(json_extract(stage.definition_json,'$.not_before_utc_ns'),0)) AS logical_age,
                max(work.created_at_utc_ns,work.eligible_at_utc_ns,
                    CASE WHEN attempt.state='pending' THEN 0 ELSE stage.created_at_utc_ns END,
                    coalesce(json_extract(stage.definition_json,'$.not_before_utc_ns'),0)) AS old_age
            FROM work_stages stage INDEXED BY work_stages_active_work
            CROSS JOIN work_items work ON work.id=stage.work_item_id
            LEFT JOIN stage_attempts attempt ON attempt.operation_id=stage.operation_id
            WHERE stage.state IN ('pending','running')
        ), corrections AS MATERIALIZED (
            SELECT ages.work_item_id,ages.id,
                CASE WHEN ready.ready_since_utc_ns=ages.old_age
                    AND ages.id NOT IN (SELECT continuation_id FROM progress_wait_builds)
                THEN ages.logical_age ELSE max(ready.ready_since_utc_ns,ages.logical_age) END AS ready_age
            FROM ages CROSS JOIN fair_ready ready ON ready.work_item_id=ages.work_item_id
                AND ready.stage_id=ages.id
        ) UPDATE fair_ready SET ready_since_utc_ns=corrections.ready_age FROM corrections
        WHERE fair_ready.work_item_id=corrections.work_item_id
            AND fair_ready.ready_since_utc_ns!=corrections.ready_age"""
    )
    # Schema 23 also retained the previous age when a delayed logical retry
    # changed a NULL-stage row. A pending, unstarted attempt uses its own deadline.
    await connection.execute(
        """WITH initial AS MATERIALIZED (
            SELECT work.id,max(work.created_at_utc_ns,work.eligible_at_utc_ns) AS ready_age
            FROM work_items work INDEXED BY work_items_claim
            CROSS JOIN fair_ready ready ON ready.work_item_id=work.id AND ready.stage_id IS NULL
            WHERE work.state='pending' AND NOT EXISTS(
                SELECT 1 FROM stage_attempts attempt WHERE attempt.work_item_id=work.id
                    AND attempt.state IN ('pending','active'))
        ) UPDATE fair_ready SET ready_since_utc_ns=initial.ready_age FROM initial
        WHERE fair_ready.work_item_id=initial.id AND fair_ready.ready_since_utc_ns!=initial.ready_age"""
    )
    # Offers copy readiness for weighted CPU draining. Keep their stage binding,
    # signature and expiry intact while bringing their copied age up to date.
    await connection.execute(
        """UPDATE fair_offers SET ready_since_utc_ns=ready.ready_since_utc_ns
        FROM fair_ready ready WHERE fair_offers.work_item_id=ready.work_item_id
            AND fair_offers.stage_id IS ready.stage_id
            AND fair_offers.ready_since_utc_ns!=ready.ready_since_utc_ns"""
    )


def _integer(value: object) -> int:
    assert isinstance(value, int)
    return value


async def enabled(connection: AsyncConnection, scope: str) -> bool:
    row = await (
        await connection.execute("SELECT 1 FROM fair_scopes WHERE scope=? AND enabled=1", (scope,))
    ).fetchone()
    return row is not None


async def ensure_services(
    connection: AsyncConnection, context: SchedulingContext, root_key: str, key: str
) -> None:
    await connection.execute(
        """INSERT OR IGNORE INTO fair_workspace_service
        (scope,lane,service_class,workspace_key,sequence,ready_count)
        SELECT l.scope,l.lane,?,?,l.sequence,
            (SELECT count(*) FROM fair_ready f WHERE f.workspace_key=?
                AND f.service_class=? AND f.lane=l.lane AND f.active=1)
        FROM fair_lanes l WHERE NOT EXISTS(SELECT 1 FROM fair_workspace_service s
            WHERE s.scope=l.scope AND s.lane=l.lane AND s.service_class=? AND s.workspace_key=?)""",
        (
            context.service_class.value,
            key,
            key,
            context.service_class.value,
            context.service_class.value,
            key,
        ),
    )
    await connection.execute(
        """INSERT OR IGNORE INTO fair_root_service
        (scope,lane,service_class,workspace_key,root_request_identifier,sequence,ready_count)
        SELECT l.scope,l.lane,?,?,?,l.sequence,
            (SELECT count(*) FROM fair_ready f WHERE f.root_request_identifier=?
                AND f.lane=l.lane AND f.active=1)
        FROM fair_lanes l WHERE NOT EXISTS(SELECT 1 FROM fair_root_service s
            WHERE s.scope=l.scope AND s.lane=l.lane AND s.root_request_identifier=?)""",
        (context.service_class.value, key, root_key, root_key, root_key),
    )


async def initialize_service_counts(connection: AsyncConnection) -> None:
    """Initialize the live-group projection once during the schema upgrade."""
    await connection.execute(
        """UPDATE fair_workspace_service SET ready_count=(SELECT count(*) FROM fair_ready f
        WHERE f.workspace_key=fair_workspace_service.workspace_key
          AND f.service_class=fair_workspace_service.service_class
          AND f.lane=fair_workspace_service.lane AND f.active=1)"""
    )
    await connection.execute(
        """UPDATE fair_root_service SET ready_count=(SELECT count(*) FROM fair_ready f
        WHERE f.root_request_identifier=fair_root_service.root_request_identifier
          AND f.lane=fair_root_service.lane AND f.active=1)"""
    )


async def initialize_legacy_lease_projection(connection: AsyncConnection) -> None:
    """Populate the expiry seek once when upgrading a pre-watch database."""
    await connection.execute(
        """UPDATE fair_ready SET legacy_lease_expires_at_utc_ns=(
        SELECT w.lease_expires_at_utc_ns FROM work_items w WHERE w.id=fair_ready.work_item_id
          AND w.state IN ('leased','cancelling') AND NOT EXISTS(
            SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active'))
          AND NOT EXISTS(SELECT 1 FROM work_stages s WHERE s.work_item_id=w.id AND s.state IN ('pending','running')))"""
    )


async def scheduling(connection: AsyncConnection, work_identifier: str) -> SchedulingContext:
    row = await (
        await connection.execute(
            "SELECT scheduling_json FROM work_scheduling WHERE work_item_id=?", (work_identifier,)
        )
    ).fetchone()
    if row is None:
        exists = await (
            await connection.execute("SELECT 1 FROM work_items WHERE id=?", (work_identifier,))
        ).fetchone()
        if exists is None:
            raise KeyError(work_identifier)
        return LEGACY_CONTEXT
    return SchedulingContext.model_validate_json(str(row[0]))


def _immediate_parent_kind(kind: tuple[str, ...]) -> bool:
    return kind[:3] in {
        ("carl", "facebook", "search_refresh"),
        ("carl", "ebay", "search_refresh"),
        ("carl", "facebook", "listing_details"),
        ("carl", "marketplace", "pipeline_listing"),
    } or kind in {("carl", "facebook", "analysis_batch"), ("carl", "ebay", "followup")}


async def assign_owner(
    connection: AsyncConnection, work_identifier: str, requester: WorkRequester
) -> None:
    context = requester.scheduling
    inherited_key: str | None = None
    inherited_workspace: str | None = None
    if requester.parent_work_identifier is not None:
        parent = await scheduling(connection, requester.parent_work_identifier)
        if context is not None and context != parent:
            raise ValueError(
                "Scheduling context differs from the explicit parent's canonical owner"
            )
        context = parent
        row = await (
            await connection.execute(
                "SELECT root_request_identifier,workspace_key FROM work_scheduling WHERE work_item_id=?",
                (requester.parent_work_identifier,),
            )
        ).fetchone()
        inherited_key, inherited_workspace = (
            ('["legacy"]', '["legacy"]') if row is None else (str(row[0]), str(row[1]))
        )
    existing = await (
        await connection.execute(
            "SELECT 1 FROM work_scheduling WHERE work_item_id=?", (work_identifier,)
        )
    ).fetchone()
    if existing is not None:
        return
    if context is None:
        # Only recognized immediate-work parent IDs and strong child context keys
        # are scheduling ancestry. Arbitrary evidence strings are not authority.
        identifier_is_parent = _immediate_parent_kind(requester.kind)
        row = await (
            await connection.execute(
                """SELECT work.id FROM work_items work WHERE work.id!=? AND work.id IN (
                ?,json_extract(?,'$.search_refresh_work_identifier'),
                json_extract(?,'$.root_work_identifier'),json_extract(?,'$.listing_details_work_identifier'),
                (SELECT work_item_id FROM work_operations WHERE operation_id=?))
                ORDER BY work.created_at_utc_ns,work.id LIMIT 1""",
                (
                    work_identifier,
                    requester.identifier if identifier_is_parent else None,
                    encode_json(requester.context),
                    encode_json(requester.context),
                    encode_json(requester.context),
                    requester.context.get("operation_identifier")
                    if requester.kind == ("carl", "http", "acquisition")
                    and isinstance(requester.context, dict)
                    and isinstance(requester.context.get("operation_identifier"), str)
                    else None,
                ),
            )
        ).fetchone()
        if row is not None:
            context = await scheduling(connection, str(row[0]))
            parent_row = await (
                await connection.execute(
                    "SELECT root_request_identifier,workspace_key FROM work_scheduling WHERE work_item_id=?",
                    (str(row[0]),),
                )
            ).fetchone()
            inherited_key, inherited_workspace = (
                ('["legacy"]', '["legacy"]')
                if parent_row is None
                else (str(parent_row[0]), str(parent_row[1]))
            )
        elif requester.kind in {
            ("carl", "mcp", "create_workspace_search"),
            ("carl", "mcp", "create_workspace_search_revision"),
            ("carl", "mcp", "request_workspace_refresh"),
            ("carl", "mcp", "request_selection_analyses"),
        }:
            context = SchedulingContext(
                root_request_identifier=work_identifier, workspace_identifier=requester.identifier
            )
        elif requester.kind[:2] == ("carl", "mcp"):
            workspace = None
            if requester.kind[-1] == "request_listing_route_refresh":
                route = await (
                    await connection.execute(
                        "SELECT json_extract(payload_json,'$.request.workspace_record_identifier') FROM work_items WHERE id=?",
                        (work_identifier,),
                    )
                ).fetchone()
                workspace = None if route is None or route[0] is None else str(route[0])
            context = SchedulingContext(
                root_request_identifier=work_identifier, workspace_identifier=workspace
            )
        else:
            context = LEGACY_CONTEXT
            inherited_key, inherited_workspace = '["legacy"]', '["legacy"]'
    await store_owner(connection, work_identifier, context, inherited_key, inherited_workspace)


async def store_owner(
    connection: AsyncConnection,
    work_identifier: str,
    context: SchedulingContext,
    root_key: str | None = None,
    key: str | None = None,
) -> None:
    root_key = (
        encode_json(["root", context.root_request_identifier]) if root_key is None else root_key
    )
    key = workspace_key(context) if key is None else key
    row = await (
        await connection.execute(
            "SELECT scheduling_json FROM fair_roots WHERE root_request_identifier=?", (root_key,)
        )
    ).fetchone()
    if row is not None and SchedulingContext.model_validate_json(str(row[0])) != context:
        raise ValueError("A scheduling root cannot change workspace or service class")
    await connection.execute(
        "INSERT OR IGNORE INTO fair_roots VALUES (?,?,?,?)",
        (root_key, key, context.service_class.value, context.model_dump_json()),
    )
    await connection.execute(
        "INSERT OR IGNORE INTO work_scheduling VALUES (?,?,?,?,?)",
        (work_identifier, root_key, key, context.service_class.value, context.model_dump_json()),
    )
    await ensure_services(connection, context, root_key, key)


async def backfill_owner(
    connection: AsyncConnection,
    work_identifier: str,
    *,
    ancestors: frozenset[str] = frozenset(),
) -> SchedulingContext:
    existing = await (
        await connection.execute(
            "SELECT scheduling_json FROM work_scheduling WHERE work_item_id=?", (work_identifier,)
        )
    ).fetchone()
    if existing is not None:
        return SchedulingContext.model_validate_json(str(existing[0]))
    if work_identifier in ancestors or len(ancestors) >= 64:
        return LEGACY_CONTEXT
    row = await (
        await connection.execute(
            """SELECT w.kind_parts_json,json_extract(w.payload_json,'$.workspace_record_identifier'),
            json_extract(w.payload_json,'$.request_identifier'),r.id,r.requester_kind_parts_json,
            r.requester_identifier,r.parent_work_identifier,r.scheduling_json,
            json_object('search_refresh_work_identifier',json_extract(r.context_json,'$.search_refresh_work_identifier'),
                'root_work_identifier',json_extract(r.context_json,'$.root_work_identifier'),
                'listing_details_work_identifier',json_extract(r.context_json,'$.listing_details_work_identifier'),
                'operation_identifier',json_extract(r.context_json,'$.operation_identifier')),
            json_extract(w.payload_json,'$.request.workspace_record_identifier')
        FROM work_items w LEFT JOIN work_requests r ON r.id=(
            SELECT id FROM work_requests WHERE work_item_id=w.id ORDER BY rowid LIMIT 1)
        WHERE w.id=?""",
            (work_identifier,),
        )
    ).fetchone()
    if row is None:
        raise KeyError(work_identifier)
    context = None if row[7] is None else SchedulingContext.model_validate_json(str(row[7]))
    parent = None if row[6] is None else str(row[6])
    safe = cast(dict[str, JsonValue], decode_json(str(row[8])))
    if parent is None:
        request_kind = () if row[4] is None else tuple(cast(list[str], decode_json(str(row[4]))))
        identifier_is_parent = _immediate_parent_kind(request_kind)
        possible = ([str(row[5])] if identifier_is_parent else []) + [
            str(value)
            for key, value in safe.items()
            if key != "operation_identifier" and isinstance(value, str)
        ]
        if request_kind == ("carl", "http", "acquisition") and isinstance(
            safe.get("operation_identifier"), str
        ):
            operation = await (
                await connection.execute(
                    "SELECT work_item_id FROM work_operations WHERE operation_id=?",
                    (safe["operation_identifier"],),
                )
            ).fetchone()
            if operation is not None:
                possible.insert(0, str(operation[0]))
        for candidate in possible:
            if candidate == work_identifier:
                continue
            exists = await (
                await connection.execute("SELECT 1 FROM work_items WHERE id=?", (candidate,))
            ).fetchone()
            if exists is not None:
                parent = candidate
                break
    if parent is not None:
        inherited = await backfill_owner(
            connection, parent, ancestors=ancestors | {work_identifier}
        )
        context = inherited
    if context is None and str(row[0]) == '["carl","marketplace","work","search_pipeline"]':
        context = SchedulingContext(
            root_request_identifier=work_identifier,
            workspace_identifier=None if row[1] is None else str(row[1]),
        )
    kind = ("legacy",) if row[4] is None else tuple(cast(list[str], decode_json(str(row[4]))))
    if context is None and kind[:2] == ("carl", "mcp"):
        workspace = (
            str(row[5])
            if kind[-1]
            in {
                "create_workspace_search",
                "create_workspace_search_revision",
                "request_workspace_refresh",
                "request_selection_analyses",
            }
            else None
        )
        if kind[-1] == "request_listing_route_refresh" and row[9] is not None:
            workspace = str(row[9])
        context = SchedulingContext(
            root_request_identifier=work_identifier, workspace_identifier=workspace
        )
    context = LEGACY_CONTEXT if context is None else context
    if parent is not None:
        parent_row = await (
            await connection.execute(
                "SELECT root_request_identifier,workspace_key FROM work_scheduling WHERE work_item_id=?",
                (parent,),
            )
        ).fetchone()
        internal, key = (
            ('["legacy"]', '["legacy"]')
            if parent_row is None
            else (str(parent_row[0]), str(parent_row[1]))
        )
    elif context is LEGACY_CONTEXT:
        internal, key = '["legacy"]', '["legacy"]'
    else:
        internal, key = (
            encode_json(["root", context.root_request_identifier]),
            workspace_key(context),
        )
    await store_owner(connection, work_identifier, context, internal, key)
    return context


async def enable(connection: AsyncConnection, scope: str, now: int) -> None:
    if not scope or now < 0:
        raise ValueError("Fair scheduling scope/time is invalid")
    await connection.execute(
        "INSERT OR IGNORE INTO fair_scopes(scope,enabled_at_utc_ns) VALUES (?,?)", (scope, now)
    )
    await connection.executemany(
        "INSERT OR IGNORE INTO fair_lanes(scope,lane) VALUES (?,?)",
        [(scope, lane.value) for lane in FairLane],
    )


async def discover(
    connection: AsyncConnection,
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
        snapshot = await (
            await connection.execute(
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
    rows = await (
        await connection.execute(
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
            root = await (await connection.execute(root_sql, root_parameters)).fetchone()
            if root is None and used:
                used.clear()
                root = await (
                    await connection.execute(
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
            oldest = await (
                await connection.execute(
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
                or await (
                    await connection.execute(
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


async def promote_initial_offers(
    connection: AsyncConnection,
    *,
    work_identifier: str,
    stage_identifier: str,
    serialized_definition: str,
    resources: tuple[ResourceRequest, ...],
) -> None:
    """Keep a driver's matching initial offer live across atomic registration."""
    lane = lane_for(resources)
    encoded = encode_json([request.model_dump(mode="json") for request in resources])
    await connection.execute(
        """UPDATE fair_offers SET stage_id=?,signature=? WHERE work_item_id=?
        AND stage_id IS NULL AND lane=? AND resources_json=?""",
        (
            stage_identifier,
            hashlib.sha256(serialized_definition.encode()).hexdigest(),
            work_identifier,
            lane.value,
            encoded,
        ),
    )
    await connection.execute(
        "DELETE FROM fair_offers WHERE work_item_id=? AND stage_id IS NULL AND lane!='resident'",
        (work_identifier,),
    )


async def offer(
    connection: AsyncConnection,
    *,
    scope: str,
    lane: FairLane,
    work_identifier: str,
    stage_identifier: str | None,
    signature: str,
    resources: tuple[ResourceRequest, ...],
    now: int,
    initial: bool = False,
) -> None:
    row = await (
        await connection.execute(
            "SELECT ready_since_utc_ns FROM fair_ready WHERE work_item_id=?", (work_identifier,)
        )
    ).fetchone()
    ready = now if row is None else _integer(row[0])
    await connection.execute(
        """INSERT INTO fair_offers VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(scope,lane,work_item_id) DO UPDATE SET stage_id=excluded.stage_id,
        signature=excluded.signature,resources_json=excluded.resources_json,
        ready_since_utc_ns=excluded.ready_since_utc_ns,expires_at_utc_ns=excluded.expires_at_utc_ns""",
        (
            scope,
            lane.value,
            work_identifier,
            stage_identifier,
            ("initial:" if initial else "") + hashlib.sha256(signature.encode()).hexdigest(),
            encode_json([r.model_dump(mode="json") for r in resources]),
            ready,
            now + FAIR_OFFER_NS,
        ),
    )


async def clear_offer(connection: AsyncConnection, scope: str, lane: FairLane, work: str) -> None:
    await connection.execute(
        "DELETE FROM fair_offers WHERE scope=? AND lane=? AND work_item_id=?",
        (scope, lane.value, work),
    )


async def admission_allowed(
    connection: AsyncConnection,
    *,
    scope: str,
    lane: FairLane,
    work_identifier: str,
    capacities: dict[str, int],
    now: int,
    eligible: Callable[[tuple[tuple[str, str | None], ...], FairLane], Awaitable[frozenset[str]]],
) -> bool:
    """Arbitrate only live driver offers; inactive queue rows reserve no turn."""
    occupied_rows = await (
        await connection.execute(
            """SELECT allocation.name,sum(allocation.units) FROM resource_allocations allocation
            JOIN resource_permits permit ON permit.token=allocation.permit_token
            WHERE permit.scope=? AND permit.state='held' GROUP BY allocation.name""",
            (scope,),
        )
    ).fetchall()
    occupied = {str(row[0]): _integer(row[1]) for row in occupied_rows}
    rows = await (
        await connection.execute(
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
    eligible_work = await eligible(
        tuple((str(row[0]), None if row[1] is None else str(row[1])) for row in rows), lane
    )
    candidates: list[tuple[str, str, tuple[ResourceRequest, ...], int]] = []
    for row in rows:
        identifier = str(row[0])
        requests = tuple(
            ResourceRequest.model_validate_json(encode_json(value))
            for value in cast(list[JsonValue], decode_json(str(row[3])))
        )
        if identifier not in eligible_work:
            await clear_offer(connection, scope, lane, identifier)
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
        previous = await (
            await connection.execute(
                "SELECT intent_json,retry_not_before_utc_ns FROM fair_cpu_drains WHERE scope=?",
                (scope,),
            )
        ).fetchone()
        current = (
            None
            if previous is None or previous[0] is None
            else _DRAIN_ADAPTER.validate_json(str(previous[0]))
        )
        unconfirmed_row = await (
            await connection.execute(
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
        await connection.execute(
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


async def charge(
    connection: AsyncConnection,
    *,
    scope: str,
    lane: FairLane,
    work_identifier: str,
    token: str,
    now: int,
) -> FairAdmission:
    context = await scheduling(connection, work_identifier)
    owner = await (
        await connection.execute(
            "SELECT root_request_identifier,workspace_key FROM work_scheduling WHERE work_item_id=?",
            (work_identifier,),
        )
    ).fetchone()
    root_key, key = (
        ('["legacy"]', '["legacy"]') if owner is None else (str(owner[0]), str(owner[1]))
    )
    await ensure_services(connection, context, root_key, key)
    await connection.execute(
        "UPDATE fair_lanes SET sequence=sequence+1, interactive_streak=CASE WHEN ?='interactive' THEN min(interactive_streak+1,?) ELSE 0 END WHERE scope=? AND lane=?",
        (context.service_class.value, FAIR_INTERACTIVE_BURST, scope, lane.value),
    )
    row = await (
        await connection.execute(
            "SELECT sequence FROM fair_lanes WHERE scope=? AND lane=?", (scope, lane.value)
        )
    ).fetchone()
    assert row is not None
    sequence = _integer(row[0])
    await connection.execute(
        "UPDATE fair_workspace_service SET sequence=? WHERE scope=? AND lane=? AND service_class=? AND workspace_key=?",
        (sequence, scope, lane.value, context.service_class.value, key),
    )
    await connection.execute(
        "UPDATE fair_root_service SET sequence=? WHERE scope=? AND lane=? AND root_request_identifier=?",
        (sequence, scope, lane.value, root_key),
    )
    await connection.execute(
        "INSERT INTO fair_admissions VALUES (?,?,?,?,?,?,?)",
        (token, scope, lane.value, sequence, work_identifier, context.model_dump_json(), now),
    )
    await clear_offer(connection, scope, lane, work_identifier)
    if lane is FairLane.CPU:
        await connection.execute(
            "UPDATE fair_cpu_drains SET intent_json=NULL WHERE scope=? AND json_extract(intent_json,'$.identifier')=?",
            (scope, work_identifier),
        )
    return FairAdmission(
        scope=scope,
        lane=lane,
        sequence=sequence,
        work_item_identifier=work_identifier,
        token=token,
        scheduling=context,
        admitted_at_utc_ns=now,
    )
