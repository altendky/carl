"""Bounded diagnostic projections; telemetry never grants execution authority."""

from __future__ import annotations

import hashlib
from collections import Counter
from typing import TYPE_CHECKING, cast

from carl.core.json import decode_json, encode_json
from carl.core.models import JsonValue
from carl.core.scheduler_activity import (
    SCHEDULER_DURATION_BUCKETS_NS,
    SchedulerActivity,
    SchedulerCoverage,
    SchedulerResourceActivity,
    SchedulerStageCounts,
    SchedulerThroughput,
    SchedulerTimingSummary,
    SchedulerWaitGroup,
    SchedulerWaitObservation,
    SchedulerWorkCounts,
    SchedulerWorkStatus,
)

if TYPE_CHECKING:
    from apsw import AsyncConnection

MINUTE_NS = 60_000_000_000
RETENTION_NS = 24 * 60 * MINUTE_NS
WAIT_TTL_NS = 5_000_000_000
WAIT_ROW_CAP = 4096
SAMPLE_ROW_CAP = 8192
ROLLUP_ROW_CAP = 65536
RESOURCE_NAMES = (
    "http",
    "cpu",
    "analysis",
    "routing_engines",
    "resident_sessions",
    "resident_state_bytes",
)


def bounded_parts(value: str) -> tuple[tuple[str, ...], bool]:
    raw = cast(list[JsonValue], decode_json(value))
    return tuple(str(part)[:64] for part in raw[:8]), len(raw) > 8 or any(
        len(str(part)) > 64 for part in raw
    )


def label(value: str | None) -> str | None:
    return None if value is None else value[:160]


def payload_projection(column: str) -> str:
    def scalar(path: str) -> str:
        return f"substr(json_extract({column},'{path}'),1,161)"

    return f"""json_object('request',json_object('query',{scalar("$.request.query")}),
        'search',json_object('request',json_object('query',{scalar("$.search.request.query")})),
        'listing_id',{scalar("$.listing_id")},'reference',json_object('listing_id',{scalar("$.reference.listing_id")},
        'gallery_order',CASE WHEN typeof(json_extract({column},'$.reference.gallery_order'))='integer' THEN json_extract({column},'$.reference.gallery_order') END),
        'model',{scalar("$.model")},'effort',{scalar("$.effort")})"""


def result_projection(column: str) -> str:
    return f"json_object('stage',substr(json_extract({column},'$.stage'),1,161))"


def error_projection(column: str) -> str:
    return f"json_object('kind',substr(json_extract({column},'$.kind'),1,161))"


def probe_projection(column: str) -> str:
    return f"""CASE WHEN {column} IS NULL THEN NULL ELSE json_object(
        'reachable',json(CASE WHEN json_extract({column},'$.reachable')=1 THEN 'true' ELSE 'false' END),
        'checks',json(coalesce((SELECT json_group_array(json_patch(json_object(
            'host',substr(json_extract(value,'$.host'),1,160),
            'reachable',json(CASE WHEN json_extract(value,'$.reachable')=1 THEN 'true' ELSE 'false' END)
        ),CASE WHEN json_extract(value,'$.type') IN ('TimeoutError','OSError','ConnectionError','gaierror') THEN json_object('type',json_extract(value,'$.type')) ELSE '{{}}' END)) FROM (SELECT value FROM json_each({column},'$.checks') LIMIT 8)),'[]'))) END"""


_gauge_schema = ""
for _subject, _table, _states in (
    (
        "work",
        "work_items",
        ("pending", "leased", "completed", "terminal_failure", "cancelling", "cancelled"),
    ),
    ("stage", "work_stages", ("pending", "running", "completed", "cancelled")),
):
    for _state in _states:
        _gauge_schema += (
            f"INSERT OR IGNORE INTO scheduler_gauges VALUES('{_subject}','{_state}',0);\n"
        )
    _gauge_schema += f"""
    CREATE TRIGGER IF NOT EXISTS scheduler_{_subject}_insert AFTER INSERT ON {_table} BEGIN
        UPDATE scheduler_gauges SET value=value+1 WHERE subject='{_subject}' AND state=NEW.state;
    END;
    CREATE TRIGGER IF NOT EXISTS scheduler_{_subject}_state AFTER UPDATE OF state ON {_table} WHEN NEW.state!=OLD.state BEGIN
        UPDATE scheduler_gauges SET value=value-1 WHERE subject='{_subject}' AND state=OLD.state;
        UPDATE scheduler_gauges SET value=value+1 WHERE subject='{_subject}' AND state=NEW.state;
    END;
    CREATE TRIGGER IF NOT EXISTS scheduler_{_subject}_delete AFTER DELETE ON {_table} BEGIN
        UPDATE scheduler_gauges SET value=value-1 WHERE subject='{_subject}' AND state=OLD.state;
    END;
    """

SCHEDULER_SCHEMA = (
    """
CREATE TABLE IF NOT EXISTS scheduler_meta(key TEXT PRIMARY KEY,value INTEGER NOT NULL) STRICT;
INSERT OR IGNORE INTO scheduler_meta VALUES('initialized',0),('started',0),('rollup_rows',0),('sample_rows',0),('retained_since',0),('dropped_samples',0),('active_loss_at',0);
CREATE TABLE IF NOT EXISTS scheduler_gauges(subject TEXT NOT NULL,state TEXT NOT NULL,value INTEGER NOT NULL,PRIMARY KEY(subject,state)) STRICT,WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS scheduler_waits(
    scope TEXT NOT NULL,work_id TEXT NOT NULL REFERENCES work_items(id),stage_id TEXT,
    reason TEXT NOT NULL,resources_json TEXT NOT NULL,source TEXT NOT NULL,
    started_ns INTEGER NOT NULL,last_seen_ns INTEGER NOT NULL,expires_ns INTEGER NOT NULL,
    PRIMARY KEY(scope,work_id)
) STRICT,WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS scheduler_waits_expiry ON scheduler_waits(expires_ns,scope,work_id);
CREATE INDEX IF NOT EXISTS scheduler_waits_scope ON scheduler_waits(scope,started_ns,work_id);
CREATE INDEX IF NOT EXISTS scheduler_waits_work ON scheduler_waits(work_id,scope);
CREATE TABLE IF NOT EXISTS scheduler_executions(
    stage_id TEXT NOT NULL,execution INTEGER NOT NULL,scope TEXT NOT NULL,work_id TEXT NOT NULL,
    started_ns INTEGER NOT NULL,ended_ns INTEGER,outcome TEXT,
    PRIMARY KEY(stage_id,execution)
) STRICT,WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS scheduler_executions_time ON scheduler_executions(started_ns,stage_id,execution);
CREATE INDEX IF NOT EXISTS scheduler_executions_finished ON scheduler_executions(ended_ns,stage_id,execution) WHERE ended_ns IS NOT NULL;
CREATE TABLE IF NOT EXISTS scheduler_rollups(
    scope TEXT NOT NULL,minute_ns INTEGER NOT NULL,metric TEXT NOT NULL,method TEXT NOT NULL,bucket INTEGER NOT NULL,
    count INTEGER NOT NULL,total_ns INTEGER NOT NULL,maximum_ns INTEGER NOT NULL,censored INTEGER NOT NULL,
    PRIMARY KEY(scope,minute_ns,metric,method,bucket)
) STRICT,WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS scheduler_rollups_time ON scheduler_rollups(minute_ns,scope,metric,method,bucket);
CREATE TABLE IF NOT EXISTS scheduler_cooldowns(work_id TEXT PRIMARY KEY REFERENCES work_items(id),reason TEXT NOT NULL,until_ns INTEGER NOT NULL) STRICT;
CREATE INDEX IF NOT EXISTS scheduler_cooldowns_expiry ON scheduler_cooldowns(until_ns,work_id);
CREATE TRIGGER IF NOT EXISTS scheduler_rollup_insert AFTER INSERT ON scheduler_rollups BEGIN
    UPDATE scheduler_meta SET value=value+1 WHERE key='rollup_rows'; END;
CREATE TRIGGER IF NOT EXISTS scheduler_rollup_delete AFTER DELETE ON scheduler_rollups BEGIN
    UPDATE scheduler_meta SET value=value-1 WHERE key='rollup_rows'; END;
CREATE TRIGGER IF NOT EXISTS scheduler_sample_insert AFTER INSERT ON scheduler_executions BEGIN
    UPDATE scheduler_meta SET value=value+1 WHERE key='sample_rows'; END;
CREATE TRIGGER IF NOT EXISTS scheduler_sample_delete AFTER DELETE ON scheduler_executions BEGIN
    UPDATE scheduler_meta SET value=value-1 WHERE key='sample_rows'; END;
INSERT OR IGNORE INTO scheduler_gauges VALUES('stage','progress_waiting',0);
CREATE TRIGGER IF NOT EXISTS scheduler_progress_insert AFTER INSERT ON work_stages WHEN NEW.state='pending' AND NEW.progress_waiting=1 BEGIN
    UPDATE scheduler_gauges SET value=value+1 WHERE subject='stage' AND state='progress_waiting'; END;
CREATE TRIGGER IF NOT EXISTS scheduler_progress_state AFTER UPDATE OF state,progress_waiting ON work_stages BEGIN
    UPDATE scheduler_gauges SET value=value-(OLD.state='pending' AND OLD.progress_waiting=1)+(NEW.state='pending' AND NEW.progress_waiting=1)
    WHERE subject='stage' AND state='progress_waiting'; END;
CREATE TRIGGER IF NOT EXISTS scheduler_progress_delete AFTER DELETE ON work_stages WHEN OLD.state='pending' AND OLD.progress_waiting=1 BEGIN
    UPDATE scheduler_gauges SET value=value-1 WHERE subject='stage' AND state='progress_waiting'; END;
"""
    + _gauge_schema
)


def handle(value: str | None) -> str:
    return hashlib.sha256((value or "unconfigured").encode()).hexdigest()[:24]


async def initialize(connection: AsyncConnection) -> None:
    row = await (
        await connection.execute("SELECT value FROM scheduler_meta WHERE key='initialized'")
    ).fetchone()
    if row is not None and row[0] == 1:
        return
    for subject, table in (("work", "work_items"), ("stage", "work_stages")):
        await connection.execute("UPDATE scheduler_gauges SET value=0 WHERE subject=?", (subject,))
        rows = await (
            await connection.execute(f"SELECT state,count(*) FROM {table} GROUP BY state")
        ).fetchall()
        for state, count in rows:
            await connection.execute(
                "UPDATE scheduler_gauges SET value=? WHERE subject=? AND state=?",
                (count, subject, state),
            )
    await connection.execute(
        "UPDATE scheduler_gauges SET value=(SELECT count(*) FROM work_stages WHERE state='pending' AND progress_waiting=1) WHERE subject='stage' AND state='progress_waiting'"
    )
    await connection.execute("UPDATE scheduler_meta SET value=1 WHERE key='initialized'")


async def _started(connection: AsyncConnection, now: int) -> None:
    await connection.execute(
        "UPDATE scheduler_meta SET value=? WHERE key='started' AND (value=0 OR value>?)", (now, now)
    )


async def rollup(
    connection: AsyncConnection,
    scope: str,
    now: int,
    metric: str,
    *,
    duration: int | None = None,
    method: str = "observed_transitions",
    count: int = 1,
    censored: bool = False,
) -> None:
    await _started(connection, now)
    bucket = -1
    if duration is not None and duration >= 0 and not censored:
        bucket = next(
            (
                index
                for index, upper in enumerate(SCHEDULER_DURATION_BUCKETS_NS)
                if duration <= upper
            ),
            len(SCHEDULER_DURATION_BUCKETS_NS),
        )
    valid = duration is not None and duration >= 0 and not censored
    await connection.execute(
        """INSERT INTO scheduler_rollups VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(scope,minute_ns,metric,method,bucket) DO UPDATE SET
        count=count+excluded.count,total_ns=total_ns+excluded.total_ns,maximum_ns=max(maximum_ns,excluded.maximum_ns),censored=censored+excluded.censored""",
        (
            scope,
            now // MINUTE_NS * MINUTE_NS,
            metric,
            method,
            bucket,
            0 if censored else count,
            duration if valid else 0,
            duration if valid else 0,
            int(censored),
        ),
    )
    size = await (
        await connection.execute("SELECT value FROM scheduler_meta WHERE key='rollup_rows'")
    ).fetchone()
    if size is not None and cast(int, size[0]) > ROLLUP_ROW_CAP:
        evicted = await (
            await connection.execute(
                "SELECT minute_ns FROM scheduler_rollups ORDER BY minute_ns,scope,metric,method,bucket LIMIT 64"
            )
        ).fetchall()
        if evicted:
            await connection.execute(
                "UPDATE scheduler_meta SET value=max(value,?) WHERE key='retained_since'",
                (max(cast(int, row[0]) for row in evicted) + MINUTE_NS,),
            )
        await connection.execute(
            "DELETE FROM scheduler_rollups WHERE (scope,minute_ns,metric,method,bucket) IN (SELECT scope,minute_ns,metric,method,bucket FROM scheduler_rollups ORDER BY minute_ns,scope,metric,method,bucket LIMIT 64)"
        )


async def close_wait(
    connection: AsyncConnection, scope: str, work: str, now: int, *, admitted: bool = False
) -> None:
    row = await (
        await connection.execute(
            "SELECT reason,started_ns,last_seen_ns,expires_ns FROM scheduler_waits WHERE scope=? AND work_id=?",
            (scope, work),
        )
    ).fetchone()
    if row is None:
        return
    if row[0] == "resource":
        duration = now - cast(int, row[1])
        await rollup(
            connection,
            scope,
            now,
            "observed_resource_wait",
            duration=duration,
            method="observed_lower_bound",
            censored=not admitted or cast(int, row[3]) < now or duration < 0,
        )
    await connection.execute(
        "DELETE FROM scheduler_waits WHERE scope=? AND work_id=?", (scope, work)
    )


async def observe(
    connection: AsyncConnection,
    scope: str,
    observations: tuple[SchedulerWaitObservation, ...],
    now: int,
) -> None:
    for observation in observations:
        row = await (
            await connection.execute(
                "SELECT w.state,s.id,s.state FROM work_items w LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running') WHERE w.id=?",
                (observation.work_identifier,),
            )
        ).fetchone()
        if (
            row is None
            or row[0] not in ("pending", "leased")
            or row[2] == "running"
            or observation.stage_identifier != row[1]
        ):
            continue
        old = await (
            await connection.execute(
                "SELECT stage_id,reason,source,started_ns,last_seen_ns FROM scheduler_waits WHERE scope=? AND work_id=?",
                (scope, observation.work_identifier),
            )
        ).fetchone()
        same = (
            old is not None
            and cast(int, old[3]) <= now <= cast(int, old[4]) + WAIT_TTL_NS
            and (old[0], old[1], old[2])
            == (
                observation.stage_identifier,
                observation.reason,
                observation.source,
            )
        )
        if same and old is not None and now - cast(int, old[4]) < 1_000_000_000:
            continue
        if old is not None and not same:
            await close_wait(connection, scope, observation.work_identifier, now)
        await connection.execute(
            """INSERT INTO scheduler_waits VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(scope,work_id) DO UPDATE SET stage_id=excluded.stage_id,reason=excluded.reason,
            resources_json=excluded.resources_json,source=excluded.source,started_ns=excluded.started_ns,
            last_seen_ns=excluded.last_seen_ns,expires_ns=excluded.expires_ns""",
            (
                scope,
                observation.work_identifier,
                observation.stage_identifier,
                observation.reason,
                encode_json(list(observation.resource_names)),
                observation.source,
                old[3] if same and old is not None else now,
                now,
                now + WAIT_TTL_NS,
            ),
        )
    # A hard cap bounds open observations even for custom enormous queues.
    count = await (await connection.execute("SELECT count(*) FROM scheduler_waits")).fetchone()
    if count is not None and cast(int, count[0]) > WAIT_ROW_CAP:
        await connection.execute(
            "DELETE FROM scheduler_waits WHERE (scope,work_id) IN (SELECT scope,work_id FROM scheduler_waits ORDER BY expires_ns,scope,work_id LIMIT 32)"
        )
        await connection.execute(
            "UPDATE scheduler_meta SET value=value+? WHERE key='dropped_samples'",
            (await connection.changes(),),
        )


async def claimed(
    connection: AsyncConnection, *, stage: str, execution: int, scope: str, work: str, now: int
) -> None:
    await close_wait(connection, scope, work, now, admitted=True)
    others = await (
        await connection.execute(
            "SELECT scope FROM scheduler_waits WHERE work_id=? LIMIT 32", (work,)
        )
    ).fetchall()
    for other in others:
        await close_wait(connection, str(other[0]), work, now)
    await connection.execute(
        "INSERT OR IGNORE INTO scheduler_executions(stage_id,execution,scope,work_id,started_ns) VALUES(?,?,?,?,?)",
        (stage, execution, scope, work, now),
    )
    if not await connection.changes():
        return
    await rollup(connection, scope, now, "admissions")
    ready = await (
        await connection.execute(
            "SELECT ready_since_utc_ns FROM fair_ready WHERE work_item_id=?", (work,)
        )
    ).fetchone()
    if ready is not None:
        duration = now - cast(int, ready[0])
        await rollup(
            connection,
            scope,
            now,
            "queue_delay",
            duration=duration,
            method="utc_estimate",
            censored=duration < 0,
        )
    size = await (
        await connection.execute("SELECT value FROM scheduler_meta WHERE key='sample_rows'")
    ).fetchone()
    if size is not None and cast(int, size[0]) > SAMPLE_ROW_CAP:
        await connection.execute(
            "DELETE FROM scheduler_executions WHERE (stage_id,execution) IN (SELECT stage_id,execution FROM scheduler_executions WHERE ended_ns IS NOT NULL ORDER BY ended_ns,stage_id,execution LIMIT 32)"
        )
        size = await (
            await connection.execute("SELECT value FROM scheduler_meta WHERE key='sample_rows'")
        ).fetchone()
        if size is not None and cast(int, size[0]) > SAMPLE_ROW_CAP:
            await connection.execute(
                "DELETE FROM scheduler_executions WHERE (stage_id,execution) IN (SELECT stage_id,execution FROM scheduler_executions ORDER BY started_ns,stage_id,execution LIMIT 32)"
            )
            await connection.execute(
                "UPDATE scheduler_meta SET value=value+? WHERE key='dropped_samples'",
                (await connection.changes(),),
            )
            await connection.execute(
                "UPDATE scheduler_meta SET value=max(value,?) WHERE key='active_loss_at'", (now,)
            )


async def after_transition(connection: AsyncConnection, now: int) -> None:
    """End a censored interval only after all potentially missing executions stop."""
    row = await (
        await connection.execute(
            "SELECT value FROM scheduler_meta WHERE key='active_loss_at' AND value>0"
        )
    ).fetchone()
    if row is None:
        return
    running = await (
        await connection.execute(
            "SELECT value FROM scheduler_gauges WHERE subject='stage' AND state='running'"
        )
    ).fetchone()
    if running is not None and running[0] == 0:
        await connection.execute(
            "UPDATE scheduler_meta SET value=max(value,?) WHERE key='retained_since'",
            ((max(now, cast(int, row[0])) // MINUTE_NS + 1) * MINUTE_NS,),
        )
        await connection.execute("UPDATE scheduler_meta SET value=0 WHERE key='active_loss_at'")


async def finished(
    connection: AsyncConnection,
    *,
    stage: str,
    execution: int,
    now: int,
    duration: int | None,
    outcome: str,
) -> None:
    row = await (
        await connection.execute(
            "SELECT scope,ended_ns,work_id FROM scheduler_executions WHERE stage_id=? AND execution=?",
            (stage, execution),
        )
    ).fetchone()
    if row is None or row[1] is not None:
        return
    await connection.execute(
        "UPDATE scheduler_executions SET ended_ns=?,outcome=? WHERE stage_id=? AND execution=?",
        (now, outcome, stage, execution),
    )
    scope = str(row[0])
    await rollup(
        connection,
        scope,
        now,
        "phase_runtime",
        duration=duration,
        method="monotonic",
        censored=duration is None or duration < 0,
    )
    kind = (
        "stage_completed"
        if outcome in ("completed", "suspended")
        else "stage_cancelled"
        if outcome == "cancelled"
        else "stage_failed"
    )
    await rollup(connection, scope, now, kind)


async def physical_release(
    connection: AsyncConnection, token: str, now: int, *, reclaimed: bool = False
) -> None:
    row = await (
        await connection.execute(
            "SELECT scope,acquired_at_utc_ns FROM resource_permits WHERE token=?", (token,)
        )
    ).fetchone()
    if row is not None:
        duration = now - cast(int, row[1])
        await rollup(
            connection,
            str(row[0]),
            now,
            "physical_hold",
            duration=duration,
            method="utc_estimate",
            censored=reclaimed or duration < 0,
        )


async def prune(connection: AsyncConnection, now: int, limit: int) -> bool:
    rollup_cutoff = max(0, now - RETENTION_NS) // MINUTE_NS * MINUTE_NS
    expired = await (
        await connection.execute(
            "SELECT scope,work_id FROM scheduler_waits WHERE expires_ns<? ORDER BY expires_ns,scope,work_id LIMIT ?",
            (now, limit),
        )
    ).fetchall()
    for scope, work in expired:
        await close_wait(connection, str(scope), str(work), now)
    await connection.execute(
        "DELETE FROM scheduler_rollups WHERE (scope,minute_ns,metric,method,bucket) IN (SELECT scope,minute_ns,metric,method,bucket FROM scheduler_rollups WHERE minute_ns<? ORDER BY minute_ns,scope,metric,method,bucket LIMIT ?)",
        (rollup_cutoff, limit),
    )
    await connection.execute(
        "DELETE FROM scheduler_executions WHERE (stage_id,execution) IN (SELECT stage_id,execution FROM scheduler_executions WHERE ended_ns<? ORDER BY ended_ns,stage_id,execution LIMIT ?)",
        (max(0, now - RETENTION_NS), limit),
    )
    await connection.execute(
        "DELETE FROM scheduler_cooldowns WHERE work_id IN (SELECT work_id FROM scheduler_cooldowns WHERE until_ns<? ORDER BY until_ns,work_id LIMIT ?)",
        (now, limit),
    )
    row = await (
        await connection.execute(
            "SELECT EXISTS(SELECT 1 FROM scheduler_waits WHERE expires_ns<?) OR EXISTS(SELECT 1 FROM scheduler_rollups WHERE minute_ns<?) OR EXISTS(SELECT 1 FROM scheduler_executions WHERE ended_ns<?) OR EXISTS(SELECT 1 FROM scheduler_cooldowns WHERE until_ns<?)",
            (now, rollup_cutoff, max(0, now - RETENTION_NS), now),
        )
    ).fetchone()
    return bool(row is not None and row[0])


async def logical_event(
    connection: AsyncConnection, work: str, kind: str, data: JsonValue, now: int
) -> None:
    metric = {
        "completed": "logical_completed",
        "terminal_failure": "logical_failed",
        "cancelled": "logical_cancelled",
    }.get(kind)
    if metric is not None:
        await rollup(connection, "", now, metric)
    if kind in ("cancel_requested", "cancelled"):
        await connection.execute("DELETE FROM scheduler_cooldowns WHERE work_id=?", (work,))
    if kind not in ("terminal_failure", "released"):
        return
    mapping = data if isinstance(data, dict) else {}
    reason = mapping.get("error", mapping.get("reason"))
    reason = reason if isinstance(reason, dict) else {}
    raw_code = reason.get("kind")
    code = raw_code[:160].lower() if isinstance(raw_code, str) else ""
    category = "other"
    for fragment, safe in (
        ("connect", "connectivity"),
        ("throttl", "throttled"),
        ("rate_limit", "throttled"),
        ("challenge", "provider_challenge"),
        ("capacity", "capacity"),
        ("cancel", "cancelled"),
        ("invalid", "invalid_output"),
    ):
        if fragment in code:
            category = safe
            break
    deadline = None
    for key in ("cooldown", "facebook_search_cooldown"):
        value = reason.get(key)
        if isinstance(value, dict) and type(value.get("until_utc_ns")) is int:
            deadline = cast(int, value["until_utc_ns"])
            category = "throttled"
    await rollup(connection, "", now, "failure." + category)
    if deadline is not None and deadline > now:
        await connection.execute(
            "INSERT INTO scheduler_cooldowns VALUES(?,?,?) ON CONFLICT(work_id) DO UPDATE SET reason=excluded.reason,until_ns=max(until_ns,excluded.until_ns)",
            (work, category, deadline),
        )
        size = await (
            await connection.execute("SELECT count(*) FROM scheduler_cooldowns")
        ).fetchone()
        if size is not None and cast(int, size[0]) > WAIT_ROW_CAP:
            await connection.execute(
                "DELETE FROM scheduler_cooldowns WHERE work_id IN (SELECT work_id FROM scheduler_cooldowns ORDER BY until_ns,work_id LIMIT 32)"
            )
            await connection.execute(
                "UPDATE scheduler_meta SET value=value+? WHERE key='dropped_samples'",
                (await connection.changes(),),
            )


async def _resources(
    connection: AsyncConnection, scope: str, now: int
) -> tuple[tuple[SchedulerResourceActivity, ...], bool, int, int, bool]:
    budgets = await (
        await connection.execute(
            "SELECT name,capacity FROM resource_budgets WHERE scope=? ORDER BY name LIMIT 33",
            (scope,),
        )
    ).fetchall()
    capacities = {str(row[0]): cast(int, row[1]) for row in budgets if row[0] in RESOURCE_NAMES}
    rows = await (
        await connection.execute(
            """SELECT a.name,a.units,p.acquired_at_utc_ns,p.expires_at_utc_ns
        FROM resource_permits p INDEXED BY resource_permits_occupied
        JOIN resource_allocations a ON a.permit_token=p.token AND a.scope=p.scope
        WHERE p.scope=? AND p.state='held' ORDER BY p.expires_at_utc_ns,p.token,a.name LIMIT 513""",
            (scope,),
        )
    ).fetchall()
    complete = len(rows) <= 512 and len(budgets) <= 32
    held: Counter[str] = Counter()
    expired: Counter[str] = Counter()
    oldest: dict[str, int] = {}
    for name, units, acquired, expires in rows[:512]:
        key = str(name)
        if key not in RESOURCE_NAMES:
            continue
        held[key] += cast(int, units)
        if cast(int, expires) <= now:
            expired[key] += cast(int, units)
        oldest[key] = min(oldest.get(key, cast(int, acquired)), cast(int, acquired))
    resources = tuple(
        SchedulerResourceActivity.model_validate(
            {
                "name": name,
                "capacity": capacities.get(name),
                "held_units": held[name],
                "expired_held_units": expired[name],
                "oldest_held_age_ns": max(0, now - oldest[name]) if name in oldest else None,
                "complete": complete,
            }
        )
        for name in RESOURCE_NAMES
    )
    return (
        resources,
        bool(budgets),
        sum(row[0] not in RESOURCE_NAMES for row in budgets),
        len(rows[:512]),
        not complete,
    )


async def work_status(
    connection: AsyncConnection,
    work: str,
    scope: str,
    now: int,
    resources: tuple[SchedulerResourceActivity, ...] | None = None,
) -> SchedulerWorkStatus:
    row = await (
        await connection.execute(
            """SELECT w.state,s.id,s.state,s.execution,s.coordination_scope,s.wait_kind,s.progress_waiting,
        json_extract(s.definition_json,'$.resources'),json_extract(s.definition_json,'$.kind'),
        json_extract(s.definition_json,'$.resident_session_identifier'),r.ready_since_utc_ns,r.available_at_utc_ns,
        s.created_at_utc_ns,b.state,s.wait_entered_at_utc_ns FROM work_items w LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running')
        LEFT JOIN fair_ready r ON r.work_item_id=w.id LEFT JOIN progress_wait_builds b ON b.stage_id=s.id AND b.state IN ('building','armed') WHERE w.id=?""",
            (work,),
        )
    ).fetchone()
    if row is None:
        raise KeyError(work)
    if resources is None:
        resources = (await _resources(connection, scope, now))[0]
    execution_started = None
    if row[2] == "running":
        timing = await (
            await connection.execute(
                "SELECT started_ns FROM scheduler_executions WHERE stage_id=? AND execution=? AND ended_ns IS NULL",
                (row[1], row[3]),
            )
        ).fetchone()
        execution_started = None if timing is None else cast(int, timing[0])
    capacity = {resource.name: resource for resource in resources}
    requirements = []
    unknown = False
    encoded = row[7]
    if row[1] is None:
        hint = await (
            await connection.execute(
                "SELECT h.resources_json FROM work_items w JOIN fair_initial_resources h ON h.kind_parts_json=w.kind_parts_json AND h.payload_schema_version=w.payload_schema_version WHERE w.id=?",
                (work,),
            )
        ).fetchone()
        encoded = None if hint is None else hint[0]
    if encoded is not None:
        for request in cast(list[dict[str, JsonValue]], decode_json(str(encoded))):
            name = request.get("name")
            units = request.get("units")
            if name in RESOURCE_NAMES and type(units) is int and units > 0:
                requirements.append({"name": name, "units": units})
            else:
                unknown = True
    else:
        unknown = True
    blocked = []
    blocked_resources = []
    state = str(row[0])
    stage_state = (
        "unregistered"
        if row[1] is None and state in ("pending", "leased")
        else "none"
        if row[1] is None
        else str(row[2])
    )
    if state not in ("pending", "leased", "cancelling"):
        blocked.append("terminal")
    elif row[2] == "running":
        blocked.append("running")
    else:
        if row[6] == 1:
            blocked.append("building_dependencies" if row[13] == "building" else "dependency")
        if row[11] is not None and cast(int, row[11]) > now:
            blocked.append("time")
        for request in requirements:
            resource = capacity[str(request["name"])]
            if (
                resource.capacity is not None
                and resource.complete
                and resource.held_units + cast(int, request["units"]) > resource.capacity
            ):
                blocked_resources.append(request["name"])
        if blocked_resources:
            blocked.append("resource")
        if any(request["name"] == "http" for request in requirements):
            connectivity = await (
                await connection.execute(
                    "SELECT paused FROM network_connectivity WHERE singleton=1"
                )
            ).fetchone()
            if connectivity is not None and connectivity[0] == 1:
                blocked.append("connectivity")
        if row[9] is not None:
            resident = await (
                await connection.execute(
                    "SELECT state FROM resident_sessions WHERE id=?", (row[9],)
                )
            ).fetchone()
            if resident is None or resident[0] not in ("opening", "ready"):
                blocked.append("resident_ownership")
    if not blocked:
        blocked.append("unknown" if unknown else "none_of_checked_conditions")
    phase = "other"
    kind = () if row[8] is None else tuple(cast(list[str], decode_json(str(row[8]))))
    if any("dataset" in part for part in kind):
        phase = "dataset"
    elif any("travel" in part for part in kind):
        phase = "travel"
    elif any("coordinator" in part for part in kind):
        phase = "coordinator"
    elif row[9] is not None:
        phase = "resident"
    else:
        phase = next(
            (
                name
                for name in ("analysis", "http", "cpu")
                if any(request["name"] == name for request in requirements)
            ),
            "other",
        )
    observation = await (
        await connection.execute(
            "SELECT reason,source,started_ns,last_seen_ns,expires_ns FROM scheduler_waits WHERE scope=? AND work_id=?",
            (scope, work),
        )
    ).fetchone()
    wait_age = None
    source = "unavailable"
    if observation is not None and cast(int, observation[4]) >= now:
        wait_age = max(0, now - cast(int, observation[2]))
        source = str(observation[1])
    elif row[14] is not None and row[5] is not None:
        wait_age = max(0, now - cast(int, row[14]))
        source = "transition"
    return SchedulerWorkStatus.model_validate(
        {
            "work_handle": handle(work),
            "stage_handle": None if row[1] is None else handle(str(row[1])),
            "logical_state": state,
            "stage_state": stage_state,
            "phase_family": phase,
            "execution": 0 if row[3] is None else row[3],
            "execution_started_at_utc_ns": execution_started,
            "execution_age_ns": now - execution_started
            if execution_started is not None and now >= execution_started
            else None,
            "execution_age_method": "utc_estimate"
            if execution_started is not None and now >= execution_started
            else "unavailable",
            "scope_handle": None if row[4] is None else handle(str(row[4])),
            "resources": tuple(requirements),
            "progress_state": row[13],
            "recorded_wait_kind": row[5],
            "observed_blockers": tuple(blocked[:8]),
            "blocked_resources": tuple(blocked_resources[:8]),
            "observed_at_utc_ns": now,
            "phase_ready_age_ns": None if row[10] is None else max(0, now - cast(int, row[10])),
            "wait_age_ns": wait_age,
            "wait_observation_source": source,
            "checks_complete": False,
            "identity_omitted": True,
            "recorded_observation_reason": None if observation is None else observation[0],
            "recorded_observation_at_utc_ns": None if observation is None else observation[3],
        }
    )


async def snapshot(
    connection: AsyncConnection, scope: str, now: int, window: int, maximum: int
) -> SchedulerActivity:
    resources, configured, omitted, examined, truncated = await _resources(connection, scope, now)
    gauges = await (
        await connection.execute("SELECT subject,state,value FROM scheduler_gauges")
    ).fetchall()
    work = {str(row[1]): cast(int, row[2]) for row in gauges if row[0] == "work"}
    stages = {str(row[1]): cast(int, row[2]) for row in gauges if row[0] == "stage"}
    identifiers = []
    for sql, bindings in (
        (
            "SELECT work_id FROM scheduler_waits WHERE scope=? AND expires_ns>=? ORDER BY started_ns,work_id LIMIT ?",
            (scope, now, maximum),
        ),
        (
            "SELECT work_item_id FROM work_stages WHERE coordination_scope=? AND state='running' ORDER BY lease_expires_at_utc_ns,id LIMIT ?",
            (scope, maximum),
        ),
        (
            "SELECT work_item_id FROM work_stages INDEXED BY work_stages_progress_sleep WHERE state='pending' AND progress_waiting=1 ORDER BY id LIMIT ?",
            (maximum,),
        ),
        (
            "SELECT work_item_id FROM fair_ready INDEXED BY fair_ready_dispatch WHERE active=1 ORDER BY priority DESC,work_eligible_at_utc_ns,created_at_utc_ns,work_item_id LIMIT ?",
            (maximum,),
        ),
    ):
        rows = await (await connection.execute(sql, bindings)).fetchall()
        for row in rows:
            if str(row[0]) not in identifiers:
                identifiers.append(str(row[0]))
        if len(identifiers) >= maximum:
            break
    samples = tuple(
        [
            await work_status(connection, work_id, scope, now, resources)
            for work_id in identifiers[:maximum]
        ]
    )
    groups: Counter[str] = Counter()
    ages: dict[str, int] = {}
    for sample in samples:
        for reason in sample.observed_blockers:
            groups[reason] += 1
            if sample.wait_age_ns is not None:
                ages[reason] = max(ages.get(reason, 0), sample.wait_age_ns)
    wait_groups = tuple(
        SchedulerWaitGroup.model_validate(
            {"reason": reason, "count": count, "oldest_age_ns": ages.get(reason), "sampled": True}
        )
        for reason, count in sorted(groups.items())[:8]
    )
    requested_start = max(0, now - min(window, RETENTION_NS)) // MINUTE_NS * MINUTE_NS
    retention = await (
        await connection.execute(
            "SELECT key,value FROM scheduler_meta WHERE key IN ('retained_since','dropped_samples','active_loss_at')"
        )
    ).fetchall()
    retained = {str(row[0]): cast(int, row[1]) for row in retention}
    start = min(now, max(requested_start, retained.get("retained_since", 0)))
    telemetry_truncated = (
        window > RETENTION_NS or start > requested_start or retained.get("active_loss_at", 0) > 0
    )
    rows = await (
        await connection.execute(
            """SELECT metric,method,bucket,sum(count),sum(total_ns),max(maximum_ns),sum(censored)
        FROM scheduler_rollups WHERE scope IN (?, '') AND minute_ns>=? AND minute_ns<=?
        GROUP BY metric,method,bucket ORDER BY metric,method,bucket""",
            (scope, start, now // MINUTE_NS * MINUTE_NS),
        )
    ).fetchall()
    timings = []
    for metric, method in sorted(
        {
            (str(row[0]), str(row[1]))
            for row in rows
            if row[0] in ("queue_delay", "observed_resource_wait", "phase_runtime", "physical_hold")
        }
    ):
        selected = [row for row in rows if row[0] == metric and row[1] == method]
        histogram = [0] * (len(SCHEDULER_DURATION_BUCKETS_NS) + 1)
        for row in selected:
            if cast(int, row[2]) >= 0:
                histogram[cast(int, row[2])] += cast(int, row[3])
        count = sum(cast(int, row[3]) for row in selected)
        timings.append(
            SchedulerTimingSummary.model_validate(
                {
                    "metric": metric,
                    "count": count,
                    "total_ns": sum(cast(int, row[4]) for row in selected),
                    "maximum_ns": max((cast(int, row[5]) for row in selected), default=0)
                    if count
                    else None,
                    "histogram_counts": tuple(histogram),
                    "censored_count": sum(cast(int, row[6]) for row in selected),
                    "method": method if count else "unavailable",
                }
            )
        )
    counters = {
        str(row[0]): sum(cast(int, value[3]) for value in rows if value[0] == row[0])
        for row in rows
    }
    meta = await (
        await connection.execute("SELECT value FROM scheduler_meta WHERE key='started'")
    ).fetchone()
    started = None if meta is None or meta[0] == 0 else cast(int, meta[0])
    throughput = SchedulerThroughput.model_validate(
        {
            "window_start_utc_ns": start,
            "window_end_utc_ns": now,
            **{
                key: counters.get(key, 0)
                for key in (
                    "logical_completed",
                    "logical_failed",
                    "logical_cancelled",
                    "stage_completed",
                    "stage_failed",
                    "stage_cancelled",
                    "admissions",
                )
            },
            "complete": started is not None
            and started <= start
            and not truncated
            and not telemetry_truncated,
        }
    )
    from carl.core.scheduler_activity import SchedulerFailureGroup

    failures = []
    cooldown = await (
        await connection.execute(
            "SELECT reason,max(until_ns) FROM scheduler_cooldowns WHERE until_ns>? GROUP BY reason LIMIT 8",
            (now,),
        )
    ).fetchall()
    deadlines = {str(row[0]): cast(int, row[1]) for row in cooldown}
    for reason in (
        "connectivity",
        "throttled",
        "provider_challenge",
        "capacity",
        "cancelled",
        "invalid_output",
        "other",
    ):
        if counters.get("failure." + reason, 0) or reason in deadlines:
            failures.append(
                SchedulerFailureGroup.model_validate(
                    {
                        "reason": reason,
                        "count": counters.get("failure." + reason, 0),
                        "cooldown_remaining_ns": max(0, deadlines[reason] - now)
                        if reason in deadlines
                        else None,
                    }
                )
            )
    return SchedulerActivity(
        captured_at_utc_ns=now,
        scope_handle=handle(scope),
        scope_configured=configured,
        resources=resources,
        work_counts=SchedulerWorkCounts.model_validate(work),
        stage_counts=SchedulerStageCounts.model_validate(stages),
        wait_groups=wait_groups,
        samples=samples,
        timings=tuple(timings[:8]),
        failures=tuple(failures),
        throughput=throughput,
        coverage=SchedulerCoverage(
            method="current_metadata",
            examined_rows=examined + len(samples),
            row_limit=512 + maximum,
            truncated=truncated or telemetry_truncated,
            instrumented_since_utc_ns=started,
            censored_count=sum(timing.censored_count for timing in timings)
            + retained.get("dropped_samples", 0),
        ),
        omitted_resource_names=omitted,
    )
