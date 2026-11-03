"""Durable bounded progress fanout; callers own the short SQLite transaction."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, cast

from carl.core.json import decode_json, encode_json
from carl.core.work_progress import ProgressWait

if TYPE_CHECKING:
    from apsw import AsyncConnection


_MARK_SQL = """
UPDATE work_progress_clock SET revision=revision+1 WHERE singleton=1;
INSERT INTO work_progress_revisions(work_id,revision)
VALUES ({work},(SELECT revision FROM work_progress_clock WHERE singleton=1))
ON CONFLICT(work_id) DO UPDATE SET revision=excluded.revision;
UPDATE progress_notification_clock SET ticket=ticket+1 WHERE singleton=1
    AND NOT EXISTS(SELECT 1 FROM progress_notifications WHERE child_work_id={work});
INSERT INTO progress_notifications(child_work_id,latest_revision,ticket)
VALUES ({work},(SELECT revision FROM work_progress_clock WHERE singleton=1),
    (SELECT ticket FROM progress_notification_clock WHERE singleton=1))
ON CONFLICT(child_work_id) DO UPDATE SET latest_revision=excluded.latest_revision;
"""

PROGRESS_SCHEMA = (
    """
CREATE TABLE IF NOT EXISTS work_progress_clock(singleton INTEGER PRIMARY KEY CHECK(singleton=1),revision INTEGER NOT NULL) STRICT;
INSERT OR IGNORE INTO work_progress_clock VALUES(1,0);
CREATE TABLE IF NOT EXISTS progress_notification_clock(singleton INTEGER PRIMARY KEY CHECK(singleton=1),ticket INTEGER NOT NULL) STRICT;
INSERT OR IGNORE INTO progress_notification_clock VALUES(1,0);
CREATE TABLE IF NOT EXISTS work_progress_revisions(work_id TEXT PRIMARY KEY REFERENCES work_items(id),revision INTEGER NOT NULL) STRICT;
CREATE TABLE IF NOT EXISTS work_consumer_edges(
    parent_work_id TEXT NOT NULL REFERENCES work_items(id),kind_json TEXT NOT NULL CHECK(json_valid(kind_json)),
    child_work_id TEXT NOT NULL REFERENCES work_items(id),PRIMARY KEY(parent_work_id,kind_json,child_work_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS work_consumer_edges_child ON work_consumer_edges(child_work_id,parent_work_id,kind_json);
CREATE TABLE IF NOT EXISTS consumer_group_progress(
    parent_work_id TEXT NOT NULL REFERENCES work_items(id),kind_json TEXT NOT NULL,revision INTEGER NOT NULL,
    PRIMARY KEY(parent_work_id,kind_json)
) STRICT, WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS progress_wait_builds(
    id TEXT PRIMARY KEY,work_id TEXT NOT NULL REFERENCES work_items(id),operation_id TEXT NOT NULL,
    source_stage_id TEXT NOT NULL REFERENCES work_stages(id),source_token TEXT NOT NULL,
    continuation_id TEXT NOT NULL,stage_id TEXT UNIQUE REFERENCES work_stages(id),
    baseline INTEGER NOT NULL,digest TEXT NOT NULL,groups_json TEXT NOT NULL CHECK(json_valid(groups_json)),
    state TEXT NOT NULL CHECK(state IN ('source','prepared','building','armed','inactive')),
    chunk_count INTEGER NOT NULL DEFAULT 0,next_chunk INTEGER NOT NULL DEFAULT 0,chunk_offset INTEGER NOT NULL DEFAULT 0,
    max_revision INTEGER NOT NULL DEFAULT 0,reconcile_after TEXT NOT NULL DEFAULT '',ticket INTEGER NOT NULL DEFAULT 0
) STRICT;
CREATE INDEX IF NOT EXISTS progress_wait_builds_ready ON progress_wait_builds(state,ticket,id);
CREATE INDEX IF NOT EXISTS progress_wait_builds_source ON progress_wait_builds(source_stage_id,source_token);
CREATE TABLE IF NOT EXISTS progress_wait_source_chunks(
    build_id TEXT NOT NULL REFERENCES progress_wait_builds(id),ordinal INTEGER NOT NULL,
    identifiers_json TEXT NOT NULL CHECK(json_valid(identifiers_json)),PRIMARY KEY(build_id,ordinal)
) STRICT, WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS progress_wait_members(
    build_id TEXT NOT NULL REFERENCES progress_wait_builds(id),child_work_id TEXT NOT NULL REFERENCES work_items(id),
    PRIMARY KEY(build_id,child_work_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS progress_wait_members_child ON progress_wait_members(child_work_id,build_id);
CREATE TABLE IF NOT EXISTS progress_wait_groups(
    build_id TEXT NOT NULL REFERENCES progress_wait_builds(id),parent_work_id TEXT NOT NULL REFERENCES work_items(id),
    kind_json TEXT NOT NULL,PRIMARY KEY(build_id,kind_json)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS progress_wait_groups_parent ON progress_wait_groups(parent_work_id,kind_json,build_id);
CREATE TABLE IF NOT EXISTS progress_notifications(
    child_work_id TEXT PRIMARY KEY REFERENCES work_items(id),latest_revision INTEGER NOT NULL,
    pass_revision INTEGER NOT NULL DEFAULT 0,phase INTEGER NOT NULL DEFAULT 0,
    after_parent TEXT NOT NULL DEFAULT '',after_kind TEXT NOT NULL DEFAULT '',after_build TEXT NOT NULL DEFAULT '',
    ticket INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS progress_notifications_turn ON progress_notifications(ticket,child_work_id);
CREATE INDEX IF NOT EXISTS work_stages_progress_ready ON work_stages(state,eligible_at_utc_ns,created_at_utc_ns,id)
    WHERE progress_waiting=0;
CREATE INDEX IF NOT EXISTS work_stages_progress_sleep ON work_stages(id) WHERE state='pending' AND progress_waiting=1;
CREATE TRIGGER IF NOT EXISTS progress_work_changed AFTER UPDATE ON work_items
WHEN NEW.result_json IS NOT OLD.result_json OR NEW.error_json IS NOT OLD.error_json
    OR NEW.eligible_at_utc_ns!=OLD.eligible_at_utc_ns
    OR (NEW.state!=OLD.state AND (NEW.state IN ('completed','terminal_failure','cancelled','cancelling')
        OR (NEW.state='pending' AND OLD.state IN ('leased','completed','terminal_failure','cancelled','cancelling'))))
BEGIN
"""
    + _MARK_SQL.format(work="NEW.id")
    + """
END;
CREATE TRIGGER IF NOT EXISTS progress_stage_inactive AFTER UPDATE OF state ON work_stages
WHEN NEW.state!='pending'
BEGIN
    UPDATE progress_wait_builds SET state='inactive' WHERE stage_id=NEW.id AND state IN ('building','armed');
END;
"""
)


def build_identifier(source_token: str, continuation_id: str) -> str:
    return hashlib.sha256(encode_json([source_token, continuation_id]).encode()).hexdigest()


def wait_digest(wait: ProgressWait) -> str:
    return wait.fingerprint


async def watermark(connection: AsyncConnection) -> int:
    row = await (
        await connection.execute("SELECT revision FROM work_progress_clock WHERE singleton=1")
    ).fetchone()
    assert row is not None
    return cast(int, row[0])


async def ticket(connection: AsyncConnection) -> int:
    row = await (
        await connection.execute(
            "UPDATE progress_notification_clock SET ticket=ticket+1 WHERE singleton=1 RETURNING ticket"
        )
    ).fetchone()
    assert row is not None
    return cast(int, row[0])


async def mark(connection: AsyncConnection, work: str) -> None:
    # A single bound parameter is reused across this short semantic publication.
    for statement in _MARK_SQL.format(work="?").split(";"):
        if statement.strip():
            await connection.execute(statement, (work,) if "?" in statement else ())


async def wake(connection: AsyncConnection, build: str, now: int) -> int:
    row = await (
        await connection.execute(
            "SELECT stage_id FROM progress_wait_builds WHERE id=? AND state IN ('building','armed')",
            (build,),
        )
    ).fetchone()
    if row is None or row[0] is None:
        return 0
    stage = str(row[0])
    await connection.execute(
        """UPDATE work_stages SET progress_waiting=0,wait_kind=NULL
        WHERE id=? AND state='pending' AND progress_waiting=1
          AND EXISTS(SELECT 1 FROM stage_attempts a WHERE a.operation_id=work_stages.operation_id AND a.state='active')""",
        (stage,),
    )
    changed = await connection.changes()
    if changed:
        await connection.execute(
            "UPDATE fair_ready SET ready_since_utc_ns=max(?,coalesce((SELECT json_extract(definition_json,'$.not_before_utc_ns') FROM work_stages WHERE id=?),0)) WHERE stage_id=?",
            (now, stage, stage),
        )
    await connection.execute(
        "UPDATE progress_wait_builds SET state='inactive',stage_id=NULL WHERE id=?", (build,)
    )
    return int(changed)


async def observe(connection: AsyncConnection, build: str, revision: int, now: int) -> int:
    await connection.execute(
        "UPDATE progress_wait_builds SET max_revision=max(max_revision,?) WHERE id=? AND state IN ('building','armed')",
        (revision, build),
    )
    if not await connection.changes():
        return 0
    row = await (
        await connection.execute(
            "SELECT state,baseline,max_revision FROM progress_wait_builds WHERE id=?", (build,)
        )
    ).fetchone()
    return (
        await wake(connection, build, now)
        if row is not None and row[0] == "armed" and cast(int, row[2]) > cast(int, row[1])
        else 0
    )


async def group_progress(
    connection: AsyncConnection, parent: str, kind: str, revision: int, now: int
) -> int:
    await connection.execute(
        "INSERT INTO consumer_group_progress VALUES(?,?,?) ON CONFLICT(parent_work_id,kind_json) DO UPDATE SET revision=max(revision,excluded.revision)",
        (parent, kind, revision),
    )
    # One current continuation per logical consumer; historical builds do not
    # enlarge the group fanout query's output or its scan.
    rows = await (
        await connection.execute(
            """SELECT b.id FROM work_stages s JOIN progress_wait_builds b ON b.stage_id=s.id
        JOIN progress_wait_groups g ON g.build_id=b.id AND g.kind_json=?
        WHERE s.work_item_id=? AND s.state='pending' AND b.state IN ('building','armed')""",
            (kind, parent),
        )
    ).fetchall()
    woke = 0
    for row in rows:
        woke += await observe(connection, str(row[0]), revision, now)
    return woke


async def arm(connection: AsyncConnection, build: str, now: int) -> int:
    row = await (
        await connection.execute(
            "SELECT work_id,max_revision FROM progress_wait_builds WHERE id=? AND state='building'",
            (build,),
        )
    ).fetchone()
    if row is None:
        return 0
    revisions = await (
        await connection.execute(
            """SELECT coalesce(max(p.revision),0) FROM progress_wait_groups g
        LEFT JOIN consumer_group_progress p ON p.parent_work_id=g.parent_work_id AND p.kind_json=g.kind_json WHERE g.build_id=?""",
            (build,),
        )
    ).fetchone()
    assert revisions is not None
    await connection.execute(
        "UPDATE progress_wait_builds SET state='armed',max_revision=max(max_revision,?) WHERE id=?",
        (revisions[0], build),
    )
    return await observe(connection, build, 0, now)


async def build_batch(
    connection: AsyncConnection, build: str, limit: int, now: int
) -> tuple[int, int]:
    row = await (
        await connection.execute(
            "SELECT next_chunk,chunk_offset,chunk_count FROM progress_wait_builds WHERE id=? AND state='building'",
            (build,),
        )
    ).fetchone()
    if row is None:
        return 0, 0
    ordinal, offset, count = (cast(int, value) for value in row)
    if ordinal >= count:
        return 0, await arm(connection, build, now)
    chunk = await (
        await connection.execute(
            "SELECT identifiers_json FROM progress_wait_source_chunks WHERE build_id=? AND ordinal=?",
            (build, ordinal),
        )
    ).fetchone()
    assert chunk is not None
    identifiers = cast(list[str], decode_json(str(chunk[0])))
    selected = identifiers[offset : offset + limit]
    for identifier in selected:
        await connection.execute(
            "INSERT OR IGNORE INTO progress_wait_members VALUES(?,?)", (build, identifier)
        )
    revision = await (
        await connection.execute(
            "SELECT coalesce(max(p.revision),0) FROM json_each(?) ids LEFT JOIN work_progress_revisions p ON p.work_id=ids.value",
            (encode_json(selected),),
        )
    ).fetchone()
    assert revision is not None
    offset += len(selected)
    if offset == len(identifiers):
        ordinal, offset = ordinal + 1, 0
    await connection.execute(
        "UPDATE progress_wait_builds SET next_chunk=?,chunk_offset=?,max_revision=max(max_revision,?),ticket=? WHERE id=?",
        (ordinal, offset, revision[0], await ticket(connection), build),
    )
    return len(selected), await arm(connection, build, now) if ordinal >= count else 0


async def drain_source(
    connection: AsyncConnection, child: str, limit: int, now: int
) -> tuple[int, int]:
    row = await (
        await connection.execute(
            "SELECT latest_revision,pass_revision,phase,after_parent,after_kind,after_build FROM progress_notifications WHERE child_work_id=?",
            (child,),
        )
    ).fetchone()
    if row is None:
        return 0, 0
    latest, revision, phase = cast(int, row[0]), cast(int, row[1]), cast(int, row[2])
    revision = latest if revision == 0 else revision
    woke = 0
    used = 0
    if phase == 0:
        edges = await (
            await connection.execute(
                "SELECT parent_work_id,kind_json FROM work_consumer_edges WHERE child_work_id=? AND (parent_work_id,kind_json)>(?,?) ORDER BY parent_work_id,kind_json LIMIT ?",
                (child, row[3], row[4], limit),
            )
        ).fetchall()
        for parent, kind in edges:
            woke += await group_progress(connection, str(parent), str(kind), revision, now)
        used = len(edges)
        if edges:
            await connection.execute(
                "UPDATE progress_notifications SET after_parent=?,after_kind=? WHERE child_work_id=?",
                (*edges[-1], child),
            )
        if used < limit:
            phase = 1
    if phase == 1 and used < limit:
        members = await (
            await connection.execute(
                "SELECT build_id FROM progress_wait_members WHERE child_work_id=? AND build_id>? ORDER BY build_id LIMIT ?",
                (child, row[5], limit - used),
            )
        ).fetchall()
        for member in members:
            woke += await observe(connection, str(member[0]), revision, now)
        if members:
            await connection.execute(
                "UPDATE progress_notifications SET after_build=? WHERE child_work_id=?",
                (members[-1][0], child),
            )
        finished = len(members) < limit - used
        used += len(members)
        if finished:
            if latest > revision:
                await connection.execute(
                    "UPDATE progress_notifications SET pass_revision=0,phase=0,after_parent='',after_kind='',after_build='',ticket=? WHERE child_work_id=?",
                    (await ticket(connection), child),
                )
            else:
                await connection.execute(
                    "DELETE FROM progress_notifications WHERE child_work_id=?", (child,)
                )
            return used, woke
    await connection.execute(
        "UPDATE progress_notifications SET pass_revision=?,phase=?,ticket=? WHERE child_work_id=?",
        (revision, phase, await ticket(connection), child),
    )
    return used, woke


async def publish_consumer_edge(connection: AsyncConnection, request: str, now: int) -> None:
    row = await (
        await connection.execute(
            "SELECT work_item_id,requester_kind_parts_json,requester_identifier,context_json,parent_work_identifier FROM work_requests WHERE id=?",
            (request,),
        )
    ).fetchone()
    if row is None:
        return
    child, kind_json, identifier = str(row[0]), str(row[1]), str(row[2])
    kind = tuple(cast(list[str], decode_json(kind_json)))
    context = decode_json(str(row[3]))
    context = context if isinstance(context, dict) else {}
    parent = None if row[4] is None else str(row[4])
    if parent is None and kind == ("carl", "http", "acquisition"):
        operation = context.get("operation_identifier")
        if isinstance(operation, str):
            owner = await (
                await connection.execute(
                    "SELECT work_item_id FROM work_operations WHERE operation_id=?", (operation,)
                )
            ).fetchone()
            parent = None if owner is None else str(owner[0])
    known = kind[:3] in {
        ("carl", "facebook", "search_refresh"),
        ("carl", "ebay", "search_refresh"),
        ("carl", "facebook", "listing_details"),
        ("carl", "marketplace", "pipeline_listing"),
    } or kind in {
        ("carl", "facebook", "analysis_batch"),
        ("carl", "ebay", "followup"),
        ("carl", "marketplace", "pipeline_details"),
        ("carl", "marketplace", "pipeline_analysis"),
    }
    if parent is None and known:
        candidates = [
            context.get("listing_details_work_identifier"),
            context.get("search_refresh_work_identifier"),
            identifier,
            context.get("root_work_identifier"),
        ]
        for candidate in candidates:
            if (
                isinstance(candidate, str)
                and candidate != child
                and await (
                    await connection.execute("SELECT 1 FROM work_items WHERE id=?", (candidate,))
                ).fetchone()
                is not None
            ):
                parent = candidate
                break
    if parent is None and kind == ("carl", "facebook", "gallery_image_reference"):
        # A retained reference can have a different producer. Only its known
        # immediate consumer context is authority for this legacy edge.
        for key in ("listing_details_work_identifier", "search_refresh_work_identifier"):
            candidate = context.get(key)
            if (
                isinstance(candidate, str)
                and candidate != child
                and await (
                    await connection.execute("SELECT 1 FROM work_items WHERE id=?", (candidate,))
                ).fetchone()
                is not None
            ):
                parent = candidate
                break
    if parent is None or parent == child:
        return
    await connection.execute(
        "INSERT OR IGNORE INTO work_consumer_edges VALUES(?,?,?)", (parent, kind_json, child)
    )
    if await connection.changes():
        await connection.execute(
            "UPDATE work_progress_clock SET revision=revision+1 WHERE singleton=1"
        )
        await group_progress(connection, parent, kind_json, await watermark(connection), now)
