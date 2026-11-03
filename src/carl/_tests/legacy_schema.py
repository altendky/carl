"""Keep synthetic historical fixtures free of schema additions from v20."""

import sqlite3

from carl.io.fair_scheduling import FAIR_SCHEMA
from carl.io.scheduler_activity import SCHEDULER_SCHEMA
from carl.io.work_progress import PROGRESS_SCHEMA

_REQUESTER_COLUMNS = (
    "    scheduling_json TEXT CHECK(scheduling_json IS NULL OR json_valid(scheduling_json)),\n",
    "    parent_work_identifier TEXT REFERENCES work_items(id),\n",
)


def without_fair_schema(schema: str) -> str:
    """Strip appended fairness DDL before executing a historical schema slice."""
    schema = schema.removesuffix(SCHEDULER_SCHEMA).removesuffix(PROGRESS_SCHEMA)
    if not schema.endswith(FAIR_SCHEMA):
        raise ValueError("Historical schema fixture must contain the current fairness suffix")
    schema = schema.removesuffix(FAIR_SCHEMA)
    for definition in _REQUESTER_COLUMNS:
        schema = schema.replace(definition, "")
    schema = schema.replace(
        "    progress_waiting INTEGER NOT NULL DEFAULT 0 CHECK(progress_waiting IN (0,1)),\n", ""
    )
    schema = schema.replace("    wait_entered_at_utc_ns INTEGER,\n", "")
    return schema


def remove_fair_schema(connection: sqlite3.Connection) -> None:
    """Remove only v20 additions before an existing fixture emulates an older DB.

    Drop dependent objects first, preserving foreign-key enforcement, evidence,
    requester history and the fixture's historical metadata/upgrade assertions.
    """
    triggers = connection.execute(
        "SELECT name FROM sqlite_schema WHERE type='trigger' AND (name GLOB 'fair_*' OR name GLOB 'progress_*' OR name GLOB 'scheduler_*')"
    ).fetchall()
    for (name,) in triggers:
        escaped = str(name).replace('"', '""')
        connection.execute(f'DROP TRIGGER "{escaped}"')
    for name in (
        "scheduler_waits",
        "scheduler_executions",
        "scheduler_rollups",
        "scheduler_cooldowns",
        "scheduler_gauges",
        "scheduler_meta",
        "progress_wait_members",
        "progress_wait_groups",
        "progress_wait_source_chunks",
        "progress_wait_builds",
        "progress_notifications",
        "consumer_group_progress",
        "work_consumer_edges",
        "work_progress_revisions",
        "progress_notification_clock",
        "work_progress_clock",
        "fair_cpu_drains",
        "fair_admissions",
        "fair_offers",
        "fair_ready",
        "fair_unowned",
        "work_scheduling",
        "fair_root_service",
        "fair_workspace_service",
        "fair_lanes",
        "fair_scopes",
        "fair_roots",
        "fair_initial_resources",
    ):
        connection.execute(f'DROP TABLE IF EXISTS "{name}"')
    for name in ("work_stages_progress_ready", "work_stages_progress_sleep"):
        connection.execute(f'DROP INDEX IF EXISTS "{name}"')
    stages = {row[1] for row in connection.execute("PRAGMA table_info(work_stages)")}
    if "progress_waiting" in stages:
        connection.execute("ALTER TABLE work_stages DROP COLUMN progress_waiting")
    if "wait_entered_at_utc_ns" in stages:
        connection.execute("ALTER TABLE work_stages DROP COLUMN wait_entered_at_utc_ns")
    columns = {row[1] for row in connection.execute("PRAGMA table_info(work_requests)")}
    for name in ("scheduling_json", "parent_work_identifier"):
        if name in columns:
            connection.execute(f'ALTER TABLE work_requests DROP COLUMN "{name}"')
