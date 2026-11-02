"""Keep synthetic historical fixtures free of schema additions from v20."""

import sqlite3

from carl.io.fair_scheduling import FAIR_SCHEMA

_REQUESTER_COLUMNS = (
    "    scheduling_json TEXT CHECK(scheduling_json IS NULL OR json_valid(scheduling_json)),\n",
    "    parent_work_identifier TEXT REFERENCES work_items(id),\n",
)


def without_fair_schema(schema: str) -> str:
    """Strip appended fairness DDL before executing a historical schema slice."""
    if not schema.endswith(FAIR_SCHEMA):
        raise ValueError("Historical schema fixture must contain the current fairness suffix")
    schema = schema.removesuffix(FAIR_SCHEMA)
    for definition in _REQUESTER_COLUMNS:
        schema = schema.replace(definition, "")
    return schema


def remove_fair_schema(connection: sqlite3.Connection) -> None:
    """Remove only v20 additions before an existing fixture emulates an older DB.

    Drop dependent objects first, preserving foreign-key enforcement, evidence,
    requester history and the fixture's historical metadata/upgrade assertions.
    """
    triggers = connection.execute(
        "SELECT name FROM sqlite_schema WHERE type='trigger' AND name GLOB 'fair_*'"
    ).fetchall()
    for (name,) in triggers:
        escaped = str(name).replace('"', '""')
        connection.execute(f'DROP TRIGGER "{escaped}"')
    for name in (
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
    columns = {row[1] for row in connection.execute("PRAGMA table_info(work_requests)")}
    for name in ("scheduling_json", "parent_work_identifier"):
        if name in columns:
            connection.execute(f'ALTER TABLE work_requests DROP COLUMN "{name}"')
