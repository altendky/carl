"""Acquisition reuse must not scan completed work from unrelated job kinds."""

# pyright: reportPrivateUsage=false

import json
from pathlib import Path

import pytest

from carl.core.acquisition_identity import acquisition_resource_identity
from carl.core.models import JsonValue
from carl.io.sqlite import Database


async def _history(database: Database, start: int, count: int) -> None:
    async with database._connections.writer() as connection:
        await connection.execute(
            """
            WITH RECURSIVE numbers(n) AS (
                SELECT ? UNION ALL SELECT n+1 FROM numbers WHERE n<?
            )
            INSERT INTO work_items(
                id,kind_parts_json,payload_schema_version,payload_json,
                deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns
            ) SELECT 'history-'||n,'["unrelated","image"]',1,'{}',json_array('history-'||n),
                     'completed',0,0,0 FROM numbers
            """,
            (start, start + count - 1),
        )


async def _cost(
    database: Database, kind: tuple[str, ...], resource: tuple[str, ...]
) -> tuple[tuple[dict[str, JsonValue], ...], int]:
    steps = 0

    def count() -> bool:
        nonlocal steps
        steps += 100
        return False

    async with database._connections.reader() as connection:
        await connection.set_progress_handler(count, nsteps=100, id="acquisition-budget")
        try:
            result = await database.completed_acquisition_candidates(
                kind, resource, work_identifier="candidate"
            )
        finally:
            await connection.set_progress_handler(None, id="acquisition-budget")
    return result, steps


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("kind", "payload"),
    (
        (
            ("carl", "ebay", "collect", "item"),
            {"request": {"item_identifier": "123", "stack_identifier": "ebay_anonymous"}},
        ),
        (
            ("carl", "ebay", "collect", "description"),
            {"request": {"stack_identifier": "ebay_anonymous"}, "url": "https://example.test/123"},
        ),
        (
            ("carl", "facebook", "work", "collect_item"),
            {"listing_id": "123", "request_plan": {"url": "https://example.test/123"}},
        ),
    ),
)
async def test_acquisition_candidate_cost_ignores_other_work_kinds(
    tmp_path: Path, kind: tuple[str, ...], payload: dict[str, JsonValue]
) -> None:
    resource = acquisition_resource_identity(kind, payload)
    assert resource is not None
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        async with database._connections.writer() as connection:
            await connection.executemany(
                """
                INSERT INTO work_items(
                    id,kind_parts_json,payload_schema_version,payload_json,
                    deduplication_identity_json,state,priority,eligible_at_utc_ns,
                    created_at_utc_ns,result_json
                ) VALUES (?,?,1,?,?,?,0,0,?,?)
                """,
                (
                    (
                        identifier,
                        json.dumps(kind, separators=(",", ":")),
                        json.dumps(payload),
                        json.dumps([identifier]),
                        state,
                        created,
                        result,
                    )
                    for identifier, state, created, result in (
                        ("candidate", "pending", 100, None),
                        (
                            "source",
                            "completed",
                            1,
                            '{"acquisition_record_identifier":"acquisition"}',
                        ),
                        ("too-old", "completed", 1, "{}"),
                    )
                ),
            )
            # A source acquired after the candidate was queued is reusable;
            # a source whose completion predates it remains excluded.
            await connection.execute(
                """INSERT INTO work_events(
                    id,work_item_id,event_kind,recorded_at_utc_ns,data_json
                ) VALUES ('candidate-event','candidate','enqueued',100,'{}')"""
            )
            await connection.executemany(
                """INSERT INTO work_events(
                    id,work_item_id,event_kind,recorded_at_utc_ns,data_json
                ) VALUES (?,?,'completed',?,'{"operation_identifier":"acquire"}')""",
                (("source-event", "source", 101), ("old-event", "too-old", 99)),
            )
            await connection.execute(
                """INSERT INTO operations(
                    id,component_parts_json,output_schema_version,started_at_utc,
                    code_provenance_json,invocation_json,configuration_json,state
                ) VALUES ('acquire','["test","acquire"]',1,'2026-10-17T00:00:00Z','{}','{}','{}','completed')"""
            )
            await connection.execute(
                """INSERT INTO work_operations VALUES ('acquire','source',1,'token','worker')"""
            )
            await connection.execute(
                """INSERT INTO objects(id,object_type,kind_parts_json,created_by_operation_id)
                VALUES ('acquisition','record','["carl","http","acquisition"]','acquire')"""
            )
        await _history(database, 0, 64)
        before, before_steps = await _cost(database, kind, resource)
        await _history(database, 64, 4096)
        after, after_steps = await _cost(database, kind, resource)
        assert before == after
        assert [source["identifier"] for source in after] == ["source"]
        assert after_steps <= before_steps + 500
