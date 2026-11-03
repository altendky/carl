"""Acquisition reuse seeks its exact resource without scanning retained history."""

# pyright: reportPrivateUsage=false

import json
from pathlib import Path

import pytest

from carl.core.acquisition_identity import (
    acquisition_resource_identity,
    acquisition_resource_identity_json,
)
from carl.core.models import JsonValue
from carl.io.sqlite import Database


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


async def _history(
    database: Database,
    start: int,
    count: int,
    *,
    kind: tuple[str, ...] | None = None,
    payload: dict[str, JsonValue] | None = None,
) -> None:
    async with database._connections.writer() as connection:
        if kind is not None:
            assert payload is not None
            rows = []
            for number in range(start, start + count):
                other = json.loads(json.dumps(payload))
                if kind == ("carl", "ebay", "collect", "item"):
                    other["request"]["item_identifier"] = f"history-{number}"
                elif kind == ("carl", "ebay", "collect", "description"):
                    other["url"] = f"https://example.test/history-{number}"
                else:
                    other["listing_id"] = f"history-{number}"
                rows.append(
                    (
                        f"history-{number}",
                        json.dumps(kind, separators=(",", ":")),
                        json.dumps(other),
                        acquisition_resource_identity_json(kind, other),
                        json.dumps([f"history-{number}"]),
                    )
                )
            await connection.executemany(
                """INSERT INTO work_items(
                    id,kind_parts_json,payload_schema_version,payload_json,
                    acquisition_resource_identity_json,deduplication_identity_json,
                    state,priority,eligible_at_utc_ns,created_at_utc_ns
                ) VALUES (?,?,1,?,?,?,'completed',0,0,0)""",
                rows,
            )
            return
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
@pytest.mark.parametrize("same_kind", (False, True))
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
async def test_acquisition_candidate_cost_ignores_unrelated_history(
    tmp_path: Path,
    kind: tuple[str, ...],
    payload: dict[str, JsonValue],
    same_kind: bool,
) -> None:
    resource = acquisition_resource_identity(kind, payload)
    assert resource is not None
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        async with database._connections.writer() as connection:
            await connection.executemany(
                """
                INSERT INTO work_items(
                    id,kind_parts_json,payload_schema_version,payload_json,
                    acquisition_resource_identity_json,deduplication_identity_json,
                    state,priority,eligible_at_utc_ns,
                    created_at_utc_ns,result_json
                ) VALUES (?,?,1,?,?,?,?,0,0,?,?)
                """,
                (
                    (
                        identifier,
                        json.dumps(kind, separators=(",", ":")),
                        json.dumps(payload),
                        acquisition_resource_identity_json(kind, payload),
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
        await _history(database, 0, 64, kind=kind if same_kind else None, payload=payload)
        before, before_steps = await _cost(database, kind, resource)
        await _history(database, 64, 4096, kind=kind if same_kind else None, payload=payload)
        after, after_steps = await _cost(database, kind, resource)
        assert before == after
        assert [source["identifier"] for source in after] == ["source"]
        assert after_steps <= before_steps + 500


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
            {
                "listing_id": "123",
                "request_plan": {
                    "url": "https://example.test/123",
                    "headers": [["X-Test-Route", "one"]],
                },
            },
        ),
        (
            ("carl", "ebay", "collect", "description"),
            {
                "request": {"stack_identifier": "étagère-望遠鏡"},
                "url": "https://example.test/説明/é",
            },
        ),
    ),
)
async def test_acquisition_candidates_preserve_route_and_evidence_freshness(
    tmp_path: Path, kind: tuple[str, ...], payload: dict[str, JsonValue]
) -> None:
    resource = acquisition_resource_identity(kind, payload)
    assert resource is not None
    variant = json.loads(json.dumps(payload))
    if kind[1] == "ebay":
        variant["request"]["stack_identifier"] = "different-stack"
    else:
        variant["request_plan"]["headers"] = [["X-Test-Route", "two"]]

    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        async with database._connections.writer() as connection:
            for identifier, source_payload, created, completed, acquisition, event_operation in (
                ("candidate", payload, 100, None, None, None),
                # Work enqueued before this refresh can finish afterwards and
                # supply newly acquired evidence. The boundary is inclusive.
                ("fresh", payload, 1, 101, "acq-fresh", "op-fresh"),
                ("boundary", payload, 1, 100, "acq-boundary", "op-boundary"),
                ("old", payload, 1, 99, "acq-old", "op-old"),
                # Completing a new wrapper around old evidence does not refresh
                # the acquisition's provenance timestamp.
                ("borrowed-old", payload, 101, 150, "acq-old", "op-borrowed-old"),
                ("wrong-event", payload, 101, 150, "acq-wrong-event", "unrelated-operation"),
                ("other-route", variant, 1, 101, "acq-other-route", "op-other-route"),
            ):
                await connection.execute(
                    """INSERT INTO work_items(
                        id,kind_parts_json,payload_schema_version,payload_json,
                        acquisition_resource_identity_json,deduplication_identity_json,
                        state,priority,eligible_at_utc_ns,created_at_utc_ns,result_json
                    ) VALUES (?,?,1,?,?,?,?,0,0,?,?)""",
                    (
                        identifier,
                        json.dumps(kind, separators=(",", ":")),
                        # Different key order must still identify the same
                        # request through its canonical resource metadata.
                        json.dumps(source_payload, sort_keys=identifier == "fresh"),
                        acquisition_resource_identity_json(kind, source_payload),
                        json.dumps([identifier]),
                        "pending" if completed is None else "completed",
                        created,
                        None
                        if acquisition is None
                        else json.dumps({"acquisition_record_identifier": acquisition}),
                    ),
                )
                await connection.execute(
                    """INSERT INTO work_events(
                        id,work_item_id,event_kind,recorded_at_utc_ns,data_json
                    ) VALUES (?,?,?,?,?)""",
                    (
                        f"event-{identifier}",
                        identifier,
                        "enqueued" if completed is None else "completed",
                        created if completed is None else completed,
                        json.dumps({"operation_identifier": event_operation}),
                    ),
                )
                if completed is None:
                    continue
                await connection.execute(
                    """INSERT INTO operations(
                        id,component_parts_json,output_schema_version,started_at_utc,
                        code_provenance_json,invocation_json,configuration_json,state
                    ) VALUES (?,'["test","acquire"]',1,'2026-10-17T00:00:00Z',
                              '{}','{}','{}','completed')""",
                    (f"op-{identifier}",),
                )
                await connection.execute(
                    "INSERT INTO work_operations VALUES (?,?,1,'token','worker')",
                    (f"op-{identifier}", identifier),
                )
                if identifier != "borrowed-old":
                    await connection.execute(
                        """INSERT INTO objects(
                            id,object_type,kind_parts_json,created_by_operation_id
                        ) VALUES (?,'record','["carl","http","acquisition"]',?)""",
                        (acquisition, f"op-{identifier}"),
                    )
        found = await database.completed_acquisition_candidates(
            kind, resource, work_identifier="candidate"
        )
        assert {source["identifier"] for source in found} == {"fresh", "boundary"}

        # The opt-in historical reuse path keeps its existing meaning; exact
        # resource isolation still applies when freshness checks are disabled.
        historical = await database.completed_acquisition_candidates(
            kind, resource, work_identifier="candidate", since_work_created=False
        )
        assert {source["identifier"] for source in historical} == {
            "fresh",
            "boundary",
            "old",
            "borrowed-old",
            "wrong-event",
        }
