"""Sleeping distinct roots stay outside both ordinary and fair dispatch work."""

from __future__ import annotations

# Genuine sleep metadata is cloned in bounded fixture transactions, not production APIs.
# pyright: reportPrivateUsage=false
from dataclasses import dataclass
from pathlib import Path
from time import time_ns
from typing import Literal, cast
from uuid import NAMESPACE_URL, uuid4, uuid5

import anyio
import apsw
import pytest

import carl.io.sqlite as sqlite_io
from carl import work_runtime
from carl._tests.test_progress_runtime import _passes, _ProgressRuntime, _sleeping
from carl._tests.test_worker import _provenance
from carl.core.json import decode_json, encode_json
from carl.core.models import JsonValue
from carl.core.pipeline import SEARCH_PIPELINE_WORK_KIND
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.sqlite import Database
from carl.io.stage_dispatcher import StageDispatcher
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import stage_manager
from carl.io.work_progress import build_identifier

pytestmark = pytest.mark.anyio
_CAPABILITY = WorkCapability(kind=SEARCH_PIPELINE_WORK_KIND, payload_schema_version=1)


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@dataclass(frozen=True)
class _Rows:
    columns: tuple[str, ...]
    rows: tuple[dict[str, apsw.SQLiteValue], ...]


async def _template(database: Database) -> dict[str, _Rows]:
    work = "template"
    filters = (
        ("operations", "id IN (SELECT operation_id FROM work_operations WHERE work_item_id=?)"),
        ("work_items", "id=?"),
        (
            "fair_roots",
            "root_request_identifier IN (SELECT root_request_identifier FROM work_scheduling WHERE work_item_id=?)",
        ),
        (
            "fair_workspace_service",
            "workspace_key IN (SELECT workspace_key FROM work_scheduling WHERE work_item_id=?)",
        ),
        (
            "fair_root_service",
            "root_request_identifier IN (SELECT root_request_identifier FROM work_scheduling WHERE work_item_id=?)",
        ),
        ("work_scopes", "work_item_id=?"),
        ("work_requests", "work_item_id=?"),
        ("work_events", "work_item_id=?"),
        ("work_operations", "work_item_id=?"),
        ("stage_attempts", "work_item_id=?"),
        ("work_stages", "work_item_id=?"),
        ("stage_execution_tokens", "stage_id IN (SELECT id FROM work_stages WHERE work_item_id=?)"),
        ("stage_events", "stage_id IN (SELECT id FROM work_stages WHERE work_item_id=?)"),
        ("work_progress_revisions", "work_id=?"),
        ("work_scheduling", "work_item_id=?"),
        ("progress_wait_builds", "work_id=? AND state='armed'"),
        (
            "progress_wait_source_chunks",
            "build_id IN (SELECT id FROM progress_wait_builds WHERE work_id=?)",
        ),
        (
            "progress_wait_members",
            "build_id IN (SELECT id FROM progress_wait_builds WHERE work_id=?)",
        ),
        (
            "progress_wait_groups",
            "build_id IN (SELECT id FROM progress_wait_builds WHERE work_id=?)",
        ),
    )
    result: dict[str, _Rows] = {}
    async with database._connections.reader() as connection:
        for table, condition in filters:
            info = await (await connection.execute(f"PRAGMA table_info({table})")).fetchall()
            columns = tuple(str(row[1]) for row in info)
            rows = await (
                await connection.execute(f"SELECT * FROM {table} WHERE {condition}", (work,))
            ).fetchall()
            result[table] = _Rows(
                columns, tuple(dict(zip(columns, row, strict=True)) for row in rows)
            )
    assert len(result["progress_wait_builds"].rows) == 1
    assert len(result["progress_wait_members"].rows) == 1
    assert all(row["ready_count"] == 0 for row in result["fair_root_service"].rows)
    return result


def _rewrite(value: JsonValue, mapping: dict[str, str]) -> JsonValue:
    if isinstance(value, str):
        return mapping.get(value, value)
    if isinstance(value, list):
        return [_rewrite(item, mapping) for item in cast(list[JsonValue], value)]
    if isinstance(value, dict):
        return {
            key: _rewrite(item, mapping) for key, item in cast(dict[str, JsonValue], value).items()
        }
    return value


def _mapping(template: dict[str, _Rows], ordinal: int) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for rows in template.values():
        for row in rows.rows:
            for column, value in row.items():
                if column in {"id", "token", "lease_token"} and isinstance(value, str):
                    mapping[value] = str(
                        uuid5(NAMESPACE_URL, f"progress-sleeper:{ordinal}:{value}")
                    )
    root = f"sleep-root-{ordinal:05d}"
    workspace = f"sleep-workspace-{ordinal:05d}"
    mapping.update(
        {
            "template": f"sleeper-{ordinal:05d}",
            "template-root": root,
            "workspace-template-root": workspace,
            '["root","template-root"]': encode_json(["root", root]),
            '["workspace","workspace-template-root"]': encode_json(["workspace", workspace]),
        }
    )
    for row in template["progress_wait_builds"].rows:
        mapping[str(row["id"])] = build_identifier(
            mapping[str(row["source_token"])], mapping[str(row["continuation_id"])]
        )
    return mapping


async def _clone(database: Database, template: dict[str, _Rows], start: int, end: int) -> None:
    for first in range(start, end, 128):
        maps = [_mapping(template, ordinal) for ordinal in range(first, min(first + 128, end))]
        async with database._connections.writer() as connection:
            for table, data in template.items():
                if not data.rows:
                    continue
                columns = tuple(
                    column
                    for column in data.columns
                    if column != "sequence" or table not in {"work_events", "stage_events"}
                )
                values: list[tuple[apsw.SQLiteValue, ...]] = []
                for mapping in maps:
                    for row in data.rows:
                        rewritten: list[apsw.SQLiteValue] = []
                        for column in columns:
                            value = row[column]
                            if isinstance(value, str):
                                value = (
                                    encode_json(_rewrite(decode_json(value), mapping))
                                    if column.endswith("_json")
                                    else mapping.get(value, value)
                                )
                            rewritten.append(value)
                        values.append(tuple(rewritten))
                statement = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})"
                _ = await connection.executemany(statement, values)
    async with database._connections.reader() as connection:
        assert await (await connection.execute("PRAGMA foreign_key_check")).fetchall() == []


async def _measure(
    database: Database, mode: Literal["ordinary", "fair", "legacy", "loop"]
) -> tuple[int, list[tuple[int, str]]]:
    steps = 0
    starts: list[tuple[int, str]] = []

    def progress() -> bool:
        nonlocal steps
        steps += 1
        return False

    def trace(_cursor: object, statement: str, _bindings: object) -> bool:
        starts.append((steps, statement))
        return True

    connections = (database._connections._writer, *database._connections._readers)
    for connection in connections:
        await connection.set_progress_handler(progress, nsteps=1, id="progress-dispatch-cost")
        connection.set_exec_trace(trace)
    try:
        if mode == "loop":
            manager = stage_manager(database)
            registry = StageHandlerRegistry((), ())
            residents = ResidentSessionPool(manager, registry, _provenance(), {})

            async def no_owned(_identifier: str) -> bool:
                raise AssertionError("No sleeping stage may submit an owned task")

            dispatcher = StageDispatcher(
                manager, registry, residents, _provenance(), {}, no_owned, lambda _kind: False
            )
            _ = await dispatcher._progress()
            await dispatcher._recover()
            assert await database.resource_occupancy(manager.resources.scope) == {}
        elif mode == "legacy":
            page = await database.expired_legacy_work_candidates(now_utc_ns=time_ns(), limit=32)
            assert page.items == ()
        elif mode == "fair":
            page = await database.fair_dispatch_candidates(
                scope=stage_manager(database).resources.scope,
                supported_capabilities=(_CAPABILITY,),
                now_utc_ns=time_ns(),
                limit=32,
            )
            assert page.items == ()
        else:
            page = await database.dispatch_candidates(
                supported_capabilities=(_CAPABILITY,),
                now_utc_ns=time_ns(),
                limit=32,
            )
            assert page.items == ()
    finally:
        for connection in connections:
            connection.set_exec_trace(None)
            await connection.set_progress_handler(None, id="progress-dispatch-cost")
    assert steps > 0, "The SQLite VM counter must measure actual execution"
    ends = [start for start, _ in starts[1:]] + [steps]
    return steps, [
        (end - start, " ".join(statement.split())[:180])
        for (start, statement), end in zip(starts, ends, strict=True)
    ]


async def test_ten_thousand_distinct_sleeping_roots_are_not_dispatched_or_scanned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        harness = _ProgressRuntime(database)
        harness.install(monkeypatch, tmp_path)
        _ = await database.enqueue_work(
            WorkDefinition(
                identifier="PEER_PAYLOAD-unsupported-child",
                kind=("test", "unsupported_child"),
                payload_schema_version=1,
                payload=None,
                deduplication_identity=("child",),
                not_before_utc_ns=0,
                scopes=(
                    SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                    SchedulingScope(
                        kind=SchedulingScopeKind.WORK_KIND, identity=("test", "unsupported_child")
                    ),
                ),
            ),
            WorkRequester(
                request_identifier=str(uuid4()), kind=("test",), identifier="fixture", context={}
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        _ = await harness.enqueue_parent(
            "template", "PEER_PAYLOAD-unsupported-child", root="template-root"
        )
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            _ = await _sleeping(database, "template")
        # Quiet the initial parent's unrelated progress notification before
        # cloning, so the sample measures sleepers, not a pending fanout pass.
        while (await database.drain_progress_notifications(now_utc_ns=time_ns())).more:
            pass
        template = await _template(database)
        await _clone(database, template, 1, 32)
        original = decode_json

        def metadata_only(value: str | bytes) -> JsonValue:
            assert "PEER_PAYLOAD" not in str(value), "Dispatch decoded a sleeping work payload"
            return original(value)

        monkeypatch.setattr(sqlite_io, "decode_json", metadata_only)
        modes: tuple[Literal["ordinary", "fair", "legacy", "loop"], ...] = (
            "ordinary",
            "fair",
            "legacy",
            "loop",
        )
        small = {mode: await _measure(database, mode) for mode in modes}
        await _clone(database, template, 32, 10000)
        large = {mode: await _measure(database, mode) for mode in modes}
        measurements: dict[str, JsonValue] = {}
        for mode in modes:
            measurements[f"{mode}_steps_32_roots"] = small[mode][0]
            measurements[f"{mode}_steps_10000_roots"] = large[mode][0]
            assert large[mode][0] <= small[mode][0] + 1000, (mode, small[mode], large[mode])
        _ = (tmp_path / "dispatch-cost.json").write_text(encode_json(measurements))
        async with database._connections.reader() as connection:
            before = await (
                await connection.execute(
                    "SELECT sum(attempt) FROM work_items WHERE kind_parts_json=?",
                    (encode_json(list(SEARCH_PIPELINE_WORK_KIND)),),
                )
            ).fetchone()
            tokens = await (
                await connection.execute("SELECT count(*) FROM stage_execution_tokens")
            ).fetchone()
            assert await (
                await connection.execute(
                    "SELECT count(DISTINCT root_request_identifier) FROM work_scheduling WHERE work_item_id!='PEER_PAYLOAD-unsupported-child'"
                )
            ).fetchone() == (10000,)
        calls = sum(len(contexts) for contexts in harness.calls.values())
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            passes = _passes(monkeypatch)
            harness.child.clock.advance(60)
            with anyio.fail_after(15):
                await passes.wait()
            assert sum(len(contexts) for contexts in harness.calls.values()) == calls == 1
            assert await database.resource_occupancy(stage_manager(database).resources.scope) == {}
            async with database._connections.reader() as connection:
                assert (
                    await (
                        await connection.execute(
                            "SELECT sum(attempt) FROM work_items WHERE kind_parts_json=?",
                            (encode_json(list(SEARCH_PIPELINE_WORK_KIND)),),
                        )
                    ).fetchone()
                    == before
                )
                assert (
                    await (
                        await connection.execute("SELECT count(*) FROM stage_execution_tokens")
                    ).fetchone()
                    == tokens
                )
                assert await (
                    await connection.execute(
                        "SELECT count(*) FROM fair_ready WHERE active=1 AND kind_parts_json=?",
                        (encode_json(list(SEARCH_PIPELINE_WORK_KIND)),),
                    )
                ).fetchone() == (0,)
                assert await (
                    await connection.execute("SELECT count(*) FROM fair_offers")
                ).fetchone() == (0,)
