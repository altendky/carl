"""Route-capacity retirement examines bounded metadata pages and survives restart."""

from __future__ import annotations

# Query plans, retirement tombstones and deliberately bulk-seeded metadata are
# the storage boundary under test; no acquisition or work payload is executed.
# pyright: reportPrivateUsage=false
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import apsw
import pytest

from carl._tests.test_worker import _provenance
from carl.core.components import Component, ComponentId
from carl.core.json import decode_json, encode_json
from carl.core.legacy_capacity import (
    FACEBOOK_ROUTE_CAPACITY_FAMILIES,
    is_legacy_facebook_route_capacity_constraint,
)
from carl.core.models import JsonValue
from carl.core.work import (
    ConcurrencyConstraint,
    Constraint,
    SchedulingScope,
    SchedulingScopeKind,
)
from carl.io.sqlite import Database
from carl.work_runtime import prepare_worker_constraints

_COMPONENT = Component(ComponentId(("test", "route_capacity_metadata")), 1, lambda: None)


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _route(
    family: tuple[str, ...], name: str, *, maximum: int = 1, version: str = "v2"
) -> ConcurrencyConstraint:
    scope_family = (
        "search_acquisition" if family[3] == "route_concurrency" else "effective_search_acquisition"
    )
    routing = ("decodo", "account", name)
    return ConcurrencyConstraint(
        identifier=(*family, version, *routing),
        scope=SchedulingScope(
            kind=SchedulingScopeKind.NETWORK_PATH, identity=(scope_family, *routing)
        ),
        maximum_active=maximum,
    )


async def _seed(database: Database, constraints: tuple[ConcurrencyConstraint, ...]) -> None:
    # Exact public model serialization, inserted in one transaction to keep the
    # 10k fixture setup independent of one-await-per-row scheduler overhead.
    async with database._connections.writer() as connection:
        _ = await connection.executemany(
            "INSERT INTO scheduling_constraints VALUES(?,?,?,?,?,?,?,?)",
            [
                (
                    encode_json(list(item.identifier)),
                    item.kind.value,
                    item.subject_kind.value,
                    item.scope.kind.value,
                    encode_json(list(item.scope.identity)),
                    item.schema_version,
                    encode_json(item.model_dump(mode="json")),
                    1,
                )
                for item in constraints
            ],
        )


async def _retire(database: Database, constraints: tuple[ConcurrencyConstraint, ...]) -> None:
    await database.begin_operation(
        operation_id="preexisting-retirement",
        component=_COMPONENT,
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc=datetime.now(UTC).isoformat(),
    )
    await database.supersede_constraints(
        retired_identifiers=tuple(item.identifier for item in constraints),
        replacements=(),
        operation_identifier="preexisting-retirement",
        at_utc_ns=2,
        reason="preexisting policy retirement",
    )
    await database.complete_operation(
        operation_id="preexisting-retirement",
        records=(),
        artifacts=(),
        outputs=(),
        result={},
        ended_at_utc=datetime.now(UTC).isoformat(),
        duration_ns=0,
    )


async def _tombstones(database: Database) -> dict[str, tuple[object, ...]]:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                "SELECT identifier_parts_json,retired_at_utc_ns,operation_id,reason FROM scheduling_constraint_retirements ORDER BY identifier_parts_json"
            )
        ).fetchall()
    return {str(row[0]): tuple(row[1:]) for row in rows}


async def _audits(database: Database) -> tuple[dict[str, JsonValue], ...]:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                """SELECT state,configuration_json,result_json FROM operations
            WHERE component_parts_json='["carl","work","retire_route_capacity"]'
            ORDER BY id"""
            )
        ).fetchall()
    audits = tuple(
        {
            "state": row[0],
            "configuration": decode_json(str(row[1])),
            "result": decode_json(str(row[2])),
        }
        for row in rows
    )
    for audit in audits:
        configuration = audit["configuration"]
        result = audit["result"]
        assert isinstance(configuration, dict) and isinstance(result, dict)
        configuration = cast(dict[str, JsonValue], configuration)
        result = cast(dict[str, JsonValue], result)
        assert configuration["page_limit"] == 128
        assert len(configuration["retired_definitions"]) <= 128
        assert 0 < result["retired_candidates"] <= result["scanned"] <= 128
    return audits


async def _measure[T](
    database: Database, action: Callable[[], Awaitable[T]]
) -> tuple[T, int, tuple[str, ...]]:
    steps = 0
    statements: list[tuple[str, apsw.Bindings | None]] = []

    def progress() -> bool:
        nonlocal steps
        steps += 1
        return False

    def trace(_cursor: apsw.Cursor, sql: str, bindings: apsw.Bindings | None) -> bool:
        if sql.lstrip().upper().startswith("SELECT"):
            statements.append((sql, bindings))
        return True

    def authorize(
        action: int,
        table: str | None,
        _column: str | None,
        _database: str | None,
        _trigger: str | None,
    ) -> int:
        if action == apsw.SQLITE_READ and table not in {
            "scheduling_constraints",
            "scheduling_constraint_retirements",
        }:
            return apsw.SQLITE_DENY
        return apsw.SQLITE_OK

    async with database._connections.reader() as connection:
        await connection.set_progress_handler(progress, nsteps=1, id="route-capacity-cost")
        await connection.set_authorizer(authorize)
        connection.set_exec_trace(trace)
        try:
            result = await action()
        finally:
            connection.set_exec_trace(None)
            await connection.set_authorizer(None)
            await connection.set_progress_handler(None, id="route-capacity-cost")
        plans: list[str] = []
        for sql, bindings in statements:
            rows = await (
                await connection.execute("EXPLAIN QUERY PLAN " + sql, bindings)
            ).fetchall()
            plans.extend(str(row[3]) for row in rows)
    assert statements and steps > 0
    assert any("SEARCH" in plan and "scheduling_constraints" in plan for plan in plans), plans
    assert not any("SCAN c" in plan or "SCAN scheduling_constraints" in plan for plan in plans), (
        plans
    )
    return result, steps, tuple(plans)


@pytest.mark.anyio
@pytest.mark.parametrize("family", FACEBOOK_ROUTE_CAPACITY_FAMILIES)
async def test_ten_thousand_metadata_rows_use_bounded_indexed_pages(
    tmp_path: Path, family: tuple[str, ...]
) -> None:
    constraints = tuple(
        _route(family, f"route-{index:05d}", maximum=7 if index % 4 == 1 else 1)
        for index in range(10_000)
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database, constraints)
        await _seed(
            database,
            tuple(
                _route(("unrelated", "metadata", "history", "capacity"), f"history-{index:05d}")
                for index in range(10_000)
            ),
        )
        await _retire(database, constraints[:128])
        through, high_steps, _ = await _measure(
            database, lambda: database.facebook_route_capacity_high_water(family)
        )
        assert through == encode_json(list(constraints[-1].identifier))
        assert through is not None
        first, first_steps, _ = await _measure(
            database,
            lambda: database.facebook_route_capacity_constraint_page(
                family=family, through=through
            ),
        )
        assert len(first) == 128 and all(retired for _, _, retired in first)
        assert first[-1][0] == encode_json(list(constraints[127].identifier))
        deep_after = encode_json(list(constraints[9_743].identifier))
        deep, deep_steps, _ = await _measure(
            database,
            lambda: database.facebook_route_capacity_constraint_page(
                family=family, through=through, after=deep_after
            ),
        )
        assert len(deep) == 128 and all(not retired for _, _, retired in deep)
        assert deep[0][0] == encode_json(list(constraints[9_744].identifier))
        assert deep_steps <= first_steps + 1000
        assert first_steps <= 12_000 and deep_steps <= 12_000 and high_steps <= 4096
        print(f"route metadata VM: high={high_steps} first128={first_steps} deep128={deep_steps}")
        # Unknown/custom definitions count as examined rows rather than causing
        # the helper to walk until it finds128 actionable definitions.
        second = await database.facebook_route_capacity_constraint_page(
            family=family, through=through, after=first[-1][0]
        )
        assert len(second) == 128
        assert any(not is_legacy_facebook_route_capacity_constraint(item) for _, item, _ in second)
        tail = await database.facebook_route_capacity_constraint_page(
            family=family, through=through, after=encode_json(list(constraints[-2].identifier))
        )
        assert len(tail) == 1 and tail[0][0] == through
        assert (
            await database.facebook_route_capacity_constraint_page(
                family=family, through=through, after=through
            )
            == ()
        )


@pytest.mark.anyio
async def test_startup_preserves_unknown_and_customized_route_definitions(tmp_path: Path) -> None:
    known = tuple(
        _route(family, f"known-{index}", version=version, maximum=20 if version == "v3" else 1)
        for family in FACEBOOK_ROUTE_CAPACITY_FAMILIES
        for index, version in enumerate(("v1", "v2", "v3"))
    )
    custom = tuple(
        item.model_copy(
            update={"identifier": (*item.identifier[:-1], "custom"), "maximum_active": 7}
        )
        for item in known
    )
    unknown = tuple(
        _route(family, "unknown", version="v99") for family in FACEBOOK_ROUTE_CAPACITY_FAMILIES
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database, (*known, *custom, *unknown))
        await _retire(database, known[:1])
        before = await _tombstones(database)
        await prepare_worker_constraints(database, tmp_path)
        retired = await _tombstones(database)
        assert set(retired) == {encode_json(list(item.identifier)) for item in known}
        assert all(retired[key] == value for key, value in before.items())
        audits = await _audits(database)
        assert audits and all(audit["state"] == "completed" for audit in audits)
        await prepare_worker_constraints(database, tmp_path)
        assert await _tombstones(database) == retired
        assert await _audits(database) == audits


class _Interrupted(Exception):
    pass


@pytest.mark.anyio
async def test_restart_continues_after_one_committed_batch_without_rewriting_tombstones(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "carl.sqlite3"
    family = FACEBOOK_ROUTE_CAPACITY_FAMILIES[0]
    constraints = tuple(_route(family, f"resume-{index:04d}") for index in range(300))
    async with Database.managed(path, initialize=True) as database:
        await _seed(database, constraints)
        original = database.facebook_route_capacity_constraint_page
        calls = 0

        async def interrupt(
            *, family: tuple[str, ...], through: str, after: str | None = None, limit: int = 128
        ) -> tuple[tuple[str, Constraint, bool], ...]:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise _Interrupted("after first audited page")
            return await original(family=family, through=through, after=after, limit=limit)

        with monkeypatch.context() as patch:
            patch.setattr(database, "facebook_route_capacity_constraint_page", interrupt)
            with pytest.raises(_Interrupted):
                await prepare_worker_constraints(database, tmp_path)
        before = await _tombstones(database)
        assert len(before) == 128
        audits = await _audits(database)
        assert len(audits) == 1 and audits[0]["state"] == "completed"
    async with Database.managed(path) as database:
        await prepare_worker_constraints(database, tmp_path)
        after = await _tombstones(database)
        assert len(after) == 300
        assert all(after[key] == value for key, value in before.items())
        assert len(await _audits(database)) == 3
        await prepare_worker_constraints(database, tmp_path)
        assert await _tombstones(database) == after
        assert len(await _audits(database)) == 3


@pytest.mark.anyio
async def test_runtime_high_water_excludes_later_higher_identity_until_next_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    families = FACEBOOK_ROUTE_CAPACITY_FAMILIES
    initial = tuple(_route(family, "route-a") for family in families)
    later = tuple(_route(family, "route-z") for family in families)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database, initial)
        original = database.facebook_route_capacity_constraint_page
        inserted = False

        async def insert_after_snapshot(
            *, family: tuple[str, ...], through: str, after: str | None = None, limit: int = 128
        ) -> tuple[tuple[str, Constraint, bool], ...]:
            nonlocal inserted
            if not inserted:
                inserted = True
                await _seed(database, later)
            return await original(family=family, through=through, after=after, limit=limit)

        with monkeypatch.context() as patch:
            patch.setattr(
                database, "facebook_route_capacity_constraint_page", insert_after_snapshot
            )
            await prepare_worker_constraints(database, tmp_path)
        first = await _tombstones(database)
        assert set(first) == {encode_json(list(item.identifier)) for item in initial}
        await prepare_worker_constraints(database, tmp_path)
        assert set(await _tombstones(database)) == {
            encode_json(list(item.identifier)) for item in (*initial, *later)
        }


@pytest.mark.anyio
async def test_page_rejects_cross_family_cursors_and_unbounded_limits(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        family, other = FACEBOOK_ROUTE_CAPACITY_FAMILIES
        item = _route(family, "route")
        await _seed(database, (item,))
        through = encode_json(list(item.identifier))
        for limit in (0, 129, True):
            with pytest.raises(ValueError):
                _ = await database.facebook_route_capacity_constraint_page(
                    family=family, through=through, limit=limit
                )
        with pytest.raises(ValueError):
            _ = await database.facebook_route_capacity_constraint_page(
                family=other, through=through
            )
        assert await database.facebook_route_capacity_high_water(other) is None
