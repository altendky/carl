"""Admission counts live permits without visiting completed scope history."""

# pyright: reportPrivateUsage=false

import json
from pathlib import Path

import pytest

from carl._tests.test_worker import _provenance
from carl.core.components import Component, ComponentId
from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
)
from carl.io.sqlite import Database

type SeedRow = tuple[str, str, int | None, str]


def _operation() -> None:
    pass


async def _prepare(
    database: Database,
    subject: SchedulingSubjectKind,
    scope: SchedulingScope,
    maximum_active: int,
) -> None:
    await database.register_constraint(
        ConcurrencyConstraint(
            identifier=("test", "admission"),
            subject_kind=subject,
            scope=scope,
            maximum_active=maximum_active,
        ),
        registered_at_utc_ns=1,
    )
    if subject is SchedulingSubjectKind.NETWORK_ACTIVITY:
        await database.begin_operation(
            operation_id="operation",
            component=Component(ComponentId(("test", "admission")), 1, _operation),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-10-17T00:00:00+00:00",
        )


async def _seed(
    database: Database,
    subject: SchedulingSubjectKind,
    scope_kind: SchedulingScopeKind,
    rows: tuple[SeedRow, ...],
) -> None:
    async with database._connections.writer() as connection:
        if subject is SchedulingSubjectKind.WORK_ITEM:
            await connection.executemany(
                """
                INSERT INTO work_items(
                    id,kind_parts_json,payload_schema_version,payload_json,
                    deduplication_identity_json,state,priority,eligible_at_utc_ns,
                    created_at_utc_ns,lease_token,lease_owner,lease_expires_at_utc_ns
                ) VALUES (?,'["test","work"]',1,'{}',?,?,0,0,0,?,?,?)
                """,
                (
                    (
                        identifier,
                        json.dumps([identifier]),
                        state,
                        identifier if expiry is not None else None,
                        "worker" if expiry is not None else None,
                        expiry,
                    )
                    for identifier, state, expiry, _ in rows
                ),
            )
            scopes = "work_scopes"
        else:
            await connection.executemany(
                """
                INSERT INTO network_activities(
                    id,kind_parts_json,operation_id,network_session_identifier,
                    ordinal,attempt,state,eligible_at_utc_ns,created_at_utc_ns,
                    admission_token,admitted_at_utc_ns,permit_expires_at_utc_ns
                ) VALUES (?,'["test","network"]','operation',?,1,1,?,0,0,?,?,?)
                """,
                (
                    (
                        identifier,
                        identifier,
                        state,
                        identifier if expiry is not None else None,
                        1 if expiry is not None else None,
                        expiry,
                    )
                    for identifier, state, expiry, _ in rows
                ),
            )
            scopes = "network_activity_scopes"
        await connection.executemany(
            f"INSERT INTO {scopes} VALUES (?, ?, ?)",
            ((identifier, scope_kind.value, scope) for identifier, _, _, scope in rows),
        )


async def _availability(
    database: Database, subject: SchedulingSubjectKind, now: int
) -> tuple[bool, int | None]:
    async with database._connections.reader() as connection:
        if subject is SchedulingSubjectKind.WORK_ITEM:
            available, eligible, _ = await database._constraint_availability(
                connection, work_item_identifier="candidate", now_utc_ns=now
            )
        else:
            available, eligible, _ = await database._network_activity_constraint_availability(
                connection, network_activity_identifier="candidate", now_utc_ns=now
            )
        return available, eligible


@pytest.mark.anyio
@pytest.mark.parametrize("subject", list(SchedulingSubjectKind))
@pytest.mark.parametrize("maximum_active", (1, 2, 3))
async def test_admission_counts_only_live_matching_owners(
    tmp_path: Path, subject: SchedulingSubjectKind, maximum_active: int
) -> None:
    scope = SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=("matching",))
    matching = '["matching"]'
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _prepare(database, subject, scope, maximum_active)
        if subject is SchedulingSubjectKind.WORK_ITEM:
            live = "leased"
            second_live = "cancelling"
            terminal_states = ("completed", "terminal_failure", "cancelled")
        else:
            live = second_live = "admitted"
            terminal_states = ("completed", "skipped", "failed", "cancelled")
        await _seed(
            database,
            subject,
            scope.kind,
            (
                ("candidate", "pending", None, matching),
                ("active", live, 200, matching),
                ("second-active", second_live, 160, matching),
                ("expired", live, 100, matching),
                ("other-route", live, 120, '["other"]'),
                ("pending", "pending", None, matching),
                *((state, state, None, matching) for state in terminal_states),
            ),
        )
        # Exactly two count. For work, one is cancelling and still owns its permit.
        assert await _availability(database, subject, 100) == (
            (True, None) if maximum_active > 2 else (False, 160)
        )
        # Expiry is exclusive, including the earliest owner's exact deadline.
        assert await _availability(database, subject, 160) == (
            (False, 200) if maximum_active == 1 else (True, None)
        )
        assert await _availability(database, subject, 200) == (True, None)


async def _history(
    database: Database,
    subject: SchedulingSubjectKind,
    scope: SchedulingScope,
    start: int,
    count: int,
) -> None:
    """Populate historical owners in two SQL batches, not scheduler calls per row."""
    async with database._connections.writer() as connection:
        sequence = (
            "WITH RECURSIVE numbers(n) AS (SELECT ? UNION ALL SELECT n+1 FROM numbers WHERE n<?)"
        )
        bindings = (start, start + count - 1)
        if subject is SchedulingSubjectKind.WORK_ITEM:
            await connection.execute(
                sequence
                + """
                INSERT INTO work_items(
                    id,kind_parts_json,payload_schema_version,payload_json,
                    deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns
                ) SELECT 'history-'||n,'["test","work"]',1,'{}',json_array('history-'||n),
                         CASE n%4 WHEN 0 THEN 'pending' WHEN 1 THEN 'completed'
                                  WHEN 2 THEN 'terminal_failure' ELSE 'cancelled' END,0,0,0
                  FROM numbers
                """,
                bindings,
            )
            table = "work_scopes"
        else:
            await connection.execute(
                sequence
                + """
                INSERT INTO network_activities(
                    id,kind_parts_json,operation_id,network_session_identifier,ordinal,attempt,
                    state,eligible_at_utc_ns,created_at_utc_ns
                ) SELECT 'history-'||n,'["test","network"]','operation','session-'||n,1,1,
                         CASE n%5 WHEN 0 THEN 'pending' WHEN 1 THEN 'completed'
                                  WHEN 2 THEN 'skipped' WHEN 3 THEN 'failed' ELSE 'cancelled' END,0,0
                  FROM numbers
                """,
                bindings,
            )
            table = "network_activity_scopes"
        await connection.execute(
            sequence + f" INSERT INTO {table} SELECT 'history-'||n,?,? FROM numbers",
            (*bindings, scope.kind.value, json.dumps(scope.identity, separators=(",", ":"))),
        )


async def _cost(
    database: Database,
    subject: SchedulingSubjectKind,
    scope: SchedulingScope,
    *,
    scope_first_control: bool = False,
) -> tuple[tuple[bool, int | None], int]:
    steps = 0

    def count() -> bool:
        nonlocal steps
        steps += 100
        return False

    async with database._connections.reader() as connection:
        await connection.set_progress_handler(count, nsteps=100, id="admission-budget")
        try:
            if scope_first_control:
                # Reproduce the historical-scope-first plan as a negative control.
                if subject is SchedulingSubjectKind.WORK_ITEM:
                    owners, scopes, key = "work_items", "work_scopes", "work_item_id"
                    expiry, states = "lease_expires_at_utc_ns", "('leased','cancelling')"
                else:
                    owners, scopes, key = (
                        "network_activities",
                        "network_activity_scopes",
                        "network_activity_id",
                    )
                    expiry, states = "permit_expires_at_utc_ns", "('admitted')"
                cursor = await connection.execute(
                    f"""
                    SELECT count(*),min(owner.{expiry})
                    FROM {scopes} AS scope INDEXED BY {scopes}_lookup
                    CROSS JOIN {owners} AS owner ON owner.id=scope.{key}
                    WHERE scope.scope_kind=? AND scope.scope_identity_json=?
                      AND owner.state IN {states} AND owner.{expiry}>100
                    """,
                    (scope.kind.value, json.dumps(scope.identity, separators=(",", ":"))),
                )
                row = await cursor.fetchone()
                assert row is not None and row[0] == 1 and row[1] == 200
                result = (False, 200)
            else:
                result = await _availability(database, subject, 100)
        finally:
            await connection.set_progress_handler(None, id="admission-budget")
    return result, steps


@pytest.mark.anyio
@pytest.mark.parametrize("subject", list(SchedulingSubjectKind))
@pytest.mark.parametrize(
    "scope_kind", (SchedulingScopeKind.OVERALL, SchedulingScopeKind.NETWORK_PATH)
)
async def test_admission_cost_does_not_grow_with_inactive_scope_history(
    tmp_path: Path, subject: SchedulingSubjectKind, scope_kind: SchedulingScopeKind
) -> None:
    scope = SchedulingScope(
        kind=scope_kind, identity=() if scope_kind is SchedulingScopeKind.OVERALL else ("matching",)
    )
    identity = json.dumps(scope.identity, separators=(",", ":"))
    live = "leased" if subject is SchedulingSubjectKind.WORK_ITEM else "admitted"
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _prepare(database, subject, scope, 1)
        await _seed(
            database,
            subject,
            scope.kind,
            (("candidate", "pending", None, identity), ("active", live, 200, identity)),
        )
        await _history(database, subject, scope, 0, 64)
        before, before_steps = await _cost(database, subject, scope)
        _, control_before = await _cost(database, subject, scope, scope_first_control=True)
        await _history(database, subject, scope, 64, 4096)
        after, after_steps = await _cost(database, subject, scope)
        _, control_after = await _cost(database, subject, scope, scope_first_control=True)
        assert before == after == (False, 200)
        assert after_steps <= before_steps + 500
        # The old loop order fails the same budget by a wide margin.
        assert control_after > control_before + 10_000
