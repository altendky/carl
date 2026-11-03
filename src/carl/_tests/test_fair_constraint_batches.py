"""Batched fair constraints preserve policy boundaries without reading archives."""

# Raw fixtures retain historical metadata without thousands of unrelated audits.
# pyright: reportPrivateUsage=false

from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest

from carl._tests.test_worker import _provenance
from carl.core.components import Component, ComponentId
from carl.core.json import encode_json
from carl.core.work import (
    ConcurrencyConstraint,
    NetworkActivityDefinition,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
)
from carl.io.fair_constraints import (
    NetworkConstraintCandidate,
    WorkConstraintCandidate,
    eligible_network,
    eligible_work,
)
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
NOW = 1_000
WORK = SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=("test", "work"))
NETWORK = SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=("route", "café"))


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _operation(database: Database) -> None:
    await database.begin_operation(
        operation_id="operation",
        component=Component(ComponentId(("test", "constraints")), 1, lambda: None),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-11-03T00:00:00Z",
    )


async def _work(
    database: Database,
    identifier: str,
    *,
    state: str = "pending",
    expiry: int | None = None,
    scope: SchedulingScope = WORK,
) -> None:
    async with database._connections.writer() as connection:
        await connection.execute(
            """INSERT INTO work_items(
                id,kind_parts_json,payload_schema_version,payload_json,
                deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns,
                lease_token,lease_owner,lease_expires_at_utc_ns
            ) VALUES (?,'["test","work"]',1,'{}',?,?,0,0,0,?,?,?)""",
            (
                identifier,
                encode_json([identifier]),
                state,
                identifier if expiry is not None else None,
                "owner" if expiry is not None else None,
                expiry,
            ),
        )
        await connection.execute(
            "INSERT INTO work_scopes VALUES (?,?,?)",
            (identifier, scope.kind.value, encode_json(list(scope.identity))),
        )


def _network_definition(
    identifier: str, *, not_before: int = 0, scope: SchedulingScope = NETWORK
) -> NetworkActivityDefinition:
    return NetworkActivityDefinition(
        identifier=identifier,
        kind=("test", "network"),
        operation_identifier="operation",
        network_session_identifier=identifier,
        ordinal=1,
        not_before_utc_ns=not_before,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND, identity=("test", "network")
            ),
            scope,
        ),
    )


def _network_candidate(
    identifier: str,
    *,
    not_before: int = 0,
    stage: str = "stage-ø",
    execution: int = 0,
    created: int = 0,
    scope: SchedulingScope = NETWORK,
) -> NetworkConstraintCandidate:
    return NetworkConstraintCandidate(
        identifier,
        stage,
        execution,
        created,
        _network_definition(identifier, not_before=not_before, scope=scope),
    )


async def _network(database: Database, identifier: str, *, state: str, expiry: int | None) -> None:
    async with database._connections.writer() as connection:
        await connection.execute(
            """INSERT INTO network_activities(
                id,kind_parts_json,operation_id,network_session_identifier,ordinal,attempt,
                state,eligible_at_utc_ns,created_at_utc_ns,admission_token,
                admitted_at_utc_ns,permit_expires_at_utc_ns
            ) VALUES (?,'["test","network"]','operation',?,1,1,?,0,0,?,?,?)""",
            (
                identifier,
                identifier,
                state,
                identifier if expiry is not None else None,
                0 if expiry is not None else None,
                expiry,
            ),
        )
        await connection.executemany(
            "INSERT INTO network_activity_scopes VALUES (?,?,?)",
            (
                (identifier, scope.kind.value, encode_json(list(scope.identity)))
                for scope in _network_definition(identifier).scopes
            ),
        )


async def _rate(
    database: Database,
    constraint: SlidingWindowRateConstraint,
    identifier: str,
    at: int,
    *,
    subject: SchedulingSubjectKind | None = None,
) -> None:
    async with database._connections.writer() as connection:
        await connection.execute(
            "INSERT INTO rate_starts VALUES (?,?,?,?,?,?)",
            (
                identifier,
                encode_json(list(constraint.identifier)),
                (subject or constraint.subject_kind).value,
                identifier,
                identifier,
                at,
            ),
        )


async def _retire(database: Database, identifier: tuple[str, ...]) -> None:
    await database.supersede_constraints(
        retired_identifiers=(identifier,),
        replacements=(),
        operation_identifier="operation",
        at_utc_ns=NOW,
        reason="retired fixture policy",
    )


async def test_work_caps_count_live_leases_once_and_retire_without_rewriting(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _operation(database)
        for identifier in ("first", "second"):
            await _work(database, identifier)
        await _work(database, "expired", state="leased", expiry=NOW)
        await _work(database, "live", state="cancelling", expiry=NOW + 1)
        await _work(
            database,
            "other-scope",
            state="leased",
            expiry=NOW + 1,
            scope=SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=("other",)),
        )
        for maximum in (1, 2):
            await database.register_constraint(
                ConcurrencyConstraint(
                    identifier=("test", f"cap-{maximum}"), scope=WORK, maximum_active=maximum
                ),
                registered_at_utc_ns=0,
            )
        candidates = tuple(
            WorkConstraintCandidate(identifier, True) for identifier in ("first", "second")
        )
        async with database._connections.writer() as connection:
            assert await eligible_work(connection, candidates, now_utc_ns=NOW) == frozenset()
        await _retire(database, ("test", "cap-1"))
        # Two policies on one scope must not count the same live owner twice.
        async with database._connections.writer() as connection:
            assert await eligible_work(connection, candidates, now_utc_ns=NOW) == {
                "first",
                "second",
            }
        await database.register_constraint(
            UniformHoldoffConstraint(
                identifier=("test", "work-hold"), scope=WORK, minimum_ns=100_000, maximum_ns=100_000
            ),
            registered_at_utc_ns=0,
        )
        async with database._connections.writer() as connection:
            assert await eligible_work(connection, candidates, now_utc_ns=NOW) == {
                "first",
                "second",
            }


async def test_work_rates_skip_active_attempts_and_preserve_strict_window_boundary(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _work(database, "pending-attempt")
        await _work(database, "active-attempt")
        rate = SlidingWindowRateConstraint(
            identifier=("test", "réservations"), scope=WORK, period_ns=10, maximum_starts=1
        )
        await database.register_constraint(rate, registered_at_utc_ns=0)
        await _rate(database, rate, "boundary", NOW - 10)
        candidates = (
            WorkConstraintCandidate("pending-attempt", True),
            WorkConstraintCandidate("active-attempt", False),
        )
        async with database._connections.writer() as connection:
            assert await eligible_work(connection, candidates, now_utc_ns=NOW) == {
                "pending-attempt",
                "active-attempt",
            }
        # Existing availability counts by constraint ID regardless of the
        # reservation's subject metadata, including a timestamp ahead of now.
        await _rate(
            database, rate, "future", NOW + 1, subject=SchedulingSubjectKind.NETWORK_ACTIVITY
        )
        async with database._connections.writer() as connection:
            assert await eligible_work(connection, candidates, now_utc_ns=NOW) == {"active-attempt"}


async def test_network_caps_use_unexpired_admissions_and_exact_scopes(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _operation(database)
        await _network(database, "expired", state="admitted", expiry=NOW)
        await _network(database, "live", state="admitted", expiry=NOW + 1)
        await _network(database, "completed", state="completed", expiry=None)
        await _network(database, "pending", state="pending", expiry=None)
        cap = ConcurrencyConstraint(
            identifier=("test", "network-cap"),
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=NETWORK,
            maximum_active=1,
        )
        await database.register_constraint(cap, registered_at_utc_ns=0)
        candidates = (
            _network_candidate("blocked"),
            _network_candidate(
                "other",
                scope=SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=("other",)),
            ),
            _network_candidate("future", not_before=NOW + 1),
        )
        async with database._connections.writer() as connection:
            assert await eligible_network(connection, candidates, now_utc_ns=NOW) == {"other"}
        await _retire(database, cap.identifier)
        async with database._connections.writer() as connection:
            assert await eligible_network(connection, candidates, now_utc_ns=NOW) == {
                "blocked",
                "other",
            }


async def test_network_windows_and_retirements_keep_independent_policy_histories(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _operation(database)
        short = SlidingWindowRateConstraint(
            identifier=("test", "short"),
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=NETWORK,
            maximum_starts=1,
            period_ns=10,
        )
        long = short.model_copy(
            update={"identifier": ("test", "long"), "period_ns": 20, "maximum_starts": 2}
        )
        for policy in (short, long):
            await database.register_constraint(policy, registered_at_utc_ns=0)
        await _rate(database, short, "short-boundary", NOW - 10)
        await _rate(database, long, "long-boundary", NOW - 20)
        await _rate(database, long, "long-live", NOW - 5)
        candidates = (_network_candidate("selected"),)
        async with database._connections.writer() as connection:
            assert await eligible_network(connection, candidates, now_utc_ns=NOW) == {"selected"}
        await _rate(database, short, "short-live", NOW - 9)
        async with database._connections.writer() as connection:
            assert await eligible_network(connection, candidates, now_utc_ns=NOW) == frozenset()
        await _retire(database, short.identifier)
        async with database._connections.writer() as connection:
            assert await eligible_network(connection, candidates, now_utc_ns=NOW) == {"selected"}
        await _rate(database, long, "long-future", NOW + 1, subject=SchedulingSubjectKind.WORK_ITEM)
        async with database._connections.writer() as connection:
            assert await eligible_network(connection, candidates, now_utc_ns=NOW) == frozenset()


async def test_network_holdoff_hash_uses_next_execution_and_stage_creation_time(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.register_constraint(
            UniformHoldoffConstraint(
                identifier=("test", "holdoff"),
                subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
                scope=NETWORK,
                minimum_ns=3,
                maximum_ns=11,
            ),
            registered_at_utc_ns=0,
        )
        # Golden samples for UTF-8 stage-ø with next executions 1, 2 and 4 are
        # 8, 10 and 7. The constraint identity is deliberately not in the hash.
        candidates = (
            _network_candidate("at-boundary", created=NOW - 8),
            _network_candidate("too-soon", created=NOW - 7),
            _network_candidate("second-execution", execution=1, created=NOW - 8),
            _network_candidate("fourth-execution", execution=3, created=NOW - 7),
        )
        async with database._connections.writer() as connection:
            assert await eligible_network(connection, candidates, now_utc_ns=NOW) == {
                "at-boundary",
                "fourth-execution",
            }


async def _measure(
    database: Database,
    action: Callable[[], Awaitable[frozenset[str]]],
) -> tuple[frozenset[str], int, tuple[str, ...]]:
    steps = 0
    statements: list[str] = []

    def progress() -> bool:
        nonlocal steps
        steps += 100
        return False

    def trace(_cursor: object, statement: str, _bindings: object) -> bool:
        upper = statement.lstrip().upper()
        assert upper.startswith(("SELECT", "SAVEPOINT", "RELEASE", "ROLLBACK")), statement
        if upper.startswith("SELECT"):
            statements.append(statement)
        return True

    async with database._connections.writer() as connection:
        await connection.set_progress_handler(progress, nsteps=100, id="fair-constraint-cost")
        connection.set_exec_trace(trace)
        try:
            result = await action()
        finally:
            connection.set_exec_trace(None)
            await connection.set_progress_handler(None, id="fair-constraint-cost")
    assert all(statement.lstrip().upper().startswith("SELECT") for statement in statements)
    return result, steps, tuple(statements)


@pytest.mark.parametrize("network", (False, True))
async def test_full_constraint_batch_uses_constant_queries_without_archive_scans(
    tmp_path: Path, network: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _operation(database)
        subject = (
            SchedulingSubjectKind.NETWORK_ACTIVITY if network else SchedulingSubjectKind.WORK_ITEM
        )
        scope = NETWORK if network else WORK
        await database.register_constraint(
            ConcurrencyConstraint(
                identifier=("test", "cap"), subject_kind=subject, scope=scope, maximum_active=2
            ),
            registered_at_utc_ns=0,
        )
        rate = SlidingWindowRateConstraint(
            identifier=("test", "rate"),
            subject_kind=subject,
            scope=scope,
            period_ns=10,
            maximum_starts=2,
        )
        await database.register_constraint(rate, registered_at_utc_ns=0)
        await _rate(database, rate, "live-start", NOW)
        await _work(database, "live-work", state="leased", expiry=NOW + 1)
        await _network(database, "live-network", state="admitted", expiry=NOW + 1)
        async with database._connections.writer() as connection:
            await connection.execute(
                """WITH RECURSIVE n(i) AS(SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<255)
                INSERT INTO work_items(id,kind_parts_json,payload_schema_version,payload_json,
                    deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns)
                SELECT 'candidate-'||i,'["test","work"]',1,'{}',json_array(i),'pending',0,0,0 FROM n"""
            )
            await connection.execute(
                """INSERT INTO work_scopes SELECT id,?,? FROM work_items WHERE id LIKE 'candidate-%'""",
                (WORK.kind.value, encode_json(list(WORK.identity))),
            )

        async def run(count: int) -> frozenset[str]:
            async with database._connections.writer() as connection:
                if network:
                    return await eligible_network(
                        connection,
                        tuple(_network_candidate(f"candidate-{i}") for i in range(count)),
                        now_utc_ns=NOW,
                    )
                return await eligible_work(
                    connection,
                    tuple(WorkConstraintCandidate(f"candidate-{i}", True) for i in range(count)),
                    now_utc_ns=NOW,
                )

        _, _, one_statements = await _measure(database, lambda: run(1))
        before, before_steps, before_statements = await _measure(database, lambda: run(256))
        async with database._connections.writer() as connection:
            await connection.execute(
                """WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10000)
                INSERT INTO work_items(id,kind_parts_json,payload_schema_version,payload_json,
                    deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns)
                SELECT 'archive-'||i,'["test","work"]',1,'{}',json_array('archive',i),'completed',0,0,0 FROM n"""
            )
            await connection.execute(
                """INSERT INTO work_scopes SELECT id,?,? FROM work_items WHERE id LIKE 'archive-%'""",
                (WORK.kind.value, encode_json(list(WORK.identity))),
            )
            await connection.execute(
                """WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10000)
                INSERT INTO network_activities(id,kind_parts_json,operation_id,network_session_identifier,
                    ordinal,attempt,state,eligible_at_utc_ns,created_at_utc_ns)
                SELECT 'archive-'||i,'["test","network"]','operation','session-'||i,1,1,'completed',0,0 FROM n"""
            )
            await connection.execute(
                """INSERT INTO network_activity_scopes SELECT id,?,? FROM network_activities WHERE id LIKE 'archive-%'""",
                (NETWORK.kind.value, encode_json(list(NETWORK.identity))),
            )
            await connection.execute(
                """WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10000)
                INSERT INTO rate_starts SELECT 'archive-'||i,?,?,'archive-'||i,'token-'||i,? FROM n""",
                (encode_json(list(rate.identifier)), subject.value, NOW - 10),
            )
        after, after_steps, after_statements = await _measure(database, lambda: run(256))
        assert before == after == frozenset(f"candidate-{i}" for i in range(256))
        assert len(one_statements) == len(before_statements) == len(after_statements) == 3
        assert after_steps <= before_steps + 5_000, (before_steps, after_steps)


async def test_empty_and_unconstrained_batches_need_no_availability_counters(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _work(database, "unconstrained")
        async with database._connections.writer() as connection:
            assert await eligible_work(connection, (), now_utc_ns=NOW) == frozenset()
            assert await eligible_network(connection, (), now_utc_ns=NOW) == frozenset()
            assert await eligible_work(
                connection, (WorkConstraintCandidate("unconstrained", True),), now_utc_ns=NOW
            ) == {"unconstrained"}
            assert await eligible_network(
                connection, (_network_candidate("unconstrained", not_before=NOW),), now_utc_ns=NOW
            ) == {"unconstrained"}
