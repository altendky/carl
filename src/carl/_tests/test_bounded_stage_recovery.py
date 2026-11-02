"""Expired executors are discovered in bounded pages before guarded point reads."""

# Tests inspect durable state and the actual cross-process guard boundary.
# pyright: reportPrivateUsage=false

import fcntl
import os
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import anyio
import pytest

import carl.io.stages as stages
from carl._tests.test_stage_execution import _Clock
from carl._tests.test_stage_storage import COMPONENT, NOW, _definition, _enqueue
from carl._tests.test_worker import _provenance
from carl.core.stages import StageLease
from carl.core.work_dispatch import ExpiredStagePage, ExpiryCursor
from carl.io.resource_permits import current_resource_manager
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _expired(
    database: Database,
    identifier: str,
    *,
    duration: int = 5,
    scope: str | None = None,
) -> StageLease:
    await _enqueue(database, identifier, payload={"retained": "x" * 100_000})
    definition = _definition(identifier, identifier=f"stage-{identifier}", resources=())
    await database.register_stage(definition, created_at_utc_ns=NOW)
    manager = current_resource_manager()
    assert manager is not None
    result = await database.try_claim_stage(
        stage_identifier=definition.identifier,
        token=str(uuid4()),
        worker_identifier="interrupted-worker",
        resource_scope=manager.scope if scope is None else scope,
        capacities=manager.capacities,
        lease_duration_ns=duration,
        now_utc_ns=NOW,
        component=COMPONENT,
        provenance=_provenance(),
        invocation={},
    )
    assert result.lease is not None
    return result.lease


async def _states(database: Database) -> dict[str, str]:
    async with database._connections.reader() as connection:
        cursor = await connection.execute("SELECT id, state FROM work_stages ORDER BY id")
        return {str(row[0]): str(row[1]) for row in await cursor.fetchall()}


async def test_busy_guard_advances_bounded_frontier_across_manager_instances(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stages, "_RECOVERY_PAGE_SIZE", 2)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        leases = [await _expired(database, f"{index:02}") for index in range(4)]
        clock = _Clock(NOW + 10)
        manager = replace(stages.stage_manager(database), utc_now_ns=clock)
        descriptor = manager.resources._guard(leases[0].token)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        point_reads: list[str] = []
        point_read = database.stage_execution_lease

        async def guarded_read(
            identifier: str,
            *,
            expected_token: str,
            expected_execution: int,
            coordination_scope: str | None = None,
        ) -> StageLease | None:
            assert identifier != leases[0].definition.identifier
            witness = manager.resources._guard(expected_token)
            try:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(witness, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(witness)
            point_reads.append(identifier)
            return await point_read(
                identifier,
                expected_token=expected_token,
                expected_execution=expected_execution,
                coordination_scope=coordination_scope,
            )

        async def no_full_scan(**_kwargs: object) -> tuple[StageLease, ...]:
            raise AssertionError("Recovery materialized all expired work payloads")

        monkeypatch.setattr(database, "stage_execution_lease", guarded_read)
        monkeypatch.setattr(database, "expired_stage_executions", no_full_scan)
        try:
            await manager.recover()
            assert point_reads == [leases[1].definition.identifier]
            assert list((await _states(database)).values()) == [
                "running",
                "pending",
                "running",
                "running",
            ]
            # Factory churn must not repeatedly return the busy first page.
            manager = replace(stages.stage_manager(database), utc_now_ns=clock)
            await manager.recover()
            assert point_reads == [lease.definition.identifier for lease in leases[1:]]
            assert list((await _states(database)).values()) == [
                "running",
                "pending",
                "pending",
                "pending",
            ]
        finally:
            os.close(descriptor)
        monkeypatch.setattr(database, "stage_execution_lease", point_read)
        await manager.recover()
        assert set((await _states(database)).values()) == {"pending"}
        for lease in leases:
            assert (await database.work(lease.definition.work_item_identifier))["attempt"] == 1


async def test_recovery_pass_uses_fixed_time_then_observes_newly_expired_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stages, "_RECOVERY_PAGE_SIZE", 1)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _ = await _expired(database, "00")
        _ = await _expired(database, "01")
        _ = await _expired(database, "02", duration=20)
        clock = _Clock(NOW + 10)
        manager = replace(stages.stage_manager(database), utc_now_ns=clock)
        await manager.recover()
        assert list((await _states(database)).values()) == ["pending", "running", "running"]
        clock.now = NOW + 30
        await manager.recover()
        assert list((await _states(database)).values()) == ["pending", "pending", "running"]
        await manager.recover()
        assert set((await _states(database)).values()) == {"pending"}


async def test_renewed_lease_and_foreign_scope_survive_stale_recovery_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        lease = await _expired(database, "local")
        _ = await _expired(database, "foreign", scope="other-host")
        manager = replace(stages.stage_manager(database), utc_now_ns=_Clock(NOW + 10))
        point_read = database.stage_execution_lease
        loaded: list[str] = []

        async def renewed_read(
            identifier: str,
            *,
            expected_token: str,
            expected_execution: int,
            coordination_scope: str | None = None,
        ) -> StageLease | None:
            loaded.append(identifier)
            renewed = await database.renew_stage(lease, now_utc_ns=NOW + 4, lease_duration_ns=1_000)
            assert renewed is not None
            return await point_read(
                identifier,
                expected_token=expected_token,
                expected_execution=expected_execution,
                coordination_scope=coordination_scope,
            )

        monkeypatch.setattr(database, "stage_execution_lease", renewed_read)
        await manager.recover()
        assert loaded == [lease.definition.identifier]
        assert set((await _states(database)).values()) == {"running"}


async def test_replaced_execution_requires_its_own_guard_and_fresh_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        lease = await _expired(database, "replaced")
        manager = replace(stages.stage_manager(database), utc_now_ns=_Clock(NOW + 12))
        point_read = database.stage_execution_lease
        replacement: StageLease | None = None

        async def replaced_read(
            identifier: str,
            *,
            expected_token: str,
            expected_execution: int,
            coordination_scope: str | None = None,
        ) -> StageLease | None:
            nonlocal replacement
            assert await database.recover_stage_execution(lease, now_utc_ns=NOW + 10)
            result = await database.try_claim_stage(
                stage_identifier=identifier,
                token=str(uuid4()),
                worker_identifier="replacement-worker",
                resource_scope=manager.resources.scope,
                capacities=manager.resources.capacities,
                lease_duration_ns=1,
                now_utc_ns=NOW + 10,
                component=COMPONENT,
                provenance=_provenance(),
                invocation={},
            )
            replacement = result.lease
            assert replacement is not None and replacement.execution == 2
            return await point_read(
                identifier,
                expected_token=expected_token,
                expected_execution=expected_execution,
                coordination_scope=coordination_scope,
            )

        monkeypatch.setattr(database, "stage_execution_lease", replaced_read)
        await manager.recover()
        assert replacement is not None
        assert set((await _states(database)).values()) == {"running"}
        current = await point_read(
            lease.definition.identifier,
            expected_token=replacement.token,
            expected_execution=2,
            coordination_scope=manager.resources.scope,
        )
        assert current == replacement
        monkeypatch.setattr(database, "stage_execution_lease", point_read)
        await manager.recover()
        assert set((await _states(database)).values()) == {"pending"}
        work = await database.work(lease.definition.work_item_identifier)
        assert work["attempt"] == 1 and len(work["operations"]) == 1


async def test_concurrent_recovery_does_not_wait_for_another_sweep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        manager = stages.stage_manager(database)
        entered, release, second_finished = anyio.Event(), anyio.Event(), anyio.Event()
        query = database.expired_stage_candidates
        scans = 0

        async def paused_page(
            *,
            coordination_scope: str,
            now_utc_ns: int,
            limit: int,
            after: ExpiryCursor | None = None,
        ) -> ExpiredStagePage:
            nonlocal scans
            scans += 1
            page = await query(
                coordination_scope=coordination_scope,
                now_utc_ns=now_utc_ns,
                limit=limit,
                after=after,
            )
            entered.set()
            await release.wait()
            return page

        async def second() -> None:
            await stages.stage_manager(database).recover()
            second_finished.set()

        monkeypatch.setattr(database, "expired_stage_candidates", paused_page)
        with anyio.fail_after(5):
            async with anyio.create_task_group() as tasks:
                try:
                    tasks.start_soon(manager.recover)
                    await entered.wait()
                    tasks.start_soon(second)
                    await second_finished.wait()
                    assert scans == 1
                finally:
                    release.set()
