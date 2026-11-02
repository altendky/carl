"""Atomic, scoped resource accounting and durable owner fencing."""

# Schema fixture and reservation inspection deliberately exercise private storage boundaries.
# pyright: reportPrivateUsage=false

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import anyio
import pytest
from pydantic import ValidationError

from carl.core.resources import ResourceLease, ResourceRequest
from carl.io.sqlite import _SCHEMA, DATABASE_SCHEMA, Database


async def _acquire(
    database: Database,
    token: str,
    *,
    scope: str = "test-host",
    owner: str = "owner",
    requests: tuple[ResourceRequest, ...] = (ResourceRequest(name="http"),),
    capacities: dict[str, int] | None = None,
    now: int = 10,
    duration: int = 10,
) -> ResourceLease | None:
    return await database.try_acquire_resource_permit(
        scope=scope,
        token=token,
        owner=owner,
        requests=requests,
        capacities={"http": 1, "cpu": 1} if capacities is None else capacities,
        now_utc_ns=now,
        lease_duration_ns=duration,
    )


@pytest.mark.anyio
async def test_multiple_resource_reservations_are_atomic_and_scoped(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        assert await _acquire(database, "cpu-owner", requests=(ResourceRequest(name="cpu"),))
        assert (
            await _acquire(
                database,
                "blocked",
                requests=(ResourceRequest(name="http"), ResourceRequest(name="cpu")),
            )
            is None
        )
        # The unavailable CPU budget did not consume the otherwise-free HTTP budget.
        assert await _acquire(database, "http-owner")
        assert await _acquire(database, "other-host", scope="another-host")
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT count(*) FROM resource_allocations WHERE permit_token = 'blocked'"
            )
            assert await cursor.fetchone() == (0,)
            cursor = await connection.execute(
                "SELECT count(*) FROM resource_permits WHERE token = 'blocked'"
            )
            assert await cursor.fetchone() == (0,)
        assert await database.release_resource_permit(token="cpu-owner", owner="owner")
        assert await database.release_resource_permit(token="http-owner", owner="owner")
        lease = await _acquire(
            database,
            "both",
            requests=(ResourceRequest(name="http"), ResourceRequest(name="cpu")),
        )
        assert lease is not None
        assert tuple(request.name for request in lease.allocations) == ("cpu", "http")


@pytest.mark.anyio
async def test_expired_permits_keep_capacity_until_guarded_reclamation(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        original = await _acquire(database, "old", owner="old-owner")
        assert original is not None
        assert not await database.validate_resource_permit(
            token="old", owner="old-owner", now_utc_ns=20
        )
        assert (
            await database.renew_resource_permit(
                token="old", owner="old-owner", now_utc_ns=20, lease_duration_ns=10
            )
            is None
        )
        assert await database.expired_resource_permits(scope="test-host", now_utc_ns=20) == (
            original,
        )
        assert await _acquire(database, "replacement", now=20) is None
        assert not await database.reclaim_expired_resource_permit(
            token="old", owner="wrong-owner", now_utc_ns=20
        )
        assert not await database.reclaim_expired_resource_permit(
            token="old", owner="old-owner", now_utc_ns=19
        )
        # This synthetic owner has no live executor. The manager tests exercise its OS guard.
        assert await database.reclaim_expired_resource_permit(
            token="old", owner="old-owner", now_utc_ns=20
        )
        replacement = await _acquire(database, "replacement", owner="new-owner", now=20)
        assert replacement is not None
        assert not await database.release_resource_permit(token="old", owner="old-owner")
        assert (
            await database.renew_resource_permit(
                token="old", owner="old-owner", now_utc_ns=21, lease_duration_ns=100
            )
            is None
        )
        assert await database.validate_resource_permit(
            token="replacement", owner="new-owner", now_utc_ns=21
        )
        with pytest.raises(ValueError, match="tokens may never be reused"):
            await _acquire(database, "old", owner="new-owner", now=21)


@pytest.mark.anyio
async def test_renewal_release_and_publication_validation_are_owner_fenced(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        assert await _acquire(database, "active")
        assert (
            await database.renew_resource_permit(
                token="active", owner="stranger", now_utc_ns=15, lease_duration_ns=100
            )
            is None
        )
        renewed = await database.renew_resource_permit(
            token="active", owner="owner", now_utc_ns=15, lease_duration_ns=100
        )
        assert renewed is not None
        assert renewed.expires_at_utc_ns == 115
        assert not await database.release_resource_permit(token="active", owner="stranger")
        async with database.transaction():
            assert await database.validate_resource_permit(
                token="active", owner="owner", now_utc_ns=100
            )
            assert not await database.validate_resource_permit(
                token="active", owner="stranger", now_utc_ns=100
            )
        # An expired owner may release its own completed execution, but cannot publish or renew.
        assert not await database.validate_resource_permit(
            token="active", owner="owner", now_utc_ns=115
        )
        assert await database.release_resource_permit(token="active", owner="owner")
        assert not await database.release_resource_permit(token="active", owner="owner")
        assert await _acquire(database, "next", now=115)


@pytest.mark.anyio
async def test_capacity_configuration_changes_require_an_unoccupied_scope(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        assert await _acquire(database, "active")
        with pytest.raises(ValueError, match="capacities cannot change"):
            await _acquire(database, "changed", capacities={"http": 2, "cpu": 1}, now=30)
        assert await database.release_resource_permit(token="active", owner="owner")
        assert await _acquire(
            database,
            "changed",
            capacities={"http": 2, "cpu": 1},
            requests=(ResourceRequest(name="http", units=2),),
            now=30,
        )


@pytest.mark.anyio
async def test_invalid_multi_budget_requests_do_not_register_partial_allocations(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for requests in (
            (ResourceRequest(name="unknown"),),
            (ResourceRequest(name="http", units=2),),
            (ResourceRequest(name="http"), ResourceRequest(name="http")),
            (),
        ):
            with pytest.raises(ValueError):
                await _acquire(database, "invalid", requests=requests)
        with pytest.raises(ValidationError):
            ResourceRequest(name="http", units=True)
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT count(*) FROM resource_permits WHERE scope = 'test-host'"
            )
            assert await cursor.fetchone() == (0,)


@pytest.mark.anyio
async def test_independent_processes_share_atomic_multi_budget_capacity(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True):
        pass
    child = """
import json, sys
from pathlib import Path
import anyio
from carl.core.resources import ResourceRequest
from carl.io.sqlite import Database
async def main():
    async with Database.managed(Path(sys.argv[1])) as database:
        permit = await database.try_acquire_resource_permit(
            scope="cross-process-host", token=sys.argv[2], owner=sys.argv[2],
            requests=(ResourceRequest(name="http"), ResourceRequest(name="cpu")),
            capacities={"http": 4, "cpu": 2}, now_utc_ns=100, lease_duration_ns=1000,
        )
        print(json.dumps(permit is not None))
anyio.run(main, backend="trio")
"""
    admitted: list[bool] = []

    async def contender(index: int) -> None:
        result = await anyio.run_process((sys.executable, "-c", child, str(path), f"child-{index}"))
        admitted.append(json.loads(result.stdout))

    with anyio.fail_after(30):
        async with anyio.create_task_group() as group:
            for index in range(8):
                group.start_soon(contender, index)
    assert sum(admitted) == 2
    async with Database.managed(path) as database, database._connections.reader() as connection:
        cursor = await connection.execute(
            """
                SELECT name, SUM(units) FROM resource_allocations
                WHERE scope = 'cross-process-host' GROUP BY name ORDER BY name
                """
        )
        assert await cursor.fetchall() == [("cpu", 2), ("http", 2)]


@pytest.mark.anyio
async def test_version_13_migration_adds_resource_ledger_without_changing_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy.sqlite3"
    content = b"retained evidence"
    digest = hashlib.sha256(content).hexdigest()
    # Resource tables are the only schema addition relative to the exact known v13 identity.
    legacy_schema = _SCHEMA[_SCHEMA.index("CREATE TABLE IF NOT EXISTS network_connectivity (") :]
    with sqlite3.connect(path) as connection:
        connection.executescript(legacy_schema)
        connection.executemany(
            "INSERT INTO schema_metadata(key, value) VALUES (?, ?)",
            Database._v13_metadata().items(),
        )
        connection.execute(
            "INSERT INTO content VALUES (?, ?, 'sqlite', ?, NULL)",
            (digest, len(content), content),
        )
    for _ in range(2):
        async with Database.managed(path) as database:
            await database.validate_schema()
            async with database._connections.reader() as connection:
                cursor = await connection.execute(
                    "SELECT inline_bytes FROM content WHERE sha256 = ?", (digest,)
                )
                assert await cursor.fetchone() == (content,)
                cursor = await connection.execute(
                    "SELECT value FROM schema_metadata WHERE key = 'schema_version'"
                )
                assert await cursor.fetchone() == (str(DATABASE_SCHEMA.version),)
    async with Database.managed(path) as database:
        assert await _acquire(database, "after-migration")
