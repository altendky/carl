"""Effective search routes share renewable work admission across SQLite connections."""

from __future__ import annotations

# Reuse offline payload and route fixtures.
# pyright: reportPrivateUsage=false
import json
import sys
from pathlib import Path
from time import time_ns

import anyio
import apsw
import pytest

import carl.facebook_routed_workers as routed
from carl._tests import test_datacenter_routing as routes
from carl._tests import test_ebay_item_workers as workers
from carl._tests.test_configuration import _directories
from carl._tests.test_facebook_work import _payload
from carl.core.facebook_work import (
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_SEARCH_WORK_KIND,
    FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY,
    collect_search_work,
)
from carl.core.work import WorkCapability, WorkLease, WorkRequester
from carl.core.worker import AttemptContext
from carl.io.configuration import LoadedCarlConfiguration
from carl.io.sqlite import Database, LeaseLostError

TARGET = ("proton", "personal", "carl")
OTHER = ("proton", "personal", "other")
DATACENTER = ("decodo", "personal", "datacenter")
CAPABILITY = WorkCapability(
    kind=COLLECT_SEARCH_WORK_KIND, payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION
)


async def _claim(
    database: Database,
    identifier: str,
    routing: tuple[str, ...],
    *,
    now: int = 10,
    duration: int = 1_000,
) -> WorkLease:
    payload = _payload().model_copy(
        update={
            "routing": routing,
            "request": _payload().request.model_copy(update={"query": identifier}),
        }
    )
    _ = await database.enqueue_work(
        collect_search_work(identifier=identifier, payload=payload, not_before_utc_ns=0),
        WorkRequester(
            request_identifier=f"request-{identifier}",
            kind=("test", "effective_search"),
            identifier=identifier,
            context={},
        ),
        event_identifier=f"enqueue-{identifier}",
        enqueued_at_utc_ns=now,
    )
    result = await database.claim_work(
        supported_capabilities=(CAPABILITY,),
        worker_identifier=f"worker-{identifier}",
        lease_token=f"lease-{identifier}",
        lease_duration_ns=duration,
        utc_now_ns=lambda: now,
        event_identifier=f"claim-{identifier}",
    )
    assert result.lease is not None
    assert result.lease.work_item_identifier == identifier
    return result.lease


async def _admit(
    database: Database, lease: WorkLease, routing: tuple[str, ...], now: int = 10
) -> bool:
    return await database.try_admit_facebook_search_route(
        work_item_identifier=lease.work_item_identifier,
        lease_token=lease.token,
        worker_identifier=lease.worker_identifier,
        routing=routing,
        utc_now_ns=lambda: now,
    )


async def _rows(
    database: Database, sql: str, bindings: tuple[str, ...] = ()
) -> list[tuple[apsw.SQLiteValue, ...]]:
    async with database._connections.reader() as connection:
        cursor = await connection.execute(sql, bindings)
        return await cursor.fetchall()


async def _scopes(database: Database, identifier: str) -> set[tuple[str, ...]]:
    rows = await _rows(
        database,
        "SELECT scope_identity_json FROM work_scopes WHERE work_item_id=? AND scope_kind='network_path'",
        (identifier,),
    )
    return {tuple(json.loads(str(row[0]))) for row in rows}


def _external_scopes(path: Path, identifier: str) -> set[tuple[str, ...]]:
    connection = apsw.Connection(str(path), flags=apsw.SQLITE_OPEN_READONLY)
    try:
        rows = connection.execute(
            "SELECT scope_identity_json FROM work_scopes WHERE work_item_id=? AND scope_kind='network_path'",
            (identifier,),
        ).fetchall()
        return {tuple(json.loads(str(row[0]))) for row in rows}
    finally:
        connection.close()


@pytest.mark.anyio
@pytest.mark.parametrize("second_route", (("proton", "personal", "alias"), TARGET))
async def test_legacy_alias_and_direct_route_share_admission(
    tmp_path: Path, second_route: tuple[str, ...]
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        first = await _claim(database, "first", ("proton", "personal", "legacy"))
        second = await _claim(database, "second", second_route)
        before = await _rows(database, "SELECT id,payload_json FROM work_items ORDER BY id")
        history = await _rows(database, "SELECT * FROM work_events ORDER BY sequence")
        assert await _admit(database, first, TARGET)
        assert not await _admit(database, second, TARGET)
        assert await _admit(database, first, TARGET)  # Idempotent admission excludes itself.
        assert await _admit(database, second, OTHER)
        assert await _rows(database, "SELECT id,payload_json FROM work_items ORDER BY id") == before
        assert await _rows(database, "SELECT * FROM work_events ORDER BY sequence") == history
        assert await _scopes(database, "first") == {
            ("proton", "personal", "legacy"),
            ("search_acquisition", "proton", "personal", "legacy"),
            (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *TARGET),
        }
        assert _external_scopes(path, "first") == await _scopes(database, "first")


@pytest.mark.anyio
@pytest.mark.parametrize("second_route", (("proton", "personal", "alias"), DATACENTER))
async def test_decodo_alias_and_direct_route_admit_concurrent_searches(
    tmp_path: Path, second_route: tuple[str, ...]
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        first = await _claim(database, "first", ("proton", "personal", "legacy"))
        second = await _claim(database, "second", second_route)
        before = await _rows(database, "SELECT id,payload_json FROM work_items ORDER BY id")
        history = await _rows(database, "SELECT * FROM work_events ORDER BY sequence")
        assert await _admit(database, first, DATACENTER)
        async with Database.managed(path) as other_database:
            assert await _admit(other_database, second, DATACENTER)
        assert await _admit(database, first, DATACENTER)
        assert await _rows(database, "SELECT id,payload_json FROM work_items ORDER BY id") == before
        assert await _rows(database, "SELECT * FROM work_events ORDER BY sequence") == history
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *DATACENTER) in await _scopes(
            database, "first"
        )
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *DATACENTER) in await _scopes(
            database, "second"
        )
        assert await _rows(database, "SELECT count(*) FROM scheduling_constraints") == [(0,)]


@pytest.mark.anyio
async def test_decodo_admission_validates_lease_and_rebinding_preserves_proton_exclusion(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        first = await _claim(database, "first", TARGET)
        second = await _claim(database, "second", DATACENTER)
        expired = await _claim(database, "expired", DATACENTER, duration=100)
        assert await _admit(database, first, TARGET)
        assert await _admit(database, second, DATACENTER)
        scopes = await _scopes(database, "second")
        with pytest.raises(LeaseLostError):
            _ = await _admit(database, second.model_copy(update={"token": "wrong"}), OTHER)
        assert await _scopes(database, "second") == scopes
        with pytest.raises(LeaseLostError):
            _ = await _admit(database, expired, DATACENTER, now=110)
        assert not await _admit(database, second, TARGET)
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *DATACENTER) not in await _scopes(
            database, "second"
        )
        assert await _admit(database, second, DATACENTER)
        assert await _admit(database, first, DATACENTER)
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *TARGET) not in await _scopes(
            database, "first"
        )
        assert await _admit(database, second, TARGET)
        assert not await _admit(database, first, TARGET)


@pytest.mark.anyio
async def test_independent_connection_observes_admitted_scope(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        first = await _claim(database, "first", ("proton", "personal", "first"))
        second = await _claim(database, "second", ("proton", "personal", "second"))
        assert await _admit(database, first, TARGET)
        async with Database.managed(path) as other_database:
            assert not await _admit(other_database, second, TARGET)
            assert await _admit(other_database, second, OTHER)
            assert _external_scopes(path, "second") == await _scopes(other_database, "second")


@pytest.mark.anyio
async def test_simultaneous_processes_admit_only_one_before_policy_registration(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        # Same original route, with both leases claimed before any route constraint exists.
        first = await _claim(database, "first", TARGET)
        second = await _claim(database, "second", TARGET)
    code = """
import sys
from pathlib import Path
import anyio
from carl.core.work import WorkLease
from carl.io.sqlite import Database
from carl._tests.test_effective_search_admission import _admit, TARGET
async def main():
    async with Database.managed(Path(sys.argv[1])) as database:
        print(await _admit(database, WorkLease.model_validate_json(sys.argv[2]), TARGET))
anyio.run(main, backend="trio")
"""
    results: list[bool] = []

    async def admit(lease: WorkLease) -> None:
        process = await anyio.run_process(
            (sys.executable, "-c", code, str(path), lease.model_dump_json())
        )
        results.append(process.stdout.strip() == b"True")

    async with anyio.create_task_group() as group:
        _ = group.start_soon(admit, first)
        _ = group.start_soon(admit, second)
    assert sorted(results) == [False, True]


@pytest.mark.anyio
async def test_renewal_keeps_route_blocked_until_release(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        first = await _claim(database, "first", ("proton", "personal", "one"))
        second = await _claim(database, "second", ("proton", "personal", "two"))
        assert await _admit(database, first, TARGET)
        for lease in (first, second):
            await database.renew_lease(
                work_item_identifier=lease.work_item_identifier,
                lease_token=lease.token,
                worker_identifier=lease.worker_identifier,
                lease_duration_ns=10_000,
                utc_now_ns=lambda: 900,
                event_identifier=f"renew-{lease.work_item_identifier}",
            )
        assert not await _admit(database, second, TARGET, now=2_000)
        await database.release_lease(
            work_item_identifier=first.work_item_identifier,
            lease_token=first.token,
            worker_identifier=first.worker_identifier,
            utc_now_ns=lambda: 2_001,
            eligible_at_utc_ns=2_001,
            reason={"kind": "test_release"},
            event_identifier="release-first",
        )
        assert await _admit(database, second, TARGET, now=2_001)


@pytest.mark.anyio
async def test_expired_owner_releases_capacity_and_expired_caller_is_rejected(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        first = await _claim(database, "first", ("proton", "personal", "one"), duration=100)
        second = await _claim(database, "second", ("proton", "personal", "two"))
        assert await _admit(database, first, TARGET)
        assert await _admit(database, second, TARGET, now=110)
        with pytest.raises(LeaseLostError):
            _ = await _admit(database, first, OTHER, now=110)
        with pytest.raises(LeaseLostError):
            _ = await _admit(database, second.model_copy(update={"token": "wrong"}), OTHER, now=110)


@pytest.mark.anyio
@pytest.mark.parametrize("blocked", (False, True))
async def test_rebinding_removes_only_obsolete_effective_scope(
    tmp_path: Path, blocked: bool
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        first = await _claim(database, "first", ("proton", "personal", "one"))
        second = await _claim(database, "second", ("proton", "personal", "two"))
        assert await _admit(database, first, TARGET)
        if blocked:
            assert await _admit(database, second, OTHER)
        assert await _admit(database, first, OTHER) is not blocked
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *TARGET) not in await _scopes(
            database, "first"
        )
        assert ("search_acquisition", "proton", "personal", "one") in await _scopes(
            database, "first"
        )
        assert await _admit(database, second, TARGET)


@pytest.mark.anyio
async def test_cancelled_routed_wait_does_not_access_provider_or_consume_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = time_ns()
    path = tmp_path / "carl.sqlite3"
    loaded = routes._loaded(tmp_path, override=False)

    def load(_path: Path) -> LoadedCarlConfiguration:
        return loaded

    monkeypatch.setattr(routed, "load_configuration", load)

    def unexpected(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Waiting for effective admission must precede provider access")

    monkeypatch.setattr(routed, "decodo_settings", unexpected)
    monkeypatch.setattr(routed, "proton_settings", unexpected)
    monkeypatch.setattr(routed, "brave_navigation_headers", unexpected)
    async with Database.managed(path, initialize=True) as database:
        first = await _claim(database, "first", routes.PROTON, now=now, duration=60_000_000_000)
        second = await _claim(database, "second", routes.PROTON, now=now, duration=60_000_000_000)
        assert await _admit(database, first, TARGET, now=now)
        registry = routed.build_routed_facebook_worker_registry(
            database=database,
            directories=_directories(tmp_path),
            new_identifier=workers._identifiers(),
        )
        handler = registry.require(second)
        with anyio.move_on_after(0.03) as cancelled:
            _ = await handler.execute(
                second.payload,
                AttemptContext(
                    work_item_identifier=second.work_item_identifier,
                    lease_token=second.token,
                    worker_identifier=second.worker_identifier,
                    attempt=second.attempt,
                    operation_identifier="never-dispatched",
                ),
            )
        assert cancelled.cancel_called
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *TARGET) not in await _scopes(
            database, "second"
        )
        row = (
            await _rows(database, "SELECT attempt,payload_json FROM work_items WHERE id='second'")
        )[0]
        assert row[0] == 1
        assert json.loads(str(row[1]))["routing"] == list(routes.PROTON)
        assert await _rows(database, "SELECT count(*) FROM network_activities") == [(0,)]
