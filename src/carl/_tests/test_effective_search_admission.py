"""Effective route metadata remains fenced without a numeric admission gate."""

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
from carl._tests import test_facebook_session_stages as resident
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
from carl.io.sqlite import Database, LeaseLostError, WorkCancellationRequestedError

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
        eligible_identifiers=(identifier,),
    )
    assert result.lease is not None
    assert result.lease.work_item_identifier == identifier
    return result.lease


async def _bind(
    database: Database, lease: WorkLease, routing: tuple[str, ...], now: int = 10
) -> None:
    await database.bind_facebook_search_route(
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
async def test_legacy_alias_and_direct_route_bind_without_a_capacity_policy(
    tmp_path: Path, second_route: tuple[str, ...]
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        first = await _claim(database, "first", ("proton", "personal", "legacy"))
        second = await _claim(database, "second", second_route)
        before = await _rows(database, "SELECT id,payload_json FROM work_items ORDER BY id")
        history = await _rows(database, "SELECT * FROM work_events ORDER BY sequence")
        await _bind(database, first, TARGET)
        await _bind(database, second, TARGET)
        await _bind(database, first, TARGET)  # Repeated binding is idempotent.
        assert await _rows(database, "SELECT id,payload_json FROM work_items ORDER BY id") == before
        assert await _rows(database, "SELECT * FROM work_events ORDER BY sequence") == history
        assert await _scopes(database, "first") == {
            ("proton", "personal", "legacy"),
            ("search_acquisition", "proton", "personal", "legacy"),
            (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *TARGET),
        }
        assert _external_scopes(path, "first") == await _scopes(database, "first")
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *TARGET) in await _scopes(
            database, "second"
        )
        assert await _rows(database, "SELECT count(*) FROM scheduling_constraints") == [(0,)]


@pytest.mark.anyio
@pytest.mark.parametrize("second_route", (("proton", "personal", "alias"), DATACENTER))
async def test_more_than_twenty_decodo_alias_and_direct_routes_bind(
    tmp_path: Path, second_route: tuple[str, ...]
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        for index in range(21):
            lease = await _claim(database, f"prefill-{index}", DATACENTER)
            await _bind(database, lease, DATACENTER)
        first = await _claim(database, "first", ("proton", "personal", "legacy"))
        second = await _claim(database, "second", second_route)
        before = await _rows(database, "SELECT id,payload_json FROM work_items ORDER BY id")
        history = await _rows(database, "SELECT * FROM work_events ORDER BY sequence")
        await _bind(database, first, DATACENTER)
        async with Database.managed(path) as other_database:
            await _bind(other_database, second, DATACENTER)
        await _bind(database, first, DATACENTER)
        assert await _rows(database, "SELECT id,payload_json FROM work_items ORDER BY id") == before
        assert await _rows(database, "SELECT * FROM work_events ORDER BY sequence") == history
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *DATACENTER) in await _scopes(
            database, "first"
        )
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *DATACENTER) in await _scopes(
            database, "second"
        )
        assert await _rows(
            database,
            "SELECT count(*) FROM scheduling_constraints WHERE identifier_parts_json LIKE '%effective_route_concurrency%'",
        ) == [(0,)]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "invalid", ("token", "owner", "expired", "reclaimed", "cancelled", "empty_route", "empty_part")
)
async def test_binding_rejects_invalid_ownership_or_route_without_changing_scopes(
    tmp_path: Path,
    invalid: str,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        lease = await _claim(database, "search", TARGET, duration=100)
        await _bind(database, lease, TARGET)
        scopes = await _scopes(database, "search")
        now = 10
        routing: tuple[str, ...] = OTHER
        error: type[Exception] = LeaseLostError
        if invalid == "token":
            lease = lease.model_copy(update={"token": "wrong"})
        elif invalid == "owner":
            lease = lease.model_copy(update={"worker_identifier": "wrong"})
        elif invalid == "expired":
            now = 110
        elif invalid == "reclaimed":
            now = 110
            replacement = await database.claim_work(
                supported_capabilities=(CAPABILITY,),
                worker_identifier="replacement-worker",
                lease_token="replacement-token",
                lease_duration_ns=100,
                utc_now_ns=lambda: now,
                event_identifier="replacement-claim",
                eligible_identifiers=(lease.work_item_identifier,),
            )
            assert replacement.lease is not None
        elif invalid == "cancelled":
            await database.cancel_work(
                "search", reason="stop", caller="test", requested_at_utc_ns=11
            )
            error = WorkCancellationRequestedError
        else:
            routing = () if invalid == "empty_route" else ("proton", "", "other")
            error = ValueError
        with pytest.raises(error):
            await _bind(database, lease, routing, now=now)
        assert await _scopes(database, "search") == scopes


@pytest.mark.anyio
async def test_independent_connection_observes_bound_scope(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        first = await _claim(database, "first", ("proton", "personal", "first"))
        second = await _claim(database, "second", ("proton", "personal", "second"))
        await _bind(database, first, TARGET)
        async with Database.managed(path) as other_database:
            await _bind(other_database, second, TARGET)
            assert _external_scopes(path, "second") == await _scopes(other_database, "second")


@pytest.mark.anyio
async def test_simultaneous_processes_bind_the_same_route_without_installing_a_cap(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        # Distinct requested aliases resolve to one effective acquisition route.
        first = await _claim(database, "first", ("proton", "personal", "first"))
        second = await _claim(database, "second", ("proton", "personal", "second"))
    code = """
import sys
from pathlib import Path
import anyio
from carl.core.work import WorkLease
from carl.io.sqlite import Database
from carl._tests.test_effective_search_admission import _bind, TARGET
async def main():
    async with Database.managed(Path(sys.argv[1])) as database:
        await _bind(database, WorkLease.model_validate_json(sys.argv[2]), TARGET)
        print('bound')
anyio.run(main, backend="trio")
"""
    results: list[bool] = []

    async def bind(lease: WorkLease) -> None:
        process = await anyio.run_process(
            (sys.executable, "-c", code, str(path), lease.model_dump_json())
        )
        results.append(process.stdout.strip() == b"bound")

    async with anyio.create_task_group() as group:
        _ = group.start_soon(bind, first)
        _ = group.start_soon(bind, second)
    assert results == [True, True]
    async with Database.managed(path) as database:
        for lease in (first, second):
            assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *TARGET) in await _scopes(
                database, lease.work_item_identifier
            )
        assert await _rows(database, "SELECT count(*) FROM scheduling_constraints") == [(0,)]


@pytest.mark.anyio
async def test_renewed_lease_can_rebind_and_released_lease_cannot(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        first = await _claim(database, "first", ("proton", "personal", "one"))
        second = await _claim(database, "second", ("proton", "personal", "two"))
        await _bind(database, first, TARGET)
        for lease in (first, second):
            await database.renew_lease(
                work_item_identifier=lease.work_item_identifier,
                lease_token=lease.token,
                worker_identifier=lease.worker_identifier,
                lease_duration_ns=10_000,
                utc_now_ns=lambda: 900,
                event_identifier=f"renew-{lease.work_item_identifier}",
            )
        await _bind(database, first, OTHER, now=2_000)
        await _bind(database, second, OTHER, now=2_000)
        await database.release_lease(
            work_item_identifier=first.work_item_identifier,
            lease_token=first.token,
            worker_identifier=first.worker_identifier,
            utc_now_ns=lambda: 2_001,
            eligible_at_utc_ns=2_001,
            reason={"kind": "test_release"},
            event_identifier="release-first",
        )
        await _bind(database, second, TARGET, now=2_001)
        with pytest.raises(LeaseLostError):
            await _bind(database, first, TARGET, now=2_001)


@pytest.mark.anyio
async def test_rebinding_removes_only_obsolete_effective_scope(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        first = await _claim(database, "first", ("proton", "personal", "one"))
        second = await _claim(database, "second", ("proton", "personal", "two"))
        await _bind(database, first, TARGET)
        await _bind(database, second, OTHER)
        await _bind(database, first, OTHER)
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *TARGET) not in await _scopes(
            database, "first"
        )
        assert ("search_acquisition", "proton", "personal", "one") in await _scopes(
            database, "first"
        )
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *OTHER) in await _scopes(database, "first")
        await _bind(database, second, TARGET)


@pytest.mark.anyio
async def test_cancelled_routed_work_does_not_access_provider_or_consume_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = time_ns()
    path = tmp_path / "carl.sqlite3"
    loaded = routes._loaded(tmp_path, override=False)

    def load(_path: Path) -> LoadedCarlConfiguration:
        return loaded

    monkeypatch.setattr(routed, "load_configuration", load)

    def unexpected(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Lease validation must precede provider access")

    monkeypatch.setattr(routed, "decodo_settings", unexpected)
    monkeypatch.setattr(routed, "proton_settings", unexpected)
    monkeypatch.setattr(routed, "brave_navigation_headers", unexpected)
    async with Database.managed(path, initialize=True) as database:
        second = await _claim(database, "second", routes.PROTON, now=now, duration=60_000_000_000)
        await database.cancel_work("second", reason="stop", caller="test", requested_at_utc_ns=now)
        registry = routed.build_routed_facebook_worker_registry(
            database=database,
            directories=_directories(tmp_path),
            new_identifier=workers._identifiers(),
        )
        handler = registry.require(second)
        with pytest.raises(WorkCancellationRequestedError):
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
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *TARGET) not in await _scopes(
            database, "second"
        )
        row = (
            await _rows(database, "SELECT attempt,payload_json FROM work_items WHERE id='second'")
        )[0]
        assert row[0] == 1
        assert json.loads(str(row[1]))["routing"] == list(routes.PROTON)
        assert await _rows(database, "SELECT count(*) FROM network_activities") == [(0,)]


@pytest.mark.anyio
async def test_resident_search_advances_past_another_lease_on_the_same_effective_route(
    tmp_path: Path,
) -> None:
    clock, sessions = resident._Clock(), resident._Sessions()
    route = ("proton", "fixture", "search")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        blocker = await _claim(
            database, "other-search", route, now=clock(), duration=120_000_000_000
        )
        await _bind(database, blocker, route, now=clock())
        pool = resident._pool(database, clock)
        pool.registry = resident._registry(database, tmp_path, pool, sessions, clock)
        stage = await resident._seed(database, pool.registry)
        async with pool.managed():
            await resident._submit(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        assert work["result"]["pages_processed"] == 2 and work["result"]["unique_listings"] == 4
        assert (await database.operation(stage.operation_identifier))["state"] == "completed"
        assert [request[1] for request in sessions.requests] == ["bootstrap", "0", "90"]
        assert len(sessions.sessions) == 1 and sessions.sessions[0].closed
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *route) in await _scopes(
            database, stage.work_item_identifier
        )
        assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *route) in await _scopes(
            database, blocker.work_item_identifier
        )
        rows = await _rows(
            database,
            "SELECT definition_json FROM work_stages WHERE work_item_id=?",
            (stage.work_item_identifier,),
        )
        route_stages = [
            json.loads(str(row[0]))
            for row in rows
            if json.loads(str(row[0]))["kind"] == ["carl", "facebook", "stage", "search", "route"]
        ]
        assert len(route_stages) == 1
        assert route_stages[0]["not_before_utc_ns"] == 0
