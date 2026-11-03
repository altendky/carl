"""Real marketplace dispatch removes policy waits while retaining paid bounds."""

from __future__ import annotations

# Fixture providers are offline; inspect only retained ownership/count metadata.
# pyright: reportPrivateUsage=false
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from time import time_ns
from uuid import uuid4

import anyio
import pytest

from carl import work_runtime
from carl._tests.test_ebay_session_stages import _html
from carl._tests.test_ebay_session_stages import _Sessions as EbaySessions
from carl._tests.test_ebay_workers import _directories
from carl._tests.test_facebook_session_stages import _Sessions as FacebookSessions
from carl._tests.test_facebook_work import _payload
from carl._tests.test_worker import _provenance
from carl.core.configuration import EbayItemConfiguration, EbaySearchConfiguration
from carl.core.ebay import CollectEbaySearchPayload, EbaySearchRequest, collect_ebay_search_work
from carl.core.facebook_search import OverlappingPricePartitionSearchTraversalStrategy
from carl.core.facebook_work import collect_search_work
from carl.core.models import Header, JsonValue
from carl.core.stages import StageAttemptContext, StageDefinition, StageWaitKind, WaitWork
from carl.core.work import WorkCapability, WorkDefinition, WorkRequester
from carl.ebay_session_stages import (
    EbaySessionStageDependencies,
    SessionPhasePayload,
    build_ebay_session_stage_registry,
)
from carl.facebook_session_stages import FacebookSessionStageDependencies
from carl.io.proton import SharedProtonWireproxyManager
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.routing_cache import RoutingCache
from carl.io.sqlite import Database
from carl.io.stage_publication import StageHandlerResult
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler
from carl.io.stages import stage_manager
from carl.session_stages import build_session_stage_registry


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture(autouse=True)
def no_live_network(monkeypatch: pytest.MonkeyPatch) -> None:
    async def forbidden(*_arguments: object, **_keywords: object) -> None:
        raise AssertionError("Temporal-policy tests must never connect to a live provider")

    monkeypatch.setattr(anyio, "connect_tcp", forbidden)


async def _healthy() -> dict[str, JsonValue]:
    return {"reachable": True, "checks": [{"fixture": True, "reachable": True}]}


def _composition(root: Path, now: int | None = None) -> work_runtime.RuntimeComposition:
    return work_runtime.RuntimeComposition(
        directories=_directories(root),
        connectivity_probe=_healthy,
        scheduling_utc_now_ns=time_ns if now is None else lambda: now,
    )


def _install(
    monkeypatch: pytest.MonkeyPatch,
    *,
    ebay: EbaySessions | None = None,
    facebook: FacebookSessions | None = None,
    policy: EbaySearchConfiguration | None = None,
    now: int | None = None,
) -> None:
    original = build_session_stage_registry

    def build(
        *,
        facebook: FacebookSessionStageDependencies,
        ebay: EbaySessionStageDependencies,
    ) -> StageHandlerRegistry:
        return original(
            facebook=facebook
            if facebook_sessions is None
            else replace(
                facebook,
                session_factory=facebook_sessions,
                navigation_headers=(Header(name=b"User-Agent", value=b"fixture-browser"),),
                utc_now_ns=time_ns if now is None else lambda: now,
            ),
            ebay=ebay
            if ebay_sessions is None
            else replace(
                ebay,
                session_factory=ebay_sessions,
                search_policy=policy,
                item_policy=EbayItemConfiguration(),
                fetch_pagination=False,
                utc_now_ns=time_ns if now is None else lambda: now,
            ),
        )

    facebook_sessions, ebay_sessions = facebook, ebay
    monkeypatch.setattr(work_runtime, "build_session_stage_registry", build)


async def _enqueue(
    database: Database, definition: WorkDefinition, *, now: int | None = None
) -> None:
    _ = await database.enqueue_work(
        definition,
        WorkRequester(
            request_identifier=str(uuid4()),
            kind=("test", "temporal_cutover"),
            identifier=definition.identifier,
            context={},
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns() if now is None else now,
    )


def _ebay(identifier: str = "search", *, not_before: int = 0) -> WorkDefinition:
    return collect_ebay_search_work(
        identifier=identifier,
        not_before_utc_ns=not_before,
        payload=CollectEbaySearchPayload(
            request=EbaySearchRequest(query=identifier, maximum_pages=1)
        ),
    )


async def _terminal(database: Database, identifier: str) -> dict[str, JsonValue]:
    with anyio.fail_after(15):
        while True:
            row = (await database.work_progress((identifier,)))[0]
            if row["state"] not in {"pending", "leased", "cancelling"}:
                return await database.work(identifier)
            await anyio.sleep(0.01)


async def _charges(database: Database, identifier: str) -> list[tuple[str, int, int]]:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                """SELECT c.request_identity_json,c.ordinal,c.maximum_attempts FROM resident_request_charges c
            JOIN work_operations o ON o.operation_id=c.operation_id WHERE o.work_item_id=?
            ORDER BY c.request_identity_json,c.ordinal""",
                (identifier,),
            )
        ).fetchall()
    result: list[tuple[str, int, int]] = []
    for row in rows:
        assert isinstance(row[1], int) and isinstance(row[2], int)
        result.append((str(row[0]), row[1], row[2]))
    return result


@pytest.mark.anyio
@pytest.mark.parametrize("page_attempts", (7, 9))
async def test_same_session_retries_ignore_nonzero_holdoff_without_tightening_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    page_attempts: int,
) -> None:
    now = time_ns()
    sessions = EbaySessions(
        [
            (b"blocked", 403),
            (b"home", 200),
            *[(b"blocked", 403)] * (page_attempts - 1),
            (_html(), 200),
        ]
    )
    policy = EbaySearchConfiguration(
        maximum_homepage_attempts=2,
        maximum_page_attempts=page_attempts,
        minimum_holdoff_seconds=60.0,
        maximum_holdoff_seconds=60.0,
    )
    _install(monkeypatch, ebay=sessions, policy=policy, now=now)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"http": 1, "cpu": 1}
    ) as database:
        definition = _ebay()
        await _enqueue(database, definition, now=now)
        async with work_runtime.managed_worker_pool(
            database,
            tmp_path,
            composition=_composition(tmp_path, now),
            eligible_identifiers=(definition.identifier,),
        ):
            work = await _terminal(database, definition.identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        assert len(sessions.clients) == 1 and sessions.clients[0].closed
        assert not sessions.responses and len(sessions.clients[0].plans) == page_attempts + 2
        charges = await _charges(database, definition.identifier)
        home = [row for row in charges if row[0] == '["ebay","homepage"]']
        page = [row for row in charges if row[0] == '["ebay","page","1"]']
        assert [row[1:] for row in home] == [(1, 2), (2, 2)]
        assert [row[1:] for row in page] == [
            (attempt, page_attempts) for attempt in range(1, page_attempts + 1)
        ]
        assert await database.resource_occupancy(stage_manager(database).resources.scope) == {}


@pytest.mark.anyio
async def test_facebook_challenge_retries_are_immediate_and_still_exhaust_five_attempts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = time_ns()
    sessions = FacebookSessions(challenges=5)
    _install(monkeypatch, facebook=sessions, now=now)
    payload = _payload()
    assert payload.request.price is not None
    payload = payload.model_copy(
        update={
            "routing": ("proton", "fixture", "search"),
            "request": payload.request.model_copy(
                update={
                    "price": payload.request.price.model_copy(update={"maximum": Decimal("180")})
                }
            ),
            "traversal_strategy": OverlappingPricePartitionSearchTraversalStrategy(
                width=Decimal("90")
            ),
        }
    )
    definition = collect_search_work(identifier="facebook", payload=payload, not_before_utc_ns=0)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"http": 1, "cpu": 1}
    ) as database:
        await _enqueue(database, definition, now=now)
        async with work_runtime.managed_worker_pool(
            database,
            tmp_path,
            composition=_composition(tmp_path, now),
            eligible_identifiers=(definition.identifier,),
        ):
            work = await _terminal(database, definition.identifier)
        assert work["state"] == "terminal_failure" and work["attempt"] == 5
        assert len(sessions.sessions) == 5 and all(session.closed for session in sessions.sessions)
        assert [phase for _, phase, _ in sessions.requests] == [
            "bootstrap",
            "0",
            "90",
            *[phase for _ in range(4) for phase in ("bootstrap", "90")],
        ]
        assert sessions.challenges == 0
        assert await database.resource_occupancy(stage_manager(database).resources.scope) == {}


@pytest.mark.anyio
async def test_persisted_resident_delay_retires_without_replaying_paid_capture_or_resetting_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "carl.sqlite3"
    sessions = EbaySessions(
        [(b"blocked", 403), (b"home", 200), *[(b"blocked", 403)] * 6, (_html(), 200)]
    )
    parked: list[StageDefinition] = []
    recorded = EbaySearchConfiguration(
        maximum_homepage_attempts=2,
        maximum_page_attempts=7,
        minimum_holdoff_seconds=60.0,
        maximum_holdoff_seconds=60.0,
    )
    async with Database.managed(path, initialize=True) as database:
        pool = ResidentSessionPool(
            stage_manager(database), StageHandlerRegistry((), ()), _provenance(), {}
        )
        registry = build_ebay_session_stage_registry(
            EbaySessionStageDependencies(
                database,
                _directories(tmp_path),
                lambda: str(uuid4()),
                SharedProtonWireproxyManager(),
                pool,
                session_factory=sessions,
                search_policy=recorded,
                item_policy=EbayItemConfiguration(),
            )
        )
        original = next(handler for handler in registry.handlers if handler.kind[-1] == "process")

        async def retain_old_wait(
            payload: SessionPhasePayload, context: StageAttemptContext
        ) -> StageHandlerResult:
            outcome = await original.execute(payload.model_dump(mode="json"), context)
            if not parked and isinstance(outcome, WaitWork):
                assert outcome.continuation.kind[-1] == "capture"
                deadline = time_ns() + 45_000_000_000
                continuation = outcome.continuation.model_copy(
                    update={"not_before_utc_ns": deadline}
                )
                parked.append(continuation)
                return outcome.model_copy(
                    update={"continuation": continuation, "wait_kind": StageWaitKind.TIME}
                )
            return outcome

        pool.registry = replace(
            registry,
            handlers=tuple(
                TypedStageHandler(handler.kind, 1, SessionPhasePayload, retain_old_wait)
                if handler is original
                else handler
                for handler in registry.handlers
            ),
        )
        definition = _ebay()
        await _enqueue(database, definition)
        adapter = registry.require_adapter(
            WorkCapability(
                kind=definition.kind, payload_schema_version=definition.payload_schema_version
            )
        )
        initial = adapter.initial_stage(definition, "initial", "operation")
        async with pool.managed():
            with anyio.fail_after(10):
                assert await pool.submit(initial, component=adapter.component)
                await pool.wait_idle()
        assert not pool.errors and parked
        assert (await database.work(definition.identifier))["attempt"] == 1
        assert len(sessions.clients) == 1 and sessions.clients[0].closed
        assert len(sessions.clients[0].plans) == 1
        assert await database.resource_occupancy(stage_manager(database).resources.scope) == {}
        old = parked[0]
        explicit = _ebay("explicit", not_before=time_ns() + 3_600_000_000_000)
        await _enqueue(database, explicit)
    # The recorded retry policy must survive the administrator lowering live
    # configuration: this continuation still owns the old7/2 request maxima.
    _install(
        monkeypatch,
        ebay=sessions,
        policy=EbaySearchConfiguration(maximum_page_attempts=1, maximum_homepage_attempts=1),
    )
    async with Database.managed(path) as database:
        async with work_runtime.managed_worker_pool(
            database, tmp_path, composition=_composition(tmp_path)
        ):
            work = await _terminal(database, definition.identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        assert len(work["operations"]) == 1
        assert await database.stage(old.identifier) == old
        assert (await database.operation(initial.operation_identifier))["state"] == "completed"
        assert len(sessions.clients) == 2 and all(client.closed for client in sessions.clients)
        assert sum(len(client.plans) for client in sessions.clients) == 9 and not sessions.responses
        charges = await _charges(database, definition.identifier)
        assert [row[1:] for row in charges if row[0] == '["ebay","homepage"]'] == [(1, 2), (2, 2)]
        assert [row[1:] for row in charges if row[0] == '["ebay","page","1"]'] == [
            (attempt, 7) for attempt in range(1, 8)
        ]
        untouched = await database.work(explicit.identifier)
        assert untouched["state"] == "pending" and untouched["attempt"] == 0
        assert untouched["eligible_at_utc_ns"] == explicit.not_before_utc_ns
        assert await database.resource_occupancy(stage_manager(database).resources.scope) == {}


@pytest.mark.anyio
async def test_marketplace_startup_preserves_hosted_travel_pacing_and_cooldown(
    tmp_path: Path,
) -> None:
    cache = RoutingCache(_directories(tmp_path), now=lambda: 100.0)
    assert (
        await cache.try_gate("geocode", action_identifier="geo", now=100)
    ).next_eligible_at == 100.65
    assert (
        await cache.try_gate("matrix", action_identifier="matrix", now=100)
    ).next_eligible_at == 101.6
    assert await cache.block_until(130) == 130
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await work_runtime.prepare_worker_constraints(database, tmp_path)
        blocked = await cache.try_gate("geocode", action_identifier="next-geo", now=100)
        assert blocked.reason == "provider_cooldown" and blocked.next_eligible_at == 130
        assert await cache.block_until(160) == 160
        await work_runtime.prepare_worker_constraints(database, tmp_path)
        blocked = await cache.try_gate("matrix", action_identifier="next-matrix", now=100)
        assert blocked.reason == "provider_cooldown" and blocked.next_eligible_at == 160
