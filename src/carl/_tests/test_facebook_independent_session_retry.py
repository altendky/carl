"""Verified independent sessions retain their own retries without pausing peers."""

# Reuse offline sessions and durable-clock fixtures; no provider calls are made.
# pyright: reportPrivateUsage=false

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal, cast

import pytest

import carl.facebook_routed_workers as routed_workers
from carl._tests.test_configuration import _directories
from carl._tests.test_datacenter_routing import DATACENTER, PROTON, _loaded
from carl._tests.test_decodo_facebook_sessions import _credentials, _settings
from carl._tests.test_facebook_search_challenge_retry import (
    MINUTE_NS,
    SETTINGS,
    _Harness,
    _PartitionSessionFactory,
)
from carl._tests.test_facebook_search_cooldown import _activity
from carl._tests.test_facebook_search_workers import _FakeSession
from carl.core.facebook_search_retry import FACEBOOK_SEARCH_NETWORK_ACTIVITY_KINDS
from carl.core.facebook_work import COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION, COLLECT_SEARCH_WORK_KIND
from carl.core.models import JsonValue
from carl.core.routing import DecodoProduct
from carl.core.work import WorkCapability, WorkState
from carl.core.worker import AttemptContext, CompletedWork, RetryWork, TerminalFailureWork
from carl.facebook_search_workers import FacebookSearchWorkerDependencies
from carl.facebook_workers import FacebookWorkerDependencies, build_facebook_worker_registry
from carl.io.configuration import LoadedCarlConfiguration
from carl.io.decodo import DecodoCredentialSource, DecodoProxySettings
from carl.io.facebook_search import DecodoFacebookSearchSessionFactory, FacebookSearchSessionFailure
from carl.io.httpx import AcquisitionFailure
from carl.io.sqlite import Database
from carl.io.worker import WorkHandlerRegistry

_RetryScope = Literal["stack", "independent_session"]


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ("challenge", "login"))
@pytest.mark.parametrize("activity_kind", sorted(FACEBOOK_SEARCH_NETWORK_ACTIVITY_KINDS))
async def test_independent_challenge_budget_does_not_cool_queued_or_active_peers(
    tmp_path: Path, failure: str, activity_kind: tuple[str, ...]
) -> None:
    factory = _PartitionSessionFactory(
        challenges=5 if failure == "challenge" else 0,
        login_walls_by_partition={"90": 5} if failure == "login" else {},
    )
    harness = _Harness(tmp_path / "carl.sqlite3", factory, retry_scope="independent_session")
    factory.clock = harness.clock
    async with Database.managed(harness.path, initialize=True) as database:
        _, source = await harness.create(database)
        _, queued = await harness.create(database, query="queued peer")
        queued_deadline = (await database.work(queued))["eligible_at_utc_ns"]

    for attempt in range(1, 6):
        async with Database.managed(harness.path) as database:
            outcome = await harness.run(database, source)
            assert isinstance(outcome, (RetryWork, TerminalFailureWork))
            work = await database.work(source)
            assert work["attempt"] == attempt
            result = cast(dict[str, JsonValue], outcome.result)
            assert result["retry_scope"] == "independent_session"
            diagnostic = outcome.reason if isinstance(outcome, RetryWork) else outcome.error
            assert isinstance(diagnostic, dict)
            assert diagnostic["retry_scope"] == "independent_session"
            assert "facebook_search_cooldown" not in diagnostic
            assert (await database.work(queued))["eligible_at_utc_ns"] == queued_deadline

            # An already-running peer must not inherit the failed session's wait.
            # Advance beyond the ordinary short request-rate window first.
            harness.clock.now += 1_000_000_000
            activity = await _activity(
                database, f"active-peer-{attempt}", kind=activity_kind, route=PROTON
            )
            admission = await database.try_admit_network_activity(
                network_activity_identifier=activity.identifier,
                admission_token=f"permit-{attempt}",
                permit_duration_ns=MINUTE_NS,
                now_utc_ns=harness.clock.now,
                event_identifier=harness.new_identifier(),
            )
            assert admission.admission is not None
            if attempt < 5:
                assert isinstance(outcome, RetryWork)
                assert outcome.delay_ns == (10, 20, 40, 60)[attempt - 1] * MINUTE_NS
                assert work["state"] == WorkState.PENDING
                eligible = work["eligible_at_utc_ns"]
                assert isinstance(eligible, int)
                assert eligible == harness.clock.now - 1_000_000_000 + outcome.delay_ns
                harness.clock.now = eligible
            else:
                assert isinstance(outcome, TerminalFailureWork)
                assert work["state"] == WorkState.TERMINAL_FAILURE

    async with Database.managed(harness.path) as database:
        _, future = await harness.create(database, query="future peer")
        assert (await database.work(future))["eligible_at_utc_ns"] <= harness.clock.now
        claim = await database.claim_work(
            supported_capabilities=(
                WorkCapability(
                    kind=COLLECT_SEARCH_WORK_KIND,
                    payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
                ),
            ),
            eligible_identifiers=(queued,),
            worker_identifier="queued-peer-worker",
            lease_token=harness.new_identifier(),
            lease_duration_ns=SETTINGS.lease_duration_ns,
            utc_now_ns=harness.clock,
            event_identifier=harness.new_identifier(),
        )
        assert claim.lease is not None and claim.lease.work_item_identifier == queued
    assert len(factory.sessions) == 5


@pytest.mark.anyio
@pytest.mark.parametrize("retry_scope", ("stack", "independent_session"))
@pytest.mark.parametrize("failure_phase", ("request", "session_open", "session_close"))
async def test_transport_backoff_only_defers_peers_for_shared_sessions(
    tmp_path: Path, retry_scope: _RetryScope, failure_phase: str
) -> None:
    base_factory = _PartitionSessionFactory(
        challenges=0,
        transport_failures_by_partition={"0": 1} if failure_phase == "request" else {},
    )

    @asynccontextmanager
    async def factory(identifier: str) -> AsyncGenerator[_FakeSession]:
        if failure_phase == "session_open":
            raise FacebookSearchSessionFailure("wireproxy_device_identity_busy", provider="proton")
        async with base_factory(identifier) as session:
            yield session
        if failure_phase == "session_close":
            raise AcquisitionFailure(
                "Synthetic close failure",
                result={
                    "stopping_condition": "transport_failure",
                    "exception_type": "OSError",
                    "failure_phase": "client_close",
                },
            )

    harness = _Harness(tmp_path / "carl.sqlite3", factory, retry_scope=retry_scope)
    base_factory.clock = harness.clock
    async with Database.managed(harness.path, initialize=True) as database:
        _, source = await harness.create(database)
        _, sibling = await harness.create(database, query="transport peer")
        original_deadline = (await database.work(sibling))["eligible_at_utc_ns"]
        outcome = await harness.run(database, source)
        assert isinstance(outcome, RetryWork)
        assert cast(dict[str, JsonValue], outcome.reason)["retry_scope"] == retry_scope
        assert cast(dict[str, JsonValue], outcome.result)["retry_scope"] == retry_scope
        own = await database.work(source)
        assert own["state"] == WorkState.PENDING
        assert own["eligible_at_utc_ns"] == harness.clock.now + outcome.delay_ns
        expected = (
            original_deadline
            if retry_scope == "independent_session"
            else harness.clock.now + outcome.delay_ns
        )
        assert (await database.work(sibling))["eligible_at_utc_ns"] == expected
    async with Database.managed(harness.path) as database:
        assert (await database.work(sibling))["eligible_at_utc_ns"] == expected


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("product", "pool", "expected"),
    (
        (DecodoProduct.MOBILE_PROXY, "fixed", "independent_session"),
        (DecodoProduct.RESIDENTIAL_PROXY, "fixed", "independent_session"),
        (DecodoProduct.DATACENTER_PROXY, "range", "independent_session"),
        (DecodoProduct.DATACENTER_PROXY, "list", "independent_session"),
        (DecodoProduct.DATACENTER_PROXY, "fixed", "stack"),
        (DecodoProduct.DATACENTER_PROXY, "single", "stack"),
    ),
)
async def test_resolved_decodo_settings_determine_scope_before_session_port_selection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    product: DecodoProduct,
    pool: str,
    expected: _RetryScope,
) -> None:
    settings = _settings()
    settings = settings.model_copy(
        update={"route": settings.route.model_copy(update={"product": product})}
    )
    if pool == "range":
        settings = settings.model_copy(update={"endpoint_port_end": 19000})
    elif pool in {"list", "single"}:
        settings = settings.model_copy(
            update={"endpoint_ports": (10001, 10002) if pool == "list" else (10001,)}
        )
    captured: list[FacebookSearchWorkerDependencies] = []
    original_registry = build_facebook_worker_registry

    def resolve_settings(*_args: object) -> tuple[DecodoProxySettings, DecodoCredentialSource]:
        return settings, _credentials()

    def registry(
        _dependencies: FacebookWorkerDependencies,
        search_dependencies: FacebookSearchWorkerDependencies | None = None,
    ) -> WorkHandlerRegistry:
        if search_dependencies is not None:
            captured.append(search_dependencies)
        return original_registry(_dependencies, search_dependencies)

    def load(_path: Path) -> LoadedCarlConfiguration:
        return _loaded(tmp_path, override=True)

    async def bind(*_args: object, **_kwargs: object) -> None:
        pass

    async def supersede(*_args: object, **_kwargs: object) -> None:
        pass

    async def execute(*_args: object, **_kwargs: object) -> CompletedWork:
        return CompletedWork(result={"state": "captured_dependencies"})

    monkeypatch.setattr(routed_workers, "load_configuration", load)
    monkeypatch.setattr(routed_workers, "decodo_settings", resolve_settings)
    monkeypatch.setattr(routed_workers, "build_facebook_worker_registry", registry)
    monkeypatch.setattr(routed_workers, "brave_navigation_headers", lambda: ())
    monkeypatch.setattr(routed_workers, "_execute", execute)
    monkeypatch.setattr(Database, "bind_facebook_search_route", bind)
    monkeypatch.setattr(Database, "supersede_constraints", supersede)
    harness = _Harness(tmp_path / "carl.sqlite3", _PartitionSessionFactory(challenges=0))
    async with Database.managed(harness.path, initialize=True) as database:
        _, identifier = await harness.create(database)
        work = await database.work(identifier)
        worker_registry = routed_workers.build_routed_facebook_worker_registry(
            database=database,
            directories=_directories(tmp_path),
            new_identifier=harness.new_identifier,
        )
        handler = next(
            value
            for value in worker_registry.handlers
            if value.capability.kind == COLLECT_SEARCH_WORK_KIND
        )
        payload = work["payload"]
        assert isinstance(payload, dict) and payload["routing"] == list(PROTON)
        outcome = await handler.execute(
            payload,
            AttemptContext(
                work_item_identifier=identifier,
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="operation",
            ),
        )
        assert isinstance(outcome, CompletedWork)
    assert len(captured) == 1
    assert captured[0].retry_scope == expected
    factory = captured[0].session_factory
    assert isinstance(factory, DecodoFacebookSearchSessionFactory)
    assert factory.settings == settings
    assert factory.settings.route.network_path == DATACENTER
