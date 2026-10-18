"""Search-only route cooldowns survive restart and disappear with cancelled sources."""

# Reuse the offline provenance fixture; these tests perform no network requests.
# pyright: reportPrivateUsage=false

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from carl._tests.test_facebook_search_workers import _async_provenance
from carl.core.components import Component, ComponentId
from carl.core.facebook_search import SearchTraversalPolicy
from carl.core.facebook_search_retry import FACEBOOK_SEARCH_NETWORK_ACTIVITY_KINDS
from carl.core.facebook_work import (
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_SEARCH_WORK_KIND,
    CollectSearchPayload,
    FacebookSearchRequest,
    SearchDistanceUnit,
    SearchFacebookLocation,
    SearchRadius,
    collect_search_work,
)
from carl.core.work import WorkCapability, WorkRequester
from carl.io.network_activity import NetworkActivityScheduler, network_activity_definition
from carl.io.sqlite import Database

_ROUTE = ("decodo", "personal", "datacenter")
_OTHER_ROUTE = ("decodo", "personal", "other")
_NOW = 1_800_000_000_000_000_000
_MINUTE = 60_000_000_000
_CAPABILITY = WorkCapability(
    kind=COLLECT_SEARCH_WORK_KIND, payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION
)


def _anchor() -> None:
    """Identity anchor for offline scheduler evidence."""


async def _enqueue(
    database: Database, identifier: str, *, route: tuple[str, ...] = _ROUTE, not_before: int = 0
) -> None:
    _ = await database.enqueue_work(
        collect_search_work(
            identifier=identifier,
            payload=CollectSearchPayload(
                request=FacebookSearchRequest(
                    query=identifier,
                    location=SearchFacebookLocation(identifier="456"),
                    radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
                ),
                traversal=SearchTraversalPolicy(maximum_pages=2),
                routing=route,
            ),
            not_before_utc_ns=not_before,
        ),
        WorkRequester(
            request_identifier=str(uuid4()),
            kind=("carl", "test"),
            identifier=identifier,
            context={},
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=_NOW,
    )


async def _claim(database: Database, identifier: str, *, now: int = _NOW):
    return await database.claim_work(
        supported_capabilities=(_CAPABILITY,),
        eligible_identifiers=(identifier,),
        worker_identifier="worker",
        lease_token=str(uuid4()),
        lease_duration_ns=60 * _MINUTE,
        utc_now_ns=lambda: now,
        event_identifier=str(uuid4()),
    )


async def _cooldown(database: Database, identifier: str, *, minutes: int = 10) -> None:
    await _enqueue(database, identifier)
    claim = await _claim(database, identifier)
    assert claim.lease is not None
    lease = claim.lease
    await database.begin_leased_operation(
        work_item_identifier=identifier,
        lease_token=lease.token,
        worker_identifier=lease.worker_identifier,
        lease_duration_ns=60 * _MINUTE,
        utc_now_ns=lambda: _NOW,
        event_identifier=str(uuid4()),
        operation_id=identifier,
        component=Component(ComponentId(("carl", "test", "cooldown")), 1, _anchor),
        provenance=await _async_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-10-17T00:00:00+00:00",
    )
    await database.retry_leased_operation(
        work_item_identifier=identifier,
        lease_token=lease.token,
        worker_identifier=lease.worker_identifier,
        utc_now_ns=lambda: _NOW,
        delay_ns=minutes * _MINUTE,
        event_identifier=str(uuid4()),
        operation_id=identifier,
        reason={
            "kind": "guest_access_interrupted",
            "facebook_search_cooldown": {
                "network_path": list(_ROUTE),
                "until_utc_ns": _NOW + minutes * _MINUTE,
            },
        },
        result={"state": "challenge_backoff"},
        ended_at_utc="2026-10-17T00:00:00+00:00",
        duration_ns=1,
    )


async def _activity(
    database: Database,
    identifier: str,
    *,
    kind: tuple[str, ...] = ("carl", "facebook", "network_activity", "search_route_definition"),
    route: tuple[str, ...] = _ROUTE,
    operation_started: bool = False,
):
    if not operation_started:
        await database.begin_operation(
            operation_id=identifier,
            component=Component(ComponentId(("carl", "test", "active_search")), 1, _anchor),
            provenance=await _async_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-10-17T00:00:00+00:00",
        )
    definition = network_activity_definition(
        identifier=identifier,
        kind=kind,
        operation_identifier=identifier,
        network_session_identifier=identifier,
        network_path=route,
    )
    _ = await database.create_network_activity(
        definition,
        created_at_utc_ns=_NOW,
        event_identifier=str(uuid4()),
        sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
    )
    return definition


@pytest.mark.anyio
async def test_cooldown_gates_existing_and_future_searches_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _enqueue(database, "existing")
        await _cooldown(database, "source")
    async with Database.managed(path) as database:
        await _enqueue(database, "future")
        await _enqueue(database, "other", route=_OTHER_ROUTE)
        for identifier in ("existing", "future"):
            claim = await _claim(database, identifier)
            assert claim.lease is None
            assert claim.next_eligible_at_utc_ns == _NOW + 10 * _MINUTE
            assert (await database.work(identifier))["eligible_at_utc_ns"] == _NOW
        assert (await _claim(database, "other")).lease is not None
        assert (await _claim(database, "existing", now=_NOW + 10 * _MINUTE)).lease is not None


@pytest.mark.anyio
@pytest.mark.parametrize("kind", sorted(FACEBOOK_SEARCH_NETWORK_ACTIVITY_KINDS))
async def test_active_search_admission_obeys_route_cooldown(
    tmp_path: Path, kind: tuple[str, ...]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _activity(database, "already-waiting", kind=kind)
        await _cooldown(database, "source")
        result = await database.try_admit_network_activity(
            network_activity_identifier="already-waiting",
            admission_token="token",
            permit_duration_ns=_MINUTE,
            now_utc_ns=_NOW,
            event_identifier=str(uuid4()),
        )
        assert result.admission is None
        assert result.next_eligible_at_utc_ns == _NOW + 10 * _MINUTE
        for identifier, unrelated_kind, route in (
            ("other", kind, _OTHER_ROUTE),
            ("image", ("carl", "facebook", "network_activity", "image"), _ROUTE),
            ("item", ("carl", "facebook", "network_activity", "item_page"), _ROUTE),
        ):
            await _activity(database, identifier, kind=unrelated_kind, route=route)
            admitted = await database.try_admit_network_activity(
                network_activity_identifier=identifier,
                admission_token=identifier,
                permit_duration_ns=_MINUTE,
                now_utc_ns=_NOW,
                event_identifier=str(uuid4()),
            )
            assert admitted.admission is not None


@pytest.mark.anyio
async def test_cancelled_source_preserves_other_cooldowns_and_not_before(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _cooldown(database, "first", minutes=10)
        # Model an independently leased search that was already active before
        # the first response installed its shared route cooldown.
        original = database._facebook_search_route_cooldowns

        async def no_cooldown(*args: Any, **kwargs: Any):
            return {}

        with monkeypatch.context() as patch:
            patch.setattr(database, "_facebook_search_route_cooldowns", no_cooldown)
            await _cooldown(database, "second", minutes=20)
        assert database._facebook_search_route_cooldowns == original
        await _enqueue(database, "future")
        await _enqueue(database, "explicit", not_before=_NOW + 30 * _MINUTE)
        _ = await database.cancel_work(
            "first", reason="stop", caller="test", requested_at_utc_ns=_NOW
        )
        assert (await _claim(database, "future")).next_eligible_at_utc_ns == _NOW + 20 * _MINUTE
        _ = await database.cancel_work(
            "second", reason="stop", caller="test", requested_at_utc_ns=_NOW
        )
        assert (await _claim(database, "future")).lease is not None
        assert (await database.work("explicit"))["eligible_at_utc_ns"] == _NOW + 30 * _MINUTE
        assert (await _claim(database, "explicit")).lease is None


@pytest.mark.anyio
async def test_active_admission_notices_cancelled_source_within_one_second(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _cooldown(database, "source")
        definition = await _activity(database, "active")

        # The scheduler creates this activity itself; remove no durable rows.
        async def already_created(*args: Any, **kwargs: Any) -> int:
            return _NOW

        monkeypatch.setattr(database, "create_network_activity", already_created)
        sleeps: list[float] = []

        async def cancel_during_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            _ = await database.cancel_work(
                "source", reason="stop", caller="test", requested_at_utc_ns=_NOW
            )

        monkeypatch.setattr("carl.io.network_activity.anyio.sleep", cancel_during_sleep)
        scheduler = NetworkActivityScheduler(
            database=database,
            new_identifier=lambda: str(uuid4()),
            utc_now_ns=lambda: _NOW,
            sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
            permit_duration_ns=_MINUTE,
        )
        async with scheduler.admit(definition) as permit:
            permit.mark_dispatched()
        assert sleeps == [1.0]


@pytest.mark.anyio
async def test_requested_alias_cannot_bypass_effective_route_cooldown(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _cooldown(database, "source")
        requested_alias = ("proton", "personal", "old_alias")
        await _enqueue(database, "alias", route=requested_alias)
        claim = await _claim(database, "alias")
        assert claim.lease is not None
        lease = claim.lease
        assert await database.try_admit_facebook_search_route(
            work_item_identifier="alias",
            lease_token=lease.token,
            worker_identifier=lease.worker_identifier,
            routing=_ROUTE,
            utc_now_ns=lambda: _NOW,
        )
        await database.begin_leased_operation(
            work_item_identifier="alias",
            lease_token=lease.token,
            worker_identifier=lease.worker_identifier,
            lease_duration_ns=60 * _MINUTE,
            utc_now_ns=lambda: _NOW,
            event_identifier=str(uuid4()),
            operation_id="alias",
            component=Component(ComponentId(("carl", "test", "active_search")), 1, _anchor),
            provenance=await _async_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-10-17T00:00:00+00:00",
        )
        await _activity(
            database,
            "alias",
            route=_ROUTE,
            operation_started=True,
            kind=("carl", "facebook", "network_activity", "search_session_open"),
        )
        result = await database.try_admit_network_activity(
            network_activity_identifier="alias",
            admission_token="alias",
            permit_duration_ns=_MINUTE,
            now_utc_ns=_NOW,
            event_identifier=str(uuid4()),
        )
        assert result.admission is None
        assert result.next_eligible_at_utc_ns == _NOW + 10 * _MINUTE
