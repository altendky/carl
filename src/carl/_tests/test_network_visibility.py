"""Connection and session failures remain visible before any HTTP response exists."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from itertools import count
from pathlib import Path
from time import perf_counter_ns, time_ns
from typing import cast

import pytest

from carl.core.ebay import EbaySearchRequest
from carl.core.facebook_search import SearchTraversalPolicy
from carl.core.facebook_work import (
    CollectSearchPayload,
    FacebookSearchRequest,
    SearchDistanceUnit,
    SearchFacebookLocation,
    SearchRadius,
)
from carl.core.http import RequestPlan
from carl.core.models import CodeProvenance
from carl.core.worker import AttemptContext, RetryWork
from carl.ebay import collect_ebay_search
from carl.facebook_search_workers import FacebookSearchWorkerDependencies
from carl.facebook_workers import FacebookWorkerDependencies, build_facebook_worker_registry
from carl.io.connectivity import ConnectivityMonitor
from carl.io.facebook_search import FacebookSearchHttpSession, FacebookSearchSessionFailure
from carl.io.httpx import Acquisition, AcquisitionFailure, IdentifierFactory
from carl.io.network_activity import network_activity_scheduler
from carl.io.sqlite import Database


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="dirty",
        package_version="test",
        python_implementation="test",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


class _DisconnectedAcquirer:
    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        _ = plan, new_identifier
        raise AcquisitionFailure(
            "Connection contains secret diagnostic that must not enter activity ledger",
            result={
                "stopping_condition": "transport_failure",
                "exception_type": "ProxyConnectionError",
                "failure_phase": "connect",
                "hops": [],
            },
        )


@pytest.mark.anyio
async def test_ebay_connection_failure_without_response_is_counted(tmp_path: Path) -> None:
    identifiers = count()

    def new_identifier() -> str:
        return f"visibility-{next(identifiers)}"

    route = ("decodo", "personal", "carl")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        with pytest.raises(AcquisitionFailure) as caught:
            _ = await collect_ebay_search(
                database,
                request=EbaySearchRequest(query="telescope"),
                network_path=route,
                acquirer=_DisconnectedAcquirer(),
                new_identifier=new_identifier,
                provenance=_provenance(),
            )
        activity = await database.network_activity(
            str(caught.value.result["network_activity_identifier"])
        )
        assert activity["state"] == "failed"
        assert activity["result"] == {
            "kind": "exception",
            "type": "AcquisitionFailure",
            "exception_type": "ProxyConnectionError",
            "stopping_condition": "transport_failure",
            "failure_phase": "connect",
        }
        snapshot = await database.activity_snapshot(
            captured_at_utc_ns=time_ns(), recent_window_ns=60_000_000_000, maximum_rows=10
        )
        assert snapshot.network.recent_failed == 1
        assert snapshot.network_paths[0].path == route
        assert snapshot.network_paths[0].recent_failed == 1


@pytest.mark.anyio
async def test_snapshot_exposes_connectivity_pause_and_latest_probe(tmp_path: Path) -> None:
    from carl.core.models import JsonValue

    async def disconnected() -> dict[str, JsonValue]:
        return {"reachable": False, "checks": [{"host": "test", "reachable": False}]}

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        monitor = ConnectivityMonitor(
            database=database,
            new_identifier=lambda: "probe-token",
            utc_now_ns=lambda: 10,
            probe=disconnected,
        )
        assert not await monitor.available(after_failure=True)
        snapshot = await database.activity_snapshot(
            captured_at_utc_ns=11, recent_window_ns=10, maximum_rows=10
        )
        assert snapshot.connectivity.paused
        assert snapshot.connectivity.reason == "connectivity_outage"
        assert not snapshot.connectivity.probe_in_progress
        assert snapshot.connectivity.last_probe_at_utc_ns == 10
        assert snapshot.connectivity.next_probe_at_utc_ns == 30_000_000_010
        assert snapshot.connectivity.last_probe_result == await disconnected()


@pytest.mark.anyio
async def test_facebook_session_open_failure_is_counted(tmp_path: Path) -> None:
    identifiers = count()

    def new_identifier() -> str:
        return f"visibility-{next(identifiers)}"

    route = ("proton", "personal", "carl")

    @asynccontextmanager
    async def failed_session(identifier: str) -> AsyncGenerator[FacebookSearchHttpSession]:
        if identifier:
            raise FacebookSearchSessionFailure("proton_egress_probe_failed", provider="proton")
        yield cast(FacebookSearchHttpSession, object())  # pragma: no cover

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(
                database=database, acquirer=_DisconnectedAcquirer(), new_identifier=new_identifier
            ),
            FacebookSearchWorkerDependencies(
                database=database,
                session_factory=failed_session,
                navigation_headers=(),
                new_identifier=new_identifier,
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                network_activity_scheduler=network_activity_scheduler(database, new_identifier),
            ),
        )
        handler = next(h for h in registry.handlers if h.capability.kind[-1] == "collect_search")
        await database.begin_operation(
            operation_id="operation",
            component=handler.component,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        outcome = await handler.execute(
            CollectSearchPayload(
                request=FacebookSearchRequest(
                    query="telescope",
                    location=SearchFacebookLocation(identifier="456"),
                    radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
                ),
                routing=route,
                traversal=SearchTraversalPolicy(maximum_pages=1),
            ).as_json(),
            AttemptContext(
                work_item_identifier="work",
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="operation",
            ),
        )
        assert isinstance(outcome, RetryWork)
        snapshot = await database.activity_snapshot(
            captured_at_utc_ns=time_ns(), recent_window_ns=60_000_000_000, maximum_rows=10
        )
        assert snapshot.network.recent_failed == 1
        assert snapshot.network_paths[0].path == route
        assert snapshot.network_paths[0].recent_failed == 1
