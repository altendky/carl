"""Offline request deadlines and durable search retry budgets."""

# pyright: reportPrivateUsage=false, reportUnusedCallResult=false

from pathlib import Path
from typing import cast
from unittest.mock import patch
from uuid import uuid4

import anyio
import pytest

from carl._tests.test_ebay import _Acquirer, _identifiers, _provenance
from carl._tests.test_ebay_cooldown import _enqueue
from carl._tests.test_ebay_workers import _directories
from carl._tests.test_ebay_workers import _provenance as _async_provenance
from carl.core.ebay import (
    COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
    EbaySearchRequest,
    ebay_search_work_constraint,
)
from carl.core.ebay_refresh import RefreshEbaySearchPayload
from carl.core.http import RequestPlan
from carl.core.models import JsonValue
from carl.core.worker import AttemptContext, RetryWork, WorkerSettings
from carl.ebay import collect_ebay_search
from carl.ebay_refresh_workers import EbayRefreshWorkerDependencies, _search_phase
from carl.ebay_workers import EbaySearchWorkerDependencies, build_ebay_worker_registry
from carl.io import network_activity
from carl.io.httpx import Acquisition, AcquisitionFailure, IdentifierFactory
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, execute_lease


class _DeadlineAcquirer:
    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        del plan, new_identifier
        with anyio.fail_after(0.001):
            await anyio.sleep_forever()
        raise AssertionError("Deadline did not expire")


@pytest.mark.anyio
async def test_request_deadline_is_normalized_and_persisted(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        with pytest.raises(AcquisitionFailure) as raised:
            await collect_ebay_search(
                database,
                request=EbaySearchRequest(query="ES 68°"),
                network_path=("test",),
                acquirer=_DeadlineAcquirer(),
                new_identifier=_identifiers(),
                provenance=_provenance(),
            )
        failure = raised.value.result
        assert isinstance(raised.value.__cause__, TimeoutError)
        assert failure["stopping_condition"] == "transport_failure"
        assert failure["exception_type"] == "TimeoutError"
        assert failure["failure_phase"] == "request_timeout"
        operation = await database.operation("identifier-1")
        assert operation["state"] == "failed"
        assert operation["result"] == {"state": "acquisition_failed", "acquisition": failure}
        activity = await database.network_activity(str(failure["network_activity_identifier"]))
        assert activity["state"] == "failed"
        assert activity["result"] == {
            "kind": "exception",
            "type": "AcquisitionFailure",
            "exception_type": "TimeoutError",
            "stopping_condition": "transport_failure",
            "failure_phase": "request_timeout",
        }


@pytest.mark.anyio
async def test_unrelated_acquirer_error_is_not_normalized(tmp_path: Path) -> None:
    class InvalidAcquirer:
        async def acquire(
            self, plan: RequestPlan, new_identifier: IdentifierFactory
        ) -> Acquisition:
            del plan, new_identifier
            raise ValueError("programming error")

    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        with pytest.raises(ValueError, match="programming error"):
            await collect_ebay_search(
                database,
                request=EbaySearchRequest(query="scope"),
                network_path=("test",),
                acquirer=InvalidAcquirer(),
                new_identifier=_identifiers(),
                provenance=_provenance(),
            )


@pytest.mark.anyio
async def test_cancellation_is_not_normalized(tmp_path: Path) -> None:
    # Trio's cancellation exception cannot be constructed directly. Cancel an
    # actual scope at the request boundary instead.
    class ScopeCancelledAcquirer:
        async def acquire(
            self, plan: RequestPlan, new_identifier: IdentifierFactory
        ) -> Acquisition:
            del plan, new_identifier
            scope.cancel()
            await anyio.sleep_forever()
            raise AssertionError("Cancellation did not propagate")

    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        with anyio.CancelScope() as scope:
            await collect_ebay_search(
                database,
                request=EbaySearchRequest(query="scope"),
                network_path=("test",),
                acquirer=ScopeCancelledAcquirer(),
                new_identifier=_identifiers(),
                provenance=_provenance(),
            )
        assert scope.cancelled_caught
        activity = await database.network_activity("identifier-2")
        assert activity["state"] == "cancelled"


@pytest.mark.anyio
@pytest.mark.parametrize("own_deadline", [True, False])
async def test_finalization_deadline_retains_http_but_database_timeout_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, own_deadline: bool
) -> None:
    monkeypatch.setattr(network_activity, "NETWORK_ACTIVITY_FINALIZATION_TIMEOUT_SECONDS", 0.001)

    async def finish(*_args: object, **_kwargs: object) -> None:
        if own_deadline:
            await anyio.sleep_forever()
        raise TimeoutError("database timeout unrelated to scheduler deadline")

    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        monkeypatch.setattr(database, "finish_network_activity", finish)
        with pytest.raises(AcquisitionFailure if own_deadline else TimeoutError) as raised:
            await collect_ebay_search(
                database,
                request=EbaySearchRequest(query="scope"),
                network_path=("test",),
                acquirer=_Acquirer(b"0 results for scope"),
                new_identifier=_identifiers(),
                provenance=_provenance(),
            )
        if own_deadline:
            assert isinstance(raised.value, AcquisitionFailure)
            failure = raised.value.result
            assert failure["stopping_condition"] == "network_activity_failure"
            assert failure["failure_phase"] == "network_activity_finalization_timeout"
            _, _, record = await database.get_record(str(failure["acquisition_record_identifier"]))
            assert isinstance(record, dict)
            assert record["stopping_condition"] == "terminal_response"
            assert record["collection_completeness"] == "complete"
            _, body = await database.get_artifact(raised.value.bodies[0].identifier)
            assert body == b"0 results for scope"
            operation = await database.operation("identifier-1")
            assert operation["state"] == "failed"
            activity = await database.network_activity(str(failure["network_activity_identifier"]))
            assert activity["state"] == "cancelled"


async def _execute(database: Database, root: Path, now: int, *, response: str) -> None:
    async def collect(
        database: Database,
        *,
        request: EbaySearchRequest,
        search_run_identifier: str,
        new_identifier: IdentifierFactory,
        **_kwargs: object,
    ) -> dict[str, JsonValue]:
        acquirer = (
            _DeadlineAcquirer()
            if response == "timeout"
            else _Acquirer(b"Forbidden", status_code=403)
            if response == "403"
            else _Acquirer(b"0 results for scope")
        )

        async def search() -> dict[str, JsonValue]:
            return await collect_ebay_search(
                database,
                request=request,
                search_run_identifier=search_run_identifier,
                new_identifier=new_identifier,
                network_path=("test",),
                acquirer=acquirer,
                provenance=_provenance(),
            )

        if response == "finalization_timeout":

            async def stall_finish(*_args: object, **_kwargs: object) -> None:
                await anyio.sleep_forever()

            with (
                patch.object(
                    network_activity, "NETWORK_ACTIVITY_FINALIZATION_TIMEOUT_SECONDS", 0.001
                ),
                patch.object(network_activity.NetworkActivityScheduler, "_finish", stall_finish),
            ):
                return await search()
        return await search()

    registry = build_ebay_worker_registry(
        EbaySearchWorkerDependencies(
            database,
            _directories(root),
            lambda: str(uuid4()),
            collector=collect,
            utc_now_ns=lambda: now,
        )
    )
    claimed = await database.claim_work(
        supported_capabilities=registry.capabilities,
        worker_identifier="test",
        lease_token=str(uuid4()),
        lease_duration_ns=60_000_000_000,
        utc_now_ns=lambda: now,
        event_identifier=str(uuid4()),
        eligible_identifiers=("search",),
    )
    assert claimed.lease is not None
    await execute_lease(
        database=database,
        registry=registry,
        lease=claimed.lease,
        settings=WorkerSettings(
            worker_count=1,
            lease_duration_ns=60_000_000_000,
            renewal_interval_ns=1_000_000_000,
            idle_poll_interval_ns=1,
        ),
        services=WorkerRuntimeServices(
            new_identifier=lambda: str(uuid4()),
            utc_now_ns=lambda: now,
            monotonic_ns=lambda: now,
            code_provenance=_async_provenance,
            invocation=lambda: {},
        ),
    )


@pytest.mark.anyio
@pytest.mark.parametrize("response", ["timeout", "finalization_timeout"])
async def test_403_then_deadline_retries_durably_with_shared_cooldown(
    tmp_path: Path,
    response: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, now = tmp_path / "test.sqlite3", 1_000_000_000_000
    async with Database.managed(path, initialize=True) as database:
        await database.register_constraint(ebay_search_work_constraint(), registered_at_utc_ns=now)
        await _enqueue(database, "search", now)
        await _enqueue(database, "sibling", now)
        await _execute(database, tmp_path, now, response="403")
        now += 600_000_000_000
        assert (await database.work("search"))["eligible_at_utc_ns"] == now
    async with Database.managed(path) as database:
        await _execute(database, tmp_path, now, response=response)
        work = await database.work("search")
        assert work["state"] == "pending" and work["attempt"] == 2
        assert work["error"] is None
        acquisition = work["result"]["acquisition"]
        assert acquisition["failure_phase"] == (
            "request_timeout" if response == "timeout" else "network_activity_finalization_timeout"
        )
        if response == "finalization_timeout":
            _, _, retained = await database.get_record(acquisition["acquisition_record_identifier"])
            assert isinstance(retained, dict)
            retained = cast(dict[str, JsonValue], retained)
            assert retained["stopping_condition"] == "terminal_response"
            hops = retained["hops"]
            assert isinstance(hops, list) and isinstance(hops[0], dict)
            retained_response = cast(dict[str, JsonValue], hops[0])["response"]
            assert isinstance(retained_response, dict)
            retained_body = cast(dict[str, JsonValue], retained_response)["body"]
            assert isinstance(retained_body, dict)
            body_identifier = cast(dict[str, JsonValue], retained_body)["artifact_id"]
            assert isinstance(body_identifier, str)
            _, body = await database.get_artifact(body_identifier)
            assert body == b"0 results for scope"

        async def requested(*_args: object) -> str:
            return "search"

        monkeypatch.setattr("carl.ebay_refresh_workers._requested", requested)
        parent = await _search_phase(
            RefreshEbaySearchPayload(
                base_search_run_record_identifier="base",
                search_work_identifier="search",
                search=EbaySearchRequest(query="scope"),
            ),
            AttemptContext(
                work_item_identifier="parent",
                lease_token="lease",
                worker_identifier="test",
                operation_identifier="operation",
                attempt=2,
            ),
            EbayRefreshWorkerDependencies(database, lambda: str(uuid4()), utc_now_ns=lambda: now),
        )
        assert isinstance(parent, RetryWork)
        assert parent.delay_ns == 1200_000_000_000
        assert parent.reason == {"kind": "waiting_for_search_collection"}
        now += 1200_000_000_000
        assert work["eligible_at_utc_ns"] == now
        assert (await database.work("sibling"))["eligible_at_utc_ns"] == now
    async with Database.managed(path) as database:
        await _execute(database, tmp_path, now, response="success")
        work = await database.work("search")
        assert work["state"] == "completed" and work["attempt"] == 3
        assert len(work["operations"]) == 3


@pytest.mark.anyio
async def test_deadline_exhaustion_and_operator_retry_get_fresh_budget(tmp_path: Path) -> None:
    now = 1_000_000_000_000
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _enqueue(database, "search", now)
        await _enqueue(database, "sibling", now)
        for delay in (0, 600_000_000_000, 1200_000_000_000, 2400_000_000_000, 3600_000_000_000):
            now += delay
            await _execute(database, tmp_path, now, response="timeout")
        work = await database.work("search")
        assert work["state"] == "terminal_failure" and work["attempt"] == 5
        assert work["error"]["maximum_attempts"] == 5
        now += 3600_000_000_000
        assert (await database.work("sibling"))["eligible_at_utc_ns"] == now
        await database.retry_terminal_ebay_search_work(
            work_item_identifier="search",
            retried_at_utc_ns=now,
            event_identifier=str(uuid4()),
            reason={"kind": "operator_retry"},
            payload_schema_version=COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
        )
        await _execute(database, tmp_path, now, response="timeout")
        work = await database.work("search")
        assert work["state"] == "pending" and work["attempt"] == 6
        assert work["eligible_at_utc_ns"] == now + 600_000_000_000
        assert work["payload"]["retry_attempt_offset"] == 5
