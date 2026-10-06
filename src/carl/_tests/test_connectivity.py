import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import anyio
import pytest

from carl._tests.test_worker import _async_provenance, _definition, _requester, _settings
from carl.core.components import Component, ComponentId
from carl.core.connectivity import connectivity_failure
from carl.core.models import JsonValue, StrictModel
from carl.core.work import WorkCapability
from carl.core.worker import AttemptContext, RetryWork, TerminalFailureWork, WorkOutcome
from carl.io.connectivity import PROBE_INTERVAL_NS, ConnectivityMonitor, probe_internet_connectivity
from carl.io.sqlite import Database
from carl.io.worker import (
    TypedWorkHandler,
    WorkerRuntimeServices,
    WorkHandlerRegistry,
    execute_lease,
)


class Payload(StrictModel):
    value: int


def test_outage_budget_respects_physical_manual_retry_baseline() -> None:
    context = AttemptContext(
        work_item_identifier="work",
        lease_token="lease",
        worker_identifier="worker",
        operation_identifier="operation",
        attempt=8,
        outage_attempts=(1, 3, 6, 7),
        retry_budget_start_attempt=5,
    )
    assert context.retry_attempt() == 1
    assert context.retry_attempt(6) == 1
    assert not connectivity_failure(
        TerminalFailureWork(
            error={"kind": "error_page"},
            result={"response_classification": {"kind": "challenge", "http_status": 403}},
        )
    )
    assert connectivity_failure(
        RetryWork(
            delay_ns=1,
            reason={"kind": "network_session_failure", "code": "proton_egress_probe_failed"},
            result={"state": "transport_failed"},
        )
    )


@pytest.mark.anyio
async def test_outage_defers_work_without_spending_budget_and_allows_offline_work(
    tmp_path: Path,
) -> None:
    now = 1_000_000_000
    online = False
    probes = 0
    observed_budgets: list[int] = []
    network_kind = ("carl", "ebay", "collect", "item")
    offline_kind = ("carl", "ebay", "extract", "item")

    async def probe() -> dict[str, JsonValue]:
        nonlocal probes
        probes += 1
        return {"reachable": online}

    async def handle(payload: Payload, context: AttemptContext) -> WorkOutcome:
        del payload
        observed_budgets.append(context.retry_attempt())
        # An outage on a physical final attempt must still be deferred.
        return TerminalFailureWork(
            error={"kind": "ebay_acquisition_failure"},
            result={"acquisition": {"stopping_condition": "transport_failure"}},
        )

    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=network_kind, payload_schema_version=1),
                component=Component(ComponentId(("carl", "test", "outage")), 1, handle),
                payload_type=Payload,
                handler=handle,
            ),
        )
    )
    async with Database.managed(tmp_path / "database.sqlite3", initialize=True) as database:
        monitor = ConnectivityMonitor(database, lambda: str(uuid4()), lambda: now, probe)
        services = WorkerRuntimeServices(
            new_identifier=lambda: str(uuid4()),
            utc_now_ns=lambda: now,
            monotonic_ns=lambda: now,
            code_provenance=_async_provenance,
            invocation=lambda: {},
            connectivity_monitor=monitor,
        )
        for identifier, kind in (("network", network_kind), ("offline", offline_kind)):
            await database.enqueue_work(
                _definition(identifier=identifier, kind=kind),
                _requester(identifier),
                event_identifier=str(uuid4()),
                enqueued_at_utc_ns=now,
            )

        async def claim(kind: tuple[str, ...]):
            return await database.claim_work(
                supported_capabilities=(WorkCapability(kind=kind, payload_schema_version=1),),
                worker_identifier="worker",
                lease_token=str(uuid4()),
                lease_duration_ns=_settings().lease_duration_ns,
                utc_now_ns=lambda: now,
                event_identifier=str(uuid4()),
            )

        first = await claim(network_kind)
        assert first.lease is not None
        outcome = await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=services,
            lease=first.lease,
        )
        assert isinstance(outcome, RetryWork)
        assert probes == 1
        assert (await claim(network_kind)).lease is None
        assert (await claim(offline_kind)).lease is not None
        assert not await monitor.available()
        assert probes == 1
        now += PROBE_INTERVAL_NS
        assert not await monitor.available()
        assert probes == 2
        # Reopening the monitor retains the outage across process lifetimes.
        replacement = ConnectivityMonitor(database, lambda: str(uuid4()), lambda: now, probe)
        assert not await replacement.available()
        assert probes == 2
        online = True
        now += PROBE_INTERVAL_NS
        assert await replacement.available()
        second = await claim(network_kind)
        assert second.lease is not None
        assert second.lease.attempt == 2
        await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=services,
            lease=second.lease,
        )
        assert observed_budgets == [1, 1]


@pytest.mark.anyio
async def test_v9_migration_retains_work_and_adds_connectivity_state(tmp_path: Path) -> None:
    path = tmp_path / "database.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="retained", kind=("carl", "ebay", "collect", "item")),
            _requester("retained"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=1,
        )
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("DROP TABLE network_connectivity")
        connection.executemany(
            "UPDATE schema_metadata SET value = ? WHERE key = ?",
            ((value, key) for key, value in Database._v9_metadata().items()),
        )
        connection.commit()
    async with Database.managed(path) as database:
        await database.validate_schema()
        monitor = ConnectivityMonitor(database, lambda: str(uuid4()), lambda: 1)
        assert await monitor.available()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT id FROM work_items").fetchall() == [("retained",)]
        assert connection.execute("SELECT count(*) FROM network_connectivity").fetchone() == (0,)


@pytest.mark.anyio
async def test_probe_covers_dns_failure_and_accepts_one_reachable_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted: list[str] = []
    online = False

    class Stream:
        async def aclose(self) -> None:
            pass

    async def connect(host: str, port: int) -> Stream:
        attempted.append(host)
        assert port == 443
        if online and host == "dns.google":
            return Stream()
        raise OSError("synthetic DNS failure")

    monkeypatch.setattr(anyio, "connect_tcp", connect)
    result = await probe_internet_connectivity()
    assert result["reachable"] is False
    assert set(attempted) == {"one.one.one.one", "dns.google"}
    online = True
    assert (await probe_internet_connectivity())["reachable"] is True


@pytest.mark.anyio
async def test_concurrent_failures_share_one_connectivity_probe(tmp_path: Path) -> None:
    probes = 0

    async def probe() -> dict[str, JsonValue]:
        nonlocal probes
        probes += 1
        await anyio.sleep(0.05)
        return {"reachable": False}

    async with Database.managed(tmp_path / "database.sqlite3", initialize=True) as database:
        monitors = [
            ConnectivityMonitor(database, lambda: str(uuid4()), lambda: 1, probe) for _ in range(5)
        ]
        outcomes: list[bool] = []

        async def assess(monitor: ConnectivityMonitor) -> None:
            outcomes.append(await monitor.available(after_failure=True))

        async with anyio.create_task_group() as task_group:
            for monitor in monitors:
                task_group.start_soon(assess, monitor)
        assert outcomes == [False] * 5
        assert probes == 1


@pytest.mark.anyio
async def test_crashed_probe_is_replaced_after_fenced_lease_expires(tmp_path: Path) -> None:
    now = 20_000_000_000

    async def probe() -> dict[str, JsonValue]:
        return {"reachable": True}

    async with Database.managed(tmp_path / "database.sqlite3", initialize=True) as database:
        async with database._connections.writer() as connection:
            await connection.execute(
                """
                INSERT INTO network_connectivity(
                    singleton, paused, next_probe_utc_ns, probe_token, probe_expires_utc_ns
                ) VALUES (1, 1, 0, 'crashed-worker', 15000000000)
                """
            )
        replacement = ConnectivityMonitor(database, lambda: str(uuid4()), lambda: now, probe)
        assert await replacement.available()
        # Durable admission is reopened, not just this replacement's local state.
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT paused, probe_token FROM network_connectivity"
            )
            assert await cursor.fetchone() == (0, None)


@pytest.mark.anyio
@pytest.mark.parametrize("replacement", [False, True])
async def test_cancelled_probe_releases_only_its_token_and_retains_pause(
    tmp_path: Path,
    replacement: bool,
) -> None:
    now = 1_000_000_000
    async with Database.managed(tmp_path / "database.sqlite3", initialize=True) as database:
        with anyio.CancelScope() as scope:

            async def probe() -> dict[str, JsonValue]:
                if replacement:
                    async with database._connections.writer() as connection:
                        await connection.execute(
                            "UPDATE network_connectivity SET probe_token = ?, probe_expires_utc_ns = ?",
                            ("replacement", now + 15_000_000_000),
                        )
                scope.cancel()
                await anyio.lowlevel.checkpoint()
                raise AssertionError("Cancelled probe returned")

            monitor = ConnectivityMonitor(database, lambda: "cancelled", lambda: now, probe)
            await monitor.available(after_failure=True)

        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT paused, probe_token, probe_expires_utc_ns FROM network_connectivity"
            )
            assert await cursor.fetchone() == (
                (1, "replacement", now + 15_000_000_000) if replacement else (1, None, None)
            )

        calls: list[str] = []

        async def healthy_probe() -> dict[str, JsonValue]:
            calls.append("probed")
            return {"reachable": True}

        if not replacement:
            monitor = ConnectivityMonitor(database, lambda: "new", lambda: now, healthy_probe)
            assert await monitor.available()
            assert calls == ["probed"]
