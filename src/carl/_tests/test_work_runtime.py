"""Independent durable-worker runtime ownership."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import anyio
import pytest

from carl import cli, work_runtime
from carl.core.facebook_images import (
    COLLECT_IMAGE_WORK_KIND,
    COLLECT_IMAGE_WORK_SCHEMA_VERSION,
    IMAGE_SESSION_MAXIMUM_ACTIVE,
    LEGACY_COLLECT_IMAGE_WORK_SCHEMA_VERSION,
    image_session_work_constraint,
)
from carl.core.facebook_refresh import (
    LEGACY_REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION,
    REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION,
    REFRESH_SEARCH_WORK_KIND,
)
from carl.core.facebook_work import (
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_SEARCH_WORK_KIND,
    LEGACY_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    PREVIOUS_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    CollectSearchPayload,
)
from carl.core.item_analysis import (
    ANALYSIS_MAXIMUM_ACTIVE,
    ANALYZE_ITEM_WORK_KIND,
    ANALYZE_ITEM_WORK_SCHEMA_VERSION,
    LEGACY_ANALYZE_ITEM_WORK_SCHEMA_VERSION,
    analysis_work_constraints,
)
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.core.worker import AttemptContext, TerminalFailureWork
from carl.facebook_routed_workers import build_routed_facebook_worker_registry
from carl.io.configuration import ConfigurationFailure
from carl.io.paths import CarlDirectories
from carl.io.sqlite import Database
from carl.work_runtime import managed_work_runtime


class _ConfigurationFailureDatabase:
    def __init__(self, path: Path):
        self.path = path

    async def supersede_constraints(self, **_kwargs: Any) -> None:
        pass


@pytest.mark.anyio
async def test_routed_search_configuration_failure_is_terminal_work_not_pool_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _ConfigurationFailureDatabase(tmp_path / "carl.sqlite3")
    directories = CarlDirectories(
        config=tmp_path / "config",
        data=tmp_path / "data",
        cache=tmp_path / "cache",
        state=tmp_path / "state",
        runtime=tmp_path / "runtime",
    )

    def fail_configuration(_path: Path) -> None:
        raise ConfigurationFailure("configuration_unavailable")

    monkeypatch.setattr("carl.facebook_routed_workers.load_configuration", fail_configuration)
    registry = build_routed_facebook_worker_registry(
        database=cast(Database, cast(object, database)),
        directories=directories,
        new_identifier=lambda: "identifier",
    )
    handler = next(
        handler
        for handler in registry.handlers
        if handler.capability
        == WorkCapability(
            kind=COLLECT_SEARCH_WORK_KIND,
            payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
        )
    )
    payload = CollectSearchPayload.model_validate(
        {
            "request": {
                "query": "telescope",
                "location": {"kind": "facebook_location", "identifier": "456"},
                "radius": {"value": 60, "unit": "miles"},
            },
            "traversal": {"maximum_pages": 1},
            "routing": ("proton", "personal", "carl"),
        }
    )

    outcome = await handler.execute(
        payload.model_dump(mode="json"),
        AttemptContext(
            work_item_identifier="search-work",
            lease_token="lease",
            worker_identifier="worker",
            attempt=1,
            operation_identifier="operation",
        ),
    )

    assert isinstance(outcome, TerminalFailureWork)
    assert outcome.error == {
        "kind": "configuration_failure",
        "code": "configuration_unavailable",
        "decision": "terminal",
    }


def test_monitor_work_option_owns_worker_pool_for_monitor_lifetime(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    database_path = tmp_path / "carl.sqlite3"

    async def initialize() -> None:
        async with Database.managed(database_path, initialize=True):
            pass

    anyio.run(initialize, backend="trio")
    lifecycle: list[str] = []

    @asynccontextmanager
    async def fake_worker_pool(*args: Any, **kwargs: Any) -> AsyncGenerator[None]:
        del args, kwargs
        lifecycle.append("started")
        try:
            yield
        finally:
            lifecycle.append("stopped")

    monkeypatch.setattr(cli, "managed_worker_pool", fake_worker_pool)
    cli.monitor(database=database_path, maximum_rows=1, once=True, work=True)

    assert lifecycle == ["started", "stopped"]


@pytest.mark.anyio
async def test_work_runtime_starts_and_cancels_cleanly(tmp_path: Path) -> None:
    database_path = tmp_path / "carl.sqlite3"
    async with Database.managed(database_path, initialize=True):
        pass

    async with managed_work_runtime(database_path, tmp_path) as runtime:
        assert {capability.kind for capability in runtime.registry.capabilities} == {
            ("carl", "ebay", "collect", "description"),
            ("carl", "ebay", "collect", "image"),
            ("carl", "ebay", "collect", "item"),
            ("carl", "ebay", "collect", "search"),
            ("carl", "ebay", "extract", "item"),
            ("carl", "ebay", "work", "analyze_item"),
            ("carl", "ebay", "work", "refresh_search"),
            ("carl", "facebook", "work", "analyze_item"),
            ("carl", "facebook", "work", "collect_image"),
            ("carl", "facebook", "work", "collect_item"),
            ("carl", "facebook", "work", "collect_search"),
            ("carl", "facebook", "work", "extract_image"),
            ("carl", "facebook", "work", "extract_item"),
            ("carl", "facebook", "work", "listing_details"),
            ("carl", "facebook", "work", "refresh_search"),
            ("carl", "facebook", "work", "request_missing_listing_analyses"),
        }
        assert {
            capability.payload_schema_version
            for capability in runtime.registry.capabilities
            if capability.kind == ANALYZE_ITEM_WORK_KIND
        } == {
            LEGACY_ANALYZE_ITEM_WORK_SCHEMA_VERSION,
            ANALYZE_ITEM_WORK_SCHEMA_VERSION,
        }
        assert {
            capability.payload_schema_version
            for capability in runtime.registry.capabilities
            if capability.kind == COLLECT_IMAGE_WORK_KIND
        } == {
            LEGACY_COLLECT_IMAGE_WORK_SCHEMA_VERSION,
            COLLECT_IMAGE_WORK_SCHEMA_VERSION,
        }
        assert {
            capability.payload_schema_version
            for capability in runtime.registry.capabilities
            if capability.kind == COLLECT_SEARCH_WORK_KIND
        } == {
            LEGACY_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
            PREVIOUS_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
            COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
        }
        assert {
            capability.payload_schema_version
            for capability in runtime.registry.capabilities
            if capability.kind == REFRESH_SEARCH_WORK_KIND
        } == {
            LEGACY_REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION,
            REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION,
        }


@pytest.mark.anyio
async def test_runtime_joins_workers_and_watchers_before_closing_shared_transports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lifecycle: list[str] = []
    worker_started = anyio.Event()
    watcher_started = anyio.Event()

    class SharedManager:
        watcher_task_group: Any = None

        async def __aenter__(self) -> "SharedManager":
            lifecycle.append("transport_opened")
            return self

        async def __aexit__(self, *args: Any) -> None:
            del args
            assert "worker_closed" in lifecycle
            assert "watcher_closed" in lifecycle
            lifecycle.append("transport_closed")

    manager = SharedManager()

    async def watcher() -> None:
        watcher_started.set()
        try:
            await anyio.sleep_forever()
        finally:
            with anyio.CancelScope(shield=True):
                await anyio.lowlevel.checkpoint()
                assert "transport_closed" not in lifecycle
                lifecycle.append("watcher_closed")

    async def worker_pool(**kwargs: Any) -> None:
        del kwargs
        assert "transport_opened" in lifecycle
        manager.watcher_task_group.start_soon(watcher)
        worker_started.set()
        try:
            await anyio.sleep_forever()
        finally:
            with anyio.CancelScope(shield=True):
                await anyio.lowlevel.checkpoint()
                assert "transport_closed" not in lifecycle
                lifecycle.append("worker_closed")

    async def prepare_constraints(*args: Any) -> None:
        del args

    monkeypatch.setattr(work_runtime, "SharedProtonWireproxyManager", lambda: manager)
    monkeypatch.setattr(work_runtime, "run_worker_pool", worker_pool)
    monkeypatch.setattr(work_runtime, "prepare_worker_constraints", prepare_constraints)

    with anyio.fail_after(2):
        async with (
            Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database,
            work_runtime.managed_worker_pool(database, tmp_path),
        ):
            await worker_started.wait()
            await watcher_started.wait()

    assert lifecycle[0] == "transport_opened"
    assert lifecycle[-1] == "transport_closed"


@pytest.mark.anyio
async def test_analysis_concurrency_is_shared_database_state(tmp_path: Path) -> None:
    now = 100
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for constraint in analysis_work_constraints():
            await database.register_constraint(constraint, registered_at_utc_ns=now)
        for index in range(ANALYSIS_MAXIMUM_ACTIVE + 1):
            identifier = f"analysis-{index}"
            await database.enqueue_work(
                WorkDefinition(
                    identifier=identifier,
                    kind=ANALYZE_ITEM_WORK_KIND,
                    payload_schema_version=ANALYZE_ITEM_WORK_SCHEMA_VERSION,
                    payload={},
                    deduplication_identity=("test", identifier),
                    not_before_utc_ns=0,
                    scopes=(
                        SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                        SchedulingScope(
                            kind=SchedulingScopeKind.WORK_KIND,
                            identity=ANALYZE_ITEM_WORK_KIND,
                        ),
                    ),
                ),
                WorkRequester(
                    request_identifier=f"request-{index}",
                    kind=("test", "analysis"),
                    identifier=identifier,
                    context={},
                ),
                event_identifier=f"event-{index}",
                enqueued_at_utc_ns=now,
            )
        for index in range(ANALYSIS_MAXIMUM_ACTIVE):
            claim = await database.claim_work(
                supported_capabilities=(
                    WorkCapability(
                        kind=ANALYZE_ITEM_WORK_KIND,
                        payload_schema_version=ANALYZE_ITEM_WORK_SCHEMA_VERSION,
                    ),
                ),
                worker_identifier=f"process-worker-{index}",
                lease_token=f"lease-{index}",
                lease_duration_ns=1_000,
                utc_now_ns=lambda: now,
                event_identifier=f"claim-{index}",
            )
            assert claim.lease is not None

        blocked = await database.claim_work(
            supported_capabilities=(
                WorkCapability(
                    kind=ANALYZE_ITEM_WORK_KIND,
                    payload_schema_version=ANALYZE_ITEM_WORK_SCHEMA_VERSION,
                ),
            ),
            worker_identifier="another-process-worker",
            lease_token="blocked-lease",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: now,
            event_identifier="blocked-claim",
        )

        assert blocked.lease is None
        assert blocked.next_eligible_at_utc_ns == now + 1_000


@pytest.mark.anyio
async def test_image_session_concurrency_is_shared_database_state(tmp_path: Path) -> None:
    now = 100
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.register_constraint(
            image_session_work_constraint(), registered_at_utc_ns=now
        )
        for index in range(IMAGE_SESSION_MAXIMUM_ACTIVE + 1):
            identifier = f"image-{index}"
            _ = await database.enqueue_work(
                WorkDefinition(
                    identifier=identifier,
                    kind=COLLECT_IMAGE_WORK_KIND,
                    payload_schema_version=COLLECT_IMAGE_WORK_SCHEMA_VERSION,
                    payload={},
                    deduplication_identity=("test", identifier),
                    not_before_utc_ns=0,
                    scopes=(
                        SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                        SchedulingScope(
                            kind=SchedulingScopeKind.WORK_KIND,
                            identity=COLLECT_IMAGE_WORK_KIND,
                        ),
                    ),
                ),
                WorkRequester(
                    request_identifier=f"request-{index}",
                    kind=("test", "image"),
                    identifier=identifier,
                    context={},
                ),
                event_identifier=f"event-{index}",
                enqueued_at_utc_ns=now,
            )
        capability = WorkCapability(
            kind=COLLECT_IMAGE_WORK_KIND,
            payload_schema_version=COLLECT_IMAGE_WORK_SCHEMA_VERSION,
        )
        for index in range(IMAGE_SESSION_MAXIMUM_ACTIVE):
            claimed = await database.claim_work(
                supported_capabilities=(capability,),
                worker_identifier=f"process-worker-{index}",
                lease_token=f"lease-{index}",
                lease_duration_ns=1_000,
                utc_now_ns=lambda: now,
                event_identifier=f"claim-{index}",
            )
            assert claimed.lease is not None

        blocked = await database.claim_work(
            supported_capabilities=(capability,),
            worker_identifier="second-process-worker",
            lease_token="blocked-lease",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: now,
            event_identifier="blocked-claim",
        )

        assert blocked.lease is None
        assert blocked.next_eligible_at_utc_ns == now + 1_000
