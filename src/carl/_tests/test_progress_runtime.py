"""Durable progress waits are exercised through the actual production runtime."""

# Fenced fixture publication and bounded ownership inspection are intentional.
# pyright: reportPrivateUsage=false

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast, override
from uuid import uuid4

import anyio
import pytest

from carl import work_runtime
from carl._tests.test_pipeline import _application, _request
from carl._tests.test_pipeline_search_occurrences import _attempt, _checkpoint
from carl._tests.test_production_fairness import _context, _Harness
from carl._tests.test_stage_execution import _COMPONENT
from carl._tests.test_worker import _provenance
from carl.coordinator_stages import (
    COORDINATOR_CPU_STAGE_KIND,
    CoordinatorWorkReference,
    build_coordinator_stage_registry,
)
from carl.core.models import JsonValue, NamedOutput, RecordDraft, StrictModel
from carl.core.pipeline import SEARCH_PIPELINE_WORK_KIND
from carl.core.resources import ResourceRequest
from carl.core.stages import StageAttemptContext, StageDefinition, WaitWork
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkDefinition, WorkRequester
from carl.core.work_progress import WorkProgressWatch
from carl.core.worker import AttemptContext, CompletedWork, RetryWork, WorkOutcome
from carl.io.sqlite import Database
from carl.io.stage_dispatcher import StageDispatcher
from carl.io.stage_publication import ChildOperationDraft, StageHandlerResult, StagePublication
from carl.io.stage_workers import StageHandler, StageHandlerRegistry
from carl.io.stages import stage_manager
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


class _Parent(StrictModel):
    children: tuple[str, ...]
    required_revision: int = 0


@dataclass(frozen=True)
class _AsPublication(StageHandler):
    kind: tuple[str, ...]
    schema_version: int
    original: StageHandler

    @override
    async def execute(self, payload: JsonValue, context: StageAttemptContext) -> StageHandlerResult:
        result = await self.original.execute(payload, context)
        if not isinstance(result, WaitWork):
            return result
        witness = RecordDraft(
            identifier="large-watch-witness",
            kind=("test", "progress", "witness"),
            schema_version=1,
            value={"captured": True},
        )
        return StagePublication(
            result,
            (
                ChildOperationDraft(
                    identifier="large-watch-publication",
                    component=_COMPONENT,
                    configuration=None,
                    inputs=(),
                    records=(witness,),
                    artifacts=(),
                    outputs=(NamedOutput(name=("witness",), object_identifier=witness.identifier),),
                    result={"captured": True},
                    started_at_utc="2026-11-02T00:00:00+00:00",
                    ended_at_utc="2026-11-02T00:00:00+00:00",
                    duration_ns=0,
                ),
            ),
        )


@dataclass
class _ProgressRuntime:
    database: Database
    child: _Harness = field(default_factory=_Harness)
    calls: dict[str, list[AttemptContext]] = field(default_factory=dict)
    after_read: Callable[[str], Awaitable[None]] | None = None

    async def parent(self, payload: _Parent, context: AttemptContext) -> WorkOutcome:
        self.calls.setdefault(context.work_item_identifier, []).append(context)
        rows = await self.database.work_progress(payload.children)
        if payload.required_revision:
            ready = all(
                isinstance(row["result"], dict)
                and cast(dict[str, JsonValue], row["result"]).get("revision", 0)
                == payload.required_revision
                for row in rows
            )
        else:
            ready = all(
                row["state"] in {"completed", "terminal_failure", "cancelled"} for row in rows
            )
        if ready:
            record = RecordDraft(
                identifier=f"result-{context.work_item_identifier}",
                kind=("test", "progress", "completed"),
                schema_version=1,
                value={"parent": context.work_item_identifier},
            )
            return CompletedWork(
                records=(record,),
                outputs=(NamedOutput(name=("completed",), object_identifier=record.identifier),),
                result={"state": "completed"},
            )
        if self.after_read is not None:
            await self.after_read(context.work_item_identifier)
        return RetryWork(
            delay_ns=2_000_000_000,
            reason={"kind": "pipeline_continuation"},
            progress_watch=WorkProgressWatch(work_identifiers=payload.children),
            result={"state": "waiting"},
        )

    def install(self, monkeypatch: pytest.MonkeyPatch, directory: Path) -> None:
        self.child.install(monkeypatch, directory)
        monkeypatch.setattr(work_runtime, "build_coordinator_stage_registry", self.registry)

    def registry(self, database: Database, legacy: WorkHandlerRegistry) -> StageHandlerRegistry:
        replacement = WorkHandlerRegistry(
            tuple(
                TypedWorkHandler(handler.capability, handler.component, _Parent, self.parent)
                if handler.capability.kind == SEARCH_PIPELINE_WORK_KIND
                else handler
                for handler in legacy.handlers
            )
        )
        return build_coordinator_stage_registry(database, replacement, utc_now_ns=self.child.clock)

    async def enqueue_parent(
        self, identifier: str, *children: str, required_revision: int = 0, root: str | None = None
    ) -> WorkDefinition:
        definition = WorkDefinition(
            identifier=identifier,
            kind=SEARCH_PIPELINE_WORK_KIND,
            payload_schema_version=1,
            payload=_Parent(children=children, required_revision=required_revision).model_dump(
                mode="json"
            ),
            deduplication_identity=(identifier,),
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(
                    kind=SchedulingScopeKind.WORK_KIND, identity=SEARCH_PIPELINE_WORK_KIND
                ),
            ),
        )
        _ = await self.database.enqueue_work(
            definition,
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("test", "progress", "parent"),
                identifier=identifier,
                context={},
                scheduling=_context(f"workspace-{root or identifier}", root or identifier),
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=self.child.clock(),
        )
        return definition


async def _sleeping(database: Database, identifier: str) -> StageDefinition:
    with anyio.fail_after(15):
        while True:
            async with database._connections.reader() as connection:
                row = await (
                    await connection.execute(
                        "SELECT definition_json FROM work_stages WHERE work_item_id=? "
                        + "AND state='pending' AND wait_kind='dependency'",
                        (identifier,),
                    )
                ).fetchone()
            if row is not None:
                return StageDefinition.model_validate_json(str(row[0]))
            await anyio.sleep(0.01)


async def _terminal(database: Database, identifier: str) -> dict[str, JsonValue]:
    with anyio.fail_after(15):
        while True:
            row = (await database.work_progress((identifier,)))[0]
            if row["state"] not in {"pending", "leased", "cancelling"}:
                return row
            await anyio.sleep(0.01)


async def _finish_child(database: Database, identifier: str) -> None:
    async with stage_manager(database).admit(
        f"stage-{identifier}", component=_COMPONENT, provenance=_provenance(), invocation={}
    ) as admission:
        assert admission.execution is not None
        await admission.execution.apply(CompletedWork(result={"state": "completed"}))


def _passes(monkeypatch: pytest.MonkeyPatch, count: int = 4) -> anyio.Event:
    reached = anyio.Event()
    observed = 0
    original = StageDispatcher._recover

    async def recover(dispatcher: StageDispatcher) -> None:
        nonlocal observed
        await original(dispatcher)
        observed += 1
        if observed >= count:
            reached.set()

    monkeypatch.setattr(StageDispatcher, "_recover", recover)
    return reached


async def test_child_finishing_between_read_and_watch_registration_is_not_lost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        harness = _ProgressRuntime(database)
        harness.install(monkeypatch, tmp_path)
        _ = await harness.child.enqueue(
            database, "child", _context("child", "child"), register=True
        )
        _ = await harness.enqueue_parent("parent", "child")
        read, release = anyio.Event(), anyio.Event()

        async def after_read(_identifier: str) -> None:
            read.set()
            await release.wait()

        harness.after_read = after_read
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=("parent",), worker_count=1
        ):
            with anyio.fail_after(15):
                await read.wait()
                await _finish_child(database, "child")
                release.set()
            assert (await _terminal(database, "parent"))["state"] == "completed"
            calls = harness.calls["parent"]
            assert len(calls) == 2
            assert {context.attempt for context in calls} == {1}
            assert len({context.operation_identifier for context in calls}) == 1
            async with database._connections.reader() as connection:
                assert await (
                    await connection.execute(
                        "SELECT count(*) FROM objects WHERE id='result-parent'"
                    )
                ).fetchone() == (1,)


async def test_unchanged_dependency_does_not_reexecute_or_hold_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        harness = _ProgressRuntime(database)
        harness.install(monkeypatch, tmp_path)
        _ = await harness.child.enqueue(
            database, "child", _context("child", "child"), register=True
        )
        _ = await harness.enqueue_parent("parent", "child")
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=("parent",), worker_count=1
        ):
            stage = await _sleeping(database, "parent")
            before = await database.fair_admissions(stage_manager(database).resources.scope)
            calls = len(harness.calls["parent"])
            passes = _passes(monkeypatch)
            harness.child.clock.advance(60)
            with anyio.fail_after(10):
                await passes.wait()
            assert len(harness.calls["parent"]) == calls == 1
            assert await database.stage(stage.identifier) == stage
            assert await database.fair_admissions(stage_manager(database).resources.scope) == before
            assert await database.resource_occupancy(stage_manager(database).resources.scope) == {}
            assert (await database.work("parent"))["attempt"] == 1


async def test_shared_child_wakes_both_canonical_consumer_roots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        harness = _ProgressRuntime(database)
        harness.install(monkeypatch, tmp_path)
        _ = await harness.child.enqueue(
            database, "shared", _context("workspace-A", "A"), register=True
        )
        _ = await harness.enqueue_parent("parent-A", "shared", root="A")
        _ = await harness.enqueue_parent("parent-B", "shared", root="B")
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=("parent-A", "parent-B"), worker_count=2
        ):
            _ = await _sleeping(database, "parent-A")
            _ = await _sleeping(database, "parent-B")
            await _finish_child(database, "shared")
            for identifier in ("parent-A", "parent-B"):
                assert (await _terminal(database, identifier))["state"] == "completed"
                assert len(harness.calls[identifier]) == 2
                assert {context.attempt for context in harness.calls[identifier]} == {1}
            turns = await database.fair_admissions(stage_manager(database).resources.scope)
            shared = [turn for turn in turns if turn.work_item_identifier == "shared"]
            assert len(shared) == 1 and shared[0].scheduling.root_request_identifier == "A"


async def test_coalesced_duplicate_checkpoints_wake_once_while_source_stays_leased(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        harness = _ProgressRuntime(database)
        harness.install(monkeypatch, tmp_path)
        _ = await harness.child.enqueue(
            database, "source", _context("source", "source"), register=True
        )
        _ = await harness.enqueue_parent("parent", "source", required_revision=2)
        async with stage_manager(database).admit(
            "stage-source", component=_COMPONENT, provenance=_provenance(), invocation={}
        ) as source:
            assert source.execution is not None
            execution = source.execution
            async with work_runtime.managed_worker_pool(
                database, tmp_path, eligible_identifiers=("parent",), worker_count=1
            ):
                _ = await _sleeping(database, "parent")
                assert await database.resource_occupancy(
                    stage_manager(database).resources.scope
                ) == {"cpu": 1}
                blocked, release = anyio.Event(), anyio.Event()
                original = StageDispatcher._progress

                async def paused_progress(dispatcher: StageDispatcher) -> bool:
                    blocked.set()
                    await release.wait()
                    return await original(dispatcher)

                monkeypatch.setattr(StageDispatcher, "_progress", paused_progress)
                with anyio.fail_after(10):
                    await blocked.wait()

                async def checkpoint(revision: int) -> None:
                    await database.publish_leased_operation_checkpoint(
                        work_item_identifier="source",
                        lease_token=execution.lease.token,
                        worker_identifier=execution.lease.work_lease.worker_identifier,
                        utc_now_ns=harness.child.clock,
                        operation_id=execution.lease.definition.operation_identifier,
                        records=(),
                        artifacts=(),
                        outputs=(),
                        checkpoint_result={"revision": revision},
                    )

                await checkpoint(1)
                await checkpoint(2)
                revision = await database.work_progress_watermark()
                await checkpoint(2)
                assert await database.work_progress_watermark() == revision
                release.set()
                assert (await _terminal(database, "parent"))["state"] == "completed"
                assert len(harness.calls["parent"]) == 2
                assert (await database.work("source"))["state"] == "leased"
                assert (await database.operation(execution.lease.definition.operation_identifier))[
                    "state"
                ] == "started"
            await execution.apply(CompletedWork(result={"state": "completed"}))


async def test_large_publication_watch_build_releases_cpu_before_background_normalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        harness = _ProgressRuntime(database)
        harness.install(monkeypatch, tmp_path)
        sources = tuple(f"source-{index:03}" for index in range(300))
        for start in range(0, len(sources), 128):
            async with database.transaction():
                for identifier in sources[start : start + 128]:
                    _ = await harness.child.enqueue(
                        database,
                        identifier,
                        _context("sources", "sources"),
                        kind=("test", "unsupported_source"),
                    )
        parent = await harness.enqueue_parent("parent", *sources)
        original_registry = harness.registry

        def publication_registry(
            database: Database, legacy: WorkHandlerRegistry
        ) -> StageHandlerRegistry:
            registry = original_registry(database, legacy)
            return StageHandlerRegistry(
                registry.adapters,
                tuple(
                    _AsPublication(handler.kind, handler.schema_version, handler)
                    for handler in registry.handlers
                ),
            )

        monkeypatch.setattr(work_runtime, "build_coordinator_stage_registry", publication_registry)
        await database.register_stage(
            StageDefinition(
                identifier="large-cpu-stage",
                work_item_identifier=parent.identifier,
                operation_identifier="large-parent-operation",
                kind=COORDINATOR_CPU_STAGE_KIND,
                payload=CoordinatorWorkReference(
                    work_item_identifier=parent.identifier,
                    work_kind=parent.kind,
                    payload_schema_version=parent.payload_schema_version,
                ).model_dump(mode="json"),
                resources=(ResourceRequest(name="cpu"),),
            ),
            created_at_utc_ns=harness.child.clock(),
        )
        building, release = anyio.Event(), anyio.Event()
        original_progress = StageDispatcher._progress

        async def pause_build(dispatcher: StageDispatcher) -> bool:
            async with database._connections.reader() as connection:
                row = await (
                    await connection.execute(
                        "SELECT 1 FROM progress_wait_builds WHERE work_id='parent' AND state='building'"
                    )
                ).fetchone()
            if row is not None:
                building.set()
                await release.wait()
            return await original_progress(dispatcher)

        monkeypatch.setattr(StageDispatcher, "_progress", pause_build)
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=("parent",), worker_count=1
        ):
            with anyio.fail_after(20):
                await building.wait()
                while await database.resource_occupancy(stage_manager(database).resources.scope):
                    await anyio.sleep(0.01)
            assert len(harness.calls["parent"]) == 1
            assert (await database.get_record("large-watch-witness"))[2] == {"captured": True}
            assert (await database.operation("large-watch-publication"))["state"] == "completed"
            release.set()
            with anyio.fail_after(15):
                while True:
                    async with database._connections.reader() as connection:
                        row = await (
                            await connection.execute(
                                "SELECT state FROM progress_wait_builds WHERE work_id='parent' AND stage_id IS NOT NULL"
                            )
                        ).fetchone()
                    if row == ("armed",):
                        break
                    await anyio.sleep(0.01)
            assert len(harness.calls["parent"]) == 1
            assert await database.resource_occupancy(stage_manager(database).resources.scope) == {}


async def test_cancelled_consumer_stays_dead_and_restart_recovers_missed_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        harness = _ProgressRuntime(database)
        harness.install(monkeypatch, tmp_path)
        _ = await harness.child.enqueue(
            database, "shared", _context("child", "child"), register=True
        )
        _ = await harness.enqueue_parent("cancelled", "shared")
        _ = await harness.enqueue_parent("survivor", "shared")
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=("cancelled", "survivor"), worker_count=2
        ):
            _ = await _sleeping(database, "cancelled")
            _ = await _sleeping(database, "survivor")
        _ = await database.cancel_work(
            "cancelled",
            reason="fixture cancel",
            caller="test",
            requested_at_utc_ns=harness.child.clock(),
            include_descendants=False,
        )
        await _finish_child(database, "shared")
        original_operation = harness.calls["survivor"][0].operation_identifier
        original_cancelled_calls = len(harness.calls["cancelled"])
    async with Database.managed(path) as database:
        harness.database = database
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=("cancelled", "survivor"), worker_count=2
        ):
            assert (await _terminal(database, "survivor"))["state"] == "completed"
            assert (await database.work("cancelled"))["state"] == "cancelled"
            assert len(harness.calls["cancelled"]) == original_cancelled_calls
            assert {context.attempt for context in harness.calls["survivor"]} == {1}
            assert {context.operation_identifier for context in harness.calls["survivor"]} == {
                original_operation
            }


def _occurrences(count: int) -> tuple[RecordDraft, ...]:
    return tuple(
        RecordDraft(
            identifier=f"occurrence-{index}",
            kind=("carl", "facebook", "search_listing_occurrence"),
            schema_version=1,
            value={
                "listing_identifier": str(1_000_000_000 + index),
                "search_run_identifier": "run",
                "page_ordinal": 1,
                "edge_index": index,
                "original": {"marketplace_listing_title": f"Scope {index}"},
            },
        )
        for index in range(count)
    )


async def _selected(database: Database, identifier: str, count: int) -> dict[str, JsonValue]:
    with anyio.fail_after(20):
        while True:
            result = (await database.work_progress((identifier,)))[0]["result"]
            if isinstance(result, dict):
                result = cast(dict[str, JsonValue], result)
                if result.get("selected_listings") == count:
                    return result
            await anyio.sleep(0.01)


async def test_partial_page_checkpoint_wakes_actual_pipeline_before_search_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        fixture = _Harness()
        fixture.install(monkeypatch, tmp_path)
        source = await _attempt(database, "source", "facebook")
        application = _application(database, tmp_path)
        requested = await application.request_search_pipeline(
            _request("source", stop_after="details")
        )
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=(requested.work_identifier,), worker_count=1
        ):
            _ = await _sleeping(database, requested.work_identifier)
            assert await database.resource_occupancy(stage_manager(database).resources.scope) == {}
            await _checkpoint(database, source, _occurrences(1))
            _ = await _selected(database, requested.work_identifier, 1)
            assert (await database.work("source"))["state"] == "leased"
            assert (await database.operation(source.operation_identifier))["state"] == "started"
            assert (await database.work(requested.work_identifier))["attempt"] == 1


async def test_retained_more_than_fifty_cards_continue_without_a_future_source_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        fixture = _Harness()
        fixture.install(monkeypatch, tmp_path)
        source = await _attempt(database, "source", "facebook")
        await _checkpoint(database, source, _occurrences(61))
        application = _application(database, tmp_path)
        requested = await application.request_search_pipeline(
            _request(
                "source", stop_after="details", maximum_items=100, maximum_inflight_listings=100
            )
        )
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=(requested.work_identifier,), worker_count=1
        ):
            result = await _selected(database, requested.work_identifier, 61)
            assert result["candidates_examined"] == 61
            assert (await database.work("source"))["state"] == "leased"
            assert (await database.work(requested.work_identifier))["attempt"] == 1
            _ = await _sleeping(database, requested.work_identifier)
            assert await database.resource_occupancy(stage_manager(database).resources.scope) == {}
