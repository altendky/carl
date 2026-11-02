"""Fair admission is exercised through the actual managed production dispatcher."""

# Bounded fake phases replace provider work, while ownership/admission stay real.
# pyright: reportPrivateUsage=false

from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from time import time_ns
from typing import cast
from uuid import uuid4

import anyio
import httpx
import pytest

from carl import work_runtime
from carl._tests.test_dataset_setup_stages import _Engine
from carl._tests.test_stage_execution import _COMPONENT
from carl._tests.test_worker import _provenance
from carl.core.ebay_items import COLLECT_EBAY_ITEM_WORK_KIND
from carl.core.facebook_images import COLLECT_IMAGE_WORK_KIND, COLLECT_IMAGE_WORK_SCHEMA_VERSION
from carl.core.fair_scheduling import (
    FairDispatchCursor,
    FairDispatchPage,
    SchedulingContext,
    SchedulingServiceClass,
)
from carl.core.models import CodeProvenance, JsonValue, StrictModel
from carl.core.resources import ResourceRequest
from carl.core.stages import StageAttemptContext, StageDefinition
from carl.core.travel_data import TravelDataSetupRequest, travel_data_work
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.core.work_dispatch import DispatchCandidate
from carl.core.worker import CompletedWork
from carl.ebay_item_workers import EbayItemWorkerDependencies
from carl.ebay_session_stages import EbaySessionStageDependencies
from carl.facebook_session_stages import FacebookSessionStageDependencies
from carl.facebook_stateless_stages import FacebookStatelessStageDependencies
from carl.io.dataset_setup import DatasetSetupEngine
from carl.io.paths import CarlDirectories
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler
from carl.io.stages import StageManager, stage_manager
from carl.io.travel_data import TravelDataManager
from carl.session_stages import build_session_stage_registry
from carl.stateless_stages import build_stateless_stage_registry

pytestmark = pytest.mark.anyio
_PHASE = ("test", "fairness", "phase")


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@dataclass
class _Clock:
    offset_ns: int = 0

    def __call__(self) -> int:
        # Advance aging deliberately while ordinary short admission cooldowns
        # still become eligible during the actual runtime's polling loop.
        return time_ns() + self.offset_ns

    def advance(self, seconds: int) -> None:
        self.offset_ns += seconds * 1_000_000_000


class _Phase(StrictModel):
    work_identifier: str


def _context(
    workspace: str,
    root: str,
    service_class: SchedulingServiceClass = SchedulingServiceClass.BACKGROUND,
) -> SchedulingContext:
    return SchedulingContext(
        root_request_identifier=root, workspace_identifier=workspace, service_class=service_class
    )


@dataclass
class _Harness:
    clock: _Clock = field(default_factory=_Clock)
    order: list[str] = field(default_factory=list)
    owners: dict[str, SchedulingContext] = field(default_factory=dict)
    gates: dict[str, anyio.Event] = field(default_factory=dict)
    entered: dict[str, anyio.Event] = field(default_factory=dict)
    pause_after: int | None = None
    paused: anyio.Event = field(default_factory=anyio.Event)
    resume: anyio.Event = field(default_factory=anyio.Event)

    def initial(self, work: WorkDefinition, identifier: str, operation: str) -> StageDefinition:
        assert isinstance(work.payload, dict)
        payload = cast(dict[str, JsonValue], work.payload)
        resource, units = payload.get("resource", "cpu"), payload.get("units", 1)
        assert isinstance(resource, str) and isinstance(units, int)
        return StageDefinition(
            identifier=identifier,
            work_item_identifier=work.identifier,
            operation_identifier=operation,
            kind=_PHASE,
            payload=_Phase(work_identifier=work.identifier).model_dump(mode="json"),
            resources=(ResourceRequest(name=resource, units=units),),
        )

    async def phase(self, payload: _Phase, context: StageAttemptContext) -> CompletedWork:
        assert payload.work_identifier == context.work_item_identifier
        self.order.append(payload.work_identifier)
        event = self.entered.get(payload.work_identifier)
        if event is not None:
            event.set()
        if self.pause_after is not None and len(self.order) == self.pause_after:
            self.paused.set()
            await self.resume.wait()
        gate = self.gates.get(payload.work_identifier)
        if gate is not None:
            await gate.wait()
        return CompletedWork(result={"completed": payload.work_identifier})

    def install(self, monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
        def build(
            *, facebook: FacebookStatelessStageDependencies, ebay: EbayItemWorkerDependencies
        ) -> StageHandlerRegistry:
            original = build_stateless_stage_registry(facebook=facebook, ebay=ebay)
            return StageHandlerRegistry(
                tuple(
                    replace(
                        adapter,
                        initial_stage=self.initial,
                        initial_resources=(ResourceRequest(name="cpu"),),
                    )
                    if adapter.capability.kind == COLLECT_IMAGE_WORK_KIND
                    and adapter.capability.payload_schema_version
                    == COLLECT_IMAGE_WORK_SCHEMA_VERSION
                    else adapter
                    for adapter in original.adapters
                ),
                (*original.handlers, TypedStageHandler(_PHASE, 1, _Phase, self.phase)),
            )

        async def provenance(_root: Path) -> CodeProvenance:
            return _provenance()

        def manager(database: Database) -> StageManager:
            return replace(
                stage_manager(database),
                utc_now_ns=self.clock,
                lease_duration_ns=300_000_000_000,
                renewal_interval_ns=20_000_000,
            )

        monkeypatch.setattr(work_runtime, "build_stateless_stage_registry", build)
        monkeypatch.setattr(work_runtime, "collect_code_provenance_async", provenance)
        monkeypatch.setattr(work_runtime, "stage_manager", manager)
        monkeypatch.setattr(work_runtime, "time_ns", self.clock)
        monkeypatch.setattr(
            work_runtime,
            "user_directories",
            lambda: CarlDirectories(
                config=root / "config",
                data=root,
                cache=root / "cache",
                state=root / "state",
                runtime=root / "runtime",
            ),
        )

    async def enqueue(
        self,
        database: Database,
        identifier: str,
        scheduling: SchedulingContext,
        *,
        priority: int = 0,
        resource: str = "cpu",
        units: int = 1,
        register: bool = False,
        kind: tuple[str, ...] = COLLECT_IMAGE_WORK_KIND,
    ) -> WorkDefinition:
        schema = COLLECT_IMAGE_WORK_SCHEMA_VERSION if kind == COLLECT_IMAGE_WORK_KIND else 1
        definition = WorkDefinition(
            identifier=identifier,
            kind=kind,
            payload_schema_version=schema,
            payload={"resource": resource, "units": units},
            deduplication_identity=(identifier,),
            priority=priority,
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
            ),
        )
        result = await database.enqueue_work(
            definition,
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("test", "fairness"),
                identifier=scheduling.root_request_identifier,
                context={},
                scheduling=scheduling,
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=self.clock(),
        )
        assert result.created
        self.owners[identifier] = scheduling
        if register:
            await database.register_stage(
                self.initial(definition, f"stage-{identifier}", f"operation-{identifier}"),
                created_at_utc_ns=self.clock(),
            )
        return definition


async def _wait_count(harness: _Harness, count: int) -> None:
    with anyio.fail_after(15):
        while len(harness.order) < count:
            await anyio.sleep(0.01)


async def _class_failure_state(database: Database, harness: _Harness) -> dict[str, object]:
    result: dict[str, object] = {"order": list(harness.order)}
    async with database._connections.reader() as connection:
        for table in ("fair_admissions", "fair_lanes", "fair_offers", "fair_ready"):
            result[table] = await (
                await connection.execute(f"SELECT * FROM {table} LIMIT 40")
            ).fetchall()
    return result


async def test_late_workspace_progress_is_independent_of_ten_thousand_old_children(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness()
    harness.install(monkeypatch, tmp_path)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        owner_a = _context("workspace-A", "root-A")
        _ = await harness.enqueue(database, "A-00000", owner_a)
        # Clone only the fixture's pending definitions and canonical ownership
        # in one transaction. Runtime selection and every actual claim remain
        # production code; no 10,000 fixture-side enqueue commits are needed.
        async with database._connections.writer() as connection:
            _ = await connection.execute(
                """WITH RECURSIVE children(n) AS (
                    VALUES(1) UNION ALL SELECT n+1 FROM children WHERE n<9999
                ) INSERT INTO work_items (
                    id,kind_parts_json,payload_schema_version,payload_json,
                    acquisition_resource_identity_json,deduplication_identity_json,
                    state,priority,eligible_at_utc_ns,created_at_utc_ns,attempt
                ) SELECT printf('A-%05d',n),w.kind_parts_json,w.payload_schema_version,
                    w.payload_json,NULL,json_array(printf('A-%05d',n)),
                    'pending',w.priority,w.eligible_at_utc_ns,w.created_at_utc_ns,0
                    FROM children CROSS JOIN work_items w WHERE w.id='A-00000'"""
            )
            _ = await connection.execute(
                """INSERT INTO work_scopes SELECT w.id,s.scope_kind,s.scope_identity_json
                    FROM work_items w CROSS JOIN work_scopes s
                    WHERE w.id LIKE 'A-%' AND w.id!='A-00000' AND s.work_item_id='A-00000'"""
            )
            _ = await connection.execute(
                """INSERT INTO work_scheduling
                    SELECT w.id,s.root_request_identifier,s.workspace_key,
                        s.service_class,s.scheduling_json
                    FROM work_items w CROSS JOIN work_scheduling s
                    WHERE w.id LIKE 'A-%' AND w.id!='A-00000' AND s.work_item_id='A-00000'"""
            )
        first = harness.entered["A-00000"] = anyio.Event()
        release = harness.gates["A-00000"] = anyio.Event()
        late = harness.entered["B-late"] = anyio.Event()
        harness.gates["B-late"] = anyio.Event()
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            with anyio.fail_after(15):
                await first.wait()
                _ = await harness.enqueue(database, "B-late", _context("workspace-B", "root-B"))
                release.set()
                await late.wait()
            # One old child may already have an equally recent root turn. A
            # further 9,998 siblings add no scheduling weight or payload reads.
            assert harness.order.index("B-late") <= 2
            assert len(harness.order) <= 3


async def test_sustained_interactive_load_preserves_background_turn_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness(pause_after=8)
    harness.install(monkeypatch, tmp_path)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        for index in range(12):
            _ = await harness.enqueue(
                database,
                f"interactive-{index:02}",
                _context("interactive", "interactive-root", SchedulingServiceClass.INTERACTIVE),
            )
        for index in range(4):
            _ = await harness.enqueue(
                database, f"background-{index:02}", _context("background", "background-root")
            )
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            with anyio.fail_after(15):
                await harness.paused.wait()
            classes = [harness.owners[identifier].service_class for identifier in harness.order]
            assert classes.count(SchedulingServiceClass.INTERACTIVE) == 6, (
                await _class_failure_state(database, harness)
            )
            assert classes.count(SchedulingServiceClass.BACKGROUND) == 2, (
                await _class_failure_state(database, harness)
            )
            assert all(
                SchedulingServiceClass.BACKGROUND in classes[start : start + 4]
                for start in range(5)
            ), await _class_failure_state(database, harness)


async def test_stale_interactive_page_cannot_override_a_committed_background_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness(pause_after=1)
    harness.install(monkeypatch, tmp_path)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        interactive = _context(
            "interactive", "interactive-root", SchedulingServiceClass.INTERACTIVE
        )
        for identifier in ("warm-0", "warm-1", "competing", "interactive-runtime"):
            _ = await harness.enqueue(database, identifier, interactive, register=True)
        _ = await harness.enqueue(
            database, "background-runtime", _context("background", "background-root"), register=True
        )
        manager = stage_manager(database)
        await database.enable_fair_scheduling(manager.resources.scope, harness.clock())

        async def competing_turn(identifier: str) -> None:
            async with manager.admit(
                f"stage-{identifier}", component=_COMPONENT, provenance=_provenance(), invocation={}
            ) as admission:
                assert admission.execution is not None
                await admission.execution.apply(CompletedWork(result={"completed": identifier}))

        await competing_turn("warm-0")
        await competing_turn("warm-1")
        captured, changed = anyio.Event(), anyio.Event()
        original = database.fair_dispatch_candidates

        async def stale_page(
            *,
            scope: str,
            supported_capabilities: Sequence[WorkCapability],
            now_utc_ns: int,
            limit: int,
            after: FairDispatchCursor | None = None,
            eligible_identifiers: Sequence[str] | None = None,
            excluded_identifiers: Sequence[str] = (),
        ) -> FairDispatchPage[DispatchCandidate]:
            page = await original(
                scope=scope,
                supported_capabilities=supported_capabilities,
                now_utc_ns=now_utc_ns,
                limit=limit,
                after=after,
                eligible_identifiers=eligible_identifiers,
                excluded_identifiers=excluded_identifiers,
            )
            if not captured.is_set():
                assert page.items[0].work_item_identifier == "interactive-runtime"
                assert {item.work_item_identifier for item in page.items} == {
                    "interactive-runtime",
                    "background-runtime",
                }
                captured.set()
                await changed.wait()
            return page

        monkeypatch.setattr(database, "fair_dispatch_candidates", stale_page)
        async with work_runtime.managed_worker_pool(
            database,
            tmp_path,
            worker_count=1,
            eligible_identifiers=("interactive-runtime", "background-runtime"),
        ):
            with anyio.fail_after(15):
                await captured.wait()
                # Another runtime/process may commit a legitimate third
                # interactive turn after this runtime's discovery snapshot.
                await competing_turn("competing")
                changed.set()
                await harness.paused.wait()
            assert harness.order == ["background-runtime"], await _class_failure_state(
                database, harness
            )
            admissions = await database.fair_admissions(manager.resources.scope, limit=10)
            assert (
                sorted(admissions, key=lambda admission: admission.sequence)[
                    3
                ].scheduling.service_class
                == SchedulingServiceClass.BACKGROUND
            )


async def test_workspace_and_root_round_robin_then_spare_turns_are_borrowed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness(pause_after=8)
    harness.install(monkeypatch, tmp_path)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        for root in range(4):
            for child in range(2):
                _ = await harness.enqueue(
                    database, f"A-{root}-{child}", _context("workspace-A", f"A-root-{root}")
                )
        for child in range(4):
            _ = await harness.enqueue(database, f"B-{child}", _context("workspace-B", "B-root"))
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            with anyio.fail_after(15):
                await harness.paused.wait()
            first = [harness.owners[identifier] for identifier in harness.order]
            assert [owner.workspace_identifier for owner in first].count("workspace-A") == 4
            assert [owner.workspace_identifier for owner in first].count("workspace-B") == 4
            assert {
                owner.root_request_identifier
                for owner in first
                if owner.workspace_identifier == "workspace-A"
            } == {f"A-root-{index}" for index in range(4)}
            harness.resume.set()
            await _wait_count(harness, 12)
            assert all(
                harness.owners[identifier].workspace_identifier == "workspace-A"
                for identifier in harness.order[8:]
            )


async def test_aged_low_priority_work_precedes_unaged_high_priority_in_same_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness(pause_after=1)
    harness.install(monkeypatch, tmp_path)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        owner = _context("workspace", "root")
        _ = await harness.enqueue(database, "old-low", owner, priority=-100, register=True)
        harness.clock.advance(31)
        _ = await harness.enqueue(database, "new-high", owner, priority=10**9)
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            with anyio.fail_after(15):
                await harness.paused.wait()
            assert harness.order == ["old-low"]


async def test_shared_child_keeps_one_turn_and_first_owner_through_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness(pause_after=2)
    harness.install(monkeypatch, tmp_path)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        first = _context("workspace-A", "first-owner")
        attached = _context("workspace-B", "attached", SchedulingServiceClass.INTERACTIVE)
        child = await harness.enqueue(database, "shared", first)
        for index in range(20):
            attachment = await database.enqueue_work(
                child.model_copy(update={"identifier": f"unused-attachment-{index}"}),
                WorkRequester(
                    request_identifier=str(uuid4()),
                    kind=("test", "attachment"),
                    identifier=attached.root_request_identifier,
                    context={},
                    scheduling=attached,
                ),
                event_identifier=str(uuid4()),
                enqueued_at_utc_ns=harness.clock(),
            )
            assert not attachment.created
        _ = await harness.enqueue(database, "other", _context("workspace-C", "other"))
        entered = harness.entered["shared"] = anyio.Event()
        harness.gates["shared"] = anyio.Event()
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            with anyio.fail_after(15):
                await entered.wait()
                _ = await database.cancel_work(
                    "shared",
                    reason="fixture cancel",
                    caller="test",
                    requested_at_utc_ns=harness.clock(),
                )
                await harness.paused.wait()
            assert sorted(harness.order) == ["other", "shared"]
            cancelled = await database.work("shared")
            assert cancelled["state"] == "cancelled"
            assert cancelled["result"] is None
            admissions = await database.fair_admissions(
                scope=stage_manager(database).resources.scope, limit=10
            )
            shared = [entry for entry in admissions if entry.work_item_identifier == "shared"]
            assert len(shared) == 1
            assert shared[0].scheduling == first


async def test_resident_actor_claim_and_phase_use_the_canonical_shared_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness(pause_after=1)
    harness.install(monkeypatch, tmp_path)

    def build(
        *, facebook: FacebookSessionStageDependencies, ebay: EbaySessionStageDependencies
    ) -> StageHandlerRegistry:
        original = build_session_stage_registry(facebook=facebook, ebay=ebay)

        def initial(work: WorkDefinition, identifier: str, operation: str) -> StageDefinition:
            return harness.initial(work, identifier, operation).model_copy(
                update={"resident_session_identifier": work.identifier}
            )

        return StageHandlerRegistry(
            tuple(
                replace(adapter, initial_stage=initial)
                if adapter.capability.kind == COLLECT_EBAY_ITEM_WORK_KIND
                else adapter
                for adapter in original.adapters
            ),
            original.handlers,
        )

    monkeypatch.setattr(work_runtime, "build_session_stage_registry", build)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        owner = _context("workspace", "resident-owner", SchedulingServiceClass.INTERACTIVE)
        _ = await harness.enqueue(database, "resident", owner, kind=COLLECT_EBAY_ITEM_WORK_KIND)
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            with anyio.fail_after(15):
                await harness.paused.wait()
            admissions = await database.fair_admissions(
                scope=stage_manager(database).resources.scope, limit=10
            )
            assert len(admissions) == 2
            assert {entry.lane.value for entry in admissions} == {"resident", "cpu"}
            assert all(entry.scheduling == owner for entry in admissions)
            occupancy = await database.resource_occupancy(stage_manager(database).resources.scope)
            assert occupancy["cpu"] == 1 and occupancy["resident_sessions"] == 1


async def test_owned_dataset_http_and_cpu_claims_share_runtime_fairness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness()
    harness.install(monkeypatch, tmp_path)
    directories = CarlDirectories(
        config=tmp_path / "config",
        data=tmp_path,
        cache=tmp_path / "cache",
        state=tmp_path / "state",
        runtime=tmp_path / "runtime",
    )
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1, "http": 1}
    ) as database:
        calls = 0

        async def wire(_request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(200, content=b"fixture source data")

        engine = _Engine(database, directories, httpx.MockTransport(wire))
        engine.verify_release = anyio.Event()

        def injected_engine(
            _directories: CarlDirectories, *, manager: TravelDataManager
        ) -> DatasetSetupEngine:
            assert isinstance(manager, TravelDataManager)
            return engine

        monkeypatch.setattr(work_runtime, "DatasetSetupEngine", injected_engine)
        owner = _context("workspace", "dataset-owner", SchedulingServiceClass.INTERACTIVE)
        work = travel_data_work(
            TravelDataSetupRequest(
                request_identifier="dataset", provider="geonames", activate=False
            )
        )
        _ = await database.enqueue_work(
            work,
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("test", "dataset"),
                identifier=owner.root_request_identifier,
                context={},
                scheduling=owner,
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=harness.clock(),
        )
        async with work_runtime.managed_worker_pool(database, tmp_path, worker_count=1):
            with anyio.fail_after(15):
                await engine.verify_started.wait()
            assert calls == 1
            admissions = await database.fair_admissions(
                scope=stage_manager(database).resources.scope, limit=20
            )
            assert {entry.lane.value for entry in admissions} >= {"http", "cpu"}
            assert all(entry.scheduling == owner for entry in admissions)
            assert all(entry.work_item_identifier == work.identifier for entry in admissions)
            assert len({entry.token for entry in admissions}) == len(admissions)
            engine.verify_release.set()
            with anyio.fail_after(15):
                while (await database.work(work.identifier))["state"] != "completed":
                    await anyio.sleep(0.01)
            assert calls == 1
