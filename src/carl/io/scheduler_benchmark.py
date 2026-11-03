"""Synthetic offline workloads composed through the real durable worker runtime."""

from __future__ import annotations

import hashlib
import io
import platform
import random
import shutil
import sys
import tempfile
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter_ns, time_ns
from typing import cast, final, override
from uuid import uuid4

import anyio
import apsw
import httpx
from PIL import Image
from PIL import __version__ as pillow_version

from carl.coordinator_stages import build_coordinator_stage_registry
from carl.core.benchmark import (
    BenchmarkConfig,
    BenchmarkIteration,
    BenchmarkMode,
    BenchmarkReport,
    BenchmarkScenario,
)
from carl.core.components import Component, ComponentId
from carl.core.ebay_items import CollectEbayImagePayload, collect_ebay_image_work
from carl.core.facebook_images import CollectImagePayload, GalleryImageReference, collect_image_work
from carl.core.fair_scheduling import SchedulingContext, SchedulingServiceClass
from carl.core.http import RequestPlan
from carl.core.item_analysis import (
    AnalyzeItemPayload,
    ClaudeEffort,
    ProductGuideKind,
    analyze_item_work,
    product_guide_definition,
)
from carl.core.json import encode_json
from carl.core.models import (
    BytesDraft,
    CodeProvenance,
    JsonValue,
    NamedOutput,
    RecordDraft,
    StrictModel,
)
from carl.core.pipeline import SEARCH_PIPELINE_WORK_KIND
from carl.core.resources import ResourceRequest
from carl.core.stages import StageDefinition
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkDefinition, WorkRequester
from carl.core.work_progress import WorkProgressWatch
from carl.core.worker import AttemptContext, CompletedWork, RetryWork, WorkOutcome
from carl.ebay_item_workers import EbayItemWorkerDependencies
from carl.facebook_analysis_workers import REGISTER_PRODUCT_GUIDE, build_analysis_component_registry
from carl.facebook_stateless_stages import FacebookStatelessStageDependencies
from carl.io.claude import ClaudeCli
from carl.io.httpx import DirectHttpxAcquirer
from carl.io.paths import CarlDirectories
from carl.io.provenance import collect_code_provenance_async
from carl.io.resource_permits import current_resource_manager, run_cpu
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import stage_manager
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry
from carl.stateless_stages import build_stateless_stage_registry
from carl.work_runtime import RuntimeComposition, managed_worker_pool

_SEED = Component(ComponentId(("carl", "benchmark", "synthetic_evidence")), 1, lambda: None)
_CONTROL = Component(ComponentId(("carl", "benchmark", "synthetic_control")), 1, lambda: None)


def _check(condition: bool, code: str) -> None:
    if not condition:
        raise RuntimeError(f"Offline benchmark invariant failed: {code}")


def _png(config: BenchmarkConfig) -> bytes:
    # One immutable body is shared across requests. Every validator still loads
    # the full image; disk CAS deduplication is explicitly reported.
    pixels = random.Random(config.seed).randbytes(config.image_width * config.image_height * 3)
    output = io.BytesIO()
    Image.frombytes("RGB", (config.image_width, config.image_height), pixels).save(
        output, format="PNG"
    )
    return output.getvalue()


class _Parent(StrictModel):
    source: str | None = None


@dataclass
class _State:
    config: BenchmarkConfig
    root: Path
    body: bytes
    provenance: CodeProvenance
    offset_ns: int = 0
    calls: dict[str, int] = field(default_factory=dict)
    successful_calls: dict[str, int] = field(default_factory=dict)
    failed_transport_calls: int = 0
    active_http: int = 0
    max_http: int = 0
    closed_http: int = 0
    http_gate: anyio.Event = field(default_factory=anyio.Event)
    entered_http: anyio.Event = field(default_factory=anyio.Event)
    probe_entered: anyio.Event = field(default_factory=anyio.Event)
    outage: bool = False
    analysis_gate: bool = False
    analysis_authorizations: int = 0
    parent_calls: dict[str, int] = field(default_factory=dict)
    urls: set[str] = field(default_factory=set)
    peak: dict[str, int] = field(default_factory=dict)
    checks: list[str] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)

    def now(self) -> int:
        return time_ns() + self.offset_ns

    async def probe(self) -> dict[str, JsonValue]:
        self.probe_entered.set()
        return {
            "reachable": not self.outage,
            "checks": [{"fixture": True, "reachable": not self.outage}],
        }

    async def respond(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        _check(url in self.urls, "unexpected_transport_destination")
        self.calls[url] = self.calls.get(url, 0) + 1
        if self.outage:
            self.failed_transport_calls += 1
            raise httpx.ConnectError("Synthetic offline transport unavailable", request=request)
        self.successful_calls[url] = self.successful_calls.get(url, 0) + 1
        self.active_http += 1
        self.max_http = max(self.max_http, self.active_http)
        self.entered_http.set()
        return httpx.Response(200, headers={"Content-Type": "image/png"}, stream=_Stream(self))

    async def parent(
        self, payload: _Parent, context: AttemptContext, database: Database
    ) -> WorkOutcome:
        identifier = context.work_item_identifier
        self.parent_calls[identifier] = self.parent_calls.get(identifier, 0) + 1
        if payload.source is None:
            return CompletedWork(result={"synthetic_control": "completed"})
        row = (await database.work_progress((payload.source,)))[0]
        result = row["result"]
        tick = cast(dict[str, JsonValue], result).get("tick", 0) if isinstance(result, dict) else 0
        return RetryWork(
            delay_ns=0,
            reason={"kind": "pipeline_continuation"},
            result={"synthetic_control": "waiting", "tick": tick},
            progress_watch=WorkProgressWatch(work_identifiers=(payload.source,)),
        )

    def coordinators(self, database: Database, legacy: WorkHandlerRegistry) -> StageHandlerRegistry:
        replacement = WorkHandlerRegistry(
            tuple(
                TypedWorkHandler(
                    handler.capability,
                    _CONTROL,
                    _Parent,
                    lambda payload, context: self.parent(payload, context, database),
                )
                if handler.capability.kind == SEARCH_PIPELINE_WORK_KIND
                else handler
                for handler in legacy.handlers
            )
        )
        return build_coordinator_stage_registry(database, replacement, utc_now_ns=self.now)

    def stateless(
        self, *, facebook: FacebookStatelessStageDependencies, ebay: EbayItemWorkerDependencies
    ) -> StageHandlerRegistry:
        acquirer = self.acquirer()
        return build_stateless_stage_registry(
            facebook=replace(facebook, acquirer=acquirer, item_acquirer=acquirer),
            ebay=replace(ebay, image_acquirer=acquirer, item_acquirer=acquirer),
        )

    def acquirer(self) -> DirectHttpxAcquirer:
        return DirectHttpxAcquirer(transport=httpx.MockTransport(self.respond))


@final
class _Stream(httpx.AsyncByteStream):
    def __init__(self, state: _State):
        self.state = state
        self.closed = False

    @override
    async def __aiter__(self) -> AsyncIterator[bytes]:
        await self.state.http_gate.wait()
        for start in range(0, len(self.state.body), self.state.config.chunk_bytes):
            if self.state.config.chunk_delay_ms:
                await anyio.sleep(self.state.config.chunk_delay_ms / 1000)
            yield self.state.body[start : start + self.state.config.chunk_bytes]

    @override
    async def aclose(self) -> None:
        if not self.closed:
            self.closed = True
            self.state.active_http -= 1
            self.state.closed_http += 1


@dataclass(frozen=True, slots=True)
class _OfflineClaude(ClaudeCli):
    state: _State | None = None

    @override
    def argv(
        self,
        prompt: str,
        model: str = "offline-benchmark",
        effort: ClaudeEffort = ClaudeEffort.MEDIUM,
        maximum_turns: int | None = None,
    ) -> tuple[str, ...]:
        del prompt, model, effort, maximum_turns
        if self.state is None:
            raise ValueError("Offline analysis requires its explicit benchmark state")
        self.state.analysis_authorizations += 1
        identifier = hashlib.sha256(str(self.state.analysis_authorizations).encode()).hexdigest()
        return (
            sys.executable,
            "-m",
            "carl.io.benchmark_analysis_child",
            "--delay-ms",
            str(self.state.config.analysis_delay_ms),
            "--control",
            str(self.state.root / "children"),
            "--identifier",
            identifier,
            *(("--gated",) if self.state.analysis_gate else ()),
        )

    @override
    async def version(self, directory: Path | None = None) -> str:
        del directory
        return "offline-benchmark-v1"


def _directories(root: Path) -> CarlDirectories:
    return CarlDirectories(
        config=root / "config",
        data=root / "data",
        cache=root / "cache",
        state=root / "state",
        runtime=root / "runtime",
    )


async def _publish(
    database: Database,
    state: _State,
    records: tuple[RecordDraft, ...],
    *,
    artifacts: tuple[BytesDraft, ...] = (),
    inputs: tuple[tuple[tuple[str, ...], str], ...] = (),
    component: Component = _SEED,
) -> None:
    operation = str(uuid4())
    now = datetime.now(UTC).isoformat()
    await database.begin_operation(
        operation_id=operation,
        component=component,
        provenance=state.provenance,
        invocation={"synthetic_benchmark": True},
        configuration={},
        started_at_utc=now,
        inputs=inputs,
    )
    await database.complete_operation(
        operation_id=operation,
        records=records,
        artifacts=artifacts,
        outputs=tuple(
            NamedOutput(name=("record", str(i)), object_identifier=record.identifier)
            for i, record in enumerate(records)
        )
        + tuple(
            NamedOutput(name=("guide_text",), object_identifier=artifact.identifier)
            for artifact in artifacts
        ),
        result={"synthetic_benchmark": True},
        ended_at_utc=now,
        duration_ns=0,
    )


def _control_work(identifier: str, source: str | None = None) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=SEARCH_PIPELINE_WORK_KIND,
        payload_schema_version=1,
        payload=_Parent(source=source).model_dump(mode="json"),
        deduplication_identity=(identifier,),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=SEARCH_PIPELINE_WORK_KIND),
        ),
    )


async def _images(database: Database, state: _State, count: int) -> list[WorkDefinition]:
    records: list[RecordDraft] = []
    definitions: list[WorkDefinition] = []
    for index in range(count):
        identifier = f"image-{index}"
        reference_id = f"reference-{index}"
        if index % 2 == 0:
            url = f"https://benchmark.fbcdn.net/image-{index}.png"
            reference = GalleryImageReference(
                listing_id=str(index + 1000),
                listing_observation_record_identifier="synthetic-observation",
                acquisition_record_identifier="synthetic-source",
                block_index=0,
                json_path=("synthetic",),
                original_url=url,
                gallery_order=0,
                photo_id=f"photo-{index}",
            )
            records.append(
                RecordDraft(
                    identifier=reference_id,
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=reference.model_dump(mode="json"),
                )
            )
            definitions.append(
                collect_image_work(
                    identifier=identifier,
                    payload=CollectImagePayload(
                        reference_record_identifier=reference_id,
                        reference=reference,
                        request_plan=RequestPlan(
                            method="GET", url=url, routing=("benchmark",), follow_redirects=False
                        ),
                    ),
                    not_before_utc_ns=0,
                )
            )
        else:
            url = f"https://i.ebayimg.com/images/g/benchmark-{index}/s-l1600.png"
            item = str(1_000_000_000 + index)
            records.append(
                RecordDraft(
                    identifier=reference_id,
                    kind=("carl", "ebay", "gallery_image_reference"),
                    schema_version=1,
                    value={
                        "item_identifier": item,
                        "observation_record_identifier": "synthetic-observation",
                        "acquisition_record_identifier": "synthetic-source",
                        "url": url,
                        "gallery_order": 0,
                    },
                )
            )
            definitions.append(
                collect_ebay_image_work(
                    identifier=identifier,
                    payload=CollectEbayImagePayload(
                        item_identifier=item,
                        observation_record_identifier="synthetic-observation",
                        reference_record_identifier=reference_id,
                        url=url,
                    ),
                )
            )
        state.urls.add(url)
    records.append(
        RecordDraft(
            identifier="synthetic-observation",
            kind=("carl", "benchmark", "observation"),
            schema_version=1,
            value={},
        )
    )
    await _publish(database, state, tuple(records))
    return definitions


async def _analyses(database: Database, state: _State, count: int) -> list[WorkDefinition]:
    guide = product_guide_definition(ProductGuideKind.TELESCOPE)
    await _publish(
        database,
        state,
        (
            RecordDraft(
                identifier="guide",
                kind=("carl", "analysis", "product_guide"),
                schema_version=1,
                value={"identity": list(guide.identity), "version": guide.version},
            ),
        ),
        artifacts=(
            BytesDraft(
                identifier="guide-text",
                kind=("carl", "analysis", "product_guide_text"),
                media_type="text/plain; charset=utf-8",
                representation={"exact_product_guide": True},
                content=guide.text.encode(),
            ),
        ),
        component=build_analysis_component_registry().require(REGISTER_PRODUCT_GUIDE),
    )
    definitions: list[WorkDefinition] = []
    for index in range(count):
        observation, evidence = f"observation-{index}", f"evidence-{index}"
        await _publish(
            database,
            state,
            (
                RecordDraft(
                    identifier=observation,
                    kind=("carl", "facebook", "listing_observation"),
                    schema_version=1,
                    value={
                        "listing_id": str(index),
                        "fields": {
                            "title": {
                                "state": "present",
                                "evidence": [
                                    {
                                        "state": "present",
                                        "normalized": f"Synthetic benchmark object {index}",
                                        "original": f"Synthetic benchmark object {index}",
                                        "evidence_kind": "seller_claim",
                                    }
                                ],
                            }
                        },
                    },
                ),
            ),
        )
        await _publish(
            database,
            state,
            (
                RecordDraft(
                    identifier=evidence,
                    kind=("carl", "facebook", "listing_analysis_evidence"),
                    schema_version=1,
                    value={"gallery_absence_reason": "Synthetic benchmark has no source gallery"},
                ),
            ),
            inputs=((("listing_observation",), observation),),
        )
        definitions.append(
            analyze_item_work(
                identifier=f"analysis-{index}",
                payload=AnalyzeItemPayload(
                    evidence_set_record_identifier=evidence,
                    product_guide_record_identifier="guide",
                    claude_version="offline-benchmark-v1",
                    model="offline-benchmark",
                    timeout_seconds=min(120, state.config.timeout_seconds),
                ),
            )
        )
    return definitions


async def _enqueue(
    database: Database, state: _State, definitions: list[WorkDefinition], *, fair: bool = False
) -> None:
    for index, definition in enumerate(definitions):
        interactive = index % 4 != 3
        workspace = "A" if index % 2 == 0 else "B"
        root = f"{workspace}-{index % 3 if workspace == 'A' else 0}-{interactive}"
        _ = await database.enqueue_work(
            definition,
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("carl", "benchmark", "synthetic"),
                identifier=definition.identifier,
                context={"synthetic_benchmark": True},
                scheduling=SchedulingContext(
                    root_request_identifier=root if fair else definition.identifier,
                    workspace_identifier=workspace if fair else definition.identifier,
                    service_class=SchedulingServiceClass.INTERACTIVE
                    if fair and interactive
                    else SchedulingServiceClass.BACKGROUND,
                ),
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=state.now(),
        )


async def _until(predicate: Callable[[], Awaitable[bool]]) -> None:
    while not await predicate():
        await anyio.sleep(0.01)


async def _terminal(database: Database, identifiers: tuple[str, ...]) -> list[dict[str, JsonValue]]:
    rows: list[dict[str, JsonValue]] = []

    async def ready() -> bool:
        nonlocal rows
        rows = []
        for start in range(0, len(identifiers), 1000):
            rows.extend(await database.work_progress(identifiers[start : start + 1000]))
        return all(row["state"] not in {"pending", "leased", "cancelling"} for row in rows)

    await _until(ready)
    return rows


async def _outage_checkpointed(database: Database, identifiers: tuple[str, ...], now: int) -> bool:
    # The probe callback runs before its durable decision and connectivity
    # reviews finish. Advance the fixture clock only after every image has
    # published its retry deadline or is blocked at network admission.
    async with database._connections.reader() as connection:  # pyright: ignore[reportPrivateUsage]
        row = await (
            await connection.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM network_connectivity
                    WHERE singleton=1 AND paused=1 AND probe_token IS NULL
                      AND json_extract(result_json,'$.reachable')=0
                ) AND (
                    SELECT count(*) FROM json_each(?) AS requested
                    JOIN work_items AS work ON work.id=requested.value
                    LEFT JOIN work_stages AS stage ON stage.work_item_id=work.id
                        AND stage.state IN ('pending','running')
                    WHERE (
                        work.state IN ('completed','terminal_failure','cancelled')
                        AND stage.id IS NULL
                    ) OR (
                        work.state='pending' AND stage.id IS NULL
                        AND json_extract(work.result_json,'$.state')='waiting_for_connectivity'
                        AND work.eligible_at_utc_ns>?
                    ) OR (
                        work.state IN ('pending','leased') AND stage.state='pending'
                        AND stage.wait_kind='connectivity' AND stage.eligible_at_utc_ns>?
                        AND (
                            json_extract(stage.definition_json,'$.network_activity') IS NOT NULL
                            OR EXISTS (
                                SELECT 1 FROM json_each(stage.definition_json,'$.resources')
                                WHERE json_extract(value,'$.name')='http'
                            )
                        )
                    )
                )=?
                """,
                (encode_json(list(identifiers)), now, now, len(identifiers)),
            )
        ).fetchone()
    return row is not None and row[0] == 1


async def _scalar(database: Database, statement: str) -> int:
    # This benchmark owns the entire synthetic database. Only bounded scalar
    # lifecycle/cost measurements cross this private adapter boundary.
    async with database._connections.reader() as connection:  # pyright: ignore[reportPrivateUsage]
        row = await (await connection.execute(statement)).fetchone()
    assert row is not None and isinstance(row[0], int)
    return row[0]


async def _sample(database: Database, state: _State) -> None:
    manager = current_resource_manager()
    assert manager is not None
    while True:
        held = await database.resource_occupancy(manager.scope)
        for name, units in held.items():
            _check(units <= manager.capacities[name], "capacity_exceeded")
            state.peak[name] = max(state.peak.get(name, 0), units)
        await anyio.sleep(0.01)


@asynccontextmanager
async def _sampling(database: Database, state: _State) -> AsyncGenerator[None]:
    async with anyio.create_task_group() as tasks:
        _ = tasks.start_soon(_sample, database, state)
        try:
            yield
        finally:
            tasks.cancel_scope.cancel()


@asynccontextmanager
async def _vm_measurement(database: Database, state: _State) -> AsyncGenerator[None]:
    # This private database belongs exclusively to the benchmark. Count every
    # SQLite VM instruction, including runtime polling and sampling overhead.
    steps = 0

    def progress() -> bool:
        nonlocal steps
        steps += 1
        return False

    connections = (database._connections._writer, *database._connections._readers)  # pyright: ignore[reportPrivateUsage]
    for connection in connections:
        await connection.set_progress_handler(progress, nsteps=1, id="benchmark-vm")
    try:
        yield
    finally:
        for connection in connections:
            await connection.set_progress_handler(None, id="benchmark-vm")
        state.counters["timed_sqlite_vm_steps"] = steps


async def _verify_images(
    database: Database, state: _State, rows: list[dict[str, JsonValue]]
) -> None:
    expected = hashlib.sha256(state.body).hexdigest()
    checked = 0
    for row in rows:
        summary = row["result"]
        if not isinstance(summary, dict) or "image_result_record_identifier" not in summary:
            continue
        identifier: JsonValue = cast(dict[str, JsonValue], summary)[
            "image_result_record_identifier"
        ]
        _check(isinstance(identifier, str), "image_result_identifier_missing")
        _, _, record = await database.get_record(cast(str, identifier))
        _check(isinstance(record, dict), "image_result_missing")
        record = cast(dict[str, JsonValue], record)
        _check(
            record.get("sha256") == expected and record.get("mime_type") == "image/png",
            "image_content_identity_changed",
        )
        checked += 1
    if state.successful_calls:
        _check(checked == len(state.successful_calls), "image_output_missing")
        state.checks.append("image_hash_and_media_identity")
        state.counters["verified_image_outputs"] = checked


async def _sleepers(database: Database, count: int) -> None:
    async def ready() -> bool:
        return (
            await _scalar(database, "SELECT count(*) FROM progress_wait_builds WHERE state='armed'")
            == count
        )

    await _until(ready)


async def _progress_profile(
    database: Database, state: _State, gate: anyio.Event
) -> tuple[int, int]:
    count = state.config.items
    source = WorkDefinition(
        identifier="source",
        kind=("carl", "benchmark", "source"),
        payload_schema_version=1,
        payload=None,
        deduplication_identity=("source",),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND, identity=("carl", "benchmark", "source")
            ),
        ),
    )
    await _enqueue(database, state, [source])
    await _enqueue(
        database, state, [_control_work(f"parent-{index}", "source") for index in range(count)]
    )
    gate.set()
    await _sleepers(database, count)
    if state.config.scenario == BenchmarkScenario.IDLE_PROGRESS_ROOTS:
        baseline = sum(state.parent_calls.values())
        started = perf_counter_ns()
        if state.config.mode == BenchmarkMode.VERIFY:
            state.offset_ns += 60_000_000_000
        async with _vm_measurement(database, state):
            await anyio.sleep(1.1)
        _check(sum(state.parent_calls.values()) == baseline, "idle_parent_reexecuted")
        state.counters["sleeping_roots"] = count
        state.counters["timed_parent_executions"] = 0
        state.checks.append("idle_roots_do_not_execute")
        return started, perf_counter_ns()
    await database.register_stage(
        StageDefinition(
            identifier="source-stage",
            work_item_identifier="source",
            operation_identifier="source-operation",
            kind=("carl", "benchmark", "source"),
            payload=None,
        ),
        created_at_utc_ns=state.now(),
    )
    async with stage_manager(database).admit(
        "source-stage",
        component=_CONTROL,
        provenance=state.provenance,
        invocation={"synthetic_benchmark": True},
    ) as admission:
        assert admission.execution is not None
        execution = admission.execution

        async def tick(ordinal: int) -> None:
            await database.publish_leased_operation_checkpoint(
                work_item_identifier="source",
                lease_token=execution.lease.token,
                worker_identifier=execution.lease.work_lease.worker_identifier,
                utc_now_ns=state.now,
                operation_id="source-operation",
                records=(),
                artifacts=(),
                outputs=(),
                checkpoint_result={"tick": ordinal},
            )

            async def reacted() -> bool:
                return (
                    all(value >= ordinal + 1 for value in state.parent_calls.values())
                    and len(state.parent_calls) == count
                )

            await _until(reacted)
            await _sleepers(database, count)

        for ordinal in range(1, state.config.history_rounds + 1):
            await tick(ordinal)
        state.counters["inactive_members_before"] = await _scalar(
            database,
            "SELECT count(*) FROM progress_wait_members m JOIN progress_wait_builds b ON b.id=m.build_id WHERE b.state='inactive'",
        )
        before = sum(state.parent_calls.values())
        started = perf_counter_ns()
        async with _vm_measurement(database, state):
            await tick(state.config.history_rounds + 1)
        ended = perf_counter_ns()
        state.counters["timed_parent_executions"] = sum(state.parent_calls.values()) - before
        state.counters["live_consumers"] = count
        state.checks.append("historical_memberships_preserve_live_wakes")
        await execution.apply(CompletedWork(result={"synthetic_control": "completed"}))
    return started, ended


async def _run_iteration(
    config: BenchmarkConfig, root: Path, repository_root: Path, provenance: CodeProvenance
) -> tuple[BenchmarkIteration, dict[str, int]]:
    if config.scenario == BenchmarkScenario.CANCEL_CHECKPOINT_RESTART:
        return await _cancel_iteration(config, root, repository_root, provenance)
    setup_started = perf_counter_ns()
    directories = _directories(root)
    directories.data.mkdir(mode=0o700, parents=True)
    rows: list[dict[str, JsonValue]] = []
    started = ended = setup_ns = cleanup_started = 0
    async with AsyncExitStack() as stack:
        database = await stack.enter_async_context(
            Database.managed(
                directories.database_file, initialize=True, resource_capacities=config.capacities
            )
        )
        body = await run_cpu(_png, config)
        state = _State(config, root, body, provenance)
        state.http_gate.set()
        scenario = config.scenario
        state.analysis_gate = (
            scenario == BenchmarkScenario.ANALYSIS_PRESSURE and config.mode == BenchmarkMode.VERIFY
        )
        state.outage = scenario == BenchmarkScenario.OUTAGE_AND_PACING
        gate = anyio.Event()
        composition = RuntimeComposition(
            directories, state.stateless, state.probe, state.now, state.coordinators, gate
        )
        manager = current_resource_manager()
        assert manager is not None
        capacities = dict(manager.capacities)
        if scenario in {
            BenchmarkScenario.IDLE_PROGRESS_ROOTS,
            BenchmarkScenario.WATCH_HISTORY_FANOUT,
        }:
            definitions: list[WorkDefinition] = []
        elif scenario == BenchmarkScenario.ANALYSIS_PRESSURE:
            definitions = await _analyses(database, state, config.items)
        elif scenario == BenchmarkScenario.REQUESTER_FAIRNESS:
            definitions = [
                _control_work(f"control-{index}") for index in range(max(8, config.items))
            ]
        else:
            definitions = await _images(database, state, config.items)
        await _enqueue(
            database, state, definitions, fair=scenario == BenchmarkScenario.REQUESTER_FAIRNESS
        )
        if (
            scenario == BenchmarkScenario.CPU_DECODE_PRESSURE
            and config.mode == BenchmarkMode.VERIFY
        ):
            state.http_gate = anyio.Event()
        async with anyio.create_task_group() as sampling:
            _ = sampling.start_soon(_sample, database, state)
            try:
                async with managed_worker_pool(
                    database,
                    repository_root,
                    claude=_OfflineClaude(state=state),
                    composition=composition,
                ):
                    setup_ns = perf_counter_ns() - setup_started
                    if scenario in {
                        BenchmarkScenario.IDLE_PROGRESS_ROOTS,
                        BenchmarkScenario.WATCH_HISTORY_FANOUT,
                    }:
                        started, ended = await _progress_profile(database, state, gate)
                        setup_ns = started - setup_started
                    else:
                        started = perf_counter_ns()
                        gate.set()
                        if scenario == BenchmarkScenario.ANALYSIS_PRESSURE and state.analysis_gate:

                            async def saturated() -> bool:
                                return len(tuple((root / "children").glob("*.started"))) >= min(
                                    config.items, capacities["analysis"]
                                )

                            await _until(saturated)
                            _ = (root / "children" / "release").write_text("release")
                            state.checks.append("real_supervised_analysis_capacity")
                        if scenario == BenchmarkScenario.OUTAGE_AND_PACING:
                            await state.probe_entered.wait()
                            probe = _control_work("unrelated")
                            await _enqueue(database, state, [probe])
                            _ = await _terminal(database, (probe.identifier,))
                            state.checks.append("unrelated_progress_during_outage")
                            if config.mode == BenchmarkMode.VERIFY:

                                async def outage_checkpointed() -> bool:
                                    return await _outage_checkpointed(
                                        database,
                                        tuple(definition.identifier for definition in definitions),
                                        state.now(),
                                    )

                                await _until(outage_checkpointed)
                            state.outage = False
                            if config.mode == BenchmarkMode.VERIFY:
                                state.offset_ns += 31_000_000_000
                        if scenario == BenchmarkScenario.CPU_DECODE_PRESSURE:
                            await state.entered_http.wait()
                            held, release_cpu = anyio.Event(), anyio.Event()

                            async def hold_cpu() -> None:
                                async with manager.admit(
                                    ResourceRequest(name="cpu", units=capacities["cpu"])
                                ):
                                    held.set()
                                    await release_cpu.wait()

                            if config.mode == BenchmarkMode.VERIFY:
                                async with anyio.create_task_group() as holders:
                                    _ = holders.start_soon(hold_cpu)
                                    try:
                                        await held.wait()
                                        state.http_gate.set()

                                        async def checkpointed() -> bool:
                                            return (
                                                state.closed_http > 0
                                                and state.active_http == 0
                                                and (
                                                    await database.resource_occupancy(manager.scope)
                                                ).get("http", 0)
                                                == 0
                                            )

                                        await _until(checkpointed)
                                        _check(
                                            (await database.resource_occupancy(manager.scope)).get(
                                                "http", 0
                                            )
                                            == 0,
                                            "http_retained_during_cpu_wait",
                                        )
                                        await _enqueue(
                                            database, state, [_control_work("unrelated")]
                                        )
                                        _ = await _terminal(database, ("unrelated",))
                                        state.checks.append(
                                            "no_http_or_metadata_starvation_during_cpu_wait"
                                        )
                                    finally:
                                        release_cpu.set()
                        rows = await _terminal(
                            database, tuple(definition.identifier for definition in definitions)
                        )
                        ended = perf_counter_ns()
                        if scenario == BenchmarkScenario.REQUESTER_FAIRNESS:
                            turns = sorted(
                                await database.fair_admissions(manager.scope, limit=1000),
                                key=lambda turn: turn.sequence,
                            )
                            remaining_bg = sum(index % 4 == 3 for index in range(len(definitions)))
                            streak = 0
                            for turn in turns:
                                if (
                                    turn.scheduling.service_class
                                    == SchedulingServiceClass.BACKGROUND
                                ):
                                    remaining_bg -= 1
                                    streak = 0
                                else:
                                    streak += 1
                                    _check(
                                        remaining_bg <= 0 or streak <= 3,
                                        "interactive_background_turn_bound",
                                    )
                            state.checks.append("fair_turns_with_shared_root_fanout")
                cleanup_started = ended
            finally:
                sampling.cancel_scope.cancel()
        _check(
            await database.resource_occupancy(manager.scope) == {}, "physical_capacity_not_released"
        )
        await _verify_images(database, state, rows)
        failed = sum(row["state"] == "terminal_failure" for row in rows)
        cancelled = sum(row["state"] == "cancelled" for row in rows)
        completed = sum(row["state"] == "completed" for row in rows)
        if scenario != BenchmarkScenario.OUTAGE_AND_PACING:
            _check(failed == 0, "synthetic_work_failed")
        if state.calls:
            _check(
                max(state.successful_calls.values(), default=0) <= 1,
                "duplicate_successful_transport_invocation",
            )
            state.checks.append("single_successful_capture_per_acquisition")
            if scenario != BenchmarkScenario.OUTAGE_AND_PACING:
                _check(max(state.calls.values()) == 1, "duplicate_transport_invocation")
        state.counters["failed_transport_invocations"] = state.failed_transport_calls
        state.counters["fixture_unique_bodies"] = 1
        state.counters["fixture_body_bytes"] = len(body)
        state.counters["fixture_pixels"] = config.image_width * config.image_height
        elapsed = max(1, ended - started)
        result = BenchmarkIteration(
            setup_ns=setup_ns,
            elapsed_ns=elapsed,
            cleanup_ns=max(0, perf_counter_ns() - cleanup_started),
            completed=completed,
            failed=failed,
            cancelled=cancelled,
            completed_per_second=completed * 1_000_000_000 / elapsed
            if config.mode == BenchmarkMode.THROUGHPUT and completed
            else None,
            capture_invocations=sum(state.calls.values()),
            analysis_invocations=len(tuple((root / "children").glob("*.started"))),
            synthetic_parent_executions=state.counters.get(
                "timed_parent_executions", sum(state.parent_calls.values())
            ),
            sampled_peak_held_units=state.peak,
            counters=state.counters,
            scheduler_activity=(
                await database.scheduler_activity(now_utc_ns=state.now())
            ).model_copy(update={"samples": ()}),
            checks=tuple(dict.fromkeys([*state.checks, "capacity_bounds", "physical_cleanup"])),
        )
    return result.model_copy(
        update={"cleanup_ns": max(0, perf_counter_ns() - cleanup_started)}
    ), capacities


async def _cancel_iteration(
    config: BenchmarkConfig, root: Path, repository_root: Path, provenance: CodeProvenance
) -> tuple[BenchmarkIteration, dict[str, int]]:
    setup_started = perf_counter_ns()
    started = setup_ns = 0
    before: dict[str, int] = {}
    directories = _directories(root)
    directories.data.mkdir(mode=0o700, parents=True)
    async with AsyncExitStack() as stack:
        database = await stack.enter_async_context(
            Database.managed(
                directories.database_file, initialize=True, resource_capacities=config.capacities
            )
        )
        state = _State(config, root, await run_cpu(_png, config), provenance)
        state.analysis_gate = True
        definitions = await _images(database, state, config.items)
        definitions += await _analyses(database, state, 1)
        # Only the checkpoint target and gated child run before restart. Other
        # images enter afterwards so unrelated half-dispatched acquisitions do
        # not turn this checkpoint replay profile into a transport retry test.
        await _enqueue(database, state, [definitions[0], definitions[-1]])
        gate = anyio.Event()
        composition = RuntimeComposition(
            directories, state.stateless, state.probe, state.now, state.coordinators, gate
        )
        manager = current_resource_manager()
        assert manager is not None
        capacities = dict(manager.capacities)
        held, release = anyio.Event(), anyio.Event()

        async def hold() -> None:
            async with manager.admit(ResourceRequest(name="cpu", units=capacities["cpu"])):
                held.set()
                await release.wait()

        async with _sampling(database, state), anyio.create_task_group() as holders:
            try:
                async with managed_worker_pool(
                    database,
                    repository_root,
                    claude=_OfflineClaude(state=state),
                    composition=composition,
                ):
                    setup_ns = perf_counter_ns() - setup_started
                    started = perf_counter_ns()
                    gate.set()

                    async def child_started() -> bool:
                        return bool(tuple((root / "children").glob("*.started")))

                    await _until(child_started)
                    await state.entered_http.wait()
                    _ = holders.start_soon(hold)
                    await held.wait()
                    state.http_gate.set()

                    async def captured() -> bool:
                        snapshot = await database.work_dispatch_snapshot("image-0")
                        if snapshot.active_stage_identifier is None:
                            return False
                        stage = await database.stage(snapshot.active_stage_identifier)
                        return (
                            stage.kind[-1] == "publish"
                            and (await database.resource_occupancy(manager.scope)).get("http", 0)
                            == 0
                        )

                    await _until(captured)
                    before = dict(state.calls)
                    await _enqueue(database, state, [_control_work("unrelated")])
                    _ = await _terminal(database, ("unrelated",))
                    _ = await database.cancel_work(
                        "analysis-0",
                        reason="synthetic benchmark cancellation",
                        caller="benchmark",
                        requested_at_utc_ns=state.now(),
                    )
                # Pool cancellation kills/reaps the real gated analysis child.
                held_units = await database.resource_occupancy(manager.scope)
                _check(
                    held_units.get("analysis", 0) == 0 and held_units.get("http", 0) == 0,
                    "cancel_cleanup_incomplete",
                )
            finally:
                release.set()
        _check(
            await database.resource_occupancy(manager.scope) == {}, "old_runtime_capacity_retained"
        )
        await stack.aclose()
        database = await stack.enter_async_context(
            Database.managed(directories.database_file, resource_capacities=config.capacities)
        )
        await _enqueue(database, state, definitions[1:-1])
        async with (
            _sampling(database, state),
            managed_worker_pool(
                database,
                repository_root,
                claude=_OfflineClaude(state=state),
                composition=replace(composition, dispatch_gate=None),
            ),
        ):
            rows = await _terminal(
                database, tuple(definition.identifier for definition in definitions)
            )
        ended = perf_counter_ns()
        _check(
            all(state.calls.get(url) == count for url, count in before.items()),
            "checkpoint_restart_redispatch",
        )
        _check(all(count == 1 for count in state.calls.values()), "duplicate_capture_after_restart")
        _check(
            (await database.work("analysis-0"))["state"] == "cancelled",
            "analysis_cancellation_lost",
        )
        active_manager = current_resource_manager()
        assert active_manager is not None
        _check(
            await database.resource_occupancy(active_manager.scope) == {},
            "restarted_runtime_capacity_retained",
        )
        await _verify_images(database, state, rows)
        completed = sum(row["state"] == "completed" for row in rows)
        _check(completed == config.items, "restarted_images_incomplete")
        activity = (await database.scheduler_activity(now_utc_ns=state.now())).model_copy(
            update={"samples": ()}
        )
    elapsed = max(1, ended - started)
    return BenchmarkIteration(
        setup_ns=setup_ns,
        elapsed_ns=elapsed,
        cleanup_ns=max(0, perf_counter_ns() - ended),
        completed=completed,
        failed=0,
        cancelled=1,
        completed_per_second=completed * 1_000_000_000 / elapsed
        if config.mode == BenchmarkMode.THROUGHPUT
        else None,
        capture_invocations=sum(state.calls.values()),
        analysis_invocations=len(tuple((root / "children").glob("*.started"))),
        synthetic_parent_executions=sum(state.parent_calls.values()),
        sampled_peak_held_units=state.peak,
        counters={**state.counters, "database_reopens": 1, "fixture_body_bytes": len(state.body)},
        scheduler_activity=activity,
        checks=(
            "supervised_child_cancelled",
            "checkpoint_survives_database_restart",
            "no_capture_redispatch",
            "unrelated_metadata_progress",
            "physical_cleanup",
        ),
    ), capacities


async def run_scheduler_benchmark(
    config: BenchmarkConfig, *, repository_root: Path, work_parent: Path | None = None
) -> BenchmarkReport:
    """Run a fresh private synthetic database; cleanup failure stops all later runs."""
    if work_parent is not None:
        work_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    provenance = await collect_code_provenance_async(repository_root)
    iterations: list[BenchmarkIteration] = []
    capacities: dict[str, int] = {}
    for index in range(config.warmups + config.repetitions):
        root = Path(tempfile.mkdtemp(prefix="carl-benchmark-", dir=work_parent)).resolve()
        successful = False
        try:
            with anyio.fail_after(config.timeout_seconds):
                # Fixture generation uses the standard nonabandoning CPU helper
                # inside a temporary managed resource scope.
                result, capacities = await _run_iteration(config, root, repository_root, provenance)
            if index >= config.warmups:
                iterations.append(result)
            successful = True
        finally:
            if successful:
                await anyio.to_thread.run_sync(shutil.rmtree, root)
    return BenchmarkReport(
        configuration=config,
        configuration_sha256=hashlib.sha256(
            encode_json(config.model_dump(mode="json")).encode()
        ).hexdigest(),
        source_commit=provenance.commit_hash,
        source_worktree_state=provenance.worktree_state,
        python_version=platform.python_version(),
        pillow_version=pillow_version,
        sqlite_version=apsw.sqlitelibversion(),
        capacities=capacities,
        iterations=tuple(iterations),
        coverage=(
            "synthetic_offline",
            "real_pillow_validation_when_exercised",
            "real_analysis_supervisor_when_exercised",
            "sampled_occupancy_10ms",
            "timing_excludes_initial_fixture_queue_and_runtime_setup",
            "scheduler_queue_age_includes_staging_setup",
            "occupancy_sampling_includes_runtime_setup_and_cleanup",
            "controlled_utc_offsets_verify_only",
            "vm_instruction_counts_include_polling_sampling_and_instrumentation",
            "resident_and_native_engines_not_exercised",
            "database_and_coordination_scope_local",
            "no_paid_provider_measurements",
        ),
    )
