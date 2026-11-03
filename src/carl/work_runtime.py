"""Long-lived ownership of Carl's durable work queue."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter_ns, time_ns
from typing import Protocol
from uuid import uuid4

import anyio

from carl.analysis_batch_workers import (
    AnalysisBatchWorkerDependencies,
    build_analysis_batch_worker_registry,
)
from carl.coordinator_stages import build_coordinator_stage_registry
from carl.core.analysis_batch import request_missing_analyses_work_constraints
from carl.core.components import Component, ComponentId
from carl.core.ebay import ebay_search_work_constraint
from carl.core.ebay_items import (
    ebay_item_work_constraints,
    legacy_ebay_item_work_constraint_identifiers,
)
from carl.core.ebay_refresh import refresh_ebay_search_work_constraints
from carl.core.facebook_images import (
    image_session_work_constraint,
    legacy_image_session_work_constraint_identifiers,
)
from carl.core.facebook_refresh import refresh_search_work_constraints
from carl.core.facebook_work import (
    facebook_effective_search_work_constraint,
    facebook_network_path_constraint,
    facebook_search_work_constraint,
    legacy_facebook_network_path_constraint_identifier,
)
from carl.core.item_analysis import analysis_work_constraints
from carl.core.marketplace_images import marketplace_image_network_constraint
from carl.core.models import JsonValue
from carl.core.pipeline import pipeline_work_constraints
from carl.core.resources import ResourceRequest
from carl.dataset_setup_stages import (
    OWNED_STAGE_KIND,
    build_dataset_setup_owned_registry,
    build_dataset_setup_prepare_registry,
    submit_dataset_setup_stage,
)
from carl.ebay_analysis_stages import build_ebay_analysis_stage_registry
from carl.ebay_analysis_workers import build_ebay_analysis_worker_registry
from carl.ebay_item_workers import EbayItemWorkerDependencies, build_ebay_item_worker_registry
from carl.ebay_refresh_workers import (
    EbayRefreshWorkerDependencies,
    build_ebay_refresh_worker_registry,
)
from carl.ebay_session_stages import EbaySessionStageDependencies
from carl.ebay_workers import EbaySearchWorkerDependencies, build_ebay_worker_registry
from carl.facebook_analysis_stages import build_facebook_analysis_stage_registry
from carl.facebook_analysis_workers import (
    AnalysisWorkerDependencies,
    build_analysis_worker_registry,
)
from carl.facebook_listing_workers import (
    FacebookListingWorkerDependencies,
    build_facebook_listing_worker_registry,
)
from carl.facebook_refresh_workers import (
    RefreshWorkerDependencies,
    build_refresh_worker_registry,
)
from carl.facebook_routed_workers import build_routed_facebook_worker_registry
from carl.facebook_session_stages import FacebookSessionStageDependencies
from carl.facebook_stateless_stages import FacebookStatelessStageDependencies
from carl.io.claude import ClaudeCli
from carl.io.connectivity import ConnectivityMonitor
from carl.io.dataset_setup import DatasetSetupEngine
from carl.io.dataset_workflow_owner import managed_dataset_workflow_service
from carl.io.paths import CarlDirectories, user_directories
from carl.io.proton import SharedProtonWireproxyManager
from carl.io.provenance import collect_code_provenance_async, process_invocation
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.routing import ConfiguredTravelService
from carl.io.sqlite import Database
from carl.io.stage_dispatcher import StageDispatcher
from carl.io.stage_policy import PolicyStageRegistry, StagePolicy, invalid_initial_handler
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import stage_manager
from carl.io.travel_data import TravelDataManager
from carl.io.worker import WorkHandlerRegistry
from carl.pipeline_listing_workers import (
    PipelineListingWorkerDependencies,
    build_pipeline_listing_worker_registry,
)
from carl.pipeline_workers import PipelineWorkerDependencies, build_pipeline_worker_registry
from carl.review import ReviewApplication
from carl.session_stages import build_session_stage_registry
from carl.stateless_stages import build_stateless_stage_registry
from carl.travel_data_workers import build_travel_data_worker_registry
from carl.travel_stages import TravelStageDependencies, build_travel_stage_registry
from carl.travel_workers import TravelWorkerDependencies, build_travel_worker_registry


def _identifier() -> str:
    return str(uuid4())


@dataclass(frozen=True, slots=True)
class WorkRuntime:
    """Resources owned by one bounded worker-pool lifetime."""

    database: Database
    registry: WorkHandlerRegistry
    stages: StageHandlerRegistry


class StatelessRegistryBuilder(Protocol):
    def __call__(
        self, *, facebook: FacebookStatelessStageDependencies, ebay: EbayItemWorkerDependencies
    ) -> StageHandlerRegistry: ...


@dataclass(frozen=True, slots=True)
class RuntimeComposition:
    """Explicit dependency composition; ordinary worker defaults remain unchanged."""

    directories: CarlDirectories
    stateless_registry_builder: StatelessRegistryBuilder = build_stateless_stage_registry
    connectivity_probe: Callable[[], Awaitable[dict[str, JsonValue]]] | None = None
    scheduling_utc_now_ns: Callable[[], int] = time_ns
    coordinator_registry_builder: (
        Callable[[Database, WorkHandlerRegistry], StageHandlerRegistry] | None
    ) = None
    dispatch_gate: anyio.Event | None = None


async def prepare_worker_constraints(database: Database, repository_root: Path) -> None:
    """Upgrade immutable limits before workers can claim previously queued work."""

    search_routes = await database.facebook_search_work_routes()
    request_budget_routes = await database.legacy_facebook_request_budget_routes()
    constraints = (
        *analysis_work_constraints(),
        *refresh_search_work_constraints(),
        *request_missing_analyses_work_constraints(),
        image_session_work_constraint(),
        marketplace_image_network_constraint(),
        ebay_search_work_constraint(),
        *ebay_item_work_constraints(),
        *refresh_ebay_search_work_constraints(),
        *pipeline_work_constraints(),
        *(facebook_search_work_constraint(route) for route in search_routes),
        *(facebook_effective_search_work_constraint(route) for route in search_routes),
        *(facebook_network_path_constraint(route) for route in request_budget_routes),
    )
    retired_identifiers = (
        *legacy_image_session_work_constraint_identifiers(),
        ("carl", "facebook", "image", "network_activity_concurrency", "all_cdns", "v3"),
        *legacy_ebay_item_work_constraint_identifiers(),
        *await database.legacy_facebook_search_constraint_identifiers(),
        *(
            legacy_facebook_network_path_constraint_identifier(route)
            for route in request_budget_routes
        ),
    )
    operation_identifier = _identifier()
    started = perf_counter_ns()
    provenance = await collect_code_provenance_async(repository_root)
    # Complete the policy's audit even when shutdown arrives during its upgrade.
    with anyio.CancelScope(shield=True):
        await database.begin_operation(
            operation_id=operation_identifier,
            component=Component(
                identifier=ComponentId(("carl", "work", "prepare_constraints")),
                output_schema_version=1,
                implementation=prepare_worker_constraints,
            ),
            provenance=provenance,
            invocation=process_invocation(),
            configuration={
                "retired_identifiers": [list(identifier) for identifier in retired_identifiers],
                "constraints": [constraint.model_dump(mode="json") for constraint in constraints],
            },
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        try:
            await database.supersede_constraints(
                retired_identifiers=retired_identifiers,
                replacements=constraints,
                operation_identifier=operation_identifier,
                at_utc_ns=time_ns(),
                reason=(
                    "Apply shared image concurrency 25, eBay description concurrency 10, "
                    "and Facebook search concurrency one for Proton, twenty for Decodo "
                    "per requested/effective acquisition route; count only Facebook HTTP "
                    "requests in Facebook route budgets"
                ),
            )
            await database.complete_operation(
                operation_id=operation_identifier,
                records=(),
                artifacts=(),
                outputs=(),
                result={"state": "prepared"},
                ended_at_utc=datetime.now(UTC).isoformat(),
                duration_ns=perf_counter_ns() - started,
            )
        except BaseException as error:
            await database.fail_operation(
                operation_id=operation_identifier,
                error={"kind": "constraint_policy_failure", "type": type(error).__name__},
                result={"state": "failed"},
                ended_at_utc=datetime.now(UTC).isoformat(),
                duration_ns=perf_counter_ns() - started,
            )
            raise


@asynccontextmanager
async def managed_worker_pool(
    database: Database,
    repository_root: Path,
    *,
    eligible_identifiers: tuple[str, ...] | None = None,
    claude: ClaudeCli | None = None,
    worker_count: int | None = None,
    composition: RuntimeComposition | None = None,
) -> AsyncGenerator[WorkRuntime]:
    """Run bounded workers against one shared durable queue."""

    directories = user_directories() if composition is None else composition.directories
    scheduling_now = time_ns if composition is None else composition.scheduling_utc_now_ns

    await prepare_worker_constraints(database, repository_root)
    _ = await database.reconcile_terminal_network_activities(recorded_at_utc_ns=time_ns())
    claude = ClaudeCli() if claude is None else claude
    analysis_registry = build_analysis_worker_registry(
        AnalysisWorkerDependencies(
            database=database,
            claude=claude,
            new_identifier=_identifier,
        )
    )
    refresh_registry = build_refresh_worker_registry(
        RefreshWorkerDependencies(
            database=database,
            new_identifier=_identifier,
            code_provenance=lambda: collect_code_provenance_async(repository_root),
        )
    )
    ebay_analysis_registry = build_ebay_analysis_worker_registry(
        AnalysisWorkerDependencies(database=database, claude=claude, new_identifier=_identifier)
    )
    ebay_refresh_registry = build_ebay_refresh_worker_registry(
        EbayRefreshWorkerDependencies(database=database, new_identifier=_identifier)
    )
    facebook_listing_registry = build_facebook_listing_worker_registry(
        FacebookListingWorkerDependencies(database=database, new_identifier=_identifier)
    )
    proton_manager = SharedProtonWireproxyManager()
    facebook_registry = build_routed_facebook_worker_registry(
        database=database,
        directories=directories,
        new_identifier=_identifier,
        proton_manager=proton_manager,
    )
    ebay_registry = build_ebay_worker_registry(
        EbaySearchWorkerDependencies(
            database=database,
            directories=directories,
            new_identifier=_identifier,
        )
    )
    analysis_batch_registry = build_analysis_batch_worker_registry(
        AnalysisBatchWorkerDependencies(
            database=database,
            application=ReviewApplication(
                database=database,
                repository_root=repository_root,
                claude=claude,
            ),
        )
    )
    pipeline_registry = build_pipeline_worker_registry(
        PipelineWorkerDependencies(database=database)
    )
    pipeline_listing_registry = build_pipeline_listing_worker_registry(
        PipelineListingWorkerDependencies(
            database=database,
            application=ReviewApplication(
                database=database,
                repository_root=repository_root,
                claude=claude,
            ),
        )
    )
    ebay_item_registry = build_ebay_item_worker_registry(
        EbayItemWorkerDependencies(
            database=database,
            directories=directories,
            new_identifier=_identifier,
            proton_manager=proton_manager,
        )
    )
    travel_registry = build_travel_worker_registry(
        TravelWorkerDependencies(
            service=ConfiguredTravelService(directories=directories),
            new_identifier=_identifier,
        )
    )
    registry = WorkHandlerRegistry(
        handlers=(
            *analysis_registry.handlers,
            *refresh_registry.handlers,
            *facebook_registry.handlers,
            *ebay_registry.handlers,
            *ebay_item_registry.handlers,
            *analysis_batch_registry.handlers,
            *ebay_analysis_registry.handlers,
            *ebay_refresh_registry.handlers,
            *facebook_listing_registry.handlers,
            *pipeline_registry.handlers,
            *pipeline_listing_registry.handlers,
            *travel_registry.handlers,
            *build_travel_data_worker_registry(TravelDataManager(directories)).handlers,
        )
    )
    manager = stage_manager(database)
    if composition is not None:
        manager = replace(manager, utc_now_ns=scheduling_now)
    provenance = await collect_code_provenance_async(repository_root)
    invocation = process_invocation()
    residents = ResidentSessionPool(manager, StageHandlerRegistry((), ()), provenance, invocation)
    engine = DatasetSetupEngine(directories, manager=TravelDataManager(directories))
    monitor = (
        ConnectivityMonitor(database, _identifier, scheduling_now)
        if composition is None or composition.connectivity_probe is None
        else ConnectivityMonitor(
            database, _identifier, scheduling_now, probe=composition.connectivity_probe
        )
    )
    policy = StagePolicy(monitor)
    session_registry = build_session_stage_registry(
        facebook=FacebookSessionStageDependencies(
            database, directories, _identifier, proton_manager, residents
        ),
        ebay=EbaySessionStageDependencies(
            database, directories, _identifier, proton_manager, residents
        ),
    )
    stage_registries = (
        (
            build_stateless_stage_registry
            if composition is None
            else composition.stateless_registry_builder
        )(
            facebook=FacebookStatelessStageDependencies(
                database, directories, _identifier, proton_manager, utc_now_ns=scheduling_now
            ),
            ebay=EbayItemWorkerDependencies(
                database, directories, _identifier, proton_manager=proton_manager
            ),
        ),
        session_registry,
        build_facebook_analysis_stage_registry(
            AnalysisWorkerDependencies(database, claude, _identifier)
        ),
        build_ebay_analysis_stage_registry(
            AnalysisWorkerDependencies(database, claude, _identifier)
        ),
        (
            composition.coordinator_registry_builder(database, registry)
            if composition is not None and composition.coordinator_registry_builder is not None
            else build_coordinator_stage_registry(database, registry)
            if composition is None
            else build_coordinator_stage_registry(database, registry, utc_now_ns=scheduling_now)
        ),
        build_travel_stage_registry(
            TravelStageDependencies(
                database, ConfiguredTravelService(directories=directories), _identifier
            )
        ),
        build_dataset_setup_prepare_registry(database, engine),
    )
    stages = PolicyStageRegistry(
        adapters=tuple(
            replace(adapter, initial_resources=(ResourceRequest(name="cpu"),))
            if adapter.initial_resources is None
            else adapter
            for stage_registry in stage_registries
            for adapter in stage_registry.adapters
        ),
        handlers=(
            *(
                handler
                for stage_registry in stage_registries
                for handler in stage_registry.handlers
            ),
            policy.handler(),
            invalid_initial_handler(),
        ),
        policy=policy,
    )
    if {
        (adapter.capability.kind, adapter.capability.payload_schema_version)
        for adapter in stages.adapters
    } != {
        (capability.kind, capability.payload_schema_version) for capability in registry.capabilities
    }:
        raise ValueError("The stage runtime must cover every supported work capability")
    await database.enable_fair_scheduling(
        scope=manager.resources.scope,
        now_utc_ns=manager.utc_now_ns(),
        initial_resources=tuple(
            (adapter.capability, adapter.initial_resources or ()) for adapter in stages.adapters
        ),
        resident_capabilities=tuple(adapter.capability for adapter in session_registry.adapters),
        resident_state_bytes=residents.state_bytes,
    )
    residents.registry = stages
    async with proton_manager, anyio.create_task_group() as provider_tasks:
        proton_manager.watcher_task_group = provider_tasks
        try:
            async with (
                residents.managed(),
                managed_dataset_workflow_service(
                    manager,
                    directories,
                    lambda owner: build_dataset_setup_owned_registry(database, engine, owner),
                    provenance,
                    invocation,
                ) as datasets,
                anyio.create_task_group() as task_group,
            ):

                async def submit_owned(identifier: str) -> bool:
                    stage = await database.stage(identifier)
                    definition = await database.work_definition(stage.work_item_identifier)
                    adapter = next(
                        adapter
                        for adapter in stages.adapters
                        if adapter.capability.kind == definition.kind
                        and adapter.capability.payload_schema_version
                        == definition.payload_schema_version
                    )
                    return await submit_dataset_setup_stage(
                        manager,
                        engine,
                        stage,
                        adapter.component,
                        provenance,
                        invocation,
                        service=datasets,
                    )

                dispatcher = StageDispatcher(
                    manager,
                    stages,
                    residents,
                    provenance,
                    invocation,
                    submit_owned,
                    lambda kind: kind == OWNED_STAGE_KIND,
                    eligible_identifiers=eligible_identifiers,
                    execution_count=worker_count,
                )

                async def dispatch() -> None:
                    if composition is not None and composition.dispatch_gate is not None:
                        await composition.dispatch_gate.wait()
                    await dispatcher.run()

                # Pool tasks own independent admission, even if a caller opens
                # this scope while holding an unrelated resource reservation.
                _ = task_group.start_soon(manager.resources._run_background, policy.run, ())  # pyright: ignore[reportPrivateUsage]
                _ = task_group.start_soon(manager.resources._run_background, dispatch, ())  # pyright: ignore[reportPrivateUsage]
                try:
                    yield WorkRuntime(database=database, registry=registry, stages=stages)
                finally:
                    task_group.cancel_scope.cancel()
        finally:
            provider_tasks.cancel_scope.cancel()


@asynccontextmanager
async def managed_work_runtime(
    database_path: Path,
    repository_root: Path,
) -> AsyncGenerator[WorkRuntime]:
    """Own a database and worker pool independently of an MCP client."""

    async with (
        Database.managed(database_path) as database,
        managed_worker_pool(database, repository_root) as runtime,
    ):
        yield runtime


async def work_forever(database_path: Path, repository_root: Path) -> None:
    """Process durable work until the foreground process is interrupted."""

    async with managed_work_runtime(database_path, repository_root):
        await anyio.sleep_forever()
