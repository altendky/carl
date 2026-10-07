"""Long-lived ownership of Carl's durable work queue."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from time import perf_counter_ns, time_ns
from uuid import uuid4

import anyio

from carl.analysis_batch_workers import (
    AnalysisBatchWorkerDependencies,
    build_analysis_batch_worker_registry,
)
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
from carl.core.item_analysis import analysis_work_constraints
from carl.core.marketplace_images import marketplace_image_network_constraint
from carl.core.pipeline import pipeline_work_constraints
from carl.core.worker import WorkerSettings
from carl.ebay_analysis_workers import build_ebay_analysis_worker_registry
from carl.ebay_item_workers import EbayItemWorkerDependencies, build_ebay_item_worker_registry
from carl.ebay_refresh_workers import (
    EbayRefreshWorkerDependencies,
    build_ebay_refresh_worker_registry,
)
from carl.ebay_workers import EbaySearchWorkerDependencies, build_ebay_worker_registry
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
from carl.io.claude import ClaudeCli
from carl.io.connectivity import ConnectivityMonitor
from carl.io.paths import user_directories
from carl.io.proton import SharedProtonWireproxyManager
from carl.io.provenance import collect_code_provenance_async, process_invocation
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, WorkHandlerRegistry, run_worker_pool
from carl.pipeline_listing_workers import (
    PipelineListingWorkerDependencies,
    build_pipeline_listing_worker_registry,
)
from carl.pipeline_workers import PipelineWorkerDependencies, build_pipeline_worker_registry
from carl.review import ReviewApplication


def _identifier() -> str:
    return str(uuid4())


@dataclass(frozen=True, slots=True)
class WorkRuntime:
    """Resources owned by one bounded worker-pool lifetime."""

    database: Database
    registry: WorkHandlerRegistry


async def prepare_worker_constraints(database: Database, repository_root: Path) -> None:
    """Upgrade immutable limits before workers can claim previously queued work."""

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
    )
    retired_identifiers = (
        *legacy_image_session_work_constraint_identifiers(),
        ("carl", "facebook", "image", "network_activity_concurrency", "all_cdns", "v3"),
        *legacy_ebay_item_work_constraint_identifiers(),
        *await database.legacy_facebook_search_constraint_identifiers(),
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
                    "and effective-provider Facebook search admission"
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
) -> AsyncGenerator[WorkRuntime]:
    """Run bounded workers against one shared durable queue."""

    settings = WorkerSettings(
        worker_count=30,
        lease_duration_ns=600_000_000_000,
        renewal_interval_ns=30_000_000_000,
        idle_poll_interval_ns=1_000_000_000,
    )
    await prepare_worker_constraints(database, repository_root)
    await database.reconcile_terminal_network_activities(recorded_at_utc_ns=time_ns())
    claude = ClaudeCli()
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
        directories=user_directories(),
        new_identifier=_identifier,
        proton_manager=proton_manager,
    )
    ebay_registry = build_ebay_worker_registry(
        EbaySearchWorkerDependencies(
            database=database,
            directories=user_directories(),
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
            directories=user_directories(),
            new_identifier=_identifier,
            proton_manager=proton_manager,
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
        )
    )
    services = WorkerRuntimeServices(
        new_identifier=_identifier,
        utc_now_ns=time_ns,
        monotonic_ns=perf_counter_ns,
        code_provenance=lambda: collect_code_provenance_async(repository_root),
        invocation=process_invocation,
        connectivity_monitor=ConnectivityMonitor(
            database=database, new_identifier=_identifier, utc_now_ns=time_ns
        ),
    )
    stop = anyio.Event()
    async with proton_manager, anyio.create_task_group() as task_group:
        proton_manager.watcher_task_group = task_group
        task_group.start_soon(
            partial(
                run_worker_pool,
                database=database,
                registry=registry,
                settings=settings,
                services=services,
                stop=stop,
            )
        )
        try:
            yield WorkRuntime(database=database, registry=registry)
        finally:
            stop.set()
            task_group.cancel_scope.cancel()


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
