"""Long-lived ownership of Carl's durable work queue."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
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
from carl.core.facebook_images import image_session_work_constraint
from carl.core.facebook_refresh import refresh_search_work_constraints
from carl.core.item_analysis import analysis_work_constraints
from carl.core.worker import WorkerSettings
from carl.facebook_analysis_workers import (
    AnalysisWorkerDependencies,
    build_analysis_worker_registry,
)
from carl.facebook_refresh_workers import (
    RefreshWorkerDependencies,
    build_refresh_worker_registry,
)
from carl.facebook_routed_workers import build_routed_facebook_worker_registry
from carl.io.claude import ClaudeCli
from carl.io.paths import user_directories
from carl.io.proton import SharedProtonWireproxyManager
from carl.io.provenance import collect_code_provenance_async, process_invocation
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, WorkHandlerRegistry, run_worker_pool
from carl.review import ReviewApplication


def _identifier() -> str:
    return str(uuid4())


@dataclass(frozen=True, slots=True)
class WorkRuntime:
    """Resources owned by one bounded worker-pool lifetime."""

    database: Database
    registry: WorkHandlerRegistry


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
    claude = ClaudeCli()
    for constraint in (
        *analysis_work_constraints(),
        *refresh_search_work_constraints(),
        *request_missing_analyses_work_constraints(),
        image_session_work_constraint(),
    ):
        await database.register_constraint(constraint, registered_at_utc_ns=time_ns())
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
    proton_manager = SharedProtonWireproxyManager()
    facebook_registry = build_routed_facebook_worker_registry(
        database=database,
        directories=user_directories(),
        new_identifier=_identifier,
        proton_manager=proton_manager,
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
    registry = WorkHandlerRegistry(
        handlers=(
            *analysis_registry.handlers,
            *refresh_registry.handlers,
            *facebook_registry.handlers,
            *analysis_batch_registry.handlers,
        )
    )
    services = WorkerRuntimeServices(
        new_identifier=_identifier,
        utc_now_ns=time_ns,
        monotonic_ns=perf_counter_ns,
        code_provenance=lambda: collect_code_provenance_async(repository_root),
        invocation=process_invocation,
    )
    stop = anyio.Event()
    async with anyio.create_task_group() as task_group:
        proton_manager.watcher_task_group = task_group
        async with proton_manager:
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
