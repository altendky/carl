"""One physically fenced root owner for dataset work, without execution residency."""

# The root service is the sole caller of the owner transfer/fence internals.
# pyright: reportPrivateUsage=false

import json
import secrets
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractContextManager, asynccontextmanager
from pathlib import Path
from time import time_ns
from typing import Literal, final

import anyio

from carl.core.components import Component
from carl.core.dataset_workflows import (
    TERMINAL_DATASET_WORKFLOW_STATES,
    DatasetWorkflowDefinition,
    DatasetWorkflowFence,
    DatasetWorkflowJournal,
)
from carl.core.models import CodeProvenance, JsonValue
from carl.core.stages import StageDefinition, StageWaitKind, WaitWork
from carl.core.worker import CompletedWork
from carl.io.dataset_workflow_files import (
    DatasetWorkflowBusy,
    DatasetWorkflowError,
    DatasetWorkflowFenceLost,
    activate_journal,
    active_journal,
    dataset_workflow_gate,
    lock_identity,
    private_directory,
    read_journal,
    workflow_directory,
    write_journal,
)
from carl.io.native_build_protocol import NativeBuildCleanupError
from carl.io.paths import CarlDirectories
from carl.io.resource_permits import current_resource_manager
from carl.io.stage_publication import StagePublication
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import StageManager

__all__ = [
    "DatasetWorkflowBusy",
    "DatasetWorkflowError",
    "DatasetWorkflowFenceLost",
    "DatasetWorkflowOwner",
    "DatasetWorkflowService",
    "get_dataset_workflow_service",
    "managed_dataset_workflow_service",
    "open_dataset_workflow",
]


def _unconfirmed_cleanup(error: BaseException) -> bool:
    if isinstance(error, NativeBuildCleanupError):
        return True
    return isinstance(error, BaseExceptionGroup) and any(
        _unconfirmed_cleanup(child) for child in error.exceptions
    )


@final
class DatasetWorkflowOwner:
    """A generation handle; mutations belong to exactly one task, never an OFD peer."""

    def __init__(
        self,
        directory: Path,
        descriptor: int,
        gate: AbstractContextManager[int],
        journal: DatasetWorkflowJournal,
    ) -> None:
        self.directory = directory
        self._descriptor = descriptor
        self._gate = gate
        self._journal = journal
        self._task: int | None = anyio.get_current_task().id
        self._transfer_pending = False
        self._transferred = False
        self._closed = False

    @property
    def fence(self) -> DatasetWorkflowFence:
        return self._journal.fence

    @property
    def journal(self) -> DatasetWorkflowJournal:
        return DatasetWorkflowJournal.model_validate_json(self._journal.model_dump_json())

    @property
    def lock_fd(self) -> int:
        self._check()
        return self._descriptor

    def _check(self) -> None:
        if self._closed or self._task != anyio.get_current_task().id:
            raise DatasetWorkflowFenceLost(
                "Dataset workflow handle belongs to another task or closed generation"
            )
        if lock_identity(self._descriptor) != self._journal.lock_identity:
            raise DatasetWorkflowFenceLost("Dataset workflow physical gate changed")
        latest = read_journal(self.directory)
        if (
            latest is None
            or latest.fence != self.fence
            or latest.revision != self._journal.revision
        ):
            raise DatasetWorkflowFenceLost("Dataset workflow generation or revision changed")

    def _prepare_transfer(self) -> None:
        self._check()
        if self._transferred:
            raise DatasetWorkflowFenceLost("Dataset workflow ownership can transfer only once")
        self._task = None
        self._transfer_pending = True

    def _take_transfer(self) -> None:
        if self._closed or not self._transfer_pending or self._transferred:
            raise DatasetWorkflowFenceLost("Dataset workflow transfer is no longer available")
        self._task = anyio.get_current_task().id
        self._transfer_pending = False
        self._transferred = True

    async def _update(self, **changes: object) -> None:
        self._check()
        if self._journal.state in TERMINAL_DATASET_WORKFLOW_STATES:
            raise DatasetWorkflowFenceLost("Dataset workflow is terminal")
        if all(getattr(self._journal, name) == value for name, value in changes.items()):
            return
        value = self._journal.model_dump(mode="json")
        value.update(changes)
        value.update(revision=self._journal.revision + 1, updated_at_utc_ns=time_ns())
        updated = DatasetWorkflowJournal.model_validate_json(json.dumps(value))
        self._journal = write_journal(
            self.directory,
            updated,
            expected_fence=self.fence,
            expected_revision=self._journal.revision,
        )

    async def freeze_plan(self, plan: JsonValue) -> None:
        self._check()
        if self._journal.plan_frozen:
            if self._journal.plan != plan:
                raise DatasetWorkflowFenceLost("Dataset workflow plan is immutable")
            return
        await self._update(plan=plan, plan_frozen=True)

    async def checkpoint(
        self, phase: str, value: JsonValue, *, receipt_references: tuple[str, ...] = ()
    ) -> None:
        await self._update(
            phase=phase,
            checkpoint=value,
            receipt_references=receipt_references,
            state="running",
            next_eligible_at_utc_ns=None,
        )

    async def wait_until(self, deadline_utc_ns: int | None) -> None:
        await self._update(state="waiting", next_eligible_at_utc_ns=deadline_utc_ns)

    async def finish(
        self, state: Literal["completed", "cancelled", "failed"] = "completed"
    ) -> None:
        if state not in TERMINAL_DATASET_WORKFLOW_STATES:
            raise ValueError("Dataset workflow finish requires a terminal state")
        await self._update(state=state, next_eligible_at_utc_ns=None)

    async def park(self, *, quarantined: bool = False) -> None:
        self._check()
        if self._journal.state not in TERMINAL_DATASET_WORKFLOW_STATES:
            await self._update(state="quarantined" if quarantined else "parked")

    async def aclose(self) -> None:
        if self._closed:
            return
        if self._task != anyio.get_current_task().id:
            raise DatasetWorkflowFenceLost("Only the owning task can close a dataset workflow")
        try:
            self._check()
            if (
                self._journal.state not in TERMINAL_DATASET_WORKFLOW_STATES
                and self._journal.state != "quarantined"
            ):
                await self.park()
        finally:
            # Other retained native guard copies continue holding this same OFD.
            # Explicit LOCK_UN here would break their physical ownership fence.
            self._closed = True
            _ = self._gate.__exit__(None, None, None)


def _open_owner(
    directories: CarlDirectories, definition: DatasetWorkflowDefinition, *, resume: bool
) -> DatasetWorkflowOwner:
    gate = dataset_workflow_gate(directories, allow_reserved=True)
    descriptor = gate.__enter__()
    try:
        active = active_journal(directories)
        if (
            active is not None
            and active.state not in TERMINAL_DATASET_WORKFLOW_STATES
            and (active.definition != definition or not resume)
        ):
            raise DatasetWorkflowBusy(
                "A nonterminal dataset workflow requires explicit matching resume"
            )
        private_directory(directories.travel_data_directory / ".workflows")
        directory = workflow_directory(directories, definition.workflow_identifier)
        private_directory(directory)
        previous = read_journal(directory)
        if previous is not None:
            if previous.definition != definition:
                raise DatasetWorkflowFenceLost(
                    "Dataset workflow definition and queued binding are immutable"
                )
            if not resume or previous.state in TERMINAL_DATASET_WORKFLOW_STATES:
                raise DatasetWorkflowBusy(
                    "Dataset workflow already exists; resume a nonterminal generation"
                )
            if previous.lock_identity != lock_identity(descriptor):
                raise DatasetWorkflowFenceLost("Dataset workflow lock inode changed")
        fence = DatasetWorkflowFence(
            workflow_identifier=definition.workflow_identifier,
            generation=1 if previous is None else previous.fence.generation + 1,
            token=secrets.token_hex(16),
        )
        value = {} if previous is None else previous.model_dump(mode="json")
        value.update(
            definition=definition.model_dump(mode="json"),
            fence=fence.model_dump(mode="json"),
            lock_identity=list(lock_identity(descriptor)),
            revision=1 if previous is None else previous.revision + 1,
            state="running",
            updated_at_utc_ns=time_ns(),
        )
        journal = DatasetWorkflowJournal.model_validate_json(json.dumps(value))
        journal = write_journal(
            directory,
            journal,
            expected_fence=None if previous is None else previous.fence,
            expected_revision=None if previous is None else previous.revision,
        )
        activate_journal(directories, journal)
        return DatasetWorkflowOwner(directory, descriptor, gate, journal)
    except BaseException:
        _ = gate.__exit__(None, None, None)
        raise


@asynccontextmanager
async def open_dataset_workflow(
    directories: CarlDirectories, definition: DatasetWorkflowDefinition, *, resume: bool = False
) -> AsyncGenerator[DatasetWorkflowOwner]:
    """Foreground owner: no work row or execution resource is invented."""

    owner = _open_owner(directories, definition, resume=resume)
    try:
        yield owner
    except BaseException as error:
        if _unconfirmed_cleanup(error):
            with anyio.CancelScope(shield=True):
                await owner.park(quarantined=True)
        raise
    finally:
        with anyio.CancelScope(shield=True):
            await owner.aclose()


@final
class DatasetWorkflowService:
    """A bounded root task serializes ordinary stages while retaining only the file gate."""

    def __init__(
        self,
        manager: StageManager,
        directories: CarlDirectories,
        registry_for_owner: Callable[[DatasetWorkflowOwner], StageHandlerRegistry],
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        *,
        _register_root_closer: bool = True,
    ) -> None:
        self.manager = manager
        self.directories = directories
        self.registry_for_owner = registry_for_owner
        self.provenance = provenance
        self.invocation = invocation
        self._active: DatasetWorkflowOwner | None = None
        self._scope: anyio.CancelScope | None = None
        self._done = anyio.Event()
        self._done.set()
        self._closing = False
        if _register_root_closer:
            manager.resources.register_closer("dataset-workflow", self.aclose)

    async def submit(
        self,
        stage: StageDefinition,
        *,
        component: Component,
        definition: DatasetWorkflowDefinition,
        resume: bool = False,
    ) -> bool:
        if current_resource_manager() is not self.manager.resources or self._closing:
            raise DatasetWorkflowError(
                "Dataset workflow requires its active managed root lifecycle"
            )
        binding = definition.binding
        if (
            binding is None
            or binding.database_path != self.manager.database.path.absolute()
            or binding.work_item_identifier != stage.work_item_identifier
            or binding.operation_identifier != stage.operation_identifier
            or stage.resident_session_identifier is not None
        ):
            raise DatasetWorkflowError(
                "Dataset stages require their genuine immutable queued binding"
            )
        if self._active is not None:
            return False
        try:
            owner = _open_owner(self.directories, definition, resume=resume)
        except DatasetWorkflowBusy:
            return False
        try:
            current = await self.manager.database.active_stage_for_work(
                stage.work_item_identifier, stage.operation_identifier
            )
            if current is None:
                state = await self._terminal_state(definition)
                if state is not None:
                    await owner.finish(state)
                    await owner.aclose()
                    return True
                await self.manager.database.register_stage(stage, created_at_utc_ns=time_ns())
                current = stage
            await owner._update(current_stage_identifier=current.identifier)
            owner._prepare_transfer()
            self._active = owner
            self._done = anyio.Event()
            self.manager.resources.start_background(self._run, owner, component)
        except BaseException:
            if owner._transfer_pending:
                owner._take_transfer()
            with anyio.CancelScope(shield=True):
                await owner.aclose()
            self._active = None
            self._done.set()
            raise
        return True

    async def _run(self, owner: object, component: object) -> None:
        if not isinstance(owner, DatasetWorkflowOwner) or not isinstance(component, Component):
            raise TypeError("Invalid dataset root owner task")
        owner._take_transfer()
        try:
            with anyio.CancelScope() as scope:
                self._scope = scope
                registry = self.registry_for_owner(owner)
                body_error: BaseException | None = None
                async with anyio.create_task_group() as tasks:
                    _ = tasks.start_soon(self._watch_cancellation, owner.journal.definition, scope)
                    try:
                        await self._stages(owner, registry, component)
                    except BaseException as error:
                        body_error = error
                    finally:
                        tasks.cancel_scope.cancel()
                if body_error is not None:
                    raise body_error
            if not self._closing and owner.journal.state not in TERMINAL_DATASET_WORKFLOW_STATES:
                state = await self._terminal_state(owner.journal.definition)
                if state is not None:
                    await owner.finish(state)
        except BaseException as error:
            # Unknown phase failures keep their durable reservation and files.
            with anyio.CancelScope(shield=True):
                if _unconfirmed_cleanup(error):
                    await owner.park(quarantined=True)
                elif isinstance(error, Exception):
                    state = await self._terminal_state(owner.journal.definition)
                    if state is not None:
                        await owner.finish(state)
                    else:
                        await owner.park()
                else:
                    raise
        finally:
            with anyio.CancelScope(shield=True):
                try:
                    await owner.aclose()
                finally:
                    self._active = None
                    self._scope = None
                    self._done.set()

    async def _terminal_state(
        self, definition: DatasetWorkflowDefinition
    ) -> Literal["completed", "cancelled", "failed"] | None:
        binding = definition.binding
        assert binding is not None
        work, operation = await self.manager.database.dataset_workflow_binding_state(
            binding.work_item_identifier, binding.operation_identifier
        )
        # A later attempt may already have changed the work's state. The frozen
        # operation's result remains authoritative for this workflow generation.
        # Cancellation can complete the operation as bookkeeping, so explicit
        # cancellation still wins after physical stage cleanup has finished.
        if work in ("cancelled", "cancelling"):
            return "cancelled"
        if operation == "completed":
            return "completed"
        if operation == "failed":
            return "failed"
        if work == "terminal_failure":
            return "failed"
        if work == "completed":
            return "completed"
        return None

    async def _watch_cancellation(
        self, definition: DatasetWorkflowDefinition, scope: anyio.CancelScope
    ) -> None:
        binding = definition.binding
        assert binding is not None
        while True:
            if await self.manager.database.work_cancellation_requested(
                binding.work_item_identifier
            ):
                scope.cancel()
                return
            await anyio.sleep(0.1)

    async def _stages(
        self, owner: DatasetWorkflowOwner, registry: StageHandlerRegistry, component: Component
    ) -> None:
        binding = owner.journal.definition.binding
        assert binding is not None
        while not self._closing:
            database = self.manager.database
            if await database.work_cancellation_requested(binding.work_item_identifier):
                await owner.finish("cancelled")
                return
            stage = await database.active_stage_for_work(
                binding.work_item_identifier, binding.operation_identifier
            )
            if stage is None:
                state = await self._terminal_state(owner.journal.definition)
                if state is not None:
                    await owner.finish(state)
                return
            await owner._update(current_stage_identifier=stage.identifier)
            _ = registry.require_stage(stage)
            deadline: int | None = None
            timed_wait = False
            finished: Literal["completed", "failed"] | None = None
            next_stage_identifier: str | None = None
            async with self.manager.admit(
                stage.identifier,
                component=component,
                provenance=self.provenance,
                invocation=self.invocation,
            ) as admission:
                execution = admission.execution
                if execution is None:
                    deadline = admission.result.next_eligible_at_utc_ns
                    timed_wait = admission.result.wait_kind == StageWaitKind.TIME
                else:
                    result = await registry.run(execution)
                    await execution.apply(result)
                    outcome = result.outcome if isinstance(result, StagePublication) else result
                    if isinstance(outcome, WaitWork):
                        deadline = outcome.continuation.not_before_utc_ns
                        timed_wait = True
                        next_stage_identifier = outcome.continuation.identifier
                    else:
                        finished = "completed" if isinstance(outcome, CompletedWork) else "failed"
                    del result, outcome
            if finished is not None:
                await owner.finish(finished)
                return
            if next_stage_identifier is not None:
                await owner._update(current_stage_identifier=next_stage_identifier)
            # No stage guard, HTTP or CPU permit survives a coordinator wait.
            await owner.wait_until(deadline)
            delay = 0.01 if timed_wait else 0.25
            if deadline is not None:
                delay = max(0.01, (deadline - time_ns()) / 1e9)
            await anyio.sleep(delay if timed_wait else min(0.25, delay))

    async def aclose(self) -> None:
        self._closing = True
        if self._scope is not None:
            self._scope.cancel()
        with anyio.CancelScope(shield=True):
            await self._done.wait()


def get_dataset_workflow_service(
    manager: StageManager,
    directories: CarlDirectories,
    registry_for_owner: Callable[[DatasetWorkflowOwner], StageHandlerRegistry],
    provenance: CodeProvenance,
    invocation: dict[str, JsonValue],
) -> DatasetWorkflowService:
    return manager.resources.get_or_create_service(
        "dataset-workflow",
        lambda: DatasetWorkflowService(
            manager, directories, registry_for_owner, provenance, invocation
        ),
    )


@asynccontextmanager
async def managed_dataset_workflow_service(
    manager: StageManager,
    directories: CarlDirectories,
    registry_for_owner: Callable[[DatasetWorkflowOwner], StageHandlerRegistry],
    provenance: CodeProvenance,
    invocation: dict[str, JsonValue],
) -> AsyncGenerator[DatasetWorkflowService]:
    """Own a fresh queued dataset service for one worker runtime lifetime."""

    if current_resource_manager() is not manager.resources:
        raise DatasetWorkflowError("Dataset workflow service requires its managed database scope")
    service = DatasetWorkflowService(
        manager,
        directories,
        registry_for_owner,
        provenance,
        invocation,
        _register_root_closer=False,
    )
    try:
        yield service
    finally:
        with anyio.CancelScope(shield=True):
            await service.aclose()
