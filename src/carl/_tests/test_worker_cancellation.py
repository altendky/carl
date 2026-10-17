"""Explicit queue cancellation stops attempts without failure or lost checkpoints."""

from collections.abc import Awaitable, Callable
from pathlib import Path
from time import perf_counter_ns, time_ns
from typing import Any
from uuid import uuid4

import anyio
import pytest

from carl.core.components import Component, ComponentId
from carl.core.models import CodeProvenance, NamedOutput, RecordDraft, StrictModel
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
    WorkState,
)
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkerSettings,
    WorkOutcome,
)
from carl.io import worker as worker_io
from carl.io.sqlite import Database
from carl.io.worker import (
    TypedWorkHandler,
    WorkerRuntimeServices,
    WorkHandlerRegistry,
    execute_lease,
)


class _Payload(StrictModel):
    value: int


_KIND = ("carl", "test", "explicit_cancellation")
_SETTINGS = WorkerSettings(
    worker_count=1,
    lease_duration_ns=1_000_000_000,
    renewal_interval_ns=10_000_000,
    idle_poll_interval_ns=10_000_000,
)


async def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="dirty",
        package_version="test",
        python_implementation="test",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


def _registry(
    handler: Callable[[_Payload, AttemptContext], Awaitable[WorkOutcome]],
) -> WorkHandlerRegistry:
    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=_KIND, payload_schema_version=1),
                component=Component(ComponentId(_KIND), 1, handler),
                payload_type=_Payload,
                handler=handler,
            ),
        )
    )


async def _enqueue(database: Database, identifier: str, *, parent: str | None = None) -> None:
    await database.enqueue_work(
        WorkDefinition(
            identifier=identifier,
            kind=_KIND,
            payload_schema_version=1,
            payload={"value": 1},
            deduplication_identity=(identifier,),
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=_KIND),
            ),
        ),
        WorkRequester(
            request_identifier=str(uuid4()),
            kind=("carl", "test", "requester"),
            identifier=parent if parent is not None else f"requester-{identifier}",
            context={},
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )


def _services() -> WorkerRuntimeServices:
    return WorkerRuntimeServices(
        new_identifier=lambda: str(uuid4()),
        utc_now_ns=time_ns,
        monotonic_ns=perf_counter_ns,
        code_provenance=_provenance,
        invocation=lambda: {},
    )


@pytest.mark.anyio
@pytest.mark.parametrize("cancel_before_dispatch", (False, True))
@pytest.mark.parametrize("cancel_target", ("root", "child"))
async def test_explicit_cancellation_stops_at_dispatch_or_renewal(
    tmp_path: Path, cancel_before_dispatch: bool, cancel_target: str
) -> None:
    started = anyio.Event()
    stopped = anyio.Event()
    checkpoint = {"stage": "collecting_images", "saved_images": 1}
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:

        async def handler(payload: _Payload, context: AttemptContext) -> WorkOutcome:
            assert not cancel_before_dispatch
            assert payload.value == 1
            record = RecordDraft(
                identifier="completed-evidence",
                kind=("carl", "test", "evidence"),
                schema_version=1,
                value={"saved": True},
            )
            await database.publish_leased_operation_checkpoint(
                work_item_identifier=context.work_item_identifier,
                lease_token=context.lease_token,
                worker_identifier=context.worker_identifier,
                utc_now_ns=time_ns,
                operation_id=context.operation_identifier,
                records=(record,),
                artifacts=(),
                outputs=(NamedOutput(name=("evidence",), object_identifier=record.identifier),),
                checkpoint_result=checkpoint,
            )
            started.set()
            try:
                await anyio.sleep_forever()
            finally:
                stopped.set()
            raise AssertionError("cancelled handler returned")

        registry = _registry(handler)
        await _enqueue(database, "root")
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=_SETTINGS.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None
        if cancel_target == "child":
            await _enqueue(database, "child", parent="root")

        async def cancel() -> None:
            await database.cancel_work(
                cancel_target,
                reason="user stopped the run",
                caller="test",
                requested_at_utc_ns=time_ns(),
            )
            assert await database.work_state(cancel_target) is (
                WorkState.CANCELLING if cancel_target == "root" else WorkState.CANCELLED
            )

        async def run() -> None:
            assert claim.lease is not None
            await execute_lease(
                database=database,
                registry=registry,
                settings=_SETTINGS,
                services=_services(),
                lease=claim.lease,
            )

        with anyio.fail_after(5):
            if cancel_before_dispatch:
                await cancel()
                await run()
            else:
                async with anyio.create_task_group() as tasks:
                    tasks.start_soon(run)
                    await started.wait()
                    await cancel()

        work = await database.work("root")
        assert work["state"] == "cancelled"
        assert work["error"] is None
        if not cancel_before_dispatch:
            assert stopped.is_set()
            assert work["result"] == checkpoint
            assert (await database.get_record("completed-evidence"))[2] == {"saved": True}
            operations = work["operations"]
            assert isinstance(operations, list) and len(operations) == 1
            operation = operations[0]
            assert isinstance(operation, dict)
            operation_identifier = operation["operation_identifier"]
            assert isinstance(operation_identifier, str)
            assert (await database.operation(operation_identifier))["state"] != "failed"


@pytest.mark.anyio
async def test_cancelled_child_cancels_parent_before_it_can_restart_children(
    tmp_path: Path,
) -> None:
    async def handler(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        raise AssertionError((payload, context, "parent of cancelled child must not run"))

    registry = _registry(handler)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "root")
        await _enqueue(database, "child", parent="root")
        await database.cancel_work(
            "child", reason="skip this subtree", caller="test", requested_at_utc_ns=time_ns()
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=_SETTINGS.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None and claim.lease.work_item_identifier == "root"
        await execute_lease(
            database=database,
            registry=registry,
            settings=_SETTINGS,
            services=_services(),
            lease=claim.lease,
        )
        assert await database.work_state("root") is WorkState.CANCELLED
        assert (await database.work("root"))["error"] is None


@pytest.mark.anyio
async def test_cancellation_racing_with_success_cannot_publish_success(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:

        async def handler(payload: _Payload, context: AttemptContext) -> WorkOutcome:
            await database.cancel_work(
                context.work_item_identifier,
                reason="cancel at commit",
                caller="test",
                requested_at_utc_ns=time_ns(),
            )
            record = RecordDraft(
                identifier="just-finished-evidence",
                kind=("carl", "test", "evidence"),
                schema_version=1,
                value={"value": payload.value},
            )
            return CompletedWork(
                records=(record,),
                outputs=(NamedOutput(name=("evidence",), object_identifier=record.identifier),),
                result={"value": payload.value},
            )

        registry = _registry(handler)
        await _enqueue(database, "root")
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=_SETTINGS.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None
        outcome = await execute_lease(
            database=database,
            registry=registry,
            settings=_SETTINGS,
            services=_services(),
            lease=claim.lease,
        )
        assert await database.work_state("root") is WorkState.CANCELLED
        assert (await database.work("root"))["error"] is None
        assert outcome is None
        assert (await database.get_record("just-finished-evidence"))[2] == {"value": 1}


@pytest.mark.anyio
async def test_cancellation_at_handler_checkpoint_is_not_an_unhandled_failure(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:

        async def handler(payload: _Payload, context: AttemptContext) -> WorkOutcome:
            assert payload.value == 1
            await database.cancel_work(
                context.work_item_identifier,
                reason="stop before the next operation",
                caller="test",
                requested_at_utc_ns=time_ns(),
            )
            await database.publish_leased_operation_checkpoint(
                work_item_identifier=context.work_item_identifier,
                lease_token=context.lease_token,
                worker_identifier=context.worker_identifier,
                utc_now_ns=time_ns,
                operation_id=context.operation_identifier,
                records=(),
                artifacts=(),
                outputs=(),
                checkpoint_result={"must_not_publish": True},
            )
            raise AssertionError("operation accepted a cancelled lease")

        registry = _registry(handler)
        await _enqueue(database, "root")
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=_SETTINGS.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None
        outcome = await execute_lease(
            database=database,
            registry=registry,
            settings=_SETTINGS,
            services=_services(),
            lease=claim.lease,
        )
        assert outcome is None
        work = await database.work("root")
        assert work["state"] == "cancelled"
        assert work["error"] is None
        assert work["result"] is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    "commit_method",
    ("complete_leased_operation", "retry_leased_operation", "terminally_fail_leased_operation"),
)
async def test_child_cancellation_between_dependency_read_and_commit_cancels_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, commit_method: str
) -> None:
    checkpoint = {"stage": "collecting_images", "saved_images": 1}
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:

        async def handler(payload: _Payload, context: AttemptContext) -> WorkOutcome:
            assert payload.value == 1
            await database.publish_leased_operation_checkpoint(
                work_item_identifier=context.work_item_identifier,
                lease_token=context.lease_token,
                worker_identifier=context.worker_identifier,
                utc_now_ns=time_ns,
                operation_id=context.operation_identifier,
                records=(),
                artifacts=(),
                outputs=(),
                checkpoint_result=checkpoint,
            )
            record = RecordDraft(
                identifier="just-finished-evidence",
                kind=("carl", "test", "evidence"),
                schema_version=1,
                value={"saved": True},
            )
            outputs = (NamedOutput(name=("evidence",), object_identifier=record.identifier),)
            if commit_method == "complete_leased_operation":
                return CompletedWork(
                    records=(record,), outputs=outputs, result={"state": "completed_with_failures"}
                )
            if commit_method == "retry_leased_operation":
                return RetryWork(
                    records=(record,),
                    outputs=outputs,
                    result={"stage": "collecting_images"},
                    reason={"kind": "waiting_for_image_collections"},
                    delay_ns=5_000_000_000,
                )
            return TerminalFailureWork(
                records=(record,),
                outputs=outputs,
                result={"state": "mostly_failed_refresh"},
                error={"kind": "mostly_failed_refresh"},
            )

        registry = _registry(handler)
        await _enqueue(database, "root")
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=_SETTINGS.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None
        await _enqueue(database, "child", parent="root")
        original_commit = getattr(database, commit_method)

        async def cancel_child_then_commit(**kwargs: Any) -> None:
            # execute_lease has already completed its final dependency read.
            await database.cancel_work(
                "child",
                reason="cancel during parent commit",
                caller="test",
                requested_at_utc_ns=time_ns(),
            )
            assert await database.work_state("root") is WorkState.LEASED
            await original_commit(**kwargs)

        monkeypatch.setattr(database, commit_method, cancel_child_then_commit)
        outcome = await execute_lease(
            database=database,
            registry=registry,
            settings=_SETTINGS,
            services=_services(),
            lease=claim.lease,
        )
        assert outcome is None
        work = await database.work("root")
        assert work["state"] == "cancelled"
        assert work["error"] is None
        assert work["result"] == checkpoint
        assert (await database.get_record("just-finished-evidence"))[2] == {"saved": True}


@pytest.mark.anyio
async def test_cancellation_racing_with_interruption_cleanup_preserves_returned_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkpoint = {"stage": "collecting_images", "saved_images": 1}

    async def handler(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        raise AssertionError((payload, context, "the interrupted handler already returned"))

    registry = _registry(handler)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "root")
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=_SETTINGS.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None
        await database.begin_leased_operation(
            work_item_identifier="root",
            lease_token="lease",
            worker_identifier="worker",
            lease_duration_ns=_SETTINGS.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
            operation_id="attempt",
            component=registry.handlers[0].component,
            provenance=await _provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-10-17T00:00:00+00:00",
        )
        await database.publish_leased_operation_checkpoint(
            work_item_identifier="root",
            lease_token="lease",
            worker_identifier="worker",
            utc_now_ns=time_ns,
            operation_id="attempt",
            records=(),
            artifacts=(),
            outputs=(),
            checkpoint_result=checkpoint,
        )
        record = RecordDraft(
            identifier="returned-evidence",
            kind=("carl", "test", "evidence"),
            schema_version=1,
            value={"saved": True},
        )
        outcome = CompletedWork(
            records=(record,),
            outputs=(NamedOutput(name=("evidence",), object_identifier=record.identifier),),
            result={"state": "completed"},
        )
        original_finalize = database.finalize_interrupted_work

        async def cancel_then_finalize(**kwargs: Any) -> bool:
            # The runtime helper has already observed no cancellation request.
            await database.cancel_work(
                "root", reason="stop during cleanup", caller="test", requested_at_utc_ns=time_ns()
            )
            return await original_finalize(**kwargs)

        monkeypatch.setattr(database, "finalize_interrupted_work", cancel_then_finalize)
        await worker_io._finalize_interrupted_lease(
            database=database,
            lease=claim.lease,
            operation_identifier="attempt",
            services=_services(),
            started_monotonic_ns=perf_counter_ns(),
            reason="cancelled",
            possibly_dispatched=True,
            primary_error=None,
            outcome=outcome,
        )
        work = await database.work("root")
        assert work["state"] == "cancelled"
        assert work["error"] is None
        assert work["result"] == checkpoint
        assert (await database.get_record(record.identifier))[2] == {"saved": True}
        operation = await database.operation("attempt")
        assert operation["state"] == "completed"
        assert operation["error"] is None
        assert operation["result"] == {"state": "cancelled"}
