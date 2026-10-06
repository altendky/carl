import sqlite3
from contextlib import closing
from pathlib import Path
from time import perf_counter_ns, time_ns
from typing import Any
from uuid import uuid4

import anyio
import pytest

from carl.core.components import Component, ComponentId
from carl.core.models import CodeProvenance, NamedOutput, RecordDraft, StrictModel
from carl.core.work import (
    ClaimResult,
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
    FollowOnWork,
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
    run_worker_pool,
)


class _Payload(StrictModel):
    value: int


def _provenance() -> CodeProvenance:
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


async def _async_provenance() -> CodeProvenance:
    return _provenance()


def _services() -> WorkerRuntimeServices:
    return WorkerRuntimeServices(
        new_identifier=lambda: str(uuid4()),
        utc_now_ns=time_ns,
        monotonic_ns=perf_counter_ns,
        code_provenance=_async_provenance,
        invocation=lambda: {"kind": "test"},
    )


def _settings() -> WorkerSettings:
    return WorkerSettings(
        worker_count=1,
        lease_duration_ns=1_000_000_000,
        renewal_interval_ns=100_000_000,
        idle_poll_interval_ns=10_000_000,
    )


def _definition(*, identifier: str, kind: tuple[str, ...]) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=kind,
        payload_schema_version=1,
        payload={"value": 7},
        deduplication_identity=(identifier,),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
        ),
    )


def _requester(identifier: str) -> WorkRequester:
    return WorkRequester(
        request_identifier=f"request-{identifier}",
        kind=("carl", "test", "requester"),
        identifier=identifier,
        context={},
    )


@pytest.mark.anyio
async def test_idle_worker_pool_uses_one_queue_probe_per_poll_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "carl.sqlite3"
    kind = ("carl", "test", "idle-poll")
    first_claim = anyio.Event()
    stop = anyio.Event()
    claim_count = 0

    async def no_work_available(*args: object, **kwargs: object) -> ClaimResult:
        nonlocal claim_count
        del args, kwargs
        claim_count += 1
        first_claim.set()
        return ClaimResult(lease=None, next_eligible_at_utc_ns=None)

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        raise AssertionError((payload, context))

    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=Component(ComponentId(("carl", "test", "idle-worker")), 1, handle),
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )
    settings = WorkerSettings(
        worker_count=10,
        lease_duration_ns=1_000_000_000,
        renewal_interval_ns=100_000_000,
        idle_poll_interval_ns=1_000_000_000,
    )

    async with Database.managed(path, initialize=True) as database:
        monkeypatch.setattr(database, "claim_work", no_work_available)

        async def run_pool() -> None:
            await run_worker_pool(
                database=database,
                registry=registry,
                settings=settings,
                services=_services(),
                stop=stop,
            )

        async with anyio.create_task_group() as task_group:
            _ = task_group.start_soon(run_pool)
            await first_claim.wait()
            await anyio.sleep(0.05)
            assert claim_count == 1
            stop.set()


@pytest.mark.anyio
async def test_worker_pool_processes_only_registered_capabilities(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    supported_kind = ("carl", "test", "supported")
    unsupported_kind = ("carl", "test", "unsupported")
    stop = anyio.Event()

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        stop.set()
        record = RecordDraft(
            identifier=f"record-{context.work_item_identifier}",
            kind=("carl", "test", "result"),
            schema_version=1,
            value={"value": payload.value},
        )
        return CompletedWork(
            records=(record,),
            outputs=(NamedOutput(name=("result",), object_identifier=record.identifier),),
            follow_on_work=(
                FollowOnWork(
                    definition=_definition(identifier="follow-on", kind=unsupported_kind),
                    requester=_requester("follow-on"),
                    event_identifier=str(uuid4()),
                ),
            ),
            result={"value": payload.value},
        )

    component = Component(ComponentId(("carl", "test", "worker")), 1, handle)
    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=supported_kind, payload_schema_version=1),
                component=component,
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )

    async with Database.managed(path, initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="supported", kind=supported_kind),
            _requester("supported"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        await database.enqueue_work(
            _definition(identifier="unsupported", kind=unsupported_kind),
            _requester("unsupported"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )

        with anyio.fail_after(2):
            await run_worker_pool(
                database=database,
                registry=registry,
                settings=_settings(),
                services=_services(),
                stop=stop,
            )

        _, _, result = await database.get_record("record-supported")
        assert result == {"value": 7}

    with closing(sqlite3.connect(path)) as connection:
        states = dict(connection.execute("SELECT id, state FROM work_items"))

    assert states == {
        "follow-on": "pending",
        "supported": "completed",
        "unsupported": "pending",
    }


@pytest.mark.anyio
async def test_invalid_registered_payload_becomes_terminal_failure(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    kind = ("carl", "test", "typed")
    stop = anyio.Event()

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        raise AssertionError((payload, context))

    component = Component(ComponentId(("carl", "test", "typed-worker")), 1, handle)
    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=component,
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )
    malformed = _definition(identifier="malformed", kind=kind).model_copy(
        update={"payload": {"value": "not-an-integer"}}
    )

    async with Database.managed(path, initialize=True) as database:
        await database.enqueue_work(
            malformed,
            _requester("malformed"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )

        async def stop_after_terminal_failure() -> None:
            while True:
                await anyio.sleep(0.01)
                if await database.work_state("malformed") is WorkState.TERMINAL_FAILURE:
                    stop.set()
                    return

        async def run_pool() -> None:
            await run_worker_pool(
                database=database,
                registry=registry,
                settings=_settings(),
                services=_services(),
                stop=stop,
            )

        with anyio.fail_after(2):
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(stop_after_terminal_failure)
                task_group.start_soon(run_pool)

    with closing(sqlite3.connect(path)) as connection:
        row = connection.execute(
            "SELECT state, error_json FROM work_items WHERE id = 'malformed'"
        ).fetchone()

    assert row is not None
    assert row[0] == "terminal_failure"
    assert '"kind":"unhandled_handler_error"' in row[1]


@pytest.mark.anyio
async def test_unhandled_exception_group_records_nested_types(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    kind = ("carl", "test", "exception-group")

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        assert (payload.value, context.attempt) == (7, 1)
        raise ExceptionGroup("sensitive", [ValueError("one"), RuntimeError("two")])

    component = Component(ComponentId(("carl", "test", "exception-group-worker")), 1, handle)
    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=component,
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )

    async with Database.managed(path, initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="exception-group", kind=kind),
            _requester("exception-group"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=_settings().lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None
        await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=_services(),
            lease=claim.lease,
        )
        work = await database.work("exception-group")

    assert work["error"] == {
        "kind": "unhandled_handler_error",
        "stage": "handler",
        "type": "ExceptionGroup",
        "exception": {
            "type": "ExceptionGroup",
            "exceptions": [{"type": "ValueError"}, {"type": "RuntimeError"}],
        },
    }


@pytest.mark.anyio
async def test_retry_terminal_work_restores_prior_operation_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    kind = ("carl", "test", "recover")
    outcome_number = 0

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        nonlocal outcome_number
        outcome_number += 1
        if outcome_number == 1:
            return RetryWork(
                delay_ns=0,
                reason={"kind": "continue"},
                result={"stage": "checkpoint", "value": payload.value},
            )
        return TerminalFailureWork(
            error={"kind": "later_failure"},
            result={"state": "terminal_failure"},
        )

    component = Component(ComponentId(("carl", "test", "recover-worker")), 1, handle)
    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=component,
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )

    async with Database.managed(path, initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="recover", kind=kind),
            _requester("recover"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        for attempt in range(2):
            claim = await database.claim_work(
                supported_capabilities=registry.capabilities,
                worker_identifier="worker",
                lease_token=f"lease-{attempt}",
                lease_duration_ns=_settings().lease_duration_ns,
                utc_now_ns=time_ns,
                event_identifier=str(uuid4()),
            )
            assert claim.lease is not None
            await execute_lease(
                database=database,
                registry=registry,
                settings=_settings(),
                services=_services(),
                lease=claim.lease,
            )

        failed = await database.work("recover")
        checkpoint_operation = failed["operations"][0]["operation_identifier"]
        assert isinstance(checkpoint_operation, str)
        await database.retry_terminal_work_from_operation(
            work_item_identifier="recover",
            checkpoint_operation_identifier=checkpoint_operation,
            retried_at_utc_ns=time_ns(),
            event_identifier=str(uuid4()),
            reason={"kind": "operator_recovery"},
            payload_schema_version=2,
        )
        recovered = await database.work("recover")

    assert recovered["state"] == "pending"
    assert recovered["attempt"] == 2
    assert recovered["payload_schema_version"] == 2
    assert recovered["result"] == {"stage": "checkpoint", "value": 7}
    assert recovered["error"] is None


@pytest.mark.anyio
async def test_cancelling_worker_records_ambiguous_attempt_and_releases_lease(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    kind = ("carl", "test", "cancelled")
    started = anyio.Event()
    scopes: list[anyio.CancelScope] = []

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        assert payload.value == 7
        assert context.attempt == 1
        started.set()
        await anyio.sleep_forever()
        raise AssertionError("sleep_forever returned")

    component = Component(ComponentId(("carl", "test", "cancelled-worker")), 1, handle)
    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=component,
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )

    async with Database.managed(path, initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="cancelled", kind=kind),
            _requester("cancelled"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )

        async def run_cancelled_pool() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                await run_worker_pool(
                    database=database,
                    registry=registry,
                    settings=_settings(),
                    services=_services(),
                    stop=anyio.Event(),
                )

        with anyio.fail_after(2):
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(run_cancelled_pool)
                await started.wait()
                scopes[0].cancel()

        assert await database.work_state("cancelled") is WorkState.PENDING

    with closing(sqlite3.connect(path)) as connection:
        operation = connection.execute(
            "SELECT state, error_json FROM operations ORDER BY rowid DESC LIMIT 1"
        ).fetchone()

    assert operation is not None
    assert operation[0] == "failed"
    assert '"kind":"cancelled"' in operation[1]
    assert '"possibly_dispatched":true' in operation[1]

    with closing(sqlite3.connect(path)) as connection:
        event = connection.execute(
            "SELECT data_json FROM work_events "
            "WHERE work_item_id = 'cancelled' AND event_kind = 'released'"
        ).fetchone()
    assert event is not None
    assert '"kind":"worker_cancelled"' in event[0]
    assert '"decision":"retry"' in event[0]


@pytest.mark.anyio
async def test_cancelling_nested_handler_group_releases_lease(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    kind = ("carl", "test", "nested-cancelled")
    started = anyio.Event()
    scopes: list[anyio.CancelScope] = []

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        assert payload.value == 7
        assert context.attempt == 1
        async with anyio.create_task_group() as children:
            children.start_soon(anyio.sleep_forever)
            started.set()
            await anyio.sleep_forever()
        raise AssertionError("nested task group exited")

    component = Component(ComponentId(("carl", "test", "nested-cancelled-worker")), 1, handle)
    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=component,
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )

    async with Database.managed(path, initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="nested-cancelled", kind=kind),
            _requester("nested-cancelled"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )

        async def run_cancelled_pool() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                await run_worker_pool(
                    database=database,
                    registry=registry,
                    settings=_settings(),
                    services=_services(),
                    stop=anyio.Event(),
                )

        with anyio.fail_after(2):
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(run_cancelled_pool)
                await started.wait()
                scopes[0].cancel()

        assert await database.work_state("nested-cancelled") is WorkState.PENDING


@pytest.mark.anyio
async def test_lost_lease_cancels_only_its_attempt_and_records_failure(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    kind = ("carl", "test", "lease-loss")
    started = anyio.Event()
    cancelled = anyio.Event()

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        assert payload.value == 7
        assert context.attempt == 1
        started.set()
        try:
            await anyio.sleep_forever()
        finally:
            cancelled.set()
        raise AssertionError("sleep_forever returned")

    component = Component(ComponentId(("carl", "test", "lease-loss-worker")), 1, handle)
    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=component,
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )
    settings = WorkerSettings(
        worker_count=1,
        lease_duration_ns=1_000_000_000,
        renewal_interval_ns=10_000_000,
        idle_poll_interval_ns=10_000_000,
    )

    async with Database.managed(path, initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="lease-loss", kind=kind),
            _requester("lease-loss"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="original-lease",
            lease_duration_ns=settings.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None

        async def run_attempt() -> None:
            assert claim.lease is not None
            await execute_lease(
                database=database,
                registry=registry,
                settings=settings,
                services=_services(),
                lease=claim.lease,
            )

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(run_attempt)
            await started.wait()
            await database.release_lease(
                work_item_identifier="lease-loss",
                lease_token="original-lease",
                worker_identifier="worker",
                utc_now_ns=time_ns,
                eligible_at_utc_ns=time_ns() + 1_000_000_000,
                reason={"kind": "test_lease_replacement"},
                event_identifier=str(uuid4()),
            )

        assert cancelled.is_set()
        assert await database.work_state("lease-loss") is WorkState.PENDING

    with closing(sqlite3.connect(path)) as connection:
        operation = connection.execute(
            "SELECT state, error_json FROM operations ORDER BY rowid DESC LIMIT 1"
        ).fetchone()

    assert operation is not None
    assert operation[0] == "failed"
    assert '"kind":"lease_lost"' in operation[1]


@pytest.mark.anyio
async def test_expired_lease_during_setup_never_dispatches_handler(tmp_path: Path) -> None:
    kind = ("carl", "test", "setup-lease-loss")
    now = 100
    provenance_started = anyio.Event()
    allow_provenance = anyio.Event()
    handler_started = False

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        nonlocal handler_started
        handler_started = True
        return CompletedWork(result={})

    async def delayed_provenance() -> CodeProvenance:
        provenance_started.set()
        await allow_provenance.wait()
        return _provenance()

    component = Component(ComponentId(("carl", "test", "setup-lease-loss-worker")), 1, handle)
    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=component,
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )
    services = WorkerRuntimeServices(
        new_identifier=lambda: str(uuid4()),
        utc_now_ns=lambda: now,
        monotonic_ns=lambda: now,
        code_provenance=delayed_provenance,
        invocation=lambda: {"kind": "test"},
    )

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="setup-lease-loss", kind=kind),
            _requester("setup-lease-loss"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=now,
        )
        first = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker-1",
            lease_token="lease-1",
            lease_duration_ns=10,
            utc_now_ns=lambda: now,
            event_identifier=str(uuid4()),
        )
        assert first.lease is not None

        async def run_attempt() -> None:
            assert first.lease is not None
            await execute_lease(
                database=database,
                registry=registry,
                settings=WorkerSettings(
                    worker_count=1,
                    lease_duration_ns=10,
                    renewal_interval_ns=5,
                    idle_poll_interval_ns=1,
                ),
                services=services,
                lease=first.lease,
            )

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(run_attempt)
            await provenance_started.wait()
            now = 110
            replacement = await database.claim_work(
                supported_capabilities=registry.capabilities,
                worker_identifier="worker-2",
                lease_token="lease-2",
                lease_duration_ns=10,
                utc_now_ns=lambda: now,
                event_identifier=str(uuid4()),
            )
            assert replacement.lease is not None
            allow_provenance.set()

        assert not handler_started
        assert await database.work_state("setup-lease-loss") is WorkState.LEASED


@pytest.mark.anyio
@pytest.mark.parametrize(
    "stage",
    (
        "retry_budget",
        "provenance",
        "begin_rollback",
        "completed",
        "retried",
        "terminally_failed",
        "cleanup_error",
    ),
)
async def test_cancellation_reconciles_the_entire_leased_attempt_lifetime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    """Missing operations, rolled-back starts, and committed outcomes differ."""

    kind = ("carl", "test", "cancelled-lifetime")
    handler_started = False
    cancellation: BaseException | None = None
    committed_work: Any = None

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        nonlocal handler_started
        handler_started = True
        if stage == "retried":
            return RetryWork(
                delay_ns=1_000_000_000, reason={"kind": "fixture"}, result={"value": 7}
            )
        if stage == "terminally_failed":
            return TerminalFailureWork(error={"kind": "fixture"}, result={"value": 7})
        return CompletedWork(result={"value": payload.value})

    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=Component(ComponentId(("carl", "test", "lifetime-worker")), 1, handle),
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="cancelled-lifetime", kind=kind),
            _requester("cancelled-lifetime"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=_settings().lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None

        with anyio.CancelScope() as scope:

            async def cancel_setup(*args: Any, **kwargs: Any) -> Any:
                del args, kwargs
                scope.cancel()
                await anyio.lowlevel.checkpoint()
                raise AssertionError("cancellation checkpoint returned")

            if stage == "retry_budget":
                monkeypatch.setattr("carl.io.worker.work_retry_budget", cancel_setup)
            elif stage == "begin_rollback":
                begin_operation = database._begin_operation

                async def cancel_after_insert(*args: Any, **kwargs: Any) -> None:
                    await begin_operation(*args, **kwargs)
                    await cancel_setup()

                monkeypatch.setattr(database, "_begin_operation", cancel_after_insert)
            elif stage in {"completed", "retried", "terminally_failed"}:
                complete_operation = {
                    "completed": database.complete_leased_operation,
                    "retried": database.retry_leased_operation,
                    "terminally_failed": database.terminally_fail_leased_operation,
                }[stage]

                async def cancel_after_commit(*args: Any, **kwargs: Any) -> None:
                    nonlocal committed_work
                    await complete_operation(*args, **kwargs)
                    committed_work = await database.work("cancelled-lifetime")
                    await cancel_setup()

                monkeypatch.setattr(
                    database,
                    {
                        "completed": "complete_leased_operation",
                        "retried": "retry_leased_operation",
                        "terminally_failed": "terminally_fail_leased_operation",
                    }[stage],
                    cancel_after_commit,
                )
            elif stage == "cleanup_error":

                async def fail_cleanup(*args: Any, **kwargs: Any) -> bool:
                    del args, kwargs
                    raise RuntimeError("sensitive fixture detail must not be logged")

                monkeypatch.setattr(database, "finalize_interrupted_work", fail_cleanup)

            services = _services()
            if stage in {"provenance", "cleanup_error"}:
                services = WorkerRuntimeServices(
                    new_identifier=services.new_identifier,
                    utc_now_ns=services.utc_now_ns,
                    monotonic_ns=services.monotonic_ns,
                    code_provenance=cancel_setup,
                    invocation=services.invocation,
                )
            with pytest.raises(anyio.get_cancelled_exc_class()) as caught:
                await execute_lease(
                    database=database,
                    registry=registry,
                    settings=_settings(),
                    services=services,
                    lease=claim.lease,
                )
            cancellation = caught.value

        work = await database.work("cancelled-lifetime")
        if stage in {"completed", "retried", "terminally_failed"}:
            assert work == committed_work
            assert (
                work["state"]
                == {
                    "completed": "completed",
                    "retried": "pending",
                    "terminally_failed": "terminal_failure",
                }[stage]
            )
            assert work["result"] == {"value": 7}
            assert len(work["operations"]) == 1
            operation_identifier = work["operations"][0]["operation_identifier"]
            assert isinstance(operation_identifier, str)
            assert (await database.operation(operation_identifier))["state"] == (
                "completed" if stage == "completed" else "failed"
            )
            assert handler_started
        elif stage == "cleanup_error":
            assert work["state"] == "leased"
            assert cancellation is not None
            assert any("RuntimeError" in note for note in cancellation.__notes__)
            assert not any("sensitive fixture" in note for note in cancellation.__notes__)
        else:
            assert work["state"] == "pending"
            assert work["operations"] == []
            assert not handler_started


@pytest.mark.anyio
async def test_cancellation_at_committed_claim_handoff_releases_undispatched_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kind = ("carl", "test", "claim-handoff")
    handed_off = False

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        raise AssertionError("a cancelled claim must not dispatch")

    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=Component(ComponentId(("carl", "test", "handoff-worker")), 1, handle),
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="claim-handoff", kind=kind),
            _requester("claim-handoff"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        claim_work = database.claim_work
        with anyio.CancelScope() as scope:

            async def cancel_after_claim_commit(*args: Any, **kwargs: Any) -> ClaimResult:
                nonlocal handed_off
                claim = await claim_work(*args, **kwargs)
                assert claim.lease is not None
                scope.cancel()
                await anyio.lowlevel.checkpoint()
                handed_off = True
                return claim

            monkeypatch.setattr(database, "claim_work", cancel_after_claim_commit)
            await run_worker_pool(
                database=database,
                registry=registry,
                settings=_settings(),
                services=_services(),
                stop=anyio.Event(),
            )

        assert handed_off
        work = await database.work("claim-handoff")
        assert work["state"] == "pending"
        assert work["operations"] == []


@pytest.mark.anyio
async def test_non_cancellation_base_exception_group_finalizes_the_interrupted_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FixtureAbort(BaseException):
        pass

    kind = ("carl", "test", "base-exception")
    failure = BaseExceptionGroup("fixture abort", [FixtureAbort()])
    unwound_error: BaseException | None = None
    finalize = worker_io._finalize_interrupted_lease

    async def capture_primary(*args: Any, **kwargs: Any) -> None:
        nonlocal unwound_error
        unwound_error = kwargs["primary_error"]
        await finalize(*args, **kwargs)

    monkeypatch.setattr(worker_io, "_finalize_interrupted_lease", capture_primary)

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        raise failure

    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=Component(ComponentId(("carl", "test", "abort-worker")), 1, handle),
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="base-exception", kind=kind),
            _requester("base-exception"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=_settings().lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None
        with pytest.raises(BaseExceptionGroup) as caught:
            await execute_lease(
                database=database,
                registry=registry,
                settings=_settings(),
                services=_services(),
                lease=claim.lease,
            )

        # Trio wraps/derives groups on nursery exit; lease cleanup must not replace that error.
        assert caught.value is unwound_error
        handler_group = caught.value.exceptions[0]
        assert isinstance(handler_group, BaseExceptionGroup)
        assert handler_group.message == failure.message
        assert handler_group.exceptions == failure.exceptions
        work = await database.work("base-exception")
        assert work["state"] == "pending"
        assert work["latest_event_kind"] == "released"
        operation_identifier = work["operations"][0]["operation_identifier"]
        assert isinstance(operation_identifier, str)
        operation = await database.operation(operation_identifier)
        assert operation["state"] == "failed"
        assert operation["error"] == {
            "kind": "worker_runtime_error",
            "possibly_dispatched": True,
        }


@pytest.mark.anyio
@pytest.mark.parametrize("operation_started", (False, True))
@pytest.mark.parametrize("replacement_lease", (False, True))
async def test_interrupted_finalization_is_idempotent_and_fenced_to_its_attempt(
    tmp_path: Path, operation_started: bool, replacement_lease: bool
) -> None:
    kind = ("carl", "test", "finalizer-fence")
    now = 100

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        return CompletedWork(result={})

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="finalizer-fence", kind=kind),
            _requester("finalizer-fence"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=now,
        )
        capabilities = (WorkCapability(kind=kind, payload_schema_version=1),)
        claim = await database.claim_work(
            supported_capabilities=capabilities,
            worker_identifier="original-worker",
            lease_token="original-lease",
            lease_duration_ns=10,
            utc_now_ns=lambda: now,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None
        if operation_started:
            await database.begin_leased_operation(
                work_item_identifier="finalizer-fence",
                lease_token="original-lease",
                worker_identifier="original-worker",
                lease_duration_ns=10,
                utc_now_ns=lambda: now,
                event_identifier=str(uuid4()),
                operation_id="interrupted-operation",
                component=Component(ComponentId(("carl", "test", "fence-worker")), 1, handle),
                provenance=_provenance(),
                invocation={},
                configuration={},
                started_at_utc="2026-10-03T00:00:00+00:00",
            )
        if replacement_lease:
            now = 200
            replacement = await database.claim_work(
                supported_capabilities=capabilities,
                worker_identifier="replacement-worker",
                lease_token="replacement-lease",
                lease_duration_ns=10,
                utc_now_ns=lambda: now,
                event_identifier=str(uuid4()),
            )
            assert replacement.lease is not None

        async def finalize() -> bool:
            return await database.finalize_interrupted_work(
                work_item_identifier="finalizer-fence",
                lease_token="original-lease",
                worker_identifier="original-worker",
                operation_id="interrupted-operation",
                utc_now_ns=lambda: now,
                event_identifier=str(uuid4()),
                reason="cancelled",
                ended_at_utc="2026-10-03T00:00:01+00:00",
                duration_ns=1,
                possibly_dispatched=operation_started,
            )

        assert await finalize() is (not replacement_lease)
        work = await database.work("finalizer-fence")
        assert await finalize() is False
        assert await database.work("finalizer-fence") == work
        assert work["state"] == ("leased" if replacement_lease else "pending")
        assert work["worker_identifier"] == ("replacement-worker" if replacement_lease else None)
        if operation_started:
            assert (await database.operation("interrupted-operation"))["state"] == "failed"


@pytest.mark.anyio
async def test_heartbeat_storage_failure_is_not_a_terminal_handler_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "carl.sqlite3"
    kind = ("carl", "test", "heartbeat-failure")
    handler_cancelled = anyio.Event()

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        try:
            await anyio.sleep_forever()
        finally:
            handler_cancelled.set()
        raise AssertionError("sleep_forever returned")

    async def fail_renewal(*args: object, **kwargs: object) -> None:
        raise RuntimeError("fixture renewal failure")

    component = Component(ComponentId(("carl", "test", "heartbeat-failure-worker")), 1, handle)
    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=component,
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )
    settings = WorkerSettings(
        worker_count=1,
        lease_duration_ns=1_000_000_000,
        renewal_interval_ns=10_000_000,
        idle_poll_interval_ns=10_000_000,
    )

    async with Database.managed(path, initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="heartbeat-failure", kind=kind),
            _requester("heartbeat-failure"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=settings.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None
        monkeypatch.setattr(Database, "renew_lease", fail_renewal)

        with anyio.fail_after(2), pytest.raises(ExceptionGroup):
            await execute_lease(
                database=database,
                registry=registry,
                settings=settings,
                services=_services(),
                lease=claim.lease,
            )

        assert handler_cancelled.is_set()
        assert await database.work_state("heartbeat-failure") is WorkState.PENDING

    with closing(sqlite3.connect(path)) as connection:
        operation = connection.execute(
            "SELECT state, error_json FROM operations ORDER BY rowid DESC LIMIT 1"
        ).fetchone()

    assert operation is not None
    assert operation[0] == "failed"
    assert '"kind":"worker_runtime_error"' in operation[1]


@pytest.mark.anyio
async def test_retry_outcome_sets_fresh_eligibility_and_preserves_attempt(
    tmp_path: Path,
) -> None:
    kind = ("carl", "test", "retry")
    now = 100

    async def handle(payload: _Payload, context: AttemptContext) -> WorkOutcome:
        assert payload.value == 7
        assert context.attempt == 1
        return RetryWork(
            delay_ns=20,
            reason={"kind": "temporary"},
            result={"stage": "waiting"},
        )

    component = Component(ComponentId(("carl", "test", "retry-worker")), 1, handle)
    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=component,
                payload_type=_Payload,
                handler=handle,
            ),
        )
    )
    services = WorkerRuntimeServices(
        new_identifier=lambda: str(uuid4()),
        utc_now_ns=lambda: now,
        monotonic_ns=lambda: now,
        code_provenance=_async_provenance,
        invocation=lambda: {"kind": "test"},
    )
    database_path = tmp_path / "carl.sqlite3"

    async with Database.managed(database_path, initialize=True) as database:
        await database.enqueue_work(
            _definition(identifier="retry", kind=kind),
            _requester("retry"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=now,
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease-1",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: now,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None

        await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=services,
            lease=claim.lease,
        )
        assert await database.work_state("retry") is WorkState.PENDING
        assert (await database.work("retry"))["result"] == {"stage": "waiting"}

    async with Database.managed(database_path) as database:
        assert (await database.work("retry"))["result"] == {"stage": "waiting"}
        too_early = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease-2",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: 119,
            event_identifier=str(uuid4()),
        )
        assert too_early.lease is None
        assert too_early.next_eligible_at_utc_ns == 120

        replacement = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease-2",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: 120,
            event_identifier=str(uuid4()),
        )
        assert replacement.lease is not None
        assert replacement.lease.attempt == 2
