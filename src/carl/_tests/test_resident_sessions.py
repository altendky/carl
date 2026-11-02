"""Resident ownership is bounded independently from stage execution and waits."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from time import time_ns
from uuid import uuid4

import anyio
import pytest

from carl.core.components import Component, ComponentId
from carl.core.models import CodeProvenance, NamedOutput, RecordDraft, StrictModel
from carl.core.resources import ResourceRequest
from carl.core.stages import StageAttemptContext, StageDefinition, StageWaitKind, WaitWork
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkDefinition, WorkRequester
from carl.core.worker import CompletedWork
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio

_COMPONENT = Component(ComponentId(("test", "resident")), 1, lambda: None)
_PROVENANCE = CodeProvenance(
    repository_url=None,
    commit_hash=None,
    worktree_state="dirty",
    package_version="test",
    python_implementation="test",
    python_version="test",
    dependencies=(),
    lockfile_sha256=None,
)


class _Payload(StrictModel):
    step: int


@dataclass
class _Session:
    size: int = 8
    closed: bool = False

    def retained_bytes(self) -> int:
        return self.size

    def is_closed(self) -> bool:
        return self.closed


async def _initial(
    database: Database, *, resources: tuple[ResourceRequest, ...]
) -> StageDefinition:
    identifier = str(uuid4())
    work = WorkDefinition(
        identifier=identifier,
        kind=("test", "resident"),
        payload_schema_version=1,
        payload={},
        deduplication_identity=(identifier,),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=("test", "resident")),
        ),
    )
    _ = await database.enqueue_work(
        work,
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier=identifier, context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    return StageDefinition(
        identifier=str(uuid4()),
        work_item_identifier=identifier,
        operation_identifier=str(uuid4()),
        kind=("test", "resident", "phase"),
        payload={"step": 0},
        resources=resources,
    )


def _next(
    context: StageAttemptContext,
    *,
    step: int,
    resources: tuple[ResourceRequest, ...] = (),
    delay_ns: int = 0,
) -> WaitWork:
    return WaitWork(
        continuation=StageDefinition(
            identifier=str(uuid4()),
            work_item_identifier=context.work_item_identifier,
            operation_identifier=context.operation_identifier,
            kind=("test", "resident", "phase"),
            payload={"step": step},
            resources=resources,
            resident_session_identifier=context.resident_session_identifier,
            not_before_utc_ns=time_ns() + delay_ns,
        ),
        wait_kind=StageWaitKind.TIME if delay_ns else StageWaitKind.RESOURCE,
        result={"step": step},
    )


async def test_session_spans_stage_task_scopes_and_idle_releases_execution_permits(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3",
        initialize=True,
        resource_capacities={
            "http": 1,
            "cpu": 1,
            "resident_sessions": 1,
            "resident_state_bytes": 8,
        },
    ) as database:
        manager = replace(
            stage_manager(database), lease_duration_ns=5_000_000_000, renewal_interval_ns=50_000_000
        )
        pool = ResidentSessionPool(
            manager, StageHandlerRegistry((), ()), _PROVENANCE, {}, state_bytes=8, poll_seconds=0.01
        )
        session = _Session()
        opened = 0
        waiting = anyio.Event()
        continue_wait = anyio.Event()
        resident_identifiers: set[str] = set()

        @asynccontextmanager
        async def factory(_identifier: str) -> AsyncIterator[_Session]:
            nonlocal opened
            opened += 1
            # Real provider contexts contain task groups. They must close in
            # stack order despite stages being admitted and released in between.
            async with anyio.create_task_group():
                try:
                    yield session
                finally:
                    session.closed = True

        async def phase(
            payload: _Payload, context: StageAttemptContext
        ) -> WaitWork | CompletedWork:
            identifier = context.resident_session_identifier
            assert identifier is not None
            resident_identifiers.add(identifier)
            if payload.step == 0:
                handle = await pool.get_or_open(identifier, factory)
                assert handle.value is session
                return _next(context, step=1, resources=(ResourceRequest(name="cpu"),))
            if payload.step == 1:
                assert pool.peek(identifier) is not None
                return _next(context, step=2, delay_ns=200_000_000)
            waiting.set()
            await continue_wait.wait()
            assert opened == 1 and not session.closed
            await pool.close(identifier)
            return CompletedWork(result={"done": True})

        pool.registry = StageHandlerRegistry(
            (), (TypedStageHandler(("test", "resident", "phase"), 1, _Payload, phase),)
        )
        first = await _initial(database, resources=(ResourceRequest(name="http"),))
        second = await _initial(database, resources=(ResourceRequest(name="http"),))
        async with pool.managed():
            assert await pool.submit(first, component=_COMPONENT)
            with anyio.fail_after(3):
                await waiting.wait()
            resident_identifier = next(iter(resident_identifiers))
            before = await database.resident_session(resident_identifier)
            assert before is not None
            # Hundreds of queued candidates do not create tasks or transports.
            for _ in range(100):
                assert not await pool.submit(second, component=_COMPONENT)
            assert pool.active_count == 1 and opened == 1
            await anyio.sleep(0.1)
            renewed = await database.resident_session(resident_identifier)
            assert renewed is not None and renewed.expires_at_utc_ns > before.expires_at_utc_ns
            async with manager.resources.admit(
                ResourceRequest(name="http"), ResourceRequest(name="cpu")
            ):
                assert not session.closed
            continue_wait.set()
            with anyio.fail_after(3):
                await pool.wait_idle()
            assert pool.active_count == 0 and not pool.errors
        assert session.closed
        work = await database.work(first.work_item_identifier)
        assert work["attempt"] == 1 and work["state"] == "completed"


async def test_long_wait_parks_then_rebinds_new_incarnation_without_new_attempt(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        clock_offset = [0]
        pool = ResidentSessionPool(
            replace(stage_manager(database), utc_now_ns=lambda: time_ns() + clock_offset[0]),
            StageHandlerRegistry((), ()),
            _PROVENANCE,
            {},
            maximum_idle_ns=1_000_000_000,
        )
        continuation: StageDefinition | None = None
        incarnations: list[str] = []

        async def phase(
            payload: _Payload, context: StageAttemptContext
        ) -> WaitWork | CompletedWork:
            nonlocal continuation
            assert context.resident_session_identifier is not None
            incarnations.append(context.resident_session_identifier)
            if payload.step == 0:
                outcome = _next(context, step=1, delay_ns=10_000_000_000)
                continuation = outcome.continuation
                return outcome
            return CompletedWork(result={"done": True})

        pool.registry = StageHandlerRegistry(
            (), (TypedStageHandler(("test", "resident", "phase"), 1, _Payload, phase),)
        )
        initial = await _initial(database, resources=())
        async with pool.managed():
            assert await pool.submit(initial, component=_COMPONENT)
            await pool.wait_idle()
            assert not pool.errors and continuation is not None
            clock_offset[0] = 20_000_000_000
            assert await pool.submit(continuation, component=_COMPONENT)
            await pool.wait_idle()
            assert not pool.errors
        assert len(incarnations) == 2 and incarnations[0] != incarnations[1]
        work = await database.work(initial.work_item_identifier)
        assert work["attempt"] == 1 and work["state"] == "completed"


async def test_cancellation_closes_session_and_recovers_all_capacity(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        manager = replace(
            stage_manager(database), lease_duration_ns=300_000_000, renewal_interval_ns=25_000_000
        )
        pool = ResidentSessionPool(manager, StageHandlerRegistry((), ()), _PROVENANCE, {})
        session = _Session()
        running = anyio.Event()

        @asynccontextmanager
        async def factory(_identifier: str) -> AsyncIterator[_Session]:
            async with anyio.create_task_group():
                try:
                    yield session
                finally:
                    session.closed = True

        async def phase(
            payload: _Payload, context: StageAttemptContext
        ) -> WaitWork | CompletedWork:
            identifier = context.resident_session_identifier
            assert identifier is not None
            if payload.step == 0:
                _ = await pool.get_or_open(identifier, factory)
                return _next(context, step=1, resources=(ResourceRequest(name="cpu"),))
            running.set()
            await anyio.sleep_forever()
            raise AssertionError("Cancelled phase unexpectedly resumed")

        pool.registry = StageHandlerRegistry(
            (), (TypedStageHandler(("test", "resident", "phase"), 1, _Payload, phase),)
        )
        initial = await _initial(database, resources=(ResourceRequest(name="http"),))
        async with pool.managed():
            assert await pool.submit(initial, component=_COMPONENT)
            await running.wait()
            _ = await database.cancel_work(
                initial.work_item_identifier,
                reason="test",
                caller="test",
                requested_at_utc_ns=time_ns(),
            )
            with anyio.fail_after(3):
                await pool.wait_idle()
            assert session.closed and pool.active_count == 0
            work = await database.work(initial.work_item_identifier)
            assert work["state"] == "cancelled"
            async with manager.resources.admit(
                ResourceRequest(name="http"),
                ResourceRequest(name="cpu"),
                ResourceRequest(name="resident_sessions"),
            ):
                pass


async def test_completed_capture_survives_resident_state_limit_without_refetch(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool = ResidentSessionPool(
            stage_manager(database), StageHandlerRegistry((), ()), _PROVENANCE, {}, state_bytes=8
        )
        session = _Session()
        captures = 0
        record = RecordDraft(
            identifier=str(uuid4()),
            kind=("test", "paid_capture"),
            schema_version=1,
            value={"captured": True},
        )

        @asynccontextmanager
        async def factory(_identifier: str) -> AsyncIterator[_Session]:
            try:
                yield session
            finally:
                session.closed = True

        async def phase(_payload: _Payload, context: StageAttemptContext) -> WaitWork:
            nonlocal captures
            assert context.resident_session_identifier is not None
            _ = await pool.get_or_open(context.resident_session_identifier, factory)
            captures += 1
            session.size = 9
            return _next(context, step=1).model_copy(
                update={
                    "records": (record,),
                    "outputs": (
                        NamedOutput(name=("capture",), object_identifier=record.identifier),
                    ),
                }
            )

        pool.registry = StageHandlerRegistry(
            (), (TypedStageHandler(("test", "resident", "phase"), 1, _Payload, phase),)
        )
        initial = await _initial(database, resources=(ResourceRequest(name="http"),))
        async with pool.managed():
            assert await pool.submit(initial, component=_COMPONENT)
            await pool.wait_idle()
            assert pool.active_count == 0 and not pool.errors
        assert captures == 1 and session.closed
        assert (await database.get_record(record.identifier))[2] == {"captured": True}
        work = await database.work(initial.work_item_identifier)
        assert work["state"] == "terminal_failure" and work["attempt"] == 1
        assert work["error"] == {"kind": "resident_state_limit"}


async def test_admission_only_polls_do_not_extend_resident_idle_deadline(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        offset = [0]
        pool = ResidentSessionPool(
            replace(stage_manager(database), utc_now_ns=lambda: time_ns() + offset[0]),
            StageHandlerRegistry((), ()),
            _PROVENANCE,
            {},
        )
        polls = 0

        async def phase(_payload: _Payload, context: StageAttemptContext) -> WaitWork:
            nonlocal polls
            polls += 1
            offset[0] += 8_000_000_000
            return _next(context, step=0)

        pool.registry = StageHandlerRegistry(
            (), (TypedStageHandler(("test", "resident", "phase"), 1, _Payload, phase),)
        )
        initial = await _initial(database, resources=())
        async with pool.managed():
            assert await pool.submit(initial, component=_COMPONENT)
            with anyio.fail_after(3):
                await pool.wait_idle()
            assert polls == 4 and pool.active_count == 0 and not pool.errors
        work = await database.work(initial.work_item_identifier)
        assert work["attempt"] == 1 and work["state"] == "leased"
