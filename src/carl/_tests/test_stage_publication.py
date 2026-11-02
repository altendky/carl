"""Child operation history and a continuation publish under one exact fence."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from time import time_ns
from uuid import uuid4

import anyio
import apsw
import pytest

from carl._tests.test_resident_sessions import _COMPONENT, _PROVENANCE, _initial
from carl.core.models import NamedOutput, RecordDraft
from carl.core.stages import StageWaitKind, WaitWork
from carl.core.worker import CompletedWork
from carl.io.sqlite import Database
from carl.io.stage_publication import ChildOperationDraft, StagePublication
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("rollback", (False, True))
async def test_child_publication_is_atomic_with_parent_outcome(
    tmp_path: Path, rollback: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _initial(database, resources=())
        await database.register_stage(stage, created_at_utc_ns=time_ns())
        child_identifier = str(uuid4())
        record = RecordDraft(
            identifier=str(uuid4()), kind=("test", "child"), schema_version=1, value={"saved": True}
        )
        child = ChildOperationDraft(
            identifier=child_identifier,
            component=_COMPONENT,
            configuration={},
            inputs=(),
            records=(record,),
            artifacts=(),
            outputs=(NamedOutput(name=("record",), object_identifier=record.identifier),),
            result={"saved": True},
            started_at_utc="2026-01-01T00:00:00+00:00",
            ended_at_utc="2026-01-01T00:00:01+00:00",
            duration_ns=1_000_000_000,
        )
        manager = stage_manager(database)
        async with manager.admit(
            stage.identifier,
            component=_COMPONENT,
            provenance=_PROVENANCE,
            invocation={"test": True},
        ) as admission:
            execution = admission.execution
            assert execution is not None
            outcome = CompletedWork(records=(record,) if rollback else (), result={"done": True})
            if rollback:
                with pytest.raises(apsw.ConstraintError):
                    await execution.apply(StagePublication(outcome, (child,)))
                with pytest.raises(KeyError):
                    _ = await database.operation(child_identifier)
            else:
                await execution.apply(StagePublication(outcome, (child,)))
        work = await database.work(stage.work_item_identifier)
        if rollback:
            assert work["state"] == "leased"
            with pytest.raises(KeyError):
                _ = await database.get_record(record.identifier)
        else:
            assert work["state"] == "completed"
            child_operation = await database.operation(child_identifier)
            assert child_operation["state"] == "completed"
            assert child_operation["invocation"] == {"test": True}
            assert child_operation["code_provenance"] == _PROVENANCE.as_json()


async def test_resumed_child_records_actual_execution_provenance(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _initial(database, resources=())
        await database.register_stage(stage, created_at_utc_ns=time_ns())
        manager = stage_manager(database)
        async with manager.admit(
            stage.identifier, component=_COMPONENT, provenance=_PROVENANCE, invocation={"old": True}
        ) as admission:
            assert admission.execution is not None
            await admission.execution.suspend(
                WaitWork(continuation=stage, wait_kind=StageWaitKind.RESOURCE, result={})
            )
        current = _PROVENANCE.model_copy(update={"commit_hash": "b" * 40})
        child = ChildOperationDraft(
            identifier=str(uuid4()),
            component=_COMPONENT,
            configuration={},
            inputs=(),
            records=(),
            artifacts=(),
            outputs=(),
            result={},
            started_at_utc="2026-01-01T00:00:00+00:00",
            ended_at_utc="2026-01-01T00:00:01+00:00",
            duration_ns=1,
        )
        async with manager.admit(
            stage.identifier, component=_COMPONENT, provenance=current, invocation={"new": True}
        ) as admission:
            assert admission.execution is not None
            await admission.execution.apply(StagePublication(CompletedWork(result={}), (child,)))
        parent = await database.operation(stage.operation_identifier)
        actual = await database.operation(child.identifier)
        assert parent["code_provenance"] == _PROVENANCE.as_json()
        assert parent["invocation"] == {"old": True}
        assert actual["code_provenance"] == current.as_json()
        assert actual["invocation"] == {"new": True}


async def test_outer_publication_rollback_releases_interrupted_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _initial(database, resources=())
        await database.register_stage(stage, created_at_utc_ns=time_ns())
        manager = stage_manager(database)
        async with manager.admit(
            stage.identifier, component=_COMPONENT, provenance=_PROVENANCE, invocation={}
        ) as admission:
            execution = admission.execution
            assert execution is not None
            transaction = database.transaction
            depth = 0

            @asynccontextmanager
            async def failing_commit() -> AsyncIterator[None]:
                nonlocal depth
                depth += 1
                try:
                    async with transaction():
                        yield
                        if depth == 1:
                            raise RuntimeError("Outer publication rolled back")
                finally:
                    depth -= 1

            with monkeypatch.context() as patch:
                patch.setattr(database, "transaction", failing_commit)
                with pytest.raises(RuntimeError, match="Outer publication rolled back"):
                    await execution.apply(StagePublication(CompletedWork(result={})))
                assert not execution.finalized
        assert not await database.expired_stage_executions(
            coordination_scope=manager.resources.scope, now_utc_ns=time_ns() + 120_000_000_000
        )
        operation = await database.operation(stage.operation_identifier)
        assert operation["state"] == "started"


@pytest.mark.parametrize("terminal", (False, True))
async def test_default_stage_heartbeat_tolerates_committed_publication_return_delay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, terminal: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _initial(database, resources=())
        await database.register_stage(stage, created_at_utc_ns=time_ns())
        manager = replace(
            stage_manager(database), lease_duration_ns=5_000_000_000, renewal_interval_ns=10_000_000
        )
        async with manager.admit(
            stage.identifier, component=_COMPONENT, provenance=_PROVENANCE, invocation={}
        ) as admission:
            execution = admission.execution
            assert execution is not None
            transaction = database.transaction

            @asynccontextmanager
            async def delayed_return() -> AsyncIterator[None]:
                async with transaction():
                    yield
                await anyio.sleep(0.1)

            with monkeypatch.context() as patch:
                patch.setattr(database, "transaction", delayed_return)
                with anyio.fail_after(3):
                    if terminal:
                        await execution.finish(CompletedWork(result={}))
                    else:
                        await execution.suspend(
                            WaitWork(
                                continuation=stage, wait_kind=StageWaitKind.RESOURCE, result={}
                            )
                        )
                assert execution.finalized
