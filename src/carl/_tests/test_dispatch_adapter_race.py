"""Fresh snapshot capabilities, not stale discovery metadata, select adapters."""

# Exercise one real dispatcher runner and its private zero-buffer handoff.
# pyright: reportPrivateUsage=false

from pathlib import Path
from time import time_ns
from uuid import uuid4

import anyio
import pytest

from carl._tests.test_stage_execution import _PROVENANCE
from carl._tests.test_worker import _requester
from carl.core.components import Component, ComponentId
from carl.core.models import StrictModel
from carl.core.stages import StageAttemptContext, StageDefinition
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkCapability, WorkDefinition
from carl.core.worker import CompletedWork
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.sqlite import Database
from carl.io.stage_dispatcher import StageDispatcher
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler, WorkStageAdapter
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio
_KIND = ("test", "versioned_dispatch")


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


class _Reference(StrictModel):
    version: int


@pytest.mark.parametrize("current_version", (2, 99))
async def test_changed_capability_before_snapshot_never_uses_discovered_adapter(
    tmp_path: Path, current_version: int
) -> None:
    factories: list[int] = []
    executed: list[int] = []
    components = {
        version: Component(ComponentId(("test", "adapter", str(version))), version, lambda: None)
        for version in (1, 2)
    }

    def adapter(version: int) -> WorkStageAdapter:
        def initial(work: WorkDefinition, identifier: str, operation: str) -> StageDefinition:
            factories.append(version)
            assert work.payload_schema_version == version
            return StageDefinition(
                identifier=identifier,
                work_item_identifier=work.identifier,
                operation_identifier=operation,
                kind=(*_KIND, str(version)),
                payload=_Reference(version=version).model_dump(mode="json"),
            )

        return WorkStageAdapter(
            WorkCapability(kind=_KIND, payload_schema_version=version),
            components[version],
            initial,
            initial_resources=(),
        )

    async def execute(payload: _Reference, _context: StageAttemptContext) -> CompletedWork:
        executed.append(payload.version)
        return CompletedWork(result={"version": payload.version})

    registry = StageHandlerRegistry(
        adapters=tuple(adapter(version) for version in (1, 2)),
        handlers=tuple(
            TypedStageHandler((*_KIND, str(version)), 1, _Reference, execute) for version in (1, 2)
        ),
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        identifier = str(uuid4())
        work = WorkDefinition(
            identifier=identifier,
            kind=_KIND,
            payload_schema_version=1,
            payload={"retained": "before upgrade"},
            deduplication_identity=(identifier,),
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=_KIND),
            ),
        )
        await database.enqueue_work(
            work,
            _requester(identifier),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        page = await database.dispatch_candidates(
            supported_capabilities=tuple(item.capability for item in registry.adapters),
            now_utc_ns=time_ns(),
            limit=1,
        )
        assert len(page.items) == 1 and page.items[0].capability.payload_schema_version == 1
        # A retry/schema migration changes the queued definition after metadata
        # discovery but before this runner's authoritative point snapshot.
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET payload_schema_version=?,payload_json=? WHERE id=?",
                (current_version, '{"retained":"after upgrade"}', identifier),
            )
        manager = stage_manager(database)
        residents = ResidentSessionPool(manager, registry, _PROVENANCE, {})

        async def never_owned(_identifier: str) -> bool:
            raise AssertionError("Ordinary versioned work was routed to a dataset owner")

        dispatcher = StageDispatcher(
            manager, registry, residents, _PROVENANCE, {}, never_owned, lambda _kind: False
        )
        send, receive = anyio.create_memory_object_stream(1)
        await send.send(page.items[0])
        await send.aclose()
        await dispatcher._execute(receive)
        retained = await database.work(identifier)
        if current_version == 99:
            assert factories == executed == []
            assert retained["state"] == "pending" and retained["attempt"] == 0
            assert retained["operations"] == []
            async with database._connections.reader() as connection:
                rows = await (
                    await connection.execute("SELECT count(*) FROM work_stages")
                ).fetchall()
            assert rows == [(0,)]
        else:
            assert factories == executed == [2]
            assert retained["state"] == "completed" and retained["attempt"] == 1
            operations = retained["operations"]
            assert isinstance(operations, list) and len(operations) == 1
            operation_entry = operations[0]
            assert isinstance(operation_entry, dict)
            operation_id = operation_entry["operation_identifier"]
            assert isinstance(operation_id, str)
            operation = await database.operation(operation_id)
            assert operation["component_parts"] == list(components[2].identifier.parts)
            assert operation["output_schema_version"] == 2
