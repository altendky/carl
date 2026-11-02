"""A sibling fan-out does not grow fair group-head selection or decode payloads."""

# Bulk fixtures clone canonical metadata without creating thousands of transactions.
# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

import carl.io.sqlite as sqlite_io
from carl._tests.test_fair_storage import _seed
from carl._tests.test_stage_storage import NOW
from carl.core.json import encode_json
from carl.core.models import JsonValue
from carl.core.resources import ResourceRequest
from carl.core.work import WorkCapability
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def test_fair_group_head_cost_ignores_ten_thousand_siblings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        first = await _seed(database, "template", root="A", workspace="workspace-A")
        await _seed(database, "selected-B", root="B", workspace="workspace-B")
        await database.enable_fair_scheduling(
            "test-host",
            NOW,
            initial_resources=(
                (
                    WorkCapability(kind=("test", "logical_stage_work"), payload_schema_version=1),
                    (ResourceRequest(name="cpu"),),
                ),
            ),
        )
        original = sqlite_io.decode_json

        def no_payload(value: str | bytes) -> JsonValue:
            assert "PEER_PAYLOAD" not in str(value), "Fair discovery decoded a work payload"
            return original(value)

        monkeypatch.setattr(sqlite_io, "decode_json", no_payload)
        expensive: list[str] = []

        async def measure() -> int:
            steps = 0
            checkpoints: list[tuple[int, str]] = []

            def trace(_cursor: object, statement: str, _bindings: object) -> bool:
                checkpoints.append((steps, statement))
                return True

            def progress() -> bool:
                nonlocal steps
                steps += 100
                return False

            # Preserve production ordering: read-only discovery first, then a
            # separate writer publishes the bounded page's readiness.
            async with database._connections.reader() as connection:
                await connection.set_progress_handler(progress, nsteps=100, id="fair-discovery")
                connection.set_exec_trace(trace)
                try:
                    page = await database.fair_dispatch_candidates(
                        scope="test-host",
                        supported_capabilities=(
                            WorkCapability(
                                kind=("test", "logical_stage_work"), payload_schema_version=1
                            ),
                        ),
                        now_utc_ns=NOW,
                        limit=2,
                        excluded_identifiers=(first.work_item_identifier,),
                    )
                finally:
                    connection.set_exec_trace(None)
                    await connection.set_progress_handler(None, id="fair-discovery")
            async with database._connections.writer() as connection:
                await connection.set_progress_handler(progress, nsteps=100, id="fair-discovery")
                connection.set_exec_trace(trace)
                try:
                    await database.offer_fair_dispatch_candidates(
                        scope="test-host",
                        now_utc_ns=NOW,
                        work_identifiers=tuple(item.work_item_identifier for item in page.items),
                    )
                finally:
                    connection.set_exec_trace(None)
                    await connection.set_progress_handler(None, id="fair-discovery")
            expensive.extend(
                f"{end - start}: {' '.join(statement.split())[:160]}"
                for (start, statement), (end, _) in zip(
                    checkpoints, (*checkpoints[1:], (steps, "")), strict=True
                )
                if end - start > 2500
            )
            assert "selected-B" in {item.work_item_identifier for item in page.items}
            return steps

        before = await measure()
        async with database._connections.writer() as connection:
            await connection.executemany(
                """INSERT INTO work_items(id,kind_parts_json,payload_schema_version,payload_json,deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns)
                SELECT ?,kind_parts_json,payload_schema_version,'{"PEER_PAYLOAD":"never decode"}',?,'pending',0,eligible_at_utc_ns,created_at_utc_ns FROM work_items WHERE id='template'""",
                [(f"sibling-{i:05d}", encode_json([f"sibling-{i:05d}"])) for i in range(10000)],
            )
            await connection.executemany(
                "INSERT INTO work_scheduling SELECT ?,root_request_identifier,workspace_key,service_class,scheduling_json FROM work_scheduling WHERE work_item_id='template'",
                [(f"sibling-{i:05d}",) for i in range(10000)],
            )
            await connection.executemany(
                "INSERT INTO work_scopes SELECT ?,scope_kind,scope_identity_json FROM work_scopes WHERE work_item_id='template'",
                [(f"sibling-{i:05d}",) for i in range(10000)],
            )
        after = await measure()
        request.node.user_properties.extend(
            (("sqlite_steps_small", before), ("sqlite_steps_10000_siblings", after))
        )
        assert after <= before + 2500, (before, after, expensive)
