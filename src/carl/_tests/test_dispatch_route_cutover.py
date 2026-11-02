"""Route revisions never mistake suspended stage ownership for an expired legacy lease."""

# Exact SQL snapshots verify rollback without mutating production route code.
# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl._tests.test_route_cutover import NEW, _begin, _cutover, _definitions, _enqueue
from carl._tests.test_stage_storage import DURATION, NOW, _claim, _definition, _scalar
from carl.core.stages import StageWaitKind, WaitWork
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.mark.parametrize("stage_state", ("pending", "running_expired", "suspended"))
async def test_cutover_rejects_any_live_stage_attempt_atomically(
    tmp_path: Path, stage_state: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        search, _, _, other, _ = _definitions()
        await _enqueue(database, search)
        await _enqueue(database, other)
        stage = _definition(search.identifier, identifier="owned-stage", resources=())
        await database.register_stage(stage, created_at_utc_ns=NOW)
        if stage_state != "pending":
            admission = await _claim(database, stage.identifier)
            assert admission.lease is not None
            if stage_state == "suspended":
                continuation = stage.model_copy(update={"identifier": "continuation"})
                await database.suspend_stage(
                    admission.lease,
                    WaitWork(
                        continuation=continuation,
                        wait_kind=StageWaitKind.RESOURCE,
                        result={"retained": "checkpoint"},
                    ),
                    now_utc_ns=NOW + 1,
                )
        before = tuple(
            [await database.work_definition(item.identifier) for item in (search, other)]
        )
        async with database._connections.reader() as connection:
            rows_before = await (
                await connection.execute("SELECT * FROM work_items ORDER BY id")
            ).fetchall()
        await _begin(database)
        with pytest.raises(ValueError):
            await _cutover(database, now=NOW + DURATION + 1)
        after = tuple([await database.work_definition(item.identifier) for item in (search, other)])
        assert after == before
        async with database._connections.reader() as connection:
            rows_after = await (
                await connection.execute("SELECT * FROM work_items ORDER BY id")
            ).fetchall()
        assert rows_after == rows_before
        assert await _scalar(database, "SELECT count(*) FROM records WHERE object_id='audit'") == 0
        assert await _scalar(database, "SELECT state FROM stage_attempts") in ("pending", "active")
        if stage_state == "suspended":
            assert await database.stage("continuation") == stage.model_copy(
                update={"identifier": "continuation"}
            )
            assert (await database.operation(stage.operation_identifier))["state"] == "started"


async def test_completed_stage_history_allows_pending_retry_route_revision(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        search = _definitions()[0]
        await _enqueue(database, search)
        stage = _definition(search.identifier, resources=())
        await database.register_stage(stage, created_at_utc_ns=NOW)
        claim = await _claim(database)
        assert claim.lease is not None
        lease = claim.lease
        async with database.transaction():
            await database.complete_stage(lease, now_utc_ns=NOW + 1)
            await database.retry_leased_operation(
                work_item_identifier=search.identifier,
                lease_token=lease.token,
                worker_identifier=lease.work_lease.worker_identifier,
                utc_now_ns=lambda: NOW + 1,
                event_identifier="retry",
                operation_id=stage.operation_identifier,
                records=(),
                artifacts=(),
                outputs=(),
                result={"retained": "retry"},
                reason={"kind": "actual_acquisition_retry"},
                delay_ns=0,
                ended_at_utc="2027-01-15T08:00:00+00:00",
                duration_ns=1,
            )
        await _begin(database)
        assert await _cutover(database, now=NOW + 2) == 1
        revised = await database.work_definition(search.identifier)
        assert isinstance(revised.payload, dict) and revised.payload["routing"] == list(NEW)
        assert (await database.stage(stage.identifier)).payload == stage.payload
        assert await _scalar(database, "SELECT state FROM stage_attempts") == "completed"
        assert (await database.work(search.identifier))["attempt"] == 1
