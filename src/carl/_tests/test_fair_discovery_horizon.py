"""A discovery pass stays finite while other admissions advance its turn ledger."""

# Seed exact turn counters and charge actual fairness admissions between pages.
# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest
from pydantic import ValidationError

from carl._tests.test_fair_storage import _seed
from carl._tests.test_stage_storage import CAPABILITY, NOW
from carl.core.fair_scheduling import (
    FairDispatchCursor,
    FairLane,
    SchedulingServiceClass,
)
from carl.core.json import encode_json
from carl.core.resources import ResourceRequest
from carl.io import fair_scheduling as fair_sql
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
SCOPE = "test-host"


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _work(
    database: Database,
    identifier: str,
    *,
    workspace: str = "shared",
    root: str = "root",
    service: SchedulingServiceClass = SchedulingServiceClass.BACKGROUND,
    not_before: int = 0,
) -> None:
    stage = await _seed(
        database,
        identifier,
        workspace=workspace,
        root=root,
        service=service,
        register=False,
    )
    await database.register_stage(
        stage.model_copy(
            update={
                "resources": (ResourceRequest(name=identifier.split("-")[0]),),
                "not_before_utc_ns": not_before,
            }
        ),
        created_at_utc_ns=NOW,
    )


async def _enable(database: Database, *, cpu: int = 1000, http: int = 10) -> None:
    await database.enable_fair_scheduling(SCOPE, NOW)
    async with database._connections.writer() as connection:
        for lane, sequence in (("cpu", cpu), ("http", http)):
            for table in ("fair_lanes", "fair_workspace_service", "fair_root_service"):
                await connection.execute(
                    f"UPDATE {table} SET sequence=? WHERE scope=? AND lane=?",
                    (sequence, SCOPE, lane),
                )


async def _page(
    database: Database,
    *,
    after: FairDispatchCursor | None = None,
    now: int = NOW,
    limit: int = 8,
):
    return await database.fair_dispatch_candidates(
        scope=SCOPE,
        supported_capabilities=(CAPABILITY,),
        now_utc_ns=now,
        limit=limit,
        after=after,
    )


async def _charge(database: Database, identifiers: tuple[str, ...]) -> None:
    async with database._connections.writer() as connection:
        for identifier in identifiers:
            await fair_sql.charge(
                connection,
                scope=SCOPE,
                lane=FairLane(identifier.split("-")[0]),
                work_identifier=identifier,
                token="admission-" + identifier,
                now=NOW + 1,
            )
            await connection.execute(
                "UPDATE work_items SET state='completed' WHERE id=?", (identifier,)
            )


async def test_unequal_lane_turns_cannot_keep_a_pass_on_cpu_until_its_backlog_drains(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        for lane in ("cpu", "http"):
            for index in range(12):
                await _work(database, f"{lane}-{index:02}")
        await _enable(database)
        first = await _page(database)
        assert len(first.items) == 8 and first.next_cursor is not None
        identifiers = tuple(item.work_item_identifier for item in first.items)
        assert sum(identifier.startswith("http-") for identifier in identifiers) == 4
        horizon = first.next_cursor.horizon
        assert {lane.lane: lane.sequence for lane in horizon.lanes} == {
            FairLane.CPU: 1000,
            FairLane.HTTP: 10,
            FairLane.ANALYSIS: 0,
            FairLane.METADATA: 0,
            FairLane.RESIDENT: 0,
        }
        assert (
            FairDispatchCursor.model_validate_json(first.next_cursor.model_dump_json())
            == first.next_cursor
        )
        await _charge(database, identifiers)
        # Both admitted groups have advanced beyond their own lane's horizon.
        # The CPU counter must not pull the same workspace back into this pass.
        end = await _page(database, after=first.next_cursor)
        assert not end.items and not end.has_more and end.next_cursor is None
        fresh = await _page(database, now=NOW + 2)
        assert any(item.work_item_identifier.startswith("http-") for item in fresh.items)
        assert any(item.work_item_identifier.startswith("cpu-") for item in fresh.items)


async def test_another_connection_cannot_move_a_charged_group_back_into_the_pass(
    tmp_path: Path,
) -> None:
    path = tmp_path / "test.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        for lane in ("cpu", "http"):
            for index in range(4):
                await _work(database, f"{lane}-{index:02}")
        await _enable(database)
        first = await _page(database, limit=1)
        assert first.next_cursor is not None
        assert first.items[0].work_item_identifier.startswith("http-")
        async with Database.managed(path) as external:
            await _charge(external, ("cpu-00",))
        end = await _page(database, after=first.next_cursor, limit=1)
        assert not end.items and not end.has_more
        fresh = await _page(database, now=NOW + 2)
        assert any(item.work_item_identifier.startswith("cpu-") for item in fresh.items)
        assert any(item.work_item_identifier.startswith("http-") for item in fresh.items)


async def test_groups_inserted_after_the_cutoff_wait_for_the_next_pass(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _work(database, "cpu-old")
        await _work(database, "http-old")
        await _enable(database)
        first = await _page(database, limit=1)
        assert first.next_cursor is not None
        await _work(database, "http-new", workspace="zz-new-workspace", root="new-root")
        page = first
        seen = {item.work_item_identifier for item in page.items}
        for _ in range(4):
            if not page.has_more:
                break
            assert page.next_cursor is not None
            page = await _page(database, after=page.next_cursor, limit=1)
            seen.update(item.work_item_identifier for item in page.items)
        assert not page.has_more
        assert seen == {"cpu-old", "http-old"}
        fresh = await _page(database, now=NOW + 1)
        assert "http-new" in {item.work_item_identifier for item in fresh.items}


async def test_blocked_cpu_groups_still_allow_finite_paging_to_http(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        for index in range(6):
            await _work(database, f"cpu-{index}", workspace=f"a-{index}", root=f"root-{index}")
        await _work(database, "http-ready", workspace="zz-http", root="http-root")
        await _enable(database, cpu=0, http=0)
        held = await database.try_acquire_resource_permit(
            scope=SCOPE,
            token="held-cpu",
            owner="fixture",
            requests=(ResourceRequest(name="cpu", units=4),),
            capacities={"cpu": 4, "http": 25},
            now_utc_ns=NOW,
            lease_duration_ns=1_000_000_000,
        )
        assert held is not None
        cursor = None
        page = None
        seen = set()
        for _ in range(5):
            page = await _page(database, after=cursor, limit=2)
            seen.update(item.work_item_identifier for item in page.items)
            if not page.has_more:
                break
            assert page.next_cursor is not None
            cursor = page.next_cursor
        assert page is not None and not page.has_more and "http-ready" in seen
        assert await database.resource_occupancy(SCOPE) == {"cpu": 4}


async def test_a_future_http_head_becomes_visible_when_the_next_pass_refreshes_time(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        for index in range(8):
            await _work(database, f"cpu-{index}")
        await _work(database, "http-future", not_before=NOW + 100)
        await _enable(database)
        first = await _page(database, limit=4)
        assert first.next_cursor is not None
        assert all(item.work_item_identifier.startswith("cpu-") for item in first.items)
        await _charge(database, tuple(item.work_item_identifier for item in first.items))
        end = await _page(database, after=first.next_cursor, limit=4)
        assert not end.items and not end.has_more
        fresh = await _page(database, now=NOW + 200)
        assert "http-future" in {item.work_item_identifier for item in fresh.items}


async def test_external_interactive_turn_does_not_reorder_an_existing_pass(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _work(
            database,
            "cpu-interactive",
            root="interactive",
            service=SchedulingServiceClass.INTERACTIVE,
        )
        await _work(database, "cpu-background", root="background")
        await _work(
            database,
            "cpu-external",
            root="external",
            workspace="zz-external",
            service=SchedulingServiceClass.INTERACTIVE,
        )
        await _enable(database, cpu=0, http=0)
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE fair_lanes SET interactive_streak=2 WHERE scope=? AND lane='cpu'", (SCOPE,)
            )
        first = await _page(database, limit=1)
        assert first.next_cursor is not None
        assert first.items[0].work_item_identifier == "cpu-interactive"
        await _charge(database, ("cpu-external",))
        second = await _page(database, after=first.next_cursor, limit=1)
        assert [item.work_item_identifier for item in second.items] == ["cpu-background"]


async def test_ephemeral_cursor_requires_its_complete_horizon_and_original_scope_time(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _work(database, "cpu-one")
        await _work(database, "cpu-two")
        await _enable(database)
        first = await _page(database, limit=1)
        cursor = first.next_cursor
        assert cursor is not None
        legacy = cursor.model_dump(mode="json")
        del legacy["horizon"]
        with pytest.raises(ValidationError, match="horizon"):
            FairDispatchCursor.model_validate_json(encode_json(legacy))
        malformed = cursor.model_dump(mode="json")
        malformed["horizon"]["lanes"][0] = malformed["horizon"]["lanes"][1]
        with pytest.raises(ValidationError, match="exactly once"):
            FairDispatchCursor.model_validate_json(encode_json(malformed))
        with pytest.raises(ValueError, match="original scope and frozen pass time"):
            await _page(database, after=cursor, now=NOW + 1)
        await database.enable_fair_scheduling("other-host", NOW)
        with pytest.raises(ValueError, match="original scope and frozen pass time"):
            await database.fair_dispatch_candidates(
                scope="other-host",
                supported_capabilities=(CAPABILITY,),
                now_utc_ns=NOW,
                limit=1,
                after=cursor,
            )
