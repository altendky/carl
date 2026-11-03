"""The group seek index follows actual runnable membership in every host scope."""

# Projection invariants inspect the persisted mirror without loading work payloads.
# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl._tests.test_fair_storage import _seed
from carl._tests.test_stage_storage import NOW
from carl.core.resources import ResourceRequest
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _assert_counts(database: Database) -> None:
    async with database._connections.reader() as connection:
        roots = await (
            await connection.execute(
                """SELECT root_request_identifier,lane,ready_count,
                (SELECT count(*) FROM fair_ready f WHERE f.active=1
                  AND f.root_request_identifier=s.root_request_identifier AND f.lane=s.lane)
                FROM fair_root_service s"""
            )
        ).fetchall()
        workspaces = await (
            await connection.execute(
                """SELECT workspace_key,lane,ready_count,
                (SELECT count(*) FROM fair_ready f WHERE f.active=1
                  AND f.workspace_key=s.workspace_key AND f.service_class=s.service_class
                  AND f.lane=s.lane) FROM fair_workspace_service s"""
            )
        ).fetchall()
    assert roots and workspaces
    assert all(row[2] == row[3] for row in roots), roots
    assert all(row[2] == row[3] for row in workspaces), workspaces


async def test_runnable_group_counts_follow_sleep_wake_lane_and_new_scope(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        first = await _seed(database, "first", root="A", workspace="shared")
        await _seed(database, "second", root="A", workspace="shared")
        await _seed(database, "third", root="B", workspace="shared")
        await database.enable_fair_scheduling("one", NOW)
        await _assert_counts(database)
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_stages SET progress_waiting=1 WHERE id=?", (first.identifier,)
            )
        await _assert_counts(database)
        # Scope creation while a parent sleeps must start with the current live
        # membership, rather than resurrecting the historical group's size.
        await database.enable_fair_scheduling("two", NOW)
        await _assert_counts(database)
        await _seed(database, "fourth", root="A", workspace="shared")
        await _assert_counts(database)
        metadata = first.model_copy(update={"resources": ()})
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_stages SET progress_waiting=0,definition_json=? WHERE id=?",
                (metadata.model_dump_json(), first.identifier),
            )
        await _assert_counts(database)
        cpu = first.model_copy(update={"resources": (ResourceRequest(name="cpu"),)})
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_stages SET definition_json=? WHERE id=?",
                (cpu.model_dump_json(), first.identifier),
            )
            await connection.execute("UPDATE work_items SET state='completed' WHERE id='first'")
        await _assert_counts(database)


async def test_repeated_nonmembership_updates_do_not_change_counts(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database, "one", root="A")
        await database.enable_fair_scheduling("one", NOW)
        async with database._connections.writer() as connection:
            for priority in range(10):
                await connection.execute(
                    "UPDATE work_items SET priority=? WHERE id='one'", (priority,)
                )
                await connection.execute(
                    "UPDATE work_stages SET eligible_at_utc_ns=? WHERE work_item_id='one'",
                    (NOW + priority,),
                )
        await _assert_counts(database)
