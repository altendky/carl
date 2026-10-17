"""Database identity checks tolerate short locks before the pool is opened."""

# pyright: reportPrivateUsage=false

from contextlib import closing
from pathlib import Path

import anyio
import apsw
import pytest

from carl.io.sqlite import Database


@pytest.mark.anyio
async def test_preflight_waits_for_transient_exclusive_wal_lock(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True):
        pass

    started = anyio.Event()
    completed = anyio.Event()

    async def preflight() -> None:
        started.set()
        await Database._preflight_path(path, allow_empty=False)
        completed.set()

    with closing(apsw.Connection(str(path))) as blocker:
        assert blocker.pragma("journal_mode") == "wal"
        blocker.pragma("locking_mode", "exclusive")
        blocker.execute("BEGIN EXCLUSIVE")
        _ = blocker.execute("SELECT count(*) FROM sqlite_schema").fetchall()

        with anyio.fail_after(5):
            async with anyio.create_task_group() as group:
                group.start_soon(preflight)
                try:
                    await started.wait()
                    await anyio.sleep(0.15)
                    assert not completed.is_set()
                finally:
                    blocker.close()
            assert completed.is_set()
