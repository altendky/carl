"""Separate processes arbitrate one shared fair sequence under the resource writer."""

import subprocess
import sys
from pathlib import Path

import anyio
import pytest

from carl._tests.test_fair_storage import _seed
from carl._tests.test_stage_storage import COMPONENT, DURATION, NOW
from carl._tests.test_worker import _provenance
from carl.core.resources import ResourceRequest
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.mark.parametrize("capacity", (1, 2))
async def test_two_processes_share_atomic_sequence_and_never_overallocate(
    tmp_path: Path, capacity: int
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        for identifier in ("A", "B"):
            await _seed(database, identifier, root=identifier)
        await database.enable_fair_scheduling("test-host", NOW)
        hold = await database.try_acquire_resource_permit(
            scope="test-host",
            token="hold",
            owner="holder",
            requests=(ResourceRequest(name="cpu", units=capacity),),
            capacities={"cpu": capacity},
            now_utc_ns=NOW,
            lease_duration_ns=DURATION,
        )
        assert hold is not None
        # Both processes have a live contender before the first free unit.
        for identifier in ("A", "B"):
            result = await database.try_claim_stage(
                stage_identifier="stage-" + identifier,
                token="offer-" + identifier,
                worker_identifier=identifier,
                resource_scope="test-host",
                capacities={"cpu": capacity},
                lease_duration_ns=DURATION,
                now_utc_ns=NOW,
                component=COMPONENT,
                provenance=_provenance(),
                invocation={},
            )
            assert result.lease is None
        assert await database.release_resource_permit(token="hold", owner="holder")
    script = """
import sys
from pathlib import Path
import anyio
from carl.io.sqlite import Database
from carl._tests.test_stage_storage import COMPONENT,NOW,DURATION
from carl._tests.test_worker import _provenance
async def main():
    path,identifier,capacity,ready,go=sys.argv[1:]
    async with Database.managed(Path(path)) as database:
        Path(ready).touch()
        while not Path(go).exists(): await anyio.sleep(.01)
        for attempt in range(5):
            result=await database.try_claim_stage(stage_identifier='stage-'+identifier,token=identifier+str(attempt),worker_identifier=identifier,resource_scope='test-host',capacities={'cpu':int(capacity)},lease_duration_ns=DURATION,now_utc_ns=NOW+100_000_001+attempt*100_000_001,component=COMPONENT,provenance=_provenance(),invocation={})
            if result.lease is not None:
                print('yes')
                return
            await anyio.sleep(.01)
        print('no')
anyio.run(main,backend='asyncio')
"""
    children: list[anyio.abc.Process] = []
    try:
        for identifier in ("A", "B"):
            children.append(
                await anyio.open_process(
                    (
                        sys.executable,
                        "-c",
                        script,
                        str(path),
                        identifier,
                        str(capacity),
                        str(tmp_path / ("ready-" + identifier)),
                        str(tmp_path / "go"),
                    ),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            )
        with anyio.fail_after(20):
            while not all(
                (tmp_path / ("ready-" + identifier)).exists() for identifier in ("A", "B")
            ):
                await anyio.sleep(0.01)
            (tmp_path / "go").touch()
            granted = 0
            for child in children:
                assert child.stdout is not None
                granted += int((await child.stdout.receive(64)).strip() == b"yes")
                assert await child.wait() == 0
        assert granted == capacity
    finally:
        with anyio.CancelScope(shield=True):
            for child in children:
                if child.returncode is None:
                    child.kill()
                    await child.wait()
                await child.aclose()
    async with Database.managed(path) as database:
        turns = await database.fair_admissions("test-host")
        assert len(turns) == capacity
        assert sorted(turn.sequence for turn in turns) == list(range(1, capacity + 1))
