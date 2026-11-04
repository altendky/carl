"""Only definitive declines can omit permit-release traffic during guard cleanup."""

# Physical guard and ledger assertions exercise the ownership boundary.
# pyright: reportPrivateUsage=false

import os
from contextlib import AsyncExitStack
from pathlib import Path
from time import time_ns
from typing import Any

import pytest

from carl._tests.test_native_routing_pool import _dataset, _Factory, _held, _until
from carl._tests.test_resident_sessions import _COMPONENT, _PROVENANCE, _initial
from carl._tests.test_resource_adoption import _reservation
from carl.core.resources import ResourceRequest
from carl.io.native_routing_pool import NativeRoutingPool
from carl.io.native_routing_protocol import NativeRoutingProtocolError
from carl.io.resource_permits import current_resource_manager
from carl.io.sqlite import Database
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("blocked_by", ("time", "capacity"))
async def test_repeated_real_declines_skip_release_and_remove_their_guards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocked_by: str
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        manager = stage_manager(database)
        initial = await _initial(database, resources=(ResourceRequest(name="cpu"),))
        if blocked_by == "time":
            initial = initial.model_copy(update={"not_before_utc_ns": time_ns() + 60_000_000_000})
        await database.register_stage(initial, created_at_utc_ns=time_ns())
        async with AsyncExitStack() as stack:
            if blocked_by == "capacity":
                await stack.enter_async_context(_reservation(database, ResourceRequest(name="cpu")))
            guards = set(manager.resources.guard_directory.glob("*.lock"))
            releases: list[str] = []
            original = database.release_resource_permit

            async def release(*, token: str, owner: str) -> bool:
                releases.append(token)
                return await original(token=token, owner=owner)

            monkeypatch.setattr(database, "release_resource_permit", release)
            for _ in range(3):
                async with manager.admit(
                    initial.identifier, component=_COMPONENT, provenance=_PROVENANCE, invocation={}
                ) as admission:
                    assert admission.execution is None and admission.result.lease is None
            assert releases == []
            assert set(manager.resources.guard_directory.glob("*.lock")) == guards
            assert (await database.work(initial.work_item_identifier))["attempt"] == 0


@pytest.mark.parametrize("uncertain", (False, True))
async def test_admitted_and_uncertain_claims_retain_release_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, uncertain: bool
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        manager = stage_manager(database)
        initial = await _initial(database, resources=(ResourceRequest(name="cpu"),))
        await database.register_stage(initial, created_at_utc_ns=time_ns())
        releases: list[str] = []
        original_release = database.release_resource_permit
        original_claim = database.try_claim_stage

        async def release(*, token: str, owner: str) -> bool:
            releases.append(token)
            return await original_release(token=token, owner=owner)

        async def uncertain_claim(**kwargs: Any):
            result = await original_claim(**kwargs)
            assert result.lease is not None
            raise RuntimeError("uncertain claim after commit")

        monkeypatch.setattr(database, "release_resource_permit", release)
        if uncertain:
            monkeypatch.setattr(database, "try_claim_stage", uncertain_claim)
        with pytest.raises(RuntimeError, match="after commit" if uncertain else "execution body"):
            async with manager.admit(
                initial.identifier, component=_COMPONENT, provenance=_PROVENANCE, invocation={}
            ) as admission:
                assert admission.execution is not None
                raise RuntimeError("execution body")
        assert len(releases) == 1
        assert await _held(database, "cpu") == 0
        guard_path = manager.resources.guard_directory / f"{releases[0]}.lock"
        assert guard_path.exists()
        descriptor = os.open(guard_path, os.O_RDONLY)
        try:
            import fcntl

            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)


@pytest.mark.parametrize("uncertain", (False, True))
async def test_native_engine_distinguishes_decline_from_uncertain_acquisition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, uncertain: bool
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3",
        initialize=True,
        resource_capacities={"cpu": 1, "routing_engines": 1},
    ) as database:
        manager = current_resource_manager()
        assert manager is not None
        original = database.try_acquire_resource_permit
        releases: list[str] = []
        original_release = database.release_resource_permit

        async def acquire(**kwargs: Any):
            if not uncertain:
                return None
            assert await original(**kwargs) is not None
            raise RuntimeError("uncertain native admission after commit")

        async def release(*, token: str, owner: str) -> bool:
            releases.append(token)
            return await original_release(token=token, owner=owner)

        monkeypatch.setattr(database, "try_acquire_resource_permit", acquire)
        monkeypatch.setattr(database, "release_resource_permit", release)
        guards = set(manager.guard_directory.glob("*.lock"))
        factory = _Factory()
        pool = NativeRoutingPool(manager, process_factory=factory)
        if uncertain:
            with pytest.raises(NativeRoutingProtocolError):
                await pool.try_borrow(_dataset(tmp_path))
        else:
            assert await pool.try_borrow(_dataset(tmp_path)) is None
        await _until(lambda: pool.owner_count == 0)
        assert not factory.processes
        assert await _held(database, "routing_engines") == 0
        assert len(releases) == int(uncertain)
        if not uncertain:
            assert set(manager.guard_directory.glob("*.lock")) == guards
