"""Retained image reuse checks bounded content under shared CPU admission."""

# Deliberately corrupt stored lengths and observe the physical read boundary.
# pyright: reportPrivateUsage=false

import os
import threading
from pathlib import Path
from time import time_ns

import anyio
import pytest

import carl.io.image_reuse as image_reuse
import carl.io.sqlite as sqlite
from carl._tests.test_artifact_read_bounds import _publish
from carl._tests.test_resource_adoption import _reservation
from carl.core.content_encoding import MAX_DECODED_BYTES
from carl.core.models import JsonValue
from carl.core.resources import ResourceRequest
from carl.facebook_image_workers import validate_saved_image_results
from carl.io.resource_permits import (
    cpu_reservation,
    current_cpu_guard_descriptor,
    current_resource_manager,
)
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.mark.parametrize("external", (False, True))
async def test_reuse_uses_existing_decoded_image_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, external: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _ = await _publish(database, content=b"image", external=external)
        read = database.get_artifact
        bounds: list[int | None] = []

        async def bounded_read(
            identifier: str, *, maximum_bytes: int | None = None
        ) -> tuple[dict[str, JsonValue], bytes]:
            bounds.append(maximum_bytes)
            return await read(identifier, maximum_bytes=maximum_bytes)

        monkeypatch.setattr(database, "get_artifact", bounded_read)
        assert await image_reuse.image_artifact_available(database, "body")
        assert bounds == [MAX_DECODED_BYTES] == [100_000_000]


@pytest.mark.parametrize("forged_length", (False, True))
async def test_oversized_inline_reuse_never_selects_blob(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, forged_length: bool
) -> None:
    # A small substituted policy exercises the same pre-allocation rejection
    # without allocating a 100 MB fixture. The real policy is asserted above.
    monkeypatch.setattr(image_reuse, "MAX_DECODED_BYTES", 5 if forged_length else 4)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _ = await _publish(database, content=b"image", external=False)
        if forged_length:
            async with database._connections.writer() as connection:
                _ = await connection.execute("PRAGMA ignore_check_constraints = ON")
                _ = await connection.execute("UPDATE content SET inline_bytes = ?", (b"x" * 10,))
                _ = await connection.execute("PRAGMA ignore_check_constraints = OFF")
        statements: list[str] = []

        def trace(_cursor: object, statement: str, _bindings: object) -> bool:
            statements.append(statement)
            return True

        # Admit before pinning the tracing reader's snapshot; the nested helper
        # must observe this already-committed permit when validating its fence.
        async with cpu_reservation(), database._connections.reader() as connection:
            connection.set_exec_trace(trace)
            try:
                assert not await image_reuse.image_artifact_available(database, "body")
            finally:
                connection.set_exec_trace(None)
        reads = [statement for statement in statements if "inline_bytes" in statement]
        assert reads and all("length(content.inline_bytes)" in statement for statement in reads)


@pytest.mark.parametrize("forged_length", (False, True))
async def test_oversized_external_reuse_rejected_before_content_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, forged_length: bool
) -> None:
    monkeypatch.setattr(image_reuse, "MAX_DECODED_BYTES", 5 if forged_length else 4)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        path = await _publish(database, content=b"image", external=True)
        assert path is not None
        if forged_length:
            _ = path.write_bytes(b"x" * 10)
        read = os.read
        stat = os.fstat
        inode = path.stat().st_ino

        def no_body_read(descriptor: int, length: int) -> bytes:
            assert stat(descriptor).st_ino != inode, "Oversized image reached content read"
            return read(descriptor, length)

        monkeypatch.setattr(os, "read", no_body_read)
        assert not await image_reuse.image_artifact_available(database, "body")


async def test_external_growth_after_stat_remains_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(image_reuse, "MAX_DECODED_BYTES", 5)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        path = await _publish(database, content=b"image", external=True)
        assert path is not None
        stat, read = os.fstat, os.read
        inode = path.stat().st_ino
        requested: list[int] = []
        grew = False

        def grow_after_stat(descriptor: int) -> os.stat_result:
            nonlocal grew
            info = stat(descriptor)
            if info.st_ino == inode and not grew:
                grew = True
                with path.open("ab") as stream:
                    _ = stream.write(b"x" * 10)
            return info

        def bounded_read(descriptor: int, length: int) -> bytes:
            if stat(descriptor).st_ino == inode:
                requested.append(length)
            return read(descriptor, length)

        monkeypatch.setattr(os, "fstat", grow_after_stat)
        monkeypatch.setattr(os, "read", bounded_read)
        assert not await image_reuse.image_artifact_available(database, "body")
        assert grew and requested == [6]


@pytest.mark.parametrize("failure", ("missing", "corrupt"))
async def test_missing_or_corrupt_external_image_is_unavailable(
    tmp_path: Path, failure: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        path = await _publish(database, content=b"image", external=True)
        assert path is not None
        if failure == "missing":
            path.unlink()
        else:
            _ = path.write_bytes(b"other")
        assert not await image_reuse.image_artifact_available(database, "body")
        assert not await image_reuse.image_artifact_available(database, "unknown")


async def test_cancelled_external_verification_retains_cpu_until_thread_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        _ = await _publish(database, content=b"image", external=True)
        manager = current_resource_manager()
        assert manager is not None
        manager.poll_seconds = 0.01
        read = sqlite._read_external_artifact
        started, release, second_started = threading.Event(), threading.Event(), threading.Event()
        finished, second_finished, second_requested = anyio.Event(), anyio.Event(), anyio.Event()
        calls = 0

        def blocking_read(
            database_path: Path,
            locator: str,
            expected_sha256: str,
            expected_size: int,
            maximum_bytes: int | None = None,
        ) -> tuple[Path, bytes]:
            nonlocal calls
            calls += 1
            result = read(database_path, locator, expected_sha256, expected_size, maximum_bytes)
            if calls == 1:
                # Even after the physical hash, the actual worker has not returned.
                started.set()
                assert release.wait(10), "test did not release verification thread"
            else:
                second_started.set()
            return result

        monkeypatch.setattr(sqlite, "_read_external_artifact", blocking_read)
        scope = anyio.CancelScope()

        async def first() -> None:
            with scope:
                _ = await image_reuse.image_artifact_available(database, "body")
            finished.set()

        async def second() -> None:
            second_requested.set()
            assert await image_reuse.image_artifact_available(database, "body")
            second_finished.set()

        async with anyio.create_task_group() as tasks:
            try:
                _ = tasks.start_soon(first)
                with anyio.fail_after(5):
                    while not started.is_set():
                        await anyio.sleep(0.01)
                scope.cancel()
                _ = tasks.start_soon(second)
                await second_requested.wait()
                await anyio.sleep(0.1)
                assert not finished.is_set()
                assert not second_started.is_set()
            finally:
                release.set()
            with anyio.fail_after(5):
                await finished.wait()
                await second_finished.wait()
        assert calls == 2


async def test_waiting_reuse_can_cancel_without_starting_file_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        _ = await _publish(database, content=b"image", external=True)
        manager = current_resource_manager()
        assert manager is not None
        manager.poll_seconds = 0.01
        started, finished = anyio.Event(), anyio.Event()
        scope = anyio.CancelScope()

        def no_read(*_args: object) -> tuple[Path, bytes]:
            raise AssertionError("CPU waiter entered physical image verification")

        monkeypatch.setattr(sqlite, "_read_external_artifact", no_read)

        async def waiter() -> None:
            with scope:
                started.set()
                _ = await image_reuse.image_artifact_available(database, "body")
            finished.set()

        async with (
            anyio.create_task_group() as tasks,
            manager.admit(ResourceRequest(name="cpu")),
        ):
            _ = tasks.start_soon(waiter)
            await started.wait()
            await anyio.sleep(0.05)
            scope.cancel()
            with anyio.fail_after(5):
                await finished.wait()


async def test_reuse_borrows_existing_stage_cpu_allocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        _ = await _publish(database, content=b"image", external=True)
        read = database.get_artifact
        descriptors: list[int] = []

        async def observed_read(
            identifier: str, *, maximum_bytes: int | None = None
        ) -> tuple[dict[str, JsonValue], bytes]:
            descriptors.append(await current_cpu_guard_descriptor())
            return await read(identifier, maximum_bytes=maximum_bytes)

        monkeypatch.setattr(database, "get_artifact", observed_read)
        async with _reservation(database, ResourceRequest(name="cpu")) as (manager, lease, guard):
            with anyio.fail_after(5):
                async with manager.adopt(lease, guard):
                    assert await image_reuse.image_artifact_available(database, "body")
                assert descriptors == [guard.descriptor]
                assert guard.active_borrowers == 0
                assert await database.validate_resource_permit(
                    token=lease.token, owner=lease.owner, now_utc_ns=time_ns()
                )


async def test_facebook_shared_artifact_verifies_once_and_preserves_consumer_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _ = await _publish(database, content=b"image", external=True)
        read = database.get_artifact
        checked: list[str] = []

        async def observed_read(
            identifier: str, *, maximum_bytes: int | None = None
        ) -> tuple[dict[str, JsonValue], bytes]:
            checked.append(identifier)
            assert maximum_bytes == MAX_DECODED_BYTES
            return await read(identifier, maximum_bytes=maximum_bytes)

        monkeypatch.setattr(database, "get_artifact", observed_read)
        first: dict[str, JsonValue] = {"image_artifact_identifier": "body", "listing": "first"}
        second: dict[str, JsonValue] = {"image_artifact_identifier": "body", "listing": "second"}
        assert await validate_saved_image_results(
            database,
            (
                ("result-first", first),
                ("result-second", second),
                ("missing", {"image_artifact_identifier": "missing"}),
                ("missing-again", {"image_artifact_identifier": "missing"}),
            ),
        ) == (("result-first", first), ("result-second", second))
        assert checked == ["body", "missing"]
