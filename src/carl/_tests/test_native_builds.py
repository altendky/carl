"""Real offline commands verify weighted admission, phase fencing and replay."""

# These assertions inspect physical guard and durable resource state directly.
# pyright: reportPrivateUsage=false

import fcntl
import hashlib
import json
import os
import sys
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import cast
from uuid import uuid4

import anyio
import pytest

from carl._tests.test_stage_execution import _admit, _seed
from carl.core.native_build import NativeBuildSpec
from carl.core.resources import ResourceRequest
from carl.io.native_build_protocol import NativeBuildCleanupError, NativeBuildProcess
from carl.io.native_build_receipts import (
    NativeBuildReceiptError,
    NativeBuildUncertain,
    command_fingerprint,
    open_receipt_directory,
    write_start_receipt,
)
from carl.io.native_builds import (
    BuildCommandCapture,
    NativeBuildBusy,
    close_native_build_quarantine,
    run_native_build_command,
)
from carl.io.resource_permits import ResourceLeaseLost, current_resource_manager
from carl.io.sqlite import Database
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@contextmanager
def _workflow(root: Path) -> Generator[int]:
    descriptor = os.open(
        root / "workflow.lock", os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600
    )
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


def _spec(
    root: Path, *, phase: str = "compile", threads: int = 1, blocking: bool = False
) -> NativeBuildSpec:
    code = f"from pathlib import Path; import os,time; p=Path({str(root / (phase + '.runs'))!r}); f=p.open('a'); f.write('run\\n'); f.flush(); os.fsync(f.fileno()); f.close(); print('native-version',flush=True)"
    if blocking:
        code += f"; Path({str(root / (phase + '.started'))!r}).touch()\nwhile not Path({str(root / (phase + '.proceed'))!r}).exists(): time.sleep(0.01)"
    return NativeBuildSpec(
        recipe_identifier="fixture-recipe",
        phase_identifier=phase,
        input_identity="source-sha256-fixture",
        execution_identifier=str(uuid4()),
        argv=(sys.executable, "-c", code),
        directory=root,
        receipt_directory=root / (phase + "-receipts"),
        threads=threads,
        timeout_seconds=10,
        stdout_limit_bytes=8192,
        stderr_limit_bytes=8192,
        combined_limit_bytes=16384,
    )


def _phase_lock_path(root: Path, spec: NativeBuildSpec) -> Path:
    name = hashlib.sha256(
        json.dumps((spec.recipe_identifier, spec.phase_identifier), separators=(",", ":")).encode()
    ).hexdigest()
    return root / ".native-build-phase-locks" / (name + ".lock")


async def _held(database: Database) -> int:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                "SELECT coalesce(sum(a.units),0) FROM resource_allocations a JOIN resource_permits p ON p.token=a.permit_token WHERE p.state='held' AND a.name='cpu'"
            )
        ).fetchall()
    assert isinstance(rows[0][0], int)
    return rows[0][0]


async def _wait_file(path: Path) -> None:
    with anyio.fail_after(10):
        while not path.exists():
            await anyio.sleep(0.005)


async def test_completed_receipt_replays_actual_execution_without_another_command_or_env_dump(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NATIVE_BUILD_PRIVATE_SENTINEL", "must-not-enter-receipt")
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        spec = _spec(tmp_path, threads=2)
        with _workflow(tmp_path) as workflow:
            first = await run_native_build_command(spec, workflow_lock_fd=workflow)
            replay_spec = spec.model_copy(update={"execution_identifier": str(uuid4())})
            assert command_fingerprint(replay_spec) == command_fingerprint(spec)
            replay = await run_native_build_command(replay_spec, workflow_lock_fd=workflow)
        assert not first.replayed and replay.replayed
        assert replay.result == first.result
        assert replay.result.execution_identifier == spec.execution_identifier
        assert replay.result.reason == "completed" and replay.result.returncode == 0
        assert replay.result.stdout_head == "native-version\n"
        assert (tmp_path / "compile.runs").read_text() == "run\n"
        assert await _held(database) == 0
        for name in ("start.json", "complete.json", "stdout.log", "stderr.log"):
            path = spec.receipt_directory / name
            assert path.stat().st_mode & 0o077 == 0
            if name.endswith("json"):
                content = path.read_text()
                assert (
                    "must-not-enter-receipt" not in content
                    and "argv" not in content
                    and "os.environ" not in content
                )


async def test_existing_incomplete_start_never_automatically_executes(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        spec = _spec(tmp_path)
        os.close(open_receipt_directory(spec.receipt_directory, create=True))
        _ = write_start_receipt(spec)
        with _workflow(tmp_path) as workflow, pytest.raises(NativeBuildUncertain):
            _ = await run_native_build_command(
                spec.model_copy(update={"execution_identifier": str(uuid4())}),
                workflow_lock_fd=workflow,
            )
        assert not (tmp_path / "compile.runs").exists() and await _held(database) == 0


@pytest.mark.parametrize("field", ["input_identity", "argv"])
async def test_completed_receipt_with_changed_identity_is_not_reused_or_overwritten(
    tmp_path: Path, field: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        spec = _spec(tmp_path)
        with _workflow(tmp_path) as workflow:
            _ = await run_native_build_command(spec, workflow_lock_fd=workflow)
            before = (spec.receipt_directory / "complete.json").read_bytes()
            changed = spec.model_copy(
                update={
                    field: "different-input"
                    if field == "input_identity"
                    else (sys.executable, "-c", "print('different')")
                }
            )
            with pytest.raises(NativeBuildUncertain):
                _ = await run_native_build_command(changed, workflow_lock_fd=workflow)
        assert (spec.receipt_directory / "complete.json").read_bytes() == before
        assert (tmp_path / "compile.runs").read_text() == "run\n"


async def test_concurrent_same_phase_sharing_workflow_fd_fails_busy_before_cpu_wait(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        spec = _spec(tmp_path, threads=3, blocking=True)
        captures: list[BuildCommandCapture] = []

        async def run(workflow: int) -> None:
            captures.append(await run_native_build_command(spec, workflow_lock_fd=workflow))

        with _workflow(tmp_path) as workflow:
            async with anyio.create_task_group() as tasks:
                _ = tasks.start_soon(run, workflow)
                await _wait_file(tmp_path / "compile.started")
                try:
                    assert await _held(database) == 3
                    with anyio.fail_after(1), pytest.raises(NativeBuildBusy):
                        _ = await run_native_build_command(
                            spec.model_copy(
                                update={
                                    "execution_identifier": str(uuid4()),
                                    "receipt_directory": tmp_path / "alternate-receipts",
                                }
                            ),
                            workflow_lock_fd=workflow,
                        )
                finally:
                    (tmp_path / "compile.proceed").touch()
        assert len(captures) == 1 and captures[0].result.reason == "completed"
        assert (tmp_path / "compile.runs").read_text() == "run\n"


async def test_weighted_three_plus_two_commands_never_overlap_under_capacity_four(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        first, second = (
            _spec(tmp_path, phase="first", threads=3, blocking=True),
            _spec(tmp_path, phase="second", threads=2, blocking=True),
        )
        captures: list[BuildCommandCapture] = []

        async def run(spec: NativeBuildSpec, workflow: int) -> None:
            captures.append(await run_native_build_command(spec, workflow_lock_fd=workflow))

        with _workflow(tmp_path) as workflow:
            async with anyio.create_task_group() as tasks:
                _ = tasks.start_soon(run, first, workflow)
                await _wait_file(tmp_path / "first.started")
                _ = tasks.start_soon(run, second, workflow)
                await _wait_file(_phase_lock_path(tmp_path, second))
                try:
                    assert await _held(database) == 3
                    assert not (tmp_path / "second.started").exists()
                finally:
                    (tmp_path / "first.proceed").touch()
                await _wait_file(tmp_path / "second.started")
                try:
                    assert await _held(database) == 2
                finally:
                    (tmp_path / "second.proceed").touch()
        assert len(captures) == 2 and all(item.result.reason == "completed" for item in captures)
        assert await _held(database) == 0


async def test_missing_manager_and_insufficient_cpu_fail_before_receipt_or_start_io(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path, threads=3)
    with pytest.raises(ResourceLeaseLost):
        _ = await run_native_build_command(spec, workflow_lock_fd=-1)
    assert not spec.receipt_directory.exists()
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 2}
    ) as database:
        with pytest.raises(ValueError, match="configured CPU"):
            _ = await run_native_build_command(spec, workflow_lock_fd=-1)
        assert not spec.receipt_directory.exists() and await _held(database) == 0


async def test_insufficient_current_adopted_allocation_fails_before_start(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 4}
    ):
        manager = current_resource_manager()
        assert manager is not None
        async with manager.admit(ResourceRequest(name="cpu")):
            with pytest.raises(ValueError, match="current permit reserves"):
                _ = await run_native_build_command(_spec(tmp_path, threads=2), workflow_lock_fd=-1)
        assert not (tmp_path / "compile-receipts").exists()


@pytest.mark.parametrize(
    "corruption", ["same_size_log", "oversize_receipt", "symlink_log", "missing_log"]
)
async def test_completed_replay_validates_bounded_receipts_and_actual_log_files(
    tmp_path: Path, corruption: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        spec = _spec(tmp_path)
        with _workflow(tmp_path) as workflow:
            _ = await run_native_build_command(spec, workflow_lock_fd=workflow)
            stdout = spec.receipt_directory / "stdout.log"
            if corruption == "same_size_log":
                _ = stdout.write_bytes(b"x" * stdout.stat().st_size)
            elif corruption == "oversize_receipt":
                _ = (spec.receipt_directory / "complete.json").write_bytes(b" " * 65537)
            else:
                stdout.unlink()
                if corruption == "symlink_log":
                    stdout.symlink_to(tmp_path / "compile.runs")
            with pytest.raises((NativeBuildReceiptError, OSError)):
                _ = await run_native_build_command(spec, workflow_lock_fd=workflow)
        assert (tmp_path / "compile.runs").read_text() == "run\n"


async def test_workflow_descriptor_must_itself_own_the_lock(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        with _workflow(tmp_path):
            unlocked = os.open(tmp_path / "workflow.lock", os.O_RDWR | os.O_CLOEXEC)
            try:
                with pytest.raises(NativeBuildReceiptError):
                    _ = await run_native_build_command(_spec(tmp_path), workflow_lock_fd=unlocked)
            finally:
                os.close(unlocked)
        assert not (tmp_path / "compile-receipts").exists()


async def test_unknown_physical_cleanup_poison_blocks_reusing_adopted_stage_units(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Uncertain:
        closed: bool = False

        def owner(self) -> NativeBuildProcess:
            return cast(NativeBuildProcess, cast(object, self))

        async def wait(self):
            raise NativeBuildCleanupError(self.owner())

        async def aclose(self) -> None:
            if not self.closed:
                raise NativeBuildCleanupError(self.owner())

        def is_closed(self) -> bool:
            return self.closed

    process, starts = Uncertain(), 0

    async def start(
        _spec: NativeBuildSpec,
        *,
        guard_descriptors: tuple[int, ...],
        startup_timeout_seconds: float,
    ) -> NativeBuildProcess:
        nonlocal starts
        assert len(guard_descriptors) == 3 and startup_timeout_seconds > 0
        starts += 1
        return process.owner()

    monkeypatch.setattr(NativeBuildProcess, "start", start)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 4}
    ) as database:
        manager = current_resource_manager()
        assert manager is not None
        stage = await _seed(database, resources=(ResourceRequest(name="cpu", units=4),))
        with _workflow(tmp_path) as workflow:
            async with _admit(stage_manager(database), stage) as admission:
                assert admission.execution is not None
                try:
                    with pytest.raises(NativeBuildCleanupError):
                        _ = await run_native_build_command(
                            _spec(tmp_path, threads=4), workflow_lock_fd=workflow
                        )
                    assert admission.execution.guard.closing
                    with pytest.raises(ResourceLeaseLost):
                        _ = await run_native_build_command(
                            _spec(tmp_path, phase="different-phase", threads=4),
                            workflow_lock_fd=workflow,
                        )
                    assert starts == 1 and await _held(database) == 4
                    assert not (tmp_path / "different-phase-receipts").exists()
                finally:
                    process.closed = True
                    await close_native_build_quarantine(manager)
                # Physical proof does not unpoison this already-failed stage.
                with pytest.raises(ResourceLeaseLost):
                    _ = await run_native_build_command(
                        _spec(tmp_path, phase="after-proof", threads=4), workflow_lock_fd=workflow
                    )
        assert await _held(database) == 0


async def test_late_cleanup_proof_reconciles_quarantine_before_database_scope_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class LateProof:
        closed: bool = False
        close_calls: int = 0

        def owner(self) -> NativeBuildProcess:
            return cast(NativeBuildProcess, cast(object, self))

        async def wait(self):
            raise NativeBuildCleanupError(self.owner())

        async def aclose(self) -> None:
            self.close_calls += 1
            if not self.closed:
                raise NativeBuildCleanupError(self.owner())

        def is_closed(self) -> bool:
            return self.closed

    process = LateProof()
    paths: list[Path] = []

    async def start(
        _spec: NativeBuildSpec,
        *,
        guard_descriptors: tuple[int, ...],
        startup_timeout_seconds: float,
    ) -> NativeBuildProcess:
        assert startup_timeout_seconds > 0
        paths.extend(
            Path(os.readlink(f"/proc/self/fd/{descriptor}")) for descriptor in guard_descriptors
        )
        return process.owner()

    monkeypatch.setattr(NativeBuildProcess, "start", start)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        manager = current_resource_manager()
        assert manager is not None
        with _workflow(tmp_path) as workflow:
            with pytest.raises(NativeBuildCleanupError):
                _ = await run_native_build_command(_spec(tmp_path), workflow_lock_fd=workflow)
            try:
                assert len(paths) == 3 and process.close_calls == 1
                async with database._connections.writer() as connection:
                    _ = await connection.execute(
                        "UPDATE resource_permits SET acquired_at_utc_ns=0,expires_at_utc_ns=1 WHERE state='held'"
                    )
                await manager._recover()
                assert await _held(database) == 1
                # Passive polling observes proof; it must not repeatedly initiate
                # slow aclose() while this subtree remains unproven.
                await anyio.sleep(0.25)
                assert process.close_calls == 1
                process.closed = True
                with anyio.fail_after(5):
                    while process.close_calls == 1:
                        await anyio.sleep(0.005)
                for index in (0, 2):
                    descriptor = os.open(paths[index], os.O_RDONLY | os.O_CLOEXEC)
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    finally:
                        os.close(descriptor)
                await manager._recover()
                assert await _held(database) == 0
                async with manager.admit(ResourceRequest(name="cpu")):
                    assert await _held(database) == 1
            finally:
                process.closed = True
                await close_native_build_quarantine(manager)


async def test_cancelled_startup_releases_parent_duplicates_and_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths: list[Path] = []
    entered, finished = anyio.Event(), anyio.Event()
    scopes: list[anyio.CancelScope] = []

    async def start(
        _spec: NativeBuildSpec,
        *,
        guard_descriptors: tuple[int, ...],
        startup_timeout_seconds: float,
    ) -> NativeBuildProcess:
        assert startup_timeout_seconds > 0
        paths.extend(
            Path(os.readlink(f"/proc/self/fd/{descriptor}")) for descriptor in guard_descriptors
        )
        entered.set()
        await anyio.sleep_forever()
        raise AssertionError("The startup fixture must be cancelled")

    monkeypatch.setattr(NativeBuildProcess, "start", start)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        spec = _spec(tmp_path)
        with _workflow(tmp_path) as workflow:

            async def call() -> None:
                with anyio.CancelScope() as scope:
                    scopes.append(scope)
                    try:
                        _ = await run_native_build_command(spec, workflow_lock_fd=workflow)
                    finally:
                        finished.set()

            async with anyio.create_task_group() as tasks:
                _ = tasks.start_soon(call)
                with anyio.fail_after(10):
                    await entered.wait()
                    scopes[0].cancel()
                    await finished.wait()
            assert scopes[0].cancel_called and len(paths) == 3
            assert await _held(database) == 0
            assert not (spec.receipt_directory / "start.json").exists()
            for index in (0, 2):
                descriptor = os.open(paths[index], os.O_RDONLY | os.O_CLOEXEC)
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(descriptor)


async def test_explicit_late_proof_cleanup_works_after_manager_context_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class LateProof:
        closed: bool = False

        def owner(self) -> NativeBuildProcess:
            return cast(NativeBuildProcess, cast(object, self))

        async def wait(self):
            raise NativeBuildCleanupError(self.owner())

        async def aclose(self) -> None:
            if not self.closed:
                raise NativeBuildCleanupError(self.owner())

        def is_closed(self) -> bool:
            return self.closed

    process = LateProof()
    paths: list[Path] = []

    async def start(
        _spec: NativeBuildSpec,
        *,
        guard_descriptors: tuple[int, ...],
        startup_timeout_seconds: float,
    ) -> NativeBuildProcess:
        assert startup_timeout_seconds > 0
        paths.extend(
            Path(os.readlink(f"/proc/self/fd/{descriptor}")) for descriptor in guard_descriptors
        )
        return process.owner()

    monkeypatch.setattr(NativeBuildProcess, "start", start)
    manager = None
    try:
        with pytest.raises(NativeBuildCleanupError):
            async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
                manager = current_resource_manager()
                assert manager is not None
                with _workflow(tmp_path) as workflow, pytest.raises(NativeBuildCleanupError):
                    _ = await run_native_build_command(_spec(tmp_path), workflow_lock_fd=workflow)
        assert manager is not None and current_resource_manager() is None
        assert len(paths) == 3
        for path in paths:
            descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
            try:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(descriptor)
        process.closed = True
        await close_native_build_quarantine(manager)
        await close_native_build_quarantine(manager)
        for path in paths:
            descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(descriptor)
    finally:
        process.closed = True
        if manager is not None:
            await close_native_build_quarantine(manager)
