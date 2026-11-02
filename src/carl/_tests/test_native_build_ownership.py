"""Build guards and receipts compose with real stage fencing and Carl process death."""

# Real physical guards, controlled lease recovery and rollback are the contracts here.
# pyright: reportPrivateUsage=false

import fcntl
import json
import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent
from time import time_ns
from uuid import uuid4

import anyio
import pytest

import carl.io.resource_permits as permits_module
from carl._tests.test_stage_execution import _admit, _seed
from carl.core.native_build import NativeBuildSpec
from carl.core.resources import ResourceRequest
from carl.core.worker import CompletedWork
from carl.io.native_build_receipts import read_build_receipt
from carl.io.native_builds import run_native_build_command
from carl.io.resource_permits import current_resource_manager
from carl.io.sqlite import Database
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _guard(path: Path) -> int:
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _spec(
    root: Path, execution: str, command: str, *arguments: str, threads: int = 4
) -> NativeBuildSpec:
    return NativeBuildSpec(
        recipe_identifier="offline-build-v1",
        phase_identifier="native-phase",
        input_identity="verified-source-fixture",
        execution_identifier=execution,
        argv=(sys.executable, "-c", command, *arguments),
        directory=root,
        receipt_directory=root / "receipts",
        threads=threads,
        timeout_seconds=30,
    )


async def _held(database: Database) -> int:
    async with database._connections.reader() as connection:
        row = await (
            await connection.execute(
                "SELECT coalesce(sum(a.units),0) FROM resource_allocations a "
                "JOIN resource_permits p ON p.token=a.permit_token WHERE p.state='held' AND a.name='cpu'"
            )
        ).fetchone()
    assert row is not None
    return int(row[0])


async def test_actual_command_inherits_stage_and_workflow_guards_before_publication(
    tmp_path: Path,
) -> None:
    workflow_path = tmp_path / "workflow.lock"
    descriptor = _guard(workflow_path)
    command = dedent(r"""
        import fcntl, os, sys
        for path in sys.argv[1:]:
            descriptor = os.open(path, os.O_RDWR)
            try:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass
                else:
                    raise RuntimeError('Actual native command did not inherit its guard')
            finally:
                os.close(descriptor)
        os.write(1, b'native command\n')
    """)
    try:
        async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
            manager = current_resource_manager()
            assert manager is not None
            stage = await _seed(database, resources=(ResourceRequest(name="cpu", units=4),))
            async with _admit(stage_manager(database), stage) as admission:
                execution = admission.execution
                assert execution is not None
                guard_path = manager.guard_directory / (execution.lease.token + ".lock")
                capture = await run_native_build_command(
                    _spec(
                        tmp_path,
                        execution.lease.token,
                        command,
                        str(guard_path),
                        str(workflow_path),
                    ),
                    workflow_lock_fd=descriptor,
                )
                assert capture.result.reason == "completed" and capture.result.returncode == 0
                assert capture.result.stdout_head == "native command\n"
                assert not capture.replayed and await _held(database) == 4
                # Every supervisor/command copy must be gone before this seal.
                await execution.finish(CompletedWork(result={"built": True}))
            assert await _held(database) == 0
    finally:
        os.close(descriptor)


async def test_completed_command_replays_after_stage_publication_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    descriptor = _guard(tmp_path / "workflow.lock")
    marker = tmp_path / "launches"
    command = "import pathlib,sys; p=pathlib.Path(sys.argv[1]); p.write_text(p.read_text()+'launch\\n' if p.exists() else 'launch\\n')"
    try:
        async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
            stage = await _seed(database, resources=(ResourceRequest(name="cpu", units=4),))
            first_execution = ""
            with pytest.raises(OSError, match="publication interrupted"):
                async with _admit(stage_manager(database), stage) as admission:
                    execution = admission.execution
                    assert execution is not None
                    first_execution = execution.lease.token
                    first = await run_native_build_command(
                        _spec(tmp_path, first_execution, command, str(marker)),
                        workflow_lock_fd=descriptor,
                    )
                    assert not first.replayed and marker.read_text() == "launch\n"

                    async def interrupted(**_kwargs: object) -> None:
                        raise OSError("publication interrupted")

                    with monkeypatch.context() as failure:
                        failure.setattr(database, "complete_leased_operation", interrupted)
                        await execution.finish(CompletedWork(result={"built": True}))
            async with _admit(stage_manager(database), stage) as admission:
                execution = admission.execution
                assert execution is not None and execution.lease.token != first_execution
                assert execution.lease.work_lease.attempt == 1
                assert execution.lease.definition.operation_identifier == stage.operation_identifier
                replay = await run_native_build_command(
                    _spec(tmp_path, execution.lease.token, command, str(marker)),
                    workflow_lock_fd=descriptor,
                )
                assert replay.replayed and replay.result.execution_identifier == first_execution
                assert marker.read_text() == "launch\n"
                await execution.finish(CompletedWork(result={"built": True}))
            assert await _held(database) == 0
    finally:
        os.close(descriptor)


async def test_parent_death_reaps_forked_command_and_retains_completion_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, workflow, ready = (
        tmp_path / "carl.sqlite3",
        tmp_path / "workflow.lock",
        tmp_path / "native-pids.json",
    )
    command = dedent("""
        import json,os,pathlib,subprocess,sys,time
        child = subprocess.Popen([sys.executable,'-c','import time; time.sleep(300)'])
        path=pathlib.Path(sys.argv[1]); pending=path.with_suffix('.new')
        pending.write_text(json.dumps([os.getpid(),child.pid])); pending.replace(path)
        time.sleep(300)
    """)
    spec = _spec(tmp_path, str(uuid4()), command, str(ready), threads=3)
    program = dedent("""
        import fcntl,os,sys
        from pathlib import Path
        import anyio
        from carl.core.native_build import NativeBuildSpec
        from carl.io.native_builds import run_native_build_command
        from carl.io.sqlite import Database
        async def main():
            spec=NativeBuildSpec.model_validate_json(sys.argv[2])
            descriptor=os.open(sys.argv[3],os.O_CREAT|os.O_RDWR|os.O_CLOEXEC,0o600)
            fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
            try:
                async with Database.managed(Path(sys.argv[1])) as database:
                    await run_native_build_command(spec,workflow_lock_fd=descriptor)
            finally:
                os.close(descriptor)
        anyio.run(main)
    """)
    async with Database.managed(path, initialize=True) as database:
        manager = current_resource_manager()
        assert manager is not None
        process = await anyio.open_process(
            (sys.executable, "-c", program, str(path), spec.model_dump_json(), str(workflow)),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        pidfds: list[int] = []
        try:
            with anyio.fail_after(20):
                while not ready.exists():
                    if process.returncode is not None:
                        diagnostic = (
                            await process.stderr.receive(4096)
                            if process.stderr is not None
                            else b""
                        )
                        pytest.fail(f"Fixture Carl exited: {diagnostic!r}")
                    await anyio.sleep(0.01)
            pids = json.loads(ready.read_text())
            pidfds = [os.pidfd_open(int(pid)) for pid in pids]
            assert await _held(database) == 3
            process.kill()
            await process.wait()
            with anyio.fail_after(20):
                for pidfd in pidfds:
                    await anyio.wait_readable(pidfd)
                while not (spec.receipt_directory / "complete.json").exists():
                    await anyio.sleep(0.01)
            receipt = read_build_receipt(spec)
            assert receipt is not None and receipt.result is not None
            assert receipt.result.reason == "cancelled"
            with monkeypatch.context() as recovery:
                future = time_ns() + 120_000_000_000
                recovery.setattr(permits_module, "time_ns", lambda: future)
                with anyio.fail_after(20):
                    while await _held(database):
                        await manager._recover()
                        await anyio.sleep(0.01)
            with anyio.fail_after(20):
                while True:
                    try:
                        descriptor = _guard(workflow)
                        break
                    except BlockingIOError:
                        await anyio.sleep(0.01)
            try:
                after = spec.model_copy(
                    update={
                        "execution_identifier": str(uuid4()),
                        "phase_identifier": "after_recovery",
                        "receipt_directory": tmp_path / "after-receipts",
                        "argv": (sys.executable, "-c", "print('recovered')"),
                        "threads": 4,
                    }
                )
                capture = await run_native_build_command(after, workflow_lock_fd=descriptor)
                assert capture.result.reason == "completed"
            finally:
                os.close(descriptor)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
            await process.aclose()
            for pidfd in pidfds:
                os.close(pidfd)
