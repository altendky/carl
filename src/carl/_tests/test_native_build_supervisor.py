"""Fresh native commands retain guards until bounded logs and subtree cleanup finish."""

# Tests use private process fields only to dispose independently verified fake commands.
# pyright: reportPrivateUsage=false

import fcntl
import json
import os
import select
import signal
import socket
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import anyio
import pytest

import carl.io.native_build_protocol as protocol
import carl.io.native_build_supervisor as supervisor_module
from carl.core.native_build import NativeBuildResult, NativeBuildSpec
from carl.io.native_build_protocol import (
    NativeBuildCleanupError,
    NativeBuildProcess,
    NativeBuildProtocolError,
)
from carl.io.native_build_receipts import (
    NativeBuildReceiptError,
    read_build_receipt,
    verify_build_logs,
)
from carl.io.native_routing_protocol import close_fds

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _guard(path: Path) -> int:
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return descriptor


def _available(path: Path) -> bool:
    descriptor = os.open(path, os.O_RDWR)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False
    finally:
        os.close(descriptor)


def _spec(root: Path, mode: str = "success", **changes: object) -> NativeBuildSpec:
    receipts = root / "receipts"
    receipts.mkdir(mode=0o700)
    value = NativeBuildSpec(
        recipe_identifier="fixture-v1",
        phase_identifier="extract",
        input_identity="fixture-input",
        execution_identifier="fixture-execution",
        directory=root,
        receipt_directory=receipts,
        threads=2,
        timeout_seconds=5,
        argv=(sys.executable, "-m", "carl._tests.native_build_command", mode, str(root)),
    )
    return NativeBuildSpec.model_validate_json(value.model_copy(update=changes).model_dump_json())


async def _start(spec: NativeBuildSpec) -> NativeBuildProcess:
    guards = tuple(
        _guard(spec.directory / name) for name in ("cpu.lock", "workflow.lock", "phase.lock")
    )
    try:
        return await NativeBuildProcess.start(spec, guard_descriptors=guards)
    finally:
        close_fds(guards)


async def _file(path: Path) -> None:
    with anyio.fail_after(5):
        while not path.exists():
            await anyio.sleep(0.01)


def _dead(pid: int) -> bool:
    try:
        descriptor = os.pidfd_open(pid)
    except ProcessLookupError:
        return True
    try:
        return bool(select.select((descriptor,), (), (), 0)[0])
    finally:
        os.close(descriptor)


async def test_completed_command_has_durable_receipts_bounded_logs_and_no_idle_guard(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    owner = await _start(spec)
    try:
        result = await owner.wait()
    finally:
        await owner.aclose()
    assert result.reason == "completed" and result.returncode == 0
    assert result.stdout_head == "fixture-version\n"
    assert (tmp_path / "threads").read_text() == "2"
    receipt = read_build_receipt(spec)
    assert receipt is not None and receipt.result == result
    verify_build_logs(spec, receipt)
    assert owner.is_closed()
    assert all(_available(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock"))


async def test_actual_exec_command_inherits_every_guard_descriptor(tmp_path: Path) -> None:
    spec = _spec(tmp_path, "guards")
    guards = tuple(_guard(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock"))
    expected = [os.fstat(value).st_ino for value in guards]
    spec = spec.model_copy(update={"argv": (*spec.argv, ",".join(str(value) for value in guards))})
    try:
        owner = await NativeBuildProcess.start(spec, guard_descriptors=guards)
    finally:
        close_fds(guards)
    try:
        assert (await owner.wait()).reason == "completed"
        assert json.loads((tmp_path / "inherited-guards").read_text()) == expected
    finally:
        await owner.aclose()


@pytest.mark.parametrize("failure", ("nonzero", "launch"))
async def test_failed_command_returns_a_durable_coherent_result(
    tmp_path: Path, failure: str
) -> None:
    spec = _spec(tmp_path, "fail")
    if failure == "launch":
        spec = spec.model_copy(update={"directory": tmp_path / "missing-directory"})
    # The guard files belong to the workflow even if the command cwd is absent.
    guards = tuple(_guard(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock"))
    try:
        owner = await NativeBuildProcess.start(spec, guard_descriptors=guards)
    finally:
        close_fds(guards)
    try:
        result = await owner.wait()
        assert result.reason == ("exit_nonzero" if failure == "nonzero" else "launch_failure")
        assert result.returncode == (7 if failure == "nonzero" else None)
        receipt = read_build_receipt(spec)
        assert receipt is not None and receipt.result == result
        verify_build_logs(spec, receipt)
        assert all(
            _available(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock")
        )
    finally:
        await owner.aclose()


@pytest.mark.parametrize("mode", ("block", "fork-block", "noisy"))
async def test_deadline_kills_and_subreaps_command_tree_even_with_noisy_pipes(
    tmp_path: Path, mode: str
) -> None:
    spec = _spec(
        tmp_path,
        mode,
        timeout_seconds=0.25,
        on_output_limit="truncate",
        stdout_limit_bytes=8192,
        stderr_limit_bytes=8192,
        combined_limit_bytes=16384,
    )
    owner = await _start(spec)
    try:
        with anyio.fail_after(5):
            result = await owner.wait()
        assert result.reason == "timeout"
        assert owner.command_pid is not None and _dead(owner.command_pid)
        if (tmp_path / "descendant.pid").exists():
            pid = int((tmp_path / "descendant.pid").read_text())
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)
        assert (spec.receipt_directory / "stdout.log").stat().st_size <= 8192
        assert (spec.receipt_directory / "stderr.log").stat().st_size <= 8192
        receipt = read_build_receipt(spec)
        assert receipt is not None
        verify_build_logs(spec, receipt)
    finally:
        await owner.aclose()


async def test_normal_wrapper_exit_still_kills_and_reaps_forked_descendant(tmp_path: Path) -> None:
    spec = _spec(tmp_path, "fork-exit")
    owner = await _start(spec)
    try:
        assert (await owner.wait()).reason == "completed"
        pid = int((tmp_path / "descendant.pid").read_text())
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
        assert all(
            _available(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock")
        )
    finally:
        await owner.aclose()


async def test_adopted_escaped_child_is_cleaned_without_signalling_unrelated_sentinel(
    tmp_path: Path,
) -> None:
    sentinel = await anyio.open_process(
        (sys.executable, "-c", "import time; time.sleep(30)"),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    owner = await _start(_spec(tmp_path, "escape-exit"))
    try:
        result = await owner.wait()
        assert result.reason == "completed"
        escaped = int((tmp_path / "descendant.pid").read_text())
        with pytest.raises(ProcessLookupError):
            os.kill(escaped, 0)
        assert sentinel.returncode is None
        os.kill(sentinel.pid, 0)
        assert all(
            _available(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock")
        )
    finally:
        await owner.aclose()
        if sentinel.returncode is None:
            sentinel.kill()
        await sentinel.aclose()


def test_cleanup_never_signals_numeric_group_after_leader_reap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from typing import cast

    reaped = False
    group_signals: list[int] = []
    polls = iter((0, 1))

    class Leader:
        pid = 12345
        returncode: int | None = None

        def wait(self) -> int:
            nonlocal reaped
            reaped = True
            self.returncode = 0
            return 0

    def group_signal(pid: int, _signal: int) -> None:
        assert not reaped, "A numeric group may have been reused after its leader was reaped"
        group_signals.append(pid)

    def reap(_pid: int, _flags: int) -> tuple[int, int]:
        if next(polls) == 0:
            return (0, 0)
        raise ChildProcessError

    monkeypatch.setattr(supervisor_module.os, "killpg", group_signal)
    monkeypatch.setattr(supervisor_module.os, "waitpid", reap)
    monkeypatch.setattr(supervisor_module, "_terminate_adopted_children", lambda: None)
    assert supervisor_module._cleanup(cast(subprocess.Popen[bytes], cast(object, Leader()))) == 0
    assert group_signals == [12345]


async def test_output_limit_terminates_with_bounded_streams_and_combined_size(
    tmp_path: Path,
) -> None:
    spec = _spec(
        tmp_path,
        "noisy",
        stdout_limit_bytes=2048,
        stderr_limit_bytes=2048,
        combined_limit_bytes=3000,
    )
    owner = await _start(spec)
    try:
        result = await owner.wait()
        assert result.reason == "output_limit" and result.output_truncated
        assert result.stdout_bytes <= 2048 and result.stderr_bytes <= 2048
        assert result.stdout_bytes + result.stderr_bytes <= 3000
        assert _available(tmp_path / "cpu.lock")
    finally:
        await owner.aclose()


async def test_truncate_policy_drains_and_retains_bounded_head_and_tail(tmp_path: Path) -> None:
    spec = _spec(
        tmp_path,
        "finite-noisy",
        stdout_limit_bytes=8192,
        stderr_limit_bytes=4096,
        combined_limit_bytes=12288,
        on_output_limit="truncate",
    )
    owner = await _start(spec)
    try:
        result = await owner.wait()
        assert result.reason == "completed" and result.output_truncated
        content = (spec.receipt_directory / "stdout.log").read_bytes()
        assert (
            len(content) == 8192
            and content.startswith(b"HEAD\n")
            and content.endswith(b"TAIL-FINISHED\n")
        )
        assert result.stdout_head == content[:4096].decode()
        receipt = read_build_receipt(spec)
        assert receipt is not None
        verify_build_logs(spec, receipt)
    finally:
        await owner.aclose()


async def test_cancellation_finishes_subtree_cleanup_and_receipt_before_releasing_guards(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path, "fork-block")
    owner = await _start(spec)
    try:
        await _file(tmp_path / "started")
        with anyio.move_on_after(0.05) as cancelled:
            _ = await owner.wait()
        assert cancelled.cancel_called and owner.is_closed()
        receipt = read_build_receipt(spec)
        assert (
            receipt is not None
            and receipt.result is not None
            and receipt.result.reason == "cancelled"
        )
        assert all(
            _available(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock")
        )
    finally:
        await owner.aclose()


async def test_direct_exec_parent_death_is_not_mistaken_for_complete_subtree_proof(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path, "block")
    owner = await _start(spec)
    await _file(tmp_path / "started")
    assert owner.command_pid is not None
    os.kill(owner.pid, signal.SIGKILL)
    try:
        with pytest.raises(NativeBuildCleanupError) as failure:
            _ = await owner.wait()
        assert failure.value.process is owner and not owner.is_closed()
        with anyio.fail_after(5):
            while not _dead(owner.command_pid):
                await anyio.sleep(0.01)
        assert not _available(tmp_path / "cpu.lock")
    finally:
        # This fixture is known not to fork. Independently prove its only
        # command died before disposing test-owned quarantine handles.
        assert _dead(owner.command_pid)
        close_fds(owner._owned_guards)
        owner._owned_guards = ()
        owner._closed = True
        await owner.aclose()


async def test_complete_receipt_write_failure_never_reports_success_or_loses_cleanup(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path, "receipt-fault")
    spec = spec.model_copy(update={"argv": (*spec.argv, str(spec.receipt_directory))})
    owner = await _start(spec)
    try:
        with pytest.raises(NativeBuildProtocolError):
            _ = await owner.wait()
        assert owner.is_closed() and _available(tmp_path / "cpu.lock")
        with pytest.raises(NativeBuildReceiptError):
            read_build_receipt(spec)
    finally:
        await owner.aclose()


async def test_abrupt_carl_death_leaves_supervisor_to_reap_and_finish_receipt(
    tmp_path: Path,
) -> None:
    parent = await anyio.open_process(
        (sys.executable, "-m", "carl._tests.native_build_parent", str(tmp_path)),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    command = supervisor = None
    try:
        await _file(tmp_path / "started")
        await _file(tmp_path / "supervisor.pid")
        command = int((tmp_path / "command.pid").read_text())
        descendant = int((tmp_path / "descendant.pid").read_text())
        supervisor = int((tmp_path / "supervisor.pid").read_text())
        assert not _available(tmp_path / "workflow.lock")
        parent.kill()
        await parent.wait()
        with anyio.fail_after(5):
            while not (tmp_path / "receipts" / "complete.json").exists() or not _dead(supervisor):
                await anyio.sleep(0.01)
        receipt = json.loads((tmp_path / "receipts" / "complete.json").read_bytes())
        assert receipt["result"]["reason"] == "cancelled"
        with pytest.raises(ProcessLookupError):
            os.kill(command, 0)
        with pytest.raises(ProcessLookupError):
            os.kill(descendant, 0)
        assert all(
            _available(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock")
        )
    finally:
        if parent.returncode is None:
            parent.kill()
        await parent.aclose()
        if command is not None and not _dead(command):
            os.killpg(command, signal.SIGKILL)
        if supervisor is not None and not _dead(supervisor):
            os.kill(supervisor, signal.SIGTERM)


async def test_startup_cancellation_keeps_spawn_handle_until_guard_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path, "block")
    original = anyio.open_process
    spawned: list[anyio.abc.Process] = []

    async def delayed(command: Sequence[str], **kwargs: Any) -> anyio.abc.Process:
        process = await original(command, **kwargs)
        spawned.append(process)
        await anyio.sleep(0.1)
        return process

    monkeypatch.setattr(anyio, "open_process", delayed)
    guards = tuple(_guard(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock"))
    try:
        with anyio.move_on_after(0.02) as cancelled:
            owner = await NativeBuildProcess.start(spec, guard_descriptors=guards)
            await owner.aclose()
            pytest.fail("Cancellation must propagate after bounded startup cleanup")
        assert cancelled.cancel_called
        assert len(spawned) == 1 and spawned[0].returncode == 0
    finally:
        close_fds(guards)
    assert all(_available(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock"))


async def test_unconfirmed_startup_exposes_owner_and_keeps_guard_copies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path, "block")
    close = NativeBuildProcess.aclose

    async def failed_receive(_connection: object) -> dict[str, object]:
        raise NativeBuildProtocolError

    async def failed_close(self: NativeBuildProcess) -> None:
        raise NativeBuildCleanupError(self)

    monkeypatch.setattr(protocol, "_receive", failed_receive)
    monkeypatch.setattr(NativeBuildProcess, "aclose", failed_close)
    guards = tuple(_guard(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock"))
    try:
        with pytest.raises(NativeBuildCleanupError) as failure:
            await NativeBuildProcess.start(spec, guard_descriptors=guards)
    finally:
        close_fds(guards)
    owner = failure.value.process
    assert owner is not None and not owner.is_closed()
    assert not _available(tmp_path / "cpu.lock")
    await close(owner)
    assert owner.is_closed() and _available(tmp_path / "cpu.lock")


@pytest.mark.parametrize("failure", ("socketpair", "setblocking"))
async def test_socket_setup_failure_closes_all_guard_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    spec = _spec(tmp_path)
    original = socket.socketpair

    class BrokenSocket:
        def __init__(self, connection: socket.socket) -> None:
            self.connection = connection

        def setblocking(self, _blocking: bool) -> None:
            raise OSError("offline setblocking failure")

        def close(self) -> None:
            self.connection.close()

    def failed_socketpair(*args: Any, **kwargs: Any) -> tuple[socket.socket, socket.socket]:
        if failure == "socketpair":
            raise OSError("offline descriptor exhaustion")
        parent, child = original(*args, **kwargs)
        return cast(socket.socket, cast(object, BrokenSocket(parent))), child

    monkeypatch.setattr(protocol.socket, "socketpair", failed_socketpair)
    guards = tuple(_guard(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock"))
    try:
        with pytest.raises(OSError):
            await NativeBuildProcess.start(spec, guard_descriptors=guards)
    finally:
        close_fds(guards)
    assert all(_available(tmp_path / name) for name in ("cpu.lock", "workflow.lock", "phase.lock"))


def test_result_exit_codes_cannot_forge_success() -> None:
    common = dict(
        execution_identifier="fixture",
        stdout_head="",
        stdout_bytes=0,
        stderr_bytes=0,
        stdout_sha256="0" * 64,
        stderr_sha256="0" * 64,
    )
    with pytest.raises(ValueError):
        _ = NativeBuildResult.model_validate({**common, "reason": "completed", "returncode": 7})
    with pytest.raises(ValueError):
        _ = NativeBuildResult.model_validate({**common, "reason": "exit_nonzero", "returncode": 0})
