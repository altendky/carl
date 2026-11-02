"""Subprocess output has byte budgets and reaching one reaps the child."""

import json
import os
import signal
import sys
from contextlib import suppress
from dataclasses import replace
from pathlib import Path

import anyio
import pytest

from carl.core.item_analysis import ClaudeEffort
from carl.core.resources import ResourceRequest
from carl.io.claude import ClaudeCli
from carl.io.resource_permits import current_resource_manager
from carl.io.sqlite import Database


def _noisy_cli(path: Path, *, stream: str, version_only: bool = False) -> None:
    path.write_text(
        f"#!{sys.executable}\n"
        "import json, os\n"
        "from pathlib import Path\n"
        "Path('child.pid').write_text(str(os.getpid()))\n"
        "print(json.dumps({'type': 'system', 'subtype': 'init', "
        "'claude_code_version': 'fixture-version'}), flush=True)\n"
        "print(json.dumps({'type': 'assistant', 'message': {'content': "
        "[{'type': 'tool_use', 'id': 'read-1', 'name': 'Read'}]}}), flush=True)\n"
        + ("assert '--version' in __import__('sys').argv\n" if version_only else "")
        + "while True:\n"
        + ("    os.write(1, b'x' * 1024)\n" if stream in {"stdout", "combined"} else "")
        + ("    os.write(2, b'e' * 1024)\n" if stream in {"stderr", "combined"} else "")
    )
    path.chmod(0o700)


def _assert_child_reaped(directory: Path) -> None:
    pid = int((directory / "child.pid").read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.anyio
@pytest.mark.parametrize("stream", ("stdout", "stderr", "combined"))
async def test_output_overflow_keeps_bounded_evidence_and_reaps_child(
    tmp_path: Path, stream: str
) -> None:
    executable = tmp_path / "noisy-claude"
    _noisy_cli(executable, stream=stream)
    cli = ClaudeCli(
        str(executable),
        maximum_stdout_bytes=2048 if stream == "combined" else 1024,
        maximum_stderr_bytes=2048 if stream == "combined" else 1024,
        maximum_output_bytes=1500 if stream == "combined" else 4096,
    )
    with anyio.fail_after(3):
        run = await cli.run(
            directory=tmp_path,
            prompt="inspect retained evidence",
            model="fixture-model",
            effort=ClaudeEffort.MEDIUM,
            timeout_seconds=30,
        )
    assert run.failure_kind == "claude_output_limit"
    assert run.exit_code is None and run.text is None
    assert len(run.stdout) <= cli.maximum_stdout_bytes
    assert len(run.stderr) <= cli.maximum_stderr_bytes
    assert len(run.stdout) + len(run.stderr) <= cli.maximum_output_bytes
    if stream == "combined":
        assert len(run.stdout) + len(run.stderr) == cli.maximum_output_bytes
    else:
        assert len(run.stdout if stream == "stdout" else run.stderr) == 1024
    # Separate OS pipes have no shared delivery order: stderr may consume the
    # combined budget before stdout's initial events are received. Metadata must
    # reflect the complete events actually retained within that budget.
    events = [json.loads(line) for line in run.stdout.split(b"\n")[:-1] if line]
    assert run.version == (
        "fixture-version" if any(event.get("type") == "system" for event in events) else None
    )
    assert run.tool_calls == (
        ("Read",) if any(event.get("type") == "assistant" for event in events) else ()
    )
    _assert_child_reaped(tmp_path)


@pytest.mark.anyio
async def test_version_check_uses_bounded_capture_and_reaps_noisy_process(tmp_path: Path) -> None:
    executable = tmp_path / "noisy-version"
    _noisy_cli(executable, stream="stdout", version_only=True)
    cli = ClaudeCli(str(executable), maximum_output_bytes=1024, maximum_stdout_bytes=1024)
    with anyio.fail_after(3):
        with pytest.raises(RuntimeError, match="output exceeded its byte limit"):
            await cli.version(tmp_path)
    _assert_child_reaped(tmp_path)


@pytest.mark.anyio
async def test_exact_output_limit_is_successful(tmp_path: Path) -> None:
    executable = tmp_path / "normal-claude"
    stdout = (
        b'{"type":"system","subtype":"init","claude_code_version":"fixture-version"}\n'
        b'{"type":"result","result":"retained report","is_error":false}\n'
    )
    executable.write_text(f"#!{sys.executable}\nimport os\nos.write(1, {stdout!r})\n")
    executable.chmod(0o700)
    cli = ClaudeCli(
        str(executable), maximum_stdout_bytes=len(stdout), maximum_output_bytes=len(stdout)
    )
    run = await cli.run(
        directory=tmp_path,
        prompt="inspect",
        model="fixture-model",
        effort=ClaudeEffort.MEDIUM,
        timeout_seconds=3,
    )
    assert run.failure_kind is None and run.text == "retained report"
    assert run.stdout == stdout and run.stderr == b""


@pytest.mark.anyio
@pytest.mark.parametrize("relative_executable", (False, True))
async def test_exited_cli_cleans_up_grandchild_before_releasing_analysis_capacity(
    tmp_path: Path,
    relative_executable: bool,
) -> None:
    executable = tmp_path / "forking-claude"
    grandchild_code = (
        "import os,time; from pathlib import Path; "
        "Path('grandchild.pid').write_text(str(os.getpid())); time.sleep(30)"
    )
    executable.write_text(
        f"#!{sys.executable}\n"
        "import os, subprocess, sys, time\n"
        "from pathlib import Path\n"
        "Path('child.pid').write_text(str(os.getpid()))\n"
        f"subprocess.Popen((sys.executable, '-c', {grandchild_code!r}))\n"
        "while not Path('grandchild.pid').exists(): time.sleep(0.01)\n"
        'print(\'{"type":"system","subtype":"init",'
        '"claude_code_version":"fixture-version"}\', flush=True)\n'
        'print(\'{"type":"result","result":"retained report",'
        '"is_error":false}\', flush=True)\n'
    )
    executable.chmod(0o700)
    try:
        async with Database.managed(
            tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"analysis": 1}
        ):
            with anyio.fail_after(3):
                run = await ClaudeCli(
                    f"./{executable.name}" if relative_executable else str(executable)
                ).run(
                    directory=tmp_path,
                    prompt="inspect",
                    model="fixture-model",
                    effort=ClaudeEffort.MEDIUM,
                    timeout_seconds=30,
                )
            assert run.failure_kind is None and run.text == "retained report"
            _assert_child_reaped(tmp_path)
            grandchild_pid = int((tmp_path / "grandchild.pid").read_text())
            # An orphaned zombie can remain until the host's init reaps it; it
            # has stopped executing and cannot retain an execution guard.
            with anyio.fail_after(2):
                while True:
                    try:
                        state = (
                            Path(f"/proc/{grandchild_pid}/stat").read_text().split(") ", 1)[1][0]
                        )
                    except FileNotFoundError:
                        break
                    if state == "Z":
                        break
                    await anyio.sleep(0.01)
            manager = current_resource_manager()
            assert manager is not None
            with anyio.fail_after(2):
                async with manager.admit(ResourceRequest(name="analysis")):
                    pass
    finally:
        for marker in ("child.pid", "grandchild.pid"):
            if (tmp_path / marker).exists():
                with suppress(ProcessLookupError):
                    os.kill(int((tmp_path / marker).read_text()), signal.SIGKILL)


@pytest.mark.parametrize(
    "field", ("maximum_stdout_bytes", "maximum_stderr_bytes", "maximum_output_bytes")
)
def test_output_limits_must_be_positive(field: str) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        _ = replace(ClaudeCli(), **{field: 0})
