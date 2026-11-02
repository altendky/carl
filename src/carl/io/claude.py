"""Bounded, noninteractive Claude Code invocation for saved listing evidence."""

import json
import os
import shutil
import subprocess
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter_ns, time_ns

import anyio

from carl.core.item_analysis import ClaudeEffort
from carl.io.resource_permits import (
    guarded_process_arguments,
    resource_operation,
    terminate_guarded_process,
)

CLAUDE_ANALYSIS_TOOLS = ("Read", "WebSearch", "WebFetch")
CLAUDE_ANALYSIS_MAXIMUM_TURNS = 8


@asynccontextmanager
async def temporary_analysis_directory(root: Path | None = None) -> AsyncIterator[Path]:
    """Own a private per-item directory and remove it after the process exits."""

    if root is None:
        root = Path(os.environ.get("TMPDIR", "/tmp")) / "agents"
    await anyio.to_thread.run_sync(lambda: root.mkdir(parents=True, exist_ok=True))
    await anyio.to_thread.run_sync(root.chmod, 0o700)
    directory = Path(
        await anyio.to_thread.run_sync(lambda: tempfile.mkdtemp(prefix="carl-analysis-", dir=root))
    )
    try:
        yield directory
    finally:
        with anyio.fail_after(30, shield=True):
            await anyio.to_thread.run_sync(shutil.rmtree, directory)


@dataclass(frozen=True, slots=True)
class ClaudeRun:
    argv: tuple[str, ...]
    version: str | None
    started_at_utc_ns: int
    ended_at_utc_ns: int
    duration_ns: int
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    text: str | None
    output_metadata: dict[str, object] | None
    tool_calls: tuple[str, ...]
    failure_kind: str | None


@dataclass(frozen=True, slots=True)
class ClaudeCli:
    executable: str = "claude"
    tools: tuple[str, ...] = CLAUDE_ANALYSIS_TOOLS
    maximum_turns: int = CLAUDE_ANALYSIS_MAXIMUM_TURNS
    maximum_stdout_bytes: int = 8 * 1024 * 1024
    maximum_stderr_bytes: int = 1024 * 1024
    maximum_output_bytes: int = 8 * 1024 * 1024

    def __post_init__(self) -> None:
        if min(self.maximum_stdout_bytes, self.maximum_stderr_bytes, self.maximum_output_bytes) < 1:
            raise ValueError("Claude output byte limits must be positive")

    async def version(self, directory: Path | None = None) -> str:
        exit_code, stdout, _stderr, timed_out, output_limited = await self._capture(
            (self.executable, "--version"), directory, 10
        )
        if timed_out:
            raise TimeoutError("Claude CLI version check timed out")
        if output_limited:
            raise RuntimeError("Claude CLI version output exceeded its byte limit")
        version = stdout.decode("utf-8", errors="replace").strip()
        if exit_code != 0 or not version:
            raise RuntimeError("Claude CLI version check failed")
        return version

    def argv(
        self,
        prompt: str,
        model: str,
        effort: ClaudeEffort,
        maximum_turns: int | None = None,
    ) -> tuple[str, ...]:
        tools = ",".join(self.tools)
        turns = self.maximum_turns if maximum_turns is None else maximum_turns
        return (
            self.executable,
            "--safe-mode",
            "--restricted",
            "-p",
            prompt,
            "--output-format",
            "stream-json",
            "--verbose",
            "--permission-mode",
            "dontAsk",
            "--permission-prompts",
            "none",
            "--tools",
            tools,
            "--allowedTools",
            tools,
            "--no-session-persistence",
            "--max-turns",
            str(turns),
            "--model",
            model,
            "--effort",
            effort.value,
        )

    @resource_operation("analysis")
    async def _capture(
        self, argv: tuple[str, ...], directory: Path | None, timeout_seconds: int
    ) -> tuple[int | None, bytes, bytes, bool, bool]:
        """Capture bounded output while retaining capacity through child cleanup."""

        stdout = bytearray()
        stderr = bytearray()
        timed_out = False
        output_limited = False
        guarded_argv, pass_fds = guarded_process_arguments(
            argv, timeout_seconds, directory=directory
        )
        process = await anyio.open_process(
            guarded_argv,
            cwd=directory,
            stdin=subprocess.DEVNULL,
            pass_fds=pass_fds,
            start_new_session=True,
        )

        async def drain(
            stream: anyio.abc.ByteReceiveStream, output: bytearray, stream_limit: int
        ) -> None:
            nonlocal output_limited
            async for chunk in stream:
                remaining = min(
                    stream_limit - len(output),
                    self.maximum_output_bytes - len(stdout) - len(stderr),
                )
                output.extend(chunk[:remaining])
                if len(chunk) > remaining:
                    output_limited = True
                    tasks.cancel_scope.cancel()
                    return

        try:
            try:
                with anyio.fail_after(timeout_seconds):
                    async with anyio.create_task_group() as tasks:
                        assert process.stdout is not None and process.stderr is not None
                        tasks.start_soon(drain, process.stdout, stdout, self.maximum_stdout_bytes)
                        tasks.start_soon(drain, process.stderr, stderr, self.maximum_stderr_bytes)
                        await process.wait()
            except TimeoutError:
                timed_out = True
        finally:
            with anyio.fail_after(10, shield=True):
                await terminate_guarded_process(process, guarded=bool(pass_fds))
        return (
            None if timed_out or output_limited else process.returncode,
            bytes(stdout),
            bytes(stderr),
            timed_out,
            output_limited,
        )

    async def run(
        self,
        *,
        directory: Path,
        prompt: str,
        model: str,
        effort: ClaudeEffort,
        timeout_seconds: int,
        maximum_turns: int | None = None,
    ) -> ClaudeRun:
        argv = self.argv(prompt, model, effort, maximum_turns)
        started_at_utc_ns = time_ns()
        started_monotonic_ns = perf_counter_ns()
        version: str | None = None
        stdout = b""
        stderr = b""
        exit_code: int | None = None
        text: str | None = None
        output_metadata: dict[str, object] | None = None
        failure_kind: str | None = None
        tool_calls: tuple[str, ...] = ()
        try:
            exit_code, stdout, stderr, timed_out, output_limited = await self._capture(
                argv, directory, timeout_seconds
            )
            if timed_out or output_limited:
                try:
                    partial_output, tool_calls, version = _parse_stream_events(
                        stdout, allow_trailing_partial=True
                    )
                    if partial_output is not None:
                        output_metadata = _output_metadata(partial_output)
                except (UnicodeDecodeError, ValueError):
                    tool_calls = ()
                failure_kind = "claude_output_limit" if output_limited else "claude_timeout"
            else:
                try:
                    version = _parse_stream_version(stdout)
                except (UnicodeDecodeError, ValueError):
                    version = None
                try:
                    output, tool_calls, parsed_version = _parse_stream_output(stdout)
                    if parsed_version != version:
                        raise ValueError("Claude stream version parsing is inconsistent")
                    if version is None and exit_code == 0:
                        raise ValueError("Claude stream has no CLI version")
                    output_metadata = _output_metadata(output)
                    result = output.get("result")
                    if not isinstance(result, str):
                        raise ValueError("Claude output has no text result")
                    maximum_turns_exceeded = (
                        output.get("subtype") == "error_max_turns"
                        or output.get("terminal_reason") == "max_turns"
                    )
                    if maximum_turns_exceeded:
                        failure_kind = "claude_maximum_turns_exceeded"
                    elif exit_code != 0:
                        failure_kind = "claude_process_failure"
                    elif output.get("is_error") is True or not result.strip():
                        failure_kind = "claude_reported_failure"
                    else:
                        text = result
                except (UnicodeDecodeError, ValueError):
                    failure_kind = (
                        "claude_process_failure" if exit_code != 0 else "malformed_claude_output"
                    )
        except TimeoutError:
            failure_kind = "claude_timeout"
        except OSError:
            failure_kind = "claude_launch_failure"
        ended_at_utc_ns = time_ns()
        return ClaudeRun(
            argv=argv,
            version=version,
            started_at_utc_ns=started_at_utc_ns,
            ended_at_utc_ns=ended_at_utc_ns,
            duration_ns=max(0, perf_counter_ns() - started_monotonic_ns),
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            text=text,
            output_metadata=output_metadata,
            tool_calls=tool_calls,
            failure_kind=failure_kind,
        )


def _parse_stream_output(
    stdout: bytes,
) -> tuple[dict[str, object], tuple[str, ...], str | None]:
    """Parse Claude NDJSON, retaining its final result, tools, and self-reported version."""

    final, calls, version = _parse_stream_events(stdout)
    if final is None:
        raise ValueError("Claude stream has no final result")
    return final, calls, version


def _parse_stream_events(
    stdout: bytes,
    *,
    allow_trailing_partial: bool = False,
) -> tuple[dict[str, object] | None, tuple[str, ...], str | None]:
    """Parse all complete NDJSON events, including a stream without a final result."""

    final: dict[str, object] | None = None
    calls: list[str] = []
    tool_use_identifiers: set[str] = set()
    version: str | None = None
    raw_lines = stdout.splitlines(keepends=True)
    for index, raw_line in enumerate(raw_lines):
        if not raw_line.strip():
            continue
        try:
            event = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            if (
                allow_trailing_partial
                and index == len(raw_lines) - 1
                and not raw_line.endswith((b"\n", b"\r"))
            ):
                break
            raise
        if not isinstance(event, dict):
            raise ValueError("Claude stream event is not an object")
        if event.get("type") == "result" or (
            "result" in event and isinstance(event.get("result"), str)
        ):
            final = event
        if event.get("type") == "system" and event.get("subtype") == "init":
            reported_version = event.get("claude_code_version")
            if not isinstance(reported_version, str) or not reported_version:
                raise ValueError("Claude init event has no CLI version")
            if version is not None and version != reported_version:
                raise ValueError("Claude stream reports conflicting CLI versions")
            version = reported_version
        message = event.get("message")
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "tool_use":
                continue
            name = block.get("name")
            identifier = block.get("id")
            if not isinstance(name, str):
                continue
            if isinstance(identifier, str):
                if identifier in tool_use_identifiers:
                    continue
                tool_use_identifiers.add(identifier)
            calls.append(name)
    return final, tuple(calls), version


def _output_metadata(output: dict[str, object]) -> dict[str, object]:
    return {
        key: value
        for key, value in output.items()
        if key
        in {
            "session_id",
            "total_cost_usd",
            "usage",
            "modelUsage",
            "is_error",
            "num_turns",
            "terminal_reason",
            "subtype",
            "errors",
            "stop_reason",
        }
    }


def _parse_stream_version(stdout: bytes) -> str | None:
    """Read the CLI version emitted by the exact Claude process invocation."""

    version: str | None = None
    for raw_line in stdout.decode("utf-8").splitlines():
        if not raw_line.strip():
            continue
        event = json.loads(raw_line)
        if not isinstance(event, dict):
            raise ValueError("Claude stream event is not an object")
        if event.get("type") != "system" or event.get("subtype") != "init":
            continue
        reported_version = event.get("claude_code_version")
        if not isinstance(reported_version, str) or not reported_version:
            raise ValueError("Claude init event has no CLI version")
        if version is not None and version != reported_version:
            raise ValueError("Claude stream reports conflicting CLI versions")
        version = reported_version
    return version
