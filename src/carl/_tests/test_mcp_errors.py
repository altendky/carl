"""Persistent diagnostics stay private, bounded, and independent of server health."""

import json
import multiprocessing
import os
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel, ValidationError

from carl.io.mcp_errors import (
    McpFailure,
    append_failure,
    diagnostic_traceback,
    redact_diagnostic,
    report_mcp_failure,
)
from carl.io.sqlite import Database
from carl.mcp_server import _DatabaseReadiness, _expected, _instrument_tool, serve_stdio


def _failure(identifier: str) -> McpFailure:
    return McpFailure(
        error_identifier=identifier,
        timestamp_utc="2026-09-30T00:00:00+00:00",
        process_identifier=os.getpid(),
        stage="tool",
        tool_name="test_tool",
        duration_ns=1,
        server_version="test",
        source_tree_sha256=None,
        exception_type="RuntimeError",
        traceback="RuntimeError: test failure\n",
    )


@pytest.mark.parametrize(
    ("value", "secret"),
    [
        ("https://username:url-password@proxy.example/path", "url-password"),
        ("Authorization: Bearer auth-secret\nnext", "auth-secret"),
        ("Proxy-Authorization: Basic proxy-secret", "proxy-secret"),
        ("Cookie: session=cookie-secret; second=another\nnext", "cookie-secret"),
        ('{"password": "json-secret", "query": "public"}', "json-secret"),
        ("{'api_key': 'key-secret'}", "key-secret"),
        ("https://example.test/?access_token=query-secret&other=public", "query-secret"),
        ("password=bare-secret other=public", "bare-secret"),
        ('{"password": "escaped\\"private-tail"}', "private-tail"),
        ('{"access_token": 123456789}', "123456789"),
    ],
)
def test_redact_diagnostic(value: str, secret: str) -> None:
    result = redact_diagnostic(value)
    assert secret not in result
    assert "[REDACTED]" in result


def test_traceback_preserves_chains_and_groups_without_source_or_locals() -> None:
    private_local = "must-not-appear-in-traceback"
    try:
        try:
            raise ValueError("password=hidden-password")
        except ValueError as error:
            raise ExceptionGroup("group failure", [RuntimeError("public detail"), error]) from error
    except ExceptionGroup as error:
        text = diagnostic_traceback(error)
    assert private_local not in text
    assert "hidden-password" not in text
    assert "raise ValueError" not in text
    assert "ExceptionGroup: group failure" in text
    assert "RuntimeError: public detail" in text
    assert "direct cause" in text
    assert "test_traceback_preserves_chains_and_groups_without_source_or_locals" in text


def test_validation_traceback_omits_rejected_input() -> None:
    class Input(BaseModel):
        number: int

    with pytest.raises(ValidationError) as raised:
        Input.model_validate({"number": "unlabeled-private-input"})
    text = diagnostic_traceback(raised.value)
    assert "unlabeled-private-input" not in text
    assert "input_value=[OMITTED]" in text
    assert "number" in text


def test_full_exception_group_is_not_limited_to_fifteen_children() -> None:
    text = diagnostic_traceback(
        ExceptionGroup("many errors", [RuntimeError(f"child-{index}") for index in range(20)])
    )
    assert "RuntimeError: child-19" in text
    assert "and 5 more exceptions" not in text


def test_rotation_and_private_permissions(tmp_path: Path) -> None:
    path = tmp_path / "state" / "mcp-errors.jsonl"
    for index in range(6):
        append_failure(path, _failure(str(index)), maximum_bytes=1, backup_count=2)
    assert json.loads(path.read_text())["error_identifier"] == "5"
    assert json.loads(path.with_name(path.name + ".1").read_text())["error_identifier"] == "4"
    assert json.loads(path.with_name(path.name + ".2").read_text())["error_identifier"] == "3"
    assert not path.with_name(path.name + ".3").exists()
    for file in path.parent.iterdir():
        assert file.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700


def _write_failures(path: Path, prefix: str) -> None:
    for index in range(12):
        append_failure(path, _failure(f"{prefix}-{index}"), maximum_bytes=1000, backup_count=30)


def test_multiple_processes_rotate_and_append_complete_records(tmp_path: Path) -> None:
    path = tmp_path / "mcp-errors.jsonl"
    context = multiprocessing.get_context("spawn")
    processes = [context.Process(target=_write_failures, args=(path, str(i))) for i in range(3)]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=15)
            assert process.exitcode == 0
    finally:
        for process in processes:
            if process.is_alive():
                process.kill()
                process.join()
    entries = [
        json.loads(line)
        for file in path.parent.glob("mcp-errors.jsonl*")
        if not file.name.endswith(".lock")
        for line in file.read_text().splitlines()
    ]
    assert {entry["error_identifier"] for entry in entries} == {
        f"{prefix}-{index}" for prefix in range(3) for index in range(12)
    }
    assert len(entries) == 36


@pytest.mark.anyio
async def test_logging_failure_keeps_original_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    parent = tmp_path / "not-a-directory"
    parent.touch()
    message = await report_mcp_failure(
        RuntimeError("original error password=hidden"), log_path=parent / "mcp-errors.jsonl"
    )
    assert "RuntimeError: original error" in message
    assert "Error log unavailable" in message
    assert "hidden" not in message
    stderr = capsys.readouterr().err
    assert message in stderr
    assert "hidden" not in stderr


@pytest.mark.anyio
async def test_broken_stderr_keeps_original_tool_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class BrokenStderr:
        def write(self, value: str) -> int:
            raise BrokenPipeError("stderr closed")

        def flush(self) -> None:
            raise BrokenPipeError("stderr closed")

    async def fail() -> None:
        raise RuntimeError("original tool failure")

    monkeypatch.setattr("carl.io.mcp_errors.sys.stderr", BrokenStderr())
    with pytest.raises(ToolError, match="RuntimeError: original tool failure"):
        await _expected(fail, error_log_path=tmp_path / "mcp-errors.jsonl")


@pytest.mark.anyio
async def test_redaction_matches_log_stderr_and_tool_response(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    async def fail() -> None:
        raise RuntimeError('credentials {"password": "escaped\\"private-tail"}')

    path = tmp_path / "mcp-errors.jsonl"
    with pytest.raises(ToolError) as raised:
        await _expected(fail, error_log_path=path)
    response = str(raised.value)
    entry = json.loads(path.read_text())
    stderr = capsys.readouterr().err
    assert entry["traceback"] in response
    assert entry["traceback"] in stderr
    assert entry["error_identifier"] in response
    for destination in (response, stderr, entry["traceback"]):
        assert "private-tail" not in destination
        assert "[REDACTED]" in destination


@pytest.mark.anyio
async def test_concurrent_failures_keep_tool_names(tmp_path: Path) -> None:
    import anyio

    path = tmp_path / "mcp-errors.jsonl"
    started = anyio.Event()
    release = anyio.Event()

    async def slow() -> None:
        started.set()
        await release.wait()
        raise RuntimeError("slow error")

    async def fast() -> None:
        raise RuntimeError("fast error")

    async def invoke(function: Callable[[], Awaitable[None]]) -> None:
        with pytest.raises(ToolError):
            await function()

    async def run_slow() -> None:
        await _expected(slow, error_log_path=path)

    async def run_fast() -> None:
        await _expected(fast, error_log_path=path)

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(invoke, _instrument_tool(run_slow, "slow_tool"))
        await started.wait()
        await invoke(_instrument_tool(run_fast, "fast_tool"))
        release.set()
    entries = [json.loads(line) for line in path.read_text().splitlines()]
    assert [entry["tool_name"] for entry in entries] == ["fast_tool", "slow_tool"]
    assert all(entry["duration_ns"] > 0 for entry in entries)


@pytest.mark.anyio
async def test_preparation_failure_is_logged_once_and_returned_to_caller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fail_migration(self: Database) -> None:
        raise RuntimeError("migration failure")

    path = tmp_path / "mcp-errors.jsonl"
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        monkeypatch.setattr(Database, "migrate", fail_migration)
        readiness = _DatabaseReadiness(path)
        await readiness.prepare(database)
        with pytest.raises(ToolError, match="RuntimeError: migration failure") as raised:
            await _expected(readiness.wait, error_log_path=path)
    entries = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(entries) == 1
    assert entries[0]["stage"] == "database_preparation"
    assert entries[0]["error_identifier"] in str(raised.value)


@pytest.mark.anyio
async def test_server_startup_failure_is_logged(tmp_path: Path) -> None:
    path = tmp_path / "mcp-errors.jsonl"
    with pytest.raises(FileNotFoundError):
        await serve_stdio(
            tmp_path / "missing.sqlite3", Path(__file__).resolve().parents[3], error_log_path=path
        )
    entry = json.loads(path.read_text())
    assert entry["stage"] == "server"
    assert entry["exception_type"] == "FileNotFoundError"
