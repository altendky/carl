"""Private, bounded MCP failure diagnostics independent of the evidence database."""

import fcntl
import json
import os
import re
import sys
import traceback
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import anyio

from carl.io.paths import user_directories

_SENSITIVE_KEY = (
    r"(?:[\w-]*(?:password|passwd|secret|token|credential|api[_-]?key|"
    r"access[_-]?key|authorization|cookie)[\w-]*|pwd)"
)


def redact_diagnostic(value: str) -> str:
    """Remove common labeled credentials; never collect locals or request payloads."""

    value = re.sub(r"(\w+://)[^\s/@]+@", r"\1[REDACTED]@", value)
    # HTTP headers can contain spaces, commas, and multiple cookies.
    value = re.sub(
        r"(?im)\b((?:proxy-)?authorization|(?:set-)?cookie)(\s*:\s*)[^\r\n]+",
        r"\1\2[REDACTED]",
        value,
    )
    value = re.sub(
        rf"(?i)([\"']?{_SENSITIVE_KEY}[\"']?\s*[:=]\s*)([\"'])((?:\\.|(?!\2).)*?)\2",
        r"\1[REDACTED]",
        value,
        flags=re.DOTALL,
    )
    value = re.sub(
        rf"(?i)([\"']?\b{_SENSITIVE_KEY}[\"']?\s*[:=]\s*)(?!\[REDACTED\])[^\s,;&}}\]\"']+",
        r"\1[REDACTED]",
        value,
    )
    # Pydantic embeds the rejected input in its otherwise useful validation message.
    return re.sub(
        r"input_value=.*?(?=, input_type=)",
        "input_value=[OMITTED]",
        value,
        flags=re.DOTALL,
    )


def diagnostic_traceback(error: BaseException) -> str:
    summary = traceback.TracebackException.from_exception(
        error,
        capture_locals=False,
        lookup_lines=False,
        max_group_width=sys.maxsize,
        max_group_depth=sys.maxsize,
    )

    def omit_source_lines(value: traceback.TracebackException) -> None:
        # Source snippets may themselves contain literal secrets or request inputs.
        value.stack = traceback.StackSummary.from_list(
            [
                traceback.FrameSummary(
                    frame.filename, frame.lineno, frame.name, lookup_line=False, line=""
                )
                for frame in value.stack
            ]
        )
        for child in (value.__cause__, value.__context__, *(value.exceptions or ())):
            if child is not None:
                omit_source_lines(child)

    omit_source_lines(summary)
    return redact_diagnostic("".join(summary.format()))


@dataclass(frozen=True, slots=True)
class McpFailure:
    error_identifier: str
    timestamp_utc: str
    process_identifier: int
    stage: str
    tool_name: str | None
    duration_ns: int | None
    server_version: str
    source_tree_sha256: str | None
    exception_type: str
    traceback: str


def append_failure(
    path: Path,
    failure: McpFailure,
    *,
    maximum_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 3,
) -> None:
    """Serialize rotation across processes using a stable, non-rotating lock file."""

    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    line = (json.dumps(asdict(failure), ensure_ascii=True) + "\n").encode("utf-8")
    flags = os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
    descriptor = os.open(path.with_name(path.name + ".lock"), flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        deadline = monotonic() + 1
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if monotonic() >= deadline:
                    raise TimeoutError("MCP diagnostic log lock unavailable") from None
                sleep(0.01)
        if path.exists() and path.stat().st_size + len(line) > maximum_bytes:
            for index in range(backup_count, 0, -1):
                source = path if index == 1 else path.with_name(f"{path.name}.{index - 1}")
                if source.exists():
                    source.replace(path.with_name(f"{path.name}.{index}"))
        log_descriptor = os.open(path, flags | os.O_APPEND, 0o600)
        with os.fdopen(log_descriptor, "ab") as log:
            os.fchmod(log.fileno(), 0o600)
            log.write(line)
            log.flush()
    finally:
        os.close(descriptor)


async def report_mcp_failure(
    error: Exception,
    *,
    log_path: Path | None = None,
    stage: str = "tool",
    tool_name: str | None = None,
    duration_ns: int | None = None,
    source_tree_sha256: str | None = None,
) -> str:
    """Return the same redacted diagnostic sent to stderr and the persistent log."""

    failure = McpFailure(
        error_identifier=str(uuid4()),
        timestamp_utc=datetime.now(UTC).isoformat(),
        process_identifier=os.getpid(),
        stage=stage,
        tool_name=tool_name,
        duration_ns=duration_ns,
        server_version=version("carl"),
        source_tree_sha256=source_tree_sha256,
        exception_type=type(error).__name__,
        traceback=diagnostic_traceback(error),
    )
    path = log_path if log_path is not None else user_directories().mcp_error_log_file
    log_status = f"Error log: {path}"
    try:
        with anyio.fail_after(2, shield=True):
            await anyio.to_thread.run_sync(
                lambda: append_failure(path, failure), abandon_on_cancel=True
            )
    except Exception as logging_error:
        # A full disk, unavailable lock, or read-only state directory must not mask the failure.
        log_status = f"Error log unavailable: {path} ({type(logging_error).__name__})"
    message = (
        f"Carl encountered an internal error ({failure.error_identifier})\n"
        f"Stage: {stage}; tool: {tool_name or 'n/a'}; exception: {failure.exception_type}\n"
        f"{log_status}\n{failure.traceback}"
    )
    # Stderr may itself be a closed pipe or an exhausted log destination.
    with suppress(OSError, ValueError):
        print(
            f"Carl MCP tool failure {failure.error_identifier}\n{message}",
            file=sys.stderr,
            flush=True,
        )
    return message
