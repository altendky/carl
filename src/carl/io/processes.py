"""Best-effort discovery of local processes holding Carl database files open."""

from __future__ import annotations

import os
from pathlib import Path

from carl.core.activity import DatabaseProcessActivity

_PROC_ROOT = Path("/proc")


def _nul_fields(path: Path) -> tuple[str, ...]:
    try:
        content = path.read_bytes()
    except OSError:
        return ()
    return tuple(field.decode(errors="replace") for field in content.split(b"\0") if field)


def _carl_worker_database(command: tuple[str, ...], working_directory: Path | None) -> Path | None:
    """Infer a Carl MCP/worker database from its command line."""

    if command and Path(command[0]).name == "uv":
        return None
    command_index: int | None = None
    for index, argument in enumerate(command[:-1]):
        if Path(argument).name == "carl" and command[index + 1] in {"mcp", "work"}:
            command_index = index + 1
            break
    if command_index is None:
        return None
    arguments = command[command_index + 1 :]
    for index, argument in enumerate(arguments):
        if argument == "--database" and index + 1 < len(arguments):
            selected = Path(arguments[index + 1]).expanduser()
            return (
                working_directory / selected
                if not selected.is_absolute() and working_directory
                else selected
            ).resolve()
        if argument.startswith("--database="):
            selected = Path(argument.partition("=")[2]).expanduser()
            return (
                working_directory / selected
                if not selected.is_absolute() and working_directory
                else selected
            ).resolve()
    return Path.home() / ".local" / "share" / "carl" / "carl.sqlite3"


def database_process_activity(
    database_path: Path,
    current_source_tree_sha256: str | None = None,
    *,
    proc_root: Path = _PROC_ROOT,
    current_process_identifier: int | None = None,
) -> tuple[DatabaseProcessActivity, ...]:
    """Inspect Linux procfs without failing activity reporting on inaccessible peers."""

    current_pid = os.getpid() if current_process_identifier is None else current_process_identifier
    resolved_database = database_path.resolve()
    targets = {
        str(resolved_database): "database",
        f"{resolved_database}-wal": "wal",
        f"{resolved_database}-shm": "shared_memory",
    }
    processes: list[DatabaseProcessActivity] = []
    try:
        entries = tuple(proc_root.iterdir())
    except OSError:
        return ()
    for entry in entries:
        if not entry.name.isdecimal():
            continue
        process_identifier = int(entry.name)
        files: set[str] = set()
        try:
            descriptors = tuple((entry / "fd").iterdir())
        except OSError:
            continue
        for descriptor in descriptors:
            try:
                target = os.readlink(descriptor)
            except OSError:
                continue
            kind = targets.get(target.removesuffix(" (deleted)"))
            if kind is not None:
                files.add(kind)
        try:
            working_directory_path = (entry / "cwd").resolve(strict=True)
        except OSError:
            working_directory_path = None
        command_line = _nul_fields(entry / "cmdline")
        inferred_database = _carl_worker_database(command_line, working_directory_path)
        if not files and inferred_database != resolved_database:
            continue
        if command_line:
            command = " ".join(command_line)
        else:
            try:
                command = (entry / "comm").read_text().strip()
            except (OSError, UnicodeError):
                command = "unknown"
        current = process_identifier == current_pid
        processes.append(
            DatabaseProcessActivity(
                process_identifier=process_identifier,
                command=command,
                working_directory=(
                    None if working_directory_path is None else str(working_directory_path)
                ),
                database_files=tuple(sorted(files)),
                current_process=current,
                source_tree_sha256=current_source_tree_sha256 if current else None,
            )
        )
    return tuple(
        sorted(
            processes, key=lambda process: (not process.current_process, process.process_identifier)
        )
    )
