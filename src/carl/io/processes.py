"""Best-effort discovery of local processes holding Carl database files open."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

from carl.core.activity import DatabaseFileKind, DatabaseProcessActivity, DatabaseProcessSnapshot

_PROC_ROOT = Path("/proc")


def _directory_entries(path: Path) -> Iterator[Path]:
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                yield Path(entry.path)
    except OSError:
        return


def _nul_fields(path: Path) -> tuple[str, ...]:
    try:
        with path.open("rb") as stream:
            content = stream.read(65_536)
    except OSError:
        return ()
    if not content:
        return ()
    return tuple(field.decode(errors="replace") for field in content.split(b"\0", 256)[:256])


def _command_label(command: tuple[str, ...]) -> str:
    """Identify the executable/known role without exposing command arguments."""

    executable = Path(command[0]).name if command else "unknown"
    executable = (
        "".join(
            character for character in executable[:64] if character.isalnum() or character in "._+-"
        )
        or "unknown"
    )
    for index, argument in enumerate(command[:-1]):
        if Path(argument).name == "carl" and command[index + 1] in {"mcp", "work"}:
            return f"{executable} (carl {command[index + 1]})"
    return executable


def _database_path(value: str, working_directory: Path | None) -> Path | None:
    if len(value) > 4096:
        return None
    try:
        selected = Path(value).expanduser()
        return (
            working_directory / selected
            if not selected.is_absolute() and working_directory
            else selected
        ).resolve()
    except (OSError, ValueError, RuntimeError):
        return None


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
            return _database_path(arguments[index + 1], working_directory)
        if argument.startswith("--database="):
            return _database_path(argument.partition("=")[2], working_directory)
    return Path.home() / ".local" / "share" / "carl" / "carl.sqlite3"


def database_process_activity(
    database_path: Path,
    current_source_tree_sha256: str | None = None,
    *,
    proc_root: Path = _PROC_ROOT,
    current_process_identifier: int | None = None,
) -> tuple[DatabaseProcessActivity, ...]:
    """Inspect Linux procfs without failing activity reporting on inaccessible peers."""

    return database_process_snapshot(
        database_path,
        current_source_tree_sha256,
        proc_root=proc_root,
        current_process_identifier=current_process_identifier,
    ).processes


def database_process_snapshot(
    database_path: Path,
    current_source_tree_sha256: str | None = None,
    *,
    proc_root: Path = _PROC_ROOT,
    current_process_identifier: int | None = None,
) -> DatabaseProcessSnapshot:
    """Return at most eight safe process labels and a count of matching peers."""

    current_pid = os.getpid() if current_process_identifier is None else current_process_identifier
    resolved_database = database_path.resolve()
    targets: dict[str, DatabaseFileKind] = {
        str(resolved_database): "database",
        f"{resolved_database}-wal": "wal",
        f"{resolved_database}-shm": "shared_memory",
    }
    processes: list[DatabaseProcessActivity] = []
    total_count = 0
    entries = _directory_entries(proc_root)
    for entry in entries:
        if not entry.name.isdecimal():
            continue
        process_identifier = int(entry.name)
        files: set[DatabaseFileKind] = set()
        try:
            descriptors = _directory_entries(entry / "fd")
            for descriptor in descriptors:
                try:
                    target = os.readlink(descriptor)
                except OSError:
                    continue
                kind = targets.get(target.removesuffix(" (deleted)"))
                if kind is not None:
                    files.add(kind)
        except OSError:
            continue
        try:
            working_directory_path = (entry / "cwd").resolve(strict=True)
        except OSError:
            working_directory_path = None
        command_line = _nul_fields(entry / "cmdline")
        inferred_database = _carl_worker_database(command_line, working_directory_path)
        if not files and inferred_database != resolved_database:
            continue
        if not command_line:
            try:
                with (entry / "comm").open("rb") as stream:
                    command_line = (stream.read(64).decode(errors="replace").strip(),)
            except OSError:
                command_line = ()
        current = process_identifier == current_pid
        total_count += 1
        processes.append(
            DatabaseProcessActivity(
                process_identifier=process_identifier,
                command=_command_label(command_line),
                working_directory=None,
                database_files=tuple(sorted(files)),
                current_process=current,
                source_tree_sha256=current_source_tree_sha256 if current else None,
            )
        )
        processes.sort(
            key=lambda process: (not process.current_process, process.process_identifier)
        )
        del processes[8:]
    return DatabaseProcessSnapshot(
        processes=tuple(processes),
        total_count=total_count,
    )
