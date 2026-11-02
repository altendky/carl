"""Private filesystem journal and the authoritative cross-database build gate."""

import fcntl
import os
import secrets
import stat
from collections.abc import Generator
from contextlib import contextmanager, suppress
from pathlib import Path

from carl.core.dataset_workflows import (
    MAX_DATASET_JOURNAL_BYTES,
    TERMINAL_DATASET_WORKFLOW_STATES,
    DatasetWorkflowFence,
    DatasetWorkflowJournal,
)
from carl.io.paths import CarlDirectories


class DatasetWorkflowError(Exception):
    """A bounded ownership/journal failure; paths and source data stay private."""


class DatasetWorkflowBusy(DatasetWorkflowError):
    """Another physical owner or nonterminal durable reservation exists."""


class DatasetWorkflowFenceLost(DatasetWorkflowError):
    """A stale generation, revision, task, or lock inode attempted a mutation."""


def _directory(path: Path, *, create: bool = False, private: bool = True) -> int:
    """Walk every component without following symlinks, including existing ancestors."""

    if not path.is_absolute() or ".." in path.parts:
        raise DatasetWorkflowError("Dataset workflow directory must be absolute")
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            if create:
                with suppress(FileExistsError):
                    os.mkdir(part, 0o700, dir_fd=descriptor)
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        info = os.fstat(descriptor)
        if private and (info.st_uid != os.getuid() or info.st_mode & 0o077):
            raise DatasetWorkflowError("Dataset workflow directory must be private and owned")
        result, descriptor = descriptor, -1
        return result
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def private_directory(path: Path) -> None:
    os.close(_directory(path, create=True))


def _read(path: Path, maximum: int) -> bytes | None:
    directory = _directory(path.parent)
    try:
        try:
            fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        except FileNotFoundError:
            return None
    finally:
        os.close(directory)
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
            or info.st_size > maximum
        ):
            raise DatasetWorkflowError("Invalid private dataset journal file")
        data = os.read(fd, maximum + 1)
        if len(data) > maximum:
            raise DatasetWorkflowError("Dataset journal exceeds its bound")
        return data
    finally:
        os.close(fd)


def _write(path: Path, data: bytes) -> None:
    directory = _directory(path.parent)
    name = ".journal-" + secrets.token_hex(16)
    try:
        fd = os.open(
            name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory
        )
        try:
            with os.fdopen(fd, "wb") as stream:
                _ = stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, path.name, src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(name, dir_fd=directory)
    finally:
        os.close(directory)


def workflow_directory(directories: CarlDirectories, identifier: str) -> Path:
    # Callers obtain identifiers from the validated definition or active pointer.
    from carl.core.dataset_workflows import DatasetWorkflowFence

    _ = DatasetWorkflowFence(workflow_identifier=identifier, generation=1, token="0" * 32)
    return directories.travel_data_directory / ".workflows" / identifier


def read_journal(directory: Path) -> DatasetWorkflowJournal | None:
    data = _read(directory / "workflow.json", MAX_DATASET_JOURNAL_BYTES)
    if data is None:
        return None
    try:
        return DatasetWorkflowJournal.model_validate_json(data)
    except ValueError:
        raise DatasetWorkflowError("Invalid dataset workflow journal") from None


def active_journal(directories: CarlDirectories) -> DatasetWorkflowJournal | None:
    data = _read(directories.travel_data_state_directory / "active-workflow.json", 2048)
    if data is None:
        return None
    try:
        identifier = data.decode("ascii")
        directory = workflow_directory(directories, identifier)
        os.close(_directory(directory))
        journal = read_journal(directory)
        if journal is None or journal.definition.workflow_identifier != identifier:
            raise ValueError
        return journal
    except (ValueError, UnicodeError):
        raise DatasetWorkflowError("Invalid active dataset workflow reservation") from None


def activate_journal(directories: CarlDirectories, journal: DatasetWorkflowJournal) -> None:
    _write(
        directories.travel_data_state_directory / "active-workflow.json",
        journal.definition.workflow_identifier.encode("ascii"),
    )


def lock_identity(fd: int) -> tuple[int, int]:
    info = os.fstat(fd)
    return info.st_dev, info.st_ino


@contextmanager
def dataset_workflow_gate(
    directories: CarlDirectories, *, allow_reserved: bool = False
) -> Generator[int]:
    """Acquire before creating tasks or admitting execution; never unlock shared OFDs."""

    private_directory(directories.travel_data_state_directory)
    path = directories.travel_data_state_directory / "build.lock"
    directory = _directory(path.parent)
    try:
        fd = os.open(
            path.name,
            os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK,
            0o600,
            dir_fd=directory,
        )
    finally:
        os.close(directory)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise DatasetWorkflowError("Invalid dataset workflow lock")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise DatasetWorkflowBusy("Another dataset workflow owns the build gate") from None
        journal = active_journal(directories)
        if journal is not None:
            if journal.lock_identity != lock_identity(fd):
                raise DatasetWorkflowFenceLost("Dataset workflow lock inode changed")
            if not allow_reserved and journal.state not in TERMINAL_DATASET_WORKFLOW_STATES:
                raise DatasetWorkflowBusy("A dataset workflow is reserved; explicitly resume it")
        yield fd
    finally:
        os.close(fd)


def write_journal(
    directory: Path,
    journal: DatasetWorkflowJournal,
    *,
    expected_fence: DatasetWorkflowFence | None,
    expected_revision: int | None,
) -> DatasetWorkflowJournal:
    """Compare the immutable fence and revision under the caller's physical gate."""

    previous = read_journal(directory)
    if previous is None:
        if expected_fence is not None or expected_revision is not None:
            raise DatasetWorkflowFenceLost("Dataset workflow journal disappeared")
    elif (
        previous.fence != expected_fence
        or previous.revision != expected_revision
        or previous.definition != journal.definition
        or previous.lock_identity != journal.lock_identity
    ):
        raise DatasetWorkflowFenceLost("Dataset workflow generation or revision changed")
    detached = DatasetWorkflowJournal.model_validate_json(journal.model_dump_json())
    _write(directory / "workflow.json", detached.model_dump_json().encode())
    return detached
