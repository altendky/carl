"""Delete successful workflow scratch only after bounded, anchored validation."""

import os
import re
import stat
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from carl.io.resource_permits import ResourceLeaseLost, current_resource_manager, run_cpu

MAX_DATASET_CLEANUP_ENTRIES = 4096
MAX_DATASET_CLEANUP_DEPTH = 64


class DatasetSetupCleanupError(ValueError):
    def __init__(self) -> None:
        super().__init__("Dataset setup scratch failed cleanup ownership validation")


type _Identity = tuple[int, int, int, int]


def _identity(info: os.stat_result) -> _Identity:
    return info.st_dev, info.st_ino, info.st_mode, info.st_uid


def _private_directory(descriptor: int) -> _Identity:
    info = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise DatasetSetupCleanupError
    return _identity(info)


def _root(directory: Path) -> int:
    if not directory.is_absolute() or ".." in directory.parts or len(directory.parts) < 2:
        raise DatasetSetupCleanupError
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for component in directory.parts[1:]:
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
        _ = _private_directory(descriptor)
        result, descriptor = descriptor, -1
        return result
    finally:
        if descriptor >= 0:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class _Entry:
    parts: tuple[str, ...]
    identity: _Identity
    directory: bool


def _scan(
    parent: int,
    parts: tuple[str, ...],
    entries: list[_Entry],
    directories: dict[tuple[str, ...], _Identity],
) -> None:
    if len(parts) > MAX_DATASET_CLEANUP_DEPTH or len(entries) >= MAX_DATASET_CLEANUP_ENTRIES:
        raise DatasetSetupCleanupError
    before = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
    if before.st_uid != os.getuid():
        raise DatasetSetupCleanupError
    is_directory = stat.S_ISDIR(before.st_mode)
    if not is_directory and not stat.S_ISREG(before.st_mode):
        raise DatasetSetupCleanupError
    entry = _Entry(parts, _identity(before), is_directory)
    entries.append(entry)
    if not is_directory:
        return
    descriptor = os.open(
        parts[-1],
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        dir_fd=parent,
    )
    try:
        if _private_directory(descriptor) != entry.identity:
            raise DatasetSetupCleanupError
        directories[parts] = entry.identity
        with os.scandir(descriptor) as children:
            for child in children:
                _scan(descriptor, (*parts, child.name), entries, directories)
    finally:
        os.close(descriptor)


def _parent(
    root: int,
    parts: tuple[str, ...],
    directories: dict[tuple[str, ...], _Identity],
) -> int:
    descriptor = os.dup(root)
    try:
        if _private_directory(descriptor) != directories[()]:
            raise DatasetSetupCleanupError
        for index, component in enumerate(parts[:-1], start=1):
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
            if _private_directory(descriptor) != directories[parts[:index]]:
                raise DatasetSetupCleanupError
        result, descriptor = descriptor, -1
        return result
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _cleanup(directory: Path, native_attempts: tuple[str, ...]) -> None:
    if (
        type(native_attempts) is not tuple
        or len(native_attempts) > 4096
        or any(type(item) is not str for item in native_attempts)
        or len(set(native_attempts)) != len(native_attempts)
    ):
        raise DatasetSetupCleanupError
    if any(re.fullmatch(r"native-(0|[1-9][0-9]{0,9})", item) is None for item in native_attempts):
        raise DatasetSetupCleanupError
    try:
        root = _root(directory)
        try:
            entries: list[_Entry] = []
            directories: dict[tuple[str, ...], _Identity] = {(): _private_directory(root)}
            targets: list[tuple[str, ...]] = [("downloads",), ("candidates",)]
            targets.extend(
                ("osrm-attempts", attempt, "intermediates") for attempt in native_attempts
            )
            for parts in targets:
                parent = os.dup(root)
                try:
                    missing = False
                    for index, component in enumerate(parts[:-1], start=1):
                        try:
                            child = os.open(
                                component,
                                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                dir_fd=parent,
                            )
                        except FileNotFoundError:
                            missing = True
                            break
                        os.close(parent)
                        parent = child
                        directories[parts[:index]] = _private_directory(parent)
                    if missing:
                        continue
                    try:
                        target = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    if not stat.S_ISDIR(target.st_mode):
                        raise DatasetSetupCleanupError
                    # A vanished descendant after this point is a changing tree,
                    # not an idempotently absent target.
                    _scan(parent, parts, entries, directories)
                finally:
                    os.close(parent)
            # All selected trees have passed before any file is removed.
            for entry in reversed(entries):
                parent = _parent(root, entry.parts, directories)
                try:
                    current = os.stat(entry.parts[-1], dir_fd=parent, follow_symlinks=False)
                    if _identity(current) != entry.identity:
                        raise DatasetSetupCleanupError
                    if entry.directory:
                        os.rmdir(entry.parts[-1], dir_fd=parent)
                    else:
                        os.unlink(entry.parts[-1], dir_fd=parent)
                    os.fsync(parent)
                finally:
                    os.close(parent)
        finally:
            os.close(root)
    except OSError:
        raise DatasetSetupCleanupError from None


async def cleanup_dataset_setup_scratch(
    directory: Path, *, native_attempts: tuple[str, ...] = ()
) -> None:
    """Remove successful scratch, retaining CPU admission until physical completion.

    The caller must hold the workflow's global physical lock and have verified
    the published dataset manifest and completed activation. Failed or cancelled
    workflows retain their files and must not call this helper.
    """
    if current_resource_manager() is None:
        raise ResourceLeaseLost("Dataset cleanup requires its managed resource context")
    await run_cpu(partial(_cleanup, directory, native_attempts))
