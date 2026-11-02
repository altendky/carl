"""Private immutable JSON metadata, addressed by its exact UTF-8 digest."""

import hashlib
import json
import os
import secrets
import stat
from contextlib import suppress
from pathlib import Path
from typing import NoReturn

from carl.core.dataset_setup import (
    MAX_DATASET_SETUP_STATE_BYTES,
    MAX_DATASET_SETUP_VALUE_BYTES,
    DatasetSetupReference,
    DatasetSetupState,
)
from carl.core.json import encode_json, validate_json
from carl.core.models import JsonValue


class DatasetSetupFileError(ValueError):
    def __init__(self) -> None:
        super().__init__("Dataset setup metadata failed storage or integrity validation")


def _maximum(maximum_bytes: int) -> None:
    if type(maximum_bytes) is not int or not 1 <= maximum_bytes <= MAX_DATASET_SETUP_VALUE_BYTES:
        raise DatasetSetupFileError


def _private(descriptor: int) -> None:
    info = os.fstat(descriptor)
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise DatasetSetupFileError


def _bucket(directory: Path, *, create: bool) -> int:
    if not directory.is_absolute() or ".." in directory.parts or len(directory.parts) < 2:
        raise DatasetSetupFileError
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for index, part in enumerate((*directory.parts[1:], "setup-values"), start=1):
            if create:
                with suppress(FileExistsError):
                    os.mkdir(part, mode=0o700, dir_fd=descriptor)
                    os.fsync(descriptor)
            child = os.open(
                part,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
            if index == len(directory.parts) - 1:
                _private(descriptor)
        _private(descriptor)
        result, descriptor = descriptor, -1
        return result
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise DatasetSetupFileError
        result[key] = value
    return result


def _nonfinite(_value: str) -> NoReturn:
    raise DatasetSetupFileError


def _identity(info: os.stat_result) -> tuple[int, int, int, int]:
    # Publication exposes a complete hardlink before removing its temporary
    # name. That legitimate link-count change updates ctime during other reads.
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def _read(bucket: int, reference: DatasetSetupReference, maximum_bytes: int) -> JsonValue:
    if reference.bytes > maximum_bytes:
        raise DatasetSetupFileError
    descriptor = os.open(
        f"{reference.sha256}.json",
        os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
        dir_fd=bucket,
    )
    try:
        before = os.fstat(descriptor)
        _private(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size != reference.bytes:
            raise DatasetSetupFileError
        content = bytearray()
        while len(content) <= reference.bytes:
            chunk = os.read(descriptor, min(65_536, reference.bytes + 1 - len(content)))
            if not chunk:
                break
            content.extend(chunk)
        _private(descriptor)
        if (
            len(content) != reference.bytes
            or hashlib.sha256(content).hexdigest() != reference.sha256
            or _identity(os.fstat(descriptor)) != _identity(before)
        ):
            raise DatasetSetupFileError
        return validate_json(
            json.loads(
                content.decode("utf-8"), object_pairs_hook=_unique, parse_constant=_nonfinite
            )
        )
    finally:
        os.close(descriptor)


def write_setup_value(
    directory: Path, value: JsonValue, *, maximum_bytes: int = MAX_DATASET_SETUP_VALUE_BYTES
) -> DatasetSetupReference:
    """Publish immutable bounded JSON; an existing digest must pass full validation."""
    _maximum(maximum_bytes)
    try:
        content = encode_json(value).encode()
        if len(content) > maximum_bytes:
            raise DatasetSetupFileError
        reference = DatasetSetupReference(
            sha256=hashlib.sha256(content).hexdigest(), bytes=len(content)
        )
        bucket = _bucket(directory, create=True)
        try:
            try:
                _ = _read(bucket, reference, maximum_bytes)
                return reference
            except FileNotFoundError:
                pass
            temporary = ".setup-" + secrets.token_hex(16)
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
                dir_fd=bucket,
            )
            try:
                try:
                    remaining = memoryview(content)
                    while remaining:
                        written = os.write(descriptor, remaining)
                        if written <= 0:
                            raise DatasetSetupFileError
                        remaining = remaining[written:]
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                try:
                    os.link(
                        temporary,
                        f"{reference.sha256}.json",
                        src_dir_fd=bucket,
                        dst_dir_fd=bucket,
                        follow_symlinks=False,
                    )
                except FileExistsError:
                    _ = _read(bucket, reference, maximum_bytes)
                os.fsync(bucket)
            finally:
                os.unlink(temporary, dir_fd=bucket)
                os.fsync(bucket)
            return reference
        finally:
            os.close(bucket)
    except (OSError, ValueError, TypeError, RecursionError):
        raise DatasetSetupFileError from None


def read_setup_value(
    directory: Path,
    reference: DatasetSetupReference,
    *,
    maximum_bytes: int = MAX_DATASET_SETUP_VALUE_BYTES,
) -> JsonValue:
    """Check declared/actual bounds before reading, then validate exact bytes and JSON."""
    _maximum(maximum_bytes)
    try:
        reference = DatasetSetupReference.model_validate_json(reference.model_dump_json())
        if reference.bytes > maximum_bytes:
            raise DatasetSetupFileError
        bucket = _bucket(directory, create=False)
        try:
            return _read(bucket, reference, maximum_bytes)
        finally:
            os.close(bucket)
    except (OSError, ValueError, TypeError, RecursionError):
        raise DatasetSetupFileError from None


def write_setup_state(directory: Path, state: DatasetSetupState) -> DatasetSetupReference:
    try:
        state = DatasetSetupState.model_validate(state.model_dump())
        return write_setup_value(
            directory, state.model_dump(mode="json"), maximum_bytes=MAX_DATASET_SETUP_STATE_BYTES
        )
    except (ValueError, TypeError, RecursionError):
        raise DatasetSetupFileError from None


def read_setup_state(directory: Path, reference: DatasetSetupReference) -> DatasetSetupState:
    try:
        value = read_setup_value(directory, reference, maximum_bytes=MAX_DATASET_SETUP_STATE_BYTES)
        return DatasetSetupState.model_validate_json(encode_json(value))
    except (ValueError, TypeError, RecursionError):
        raise DatasetSetupFileError from None
