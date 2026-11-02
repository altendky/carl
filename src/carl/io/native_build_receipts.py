"""Private immutable execution receipts, independent of the Carl parent process."""

import hashlib
import json
import os
import secrets
import stat
from contextlib import suppress
from pathlib import Path
from time import time_ns
from typing import Literal, NoReturn

from pydantic import Field, model_validator

from carl.core.models import StrictModel
from carl.core.native_build import NativeBuildResult, NativeBuildSpec

MAX_RECEIPT_BYTES = 65_536
_CHUNK = 1024 * 1024


class NativeBuildReceiptError(ValueError):
    def __init__(self) -> None:
        super().__init__("Native build receipt or log failed integrity validation")


class NativeBuildUncertain(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Native build has an incomplete or mismatched previous execution")


class NativeBuildReceipt(StrictModel):
    schema_version: Literal[1] = 1
    state: Literal["start", "complete"]
    command_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    recipe_identifier: str = Field(min_length=1, max_length=256)
    phase_identifier: str = Field(min_length=1, max_length=128)
    input_identity: str = Field(min_length=1, max_length=256)
    execution_identifier: str = Field(min_length=1, max_length=128)
    started_at_utc_ns: int = Field(ge=0)
    completed_at_utc_ns: int | None = Field(default=None, ge=0)
    result: NativeBuildResult | None = None

    @model_validator(mode="after")
    def validate_completion(self) -> "NativeBuildReceipt":
        if self.state == "start":
            if self.result is not None or self.completed_at_utc_ns is not None:
                raise ValueError("A start receipt cannot describe completion")
        elif (
            self.result is None
            or self.result.execution_identifier != self.execution_identifier
            or self.completed_at_utc_ns is None
            or self.completed_at_utc_ns < self.started_at_utc_ns
        ):
            raise ValueError("A completion receipt must identify its actual execution")
        return self


def command_fingerprint(spec: NativeBuildSpec) -> str:
    """Execution tokens and receipt locations do not change command identity."""
    content = spec.model_dump_json(exclude={"execution_identifier", "receipt_directory"})
    return hashlib.sha256(content.encode()).hexdigest()


def open_receipt_directory(path: Path, *, create: bool = False) -> int:
    """Pin each path component with O_NOFOLLOW and require a private final dir."""
    if not path.is_absolute() or ".." in path.parts or len(path.parts) < 2:
        raise NativeBuildReceiptError
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for component in path.parts[1:]:
            if create:
                with suppress(FileExistsError):
                    os.mkdir(component, mode=0o700, dir_fd=descriptor)
                    os.fsync(descriptor)
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
        info = os.fstat(descriptor)
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise NativeBuildReceiptError
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _require_file(descriptor: int, maximum: int) -> os.stat_result:
    info = os.fstat(descriptor)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o077
        or not 0 <= info.st_size <= maximum
    ):
        raise NativeBuildReceiptError
    return info


def _identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise NativeBuildReceiptError
        value[key] = item
    return value


def _reject_constant(_: str) -> NoReturn:
    raise NativeBuildReceiptError


def _read(directory: int, name: str) -> NativeBuildReceipt | None:
    try:
        descriptor = os.open(
            name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
    except FileNotFoundError:
        return None
    try:
        before = _require_file(descriptor, MAX_RECEIPT_BYTES)
        content = bytearray()
        while len(content) <= before.st_size:
            chunk = os.read(descriptor, before.st_size + 1 - len(content))
            if not chunk:
                break
            content.extend(chunk)
        if len(content) != before.st_size or _identity(os.fstat(descriptor)) != _identity(before):
            raise NativeBuildReceiptError
        _ = json.loads(content, object_pairs_hook=_unique, parse_constant=_reject_constant)
        return NativeBuildReceipt.model_validate_json(content)
    except (ValueError, TypeError, RecursionError):
        raise NativeBuildReceiptError from None
    finally:
        os.close(descriptor)


def _publish(directory: int, name: str, receipt: NativeBuildReceipt) -> None:
    content = receipt.model_dump_json().encode()
    if len(content) > MAX_RECEIPT_BYTES:
        raise NativeBuildReceiptError
    temporary = f".{name}.{secrets.token_hex(16)}.tmp"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
        dir_fd=directory,
    )
    try:
        try:
            remaining = memoryview(content)
            while remaining:
                written = os.write(descriptor, remaining)
                if written <= 0:
                    raise OSError("Native receipt write made no progress")
                remaining = remaining[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.link(temporary, name, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
        os.fsync(directory)
    finally:
        os.unlink(temporary, dir_fd=directory)
        os.fsync(directory)


def _matching(receipt: NativeBuildReceipt, spec: NativeBuildSpec) -> None:
    if receipt.command_fingerprint != command_fingerprint(spec) or (
        receipt.recipe_identifier,
        receipt.phase_identifier,
        receipt.input_identity,
    ) != (spec.recipe_identifier, spec.phase_identifier, spec.input_identity):
        raise NativeBuildUncertain


def read_build_receipt(spec: NativeBuildSpec) -> NativeBuildReceipt | None:
    """A missing complete receipt can never authorize repeating an existing START."""
    directory = open_receipt_directory(spec.receipt_directory)
    try:
        start, complete = _read(directory, "start.json"), _read(directory, "complete.json")
        if start is None:
            if complete is not None:
                raise NativeBuildReceiptError
            return None
        if start.state != "start":
            raise NativeBuildReceiptError
        _matching(start, spec)
        if complete is None:
            raise NativeBuildUncertain
        _matching(complete, spec)
        if (
            complete.state != "complete"
            or complete.execution_identifier != start.execution_identifier
            or complete.started_at_utc_ns != start.started_at_utc_ns
        ):
            raise NativeBuildReceiptError
        return complete
    finally:
        os.close(directory)


def write_start_receipt(spec: NativeBuildSpec) -> NativeBuildReceipt:
    directory = open_receipt_directory(spec.receipt_directory)
    try:
        if (
            _read(directory, "start.json") is not None
            or _read(directory, "complete.json") is not None
        ):
            raise NativeBuildUncertain
        receipt = NativeBuildReceipt(
            state="start",
            command_fingerprint=command_fingerprint(spec),
            recipe_identifier=spec.recipe_identifier,
            phase_identifier=spec.phase_identifier,
            input_identity=spec.input_identity,
            execution_identifier=spec.execution_identifier,
            started_at_utc_ns=time_ns(),
        )
        _publish(directory, "start.json", receipt)
        return receipt
    finally:
        os.close(directory)


def write_complete_receipt(
    spec: NativeBuildSpec, result: NativeBuildResult, *, started_at_utc_ns: int
) -> NativeBuildReceipt:
    directory = open_receipt_directory(spec.receipt_directory)
    try:
        start = _read(directory, "start.json")
        if (
            start is None
            or start.state != "start"
            or start.execution_identifier != spec.execution_identifier
            or start.started_at_utc_ns != started_at_utc_ns
            or result.execution_identifier != spec.execution_identifier
        ):
            raise NativeBuildReceiptError
        _matching(start, spec)
        receipt = NativeBuildReceipt(
            state="complete",
            command_fingerprint=start.command_fingerprint,
            recipe_identifier=start.recipe_identifier,
            phase_identifier=start.phase_identifier,
            input_identity=start.input_identity,
            execution_identifier=start.execution_identifier,
            started_at_utc_ns=start.started_at_utc_ns,
            completed_at_utc_ns=max(start.started_at_utc_ns, time_ns()),
            result=result,
        )
        _publish(directory, "complete.json", receipt)
        return receipt
    finally:
        os.close(directory)


def verify_build_logs(spec: NativeBuildSpec, receipt: NativeBuildReceipt) -> None:
    result = receipt.result
    if (
        receipt.state != "complete"
        or result is None
        or result.stdout_bytes > spec.stdout_limit_bytes
        or result.stderr_bytes > spec.stderr_limit_bytes
        or result.stdout_bytes + result.stderr_bytes > spec.combined_limit_bytes
    ):
        raise NativeBuildReceiptError
    directory = open_receipt_directory(spec.receipt_directory)
    try:
        for name, size, digest in (
            ("stdout.log", result.stdout_bytes, result.stdout_sha256),
            ("stderr.log", result.stderr_bytes, result.stderr_sha256),
        ):
            descriptor = os.open(
                name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
            )
            try:
                before = _require_file(descriptor, size)
                if before.st_size != size:
                    raise NativeBuildReceiptError
                checksum, observed = hashlib.sha256(), 0
                head = bytearray()
                while observed <= size:
                    chunk = os.read(descriptor, min(_CHUNK, size + 1 - observed))
                    if not chunk:
                        break
                    observed += len(chunk)
                    checksum.update(chunk)
                    if name == "stdout.log" and len(head) < 4096:
                        head.extend(chunk[: 4096 - len(head)])
                if (
                    observed != size
                    or checksum.hexdigest() != digest
                    or _identity(os.fstat(descriptor)) != _identity(before)
                ):
                    raise NativeBuildReceiptError
                if (
                    name == "stdout.log"
                    and head.decode("utf-8", errors="replace") != result.stdout_head
                ):
                    raise NativeBuildReceiptError
            finally:
                os.close(descriptor)
    finally:
        os.close(directory)
