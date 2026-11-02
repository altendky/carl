"""CPU verification and foreground composition of streamed dataset files."""

import hashlib
import json
import os
import re
import stat
from functools import partial
from pathlib import Path
from typing import NoReturn

import httpx

from carl.core.dataset_downloads import (
    DatasetFileError,
    DatasetPreparation,
    DatasetSource,
    DatasetTransfer,
    FileReceipt,
    dataset_source_fingerprint,
    validate_dataset_execution_identifier,
)
from carl.io.resource_permits import ResourceLeaseLost, current_resource_manager, run_cpu
from carl.io.streaming_http import (
    DATASET_CHUNK_BYTES,
    open_dataset_bucket,
    open_dataset_file,
    stream_dataset_source,
)


def _identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate dataset metadata field")
        value[key] = item
    return value


def _reject_constant(_: str) -> NoReturn:
    raise ValueError("Nonfinite dataset metadata")


def _read_preparation(directory: Path, locator: str) -> DatasetPreparation | None:
    try:
        descriptor = open_dataset_file(directory, locator)
    except FileNotFoundError:
        return None
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) & 0o077
            or not 0 < before.st_size <= 16_384
        ):
            raise DatasetFileError("dataset_transfer_metadata_invalid")
        content = bytearray()
        while len(content) <= before.st_size:
            chunk = os.read(descriptor, before.st_size + 1 - len(content))
            if not chunk:
                break
            content.extend(chunk)
        if len(content) != before.st_size or _identity(os.fstat(descriptor)) != _identity(before):
            raise DatasetFileError("dataset_transfer_metadata_invalid")
        _ = json.loads(content, object_pairs_hook=_unique, parse_constant=_reject_constant)
        return DatasetPreparation.model_validate_json(content)
    except (ValueError, TypeError, RecursionError):
        raise DatasetFileError("dataset_transfer_metadata_invalid") from None
    finally:
        os.close(descriptor)


def _recover(
    source: DatasetSource, directory: Path, execution_identifier: str
) -> DatasetTransfer | None:
    fingerprint = dataset_source_fingerprint(source)
    locator = f"downloads/{fingerprint}/{execution_identifier}.data"
    prepared_locator = f"downloads/{fingerprint}/{execution_identifier}.prepared.json"
    prepared = _read_preparation(directory, prepared_locator)
    try:
        descriptor = open_dataset_file(directory, locator)
    except FileNotFoundError:
        return None
    try:
        if prepared is None:
            raise DatasetFileError("dataset_transfer_metadata_missing")
        if (
            prepared.source_identifier != source.identifier
            or prepared.source_fingerprint != fingerprint
            or prepared.execution_identifier != execution_identifier
            or prepared.requested_url != source.url
            or prepared.locator != locator
        ):
            raise DatasetFileError("dataset_source_mismatch")
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o077
            or info.st_size == 0
        ):
            raise DatasetFileError("dataset_file_invalid")
        if source.maximum_bytes is not None and info.st_size > source.maximum_bytes:
            raise DatasetFileError("dataset_download_too_large")
        return DatasetTransfer(**prepared.model_dump(exclude={"kind"}), bytes=info.st_size)
    finally:
        os.close(descriptor)


async def recover_dataset_transfer(
    source: DatasetSource, *, directory: Path, execution_identifier: str
) -> DatasetTransfer | None:
    """Recover durable EOF evidence; verify_dataset_file certifies its content."""
    if current_resource_manager() is None:
        raise ResourceLeaseLost("Dataset recovery requires a managed resource context")
    source = DatasetSource.model_validate_json(source.model_dump_json())
    _ = validate_dataset_execution_identifier(execution_identifier)
    try:
        return await run_cpu(_recover, source, directory, execution_identifier)
    except OSError:
        raise DatasetFileError("dataset_transfer_metadata_invalid") from None


def _discard_incomplete(source: DatasetSource, directory: Path, execution_identifier: str) -> None:
    fingerprint = dataset_source_fingerprint(source)
    prepared = _read_preparation(
        directory, f"downloads/{fingerprint}/{execution_identifier}.prepared.json"
    )
    if prepared is not None and (
        prepared.source_identifier != source.identifier
        or prepared.source_fingerprint != fingerprint
        or prepared.execution_identifier != execution_identifier
        or prepared.requested_url != source.url
    ):
        raise DatasetFileError("dataset_source_mismatch")
    try:
        bucket = open_dataset_bucket(directory, fingerprint)
    except FileNotFoundError:
        return
    try:
        final = f"{execution_identifier}.data"
        try:
            _ = os.stat(final, dir_fd=bucket, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise DatasetFileError("dataset_cleanup_refused")
        pattern = re.compile(rf"\.{re.escape(execution_identifier)}\.[0-9a-f]{{32}}\.part\Z")
        selected: list[tuple[str, tuple[int, int, int, int, int]]] = []
        with os.scandir(bucket) as entries:
            for index, entry in enumerate(entries):
                if index >= 4096:
                    raise DatasetFileError("dataset_cleanup_refused")
                if pattern.fullmatch(entry.name) is None:
                    continue
                info = os.stat(entry.name, dir_fd=bucket, follow_symlinks=False)
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.getuid()
                    or info.st_mode & 0o077
                    or info.st_nlink != 1
                ):
                    raise DatasetFileError("dataset_cleanup_refused")
                selected.append((entry.name, _identity(info)))
        for name, identity in selected:
            if _identity(os.stat(name, dir_fd=bucket, follow_symlinks=False)) != identity:
                raise DatasetFileError("dataset_cleanup_refused")
        for name, _ in selected:
            os.unlink(name, dir_fd=bucket)
        os.fsync(bucket)
    finally:
        os.close(bucket)


async def discard_incomplete_dataset_transfer(
    source: DatasetSource, *, directory: Path, execution_identifier: str
) -> None:
    """Remove this failed attempt's parts after caller proves physical ownership.

    The caller must already hold the workflow's physical guard and prove the
    old producer is gone. This helper never claims a workflow or permits EOF
    evidence, prepared metadata, or another execution's files to be discarded.
    """
    if current_resource_manager() is None:
        raise ResourceLeaseLost("Dataset cleanup requires a managed resource context")
    source = DatasetSource.model_validate_json(source.model_dump_json())
    _ = validate_dataset_execution_identifier(execution_identifier)
    try:
        await run_cpu(_discard_incomplete, source, directory, execution_identifier)
    except OSError:
        raise DatasetFileError("dataset_cleanup_refused") from None


def _verify(
    source: DatasetSource, candidate: DatasetTransfer | FileReceipt, directory: Path
) -> FileReceipt:
    descriptor = open_dataset_file(directory, candidate.locator)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) & 0o077
            or before.st_size != candidate.bytes
        ):
            raise DatasetFileError("dataset_file_invalid")
        if source.maximum_bytes is not None and before.st_size > source.maximum_bytes:
            raise DatasetFileError("dataset_download_too_large")
        sha256 = hashlib.sha256()
        md5 = (
            hashlib.md5(usedforsecurity=False)
            if source.expected_md5 is not None
            or (isinstance(candidate, FileReceipt) and candidate.md5 is not None)
            else None
        )
        observed = 0
        while observed <= candidate.bytes:
            chunk = os.read(descriptor, min(DATASET_CHUNK_BYTES, candidate.bytes + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            sha256.update(chunk)
            if md5 is not None:
                md5.update(chunk)
        digest = sha256.hexdigest()
        md5_digest = md5.hexdigest() if md5 is not None else None
        if observed != candidate.bytes or _identity(os.fstat(descriptor)) != _identity(before):
            raise DatasetFileError("dataset_file_invalid")
        if (
            (source.expected_sha256 is not None and digest != source.expected_sha256)
            or (source.expected_md5 is not None and md5_digest != source.expected_md5)
            or (
                isinstance(candidate, FileReceipt)
                and (candidate.sha256 != digest or candidate.md5 != md5_digest)
            )
        ):
            raise DatasetFileError("dataset_checksum_mismatch")
        value = candidate.model_dump(exclude={"kind", "sha256", "md5"})
        return FileReceipt(**value, sha256=digest, md5=md5_digest)
    finally:
        os.close(descriptor)


async def verify_dataset_file(
    source: DatasetSource, candidate: DatasetTransfer | FileReceipt, *, directory: Path
) -> FileReceipt:
    if current_resource_manager() is None:
        raise ResourceLeaseLost("Dataset verification requires a managed resource context")
    source = DatasetSource.model_validate_json(source.model_dump_json())
    candidate = type(candidate).model_validate_json(candidate.model_dump_json())
    if (
        candidate.source_identifier != source.identifier
        or candidate.source_fingerprint != dataset_source_fingerprint(source)
        or candidate.requested_url != source.url
    ):
        raise DatasetFileError("dataset_source_mismatch")
    try:
        return await run_cpu(partial(_verify, source, candidate, directory))
    except OSError:
        raise DatasetFileError("dataset_file_invalid") from None


async def download_dataset_source(
    source: DatasetSource,
    *,
    directory: Path,
    execution_identifier: str,
    transport: httpx.AsyncBaseTransport | None = None,
) -> FileReceipt:
    transfer = await stream_dataset_source(
        source, directory=directory, execution_identifier=execution_identifier, transport=transport
    )
    return await verify_dataset_file(source, transfer, directory=directory)


def geonames_download_metadata(source: DatasetSource, receipt: FileReceipt) -> dict[str, object]:
    if receipt.source_fingerprint != dataset_source_fingerprint(source):
        raise DatasetFileError("dataset_source_mismatch")
    return {
        "name": source.name,
        "url": source.url,
        "response_url": receipt.response_url,
        "bytes": receipt.bytes,
        "sha256": receipt.sha256,
        "downloaded_at_utc": receipt.downloaded_at_utc,
        "last_modified": receipt.last_modified,
        "etag": receipt.etag,
    }


def osrm_download_metadata(
    source: DatasetSource, receipt: FileReceipt, *, snapshot: str
) -> dict[str, object]:
    if (
        receipt.source_fingerprint != dataset_source_fingerprint(source)
        or source.expected_md5 is None
        or receipt.md5 != source.expected_md5
    ):
        raise DatasetFileError("dataset_source_mismatch")
    return {
        "id": source.identifier,
        "url": source.url,
        "snapshot": snapshot,
        "md5": source.expected_md5,
        "bytes": receipt.bytes,
        "sha256": receipt.sha256,
    }
