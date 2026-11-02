"""CPU verification and foreground composition of streamed dataset files."""

import hashlib
import os
import stat
from functools import partial
from pathlib import Path

import httpx

from carl.core.dataset_downloads import (
    DatasetFileError,
    DatasetSource,
    DatasetTransfer,
    FileReceipt,
    dataset_source_fingerprint,
)
from carl.io.resource_permits import ResourceLeaseLost, current_resource_manager, run_cpu
from carl.io.streaming_http import DATASET_CHUNK_BYTES, open_dataset_file, stream_dataset_source


def _identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


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
