"""HTTP-owned, identity-only streaming to immutable private dataset files."""

import os
import secrets
import stat
import sys
from collections.abc import AsyncIterator
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import override
from urllib.parse import urljoin

import anyio
import httpx

from carl.core.dataset_downloads import (
    MAX_DATASET_FILE_BYTES,
    DatasetFileError,
    DatasetSource,
    DatasetTransfer,
    dataset_source_fingerprint,
    validate_dataset_execution_identifier,
    validate_dataset_url,
)
from carl.io.cleanup import shielded_cleanup
from carl.io.httpx import close_httpx_client
from carl.io.resource_permits import ResourceLeaseLost, current_resource_manager, resource_operation

DATASET_CHUNK_BYTES = 256 * 1024
MAX_DATASET_REDIRECTS = 20


def _cleanup_type(error: Exception) -> str:
    name = type(error).__name__[:128]
    return name if name.isascii() and name.isidentifier() else "Exception"


def open_dataset_directory(path: Path, *, create: bool = False) -> int:
    if not path.is_absolute() or ".." in path.parts or len(path.parts) < 2:
        raise DatasetFileError("dataset_file_invalid")
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
            raise DatasetFileError("dataset_file_invalid")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def open_dataset_file(directory: Path, locator: str) -> int:
    path = Path(locator)
    if (
        path.is_absolute()
        or path.as_posix() != locator
        or not path.parts
        or any(part in (".", "..") for part in path.parts)
    ):
        raise DatasetFileError("dataset_file_invalid")
    parent = open_dataset_directory(directory)
    try:
        for component in path.parts[:-1]:
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=parent,
            )
            try:
                _private_directory(child)
            except BaseException:
                os.close(child)
                raise
            os.close(parent)
            parent = child
        return os.open(
            path.name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
        )
    finally:
        os.close(parent)


def _private_directory(descriptor: int) -> None:
    info = os.fstat(descriptor)
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise DatasetFileError("dataset_file_invalid")


def _open_bucket(directory: Path, fingerprint: str) -> int:
    parent = open_dataset_directory(directory, create=True)
    try:
        for component in ("downloads", fingerprint):
            with suppress(FileExistsError):
                os.mkdir(component, mode=0o700, dir_fd=parent)
                os.fsync(parent)
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=parent,
            )
            try:
                _private_directory(child)
            except BaseException:
                os.close(child)
                raise
            os.close(parent)
            parent = child
        return parent
    except BaseException:
        os.close(parent)
        raise


def _write(descriptor: int, content: bytes) -> None:
    remaining = memoryview(content)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("Dataset file write made no progress")
        remaining = remaining[written:]


def _publish(descriptor: int, directory: int, temporary: str, final: str) -> None:
    os.fsync(descriptor)
    try:
        os.link(temporary, final, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
    except FileExistsError:
        raise DatasetFileError("dataset_transfer_exists") from None
    os.fsync(directory)


def _cleanup_storage(descriptor: int | None, bucket: int, temporary: str) -> None:
    try:
        try:
            if descriptor is not None:
                os.close(descriptor)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=bucket)
                os.fsync(bucket)
    finally:
        os.close(bucket)


class _TrackingStream(httpx.AsyncByteStream):
    def __init__(self, stream: httpx.AsyncByteStream):
        self.stream: httpx.AsyncByteStream = stream
        self.complete: bool = False

    @override
    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self.stream:
            yield chunk
        self.complete = True

    @override
    async def aclose(self) -> None:
        await self.stream.aclose()


async def stream_dataset_source(
    source: DatasetSource,
    *,
    directory: Path,
    execution_identifier: str,
    transport: httpx.AsyncBaseTransport | None = None,
) -> DatasetTransfer:
    """Finish each HTTP/client lifetime before returning a file-only transfer."""
    if current_resource_manager() is None:
        raise ResourceLeaseLost("Dataset downloads require a managed resource context")
    source = DatasetSource.model_validate_json(source.model_dump_json())
    _ = validate_dataset_execution_identifier(execution_identifier)
    return await _stream_dataset_source(source, directory, execution_identifier, transport)


@resource_operation("http")
async def _stream_dataset_source(
    source: DatasetSource,
    directory: Path,
    execution_identifier: str,
    transport: httpx.AsyncBaseTransport | None,
) -> DatasetTransfer:
    fingerprint = dataset_source_fingerprint(source)
    locator = f"downloads/{fingerprint}/{execution_identifier}.data"
    try:
        bucket = _open_bucket(directory, fingerprint)
    except OSError:
        raise DatasetFileError("dataset_storage_failure") from None
    temporary = f".{execution_identifier}.{secrets.token_hex(16)}.part"
    final = f"{execution_identifier}.data"
    descriptor = None
    transfer: DatasetTransfer | None = None
    response: httpx.Response | None = None
    cleanup_error: str | None = None
    client: httpx.AsyncClient | None = None
    try:
        try:
            _ = os.stat(final, dir_fd=bucket, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise DatasetFileError("dataset_transfer_exists")
        client = httpx.AsyncClient(
            transport=transport,
            timeout=120,
            follow_redirects=False,
            trust_env=False,
            headers={"Accept-Encoding": "identity"},
        )
        current = source.url
        for redirects in range(MAX_DATASET_REDIRECTS + 1):
            response = await client.send(
                client.build_request("GET", current), stream=True, follow_redirects=False
            )
            if not response.is_redirect:
                break
            if not source.follow_redirects or redirects == MAX_DATASET_REDIRECTS:
                raise DatasetFileError("dataset_http_failed", status_code=response.status_code)
            location = response.headers.get("location")
            if location is None:
                raise DatasetFileError("dataset_http_failed", status_code=response.status_code)
            current = validate_dataset_url(urljoin(str(response.url), location))
            async with shielded_cleanup("dataset_redirect_close", primary_error=None):
                await response.aclose()
            response = None
        assert response is not None
        if not response.is_success:
            raise DatasetFileError("dataset_http_failed", status_code=response.status_code)
        encodings = tuple(
            part.strip().lower()
            for value in response.headers.get_list("content-encoding")
            for part in value.split(",")
            if part.strip()
        )
        if any(value != "identity" for value in encodings):
            raise DatasetFileError("dataset_content_encoding_unsupported")
        # Validate all retained metadata before accepting body bytes or writing.
        metadata = DatasetTransfer(
            source_identifier=source.identifier,
            source_fingerprint=fingerprint,
            execution_identifier=execution_identifier,
            locator=locator,
            bytes=1,
            requested_url=source.url,
            response_url=str(response.url),
            downloaded_at_utc=datetime.now(UTC).isoformat(),
            last_modified=response.headers.get("last-modified"),
            etag=response.headers.get("etag"),
        )
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=bucket,
        )
        tracker = None
        if response.is_stream_consumed:
            iterator = response.aiter_bytes(DATASET_CHUNK_BYTES).__aiter__()
        else:
            if not isinstance(response.stream, httpx.AsyncByteStream):
                raise DatasetFileError("dataset_download_failed")
            tracker = _TrackingStream(response.stream)
            response.stream = tracker
            iterator = response.aiter_raw(DATASET_CHUNK_BYTES).__aiter__()
        observed = 0
        while True:
            try:
                chunk = await anext(iterator)
            except StopAsyncIteration:
                break
            except Exception as error:
                if tracker is None or not tracker.complete:
                    raise
                cleanup_error = _cleanup_type(error)
                break
            observed += len(chunk)
            if observed > (
                source.maximum_bytes if source.maximum_bytes is not None else MAX_DATASET_FILE_BYTES
            ):
                raise DatasetFileError("dataset_download_too_large")
            await anyio.to_thread.run_sync(_write, descriptor, chunk, abandon_on_cancel=False)
        if observed == 0:
            raise DatasetFileError("dataset_empty_source")
        await anyio.to_thread.run_sync(
            _publish, descriptor, bucket, temporary, final, abandon_on_cancel=False
        )
        transfer = metadata.model_copy(
            update={"bytes": observed, "cleanup_error_type": cleanup_error}
        )
    except httpx.HTTPError:
        raise DatasetFileError("dataset_download_failed") from None
    except OSError:
        raise DatasetFileError("dataset_storage_failure") from None
    except DatasetFileError:
        raise
    except Exception:
        raise DatasetFileError("dataset_download_failed") from None
    finally:
        primary = sys.exception()
        try:
            if response is not None:
                try:
                    async with shielded_cleanup("dataset_response_close", primary_error=primary):
                        await response.aclose()
                except Exception as error:
                    if transfer is None:
                        raise
                    cleanup_error = _cleanup_type(error)
            if client is not None:
                try:
                    _ = await close_httpx_client(client, primary_error=primary)
                except Exception as error:
                    if transfer is None:
                        raise
                    cleanup_error = _cleanup_type(error)
        finally:
            try:
                async with shielded_cleanup("dataset_partial_cleanup", primary_error=primary):
                    await anyio.to_thread.run_sync(
                        _cleanup_storage, descriptor, bucket, temporary, abandon_on_cancel=False
                    )
            except Exception as error:
                if transfer is None:
                    raise DatasetFileError("dataset_storage_failure") from None
                cleanup_error = _cleanup_type(error)
    assert transfer is not None
    return transfer.model_copy(update={"cleanup_error_type": cleanup_error})
