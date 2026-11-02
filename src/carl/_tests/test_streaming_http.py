"""Offline raw HTTP streams exercise ownership and atomic dataset file capture."""

# Resource counts and descriptor identities are physical ownership assertions.
# pyright: reportPrivateUsage=false

import os
import stat
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import override

import anyio
import httpx
import pytest

import carl.io.streaming_http as streaming
from carl.core.dataset_downloads import DatasetFileError, DatasetSource, DatasetTransfer
from carl.io.resource_permits import ResourceLeaseLost
from carl.io.sqlite import Database
from carl.io.streaming_http import DATASET_CHUNK_BYTES, stream_dataset_source

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _held(database: Database, name: str) -> int:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                "SELECT coalesce(sum(a.units),0) FROM resource_allocations a JOIN resource_permits p ON p.token=a.permit_token WHERE p.state='held' AND a.name=?",
                (name,),
            )
        ).fetchall()
    assert isinstance(rows[0][0], int)
    return rows[0][0]


@dataclass
class _Stream(httpx.AsyncByteStream):
    database: Database
    chunks: tuple[bytes, ...] = (b"fixture-", b"dataset")
    read_error: bool = False
    close_error: bool = False
    block: anyio.Event | None = None
    entered: anyio.Event = field(default_factory=anyio.Event)
    reads: int = 0
    closed: bool = False

    @override
    async def __aiter__(self) -> AsyncIterator[bytes]:
        assert await _held(self.database, "http") == 1 and await _held(self.database, "cpu") == 0
        for chunk in self.chunks:
            self.reads += 1
            yield chunk
        self.entered.set()
        if self.block is not None:
            await self.block.wait()
        if self.read_error:
            raise httpx.ReadError("private source failure")

    @override
    async def aclose(self) -> None:
        assert await _held(self.database, "http") == 1 and await _held(self.database, "cpu") == 0
        self.closed = True
        if self.close_error:
            raise RuntimeError("private response close detail")


@dataclass
class _Transport(httpx.AsyncBaseTransport):
    database: Database
    stream: _Stream
    encoding: str | None = None
    client_close_error: bool = False
    requests: list[httpx.Request] = field(default_factory=list)
    closed: bool = False

    @override
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        assert await _held(self.database, "http") == 1 and await _held(self.database, "cpu") == 0
        assert request.headers["accept-encoding"] == "identity"
        self.requests.append(request)
        headers = {"etag": '"fixture"', "last-modified": "Mon, 02 Nov 2026 00:00:00 GMT"}
        if self.encoding is not None:
            headers["content-encoding"] = self.encoding
        return httpx.Response(200, stream=self.stream, headers=headers)

    @override
    async def aclose(self) -> None:
        assert await _held(self.database, "http") == 1 and await _held(self.database, "cpu") == 0
        self.closed = True
        if self.client_close_error:
            raise RuntimeError("private client cleanup detail")


def _source(**changes: object) -> DatasetSource:
    return DatasetSource(
        identifier="fixture-source", name="fixture.zip", url="https://example.test/fixture.zip"
    ).model_copy(update=changes)


def _assert_closed(descriptor: int, identity: tuple[int, int]) -> None:
    try:
        info = os.fstat(descriptor)
    except OSError:
        return
    assert (info.st_dev, info.st_ino) != identity


async def test_raw_chunks_have_bounded_writes_and_http_owns_both_finalizers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sizes: list[int] = []
    original = streaming._write

    def write(descriptor: int, content: bytes) -> None:
        sizes.append(len(content))
        original(descriptor, content)

    monkeypatch.setattr(streaming, "_write", write)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stream = _Stream(database, chunks=(b"a" * (DATASET_CHUNK_BYTES * 2 + 1), b"tail"))
        transport = _Transport(database, stream)
        transfer = await stream_dataset_source(
            _source(), directory=tmp_path, execution_identifier="one", transport=transport
        )
        assert DatasetTransfer.model_validate_json(transfer.model_dump_json()) == transfer
        assert transfer.bytes == DATASET_CHUNK_BYTES * 2 + 5
        assert max(sizes) <= DATASET_CHUNK_BYTES
        assert (tmp_path / transfer.locator).read_bytes() == b"a" * (
            DATASET_CHUNK_BYTES * 2 + 1
        ) + b"tail"
        assert stream.closed and transport.closed
        assert transfer.cleanup_error_type is None and transfer.etag == '"fixture"'
        assert (tmp_path / transfer.locator).stat().st_mode & 0o077 == 0
        assert not list(tmp_path.rglob("*.part"))
        assert await _held(database, "http") == 0 and await _held(database, "cpu") == 0
        with pytest.raises(DatasetFileError, match="dataset_transfer_exists"):
            _ = await stream_dataset_source(
                _source(), directory=tmp_path, execution_identifier="one", transport=transport
            )
        assert len(transport.requests) == 1


@pytest.mark.parametrize("encoding", ["gzip", "br", "identity, gzip"])
async def test_nonidentity_response_is_rejected_without_reading_or_publishing(
    tmp_path: Path, encoding: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stream = _Stream(database)
        transport = _Transport(database, stream, encoding=encoding)
        with pytest.raises(DatasetFileError, match="dataset_content_encoding_unsupported"):
            _ = await stream_dataset_source(
                _source(), directory=tmp_path, execution_identifier="one", transport=transport
            )
        assert stream.reads == 0 and stream.closed and transport.closed
        assert not list(tmp_path.rglob("*.data")) and not list(tmp_path.rglob("*.part"))
        assert await _held(database, "http") == 0


@pytest.mark.parametrize("failure", ["limit", "empty", "read"])
async def test_incomplete_file_cleanup_preserves_completed_sibling(
    tmp_path: Path, failure: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        first = await stream_dataset_source(
            _source(),
            directory=tmp_path,
            execution_identifier="kept",
            transport=_Transport(database, _Stream(database)),
        )
        stream = _Stream(
            database,
            chunks=() if failure == "empty" else (b"long content",),
            read_error=failure == "read",
        )
        source = _source(maximum_bytes=3) if failure == "limit" else _source()
        with pytest.raises(DatasetFileError):
            _ = await stream_dataset_source(
                source,
                directory=tmp_path,
                execution_identifier="failed",
                transport=_Transport(database, stream),
            )
        assert (tmp_path / first.locator).read_bytes() == b"fixture-dataset"
        assert len(list(tmp_path.rglob("*.data"))) == 1 and not list(tmp_path.rglob("*.part"))


@pytest.mark.parametrize("where", ["response", "client"])
async def test_complete_eof_survives_late_cleanup_failure(tmp_path: Path, where: str) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stream = _Stream(database, close_error=where == "response")
        transport = _Transport(database, stream, client_close_error=where == "client")
        transfer = await stream_dataset_source(
            _source(), directory=tmp_path, execution_identifier="complete", transport=transport
        )
        assert transfer.cleanup_error_type == "RuntimeError"
        assert (tmp_path / transfer.locator).read_bytes() == b"fixture-dataset"
        assert "private" not in transfer.model_dump_json()
        assert transport.closed and stream.closed and await _held(database, "http") == 0


async def test_cancellation_closes_http_and_deletes_only_uncommitted_part(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stream = _Stream(database, chunks=(b"pending",), block=anyio.Event())
        transport = _Transport(database, stream)
        scopes: list[anyio.CancelScope] = []

        async def call() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                _ = await stream_dataset_source(
                    _source(),
                    directory=tmp_path,
                    execution_identifier="cancelled",
                    transport=transport,
                )

        async with anyio.create_task_group() as tasks:
            _ = tasks.start_soon(call)
            with anyio.fail_after(5):
                await stream.entered.wait()
                scopes[0].cancel()
        assert stream.closed and transport.closed
        assert not list(tmp_path.rglob("*.part")) and not list(tmp_path.rglob("*.data"))
        assert await _held(database, "http") == 0


@pytest.mark.parametrize("fault", ["constructor", "fsync", "unlink"])
async def test_storage_cleanup_always_closes_bucket_and_preserves_primary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    recorded: list[tuple[int, tuple[int, int]]] = []
    original_open, original_fsync, original_unlink = (
        streaming._open_bucket,
        os.fsync,
        os.unlink,
    )

    def opened(path: Path, fingerprint: str) -> int:
        descriptor = original_open(path, fingerprint)
        info = os.fstat(descriptor)
        recorded.append((descriptor, (info.st_dev, info.st_ino)))
        return descriptor

    monkeypatch.setattr(streaming, "_open_bucket", opened)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        if fault == "constructor":

            def fail_client(**_kwargs: object):
                raise ValueError("private invalid SSL configuration")

            monkeypatch.setattr(httpx, "AsyncClient", fail_client)
        elif fault == "fsync":

            def fsync(descriptor: int) -> None:
                info = os.fstat(descriptor)
                if stat.S_ISDIR(info.st_mode) and any(
                    (info.st_dev, info.st_ino) == identity for _, identity in recorded
                ):
                    raise OSError("private fsync failure")
                original_fsync(descriptor)

            monkeypatch.setattr(os, "fsync", fsync)
        else:

            def unlink(path: str, *, dir_fd: int | None = None) -> None:
                if path.endswith(".part"):
                    raise OSError("private unlink failure")
                original_unlink(path, dir_fd=dir_fd)

            monkeypatch.setattr(os, "unlink", unlink)
        if fault == "unlink":
            transfer = await stream_dataset_source(
                _source(),
                directory=tmp_path,
                execution_identifier="one",
                transport=_Transport(database, _Stream(database)),
            )
            assert transfer.cleanup_error_type == "OSError"
            assert (tmp_path / transfer.locator).read_bytes() == b"fixture-dataset"
        else:
            with pytest.raises(DatasetFileError) as error:
                _ = await stream_dataset_source(
                    _source(),
                    directory=tmp_path,
                    execution_identifier="one",
                    transport=_Transport(database, _Stream(database)),
                )
            assert "private" not in str(error.value)
        assert recorded
        for descriptor, identity in recorded:
            _assert_closed(descriptor, identity)


async def test_missing_manager_fails_before_filesystem_or_client(tmp_path: Path) -> None:
    target = tmp_path / "not-created"
    with pytest.raises(ResourceLeaseLost):
        _ = await stream_dataset_source(_source(), directory=target, execution_identifier="one")
    assert not target.exists()


@pytest.mark.parametrize("unsafe", ["root_permissions", "child_permissions", "symlink"])
async def test_unsafe_storage_is_rejected_before_dispatch(tmp_path: Path, unsafe: str) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        root = tmp_path / "storage"
        if unsafe == "root_permissions":
            root.mkdir(mode=0o755)
            root.chmod(0o755)
        elif unsafe == "child_permissions":
            root.mkdir(mode=0o700)
            (root / "downloads").mkdir(mode=0o755)
            (root / "downloads").chmod(0o755)
        else:
            target = tmp_path / "target"
            target.mkdir(mode=0o700)
            root.symlink_to(target, target_is_directory=True)
        transport = _Transport(database, _Stream(database))
        with pytest.raises(DatasetFileError):
            _ = await stream_dataset_source(
                _source(), directory=root, execution_identifier="one", transport=transport
            )
        assert transport.requests == []
        assert not list(tmp_path.rglob("*.data"))


@pytest.mark.parametrize(
    "target",
    [
        "https://cdn.example.test/file.zip",
        "http://example.test/file.zip",
        "https://user:secret@example.test/file.zip",
    ],
)
async def test_redirect_policy_preserves_identity_and_rejects_unsafe_target_before_wire(
    tmp_path: Path, target: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        requests: list[httpx.Request] = []
        redirect, body = _Stream(database, chunks=()), _Stream(database)

        async def respond(request: httpx.Request) -> httpx.Response:
            assert await _held(database, "http") == 1 and await _held(database, "cpu") == 0
            assert request.headers["accept-encoding"] == "identity"
            requests.append(request)
            if len(requests) == 1:
                return httpx.Response(302, headers={"location": target}, stream=redirect)
            return httpx.Response(200, stream=body)

        transport = httpx.MockTransport(respond)
        if target.startswith("https://cdn"):
            transfer = await stream_dataset_source(
                _source(follow_redirects=True),
                directory=tmp_path,
                execution_identifier="one",
                transport=transport,
            )
            assert transfer.requested_url == _source().url and transfer.response_url == target
            assert len(requests) == 2 and redirect.closed and body.closed
        else:
            with pytest.raises(DatasetFileError):
                _ = await stream_dataset_source(
                    _source(follow_redirects=True),
                    directory=tmp_path,
                    execution_identifier="one",
                    transport=transport,
                )
            assert len(requests) == 1 and redirect.closed
        assert await _held(database, "http") == 0


async def test_oversized_response_metadata_is_rejected_before_body(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        body = _Stream(database)

        def respond(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, headers={"etag": "x" * 1025}, stream=body)

        with pytest.raises(DatasetFileError):
            _ = await stream_dataset_source(
                _source(),
                directory=tmp_path,
                execution_identifier="one",
                transport=httpx.MockTransport(respond),
            )
        assert body.reads == 0 and body.closed
        assert not list(tmp_path.rglob("*.data")) and not list(tmp_path.rglob("*.part"))
