"""Dataset receipts remain small, verified and compatible with existing identities."""

# Physical resource counts and controlled verification failures are intentional.
# pyright: reportPrivateUsage=false

import hashlib
import json
import os
import threading
from pathlib import Path

import anyio
import pytest
from pydantic import ValidationError

import carl.io.dataset_downloads as downloads
import carl.io.streaming_http as streaming
from carl._tests.test_streaming_http import _held, _source, _Stream, _Transport
from carl.core.dataset_downloads import (
    DatasetFileError,
    DatasetSource,
    DatasetTransfer,
    FileReceipt,
    dataset_source_fingerprint,
)
from carl.io.dataset_downloads import (
    discard_incomplete_dataset_transfer,
    download_dataset_source,
    geonames_download_metadata,
    osrm_download_metadata,
    recover_dataset_transfer,
    verify_dataset_file,
)
from carl.io.resource_permits import ResourceLeaseLost
from carl.io.sqlite import Database
from carl.io.streaming_http import stream_dataset_source

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def test_foreground_composition_and_cpu_only_receipt_replay_skip_http(tmp_path: Path) -> None:
    data = b"fixture-dataset"
    source = _source(
        expected_md5=hashlib.md5(data, usedforsecurity=False).hexdigest(),
        expected_sha256=hashlib.sha256(data).hexdigest(),
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        transport = _Transport(database, _Stream(database))
        receipt = await download_dataset_source(
            source, directory=tmp_path, execution_identifier="one", transport=transport
        )
        assert FileReceipt.model_validate_json(receipt.model_dump_json()) == receipt
        assert receipt.sha256 == hashlib.sha256(data).hexdigest()
        assert receipt.md5 == source.expected_md5
        assert len(receipt.model_dump_json().encode()) <= 16384
        assert await verify_dataset_file(source, receipt, directory=tmp_path) == receipt
        assert (
            len(transport.requests) == 1
            and await _held(database, "http") == 0
            and await _held(database, "cpu") == 0
        )
        assert set(geonames_download_metadata(source, receipt)) == {
            "name",
            "url",
            "response_url",
            "bytes",
            "sha256",
            "downloaded_at_utc",
            "last_modified",
            "etag",
        }
        assert osrm_download_metadata(source, receipt, snapshot="261101") == {
            "id": source.identifier,
            "url": source.url,
            "snapshot": "261101",
            "md5": source.expected_md5,
            "bytes": len(data),
            "sha256": receipt.sha256,
        }


@pytest.mark.parametrize("checksum", ["md5", "sha256"])
async def test_checksum_failure_retains_complete_file_but_never_returns_verified_receipt(
    tmp_path: Path, checksum: str
) -> None:
    source = _source(**{f"expected_{checksum}": "0" * (32 if checksum == "md5" else 64)})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        transfer = await stream_dataset_source(
            source,
            directory=tmp_path,
            execution_identifier="one",
            transport=_Transport(database, _Stream(database)),
        )
        with pytest.raises(DatasetFileError, match="dataset_checksum_mismatch"):
            _ = await verify_dataset_file(source, transfer, directory=tmp_path)
        assert (tmp_path / transfer.locator).read_bytes() == b"fixture-dataset"
        assert await _held(database, "cpu") == 0


@pytest.mark.parametrize("damage", ["same_size", "missing", "symlink", "permissions", "source"])
async def test_saved_receipt_requires_actual_file_integrity_and_frozen_source(
    tmp_path: Path, damage: str
) -> None:
    source = _source()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        receipt = await download_dataset_source(
            source,
            directory=tmp_path,
            execution_identifier="one",
            transport=_Transport(database, _Stream(database)),
        )
        path = tmp_path / receipt.locator
        if damage == "same_size":
            _ = path.write_bytes(b"x" * receipt.bytes)
        elif damage in ("missing", "symlink"):
            path.unlink()
            if damage == "symlink":
                target = tmp_path / "elsewhere"
                _ = target.write_bytes(b"fixture-dataset")
                path.symlink_to(target)
        elif damage == "permissions":
            path.chmod(0o644)
        else:
            source = _source(url="https://example.test/different.zip")
        with pytest.raises(DatasetFileError):
            _ = await verify_dataset_file(source, receipt, directory=tmp_path)


async def test_verification_cancellation_keeps_cpu_until_physical_thread_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    original = downloads._verify

    def verify(
        source: DatasetSource, candidate: DatasetTransfer | FileReceipt, directory: Path
    ) -> FileReceipt:
        entered.set()
        assert release.wait(5)
        return original(source, candidate, directory)

    monkeypatch.setattr(downloads, "_verify", verify)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        source = _source()
        transfer = await stream_dataset_source(
            source,
            directory=tmp_path,
            execution_identifier="one",
            transport=_Transport(database, _Stream(database)),
        )
        scopes: list[anyio.CancelScope] = []

        async def run() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                _ = await verify_dataset_file(source, transfer, directory=tmp_path)

        async with anyio.create_task_group() as tasks:
            _ = tasks.start_soon(run)
            try:
                with anyio.fail_after(5):
                    while not entered.is_set():
                        await anyio.sleep(0.005)
                scopes[0].cancel()
                await anyio.lowlevel.checkpoint()
                assert await _held(database, "cpu") == 1 and await _held(database, "http") == 0
            finally:
                release.set()
        assert await _held(database, "cpu") == 0


async def test_file_changed_during_verification_is_not_certified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        source = _source()
        transfer = await stream_dataset_source(
            source,
            directory=tmp_path,
            execution_identifier="one",
            transport=_Transport(database, _Stream(database)),
        )
        path = tmp_path / transfer.locator
        identity = (path.stat().st_dev, path.stat().st_ino)
        original = os.read
        changed = False

        def read(descriptor: int, amount: int) -> bytes:
            nonlocal changed
            result = original(descriptor, amount)
            info = os.fstat(descriptor)
            if result and not changed and (info.st_dev, info.st_ino) == identity:
                changed = True
                _ = path.write_bytes(b"x" * transfer.bytes)
            return result

        monkeypatch.setattr(os, "read", read)
        with pytest.raises(DatasetFileError, match="dataset_file_invalid"):
            _ = await verify_dataset_file(source, transfer, directory=tmp_path)
        assert changed


@pytest.mark.parametrize(
    "url",
    [
        "http://example.test/a",
        "https://user:secret@example.test/a",
        "https://example.test/a#frag",
        "https://example.test/../a",
        "https://example.test/a\n",
    ],
)
def test_source_urls_are_strict_and_safe(url: str) -> None:
    with pytest.raises(ValidationError):
        _ = DatasetSource(identifier="one", name="one.pbf", url=url)


@pytest.mark.parametrize("name", ["../one.pbf", "/one.pbf", ".", "..", "a/b", "a\\b", "a\n"])
def test_source_names_cannot_escape_or_alias_storage(name: str) -> None:
    with pytest.raises(ValidationError):
        _ = DatasetSource(identifier="one", name=name, url="https://example.test/a")


def test_multi_gib_file_counts_and_optional_caps_fit_bounded_metadata() -> None:
    for maximum in (None, 8 * 1024**3):
        source = _source(maximum_bytes=maximum)
        transfer = DatasetTransfer(
            source_identifier=source.identifier,
            source_fingerprint=dataset_source_fingerprint(source),
            execution_identifier="large",
            locator=f"downloads/{dataset_source_fingerprint(source)}/large.data",
            bytes=5 * 1024**3,
            requested_url=source.url,
            response_url=source.url,
            downloaded_at_utc="2026-11-02T00:00:00+00:00",
        )
        assert DatasetTransfer.model_validate_json(transfer.model_dump_json()) == transfer
        assert len(transfer.model_dump_json().encode()) < 16384
        with pytest.raises(ValidationError):
            _ = DatasetTransfer.model_validate_json(
                transfer.model_dump_json().replace("large.data", "../escape")
            )


async def test_missing_manager_verification_rejects_before_file_io(tmp_path: Path) -> None:
    source = _source()
    transfer = DatasetTransfer(
        source_identifier=source.identifier,
        source_fingerprint=dataset_source_fingerprint(source),
        execution_identifier="one",
        locator=f"downloads/{dataset_source_fingerprint(source)}/one.data",
        bytes=1,
        requested_url=source.url,
        response_url=source.url,
        downloaded_at_utc="2026-11-02T00:00:00+00:00",
    )
    with pytest.raises(ResourceLeaseLost):
        _ = await verify_dataset_file(source, transfer, directory=tmp_path / "absent")
    with pytest.raises(ResourceLeaseLost):
        await discard_incomplete_dataset_transfer(
            source, directory=tmp_path / "absent", execution_identifier="one"
        )


class _AfterEofCrash(BaseException):
    pass


async def test_atomic_eof_before_dto_return_recovers_metadata_and_hash_without_http(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source()
    path = tmp_path / "carl.sqlite3"
    original = DatasetTransfer

    def crash(**_kwargs: object):
        raise _AfterEofCrash

    async with Database.managed(path, initialize=True) as database:
        transport = _Transport(database, _Stream(database))
        monkeypatch.setattr(streaming, "DatasetTransfer", crash)
        with pytest.raises(_AfterEofCrash):
            _ = await stream_dataset_source(
                source, directory=tmp_path, execution_identifier="lost", transport=transport
            )
        monkeypatch.setattr(streaming, "DatasetTransfer", original)
        assert len(transport.requests) == 1
    async with Database.managed(path) as database:
        transfer = await recover_dataset_transfer(
            source, directory=tmp_path, execution_identifier="lost"
        )
        assert transfer is not None and transfer.bytes == len(b"fixture-dataset")
        prepared_path = tmp_path / transfer.locator.replace(".data", ".prepared.json")
        prepared = json.loads(prepared_path.read_text())
        assert "bytes" not in prepared
        assert transfer.downloaded_at_utc == prepared["downloaded_at_utc"]
        assert (
            transfer.response_url == prepared["response_url"] and transfer.etag == prepared["etag"]
        )
        receipt = await verify_dataset_file(source, transfer, directory=tmp_path)
        assert receipt.sha256 == hashlib.sha256(b"fixture-dataset").hexdigest()
        assert len(transport.requests) == 1 and await _held(database, "http") == 0


async def test_incomplete_preparation_requires_new_execution_and_keeps_completed_sibling(
    tmp_path: Path,
) -> None:
    source = _source()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        kept = await download_dataset_source(
            source,
            directory=tmp_path,
            execution_identifier="kept",
            transport=_Transport(database, _Stream(database)),
        )
        failed = _Transport(database, _Stream(database, read_error=True))
        with pytest.raises(DatasetFileError):
            _ = await stream_dataset_source(
                source, directory=tmp_path, execution_identifier="failed", transport=failed
            )
        assert (
            await recover_dataset_transfer(
                source, directory=tmp_path, execution_identifier="failed"
            )
            is None
        )
        with pytest.raises(DatasetFileError, match="dataset_transfer_exists"):
            _ = await stream_dataset_source(
                source, directory=tmp_path, execution_identifier="failed", transport=failed
            )
        assert len(failed.requests) == 1
        retried = await download_dataset_source(
            source,
            directory=tmp_path,
            execution_identifier="explicit-new-attempt",
            transport=_Transport(database, _Stream(database)),
        )
        assert retried.sha256 == kept.sha256
        assert (tmp_path / kept.locator).exists()


@pytest.mark.parametrize(
    "damage", ["missing", "bad_json", "oversized", "symlink", "identity", "duplicate_key"]
)
async def test_recovery_rejects_missing_or_corrupt_prepared_metadata(
    tmp_path: Path, damage: str
) -> None:
    source = _source()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        transport = _Transport(database, _Stream(database))
        transfer = await stream_dataset_source(
            source, directory=tmp_path, execution_identifier="one", transport=transport
        )
        prepared = tmp_path / transfer.locator.replace(".data", ".prepared.json")
        if damage == "missing":
            prepared.unlink()
        elif damage == "symlink":
            prepared.unlink()
            prepared.symlink_to(tmp_path / transfer.locator)
        elif damage == "bad_json":
            _ = prepared.write_text("broken")
        elif damage == "oversized":
            _ = prepared.write_bytes(b" " * 16385)
        elif damage == "identity":
            value = json.loads(prepared.read_text())
            value["source_identifier"] = "different"
            _ = prepared.write_text(json.dumps(value))
        else:
            _ = prepared.write_text(
                prepared.read_text().replace(
                    '"schema_version":1', '"schema_version":1,"schema_version":1'
                )
            )
        with pytest.raises(DatasetFileError):
            _ = await recover_dataset_transfer(
                source, directory=tmp_path, execution_identifier="one"
            )
        assert len(transport.requests) == 1


def _part(path: Path) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)


async def test_discard_failed_attempt_parts_preserves_preparation_and_all_siblings(
    tmp_path: Path,
) -> None:
    source = _source()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        kept = await download_dataset_source(
            source,
            directory=tmp_path,
            execution_identifier="kept",
            transport=_Transport(database, _Stream(database)),
        )
        with pytest.raises(DatasetFileError):
            _ = await stream_dataset_source(
                source,
                directory=tmp_path,
                execution_identifier="failed",
                transport=_Transport(database, _Stream(database, read_error=True)),
            )
        bucket = tmp_path / "downloads" / dataset_source_fingerprint(source)
        target, other = (
            bucket / (".failed." + "a" * 32 + ".part"),
            bucket / (".other." + "b" * 32 + ".part"),
        )
        _part(target)
        _part(other)
        await discard_incomplete_dataset_transfer(
            source, directory=tmp_path, execution_identifier="failed"
        )
        assert not target.exists() and other.exists()
        assert (bucket / "failed.prepared.json").exists()
        assert (tmp_path / kept.locator).exists()
        assert await verify_dataset_file(source, kept, directory=tmp_path) == kept


@pytest.mark.parametrize("bad", ["symlink", "hardlink", "permissions", "directory"])
async def test_discard_validates_every_matching_file_before_deleting_any(
    tmp_path: Path, bad: str
) -> None:
    source = _source()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        kept = await download_dataset_source(
            source,
            directory=tmp_path,
            execution_identifier="kept",
            transport=_Transport(database, _Stream(database)),
        )
        bucket = tmp_path / "downloads" / dataset_source_fingerprint(source)
        valid, invalid = (
            bucket / (".failed." + "a" * 32 + ".part"),
            bucket / (".failed." + "b" * 32 + ".part"),
        )
        _part(valid)
        if bad == "symlink":
            invalid.symlink_to(tmp_path / kept.locator)
        elif bad == "hardlink":
            os.link(tmp_path / kept.locator, invalid)
        elif bad == "directory":
            invalid.mkdir(mode=0o700)
        else:
            _part(invalid)
            invalid.chmod(0o644)
        with pytest.raises(DatasetFileError, match="dataset_cleanup_refused"):
            await discard_incomplete_dataset_transfer(
                source, directory=tmp_path, execution_identifier="failed"
            )
        assert valid.exists() and invalid.exists() and (tmp_path / kept.locator).exists()


async def test_discard_refuses_complete_eof_and_bounded_scan_overflow(tmp_path: Path) -> None:
    source = _source()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        kept = await download_dataset_source(
            source,
            directory=tmp_path,
            execution_identifier="kept",
            transport=_Transport(database, _Stream(database)),
        )
        bucket = tmp_path / "downloads" / dataset_source_fingerprint(source)
        valid = bucket / (".failed." + "a" * 32 + ".part")
        _part(valid)
        with pytest.raises(DatasetFileError, match="dataset_cleanup_refused"):
            await discard_incomplete_dataset_transfer(
                source, directory=tmp_path, execution_identifier="kept"
            )
        for index in range(4096):
            _part(bucket / f"unrelated-{index}")
        with pytest.raises(DatasetFileError, match="dataset_cleanup_refused"):
            await discard_incomplete_dataset_transfer(
                source, directory=tmp_path, execution_identifier="failed"
            )
        assert valid.exists() and (tmp_path / kept.locator).exists()
