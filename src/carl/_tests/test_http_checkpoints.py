"""HTTP checkpoints retain exact bytes without inline SQLite body storage."""

# Fault injection and metadata corruption deliberately inspect storage boundaries.
# pyright: reportPrivateUsage=false

import gzip
import hashlib
import os
import stat
import threading
from pathlib import Path
from typing import cast
from uuid import uuid4

import anyio
import pytest

import carl.io.http_checkpoints as checkpoints
from carl._tests.test_worker import _provenance
from carl.core.components import Component, ComponentId
from carl.core.models import JsonValue, RecordDraft
from carl.io.http_checkpoints import HttpCheckpoint, checkpoint_acquisition, restore_acquisition
from carl.io.httpx import AcquiredBody, Acquisition
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
_COMPONENT = Component(ComponentId(("test", "http_checkpoint")), 1, lambda: None)


def _acquisition(*content: bytes) -> Acquisition:
    bodies = tuple(
        AcquiredBody(
            identifier=f"body-{index}",
            media_type="application/octet-stream",
            representation={
                "kind": "http_message_content" if index == 0 else "content_decoded_http_body",
                "content_decoded": index != 0,
                "content_encodings_removed": [] if index == 0 else ["br"],
                "transport": {"fixture": True},
            },
            content=body,
        )
        for index, body in enumerate(content)
    )
    return Acquisition(
        record={
            "effective_url": "https://example.test/final",
            "hops": [
                {
                    "request": {"url": f"https://example.test/{index}"},
                    "response": {
                        "status_code": 200,
                        "headers": [{"name_latin1": "Content-Encoding", "value_latin1": "gzip"}],
                        "body": {"state": "available", "artifact_id": body.identifier},
                    },
                }
                for index, body in enumerate(bodies)
            ],
            "transport": {"native_content_decoding": True},
        },
        bodies=bodies,
    )


async def _publish(database: Database, checkpoint: HttpCheckpoint) -> None:
    operation = str(uuid4())
    await database.begin_operation(
        operation_id=operation,
        component=_COMPONENT,
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-11-01T12:00:00+00:00",
    )
    await database.complete_operation(
        operation_id=operation,
        records=checkpoint.records,
        artifacts=checkpoint.artifacts,
        outputs=checkpoint.outputs,
        result={},
        ended_at_utc="2026-11-01T12:00:01+00:00",
        duration_ns=1_000_000_000,
    )


@pytest.mark.parametrize(
    "content", [(), (b"",), (gzip.compress(b"raw wire\x00\xff"), b"native\x00\xffdecoded")]
)
async def test_exact_acquisition_round_trip_uses_only_external_body_files(
    tmp_path: Path, content: tuple[bytes, ...]
) -> None:
    acquisition = _acquisition(*content)
    checkpoint = await checkpoint_acquisition(
        acquisition, data_directory=tmp_path, new_identifier=lambda: "checkpoint"
    )
    assert checkpoint.record_identifier == "checkpoint"
    assert checkpoint.records[0].kind == ("carl", "http", "stage_acquisition")
    assert checkpoint.records[0].schema_version == 1
    assert checkpoint.records[0].value == {
        "acquisition": acquisition.record,
        "body_identifiers": [body.identifier for body in acquisition.bodies],
    }
    assert len({output.name for output in checkpoint.outputs}) == len(checkpoint.outputs)
    for artifact, original in zip(checkpoint.artifacts, acquisition.bodies, strict=True):
        assert artifact.identifier == original.identifier
        assert artifact.representation == original.representation
        assert artifact.media_type == original.media_type
        assert artifact.sha256 == hashlib.sha256(original.content).hexdigest()
        assert artifact.size == len(original.content)
        path = tmp_path / artifact.locator
        assert path.read_bytes() == original.content
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        await _publish(database, checkpoint)
        assert await restore_acquisition(database, "checkpoint") == acquisition
        async with database._connections.reader() as connection:
            cursor = await connection.execute("SELECT count(*) FROM content")
            assert await cursor.fetchall() == [(0,)]
            cursor = await connection.execute("SELECT count(*) FROM artifacts")
            assert await cursor.fetchall() == [(0,)]
            cursor = await connection.execute("SELECT count(*) FROM external_artifacts")
            assert await cursor.fetchall() == [(len(content),)]


async def test_checkpoint_copies_record_and_body_representation(tmp_path: Path) -> None:
    acquisition = _acquisition(b"bytes")
    checkpoint = await checkpoint_acquisition(
        acquisition, data_directory=tmp_path, new_identifier=lambda: "checkpoint"
    )
    acquisition.record["transport"] = "changed"
    acquisition.bodies[0].representation["transport"] = "changed"
    value = cast(dict[str, JsonValue], checkpoint.records[0].value)
    assert cast(dict[str, JsonValue], value["acquisition"])["transport"] == {
        "native_content_decoding": True
    }
    assert checkpoint.artifacts[0].representation != acquisition.bodies[0].representation


async def test_concurrent_identical_bodies_are_atomically_deduplicated(tmp_path: Path) -> None:
    results: list[HttpCheckpoint] = []

    async def checkpoint(index: int) -> None:
        source = _acquisition(b"same exact binary\x00\xff")
        source = Acquisition(
            source.record, (AcquiredBody(f"body-{index}", None, {}, source.bodies[0].content),)
        )
        results.append(
            await checkpoint_acquisition(
                source, data_directory=tmp_path, new_identifier=lambda: str(uuid4())
            )
        )

    async with anyio.create_task_group() as tasks:
        for index in range(8):
            _ = tasks.start_soon(checkpoint, index)
    paths = {result.artifacts[0].locator for result in results}
    assert len(paths) == 1
    path = tmp_path / paths.pop()
    assert path.read_bytes() == b"same exact binary\x00\xff"
    assert path.stat().st_nlink == 1
    assert len(tuple(tmp_path.rglob("*.body"))) == 1
    assert not tuple(tmp_path.rglob("*.tmp"))


async def test_existing_corrupt_content_is_rejected_without_overwriting(tmp_path: Path) -> None:
    source = _acquisition(b"valid")
    checkpoint = await checkpoint_acquisition(
        source, data_directory=tmp_path, new_identifier=lambda: "first"
    )
    path = tmp_path / checkpoint.artifacts[0].locator
    _ = path.write_bytes(b"wrong")
    with pytest.raises(ValueError, match="integrity"):
        _ = await checkpoint_acquisition(
            source, data_directory=tmp_path, new_identifier=lambda: "second"
        )
    assert path.read_bytes() == b"wrong"
    assert not tuple(tmp_path.rglob("*.tmp"))


@pytest.mark.parametrize(
    "failure",
    [
        "missing",
        "corrupt",
        "grown",
        "permissions",
        "file_symlink",
        "bucket_symlink",
        "root_symlink",
    ],
)
async def test_restore_rejects_missing_corrupt_or_symlinked_body_files(
    tmp_path: Path, failure: str
) -> None:
    checkpoint = await checkpoint_acquisition(
        _acquisition(b"valid"), data_directory=tmp_path, new_identifier=lambda: "checkpoint"
    )
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        await _publish(database, checkpoint)
        path = tmp_path / checkpoint.artifacts[0].locator
        if failure == "missing":
            path.unlink()
        elif failure == "corrupt":
            _ = path.write_bytes(b"wrong")
        elif failure == "grown":
            with path.open("r+b") as content:
                _ = content.truncate(checkpoints.MAX_CHECKPOINT_BODY_BYTES + 1)
        elif failure == "permissions":
            path.chmod(0o644)
        elif failure == "file_symlink":
            moved = tmp_path / "elsewhere.body"
            _ = path.rename(moved)
            path.symlink_to(moved)
        elif failure == "bucket_symlink":
            bucket = path.parent
            moved = tmp_path / "elsewhere-bucket"
            _ = bucket.rename(moved)
            bucket.symlink_to(moved, target_is_directory=True)
        else:
            root = tmp_path / "responses"
            moved = tmp_path / "elsewhere-responses"
            _ = root.rename(moved)
            root.symlink_to(moved, target_is_directory=True)
        with pytest.raises((OSError, ValueError)):
            _ = await restore_acquisition(database, "checkpoint")


@pytest.mark.parametrize("component", ["responses", "sha256", "bucket", "destination"])
async def test_publish_does_not_follow_storage_symlinks(tmp_path: Path, component: str) -> None:
    body = b"fixture"
    digest = hashlib.sha256(body).hexdigest()
    outside = tmp_path / "outside"
    outside.mkdir()
    responses = tmp_path / "responses"
    if component == "responses":
        responses.symlink_to(outside, target_is_directory=True)
    else:
        responses.mkdir()
        sha = responses / "sha256"
        if component == "sha256":
            sha.symlink_to(outside, target_is_directory=True)
        else:
            sha.mkdir()
            bucket = sha / digest[:2]
            if component == "bucket":
                bucket.symlink_to(outside, target_is_directory=True)
            else:
                bucket.mkdir()
                target = outside / "unrelated"
                _ = target.write_bytes(body)
                (bucket / f"{digest}.body").symlink_to(target)
    with pytest.raises((OSError, ValueError)):
        _ = await checkpoint_acquisition(
            _acquisition(body), data_directory=tmp_path, new_identifier=lambda: "checkpoint"
        )
    assert not tuple(outside.glob("*.body"))


async def test_aggregate_body_bound_is_checked_before_any_file_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(checkpoints, "MAX_CHECKPOINT_BODY_BYTES", 7)
    with pytest.raises(ValueError, match="aggregate"):
        _ = await checkpoint_acquisition(
            _acquisition(b"1234", b"5678"),
            data_directory=tmp_path,
            new_identifier=lambda: "checkpoint",
        )
    assert not (tmp_path / "responses").exists()


async def test_empty_body_count_is_bounded_before_any_file_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(checkpoints, "MAX_CHECKPOINT_BODIES", 1)
    with pytest.raises(ValueError, match="aggregate"):
        _ = await checkpoint_acquisition(
            _acquisition(b"", b""), data_directory=tmp_path, new_identifier=lambda: "checkpoint"
        )
    assert not (tmp_path / "responses").exists()


async def test_restore_checks_aggregate_metadata_before_any_body_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkpoint = await checkpoint_acquisition(
        _acquisition(b"1234", b"5678"), data_directory=tmp_path, new_identifier=lambda: "checkpoint"
    )
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        await _publish(database, checkpoint)
        monkeypatch.setattr(checkpoints, "MAX_CHECKPOINT_BODY_BYTES", 7)

        def forbidden_read(root: Path, drafts: object) -> tuple[AcquiredBody, ...]:
            del root, drafts
            pytest.fail("body bytes were read before validating aggregate metadata")

        monkeypatch.setattr(checkpoints, "_restore_bodies", forbidden_read)
        with pytest.raises(ValueError, match="aggregate"):
            _ = await restore_acquisition(database, "checkpoint")


@pytest.mark.parametrize("field", ["size", "sha256", "locator", "representation"])
async def test_restore_validates_body_metadata_before_opening_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    checkpoint = await checkpoint_acquisition(
        _acquisition(b"body"), data_directory=tmp_path, new_identifier=lambda: "checkpoint"
    )
    replacements: dict[str, object] = {
        "size": checkpoints.MAX_CHECKPOINT_BODY_BYTES + 1,
        "sha256": "f" * 64,
        "locator": "images/sha256/not-a-response.body",
        "representation": [],
    }
    malformed = checkpoint.artifacts[0].model_copy(update={field: replacements[field]})
    altered = HttpCheckpoint(
        checkpoint.record_identifier, checkpoint.records, (malformed,), checkpoint.outputs
    )
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        await _publish(database, altered)

        def forbidden_read(root: Path, drafts: object) -> tuple[AcquiredBody, ...]:
            del root, drafts
            pytest.fail("body files were opened before validating stored metadata")

        monkeypatch.setattr(checkpoints, "_restore_bodies", forbidden_read)
        with pytest.raises(ValueError):
            _ = await restore_acquisition(database, "checkpoint")


@pytest.mark.parametrize("change", ["kind", "schema", "missing", "duplicate", "identifier_type"])
async def test_restore_rejects_invalid_checkpoint_records(tmp_path: Path, change: str) -> None:
    checkpoint = await checkpoint_acquisition(
        _acquisition(b"body"), data_directory=tmp_path, new_identifier=lambda: "checkpoint"
    )
    record = checkpoint.records[0]
    value: dict[str, JsonValue] = {"acquisition": {}, "body_identifiers": ["body-0"]}
    if change == "missing":
        value["body_identifiers"] = ["missing"]
    elif change == "duplicate":
        value["body_identifiers"] = ["body-0", "body-0"]
    elif change == "identifier_type":
        value["body_identifiers"] = [1]
    record = RecordDraft(
        identifier=record.identifier,
        kind=("wrong",) if change == "kind" else record.kind,
        schema_version=2 if change == "schema" else 1,
        value=value,
    )
    altered = HttpCheckpoint(
        checkpoint.record_identifier, (record,), checkpoint.artifacts, checkpoint.outputs
    )
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        await _publish(database, altered)
        with pytest.raises(ValueError):
            _ = await restore_acquisition(database, "checkpoint")


@pytest.mark.parametrize("phase", ["write", "fsync", "link"])
async def test_storage_failure_never_leaves_a_partial_destination_or_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    write = os.write
    fsync = os.fsync
    link = os.link

    def fail_write(descriptor: int, content: bytes) -> int:
        del descriptor, content
        raise OSError("synthetic disk write failure")

    def fail_fsync(descriptor: int) -> None:
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError("synthetic file sync failure")
        fsync(descriptor)

    def fail_link(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError("synthetic link failure")

    if phase == "write":
        monkeypatch.setattr(os, "write", fail_write)
    elif phase == "fsync":
        monkeypatch.setattr(os, "fsync", fail_fsync)
    else:
        monkeypatch.setattr(os, "link", fail_link)
    with pytest.raises(OSError, match="synthetic"):
        _ = await checkpoint_acquisition(
            _acquisition(b"body"), data_directory=tmp_path, new_identifier=lambda: "checkpoint"
        )
    monkeypatch.setattr(os, "write", write)
    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "link", link)
    assert not tuple(tmp_path.rglob("*.body"))
    assert not tuple(tmp_path.rglob("*.tmp"))


async def test_cancelled_write_finishes_atomically_before_cancellation_is_exposed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_started = threading.Event()
    release_write = threading.Event()
    finished = anyio.Event()
    scope = anyio.CancelScope()
    write = os.write
    returned = False

    def blocking_write(descriptor: int, content: bytes) -> int:
        write_started.set()
        assert release_write.wait(5), "test did not release storage thread"
        return write(descriptor, content)

    async def checkpoint() -> None:
        nonlocal returned
        with scope:
            _ = await checkpoint_acquisition(
                _acquisition(b"exact body"),
                data_directory=tmp_path,
                new_identifier=lambda: "checkpoint",
            )
            returned = True
        finished.set()

    monkeypatch.setattr(os, "write", blocking_write)
    async with anyio.create_task_group() as tasks:
        try:
            _ = tasks.start_soon(checkpoint)
            with anyio.fail_after(2):
                while not write_started.is_set():
                    await anyio.sleep(0.01)
            scope.cancel()
            await anyio.sleep(0.05)
            assert not finished.is_set()
            assert not tuple(tmp_path.rglob("*.body"))
        finally:
            release_write.set()
        with anyio.fail_after(2):
            await finished.wait()
    monkeypatch.setattr(os, "write", write)
    assert not returned
    assert not tuple(tmp_path.rglob("*.tmp"))
    paths = tuple(tmp_path.rglob("*.body"))
    assert len(paths) == 1
    assert paths[0].read_bytes() == b"exact body"
