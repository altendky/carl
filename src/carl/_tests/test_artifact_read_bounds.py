"""Artifact metadata and bounded reads reject oversized content before allocation."""

# Corruption and query-boundary tests intentionally inspect storage internals.
# pyright: reportPrivateUsage=false

import hashlib
from pathlib import Path
from typing import cast

import pytest

import carl.io.sqlite as sqlite
from carl._tests.test_stage_storage import COMPONENT
from carl._tests.test_worker import _provenance
from carl.core.models import BytesDraft, ExternalFileDraft
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


async def _publish(database: Database, *, content: bytes, external: bool) -> Path | None:
    artifact: BytesDraft | ExternalFileDraft
    path = database.path.parent / "body.bin" if external else None
    if path is not None:
        path.write_bytes(content)
        artifact = ExternalFileDraft(
            identifier="body",
            kind=("test", "image"),
            media_type="image/png",
            representation={"legacy": True},
            sha256=hashlib.sha256(content).hexdigest(),
            size=len(content),
            locator=path.name,
        )
    else:
        artifact = BytesDraft(
            identifier="body",
            kind=("test", "image"),
            media_type="image/png",
            representation={"legacy": True},
            content=content,
        )
    await database.begin_operation(
        operation_id="seed",
        component=COMPONENT,
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-11-01T12:00:00+00:00",
    )
    await database.complete_operation(
        operation_id="seed",
        records=(),
        artifacts=(artifact,),
        outputs=(),
        result={},
        ended_at_utc="2026-11-01T12:00:01+00:00",
        duration_ns=1_000_000_000,
    )
    return path


@pytest.mark.parametrize("external", (False, True))
@pytest.mark.parametrize("content", (b"", b"image\x00\xff"))
async def test_metadata_matches_full_read_without_loading_content(
    tmp_path: Path, external: bool, content: bytes, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        path = await _publish(database, content=content, external=external)
        expected, value = await database.get_artifact("body", maximum_bytes=len(content))
        assert value == content
        assert await database.get_artifact("body") == (expected, content)
        verified = await database.get_external_artifact_path("body", maximum_bytes=len(content))
        assert verified == (expected, path) if path is not None else verified is None
        if path is not None:
            path.unlink()

        def no_content_read(*_args: object) -> None:
            raise AssertionError("Metadata read touched filesystem content")

        monkeypatch.setattr(sqlite, "_read_external_artifact", no_content_read)
        statements: list[str] = []

        def trace(_cursor: object, statement: str, _bindings: object) -> bool:
            statements.append(statement)
            return True

        async with database._connections.reader() as connection:
            connection.set_exec_trace(trace)
            try:
                assert await database.get_artifact_metadata("body") == expected
            finally:
                connection.set_exec_trace(None)
        assert not any("inline_bytes" in statement for statement in statements)
        with pytest.raises(KeyError, match="missing"):
            await database.get_artifact_metadata("missing")


@pytest.mark.parametrize("corrupt_length", (False, True))
async def test_bounded_inline_read_checks_declared_and_actual_lengths_before_blob_select(
    tmp_path: Path, corrupt_length: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, content=b"image", external=False)
        if corrupt_length:
            async with database._connections.writer() as connection:
                await connection.execute("PRAGMA ignore_check_constraints = ON")
                await connection.execute("UPDATE content SET inline_bytes = ?", (b"x" * 100,))
                await connection.execute("PRAGMA ignore_check_constraints = OFF")
        statements: list[str] = []

        def trace(_cursor: object, statement: str, _bindings: object) -> bool:
            statements.append(statement)
            return True

        async with database._connections.reader() as connection:
            connection.set_exec_trace(trace)
            try:
                with pytest.raises(ValueError, match="maximum_bytes"):
                    await database.get_artifact("body", maximum_bytes=5 if corrupt_length else 4)
            finally:
                connection.set_exec_trace(None)
        reads = [statement for statement in statements if "inline_bytes" in statement]
        assert reads and all("length(content.inline_bytes)" in statement for statement in reads)
        if corrupt_length:
            with pytest.raises(ValueError, match="integrity verification"):
                await database.get_artifact("body")


async def test_external_declared_size_is_rejected_before_file_lookup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, content=b"image", external=True)

        def no_lookup(*_args: object) -> None:
            raise AssertionError("Oversized declaration reached file lookup")

        monkeypatch.setattr(sqlite, "_external_artifact_path", no_lookup)
        with pytest.raises(ValueError, match="maximum_bytes"):
            await database.get_artifact("body", maximum_bytes=4)


async def test_external_actual_size_is_rejected_before_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        path = await _publish(database, content=b"image", external=True)
        assert path is not None
        path.write_bytes(b"x" * 100)

        def no_read(*_args: object) -> bytes:
            raise AssertionError("Oversized physical file reached read")

        monkeypatch.setattr(sqlite.os, "read", no_read)
        with pytest.raises(ValueError, match="maximum_bytes"):
            await database.get_artifact("body", maximum_bytes=5)


async def test_external_growth_after_stat_cannot_escape_the_read_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        path = await _publish(database, content=b"image", external=True)
        assert path is not None
        real_stat = sqlite.os.fstat
        real_read = sqlite.os.read
        requested: list[int] = []

        def grow_after_stat(descriptor: int):
            info = real_stat(descriptor)
            with path.open("ab") as stream:
                stream.write(b"x" * 100)
            return info

        def bounded_read(descriptor: int, length: int) -> bytes:
            requested.append(length)
            return real_read(descriptor, length)

        monkeypatch.setattr(sqlite.os, "fstat", grow_after_stat)
        monkeypatch.setattr(sqlite.os, "read", bounded_read)
        with pytest.raises(ValueError, match="maximum_bytes"):
            await database.get_artifact("body", maximum_bytes=5)
        assert requested == [6]


@pytest.mark.parametrize("maximum_bytes", (-1, True, 1.5))
async def test_invalid_artifact_read_bounds_are_rejected(
    tmp_path: Path, maximum_bytes: object
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        with pytest.raises(ValueError, match="nonnegative integer"):
            await database.get_artifact("missing", maximum_bytes=cast(int, maximum_bytes))
        with pytest.raises(ValueError, match="nonnegative integer"):
            await database.get_external_artifact_path(
                "missing", maximum_bytes=cast(int, maximum_bytes)
            )
