"""Bounded transport checkpoints backed by atomic content-addressed body files."""

import hashlib
import os
import secrets
import stat
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import cast

import anyio

from carl.core.json import decode_json, encode_json
from carl.core.models import ExternalFileDraft, JsonValue, NamedOutput, RecordDraft
from carl.io.httpx import AcquiredBody, Acquisition
from carl.io.sqlite import Database

MAX_CHECKPOINT_BODY_BYTES = 100_000_000
MAX_CHECKPOINT_BODIES = 1000
CHECKPOINT_KIND = ("carl", "http", "stage_acquisition")
BODY_KIND = ("carl", "http", "response_body")
_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class HttpCheckpoint:
    record_identifier: str
    records: tuple[RecordDraft, ...]
    artifacts: tuple[ExternalFileDraft, ...]
    outputs: tuple[NamedOutput, ...]


def _owned_directory(descriptor: int) -> None:
    info = os.fstat(descriptor)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("HTTP response storage requires owned directories")


def _open_directory(parent: int, name: str, *, create: bool) -> int:
    if create:
        with suppress(FileExistsError):
            os.mkdir(name, mode=0o700, dir_fd=parent)
            os.fsync(parent)
    descriptor = os.open(
        name, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
    )
    try:
        _owned_directory(descriptor)
        if create:
            os.fchmod(descriptor, 0o700)
            os.fsync(descriptor)
        elif stat.S_IMODE(os.fstat(descriptor).st_mode) & 0o077:
            raise ValueError("HTTP response storage directory is not private")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _open_bucket(root: Path, digest: str, *, create: bool) -> int:
    if create:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(root, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        _owned_directory(descriptor)
        for name in ("responses", "sha256", digest[:2]):
            child = _open_directory(descriptor, name, create=create)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_body(bucket: int, name: str, *, digest: str, size: int, retain: bool) -> bytes:
    descriptor = os.open(
        name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=bucket
    )
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_size != size
            or stat.S_IMODE(before.st_mode) & 0o077
        ):
            raise ValueError("HTTP checkpoint body failed file metadata validation")
        observed = 0
        content = bytearray()
        checksum = hashlib.sha256()
        while observed <= size:
            chunk = os.read(descriptor, min(_CHUNK_BYTES, size + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            if observed > size:
                raise ValueError("HTTP checkpoint body exceeds its declared size")
            checksum.update(chunk)
            if retain:
                content.extend(chunk)
        after = os.fstat(descriptor)
        if (
            observed != size
            or checksum.hexdigest() != digest
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        ):
            raise ValueError("HTTP checkpoint body failed integrity verification")
        return bytes(content)
    finally:
        os.close(descriptor)


def _publish_body(
    root: Path, body: AcquiredBody, *, kind: tuple[str, ...] = BODY_KIND
) -> ExternalFileDraft:
    digest = hashlib.sha256(body.content).hexdigest()
    size = len(body.content)
    name = f"{digest}.body"
    bucket = _open_bucket(root, digest, create=True)
    try:
        try:
            _ = _read_body(bucket, name, digest=digest, size=size, retain=False)
        except FileNotFoundError:
            temporary = f".{digest}.{secrets.token_hex(16)}.tmp"
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                0o600,
                dir_fd=bucket,
            )
            try:
                try:
                    content = memoryview(body.content)
                    offset = 0
                    while offset < size:
                        written = os.write(descriptor, content[offset : offset + _CHUNK_BYTES])
                        if written <= 0:
                            raise OSError("HTTP checkpoint body write made no progress")
                        offset += written
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                try:
                    os.link(
                        temporary,
                        name,
                        src_dir_fd=bucket,
                        dst_dir_fd=bucket,
                        follow_symlinks=False,
                    )
                except FileExistsError:
                    _ = _read_body(bucket, name, digest=digest, size=size, retain=False)
                os.fsync(bucket)
            finally:
                os.unlink(temporary, dir_fd=bucket)
                os.fsync(bucket)
        return ExternalFileDraft(
            identifier=body.identifier,
            kind=kind,
            media_type=body.media_type,
            representation=decode_json(encode_json(body.representation)),
            sha256=digest,
            size=size,
            locator=f"responses/sha256/{digest[:2]}/{name}",
        )
    finally:
        os.close(bucket)


def _publish_bodies(root: Path, bodies: tuple[AcquiredBody, ...]) -> tuple[ExternalFileDraft, ...]:
    return tuple(_publish_body(root, body) for body in bodies)


async def publish_checkpoint_artifact(
    content: bytes,
    *,
    data_directory: Path,
    identifier: str,
    kind: tuple[str, ...],
    media_type: str,
    representation: dict[str, JsonValue],
    maximum_bytes: int,
) -> ExternalFileDraft:
    """Reuse private atomic checkpoint storage for a separately typed bounded artifact."""
    if not 0 < maximum_bytes <= MAX_CHECKPOINT_BODY_BYTES or len(content) > maximum_bytes:
        raise ValueError("Checkpoint artifact exceeds its byte bound")
    artifact = await anyio.to_thread.run_sync(
        partial(
            _publish_body,
            data_directory.resolve(),
            AcquiredBody(identifier, media_type, representation, content),
            kind=kind,
        ),
        abandon_on_cancel=False,
    )
    await anyio.lowlevel.checkpoint()
    return artifact


def _validate_body_bounds(sizes: Sequence[int]) -> None:
    if len(sizes) > MAX_CHECKPOINT_BODIES or sum(sizes) > MAX_CHECKPOINT_BODY_BYTES:
        raise ValueError("HTTP checkpoint bodies exceed their aggregate bound")


async def checkpoint_acquisition(
    acquisition: Acquisition,
    *,
    data_directory: Path,
    new_identifier: Callable[[], str],
) -> HttpCheckpoint:
    """Persist immutable body files; return evidence drafts for atomic stage suspension.

    ``data_directory`` is the evidence database's parent directory.
    Cancellation or a later publication failure may leave a valid unreferenced
    content-addressed file. Individual files are always complete and reusable.
    """

    _validate_body_bounds(tuple(len(body.content) for body in acquisition.bodies))
    identifiers = tuple(body.identifier for body in acquisition.bodies)
    if len(identifiers) != len(set(identifiers)) or any(
        not identifier for identifier in identifiers
    ):
        raise ValueError("HTTP checkpoint body identifiers must be nonempty and unique")
    record_identifier = new_identifier()
    if record_identifier in identifiers:
        raise ValueError("HTTP checkpoint record and body identifiers must differ")
    record = RecordDraft(
        identifier=record_identifier,
        kind=CHECKPOINT_KIND,
        schema_version=1,
        value={
            "acquisition": decode_json(encode_json(acquisition.record)),
            "body_identifiers": list(identifiers),
        },
    )
    bodies: list[AcquiredBody] = []
    for body in acquisition.bodies:
        representation = decode_json(encode_json(body.representation))
        if not isinstance(representation, dict):
            raise ValueError("HTTP body representation must be an object")
        bodies.append(
            AcquiredBody(
                body.identifier,
                body.media_type,
                cast(dict[str, JsonValue], representation),
                body.content,
            )
        )
    artifacts = await anyio.to_thread.run_sync(
        _publish_bodies, data_directory.resolve(), tuple(bodies), abandon_on_cancel=False
    )
    await anyio.lowlevel.checkpoint()
    return HttpCheckpoint(
        record_identifier,
        (record,),
        artifacts,
        (
            NamedOutput(name=("stage_acquisition",), object_identifier=record_identifier),
            *(
                NamedOutput(
                    name=("stage_acquisition", "response", str(index), "body"),
                    object_identifier=identifier,
                )
                for index, identifier in enumerate(identifiers)
            ),
        ),
    )


async def _external_metadata(
    database: Database, identifiers: tuple[str, ...]
) -> tuple[ExternalFileDraft, ...]:
    # Metadata must be checked before any bytes are read. The general artifact
    # reader supports legacy storage and currently reads before checking size.
    drafts: list[ExternalFileDraft] = []
    for identifier in identifiers:
        metadata = await database.get_external_artifact_metadata(identifier)
        if metadata is None:
            raise ValueError("HTTP checkpoint references a missing or nonexternal body artifact")
        digest = metadata.get("sha256")
        size = metadata.get("size")
        media_type = metadata.get("media_type")
        representation = metadata.get("representation")
        storage_value = metadata.get("storage")
        storage = (
            cast(dict[str, JsonValue], storage_value) if isinstance(storage_value, dict) else None
        )
        if (
            not isinstance(digest, str)
            or type(size) is not int
            or (media_type is not None and not isinstance(media_type, str))
            or not isinstance(representation, dict)
            or storage is None
            or storage.get("backend") != "filesystem"
            or not isinstance(storage.get("locator"), str)
        ):
            raise ValueError("HTTP checkpoint artifact metadata is malformed")
        draft = ExternalFileDraft(
            identifier=identifier,
            kind=BODY_KIND,
            media_type=media_type,
            representation=representation,
            sha256=digest,
            size=size,
            locator=cast(str, storage["locator"]),
        )
        expected = f"responses/sha256/{digest[:2]}/{digest}.body"
        if draft.locator != expected or not isinstance(draft.representation, dict):
            raise ValueError("HTTP checkpoint artifact is not a response body file")
        drafts.append(draft)
    _validate_body_bounds(tuple(item.size for item in drafts))
    return tuple(drafts)


def _restore_bodies(root: Path, drafts: tuple[ExternalFileDraft, ...]) -> tuple[AcquiredBody, ...]:
    bodies: list[AcquiredBody] = []
    for draft in drafts:
        bucket = _open_bucket(root, draft.sha256, create=False)
        try:
            content = _read_body(
                bucket,
                f"{draft.sha256}.body",
                digest=draft.sha256,
                size=draft.size,
                retain=True,
            )
        finally:
            os.close(bucket)
        bodies.append(
            AcquiredBody(
                draft.identifier,
                draft.media_type,
                cast(dict[str, JsonValue], draft.representation),
                content,
            )
        )
    return tuple(bodies)


async def restore_acquisition(database: Database, identifier: str) -> Acquisition:
    """Verify checkpoint identity and metadata before bounded external file reads."""

    kind, schema, value = await database.get_record(identifier)
    if kind != CHECKPOINT_KIND or schema != 1 or not isinstance(value, dict):
        raise ValueError("Not a supported HTTP acquisition checkpoint")
    checkpoint = cast(dict[str, JsonValue], value)
    acquisition = checkpoint.get("acquisition")
    identifiers_value = checkpoint.get("body_identifiers")
    identifiers = (
        cast(list[JsonValue], identifiers_value) if isinstance(identifiers_value, list) else None
    )
    if (
        not isinstance(acquisition, dict)
        or identifiers is None
        or len(identifiers) > MAX_CHECKPOINT_BODIES
        or not all(isinstance(item, str) and item for item in identifiers)
    ):
        raise ValueError("HTTP acquisition checkpoint is malformed")
    body_identifiers = tuple(cast(list[str], identifiers))
    if len(set(body_identifiers)) != len(body_identifiers):
        raise ValueError("HTTP acquisition checkpoint repeats a body artifact")
    metadata = await _external_metadata(database, body_identifiers)
    bodies = await anyio.to_thread.run_sync(
        _restore_bodies, database.path.resolve().parent, metadata, abandon_on_cancel=False
    )
    await anyio.lowlevel.checkpoint()
    record = cast(dict[str, JsonValue], acquisition)
    return Acquisition(cast(dict[str, JsonValue], decode_json(encode_json(record))), bodies)
