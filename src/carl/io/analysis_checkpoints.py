"""Private, bounded external files for durable analysis inputs and raw capture."""

import hashlib
import os
import secrets
import stat
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Literal, cast

import anyio
from pydantic import Field, model_validator

from carl.core.analysis_stages import (
    MAX_ANALYSIS_INPUT_BYTES,
    MAX_ANALYSIS_INPUT_FILES,
    MAX_ANALYSIS_OUTPUT_BYTES,
    MAX_ANALYSIS_STDERR_BYTES,
    MAX_ANALYSIS_STDOUT_BYTES,
    MAX_ANALYSIS_TEXT_BYTES,
    AnalysisImage,
    AnalysisInputManifest,
    PreparedAnalysis,
)
from carl.core.json import decode_json, encode_json
from carl.core.models import (
    ExternalFileDraft,
    JsonValue,
    NamedInput,
    NamedOutput,
    RecordDraft,
    StrictModel,
)
from carl.io.claude import ClaudeCapture
from carl.io.sqlite import Database

INPUT_CHECKPOINT_KIND = ("carl", "analysis", "input_checkpoint")
CAPTURE_CHECKPOINT_KIND = ("carl", "analysis", "capture_checkpoint")
MAX_ANALYSIS_ARGV_BYTES = 8 * 1024 * 1024
_CHUNK = 1024 * 1024


@dataclass(frozen=True, slots=True)
class AnalysisCheckpoint:
    record_identifier: str
    records: tuple[RecordDraft, ...]
    artifacts: tuple[ExternalFileDraft, ...]
    outputs: tuple[NamedOutput, ...]


class AnalysisCaptureManifest(StrictModel):
    input_checkpoint_identifier: str = Field(min_length=1)
    argv_identifier: str = Field(min_length=1)
    stdout_identifier: str = Field(min_length=1)
    stderr_identifier: str = Field(min_length=1)
    started_at_utc_ns: int = Field(ge=0)
    ended_at_utc_ns: int = Field(ge=0)
    duration_ns: int = Field(ge=0)
    exit_code: int | None
    timed_out: bool
    output_limited: bool
    launch_failed: bool

    @model_validator(mode="after")
    def validate_timing_and_identifiers(self) -> "AnalysisCaptureManifest":
        if self.ended_at_utc_ns < self.started_at_utc_ns:
            raise ValueError("Analysis capture ended before it started")
        identifiers = (self.argv_identifier, self.stdout_identifier, self.stderr_identifier)
        if len(set(identifiers)) != 3 or self.input_checkpoint_identifier in identifiers:
            raise ValueError("Analysis capture artifact identities must be unique")
        return self


class _StoredAnalysisInputs(StrictModel):
    marketplace: Literal["facebook", "ebay"]
    configuration_identifier: str = Field(min_length=1)
    inputs: tuple[NamedInput, ...]
    images: tuple[AnalysisImage, ...] = Field(max_length=MAX_ANALYSIS_INPUT_FILES)
    listing_input_identifier: str = Field(min_length=1)
    prompt_identifier: str = Field(min_length=1)


def _owned_directory(descriptor: int, *, private: bool = True) -> None:
    info = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or (private and stat.S_IMODE(info.st_mode) & 0o077)
    ):
        raise ValueError("Analysis files require private, owned directories")


def _directory(parent: int, name: str, *, create: bool) -> int:
    if create:
        with suppress(FileExistsError):
            os.mkdir(name, mode=0o700, dir_fd=parent)
            os.fsync(parent)
    descriptor = os.open(
        name, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
    )
    try:
        _owned_directory(descriptor)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _bucket(root: Path, parts: tuple[str, ...], *, create: bool) -> int:
    if create:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(root, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        _owned_directory(descriptor, private=False)
        for part in parts:
            child = _directory(descriptor, part, create=create)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_at(bucket: int, filename: str, *, size: int, digest: str, retain: bool) -> bytes:
    descriptor = os.open(
        filename, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=bucket
    )
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_size != size
            or stat.S_IMODE(before.st_mode) & 0o077
        ):
            raise ValueError("Analysis file failed metadata validation")
        observed, checksum, content = 0, hashlib.sha256(), bytearray()
        while observed <= size:
            chunk = os.read(descriptor, min(_CHUNK, size + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            if observed > size:
                raise ValueError("Analysis file exceeds its declared byte length")
            checksum.update(chunk)
            if retain:
                content.extend(chunk)
        after = os.fstat(descriptor)
        if (
            observed != size
            or checksum.hexdigest() != digest
            or (before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_ino, after.st_size, after.st_mtime_ns)
        ):
            raise ValueError("Analysis file failed integrity verification")
        return bytes(content)
    finally:
        os.close(descriptor)


def _read_external(root: Path, metadata: dict[str, JsonValue], *, retain: bool) -> bytes:
    storage = metadata.get("storage")
    locator = storage.get("locator") if isinstance(storage, dict) else None
    if not isinstance(locator, str):
        raise ValueError("Analysis checkpoint requires an external file")
    path = PurePosixPath(locator)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {".", ".."} for part in path.parts)
        or locator != str(path)
        or "\\" in locator
        or len(path.parts) > 16
    ):
        raise ValueError("Invalid analysis external locator")
    size, digest = metadata.get("size"), metadata.get("sha256")
    if (
        type(size) is not int
        or size < 0
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(c not in "0123456789abcdef" for c in digest)
    ):
        raise ValueError("Invalid analysis external metadata")
    bucket = _bucket(root, path.parts[:-1], create=False)
    try:
        return _read_at(bucket, path.name, size=size, digest=digest, retain=retain)
    finally:
        os.close(bucket)


def _publish(
    root: Path,
    identifier: str,
    kind: tuple[str, ...],
    media_type: str,
    representation: dict[str, JsonValue],
    content: bytes,
) -> ExternalFileDraft:
    digest, size = hashlib.sha256(content).hexdigest(), len(content)
    filename = digest + ".blob"
    bucket = _bucket(root, ("analysis", "sha256", digest[:2]), create=True)
    try:
        try:
            _ = _read_at(bucket, filename, size=size, digest=digest, retain=False)
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
                    offset, view = 0, memoryview(content)
                    while offset < size:
                        count = os.write(descriptor, view[offset : offset + _CHUNK])
                        if count <= 0:
                            raise OSError("Analysis file write made no progress")
                        offset += count
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                try:
                    os.link(
                        temporary,
                        filename,
                        src_dir_fd=bucket,
                        dst_dir_fd=bucket,
                        follow_symlinks=False,
                    )
                except FileExistsError:
                    _ = _read_at(bucket, filename, size=size, digest=digest, retain=False)
                os.fsync(bucket)
            finally:
                os.unlink(temporary, dir_fd=bucket)
                os.fsync(bucket)
        return ExternalFileDraft(
            identifier=identifier,
            kind=kind,
            media_type=media_type,
            representation=representation,
            sha256=digest,
            size=size,
            locator=f"analysis/sha256/{digest[:2]}/{filename}",
        )
    finally:
        os.close(bucket)


def _input_bounds(prepared: PreparedAnalysis) -> None:
    if (
        len(prepared.images) > MAX_ANALYSIS_INPUT_FILES
        or max(len(prepared.prompt), len(prepared.listing_input)) > MAX_ANALYSIS_TEXT_BYTES
        or len(prepared.prompt)
        + len(prepared.listing_input)
        + sum(image.byte_length for image in prepared.images)
        > MAX_ANALYSIS_INPUT_BYTES
    ):
        raise ValueError("Analysis inputs exceed their byte or file bound")


async def _metadata(
    database: Database, identifier: str, maximum: int, *, expected: AnalysisImage | None = None
) -> dict[str, JsonValue]:
    metadata = await database.get_external_artifact_metadata(identifier)
    if (
        metadata is None
        or type(metadata.get("size")) is not int
        or not 0 <= metadata["size"] <= maximum
    ):
        raise ValueError("Analysis checkpoint references missing or oversized external content")
    if expected is not None and (
        metadata["size"] != expected.byte_length
        or metadata.get("sha256") != expected.sha256
        or metadata.get("media_type") != expected.media_type
    ):
        raise ValueError("Analysis image metadata differs from its prepared input")
    return metadata


async def checkpoint_analysis_inputs(
    database: Database,
    prepared: PreparedAnalysis,
    *,
    marketplace: Literal["facebook", "ebay"],
    configuration: dict[str, JsonValue],
    new_identifier: Callable[[], str],
) -> AnalysisCheckpoint:
    """Validate all sizes first, then externalize exact inputs for one CPU stage."""
    _input_bounds(prepared)
    encoded_configuration = encode_json(configuration)
    configuration_bytes = encoded_configuration.encode()
    if (
        len(configuration_bytes) > MAX_ANALYSIS_TEXT_BYTES
        or len(configuration_bytes)
        + len(prepared.prompt)
        + len(prepared.listing_input)
        + sum(image.byte_length for image in prepared.images)
        > MAX_ANALYSIS_INPUT_BYTES
    ):
        raise ValueError("Analysis execution configuration exceeds its input bound")
    record_id, listing_id, prompt_id, configuration_id = (new_identifier() for _ in range(4))
    if len({record_id, listing_id, prompt_id, configuration_id}) != 4 or {
        record_id,
        listing_id,
        prompt_id,
        configuration_id,
    }.intersection(image.artifact_identifier for image in prepared.images):
        raise ValueError("Analysis checkpoint identifiers must be fresh and unique")
    manifest = AnalysisInputManifest(
        marketplace=marketplace,
        configuration=decode_json(encoded_configuration),
        inputs=prepared.inputs,
        images=prepared.images,
        listing_input_identifier=listing_id,
        prompt_identifier=prompt_id,
    )
    metadata: list[dict[str, JsonValue]] = []
    for image in prepared.images:
        value = await database.get_artifact_metadata(image.artifact_identifier)
        if (
            value.get("size") != image.byte_length
            or value.get("sha256") != image.sha256
            or value.get("media_type") != image.media_type
        ):
            raise ValueError("Analysis image metadata differs from its prepared input")
        metadata.append(value)
    artifacts: list[ExternalFileDraft] = []
    images: list[AnalysisImage] = []
    identifiers = {
        record_id,
        listing_id,
        prompt_id,
        configuration_id,
        *(image.artifact_identifier for image in prepared.images),
    }
    for image, value in zip(prepared.images, metadata, strict=True):
        storage = value.get("storage")
        if isinstance(storage, dict) and storage.get("backend") == "filesystem":
            _ = await anyio.to_thread.run_sync(
                partial(_read_external, database.path.parent, value, retain=False)
            )
            images.append(image)
        else:
            _, content = await database.get_artifact(
                image.artifact_identifier, maximum_bytes=image.byte_length
            )
            identifier = new_identifier()
            if not identifier or identifier in identifiers:
                raise ValueError("Analysis image artifact identifier must be fresh and unique")
            identifiers.add(identifier)
            artifact = await anyio.to_thread.run_sync(
                _publish,
                database.path.parent,
                identifier,
                ("carl", "analysis", "input_image"),
                image.media_type,
                {"derived_from_artifact_identifier": image.artifact_identifier},
                content,
            )
            artifacts.append(artifact)
            images.append(image.model_copy(update={"artifact_identifier": identifier}))
    for identifier, kind, media_type, representation, content in (
        (
            configuration_id,
            ("carl", "analysis", "execution_configuration"),
            "application/json",
            {"exact_execution_configuration": True},
            configuration_bytes,
        ),
        (
            listing_id,
            ("carl", marketplace, "analysis_input"),
            "application/json",
            {"filename": "listing.json", "exact_agent_input": True},
            prepared.listing_input,
        ),
        (
            prompt_id,
            ("carl", marketplace, "analysis_prompt"),
            "text/plain; charset=utf-8",
            {"exact_agent_prompt": True},
            prepared.prompt,
        ),
    ):
        artifacts.append(
            await anyio.to_thread.run_sync(
                _publish,
                database.path.parent,
                identifier,
                kind,
                media_type,
                representation,
                content,
            )
        )
    manifest = manifest.model_copy(update={"images": tuple(images)})
    stored = manifest.model_dump(mode="json")
    del stored["configuration"]
    stored["configuration_identifier"] = configuration_id
    await anyio.lowlevel.checkpoint()
    return AnalysisCheckpoint(
        record_id,
        (
            RecordDraft(
                identifier=record_id,
                kind=INPUT_CHECKPOINT_KIND,
                schema_version=1,
                value=stored,
            ),
        ),
        tuple(artifacts),
        (
            NamedOutput(name=("listing_input",), object_identifier=listing_id),
            NamedOutput(name=("prompt",), object_identifier=prompt_id),
            NamedOutput(name=("analysis_configuration",), object_identifier=configuration_id),
            NamedOutput(name=("analysis_input_checkpoint",), object_identifier=record_id),
            *(
                NamedOutput(
                    name=("prepared_image", str(image.gallery_order)),
                    object_identifier=image.artifact_identifier,
                )
                for image in images
                if image.artifact_identifier
                not in {original.artifact_identifier for original in prepared.images}
            ),
        ),
    )


async def read_analysis_input_manifest(
    database: Database, identifier: str
) -> AnalysisInputManifest:
    kind, schema, value = await database.get_record(identifier)
    if kind != INPUT_CHECKPOINT_KIND or schema != 1:
        raise ValueError("Not an analysis input checkpoint")
    stored = _StoredAnalysisInputs.model_validate_json(encode_json(value))
    identifiers = (
        stored.configuration_identifier,
        stored.listing_input_identifier,
        stored.prompt_identifier,
    )
    if len(set(identifiers)) != 3 or identifier in identifiers:
        raise ValueError("Analysis input artifact identities must be unique")
    configuration = await _metadata(
        database, stored.configuration_identifier, MAX_ANALYSIS_TEXT_BYTES
    )
    listing = await _metadata(database, stored.listing_input_identifier, MAX_ANALYSIS_TEXT_BYTES)
    prompt = await _metadata(database, stored.prompt_identifier, MAX_ANALYSIS_TEXT_BYTES)
    for image in stored.images:
        _ = await _metadata(
            database, image.artifact_identifier, MAX_ANALYSIS_INPUT_BYTES, expected=image
        )
    if (
        configuration["size"]
        + listing["size"]
        + prompt["size"]
        + sum(image.byte_length for image in stored.images)
        > MAX_ANALYSIS_INPUT_BYTES
    ):
        raise ValueError("Analysis input checkpoint exceeds its aggregate bound")
    configuration_bytes = await anyio.to_thread.run_sync(
        partial(_read_external, database.path.parent, configuration, retain=True)
    )
    restored = stored.model_dump(mode="json")
    del restored["configuration_identifier"]
    restored["configuration"] = decode_json(configuration_bytes.decode())
    return AnalysisInputManifest.model_validate_json(encode_json(restored))


async def restore_analysis_inputs(database: Database, identifier: str) -> PreparedAnalysis:
    manifest = await read_analysis_input_manifest(database, identifier)
    listing = await _metadata(database, manifest.listing_input_identifier, MAX_ANALYSIS_TEXT_BYTES)
    prompt = await _metadata(database, manifest.prompt_identifier, MAX_ANALYSIS_TEXT_BYTES)
    image_metadata = [
        await _metadata(
            database, image.artifact_identifier, MAX_ANALYSIS_INPUT_BYTES, expected=image
        )
        for image in manifest.images
    ]
    if (
        listing["size"] + prompt["size"] + sum(image.byte_length for image in manifest.images)
        > MAX_ANALYSIS_INPUT_BYTES
    ):
        raise ValueError("Analysis input checkpoint exceeds its aggregate bound")
    # Every metadata bound is validated before the first file is opened.
    for metadata in image_metadata:
        _ = await anyio.to_thread.run_sync(
            partial(_read_external, database.path.parent, metadata, retain=False)
        )
    listing_bytes = await anyio.to_thread.run_sync(
        lambda: _read_external(database.path.parent, listing, retain=True)
    )
    prompt_bytes = await anyio.to_thread.run_sync(
        lambda: _read_external(database.path.parent, prompt, retain=True)
    )
    return PreparedAnalysis(listing_bytes, prompt_bytes, manifest.images, manifest.inputs)


async def checkpoint_claude_capture(
    database: Database,
    capture: ClaudeCapture,
    *,
    input_checkpoint_identifier: str,
    new_identifier: Callable[[], str],
) -> AnalysisCheckpoint:
    """Store exact bounded raw evidence before releasing its analysis execution."""
    if (
        len(capture.stdout) > MAX_ANALYSIS_STDOUT_BYTES
        or len(capture.stderr) > MAX_ANALYSIS_STDERR_BYTES
        or len(capture.stdout) + len(capture.stderr) > MAX_ANALYSIS_OUTPUT_BYTES
    ):
        raise ValueError("Analysis capture exceeds its output bound")
    if (
        not 1 <= len(capture.argv) <= 128
        or any(not isinstance(arg, str) for arg in capture.argv)
        or sum(len(arg.encode()) for arg in capture.argv) > MAX_ANALYSIS_ARGV_BYTES
    ):
        raise ValueError("Analysis invocation arguments exceed their bound")
    argv = encode_json(list(capture.argv)).encode()
    if len(argv) > MAX_ANALYSIS_ARGV_BYTES:
        raise ValueError("Analysis invocation JSON exceeds its bound")
    _ = await read_analysis_input_manifest(database, input_checkpoint_identifier)
    record_id, argv_id, stdout_id, stderr_id = (new_identifier() for _ in range(4))
    if len({record_id, argv_id, stdout_id, stderr_id}) != 4 or input_checkpoint_identifier in {
        record_id,
        argv_id,
        stdout_id,
        stderr_id,
    }:
        raise ValueError("Analysis capture identities must be fresh and unique")
    manifest = AnalysisCaptureManifest(
        input_checkpoint_identifier=input_checkpoint_identifier,
        argv_identifier=argv_id,
        stdout_identifier=stdout_id,
        stderr_identifier=stderr_id,
        started_at_utc_ns=capture.started_at_utc_ns,
        ended_at_utc_ns=capture.ended_at_utc_ns,
        duration_ns=capture.duration_ns,
        exit_code=capture.exit_code,
        timed_out=capture.timed_out,
        output_limited=capture.output_limited,
        launch_failed=capture.launch_failed,
    )
    artifacts: list[ExternalFileDraft] = []
    for identifier, name, media_type, representation, content in (
        (argv_id, "argv", "application/json", {"exact_process_argv": True}, argv),
        (
            stdout_id,
            "stdout",
            "application/x-ndjson",
            {"exact_process_stdout": True},
            capture.stdout,
        ),
        (
            stderr_id,
            "stderr",
            "text/plain; charset=utf-8",
            {"exact_process_stderr": True},
            capture.stderr,
        ),
    ):
        artifacts.append(
            await anyio.to_thread.run_sync(
                _publish,
                database.path.parent,
                identifier,
                ("carl", "claude", name),
                media_type,
                representation,
                content,
            )
        )
    await anyio.lowlevel.checkpoint()
    return AnalysisCheckpoint(
        record_id,
        (
            RecordDraft(
                identifier=record_id,
                kind=CAPTURE_CHECKPOINT_KIND,
                schema_version=1,
                value=manifest.model_dump(mode="json"),
            ),
        ),
        tuple(artifacts),
        (
            NamedOutput(name=("claude_capture_checkpoint",), object_identifier=record_id),
            NamedOutput(name=("claude", "argv"), object_identifier=argv_id),
            NamedOutput(name=("claude", "stdout"), object_identifier=stdout_id),
            NamedOutput(name=("claude", "stderr"), object_identifier=stderr_id),
        ),
    )


async def restore_claude_capture(
    database: Database, identifier: str, *, input_checkpoint_identifier: str
) -> ClaudeCapture:
    kind, schema, value = await database.get_record(identifier)
    if kind != CAPTURE_CHECKPOINT_KIND or schema != 1:
        raise ValueError("Not a raw analysis capture checkpoint")
    manifest = AnalysisCaptureManifest.model_validate_json(encode_json(value))
    if manifest.input_checkpoint_identifier != input_checkpoint_identifier:
        raise ValueError("Analysis capture belongs to another prepared input")
    argv = await _metadata(database, manifest.argv_identifier, MAX_ANALYSIS_ARGV_BYTES)
    stdout = await _metadata(database, manifest.stdout_identifier, MAX_ANALYSIS_STDOUT_BYTES)
    stderr = await _metadata(database, manifest.stderr_identifier, MAX_ANALYSIS_STDERR_BYTES)
    if stdout["size"] + stderr["size"] > MAX_ANALYSIS_OUTPUT_BYTES:
        raise ValueError("Analysis capture exceeds its combined output bound")
    if (
        argv.get("media_type") != "application/json"
        or stdout.get("media_type") != "application/x-ndjson"
        or stderr.get("media_type") != "text/plain; charset=utf-8"
    ):
        raise ValueError("Analysis capture file media types do not match their roles")
    _ = await read_analysis_input_manifest(database, input_checkpoint_identifier)
    producer, _, _ = await database.object_operation_relations(identifier, maximum_outputs=10_000)
    for artifact in (
        manifest.argv_identifier,
        manifest.stdout_identifier,
        manifest.stderr_identifier,
    ):
        owner, _, _ = await database.object_operation_relations(artifact, maximum_outputs=10_000)
        if owner != producer:
            raise ValueError("Analysis capture files belong to another producer")
    argv_bytes = await anyio.to_thread.run_sync(
        lambda: _read_external(database.path.parent, argv, retain=True)
    )
    arguments = decode_json(argv_bytes.decode())
    if (
        not isinstance(arguments, list)
        or not 1 <= len(arguments) <= 128
        or any(not isinstance(arg, str) for arg in arguments)
    ):
        raise ValueError("Invalid retained analysis invocation")
    stdout_bytes = await anyio.to_thread.run_sync(
        lambda: _read_external(database.path.parent, stdout, retain=True)
    )
    stderr_bytes = await anyio.to_thread.run_sync(
        lambda: _read_external(database.path.parent, stderr, retain=True)
    )
    return ClaudeCapture(
        argv=tuple(cast(list[str], arguments)),
        started_at_utc_ns=manifest.started_at_utc_ns,
        ended_at_utc_ns=manifest.ended_at_utc_ns,
        duration_ns=manifest.duration_ns,
        exit_code=manifest.exit_code,
        stdout=stdout_bytes,
        stderr=stderr_bytes,
        timed_out=manifest.timed_out,
        output_limited=manifest.output_limited,
        launch_failed=manifest.launch_failed,
    )
