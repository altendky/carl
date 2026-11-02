"""Dataset coordinator metadata survives replay without trusting paths or blobs."""

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from pydantic import ValidationError

from carl.core.dataset_setup import DatasetSetupReference, DatasetSetupState
from carl.core.dataset_workflows import DatasetWorkflowDefinition
from carl.core.travel_data import TravelDataSetupRequest
from carl.io import dataset_setup_files as files
from carl.io.dataset_setup_files import (
    DatasetSetupFileError,
    read_setup_state,
    read_setup_value,
    write_setup_state,
    write_setup_value,
)


def _state() -> DatasetSetupState:
    return DatasetSetupState(
        definition=DatasetWorkflowDefinition(
            workflow_identifier="fixture",
            request=TravelDataSetupRequest(request_identifier="fixture", provider="geonames"),
            recipe_identifier="fixture-v1",
            configuration_identity="fixture-config",
        ),
        phase="geonames.download",
        value={"cursor": 1, "receipt": {"sha256": "a" * 64}},
    )


def _raw(directory: Path, content: bytes) -> DatasetSetupReference:
    bucket = directory / "setup-values"
    bucket.mkdir(mode=0o700, exist_ok=True)
    reference = DatasetSetupReference(
        sha256=hashlib.sha256(content).hexdigest(), bytes=len(content)
    )
    descriptor = os.open(directory / reference.locator, os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        _ = os.write(descriptor, content)
    finally:
        os.close(descriptor)
    return reference


def test_state_replay_is_immutable_and_keeps_full_definition(tmp_path: Path) -> None:
    state = _state()
    reference = write_setup_state(tmp_path, state)
    original = (tmp_path / reference.locator).stat()
    assert read_setup_state(tmp_path, reference) == state
    assert write_setup_state(tmp_path, state) == reference
    assert (tmp_path / reference.locator).stat().st_ino == original.st_ino
    next_state = state.model_copy(update={"phase": "geonames.verify"})
    assert write_setup_state(tmp_path, next_state) != reference
    assert read_setup_state(tmp_path, reference).phase == "geonames.download"
    assert not tuple((tmp_path / "setup-values").glob(".setup-*"))


def test_concurrent_same_content_publish_validates_one_immutable_file(tmp_path: Path) -> None:
    value = {"sources": ["dataset"], "cursor": 2}
    with ThreadPoolExecutor(max_workers=4) as pool:
        references = tuple(pool.map(lambda _: write_setup_value(tmp_path, value), range(8)))
    assert len(set(ref.model_dump_json() for ref in references)) == 1
    assert read_setup_value(tmp_path, references[0]) == value
    assert len(tuple((tmp_path / "setup-values").iterdir())) == 1


@pytest.mark.parametrize("phase", ("", "../outside", "a/b", "x" * 101, "has space"))
def test_phase_is_a_bounded_safe_identity(phase: str) -> None:
    with pytest.raises(ValidationError):
        _ = DatasetSetupState.model_validate({**_state().model_dump(), "phase": phase})


def test_utf8_state_and_value_limits_apply_before_filesystem_write(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="64 KiB"):
        _ = DatasetSetupState(definition=_state().definition, phase="prepare", value="é" * 40_000)
    with pytest.raises(DatasetSetupFileError):
        _ = write_setup_value(tmp_path, "é" * 100, maximum_bytes=100)
    assert not (tmp_path / "setup-values").exists()
    reference = write_setup_value(tmp_path, "a" * 70_000)
    with pytest.raises(DatasetSetupFileError):
        _ = read_setup_state(tmp_path, reference)


def test_copied_models_are_revalidated_before_storage_or_path_access(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        _ = DatasetSetupState(definition=_state().definition, phase="prepare", value=float("nan"))
    state = _state().model_copy(update={"value": float("nan")})
    with pytest.raises(DatasetSetupFileError):
        _ = write_setup_state(tmp_path, state)
    assert not (tmp_path / "setup-values").exists()
    reference = DatasetSetupReference(sha256="a" * 64, bytes=10).model_copy(
        update={"sha256": "../outside"}
    )
    with pytest.raises(DatasetSetupFileError):
        _ = read_setup_value(tmp_path, reference)


@pytest.mark.parametrize("maximum", (0, -1, 1_048_577, True))
def test_invalid_maximum_is_rejected(tmp_path: Path, maximum: int) -> None:
    with pytest.raises(DatasetSetupFileError):
        _ = write_setup_value(tmp_path, None, maximum_bytes=maximum)


@pytest.mark.parametrize("content", (b'{"x":1,"x":2}', b"NaN", b"1e999", b"Infinity", b'"\xff"'))
def test_hash_matching_invalid_json_is_rejected(tmp_path: Path, content: bytes) -> None:
    with pytest.raises(DatasetSetupFileError):
        _ = read_setup_value(tmp_path, _raw(tmp_path, content))


@pytest.mark.parametrize("content", (b'{"x":2}', b'{"x":111111}'))
def test_existing_corrupt_cas_is_never_accepted_or_overwritten(
    tmp_path: Path, content: bytes
) -> None:
    reference = write_setup_value(tmp_path, {"x": 1})
    path = tmp_path / reference.locator
    path.write_bytes(content)
    with pytest.raises(DatasetSetupFileError):
        _ = write_setup_value(tmp_path, {"x": 1})
    with pytest.raises(DatasetSetupFileError):
        _ = read_setup_value(tmp_path, reference)
    assert path.read_bytes() == content


def test_declared_and_actual_oversize_reject_before_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reference = write_setup_value(tmp_path, {"x": 1})
    (tmp_path / reference.locator).write_bytes(b"x" * 1000)

    def read(_descriptor: int, _size: int) -> bytes:
        pytest.fail("Oversized metadata must be rejected before reading")

    monkeypatch.setattr(files.os, "read", read)
    with pytest.raises(DatasetSetupFileError):
        _ = read_setup_value(tmp_path, reference)
    with pytest.raises(DatasetSetupFileError):
        _ = read_setup_value(tmp_path, reference, maximum_bytes=1)


@pytest.mark.parametrize(
    "kind",
    ("file_symlink", "bucket_symlink", "ancestor_symlink", "public_file", "public_bucket", "fifo"),
)
def test_unsafe_file_or_path_is_rejected(tmp_path: Path, kind: str) -> None:
    directory = tmp_path / "workspace"
    directory.mkdir(mode=0o700)
    reference = write_setup_value(directory, {"x": 1})
    path = directory / reference.locator
    if kind == "file_symlink":
        original = path.with_suffix(".original")
        path.rename(original)
        path.symlink_to(original)
    elif kind == "bucket_symlink":
        bucket = directory / "setup-values"
        original = directory / "original"
        bucket.rename(original)
        bucket.symlink_to(original, target_is_directory=True)
    elif kind == "ancestor_symlink":
        alias = tmp_path / "alias"
        alias.symlink_to(directory, target_is_directory=True)
        directory = alias
    elif kind == "public_file":
        path.chmod(0o644)
    elif kind == "public_bucket":
        path.parent.chmod(0o755)
    else:
        path.unlink()
        os.mkfifo(path, mode=0o600)
    with pytest.raises(DatasetSetupFileError):
        _ = read_setup_value(directory, reference)
    with pytest.raises(DatasetSetupFileError):
        _ = write_setup_value(directory, {"x": 1})


def test_write_failure_cleans_only_uncommitted_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reference = write_setup_value(tmp_path, {"retained": True})
    original = files.os.link

    def link(*args: object, **kwargs: object) -> None:
        raise OSError("fixture")

    monkeypatch.setattr(files.os, "link", link)
    with pytest.raises(DatasetSetupFileError):
        _ = write_setup_value(tmp_path, {"next": True})
    monkeypatch.setattr(files.os, "link", original)
    assert read_setup_value(tmp_path, reference) == {"retained": True}
    assert len(tuple((tmp_path / "setup-values").iterdir())) == 1


def test_file_mutation_during_read_fails_identity_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reference = write_setup_value(tmp_path, {"x": 1})
    path = tmp_path / reference.locator
    original = files.os.read

    def read(descriptor: int, size: int) -> bytes:
        content = original(descriptor, size)
        if content:
            path.write_text(json.dumps({"x": 22}))
        return content

    monkeypatch.setattr(files.os, "read", read)
    with pytest.raises(DatasetSetupFileError):
        _ = read_setup_value(tmp_path, reference)
