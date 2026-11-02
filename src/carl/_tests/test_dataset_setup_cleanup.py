"""Successful cleanup never traverses foreign paths or releases live CPU work."""

# Fault injection and sync structural checks inspect the deletion boundary.
# pyright: reportPrivateUsage=false

import os
import threading
from pathlib import Path

import anyio
import pytest

import carl.io.dataset_setup_cleanup as cleanup
from carl._tests.test_travel_stages import _held
from carl.io.dataset_setup_cleanup import DatasetSetupCleanupError, cleanup_dataset_setup_scratch
from carl.io.resource_permits import ResourceLeaseLost
from carl.io.sqlite import Database


def _file(root: Path, locator: str) -> Path:
    path = root / locator
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    # pathlib's parents=True uses default permissions for ancestors.
    for parent in path.parents:
        if parent == root:
            break
        parent.chmod(0o700)
    path.write_bytes(b"fixture")
    path.chmod(0o644)
    return path


def test_only_successful_scratch_is_removed_with_receipts_and_published_dataset_retained(
    tmp_path: Path,
) -> None:
    workflow = tmp_path / "workflow"
    workflow.mkdir(mode=0o700)
    deleted = (
        _file(workflow, "downloads/source/file.data"),
        _file(workflow, "candidates/geonames/build/lookup.sqlite3"),
        _file(workflow, "osrm-attempts/native-0/intermediates/region.osrm"),
    )
    retained = tuple(
        _file(workflow, locator)
        for locator in (
            "workflow.json",
            "setup-values/state.json",
            "profile/profile.lua",
            "coverage/region.geojson",
            "osrm-attempts/native-0/receipts/extract/complete.json",
            "osrm-attempts/native-0/inputs/source.pbf",
            "osrm-attempts/native-0/coarse.json",
            "osrm-attempts/native-1/intermediates/unselected.osrm",
        )
    )
    published = _file(tmp_path, "published/dataset/manifest.json")
    cleanup._cleanup(workflow, ("native-0",))
    cleanup._cleanup(workflow, ("native-0",))
    assert all(not path.exists() for path in deleted)
    assert all(path.read_bytes() == b"fixture" for path in (*retained, published))
    assert not (workflow / "downloads").exists() and not (workflow / "candidates").exists()


@pytest.mark.parametrize(
    "invalid", ("symlink", "directory_symlink", "fifo", "public_directory", "target_file")
)
def test_all_targets_are_validated_before_any_delete(tmp_path: Path, invalid: str) -> None:
    valid = _file(tmp_path, "downloads/source/retained.data")
    outside = _file(tmp_path, "outside/foreign.data")
    candidate = tmp_path / "candidates"
    if invalid == "target_file":
        candidate.write_bytes(b"unexpected")
    else:
        candidate.mkdir(mode=0o700)
        if invalid == "symlink":
            (candidate / "unsafe").symlink_to(outside)
        elif invalid == "directory_symlink":
            (candidate / "unsafe").symlink_to(outside.parent, target_is_directory=True)
        elif invalid == "fifo":
            os.mkfifo(candidate / "unsafe", mode=0o600)
        else:
            candidate.chmod(0o755)
    with pytest.raises(DatasetSetupCleanupError):
        cleanup._cleanup(tmp_path, ())
    assert valid.read_bytes() == outside.read_bytes() == b"fixture"


def test_foreign_owner_is_rejected_before_deleting_valid_sibling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    valid = _file(tmp_path, "downloads/valid.data")
    foreign = _file(tmp_path, "candidates/foreign.data")
    original = cleanup.os.stat

    def changed(
        path: int | str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        info = original(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)
        if path == "foreign.data":
            fields = list(info)
            fields[4] = os.getuid() + 1
            return os.stat_result(fields)
        return info

    monkeypatch.setattr(cleanup.os, "stat", changed)
    with pytest.raises(DatasetSetupCleanupError):
        cleanup._cleanup(tmp_path, ())
    assert valid.exists() and foreign.exists()


@pytest.mark.parametrize(
    "attempts",
    (("../native-0",), ("native-0/intermediates",), ("other-0",), ("native-0", "native-0")),
)
def test_native_attempt_identifiers_cannot_expand_deletion_scope(
    tmp_path: Path, attempts: tuple[str, ...]
) -> None:
    retained = _file(tmp_path, "downloads/retained.data")
    with pytest.raises(DatasetSetupCleanupError):
        cleanup._cleanup(tmp_path, attempts)
    assert retained.exists()


@pytest.mark.parametrize("bound", ("entries", "depth"))
def test_tree_bounds_fail_before_any_delete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bound: str
) -> None:
    retained = _file(tmp_path, "downloads/retained.data")
    extra = _file(tmp_path, "candidates/a/b/c/file")
    monkeypatch.setattr(
        cleanup,
        "MAX_DATASET_CLEANUP_ENTRIES" if bound == "entries" else "MAX_DATASET_CLEANUP_DEPTH",
        3,
    )
    with pytest.raises(DatasetSetupCleanupError):
        cleanup._cleanup(tmp_path, ())
    assert retained.exists() and extra.exists()


def test_inode_replacement_after_validation_is_not_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _file(tmp_path, "downloads/file")
    original = cleanup._parent

    def changed(
        root: int, parts: tuple[str, ...], directories: dict[tuple[str, ...], cleanup._Identity]
    ) -> int:
        if parts == ("downloads", "file"):
            path.rename(path.with_name("original"))
            path.write_bytes(b"replacement")
        return original(root, parts, directories)

    monkeypatch.setattr(cleanup, "_parent", changed)
    with pytest.raises(DatasetSetupCleanupError):
        cleanup._cleanup(tmp_path, ())
    assert path.read_bytes() == b"replacement"
    assert path.with_name("original").read_bytes() == b"fixture"


def test_ancestor_symlink_does_not_touch_target(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir(mode=0o700)
    retained = _file(actual, "downloads/file")
    alias = tmp_path / "alias"
    alias.symlink_to(actual, target_is_directory=True)
    with pytest.raises(DatasetSetupCleanupError):
        cleanup._cleanup(alias, ())
    assert retained.exists()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ("trio", "asyncio"))
async def test_cancel_keeps_cpu_until_physical_cleanup_thread_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "workflow"
    directory.mkdir(mode=0o700)
    _ = _file(directory, "downloads/file")
    started, physical_release = threading.Event(), threading.Event()
    completed, competitor = anyio.Event(), anyio.Event()
    scopes: list[anyio.CancelScope] = []
    original = cleanup._cleanup

    def blocked(root: Path, attempts: tuple[str, ...]) -> None:
        started.set()
        assert physical_release.wait(5)
        original(root, attempts)

    monkeypatch.setattr(cleanup, "_cleanup", blocked)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:

        async def run() -> None:
            try:
                with anyio.CancelScope() as scope:
                    scopes.append(scope)
                    await cleanup_dataset_setup_scratch(directory)
            finally:
                completed.set()

        async def other() -> None:
            await cleanup.run_cpu(lambda: None)
            competitor.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(run)
            try:
                with anyio.fail_after(5):
                    while not started.is_set():
                        await anyio.sleep(0.01)
                scopes[0].cancel()
                tasks.start_soon(other)
                await anyio.sleep(0.05)
                assert not completed.is_set() and not competitor.is_set()
                assert await _held(database, "cpu") == 1
            finally:
                physical_release.set()
            with anyio.fail_after(5):
                await completed.wait()
                await competitor.wait()
        assert not (directory / "downloads").exists()
        assert await _held(database, "cpu") == 0


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ("trio", "asyncio"))
async def test_unmanaged_cleanup_is_rejected_before_deletion(tmp_path: Path) -> None:
    retained = _file(tmp_path, "downloads/file")
    with pytest.raises(ResourceLeaseLost):
        await cleanup_dataset_setup_scratch(tmp_path)
    assert retained.exists()
