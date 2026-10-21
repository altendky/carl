"""Dataset lifecycle validates immutable versions and preserves private configuration."""

import hashlib
import importlib.metadata
import json
import sqlite3
import stat
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import anyio
import pytest

from carl._tests.test_configuration import _write_configuration
from carl.core.travel_data import TravelDataFailure, TravelDataSetupRequest
from carl.io.configuration import load_configuration
from carl.io.paths import CarlDirectories
from carl.io.travel_data import TravelDataManager

type Provider = Literal["osrm", "geonames"]


def _dataset(
    directories: CarlDirectories,
    identifier: str,
    *,
    provider: Provider = "geonames",
    version: str | None = None,
    created_at: str | None = None,
) -> Path:
    directories.data.mkdir(parents=True, exist_ok=True)
    directories.travel_data_directory.mkdir(mode=0o700, exist_ok=True)
    (directories.travel_data_directory / provider).mkdir(mode=0o700, exist_ok=True)
    target = directories.travel_data_directory / provider / identifier
    target.mkdir(mode=0o700)
    if provider == "geonames":
        artifact = target / "places.sqlite3"
        with closing(sqlite3.connect(artifact)) as connection:
            connection.executescript("""
                CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE state_names (name TEXT PRIMARY KEY, code TEXT NOT NULL);
                CREATE TABLE places (
                    identifier INTEGER PRIMARY KEY, state_code TEXT NOT NULL,
                    latitude REAL NOT NULL, longitude REAL NOT NULL
                );
                CREATE TABLE names (
                    name TEXT NOT NULL, place_identifier INTEGER NOT NULL,
                    PRIMARY KEY (name, place_identifier)
                );
                CREATE TABLE postal (
                    postal_code TEXT NOT NULL, name TEXT NOT NULL, state_code TEXT NOT NULL,
                    latitude REAL NOT NULL, longitude REAL NOT NULL,
                    PRIMARY KEY (postal_code, name, state_code, latitude, longitude)
                );
            """)
            connection.executemany(
                "INSERT INTO metadata VALUES (?, ?)",
                (
                    ("schema_version", "1"),
                    ("country_code", "US"),
                    ("dataset_identifier", identifier),
                ),
            )
            connection.commit()
        artifacts = (artifact,)
    else:
        artifacts = tuple(
            target / ("region.osrm." + suffix) for suffix in ("mldgr", "partition", "cell_metrics")
        )
        for artifact in artifacts:
            _ = artifact.write_bytes(b"small graph fixture")
    manifest: dict[str, object] = {
        "provider": provider,
        "dataset_identifier": identifier,
        "schema_version": 1,
        "country_code": "US",
        "created_at_utc": created_at or datetime.now(UTC).isoformat(),
        "files": [
            {
                "name": artifact.name,
                "bytes": artifact.stat().st_size,
                "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
            }
            for artifact in artifacts
        ],
    }
    if provider == "osrm":
        manifest.update(
            osrm_version=version or importlib.metadata.version("osrm-bindings"), algorithm="MLD"
        )
    path = target / "manifest.json"
    _ = path.write_text(json.dumps(manifest))
    return path


def _select(
    configuration: Path,
    manifest: Path,
    *,
    provider: Provider = "geonames",
    filename: str | None = None,
) -> None:
    identifier = manifest.parent.name
    if provider == "geonames":
        settings = f'''
[travel]
geocoding_provider = "geonames"
[geonames]
database_path = "{manifest.parent / (filename or "places.sqlite3")}"
dataset_identifier = "{identifier}"
'''
    else:
        settings = f'''
[travel]
provider = "osrm"
[osrm]
dataset_path = "{manifest.parent / (filename or "region.osrm")}"
dataset_identifier = "{identifier}"
'''
    _ = configuration.write_text(configuration.read_text() + settings)


@pytest.mark.anyio
async def test_unconfigured_and_missing_datasets_have_actionable_status(tmp_path: Path) -> None:
    directories, configuration = _write_configuration(tmp_path)
    manager = TravelDataManager(directories)
    initial = await manager.status()
    assert {item.state for item in initial.providers} == {"unconfigured"}
    assert all(item.actions and item.message for item in initial.providers)
    missing = directories.travel_data_directory / "geonames" / "missing" / "manifest.json"
    _select(configuration, missing)
    status = next(
        item for item in (await manager.status()).providers if item.provider == "geonames"
    )
    assert status.state == "missing"
    assert status.reason == "manifest_missing"
    assert status.dataset_identifier == "missing"
    assert "carl travel setup geonames" in status.actions


@pytest.mark.anyio
async def test_verified_ready_status_reports_size_and_age(tmp_path: Path) -> None:
    directories, configuration = _write_configuration(tmp_path)
    created = (datetime.now(UTC) - timedelta(days=45)).isoformat()
    manifest = _dataset(directories, "old-but-valid", created_at=created)
    _select(configuration, manifest)
    item = next(
        item
        for item in (await TravelDataManager(directories).status(verify=True)).providers
        if item.provider == "geonames"
    )
    assert item.state == "ready"
    assert item.outdated
    assert item.created_at_utc == created
    assert item.size_bytes == (manifest.parent / "places.sqlite3").stat().st_size
    assert "carl travel update geonames" in item.actions


@pytest.mark.anyio
async def test_full_verification_detects_same_size_checksum_corruption(tmp_path: Path) -> None:
    directories, configuration = _write_configuration(tmp_path)
    manifest = _dataset(directories, "damaged-map", provider="osrm")
    _select(configuration, manifest, provider="osrm")
    artifact = manifest.parent / "region.osrm.mldgr"
    _ = artifact.write_bytes(b"x" * artifact.stat().st_size)
    manager = TravelDataManager(directories)
    assert (await manager.status()).providers[0].state == "ready"
    verified = (await manager.status(verify=True)).providers[0]
    assert verified.state == "invalid"
    assert verified.actions


@pytest.mark.anyio
async def test_different_osrm_version_is_distinctly_incompatible(tmp_path: Path) -> None:
    directories, configuration = _write_configuration(tmp_path)
    manifest = _dataset(directories, "other-version", provider="osrm", version="0.0.0")
    _select(configuration, manifest, provider="osrm")
    status = (await TravelDataManager(directories).status()).providers[0]
    assert status.state == "incompatible"
    assert status.reason == "osrm_incompatible"
    assert status.actions


@pytest.mark.anyio
async def test_status_checks_configured_database_path_not_only_sibling_manifest(
    tmp_path: Path,
) -> None:
    directories, configuration = _write_configuration(tmp_path)
    manifest = _dataset(directories, "valid-sibling")
    _select(configuration, manifest, filename="missing.sqlite3")
    status = (await TravelDataManager(directories).status(verify=True)).providers[1]
    assert status.state != "ready"
    assert status.actions


@pytest.mark.anyio
async def test_healthy_sqlite_without_geonames_schema_is_not_ready(tmp_path: Path) -> None:
    directories, configuration = _write_configuration(tmp_path)
    manifest = _dataset(directories, "wrong-schema")
    database = manifest.parent / "places.sqlite3"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("DROP TABLE metadata")
        connection.commit()
    value = json.loads(manifest.read_text())
    value["files"][0].update(
        bytes=database.stat().st_size, sha256=hashlib.sha256(database.read_bytes()).hexdigest()
    )
    _ = manifest.write_text(json.dumps(value))
    _select(configuration, manifest)
    status = (await TravelDataManager(directories).status(verify=True)).providers[1]
    assert status.state == "invalid"


@pytest.mark.anyio
async def test_activation_preserves_unrelated_settings_comments_and_private_credentials(
    tmp_path: Path,
) -> None:
    directories, configuration = _write_configuration(tmp_path)
    _ = configuration.write_text(
        configuration.read_text() + "\n# User-maintained configuration comment\n"
        "[openrouteservice]\ncredential_id = 'example-key' # Keep this reference\n"
        "origin_label = 'Example City, PA'\n"
    )
    before = load_configuration(configuration).configuration
    credentials = directories.openrouteservice_credential_directory
    credentials.mkdir(parents=True, mode=0o700)
    secret = credentials / "example-key.api-key"
    _ = secret.write_text("fixture-key-that-must-stay-private")
    secret.chmod(0o600)
    manifest = _dataset(directories, "new-localities")
    result = await TravelDataManager(directories).activate("geonames", manifest.parent.name)
    after = load_configuration(configuration).configuration
    assert after.routes == before.routes
    assert after.wireproxy == before.wireproxy
    assert after.openrouteservice == before.openrouteservice
    assert after.travel is not None
    assert before.openrouteservice is not None
    assert after.travel.origin_label == before.openrouteservice.origin_label
    assert after.travel.geocoding_provider == "geonames"
    text = configuration.read_text()
    assert "# User-maintained configuration comment" in text
    assert "# Keep this reference" in text
    assert secret.read_text() == "fixture-key-that-must-stay-private"
    assert "fixture-key-that-must-stay-private" not in text
    assert stat.S_IMODE(configuration.stat().st_mode) == 0o600
    assert result.providers[1].state == "ready"
    assert not tuple(configuration.parent.glob(".travel-config-*"))


@pytest.mark.anyio
async def test_corrupt_dataset_activation_leaves_old_configuration_untouched(
    tmp_path: Path,
) -> None:
    directories, configuration = _write_configuration(tmp_path)
    manager = TravelDataManager(directories)
    old = _dataset(directories, "old-localities")
    await manager.activate("geonames", old.parent.name)
    previous = configuration.read_bytes()
    new = _dataset(directories, "corrupt-localities")
    database = new.parent / "places.sqlite3"
    data = bytearray(database.read_bytes())
    data[-1] ^= 1
    _ = database.write_bytes(data)
    with pytest.raises(TravelDataFailure):
        await manager.activate("geonames", new.parent.name)
    assert configuration.read_bytes() == previous
    assert (await manager.status(verify=True)).providers[1].dataset_identifier == old.parent.name


@pytest.mark.anyio
async def test_config_replace_failure_preserves_previous_config_and_removes_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directories, configuration = _write_configuration(tmp_path)
    manifest = _dataset(directories, "new-localities")
    previous = configuration.read_bytes()

    def fail_replace(source: object, destination: object) -> None:
        raise OSError("fixture replace failure")

    monkeypatch.setattr("carl.io.travel_data.os.replace", fail_replace)
    with pytest.raises((TravelDataFailure, OSError)):
        await TravelDataManager(directories).activate("geonames", manifest.parent.name)
    assert configuration.read_bytes() == previous
    assert not tuple(configuration.parent.glob(".travel-config-*"))


@pytest.mark.anyio
async def test_dataset_pages_are_stable_bounded_and_provider_scoped(tmp_path: Path) -> None:
    directories, configuration = _write_configuration(tmp_path)
    manager = TravelDataManager(directories)
    _ = _dataset(directories, "shared-name", provider="osrm")
    selected = _dataset(directories, "shared-name", provider="geonames")
    _ = _dataset(directories, "z-last", provider="geonames")
    _select(configuration, selected)
    first = await manager.datasets(limit=1)
    assert len(first.items) == 1
    assert first.next_cursor
    second = await manager.datasets(limit=1, cursor=first.next_cursor)
    assert len(second.items) == 1
    assert second.next_cursor
    third = await manager.datasets(limit=1, cursor=second.next_cursor)
    assert len(third.items) == 1
    assert third.next_cursor is None
    items = (*first.items, *second.items, *third.items)
    assert {(item.provider, item.dataset_identifier) for item in items} == {
        ("osrm", "shared-name"),
        ("geonames", "shared-name"),
        ("geonames", "z-last"),
    }
    assert {(item.provider, item.dataset_identifier) for item in items if item.active} == {
        ("geonames", "shared-name")
    }
    with pytest.raises(ValueError, match="limit"):
        await manager.datasets(limit=0)


@pytest.mark.anyio
async def test_build_lock_is_shared_and_stale_progress_becomes_interrupted(tmp_path: Path) -> None:
    directories, _ = _write_configuration(tmp_path)
    first, second = TravelDataManager(directories), TravelDataManager(directories)
    with first._locked():
        first._progress(provider="geonames", state="building", stage="downloading")
        with pytest.raises(TravelDataFailure) as failure, second._locked():
            raise AssertionError("A second manager must not acquire the build lock")
        assert failure.value.reason == "travel_data_busy"
        running = (await second.status()).build
        assert running is not None and running["running"] is True
    stale = (await second.status()).build
    assert stale is not None and stale["state"] == "interrupted"
    assert stale["running"] is False
    assert stale["message"]
    with second._locked():
        pass


@pytest.mark.anyio
async def test_malformed_progress_is_ignored_without_breaking_dataset_status(
    tmp_path: Path,
) -> None:
    directories, _ = _write_configuration(tmp_path)
    manager = TravelDataManager(directories)
    with manager._locked():
        progress = directories.travel_data_state_directory / "progress.json"
        _ = progress.write_text("[]")
    status = await manager.status()
    assert status.build is None
    assert len(status.providers) == 2


@pytest.mark.anyio
@pytest.mark.parametrize("fail_build", [False, True])
async def test_setup_failure_keeps_previous_active_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_build: bool
) -> None:
    directories, configuration = _write_configuration(tmp_path)
    manager = TravelDataManager(directories)
    old = _dataset(directories, "old-localities")
    await manager.activate("geonames", old.parent.name)
    previous = configuration.read_bytes()

    async def builder(root: Path) -> Path:
        if fail_build:
            raise RuntimeError("private builder diagnostic must not leak")
        manifest = _dataset(directories, "broken-localities")
        _ = (manifest.parent / "places.sqlite3").write_bytes(b"not sqlite")
        return manifest

    monkeypatch.setattr("carl.io.geonames.build_geonames_dataset", builder)
    with pytest.raises(TravelDataFailure) as failure:
        await manager.setup(
            TravelDataSetupRequest(request_identifier="failed-setup", provider="geonames")
        )
    assert "private builder diagnostic" not in str(failure.value)
    assert configuration.read_bytes() == previous
    status = await manager.status(verify=True)
    assert status.providers[1].dataset_identifier == old.parent.name
    assert status.providers[1].state == "ready"
    assert status.build is not None and status.build["state"] == "failed"


@pytest.mark.anyio
async def test_setup_validates_builder_output_and_can_leave_dataset_inactive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directories, configuration = _write_configuration(tmp_path)
    before = configuration.read_bytes()

    async def builder(root: Path) -> Path:
        assert root == directories.travel_data_directory / "geonames"
        return _dataset(directories, "prepared-localities")

    monkeypatch.setattr("carl.io.geonames.build_geonames_dataset", builder)
    manager = TravelDataManager(directories)
    result = await manager.setup(
        TravelDataSetupRequest(
            request_identifier="prepare-only", provider="geonames", activate=False
        )
    )
    assert not result.activated
    assert result.dataset_identifier == "prepared-localities"
    assert configuration.read_bytes() == before
    page = await manager.datasets()
    assert len(page.items) == 1 and not page.items[0].active


@pytest.mark.anyio
async def test_cancelled_setup_preserves_active_data_and_releases_build_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directories, configuration = _write_configuration(tmp_path)
    manager = TravelDataManager(directories)
    old = _dataset(directories, "old-localities")
    await manager.activate("geonames", old.parent.name)
    previous = configuration.read_bytes()
    entered = anyio.Event()

    async def builder(root: Path) -> Path:
        entered.set()
        await anyio.sleep_forever()
        raise AssertionError("Cancelled builder must not return a dataset")

    async def setup() -> None:
        _ = await manager.setup(
            TravelDataSetupRequest(request_identifier="cancel-build", provider="geonames")
        )

    monkeypatch.setattr("carl.io.geonames.build_geonames_dataset", builder)
    async with anyio.create_task_group() as group:
        group.start_soon(setup)
        await entered.wait()
        group.cancel_scope.cancel()
    assert configuration.read_bytes() == previous
    status = await manager.status(verify=True)
    assert status.build is not None and status.build["state"] == "cancelled"
    assert status.providers[1].dataset_identifier == old.parent.name
    with TravelDataManager(directories)._locked():
        pass


@pytest.mark.anyio
@pytest.mark.parametrize("corrupt", ["path-traversal", "duplicate-file", "file-symlink"])
async def test_manifest_rejects_unsafe_artifacts(tmp_path: Path, corrupt: str) -> None:
    directories, configuration = _write_configuration(tmp_path)
    manifest = _dataset(directories, "unsafe")
    value = json.loads(manifest.read_text())
    if corrupt == "path-traversal":
        value["files"][0]["name"] = "../places.sqlite3"
    elif corrupt == "duplicate-file":
        value["files"].append(value["files"][0])
    else:
        database = manifest.parent / "places.sqlite3"
        moved = manifest.parent / "actual.sqlite3"
        database.rename(moved)
        database.symlink_to(moved)
    _ = manifest.write_text(json.dumps(value))
    _select(configuration, manifest)
    result = await TravelDataManager(directories).status(verify=True)
    assert result.providers[1].state == "invalid"
