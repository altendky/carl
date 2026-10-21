"""Explicit, versioned local travel-data setup; startup never builds datasets."""

import fcntl
import hashlib
import importlib.metadata
import json
import os
import re
import secrets
import sqlite3
import tomllib
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import time_ns
from typing import Any

import anyio
import tomlkit

from carl.core.configuration import CarlConfiguration
from carl.core.travel_data import (
    TravelDataFailure,
    TravelDataProvider,
    TravelDatasetPage,
    TravelDatasetSummary,
    TravelDataSetupRequest,
    TravelDataSetupResult,
    TravelDataStatus,
    TravelProviderStatus,
)
from carl.io.configuration import (
    ConfigurationFailure,
    _read_regular_file,
    _require_private_directory,
    load_configuration,
)
from carl.io.paths import CarlDirectories

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,199}$")
_PROVIDERS: tuple[TravelDataProvider, ...] = ("osrm", "geonames")


def _manifest(path: Path, provider: str, identifier: str, *, verify: bool) -> dict[str, Any]:
    try:
        if path.is_symlink() or path.parent.is_symlink() or path.stat().st_size > 1_048_576:
            raise ValueError
        value = json.loads(path.read_bytes())
        if not isinstance(value, dict):
            raise ValueError
        if value["dataset_identifier"] != identifier or value.get("provider", "osrm") != provider:
            raise ValueError
        files = value["files"]
        if not isinstance(files, list) or not 1 <= len(files) <= 100:
            raise ValueError
        names = set()
        for item in files:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("bytes"), int)
                or isinstance(item["bytes"], bool)
                or item["bytes"] < 0
                or not isinstance(item.get("sha256"), str)
                or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None
            ):
                raise ValueError
            name = item["name"]
            if (
                not isinstance(name, str)
                or Path(name).name != name
                or name in (".", "..")
                or name in names
            ):
                raise ValueError
            names.add(name)
            target = path.parent / name
            if (
                target.is_symlink()
                or not target.is_file()
                or target.stat().st_size != item["bytes"]
            ):
                raise ValueError
            if verify:
                with target.open("rb") as stream:
                    if hashlib.file_digest(stream, "sha256").hexdigest() != item["sha256"]:
                        raise ValueError
        if provider == "osrm":
            version = importlib.metadata.version("osrm-bindings")
            if (
                str(value["osrm_version"]).removeprefix("v") != version
                or value["algorithm"] != "MLD"
            ):
                raise TravelDataFailure(
                    "osrm_incompatible",
                    "Rebuild with carl travel update osrm using the installed OSRM version.",
                )
            if (
                not {"region.osrm.mldgr", "region.osrm.partition", "region.osrm.cell_metrics"}
                <= names
            ):
                raise ValueError
        elif "places.sqlite3" not in names:
            raise ValueError
        else:
            with closing(
                sqlite3.connect((path.parent / "places.sqlite3").as_uri() + "?mode=ro", uri=True)
            ) as connection:
                metadata = dict(connection.execute("SELECT key,value FROM metadata"))
                if (
                    metadata.get("schema_version") not in ("1", "2")
                    or metadata.get("country_code") != "US"
                    or metadata.get("dataset_identifier") != identifier
                ):
                    raise ValueError
                if metadata["schema_version"] == "2":
                    connection.execute("SELECT is_canonical FROM names LIMIT 0")
                for query in (
                    "SELECT name,code FROM state_names LIMIT 0",
                    "SELECT identifier,state_code,latitude,longitude FROM places LIMIT 0",
                    "SELECT name,place_identifier FROM names LIMIT 0",
                    "SELECT postal_code,name,state_code,latitude,longitude FROM postal LIMIT 0",
                ):
                    connection.execute(query)
                if verify and connection.execute("PRAGMA quick_check").fetchone() != ("ok",):
                    raise ValueError
        return value
    except TravelDataFailure:
        raise
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        sqlite3.Error,
        importlib.metadata.PackageNotFoundError,
    ):
        raise TravelDataFailure(
            "dataset_invalid",
            f"Local {provider} data is missing or invalid. Run carl travel setup {provider}.",
        ) from None


class TravelDataManager:
    def __init__(self, directories: CarlDirectories):
        self.directories = directories

    def _root(self, provider: TravelDataProvider) -> Path:
        return self.directories.travel_data_directory / provider

    @contextmanager
    def _locked(self) -> Iterator[None]:
        state = self.directories.travel_data_state_directory
        try:
            self.directories.state.mkdir(parents=True, exist_ok=True)
            _require_private_directory(state, ConfigurationFailure)
        except (OSError, ConfigurationFailure):
            raise TravelDataFailure(
                "travel_state_unavailable",
                "Travel-data state directory must be writable, private and owned by the current user. Check carl locations.",
            ) from None
        descriptor = os.open(state / "build.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise TravelDataFailure(
                    "travel_data_busy",
                    "Another travel-data setup or activation is running. Check get_travel_data_status or carl travel status.",
                ) from None
            yield
        finally:
            os.close(descriptor)

    def _progress(self, **value: Any) -> None:
        path = self.directories.travel_data_state_directory / "progress.json"
        temporary = path.with_name(".progress-" + secrets.token_hex(8))
        with open(temporary, "x", opener=lambda p, flags: os.open(p, flags, 0o600)) as stream:
            json.dump({**value, "updated_at_utc": datetime.now(UTC).isoformat()}, stream)
        temporary.replace(path)

    async def setup(self, request: TravelDataSetupRequest) -> TravelDataSetupResult:
        # The shared file lock also covers foreground CLI builds and activation.
        with self._locked():
            started_at_ns = time_ns()
            self.directories.data.mkdir(parents=True, exist_ok=True)
            _require_private_directory(self.directories.travel_data_directory, ConfigurationFailure)
            _require_private_directory(self._root(request.provider), ConfigurationFailure)
            self._progress(
                provider=request.provider,
                state="building",
                stage="preparing",
                request_identifier=request.request_identifier,
            )
            try:
                if request.provider == "geonames":
                    from carl.io.geonames import build_geonames_dataset

                    manifest = await build_geonames_dataset(self._root("geonames"))
                else:
                    from carl.io.osrm_data import build_osrm_dataset

                    configuration = load_configuration(
                        self.directories.configuration_file
                    ).configuration
                    settings = configuration.travel_settings()
                    if settings is None or settings.origin is None:
                        raise TravelDataFailure(
                            "origin_coordinates_required",
                            "Set travel.origin coordinates in your private Carl configuration before building a regional routing map.",
                        )
                    manifest = await build_osrm_dataset(
                        self._root("osrm"),
                        origin=settings.origin,
                        radius_miles=request.radius_miles,
                        threads=request.threads,
                    )
                identifier = json.loads(manifest.read_bytes())["dataset_identifier"]
                await anyio.to_thread.run_sync(
                    lambda: _manifest(manifest, request.provider, identifier, verify=True)
                )
                if request.activate:
                    await anyio.to_thread.run_sync(self._activate, request.provider, identifier)
                self._progress(
                    provider=request.provider, state="completed", dataset_identifier=identifier
                )
                return TravelDataSetupResult(
                    provider=request.provider,
                    dataset_identifier=identifier,
                    manifest_path=str(manifest),
                    activated=request.activate,
                    message="Dataset verified and activated; subsequent requests use it."
                    if request.activate
                    else "Dataset verified. Activate it with carl travel activate or activate_travel_dataset.",
                )
            except BaseException as error:
                candidate_reason = getattr(error, "reason", None)
                reason = (
                    candidate_reason
                    if isinstance(candidate_reason, str)
                    and re.fullmatch(r"[a-z][a-z0-9_]{0,100}", candidate_reason)
                    else "setup_interrupted"
                    if isinstance(error, anyio.get_cancelled_exc_class())
                    else "setup_failed"
                )
                detail = None
                detail_path = self._root(request.provider) / "progress.json"
                try:
                    if (
                        detail_path.stat().st_mtime_ns >= started_at_ns
                        and detail_path.stat().st_size < 16_384
                    ):
                        candidate = json.loads(detail_path.read_bytes())
                        if isinstance(candidate, dict):
                            detail = candidate
                except (OSError, ValueError):
                    pass
                self._progress(
                    provider=request.provider,
                    state="cancelled" if reason == "setup_interrupted" else "failed",
                    reason=reason,
                    detail=detail,
                )
                if isinstance(error, TravelDataFailure):
                    raise
                if isinstance(error, Exception):
                    # Builder errors expose allowlisted codes; never copy raw native output.
                    safe_reason = getattr(error, "reason", None)
                    if not isinstance(safe_reason, str) or not re.fullmatch(
                        r"[a-z][a-z0-9_]{0,100}", safe_reason
                    ):
                        safe_reason = "travel_data_setup_failed"
                    raise TravelDataFailure(
                        safe_reason,
                        "Install the osmium-tool package and put osmium on PATH, then retry carl travel setup osrm. The previous active dataset is unchanged."
                        if safe_reason == "osrm_osmium_tool_missing"
                        else "Travel data setup failed; the previous active dataset is unchanged. Check carl travel status and the build progress/logs, correct prerequisites, then retry.",
                    ) from None
                raise

    def _activate(self, provider: TravelDataProvider, identifier: str) -> None:
        if not _IDENTIFIER.fullmatch(identifier):
            raise TravelDataFailure(
                "invalid_dataset_identifier", "Use an identifier returned by carl travel datasets."
            )
        manifest = self._root(provider) / identifier / "manifest.json"
        _manifest(manifest, provider, identifier, verify=False)
        path = self.directories.configuration_file
        original = path.lstat()
        raw = _read_regular_file(
            path,
            maximum_bytes=1_048_576,
            require_private=True,
            failure=ConfigurationFailure,
            unavailable_code="configuration_unavailable",
            permissions_code="configuration_permissions",
        )
        loaded = load_configuration(path).configuration
        document = tomlkit.parse(raw.decode())
        if "travel" not in document:
            settings = loaded.travel_settings()
            document["travel"] = (
                settings.model_dump(mode="json", exclude_none=True) if settings else {}
            )
        travel: Any = document["travel"]
        if provider == "osrm":
            travel["provider"] = "osrm"
            existing: Any = document.get("osrm", {})
            document["osrm"] = {
                **dict(existing),
                "dataset_path": str(manifest.parent / "region.osrm"),
                "dataset_identifier": identifier,
            }
        else:
            travel["geocoding_provider"] = "geonames"
            document["geonames"] = {
                "database_path": str(manifest.parent / "places.sqlite3"),
                "dataset_identifier": identifier,
            }
        updated = tomlkit.dumps(document).encode()
        # Validate without ever printing configuration or secret values.
        CarlConfiguration.model_validate(tomllib.loads(updated.decode()))
        temporary = path.with_name(".travel-config-" + secrets.token_hex(12))
        try:
            with open(
                temporary, "xb", opener=lambda p, flags: os.open(p, flags | os.O_NOFOLLOW, 0o600)
            ) as stream:
                stream.write(updated)
                stream.flush()
                os.fsync(stream.fileno())
            current = path.lstat()
            if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (
                original.st_dev,
                original.st_ino,
                original.st_size,
                original.st_mtime_ns,
            ):
                raise TravelDataFailure(
                    "configuration_changed",
                    "Configuration changed during activation; retry activation after the edit finishes.",
                )
            os.replace(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)

    async def activate(
        self, provider: TravelDataProvider, dataset_identifier: str
    ) -> TravelDataStatus:
        with self._locked():
            manifest = self._root(provider) / dataset_identifier / "manifest.json"
            if not _IDENTIFIER.fullmatch(dataset_identifier):
                raise TravelDataFailure(
                    "invalid_dataset_identifier", "Use an identifier from list_travel_datasets."
                )
            await anyio.to_thread.run_sync(
                lambda: _manifest(manifest, provider, dataset_identifier, verify=True)
            )
            try:
                await anyio.to_thread.run_sync(self._activate, provider, dataset_identifier)
            except (ConfigurationFailure, OSError):
                raise TravelDataFailure(
                    "configuration_update_failed",
                    "Dataset verification passed, but private configuration could not be updated. Check carl locations and configuration permissions, then retry activation.",
                ) from None
        return await self.status()

    def _status(self, verify: bool) -> TravelDataStatus:
        try:
            configuration = load_configuration(self.directories.configuration_file).configuration
        except ConfigurationFailure:
            configuration = None
        settings = configuration.travel_settings() if configuration else None
        statuses = []
        for provider in _PROVIDERS:
            entry = getattr(configuration, provider, None)
            identifier = entry.dataset_identifier if entry else None
            path = (
                (entry.dataset_path if provider == "osrm" else entry.database_path)
                if entry
                else None
            )
            if path is None:
                statuses.append(
                    TravelProviderStatus(
                        provider=provider,
                        state="unconfigured",
                        message=f"Local {provider} data has not been selected.",
                        actions=(f"carl travel setup {provider}", "request_travel_data_setup"),
                    )
                )
                continue
            if not path.parent.joinpath("manifest.json").is_file():
                statuses.append(
                    TravelProviderStatus(
                        provider=provider,
                        state="missing",
                        dataset_identifier=identifier,
                        reason="manifest_missing",
                        message=f"Selected {provider} dataset is missing its manifest.",
                        actions=(f"carl travel setup {provider}",),
                    )
                )
                continue
            try:
                assert isinstance(identifier, str)
                if path != path.parent / (
                    "region.osrm" if provider == "osrm" else "places.sqlite3"
                ):
                    raise TravelDataFailure(
                        "dataset_path_invalid",
                        "Configured dataset path does not match its manifest.",
                    )
                manifest = _manifest(
                    path.parent / "manifest.json", provider, identifier, verify=verify
                )
                created = manifest.get("created_at_utc")
                outdated = created is not None and datetime.fromisoformat(created) < datetime.now(
                    UTC
                ) - timedelta(days=30)
                statuses.append(
                    TravelProviderStatus(
                        provider=provider,
                        state="ready",
                        dataset_identifier=identifier,
                        message=f"Local {provider} data is ready."
                        + (
                            " Data is over 30 days old; an explicit update is recommended."
                            if outdated
                            else ""
                        ),
                        actions=(f"carl travel update {provider}",) if outdated else (),
                        size_bytes=sum(item["bytes"] for item in manifest["files"]),
                        created_at_utc=created,
                        outdated=outdated,
                    )
                )
            except (TravelDataFailure, ValueError, TypeError) as error:
                statuses.append(
                    TravelProviderStatus(
                        provider=provider,
                        state="incompatible"
                        if isinstance(error, TravelDataFailure)
                        and error.reason == "osrm_incompatible"
                        else "invalid",
                        dataset_identifier=identifier,
                        reason=error.reason
                        if isinstance(error, TravelDataFailure)
                        else "dataset_invalid",
                        message=f"Selected {provider} dataset is invalid or incompatible. Rebuild or activate another version.",
                        actions=(f"carl travel update {provider}", "carl travel datasets"),
                    )
                )
        progress = None
        state = self.directories.travel_data_state_directory
        try:
            progress = json.loads((state / "progress.json").read_bytes())
            if not isinstance(progress, dict) or progress.get("provider") not in _PROVIDERS:
                progress = None
                raise ValueError
            if progress.get("state") == "building":
                descriptor = os.open(state / "build.lock", os.O_RDWR | os.O_NOFOLLOW)
                try:
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        progress["running"] = True
                    else:
                        progress.update(
                            state="interrupted",
                            running=False,
                            message="No build owns the lock. Retry the explicit setup/update request.",
                        )
                finally:
                    os.close(descriptor)
                if progress.get("running"):
                    detail = self._root(progress["provider"]) / "progress.json"
                    if detail.exists() and detail.stat().st_size < 16_384:
                        progress["detail"] = json.loads(detail.read_bytes())
        except (OSError, ValueError, KeyError):
            progress = None
        return TravelDataStatus(
            routing_provider=settings.provider if settings else None,
            geocoding_provider=settings.geocoding_provider if settings else None,
            origin_configured=bool(
                settings and (settings.origin is not None or settings.origin_label)
            ),
            configuration_message="Carl configuration is missing or invalid. Check carl locations and your private config.toml."
            if configuration is None
            else "Set travel.origin coordinates or origin_label in private config before requesting routes."
            if settings is None or (settings.origin is None and not settings.origin_label)
            else None,
            providers=tuple(statuses),
            build=progress,
        )

    async def status(self, *, verify: bool = False) -> TravelDataStatus:
        return await anyio.to_thread.run_sync(self._status, verify)

    async def datasets(self, *, limit: int = 20, cursor: str | None = None) -> TravelDatasetPage:
        if not 1 <= limit <= 50:
            raise ValueError("Dataset page limit must be between 1 and 50")
        status = await self.status()
        active = {
            (item.provider, item.dataset_identifier)
            for item in status.providers
            if (item.provider == "osrm" and status.routing_provider == "osrm")
            or (item.provider == "geonames" and status.geocoding_provider == "geonames")
        }

        def scan() -> TravelDatasetPage:
            entries = []
            for provider in _PROVIDERS:
                for path in self._root(provider).glob("*/manifest.json"):
                    key = provider + ":" + path.parent.name
                    if cursor is not None and key <= cursor:
                        continue
                    try:
                        manifest = _manifest(path, provider, path.parent.name, verify=False)
                        entries.append(
                            (
                                key,
                                TravelDatasetSummary(
                                    provider=provider,
                                    dataset_identifier=path.parent.name,
                                    active=(provider, path.parent.name) in active,
                                    size_bytes=sum(item["bytes"] for item in manifest["files"]),
                                    created_at_utc=manifest.get("created_at_utc"),
                                ),
                            )
                        )
                    except TravelDataFailure:
                        continue
            entries.sort(key=lambda item: item[0])
            return TravelDatasetPage(
                items=tuple(item[1] for item in entries[:limit]),
                next_cursor=entries[limit - 1][0] if len(entries) > limit else None,
            )

        return await anyio.to_thread.run_sync(scan)
