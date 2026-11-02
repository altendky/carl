"""Explicit, versioned local travel-data setup; startup never builds datasets."""

import fcntl
import json
import os
import re
import secrets
import tomllib
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
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
    load_configuration,
)
from carl.io.dataset_workflow_files import (
    DatasetWorkflowBusy,
    DatasetWorkflowError,
    active_journal,
    dataset_workflow_gate,
)
from carl.io.paths import CarlDirectories
from carl.io.travel_manifest import read_travel_manifest as _manifest

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,199}$")
_PROVIDERS: tuple[TravelDataProvider, ...] = ("osrm", "geonames")


class TravelDataManager:
    def __init__(self, directories: CarlDirectories):
        self.directories = directories

    def _root(self, provider: TravelDataProvider) -> Path:
        return self.directories.travel_data_directory / provider

    @contextmanager
    def _locked(self) -> Iterator[None]:
        gate = dataset_workflow_gate(self.directories)
        try:
            _ = gate.__enter__()
        except DatasetWorkflowBusy:
            raise TravelDataFailure(
                "travel_data_busy",
                "Another travel-data setup or activation is running. Check get_travel_data_status or carl travel status.",
            ) from None
        except (OSError, DatasetWorkflowError):
            raise TravelDataFailure(
                "travel_state_unavailable",
                "Travel-data state directory must be writable, private and owned by the current user. Check carl locations.",
            ) from None
        try:
            yield
        finally:
            _ = gate.__exit__(None, None, None)

    def _progress(self, **value: Any) -> None:
        path = self.directories.travel_data_state_directory / "progress.json"
        temporary = path.with_name(".progress-" + secrets.token_hex(8))
        with open(temporary, "x", opener=lambda p, flags: os.open(p, flags, 0o600)) as stream:
            json.dump({**value, "updated_at_utc": datetime.now(UTC).isoformat()}, stream)
        temporary.replace(path)

    async def setup(self, request: TravelDataSetupRequest) -> TravelDataSetupResult:
        from carl.io.dataset_setup import setup_travel_data

        return await setup_travel_data(self, request)

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
        try:
            journal = active_journal(self.directories)
            if journal is not None and (
                progress is None
                or progress.get("request_identifier")
                != journal.definition.request.request_identifier
                or journal.state not in ("completed", "failed", "cancelled")
                or progress.get("state") != journal.state
            ):
                present = False
                descriptor = os.open(state / "build.lock", os.O_RDWR | os.O_NOFOLLOW)
                try:
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        present = True
                finally:
                    os.close(descriptor)
                journal_state = journal.state
                if journal_state in ("running", "waiting") and not present:
                    journal_state = "interrupted"
                elif journal_state == "running":
                    journal_state = "building"
                progress = {
                    "provider": journal.definition.request.provider,
                    "request_identifier": journal.definition.request.request_identifier,
                    "workflow_identifier": journal.definition.workflow_identifier,
                    "state": journal_state,
                    "stage": journal.phase,
                    "running": present and journal.state in ("running", "waiting"),
                    "physical_owner_present": present,
                    "updated_at_utc_ns": journal.updated_at_utc_ns,
                }
        except (OSError, ValueError, DatasetWorkflowError):
            pass
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
