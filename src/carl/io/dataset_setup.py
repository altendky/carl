"""Shared replayable dataset phases for explicit setup and durable stage adapters."""

import hashlib
import os
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import partial
from pathlib import Path
from time import time_ns
from typing import Protocol
from uuid import uuid4

import anyio
import httpx

from carl.core.dataset_downloads import (
    DatasetFileError,
    DatasetSource,
    DatasetTransfer,
    FileReceipt,
    dataset_source_fingerprint,
)
from carl.core.dataset_setup import DatasetSetupReference, DatasetSetupState
from carl.core.dataset_workflows import DatasetWorkflowBinding, DatasetWorkflowDefinition
from carl.core.geonames_build import GeonamesBuildPlan, GeonamesCandidate
from carl.core.json import encode_json
from carl.core.models import JsonValue, StrictModel
from carl.core.native_build import NativeBuildSpec
from carl.core.osrm_build import (
    OsrmBuildAttempt,
    OsrmBuildPlan,
    OsrmCoarseReceipt,
    OsrmPlanSeed,
    OsrmSourceCandidate,
)
from carl.core.resources import ResourceRequest
from carl.core.travel import TravelCoordinate
from carl.core.travel_data import (
    TravelDataFailure,
    TravelDataProvider,
    TravelDataSetupRequest,
    TravelDataSetupResult,
)
from carl.io.configuration import load_configuration
from carl.io.dataset_downloads import (
    discard_incomplete_dataset_transfer,
    recover_dataset_transfer,
    verify_dataset_file,
)
from carl.io.dataset_setup_cleanup import cleanup_dataset_setup_scratch
from carl.io.dataset_setup_files import read_setup_value, write_setup_value
from carl.io.dataset_workflow_files import (
    DatasetWorkflowBusy,
    active_journal,
    dataset_workflow_gate,
    private_directory,
    read_journal,
    workflow_directory,
)
from carl.io.dataset_workflow_owner import DatasetWorkflowOwner, open_dataset_workflow
from carl.io.geonames import (
    GeonamesFailure,
    build_geonames_candidate,
    existing_geonames_dataset,
    prepare_geonames_build,
    publish_geonames_candidate,
)
from carl.io.native_build_protocol import NativeBuildCleanupError
from carl.io.native_builds import BuildCommandCapture, run_native_build_command
from carl.io.osrm_build import (
    OsrmBuildError,
    abandon_osrm_attempt,
    build_osrm_attempt,
    create_osrm_attempt,
    freeze_osrm_plan,
    osrm_download_sources,
    osrm_index_source,
    osrm_probe_sources,
    parse_osrm_checksum,
    prepare_osrm_seed,
    publish_osrm_dataset,
    select_osrm_sources,
    verify_osrm_attempt,
)
from carl.io.paths import CarlDirectories
from carl.io.resource_permits import cpu_reservation, current_resource_manager, run_cpu
from carl.io.sqlite import Database
from carl.io.streaming_http import open_dataset_file, stream_dataset_source
from carl.io.travel_manifest import read_travel_manifest


class DatasetSetupManager(Protocol):
    directories: CarlDirectories

    def _activate(self, provider: TravelDataProvider, identifier: str) -> None: ...

    def _progress(self, **value: JsonValue) -> None: ...


def _decode[T: StrictModel](model: type[T], value: JsonValue) -> T:
    return model.model_validate_json(encode_json(value))


def _value(state: DatasetSetupState) -> dict[str, JsonValue]:
    if not isinstance(state.value, dict):
        raise ValueError("Dataset setup state must be an object")
    return dict(state.value)


def _next(state: DatasetSetupState, phase: str, value: dict[str, JsonValue]) -> DatasetSetupState:
    return DatasetSetupState(definition=state.definition, phase=phase, value=value)


def dataset_setup_resources(state: DatasetSetupState) -> tuple[ResourceRequest, ...]:
    if state.phase == "done":
        return ()
    if state.phase == "capture_http":
        return (ResourceRequest(name="http"),)
    units = state.definition.request.threads if state.phase == "osrm_build" else 1
    return (ResourceRequest(name="cpu", units=units),)


def _origin(directories: CarlDirectories, provider: str) -> TravelCoordinate | None:
    if provider != "osrm":
        return None
    settings = load_configuration(directories.configuration_file).configuration.travel_settings()
    if settings is None or settings.origin is None:
        raise TravelDataFailure(
            "origin_coordinates_required",
            "Set travel.origin coordinates before building a regional routing map.",
        )
    return settings.origin


def _configuration_identity(provider: str, origin: TravelCoordinate | None) -> str:
    return hashlib.sha256(
        encode_json(
            {
                "provider": provider,
                "origin": None if origin is None else origin.model_dump(mode="json"),
            }
        ).encode()
    ).hexdigest()


def dataset_setup_identifier(
    request: TravelDataSetupRequest, binding: DatasetWorkflowBinding | None = None
) -> str:
    identity = {
        "request": request.request_identifier,
        "binding": None if binding is None else binding.model_dump(mode="json"),
    }
    return "setup-" + hashlib.sha256(encode_json(identity).encode()).hexdigest()[:48]


async def prepare_dataset_setup(
    directories: CarlDirectories,
    request: TravelDataSetupRequest,
    *,
    binding: DatasetWorkflowBinding | None = None,
    origin: TravelCoordinate | None = None,
) -> DatasetSetupState:
    resources = current_resource_manager()
    if resources is None:
        raise ValueError("Dataset setup requires its managed resource context")
    if request.provider == "osrm" and request.threads > resources.capacities["cpu"]:
        raise TravelDataFailure(
            "osrm_cpu_capacity_exceeded",
            "Requested build threads exceed the configured CPU capacity.",
        )
    if origin is None:
        origin = await run_cpu(_origin, directories, request.provider)
    configuration = _configuration_identity(request.provider, origin)
    definition = DatasetWorkflowDefinition(
        workflow_identifier=dataset_setup_identifier(request, binding),
        request=request,
        recipe_identifier="carl-dataset-setup-v1",
        configuration_identity=configuration,
        binding=binding,
    )
    return DatasetSetupState(
        definition=definition,
        phase="prepare",
        value={
            "origin": None if origin is None else origin.model_dump(mode="json"),
            "anchor_date": datetime.now(UTC).date().isoformat(),
        },
    )


def _read_download(directory: Path, receipt: FileReceipt, maximum: int) -> bytes:
    if receipt.bytes > maximum:
        raise DatasetFileError("dataset_metadata_too_large")
    descriptor = open_dataset_file(directory, receipt.locator)
    try:
        content = bytearray()
        while len(content) <= receipt.bytes:
            chunk = os.read(descriptor, min(262_144, receipt.bytes + 1 - len(content)))
            if not chunk:
                break
            content.extend(chunk)
        if len(content) != receipt.bytes or hashlib.sha256(content).hexdigest() != receipt.sha256:
            raise DatasetFileError("dataset_file_invalid")
        return bytes(content)
    finally:
        os.close(descriptor)


@dataclass(slots=True)
class DatasetSetupEngine:
    directories: CarlDirectories
    manager: DatasetSetupManager | None = None
    transport: httpx.AsyncBaseTransport | None = None
    osmium_executable: str = "osmium"
    destination_root: Path | None = None
    command_runner: Callable[[NativeBuildSpec], Awaitable[BuildCommandCapture]] | None = None

    def _store(self, owner: DatasetWorkflowOwner, value: JsonValue) -> dict[str, JsonValue]:
        return write_setup_value(owner.directory, value).model_dump(mode="json")

    def _load(self, owner: DatasetWorkflowOwner, reference: JsonValue) -> JsonValue:
        return read_setup_value(owner.directory, _decode(DatasetSetupReference, reference))

    def _receipt(self, owner: DatasetWorkflowOwner, reference: JsonValue) -> FileReceipt:
        return _decode(FileReceipt, self._load(owner, reference))

    def _capture(
        self,
        state: DatasetSetupState,
        value: dict[str, JsonValue],
        source: DatasetSource,
        purpose: str,
        key: str,
    ) -> DatasetSetupState:
        value["capture"] = {
            "source": source.model_dump(mode="json"),
            "purpose": purpose,
            "key": key,
            "attempt": 0,
        }
        value.pop("wait_until", None)
        return _next(state, "capture_prepare", value)

    def _sources(
        self, state: DatasetSetupState, value: dict[str, JsonValue]
    ) -> tuple[DatasetSource, ...]:
        if state.definition.request.provider == "geonames":
            return _decode(GeonamesBuildPlan, value["plan"]).sources
        return osrm_download_sources(_decode(OsrmBuildPlan, value["plan"]))

    def _next_source(
        self, state: DatasetSetupState, value: dict[str, JsonValue]
    ) -> DatasetSetupState:
        sources = self._sources(state, value)
        cursor = int(value.get("cursor", 0))
        if cursor == len(sources):
            return _next(
                state,
                "geonames_build"
                if state.definition.request.provider == "geonames"
                else "osrm_build",
                value,
            )
        return self._capture(state, value, sources[cursor], "source", f"source-{cursor}")

    def _next_probe(
        self, state: DatasetSetupState, value: dict[str, JsonValue]
    ) -> DatasetSetupState:
        seed = _decode(OsrmPlanSeed, value["seed"])
        candidates = tuple(_decode(OsrmSourceCandidate, item) for item in value["candidates"])
        probes = osrm_probe_sources(candidates, anchor_date=seed.anchor_date, age=int(value["age"]))
        cursor = int(value["cursor"])
        return self._capture(
            state, value, probes[cursor], "checksum", f"checksum-{value['age']}-{cursor}"
        )

    async def step(
        self, owner: DatasetWorkflowOwner, state: DatasetSetupState
    ) -> DatasetSetupState:
        _ = owner.lock_fd
        if owner.journal.definition != state.definition:
            raise ValueError("Dataset setup state belongs to a different frozen workflow")
        value = _value(state)
        value.pop("wait_until", None)
        request = state.definition.request
        if state.phase == "prepare":
            if request.provider == "geonames":
                plan = (
                    _decode(GeonamesBuildPlan, owner.journal.plan)
                    if owner.journal.plan_frozen
                    else await run_cpu(prepare_geonames_build)
                )
                value.update(plan=plan.model_dump(mode="json"), cursor=0, receipts=[])
                await owner.freeze_plan(value["plan"])
                return self._next_source(state, value)
            seed = await run_cpu(
                partial(
                    prepare_osrm_seed,
                    owner.directory,
                    origin=_decode(TravelCoordinate, value["origin"]),
                    radius_miles=request.radius_miles,
                    threads=request.threads,
                    anchor_date=date.fromisoformat(value["anchor_date"]),
                    osmium_executable=self.osmium_executable,
                )
            )
            value["seed"] = seed.model_dump(mode="json")
            return self._capture(state, value, osrm_index_source(), "index", "index")
        if state.phase.startswith("capture_"):
            capture = dict(value["capture"])
            source = _decode(DatasetSource, capture["source"])
            execution = f"{capture['key']}-{capture['attempt']}"
            if state.phase == "capture_prepare":
                recovered = await recover_dataset_transfer(
                    source, directory=owner.directory, execution_identifier=execution
                )
                if recovered is not None:
                    capture["transfer"] = self._store(owner, recovered.model_dump(mode="json"))
                    value["capture"] = capture
                    return _next(state, "capture_verify", value)
                prepared = (
                    owner.directory
                    / "downloads"
                    / dataset_source_fingerprint(source)
                    / f"{execution}.prepared.json"
                )
                if prepared.exists():
                    await discard_incomplete_dataset_transfer(
                        source, directory=owner.directory, execution_identifier=execution
                    )
                    if int(capture["attempt"]) >= 2:
                        raise DatasetFileError("dataset_interrupted_download_limit")
                    capture["attempt"] = int(capture["attempt"]) + 1
                    value["wait_until"] = time_ns() + 1_000_000_000
                value["capture"] = capture
                return _next(state, "capture_http", value)
            if state.phase == "capture_http":
                try:
                    transfer = await stream_dataset_source(
                        source,
                        directory=owner.directory,
                        execution_identifier=execution,
                        transport=self.transport,
                    )
                except DatasetFileError as error:
                    if error.reason == "dataset_transfer_exists":
                        return _next(state, "capture_prepare", value)
                    if capture["purpose"] == "checksum" and error.status_code == 404:
                        value["checksum_missing"] = True
                        return _next(state, "checksum_parse", value)
                    if request.provider == "geonames" and capture["purpose"] == "source":
                        reason = {
                            "dataset_http_failed": "geonames_http_failed",
                            "dataset_download_failed": "geonames_download_failed",
                            "dataset_download_too_large": "geonames_download_too_large",
                            "dataset_empty_source": "geonames_build_failed",
                            "dataset_storage_failure": "geonames_build_failed",
                        }.get(error.reason)
                        if reason is not None:
                            raise GeonamesFailure(reason) from None
                    raise
                capture["transfer"] = self._store(owner, transfer.model_dump(mode="json"))
                value["capture"] = capture
                return _next(state, "capture_verify", value)
            transfer = _decode(DatasetTransfer, self._load(owner, capture["transfer"]))
            try:
                receipt = await verify_dataset_file(source, transfer, directory=owner.directory)
            except DatasetFileError as error:
                if request.provider == "osrm" and error.reason == "dataset_checksum_mismatch":
                    raise OsrmBuildError("osrm_source_checksum_mismatch") from None
                raise
            reference = self._store(owner, receipt.model_dump(mode="json"))
            if capture["purpose"] == "source":
                value["receipts"] = [*value["receipts"], reference]
                value["cursor"] = int(value["cursor"]) + 1
                value.pop("capture")
                return self._next_source(state, value)
            value["capture_receipt"] = reference
            return _next(
                state, "index_parse" if capture["purpose"] == "index" else "checksum_parse", value
            )
        if state.phase == "index_parse":
            receipt = self._receipt(owner, value["capture_receipt"])
            content = await run_cpu(_read_download, owner.directory, receipt, 16_777_216)
            candidates = await run_cpu(
                select_osrm_sources, content, _decode(OsrmPlanSeed, value["seed"])
            )
            value.update(
                candidates=[item.model_dump(mode="json") for item in candidates],
                cursor=0,
                age=1,
                checksums=[],
            )
            value.pop("capture", None)
            return self._next_probe(state, value)
        if state.phase == "checksum_parse":
            if value.pop("checksum_missing", False):
                value.update(age=int(value["age"]) + 1, cursor=0, checksums=[])
                if value["age"] > 7:
                    raise OsrmBuildError("osrm_coherent_snapshot_unavailable")
                return self._next_probe(state, value)
            receipt = self._receipt(owner, value["capture_receipt"])
            content = await run_cpu(_read_download, owner.directory, receipt, 4096)
            checksum = await run_cpu(parse_osrm_checksum, content)
            value["checksums"] = [*value["checksums"], checksum]
            value["cursor"] = int(value["cursor"]) + 1
            if value["cursor"] < len(value["candidates"]):
                return self._next_probe(state, value)
            plan = freeze_osrm_plan(
                _decode(OsrmPlanSeed, value["seed"]),
                tuple(_decode(OsrmSourceCandidate, item) for item in value["candidates"]),
                age=int(value["age"]),
                checksums=tuple(value["checksums"]),
            )
            value.update(plan=plan.model_dump(mode="json"), cursor=0, receipts=[])
            for key in ("seed", "candidates", "checksums", "capture", "capture_receipt"):
                value.pop(key, None)
            await owner.freeze_plan(value["plan"])
            return self._next_source(state, value)
        if state.phase in ("geonames_build", "osrm_build"):
            receipts = tuple(self._receipt(owner, ref) for ref in value["receipts"])
            attempt_number = int(value.get("build_attempt", 0))
            if state.phase == "geonames_build":
                plan = _decode(GeonamesBuildPlan, value["plan"])
                existing = await existing_geonames_dataset(
                    plan,
                    receipts,
                    root=self.destination_root
                    or self.directories.travel_data_directory / "geonames",
                )
                if existing is not None:
                    value.update(
                        manifest_path=str(existing), dataset_identifier=existing.parent.name
                    )
                    return _next(state, "activate", value)
                try:
                    candidate = await build_geonames_candidate(
                        plan,
                        receipts,
                        directory=owner.directory,
                        execution_identifier=f"geonames-{attempt_number}",
                    )
                except Exception as error:
                    if (
                        getattr(error, "reason", None) != "geonames_candidate_uncertain"
                        or attempt_number >= 2
                    ):
                        raise
                    value["build_attempt"] = attempt_number + 1
                    return _next(state, state.phase, value)
            else:
                plan = _decode(OsrmBuildPlan, value["plan"])
                attempt = OsrmBuildAttempt(
                    identifier=f"native-{attempt_number}", plan_identifier=plan.identifier
                )
                try:
                    if (owner.directory / attempt.locator).exists():
                        candidate = await verify_osrm_attempt(owner.directory, plan, attempt)
                    else:
                        attempt = await run_cpu(
                            partial(
                                create_osrm_attempt,
                                owner.directory,
                                plan,
                                identifier=attempt.identifier,
                            )
                        )

                        async def command(spec: NativeBuildSpec) -> BuildCommandCapture:
                            if self.command_runner is not None:
                                return await self.command_runner(spec)
                            return await run_native_build_command(
                                spec, workflow_lock_fd=owner.lock_fd
                            )

                        candidate = await build_osrm_attempt(
                            owner.directory, plan, attempt, receipts, run_command=command
                        )
                except OsrmBuildError as error:
                    if error.reason != "osrm_build_attempt_uncertain" or attempt_number >= 2:
                        raise
                    await abandon_osrm_attempt(owner.directory, plan, attempt)
                    value["build_attempt"] = attempt_number + 1
                    return _next(state, state.phase, value)
            value["candidate"] = self._store(owner, candidate.model_dump(mode="json"))
            return _next(state, "publish", value)
        if state.phase == "publish":
            candidate = self._load(owner, value["candidate"])
            root = (
                self.destination_root or self.directories.travel_data_directory / request.provider
            )
            if request.provider == "geonames":
                manifest = await publish_geonames_candidate(
                    _decode(GeonamesBuildPlan, value["plan"]),
                    tuple(self._receipt(owner, ref) for ref in value["receipts"]),
                    _decode(GeonamesCandidate, candidate),
                    directory=owner.directory,
                    root=root,
                )
            else:
                manifest = await publish_osrm_dataset(
                    root,
                    owner.directory,
                    _decode(OsrmBuildPlan, value["plan"]),
                    _decode(OsrmCoarseReceipt, candidate),
                )
            value["manifest_path"] = str(manifest)
            value["dataset_identifier"] = candidate["dataset_identifier"]
            return _next(state, "activate", value)
        if state.phase == "activate":
            if self.manager is not None:
                await run_cpu(
                    partial(
                        read_travel_manifest,
                        Path(value["manifest_path"]),
                        request.provider,
                        value["dataset_identifier"],
                        verify=True,
                    )
                )
                if request.activate:
                    origin = await run_cpu(_origin, self.directories, request.provider)
                    if (
                        _configuration_identity(request.provider, origin)
                        != state.definition.configuration_identity
                    ):
                        raise TravelDataFailure(
                            "configuration_changed",
                            "Build configuration changed; the previous active dataset remains selected.",
                        )
                    await run_cpu(
                        self.manager._activate, request.provider, value["dataset_identifier"]
                    )
            elif request.activate:
                raise ValueError("Dataset activation requires its configured manager")
            result = TravelDataSetupResult(
                provider=request.provider,
                dataset_identifier=value["dataset_identifier"],
                manifest_path=value["manifest_path"],
                activated=request.activate,
                message="Dataset verified and activated; subsequent requests use it."
                if request.activate
                else "Dataset verified. Activate it with carl travel activate or activate_travel_dataset.",
            )
            value["result"] = result.model_dump(mode="json")
            return _next(state, "cleanup", value)
        if state.phase == "cleanup":
            result = _decode(TravelDataSetupResult, value["result"])
            await run_cpu(
                partial(
                    read_travel_manifest,
                    Path(result.manifest_path),
                    request.provider,
                    result.dataset_identifier,
                    verify=True,
                )
            )
            attempts = (
                tuple(f"native-{index}" for index in range(int(value.get("build_attempt", 0)) + 1))
                if request.provider == "osrm"
                else ()
            )
            await cleanup_dataset_setup_scratch(owner.directory, native_attempts=attempts)
            return _next(state, "done", value)
        if state.phase == "done":
            return state
        raise ValueError("Unsupported dataset setup phase")


def _cleanup_uncertain(error: BaseException) -> bool:
    if isinstance(error, NativeBuildCleanupError):
        return True
    return isinstance(error, BaseExceptionGroup) and any(
        _cleanup_uncertain(item) for item in error.exceptions
    )


def dataset_setup_failure(error: Exception) -> TravelDataFailure:
    if isinstance(error, TravelDataFailure):
        return error
    reason = (
        "native_build_cleanup_unconfirmed"
        if _cleanup_uncertain(error)
        else getattr(error, "reason", "travel_data_setup_failed")
    )
    if isinstance(reason, str):
        reason = reason.partition(":")[0]
    if not isinstance(reason, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,100}", reason):
        reason = "travel_data_setup_failed"
    return TravelDataFailure(
        reason,
        "Install osmium-tool and put osmium on PATH, then retry setup."
        if reason == "osrm_osmium_tool_missing"
        else "Native build cleanup is unconfirmed; the workflow remains quarantined until physical recovery."
        if reason == "native_build_cleanup_unconfirmed"
        else "Travel data setup failed. Check travel status and retained workflow files before retrying.",
    )


def _previous_state(
    directories: CarlDirectories, request: TravelDataSetupRequest
) -> DatasetSetupState | None:
    with dataset_workflow_gate(directories, allow_reserved=True):
        active = active_journal(directories)
        identifier = dataset_setup_identifier(request)
        if (
            active is not None
            and active.state not in ("completed", "failed", "cancelled")
            and active.definition.workflow_identifier != identifier
        ):
            raise DatasetWorkflowBusy("Another dataset setup is reserved")
        try:
            journal = read_journal(workflow_directory(directories, identifier))
        except FileNotFoundError:
            return None
        if journal is None:
            return None
        if journal.definition.request != request or journal.definition.binding is not None:
            raise TravelDataFailure(
                "dataset_request_changed",
                "A setup request identifier cannot change its parameters.",
            )
        if journal.state in ("failed", "cancelled"):
            raise TravelDataFailure(
                "dataset_request_terminal", "Use a new request identifier for a new setup attempt."
            )
        if journal.checkpoint is None:
            if journal.plan_frozen:
                raise ValueError("A started dataset workflow lost its setup checkpoint")
            return DatasetSetupState(definition=journal.definition, phase="unprepared")
        state = _decode(DatasetSetupState, journal.checkpoint)
        if state.definition != journal.definition:
            raise ValueError("Dataset setup checkpoint and ownership differ")
        return state


async def run_dataset_setup_direct(
    engine: DatasetSetupEngine,
    request: TravelDataSetupRequest,
    *,
    origin: TravelCoordinate | None = None,
) -> TravelDataSetupResult:
    """Drive the same phases without inventing queue work or retaining idle permits."""
    state = _previous_state(engine.directories, request)
    if state is not None and state.phase == "done":
        result = _decode(TravelDataSetupResult, _value(state)["result"])
        root = (
            engine.destination_root or engine.directories.travel_data_directory / request.provider
        )
        if Path(result.manifest_path) != root / result.dataset_identifier / "manifest.json":
            raise ValueError("Dataset result does not identify its owned output")
        await run_cpu(
            partial(
                read_travel_manifest,
                Path(result.manifest_path),
                request.provider,
                result.dataset_identifier,
                verify=True,
            )
        )
        return result
    if state is None or state.phase == "unprepared":
        try:
            prepared = await prepare_dataset_setup(engine.directories, request, origin=origin)
        except Exception:
            if state is not None:
                async with open_dataset_workflow(
                    engine.directories, state.definition, resume=True
                ) as owner:
                    await owner.finish("failed")
            raise
        if state is not None and state.definition != prepared.definition:
            async with open_dataset_workflow(
                engine.directories, state.definition, resume=True
            ) as owner:
                await owner.finish("failed")
            raise TravelDataFailure(
                "configuration_changed",
                "Setup configuration changed before preparation completed; use a new request identifier.",
            )
        state = prepared
    async with open_dataset_workflow(engine.directories, state.definition, resume=True) as owner:
        try:
            # The initial snapshot must be durable before any provider I/O.
            await owner.checkpoint(state.phase, state.model_dump(mode="json"))
            while state.phase != "done":
                await owner.checkpoint(state.phase, state.model_dump(mode="json"))
                if engine.manager is not None:
                    engine.manager._progress(
                        provider=request.provider,
                        state="building",
                        stage=state.phase,
                        request_identifier=request.request_identifier,
                    )
                resources = dataset_setup_resources(state)
                if resources and resources[0].name == "cpu":
                    async with cpu_reservation(resources[0].units):
                        following = await engine.step(owner, state)
                else:
                    following = await engine.step(owner, state)
                await owner.checkpoint(following.phase, following.model_dump(mode="json"))
                state = following
                deadline = _value(state).get("wait_until")
                if isinstance(deadline, int) and deadline > time_ns():
                    await owner.wait_until(deadline)
                    await anyio.sleep((deadline - time_ns()) / 1_000_000_000)
            result = _decode(TravelDataSetupResult, _value(state)["result"])
            if engine.manager is not None:
                engine.manager._progress(
                    provider=request.provider,
                    state="completed",
                    dataset_identifier=result.dataset_identifier,
                    request_identifier=request.request_identifier,
                )
            await owner.finish()
            return result
        except NativeBuildCleanupError:
            raise
        except BaseException as error:
            with anyio.CancelScope(shield=True):
                if _cleanup_uncertain(error):
                    await owner.park(quarantined=True)
                    if engine.manager is not None:
                        engine.manager._progress(
                            provider=request.provider,
                            state="quarantined",
                            reason="native_build_cleanup_unconfirmed",
                            request_identifier=request.request_identifier,
                        )
                    raise
                if isinstance(error, anyio.get_cancelled_exc_class()):
                    await owner.finish("cancelled")
                    if engine.manager is not None:
                        engine.manager._progress(
                            provider=request.provider,
                            state="cancelled",
                            request_identifier=request.request_identifier,
                        )
                elif isinstance(error, Exception):
                    if owner.journal.state not in ("completed", "failed", "cancelled"):
                        await owner.finish("failed")
                    if engine.manager is not None:
                        engine.manager._progress(
                            provider=request.provider,
                            state="failed",
                            reason=dataset_setup_failure(error).reason,
                            request_identifier=request.request_identifier,
                        )
            if isinstance(error, Exception):
                raise dataset_setup_failure(error) from None
            raise


async def setup_travel_data(
    manager: DatasetSetupManager, request: TravelDataSetupRequest
) -> TravelDataSetupResult:
    """Bind foreground setup to the standard evidence database and shared limits."""

    async def perform() -> TravelDataSetupResult:
        try:
            result = await run_dataset_setup_direct(
                DatasetSetupEngine(manager.directories, manager), request
            )
        except DatasetWorkflowBusy:
            raise TravelDataFailure(
                "travel_data_busy",
                "Another travel-data setup or activation owns the workflow. Resume its request identifier or wait for it to finish.",
            ) from None
        except Exception as error:
            failure = dataset_setup_failure(error)
            raise failure from None
        return result

    if current_resource_manager() is not None:
        return await perform()
    private_directory(manager.directories.travel_data_directory)
    async with Database.managed(manager.directories.database_file, initialize=True):
        return await perform()


async def build_osrm_dataset_direct(
    root: Path,
    *,
    origin: TravelCoordinate,
    radius_miles: float = 250,
    threads: int = 4,
    transport: httpx.AsyncBaseTransport | None = None,
    osmium_executable: str = "osmium",
) -> Path:
    """Programmatic builder; its caller supplies the managed resource lifetime.

    This explicit low-level target uses a private gate beside its output root.
    User-facing setup uses CarlDirectories and the standard setup/activation gate.
    """
    if current_resource_manager() is None:
        raise ValueError("OSRM builds require a managed resource context")
    root = root.absolute()
    directories = CarlDirectories(
        config=root / ".dataset-config",
        data=root,
        cache=root / ".dataset-cache",
        state=root / ".dataset-state",
        runtime=root / ".dataset-runtime",
    )
    request = TravelDataSetupRequest(
        request_identifier="direct-" + str(uuid4()),
        provider="osrm",
        activate=False,
        radius_miles=radius_miles,
        threads=threads,
    )
    with dataset_workflow_gate(directories, allow_reserved=True):
        active = active_journal(directories)
        if active is not None and active.state not in ("completed", "failed", "cancelled"):
            previous = active.definition.request
            if (
                active.definition.binding is not None
                or previous.provider != "osrm"
                or previous.activate
                or previous.radius_miles != radius_miles
                or previous.threads != threads
                or active.definition.configuration_identity
                != _configuration_identity("osrm", origin)
            ):
                raise DatasetWorkflowBusy("A different direct build is reserved")
            request = previous
    engine = DatasetSetupEngine(
        directories,
        transport=transport,
        osmium_executable=osmium_executable,
        destination_root=root,
    )
    result = await run_dataset_setup_direct(engine, request, origin=origin)
    return Path(result.manifest_path)
