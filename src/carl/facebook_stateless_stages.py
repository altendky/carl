"""Durable Facebook image/detail capture followed by offline publication."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from time import time_ns
from typing import Protocol

from pydantic import Field

from carl.core.facebook_images import (
    COLLECT_IMAGE_WORK_KIND,
    EXTRACT_IMAGE_WORK_KIND,
    CollectImagePayload,
    image_network_activity,
)
from carl.core.facebook_work import (
    COLLECT_ITEM_WORK_KIND,
    EXTRACT_ITEM_WORK_KIND,
    CollectItemPayload,
    facebook_item_network_activity,
)
from carl.core.json import encode_json
from carl.core.models import JsonValue, NamedInput, NamedOutput, StrictModel
from carl.core.resources import ResourceRequest
from carl.core.stages import (
    StageAttemptContext,
    StageDefinition,
    StageOutcome,
    StageWaitKind,
    WaitWork,
    initial_work_stage,
)
from carl.core.work import WorkDefinition
from carl.core.worker import TerminalFailureWork, WorkOutcome
from carl.facebook_image_workers import (
    ImageWorkerDependencies,
    build_image_worker_registry,
    complete_image_collection,
    image_acquisition_failure_work,
    image_expired_outcome,
    image_session_failure_work,
    prepare_image_collection,
)
from carl.facebook_workers import (
    EXTRACT_FACEBOOK,
    FacebookWorkerDependencies,
    build_component_registry,
    build_facebook_worker_registry,
    item_acquisition_failure_outcome,
    item_acquisition_outcome,
    prepare_item_collection,
)
from carl.io.configuration import (
    ConfigurationFailure,
    decodo_settings,
    load_configuration,
    proton_settings,
)
from carl.io.decodo import DecodoSessionManager
from carl.io.facebook_images import (
    DecodoFacebookImageSessionFactory,
    FacebookImageSessionFailure,
    ProtonFacebookImageSessionFactory,
)
from carl.io.http_checkpoints import checkpoint_acquisition, restore_acquisition
from carl.io.httpx import (
    Acquisition,
    AcquisitionFailure,
    DirectHttpxAcquirer,
    HttpCaptureAcquirer,
    IdentifierFactory,
    RouteConfigurationFailure,
    completed_capture_after_close_failure,
)
from carl.io.image_files import ImageFileStore
from carl.io.network_activity import acquire_for_network_activity
from carl.io.paths import CarlDirectories
from carl.io.proton import ProtonSessionManager
from carl.io.sqlite import Database
from carl.io.stage_workers import (
    StageHandler,
    StageHandlerRegistry,
    TypedStageHandler,
    WorkStageAdapter,
)
from carl.io.worker import WorkHandler

_PREPARE = ("carl", "facebook", "stage", "image", "prepare")
_CAPTURE = ("carl", "facebook", "stage", "image", "capture")
_PUBLISH = ("carl", "facebook", "stage", "image", "publish")
_ITEM_PREPARE = ("carl", "facebook", "stage", "item", "prepare")
_ITEM_CAPTURE = ("carl", "facebook", "stage", "item", "capture")
_ITEM_PUBLISH = ("carl", "facebook", "stage", "item", "publish")


class ImageCaptureSession(Protocol):
    acquirer: HttpCaptureAcquirer

    def completed_observation(self) -> dict[str, JsonValue]: ...


@dataclass(frozen=True, slots=True)
class FacebookStatelessStageDependencies:
    database: Database
    directories: CarlDirectories
    new_identifier: IdentifierFactory
    proton_manager: ProtonSessionManager
    acquirer: HttpCaptureAcquirer | None = None
    session_factory: Callable[[str], AbstractAsyncContextManager[ImageCaptureSession]] | None = None
    utc_now_ns: Callable[[], int] = time_ns
    item_acquirer: HttpCaptureAcquirer | None = None
    item_session_factory: (
        Callable[[str], AbstractAsyncContextManager[ImageCaptureSession]] | None
    ) = None


class RetainedWorkPayload(StrictModel):
    work_kind: tuple[str, ...]
    payload_schema_version: int = Field(ge=1)
    payload: JsonValue


class CaptureImagePayload(StrictModel):
    image: CollectImagePayload
    session_identifier: str = Field(min_length=1)


class PublishImagePayload(StrictModel):
    image: CollectImagePayload
    checkpoint_identifier: str = Field(min_length=1)


class CaptureItemPayload(StrictModel):
    item: CollectItemPayload
    session_identifier: str = Field(min_length=1)


class PublishItemPayload(StrictModel):
    item: CollectItemPayload
    checkpoint_identifier: str = Field(min_length=1)
    acquisition_failed: bool = False


def _uncertain_dispatch() -> AcquisitionFailure:
    return AcquisitionFailure(
        "A previous request in this logical attempt has an uncertain outcome",
        result={
            "kind": "uncertain_previous_dispatch",
            "stopping_condition": "transport_failure",
            "failure_phase": "uncertain_previous_dispatch",
        },
    )


async def _charge(
    dependencies: FacebookStatelessStageDependencies,
    context: StageAttemptContext,
    request_kind: str,
) -> bool:
    charge = await dependencies.database.charge_stage_request(
        context,
        request_identity=("facebook", request_kind),
        maximum_attempts=1,
        now_utc_ns=dependencies.utc_now_ns(),
    )
    return charge is not None and charge.newly_charged


def _image_dependencies(
    dependencies: FacebookStatelessStageDependencies,
) -> ImageWorkerDependencies:
    return ImageWorkerDependencies(
        database=dependencies.database,
        acquirer=dependencies.acquirer or DirectHttpxAcquirer(),
        image_files=ImageFileStore(dependencies.database.path.parent),
        new_identifier=dependencies.new_identifier,
        utc_now_ns=dependencies.utc_now_ns,
    )


def _initial(kind: tuple[str, ...]) -> Callable[[WorkDefinition, str, str], StageDefinition]:
    def initial(work: WorkDefinition, identifier: str, operation: str) -> StageDefinition:
        return initial_work_stage(
            work,
            identifier=identifier,
            operation_identifier=operation,
            kind=kind,
            resources=(ResourceRequest(name="cpu"),),
        )

    return initial


def _factory(
    dependencies: FacebookStatelessStageDependencies, image: CollectImagePayload
) -> Callable[[str], AbstractAsyncContextManager[ImageCaptureSession]]:
    if dependencies.session_factory is not None:
        return dependencies.session_factory
    loaded = load_configuration(dependencies.directories.configuration_file)
    routing = image.request_plan.routing
    if loaded.configuration.resolve_network_path(routing) != routing:
        raise RouteConfigurationFailure("stage_route_changed_before_capture")
    if routing[0] == "decodo":
        settings, credentials = decodo_settings(loaded, dependencies.directories, routing)
        return DecodoFacebookImageSessionFactory(
            manager=DecodoSessionManager(), settings=settings, credential_source=credentials
        )
    return ProtonFacebookImageSessionFactory(
        manager=dependencies.proton_manager,
        settings=proton_settings(loaded, dependencies.directories, routing),
    )


def _item_factory(
    dependencies: FacebookStatelessStageDependencies, item: CollectItemPayload
) -> Callable[[str], AbstractAsyncContextManager[ImageCaptureSession]]:
    if dependencies.item_session_factory is not None:
        return dependencies.item_session_factory
    loaded = load_configuration(dependencies.directories.configuration_file)
    routing = item.request_plan.routing
    if loaded.configuration.resolve_network_path(routing) != routing:
        raise RouteConfigurationFailure("stage_route_changed_before_capture")
    settings, credentials = decodo_settings(loaded, dependencies.directories, routing)
    return DecodoFacebookImageSessionFactory(
        manager=DecodoSessionManager(), settings=settings, credential_source=credentials
    )


async def _retain_failure(
    outcome: WorkOutcome,
    error: AcquisitionFailure,
    dependencies: FacebookStatelessStageDependencies,
) -> StageOutcome:
    if not error.bodies:
        return outcome
    try:
        checkpoint = await checkpoint_acquisition(
            Acquisition(error.result, error.bodies),
            data_directory=dependencies.database.path.parent,
            new_identifier=dependencies.new_identifier,
        )
    except (OSError, ValueError) as storage_error:
        return _checkpoint_failure(storage_error, error.result)
    return outcome.model_copy(
        update={
            "records": (*outcome.records, *checkpoint.records),
            "artifacts": (*outcome.artifacts, *checkpoint.artifacts),
            "outputs": (*outcome.outputs, *checkpoint.outputs),
        }
    )


def _checkpoint_failure(
    error: Exception, acquisition: JsonValue, *, purpose: str = "image"
) -> TerminalFailureWork:
    summary: dict[str, JsonValue] = {}
    if isinstance(acquisition, dict):
        for key in (
            "network_activity_identifier",
            "network_activity_admitted_at_utc_ns",
            "stopping_condition",
            "failure_phase",
            "exception_type",
        ):
            value = acquisition.get(key)
            if isinstance(value, str):
                summary[key] = value[:256]
            elif isinstance(value, int) and not isinstance(value, bool):
                summary[key] = value
    return TerminalFailureWork(
        error={"kind": f"{purpose}_checkpoint_storage_failure", "type": type(error).__name__},
        result={"state": "checkpoint_failed", "acquisition": summary},
    )


def _cpu_adapter(handler: WorkHandler) -> tuple[WorkStageAdapter, StageHandler]:
    kind = (
        *handler.capability.kind,
        "stage",
        "cpu",
        str(handler.capability.payload_schema_version),
    )

    async def execute(payload: RetainedWorkPayload, context: StageAttemptContext) -> StageOutcome:
        if (
            payload.work_kind != handler.capability.kind
            or payload.payload_schema_version != handler.capability.payload_schema_version
        ):
            raise ValueError("Retained extraction payload does not match its stage adapter")
        return await handler.execute(payload.payload, context)

    return (
        WorkStageAdapter(handler.capability, handler.component, _initial(kind)),
        TypedStageHandler(
            kind=kind, schema_version=1, payload_type=RetainedWorkPayload, handler=execute
        ),
    )


def build_facebook_stateless_stage_registry(
    dependencies: FacebookStatelessStageDependencies,
) -> StageHandlerRegistry:
    image_dependencies = _image_dependencies(dependencies)
    image_registry = build_image_worker_registry(image_dependencies)
    item_dependencies = FacebookWorkerDependencies(
        database=dependencies.database,
        acquirer=dependencies.item_acquirer or DirectHttpxAcquirer(),
        new_identifier=dependencies.new_identifier,
    )

    async def prepare(payload: RetainedWorkPayload, context: StageAttemptContext) -> StageOutcome:
        if payload.work_kind != COLLECT_IMAGE_WORK_KIND:
            raise ValueError("Not a retained Facebook image collection")
        image = CollectImagePayload.model_validate_json(encode_json(payload.payload))
        prepared = await prepare_image_collection(image, context, image_dependencies)
        if prepared is not None:
            return prepared
        try:
            if dependencies.acquirer is None and dependencies.session_factory is None:
                loaded = load_configuration(dependencies.directories.configuration_file)
                routing = loaded.configuration.resolve_network_path(image.request_plan.routing)
                image = image.model_copy(
                    update={
                        "request_plan": image.request_plan.model_copy(update={"routing": routing})
                    }
                )
        except (ConfigurationFailure, RouteConfigurationFailure) as error:
            return TerminalFailureWork(
                error={"kind": "route_configuration_failure", "code": error.code},
                result={"state": "route_configuration_failed"},
            )
        session = dependencies.new_identifier()
        activity = image_network_activity(
            identifier=dependencies.new_identifier(),
            operation_identifier=context.operation_identifier,
            network_session_identifier=session,
            attempt=context.attempt,
            routing=image.request_plan.routing,
            url=image.request_plan.url,
        )
        continuation = StageDefinition(
            identifier=dependencies.new_identifier(),
            work_item_identifier=context.work_item_identifier,
            operation_identifier=context.operation_identifier,
            kind=_CAPTURE,
            payload=CaptureImagePayload(image=image, session_identifier=session).model_dump(
                mode="json"
            ),
            resources=(ResourceRequest(name="http"),),
            network_activity=activity,
        )
        return WaitWork(
            continuation=continuation,
            wait_kind=StageWaitKind.RESOURCE,
            result={"state": "waiting_for_image_capture"},
        )

    async def capture(payload: CaptureImagePayload, context: StageAttemptContext) -> StageOutcome:
        expired = image_expired_outcome(payload.image, image_dependencies)
        if expired is not None:
            return expired
        admission = context.network_admission
        if admission is None:
            raise ValueError("Image capture requires its admitted network activity")
        acquisition: Acquisition | None = None
        session: ImageCaptureSession | None = None
        try:
            if dependencies.acquirer is not None:
                if not await _charge(dependencies, context, "image"):
                    return image_acquisition_failure_work(_uncertain_dispatch(), context)
                acquisition = await acquire_for_network_activity(
                    dependencies.acquirer.capture(
                        payload.image.request_plan, dependencies.new_identifier
                    ),
                    admission,
                )
            else:
                async with _factory(dependencies, payload.image)(
                    payload.session_identifier
                ) as session:
                    expired = image_expired_outcome(payload.image, image_dependencies)
                    if expired is not None:
                        return expired
                    if not await _charge(dependencies, context, "image"):
                        return image_acquisition_failure_work(_uncertain_dispatch(), context)
                    acquisition = await acquire_for_network_activity(
                        session.acquirer.capture(
                            payload.image.request_plan, dependencies.new_identifier
                        ),
                        admission,
                    )
            if session is not None:
                acquisition.record["routing"] = {
                    "configured": list(payload.image.request_plan.routing),
                    "observed": session.completed_observation(),
                }
        except FacebookImageSessionFailure as error:
            if acquisition is None:
                return image_session_failure_work(
                    code=error.code,
                    diagnostic=error.diagnostic,
                    exit_code=error.exit_code,
                    context=context,
                )
            # A completed response survives teardown failure. Its immutable
            # checkpoint can be validated without repeating a successful request.
            acquisition.record["session_cleanup_failure"] = {
                "code": error.code,
                "diagnostic": error.diagnostic,
                "exit_code": error.exit_code,
            }
        except (RouteConfigurationFailure, TimeoutError, AcquisitionFailure) as error:
            if acquisition is None and isinstance(error, AcquisitionFailure):
                acquisition = completed_capture_after_close_failure(
                    error, plan=payload.image.request_plan
                )
            if acquisition is not None:
                diagnostic: dict[str, JsonValue] = {"type": type(error).__name__}
                if isinstance(error, AcquisitionFailure):
                    for key in ("failure_phase", "exception_type", "stopping_condition"):
                        value = error.result.get(key)
                        if isinstance(value, str):
                            diagnostic[key] = value[:256]
                elif isinstance(error, RouteConfigurationFailure):
                    diagnostic["code"] = error.code[:256]
                acquisition.record["session_cleanup_failure"] = diagnostic
            else:
                outcome = image_acquisition_failure_work(error, context)
                if isinstance(error, AcquisitionFailure):
                    return await _retain_failure(outcome, error, dependencies)
                return outcome
        except ConfigurationFailure as error:
            if acquisition is None:
                return TerminalFailureWork(
                    error={"kind": "route_configuration_failure", "code": error.code},
                    result={"state": "route_configuration_failed"},
                )
            acquisition.record["session_cleanup_failure"] = {"type": type(error).__name__[:128]}
        except Exception as error:
            if acquisition is None:
                raise
            acquisition.record["session_cleanup_failure"] = {"type": type(error).__name__[:128]}
        assert acquisition is not None
        try:
            checkpoint = await checkpoint_acquisition(
                acquisition,
                data_directory=dependencies.database.path.parent,
                new_identifier=dependencies.new_identifier,
            )
        except (OSError, ValueError) as error:
            return _checkpoint_failure(error, acquisition.record)
        continuation = StageDefinition(
            identifier=dependencies.new_identifier(),
            work_item_identifier=context.work_item_identifier,
            operation_identifier=context.operation_identifier,
            kind=_PUBLISH,
            payload=PublishImagePayload(
                image=payload.image, checkpoint_identifier=checkpoint.record_identifier
            ).model_dump(mode="json"),
            resources=(ResourceRequest(name="cpu"),),
            checkpoint_inputs=(
                NamedInput(
                    name=("http_checkpoint",), object_identifier=checkpoint.record_identifier
                ),
            ),
        )
        return WaitWork(
            continuation=continuation,
            wait_kind=StageWaitKind.RESOURCE,
            records=checkpoint.records,
            artifacts=checkpoint.artifacts,
            outputs=checkpoint.outputs,
            result={
                "state": "waiting_for_image_validation",
                "http_checkpoint_identifier": checkpoint.record_identifier,
            },
        )

    async def publish(payload: PublishImagePayload, context: StageAttemptContext) -> StageOutcome:
        try:
            acquisition = await restore_acquisition(
                dependencies.database, payload.checkpoint_identifier
            )
            outcome = await complete_image_collection(
                payload.image, context, image_dependencies, acquisition
            )
        except (KeyError, ValueError, OSError) as error:
            return TerminalFailureWork(
                error={"kind": "image_checkpoint_unavailable", "type": type(error).__name__},
                result={"state": "validation_failed"},
                inputs=(
                    NamedInput(
                        name=("http_checkpoint",), object_identifier=payload.checkpoint_identifier
                    ),
                ),
            )
        return outcome.model_copy(
            update={
                "inputs": (
                    *outcome.inputs,
                    NamedInput(
                        name=("http_checkpoint",), object_identifier=payload.checkpoint_identifier
                    ),
                )
            }
        )

    async def item_prepare(
        payload: RetainedWorkPayload, context: StageAttemptContext
    ) -> StageOutcome:
        if payload.work_kind != COLLECT_ITEM_WORK_KIND:
            raise ValueError("Not a retained Facebook detail collection")
        item = CollectItemPayload.model_validate_json(encode_json(payload.payload))
        reused = await prepare_item_collection(item, context, item_dependencies)
        if reused is not None:
            return reused
        try:
            if dependencies.item_acquirer is None and dependencies.item_session_factory is None:
                loaded = load_configuration(dependencies.directories.configuration_file)
                item = item.model_copy(
                    update={
                        "request_plan": item.request_plan.model_copy(
                            update={
                                "routing": loaded.configuration.resolve_network_path(
                                    item.request_plan.routing
                                )
                            }
                        )
                    }
                )
        except (ConfigurationFailure, RouteConfigurationFailure) as error:
            return TerminalFailureWork(
                error={
                    "kind": "route_configuration_failure",
                    "code": error.code,
                    "decision": "terminal",
                },
                result={"state": "route_configuration_failed"},
            )
        session = dependencies.new_identifier()
        return WaitWork(
            continuation=StageDefinition(
                identifier=dependencies.new_identifier(),
                work_item_identifier=context.work_item_identifier,
                operation_identifier=context.operation_identifier,
                kind=_ITEM_CAPTURE,
                payload=CaptureItemPayload(item=item, session_identifier=session).model_dump(
                    mode="json"
                ),
                resources=(ResourceRequest(name="http"),),
                network_activity=facebook_item_network_activity(
                    identifier=dependencies.new_identifier(),
                    operation_identifier=context.operation_identifier,
                    network_session_identifier=session,
                    attempt=context.attempt,
                    routing=item.request_plan.routing,
                ),
            ),
            wait_kind=StageWaitKind.RESOURCE,
            result={"state": "waiting_for_item_capture"},
        )

    async def item_capture(
        payload: CaptureItemPayload, context: StageAttemptContext
    ) -> StageOutcome:
        admission = context.network_admission
        if admission is None:
            raise ValueError("Detail capture requires its exact network admission")
        acquisition: Acquisition | None = None
        failed = False
        try:
            if dependencies.item_acquirer is not None:
                if not await _charge(dependencies, context, "item"):
                    return item_acquisition_failure_outcome(
                        error=_uncertain_dispatch(), context=context
                    )
                acquisition = await acquire_for_network_activity(
                    dependencies.item_acquirer.capture(
                        payload.item.request_plan, dependencies.new_identifier
                    ),
                    admission,
                )
            else:
                async with _item_factory(dependencies, payload.item)(
                    payload.session_identifier
                ) as session:
                    if not await _charge(dependencies, context, "item"):
                        return item_acquisition_failure_outcome(
                            error=_uncertain_dispatch(), context=context
                        )
                    acquisition = await acquire_for_network_activity(
                        session.acquirer.capture(
                            payload.item.request_plan, dependencies.new_identifier
                        ),
                        admission,
                    )
                acquisition.record["routing"] = {
                    "configured": list(payload.item.request_plan.routing),
                    "observed": session.completed_observation(),
                }
        except ConfigurationFailure as error:
            if acquisition is None:
                return TerminalFailureWork(
                    error={
                        "kind": "route_configuration_failure",
                        "code": error.code,
                        "decision": "terminal",
                    },
                    result={"state": "route_configuration_failed"},
                )
            acquisition.record["session_cleanup_failure"] = {"type": type(error).__name__[:128]}
        except RouteConfigurationFailure as error:
            if acquisition is None:
                return TerminalFailureWork(
                    error={
                        "kind": "route_configuration_failure",
                        "code": error.code,
                        "decision": "terminal",
                    },
                    result={"state": "route_configuration_failed"},
                )
            acquisition.record["session_cleanup_failure"] = {
                "type": type(error).__name__,
                "code": error.code,
            }
        except FacebookImageSessionFailure as error:
            if acquisition is None:
                return item_acquisition_failure_outcome(
                    error=AcquisitionFailure(
                        "Detail provider failed",
                        result={"stopping_condition": "transport_failure", "code": error.code},
                    ),
                    context=context,
                )
            acquisition.record["session_cleanup_failure"] = {
                "type": type(error).__name__,
                "code": error.code,
            }
        except TimeoutError:
            if acquisition is None:
                return item_acquisition_failure_outcome(
                    error=AcquisitionFailure(
                        "Detail transport timed out",
                        result={
                            "stopping_condition": "transport_failure",
                            "exception_type": "TimeoutError",
                        },
                    ),
                    context=context,
                )
            acquisition.record["session_cleanup_failure"] = {"type": "TimeoutError"}
        except AcquisitionFailure as error:
            if acquisition is None:
                acquisition = completed_capture_after_close_failure(
                    error, plan=payload.item.request_plan
                )
            if acquisition is not None:
                acquisition.record["session_cleanup_failure"] = {
                    "type": type(error).__name__,
                    "failure_phase": error.result.get("failure_phase"),
                }
            elif not error.bodies:
                return item_acquisition_failure_outcome(error=error, context=context)
            else:
                acquisition = Acquisition(error.result, error.bodies)
                failed = True
        except Exception as error:
            if acquisition is None:
                raise
            acquisition.record["session_cleanup_failure"] = {"type": type(error).__name__[:128]}
        assert acquisition is not None
        try:
            checkpoint = await checkpoint_acquisition(
                acquisition,
                data_directory=dependencies.database.path.parent,
                new_identifier=dependencies.new_identifier,
            )
        except (OSError, ValueError) as error:
            return _checkpoint_failure(error, acquisition.record, purpose="item")
        return WaitWork(
            continuation=StageDefinition(
                identifier=dependencies.new_identifier(),
                work_item_identifier=context.work_item_identifier,
                operation_identifier=context.operation_identifier,
                kind=_ITEM_PUBLISH,
                payload=PublishItemPayload(
                    item=payload.item,
                    checkpoint_identifier=checkpoint.record_identifier,
                    acquisition_failed=failed,
                ).model_dump(mode="json"),
                resources=(ResourceRequest(name="cpu"),),
                checkpoint_inputs=(
                    NamedInput(
                        name=("http_checkpoint",), object_identifier=checkpoint.record_identifier
                    ),
                ),
            ),
            wait_kind=StageWaitKind.RESOURCE,
            records=checkpoint.records,
            artifacts=checkpoint.artifacts,
            outputs=(
                *(
                    output
                    for output in checkpoint.outputs
                    if output.object_identifier
                    not in {body.identifier for body in acquisition.bodies}
                ),
                *(
                    NamedOutput(
                        name=("response", str(index), "body"), object_identifier=body.identifier
                    )
                    for index, body in enumerate(acquisition.bodies)
                ),
            ),
            result={
                "state": "waiting_for_item_publication",
                "http_checkpoint_identifier": checkpoint.record_identifier,
            },
        )

    async def item_publish(
        payload: PublishItemPayload, context: StageAttemptContext
    ) -> StageOutcome:
        try:
            acquisition = await restore_acquisition(
                dependencies.database, payload.checkpoint_identifier
            )
        except (OSError, KeyError, ValueError) as error:
            return TerminalFailureWork(
                inputs=(
                    NamedInput(
                        name=("http_checkpoint",), object_identifier=payload.checkpoint_identifier
                    ),
                ),
                error={"kind": "item_checkpoint_unavailable", "type": type(error).__name__},
                result={"state": "publication_failed"},
            )
        if payload.acquisition_failed:
            outcome = item_acquisition_failure_outcome(
                error=AcquisitionFailure(
                    "Detail capture failed", result=acquisition.record, bodies=acquisition.bodies
                ),
                context=context,
            )
        else:
            outcome = item_acquisition_outcome(
                acquisition=acquisition,
                payload=payload.item,
                context=context,
                component=build_component_registry().require(EXTRACT_FACEBOOK),
                new_identifier=dependencies.new_identifier,
            )
        body_identifiers = {body.identifier for body in acquisition.bodies}
        return outcome.model_copy(
            update={
                "artifacts": tuple(
                    artifact
                    for artifact in outcome.artifacts
                    if artifact.identifier not in body_identifiers
                ),
                "outputs": tuple(
                    output
                    for output in outcome.outputs
                    if output.object_identifier not in body_identifiers
                ),
                "inputs": (
                    *outcome.inputs,
                    NamedInput(
                        name=("http_checkpoint",), object_identifier=payload.checkpoint_identifier
                    ),
                ),
            }
        )

    adapters: list[WorkStageAdapter] = []
    handlers: list[StageHandler] = [
        TypedStageHandler(
            kind=_ITEM_PREPARE,
            schema_version=1,
            payload_type=RetainedWorkPayload,
            handler=item_prepare,
        ),
        TypedStageHandler(
            kind=_ITEM_CAPTURE,
            schema_version=1,
            payload_type=CaptureItemPayload,
            handler=item_capture,
        ),
        TypedStageHandler(
            kind=_ITEM_PUBLISH,
            schema_version=1,
            payload_type=PublishItemPayload,
            handler=item_publish,
        ),
        TypedStageHandler(
            kind=_PREPARE, schema_version=1, payload_type=RetainedWorkPayload, handler=prepare
        ),
        TypedStageHandler(
            kind=_CAPTURE, schema_version=1, payload_type=CaptureImagePayload, handler=capture
        ),
        TypedStageHandler(
            kind=_PUBLISH, schema_version=1, payload_type=PublishImagePayload, handler=publish
        ),
    ]
    for handler in image_registry.handlers:
        if handler.capability.kind == COLLECT_IMAGE_WORK_KIND:
            adapters.append(
                WorkStageAdapter(handler.capability, handler.component, _initial(_PREPARE))
            )
        elif handler.capability.kind == EXTRACT_IMAGE_WORK_KIND:
            adapter, stage_handler = _cpu_adapter(handler)
            adapters.append(adapter)
            handlers.append(stage_handler)
    facebook_registry = build_facebook_worker_registry(
        FacebookWorkerDependencies(
            database=dependencies.database,
            acquirer=dependencies.acquirer or DirectHttpxAcquirer(),
            new_identifier=dependencies.new_identifier,
        )
    )
    for handler in facebook_registry.handlers:
        if handler.capability.kind == COLLECT_ITEM_WORK_KIND:
            adapters.append(
                WorkStageAdapter(handler.capability, handler.component, _initial(_ITEM_PREPARE))
            )
        elif handler.capability.kind == EXTRACT_ITEM_WORK_KIND:
            adapter, stage_handler = _cpu_adapter(handler)
            adapters.append(adapter)
            handlers.append(stage_handler)
    return StageHandlerRegistry(adapters=tuple(adapters), handlers=tuple(handlers))
