"""Opt-in durable HTTP and CPU stages for stateless eBay collection."""

# Existing collection helpers retain the marketplace's publication and retry rules.
# pyright: reportPrivateUsage=false

from collections.abc import Awaitable, Callable
from typing import Literal

from carl.core.ebay_items import (
    COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    COLLECT_EBAY_IMAGE_WORK_KIND,
    EXTRACT_EBAY_ITEM_WORK_KIND,
    CollectEbayDescriptionPayload,
    CollectEbayImagePayload,
    ExtractEbayItemPayload,
    ebay_description_plan,
    ebay_image_plan,
)
from carl.core.http import RequestPlan
from carl.core.marketplace_images import MARKETPLACE_IMAGE_SCOPE
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
from carl.core.work import WorkCapability, WorkDefinition
from carl.core.worker import TerminalFailureWork, WorkOutcome
from carl.ebay_item_workers import (
    IMAGE_NETWORK_PATH,
    EbayItemWorkerDependencies,
    _failure,
    _item_acquirer,
    build_ebay_item_worker_registry,
    complete_description_collection,
    complete_image_collection,
    prepare_description_collection,
    prepare_image_collection,
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
    HttpAcquirer,
    HttpCaptureAcquirer,
    RouteConfigurationFailure,
    completed_capture_after_close_failure,
)
from carl.io.network_activity import acquire_for_network_activity, network_activity_definition
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler, WorkStageAdapter

_IMAGE_PREPARE = ("carl", "ebay", "stage", "image", "prepare")
_IMAGE_HTTP = ("carl", "ebay", "stage", "image", "http")
_IMAGE_CPU = ("carl", "ebay", "stage", "image", "publish")
_DESCRIPTION_PREPARE = ("carl", "ebay", "stage", "description", "prepare")
_DESCRIPTION_HTTP = ("carl", "ebay", "stage", "description", "http")
_DESCRIPTION_CPU = ("carl", "ebay", "stage", "description", "publish")
_EXTRACT = ("carl", "ebay", "stage", "item", "extract")


class _Envelope[PayloadT: StrictModel](StrictModel):
    work_kind: tuple[str, ...]
    payload_schema_version: Literal[1]
    payload: PayloadT


class _Prepared[PayloadT: StrictModel](StrictModel):
    payload: PayloadT
    network_path: tuple[str, ...]


class _Captured[PayloadT: StrictModel](StrictModel):
    payload: PayloadT
    checkpoint_identifier: str
    acquisition_failed: bool = False


def _capture_acquirer(acquirer: HttpAcquirer) -> HttpCaptureAcquirer:
    if not isinstance(acquirer, HttpCaptureAcquirer):
        raise RouteConfigurationFailure("http_acquirer_does_not_support_capture")
    return acquirer


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


def _continue(
    context: StageAttemptContext,
    dependencies: EbayItemWorkerDependencies,
    *,
    kind: tuple[str, ...],
    payload: StrictModel,
    resource: str,
    checkpoint: str | None = None,
    network_path: tuple[str, ...] | None = None,
    image: bool = False,
) -> StageDefinition:
    activity = None
    if network_path is not None:
        activity = network_activity_definition(
            identifier=dependencies.new_identifier(),
            kind=("carl", "ebay", "network_activity", "gallery_image" if image else "description"),
            operation_identifier=context.operation_identifier,
            network_session_identifier=dependencies.new_identifier(),
            network_path=network_path,
            attempt=context.attempt,
        )
        if image:
            activity = activity.model_copy(
                update={"scopes": (*activity.scopes, MARKETPLACE_IMAGE_SCOPE)}
            )
    return StageDefinition(
        identifier=dependencies.new_identifier(),
        work_item_identifier=context.work_item_identifier,
        operation_identifier=context.operation_identifier,
        kind=kind,
        payload=payload.model_dump(mode="json"),
        resources=(ResourceRequest(name=resource),),
        checkpoint_inputs=(NamedInput(name=("http_checkpoint",), object_identifier=checkpoint),)
        if checkpoint is not None
        else (),
        network_activity=activity,
    )


async def _capture_image(
    prepared: _Prepared[CollectEbayImagePayload],
    dependencies: EbayItemWorkerDependencies,
) -> Acquisition:
    plan = ebay_image_plan(prepared.payload, network_path=prepared.network_path)
    if dependencies.image_acquirer is not None:
        return await _capture_acquirer(dependencies.image_acquirer).capture(
            plan, dependencies.new_identifier
        )
    loaded = load_configuration(dependencies.directories.configuration_file)
    if prepared.network_path[0] == "decodo":
        settings, credentials = decodo_settings(
            loaded, dependencies.directories, prepared.network_path
        )
        factory = DecodoFacebookImageSessionFactory(
            manager=DecodoSessionManager(), settings=settings, credential_source=credentials
        )
    else:
        settings = proton_settings(loaded, dependencies.directories, prepared.network_path)
        factory = ProtonFacebookImageSessionFactory(
            manager=dependencies.proton_manager, settings=settings
        )
    acquisition = None
    try:
        async with factory(dependencies.new_identifier()) as session:
            acquisition = await _capture_acquirer(session.acquirer).capture(
                plan, dependencies.new_identifier
            )
        acquisition.record["routing"] = {
            "configured": list(prepared.network_path),
            "observed": session.completed_observation(),
        }
    except (AcquisitionFailure, FacebookImageSessionFailure, OSError, RuntimeError) as error:
        if acquisition is None:
            raise
        # The response is complete. Preserve teardown diagnostics and let the
        # CPU stage validate it without buying the same response again.
        cleanup: dict[str, JsonValue] = {"type": type(error).__name__}
        if isinstance(error, FacebookImageSessionFailure):
            cleanup.update(
                {"code": error.code, "diagnostic": error.diagnostic, "exit_code": error.exit_code}
            )
        elif isinstance(error, AcquisitionFailure):
            for key in ("failure_phase", "exception_type"):
                value = error.result.get(key)
                if isinstance(value, str):
                    cleanup[key] = value[:256]
        acquisition.record["session_cleanup_failure"] = cleanup
    return acquisition


def _with_checkpoint(
    outcome: WorkOutcome,
    checkpoint: str,
    acquisition: Acquisition | None = None,
) -> WorkOutcome:
    existing: set[str] = (
        {body.identifier for body in acquisition.bodies} if acquisition is not None else set()
    )
    return outcome.model_copy(
        update={
            "inputs": (
                *outcome.inputs,
                NamedInput(name=("http_checkpoint",), object_identifier=checkpoint),
            ),
            "artifacts": tuple(
                artifact for artifact in outcome.artifacts if artifact.identifier not in existing
            ),
            "outputs": tuple(
                output for output in outcome.outputs if output.object_identifier not in existing
            ),
        }
    )


def _source_inputs(payload: StrictModel) -> tuple[NamedInput, ...]:
    if isinstance(payload, CollectEbayImagePayload):
        return (
            NamedInput(
                name=("gallery_reference",), object_identifier=payload.reference_record_identifier
            ),
        )
    if isinstance(payload, CollectEbayDescriptionPayload):
        return (
            NamedInput(
                name=("observation",), object_identifier=payload.observation_record_identifier
            ),
        )
    raise ValueError("Unsupported eBay capture payload")


def build_ebay_stateless_stage_registry(
    dependencies: EbayItemWorkerDependencies,
) -> StageHandlerRegistry:
    """Adapt retained work schemas while keeping existing public result identities."""
    legacy = build_ebay_item_worker_registry(dependencies)
    sources = {handler.capability.kind: handler for handler in legacy.handlers}

    async def image_prepare(
        envelope: _Envelope[CollectEbayImagePayload],
        context: StageAttemptContext,
    ) -> StageOutcome:
        if envelope.work_kind != COLLECT_EBAY_IMAGE_WORK_KIND:
            return _failure(ValueError("Not image work"), context, image=True)
        prepared = await prepare_image_collection(envelope.payload, context, dependencies)
        if prepared is not None:
            return prepared
        try:
            path = IMAGE_NETWORK_PATH
            if dependencies.image_acquirer is not None:
                _ = _capture_acquirer(dependencies.image_acquirer)
            else:
                loaded = load_configuration(dependencies.directories.configuration_file)
                path = loaded.configuration.resolve_network_path(path)
            return WaitWork(
                continuation=_continue(
                    context,
                    dependencies,
                    kind=_IMAGE_HTTP,
                    payload=_Prepared(payload=envelope.payload, network_path=path),
                    resource="http",
                    network_path=path,
                    image=True,
                ),
                wait_kind=StageWaitKind.RESOURCE,
                result={"stage": "http"},
            )
        except (ValueError, OSError, ConfigurationFailure, RouteConfigurationFailure) as error:
            return _failure(error, context, inputs=_source_inputs(envelope.payload), image=True)

    async def description_prepare(
        envelope: _Envelope[CollectEbayDescriptionPayload],
        context: StageAttemptContext,
    ) -> StageOutcome:
        if envelope.work_kind != COLLECT_EBAY_DESCRIPTION_WORK_KIND:
            return _failure(ValueError("Not description work"), context)
        prepared = await prepare_description_collection(envelope.payload, context, dependencies)
        if prepared is not None:
            return prepared
        try:
            acquirer, path = _item_acquirer(envelope.payload.request, dependencies)
            _ = _capture_acquirer(acquirer)
            return WaitWork(
                continuation=_continue(
                    context,
                    dependencies,
                    kind=_DESCRIPTION_HTTP,
                    payload=_Prepared(payload=envelope.payload, network_path=path),
                    resource="http",
                    network_path=path,
                ),
                wait_kind=StageWaitKind.RESOURCE,
                result={"stage": "http"},
            )
        except (ValueError, OSError, ConfigurationFailure, RouteConfigurationFailure) as error:
            return _failure(error, context, inputs=_source_inputs(envelope.payload))

    async def capture[PayloadT: StrictModel](
        prepared: _Prepared[PayloadT],
        context: StageAttemptContext,
        *,
        image: bool,
        acquisition: Callable[[], Awaitable[Acquisition]],
        plan: RequestPlan,
    ) -> StageOutcome:
        failed = False
        try:
            if context.network_admission is None:
                raise ValueError("HTTP stage has no network admission")
            try:
                captured = await acquire_for_network_activity(
                    acquisition(), context.network_admission
                )
            except AcquisitionFailure as error:
                recovered = completed_capture_after_close_failure(error, plan=plan)
                if recovered is not None:
                    captured = recovered
                elif not error.bodies:
                    return _failure(
                        error, context, inputs=_source_inputs(prepared.payload), image=image
                    )
                else:
                    captured = Acquisition(error.result, error.bodies)
                    failed = True
            try:
                checkpoint = await checkpoint_acquisition(
                    captured,
                    data_directory=dependencies.database.path.parent,
                    new_identifier=dependencies.new_identifier,
                )
            except (OSError, ValueError) as error:
                # A paid response must not become another pending HTTP execution
                # merely because its checkpoint could not be stored.
                summary: dict[str, JsonValue] = {
                    "network_activity_identifier": context.network_admission.activity_identifier,
                    "complete_body_count": len(captured.bodies),
                    "complete_body_bytes": sum(len(body.content) for body in captured.bodies),
                }
                for key in ("stopping_condition", "failure_phase", "exception_type"):
                    value = captured.record.get(key)
                    if isinstance(value, str):
                        summary[key] = value[:256]
                return TerminalFailureWork(
                    inputs=_source_inputs(prepared.payload),
                    error={"kind": "checkpoint_storage_failure", "type": type(error).__name__},
                    result={"state": "failed", "acquisition": summary},
                )
            return WaitWork(
                records=checkpoint.records,
                artifacts=checkpoint.artifacts,
                outputs=(
                    NamedOutput(
                        name=("stage_acquisition",), object_identifier=checkpoint.record_identifier
                    ),
                    *(
                        NamedOutput(name=("body", str(index)), object_identifier=body.identifier)
                        for index, body in enumerate(checkpoint.artifacts)
                    ),
                ),
                continuation=_continue(
                    context,
                    dependencies,
                    kind=_IMAGE_CPU if image else _DESCRIPTION_CPU,
                    payload=_Captured(
                        payload=prepared.payload,
                        checkpoint_identifier=checkpoint.record_identifier,
                        acquisition_failed=failed,
                    ),
                    resource="cpu",
                    checkpoint=checkpoint.record_identifier,
                ),
                wait_kind=StageWaitKind.RESOURCE,
                result={"stage": "cpu"},
            )
        except (
            KeyError,
            ValueError,
            OSError,
            ConfigurationFailure,
            RouteConfigurationFailure,
            AcquisitionFailure,
            FacebookImageSessionFailure,
            TimeoutError,
        ) as error:
            return _failure(error, context, inputs=_source_inputs(prepared.payload), image=image)

    async def image_http(
        prepared: _Prepared[CollectEbayImagePayload],
        context: StageAttemptContext,
    ) -> StageOutcome:
        return await capture(
            prepared,
            context,
            image=True,
            acquisition=lambda: _capture_image(prepared, dependencies),
            plan=ebay_image_plan(prepared.payload, network_path=prepared.network_path),
        )

    async def description_http(
        prepared: _Prepared[CollectEbayDescriptionPayload],
        context: StageAttemptContext,
    ) -> StageOutcome:
        async def acquire() -> Acquisition:
            acquirer, path = _item_acquirer(prepared.payload.request, dependencies)
            if path != prepared.network_path:
                raise RouteConfigurationFailure("description_network_path_changed")
            return await _capture_acquirer(acquirer).capture(
                ebay_description_plan(prepared.payload, network_path=path),
                dependencies.new_identifier,
            )

        return await capture(
            prepared,
            context,
            image=False,
            acquisition=acquire,
            plan=ebay_description_plan(prepared.payload, network_path=prepared.network_path),
        )

    async def publish[PayloadT: StrictModel](
        captured: _Captured[PayloadT],
        context: StageAttemptContext,
        *,
        image: bool,
        complete: Callable[
            [PayloadT, StageAttemptContext, EbayItemWorkerDependencies, Acquisition],
            Awaitable[WorkOutcome],
        ],
    ) -> StageOutcome:
        try:
            acquisition = await restore_acquisition(
                dependencies.database, captured.checkpoint_identifier
            )
            if captured.acquisition_failed:
                outcome = _failure(
                    AcquisitionFailure(
                        "HTTP capture failed", result=acquisition.record, bodies=acquisition.bodies
                    ),
                    context,
                    inputs=_source_inputs(captured.payload),
                    image=image,
                )
            else:
                outcome = await complete(captured.payload, context, dependencies, acquisition)
            return _with_checkpoint(outcome, captured.checkpoint_identifier, acquisition)
        except (KeyError, ValueError, OSError) as error:
            return _with_checkpoint(
                _failure(error, context, inputs=_source_inputs(captured.payload), image=image),
                captured.checkpoint_identifier,
            )

    async def image_cpu(
        captured: _Captured[CollectEbayImagePayload],
        context: StageAttemptContext,
    ) -> StageOutcome:
        return await publish(captured, context, image=True, complete=complete_image_collection)

    async def description_cpu(
        captured: _Captured[CollectEbayDescriptionPayload],
        context: StageAttemptContext,
    ) -> StageOutcome:
        return await publish(
            captured, context, image=False, complete=complete_description_collection
        )

    async def extract(
        envelope: _Envelope[ExtractEbayItemPayload],
        context: StageAttemptContext,
    ) -> StageOutcome:
        if envelope.work_kind != EXTRACT_EBAY_ITEM_WORK_KIND:
            return _failure(ValueError("Not item extraction work"), context)
        return await sources[EXTRACT_EBAY_ITEM_WORK_KIND].execute(
            envelope.payload.model_dump(mode="json"), context
        )

    return StageHandlerRegistry(
        adapters=tuple(
            WorkStageAdapter(
                capability=WorkCapability(kind=kind, payload_schema_version=1),
                component=sources[kind].component,
                initial_stage=_initial(initial),
            )
            for kind, initial in (
                (COLLECT_EBAY_IMAGE_WORK_KIND, _IMAGE_PREPARE),
                (COLLECT_EBAY_DESCRIPTION_WORK_KIND, _DESCRIPTION_PREPARE),
                (EXTRACT_EBAY_ITEM_WORK_KIND, _EXTRACT),
            )
        ),
        handlers=(
            TypedStageHandler(_IMAGE_PREPARE, 1, _Envelope[CollectEbayImagePayload], image_prepare),
            TypedStageHandler(_IMAGE_HTTP, 1, _Prepared[CollectEbayImagePayload], image_http),
            TypedStageHandler(_IMAGE_CPU, 1, _Captured[CollectEbayImagePayload], image_cpu),
            TypedStageHandler(
                _DESCRIPTION_PREPARE,
                1,
                _Envelope[CollectEbayDescriptionPayload],
                description_prepare,
            ),
            TypedStageHandler(
                _DESCRIPTION_HTTP, 1, _Prepared[CollectEbayDescriptionPayload], description_http
            ),
            TypedStageHandler(
                _DESCRIPTION_CPU, 1, _Captured[CollectEbayDescriptionPayload], description_cpu
            ),
            TypedStageHandler(_EXTRACT, 1, _Envelope[ExtractEbayItemPayload], extract),
        ),
    )
