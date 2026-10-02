"""Durable image acquisitions and offline validation for Marketplace galleries."""

from dataclasses import dataclass

import anyio

from carl.core.components import Component, ComponentId, Registry
from carl.core.facebook_images import (
    COLLECT_IMAGE_WORK_KIND,
    COLLECT_IMAGE_WORK_SCHEMA_VERSION,
    EXTRACT_IMAGE_WORK_KIND,
    EXTRACT_IMAGE_WORK_SCHEMA_VERSION,
    LEGACY_COLLECT_IMAGE_WORK_SCHEMA_VERSION,
    CollectImagePayload,
    ExtractImagePayload,
    GalleryImageReference,
    VerifiedImage,
    gallery_references,
    image_network_activity,
    image_reuse_record,
    plan_image_followups,
    verify_image,
)
from carl.core.models import ExternalFileDraft, JsonValue, NamedInput, NamedOutput, RecordDraft
from carl.core.work import WorkCapability
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.io.httpx import (
    AcquisitionFailure,
    HttpAcquirer,
    IdentifierFactory,
    RouteConfigurationFailure,
)
from carl.io.image_files import ImageFileStore
from carl.io.network_activity import NetworkActivityScheduler
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry

COLLECT_FACEBOOK_IMAGE = ComponentId(("carl", "facebook", "collect", "gallery_image"))
EXTRACT_FACEBOOK_IMAGE = ComponentId(("carl", "facebook", "extract", "gallery_image"))
PLAN_FACEBOOK_IMAGE_FOLLOWUPS = ComponentId(("carl", "facebook", "plan", "image_followups"))
EXTRACT_FACEBOOK_GALLERY_REFERENCES = ComponentId(
    ("carl", "facebook", "extract", "gallery_references")
)
REUSE_FACEBOOK_GALLERY_IMAGE = ComponentId(("carl", "facebook", "reuse", "gallery_image"))
MAX_IMAGE_TRANSPORT_ATTEMPTS = 3
_RETRYABLE_IMAGE_SESSION_FAILURE_CODES = frozenset(
    {
        "proton_egress_probe_failed",
        "wireproxy_cleanup_failed",
        "wireproxy_device_identity_busy",
        "wireproxy_exited_during_startup",
        "wireproxy_port_allocation_busy",
        "wireproxy_start_failed",
        "wireproxy_startup_timeout",
    }
)


def _collect_component() -> None:
    """Identity anchor for collection workflow."""


def build_image_component_registry() -> Registry:
    return Registry(
        (
            Component(COLLECT_FACEBOOK_IMAGE, 3, _collect_component),
            Component(EXTRACT_FACEBOOK_IMAGE, 1, verify_image),
            Component(PLAN_FACEBOOK_IMAGE_FOLLOWUPS, 2, plan_image_followups),
            Component(EXTRACT_FACEBOOK_GALLERY_REFERENCES, 1, gallery_references),
            Component(REUSE_FACEBOOK_GALLERY_IMAGE, 1, image_reuse_record),
        )
    )


@dataclass(frozen=True, slots=True)
class ImageWorkerDependencies:
    database: Database
    acquirer: HttpAcquirer
    image_files: ImageFileStore
    new_identifier: IdentifierFactory
    network_activity_scheduler: NetworkActivityScheduler | None = None
    network_session_identifier: str | None = None


def image_session_failure_work(
    *,
    code: str,
    diagnostic: dict[str, JsonValue],
    exit_code: int | None,
    context: AttemptContext,
) -> RetryWork | TerminalFailureWork:
    """Classify a failure opening or closing a routed image session."""

    retryable = code in _RETRYABLE_IMAGE_SESSION_FAILURE_CODES
    failure: dict[str, JsonValue] = {
        "kind": "facebook_image_session_failure",
        "code": code,
        "diagnostic": diagnostic,
        "exit_code": exit_code,
    }
    result: dict[str, JsonValue] = {
        "state": "image_session_failed",
        "session_failure": failure,
    }
    if retryable and context.retry_attempt() < MAX_IMAGE_TRANSPORT_ATTEMPTS:
        return RetryWork(
            delay_ns=min(30, 2 ** (context.retry_attempt() - 1)) * 1_000_000_000,
            reason={**failure, "decision": "retry"},
            result=result,
        )
    return TerminalFailureWork(
        error={
            **failure,
            "decision": "retry_exhausted" if retryable else "terminal",
        },
        result=result,
    )


def _terminal_response(acquisition: dict[str, JsonValue]) -> dict[str, JsonValue]:
    hops = acquisition.get("hops")
    if not isinstance(hops, list) or not hops or not isinstance(hops[-1], dict):
        raise ValueError("Image acquisition has no terminal HTTP response")
    response = hops[-1].get("response")
    if not isinstance(response, dict):
        raise ValueError("Image response metadata is malformed")
    return response


def _image_acquisition_record(
    acquisition: dict[str, JsonValue],
    *,
    context: AttemptContext,
    payload: CollectImagePayload,
    retained_image_identifier: str | None,
    failure: str | None,
    discarded_representation: dict[str, JsonValue] | None = None,
) -> dict[str, JsonValue]:
    record = dict(acquisition)
    hops = record.get("hops")
    if not isinstance(hops, list) or not hops or not isinstance(hops[-1], dict):
        raise ValueError("Image acquisition has no terminal HTTP response")
    copied_hops = [dict(hop) if isinstance(hop, dict) else hop for hop in hops]
    terminal = copied_hops[-1]
    if not isinstance(terminal, dict):
        raise ValueError("Image acquisition terminal hop is malformed")
    response = terminal.get("response")
    if not isinstance(response, dict):
        raise ValueError("Image acquisition response is malformed")
    copied_response = dict(response)
    original_body = response.get("body")
    if not isinstance(original_body, dict):
        raise ValueError("Image acquisition response body metadata is malformed")
    if retained_image_identifier is None:
        copied_response["body"] = {
            "state": "unavailable",
            "reason": "image_validation_or_storage_failure",
            "received_content_bytes": original_body.get("received_content_bytes"),
            "discarded_decoded_bytes": original_body.get("bytes"),
            "detail": failure,
            "representation": discarded_representation,
        }
    else:
        retained_body = dict(original_body)
        retained_body["artifact_id"] = retained_image_identifier
        retained_body["retained_as"] = "validated_image_file"
        copied_response["body"] = retained_body
    terminal["response"] = copied_response
    record.update(
        {
            "hops": copied_hops,
            "operation_id": context.operation_identifier,
            "image_reference_record_identifier": payload.reference_record_identifier,
            "source_photo_id": payload.reference.photo_id,
            "original_url": payload.reference.original_url,
        }
    )
    return record


def _saved_image_result(
    *,
    identifier: str,
    acquisition_identifier: str,
    image_identifier: str,
    reference: GalleryImageReference,
    reference_identifier: str,
    operation_identifier: str,
    verified: VerifiedImage,
    locator: str,
    producer: ComponentId,
    producer_output_schema_version: int,
) -> RecordDraft:
    return RecordDraft(
        identifier=identifier,
        kind=("carl", "facebook", "image_result"),
        schema_version=1,
        value={
            "state": "saved",
            "acquisition_record_identifier": acquisition_identifier,
            "image_reference_record_identifier": reference_identifier,
            "listing_id": reference.listing_id,
            "source_photo_id": reference.photo_id,
            "original_url": reference.original_url,
            "image_artifact_identifier": image_identifier,
            "image_file_locator": locator,
            "sha256": verified.sha256,
            "mime_type": verified.media_type,
            "header_mime_type": verified.header_media_type,
            "format": verified.format,
            "width": verified.width,
            "height": verified.height,
            "declared_width": reference.declared_width,
            "declared_height": reference.declared_height,
            "operation_identifier": operation_identifier,
            "producer": {
                "component_parts": list(producer.parts),
                "output_schema_version": producer_output_schema_version,
            },
            "validator": {
                "component_parts": list(EXTRACT_FACEBOOK_IMAGE.parts),
                "output_schema_version": 1,
            },
        },
    )


async def _collect(
    payload: CollectImagePayload,
    context: AttemptContext,
    dependencies: ImageWorkerDependencies,
) -> WorkOutcome:
    try:
        if dependencies.network_activity_scheduler is None:
            acquisition = await dependencies.acquirer.acquire(
                payload.request_plan, dependencies.new_identifier
            )
        else:
            activity = image_network_activity(
                identifier=dependencies.new_identifier(),
                operation_identifier=context.operation_identifier,
                network_session_identifier=(
                    dependencies.network_session_identifier or dependencies.new_identifier()
                ),
                attempt=context.attempt,
                routing=payload.request_plan.routing,
                url=payload.request_plan.url,
            )
            async with dependencies.network_activity_scheduler.admit(activity) as permit:
                permit.mark_dispatched()
                acquisition = await dependencies.acquirer.acquire(
                    payload.request_plan, dependencies.new_identifier
                )
    except RouteConfigurationFailure as error:
        return TerminalFailureWork(
            error={"kind": "route_configuration_failure", "code": error.code},
            result={"state": "route_configuration_failed"},
        )
    except AcquisitionFailure as error:
        result: dict[str, JsonValue] = {
            "state": "acquisition_failed",
            "acquisition": error.result,
        }
        if (
            error.result.get("stopping_condition") == "transport_failure"
            and context.retry_attempt() < MAX_IMAGE_TRANSPORT_ATTEMPTS
        ):
            return RetryWork(
                delay_ns=1_000_000_000,
                reason={"kind": "image_transport_failure", "decision": "retry"},
                result=result,
            )
        return TerminalFailureWork(
            error={"kind": "image_acquisition_failure", "decision": "terminal"},
            result=result,
        )

    acquisition_identifier = dependencies.new_identifier()
    response = _terminal_response(acquisition.record)
    body_metadata = response.get("body")
    transient_identifier = (
        body_metadata.get("artifact_id") if isinstance(body_metadata, dict) else None
    )
    acquired_body = next(
        (body for body in acquisition.bodies if body.identifier == transient_identifier), None
    )
    inputs = (
        NamedInput(
            name=("gallery_image_reference",),
            object_identifier=payload.reference_record_identifier,
        ),
    )
    try:
        if response.get("status_code") != 200:
            raise ValueError("Image HTTP status is not 200")
        if acquisition.record.get("effective_url") != payload.reference.original_url:
            raise ValueError("Image effective URL differs from the signed reference")
        if acquired_body is None:
            raise ValueError("Image acquisition has no complete terminal body")
        verified = await anyio.to_thread.run_sync(
            verify_image,
            acquired_body.content,
            response.get("headers"),
            acquired_body.representation,
            abandon_on_cancel=True,
        )
        stored = await dependencies.image_files.publish(
            content=verified.content,
            media_type=verified.media_type,
            sha256=verified.sha256,
        )
    except (OSError, KeyError, ValueError) as error:
        result_identifier = dependencies.new_identifier()
        failed_acquisition = _image_acquisition_record(
            acquisition.record,
            context=context,
            payload=payload,
            retained_image_identifier=None,
            failure=str(error),
            discarded_representation=(
                acquired_body.representation if acquired_body is not None else None
            ),
        )
        failure_record = RecordDraft(
            identifier=result_identifier,
            kind=("carl", "facebook", "image_result"),
            schema_version=1,
            value={
                "state": "failed",
                "acquisition_record_identifier": acquisition_identifier,
                "image_reference_record_identifier": payload.reference_record_identifier,
                "error": {
                    "kind": "image_validation_or_storage_failure",
                    "type": type(error).__name__,
                    "message": str(error),
                },
                "operation_identifier": context.operation_identifier,
                "producer": {
                    "component_parts": list(COLLECT_FACEBOOK_IMAGE.parts),
                    "output_schema_version": 2,
                },
                "validator": {
                    "component_parts": list(EXTRACT_FACEBOOK_IMAGE.parts),
                    "output_schema_version": 1,
                },
            },
        )
        return TerminalFailureWork(
            inputs=inputs,
            records=(
                RecordDraft(
                    identifier=acquisition_identifier,
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value=failed_acquisition,
                ),
                failure_record,
            ),
            outputs=(
                NamedOutput(name=("acquisition",), object_identifier=acquisition_identifier),
                NamedOutput(name=("image_result",), object_identifier=result_identifier),
            ),
            error={"kind": "image_validation_or_storage_failure", "type": type(error).__name__},
            result={
                "state": "failed",
                "acquisition_record_identifier": acquisition_identifier,
                "image_result_record_identifier": result_identifier,
            },
        )

    image_identifier = dependencies.new_identifier()
    result_identifier = dependencies.new_identifier()
    record = _image_acquisition_record(
        acquisition.record,
        context=context,
        payload=payload,
        retained_image_identifier=image_identifier,
        failure=None,
    )
    result_record = _saved_image_result(
        identifier=result_identifier,
        acquisition_identifier=acquisition_identifier,
        image_identifier=image_identifier,
        reference=payload.reference,
        reference_identifier=payload.reference_record_identifier,
        operation_identifier=context.operation_identifier,
        verified=verified,
        locator=stored.locator,
        producer=COLLECT_FACEBOOK_IMAGE,
        producer_output_schema_version=2,
    )
    return CompletedWork(
        inputs=inputs,
        records=(
            RecordDraft(
                identifier=acquisition_identifier,
                kind=("carl", "http", "acquisition"),
                schema_version=1,
                value=record,
            ),
            result_record,
        ),
        artifacts=(
            ExternalFileDraft(
                identifier=image_identifier,
                kind=("carl", "facebook", "image_file"),
                media_type=verified.media_type,
                representation={
                    "kind": "validated_http_image_body",
                    "content_decoded": True,
                    "content_encodings_removed": list(verified.content_encodings_removed),
                    "exact_wire_bytes": False,
                },
                sha256=stored.sha256,
                size=stored.size,
                locator=stored.locator,
            ),
        ),
        outputs=(
            NamedOutput(name=("acquisition",), object_identifier=acquisition_identifier),
            NamedOutput(name=("image_file",), object_identifier=image_identifier),
            NamedOutput(name=("image_result",), object_identifier=result_identifier),
        ),
        result={
            "state": "saved",
            "acquisition_record_identifier": acquisition_identifier,
            "http_status": response.get("status_code"),
            "image_result_record_identifier": result_identifier,
            "image_artifact_identifier": image_identifier,
            "sha256": stored.sha256,
        },
    )


async def _extract(
    payload: ExtractImagePayload,
    context: AttemptContext,
    dependencies: ImageWorkerDependencies,
) -> WorkOutcome:
    inputs = (
        NamedInput(name=("acquisition",), object_identifier=payload.acquisition_record_identifier),
        NamedInput(
            name=("gallery_image_reference",), object_identifier=payload.reference_record_identifier
        ),
    )
    try:
        acquisition_kind, _, acquisition = await dependencies.database.get_record(
            payload.acquisition_record_identifier
        )
        reference_kind, _, reference = await dependencies.database.get_record(
            payload.reference_record_identifier
        )
        if acquisition_kind != ("carl", "http", "acquisition") or not isinstance(acquisition, dict):
            raise ValueError("Image acquisition record is invalid")
        if reference_kind != ("carl", "facebook", "gallery_image_reference") or not isinstance(
            reference, dict
        ):
            raise ValueError("Gallery reference record is invalid")
        reference_model = GalleryImageReference.model_validate(reference)
        response = _terminal_response(acquisition)
        body = response.get("body")
        if not isinstance(body, dict) or body.get("state") != "available":
            raise ValueError("Image response body is incomplete")
        body_identifier = body.get("artifact_id")
        if not isinstance(body_identifier, str):
            raise ValueError("Image response body reference is invalid")
        body_metadata, stored_body = await dependencies.database.get_artifact(body_identifier)
        inputs = (*inputs, NamedInput(name=("response", "body"), object_identifier=body_identifier))
        if response.get("status_code") != 200:
            raise ValueError("Image HTTP status is not 200")
        if acquisition.get("effective_url") != reference.get("original_url"):
            raise ValueError("Image effective URL differs from the signed reference")
        verified = await anyio.to_thread.run_sync(
            verify_image,
            stored_body,
            response.get("headers"),
            body_metadata.get("representation"),
            abandon_on_cancel=True,
        )
        stored = await dependencies.image_files.publish(
            content=verified.content,
            media_type=verified.media_type,
            sha256=verified.sha256,
        )
    except (OSError, KeyError, ValueError) as error:
        result_identifier = dependencies.new_identifier()
        record = RecordDraft(
            identifier=result_identifier,
            kind=("carl", "facebook", "image_result"),
            schema_version=1,
            value={
                "state": "failed",
                "acquisition_record_identifier": payload.acquisition_record_identifier,
                "image_reference_record_identifier": payload.reference_record_identifier,
                "error": {
                    "kind": "image_validation_failure",
                    "type": type(error).__name__,
                    "message": str(error),
                },
                "operation_identifier": context.operation_identifier,
                "producer": {
                    "component_parts": list(EXTRACT_FACEBOOK_IMAGE.parts),
                    "output_schema_version": 1,
                },
            },
        )
        return TerminalFailureWork(
            inputs=inputs,
            records=(record,),
            outputs=(NamedOutput(name=("image_result",), object_identifier=result_identifier),),
            error={"kind": "image_validation_failure", "type": type(error).__name__},
            result={"state": "failed", "image_result_record_identifier": result_identifier},
        )

    image_identifier = dependencies.new_identifier()
    result_identifier = dependencies.new_identifier()
    result_record = _saved_image_result(
        identifier=result_identifier,
        acquisition_identifier=payload.acquisition_record_identifier,
        image_identifier=image_identifier,
        reference=reference_model,
        reference_identifier=payload.reference_record_identifier,
        operation_identifier=context.operation_identifier,
        verified=verified,
        locator=stored.locator,
        producer=EXTRACT_FACEBOOK_IMAGE,
        producer_output_schema_version=1,
    )
    return CompletedWork(
        inputs=inputs,
        records=(result_record,),
        artifacts=(
            ExternalFileDraft(
                identifier=image_identifier,
                kind=("carl", "facebook", "image_file"),
                media_type=verified.media_type,
                representation={
                    "kind": "decoded_image_file",
                    "content_decoded": True,
                    "derived_from_artifact_id": body_identifier,
                    "content_encodings_removed": list(verified.content_encodings_removed),
                    "exact_wire_bytes": False,
                    "source_content_sha256": body_metadata["sha256"],
                },
                sha256=stored.sha256,
                size=stored.size,
                locator=stored.locator,
            ),
        ),
        outputs=(
            NamedOutput(name=("image_file",), object_identifier=image_identifier),
            NamedOutput(name=("image_result",), object_identifier=result_identifier),
        ),
        result={
            "state": "saved",
            "image_result_record_identifier": result_identifier,
            "image_artifact_identifier": image_identifier,
            "sha256": verified.sha256,
        },
    )


def build_image_worker_registry(dependencies: ImageWorkerDependencies) -> WorkHandlerRegistry:
    components = build_image_component_registry()
    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=COLLECT_IMAGE_WORK_KIND,
                    payload_schema_version=LEGACY_COLLECT_IMAGE_WORK_SCHEMA_VERSION,
                ),
                component=components.require(COLLECT_FACEBOOK_IMAGE),
                payload_type=CollectImagePayload,
                handler=lambda payload, context: _collect(payload, context, dependencies),
            ),
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=COLLECT_IMAGE_WORK_KIND,
                    payload_schema_version=COLLECT_IMAGE_WORK_SCHEMA_VERSION,
                ),
                component=components.require(COLLECT_FACEBOOK_IMAGE),
                payload_type=CollectImagePayload,
                handler=lambda payload, context: _collect(payload, context, dependencies),
            ),
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=EXTRACT_IMAGE_WORK_KIND,
                    payload_schema_version=EXTRACT_IMAGE_WORK_SCHEMA_VERSION,
                ),
                component=components.require(EXTRACT_FACEBOOK_IMAGE),
                payload_type=ExtractImagePayload,
                handler=lambda payload, context: _extract(payload, context, dependencies),
            ),
        )
    )
