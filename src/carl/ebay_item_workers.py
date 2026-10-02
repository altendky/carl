"""Durable eBay item evidence, offline extraction, and validated gallery images."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import anyio

from carl.core.components import Component, ComponentId
from carl.core.content_encoding import decoded_stored_body
from carl.core.ebay_items import (
    COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    COLLECT_EBAY_IMAGE_WORK_KIND,
    COLLECT_EBAY_ITEM_WORK_KIND,
    EXTRACT_EBAY_ITEM_WORK_KIND,
    CollectEbayDescriptionPayload,
    CollectEbayImagePayload,
    CollectEbayItemPayload,
    EbayItemRequest,
    ExtractEbayItemPayload,
    collect_ebay_description_work,
    collect_ebay_image_work,
    ebay_description_plan,
    ebay_image_plan,
    ebay_item_plan,
    extract_ebay_description,
    extract_ebay_item,
    extract_ebay_item_work,
)
from carl.core.facebook_images import verify_image
from carl.core.http import stored_content_encodings, stored_header_values, stored_response_charset
from carl.core.models import (
    BytesDraft,
    ExternalFileDraft,
    JsonValue,
    NamedInput,
    NamedOutput,
    RecordDraft,
)
from carl.core.work import WorkCapability, WorkDefinition, WorkRequester
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    FollowOnWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.ebay import _body_artifacts, _terminal_response
from carl.io.configuration import (
    ConfigurationFailure,
    decodo_settings,
    decodo_wreq_stack_settings,
    load_configuration,
    proton_settings,
)
from carl.io.decodo import DecodoSessionManager, ManagedDecodoWreqAcquirer
from carl.io.facebook_images import (
    DecodoFacebookImageSessionFactory,
    FacebookImageSessionFailure,
    ProtonFacebookImageSessionFactory,
)
from carl.io.httpx import Acquisition, AcquisitionFailure, HttpAcquirer, RouteConfigurationFailure
from carl.io.image_files import ImageFileStore
from carl.io.network_activity import (
    acquire_for_network_activity,
    network_activity_definition,
    network_activity_scheduler,
)
from carl.io.paths import CarlDirectories
from carl.io.proton import ProtonSessionManager
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry

COLLECT_ITEM = ComponentId(("carl", "ebay", "collect", "item"))
EXTRACT_ITEM = ComponentId(("carl", "ebay", "extract", "item"))
COLLECT_IMAGE = ComponentId(("carl", "ebay", "collect", "gallery_image"))
COLLECT_DESCRIPTION = ComponentId(("carl", "ebay", "collect", "description"))
IMAGE_NETWORK_PATH = ("proton", "personal", "carl")


@dataclass(frozen=True)
class EbayItemWorkerDependencies:
    database: Database
    directories: CarlDirectories
    new_identifier: Callable[[], str]
    proton_manager: ProtonSessionManager
    item_acquirer: HttpAcquirer | None = None
    image_acquirer: HttpAcquirer | None = None
    item_network_path: tuple[str, ...] = ("decodo", "personal", "carl")


def _follow(
    definition: WorkDefinition, context: AttemptContext, dependencies: EbayItemWorkerDependencies
) -> FollowOnWork:
    return FollowOnWork(
        definition=definition,
        requester=WorkRequester(
            request_identifier=dependencies.new_identifier(),
            kind=("carl", "ebay", "followup"),
            identifier=context.work_item_identifier,
            context={"source_operation_identifier": context.operation_identifier},
        ),
        event_identifier=dependencies.new_identifier(),
    )


def _failure(
    error: Exception,
    context: AttemptContext,
    *,
    inputs: tuple[NamedInput, ...] = (),
    image: bool = False,
) -> WorkOutcome:
    if isinstance(error, AcquisitionFailure):
        if image:
            error = AcquisitionFailure(
                str(error),
                result=_image_record(
                    Acquisition(record=error.result, bodies=()), artifact_identifier=None
                ),
            )
        artifacts = _body_artifacts(Acquisition(record=error.result, bodies=error.bodies))
        outputs = tuple(
            NamedOutput(name=("body", str(i)), object_identifier=a.identifier)
            for i, a in enumerate(artifacts)
        )
        result = {"state": "acquisition_failed", "acquisition": error.result}
        if (
            error.result.get("stopping_condition") == "transport_failure"
            and context.retry_attempt() < 3
        ):
            return RetryWork(
                inputs=inputs,
                artifacts=artifacts,
                outputs=outputs,
                result=result,
                delay_ns=2 ** (context.retry_attempt() - 1) * 1_000_000_000,
                reason={"kind": "ebay_transport_failure"},
            )
        return TerminalFailureWork(
            inputs=inputs,
            artifacts=artifacts,
            outputs=outputs,
            result=result,
            error={"kind": "ebay_acquisition_failure"},
        )
    code = (
        error.code
        if isinstance(
            error, (ConfigurationFailure, RouteConfigurationFailure, FacebookImageSessionFailure)
        )
        else None
    )
    if (
        isinstance(error, FacebookImageSessionFailure)
        and error.code
        in {
            "proton_egress_probe_failed",
            "wireproxy_cleanup_failed",
            "wireproxy_device_identity_busy",
            "wireproxy_exited_during_startup",
            "wireproxy_port_allocation_busy",
            "wireproxy_start_failed",
            "wireproxy_startup_timeout",
        }
        and context.retry_attempt() < 3
    ):
        return RetryWork(
            inputs=inputs,
            delay_ns=2 ** (context.retry_attempt() - 1) * 1_000_000_000,
            reason={"kind": "ebay_image_session_failure", "code": error.code},
            result={"state": "session_failed", "code": error.code, "diagnostic": error.diagnostic},
        )
    return TerminalFailureWork(
        inputs=inputs,
        error={
            "kind": "ebay_collection_or_extraction_failure",
            "type": type(error).__name__,
            "code": code,
        },
        result={"state": "failed"},
    )


def _item_acquirer(
    request: EbayItemRequest, dependencies: EbayItemWorkerDependencies
) -> tuple[HttpAcquirer, tuple[str, ...]]:
    if dependencies.item_acquirer is not None:
        return dependencies.item_acquirer, dependencies.item_network_path
    loaded = load_configuration(dependencies.directories.configuration_file)
    route, credentials, transport = decodo_wreq_stack_settings(
        loaded, dependencies.directories, request.stack_identifier
    )
    return ManagedDecodoWreqAcquirer(
        settings=route, credential_source=credentials, transport_settings=transport
    ), route.route.network_path


async def _collect_item(
    payload: CollectEbayItemPayload,
    context: AttemptContext,
    dependencies: EbayItemWorkerDependencies,
) -> WorkOutcome:
    try:
        acquirer, network_path = _item_acquirer(payload.request, dependencies)
        activity = network_activity_definition(
            identifier=dependencies.new_identifier(),
            kind=("carl", "ebay", "network_activity", "item_page"),
            operation_identifier=context.operation_identifier,
            network_session_identifier=dependencies.new_identifier(),
            network_path=network_path,
            attempt=context.attempt,
        )
        async with network_activity_scheduler(
            dependencies.database, dependencies.new_identifier
        ).admit(activity) as permit:
            permit.mark_dispatched()
            acquisition = await acquire_for_network_activity(
                acquirer.acquire(
                    ebay_item_plan(payload.request, network_path=network_path),
                    dependencies.new_identifier,
                ),
                permit.admission,
            )
    except (ConfigurationFailure, RouteConfigurationFailure, AcquisitionFailure) as error:
        return _failure(error, context)
    identifier = dependencies.new_identifier()
    artifacts = _body_artifacts(acquisition)
    extraction = ExtractEbayItemPayload(
        request=payload.request, acquisition_record_identifier=identifier
    )
    follow = _follow(
        extract_ebay_item_work(identifier=dependencies.new_identifier(), payload=extraction),
        context,
        dependencies,
    )
    return CompletedWork(
        records=(
            RecordDraft(
                identifier=identifier,
                kind=("carl", "http", "acquisition"),
                schema_version=1,
                value={
                    **acquisition.record,
                    "purpose": "ebay_item",
                    "ebay_item_request": payload.request.model_dump(mode="json"),
                },
            ),
        ),
        artifacts=artifacts,
        outputs=(
            NamedOutput(name=("acquisition",), object_identifier=identifier),
            *(
                NamedOutput(name=("body", str(i)), object_identifier=a.identifier)
                for i, a in enumerate(artifacts)
            ),
        ),
        follow_on_work=(follow,),
        result={
            "state": "acquired",
            "acquisition_record_identifier": identifier,
            "extraction_work_identifier": follow.definition.identifier,
        },
    )


async def _html(
    database: Database, acquisition: dict[str, JsonValue], new_identifier: Callable[[], str]
) -> tuple[str, BytesDraft, str]:
    response = _terminal_response(acquisition)
    body = response.get("body")
    if (
        not isinstance(body, dict)
        or body.get("state") != "available"
        or not isinstance(body.get("artifact_id"), str)
    ):
        raise ValueError("No complete retained HTML body")
    identifier = body["artifact_id"]
    metadata, content = await database.get_artifact(identifier)
    headers = response.get("headers")
    decoded = decoded_stored_body(
        content, metadata.get("representation"), stored_content_encodings(headers)
    )
    charset, source = stored_response_charset(headers)
    try:
        html = decoded.decode(charset)
    except (LookupError, UnicodeError):
        html = decoded.decode("utf-8", errors="replace")
        charset, source = "utf-8", "replacement_fallback"
    artifact = BytesDraft(
        identifier=new_identifier(),
        kind=("carl", "http", "decoded_text"),
        media_type="text/html; charset=utf-8",
        representation={
            "kind": "content_decoded_utf8_text",
            "derived_from_artifact_identifier": identifier,
            "source_charset": charset,
            "charset_source": source,
            "exact_wire_bytes": False,
        },
        content=html.encode("utf-8"),
    )
    return html, artifact, identifier


async def _extract_item(
    payload: ExtractEbayItemPayload,
    context: AttemptContext,
    dependencies: EbayItemWorkerDependencies,
) -> WorkOutcome:
    inputs = (
        NamedInput(name=("acquisition",), object_identifier=payload.acquisition_record_identifier),
    )
    try:
        kind, schema, acquisition = await dependencies.database.get_record(
            payload.acquisition_record_identifier
        )
        if (
            kind != ("carl", "http", "acquisition")
            or schema != 1
            or not isinstance(acquisition, dict)
            or acquisition.get("purpose") != "ebay_item"
        ):
            raise ValueError("Not an eBay item acquisition")
        retained_request = EbayItemRequest.model_validate(acquisition.get("ebay_item_request"))
        if (
            retained_request.item_identifier != payload.request.item_identifier
            or retained_request.stack_identifier != payload.request.stack_identifier
        ):
            raise ValueError("Item acquisition and extraction identity disagree")
        html, decoded, body_identifier = await _html(
            dependencies.database, acquisition, dependencies.new_identifier
        )
        response = _terminal_response(acquisition)
        extraction = extract_ebay_item(
            html,
            item_identifier=payload.request.item_identifier,
            effective_url=acquisition.get("effective_url", ""),
            status_code=response.get("status_code", 0),
        )
    except (KeyError, ValueError, OSError) as error:
        return _failure(error, context, inputs=inputs)
    identifier = dependencies.new_identifier()
    value = {
        **extraction.model_dump(mode="json"),
        "acquisition_record_identifier": payload.acquisition_record_identifier,
        "decoded_body_artifact_identifier": decoded.identifier,
        "request": payload.request.model_dump(mode="json"),
        "operation_identifier": context.operation_identifier,
    }
    records = [
        RecordDraft(
            identifier=identifier,
            kind=("carl", "ebay", "listing_observation"),
            schema_version=1,
            value=value,
        )
    ]
    outputs = [
        NamedOutput(name=("observation",), object_identifier=identifier),
        NamedOutput(name=("decoded_body",), object_identifier=decoded.identifier),
    ]
    follows: list[FollowOnWork] = []
    if extraction.classification == "detail":
        for index, url in enumerate(extraction.gallery_urls[: payload.request.maximum_images]):
            reference_identifier = dependencies.new_identifier()
            image_payload = CollectEbayImagePayload(
                item_identifier=payload.request.item_identifier,
                observation_record_identifier=identifier,
                reference_record_identifier=reference_identifier,
                url=url,
            )
            follow = _follow(
                collect_ebay_image_work(
                    identifier=dependencies.new_identifier(), payload=image_payload
                ),
                context,
                dependencies,
            )
            reference = {
                "item_identifier": payload.request.item_identifier,
                "observation_record_identifier": identifier,
                "acquisition_record_identifier": payload.acquisition_record_identifier,
                "url": url,
                "gallery_order": index,
                "work_identifier": follow.definition.identifier,
            }
            records.append(
                RecordDraft(
                    identifier=reference_identifier,
                    kind=("carl", "ebay", "gallery_image_reference"),
                    schema_version=1,
                    value=reference,
                )
            )
            outputs.append(
                NamedOutput(
                    name=("gallery_reference", str(index)), object_identifier=reference_identifier
                )
            )
            follows.append(follow)
        if extraction.description_url:
            description_payload = CollectEbayDescriptionPayload(
                request=payload.request,
                observation_record_identifier=identifier,
                url=extraction.description_url,
            )
            follows.append(
                _follow(
                    collect_ebay_description_work(
                        identifier=dependencies.new_identifier(), payload=description_payload
                    ),
                    context,
                    dependencies,
                )
            )
    records[0] = RecordDraft(
        identifier=identifier,
        kind=("carl", "ebay", "listing_observation"),
        schema_version=1,
        value={
            **value,
            "description_work_identifiers": [
                f.definition.identifier
                for f in follows
                if f.definition.kind == COLLECT_EBAY_DESCRIPTION_WORK_KIND
            ],
        },
    )
    return CompletedWork(
        inputs=(*inputs, NamedInput(name=("terminal_body",), object_identifier=body_identifier)),
        records=tuple(records),
        artifacts=(decoded,),
        outputs=tuple(outputs),
        follow_on_work=tuple(follows),
        result={
            "state": "extracted",
            "classification": extraction.classification,
            "observation_record_identifier": identifier,
            "image_work_identifiers": [
                f.definition.identifier
                for f in follows
                if f.definition.kind == COLLECT_EBAY_IMAGE_WORK_KIND
            ],
            "description_work_identifiers": [
                f.definition.identifier
                for f in follows
                if f.definition.kind == COLLECT_EBAY_DESCRIPTION_WORK_KIND
            ],
        },
    )


async def _acquire_image(
    payload: CollectEbayImagePayload,
    dependencies: EbayItemWorkerDependencies,
    network_path: tuple[str, ...] = IMAGE_NETWORK_PATH,
) -> Acquisition:
    plan = ebay_image_plan(payload, network_path=network_path)
    if dependencies.image_acquirer is not None:
        return await dependencies.image_acquirer.acquire(plan, dependencies.new_identifier)
    loaded = load_configuration(dependencies.directories.configuration_file)
    if network_path[0] == "decodo":
        settings, credential_source = decodo_settings(
            loaded, dependencies.directories, network_path
        )
        factory = DecodoFacebookImageSessionFactory(
            manager=DecodoSessionManager(),
            settings=settings,
            credential_source=credential_source,
        )
    else:
        proton_route_settings = proton_settings(loaded, dependencies.directories, network_path)
        factory = ProtonFacebookImageSessionFactory(
            manager=dependencies.proton_manager, settings=proton_route_settings
        )
    acquisition: Acquisition | None = None
    try:
        async with factory(dependencies.new_identifier()) as session:
            acquisition = await session.acquirer.acquire(plan, dependencies.new_identifier)
    except (FacebookImageSessionFailure, OSError, RuntimeError) as error:
        if acquisition is None:
            raise
        raise AcquisitionFailure(
            "Image session cleanup failed",
            result={
                **acquisition.record,
                "stopping_condition": "transport_failure",
                "failure_phase": "session_close",
                "exception_type": type(error).__name__,
            },
            bodies=acquisition.bodies,
        ) from error
    acquisition.record["routing"] = {
        "configured": list(network_path),
        "observed": session.completed_observation(),
    }
    return acquisition


def _image_record(
    acquisition: Acquisition, *, artifact_identifier: str | None
) -> dict[str, JsonValue]:
    record = dict(acquisition.record)
    hops = [dict(hop) for hop in record.get("hops", []) if isinstance(hop, dict)]
    for index, hop in enumerate(hops):
        response = dict(hop["response"]) if isinstance(hop.get("response"), dict) else {}
        body = dict(response["body"]) if isinstance(response.get("body"), dict) else {}
        if index == len(hops) - 1 and artifact_identifier is not None:
            body.update({"artifact_id": artifact_identifier, "retained_as": "validated_image_file"})
        else:
            body = {
                "state": "unavailable",
                "reason": "discarded_unvalidated_image_body",
                "bytes": body.get("bytes"),
            }
        response["body"] = body
        hop["response"] = response
    record["hops"] = hops
    return record


async def _collect_image(
    payload: CollectEbayImagePayload,
    context: AttemptContext,
    dependencies: EbayItemWorkerDependencies,
) -> WorkOutcome:
    inputs = (
        NamedInput(
            name=("gallery_reference",), object_identifier=payload.reference_record_identifier
        ),
    )
    try:
        kind, schema, reference = await dependencies.database.get_record(
            payload.reference_record_identifier
        )
        if (
            kind != ("carl", "ebay", "gallery_image_reference")
            or schema != 1
            or not isinstance(reference, dict)
            or any(
                reference.get(key) != getattr(payload, key)
                for key in ("item_identifier", "observation_record_identifier", "url")
            )
        ):
            raise ValueError("Image payload disagrees with its retained reference")
        # Exact-URL reuse shares validated bytes, never the listing relationship.
        for source_identifier, result in reversed(
            await dependencies.database.records_by_kind(("carl", "ebay", "image_result"))
        ):
            if (
                isinstance(result, dict)
                and result.get("state") == "saved"
                and result.get("url") == payload.url
            ):
                artifact_identifier = result.get("image_artifact_identifier")
                if not isinstance(artifact_identifier, str):
                    continue
                await dependencies.database.get_artifact(artifact_identifier)
                identifier = dependencies.new_identifier()
                return CompletedWork(
                    inputs=(
                        *inputs,
                        NamedInput(
                            name=("reused_image_result",), object_identifier=source_identifier
                        ),
                    ),
                    records=(
                        RecordDraft(
                            identifier=identifier,
                            kind=("carl", "ebay", "image_result"),
                            schema_version=1,
                            value={
                                **result,
                                "item_identifier": payload.item_identifier,
                                "observation_record_identifier": payload.observation_record_identifier,
                                "reference_record_identifier": payload.reference_record_identifier,
                                "reused_from_result_record_identifier": source_identifier,
                                "operation_identifier": context.operation_identifier,
                            },
                        ),
                    ),
                    outputs=(NamedOutput(name=("image_result",), object_identifier=identifier),),
                    result={
                        "state": "saved",
                        "image_result_record_identifier": identifier,
                        "reused": True,
                    },
                )
        image_network_path = IMAGE_NETWORK_PATH
        if dependencies.image_acquirer is None:
            loaded = load_configuration(dependencies.directories.configuration_file)
            image_network_path = loaded.configuration.resolve_network_path(IMAGE_NETWORK_PATH)
        activity = network_activity_definition(
            identifier=dependencies.new_identifier(),
            kind=("carl", "ebay", "network_activity", "gallery_image"),
            operation_identifier=context.operation_identifier,
            network_session_identifier=dependencies.new_identifier(),
            network_path=image_network_path,
            attempt=context.attempt,
        )
        async with network_activity_scheduler(
            dependencies.database, dependencies.new_identifier
        ).admit(activity) as permit:
            permit.mark_dispatched()
            acquisition = await acquire_for_network_activity(
                _acquire_image(payload, dependencies, image_network_path), permit.admission
            )
    except (
        KeyError,
        ValueError,
        OSError,
        ConfigurationFailure,
        RouteConfigurationFailure,
        AcquisitionFailure,
        FacebookImageSessionFailure,
    ) as error:
        return _failure(error, context, inputs=inputs, image=True)
    acquisition_identifier = dependencies.new_identifier()
    result_identifier = dependencies.new_identifier()
    identity = {
        "item_identifier": payload.item_identifier,
        "observation_record_identifier": payload.observation_record_identifier,
        "reference_record_identifier": payload.reference_record_identifier,
        "url": payload.url,
        "operation_identifier": context.operation_identifier,
        "acquisition_record_identifier": acquisition_identifier,
    }
    try:
        response = _terminal_response(acquisition.record)
        if (
            response.get("status_code") != 200
            or acquisition.record.get("effective_url") != payload.url
        ):
            raise ValueError("Image did not return the exact requested HTTP 200 resource")
        body = response.get("body")
        body_identifier = (
            body.get("artifact_id")
            if isinstance(body, dict) and body.get("state") == "available"
            else None
        )
        acquired_body = next(
            (b for b in acquisition.bodies if b.identifier == body_identifier), None
        )
        if acquired_body is None:
            raise ValueError("No complete image body")
        verified = await anyio.to_thread.run_sync(
            verify_image,
            acquired_body.content,
            response.get("headers"),
            acquired_body.representation,
        )
        stored = await ImageFileStore(dependencies.database.path.parent).publish(
            content=verified.content, media_type=verified.media_type, sha256=verified.sha256
        )
    except (ValueError, OSError, KeyError) as error:
        records = (
            RecordDraft(
                identifier=acquisition_identifier,
                kind=("carl", "http", "acquisition"),
                schema_version=1,
                value=_image_record(acquisition, artifact_identifier=None),
            ),
            RecordDraft(
                identifier=result_identifier,
                kind=("carl", "ebay", "image_result"),
                schema_version=1,
                value={
                    **identity,
                    "state": "failed",
                    "error": {
                        "kind": "image_validation_or_storage_failure",
                        "type": type(error).__name__,
                    },
                },
            ),
        )
        return TerminalFailureWork(
            inputs=inputs,
            records=records,
            outputs=(
                NamedOutput(name=("acquisition",), object_identifier=acquisition_identifier),
                NamedOutput(name=("image_result",), object_identifier=result_identifier),
            ),
            error={"kind": "image_validation_or_storage_failure"},
            result={"state": "failed", "image_result_record_identifier": result_identifier},
        )
    artifact_identifier = dependencies.new_identifier()
    records = (
        RecordDraft(
            identifier=acquisition_identifier,
            kind=("carl", "http", "acquisition"),
            schema_version=1,
            value=_image_record(acquisition, artifact_identifier=artifact_identifier),
        ),
        RecordDraft(
            identifier=result_identifier,
            kind=("carl", "ebay", "image_result"),
            schema_version=1,
            value={
                **identity,
                "state": "saved",
                "image_artifact_identifier": artifact_identifier,
                "image_file_locator": stored.locator,
                "sha256": verified.sha256,
                "mime_type": verified.media_type,
                "width": verified.width,
                "height": verified.height,
            },
        ),
    )
    artifact = ExternalFileDraft(
        identifier=artifact_identifier,
        kind=("carl", "ebay", "image_file"),
        media_type=verified.media_type,
        representation={
            "kind": "validated_http_image_body",
            "content_decoded": True,
            "exact_wire_bytes": False,
            "content_encodings_removed": list(verified.content_encodings_removed),
        },
        sha256=stored.sha256,
        size=stored.size,
        locator=stored.locator,
    )
    return CompletedWork(
        inputs=inputs,
        records=records,
        artifacts=(artifact,),
        outputs=(
            NamedOutput(name=("acquisition",), object_identifier=acquisition_identifier),
            NamedOutput(name=("image_result",), object_identifier=result_identifier),
            NamedOutput(name=("image",), object_identifier=artifact_identifier),
        ),
        result={
            "state": "saved",
            "image_result_record_identifier": result_identifier,
            "image_artifact_identifier": artifact_identifier,
            "reused": False,
        },
    )


async def _collect_description(
    payload: CollectEbayDescriptionPayload,
    context: AttemptContext,
    dependencies: EbayItemWorkerDependencies,
) -> WorkOutcome:
    inputs = (
        NamedInput(name=("observation",), object_identifier=payload.observation_record_identifier),
    )
    try:
        kind, schema, observation = await dependencies.database.get_record(
            payload.observation_record_identifier
        )
        if (
            kind != ("carl", "ebay", "listing_observation")
            or schema != 1
            or not isinstance(observation, dict)
            or observation.get("item_identifier") != payload.request.item_identifier
            or observation.get("description_url") != payload.url
        ):
            raise ValueError("Description payload disagrees with its observation")
        acquirer, network_path = _item_acquirer(payload.request, dependencies)
        activity = network_activity_definition(
            identifier=dependencies.new_identifier(),
            kind=("carl", "ebay", "network_activity", "description"),
            operation_identifier=context.operation_identifier,
            network_session_identifier=dependencies.new_identifier(),
            network_path=network_path,
            attempt=context.attempt,
        )
        async with network_activity_scheduler(
            dependencies.database, dependencies.new_identifier
        ).admit(activity) as permit:
            permit.mark_dispatched()
            acquisition = await acquire_for_network_activity(
                acquirer.acquire(
                    ebay_description_plan(payload, network_path=network_path),
                    dependencies.new_identifier,
                ),
                permit.admission,
            )
    except (
        KeyError,
        ValueError,
        ConfigurationFailure,
        RouteConfigurationFailure,
        AcquisitionFailure,
    ) as error:
        return _failure(error, context, inputs=inputs)
    acquisition_identifier = dependencies.new_identifier()
    artifacts = _body_artifacts(acquisition)
    text: str | None = None
    try:
        response = _terminal_response(acquisition.record)
        body_meta = response.get("body")
        body_identifier = (
            body_meta.get("artifact_id")
            if isinstance(body_meta, dict) and body_meta.get("state") == "available"
            else None
        )
        body = next((b for b in acquisition.bodies if b.identifier == body_identifier), None)
        if (
            response.get("status_code") != 200
            or acquisition.record.get("effective_url") != payload.url
            or body is None
        ):
            raise ValueError("Description response is not the complete requested HTTP 200 resource")
        content_types = stored_header_values(response.get("headers"), "content-type")
        if not content_types or content_types[-1].split(";", 1)[0].strip().lower() not in {
            "text/html",
            "application/xhtml+xml",
        }:
            raise ValueError("Description response is not HTML")
        decoded = decoded_stored_body(
            body.content, body.representation, stored_content_encodings(response.get("headers"))
        )
        charset, _ = stored_response_charset(response.get("headers"))
        text = extract_ebay_description(decoded.decode(charset)) or None
    except (ValueError, LookupError, UnicodeError):
        pass
    identifier = dependencies.new_identifier()
    records = (
        RecordDraft(
            identifier=acquisition_identifier,
            kind=("carl", "http", "acquisition"),
            schema_version=1,
            value={**acquisition.record, "purpose": "ebay_description"},
        ),
        RecordDraft(
            identifier=identifier,
            kind=("carl", "ebay", "description_result"),
            schema_version=1,
            value={
                "item_identifier": payload.request.item_identifier,
                "observation_record_identifier": payload.observation_record_identifier,
                "acquisition_record_identifier": acquisition_identifier,
                "state": "saved" if text is not None else "failed",
                "description": text,
                "url": payload.url,
            },
        ),
    )
    outputs = (
        NamedOutput(name=("acquisition",), object_identifier=acquisition_identifier),
        NamedOutput(name=("description",), object_identifier=identifier),
        *(
            NamedOutput(name=("body", str(i)), object_identifier=a.identifier)
            for i, a in enumerate(artifacts)
        ),
    )
    result = {
        "state": "saved" if text is not None else "failed",
        "description_result_record_identifier": identifier,
    }
    if text is None:
        return TerminalFailureWork(
            inputs=inputs,
            records=records,
            artifacts=artifacts,
            outputs=outputs,
            result=result,
            error={"kind": "description_validation_failed"},
        )
    return CompletedWork(
        inputs=inputs, records=records, artifacts=artifacts, outputs=outputs, result=result
    )


def build_ebay_item_worker_registry(
    dependencies: EbayItemWorkerDependencies,
) -> WorkHandlerRegistry:
    async def collect(payload: CollectEbayItemPayload, context: AttemptContext) -> WorkOutcome:
        return await _collect_item(payload, context, dependencies)

    async def extract(payload: ExtractEbayItemPayload, context: AttemptContext) -> WorkOutcome:
        return await _extract_item(payload, context, dependencies)

    async def image(payload: CollectEbayImagePayload, context: AttemptContext) -> WorkOutcome:
        return await _collect_image(payload, context, dependencies)

    async def description(
        payload: CollectEbayDescriptionPayload, context: AttemptContext
    ) -> WorkOutcome:
        return await _collect_description(payload, context, dependencies)

    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=COLLECT_EBAY_ITEM_WORK_KIND, payload_schema_version=1
                ),
                component=Component(COLLECT_ITEM, 2, collect),
                payload_type=CollectEbayItemPayload,
                handler=collect,
            ),
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=EXTRACT_EBAY_ITEM_WORK_KIND, payload_schema_version=1
                ),
                component=Component(EXTRACT_ITEM, 1, extract_ebay_item),
                payload_type=ExtractEbayItemPayload,
                handler=extract,
            ),
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=COLLECT_EBAY_IMAGE_WORK_KIND, payload_schema_version=1
                ),
                component=Component(COLLECT_IMAGE, 2, image),
                payload_type=CollectEbayImagePayload,
                handler=image,
            ),
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=COLLECT_EBAY_DESCRIPTION_WORK_KIND, payload_schema_version=1
                ),
                component=Component(COLLECT_DESCRIPTION, 2, extract_ebay_description),
                payload_type=CollectEbayDescriptionPayload,
                handler=description,
            ),
        )
    )
