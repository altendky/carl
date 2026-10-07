"""Single-item Facebook detail and gallery orchestration without a search run."""

from collections.abc import Callable
from dataclasses import dataclass
from time import time_ns
from typing import cast
from uuid import NAMESPACE_URL, uuid5

import anyio

from carl.core.components import Component, ComponentId
from carl.core.facebook_images import (
    CollectImagePayload,
    GalleryImageReference,
    ImageReuseMatchKind,
    SavedImageCandidate,
    collect_image_work,
    gallery_references,
    image_reuse_record,
    plan_image_followups,
)
from carl.core.facebook_listing import (
    REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
    RequestFacebookListingDetailsPayload,
)
from carl.core.facebook_work import (
    CollectItemPayload,
    collect_item_work,
    facebook_network_policy_constraints,
    legacy_facebook_network_constraint_identifiers,
)
from carl.core.http import RequestPlan
from carl.core.json import encode_json
from carl.core.models import Header, JsonValue, NamedInput, NamedOutput, RecordDraft
from carl.core.work import WorkCapability, WorkDefinition, WorkRequester
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.facebook_image_workers import publish_shared_image_reuses, validate_saved_image_results
from carl.io.browser_identity import brave_navigation_headers
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry

_WAIT_NS = 5_000_000_000


@dataclass(frozen=True, slots=True)
class FacebookListingWorkerDependencies:
    database: Database
    new_identifier: Callable[[], str]
    utc_now_ns: Callable[[], int] = time_ns


def _identifier(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, "\x1f".join(parts)))


def _mapping(value: JsonValue | None) -> dict[str, JsonValue]:
    return cast(dict[str, JsonValue], value) if isinstance(value, dict) else {}


def _strings(value: JsonValue | None) -> tuple[str, ...]:
    return (
        tuple(item for item in cast(list[JsonValue], value) if isinstance(item, str))
        if isinstance(value, list)
        else ()
    )


def _pending(work: dict[str, JsonValue]) -> bool:
    return work.get("state") in {"pending", "leased"}


def _anchor() -> None:
    """Identity anchor for selected-item follow-up."""


async def _enqueue(
    definition: WorkDefinition,
    stage: str,
    context: AttemptContext,
    dependencies: FacebookListingWorkerDependencies,
) -> str:
    result = await dependencies.database.enqueue_work(
        definition,
        WorkRequester(
            request_identifier=dependencies.new_identifier(),
            kind=("carl", "facebook", "listing_details", stage),
            identifier=context.work_item_identifier,
            context={},
        ),
        event_identifier=dependencies.new_identifier(),
        enqueued_at_utc_ns=dependencies.utc_now_ns(),
    )
    return result.work_item_identifier


async def _item_phase(
    payload: RequestFacebookListingDetailsPayload,
    checkpoint: dict[str, JsonValue],
    context: AttemptContext,
    dependencies: FacebookListingWorkerDependencies,
) -> WorkOutcome:
    database = dependencies.database
    children = await database.requested_work_identifiers(
        requester_kind=("carl", "facebook", "listing_details", "item"),
        requester_identifier=context.work_item_identifier,
    )
    if len(children) > 1:
        raise RuntimeError("Listing-details coordinator requested duplicate item work")
    if not children:
        successful = await database.successful_facebook_item_page_results(
            (payload.listing_identifier,)
        )
        if successful and not payload.refresh:
            latest = max(
                successful,
                key=lambda result: (
                    result.acquisition_completion_sequence,
                    result.extraction_completion_sequence,
                ),
            )
            return RetryWork(
                inputs=(
                    NamedInput(
                        name=("reused_item_page",),
                        object_identifier=latest.observation_record_identifier,
                    ),
                ),
                delay_ns=0,
                reason={"kind": "continue_listing_details"},
                result={
                    "stage": "items_complete",
                    "observation_record_identifier": latest.observation_record_identifier,
                    "reused_item_page": True,
                },
            )
        await database.supersede_constraints(
            retired_identifiers=legacy_facebook_network_constraint_identifiers(
                payload.item_routing
            ),
            replacements=facebook_network_policy_constraints(payload.item_routing),
            operation_identifier=context.operation_identifier,
            at_utc_ns=dependencies.utc_now_ns(),
            reason="Use existing bounded item-page collection policy",
        )
        headers = await anyio.to_thread.run_sync(brave_navigation_headers, abandon_on_cancel=True)
        definition = collect_item_work(
            identifier=_identifier(context.work_item_identifier, "item"),
            payload=CollectItemPayload(
                listing_id=payload.listing_identifier,
                request_plan=RequestPlan(
                    url=f"https://www.facebook.com/marketplace/item/{payload.listing_identifier}/",
                    headers=headers,
                    routing=payload.item_routing,
                ),
            ),
            not_before_utc_ns=0,
        )
        if payload.refresh:
            children = (await _enqueue(definition, "item", context, dependencies),)
        else:
            enqueued, retained = await database.enqueue_work_unless_facebook_item_page_is_usable(
                definition,
                WorkRequester(
                    request_identifier=dependencies.new_identifier(),
                    kind=("carl", "facebook", "listing_details", "item"),
                    identifier=context.work_item_identifier,
                    context={},
                ),
                listing_identifier=payload.listing_identifier,
                event_identifier=dependencies.new_identifier(),
                enqueued_at_utc_ns=dependencies.utc_now_ns(),
            )
            if retained is not None:
                return RetryWork(
                    delay_ns=0,
                    reason={"kind": "continue_listing_details"},
                    inputs=(
                        NamedInput(
                            name=("reused_item_page",),
                            object_identifier=retained.observation_record_identifier,
                        ),
                    ),
                    result={
                        "stage": "items_complete",
                        "observation_record_identifier": retained.observation_record_identifier,
                        "reused_item_page": True,
                    },
                )
            if enqueued is None:
                raise RuntimeError("Item acquisition registration produced no result")
            children = (enqueued.work_item_identifier,)
    work = await database.work(children[0])
    result: dict[str, JsonValue] = {
        **checkpoint,
        "stage": "collecting_items",
        "item_collection_work_identifier": children[0],
        "reused_item_page": False,
    }
    if _pending(work):
        return RetryWork(
            delay_ns=_WAIT_NS, reason={"kind": "waiting_for_item_collection"}, result=result
        )
    extraction = _mapping(work.get("result")).get("extraction_work_identifier")
    if work.get("state") != "completed" or not isinstance(extraction, str):
        return TerminalFailureWork(
            error={"kind": "listing_detail_collection_failed"}, result=result
        )
    extracted = await database.work(extraction)
    result.update(stage="extracting_items", item_extraction_work_identifier=extraction)
    if _pending(extracted):
        return RetryWork(
            delay_ns=_WAIT_NS, reason={"kind": "waiting_for_item_extraction"}, result=result
        )
    observation = _mapping(extracted.get("result")).get("observation_record_identifier")
    if extracted.get("state") != "completed" or not isinstance(observation, str):
        return TerminalFailureWork(
            error={"kind": "listing_detail_extraction_failed"}, result=result
        )
    return RetryWork(
        inputs=(NamedInput(name=("listing_observation",), object_identifier=observation),),
        delay_ns=0,
        reason={"kind": "continue_listing_details"},
        result={**result, "stage": "items_complete", "observation_record_identifier": observation},
    )


async def _image_phase(
    payload: RequestFacebookListingDetailsPayload,
    checkpoint: dict[str, JsonValue],
    context: AttemptContext,
    dependencies: FacebookListingWorkerDependencies,
) -> WorkOutcome:
    database = dependencies.database
    observation_identifier = checkpoint.get("observation_record_identifier")
    if not isinstance(observation_identifier, str):
        raise ValueError("Listing details checkpoint has no observation")
    plan_identifier = _identifier(context.work_item_identifier, "image_plan")
    try:
        kind, _, value = await database.get_record(plan_identifier)
        if kind != ("carl", "facebook", "listing_details_image_plan"):
            raise ValueError("Listing details image plan has the wrong kind")
        plan = _mapping(value)
    except KeyError:
        _, _, observation = await database.get_record(observation_identifier)
        references = gallery_references(
            observation_identifier=observation_identifier, observation=observation
        )
        saved: list[SavedImageCandidate] = []
        for identifier, value in await validate_saved_image_results(
            database,
            await database.saved_facebook_image_candidates_for_references(
                references[: payload.maximum_images]
            ),
        ):
            saved.append(
                SavedImageCandidate.model_validate(
                    {
                        "image_result_record_identifier": identifier,
                        "source_photo_id": _mapping(value).get("source_photo_id"),
                        "original_url": _mapping(value).get("original_url"),
                        "width": _mapping(value).get("width"),
                        "height": _mapping(value).get("height"),
                    }
                )
            )
        followups = plan_image_followups(
            references[: payload.maximum_images], tuple(saved), payload.maximum_images
        )
        existing = dict(
            await database.facebook_gallery_reference_identifiers((observation_identifier,))
        )
        records: list[RecordDraft] = []
        reference_ids: list[str] = []
        for index, reference in enumerate(references):
            identifier = existing.get(
                reference, _identifier(context.work_item_identifier, "reference", str(index))
            )
            reference_ids.append(identifier)
            if reference not in existing:
                records.append(
                    RecordDraft(
                        identifier=identifier,
                        kind=("carl", "facebook", "gallery_image_reference"),
                        schema_version=1,
                        value=reference.model_dump(mode="json"),
                    )
                )
        plan = {
            "reference_record_identifiers": reference_ids,
            "download_groups": [
                [reference_ids[index] for index in group] for group in followups.download_groups
            ],
            "reuses": [
                {
                    "reference_record_identifier": reference_ids[decision.reference_index],
                    "source_image_result_record_identifier": decision.candidate.image_result_record_identifier,
                    "match_kind": decision.match_kind.value,
                }
                for decision in followups.reuse_decisions
            ],
        }
        records.append(
            RecordDraft(
                identifier=plan_identifier,
                kind=("carl", "facebook", "listing_details_image_plan"),
                schema_version=1,
                value=plan,
            )
        )
        await database.publish_leased_operation_checkpoint(
            work_item_identifier=context.work_item_identifier,
            lease_token=context.lease_token,
            worker_identifier=context.worker_identifier,
            utc_now_ns=dependencies.utc_now_ns,
            operation_id=context.operation_identifier,
            records=tuple(records),
            artifacts=(),
            outputs=tuple(
                NamedOutput(
                    name=("image_plan_record", str(index)), object_identifier=record.identifier
                )
                for index, record in enumerate(records)
            ),
            checkpoint_result={
                **checkpoint,
                "stage": "images_planned",
                "image_plan_record_identifier": plan_identifier,
            },
            inputs=(
                NamedInput(name=("listing_observation",), object_identifier=observation_identifier),
            ),
        )
        return RetryWork(
            delay_ns=0,
            reason={"kind": "continue_listing_details"},
            result={
                **checkpoint,
                "stage": "images_planned",
                "image_plan_record_identifier": plan_identifier,
            },
        )
    reuse_records: list[RecordDraft] = []
    reuse_inputs: list[NamedInput] = []
    raw_reuses = plan.get("reuses")
    reuses = cast(list[JsonValue], raw_reuses) if isinstance(raw_reuses, list) else []
    for index, raw_reuse in enumerate(reuses):
        reuse = _mapping(raw_reuse)
        identifier = _identifier(context.work_item_identifier, "reuse", str(index))
        try:
            _ = await database.get_record(identifier)
        except KeyError:
            resolution = image_reuse_record(ImageReuseMatchKind(str(reuse["match_kind"])))
            reuse_records.append(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "facebook", "image_reuse"),
                    schema_version=1,
                    value=resolution.model_dump(mode="json"),
                )
            )
            reuse_inputs.extend(
                (
                    NamedInput(
                        name=("gallery_image_reference", str(index)),
                        object_identifier=str(reuse["reference_record_identifier"]),
                    ),
                    NamedInput(
                        name=("source_image_result", str(index)),
                        object_identifier=str(reuse["source_image_result_record_identifier"]),
                    ),
                )
            )
    if reuse_records:
        await database.publish_leased_operation_checkpoint(
            work_item_identifier=context.work_item_identifier,
            lease_token=context.lease_token,
            worker_identifier=context.worker_identifier,
            utc_now_ns=dependencies.utc_now_ns,
            operation_id=context.operation_identifier,
            records=tuple(reuse_records),
            artifacts=(),
            outputs=tuple(
                NamedOutput(name=("image_reuse", str(index)), object_identifier=record.identifier)
                for index, record in enumerate(reuse_records)
            ),
            inputs=tuple(reuse_inputs),
            checkpoint_result={**checkpoint, "stage": "collecting_images"},
        )
    children: list[str] = []
    groups = plan.get("download_groups")
    for index, group in enumerate(
        cast(list[JsonValue], groups) if isinstance(groups, list) else []
    ):
        identifiers = _strings(group)
        if not identifiers:
            raise ValueError("Empty listing image download group")
        _, _, value = await database.get_record(identifiers[0])
        reference = GalleryImageReference.model_validate_json(encode_json(value))
        definition = collect_image_work(
            identifier=_identifier(context.work_item_identifier, "image", str(index)),
            payload=CollectImagePayload(
                reference_record_identifier=identifiers[0],
                reference=reference,
                request_plan=RequestPlan(
                    url=reference.original_url,
                    headers=(
                        Header(
                            name=b"Accept",
                            value=b"image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                        ),
                    ),
                    follow_redirects=False,
                    max_redirects=0,
                    routing=payload.image_routing,
                    compression=("identity",),
                ),
            ),
            not_before_utc_ns=0,
        )
        requested = await database.requested_work_identifiers(
            requester_kind=("carl", "facebook", "listing_details", "image_group"),
            requester_identifier=_identifier(
                context.work_item_identifier, "image_group", str(index)
            ),
        )
        if len(requested) > 1:
            raise RuntimeError("Duplicate listing image group request")
        if requested:
            child = requested[0]
        else:
            result = await database.enqueue_work(
                definition,
                WorkRequester(
                    request_identifier=dependencies.new_identifier(),
                    kind=("carl", "facebook", "listing_details", "image_group"),
                    identifier=_identifier(context.work_item_identifier, "image_group", str(index)),
                    context={"listing_details_work_identifier": context.work_item_identifier},
                ),
                event_identifier=dependencies.new_identifier(),
                enqueued_at_utc_ns=dependencies.utc_now_ns(),
            )
            child = result.work_item_identifier
        for identifier in identifiers:
            linked = await database.requested_work_identifiers(
                requester_kind=("carl", "facebook", "gallery_image_reference"),
                requester_identifier=identifier,
            )
            if child not in linked:
                await database.attach_work_request(
                    child,
                    WorkRequester(
                        request_identifier=dependencies.new_identifier(),
                        kind=("carl", "facebook", "gallery_image_reference"),
                        identifier=identifier,
                        context={},
                    ),
                    event_identifier=dependencies.new_identifier(),
                    requested_at_utc_ns=dependencies.utc_now_ns(),
                )
        children.append(child)
    works = tuple([await database.work(identifier) for identifier in children])
    result = {
        **checkpoint,
        "stage": "collecting_images",
        "image_work_identifiers": children,
        "reused_images": len(reuses),
    }
    if any(_pending(work) for work in works):
        return RetryWork(delay_ns=_WAIT_NS, reason={"kind": "waiting_for_images"}, result=result)
    extraction_ids = tuple(
        identifier
        for work in works
        if isinstance(
            identifier := _mapping(work.get("result")).get("extraction_work_identifier"), str
        )
    )
    extracted = tuple([await database.work(identifier) for identifier in extraction_ids])
    if any(_pending(work) for work in extracted):
        return RetryWork(
            delay_ns=_WAIT_NS,
            reason={"kind": "waiting_for_image_extractions"},
            result={**result, "stage": "extracting_images"},
        )
    by_identifier = dict(zip(extraction_ids, extracted, strict=True))
    saved_count = 0
    for work in works:
        work_result = _mapping(work.get("result"))
        extraction_identifier = work_result.get("extraction_work_identifier")
        extraction_work = by_identifier.get(str(extraction_identifier))
        if work.get("state") == "completed" and (
            work_result.get("state") == "saved"
            or (
                extraction_work is not None
                and extraction_work.get("state") == "completed"
                and _mapping(extraction_work.get("result")).get("state") == "saved"
            )
        ):
            saved_count += 1
    failures = len(works) - saved_count
    await publish_shared_image_reuses(
        database=database,
        context=context,
        reference_identifiers=tuple(
            identifier
            for group in (cast(list[JsonValue], groups) if isinstance(groups, list) else [])
            for identifier in _strings(group)
        ),
        checkpoint=result,
        utc_now_ns=dependencies.utc_now_ns,
    )
    return CompletedWork(
        result={
            **result,
            "stage": "complete",
            "state": "completed" if failures == 0 else "completed_with_failures",
            "image_failures": failures,
            "saved_images": saved_count,
        }
    )


def build_facebook_listing_worker_registry(
    dependencies: FacebookListingWorkerDependencies,
) -> WorkHandlerRegistry:
    async def details(
        payload: RequestFacebookListingDetailsPayload, context: AttemptContext
    ) -> WorkOutcome:
        checkpoint = _mapping(
            (await dependencies.database.work(context.work_item_identifier)).get("result")
        )
        if checkpoint.get("stage") in {
            "items_complete",
            "images_planned",
            "collecting_images",
            "extracting_images",
        }:
            return await _image_phase(payload, checkpoint, context, dependencies)
        if checkpoint and checkpoint.get("stage") not in {"collecting_items", "extracting_items"}:
            return TerminalFailureWork(
                error={"kind": "invalid_listing_details_checkpoint"},
                result={"stage": "failed", "checkpoint": checkpoint},
            )
        return await _item_phase(payload, checkpoint, context, dependencies)

    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND, payload_schema_version=1
                ),
                component=Component(
                    ComponentId(("carl", "facebook", "listing_details")), 1, _anchor
                ),
                payload_type=RequestFacebookListingDetailsPayload,
                handler=details,
            ),
        )
    )
