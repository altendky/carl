"""Restart-safe eBay implementation of the shared search refresh workflow."""

from collections.abc import Callable
from dataclasses import dataclass
from time import time_ns
from typing import cast
from uuid import NAMESPACE_URL, uuid5

from carl.core.acquisition_identity import ebay_image_identity
from carl.core.components import Component, ComponentId
from carl.core.ebay import CollectEbaySearchPayload, collect_ebay_search_work
from carl.core.ebay_items import (
    CollectEbayImagePayload,
    CollectEbayItemPayload,
    EbayItemRequest,
    ExtractEbayItemPayload,
    collect_ebay_image_work,
    collect_ebay_item_work,
    extract_ebay_item_work,
)
from carl.core.ebay_refresh import REFRESH_EBAY_SEARCH_WORK_KIND, RefreshEbaySearchPayload
from carl.core.ebay_search_support import (
    UnsupportedEbaySearchMode,
    require_supported_ebay_search_acquisition,
)
from carl.core.models import JsonValue, NamedInput, NamedOutput, RecordDraft
from carl.core.refresh_recovery import classify_refresh_completion
from carl.core.search_scope import search_scope_changed
from carl.core.work import WorkCapability, WorkDefinition, WorkRequester, WorkState
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry
from carl.marketplace_listing import ebay_observations

_WAIT_NS = 5_000_000_000
_ITEM_REQUESTER = ("carl", "ebay", "search_refresh", "item")
_IMAGE_REQUESTER = ("carl", "ebay", "search_refresh", "image")


def _identifier(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, "\x1f".join(parts)))


def _anchor() -> None:
    """Identity anchor for eBay refresh orchestration."""


@dataclass(frozen=True, slots=True)
class EbayRefreshWorkerDependencies:
    database: Database
    new_identifier: Callable[[], str]
    utc_now_ns: Callable[[], int] = time_ns


def _mapping(value: JsonValue | None) -> dict[str, JsonValue]:
    return cast(dict[str, JsonValue], value) if isinstance(value, dict) else {}


def _strings(value: JsonValue | None) -> tuple[str, ...]:
    return (
        tuple(item for item in cast(list[JsonValue], value) if isinstance(item, str))
        if isinstance(value, list)
        else ()
    )


async def _works(
    database: Database, identifiers: tuple[str, ...]
) -> tuple[dict[str, JsonValue], ...]:
    return tuple([await database.work(identifier) for identifier in identifiers])


def _pending(works: tuple[dict[str, JsonValue], ...]) -> bool:
    return any(work.get("state") in {"pending", "leased"} for work in works)


async def _enqueue(
    definition: WorkDefinition,
    context: AttemptContext,
    dependencies: EbayRefreshWorkerDependencies,
    *,
    stage: str,
    requester_identifier: str,
) -> str:
    enqueued = await dependencies.database.enqueue_work(
        definition,
        WorkRequester(
            request_identifier=dependencies.new_identifier(),
            kind=("carl", "ebay", "search_refresh", stage),
            identifier=requester_identifier,
            context={"search_refresh_work_identifier": context.work_item_identifier},
        ),
        event_identifier=dependencies.new_identifier(),
        enqueued_at_utc_ns=dependencies.utc_now_ns(),
    )
    if stage != "search":
        await dependencies.database.attach_work_request(
            enqueued.work_item_identifier,
            WorkRequester(
                request_identifier=dependencies.new_identifier(),
                kind=("carl", "ebay", "search_refresh", stage),
                identifier=context.work_item_identifier,
                context={"search_refresh_work_identifier": context.work_item_identifier},
            ),
            event_identifier=dependencies.new_identifier(),
            requested_at_utc_ns=dependencies.utc_now_ns(),
        )
    return enqueued.work_item_identifier


async def _requested(database: Database, stage: str, requester_identifier: str) -> str | None:
    identifiers = await database.requested_work_identifiers(
        requester_kind=("carl", "ebay", "search_refresh", stage),
        requester_identifier=requester_identifier,
    )
    if len(identifiers) > 1:
        raise RuntimeError("Refresh stage requested more than one work item for one selection")
    return identifiers[0] if identifiers else None


async def _listing_identifiers(database: Database, identifier: str) -> tuple[str, ...]:
    kind, _, value = await database.get_record(identifier)
    if kind != ("carl", "ebay", "search_run"):
        raise ValueError("Refresh input is not an eBay search-run record")
    occurrences = _mapping(value).get("listing_occurrence_record_identifiers")
    if not isinstance(occurrences, list):
        raise ValueError("Search run has malformed occurrence references")
    occurrence_values = cast(list[JsonValue], occurrences)
    if len(_strings(occurrence_values)) != len(occurrence_values):
        raise ValueError("Search run has malformed occurrence references")
    identifiers: list[str] = []
    for occurrence_identifier in _strings(occurrence_values):
        occurrence_kind, _, occurrence = await database.get_record(occurrence_identifier)
        item = _mapping(occurrence).get("item_identifier")
        if occurrence_kind != ("carl", "ebay", "search_listing_occurrence") or not isinstance(
            item, str
        ):
            raise ValueError("Search run has a malformed item occurrence")
        # Validate before publishing any child work.
        _ = EbayItemRequest(item_identifier=item)
        if item not in identifiers:
            identifiers.append(item)
    return tuple(identifiers)


async def _search_phase(
    payload: RefreshEbaySearchPayload,
    context: AttemptContext,
    dependencies: EbayRefreshWorkerDependencies,
) -> WorkOutcome:
    database = dependencies.database
    identifier = await _requested(database, "search", context.work_item_identifier)
    if identifier is None:
        try:
            require_supported_ebay_search_acquisition(payload.search)
        except UnsupportedEbaySearchMode as error:
            return TerminalFailureWork(
                error={
                    "kind": "unsupported_ebay_search_mode",
                    "listing_state": payload.search.listing_state,
                    "message": str(error),
                },
                result={"stage": "search_unsupported"},
            )
        identifier = await _enqueue(
            collect_ebay_search_work(
                identifier=payload.search_work_identifier,
                payload=CollectEbaySearchPayload(request=payload.search),
                not_before_utc_ns=0,
            ),
            context,
            dependencies,
            stage="search",
            requester_identifier=context.work_item_identifier,
        )
    work = await database.work(identifier)
    result = _mapping(work.get("result"))
    checkpoint: dict[str, JsonValue] = {
        "stage": "collecting_search",
        "search_work_identifier": identifier,
    }
    inputs = (
        NamedInput(
            name=("base_search_run",), object_identifier=payload.base_search_run_record_identifier
        ),
    )
    if _pending((work,)):
        return RetryWork(
            inputs=inputs,
            delay_ns=_WAIT_NS,
            reason={"kind": "waiting_for_search_collection"},
            result=checkpoint,
        )
    refreshed = result.get("search_run_record_identifier")
    if work.get("state") != "completed" or not isinstance(refreshed, str):
        return TerminalFailureWork(
            inputs=inputs,
            error={"kind": "refresh_search_collection_failed"},
            result={**checkpoint, "stage": "search_failed", "search_work": work},
        )
    return RetryWork(
        inputs=(*inputs, NamedInput(name=("refreshed_search_run",), object_identifier=refreshed)),
        delay_ns=0,
        reason={"kind": "continue_refresh", "next_stage": "items"},
        result={
            **checkpoint,
            "stage": "search_complete",
            "refreshed_search_run_record_identifier": refreshed,
        },
    )


async def _item_phase(
    payload: RefreshEbaySearchPayload,
    checkpoint: dict[str, JsonValue],
    context: AttemptContext,
    dependencies: EbayRefreshWorkerDependencies,
) -> WorkOutcome:
    database = dependencies.database
    refreshed = checkpoint.get("refreshed_search_run_record_identifier")
    if not isinstance(refreshed, str):
        raise ValueError("Refresh checkpoint has no refreshed search run")
    base_ids = await _listing_identifiers(database, payload.base_search_run_record_identifier)
    refreshed_ids = await _listing_identifiers(database, refreshed)
    _, _, base = await database.get_record(payload.base_search_run_record_identifier)
    scope_changed = search_scope_changed(
        "ebay",
        base.get("request") if isinstance(base, dict) else None,
        payload.search.model_dump(mode="json"),
    )
    comparison_base_ids = () if scope_changed else base_ids
    selected = tuple(dict.fromkeys((*refreshed_ids, *comparison_base_ids)))[: payload.maximum_items]
    children: list[str] = []
    reused = 0
    for item in selected:
        requester = _identifier(context.work_item_identifier, "item_requester", item)
        child = await _requested(database, "item", requester)
        if child is None:
            request = EbayItemRequest(
                item_identifier=item,
                stack_identifier=payload.search.stack_identifier,
                maximum_images=0,
            )
            usable = [
                observation
                for _, observation in await ebay_observations(database, item)
                if observation.get("classification") == "detail"
                and _mapping(observation.get("request")).get("stack_identifier")
                == payload.search.stack_identifier
                and isinstance(observation.get("acquisition_record_identifier"), str)
            ]
            acquisition = usable[-1].get("acquisition_record_identifier") if usable else None
            definition = (
                extract_ebay_item_work(
                    identifier=_identifier(context.work_item_identifier, "item", item),
                    payload=ExtractEbayItemPayload(
                        request=request, acquisition_record_identifier=acquisition
                    ),
                )
                if isinstance(acquisition, str)
                else collect_ebay_item_work(
                    identifier=_identifier(context.work_item_identifier, "item", item),
                    payload=CollectEbayItemPayload(request=request),
                )
            )
            child = await _enqueue(
                definition, context, dependencies, stage="item", requester_identifier=requester
            )
        work = await database.work(child)
        if tuple(_strings(work.get("kind"))) == ("carl", "ebay", "extract", "item"):
            reused += 1
        children.append(child)
    works = await _works(database, tuple(children))
    result: dict[str, JsonValue] = {
        **checkpoint,
        "stage": "collecting_items",
        "item_collection_work_identifiers": children,
        "base_unique_listings": len(base_ids),
        "refreshed_unique_listings": len(refreshed_ids),
        "selected_unique_listings": len(selected),
        "reused_successful_item_pages": reused,
        "new_listing_identifiers": [item for item in refreshed_ids if item not in set(base_ids)],
        "absent_from_refresh_listing_identifiers": [
            item for item in comparison_base_ids if item not in set(refreshed_ids)
        ],
        "search_scope_comparison_valid": not scope_changed,
        "search_scope_changed": scope_changed,
        "comparison_warnings": ["search_scope_changed"] if scope_changed else [],
    }
    if _pending(works):
        return RetryWork(
            delay_ns=_WAIT_NS, reason={"kind": "waiting_for_item_collections"}, result=result
        )
    extractions: list[str] = []
    for identifier, work in zip(children, works, strict=True):
        extraction = _mapping(work.get("result")).get("extraction_work_identifier")
        if isinstance(extraction, str):
            extractions.append(extraction)
        elif _strings(work.get("kind")) == ("carl", "ebay", "extract", "item"):
            extractions.append(identifier)
    extracted = await _works(database, tuple(extractions))
    result.update(stage="extracting_items", item_extraction_work_identifiers=list(extractions))
    if _pending(extracted):
        return RetryWork(
            delay_ns=_WAIT_NS, reason={"kind": "waiting_for_item_extractions"}, result=result
        )
    observations: list[str] = []
    descriptions: list[str] = []
    failures = sum(
        work.get("state") != "completed"
        for work in works
        if _strings(work.get("kind")) != ("carl", "ebay", "extract", "item")
    )
    for work in extracted:
        extracted_result = _mapping(work.get("result"))
        if work.get("state") != "completed" or extracted_result.get("classification") not in {
            "detail",
            "unavailable",
        }:
            failures += 1
        observation = extracted_result.get("observation_record_identifier")
        if isinstance(observation, str) and extracted_result.get("classification") == "detail":
            observations.append(observation)
        descriptions.extend(_strings(extracted_result.get("description_work_identifiers")))
    description_works = await _works(database, tuple(descriptions))
    result.update(
        observation_record_identifiers=observations,
        description_work_identifiers=descriptions,
        item_failures=failures,
    )
    if _pending(description_works):
        return RetryWork(
            delay_ns=_WAIT_NS, reason={"kind": "waiting_for_item_descriptions"}, result=result
        )
    result.update(
        description_failures=sum(
            work.get("state") != "completed" or _mapping(work.get("result")).get("state") != "saved"
            for work in description_works
        ),
        stage="items_complete",
    )
    return RetryWork(
        delay_ns=0, reason={"kind": "continue_refresh", "next_stage": "images"}, result=result
    )


async def _image_phase(
    payload: RefreshEbaySearchPayload,
    checkpoint: dict[str, JsonValue],
    context: AttemptContext,
    dependencies: EbayRefreshWorkerDependencies,
) -> WorkOutcome:
    database = dependencies.database
    generation = checkpoint.get("item_retry_generation_identifier")
    plan_identifier = (
        _identifier(context.work_item_identifier, "image_plan", generation)
        if isinstance(generation, str)
        else _identifier(context.work_item_identifier, "image_plan")
    )
    try:
        kind, _, value = await database.get_record(plan_identifier)
        if kind != ("carl", "ebay", "image_followup_plan"):
            raise ValueError("Refresh image plan is malformed")
        reference_identifiers = _strings(_mapping(value).get("reference_record_identifiers"))
    except KeyError:
        records: list[RecordDraft] = []
        outputs: list[NamedOutput] = []
        selected_references: list[str] = []
        reused_reference_inputs: list[NamedInput] = []
        existing_references = {
            (
                _mapping(value).get("observation_record_identifier"),
                _mapping(value).get("gallery_order"),
                _mapping(value).get("url"),
                _mapping(value).get("item_identifier"),
            ): identifier
            for identifier, value in await database.records_by_kind(
                ("carl", "ebay", "gallery_image_reference")
            )
        }
        remaining = (
            max(0, payload.maximum_images - int(checkpoint.get("prior_image_collection_count", 0)))
            if payload.maximum_images is not None
            else None
        )
        observations: list[tuple[str, dict[str, JsonValue]]] = []
        for observation_identifier in _strings(checkpoint.get("observation_record_identifiers")):
            _, _, observation_value = await database.get_record(observation_identifier)
            observations.append((observation_identifier, _mapping(observation_value)))
        wanted_urls = tuple(
            dict.fromkeys(
                url
                for _, observation in observations
                for url in _strings(observation.get("gallery_urls"))
            )
        )
        wanted = {ebay_image_identity(url) for url in wanted_urls}
        selected_renditions: set[tuple[str, ...]] = set()
        saved_renditions: set[tuple[str, ...]] = set()
        validated_artifacts: dict[str, bool] = {}
        saved_candidates = []
        for start in range(0, len(wanted_urls), 1000):
            saved_candidates.extend(
                await database.saved_ebay_image_candidates(wanted_urls[start : start + 1000])
            )
        for _, raw in saved_candidates:
            saved = _mapping(raw)
            url, artifact = saved.get("url"), saved.get("image_artifact_identifier")
            if (
                saved.get("state") != "saved"
                or not isinstance(url, str)
                or not isinstance(artifact, str)
            ):
                continue
            identity = ebay_image_identity(url)
            if identity not in wanted:
                continue
            if artifact not in validated_artifacts:
                try:
                    await database.get_artifact(artifact)
                except (KeyError, ValueError, OSError):
                    validated_artifacts[artifact] = False
                else:
                    validated_artifacts[artifact] = True
            if validated_artifacts[artifact]:
                saved_renditions.add(identity)
        for observation_identifier, observation in observations:
            item = observation.get("item_identifier")
            if not isinstance(item, str):
                raise ValueError("Refresh item observation is malformed") from None
            for order, url in enumerate(_strings(observation.get("gallery_urls"))):
                identity = ebay_image_identity(url)
                needs_budget = (
                    identity not in selected_renditions and identity not in saved_renditions
                )
                if needs_budget and remaining == 0:
                    continue
                existing_reference = existing_references.get(
                    (observation_identifier, order, url, item)
                )
                reference_identifier = existing_reference or _identifier(
                    context.work_item_identifier,
                    "gallery_reference",
                    observation_identifier,
                    str(order),
                    url,
                )
                work_identifier = _identifier(
                    context.work_item_identifier, "image", reference_identifier
                )
                selected_references.append(reference_identifier)
                if existing_reference is not None:
                    reused_reference_inputs.append(
                        NamedInput(
                            name=("reused_gallery_reference", str(len(reused_reference_inputs))),
                            object_identifier=reference_identifier,
                        )
                    )
                else:
                    records.append(
                        RecordDraft(
                            identifier=reference_identifier,
                            kind=("carl", "ebay", "gallery_image_reference"),
                            schema_version=1,
                            value={
                                "item_identifier": item,
                                "observation_record_identifier": observation_identifier,
                                "acquisition_record_identifier": observation.get(
                                    "acquisition_record_identifier"
                                ),
                                "url": url,
                                "gallery_order": order,
                                "work_identifier": work_identifier,
                            },
                        )
                    )
                    outputs.append(
                        NamedOutput(
                            name=("gallery_reference", str(len(records) - 1)),
                            object_identifier=reference_identifier,
                        )
                    )
                selected_renditions.add(identity)
                if needs_budget and remaining is not None:
                    remaining -= 1
        reference_identifiers = tuple(selected_references)
        records.append(
            RecordDraft(
                identifier=plan_identifier,
                kind=("carl", "ebay", "image_followup_plan"),
                schema_version=1,
                value={"reference_record_identifiers": list(reference_identifiers)},
            )
        )
        outputs.append(
            NamedOutput(name=("image_followup_plan",), object_identifier=plan_identifier)
        )
        await database.publish_leased_operation_checkpoint(
            work_item_identifier=context.work_item_identifier,
            lease_token=context.lease_token,
            worker_identifier=context.worker_identifier,
            utc_now_ns=dependencies.utc_now_ns,
            operation_id=context.operation_identifier,
            records=tuple(records),
            artifacts=(),
            outputs=tuple(outputs),
            checkpoint_result={
                **checkpoint,
                "stage": "collecting_images",
                "image_followup_plan_record_identifier": plan_identifier,
            },
            inputs=(
                *reused_reference_inputs,
                *tuple(
                    NamedInput(
                        name=("listing_observation", str(index)), object_identifier=identifier
                    )
                    for index, identifier in enumerate(
                        _strings(checkpoint.get("observation_record_identifiers"))
                    )
                ),
            ),
        )
    children: list[str] = []
    pending_urls: set[str] = set()
    for reference_identifier in reference_identifiers:
        _, _, value = await database.get_record(reference_identifier)
        reference = _mapping(value)
        request = CollectEbayImagePayload.model_validate(
            {
                "item_identifier": reference.get("item_identifier"),
                "observation_record_identifier": reference.get("observation_record_identifier"),
                "reference_record_identifier": reference_identifier,
                "url": reference.get("url"),
            }
        )
        requester = _identifier(
            context.work_item_identifier, "image_requester", reference_identifier
        )
        child = await _requested(database, "image", requester)
        # Complete the first exact URL before queuing its other references, so
        # they reuse that download rather than racing redundant acquisitions.
        if child is None and request.url in pending_urls:
            continue
        if child is None:
            child = await _enqueue(
                collect_ebay_image_work(
                    identifier=_identifier(
                        context.work_item_identifier, "image", reference_identifier
                    ),
                    payload=request,
                ),
                context,
                dependencies,
                stage="image",
                requester_identifier=requester,
            )
        children.append(child)
        if _pending((await database.work(child),)):
            pending_urls.add(request.url)
    works = await _works(database, tuple(children))
    result: dict[str, JsonValue] = {
        **checkpoint,
        "stage": "collecting_images",
        "image_followup_plan_record_identifier": plan_identifier,
        "image_collection_work_identifiers": children,
    }
    if _pending(works):
        return RetryWork(
            delay_ns=_WAIT_NS, reason={"kind": "waiting_for_image_collections"}, result=result
        )
    saved = sum(
        work.get("state") == "completed" and _mapping(work.get("result")).get("state") == "saved"
        for work in works
    )
    failures = len(works) - saved
    total_failures = (
        failures
        + int(str(checkpoint.get("item_failures", 0)))
        + int(str(checkpoint.get("description_failures", 0)))
    )
    return classify_refresh_completion(
        CompletedWork(
            result={
                **result,
                "stage": "complete",
                "state": "completed" if total_failures == 0 else "completed_with_failures",
                "new_image_collections": len(works),
                "new_images_saved": saved,
                "new_images_failed": failures,
            }
        )
    )


async def ebay_refresh_child_work_states(
    database: Database, work_identifier: str
) -> dict[str, tuple[WorkState, ...]]:
    """Read linked children for the shared progress surface without queuing work."""
    item_edges = await database.requested_work_edges(
        requester_kind=_ITEM_REQUESTER, requester_identifier=work_identifier
    )
    image_edges = await database.requested_work_edges(
        requester_kind=_IMAGE_REQUESTER, requester_identifier=work_identifier
    )
    checkpoint = _mapping((await database.work(work_identifier)).get("result"))

    async def children(edges: tuple[dict[str, JsonValue], ...]) -> tuple[dict[str, JsonValue], ...]:
        identifiers = tuple(
            dict.fromkeys(
                str(edge["work_identifier"])
                for edge in edges
                if _mapping(edge.get("context")).get("search_refresh_work_identifier")
                == work_identifier
            )
        )
        return await _works(database, identifiers)

    item_values = await children(item_edges)
    image_values = await children(image_edges)
    items = await _works(
        database,
        tuple(
            dict.fromkeys(
                (
                    *(str(item["identifier"]) for item in item_values),
                    *_strings(checkpoint.get("item_collection_work_identifiers")),
                )
            )
        ),
    )
    images = await _works(
        database,
        tuple(
            dict.fromkeys(
                (
                    *(str(image["identifier"]) for image in image_values),
                    *_strings(checkpoint.get("image_collection_work_identifiers")),
                )
            )
        ),
    )
    extraction_ids = tuple(
        extraction
        for item in items
        if isinstance(
            extraction := _mapping(item.get("result")).get("extraction_work_identifier"), str
        )
    )
    extractions = await _works(database, extraction_ids)
    reused = tuple(
        item for item in items if _strings(item.get("kind")) == ("carl", "ebay", "extract", "item")
    )
    description_ids = tuple(
        dict.fromkeys(
            (
                *_strings(checkpoint.get("description_work_identifiers")),
                *(
                    identifier
                    for work in (*extractions, *reused)
                    for identifier in _strings(
                        _mapping(work.get("result")).get("description_work_identifiers")
                    )
                ),
            )
        )
    )
    descriptions = await _works(database, description_ids)
    return {
        "item_pages": tuple(
            WorkState(str(item["state"]))
            for item in items
            if _strings(item.get("kind")) != ("carl", "ebay", "extract", "item")
        ),
        "item_extractions": tuple(
            WorkState(str(item["state"])) for item in (*extractions, *reused)
        ),
        "images": tuple(WorkState(str(image["state"])) for image in images),
        "image_extractions": (),
        "descriptions": tuple(WorkState(str(work["state"])) for work in descriptions),
    }


def build_ebay_refresh_worker_registry(
    dependencies: EbayRefreshWorkerDependencies,
) -> WorkHandlerRegistry:
    async def refresh(payload: RefreshEbaySearchPayload, context: AttemptContext) -> WorkOutcome:
        work = await dependencies.database.work(context.work_item_identifier)
        checkpoint = _mapping(work.get("result"))
        stage = checkpoint.get("stage")
        if not checkpoint or stage == "collecting_search":
            return await _search_phase(payload, context, dependencies)
        if stage in {"search_complete", "collecting_items", "extracting_items"}:
            return await _item_phase(payload, checkpoint, context, dependencies)
        if stage in {"items_complete", "collecting_images"}:
            return await _image_phase(payload, checkpoint, context, dependencies)
        return TerminalFailureWork(
            error={"kind": "invalid_search_refresh_checkpoint"},
            result={"stage": "failed", "checkpoint": checkpoint},
        )

    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=REFRESH_EBAY_SEARCH_WORK_KIND, payload_schema_version=1
                ),
                component=Component(ComponentId(("carl", "ebay", "refresh", "search")), 4, _anchor),
                payload_type=RefreshEbaySearchPayload,
                handler=refresh,
            ),
        )
    )
