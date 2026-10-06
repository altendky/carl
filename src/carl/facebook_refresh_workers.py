"""Durable orchestration for refreshing search, item, and image evidence."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter_ns, time_ns
from uuid import NAMESPACE_URL, uuid5

import anyio

from carl.core.components import Component, ComponentId, Registry
from carl.core.facebook_images import (
    CollectImagePayload,
    GalleryImageReference,
    ImageReuseMatchKind,
    SavedImageCandidate,
    collect_image_work,
    gallery_references,
    image_network_constraints,
    image_reuse_record,
    legacy_image_network_constraint_identifiers,
    plan_image_followups,
)
from carl.core.facebook_refresh import (
    LEGACY_REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION,
    REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION,
    REFRESH_SEARCH_WORK_KIND,
    RefreshSearchPayload,
)
from carl.core.facebook_work import (
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    CollectItemPayload,
    SearchRunListingCandidates,
    SuccessfulItemPageResult,
    collect_item_work,
    collect_search_work,
    facebook_network_policy_constraints,
    legacy_facebook_network_constraint_identifiers,
    plan_item_page_followups,
    retryable_search_failure,
)
from carl.core.http import RequestPlan
from carl.core.models import (
    CodeProvenance,
    Header,
    JsonValue,
    NamedInput,
    NamedOutput,
    RecordDraft,
)
from carl.core.refresh_recovery import classify_refresh_completion, refresh_failure_summary
from carl.core.work import WorkCapability, WorkRequester, WorkState
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.facebook_image_workers import (
    EXTRACT_FACEBOOK_GALLERY_REFERENCES,
    REUSE_FACEBOOK_GALLERY_IMAGE,
    build_image_component_registry,
)
from carl.io.browser_identity import brave_navigation_headers
from carl.io.provenance import process_invocation
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry

REFRESH_FACEBOOK_SEARCH = ComponentId(("carl", "facebook", "refresh", "search"))
_CHILD_WAIT_DELAY_NS = 5_000_000_000


def _component_anchor() -> None:
    """Identity anchor for the refresh coordinator."""


def build_refresh_component_registry() -> Registry:
    return Registry((Component(REFRESH_FACEBOOK_SEARCH, 2, _component_anchor),))


def _stable_identifier(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, "\x1f".join(parts)))


def _utc_text(utc_ns: int) -> str:
    return datetime.fromtimestamp(utc_ns / 1_000_000_000, tz=UTC).isoformat()


def _saved_image_candidates(
    results: tuple[tuple[str, dict[str, JsonValue]], ...],
) -> tuple[SavedImageCandidate, ...]:
    return tuple(
        SavedImageCandidate.model_validate(
            {
                "image_result_record_identifier": identifier,
                "source_photo_id": value.get("source_photo_id"),
                "original_url": value.get("original_url"),
                "width": value.get("width"),
                "height": value.get("height"),
            }
        )
        for identifier, value in results
    )


@dataclass(frozen=True, slots=True)
class RefreshWorkerDependencies:
    database: Database
    new_identifier: Callable[[], str]
    utc_now_ns: Callable[[], int] = time_ns
    monotonic_ns: Callable[[], int] = perf_counter_ns
    code_provenance: Callable[[], Awaitable[CodeProvenance]] | None = None

    async def provenance(self) -> CodeProvenance:
        if self.code_provenance is None:
            raise RuntimeError("Refresh worker requires a code-provenance provider")
        return await self.code_provenance()


async def _work_or_none(database: Database, identifier: str) -> dict[str, JsonValue] | None:
    try:
        return await database.work(identifier)
    except KeyError:
        return None


async def _works(
    database: Database, identifiers: tuple[str, ...]
) -> tuple[dict[str, JsonValue], ...]:
    return tuple([await database.work(identifier) for identifier in identifiers])


def _unfinished(works: tuple[dict[str, JsonValue], ...]) -> bool:
    return any(
        work.get("state") in {WorkState.PENDING.value, WorkState.LEASED.value} for work in works
    )


def _search_listing_identifiers(value: JsonValue) -> tuple[str, ...]:
    if not isinstance(value, dict):
        raise ValueError("Search run is malformed")
    traversal = value.get("traversal")
    identifiers = (
        traversal.get("unique_listing_identifiers") if isinstance(traversal, dict) else None
    )
    if not isinstance(identifiers, list) or not all(
        isinstance(identifier, str) and identifier.isdecimal() for identifier in identifiers
    ):
        raise ValueError("Search run has no valid listing identifiers")
    return tuple(identifiers)


async def _activate_page_policy(
    dependencies: RefreshWorkerDependencies,
    routing: tuple[str, ...],
    operation_identifier: str,
) -> None:
    await dependencies.database.supersede_constraints(
        retired_identifiers=legacy_facebook_network_constraint_identifiers(routing),
        replacements=facebook_network_policy_constraints(routing),
        operation_identifier=operation_identifier,
        at_utc_ns=dependencies.utc_now_ns(),
        reason="Marketplace page request pace raised for a bounded trial",
    )


async def _search_phase(
    payload: RefreshSearchPayload,
    context: AttemptContext,
    dependencies: RefreshWorkerDependencies,
) -> WorkOutcome:
    database = dependencies.database
    await _activate_page_policy(dependencies, payload.search.routing, context.operation_identifier)
    requester_kind = ("carl", "facebook", "search_refresh", "search")
    requested = await database.requested_work_identifiers(
        requester_kind=requester_kind,
        requester_identifier=context.work_item_identifier,
    )
    if len(requested) > 1:
        raise RuntimeError("Search refresh has more than one requested search work item")
    search_work_identifier = requested[0] if requested else payload.search_work_identifier
    work = await _work_or_none(database, search_work_identifier)
    if work is None:
        enqueued = await database.enqueue_work(
            collect_search_work(
                identifier=search_work_identifier,
                payload=payload.search,
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier=dependencies.new_identifier(),
                kind=requester_kind,
                identifier=context.work_item_identifier,
                context={"stage": "search"},
            ),
            event_identifier=dependencies.new_identifier(),
            enqueued_at_utc_ns=dependencies.utc_now_ns(),
        )
        search_work_identifier = enqueued.work_item_identifier
    result = await database.work(search_work_identifier)
    if result.get("state") in {WorkState.PENDING.value, WorkState.LEASED.value}:
        return RetryWork(
            inputs=(
                NamedInput(
                    name=("base_search_run",),
                    object_identifier=payload.base_search_run_record_identifier,
                ),
            ),
            delay_ns=_CHILD_WAIT_DELAY_NS,
            reason={"kind": "waiting_for_search_collection"},
            result={
                "stage": "collecting_search",
                "search_work_identifier": search_work_identifier,
            },
        )
    if result.get("state") != WorkState.COMPLETED.value:
        error = result.get("error")
        attempt = result.get("attempt")
        operations = result.get("operations")
        latest_operation = operations[-1] if isinstance(operations, list) and operations else None
        checkpoint_operation_identifier = (
            latest_operation.get("operation_identifier")
            if isinstance(latest_operation, dict)
            else None
        )
        if (
            isinstance(attempt, int)
            and retryable_search_failure(error, attempt=attempt)
            and isinstance(checkpoint_operation_identifier, str)
        ):
            await database.retry_terminal_work_from_operation(
                work_item_identifier=search_work_identifier,
                checkpoint_operation_identifier=checkpoint_operation_identifier,
                retried_at_utc_ns=dependencies.utc_now_ns(),
                event_identifier=dependencies.new_identifier(),
                reason={
                    "kind": "search_refresh_resumed_transient_search_failure",
                    "refresh_work_identifier": context.work_item_identifier,
                },
                payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
            )
            return RetryWork(
                inputs=(
                    NamedInput(
                        name=("base_search_run",),
                        object_identifier=payload.base_search_run_record_identifier,
                    ),
                ),
                delay_ns=_CHILD_WAIT_DELAY_NS,
                reason={"kind": "resumed_search_collection_after_transient_failure"},
                result={
                    "stage": "collecting_search",
                    "search_work_identifier": search_work_identifier,
                },
            )
        return TerminalFailureWork(
            inputs=(
                NamedInput(
                    name=("base_search_run",),
                    object_identifier=payload.base_search_run_record_identifier,
                ),
            ),
            error={"kind": "refresh_search_collection_failed"},
            result={
                "stage": "search_failed",
                "search_work_identifier": search_work_identifier,
                "search_work": result,
            },
        )
    work_result = result.get("result")
    refreshed = (
        work_result.get("search_run_record_identifier") if isinstance(work_result, dict) else None
    )
    if not isinstance(refreshed, str):
        return TerminalFailureWork(
            error={"kind": "refresh_search_result_missing"},
            result={"stage": "search_failed", "search_work_identifier": result["identifier"]},
        )
    return RetryWork(
        inputs=(
            NamedInput(
                name=("base_search_run",),
                object_identifier=payload.base_search_run_record_identifier,
            ),
            NamedInput(name=("refreshed_search_run",), object_identifier=refreshed),
        ),
        delay_ns=0,
        reason={"kind": "continue_refresh", "next_stage": "items"},
        result={
            "stage": "search_complete",
            "search_work_identifier": search_work_identifier,
            "refreshed_search_run_record_identifier": refreshed,
        },
    )


async def _item_phase(
    payload: RefreshSearchPayload,
    refreshed_identifier: str,
    context: AttemptContext,
    dependencies: RefreshWorkerDependencies,
    *,
    checkpoint: dict[str, JsonValue] | None = None,
) -> WorkOutcome:
    checkpoint = checkpoint or {}
    database = dependencies.database
    _, _, base = await database.get_record(payload.base_search_run_record_identifier)
    _, _, refreshed = await database.get_record(refreshed_identifier)
    base_ids = _search_listing_identifiers(base)
    refreshed_ids = _search_listing_identifiers(refreshed)
    listing_ids = tuple(dict.fromkeys((*refreshed_ids, *base_ids)))[: payload.maximum_items]
    successful_results = await database.successful_facebook_item_page_results(listing_ids)
    followup_plan = plan_item_page_followups(
        (
            SearchRunListingCandidates(
                search_run_record_identifier=refreshed_identifier,
                listing_identifiers=refreshed_ids,
            ),
            SearchRunListingCandidates(
                search_run_record_identifier=payload.base_search_run_record_identifier,
                listing_identifiers=base_ids,
            ),
        ),
        successful_results,
        maximum_items=payload.maximum_items,
    )
    collection_identifiers: list[str] = []
    reused_results: list[SuccessfulItemPageResult] = []
    headers: tuple[Header, ...] | None = None
    for decision in followup_plan.decisions:
        listing_identifier = decision.listing_id
        requester_identifier = _stable_identifier(
            context.work_item_identifier, "item_requester", listing_identifier
        )
        requested = await database.requested_work_identifiers(
            requester_kind=("carl", "facebook", "search_refresh", "item"),
            requester_identifier=requester_identifier,
        )
        if len(requested) > 1:
            raise RuntimeError("Search refresh requested an item page more than once")
        if requested:
            collection_identifiers.append(requested[0])
            continue
        if decision.latest_successful_result is not None:
            reused_results.append(decision.latest_successful_result)
            continue
        if headers is None:
            await _activate_page_policy(
                dependencies, payload.item_routing, context.operation_identifier
            )
            headers = await anyio.to_thread.run_sync(
                brave_navigation_headers, abandon_on_cancel=True
            )
        requested_identifier = _stable_identifier(
            context.work_item_identifier, "item", listing_identifier
        )
        existing = await _work_or_none(database, requested_identifier)
        requester = WorkRequester(
            request_identifier=dependencies.new_identifier(),
            kind=("carl", "facebook", "search_refresh", "item"),
            identifier=requester_identifier,
            context={
                "stage": "item",
                "listing_identifier": listing_identifier,
                "search_refresh_work_identifier": context.work_item_identifier,
                "base_search_run_record_identifier": payload.base_search_run_record_identifier,
                "refreshed_search_run_record_identifier": refreshed_identifier,
            },
        )
        if existing is None:
            enqueued, late_reuse = await database.enqueue_work_unless_facebook_item_page_is_usable(
                collect_item_work(
                    identifier=requested_identifier,
                    payload=CollectItemPayload(
                        listing_id=listing_identifier,
                        request_plan=RequestPlan(
                            url=(
                                f"https://www.facebook.com/marketplace/item/{listing_identifier}/"
                            ),
                            headers=headers,
                            routing=payload.item_routing,
                        ),
                    ),
                    not_before_utc_ns=0,
                ),
                requester,
                listing_identifier=listing_identifier,
                event_identifier=dependencies.new_identifier(),
                enqueued_at_utc_ns=dependencies.utc_now_ns(),
            )
            if late_reuse is not None:
                reused_results.append(late_reuse)
                continue
            if enqueued is None:
                raise AssertionError("Item-page enqueue returned no work or reusable result")
            requested_identifier = enqueued.work_item_identifier
        else:
            await database.attach_work_request(
                requested_identifier,
                requester,
                event_identifier=dependencies.new_identifier(),
                requested_at_utc_ns=dependencies.utc_now_ns(),
            )
        collection_identifiers.append(requested_identifier)
    collections = await _works(database, tuple(collection_identifiers))
    if _unfinished(collections):
        return RetryWork(
            delay_ns=_CHILD_WAIT_DELAY_NS,
            reason={"kind": "waiting_for_item_collections"},
            result={
                "stage": "collecting_items",
                **(
                    {
                        "item_retry_generation_identifier": checkpoint[
                            "item_retry_generation_identifier"
                        ],
                        "prior_image_collection_count": checkpoint.get(
                            "prior_image_collection_count", 0
                        ),
                    }
                    if isinstance(checkpoint.get("item_retry_generation_identifier"), str)
                    else {}
                ),
                "refreshed_search_run_record_identifier": refreshed_identifier,
                "reused_successful_item_pages": len(reused_results),
                "item_collection_work_identifiers": collection_identifiers,
            },
        )
    extraction_identifiers = tuple(
        extraction_identifier
        for collection in collections
        if isinstance(collection.get("result"), dict)
        and isinstance(
            extraction_identifier := collection["result"].get("extraction_work_identifier"),
            str,
        )
    )
    extractions = await _works(database, extraction_identifiers)
    if _unfinished(extractions):
        return RetryWork(
            delay_ns=_CHILD_WAIT_DELAY_NS,
            reason={"kind": "waiting_for_item_extractions"},
            result={
                "stage": "extracting_items",
                **(
                    {
                        "item_retry_generation_identifier": checkpoint[
                            "item_retry_generation_identifier"
                        ],
                        "prior_image_collection_count": checkpoint.get(
                            "prior_image_collection_count", 0
                        ),
                    }
                    if isinstance(checkpoint.get("item_retry_generation_identifier"), str)
                    else {}
                ),
                "refreshed_search_run_record_identifier": refreshed_identifier,
                "reused_successful_item_pages": len(reused_results),
                "item_collection_work_identifiers": collection_identifiers,
                "item_extraction_work_identifiers": list(extraction_identifiers),
            },
        )
    failed = sum(work.get("state") != WorkState.COMPLETED.value for work in collections)
    failed += sum(work.get("state") != WorkState.COMPLETED.value for work in extractions)
    return RetryWork(
        inputs=(
            NamedInput(
                name=("base_search_run",),
                object_identifier=payload.base_search_run_record_identifier,
            ),
            NamedInput(name=("refreshed_search_run",), object_identifier=refreshed_identifier),
            *(
                NamedInput(
                    name=("reused_item_page", f"{index:08d}"),
                    object_identifier=result.observation_record_identifier,
                )
                for index, result in enumerate(reused_results)
            ),
        ),
        delay_ns=0,
        reason={"kind": "continue_refresh", "next_stage": "images"},
        result={
            "stage": "items_complete",
            **(
                {
                    "item_retry_generation_identifier": checkpoint[
                        "item_retry_generation_identifier"
                    ],
                    "prior_image_collection_count": checkpoint.get(
                        "prior_image_collection_count", 0
                    ),
                }
                if isinstance(checkpoint.get("item_retry_generation_identifier"), str)
                else {}
            ),
            "refreshed_search_run_record_identifier": refreshed_identifier,
            "base_unique_listings": len(base_ids),
            "refreshed_unique_listings": len(refreshed_ids),
            "selected_unique_listings": len(listing_ids),
            "new_listing_identifiers": [
                identifier for identifier in refreshed_ids if identifier not in set(base_ids)
            ],
            "absent_from_refresh_listing_identifiers": list(
                identifier for identifier in base_ids if identifier not in set(refreshed_ids)
            ),
            "reused_successful_item_pages": len(reused_results),
            "item_collections": len(collections),
            "item_extractions": len(extractions),
            "item_failures": failed,
        },
    )


async def _image_phase(
    payload: RefreshSearchPayload,
    checkpoint: dict[str, JsonValue],
    context: AttemptContext,
    dependencies: RefreshWorkerDependencies,
) -> WorkOutcome:
    database = dependencies.database
    refreshed_identifier = checkpoint.get("refreshed_search_run_record_identifier")
    if not isinstance(refreshed_identifier, str):
        raise ValueError("Refresh checkpoint has no search-run record identifier")
    _, _, base = await database.get_record(payload.base_search_run_record_identifier)
    _, _, refreshed = await database.get_record(refreshed_identifier)
    listing_ids = tuple(
        dict.fromkeys((*_search_listing_identifiers(refreshed), *_search_listing_identifiers(base)))
    )[: payload.maximum_items]
    successful = await database.successful_facebook_item_page_results(listing_ids)
    latest_by_listing = {}
    for result in successful:
        latest_by_listing[result.listing_id] = result
    references: list[GalleryImageReference] = []
    for result in latest_by_listing.values():
        _, _, observation = await database.get_record(result.observation_record_identifier)
        references.extend(
            gallery_references(
                observation_identifier=result.observation_record_identifier,
                observation=observation,
            )
        )
    existing_references = await database.facebook_gallery_reference_identifiers()
    reference_identifiers = dict(existing_references)
    generation = checkpoint.get("item_retry_generation_identifier")
    plan_identifier = (
        _stable_identifier(context.work_item_identifier, "image_plan", generation)
        if isinstance(generation, str)
        else _stable_identifier(context.work_item_identifier, "image_plan")
    )
    plan_exists = True
    stored_selected_reference_identifiers: frozenset[str] = frozenset()
    try:
        plan_kind, _, stored_plan = await database.get_record(plan_identifier)
        if plan_kind != ("carl", "facebook", "image_followup_plan") or not isinstance(
            stored_plan, dict
        ):
            raise ValueError("Stored refresh image plan is malformed")
        raw_selected = stored_plan.get("selected_reference_record_identifiers")
        if not isinstance(raw_selected, list) or not all(
            isinstance(identifier, str) for identifier in raw_selected
        ):
            raise ValueError("Stored refresh image plan has no selected references")
        stored_selected_reference_identifiers = frozenset(raw_selected)
        raw_references = stored_plan.get("reference_record_identifiers")
        if not isinstance(raw_references, list) or not all(
            isinstance(identifier, str) for identifier in raw_references
        ):
            raise ValueError("Stored refresh image plan has no reference set")
        references_by_identifier = {
            identifier: reference for reference, identifier in existing_references.items()
        }
        if any(identifier not in references_by_identifier for identifier in raw_references):
            raise ValueError("Stored refresh image plan references missing evidence")
        references = [references_by_identifier[identifier] for identifier in raw_references]
    except KeyError:
        plan_exists = False
    new_reference_records: list[RecordDraft] = []
    if not plan_exists:
        for reference in references:
            if reference in reference_identifiers:
                continue
            identifier = _stable_identifier(
                context.work_item_identifier,
                "gallery_reference",
                reference.listing_observation_record_identifier,
                str(reference.gallery_order),
                reference.original_url,
            )
            reference_identifiers[reference] = identifier
            new_reference_records.append(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value={
                        **reference.model_dump(mode="json"),
                        "reference_extractor": {
                            "component_parts": list(EXTRACT_FACEBOOK_GALLERY_REFERENCES.parts),
                            "output_schema_version": 1,
                        },
                        "operation_identifier": context.operation_identifier,
                    },
                )
            )
    configured_maximum_images = payload.maximum_images
    if configured_maximum_images is not None:
        configured_maximum_images = max(
            0, configured_maximum_images - int(checkpoint.get("prior_image_collection_count", 0))
        )
    if configured_maximum_images is None:
        configured_maximum_images = len({reference.rendition_identity for reference in references})
    saved_before_resumption = await database.saved_facebook_image_renditions()
    pending_extractions = await database.pending_facebook_image_extractions()
    resumed_keys: set[tuple[str | None, str]] = set()
    resumed_identifiers: list[str] = []
    for reference in references:
        key = (reference.photo_id, reference.original_url)
        if (
            key in saved_before_resumption
            or key in resumed_keys
            or key not in pending_extractions
            or len(resumed_keys) >= configured_maximum_images
        ):
            continue
        resumed_keys.add(key)
        resumed_identifiers.extend(pending_extractions[key])
    resumed_results = await _works(database, tuple(resumed_identifiers))
    if _unfinished(resumed_results):
        return RetryWork(
            delay_ns=_CHILD_WAIT_DELAY_NS,
            reason={"kind": "waiting_for_resumed_image_extractions"},
            result={
                **checkpoint,
                "stage": "extracting_resumed_images",
                "resumed_image_extraction_work_identifiers": resumed_identifiers,
            },
        )
    saved_candidates = _saved_image_candidates(await database.saved_facebook_image_results())
    maximum_images = (
        len({reference.rendition_identity for reference in references})
        if plan_exists
        else max(0, configured_maximum_images - len(resumed_keys))
    )
    plan = plan_image_followups(
        tuple(references),
        saved_candidates,
        maximum_images,
        frozenset(resumed_keys),
    )
    source_reuses = tuple(
        decision
        for decision in plan.reuse_decisions
        if decision.match_kind is ImageReuseMatchKind.SOURCE_PHOTO_ADEQUATE_DIMENSIONS
    )
    exact_reuses = len(plan.reuse_decisions) - len(source_reuses)
    existing_reuses = {
        (
            resolution.gallery_image_reference_record_identifier,
            resolution.source_image_result_record_identifier,
            resolution.match_kind,
        )
        for resolution in await database.facebook_image_reuse_resolutions()
    }
    reuse_records: list[RecordDraft] = []
    reuse_inputs: list[tuple[tuple[str, ...], str]] = []
    for decision in source_reuses:
        reference_identifier = reference_identifiers[references[decision.reference_index]]
        key = (
            reference_identifier,
            decision.candidate.image_result_record_identifier,
            decision.match_kind,
        )
        if key in existing_reuses:
            continue
        record_identifier = _stable_identifier(
            context.work_item_identifier,
            "image_reuse",
            reference_identifier,
            decision.candidate.image_result_record_identifier,
        )
        reuse_records.append(
            RecordDraft(
                identifier=record_identifier,
                kind=("carl", "facebook", "image_reuse"),
                schema_version=1,
                value=image_reuse_record(decision.match_kind).model_dump(mode="json"),
            )
        )
        index = f"{len(reuse_records) - 1:08d}"
        reuse_inputs.extend(
            (
                (("gallery_image_reference", index), reference_identifier),
                (
                    ("source_image_result", index),
                    decision.candidate.image_result_record_identifier,
                ),
            )
        )
    unresolved = tuple(
        tuple((reference_identifiers[references[index]], references[index]) for index in group)
        for group in plan.download_groups
    )
    selected = (
        tuple(
            entries
            for entries in unresolved
            if any(identifier in stored_selected_reference_identifiers for identifier, _ in entries)
        )
        if plan_exists
        else unresolved
    )
    if not plan_exists:
        await database.publish_leased_operation_checkpoint(
            work_item_identifier=context.work_item_identifier,
            lease_token=context.lease_token,
            worker_identifier=context.worker_identifier,
            utc_now_ns=dependencies.utc_now_ns,
            operation_id=context.operation_identifier,
            records=(
                RecordDraft(
                    identifier=plan_identifier,
                    kind=("carl", "facebook", "image_followup_plan"),
                    schema_version=1,
                    value={
                        "search_run_record_identifiers": [
                            payload.base_search_run_record_identifier,
                            refreshed_identifier,
                        ],
                        "gallery_references": len(references),
                        "already_saved_exact_renditions": exact_reuses,
                        "reused_source_photo_images": len(source_reuses),
                        "selected_new_renditions": len(selected),
                        "resumed_extractions": len(resumed_results),
                        "reference_record_identifiers": [
                            reference_identifiers[reference] for reference in references
                        ],
                        "selected_reference_record_identifiers": [
                            identifier for entries in selected for identifier, _ in entries
                        ],
                        "operation_identifier": context.operation_identifier,
                    },
                ),
                *new_reference_records,
            ),
            artifacts=(),
            outputs=(
                NamedOutput(name=("image_followup_plan",), object_identifier=plan_identifier),
                *(
                    NamedOutput(
                        name=("gallery_image_reference", f"{index:08d}"),
                        object_identifier=record.identifier,
                    )
                    for index, record in enumerate(new_reference_records)
                ),
            ),
            checkpoint_result={
                **checkpoint,
                "stage": "collecting_images",
                "image_followup_plan_record_identifier": plan_identifier,
                "selected_new_renditions": len(selected),
            },
            inputs=(
                NamedInput(
                    name=("base_search_run",),
                    object_identifier=payload.base_search_run_record_identifier,
                ),
                NamedInput(
                    name=("refreshed_search_run",),
                    object_identifier=refreshed_identifier,
                ),
                *(
                    NamedInput(
                        name=("listing_observation", f"{index:08d}"),
                        object_identifier=result.observation_record_identifier,
                    )
                    for index, result in enumerate(latest_by_listing.values())
                ),
            ),
        )
    if reuse_records:
        reuse_operation_identifier = dependencies.new_identifier()
        reuse_started_utc_ns = dependencies.utc_now_ns()
        reuse_started_monotonic_ns = dependencies.monotonic_ns()
        await database.begin_operation(
            operation_id=reuse_operation_identifier,
            component=build_image_component_registry().require(REUSE_FACEBOOK_GALLERY_IMAGE),
            provenance=await dependencies.provenance(),
            invocation=process_invocation(),
            configuration={
                "selection_policy": {
                    "source_identity": "facebook_photo_id",
                    "minimum_dimensions": ("saved_width_and_height_at_least_declared_dimensions"),
                    "missing_source_or_declared_dimensions": "require_exact_signed_url",
                }
            },
            started_at_utc=_utc_text(reuse_started_utc_ns),
            inputs=tuple(reuse_inputs),
        )
        with anyio.CancelScope(shield=True):
            await database.complete_operation(
                operation_id=reuse_operation_identifier,
                records=tuple(reuse_records),
                artifacts=(),
                outputs=tuple(
                    NamedOutput(
                        name=("image_reuse", f"{index:08d}"),
                        object_identifier=record.identifier,
                    )
                    for index, record in enumerate(reuse_records)
                ),
                result={"recorded_source_asset_reuses": len(reuse_records)},
                ended_at_utc=_utc_text(dependencies.utc_now_ns()),
                duration_ns=max(0, dependencies.monotonic_ns() - reuse_started_monotonic_ns),
            )
    await database.supersede_constraints(
        retired_identifiers=legacy_image_network_constraint_identifiers(payload.image_routing),
        replacements=image_network_constraints(payload.image_routing),
        operation_identifier=context.operation_identifier,
        at_utc_ns=dependencies.utc_now_ns(),
        reason="Share Proton transport across bounded concurrent image requests",
    )
    collection_identifiers: list[str] = []
    for entries in selected:
        reference_identifier, reference = entries[0]
        requester_identifier = _stable_identifier(
            context.work_item_identifier,
            "image_requester",
            reference.photo_id or "",
            reference.original_url,
        )
        requested = await database.requested_work_identifiers(
            requester_kind=("carl", "facebook", "search_refresh", "image"),
            requester_identifier=requester_identifier,
        )
        if len(requested) > 1:
            raise RuntimeError("Search refresh requested an image rendition more than once")
        requested_identifier = (
            requested[0]
            if requested
            else _stable_identifier(
                context.work_item_identifier,
                "image",
                reference.photo_id or "",
                reference.original_url,
            )
        )
        existing = await _work_or_none(database, requested_identifier)
        if existing is None:
            definition = collect_image_work(
                identifier=requested_identifier,
                payload=CollectImagePayload(
                    reference_record_identifier=reference_identifier,
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
            enqueued = await database.enqueue_work(
                definition,
                WorkRequester(
                    request_identifier=dependencies.new_identifier(),
                    kind=("carl", "facebook", "search_refresh", "image"),
                    identifier=requester_identifier,
                    context={
                        "stage": "image",
                        "image_followup_plan_record_identifier": plan_identifier,
                        "search_refresh_work_identifier": context.work_item_identifier,
                    },
                ),
                event_identifier=dependencies.new_identifier(),
                enqueued_at_utc_ns=dependencies.utc_now_ns(),
            )
            collection_identifier = enqueued.work_item_identifier
            for gallery_reference_identifier, image_reference in entries:
                _ = await database.enqueue_work(
                    definition,
                    WorkRequester(
                        request_identifier=dependencies.new_identifier(),
                        kind=("carl", "facebook", "gallery_image_reference"),
                        identifier=gallery_reference_identifier,
                        context={
                            "listing_id": image_reference.listing_id,
                            "image_followup_plan_record_identifier": plan_identifier,
                            "search_refresh_work_identifier": context.work_item_identifier,
                        },
                    ),
                    event_identifier=dependencies.new_identifier(),
                    enqueued_at_utc_ns=dependencies.utc_now_ns(),
                )
            requested_identifier = collection_identifier
        collection_identifiers.append(requested_identifier)
    collections = await _works(database, tuple(collection_identifiers))
    if _unfinished(collections):
        return RetryWork(
            delay_ns=_CHILD_WAIT_DELAY_NS,
            reason={"kind": "waiting_for_image_collections"},
            result={
                **checkpoint,
                "stage": "collecting_images",
                "image_collection_work_identifiers": collection_identifiers,
            },
        )
    extraction_identifiers = tuple(
        extraction_identifier
        for collection in collections
        if isinstance(collection.get("result"), dict)
        and isinstance(
            extraction_identifier := collection["result"].get("extraction_work_identifier"),
            str,
        )
    )
    extraction_results = await _works(database, extraction_identifiers)
    if _unfinished(extraction_results):
        return RetryWork(
            delay_ns=_CHILD_WAIT_DELAY_NS,
            reason={"kind": "waiting_for_image_extractions"},
            result={
                **checkpoint,
                "stage": "extracting_images",
                "image_collection_work_identifiers": collection_identifiers,
                "image_extraction_work_identifiers": list(extraction_identifiers),
            },
        )
    saved = sum(
        work.get("state") == WorkState.COMPLETED.value
        and isinstance(work.get("result"), dict)
        and work["result"].get("state") == "saved"
        for work in collections
    )
    failed = len(collections) - saved
    failed += sum(work.get("state") != WorkState.COMPLETED.value for work in extraction_results)
    resumed_saved = sum(work.get("state") == WorkState.COMPLETED.value for work in resumed_results)
    resumed_failed = len(resumed_results) - resumed_saved
    item_failures_value = checkpoint.get("item_failures", 0)
    item_failures = (
        item_failures_value
        if isinstance(item_failures_value, int) and not isinstance(item_failures_value, bool)
        else 0
    )
    result_identifier = (
        _stable_identifier(context.work_item_identifier, "result", generation)
        if isinstance(generation, str)
        else _stable_identifier(context.work_item_identifier, "result")
    )
    result_value: dict[str, JsonValue] = {
        **checkpoint,
        "state": (
            "completed"
            if item_failures + failed + resumed_failed == 0
            else "completed_with_failures"
        ),
        "stage": "complete",
        "image_followup_plan_record_identifier": plan_identifier,
        "gallery_references": len(references),
        "already_saved_exact_renditions": exact_reuses,
        "reused_source_photo_images": len(source_reuses),
        "new_image_collections": len(collections),
        "new_images_saved": saved,
        "new_images_failed": failed,
        "resumed_extractions": len(resumed_results),
        "resumed_images_saved": resumed_saved,
        "resumed_images_failed": resumed_failed,
    }
    summary = refresh_failure_summary(result_value)
    result_value["failure_summary"] = summary.model_dump(mode="json")
    if summary.severity == "failed":
        result_value["state"] = "failed"
    return classify_refresh_completion(
        CompletedWork(
            inputs=(
                ()
                if not plan_exists
                else (
                    NamedInput(
                        name=("image_followup_plan",),
                        object_identifier=plan_identifier,
                    ),
                )
            ),
            records=(
                RecordDraft(
                    identifier=result_identifier,
                    kind=("carl", "facebook", "search_refresh"),
                    schema_version=1,
                    value=result_value,
                ),
            ),
            outputs=(NamedOutput(name=("search_refresh",), object_identifier=result_identifier),),
            result={**result_value, "search_refresh_record_identifier": result_identifier},
        )
    )


def build_refresh_worker_registry(
    dependencies: RefreshWorkerDependencies,
) -> WorkHandlerRegistry:
    component = build_refresh_component_registry().require(REFRESH_FACEBOOK_SEARCH)

    async def refresh(payload: RefreshSearchPayload, context: AttemptContext) -> WorkOutcome:
        current = await dependencies.database.work(context.work_item_identifier)
        checkpoint = current.get("result")
        if not isinstance(checkpoint, dict):
            return await _search_phase(payload, context, dependencies)
        stage = checkpoint.get("stage")
        refreshed_identifier = checkpoint.get("refreshed_search_run_record_identifier")
        if stage in {"collecting_search", "search_failed"}:
            return await _search_phase(payload, context, dependencies)
        if stage in {"search_complete", "collecting_items", "extracting_items"} and isinstance(
            refreshed_identifier, str
        ):
            return await _item_phase(
                payload,
                refreshed_identifier,
                context,
                dependencies,
                checkpoint=checkpoint,
            )
        if stage in {
            "items_complete",
            "extracting_resumed_images",
            "collecting_images",
            "extracting_images",
        }:
            return await _image_phase(
                payload,
                checkpoint,
                context,
                dependencies,
            )
        return TerminalFailureWork(
            error={"kind": "invalid_search_refresh_checkpoint"},
            result={"stage": "failed", "checkpoint": checkpoint},
        )

    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=REFRESH_SEARCH_WORK_KIND,
                    payload_schema_version=LEGACY_REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION,
                ),
                component=component,
                payload_type=RefreshSearchPayload,
                handler=refresh,
            ),
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=REFRESH_SEARCH_WORK_KIND,
                    payload_schema_version=REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION,
                ),
                component=component,
                payload_type=RefreshSearchPayload,
                handler=refresh,
            ),
        )
    )
