"""Incrementally consume exact search evidence through durable listing pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from time import time_ns
from typing import cast
from uuid import NAMESPACE_URL, uuid4, uuid5

from carl.core.analysis_batch import ListingAnalysisSelectionPolicy
from carl.core.json import encode_json
from carl.core.marketplace_search import Marketplace
from carl.core.models import JsonValue, NamedInput
from carl.core.pipeline import (
    RUN_SEARCH_PIPELINE,
    SEARCH_PIPELINE_WORK_KIND,
    PipelineListingPayload,
    PipelineStage,
    RequestSearchPipelinePayload,
    build_pipeline_component_registry,
    listing_pipeline_work,
    pipeline_collection_failed,
)
from carl.core.work import WorkCapability, WorkRequester
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry

_WAIT_NS = 2_000_000_000
_PAGE_SIZE = 50
_LISTING_REQUEST_KIND = ("carl", "marketplace", "pipeline_listing")


@dataclass(frozen=True, slots=True)
class PipelineWorkerDependencies:
    database: Database


def _mapping(value: JsonValue | None) -> dict[str, JsonValue]:
    return cast(dict[str, JsonValue], value) if isinstance(value, dict) else {}


def _integer(value: JsonValue | None) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _entries(value: JsonValue | None) -> list[dict[str, JsonValue]]:
    return [_mapping(item) for item in value] if isinstance(value, list) else []


async def _run_pipeline(
    payload: RequestSearchPipelinePayload,
    context: AttemptContext,
    dependencies: PipelineWorkerDependencies,
) -> WorkOutcome:
    database = dependencies.database
    checkpoint = _mapping((await database.work(context.work_item_identifier)).get("result"))
    entries = _entries(checkpoint.get("listings"))
    child_ids = tuple(str(entry["work_identifier"]) for entry in entries)
    children = await database.pipeline_work_rows(child_ids)
    active_count = sum(child["state"] in {"pending", "leased"} for child in children)
    selected = {(entry["marketplace"], entry["external_identifier"]) for entry in entries}
    cursors = _mapping(checkpoint.get("source_cursors"))
    sources = tuple(("work", identifier) for identifier in payload.search_work_identifiers) + tuple(
        ("run", identifier) for identifier in payload.search_run_record_identifiers
    )
    source_index = _integer(checkpoint.get("next_source_index")) % len(sources)
    source_kind, source_identifier = sources[source_index]
    source_key = f"{source_kind}:{source_identifier}"
    cursor = _integer(cursors.get(source_key))
    examined = _integer(checkpoint.get("candidates_examined"))
    image_reserved = _integer(checkpoint.get("image_budget_reserved"))
    analysis_reserved = _integer(checkpoint.get("analysis_budget_reserved"))
    budget_exhausted = checkpoint.get("budget_exhausted") is True
    options = payload.options
    remaining = options.maximum_candidate_listings_examined - examined
    discovery_stopped = len(entries) >= options.maximum_items or remaining <= 0
    # Coordinators never hold a worker while awaiting network/AI dependencies.
    # A bounded outstanding window keeps discovery from flooding the queue.
    if not discovery_stopped and active_count < options.maximum_inflight_listings:
        occurrences = await database.pipeline_search_occurrences(
            search_work_identifiers=(source_identifier,) if source_kind == "work" else (),
            search_run_record_identifiers=(source_identifier,) if source_kind == "run" else (),
            after_object_rowid=cursor,
            limit=min(_PAGE_SIZE, remaining),
        )
        for occurrence in occurrences:
            if (
                active_count >= options.maximum_inflight_listings
                or len(entries) >= options.maximum_items
            ):
                break
            cursor = _integer(occurrence.get("object_rowid"))
            examined += 1
            marketplace = Marketplace(str(occurrence["marketplace"]))
            external_identifier = str(occurrence["external_identifier"])
            identity = (marketplace.value, external_identifier)
            if identity in selected:
                continue
            title = str(occurrence.get("title") or "").casefold()
            if (
                options.title_contains is not None
                and options.title_contains.casefold() not in title
            ):
                continue
            if any(keyword.casefold() in title for keyword in options.exclude_title_keywords):
                continue
            card_status = occurrence.get("listing_status")
            if isinstance(card_status, str) and card_status not in {
                status.value for status in options.statuses
            }:
                continue
            maximum_images = (
                0
                if options.stop_after is PipelineStage.DETAILS
                else min(
                    options.maximum_images_per_listing, options.maximum_images - image_reserved
                )
            )
            if (
                options.stop_after is not PipelineStage.DETAILS
                and maximum_images < options.maximum_images_per_listing
            ):
                budget_exhausted = True
            history_reused = False
            if (
                options.stop_after is PipelineStage.ANALYSIS
                and options.selection_policy
                is not ListingAnalysisSelectionPolicy.MISSING_FOR_CURRENT_EVIDENCE
            ):
                history_reused = await database.pipeline_completed_analysis_exists(
                    marketplace=marketplace.value,
                    external_identifier=external_identifier,
                    product_guide_record_identifier=options.product_guide_record_identifier
                    if options.selection_policy
                    is ListingAnalysisSelectionPolicy.MISSING_FOR_SELECTED_GUIDE
                    else None,
                )
            analysis_authorized = (
                options.stop_after is PipelineStage.ANALYSIS
                and analysis_reserved < options.maximum_analyses
                and not history_reused
            )
            owner = str(
                uuid5(
                    NAMESPACE_URL,
                    f"carl:pipeline:{context.work_item_identifier}:{marketplace.value}:{external_identifier}",
                )
            )
            # The requester edge, not an in-memory callback, closes the crash
            # window between enqueue and checkpoint publication. This also
            # recovers completed children, which queue dedup alone cannot do.
            async with database.transaction():
                requested = await database.requested_work_identifiers(
                    requester_kind=_LISTING_REQUEST_KIND, requester_identifier=owner
                )
                if len(requested) > 1:
                    raise RuntimeError("Pipeline listing has duplicate durable children")
                if requested:
                    child_id = requested[0]
                    existing = await database.work(child_id)
                    child_payload = PipelineListingPayload.model_validate_json(
                        encode_json(existing["payload"])
                    )
                    maximum_images = child_payload.maximum_images
                    analysis_authorized = child_payload.analysis_authorized
                else:
                    definition = listing_pipeline_work(
                        identifier=owner,
                        payload=PipelineListingPayload(
                            root_work_identifier=context.work_item_identifier,
                            marketplace=marketplace,
                            external_identifier=external_identifier,
                            occurrence_record_identifier=str(
                                occurrence["occurrence_record_identifier"]
                            ),
                            maximum_images=maximum_images,
                            analysis_authorized=analysis_authorized,
                            options=options,
                        ),
                    )
                    enqueued = await database.enqueue_work(
                        definition,
                        WorkRequester(
                            request_identifier=str(uuid4()),
                            kind=_LISTING_REQUEST_KIND,
                            identifier=owner,
                            context={"root_work_identifier": context.work_item_identifier},
                        ),
                        event_identifier=str(uuid4()),
                        enqueued_at_utc_ns=time_ns(),
                    )
                    child_id = enqueued.work_item_identifier
            entries.append(
                {
                    "marketplace": marketplace.value,
                    "external_identifier": external_identifier,
                    "work_identifier": child_id,
                }
            )
            selected.add(identity)
            image_reserved += maximum_images
            analysis_reserved += int(analysis_authorized)
            active_count += 1
            if (
                options.stop_after is PipelineStage.ANALYSIS
                and not analysis_authorized
                and not history_reused
            ):
                budget_exhausted = True
        discovery_stopped = (
            len(entries) >= options.maximum_items
            or examined >= options.maximum_candidate_listings_examined
        )
        if discovery_stopped:
            # We deliberately stop scanning when a finite caller bound is met.
            # Do not present a bounded subset as exhaustive processing.
            budget_exhausted = True
    cursors[source_key] = cursor
    searches = await database.pipeline_work_rows(payload.search_work_identifiers)
    searches_pending = sum(work["state"] in {"pending", "leased"} for work in searches)
    search_failures = sum(
        work["state"] == "terminal_failure"
        or (work["state"] == "completed" and pipeline_collection_failed(work.get("result")))
        for work in searches
    )
    for identifier in payload.search_run_record_identifiers:
        _, _, value = await database.get_record(identifier)
        search_failures += int(pipeline_collection_failed(value))
    children = await database.pipeline_work_rows(
        tuple(str(entry["work_identifier"]) for entry in entries)
    )
    child_pending = sum(work["state"] in {"pending", "leased"} for work in children)
    child_failures = sum(work["state"] == "terminal_failure" for work in children)
    partial_failures = sum(
        _mapping(work.get("result")).get("state") == "completed_with_failures"
        for work in children
        if work["state"] == "completed"
    )
    result: dict[str, JsonValue] = {
        "stage": "processing",
        "listings": cast(list[JsonValue], entries),
        "source_cursors": cursors,
        "next_source_index": (source_index + 1) % len(sources),
        "candidates_examined": examined,
        "image_budget_reserved": image_reserved,
        "analysis_budget_reserved": analysis_reserved,
        "budget_exhausted": budget_exhausted,
        "search_failures": search_failures,
        "listing_failures": child_failures,
        "partial_listing_failures": partial_failures,
        "selected_listings": len(entries),
    }
    inputs = (
        NamedInput(name=("pipeline_intent",), object_identifier=payload.intent_record_identifier),
    )
    # At a terminal search boundary one final scan is needed even if the first
    # scan raced the last page publication. Existing fixed runs have no waiter.
    unseen = False
    if not discovery_stopped and not searches_pending:
        for kind, identifier in sources:
            rows = await database.pipeline_search_occurrences(
                search_work_identifiers=(identifier,) if kind == "work" else (),
                search_run_record_identifiers=(identifier,) if kind == "run" else (),
                after_object_rowid=_integer(cursors.get(f"{kind}:{identifier}")),
                limit=1,
            )
            if rows:
                unseen = True
                break
    if searches_pending or child_pending or unseen:
        return RetryWork(
            inputs=inputs,
            delay_ns=_WAIT_NS,
            reason={"kind": "pipeline_continuation"},
            result=result,
        )
    if search_failures or child_failures:
        return TerminalFailureWork(
            inputs=inputs,
            error={
                "kind": "pipeline_completed_with_failures",
                "search_failures": search_failures,
                "listing_failures": child_failures,
            },
            result={**result, "stage": "complete", "state": "completed_with_failures"},
        )
    return CompletedWork(
        inputs=inputs,
        result={
            **result,
            "stage": "complete",
            "state": "completed_with_failures" if partial_failures else "completed",
        },
    )


def build_pipeline_worker_registry(dependencies: PipelineWorkerDependencies) -> WorkHandlerRegistry:
    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(kind=SEARCH_PIPELINE_WORK_KIND, payload_schema_version=1),
                component=build_pipeline_component_registry().require(RUN_SEARCH_PIPELINE),
                payload_type=RequestSearchPipelinePayload,
                handler=lambda payload, context: _run_pipeline(payload, context, dependencies),
            ),
        )
    )
