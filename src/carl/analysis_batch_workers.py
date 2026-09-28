"""Durable coordination of missing listing-analysis requests."""

from __future__ import annotations

from dataclasses import dataclass
from time import time_ns
from typing import Protocol

from carl.core.analysis_batch import (
    LEGACY_REQUEST_MISSING_ANALYSES_PAYLOAD_SCHEMA_VERSION,
    PREVIOUS_REQUEST_MISSING_ANALYSES_PAYLOAD_SCHEMA_VERSION,
    REQUEST_MISSING_ANALYSES_PAYLOAD_SCHEMA_VERSION,
    REQUEST_MISSING_ANALYSES_WORK_KIND,
    RequestMissingAnalysesPayload,
    legacy_request_missing_analyses_constraint_identifiers,
    request_missing_analyses_work_constraints,
)
from carl.core.components import Component, ComponentId, Registry
from carl.core.models import JsonValue, NamedInput
from carl.core.review import RequestAnalysisRequest, RequestAnalysisResult
from carl.core.work import WorkCapability, WorkState
from carl.core.worker import AttemptContext, CompletedWork, RetryWork, WorkOutcome
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry
from carl.review import ReviewInputError

REQUEST_MISSING_LISTING_ANALYSES = ComponentId(
    ("carl", "facebook", "request", "missing_listing_analyses")
)
_BATCH_SIZE = 1
_WAIT_DELAY_NS = 5_000_000_000


def _component_anchor() -> None:
    """Identity anchor for missing-analysis coordination."""


def build_analysis_batch_component_registry() -> Registry:
    return Registry((Component(REQUEST_MISSING_LISTING_ANALYSES, 1, _component_anchor),))


@dataclass(frozen=True, slots=True)
class AnalysisBatchWorkerDependencies:
    database: Database
    application: AnalysisRequestApplication


class AnalysisRequestApplication(Protocol):
    async def request_listing_analysis(
        self,
        request: RequestAnalysisRequest,
        *,
        requester_kind: tuple[str, ...],
        requester_identifier: str | None,
        requester_context: JsonValue | None,
    ) -> RequestAnalysisResult: ...


def _integer(value: JsonValue, default: int = 0) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def _strings(value: JsonValue) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return []
    return list(value)


def _skips(value: JsonValue) -> list[dict[str, JsonValue]]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        return []
    return list(value)


async def _request_missing_analyses(
    payload: RequestMissingAnalysesPayload,
    context: AttemptContext,
    dependencies: AnalysisBatchWorkerDependencies,
) -> WorkOutcome:
    await dependencies.database.supersede_constraints(
        retired_identifiers=legacy_request_missing_analyses_constraint_identifiers(),
        replacements=request_missing_analyses_work_constraints(),
        operation_identifier=context.operation_identifier,
        at_utc_ns=time_ns(),
        reason="Allow independent analysis batches to plan concurrently",
    )
    current = await dependencies.database.work(context.work_item_identifier)
    stored = current.get("result")
    checkpoint = stored if isinstance(stored, dict) else {}
    next_index = min(
        _integer(checkpoint.get("next_observation_index")),
        len(payload.listing_observation_record_identifiers),
    )
    created_count = _integer(checkpoint.get("newly_created_work"))
    reused_count = _integer(checkpoint.get("reused_work"))
    child_identifiers = _strings(checkpoint.get("analysis_work_identifiers"))
    skipped = _skips(checkpoint.get("skipped"))
    selected = payload.listing_observation_record_identifiers
    stop = min(len(selected), next_index + _BATCH_SIZE)
    processed_inputs: list[NamedInput] = []
    for index in range(next_index, stop):
        observation_identifier = selected[index]
        processed_inputs.append(
            NamedInput(
                name=("listing_observation", f"{index:08d}"),
                object_identifier=observation_identifier,
            )
        )
        try:
            requested = await dependencies.application.request_listing_analysis(
                RequestAnalysisRequest(
                    listing_observation_record_identifier=observation_identifier,
                    product_guide_record_identifier=payload.product_guide_record_identifier,
                    allow_incomplete_gallery=payload.allow_incomplete_gallery,
                ),
                requester_kind=("carl", "facebook", "analysis_batch"),
                requester_identifier=context.work_item_identifier,
                requester_context={
                    "source_search_refresh_work_identifier": (
                        payload.source_search_refresh_work_identifier
                    ),
                    "source_search_refresh_work_identifiers": list(
                        payload.source_search_refresh_work_identifiers
                    ),
                    "listing_observation_record_identifier": observation_identifier,
                },
            )
        except ReviewInputError as error:
            skipped.append(
                {
                    "listing_observation_record_identifier": observation_identifier,
                    "kind": type(error).__name__,
                    "message": str(error),
                }
            )
            continue
        if requested.work_identifier not in child_identifiers:
            child_identifiers.append(requested.work_identifier)
        if requested.created:
            created_count += 1
        else:
            reused_count += 1
    next_index = stop
    result: dict[str, JsonValue] = {
        "stage": "requesting" if next_index < len(selected) else "waiting_for_analyses",
        "selected_observations": len(selected),
        "next_observation_index": next_index,
        "newly_created_work": created_count,
        "reused_work": reused_count,
        "analysis_work_identifiers": child_identifiers,
        "skipped": skipped,
    }
    inputs = (
        NamedInput(
            name=("product_guide",),
            object_identifier=payload.product_guide_record_identifier,
        ),
        *processed_inputs,
    )
    if next_index < len(selected):
        return RetryWork(
            inputs=inputs,
            delay_ns=0,
            reason={"kind": "analysis_batch_continuation"},
            result=result,
        )
    states = await dependencies.database.work_states(tuple(child_identifiers))
    if any(state in {WorkState.PENDING, WorkState.LEASED} for state in states):
        return RetryWork(
            inputs=inputs,
            delay_ns=_WAIT_DELAY_NS,
            reason={"kind": "waiting_for_listing_analyses"},
            result=result,
        )
    return CompletedWork(
        inputs=inputs,
        result={**result, "stage": "completed"},
    )


def build_analysis_batch_worker_registry(
    dependencies: AnalysisBatchWorkerDependencies,
) -> WorkHandlerRegistry:
    component = build_analysis_batch_component_registry().require(REQUEST_MISSING_LISTING_ANALYSES)
    return WorkHandlerRegistry(
        handlers=tuple(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=REQUEST_MISSING_ANALYSES_WORK_KIND,
                    payload_schema_version=schema_version,
                ),
                component=component,
                payload_type=RequestMissingAnalysesPayload,
                handler=lambda payload, context: _request_missing_analyses(
                    payload, context, dependencies
                ),
            )
            for schema_version in (
                LEGACY_REQUEST_MISSING_ANALYSES_PAYLOAD_SCHEMA_VERSION,
                PREVIOUS_REQUEST_MISSING_ANALYSES_PAYLOAD_SCHEMA_VERSION,
                REQUEST_MISSING_ANALYSES_PAYLOAD_SCHEMA_VERSION,
            )
        )
    )
