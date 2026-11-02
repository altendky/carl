"""Admit legacy coordinator phases without retaining capacity across their waits."""

from collections.abc import Callable
from time import time_ns
from uuid import uuid4

from pydantic import Field

from carl.core.analysis_batch import REQUEST_MISSING_ANALYSES_WORK_KIND
from carl.core.ebay_refresh import REFRESH_EBAY_SEARCH_WORK_KIND
from carl.core.facebook_listing import REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND
from carl.core.facebook_refresh import REFRESH_SEARCH_WORK_KIND
from carl.core.models import JsonValue, StrictModel
from carl.core.pipeline import LISTING_PIPELINE_WORK_KIND, SEARCH_PIPELINE_WORK_KIND
from carl.core.resources import ResourceRequest
from carl.core.stages import (
    StageAttemptContext,
    StageDefinition,
    StageOutcome,
    StageWaitKind,
    WaitWork,
)
from carl.core.work import WorkDefinition
from carl.core.work_progress import ProgressWait
from carl.core.worker import RetryWork
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler, WorkStageAdapter
from carl.io.worker import WorkHandlerRegistry

COORDINATOR_SELECT_STAGE_KIND = ("carl", "coordinator", "select")
COORDINATOR_CPU_STAGE_KIND = ("carl", "coordinator", "cpu")
_CPU = (ResourceRequest(name="cpu"),)
_LOCAL_REASONS = frozenset(
    {"continue_refresh", "continue_listing_details", "analysis_batch_continuation"}
)

_CONTROL_REASONS: dict[tuple[str, ...], frozenset[str]] = {
    REFRESH_SEARCH_WORK_KIND: frozenset(
        {
            "waiting_for_search_collection",
            "resumed_search_collection_after_transient_failure",
            "continue_refresh",
            "waiting_for_item_collections",
            "waiting_for_item_extractions",
            "waiting_for_resumed_image_extractions",
            "waiting_for_image_collections",
            "waiting_for_image_extractions",
        }
    ),
    REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND: frozenset(
        {
            "continue_listing_details",
            "waiting_for_item_collection",
            "waiting_for_item_extraction",
            "waiting_for_images",
            "waiting_for_image_extractions",
        }
    ),
    REFRESH_EBAY_SEARCH_WORK_KIND: frozenset(
        {
            "waiting_for_search_collection",
            "continue_refresh",
            "waiting_for_item_collections",
            "waiting_for_item_extractions",
            "waiting_for_item_descriptions",
            "waiting_for_image_collections",
        }
    ),
    REQUEST_MISSING_ANALYSES_WORK_KIND: frozenset(
        {
            "analysis_batch_continuation",
            "waiting_for_listing_analyses",
        }
    ),
    SEARCH_PIPELINE_WORK_KIND: frozenset({"pipeline_continuation"}),
    LISTING_PIPELINE_WORK_KIND: frozenset({"listing_pipeline_continuation"}),
}


class CoordinatorWorkReference(StrictModel):
    """Keep large retained coordinator inputs out of the bounded continuation."""

    work_item_identifier: str = Field(min_length=1)
    work_kind: tuple[str, ...] = Field(min_length=1)
    payload_schema_version: int = Field(ge=1)


def _new_identifier() -> str:
    return str(uuid4())


def _requires_cpu(work: WorkDefinition, result: JsonValue) -> bool:
    if work.kind in {REQUEST_MISSING_ANALYSES_WORK_KIND, LISTING_PIPELINE_WORK_KIND}:
        # Their request-analysis APIs also verify retained product-guide text.
        return True
    checkpoint = result if isinstance(result, dict) else {}
    stage = checkpoint.get("stage")
    if work.kind == REFRESH_SEARCH_WORK_KIND:
        if stage in {
            "items_complete",
            "extracting_resumed_images",
            "collecting_images",
            "extracting_images",
        }:
            return True
        if stage in {"search_complete", "collecting_items", "extracting_items"}:
            maximum = work.payload.get("maximum_images") if isinstance(work.payload, dict) else None
            prior = checkpoint.get("prior_image_collection_count", 0)
            # Item planning checks expired gallery bytes while image budget remains.
            return not (
                isinstance(maximum, int)
                and not isinstance(maximum, bool)
                and isinstance(prior, int)
                and not isinstance(prior, bool)
                and maximum <= prior
            )
    elif work.kind == REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND:
        # Existing plans still validate shared reuses when their children finish.
        return stage in {
            "items_complete",
            "images_planned",
            "collecting_images",
            "extracting_images",
        }
    elif work.kind == REFRESH_EBAY_SEARCH_WORK_KIND:
        return stage in {"items_complete", "collecting_images"}
    return False


def build_coordinator_stage_registry(
    database: Database,
    legacy: WorkHandlerRegistry,
    *,
    utc_now_ns: Callable[[], int] = time_ns,
    new_identifier: Callable[[], str] = _new_identifier,
) -> StageHandlerRegistry:
    """Adapt only the six coordinator families from a supplied legacy registry.

    Image phases retain one admitted CPU unit through their existing serial checks
    and DB bookkeeping. Their input history can still determine a long CPU turn;
    this adapter bounds waiting tasks and reservations, not phase duration or RSS.
    """

    handlers = {
        (handler.capability.kind, handler.capability.payload_schema_version): handler
        for handler in legacy.handlers
        if handler.capability.kind in _CONTROL_REASONS
    }

    def initial(work: WorkDefinition, identifier: str, operation: str) -> StageDefinition:
        return StageDefinition(
            identifier=identifier,
            work_item_identifier=work.identifier,
            operation_identifier=operation,
            kind=COORDINATOR_SELECT_STAGE_KIND,
            payload=CoordinatorWorkReference(
                work_item_identifier=work.identifier,
                work_kind=work.kind,
                payload_schema_version=work.payload_schema_version,
            ).model_dump(mode="json"),
            not_before_utc_ns=work.not_before_utc_ns,
        )

    def continuation(
        reference: CoordinatorWorkReference,
        context: StageAttemptContext,
        *,
        cpu: bool,
        not_before_utc_ns: int,
    ) -> StageDefinition:
        return StageDefinition(
            identifier=new_identifier(),
            work_item_identifier=context.work_item_identifier,
            operation_identifier=context.operation_identifier,
            kind=COORDINATOR_CPU_STAGE_KIND if cpu else COORDINATOR_SELECT_STAGE_KIND,
            payload=reference.model_dump(mode="json"),
            resources=_CPU if cpu else (),
            not_before_utc_ns=not_before_utc_ns,
        )

    async def run(
        reference: CoordinatorWorkReference, context: StageAttemptContext, *, cpu: bool
    ) -> StageOutcome:
        if reference.work_item_identifier != context.work_item_identifier:
            raise ValueError("Coordinator reference does not match its logical work")
        work = await database.work_definition(context.work_item_identifier)
        if (work.kind, work.payload_schema_version) != (
            reference.work_kind,
            reference.payload_schema_version,
        ):
            raise ValueError("Coordinator reference capability changed")
        handler = handlers[(work.kind, work.payload_schema_version)]
        if cpu:
            definition = await database.stage(context.stage_identifier)
            if (
                definition.work_item_identifier != context.work_item_identifier
                or definition.operation_identifier != context.operation_identifier
                or definition.resources != _CPU
                or definition.kind != COORDINATOR_CPU_STAGE_KIND
            ):
                raise ValueError("Coordinator CPU phase requires its admitted CPU stage")
        else:
            (progress,) = await database.work_progress((work.identifier,))
            if _requires_cpu(work, progress["result"]):
                return WaitWork(
                    continuation=continuation(
                        reference, context, cpu=True, not_before_utc_ns=utc_now_ns()
                    ),
                    wait_kind=StageWaitKind.RESOURCE,
                    result=progress["result"],
                )
        observed_through_revision = await database.work_progress_watermark()
        outcome = await handler.execute(work.payload, context)
        reason = outcome.reason if isinstance(outcome, RetryWork) else None
        reason_kind = reason.get("kind") if isinstance(reason, dict) else None
        if (
            isinstance(outcome, RetryWork)
            and isinstance(reason_kind, str)
            and reason_kind in _CONTROL_REASONS[work.kind]
        ):
            progress_wait = None
            if outcome.progress_watch is not None and not outcome.progress_watch.empty:
                baseline = outcome.progress_observed_through_revision
                progress_wait = ProgressWait(
                    observed_through_revision=min(observed_through_revision, baseline)
                    if baseline is not None
                    else observed_through_revision,
                    watch=outcome.progress_watch,
                )
            immediate = (
                progress_wait is not None or reason_kind in _LOCAL_REASONS or outcome.delay_ns == 0
            )
            return WaitWork(
                continuation=continuation(
                    reference,
                    context,
                    cpu=False,
                    not_before_utc_ns=utc_now_ns() + (0 if immediate else outcome.delay_ns),
                ),
                wait_kind=StageWaitKind.DEPENDENCY
                if progress_wait is not None
                else StageWaitKind.TIME,
                progress_wait=progress_wait,
                inputs=outcome.inputs,
                records=outcome.records,
                artifacts=outcome.artifacts,
                outputs=outcome.outputs,
                result=outcome.result,
            )
        return outcome

    return StageHandlerRegistry(
        adapters=tuple(
            WorkStageAdapter(handler.capability, handler.component, initial, initial_resources=())
            for handler in handlers.values()
        ),
        handlers=(
            TypedStageHandler(
                kind=COORDINATOR_SELECT_STAGE_KIND,
                schema_version=1,
                payload_type=CoordinatorWorkReference,
                handler=lambda reference, context: run(reference, context, cpu=False),
            ),
            TypedStageHandler(
                kind=COORDINATOR_CPU_STAGE_KIND,
                schema_version=1,
                payload_type=CoordinatorWorkReference,
                handler=lambda reference, context: run(reference, context, cpu=True),
            ),
        ),
    )
