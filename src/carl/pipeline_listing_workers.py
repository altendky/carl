"""Advance each listing's durable evidence and analysis independently."""

from collections.abc import Callable
from dataclasses import dataclass
from time import time_ns
from typing import Protocol, cast

from carl.core.composed_projection import ListingStatus, normalize_listing_status
from carl.core.marketplace_listing import RequestListingDetailsRequest, RequestListingDetailsResult
from carl.core.marketplace_search import Marketplace
from carl.core.models import JsonValue, NamedInput
from carl.core.network_defaults import DEFAULT_DATACENTER_NETWORK_PATH
from carl.core.pipeline import (
    LISTING_PIPELINE_WORK_KIND,
    RUN_LISTING_PIPELINE,
    PipelineListingPayload,
    PipelineOptions,
    PipelineStage,
    build_pipeline_component_registry,
)
from carl.core.review import RequestAnalysisRequest, RequestAnalysisResult
from carl.core.review_errors import IncompleteGalleryError, ReviewInputError
from carl.core.work import WorkCapability
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry

_DETAIL_REQUESTER = ("carl", "marketplace", "pipeline_details")
_ANALYSIS_REQUESTER = ("carl", "marketplace", "pipeline_analysis")
_WAIT_NS = 5_000_000_000


class ListingPipelineApplication(Protocol):
    async def request_listing_details(
        self,
        request: RequestListingDetailsRequest,
        *,
        requester_kind: tuple[str, ...],
        requester_identifier: str | None,
    ) -> RequestListingDetailsResult: ...

    async def request_listing_analysis(
        self,
        request: RequestAnalysisRequest,
        *,
        requester_kind: tuple[str, ...],
        requester_identifier: str | None,
        requester_context: JsonValue | None,
    ) -> RequestAnalysisResult: ...

    async def pipeline_analysis_reuse(
        self, observation_identifier: str, options: PipelineOptions
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class PipelineListingWorkerDependencies:
    database: Database
    application: ListingPipelineApplication
    utc_now_ns: Callable[[], int] = time_ns


def _mapping(value: JsonValue | None) -> dict[str, JsonValue]:
    return cast(dict[str, JsonValue], value) if isinstance(value, dict) else {}


def _strings(value: JsonValue | None) -> list[str]:
    return (
        [item for item in cast(list[JsonValue], value) if isinstance(item, str)]
        if isinstance(value, list)
        else []
    )


def _pending(work: dict[str, JsonValue]) -> bool:
    return work.get("state") in {"pending", "leased"}


def _retry(result: dict[str, JsonValue], stage: str, *, delay_ns: int | None = None) -> RetryWork:
    observation = result.get("observation_record_identifier")
    return RetryWork(
        inputs=(NamedInput(name=("listing_observation",), object_identifier=observation),)
        if isinstance(observation, str)
        else (),
        delay_ns=_WAIT_NS if delay_ns is None else delay_ns,
        reason={"kind": "listing_pipeline_continuation", "stage": stage},
        result={**result, "stage": stage},
    )


def _failed(result: dict[str, JsonValue], kind: str) -> TerminalFailureWork:
    return TerminalFailureWork(
        error={"kind": kind}, result={**result, "stage": "failed", "reason": {"kind": kind}}
    )


def _skipped(result: dict[str, JsonValue], kind: str) -> CompletedWork:
    return CompletedWork(
        result={
            **result,
            "stage": "skipped",
            "state": "completed_with_failures" if result.get("evidence_failures") else "completed",
            "reason": {"kind": kind},
        }
    )


async def _requested(database: Database, kind: tuple[str, ...], owner: str) -> str | None:
    identifiers = await database.requested_work_identifiers(
        requester_kind=kind, requester_identifier=owner
    )
    if len(identifiers) > 1:
        raise RuntimeError("Listing pipeline has duplicate owned work")
    return identifiers[0] if identifiers else None


def _status(marketplace: Marketplace, observation: dict[str, JsonValue]) -> ListingStatus:
    if marketplace is Marketplace.EBAY:
        return (
            ListingStatus.UNAVAILABLE
            if observation.get("classification") == "unavailable"
            else ListingStatus.AVAILABLE
        )

    def flag(name: str) -> bool | None:
        field = _mapping(_mapping(observation.get("fields")).get(name))
        if field.get("state") != "present":
            return None
        evidence = field.get("evidence")
        values = (
            [
                _mapping(item).get("normalized", _mapping(item).get("original"))
                for item in cast(list[JsonValue], evidence)
                if _mapping(item).get("state") == "present"
            ]
            if isinstance(evidence, list)
            else []
        )
        if values and isinstance(values[0], bool) and all(value == values[0] for value in values):
            return values[0]
        return None

    classification = _mapping(observation.get("response_classification")).get("kind")
    return (
        normalize_listing_status(
            response_classification=classification if isinstance(classification, str) else None,
            is_sold=flag("availability_sold"),
            is_pending=flag("availability_pending"),
            is_live=flag("availability_live"),
        )
        or ListingStatus.UNKNOWN
    )


async def _run_listing_pipeline(
    payload: PipelineListingPayload,
    context: AttemptContext,
    dependencies: PipelineListingWorkerDependencies,
) -> WorkOutcome:
    database = dependencies.database
    checkpoint = _mapping((await database.work(context.work_item_identifier)).get("result"))
    result: dict[str, JsonValue] = {
        "item_completed": False,
        "images_completed": False,
        "analysis_reused": False,
        "analysis_completed": False,
        **checkpoint,
    }
    options = payload.options
    async with database.transaction():
        details_identifier = await _requested(
            database, _DETAIL_REQUESTER, context.work_item_identifier
        )
        if details_identifier is None:
            details = await dependencies.application.request_listing_details(
                RequestListingDetailsRequest(
                    marketplace=payload.marketplace,
                    external_identifier=payload.external_identifier,
                    maximum_images=0
                    if options.stop_after is PipelineStage.DETAILS
                    else payload.maximum_images,
                    refresh=options.refresh_details,
                    stack_identifier=options.ebay_stack_identifier
                    if payload.marketplace is Marketplace.EBAY
                    else "ebay_anonymous",
                    decodo_route=options.facebook_decodo_route
                    if payload.marketplace is Marketplace.FACEBOOK
                    else "carl",
                    image_network_path=options.requested_facebook_image_network_path
                    if payload.marketplace is Marketplace.FACEBOOK
                    else DEFAULT_DATACENTER_NETWORK_PATH,
                ),
                requester_kind=_DETAIL_REQUESTER,
                requester_identifier=context.work_item_identifier,
            )
            details_identifier = details.work_identifier
    result["detail_work_identifier"] = details_identifier
    detail_work = await database.work(details_identifier)
    detail_result = _mapping(detail_work.get("result"))
    if _pending(detail_work):
        retained_observation = detail_result.get("observation_record_identifier")
        if isinstance(retained_observation, str):
            # Facebook's details coordinator publishes its pin before its gallery settles.
            result.update(
                observation_record_identifier=retained_observation,
                item_completed=True,
                image_work_identifiers=_strings(detail_result.get("image_work_identifiers")),
            )
            return _retry(result, "collecting_evidence")
        return _retry(result, "collecting_details")
    if detail_work.get("state") != "completed":
        return _failed(result, "listing_pipeline_detail_failed")
    extraction_identifier = detail_result.get("extraction_work_identifier")
    if isinstance(extraction_identifier, str):
        extracted = await database.work(extraction_identifier)
        result["item_extraction_work_identifier"] = extraction_identifier
        if _pending(extracted):
            return _retry(result, "extracting_details")
        if extracted.get("state") != "completed":
            return _failed(result, "listing_pipeline_extraction_failed")
        detail_result = _mapping(extracted.get("result"))
    observation_identifier = result.get("observation_record_identifier")
    if not isinstance(observation_identifier, str):
        observation_identifier = detail_result.get("observation_record_identifier")
        if not isinstance(observation_identifier, str):
            return _failed(result, "listing_pipeline_missing_observation")
        result["observation_record_identifier"] = observation_identifier
        result["item_completed"] = True
        # Persist the exact observation before selecting analysis or later evidence.
        return _retry(result, "details_complete", delay_ns=0)
    kind, _, raw_observation = await database.get_record(observation_identifier)
    observation = _mapping(raw_observation)
    source = payload.marketplace.value
    if (
        kind != ("carl", source, "listing_observation")
        or observation.get(
            "listing_id" if payload.marketplace is Marketplace.FACEBOOK else "item_identifier"
        )
        != payload.external_identifier
    ):
        return _failed(result, "listing_pipeline_observation_identity_mismatch")
    classification = (
        _mapping(observation.get("response_classification")).get("kind")
        if payload.marketplace is Marketplace.FACEBOOK
        else observation.get("classification")
    )
    if classification in {"listing_unavailable", "unavailable"}:
        return _skipped(result, "listing_unavailable")
    if classification not in {"full_listing", "detail"}:
        return _failed(result, "listing_pipeline_unusable_observation")
    status = _status(payload.marketplace, observation)
    result["listing_status"] = status.value
    if status not in options.statuses:
        return _skipped(result, "listing_status_excluded")
    images = _strings(detail_result.get("image_work_identifiers"))
    descriptions = _strings(detail_result.get("description_work_identifiers"))
    result.update(image_work_identifiers=images, description_work_identifiers=descriptions)
    children = [("image", await database.work(identifier)) for identifier in images]
    children.extend(
        [("description", await database.work(identifier)) for identifier in descriptions]
    )
    if any(_pending(work) for _, work in children):
        return _retry(result, "collecting_evidence")
    image_extractions: list[str] = []
    for category, work in children:
        extraction = _mapping(work.get("result")).get("extraction_work_identifier")
        if category == "image" and isinstance(extraction, str):
            image_extractions.append(extraction)
    extracted_images = [await database.work(identifier) for identifier in image_extractions]
    result["image_extraction_work_identifiers"] = image_extractions
    if any(_pending(work) for work in extracted_images):
        return _retry(result, "extracting_images")
    failures = sum(work.get("state") != "completed" for _, work in children) + sum(
        work.get("state") != "completed" for work in extracted_images
    )
    reported_failures = detail_result.get("image_failures")
    if isinstance(reported_failures, int):
        failures = max(failures, reported_failures)
    result["evidence_failures"] = failures
    result["images_completed"] = options.stop_after is not PipelineStage.DETAILS
    if failures and not options.allow_incomplete_gallery:
        return _failed(result, "listing_pipeline_evidence_failed")
    if options.stop_after is not PipelineStage.ANALYSIS:
        return CompletedWork(
            result={
                **result,
                "stage": "complete",
                "state": "completed_with_failures" if failures else "completed",
            }
        )
    async with database.transaction():
        analysis_identifier = await _requested(
            database, _ANALYSIS_REQUESTER, context.work_item_identifier
        )
        if analysis_identifier is None:
            if await dependencies.application.pipeline_analysis_reuse(
                observation_identifier, options
            ):
                return _skipped({**result, "analysis_reused": True}, "analysis_history_reused")
            if not payload.analysis_authorized:
                return _skipped(result, "analysis_budget_exhausted")
            guide = options.product_guide_record_identifier
            if guide is None:
                return _failed(result, "listing_pipeline_missing_product_guide")
            try:
                requested = await dependencies.application.request_listing_analysis(
                    RequestAnalysisRequest(
                        listing_observation_record_identifier=observation_identifier,
                        product_guide_record_identifier=guide,
                        allow_incomplete_gallery=options.allow_incomplete_gallery,
                    ),
                    requester_kind=_ANALYSIS_REQUESTER,
                    requester_identifier=context.work_item_identifier,
                    requester_context={
                        "root_work_identifier": payload.root_work_identifier,
                        "listing_observation_record_identifier": observation_identifier,
                    },
                )
            except IncompleteGalleryError as error:
                return CompletedWork(
                    result={
                        **result,
                        "stage": "skipped",
                        "state": "completed_with_failures" if failures else "completed",
                        "reason": {
                            "kind": "incomplete_gallery",
                            "unavailable_gallery_orders": list(error.unavailable_gallery_orders),
                            "gallery_absence_reason": error.gallery_absence_reason,
                        },
                    }
                )
            except ReviewInputError:
                return _skipped(result, "analysis_input_unavailable")
            analysis_identifier = requested.work_identifier
            result["analysis_reused"] = not requested.created
            result["evidence_set_record_identifier"] = requested.evidence_set_record_identifier
    result["analysis_work_identifier"] = analysis_identifier
    analysis = await database.work(analysis_identifier)
    if _pending(analysis):
        return _retry(result, "analyzing")
    if analysis.get("state") != "completed":
        return _failed(result, "listing_pipeline_analysis_failed")
    return CompletedWork(
        result={
            **result,
            "analysis_completed": True,
            "stage": "complete",
            "state": "completed_with_failures" if failures else "completed",
        }
    )


def build_pipeline_listing_worker_registry(
    dependencies: PipelineListingWorkerDependencies,
) -> WorkHandlerRegistry:
    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=LISTING_PIPELINE_WORK_KIND, payload_schema_version=1
                ),
                component=build_pipeline_component_registry().require(RUN_LISTING_PIPELINE),
                payload_type=PipelineListingPayload,
                handler=lambda payload, context: _run_listing_pipeline(
                    payload, context, dependencies
                ),
            ),
        )
    )
