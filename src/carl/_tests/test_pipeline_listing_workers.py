"""Offline per-listing pipelines preserve ownership, readiness, and exact inputs."""

# pyright: reportPrivateUsage=false

from collections.abc import Callable
from pathlib import Path
from typing import final

import pytest

from carl._tests.test_ebay_item_workers import _enqueue, _identifiers, _mapping, _run_next
from carl.core.components import Component, ComponentId
from carl.core.composed_projection import ListingStatus
from carl.core.marketplace_listing import RequestListingDetailsRequest, RequestListingDetailsResult
from carl.core.marketplace_search import Marketplace
from carl.core.models import JsonValue, RecordDraft
from carl.core.pipeline import (
    LISTING_PIPELINE_WORK_KIND,
    PipelineListingPayload,
    PipelineOptions,
    PipelineStage,
    listing_pipeline_work,
)
from carl.core.review import RequestAnalysisRequest, RequestAnalysisResult
from carl.core.review_errors import IncompleteGalleryError
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry
from carl.pipeline_listing_workers import (
    PipelineListingWorkerDependencies,
    _run_listing_pipeline,
    build_pipeline_listing_worker_registry,
)

_DETAILS = ("test", "pipeline", "details")
_ANALYSIS = ("test", "pipeline", "analysis")
_IMAGE = ("test", "pipeline", "image")
_EXTRACTION = ("test", "pipeline", "extraction")
_IMAGE_EXTRACTION = ("test", "pipeline", "image_extraction")
_DESCRIPTION = ("test", "pipeline", "description")


@final
class _Application:
    def __init__(self, database: Database, identifiers: Callable[[], str]):
        self.database = database
        self.identifiers = identifiers
        self.detail_requests: list[RequestListingDetailsRequest] = []
        self.analysis_requests: list[RequestAnalysisRequest] = []
        self.unavailable = False
        self.image_failure = False
        self.incomplete = False
        self.history_reused = False
        self.image_identifier: str | None = None
        self.extraction_identifier: str | None = None
        self.image_extraction_identifier: str | None = None
        self.description_identifier: str | None = None
        self.pending_gallery = False

    async def _enqueue_owned(
        self, kind: tuple[str, ...], owner: str, requester_kind: tuple[str, ...], payload: JsonValue
    ) -> str:
        definition = WorkDefinition(
            identifier=self.identifiers(),
            kind=kind,
            payload_schema_version=1,
            payload=payload,
            deduplication_identity=(owner, *kind),
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
            ),
        )
        result = await self.database.enqueue_work(
            definition,
            WorkRequester(
                request_identifier=self.identifiers(),
                kind=requester_kind,
                identifier=owner,
                context={},
            ),
            event_identifier=self.identifiers(),
            enqueued_at_utc_ns=0,
        )
        return result.work_item_identifier

    async def request_listing_details(
        self,
        request: RequestListingDetailsRequest,
        *,
        requester_kind: tuple[str, ...],
        requester_identifier: str | None,
    ) -> RequestListingDetailsResult:
        assert requester_identifier is not None
        self.detail_requests.append(request)
        identifier = await self._enqueue_owned(
            _DETAILS, requester_identifier, requester_kind, request.model_dump(mode="json")
        )
        return RequestListingDetailsResult(
            marketplace=request.marketplace,
            external_identifier=request.external_identifier,
            work_identifier=identifier,
            created=True,
            state="pending",
            reused_item_page=False,
        )

    async def request_listing_analysis(
        self,
        request: RequestAnalysisRequest,
        *,
        requester_kind: tuple[str, ...],
        requester_identifier: str | None,
        requester_context: JsonValue | None,
    ) -> RequestAnalysisResult:
        _ = requester_context
        assert requester_identifier is not None
        self.analysis_requests.append(request)
        if self.incomplete and not request.allow_incomplete_gallery:
            raise IncompleteGalleryError((1,))
        identifier = await self._enqueue_owned(
            _ANALYSIS, requester_identifier, requester_kind, request.model_dump(mode="json")
        )
        return RequestAnalysisResult(
            work_identifier=identifier,
            created=True,
            evidence_set_record_identifier="exact-evidence",
            included_gallery_count=1,
            unavailable_gallery_orders=(),
            gallery_absence_reason=None,
        )

    async def pipeline_analysis_reuse(
        self, observation_identifier: str, options: PipelineOptions
    ) -> bool:
        _ = observation_identifier, options
        return self.history_reused

    async def details(
        self, request: RequestListingDetailsRequest, context: AttemptContext
    ) -> WorkOutcome:
        if self.extraction_identifier:
            return CompletedWork(result={"extraction_work_identifier": self.extraction_identifier})
        outcome = await self.extracted(request, context)
        if self.pending_gallery:
            return RetryWork(
                records=outcome.records,
                result=outcome.result,
                delay_ns=0,
                reason={"kind": "gallery_pending"},
            )
        return outcome

    async def extracted(
        self, request: RequestListingDetailsRequest, context: AttemptContext
    ) -> CompletedWork:
        _ = context
        observation = "observation-" + request.external_identifier
        image_ids = [self.image_identifier] if self.image_identifier else []
        value: JsonValue = (
            {
                "item_identifier": request.external_identifier,
                "classification": "unavailable" if self.unavailable else "detail",
            }
            if request.marketplace is Marketplace.EBAY
            else {
                "listing_id": request.external_identifier,
                "response_classification": {
                    "kind": "listing_unavailable" if self.unavailable else "full_listing"
                },
                "fields": {
                    "availability_live": {
                        "state": "present",
                        "evidence": [{"state": "present", "normalized": True}],
                    }
                },
            }
        )
        return CompletedWork(
            records=(
                RecordDraft(
                    identifier=observation,
                    kind=("carl", request.marketplace.value, "listing_observation"),
                    schema_version=1,
                    value=value,
                ),
            ),
            result={
                "observation_record_identifier": observation,
                "image_work_identifiers": image_ids,
                "description_work_identifiers": [self.description_identifier]
                if self.description_identifier
                else [],
            },
        )

    async def image(self, payload: RequestAnalysisRequest, context: AttemptContext) -> WorkOutcome:
        _ = payload, context
        if self.image_extraction_identifier:
            return CompletedWork(
                result={"extraction_work_identifier": self.image_extraction_identifier}
            )
        return (
            TerminalFailureWork(error={"kind": "image_failed"}, result={"state": "failed"})
            if self.image_failure
            else CompletedWork(result={"state": "saved"})
        )

    async def analysis(
        self, payload: RequestAnalysisRequest, context: AttemptContext
    ) -> WorkOutcome:
        _ = payload, context
        return CompletedWork(result={"state": "completed"})

    def registry(self) -> WorkHandlerRegistry:
        return WorkHandlerRegistry(
            handlers=(
                *build_pipeline_listing_worker_registry(
                    PipelineListingWorkerDependencies(self.database, self)
                ).handlers,
                TypedWorkHandler(
                    capability=WorkCapability(kind=_DETAILS, payload_schema_version=1),
                    component=Component(ComponentId(_DETAILS), 1, self.details),
                    payload_type=RequestListingDetailsRequest,
                    handler=self.details,
                ),
                TypedWorkHandler(
                    capability=WorkCapability(kind=_ANALYSIS, payload_schema_version=1),
                    component=Component(ComponentId(_ANALYSIS), 1, self.analysis),
                    payload_type=RequestAnalysisRequest,
                    handler=self.analysis,
                ),
                TypedWorkHandler(
                    capability=WorkCapability(kind=_IMAGE, payload_schema_version=1),
                    component=Component(ComponentId(_IMAGE), 1, self.image),
                    payload_type=RequestAnalysisRequest,
                    handler=self.image,
                ),
                TypedWorkHandler(
                    capability=WorkCapability(kind=_EXTRACTION, payload_schema_version=1),
                    component=Component(ComponentId(_EXTRACTION), 1, self.extracted),
                    payload_type=RequestListingDetailsRequest,
                    handler=self.extracted,
                ),
                TypedWorkHandler(
                    capability=WorkCapability(kind=_DESCRIPTION, payload_schema_version=1),
                    component=Component(ComponentId(_DESCRIPTION), 1, self.analysis),
                    payload_type=RequestAnalysisRequest,
                    handler=self.analysis,
                ),
                TypedWorkHandler(
                    capability=WorkCapability(kind=_IMAGE_EXTRACTION, payload_schema_version=1),
                    component=Component(ComponentId(_IMAGE_EXTRACTION), 1, self.analysis),
                    payload_type=RequestAnalysisRequest,
                    handler=self.analysis,
                ),
            )
        )


def _payload(
    *,
    item: str = "123456789012",
    marketplace: Marketplace = Marketplace.EBAY,
    options: dict[str, object] | None = None,
) -> PipelineListingPayload:
    settings = PipelineOptions.model_validate(
        {"product_guide_record_identifier": "guide", **(options or {})}
    )
    return PipelineListingPayload(
        root_work_identifier="root",
        marketplace=marketplace,
        external_identifier=item,
        occurrence_record_identifier="occurrence",
        maximum_images=2,
        analysis_authorized=settings.stop_after is PipelineStage.ANALYSIS
        and settings.maximum_analyses > 0,
        options=settings,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("marketplace", (Marketplace.EBAY, Marketplace.FACEBOOK))
async def test_ready_listing_starts_analysis_while_another_listing_is_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, marketplace: Marketplace
) -> None:
    monkeypatch.setattr("carl.pipeline_listing_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        application = _Application(database, identifiers)
        payload = _payload(marketplace=marketplace)
        owner = await _enqueue(
            database, listing_pipeline_work(identifier="listing-a", payload=payload), identifiers
        )
        first = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        assert first["state"] == "pending", first
        _ = await _run_next(database, application.registry(), identifiers, _DETAILS)
        _ = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        other = await application.request_listing_details(
            RequestListingDetailsRequest(
                marketplace=marketplace, external_identifier="123456789013"
            ),
            requester_kind=("test", "other"),
            requester_identifier="listing-b",
        )
        _ = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        assert (await database.work(other.work_identifier))["state"] == "pending"
        result = _mapping((await database.work(owner))["result"])
        assert result["stage"] == "analyzing"
        assert (
            application.analysis_requests[0].listing_observation_record_identifier
            == "observation-123456789012"
        )
        _ = await _run_next(database, application.registry(), identifiers, _ANALYSIS)
        completed = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        assert completed["state"] == "completed"


@pytest.mark.anyio
async def test_request_edges_recover_enqueue_before_parent_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("carl.pipeline_listing_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        application = _Application(database, identifiers)
        payload = _payload()
        _ = await _enqueue(
            database, listing_pipeline_work(identifier="listing", payload=payload), identifiers
        )
        context = AttemptContext(
            work_item_identifier="listing",
            lease_token="unused",
            worker_identifier="test",
            operation_identifier="unused",
            attempt=1,
        )
        dependencies = PipelineListingWorkerDependencies(database, application)
        # Simulate an attempt ending after enqueue and before persisting its returned checkpoint.
        _ = await _run_listing_pipeline(payload, context, dependencies)
        _ = await _run_listing_pipeline(payload, context, dependencies)
        assert len(application.detail_requests) == 1
        _ = await _run_next(database, application.registry(), identifiers, _DETAILS)
        _ = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        _ = await _run_listing_pipeline(payload, context, dependencies)
        _ = await _run_listing_pipeline(payload, context, dependencies)
        assert len(application.analysis_requests) == 1
        _ = await _run_next(database, application.registry(), identifiers, _ANALYSIS)
        recovered = await _run_listing_pipeline(payload, context, dependencies)
        assert isinstance(recovered, CompletedWork)
        assert _mapping(recovered.result)["analysis_completed"] is True
        assert len(application.analysis_requests) == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    "case,expected",
    (
        ("unavailable", "listing_unavailable"),
        ("status", "listing_status_excluded"),
        ("history", "analysis_history_reused"),
        ("history_without_budget", "analysis_history_reused"),
        ("incomplete", "incomplete_gallery"),
        ("failure", "listing_pipeline_evidence_failed"),
        ("allowed_failure", "complete"),
        ("allowed_failure_history", "analysis_history_reused"),
        ("allowed_failure_budget", "analysis_budget_exhausted"),
        ("details", "complete"),
    ),
)
async def test_listing_policies_and_terminal_evidence_outcomes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str, expected: str
) -> None:
    monkeypatch.setattr("carl.pipeline_listing_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        application = _Application(database, identifiers)
        application.unavailable = case == "unavailable"
        application.history_reused = case in {
            "history",
            "history_without_budget",
            "allowed_failure_history",
        }
        application.incomplete = case == "incomplete"
        options: dict[str, object] = {}
        if case == "status":
            options["statuses"] = [ListingStatus.SOLD]
        if case == "details":
            options["stop_after"] = PipelineStage.DETAILS
        if case in {"history_without_budget", "allowed_failure_budget"}:
            options["maximum_analyses"] = 0
        if case in {
            "failure",
            "allowed_failure",
            "allowed_failure_history",
            "allowed_failure_budget",
        }:
            application.image_failure = True
            options["allow_incomplete_gallery"] = case != "failure"
            application.image_identifier = await application._enqueue_owned(
                _IMAGE,
                "image-owner",
                ("test", "image"),
                RequestAnalysisRequest(
                    listing_observation_record_identifier="placeholder",
                    product_guide_record_identifier="guide",
                ).model_dump(mode="json"),
            )
        payload = _payload(options=options)
        _ = await _enqueue(
            database, listing_pipeline_work(identifier="listing", payload=payload), identifiers
        )
        _ = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        _ = await _run_next(database, application.registry(), identifiers, _DETAILS)
        _ = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        if application.image_identifier:
            waiting = await _run_next(
                database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
            )
            assert _mapping(waiting["result"])["stage"] == "collecting_evidence"
            assert not application.analysis_requests
            _ = await _run_next(database, application.registry(), identifiers, _IMAGE)
        completed = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        if case == "allowed_failure":
            _ = await _run_next(database, application.registry(), identifiers, _ANALYSIS)
            completed = await _run_next(
                database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
            )
        result = _mapping(completed["result"])
        assert (
            result.get("stage") == expected
            or _mapping(result.get("reason")).get("kind") == expected
        )
        if case in {"history", "history_without_budget", "allowed_failure_history"}:
            assert result["analysis_reused"] is True
        if case.startswith("allowed_failure"):
            assert result["state"] == "completed_with_failures"
        if case == "details":
            assert application.detail_requests[0].maximum_images == 0
        if case not in {"allowed_failure", "incomplete"}:
            assert not application.analysis_requests


@pytest.mark.anyio
async def test_ebay_waits_its_extractor_description_and_image_extractor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("carl.pipeline_listing_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        application = _Application(database, identifiers)
        payload = _payload()
        request = RequestAnalysisRequest(
            listing_observation_record_identifier="placeholder",
            product_guide_record_identifier="guide",
        ).model_dump(mode="json")
        application.extraction_identifier = await application._enqueue_owned(
            _EXTRACTION,
            "extractor",
            ("test", "extractor"),
            RequestListingDetailsRequest(
                external_identifier=payload.external_identifier
            ).model_dump(mode="json"),
        )
        application.description_identifier = await application._enqueue_owned(
            _DESCRIPTION, "description", ("test", "description"), request
        )
        application.image_identifier = await application._enqueue_owned(
            _IMAGE, "image", ("test", "image"), request
        )
        application.image_extraction_identifier = await application._enqueue_owned(
            _IMAGE_EXTRACTION, "image-extraction", ("test", "image-extraction"), request
        )
        _ = await _enqueue(
            database, listing_pipeline_work(identifier="listing", payload=payload), identifiers
        )
        _ = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        _ = await _run_next(database, application.registry(), identifiers, _DETAILS)
        waiting = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        assert _mapping(waiting["result"])["stage"] == "extracting_details"
        _ = await _run_next(database, application.registry(), identifiers, _EXTRACTION)
        _ = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        _ = await _run_next(database, application.registry(), identifiers, _IMAGE)
        waiting = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        assert _mapping(waiting["result"])["stage"] == "collecting_evidence"
        assert not application.analysis_requests
        _ = await _run_next(database, application.registry(), identifiers, _DESCRIPTION)
        waiting = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        assert _mapping(waiting["result"])["stage"] == "extracting_images"
        assert not application.analysis_requests
        _ = await _run_next(database, application.registry(), identifiers, _IMAGE_EXTRACTION)
        waiting = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        assert _mapping(waiting["result"])["stage"] == "analyzing"
        assert (
            application.analysis_requests[0].listing_observation_record_identifier
            == "observation-123456789012"
        )


@pytest.mark.anyio
async def test_facebook_reports_pinned_item_before_its_gallery_is_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("carl.pipeline_listing_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        application = _Application(database, identifiers)
        application.pending_gallery = True
        payload = _payload(marketplace=Marketplace.FACEBOOK)
        _ = await _enqueue(
            database, listing_pipeline_work(identifier="listing", payload=payload), identifiers
        )
        _ = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        _ = await _run_next(database, application.registry(), identifiers, _DETAILS)
        waiting = await _run_next(
            database, application.registry(), identifiers, LISTING_PIPELINE_WORK_KIND
        )
        result = _mapping(waiting["result"])
        assert result["observation_record_identifier"] == "observation-123456789012"
        assert result["item_completed"] is True
        assert result["images_completed"] is False
        assert result["stage"] == "collecting_evidence"
        assert not application.analysis_requests
