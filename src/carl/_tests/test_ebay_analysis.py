"""Offline eBay analysis provenance, completeness, scheduling and worker regression tests."""

import json
from collections.abc import Callable
from itertools import count
from pathlib import Path
from time import perf_counter_ns, time_ns

import anyio
import pytest

from carl._tests.test_item_analysis import _async_provenance, _provenance
from carl.core.components import Component, ComponentId
from carl.core.ebay_analysis import EbayAnalyzeItemPayload, analyze_ebay_item_work
from carl.core.item_analysis import (
    ANALYZE_ITEM_WORK_KIND,
    ANALYZE_ITEM_WORK_SCHEMA_VERSION,
    AnalyzeItemPayload,
    ClaudeEffort,
    analysis_work_constraints,
    analyze_item_work,
)
from carl.core.models import BytesDraft, JsonValue, NamedInput, NamedOutput, RecordDraft
from carl.core.review import (
    CreateProductGuideRequest,
    ProductGuideDetails,
    RequestAnalysisRequest,
    RequestAnalysisResult,
)
from carl.core.review_errors import IncompleteGalleryError, ReviewInputError
from carl.core.work import SchedulingScopeKind, WorkCapability, WorkRequester, WorkState
from carl.core.worker import AttemptContext, RetryWork, TerminalFailureWork, WorkerSettings
from carl.ebay_analysis_workers import (
    _analysis_input,
    _analyze,
    build_ebay_analysis_worker_registry,
    request_ebay_listing_analysis,
    select_ebay_analysis_evidence,
)
from carl.facebook_analysis_workers import AnalysisWorkerDependencies
from carl.io.claude import ClaudeCli, ClaudeRun
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, execute_lease
from carl.review import ReviewApplication

ITEM = "123456789012"
URL = "https://i.ebayimg.com/images/g/example/s-l1600.jpg"
DESCRIPTION_URL = "https://vi.vipr.ebaydesc.com/itmdesc/123456789012"


def test_ebay_analysis_defaults_to_sonnet_5_5_at_medium_effort() -> None:
    payload = EbayAnalyzeItemPayload(
        evidence_set_record_identifier="evidence-set",
        product_guide_record_identifier="product-guide",
    )
    assert payload.model == "claude-sonnet-5-5"
    assert payload.effort is ClaudeEffort.MEDIUM


def _identifiers() -> Callable[[], str]:
    numbers = count()
    return lambda: f"ebay-analysis-{next(numbers)}"


async def _publish(
    database: Database,
    identifier: str,
    records: tuple[RecordDraft, ...],
    *,
    inputs: tuple[NamedInput, ...] = (),
    artifacts: tuple[BytesDraft, ...] = (),
) -> None:
    await database.begin_operation(
        operation_id=identifier,
        component=Component(ComponentId(("test", "ebay_analysis")), 1, lambda: None),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-09-29T00:00:00+00:00",
        inputs=tuple((edge.name, edge.object_identifier) for edge in inputs),
    )
    await database.complete_operation(
        operation_id=identifier,
        records=records,
        artifacts=artifacts,
        outputs=tuple(
            NamedOutput(name=("record", str(index)), object_identifier=record.identifier)
            for index, record in enumerate((*records, *artifacts))
        ),
        result={},
        ended_at_utc="2026-09-29T00:00:01+00:00",
        duration_ns=1,
    )


async def _seed(
    database: Database,
    *,
    saved: bool = True,
    second_image: bool = False,
    no_gallery: bool = False,
    result_url: str = URL,
) -> None:
    urls = [] if no_gallery else [URL, *([URL + "?second=1"] if second_image else [])]
    records = [
        RecordDraft(
            identifier="observation",
            kind=("carl", "ebay", "listing_observation"),
            schema_version=1,
            value={
                "item_identifier": ITEM,
                "classification": "detail",
                "title": "Eyepiece",
                "condition": "Used",
                "displayed_price": "US $65.00",
                "currency": "USD",
                "description": "Inline seller summary",
                "description_url": DESCRIPTION_URL,
                "gallery_urls": urls,
                "acquisition_record_identifier": "acquisition",
            },
        ),
        RecordDraft(
            identifier="description",
            kind=("carl", "ebay", "description_result"),
            schema_version=1,
            value={
                "item_identifier": ITEM,
                "observation_record_identifier": "observation",
                "url": DESCRIPTION_URL,
                "state": "saved",
                "description": "Exact retained seller description: optics intact.",
            },
        ),
    ]
    artifacts: tuple[BytesDraft, ...] = ()
    if not no_gallery:
        records.append(
            RecordDraft(
                identifier="reference",
                kind=("carl", "ebay", "gallery_image_reference"),
                schema_version=1,
                value={
                    "item_identifier": ITEM,
                    "observation_record_identifier": "observation",
                    "gallery_order": 0,
                    "url": URL,
                },
            )
        )
        if saved:
            records.append(
                RecordDraft(
                    identifier="image-result",
                    kind=("carl", "ebay", "image_result"),
                    schema_version=1,
                    value={
                        "item_identifier": ITEM,
                        "observation_record_identifier": "observation",
                        "reference_record_identifier": "reference",
                        "url": result_url,
                        "state": "saved",
                        "image_artifact_identifier": "image-artifact",
                    },
                )
            )
            artifacts = (
                BytesDraft(
                    identifier="image-artifact",
                    kind=("carl", "ebay", "image_file"),
                    media_type="image/png",
                    representation={},
                    content=b"offline image bytes",
                ),
            )
    await _publish(database, "seed", tuple(records), artifacts=artifacts)


async def _application(database: Database, tmp_path: Path) -> tuple[ReviewApplication, str]:
    application = ReviewApplication(
        database, tmp_path, new_identifier=_identifiers(), code_provenance=_async_provenance
    )
    guide = await application.create_product_guide(
        CreateProductGuideRequest(
            identity=("eyepiece",),
            display_name="Eyepieces",
            text="Identify optical design, markings and barrel size from saved evidence.",
        )
    )
    assert isinstance(guide, ProductGuideDetails)
    return application, guide.record_identifier


def _context(attempt: int = 1) -> AttemptContext:
    return AttemptContext(
        work_item_identifier="work",
        lease_token="token",
        worker_identifier="worker",
        attempt=attempt,
        operation_identifier="operation",
    )


def _run(failure: str | None = None) -> ClaudeRun:
    return ClaudeRun(
        argv=("offline-fake",),
        version="test",
        started_at_utc_ns=1,
        ended_at_utc_ns=2,
        duration_ns=1,
        exit_code=0 if failure is None else None,
        stdout=b'{"type":"result"}\n',
        stderr=b"",
        text="Evidence-backed item report" if failure is None else None,
        output_metadata={"num_turns": 2},
        tool_calls=("Read",),
        failure_kind=failure,
    )


@pytest.mark.anyio
async def test_exact_ebay_selection_includes_external_description_and_reuses_pending(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database)
        application, guide = await _application(database, tmp_path)
        request = RequestAnalysisRequest(
            listing_observation_record_identifier="observation",
            product_guide_record_identifier=guide,
        )
        first = await application.request_listing_analysis(request)
        second = await application.request_listing_analysis(request)
        assert first.created and not second.created
        assert first.work_identifier == second.work_identifier
        assert first.evidence_set_record_identifier == second.evidence_set_record_identifier
        _, edges, _ = await database.object_operation_relations(
            first.evidence_set_record_identifier
        )
        assert NamedInput(name=("description_result",), object_identifier="description") in edges
        brief, files = await _analysis_input(database, first.evidence_set_record_identifier)
        assert brief["description_state"] == "saved"
        assert brief["fields"]["description"] == "Exact retained seller description: optics intact."
        assert files == (("images/000.png", b"offline image bytes"),)
        work = await database.work(first.work_identifier)
        assert work["kind"] == ["carl", "ebay", "work", "analyze_item"]


@pytest.mark.anyio
async def test_concurrent_requests_publish_one_evidence_selection_and_share_work(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database)
        application, guide = await _application(database, tmp_path)
        request = RequestAnalysisRequest(
            listing_observation_record_identifier="observation",
            product_guide_record_identifier=guide,
        )
        results: list[RequestAnalysisResult] = []

        async def enqueue() -> None:
            results.append(await request_ebay_listing_analysis(application, request))

        async with anyio.create_task_group() as tasks:
            for _ in range(5):
                tasks.start_soon(enqueue)
        assert len({result.work_identifier for result in results}) == 1
        assert len({result.evidence_set_record_identifier for result in results}) == 1
        assert sum(result.created for result in results) == 1
        assert (
            len(await database.records_by_kind(("carl", "ebay", "listing_analysis_evidence"))) == 1
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "case", ["missing_result", "unretained_reference", "wrong_url", "no_gallery"]
)
async def test_ebay_incomplete_gallery_is_explicit(tmp_path: Path, case: str) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(
            database,
            saved=case != "missing_result",
            second_image=case == "unretained_reference",
            no_gallery=case == "no_gallery",
            result_url=URL + "?wrong=1" if case == "wrong_url" else URL,
        )
        with pytest.raises(IncompleteGalleryError):
            await select_ebay_analysis_evidence(database, "observation")
        selection = await select_ebay_analysis_evidence(
            database, "observation", allow_incomplete_gallery=True
        )
        assert (
            selection.gallery_absence_reason is not None
            if case == "no_gallery"
            else selection.unavailable_gallery_orders
        )
        application, guide = await _application(database, tmp_path)
        requested = await request_ebay_listing_analysis(
            application,
            RequestAnalysisRequest(
                listing_observation_record_identifier="observation",
                product_guide_record_identifier=guide,
                allow_incomplete_gallery=True,
            ),
        )
        brief, _ = await _analysis_input(database, requested.evidence_set_record_identifier)
        assert (
            brief["gallery_absence_reason"]
            if case == "no_gallery"
            else brief["unavailable_gallery_images"]
        )


@pytest.mark.anyio
async def test_new_description_changes_evidence_identity(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database)
        application, guide = await _application(database, tmp_path)
        request = RequestAnalysisRequest(
            listing_observation_record_identifier="observation",
            product_guide_record_identifier=guide,
        )
        first = await request_ebay_listing_analysis(application, request)
        await _publish(
            database,
            "description-update",
            (
                RecordDraft(
                    identifier="new-description",
                    kind=("carl", "ebay", "description_result"),
                    schema_version=1,
                    value={
                        "item_identifier": ITEM,
                        "observation_record_identifier": "observation",
                        "url": DESCRIPTION_URL,
                        "state": "saved",
                        "description": "New exact retained description.",
                    },
                ),
            ),
        )
        second = await request_ebay_listing_analysis(application, request)
        assert second.created
        assert first.evidence_set_record_identifier != second.evidence_set_record_identifier
        old_brief, _ = await _analysis_input(database, first.evidence_set_record_identifier)
        new_brief, _ = await _analysis_input(database, second.evidence_set_record_identifier)
        assert old_brief["fields"]["description"] != new_brief["fields"]["description"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "field", ["url", "observation_record_identifier", "reference_record_identifier"]
)
async def test_worker_rejects_forged_image_selection(tmp_path: Path, field: str) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database)
        selection = await select_ebay_analysis_evidence(database, "observation")
        _, _, value = await database.get_record("image-result")
        assert isinstance(value, dict)
        forged = {**value, field: "wrong-evidence"}
        await _publish(
            database,
            "forged-result",
            (
                RecordDraft(
                    identifier="forged-result",
                    kind=("carl", "ebay", "image_result"),
                    schema_version=1,
                    value=forged,
                ),
            ),
        )
        edges = tuple(
            edge
            if edge.name != ("image_result", "00000000")
            else NamedInput(name=edge.name, object_identifier="forged-result")
            for edge in selection.inputs
        )
        await _publish(
            database,
            "forged-evidence",
            (
                RecordDraft(
                    identifier="forged-evidence",
                    kind=("carl", "ebay", "listing_analysis_evidence"),
                    schema_version=1,
                    value=selection.value,
                ),
            ),
            inputs=edges,
        )
        with pytest.raises(ValueError, match="does not satisfy"):
            await _analysis_input(database, "forged-evidence")


@pytest.mark.anyio
async def test_analysis_worker_completes_durably_with_exact_inputs_and_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[dict[str, JsonValue]] = []

    async def fake_run(
        self: ClaudeCli,
        *,
        directory: Path,
        prompt: str,
        model: str,
        effort: ClaudeEffort,
        timeout_seconds: int,
        maximum_turns: int | None = None,
    ) -> ClaudeRun:
        assert "single saved eBay listing" in prompt and "single saved Facebook" not in prompt
        assert "barrel size" in prompt
        captured.append(json.loads((directory / "listing.json").read_text()))
        assert (directory / "images" / "000.png").read_bytes() == b"offline image bytes"
        return _run()

    monkeypatch.setattr(ClaudeCli, "run", fake_run)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database)
        application, guide = await _application(database, tmp_path)
        request = RequestAnalysisRequest(
            listing_observation_record_identifier="observation",
            product_guide_record_identifier=guide,
        )
        requested = await request_ebay_listing_analysis(application, request)
        registry = build_ebay_analysis_worker_registry(
            AnalysisWorkerDependencies(database, ClaudeCli(), application.new_identifier)
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="token",
            lease_duration_ns=10_000_000_000,
            utc_now_ns=time_ns,
            event_identifier="claim",
        )
        assert claim.lease is not None
        await execute_lease(
            database=database,
            registry=registry,
            settings=WorkerSettings(
                worker_count=1,
                lease_duration_ns=10_000_000_000,
                renewal_interval_ns=1_000_000_000,
                idle_poll_interval_ns=1_000_000,
            ),
            lease=claim.lease,
            services=WorkerRuntimeServices(
                new_identifier=application.new_identifier,
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                code_provenance=_async_provenance,
                invocation=lambda: {},
            ),
        )
        assert await database.work_state(requested.work_identifier) is WorkState.COMPLETED
        work = await database.work(requested.work_identifier)
        record_identifier = work["result"]["analysis_record_identifier"]
        report = await application.get_listing_analysis(record_identifier)
        assert report.text == "Evidence-backed item report"
        assert report.descriptor.listing_observation_record_identifier == "observation"
        reused = await request_ebay_listing_analysis(application, request)
        assert not reused.created and reused.work_identifier == requested.work_identifier
        assert len(captured) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("attempt", [1, 3])
async def test_analysis_timeout_retains_attempt_and_uses_bounded_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, attempt: int
) -> None:
    async def fake_run(self: ClaudeCli, **kwargs: object) -> ClaudeRun:
        return _run("claude_timeout")

    monkeypatch.setattr(ClaudeCli, "run", fake_run)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database)
        application, guide = await _application(database, tmp_path)
        requested = await request_ebay_listing_analysis(
            application,
            RequestAnalysisRequest(
                listing_observation_record_identifier="observation",
                product_guide_record_identifier=guide,
            ),
        )
        payload = EbayAnalyzeItemPayload(
            evidence_set_record_identifier=requested.evidence_set_record_identifier,
            product_guide_record_identifier=guide,
        )
        outcome = await _analyze(
            payload,
            _context(attempt),
            AnalysisWorkerDependencies(database, ClaudeCli(), application.new_identifier),
        )
        assert isinstance(outcome, RetryWork if attempt == 1 else TerminalFailureWork)
        assert len(outcome.artifacts) == 4 and len(outcome.records) == 1
        assert outcome.records[0].value["claude"]["failure_kind"] == "claude_timeout"


def test_ebay_analysis_uses_existing_shared_ai_concurrency_scope() -> None:
    definition = analyze_ebay_item_work(
        identifier="work",
        payload=EbayAnalyzeItemPayload(
            evidence_set_record_identifier="evidence", product_guide_record_identifier="guide"
        ),
    )
    assert any(
        scope.kind is SchedulingScopeKind.WORK_KIND and scope.identity == ANALYZE_ITEM_WORK_KIND
        for scope in definition.scopes
    )
    assert analysis_work_constraints()[0].scope in definition.scopes


@pytest.mark.anyio
async def test_shared_ai_limit_counts_both_marketplaces(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for constraint in analysis_work_constraints():
            await database.register_constraint(constraint, registered_at_utc_ns=time_ns())
        for index in range(10):
            await database.enqueue_work(
                analyze_item_work(
                    identifier=f"facebook-{index}",
                    payload=AnalyzeItemPayload(
                        evidence_set_record_identifier=f"evidence-{index}",
                        product_guide_record_identifier="guide",
                    ),
                ),
                WorkRequester(
                    request_identifier=f"request-{index}",
                    kind=("test", "request"),
                    identifier=str(index),
                    context={},
                ),
                event_identifier=f"enqueue-{index}",
                enqueued_at_utc_ns=time_ns(),
            )
        await database.enqueue_work(
            analyze_ebay_item_work(
                identifier="ebay",
                payload=EbayAnalyzeItemPayload(
                    evidence_set_record_identifier="evidence-ebay",
                    product_guide_record_identifier="guide",
                ),
            ),
            WorkRequester(
                request_identifier="request-ebay",
                kind=("test", "request"),
                identifier=ITEM,
                context={},
            ),
            event_identifier="enqueue-ebay",
            enqueued_at_utc_ns=time_ns(),
        )
        capabilities = (
            WorkCapability(
                kind=ANALYZE_ITEM_WORK_KIND, payload_schema_version=ANALYZE_ITEM_WORK_SCHEMA_VERSION
            ),
            WorkCapability(kind=("carl", "ebay", "work", "analyze_item"), payload_schema_version=1),
        )
        leases = []
        for index in range(11):
            claim = await database.claim_work(
                supported_capabilities=capabilities,
                worker_identifier=f"worker-{index}",
                lease_token=f"token-{index}",
                lease_duration_ns=60_000_000_000,
                utc_now_ns=time_ns,
                event_identifier=f"claim-{index}",
            )
            leases.append(claim.lease)
        assert all(lease is not None for lease in leases[:10])
        assert leases[-1] is None
        assert await database.work_state("ebay") is WorkState.PENDING


@pytest.mark.anyio
async def test_non_detail_observation_is_not_analysis_evidence(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            "seed",
            (
                RecordDraft(
                    identifier="observation",
                    kind=("carl", "ebay", "listing_observation"),
                    schema_version=1,
                    value={
                        "item_identifier": ITEM,
                        "classification": "challenge",
                        "gallery_urls": [],
                    },
                ),
            ),
        )
        with pytest.raises(ReviewInputError, match="full eBay item-detail"):
            await select_ebay_analysis_evidence(
                database, "observation", allow_incomplete_gallery=True
            )
