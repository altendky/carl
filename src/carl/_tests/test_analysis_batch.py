"""Durable, restartable missing-analysis batch coordination."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from carl.analysis_batch_workers import (
    AnalysisBatchWorkerDependencies,
    build_analysis_batch_worker_registry,
)
from carl.core.analysis_batch import (
    REQUEST_MISSING_ANALYSES_MAXIMUM_ACTIVE,
    ListingAnalysisSelectionPolicy,
    RequestMissingAnalysesPayload,
    RequestMissingListingAnalysesRequest,
    apply_listing_analysis_selection_policy,
    legacy_request_missing_analyses_constraint_identifiers,
    request_missing_analyses_work,
    request_missing_analyses_work_constraints,
    select_listing_analysis_batch,
)
from carl.core.components import Component, ComponentId
from carl.core.item_analysis import AnalyzeItemPayload, analyze_item_work
from carl.core.models import CodeProvenance, NamedOutput, RecordDraft
from carl.core.review import (
    AnalysisDescriptor,
    CandidateAnalysisFilter,
    CandidateAvailability,
    CandidateFilters,
    CandidateSource,
    RequestAnalysisRequest,
    RequestAnalysisResult,
)
from carl.core.work import WorkCapability, WorkRequester, WorkState
from carl.core.worker import AttemptContext, TerminalFailureWork, WorkerSettings
from carl.io.sqlite import Database
from carl.io.worker import (
    TypedWorkHandler,
    WorkerRuntimeServices,
    WorkHandlerRegistry,
    execute_lease,
)
from carl.review import ReviewApplication, ReviewInputError


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash="a" * 40,
        worktree_state="clean",
        package_version="0",
        python_implementation="CPython",
        python_version="3.14",
        dependencies=(),
        lockfile_sha256=None,
    )


class _SkippingApplication:
    async def request_listing_analysis(
        self,
        request: RequestAnalysisRequest,
        *,
        requester_kind: tuple[str, ...],
        requester_identifier: str | None,
        requester_context: object | None,
    ) -> RequestAnalysisResult:
        _ = requester_kind, requester_identifier, requester_context
        raise ReviewInputError(
            f"No complete gallery for {request.listing_observation_record_identifier}"
        )


def test_batch_rejects_completed_analysis_filters() -> None:
    with pytest.raises(ValidationError, match="determines missing compatible analysis"):
        RequestMissingListingAnalysesRequest(
            search_refresh_work_identifier="refresh",
            product_guide_record_identifier="guide",
            filters=CandidateFilters(analysis=CandidateAnalysisFilter.ABSENT),
        )


def test_analysis_batch_concurrency_policy_is_versioned_and_parallel() -> None:
    constraints = request_missing_analyses_work_constraints()

    assert len(constraints) == 1
    assert constraints[0].maximum_active == REQUEST_MISSING_ANALYSES_MAXIMUM_ACTIVE == 10
    assert constraints[0].identifier not in legacy_request_missing_analyses_constraint_identifiers()


def _candidate_source(
    listing_identifier: str,
    observation_identifier: str,
    *,
    analyzed: bool,
    completion_sequence: int = 10,
) -> CandidateSource:
    analyses = (
        (
            AnalysisDescriptor(
                analysis_record_identifier=f"analysis-{observation_identifier}",
                evidence_set_record_identifier=f"evidence-{observation_identifier}",
                listing_observation_record_identifier=observation_identifier,
                product_guide_record_identifier="guide",
                completion_sequence=completion_sequence,
                completed_at_utc="2026-09-24T00:00:00+00:00",
                state="completed",
                warnings=(),
                model="claude-opus-4-1",
            ),
        )
        if analyzed
        else ()
    )
    return CandidateSource(
        listing_identifier=listing_identifier,
        observation_record_identifier=observation_identifier,
        acquisition_record_identifier=f"acquisition-{observation_identifier}",
        availability=CandidateAvailability.FULL_LISTING,
        acquisition_completion_sequence=completion_sequence,
        observation_completion_sequence=completion_sequence,
        observation={},
        analyses=analyses,
    )


def test_never_analyzed_policy_excludes_fresher_observation_of_analyzed_listing() -> None:
    sources = (
        _candidate_source("existing", "existing-old", analyzed=True, completion_sequence=9),
        _candidate_source("existing", "existing-fresh", analyzed=False),
        _candidate_source("new", "new-fresh", analyzed=False),
    )

    selected = apply_listing_analysis_selection_policy(
        sources,
        ListingAnalysisSelectionPolicy.NEVER_ANALYZED_LISTING,
    )

    assert tuple(source.observation_record_identifier for source in selected) == ("new-fresh",)

    batch = select_listing_analysis_batch(
        sources,
        filters=CandidateFilters(),
        selection_policy=ListingAnalysisSelectionPolicy.NEVER_ANALYZED_LISTING,
        maximum_items=None,
    )
    assert batch.matching_observation_record_identifiers == (
        "existing-fresh",
        "new-fresh",
    )
    assert batch.eligible_observation_record_identifiers == ("new-fresh",)
    assert batch.selected_observation_record_identifiers == ("new-fresh",)
    assert batch.reusable_observation_record_identifiers == ()
    assert batch.policy_excluded_observation_record_identifiers == ("existing-fresh",)
    assert batch.excluded_observation_record_identifiers == ("existing-fresh",)


def test_selected_guide_policy_reuses_only_analysis_under_exact_guide() -> None:
    analyzed_for_selected = _candidate_source(
        "selected", "selected-old", analyzed=True, completion_sequence=8
    )
    analyzed_for_other = _candidate_source(
        "other", "other-old", analyzed=True, completion_sequence=8
    ).model_copy(
        update={
            "analyses": (
                _candidate_source("other", "other-old", analyzed=True)
                .analyses[0]
                .model_copy(update={"product_guide_record_identifier": "other-guide"}),
            )
        }
    )
    sources = (
        analyzed_for_selected,
        _candidate_source("selected", "selected-fresh", analyzed=False),
        analyzed_for_other,
        _candidate_source("other", "other-fresh", analyzed=False),
        _candidate_source("new", "new-fresh", analyzed=False),
    )

    batch = select_listing_analysis_batch(
        sources,
        filters=CandidateFilters(),
        selection_policy=ListingAnalysisSelectionPolicy.MISSING_FOR_SELECTED_GUIDE,
        product_guide_record_identifier="guide",
        maximum_items=1,
    )

    assert batch.matching_observation_record_identifiers == (
        "new-fresh",
        "other-fresh",
        "selected-fresh",
    )
    assert batch.eligible_observation_record_identifiers == (
        "new-fresh",
        "other-fresh",
    )
    assert batch.selected_observation_record_identifiers == ("new-fresh",)
    assert batch.reusable_observation_record_identifiers == ("selected-fresh",)
    assert batch.policy_excluded_observation_record_identifiers == ()
    assert batch.excluded_observation_record_identifiers == ("other-fresh",)


@pytest.mark.anyio
async def test_batch_checkpoints_chunks_and_reports_skips(tmp_path: Path) -> None:
    now = 100
    payload = RequestMissingAnalysesPayload(
        request_identifier="batch-request",
        source_search_refresh_work_identifier="refresh",
        source_as_of_completion_sequence=10,
        listing_observation_record_identifiers=("observation-0", "observation-1"),
        product_guide_record_identifier="guide",
        allow_incomplete_gallery=False,
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.begin_operation(
            operation_id="source-operation",
            component=Component(ComponentId(("test", "source")), 1, _provenance),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-24T00:00:00+00:00",
            inputs=(),
        )
        source_records = (
            RecordDraft(
                identifier="guide",
                kind=("test", "guide"),
                schema_version=1,
                value={},
            ),
            *(
                RecordDraft(
                    identifier=identifier,
                    kind=("test", "observation"),
                    schema_version=1,
                    value={},
                )
                for identifier in payload.listing_observation_record_identifiers
            ),
        )
        await database.complete_operation(
            operation_id="source-operation",
            records=source_records,
            artifacts=(),
            outputs=tuple(
                NamedOutput(name=("source", f"{index:08d}"), object_identifier=record.identifier)
                for index, record in enumerate(source_records)
            ),
            result={},
            ended_at_utc="2026-09-24T00:00:01+00:00",
            duration_ns=1,
        )
        await database.enqueue_work(
            request_missing_analyses_work(identifier="batch", payload=payload),
            WorkRequester(
                request_identifier="request",
                kind=("test", "analysis_batch"),
                identifier="batch",
                context={},
            ),
            event_identifier="enqueue",
            enqueued_at_utc_ns=now,
        )
        registry = build_analysis_batch_worker_registry(
            AnalysisBatchWorkerDependencies(
                database=database,
                application=_SkippingApplication(),
            )
        )
        next_identifier = iter(f"identifier-{index}" for index in range(20))
        services = WorkerRuntimeServices(
            new_identifier=lambda: next(next_identifier),
            utc_now_ns=lambda: now,
            monotonic_ns=lambda: now,
            code_provenance=_async_provenance,
            invocation=lambda: {},
        )
        settings = WorkerSettings(
            worker_count=1,
            lease_duration_ns=10_000_000_000,
            renewal_interval_ns=1_000_000_000,
            idle_poll_interval_ns=10,
        )

        first = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker-1",
            lease_token="lease-1",
            lease_duration_ns=10_000_000_000,
            utc_now_ns=lambda: now,
            event_identifier="claim-1",
        )
        assert first.lease is not None
        await execute_lease(
            database=database,
            registry=registry,
            settings=settings,
            services=services,
            lease=first.lease,
        )
        checkpoint = await database.work("batch")
        assert checkpoint["state"] == WorkState.PENDING.value
        assert checkpoint["result"]["next_observation_index"] == 1
        assert len(checkpoint["result"]["skipped"]) == 1

        second = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker-2",
            lease_token="lease-2",
            lease_duration_ns=10_000_000_000,
            utc_now_ns=lambda: now,
            event_identifier="claim-2",
        )
        assert second.lease is not None
        await execute_lease(
            database=database,
            registry=registry,
            settings=settings,
            services=services,
            lease=second.lease,
        )

        completed = await database.work("batch")
        assert completed["state"] == WorkState.COMPLETED.value
        assert completed["result"]["next_observation_index"] == 2
        assert len(completed["result"]["skipped"]) == 2
        status = await ReviewApplication(database, tmp_path).get_work_status("batch")
        assert status.analysis_batch_progress is not None
        assert status.analysis_batch_progress.selected_observations == 2
        assert status.analysis_batch_progress.processed_observations == 2
        assert status.analysis_batch_progress.remaining_observations == 0
        assert status.analysis_batch_progress.checkpoint_processed_observations == 2
        assert status.analysis_batch_progress.observed_request_edges == 0
        assert status.analysis_batch_progress.skipped_observations == 2
        assert status.analysis_batch_progress.analyses.total == 0
        assert status.analysis_batch_progress.active_analysis_work_count == 0
        assert status.analysis_batch_progress.active_analysis_work_identifiers == ()
        assert not status.analysis_batch_progress.active_analysis_work_identifiers_truncated
        assert status.analysis_batch_progress.failure_reason_counts == ()
        assert status.analysis_batch_progress.recent_terminal_failures == ()


@pytest.mark.anyio
async def test_batch_status_observes_live_child_edges_before_parent_checkpoint(
    tmp_path: Path,
) -> None:
    payload = RequestMissingAnalysesPayload(
        request_identifier="batch-request",
        source_search_refresh_work_identifier="refresh",
        source_as_of_completion_sequence=10,
        listing_observation_record_identifiers=("observation",),
        product_guide_record_identifier="guide",
        allow_incomplete_gallery=False,
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            request_missing_analyses_work(identifier="batch", payload=payload),
            WorkRequester(
                request_identifier="batch-request-edge",
                kind=("test", "analysis_batch"),
                identifier="batch",
                context={},
            ),
            event_identifier="batch-enqueued",
            enqueued_at_utc_ns=100,
        )
        registry = build_analysis_batch_worker_registry(
            AnalysisBatchWorkerDependencies(
                database=database,
                application=_SkippingApplication(),
            )
        )
        claimed = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="batch-worker",
            lease_token="batch-lease",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: 200,
            event_identifier="batch-claimed",
        )
        assert claimed.lease is not None
        await database.enqueue_work(
            analyze_item_work(
                identifier="analysis-child",
                payload=AnalyzeItemPayload(
                    evidence_set_record_identifier="evidence",
                    product_guide_record_identifier="guide",
                ),
            ),
            WorkRequester(
                request_identifier="analysis-child-request",
                kind=("carl", "facebook", "analysis_batch"),
                identifier="batch",
                context={"listing_observation_record_identifier": "observation"},
            ),
            event_identifier="analysis-child-enqueued",
            enqueued_at_utc_ns=250,
        )

        status = await ReviewApplication(
            database,
            tmp_path,
            utc_now_ns=lambda: 300,
        ).get_work_status("batch")

        assert status.state is WorkState.LEASED
        assert status.runtime.worker_identifier == "batch-worker"
        assert status.runtime.lease_remaining_ns == 900
        assert status.runtime.latest_event_kind.value == "claimed"
        assert status.runtime.latest_event_age_ns == 100
        assert status.runtime.last_lease_activity_at_utc_ns == 200
        assert status.runtime.lease_activity_age_ns == 100
        assert status.analysis_batch_progress is not None
        assert status.analysis_batch_progress.selected_observations == 1
        assert status.analysis_batch_progress.processed_observations == 1
        assert status.analysis_batch_progress.remaining_observations == 0
        assert status.analysis_batch_progress.checkpoint_processed_observations == 0
        assert status.analysis_batch_progress.observed_request_edges == 1
        assert status.analysis_batch_progress.analyses.pending == 1
        assert status.analysis_batch_progress.active_analysis_work_count == 1
        assert status.analysis_batch_progress.active_analysis_work_identifiers == (
            "analysis-child",
        )
        assert not status.analysis_batch_progress.active_analysis_work_identifiers_truncated
        assert status.analysis_batch_progress.failure_reason_counts == ()
        assert status.analysis_batch_progress.recent_terminal_failures == ()


@pytest.mark.anyio
async def test_batch_status_combines_checkpointed_skip_with_live_observation_edge(
    tmp_path: Path,
) -> None:
    payload = RequestMissingAnalysesPayload(
        request_identifier="batch-request",
        source_search_refresh_work_identifier="refresh",
        source_as_of_completion_sequence=10,
        listing_observation_record_identifiers=("observation-0", "observation-1"),
        product_guide_record_identifier="guide",
        allow_incomplete_gallery=False,
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.begin_operation(
            operation_id="live-source-operation",
            component=Component(ComponentId(("test", "source")), 1, _provenance),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-24T00:00:00+00:00",
        )
        source_records = (
            RecordDraft(
                identifier="guide",
                kind=("test", "guide"),
                schema_version=1,
                value={},
            ),
            *(
                RecordDraft(
                    identifier=identifier,
                    kind=("test", "observation"),
                    schema_version=1,
                    value={},
                )
                for identifier in payload.listing_observation_record_identifiers
            ),
        )
        await database.complete_operation(
            operation_id="live-source-operation",
            records=source_records,
            artifacts=(),
            outputs=tuple(
                NamedOutput(name=("source", f"{index:08d}"), object_identifier=record.identifier)
                for index, record in enumerate(source_records)
            ),
            result={},
            ended_at_utc="2026-09-24T00:00:01+00:00",
            duration_ns=1,
        )
        await database.enqueue_work(
            request_missing_analyses_work(identifier="batch", payload=payload),
            WorkRequester(
                request_identifier="batch-request-edge",
                kind=("test", "analysis_batch"),
                identifier="batch",
                context={},
            ),
            event_identifier="batch-enqueued",
            enqueued_at_utc_ns=100,
        )
        registry = build_analysis_batch_worker_registry(
            AnalysisBatchWorkerDependencies(
                database=database,
                application=_SkippingApplication(),
            )
        )
        first = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="batch-worker-1",
            lease_token="batch-lease-1",
            lease_duration_ns=10_000_000_000,
            utc_now_ns=lambda: 200,
            event_identifier="batch-claimed-1",
        )
        assert first.lease is not None
        identifiers = iter(f"runtime-{index}" for index in range(10))
        await execute_lease(
            database=database,
            registry=registry,
            settings=WorkerSettings(
                worker_count=1,
                lease_duration_ns=10_000_000_000,
                renewal_interval_ns=1_000_000_000,
                idle_poll_interval_ns=10,
            ),
            services=WorkerRuntimeServices(
                new_identifier=lambda: next(identifiers),
                utc_now_ns=lambda: 200,
                monotonic_ns=lambda: 200,
                code_provenance=_async_provenance,
                invocation=lambda: {},
            ),
            lease=first.lease,
        )
        checkpoint = await database.work("batch")
        assert checkpoint["result"]["next_observation_index"] == 1
        assert len(checkpoint["result"]["skipped"]) == 1

        second = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="batch-worker-2",
            lease_token="batch-lease-2",
            lease_duration_ns=10_000_000_000,
            utc_now_ns=lambda: 250,
            event_identifier="batch-claimed-2",
        )
        assert second.lease is not None
        await database.enqueue_work(
            analyze_item_work(
                identifier="analysis-child",
                payload=AnalyzeItemPayload(
                    evidence_set_record_identifier="evidence",
                    product_guide_record_identifier="guide",
                ),
            ),
            WorkRequester(
                request_identifier="analysis-child-request",
                kind=("carl", "facebook", "analysis_batch"),
                identifier="batch",
                context={"listing_observation_record_identifier": "observation-1"},
            ),
            event_identifier="analysis-child-enqueued",
            enqueued_at_utc_ns=275,
        )

        status = await ReviewApplication(
            database,
            tmp_path,
            utc_now_ns=lambda: 300,
        ).get_work_status("batch")
        assert status.analysis_batch_progress is not None
        assert status.analysis_batch_progress.checkpoint_processed_observations == 1
        assert status.analysis_batch_progress.observed_request_edges == 1
        assert status.analysis_batch_progress.processed_observations == 2
        assert status.analysis_batch_progress.remaining_observations == 0


@pytest.mark.anyio
async def test_batch_status_summarizes_active_and_failed_children(tmp_path: Path) -> None:
    payload = RequestMissingAnalysesPayload(
        request_identifier="batch-request",
        source_search_refresh_work_identifier="refresh",
        source_as_of_completion_sequence=10,
        listing_observation_record_identifiers=("observation-0", "observation-1"),
        product_guide_record_identifier="guide",
        allow_incomplete_gallery=False,
    )
    failed_definition = analyze_item_work(
        identifier="analysis-failed",
        payload=AnalyzeItemPayload(
            evidence_set_record_identifier="evidence-failed",
            product_guide_record_identifier="guide",
        ),
    )
    pending_definition = analyze_item_work(
        identifier="analysis-pending",
        payload=AnalyzeItemPayload(
            evidence_set_record_identifier="evidence-pending",
            product_guide_record_identifier="guide",
        ),
    )

    async def fail_analysis(
        _payload: AnalyzeItemPayload, _context: AttemptContext
    ) -> TerminalFailureWork:
        return TerminalFailureWork(
            error={"kind": "claude_timeout"},
            result={"state": "terminal_failure"},
        )

    failure_registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=failed_definition.kind,
                    payload_schema_version=failed_definition.payload_schema_version,
                ),
                component=Component(ComponentId(("test", "analysis_failure")), 1, lambda: None),
                payload_type=AnalyzeItemPayload,
                handler=fail_analysis,
            ),
        )
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            request_missing_analyses_work(identifier="batch", payload=payload),
            WorkRequester(
                request_identifier="batch-request-edge",
                kind=("test", "analysis_batch"),
                identifier="batch",
                context={},
            ),
            event_identifier="batch-enqueued",
            enqueued_at_utc_ns=100,
        )
        for index, definition in enumerate((failed_definition, pending_definition)):
            await database.enqueue_work(
                definition,
                WorkRequester(
                    request_identifier=f"analysis-request-{index}",
                    kind=("carl", "facebook", "analysis_batch"),
                    identifier="batch",
                    context={"listing_observation_record_identifier": f"observation-{index}"},
                ),
                event_identifier=f"analysis-enqueued-{index}",
                enqueued_at_utc_ns=110 + index,
            )
        claimed = await database.claim_work(
            supported_capabilities=failure_registry.capabilities,
            worker_identifier="analysis-worker",
            lease_token="analysis-lease",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: 200,
            event_identifier="analysis-claimed",
        )
        assert claimed.lease is not None
        identifiers = iter(f"runtime-{index}" for index in range(10))
        await execute_lease(
            database=database,
            registry=failure_registry,
            settings=WorkerSettings(
                worker_count=1,
                lease_duration_ns=1_000,
                renewal_interval_ns=500,
                idle_poll_interval_ns=10,
            ),
            services=WorkerRuntimeServices(
                new_identifier=lambda: next(identifiers),
                utc_now_ns=lambda: 200,
                monotonic_ns=lambda: 200,
                code_provenance=_async_provenance,
                invocation=lambda: {},
            ),
            lease=claimed.lease,
        )

        application = ReviewApplication(database, tmp_path, utc_now_ns=lambda: 300)
        status = await application.get_work_status("batch")
        assert status.details is None
        assert status.analysis_batch_progress is not None
        progress = status.analysis_batch_progress
        assert progress.processed_observations == 2
        assert progress.remaining_observations == 0
        assert progress.analyses.pending == 1
        assert progress.analyses.terminal_failure == 1
        assert progress.active_analysis_work_count == 1
        assert progress.active_analysis_work_identifiers == ("analysis-pending",)
        assert tuple(item.model_dump() for item in progress.failure_reason_counts) == (
            {"kind": "claude_timeout", "count": 1},
        )
        assert tuple(item.model_dump() for item in progress.recent_terminal_failures) == (
            {"work_identifier": "analysis-failed", "kind": "claude_timeout"},
        )
        detailed = await application.get_work_status("batch", include_details=True)
        assert detailed.details is not None
        assert detailed.details.payload["request_identifier"] == "batch-request"


async def _async_provenance() -> CodeProvenance:
    return _provenance()
