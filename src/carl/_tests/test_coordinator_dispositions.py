"""Coordinator progress sleeps preserve runnable discovery and local work."""

# pyright: reportPrivateUsage=false

from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest

from carl._tests.test_coordinator_stages import _Handler, _seed
from carl._tests.test_pipeline import _application, _request, _settle
from carl._tests.test_pipeline_search_occurrences import _attempt, _card, _checkpoint
from carl._tests.test_stage_execution import _admit
from carl.coordinator_progress import consumer_progress_watch
from carl.coordinator_stages import build_coordinator_stage_registry
from carl.core.analysis_batch import REQUEST_MISSING_ANALYSES_WORK_KIND
from carl.core.facebook_listing import REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND
from carl.core.facebook_refresh import REFRESH_SEARCH_WORK_KIND
from carl.core.json import encode_json
from carl.core.models import JsonValue, RecordDraft
from carl.core.pipeline import RequestSearchPipelinePayload
from carl.core.stages import StageWaitKind, WaitWork
from carl.core.work import WorkCapability, WorkRequester
from carl.core.work_progress import WorkProgressWatch
from carl.core.worker import AttemptContext, RetryWork, WorkOutcome
from carl.io.sqlite import Database
from carl.io.stages import stage_manager
from carl.io.worker import WorkHandlerRegistry
from carl.pipeline_workers import PipelineWorkerDependencies, _run_pipeline

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _pipeline(database: Database, identifier: str) -> WorkOutcome:
    work = await database.work(identifier)
    outcome = await _run_pipeline(
        RequestSearchPipelinePayload.model_validate_json(encode_json(work["payload"])),
        AttemptContext(
            work_item_identifier=identifier,
            lease_token="test",
            worker_identifier="test",
            operation_identifier="test",
            attempt=1,
        ),
        PipelineWorkerDependencies(database),
    )
    async with database._connections.writer() as connection:
        await connection.execute(
            "UPDATE work_items SET result_json=? WHERE id=?",
            (encode_json(outcome.result), identifier),
        )
    return outcome


def _occurrence(identifier: str, *, title: str = "Scope", run: str = "run") -> RecordDraft:
    card = _card(identifier, "facebook", run)
    assert isinstance(card.value, dict)
    return card.model_copy(
        update={
            "value": {
                **card.value,
                "listing_identifier": identifier,
                "original": {"marketplace_listing_title": title},
            }
        }
    )


@pytest.mark.parametrize(
    ("kind", "reason"),
    (
        (REFRESH_SEARCH_WORK_KIND, "continue_refresh"),
        (REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND, "continue_listing_details"),
        (REQUEST_MISSING_ANALYSES_WORK_KIND, "analysis_batch_continuation"),
    ),
)
async def test_known_local_control_has_no_poll_delay(
    tmp_path: Path, kind: tuple[str, ...], reason: str
) -> None:
    async def action(_payload: JsonValue, _context: AttemptContext) -> WorkOutcome:
        return RetryWork(delay_ns=5_000_000_000, reason={"kind": reason}, result={})

    clock = time_ns()
    capability = WorkCapability(kind=kind, payload_schema_version=1)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_coordinator_stage_registry(
            database, WorkHandlerRegistry((_Handler(capability, action),)), utc_now_ns=lambda: clock
        )
        stage = await _seed(database, registry, capability)
        manager = stage_manager(database)
        async with _admit(manager, stage) as admission:
            assert admission.execution is not None
            outcome = await registry.run(admission.execution)
            assert isinstance(outcome, WaitWork)
            if outcome.wait_kind is StageWaitKind.RESOURCE:
                await admission.execution.apply(outcome)
                stage = outcome.continuation
            else:
                assert outcome.continuation.not_before_utc_ns == clock
                assert outcome.progress_wait is None
                return
        async with _admit(manager, stage) as admission:
            assert admission.execution is not None
            outcome = await registry.run(admission.execution)
            assert isinstance(outcome, WaitWork)
            assert outcome.continuation.not_before_utc_ns == clock
            assert outcome.progress_wait is None


async def test_adapter_captures_before_handler_and_preserves_prior_probe_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[int] = []
    getter = Database.work_progress_watermark

    async def watermark(database: Database) -> int:
        revision = await getter(database)
        captured.append(revision)
        return revision

    monkeypatch.setattr(Database, "work_progress_watermark", watermark)

    async def action(_payload: JsonValue, _context: AttemptContext) -> WorkOutcome:
        assert captured
        return RetryWork(
            delay_ns=5_000_000_000,
            progress_watch=WorkProgressWatch(work_identifiers=("source",)),
            progress_observed_through_revision=0,
            reason={"kind": "waiting_for_search_collection"},
            result={"stage": "collecting_search"},
        )

    capability = WorkCapability(kind=REFRESH_SEARCH_WORK_KIND, payload_schema_version=1)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _ = await _attempt(database, "source", "facebook")
        registry = build_coordinator_stage_registry(
            database, WorkHandlerRegistry((_Handler(capability, action),))
        )
        stage = await _seed(database, registry, capability)
        async with _admit(stage_manager(database), stage) as admission:
            assert admission.execution is not None
            outcome = await registry.run(admission.execution)
            assert isinstance(outcome, WaitWork)
            assert outcome.wait_kind is StageWaitKind.DEPENDENCY
            assert outcome.progress_wait is not None
            assert outcome.progress_wait.observed_through_revision == 0
            assert outcome.progress_wait.watch.work_identifiers == ("source",)


async def test_group_watch_keeps_unknown_retained_consumer_as_explicit_fallback(
    tmp_path: Path,
) -> None:
    kind = ("retained", "opaque", "consumer")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier in ("parent", "known", "unknown"):
            _ = await _attempt(database, identifier, "facebook")
        for child, parent in (("known", "parent"), ("unknown", None)):
            await database.attach_work_request(
                child,
                WorkRequester(
                    parent_work_identifier=parent,
                    request_identifier=str(uuid4()),
                    kind=kind,
                    identifier="opaque-request-identity",
                    context={},
                ),
                event_identifier=str(uuid4()),
                requested_at_utc_ns=time_ns(),
            )
        watch = await consumer_progress_watch(
            database,
            parent_work_identifier="parent",
            requester_kind=kind,
            work_identifiers=("known", "unknown", "unknown"),
        )
        assert watch.consumer_requester_kinds == (kind,)
        assert watch.work_identifiers == ("unknown",)


async def test_consumer_fallback_metadata_reads_are_bounded() -> None:
    calls: list[tuple[str, ...]] = []

    class Metadata:
        async def missing_consumer_edges(
            self,
            parent_work_identifier: str,
            requester_kind: tuple[str, ...],
            child_ids: tuple[str, ...],
        ) -> tuple[str, ...]:
            assert parent_work_identifier == "parent" and requester_kind == ("test",)
            calls.append(child_ids)
            return tuple(identifier for identifier in child_ids if identifier in {"0", "1004"})

    watch = await consumer_progress_watch(
        Metadata(),
        parent_work_identifier="parent",
        requester_kind=("test",),
        work_identifiers=tuple(str(index) for index in range(1005)),
    )
    assert tuple(map(len, calls)) == (1000, 5)
    assert watch.work_identifiers == ("0", "1004")


async def test_retained_row_51_after_filtered_page_is_runnable_while_source_leased(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        source = await _attempt(database, "source", "facebook")
        await _checkpoint(
            database,
            source,
            (
                *(_occurrence(str(index)) for index in range(50)),
                _occurrence("last", title="Target"),
            ),
        )
        requested = await _application(database, tmp_path).request_search_pipeline(
            _request("source", title_contains="Target")
        )
        first = await _pipeline(database, requested.work_identifier)
        assert isinstance(first, RetryWork)
        assert first.delay_ns == 0 and first.progress_watch is None
        assert isinstance(first.result, dict)
        assert first.result["candidates_examined"] == 50
        second = await _pipeline(database, requested.work_identifier)
        assert isinstance(second.result, dict)
        assert second.result["selected_listings"] == 1
        assert second.result["candidates_examined"] == 51
        assert (await database.work("source"))["state"] == "leased"


async def test_full_window_waits_on_listing_children_despite_retained_source_backlog(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        source = await _attempt(database, "source", "facebook")
        await _checkpoint(database, source, (_occurrence("first"), _occurrence("second")))
        requested = await _application(database, tmp_path).request_search_pipeline(
            _request("source", maximum_inflight_listings=1)
        )
        outcome = await _pipeline(database, requested.work_identifier)
        assert isinstance(outcome, RetryWork)
        assert outcome.delay_ns > 0 and outcome.progress_watch is not None
        assert outcome.progress_watch.work_identifiers == ()
        assert outcome.progress_watch.consumer_requester_kinds == (
            ("carl", "marketplace", "pipeline_listing"),
        )
        assert isinstance(outcome.result, dict)
        assert "discovery_probe" not in outcome.result


async def test_other_sources_retained_backlog_selects_runnable_source(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _ = await _attempt(database, "empty", "facebook")
        full = await _attempt(database, "full", "facebook")
        await _checkpoint(database, full, (_occurrence("card"),))
        requested = await _application(database, tmp_path).request_search_pipeline(
            _request("empty", "full")
        )
        first = await _pipeline(database, requested.work_identifier)
        assert isinstance(first, RetryWork)
        assert first.delay_ns == 0 and first.progress_watch is None
        assert isinstance(first.result, dict)
        assert first.result["next_source_index"] == 1
        second = await _pipeline(database, requested.work_identifier)
        assert isinstance(second.result, dict)
        assert second.result["selected_listings"] == 1


async def test_listing_slot_opened_during_handler_keeps_retained_discovery_runnable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        source = await _attempt(database, "source", "facebook")
        await _checkpoint(database, source, (_occurrence("first"), _occurrence("second")))
        requested = await _application(database, tmp_path).request_search_pipeline(
            _request("source", maximum_inflight_listings=1)
        )
        first = await _pipeline(database, requested.work_identifier)
        assert isinstance(first.result, dict)
        entries = first.result["listings"]
        assert isinstance(entries, list) and isinstance(entries[0], dict)
        child = entries[0]["work_identifier"]
        assert isinstance(child, str)
        read = database.pipeline_work_rows
        reads = 0

        async def work_rows(identifiers: tuple[str, ...]) -> tuple[dict[str, JsonValue], ...]:
            nonlocal reads
            if identifiers == (child,):
                reads += 1
                if reads == 2:
                    async with database._connections.writer() as connection:
                        await connection.execute(
                            "UPDATE work_items SET state='completed',result_json='{}' WHERE id=?",
                            (child,),
                        )
            return await read(identifiers)

        monkeypatch.setattr(database, "pipeline_work_rows", work_rows)
        outcome = await _pipeline(database, requested.work_identifier)
        assert reads == 2
        assert isinstance(outcome, RetryWork)
        assert outcome.delay_ns == 0 and outcome.progress_watch is None


async def test_multipass_probe_rechecks_early_source_final_publication(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        contexts = [await _attempt(database, f"source-{index}", "facebook") for index in range(33)]
        requested = await _application(database, tmp_path).request_search_pipeline(
            _request(*(context.work_item_identifier for context in contexts))
        )
        first = await _pipeline(database, requested.work_identifier)
        assert isinstance(first, RetryWork) and first.delay_ns == 0
        assert isinstance(first.result, dict)
        probe = first.result["discovery_probe"]
        assert isinstance(probe, dict) and probe["remaining_sources"] == 1
        await _checkpoint(database, contexts[2], (_occurrence("late"),))
        for context in contexts:
            await _settle(database, context)
        # This invocation discovers source1 and probes source0. Source2's final
        # publication must force another pass rather than premature completion.
        second = await _pipeline(database, requested.work_identifier)
        assert isinstance(second, RetryWork)
        assert second.delay_ns == 0 and second.progress_watch is None
        third = await _pipeline(database, requested.work_identifier)
        assert isinstance(third.result, dict)
        assert third.result["selected_listings"] == 1
