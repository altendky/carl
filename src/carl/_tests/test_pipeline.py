"""Pipeline acceptance, scope freezing, incremental discovery, and recovery."""

from collections.abc import Callable
from pathlib import Path
from time import time_ns
from typing import TypedDict, cast
from uuid import uuid4

import anyio
import pytest

from carl._tests.test_ebay_item_workers import _async_provenance
from carl._tests.test_pipeline_search_occurrences import _attempt, _card, _checkpoint, _publish
from carl.core.ebay import EbaySearchRequest
from carl.core.json import encode_json
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    SetMarketplaceSearchTargetEnabledRequest,
)
from carl.core.models import JsonValue, RecordDraft
from carl.core.pipeline import (
    LISTING_PIPELINE_WORK_KIND,
    SEARCH_PIPELINE_INTENT_KIND,
    SEARCH_PIPELINE_WORK_KIND,
    NewSearchPipelineSource,
    PipelineListingPayload,
    PipelineOptions,
    PipelineStage,
    RequestSearchPipelinePayload,
    RequestSearchPipelineRequest,
    SearchPipelineSource,
    SearchWorkPipelineSource,
    WorkspacePipelineSource,
)
from carl.core.review_workspace import CreateReviewWorkspaceRequest
from carl.core.work import WorkDefinition, WorkState
from carl.core.worker import AttemptContext, RetryWork, TerminalFailureWork
from carl.io.sqlite import Database
from carl.pipeline_workers import PipelineWorkerDependencies, _run_pipeline
from carl.review import ReviewApplication, ReviewInputError


def _application(database: Database, path: Path) -> ReviewApplication:
    return ReviewApplication(database, path, code_provenance=_async_provenance)


def _request(
    *works: str, identifier: str = "intent", **options: object
) -> RequestSearchPipelineRequest:
    return RequestSearchPipelineRequest(
        request_identifier=identifier,
        source=SearchWorkPipelineSource(search_work_identifiers=works),
        options=PipelineOptions.model_validate_json(
            encode_json(cast(JsonValue, {"stop_after": "images", **options}))
        ),
    )


async def _advance(database: Database, work_identifier: str) -> dict[str, JsonValue]:
    work = await database.work(work_identifier)
    payload = RequestSearchPipelinePayload.model_validate_json(encode_json(work["payload"]))
    outcome = await _run_pipeline(
        payload,
        AttemptContext(
            work_item_identifier=work_identifier,
            lease_token="test",
            worker_identifier="test",
            operation_identifier="test",
            attempt=1,
        ),
        PipelineWorkerDependencies(database),
    )
    async with database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
        await connection.execute(
            "UPDATE work_items SET result_json=? WHERE id=?",
            (encode_json(outcome.result), work_identifier),
        )
    assert isinstance(outcome.result, dict)
    return outcome.result


async def _settle(
    database: Database, context: AttemptContext, *, failed: bool = False, run: str | None = None
) -> None:
    kwargs = _FinalizationKwargs(
        work_item_identifier=context.work_item_identifier,
        lease_token=context.lease_token,
        worker_identifier=context.worker_identifier,
        utc_now_ns=time_ns,
        event_identifier=str(uuid4()),
        operation_id=context.operation_identifier,
        ended_at_utc="2026-10-06T00:00:01+00:00",
        duration_ns=0,
    )
    if failed:
        await database.terminally_fail_leased_operation(
            **kwargs, error={"kind": "search_failed"}, result={"state": "failed"}
        )
    else:
        await database.complete_leased_operation(
            **kwargs,
            records=(),
            artifacts=(),
            outputs=(),
            result={"search_run_record_identifier": run} if run else {},
        )


class _FinalizationKwargs(TypedDict):
    work_item_identifier: str
    lease_token: str
    worker_identifier: str
    utc_now_ns: Callable[[], int]
    event_identifier: str
    operation_id: str
    ended_at_utc: str
    duration_ns: int


@pytest.mark.anyio
async def test_request_replay_freezes_scope_and_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "pipeline.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        app = _application(database, tmp_path)
        group = await app.create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=(EbaySearchTargetSpecification(search=EbaySearchRequest(query="eyepiece")),)
            )
        )
        request = RequestSearchPipelineRequest(
            request_identifier="replay",
            source=SearchPipelineSource(search_record_identifier=group.record_identifier),
            options=PipelineOptions(stop_after=PipelineStage.DETAILS),
        )
        first = await app.request_search_pipeline(request)
        assert first.created
        await app.set_marketplace_search_target_enabled(
            SetMarketplaceSearchTargetEnabledRequest(
                search_record_identifier=group.record_identifier,
                target_record_identifier=group.targets[0].record_identifier,
                enabled=False,
            )
        )
        replay = await app.request_search_pipeline(request)
        assert not replay.created
        assert replay.work_identifier == first.work_identifier
        assert replay.search_work_identifiers == first.search_work_identifiers
        async with database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
            await connection.execute(
                "UPDATE work_items SET state='completed' WHERE id=?", (first.work_identifier,)
            )
    async with Database.managed(path) as database:
        app = _application(database, tmp_path)
        assert not (await app.request_search_pipeline(request)).created
        with pytest.raises(ReviewInputError, match="different content"):
            await app.request_search_pipeline(
                request.model_copy(
                    update={"options": PipelineOptions(stop_after=PipelineStage.IMAGES)}
                )
            )


@pytest.mark.anyio
async def test_new_search_and_pipeline_acceptance_are_atomic_and_concurrent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        app = _application(database, tmp_path)
        request = RequestSearchPipelineRequest(
            request_identifier="new-search",
            source=NewSearchPipelineSource(
                targets=(EbaySearchTargetSpecification(search=EbaySearchRequest(query="scope")),)
            ),
            options=PipelineOptions(stop_after=PipelineStage.DETAILS),
        )
        original = Database.enqueue_work

        async def fail_pipeline(
            self: Database, definition: WorkDefinition, *args: object, **kwargs: object
        ):
            if definition.kind == SEARCH_PIPELINE_WORK_KIND:
                raise RuntimeError("injected pipeline enqueue failure")
            return await original(self, definition, *args, **kwargs)  # pyright: ignore[reportArgumentType]

        with monkeypatch.context() as patch:
            patch.setattr(Database, "enqueue_work", fail_pipeline)
            with pytest.raises(RuntimeError, match="injected"):
                await app.request_search_pipeline(request)
        assert await database.records_by_kind(("carl", "marketplace", "search")) == ()
        assert await database.records_by_kind(SEARCH_PIPELINE_INTENT_KIND) == ()
        results = []

        async def accept() -> None:
            results.append(await app.request_search_pipeline(request))

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(accept)
            tasks.start_soon(accept)
        assert sum(result.created for result in results) == 1
        assert len({result.work_identifier for result in results}) == 1
        assert len(await database.records_by_kind(("carl", "marketplace", "search"))) == 1
        assert len(await database.records_by_kind(SEARCH_PIPELINE_INTENT_KIND)) == 1


@pytest.mark.anyio
async def test_incremental_mixed_discovery_has_global_budgets_and_backpressure(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        facebook = await _attempt(database, "facebook", "facebook")
        ebay = await _attempt(database, "ebay", "ebay")
        # The two sources deliberately share a numeric external identity.
        await _checkpoint(
            database,
            facebook,
            (_card("fb", "facebook", "fb-run"), _card("fb-duplicate", "facebook", "fb-run")),
        )
        await _checkpoint(
            database,
            ebay,
            (
                RecordDraft(
                    identifier="binding",
                    kind=("carl", "ebay", "search_attempt"),
                    schema_version=1,
                    value={"search_run_record_identifier": "ebay-run"},
                ),
            ),
        )
        await _publish(database, (_card("ebay", "ebay", "ebay-run"),))
        app = _application(database, tmp_path)
        requested = await app.request_search_pipeline(
            _request(
                "facebook",
                "ebay",
                maximum_images=3,
                maximum_images_per_listing=2,
                maximum_inflight_listings=1,
            )
        )
        first = await _advance(database, requested.work_identifier)
        assert first["selected_listings"] == 1 and first["image_budget_reserved"] == 2
        assert (await database.work("facebook"))["state"] == "leased"
        assert (await _advance(database, requested.work_identifier))["selected_listings"] == 1
        assert isinstance(first["listings"], list)
        entry = first["listings"][0]
        assert isinstance(entry, dict)
        async with database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
            await connection.execute(
                "UPDATE work_items SET state='completed', result_json=? WHERE id=?",
                (
                    encode_json(
                        {"stage": "complete", "item_completed": True, "images_completed": True}
                    ),
                    entry["work_identifier"],
                ),
            )
        second = await _advance(database, requested.work_identifier)
        assert second["selected_listings"] == 1  # round-robin FB duplicate only
        third = await _advance(database, requested.work_identifier)
        assert third["selected_listings"] == 2
        assert third["image_budget_reserved"] == 3
        status = await app.get_search_pipeline(requested.work_identifier)
        assert status.items_completed == status.galleries_completed == 1
        assert {progress.marketplace.value for progress in status.listing_progress} == {
            "facebook",
            "ebay",
        }
        assert status.searches_pending == 2


@pytest.mark.anyio
async def test_late_legacy_ebay_cards_are_not_lost_behind_facebook_cursor(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        facebook = await _attempt(database, "facebook", "facebook")
        ebay = await _attempt(database, "legacy-ebay", "ebay")
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="ebay-run",
                    kind=("carl", "ebay", "search_run"),
                    schema_version=1,
                    value={},
                ),
                _card("old-ebay-card", "ebay", "ebay-run"),
            ),
        )
        await _checkpoint(database, facebook, (_card("newer-fb-card", "facebook", "fb-run"),))
        app = _application(database, tmp_path)
        requested = await app.request_search_pipeline(_request("facebook", "legacy-ebay"))
        assert (await _advance(database, requested.work_identifier))["selected_listings"] == 1
        assert (await _advance(database, requested.work_identifier))["selected_listings"] == 1
        await _settle(database, ebay, run="ebay-run")
        await _advance(database, requested.work_identifier)
        assert (await _advance(database, requested.work_identifier))["selected_listings"] == 2


@pytest.mark.anyio
async def test_root_recovers_child_enqueue_without_checkpoint_and_surfaces_search_failure(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        source = await _attempt(database, "source", "facebook")
        await _checkpoint(database, source, (_card("card", "facebook", "run"),))
        app = _application(database, tmp_path)
        requested = await app.request_search_pipeline(_request("source"))
        root = await database.work(requested.work_identifier)
        payload = RequestSearchPipelinePayload.model_validate_json(encode_json(root["payload"]))
        context = AttemptContext(
            work_item_identifier=requested.work_identifier,
            lease_token="unused",
            worker_identifier="unused",
            operation_identifier="unused",
            attempt=1,
        )
        first = await _run_pipeline(payload, context, PipelineWorkerDependencies(database))
        second = await _run_pipeline(payload, context, PipelineWorkerDependencies(database))
        assert isinstance(first, RetryWork) and first.result == second.result
        assert isinstance(first.result, dict) and isinstance(first.result["listings"], list)
        entry = first.result["listings"][0]
        assert isinstance(entry, dict)
        await _advance(database, requested.work_identifier)
        await _settle(database, source, failed=True)
        async with database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
            await connection.execute(
                "UPDATE work_items SET state='completed' WHERE id=?", (entry["work_identifier"],)
            )
        completed = await _run_pipeline(payload, context, PipelineWorkerDependencies(database))
        assert isinstance(completed, TerminalFailureWork)
        assert isinstance(completed.result, dict) and completed.result["selected_listings"] == 1
        assert completed.result["search_failures"] == 1
        assert len(await database.pipeline_work_rows((str(entry["work_identifier"]),))) == 1


@pytest.mark.anyio
async def test_filters_and_scan_limit_apply_before_item_acquisition(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        source = await _attempt(database, "source", "facebook")
        await _checkpoint(database, source, (_card("card", "facebook", "run"),))
        app = _application(database, tmp_path)
        requested = await app.request_search_pipeline(
            _request(
                "source", exclude_title_keywords=["scope"], maximum_candidate_listings_examined=1
            )
        )
        result = await _advance(database, requested.work_identifier)
        assert result["selected_listings"] == 0 and result["candidates_examined"] == 1
        assert result["budget_exhausted"] is True
        assert (
            await database.requested_work_identifiers(
                requester_kind=("carl", "marketplace", "pipeline_listing"),
                requester_identifier=requested.work_identifier,
            )
            == ()
        )


@pytest.mark.anyio
async def test_workspace_pipeline_is_visible_without_refreshing_or_advancing_tracks(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        app = _application(database, tmp_path)
        group = await app.create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=(EbaySearchTargetSpecification(search=EbaySearchRequest(query="scope")),)
            )
        )
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Pipeline", search_run_record_identifier=group.record_identifier
            )
        )
        response = await app.request_search_pipeline(
            RequestSearchPipelineRequest(
                request_identifier="workspace",
                source=WorkspacePipelineSource(
                    workspace_record_identifier=workspace.record_identifier
                ),
                options=PipelineOptions(stop_after=PipelineStage.DETAILS),
            )
        )
        active = await app.get_workspace_work_status(workspace.record_identifier)
        assert any(work.identifier == response.work_identifier for work in active.active_work)
        latest = await app.get_review_workspace(workspace.record_identifier)
        assert all(track.latest_refresh_work_identifier is None for track in latest.search_tracks)
        with pytest.raises(ReviewInputError, match="exact Facebook/eBay search"):
            await app.request_search_pipeline(
                _request(response.work_identifier, identifier="bad-scope")
            )


@pytest.mark.anyio
async def test_workspace_uses_retained_current_run_after_failed_refresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        app = _application(database, tmp_path)
        group = await app.create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=(EbaySearchTargetSpecification(search=EbaySearchRequest(query="scope")),)
            )
        )
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Pipeline", search_run_record_identifier=group.record_identifier
            )
        )
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="last-successful-run",
                    kind=("carl", "ebay", "search_run"),
                    schema_version=1,
                    value={},
                ),
            ),
        )
        track = workspace.search_tracks[0].model_copy(
            update={
                "current_search_run_record_identifier": "last-successful-run",
                "latest_refresh_work_state": WorkState.TERMINAL_FAILURE,
            }
        )
        current = workspace.model_copy(update={"search_tracks": (track,)})

        async def get_workspace(_application: ReviewApplication, _identifier: str):
            return current

        monkeypatch.setattr(ReviewApplication, "get_review_workspace", get_workspace)
        response = await app.request_search_pipeline(
            RequestSearchPipelineRequest(
                request_identifier="retained-workspace",
                source=WorkspacePipelineSource(
                    workspace_record_identifier=workspace.record_identifier
                ),
                options=PipelineOptions(stop_after=PipelineStage.DETAILS),
            )
        )
        assert response.search_work_identifiers == ()
        assert response.search_run_record_identifiers == ("last-successful-run",)


@pytest.mark.anyio
@pytest.mark.parametrize("source_kind", ["retained_run", "legacy_work"])
@pytest.mark.parametrize("stop_reason_only", [False, True])
async def test_failed_collection_is_not_successful_empty_pipeline(
    tmp_path: Path, source_kind: str, stop_reason_only: bool
) -> None:
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        app = _application(database, tmp_path)
        failed = (
            {"stopping_reason": "challenge"}
            if stop_reason_only
            else {
                "response_classification": {"kind": "error_page", "evidence": ["http_status_403"]}
            }
        )
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="failed-run",
                    kind=("carl", "ebay", "search_run"),
                    schema_version=1,
                    value=failed,
                ),
            ),
        )
        if source_kind == "retained_run":
            request = RequestSearchPipelineRequest(
                request_identifier="failed",
                source=SearchPipelineSource(search_record_identifier="failed-run"),
                options=PipelineOptions(stop_after=PipelineStage.IMAGES),
            )
        else:
            context = await _attempt(database, "legacy", "ebay")
            await _settle(database, context, run="failed-run")
            async with database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
                await connection.execute(
                    "UPDATE work_items SET result_json=? WHERE id='legacy'",
                    (encode_json({**failed, "search_run_record_identifier": "failed-run"}),),
                )
            request = _request("legacy")
        response = await app.request_search_pipeline(request)
        work = await database.work(response.work_identifier)
        outcome = await _run_pipeline(
            RequestSearchPipelinePayload.model_validate_json(encode_json(work["payload"])),
            AttemptContext(
                work_item_identifier=response.work_identifier,
                lease_token="test",
                worker_identifier="test",
                operation_identifier="test",
                attempt=1,
            ),
            PipelineWorkerDependencies(database),
        )
        assert isinstance(outcome, TerminalFailureWork)
        async with database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
            await connection.execute(
                "UPDATE work_items SET state='terminal_failure',result_json=? WHERE id=?",
                (encode_json(outcome.result), response.work_identifier),
            )
        status = await app.get_search_pipeline(response.work_identifier)
        assert (
            status.successful is False
            and status.searches_failed == 1
            and status.searches_completed == 0
        )
        assert status.search_run_record_identifiers == ("failed-run",)


@pytest.mark.anyio
async def test_partial_child_failures_propagate_to_pipeline_status(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        source = await _attempt(database, "source", "facebook")
        await _checkpoint(database, source, (_card("card", "facebook", "run"),))
        app = _application(database, tmp_path)
        response = await app.request_search_pipeline(_request("source"))
        result = await _advance(database, response.work_identifier)
        assert isinstance(result["listings"], list) and isinstance(result["listings"][0], dict)
        child_identifier = result["listings"][0]["work_identifier"]
        await _settle(database, source)
        async with database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
            await connection.execute(
                "UPDATE work_items SET state='completed', result_json=? WHERE id=?",
                (
                    encode_json(
                        {
                            "stage": "complete",
                            "state": "completed_with_failures",
                            "item_completed": True,
                            "images_completed": True,
                        }
                    ),
                    child_identifier,
                ),
            )
        outcome = await _advance(database, response.work_identifier)
        assert outcome["state"] == "completed_with_failures"
        async with database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
            await connection.execute(
                "UPDATE work_items SET state='completed' WHERE id=?", (response.work_identifier,)
            )
        status = await app.get_search_pipeline(response.work_identifier)
        assert status.successful is False and status.failed_listings == 1


@pytest.mark.anyio
async def test_known_card_status_excludes_paid_details(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        source = await _attempt(database, "source", "facebook")
        card = _card("card", "facebook", "run")
        assert isinstance(card.value, dict)
        card = card.model_copy(
            update={
                "value": {
                    **card.value,
                    "original": {"marketplace_listing_title": "Scope", "is_sold": True},
                }
            }
        )
        await _checkpoint(database, source, (card,))
        app = _application(database, tmp_path)
        response = await app.request_search_pipeline(_request("source"))
        assert (await _advance(database, response.work_identifier))["selected_listings"] == 0


@pytest.mark.anyio
@pytest.mark.parametrize("listing_state", ["sold", "completed"])
async def test_known_closed_ebay_card_excludes_paid_details(
    tmp_path: Path, listing_state: str
) -> None:
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        source = await _attempt(database, "source", "ebay")
        await _checkpoint(
            database,
            source,
            (
                RecordDraft(
                    identifier="binding",
                    kind=("carl", "ebay", "search_attempt"),
                    schema_version=1,
                    value={"search_run_record_identifier": "run"},
                ),
            ),
        )
        card = _card("card", "ebay", "run")
        assert isinstance(card.value, dict)
        await _publish(
            database,
            (card.model_copy(update={"value": {**card.value, "listing_state": listing_state}}),),
        )
        app = _application(database, tmp_path)
        response = await app.request_search_pipeline(_request("source"))
        result = await _advance(database, response.work_identifier)
        assert result["candidates_examined"] == 1 and result["selected_listings"] == 0


@pytest.mark.anyio
async def test_reused_history_does_not_consume_new_listing_analysis_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def guide_valid(_application: ReviewApplication, _identifier: str) -> None:
        pass

    async def has_history(
        _database: Database,
        *,
        marketplace: str,
        external_identifier: str,
        product_guide_record_identifier: str | None = None,
    ) -> bool:
        assert marketplace == "facebook" and product_guide_record_identifier == "guide"
        return external_identifier != "987654321"

    monkeypatch.setattr(ReviewApplication, "_active_product_guide", guide_valid)
    monkeypatch.setattr(Database, "pipeline_completed_analysis_exists", has_history)
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        source = await _attempt(database, "source", "facebook")
        first = _card("first", "facebook", "run")
        second = _card("second", "facebook", "run")
        assert isinstance(second.value, dict)
        second = second.model_copy(
            update={"value": {**second.value, "listing_identifier": "987654321"}}
        )
        await _checkpoint(database, source, (first, second))
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="guide",
                    kind=("carl", "analysis", "product_guide"),
                    schema_version=1,
                    value={},
                ),
            ),
        )
        app = _application(database, tmp_path)
        response = await app.request_search_pipeline(
            _request(
                "source",
                stop_after="analysis",
                product_guide_record_identifier="guide",
                selection_policy="missing_for_selected_guide",
                maximum_analyses=1,
            )
        )
        result = await _advance(database, response.work_identifier)
        assert result["selected_listings"] == 2
        assert result["analysis_budget_reserved"] == 1
        assert result["budget_exhausted"] is False
        assert isinstance(result["listings"], list)
        authorized = []
        for entry in result["listings"]:
            assert isinstance(entry, dict)
            work = await database.work(str(entry["work_identifier"]))
            payload = PipelineListingPayload.model_validate_json(encode_json(work["payload"]))
            authorized.append(payload.analysis_authorized)
        assert authorized == [False, True]


@pytest.mark.anyio
async def test_ready_listing_is_analyzed_while_search_and_other_listing_remain_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from carl._tests.test_ebay_item_workers import _identifiers, _run_next
    from carl._tests.test_pipeline_listing_workers import _ANALYSIS, _DETAILS, _Application
    from carl.io.worker import WorkHandlerRegistry
    from carl.pipeline_workers import build_pipeline_worker_registry

    async def guide_valid(_application: ReviewApplication, _identifier: str) -> None:
        pass

    monkeypatch.setattr(ReviewApplication, "_active_product_guide", guide_valid)
    monkeypatch.setattr("carl.pipeline_workers._WAIT_NS", 0)
    monkeypatch.setattr("carl.pipeline_listing_workers._WAIT_NS", 0)
    async with Database.managed(tmp_path / "pipeline.sqlite3", initialize=True) as database:
        source = await _attempt(database, "source", "facebook")
        cards = (_card("first", "facebook", "run"), _card("second", "facebook", "run"))
        assert isinstance(cards[1].value, dict)
        cards = (
            cards[0],
            cards[1].model_copy(
                update={"value": {**cards[1].value, "listing_identifier": "987654321"}}
            ),
        )
        await _checkpoint(database, source, cards)
        # Guide input is an immutable retained record; only this test's guide validation is stubbed.
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="guide",
                    kind=("carl", "analysis", "product_guide"),
                    schema_version=1,
                    value={},
                ),
            ),
        )
        app = _application(database, tmp_path)
        response = await app.request_search_pipeline(
            _request(
                "source",
                stop_after="analysis",
                product_guide_record_identifier="guide",
                maximum_items=2,
            )
        )
        identifiers = _identifiers()
        listing_application = _Application(database, identifiers)
        registry = WorkHandlerRegistry(
            handlers=(
                *build_pipeline_worker_registry(PipelineWorkerDependencies(database)).handlers,
                *listing_application.registry().handlers,
            )
        )
        await _run_next(database, registry, identifiers, SEARCH_PIPELINE_WORK_KIND)
        await _run_next(database, registry, identifiers, LISTING_PIPELINE_WORK_KIND)
        await _run_next(database, registry, identifiers, _DETAILS)
        # Both coordinator jobs were selected at once. Run them until the first
        # analysis is queued, leaving the second detail job deliberately pending.
        for _ in range(4):
            await _run_next(database, registry, identifiers, LISTING_PIPELINE_WORK_KIND)
        assert len(listing_application.analysis_requests) == 1
        await _run_next(database, registry, identifiers, _ANALYSIS)
        for _ in range(2):
            await _run_next(database, registry, identifiers, LISTING_PIPELINE_WORK_KIND)
        status = await app.get_search_pipeline(response.work_identifier)
        assert status.analyses_completed == 1 and status.searches_pending == 1
        assert status.selected_listings == 2 and status.state.value == "pending"
