"""Acceptance, replay, and retained progress for explicit marketplace pipelines."""

# The application delegates to this adapter; the reverse import is typing-only.
# pyright: reportImportCycles=false

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from uuid import NAMESPACE_URL, uuid5

from carl.core.ebay import COLLECT_EBAY_SEARCH_WORK_KIND
from carl.core.facebook_work import COLLECT_SEARCH_WORK_KIND
from carl.core.json import encode_json
from carl.core.marketplace_search import MARKETPLACE_SEARCH_KIND, CreateMarketplaceSearchRequest
from carl.core.models import JsonValue, NamedInput, NamedOutput, RecordDraft
from carl.core.pipeline import (
    REQUEST_SEARCH_PIPELINE,
    SEARCH_PIPELINE_INTENT_KIND,
    SEARCH_PIPELINE_WORK_KIND,
    NewSearchPipelineSource,
    PipelineListingProgress,
    RequestSearchPipelinePayload,
    RequestSearchPipelineRequest,
    SearchPipelineRequestResult,
    SearchPipelineSource,
    SearchPipelineStatus,
    SearchWorkPipelineSource,
    WorkspacePipelineSource,
    build_pipeline_component_registry,
    pipeline_collection_failed,
    pipeline_request_sha256,
    pipeline_work_constraints,
    search_pipeline_work,
)
from carl.core.review_errors import ReviewInputError
from carl.core.work import WorkRequester, WorkState
from carl.io.provenance import process_invocation

if TYPE_CHECKING:
    from carl.review import ReviewApplication


def _mapping(value: JsonValue | None) -> dict[str, JsonValue]:
    return cast(dict[str, JsonValue], value) if isinstance(value, dict) else {}


def _integer(value: JsonValue | None) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _response(
    work_identifier: str, payload: RequestSearchPipelinePayload, *, created: bool
) -> SearchPipelineRequestResult:
    return SearchPipelineRequestResult(
        work_identifier=work_identifier,
        created=created,
        search_record_identifier=payload.search_record_identifier,
        workspace_record_identifier=payload.workspace_record_identifier,
        search_work_identifiers=payload.search_work_identifiers,
        search_run_record_identifiers=payload.search_run_record_identifiers,
        product_guide_record_identifier=payload.options.product_guide_record_identifier,
    )


async def request_search_pipeline(
    application: ReviewApplication, request: RequestSearchPipelineRequest
) -> SearchPipelineRequestResult:
    database = application.database
    work_identifier = str(
        uuid5(NAMESPACE_URL, f"carl:search_pipeline:{request.request_identifier}")
    )
    intent_identifier = str(
        uuid5(NAMESPACE_URL, f"carl:search_pipeline_intent:{request.request_identifier}")
    )
    request_hash = pipeline_request_sha256(request)
    # Resolve mutable scope only after the transactional replay check. The
    # same ID recovers the original scope even after targets/guides change.
    async with database.transaction():
        try:
            previous = await database.work(work_identifier)
        except KeyError:
            previous = None
        if previous is not None:
            if tuple(previous["kind"]) != SEARCH_PIPELINE_WORK_KIND:
                raise ReviewInputError("Pipeline request identity belongs to different work")
            accepted = RequestSearchPipelinePayload.model_validate_json(
                encode_json(previous["payload"])
            )
            if accepted.request_sha256 != request_hash:
                raise ReviewInputError(
                    "Pipeline request identifier was already used with different content"
                )
            return _response(work_identifier, accepted, created=False)
        if request.options.product_guide_record_identifier is not None:
            await application._active_product_guide(request.options.product_guide_record_identifier)  # pyright: ignore[reportPrivateUsage]
        source = request.source
        search_identifier: str | None = None
        workspace_identifier: str | None = None
        works: list[str] = []
        runs: list[str] = []
        inputs: list[NamedInput] = []
        if isinstance(source, NewSearchPipelineSource):
            search = await application.create_marketplace_search(
                CreateMarketplaceSearchRequest(targets=source.targets)
            )
            search_identifier = search.record_identifier
            works.extend(target.executions[-1].work_identifier for target in search.targets)
        elif isinstance(source, SearchPipelineSource):
            kind, _, _ = await database.get_record(source.search_record_identifier)
            if kind == MARKETPLACE_SEARCH_KIND:
                search = await application.get_marketplace_search(source.search_record_identifier)
                search_identifier = search.record_identifier
                for target in search.targets:
                    if not target.enabled:
                        continue
                    if not target.executions:
                        raise ReviewInputError("An enabled search target has no execution")
                    works.append(target.executions[-1].work_identifier)
            elif kind in (("carl", "facebook", "search_run"), ("carl", "ebay", "search_run")):
                runs.append(source.search_record_identifier)
            else:
                raise ReviewInputError("Pipeline source is not a marketplace search or source run")
        elif isinstance(source, SearchWorkPipelineSource):
            works.extend(source.search_work_identifiers)
        elif isinstance(source, WorkspacePipelineSource):
            workspace = await application.get_review_workspace(source.workspace_record_identifier)
            workspace_identifier = workspace.record_identifier
            requested = set(source.track_identifiers or ())
            known = {track.track_identifier for track in workspace.search_tracks}
            if requested - known:
                raise ReviewInputError("Pipeline track does not belong to this workspace")
            for track in workspace.search_tracks:
                if not track.enabled or (requested and track.track_identifier not in requested):
                    continue
                if track.latest_refresh_work_state in {WorkState.PENDING, WorkState.LEASED}:
                    raise ReviewInputError(
                        "A selected workspace track is refreshing; use its exact search work or wait for that refresh"
                    )
                if track.current_search_run_record_identifier is not None:
                    runs.append(track.current_search_run_record_identifier)
                elif track.creation_work_identifier is not None:
                    works.append(track.creation_work_identifier)
            if requested and any(
                not track.enabled
                for track in workspace.search_tracks
                if track.track_identifier in requested
            ):
                raise ReviewInputError("Pipeline explicitly selected a disabled workspace track")
            inputs.append(NamedInput(name=("workspace",), object_identifier=workspace_identifier))
        else:
            raise AssertionError("Unknown pipeline source")
        works = list(dict.fromkeys(works))
        runs = list(dict.fromkeys(runs))
        if not works and not runs:
            raise ReviewInputError(
                "Pipeline source has no enabled search executions or retained runs"
            )
        for work in await database.pipeline_work_rows(tuple(works)):
            if tuple((await database.work(str(work["identifier"])))["kind"]) not in {
                COLLECT_SEARCH_WORK_KIND,
                COLLECT_EBAY_SEARCH_WORK_KIND,
            }:
                raise ReviewInputError(
                    "Pipeline work source must be an exact Facebook/eBay search job"
                )
        if search_identifier is not None:
            inputs.append(NamedInput(name=("search",), object_identifier=search_identifier))
        inputs.extend(
            NamedInput(name=("search_run", str(index)), object_identifier=identifier)
            for index, identifier in enumerate(runs)
        )
        if request.options.product_guide_record_identifier is not None:
            inputs.append(
                NamedInput(
                    name=("product_guide",),
                    object_identifier=request.options.product_guide_record_identifier,
                )
            )
        payload = RequestSearchPipelinePayload(
            request_identifier=request.request_identifier,
            request_sha256=request_hash,
            intent_record_identifier=intent_identifier,
            search_work_identifiers=tuple(works),
            search_run_record_identifiers=tuple(runs),
            search_record_identifier=search_identifier,
            workspace_record_identifier=workspace_identifier,
            options=request.options,
        )
        started = application.monotonic_ns()
        timestamp = datetime.fromtimestamp(
            application.utc_now_ns() / 1_000_000_000, tz=UTC
        ).isoformat()
        provenance = await application._code_provenance()  # pyright: ignore[reportPrivateUsage]
        await database.publish_records_operation(
            component=build_pipeline_component_registry().require(REQUEST_SEARCH_PIPELINE),
            operation_identifier=application.new_identifier(),
            records=(
                RecordDraft(
                    identifier=intent_identifier,
                    kind=SEARCH_PIPELINE_INTENT_KIND,
                    schema_version=1,
                    value={
                        "request": request.model_dump(mode="json"),
                        "resolved": payload.model_dump(mode="json"),
                    },
                ),
            ),
            inputs=tuple(inputs),
            outputs=(NamedOutput(name=("pipeline_intent",), object_identifier=intent_identifier),),
            provenance=provenance,
            invocation=process_invocation(),
            started_at_utc=timestamp,
            ended_at_utc=timestamp,
            duration_ns=max(0, application.monotonic_ns() - started),
            result={"work_identifier": work_identifier, "state": "accepted"},
        )
        for constraint in pipeline_work_constraints():
            await database.register_constraint(
                constraint, registered_at_utc_ns=application.utc_now_ns()
            )
        enqueued = await database.enqueue_work(
            search_pipeline_work(identifier=work_identifier, payload=payload),
            WorkRequester(
                request_identifier=application.new_identifier(),
                kind=("carl", "mcp", "request_search_pipeline"),
                identifier=workspace_identifier or work_identifier,
                context={"intent_record_identifier": intent_identifier},
            ),
            event_identifier=application.new_identifier(),
            enqueued_at_utc_ns=application.utc_now_ns(),
        )
        return _response(enqueued.work_item_identifier, payload, created=enqueued.created)


async def get_search_pipeline(
    application: ReviewApplication, work_identifier: str
) -> SearchPipelineStatus:
    database = application.database
    work = await database.work(work_identifier)
    if tuple(work["kind"]) != SEARCH_PIPELINE_WORK_KIND:
        raise ReviewInputError("Work identifier is not a search pipeline")
    payload = RequestSearchPipelinePayload.model_validate_json(encode_json(work["payload"]))
    result = _mapping(work.get("result"))
    raw_entries = result.get("listings")
    entries = [_mapping(item) for item in raw_entries] if isinstance(raw_entries, list) else []
    children = await database.pipeline_work_rows(
        tuple(str(entry["work_identifier"]) for entry in entries)
    )
    searches = await database.pipeline_work_rows(payload.search_work_identifiers)
    failed_searches = sum(
        search["state"] == "terminal_failure"
        or (search["state"] == "completed" and pipeline_collection_failed(search.get("result")))
        for search in searches
    )
    failed_runs = 0
    for identifier in payload.search_run_record_identifiers:
        _, _, run = await database.get_record(identifier)
        failed_runs += int(pipeline_collection_failed(run))
    failed_searches += failed_runs
    retained_runs = list(payload.search_run_record_identifiers)
    for search in searches:
        run_identifier = _mapping(search.get("result")).get("search_run_record_identifier")
        if isinstance(run_identifier, str):
            retained_runs.append(run_identifier)
    child_results = [_mapping(child.get("result")) for child in children]
    progress = tuple(
        PipelineListingProgress(
            work_identifier=str(child["identifier"]),
            marketplace=entry["marketplace"],
            external_identifier=str(entry["external_identifier"]),
            state=WorkState(str(child["state"])),
            stage=str(details.get("stage", "pending")),
            observation_record_identifier=details.get("observation_record_identifier"),
            analysis_work_identifier=details.get("analysis_work_identifier"),
            reason=details.get("reason") or child.get("error"),
        )
        for entry, child, details in zip(
            entries[:100], children[:100], child_results[:100], strict=True
        )
    )
    state = WorkState(str(work["state"]))
    return SearchPipelineStatus(
        work_identifier=work_identifier,
        state=state,
        options=payload.options,
        search_work_identifiers=payload.search_work_identifiers,
        search_run_record_identifiers=tuple(dict.fromkeys(retained_runs)),
        searches_pending=sum(search["state"] in {"pending", "leased"} for search in searches),
        searches_completed=sum(search["state"] == "completed" for search in searches)
        + len(payload.search_run_record_identifiers)
        - sum(
            search["state"] == "completed" and pipeline_collection_failed(search.get("result"))
            for search in searches
        )
        - failed_runs,
        searches_failed=failed_searches,
        candidates_examined=_integer(result.get("candidates_examined")),
        selected_listings=len(entries),
        items_completed=sum(details.get("item_completed") is True for details in child_results),
        galleries_completed=sum(
            details.get("images_completed") is True for details in child_results
        ),
        analyses_completed=sum(
            details.get("analysis_completed") is True for details in child_results
        ),
        analyses_reused=sum(details.get("analysis_reused") is True for details in child_results),
        skipped_listings=sum(details.get("stage") == "skipped" for details in child_results),
        failed_listings=sum(
            child["state"] == "terminal_failure"
            or details.get("state") == "completed_with_failures"
            for child, details in zip(children, child_results, strict=True)
        ),
        image_budget_reserved=_integer(result.get("image_budget_reserved")),
        analysis_budget_reserved=_integer(result.get("analysis_budget_reserved")),
        successful=(not failed_searches and result.get("state") != "completed_with_failures")
        if state is WorkState.COMPLETED
        else False
        if state is WorkState.TERMINAL_FAILURE
        else None,
        budget_exhausted=result.get("budget_exhausted") is True,
        listing_progress=progress,
    )
