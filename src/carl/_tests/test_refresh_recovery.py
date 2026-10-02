"""Offline failure severity and source-neutral item recovery regression tests."""

# Reuse deliberately private worker test fixtures.
# pyright: reportPrivateUsage=false

from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest

from carl._tests.test_ebay_item_workers import _identifiers, _retain_records, _run_next
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_review_workspace import _search_run_value
from carl._tests.test_worker import _definition, _requester
from carl.core.facebook_refresh import (
    REFRESH_SEARCH_WORK_KIND,
    RefreshSearchPayload,
    refresh_search_work,
)
from carl.core.json import encode_json
from carl.core.models import JsonValue, RecordDraft
from carl.core.refresh_recovery import (
    RetryItemFailuresRequest,
    classify_refresh_completion,
    refresh_failure_summary,
)
from carl.core.review_errors import ReviewInputError
from carl.core.work import WorkRequester, WorkState
from carl.core.worker import AttemptContext, CompletedWork, TerminalFailureWork
from carl.facebook_refresh_workers import RefreshWorkerDependencies, build_refresh_worker_registry
from carl.io.connectivity import work_retry_budget
from carl.io.sqlite import Database
from carl.review import ReviewApplication


@pytest.mark.parametrize(
    ("selected", "failed", "severity"),
    (
        (199, 197, "failed"),
        (199, 198, "failed"),
        (10, 5, "failed"),
        (10, 4, "partial_failure"),
        (10, 0, "success"),
        (0, 0, "success"),
    ),
)
def test_refresh_failure_severity(selected: int, failed: int, severity: str) -> None:
    outcome = classify_refresh_completion(
        CompletedWork(
            result={
                "selected_unique_listings": selected,
                "item_failures": failed,
                "state": "completed" if failed == 0 else "completed_with_failures",
            }
        )
    )
    assert refresh_failure_summary(outcome.result).severity == severity
    assert isinstance(outcome, TerminalFailureWork) == (severity == "failed")
    assert outcome.result["failure_summary"] is not None


async def _settled_refresh(database: Database, marketplace: str) -> tuple[str, ...]:
    root_kind = ("carl", marketplace, "work", "refresh_search")
    await database.enqueue_work(
        _definition(identifier="refresh", kind=root_kind),
        _requester("refresh"),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    await _complete(
        database,
        "refresh",
        result={
            "stage": "complete",
            "state": "completed_with_failures",
            "refreshed_search_run_record_identifier": "fresh",
            "selected_unique_listings": 3,
            "item_failures": 3,
            "new_image_collections": 2,
        },
    )
    kind = (
        ("carl", "ebay", "collect", "item")
        if marketplace == "ebay"
        else ("carl", "facebook", "work", "collect_item")
    )
    identifiers = ("transient", "another-transient", "permanent", "unrelated")
    for identifier in identifiers:
        await database.enqueue_work(
            _definition(identifier=identifier, kind=kind),
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("carl", marketplace, "search_refresh", "item"),
                identifier="refresh",
                context={
                    "search_refresh_work_identifier": "other"
                    if identifier == "unrelated"
                    else "refresh"
                },
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
    async with database._connections.writer() as connection:
        for identifier in identifiers:
            stopping = "terminal_response" if identifier == "permanent" else "transport_failure"
            await connection.execute(
                "UPDATE work_items SET state='terminal_failure',attempt=3,result_json=?,error_json=? WHERE id=?",
                (
                    encode_json({"acquisition": {"stopping_condition": stopping}}),
                    encode_json({"kind": "acquisition_failed"}),
                    identifier,
                ),
            )
    return identifiers


@pytest.mark.anyio
@pytest.mark.parametrize("marketplace", ("facebook", "ebay"))
async def test_recovery_is_bounded_linked_and_resets_budget_without_search(
    tmp_path: Path,
    marketplace: str,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        identifiers = await _settled_refresh(database, marketplace)
        app = ReviewApplication(database, tmp_path)
        legacy = await app.get_work_status("refresh")
        assert legacy.state is WorkState.COMPLETED
        assert legacy.outcome == "failed"
        assert legacy.refresh_failure_summary is not None
        assert legacy.refresh_failure_summary.failed_items == 3
        before = await database.work("refresh")
        result = await app.retry_item_failures(
            RetryItemFailuresRequest(
                refresh_work_identifier="refresh",
                maximum_items=1,
            )
        )
        assert result.matched_terminal_failures == 3
        assert result.retryable_terminal_failures == 2
        assert result.retried == 1
        assert result.remaining_terminal_failures == 2
        assert result.refresh_resumed
        parent = await database.work("refresh")
        assert parent["state"] == "pending"
        assert parent["attempt"] == before["attempt"]
        assert parent["operations"] == before["operations"]
        assert isinstance(parent["result"], dict)
        checkpoint = parent["result"]
        assert checkpoint["stage"] == "collecting_items"
        assert checkpoint["refreshed_search_run_record_identifier"] == "fresh"
        assert checkpoint["prior_image_collection_count"] == 2
        assert isinstance(checkpoint["item_retry_generation_identifier"], str)
        for identifier in identifiers:
            child = await database.work(identifier)
            assert child["attempt"] == 3
            assert child["state"] == (
                "pending" if identifier == "transient" else "terminal_failure"
            )
        outages, baseline = await work_retry_budget(database, "transient")
        assert baseline == 3
        assert (
            AttemptContext(
                work_item_identifier="transient",
                lease_token="lease",
                worker_identifier="worker",
                operation_identifier="operation",
                attempt=4,
                outage_attempts=outages,
                retry_budget_start_attempt=baseline,
            ).retry_attempt()
            == 1
        )
        with pytest.raises(ReviewInputError, match="settle"):
            await app.retry_item_failures(
                RetryItemFailuresRequest(refresh_work_identifier="refresh")
            )


@pytest.mark.anyio
async def test_recovery_rejects_active_equivalent_without_changing_children(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _settled_refresh(database, "ebay")
        definition = _definition(
            identifier="refresh", kind=("carl", "ebay", "work", "refresh_search")
        )
        await database.enqueue_work(
            definition.model_copy(update={"identifier": "equivalent"}),
            _requester("equivalent"),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        app = ReviewApplication(database, tmp_path)
        with pytest.raises(ReviewInputError, match="equivalent"):
            await app.retry_item_failures(
                RetryItemFailuresRequest(refresh_work_identifier="refresh")
            )
        assert (await database.work("transient"))["state"] == "terminal_failure"
        assert (await database.work("refresh"))["state"] == "completed"


@pytest.mark.anyio
async def test_recovery_without_transient_failures_does_not_reopen_refresh(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _settled_refresh(database, "ebay")
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET result_json='{}' WHERE id IN ('transient','another-transient')"
            )
        before = await database.work("refresh")
        app = ReviewApplication(database, tmp_path)
        result = await app.retry_item_failures(
            RetryItemFailuresRequest(refresh_work_identifier="refresh")
        )
        assert result.retried == 0
        assert not result.refresh_resumed
        assert await database.work("refresh") == before


def test_description_and_image_failures_remain_visible() -> None:
    result: dict[str, JsonValue] = {
        "selected_unique_listings": 10,
        "item_failures": 0,
        "description_failures": 1,
        "new_images_failed": 2,
    }
    assert refresh_failure_summary(result).severity == "partial_failure"


@pytest.mark.anyio
async def test_facebook_retained_failure_and_recovery_records_are_consistent(
    tmp_path: Path,
) -> None:
    identifiers = _identifiers()
    source = _search_run_value("scope")
    payload = RefreshSearchPayload.model_validate_json(
        encode_json(
            {
                "base_search_run_record_identifier": "base",
                "search_work_identifier": "search",
                "search": {
                    "request": source["request"],
                    "traversal": {"maximum_pages": 1},
                    "routing": ["proton", "personal", "carl"],
                },
                "item_routing": ["decodo", "personal", "carl"],
                "image_routing": ["proton", "personal", "carl"],
                "maximum_items": 1,
                "maximum_images": 1,
            }
        )
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            tuple(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value=source,
                )
                for identifier in ("base", "fresh")
            ),
            identifiers,
        )
        await database.enqueue_work(
            refresh_search_work(identifier="refresh", payload=payload, not_before_utc_ns=0),
            _requester("refresh"),
            event_identifier=identifiers(),
            enqueued_at_utc_ns=time_ns(),
        )
        registry = build_refresh_worker_registry(RefreshWorkerDependencies(database, identifiers))
        result_identifiers: list[str] = []
        for generation, failures in ((None, 1), ("recovery", 0)):
            checkpoint: dict[str, JsonValue] = {
                "stage": "items_complete",
                "refreshed_search_run_record_identifier": "fresh",
                "selected_unique_listings": 1,
                "item_failures": failures,
            }
            if generation is not None:
                checkpoint["item_retry_generation_identifier"] = generation
            async with database._connections.writer() as connection:
                await connection.execute(
                    "UPDATE work_items SET state='pending',eligible_at_utc_ns=0,result_json=?,error_json=NULL WHERE id='refresh'",
                    (encode_json(checkpoint),),
                )
            work = await _run_next(database, registry, identifiers, REFRESH_SEARCH_WORK_KIND)
            assert work["state"] == ("terminal_failure" if failures else "completed")
            result = work["result"]
            assert isinstance(result, dict)
            identifier = result["search_refresh_record_identifier"]
            assert isinstance(identifier, str)
            result_identifiers.append(identifier)
            _, _, retained = await database.get_record(identifier)
            assert isinstance(retained, dict)
            assert retained["state"] == ("failed" if failures else "completed")
            assert retained["failure_summary"] == result["failure_summary"]
        assert result_identifiers[0] != result_identifiers[1]
        assert len(await database.records_by_kind(("carl", "facebook", "image_followup_plan"))) == 2
