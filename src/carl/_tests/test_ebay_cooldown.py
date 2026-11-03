"""Immediate bounded eBay retries preserve stack, history and explicit scheduling."""

# Offline providers and immutable event assertions deliberately reuse private fixtures.
# pyright: reportPrivateUsage=false

import json
import tomllib
from collections.abc import Awaitable, Callable
from pathlib import Path
from uuid import uuid4

import pytest

from carl._tests.test_configuration import _configuration_document
from carl._tests.test_ebay_workers import _directories, _provenance
from carl.core.configuration import CarlConfiguration
from carl.core.ebay import (
    COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
    CollectEbaySearchPayload,
    EbaySearchRequest,
    collect_ebay_search_work,
)
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
)
from carl.core.models import JsonValue
from carl.core.review_errors import ReviewInputError
from carl.core.review_workspace import (
    CreateReviewWorkspaceRequest,
    RetryWorkspaceSearchTrackRequest,
)
from carl.core.work import WorkEventKind, WorkRequester
from carl.core.worker import WorkerSettings
from carl.ebay_workers import EbaySearchWorkerDependencies, build_ebay_worker_registry
from carl.io.configuration import LoadedCarlConfiguration
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, execute_lease
from carl.review import ReviewApplication


async def _enqueue(
    database: Database,
    identifier: str,
    now: int,
    stack: str = "ebay_anonymous",
    query: str | None = None,
    not_before: int = 0,
) -> None:
    _ = await database.enqueue_work(
        collect_ebay_search_work(
            identifier=identifier,
            payload=CollectEbaySearchPayload(
                request=EbaySearchRequest(
                    query=query or identifier, stack_identifier=stack, listing_state="active"
                )
            ),
            not_before_utc_ns=not_before,
        ),
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier=identifier, context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=now,
    )


async def _fail(
    database: Database,
    directory: Path,
    identifier: str,
    now: int,
    *,
    collector: Callable[..., Awaitable[dict[str, JsonValue]]] | None = None,
) -> None:
    async def collect(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
        return {
            "state": "response_failed",
            "response_classification": {"kind": "error_page", "evidence": ["http_status_403"]},
            "stopping_reason": "error_page",
            "listing_count": 0,
        }

    registry = build_ebay_worker_registry(
        EbaySearchWorkerDependencies(
            database,
            _directories(directory),
            lambda: str(uuid4()),
            collector=collector or collect,
            utc_now_ns=lambda: now,
        )
    )
    claimed = await database.claim_work(
        supported_capabilities=registry.capabilities,
        worker_identifier="test",
        lease_token=str(uuid4()),
        lease_duration_ns=60_000_000_000,
        utc_now_ns=lambda: now,
        event_identifier=str(uuid4()),
        eligible_identifiers=(identifier,),
    )
    assert claimed.lease is not None
    _ = await execute_lease(
        database=database,
        registry=registry,
        lease=claimed.lease,
        settings=WorkerSettings(
            worker_count=1,
            lease_duration_ns=60_000_000_000,
            # This helper deliberately freezes its policy clock. It exercises
            # retry eligibility, not heartbeat renewal against advancing time.
            renewal_interval_ns=30_000_000_000,
            idle_poll_interval_ns=1,
        ),
        services=WorkerRuntimeServices(
            new_identifier=lambda: str(uuid4()),
            utc_now_ns=lambda: now,
            monotonic_ns=lambda: now,
            code_provenance=_provenance,
            invocation=lambda: {},
        ),
    )


@pytest.mark.anyio
async def test_failure_does_not_defer_siblings_or_future_enqueues_across_restart(
    tmp_path: Path,
) -> None:
    path, now = tmp_path / "test.sqlite3", 1_000_000_000_000
    async with Database.managed(path, initialize=True) as database:
        await _enqueue(database, "blocked", now)
        await _enqueue(database, "sibling", now)
        await _enqueue(database, "alternate", now, "other_stack")
        await _enqueue(database, "explicit", now, not_before=now + 3_600_000_000_000)
        await _fail(database, tmp_path, "blocked", now)
        # Model an immutable marker produced by an older executable, without
        # fabricating a newly propagated shared deadline on another work item.
        async with database._connections.writer() as connection:
            await database._append_work_event(
                connection,
                event_identifier="legacy-marker",
                work_item_identifier="blocked",
                event_kind=WorkEventKind.RELEASED,
                recorded_at_utc_ns=now,
                data={
                    "attempt": 1,
                    "eligible_at_utc_ns": now,
                    "reason": {
                        "kind": "ebay_search_response_failure",
                        "cooldown": {
                            "stack_identifier": "ebay_anonymous",
                            "until_utc_ns": now + 600_000_000_000,
                        },
                    },
                },
            )
        assert (await database.work("blocked"))["eligible_at_utc_ns"] == now
        assert (await database.work("sibling"))["eligible_at_utc_ns"] == now
        await _enqueue(database, "future", now + 1)
        assert (await database.work("future"))["eligible_at_utc_ns"] == now + 1
    async with Database.managed(path) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(database, _directories(tmp_path), lambda: str(uuid4()))
        )

        async def claim(identifier: str, when: int):
            return await database.claim_work(
                supported_capabilities=registry.capabilities,
                worker_identifier="test",
                lease_token=str(uuid4()),
                lease_duration_ns=60_000_000_000,
                utc_now_ns=lambda: when,
                event_identifier=str(uuid4()),
                eligible_identifiers=(identifier,),
            )

        for identifier in ("sibling", "alternate", "future"):
            claimed = await claim(identifier, now + 1)
            assert claimed.lease is not None and claimed.lease.attempt == 1
        blocked = await database.work("blocked")
        assert blocked["attempt"] == 1 and len(blocked["operations"]) == 1
        assert (await claim("explicit", now + 1)).lease is None
        assert (await database.work("explicit"))["eligible_at_utc_ns"] == now + 3_600_000_000_000
        async with database._connections.reader() as connection:
            marker = await (
                await connection.execute(
                    "SELECT data_json FROM work_events WHERE id='legacy-marker'"
                )
            ).fetchone()
        assert marker is not None
        assert (
            json.loads(str(marker[0]))["reason"]["cooldown"]["until_utc_ns"]
            == now + 600_000_000_000
        )


@pytest.mark.anyio
async def test_five_immediate_failures_preserve_operator_retry_budget_and_history(
    tmp_path: Path,
) -> None:
    now = 1_000_000_000_000
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _enqueue(database, "blocked", now)
        await _enqueue(database, "sibling", now)
        for _ in range(5):
            await _fail(database, tmp_path, "blocked", now)
        failed = await database.work("blocked")
        assert failed["state"] == "terminal_failure" and failed["attempt"] == 5
        assert (await database.work("sibling"))["eligible_at_utc_ns"] == now
        assert len(failed["operations"]) == 5
        assert "cooldown" not in failed["error"]
        previous = await database.retry_terminal_ebay_search_work(
            work_item_identifier="blocked",
            retried_at_utc_ns=now + 1,
            event_identifier=str(uuid4()),
            reason={"kind": "test_retry"},
            payload_schema_version=COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
        )
        retried = await database.work("blocked")
        assert previous == 5 and retried["attempt"] == 5 and len(retried["operations"]) == 5
        assert retried["payload"]["retry_attempt_offset"] == 5
        assert retried["payload"]["request"]["listing_state"] == "active"
        assert retried["eligible_at_utc_ns"] == now + 1
        await _fail(database, tmp_path, "blocked", now + 1)
        assert (await database.work("blocked"))["error"] is None
        assert (await database.work("blocked"))["attempt"] == 6
        assert (await database.work("blocked"))["eligible_at_utc_ns"] == now + 1


@pytest.mark.anyio
async def test_independent_session_exhausts_budget_without_cooling_other_work(
    tmp_path: Path,
) -> None:
    async def collect(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
        return {
            "state": "response_failed",
            "response_classification": {"kind": "error_page", "evidence": ["http_status_403"]},
            "stopping_reason": "error_page",
            "listing_count": 0,
            "retry_scope": "independent_session",
        }

    now = 1_000_000_000_000
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _enqueue(database, "blocked", now)
        await _enqueue(database, "sibling", now)
        for _ in range(5):
            await _fail(database, tmp_path, "blocked", now, collector=collect)
            assert (await database.work("sibling"))["eligible_at_utc_ns"] == now
        work = await database.work("blocked")
        assert work["state"] == "terminal_failure" and work["attempt"] == 5
        assert work["error"]["retry_scope"] == "independent_session"
        assert "cooldown" not in work["error"]
        await _enqueue(database, "future", now)
        assert (await database.work("future"))["eligible_at_utc_ns"] == now


@pytest.mark.anyio
async def test_alternate_stack_retry_updates_dedup_and_rejects_active_equivalent(
    tmp_path: Path,
) -> None:
    now = 1_000_000_000_000
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _enqueue(database, "blocked", now, query="Morpheus")
        for _ in range(5):
            await _fail(database, tmp_path, "blocked", now)
        await _enqueue(database, "equivalent", now, "other_stack", query="Morpheus")

        async def retry(stack: str) -> None:
            _ = await database.retry_terminal_ebay_search_work(
                work_item_identifier="blocked",
                retried_at_utc_ns=now,
                event_identifier=str(uuid4()),
                reason={"kind": "test_retry"},
                payload_schema_version=COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
                acquisition_stack=stack,
            )

        with pytest.raises(ValueError, match="equivalent"):
            await retry("other_stack")
        assert (await database.work("blocked"))["state"] == "terminal_failure"
        await retry("third_stack")
        retried = await database.work("blocked")
        assert retried["payload"]["request"]["stack_identifier"] == "third_stack"
        assert retried["payload"]["request"]["listing_state"] == "active"
        assert retried["eligible_at_utc_ns"] == now
        payload = CollectEbaySearchPayload.model_validate_json(json.dumps(retried["payload"]))
        definition = collect_ebay_search_work(
            identifier="duplicate", payload=payload, not_before_utc_ns=0
        )
        result = await database.enqueue_work(
            definition,
            WorkRequester(
                request_identifier=str(uuid4()), kind=("test",), identifier="duplicate", context={}
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=now,
        )
        assert not result.created and result.work_item_identifier == "blocked"


@pytest.mark.anyio
async def test_malformed_payload_is_failed_by_handler_not_scheduler(tmp_path: Path) -> None:
    now = 1_000_000_000_000
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        definition = collect_ebay_search_work(
            identifier="malformed",
            payload=CollectEbaySearchPayload(request=EbaySearchRequest(query="valid")),
            not_before_utc_ns=0,
        ).model_copy(update={"payload": {"request": {"stack_identifier": "ebay_anonymous"}}})
        _ = await database.enqueue_work(
            definition,
            WorkRequester(
                request_identifier=str(uuid4()), kind=("test",), identifier="bad", context={}
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=now,
        )
        await _fail(database, tmp_path, "malformed", now)
        assert (await database.work("malformed"))["state"] == "terminal_failure"
        assert (await database.work("malformed"))["error"]["kind"] == "unhandled_handler_error"
        await _enqueue(database, "valid", now)
        await _fail(database, tmp_path, "valid", now)
        assert (await database.work("valid"))["state"] == "pending"


@pytest.mark.anyio
async def test_legacy_default_stack_sibling_is_not_deferred(tmp_path: Path) -> None:
    now = 1_000_000_000_000
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        definition = collect_ebay_search_work(
            identifier="legacy",
            payload=CollectEbaySearchPayload(request=EbaySearchRequest(query="legacy")),
            not_before_utc_ns=0,
        ).model_copy(
            update={"payload_schema_version": 1, "payload": {"request": {"query": "legacy"}}}
        )
        _ = await database.enqueue_work(
            definition,
            WorkRequester(
                request_identifier=str(uuid4()), kind=("test",), identifier="legacy", context={}
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=now,
        )
        await _enqueue(database, "blocked", now)
        await _fail(database, tmp_path, "blocked", now)
        assert (await database.work("legacy"))["eligible_at_utc_ns"] == now
        assert (await database.work("legacy"))["attempt"] == 0


@pytest.mark.anyio
async def test_public_stack_override_checks_configuration_before_requeueing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000_000
    document = tomllib.loads(_configuration_document(tmp_path / "wireproxy").decode())
    loaded = LoadedCarlConfiguration(
        configuration=CarlConfiguration.model_validate(document),
        document_sha256="0" * 64,
        source_path=tmp_path / "config.toml",
    )

    def load(_path: Path) -> LoadedCarlConfiguration:
        return loaded

    monkeypatch.setattr("carl.review.load_configuration", load)
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        app = ReviewApplication(
            database, tmp_path, utc_now_ns=lambda: now, code_provenance=_provenance
        )
        group = await app.create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=(
                    EbaySearchTargetSpecification(
                        search=EbaySearchRequest(query="Morpheus", listing_state="active")
                    ),
                )
            )
        )
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Blocked eBay", search_run_record_identifier=group.record_identifier
            )
        )
        work_id = group.targets[0].executions[0].work_identifier
        track_id = workspace.search_tracks[0].track_identifier
        for _ in range(5):
            await _fail(database, tmp_path, work_id, now)
        with pytest.raises(ReviewInputError, match="not configured"):
            _ = await app.retry_workspace_search_track(
                RetryWorkspaceSearchTrackRequest(
                    workspace_record_identifier=workspace.record_identifier,
                    track_identifier=track_id,
                    acquisition_stack="missing_stack",
                )
            )
        assert (await database.work(work_id))["state"] == "terminal_failure"
        result = await app.retry_workspace_search_track(
            RetryWorkspaceSearchTrackRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=track_id,
                acquisition_stack="ebay_anonymous",
            )
        )
        assert result.work_identifier == work_id and result.previous_attempt_count == 5
        assert (await database.work(work_id))["payload"]["request"]["listing_state"] == "active"
        assert (await database.work(work_id))["eligible_at_utc_ns"] == now
