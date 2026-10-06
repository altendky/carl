"""Durable eBay cooldown and stack retries; no provider calls."""

import json
import tomllib
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
    ebay_search_work_constraint,
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
from carl.core.work import WorkRequester
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
) -> None:
    await database.enqueue_work(
        collect_ebay_search_work(
            identifier=identifier,
            payload=CollectEbaySearchPayload(
                request=EbaySearchRequest(
                    query=query or identifier, stack_identifier=stack, listing_state="active"
                )
            ),
            not_before_utc_ns=0,
        ),
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier=identifier, context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=now,
    )


async def _fail(database: Database, directory: Path, identifier: str, now: int) -> None:
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
            collector=collect,
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
    await execute_lease(
        database=database,
        registry=registry,
        lease=claimed.lease,
        settings=WorkerSettings(
            worker_count=1,
            lease_duration_ns=60_000_000_000,
            renewal_interval_ns=1_000_000_000,
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
async def test_cooldown_defers_siblings_future_enqueues_and_survives_restart(
    tmp_path: Path,
) -> None:
    path, now = tmp_path / "test.sqlite3", 1_000_000_000_000
    async with Database.managed(path, initialize=True) as database:
        await database.register_constraint(ebay_search_work_constraint(), registered_at_utc_ns=now)
        await _enqueue(database, "blocked", now)
        await _enqueue(database, "sibling", now)
        await _enqueue(database, "alternate", now, "other_stack")
        await _fail(database, tmp_path, "blocked", now)
        deadline = now + 120_000_000_000
        assert (await database.work("sibling"))["eligible_at_utc_ns"] == deadline
        await _enqueue(database, "future", now + 1)
        assert (await database.work("future"))["eligible_at_utc_ns"] == deadline
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

        waiting = await claim("sibling", now + 1)
        assert waiting.lease is None and waiting.next_eligible_at_utc_ns == deadline
        assert (await database.work("sibling"))["attempt"] == 0
        assert (await claim("alternate", now + 1)).lease is not None
        assert (await claim("sibling", deadline)).lease is not None


@pytest.mark.anyio
async def test_terminal_failure_cools_stack_and_operator_retry_keeps_history(
    tmp_path: Path,
) -> None:
    now = 1_000_000_000_000
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _enqueue(database, "blocked", now)
        await _enqueue(database, "sibling", now)
        for delay in (0, 120_000_000_000, 240_000_000_000):
            now += delay
            await _fail(database, tmp_path, "blocked", now)
        failed = await database.work("blocked")
        assert failed["state"] == "terminal_failure" and failed["attempt"] == 3
        deadline = now + 480_000_000_000
        assert (await database.work("sibling"))["eligible_at_utc_ns"] == deadline
        previous = await database.retry_terminal_ebay_search_work(
            work_item_identifier="blocked",
            retried_at_utc_ns=now + 1,
            event_identifier=str(uuid4()),
            reason={"kind": "test_retry"},
            payload_schema_version=COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
        )
        retried = await database.work("blocked")
        assert previous == 3 and retried["attempt"] == 3 and len(retried["operations"]) == 3
        assert retried["payload"]["retry_attempt_offset"] == 3
        assert retried["payload"]["request"]["listing_state"] == "active"
        assert retried["eligible_at_utc_ns"] == deadline
        await _fail(database, tmp_path, "blocked", deadline)
        assert (await database.work("blocked"))["error"] is None
        assert (await database.work("blocked"))["attempt"] == 4


@pytest.mark.anyio
async def test_alternate_stack_retry_updates_dedup_and_rejects_active_equivalent(
    tmp_path: Path,
) -> None:
    now = 1_000_000_000_000
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _enqueue(database, "blocked", now, query="Morpheus")
        for delay in (0, 120_000_000_000, 240_000_000_000):
            now += delay
            await _fail(database, tmp_path, "blocked", now)
        await _enqueue(database, "equivalent", now, "other_stack", query="Morpheus")

        async def retry(stack: str) -> None:
            await database.retry_terminal_ebay_search_work(
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
        await database.enqueue_work(
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
async def test_legacy_default_stack_sibling_is_deferred(tmp_path: Path) -> None:
    now = 1_000_000_000_000
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        definition = collect_ebay_search_work(
            identifier="legacy",
            payload=CollectEbaySearchPayload(request=EbaySearchRequest(query="legacy")),
            not_before_utc_ns=0,
        ).model_copy(
            update={"payload_schema_version": 1, "payload": {"request": {"query": "legacy"}}}
        )
        await database.enqueue_work(
            definition,
            WorkRequester(
                request_identifier=str(uuid4()), kind=("test",), identifier="legacy", context={}
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=now,
        )
        await _enqueue(database, "blocked", now)
        await _fail(database, tmp_path, "blocked", now)
        assert (await database.work("legacy"))["eligible_at_utc_ns"] == now + 120_000_000_000
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
    monkeypatch.setattr("carl.review.load_configuration", lambda _: loaded)
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
        for delay in (0, 120_000_000_000, 240_000_000_000):
            now += delay
            await _fail(database, tmp_path, work_id, now)
        with pytest.raises(ReviewInputError, match="not configured"):
            await app.retry_workspace_search_track(
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
        assert result.work_identifier == work_id and result.previous_attempt_count == 3
        assert (await database.work(work_id))["payload"]["request"]["listing_state"] == "active"
        assert (await database.work(work_id))["eligible_at_utc_ns"] == now + 480_000_000_000
