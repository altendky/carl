"""Search-refresh request validation and durable queueing."""

from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from carl import facebook_refresh_workers
from carl.core.components import Component, ComponentId
from carl.core.facebook import FacebookItemResponseKind
from carl.core.facebook_refresh import RefreshSearchPayload, SearchRefreshRequest
from carl.core.facebook_search import (
    OverlappingPricePartitionSearchTraversalStrategy,
    SearchTraversalPolicy,
)
from carl.core.facebook_work import (
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    CollectItemPayload,
    SuccessfulItemPageResult,
    collect_item_work,
)
from carl.core.http import RequestPlan
from carl.core.json import encode_json
from carl.core.models import CodeProvenance, NamedOutput, RecordDraft
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
    WorkState,
)
from carl.core.worker import AttemptContext, RetryWork
from carl.facebook_refresh_workers import (
    RefreshWorkerDependencies,
    build_refresh_worker_registry,
)
from carl.io.sqlite import Database
from carl.review import ReviewApplication


class _RetryableSearchFailureDatabase:
    def __init__(self) -> None:
        self.retried: dict[str, Any] | None = None

    async def work(self, identifier: str) -> dict[str, Any]:
        if identifier == "refresh-work":
            return {"result": None}
        assert identifier == "search-work"
        return {
            "state": "terminal_failure",
            "attempt": 1,
            "error": {
                "kind": "network_session_failure",
                "code": "wireproxy_device_identity_busy",
            },
            "operations": [{"operation_identifier": "search-operation"}],
        }

    async def supersede_constraints(self, **_kwargs: Any) -> None:
        pass

    async def requested_work_identifiers(self, **_kwargs: Any) -> tuple[str, ...]:
        return ("search-work",)

    async def retry_terminal_work_from_operation(self, **kwargs: Any) -> None:
        self.retried = kwargs


@pytest.mark.anyio
async def test_refresh_resumes_retryable_terminal_search_child() -> None:
    database = _RetryableSearchFailureDatabase()
    payload = RefreshSearchPayload.model_validate(
        {
            "base_search_run_record_identifier": "base-search-run",
            "search_work_identifier": "search-work",
            "search": {
                "request": {
                    "query": "telescope",
                    "location": {"kind": "facebook_location", "identifier": "456"},
                    "radius": {"value": 60, "unit": "miles"},
                },
                "traversal": {"maximum_pages": 1},
                "routing": ("proton", "personal", "carl"),
            },
            "item_routing": ("decodo", "personal", "carl"),
            "image_routing": ("proton", "personal", "carl"),
        }
    )
    registry = build_refresh_worker_registry(
        RefreshWorkerDependencies(
            database=cast(Database, cast(object, database)),
            new_identifier=lambda: "retry-event",
            utc_now_ns=lambda: 123,
        )
    )

    outcome = await registry.handlers[0].execute(
        payload.model_dump(mode="json"),
        AttemptContext(
            work_item_identifier="refresh-work",
            lease_token="lease",
            worker_identifier="worker",
            attempt=1,
            operation_identifier="refresh-operation",
        ),
    )

    assert isinstance(outcome, RetryWork)
    assert outcome.reason == {"kind": "resumed_search_collection_after_transient_failure"}
    assert database.retried == {
        "work_item_identifier": "search-work",
        "checkpoint_operation_identifier": "search-operation",
        "retried_at_utc_ns": 123,
        "event_identifier": "retry-event",
        "reason": {
            "kind": "search_refresh_resumed_transient_search_failure",
            "refresh_work_identifier": "refresh-work",
        },
        "payload_schema_version": COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    }


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


@pytest.mark.anyio
async def test_completed_refresh_is_resolved_from_its_exact_search_run(tmp_path: Path) -> None:
    kind = ("carl", "facebook", "work", "refresh_search")
    definition = WorkDefinition(
        identifier="refresh-work",
        kind=kind,
        payload_schema_version=1,
        payload={},
        deduplication_identity=("test", "refresh-work"),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
        ),
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            definition,
            WorkRequester(
                request_identifier="refresh-request",
                kind=("test", "refresh"),
                identifier="refresh-work",
                context={},
            ),
            event_identifier="refresh-enqueued",
            enqueued_at_utc_ns=1,
        )
        assert (
            await database.facebook_search_run_refresh_work_identifier("refreshed-search-run")
            is None
        )
        claimed = await database.claim_work(
            supported_capabilities=(WorkCapability(kind=kind, payload_schema_version=1),),
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=100,
            utc_now_ns=lambda: 2,
            event_identifier="refresh-claimed",
        )
        assert claimed.lease is not None
        component = Component(ComponentId(("test", "refresh")), 1, _provenance)
        await database.begin_leased_operation(
            work_item_identifier="refresh-work",
            lease_token="lease",
            worker_identifier="worker",
            lease_duration_ns=100,
            utc_now_ns=lambda: 2,
            event_identifier="refresh-dispatched",
            operation_id="refresh-operation",
            component=component,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-27T00:00:00+00:00",
        )
        await database.complete_leased_operation(
            work_item_identifier="refresh-work",
            lease_token="lease",
            worker_identifier="worker",
            utc_now_ns=lambda: 3,
            event_identifier="refresh-completed",
            operation_id="refresh-operation",
            records=(),
            artifacts=(),
            outputs=(),
            result={"refreshed_search_run_record_identifier": "refreshed-search-run"},
            ended_at_utc="2026-09-27T00:00:01+00:00",
            duration_ns=1,
        )
        assert (
            await database.facebook_search_run_refresh_work_identifier("refreshed-search-run")
            == "refresh-work"
        )


def _source_component() -> None:
    pass


@pytest.mark.anyio
@pytest.mark.parametrize(
    "network_path", (None, ("decodo", "personal", "other"), ("proton", "personal", "dedicated"))
)
async def test_request_search_refresh_inherits_request_and_accepts_strategy_override(
    tmp_path: Path, network_path: tuple[str, ...] | None
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        operation_identifier = "source-operation"
        record_identifier = "source-search-run"
        await database.begin_operation(
            operation_id=operation_identifier,
            component=Component(ComponentId(("test", "search_run")), 1, _source_component),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-24T00:00:00+00:00",
            inputs=(),
        )
        await database.complete_operation(
            operation_id=operation_identifier,
            records=(
                RecordDraft(
                    identifier=record_identifier,
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value={
                        "request": {
                            "query": "telescope",
                            "location": {
                                "kind": "facebook_location",
                                "identifier": "123",
                                "label": None,
                            },
                            "radius": {"value": 60, "unit": "miles"},
                            "price": {
                                "currency": "USD",
                                "minimum": None,
                                "maximum": "600",
                            },
                            "exact_match": False,
                        },
                        "traversal_strategy": {"kind": "cursor"},
                        "traversal": {
                            "policy": {
                                "maximum_pages": 10,
                                "maximum_results": None,
                                "maximum_elapsed_duration_ns": None,
                                "maximum_transferred_bytes": None,
                                "maximum_decoded_body_bytes": None,
                                "maximum_consecutive_pages_without_new_listings": None,
                                "requested_page_size": None,
                            },
                            "unique_listing_identifiers": ["1"],
                        },
                    },
                ),
            ),
            artifacts=(),
            outputs=(NamedOutput(name=("search_run",), object_identifier=record_identifier),),
            result={},
            ended_at_utc="2026-09-24T00:00:01+00:00",
            duration_ns=1,
        )
        application = ReviewApplication(database=database, repository_root=tmp_path)

        queued = await application.request_search_refresh(
            SearchRefreshRequest(
                base_search_run_record_identifier=record_identifier,
                search_network_path=network_path,
                traversal=SearchTraversalPolicy(maximum_results=50),
                traversal_strategy=OverlappingPricePartitionSearchTraversalStrategy(
                    width=Decimal("10"), overlap=Decimal("2")
                ),
            )
        )

        work = await database.work(queued.work_identifier)
        assert work["state"] == "pending"
        payload = work["payload"]
        assert payload["search"]["request"]["query"] == "telescope"
        assert payload["search"]["traversal"]["maximum_results"] == 50
        assert payload["search"]["traversal_strategy"]["width"] == "10"
        assert payload["search"]["routing"] == list(
            network_path or ("decodo", "personal", "datacenter")
        )
        assert payload["item_routing"] == ["decodo", "personal", "carl"]
        assert payload["image_routing"] == ["decodo", "personal", "datacenter"]
        assert await database.requested_work_identifiers(
            requester_kind=("carl", "mcp", "request_search_refresh"),
            requester_identifier=record_identifier,
        ) == (queued.work_identifier,)

        duplicate = await application.request_search_refresh(
            SearchRefreshRequest(
                base_search_run_record_identifier=record_identifier,
                search_network_path=network_path,
                traversal=SearchTraversalPolicy(maximum_results=50),
                traversal_strategy=OverlappingPricePartitionSearchTraversalStrategy(
                    width=Decimal("10"), overlap=Decimal("2")
                ),
            )
        )
        assert duplicate.work_identifier == queued.work_identifier
        assert queued.created
        assert not duplicate.created

        status = await application.get_work_status(queued.work_identifier)
        assert status.operation_count == 0
        assert status.details is None
        assert status.search_refresh_progress is not None
        assert status.search_refresh_progress.checkpoint_stage is None
        assert status.search_refresh_progress.active_phase == "search"
        detailed_status = await application.get_work_status(
            queued.work_identifier, include_details=True
        )
        assert detailed_status.details is not None
        assert detailed_status.details.payload == work["payload"]
        assert detailed_status.details.result is None
        assert detailed_status.details.error is None
        assert detailed_status.details.recent_operations == ()

        refresh_registry = build_refresh_worker_registry(
            RefreshWorkerDependencies(
                database=database,
                new_identifier=lambda: "refresh-child-request",
            )
        )
        outcome = await refresh_registry.handlers[0].execute(
            payload,
            AttemptContext(
                work_item_identifier=queued.work_identifier,
                lease_token="refresh-lease",
                worker_identifier="refresh-worker",
                attempt=1,
                operation_identifier=operation_identifier,
            ),
        )
        assert isinstance(outcome, RetryWork)
        assert outcome.reason == {"kind": "waiting_for_search_collection"}
        search_children = await database.requested_work_identifiers(
            requester_kind=("carl", "facebook", "search_refresh", "search"),
            requester_identifier=queued.work_identifier,
        )
        assert len(search_children) == 1
        assert await database.work_state(search_children[0]) is WorkState.PENDING

        child_kind = ("carl", "facebook", "work", "collect_item")
        await database.enqueue_work(
            WorkDefinition(
                identifier="item-child",
                kind=child_kind,
                payload_schema_version=1,
                payload={},
                deduplication_identity=("test", "item-child"),
                not_before_utc_ns=0,
                scopes=(
                    SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                    SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=child_kind),
                ),
            ),
            WorkRequester(
                request_identifier="item-child-request",
                kind=("carl", "facebook", "search_refresh", "item"),
                identifier="item-child-requester",
                context={
                    "search_refresh_work_identifier": queued.work_identifier,
                    "refreshed_search_run_record_identifier": "refreshed-search-run",
                },
            ),
            event_identifier="item-child-event",
            enqueued_at_utc_ns=1,
        )
        child_states = await database.search_refresh_child_work_states(
            refresh_work_identifier=queued.work_identifier,
            refreshed_search_run_record_identifier="refreshed-search-run",
        )
        assert child_states["item_pages"] == (WorkState.PENDING,)


@pytest.mark.anyio
async def test_item_phase_reuses_successful_pages_without_decodo_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_identifier = "base-search-run"
    refreshed_identifier = "refreshed-search-run"
    search_record: dict[str, Any] = {
        "request": {
            "query": "telescope",
            "location": {
                "kind": "facebook_location",
                "identifier": "123",
                "label": None,
            },
            "radius": {"value": 60, "unit": "miles"},
            "price": {"currency": "USD", "minimum": None, "maximum": "600"},
            "exact_match": False,
        },
        "traversal_strategy": {"kind": "cursor"},
        "traversal": {
            "policy": {
                "maximum_pages": 10,
                "maximum_results": None,
                "maximum_elapsed_duration_ns": None,
                "maximum_transferred_bytes": None,
                "maximum_decoded_body_bytes": None,
                "maximum_consecutive_pages_without_new_listings": None,
                "requested_page_size": None,
            },
            "unique_listing_identifiers": ["1", "2"],
        },
    }
    refreshed_record = {
        **search_record,
        "traversal": {
            **search_record["traversal"],
            "unique_listing_identifiers": ["2", "3"],
        },
    }
    payload = RefreshSearchPayload.model_validate_json(
        encode_json(
            {
                "base_search_run_record_identifier": base_identifier,
                "search_work_identifier": "search-work",
                "search": {
                    "request": search_record["request"],
                    "traversal": search_record["traversal"]["policy"],
                    "traversal_strategy": search_record["traversal_strategy"],
                    "routing": ["proton", "personal", "carl"],
                },
                "item_routing": ["decodo", "personal", "carl"],
                "image_routing": ["proton", "personal", "carl"],
            }
        )
    )

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.begin_operation(
            operation_id="search-record-operation",
            component=Component(ComponentId(("test", "search_run")), 1, _source_component),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-24T00:00:00+00:00",
        )
        await database.complete_operation(
            operation_id="search-record-operation",
            records=(
                RecordDraft(
                    identifier=base_identifier,
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value=search_record,
                ),
                RecordDraft(
                    identifier=refreshed_identifier,
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value=refreshed_record,
                ),
            ),
            artifacts=(),
            outputs=(
                NamedOutput(name=("search_run", "base"), object_identifier=base_identifier),
                NamedOutput(
                    name=("search_run", "refreshed"), object_identifier=refreshed_identifier
                ),
            ),
            result={},
            ended_at_utc="2026-09-24T00:00:01+00:00",
            duration_ns=1,
        )

        async def successful_results(
            _database: Database, listing_identifiers: tuple[str, ...]
        ) -> tuple[SuccessfulItemPageResult, ...]:
            assert listing_identifiers == ("2", "3", "1")
            return tuple(
                SuccessfulItemPageResult(
                    listing_id=listing_identifier,
                    acquisition_record_identifier=f"acquisition-{listing_identifier}",
                    observation_record_identifier=f"observation-{listing_identifier}",
                    response_classification=(
                        FacebookItemResponseKind.LISTING_UNAVAILABLE
                        if listing_identifier == "1"
                        else FacebookItemResponseKind.FULL_LISTING
                    ),
                    acquisition_completion_sequence=index,
                    extraction_completion_sequence=index,
                )
                for index, listing_identifier in enumerate(listing_identifiers, start=1)
            )

        monkeypatch.setattr(
            Database,
            "successful_facebook_item_page_results",
            successful_results,
        )
        monkeypatch.setattr(
            facebook_refresh_workers,
            "brave_navigation_headers",
            lambda: pytest.fail("reused item pages must not prepare a Decodo request"),
        )

        outcome = await facebook_refresh_workers._item_phase(  # pyright: ignore[reportPrivateUsage]
            payload,
            refreshed_identifier,
            AttemptContext(
                work_item_identifier="refresh-work",
                lease_token="refresh-lease",
                worker_identifier="refresh-worker",
                attempt=1,
                operation_identifier="refresh-operation",
            ),
            RefreshWorkerDependencies(database=database, new_identifier=lambda: "unused"),
        )

        assert isinstance(outcome, RetryWork)
        assert outcome.reason == {"kind": "continue_refresh", "next_stage": "images"}
        assert outcome.result["selected_unique_listings"] == 3
        assert outcome.result["reused_successful_item_pages"] == 3
        assert outcome.result["item_collections"] == 0
        assert outcome.result["item_extractions"] == 0
        assert tuple(input_value.object_identifier for input_value in outcome.inputs[2:]) == (
            "observation-2",
            "observation-3",
            "observation-1",
        )

        current_requester = facebook_refresh_workers._stable_identifier(  # pyright: ignore[reportPrivateUsage]
            "refresh-work", "item_requester", "2"
        )

        async def requested_work_identifiers(
            _database: Database,
            *,
            requester_kind: tuple[str, ...],
            requester_identifier: str,
        ) -> tuple[str, ...]:
            assert requester_kind == ("carl", "facebook", "search_refresh", "item")
            return ("current-child",) if requester_identifier == current_requester else ()

        async def current_work(_database: Database, work_identifier: str) -> dict[str, Any]:
            assert work_identifier == "current-child"
            return {"state": WorkState.PENDING.value}

        monkeypatch.setattr(
            Database,
            "requested_work_identifiers",
            requested_work_identifiers,
        )
        monkeypatch.setattr(Database, "work", current_work)

        resumed = await facebook_refresh_workers._item_phase(  # pyright: ignore[reportPrivateUsage]
            payload,
            refreshed_identifier,
            AttemptContext(
                work_item_identifier="refresh-work",
                lease_token="refresh-lease",
                worker_identifier="refresh-worker",
                attempt=2,
                operation_identifier="refresh-operation-2",
            ),
            RefreshWorkerDependencies(database=database, new_identifier=lambda: "unused"),
        )

        assert isinstance(resumed, RetryWork)
        assert resumed.reason == {"kind": "waiting_for_item_collections"}
        assert resumed.result["reused_successful_item_pages"] == 2
        assert resumed.result["item_collection_work_identifiers"] == ["current-child"]


@pytest.mark.anyio
async def test_item_enqueue_rolls_back_when_usable_page_wins_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retained = SuccessfulItemPageResult(
        listing_id="123",
        acquisition_record_identifier="acquisition-123",
        observation_record_identifier="observation-123",
        response_classification=FacebookItemResponseKind.FULL_LISTING,
        acquisition_completion_sequence=1,
        extraction_completion_sequence=2,
    )

    async def successful_results(
        _database: Database, listing_identifiers: tuple[str, ...]
    ) -> tuple[SuccessfulItemPageResult, ...]:
        assert listing_identifiers == ("123",)
        return (retained,)

    monkeypatch.setattr(
        Database,
        "successful_facebook_item_page_results",
        successful_results,
    )

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        enqueued, reused = await database.enqueue_work_unless_facebook_item_page_is_usable(
            collect_item_work(
                identifier="item-work",
                payload=CollectItemPayload(
                    listing_id="123",
                    request_plan=RequestPlan(
                        url="https://www.facebook.com/marketplace/item/123/",
                        routing=("decodo", "personal", "carl"),
                    ),
                ),
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier="item-request",
                kind=("carl", "facebook", "search_refresh", "item"),
                identifier="item-requester",
                context={},
            ),
            listing_identifier="123",
            event_identifier="item-enqueued",
            enqueued_at_utc_ns=1,
        )

        assert enqueued is None
        assert reused == retained
        with pytest.raises(KeyError, match="item-work"):
            await database.work("item-work")
        assert (
            await database.requested_work_identifiers(
                requester_kind=("carl", "facebook", "search_refresh", "item"),
                requester_identifier="item-requester",
            )
            == ()
        )
