"""Offline durable retries of challenged Facebook price partitions."""

# Reuse the deliberately private offline worker/session fixtures.
# pyright: reportPrivateUsage=false

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from decimal import Decimal
from itertools import count
from pathlib import Path
from time import perf_counter_ns
from typing import cast, final
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from carl._tests.test_facebook_search_workers import (
    _async_provenance,
    _FakeSession,
    _feed,
    _route_definition,
    _session_bootstrap,
    _Stream,
)
from carl.core.composed_projection import (
    ComposedListingFilters,
    GetComposedListingRequest,
    ListComposedSearchRequest,
    ListingStatus,
)
from carl.core.facebook_refresh import RefreshSearchPayload
from carl.core.facebook_search import (
    OverlappingPricePartitionSearchTraversalStrategy,
    SearchPricePartitionOrder,
    SearchTraversalPolicy,
)
from carl.core.facebook_work import (
    CollectSearchPayload,
    CreateSearchRequest,
    FacebookSearchRequest,
    SearchDistanceUnit,
    SearchFacebookLocation,
    SearchPriceRange,
    SearchRadius,
    is_transient_search_failure,
    retryable_search_failure,
)
from carl.core.json import encode_json
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    FacebookSearchTargetSpecification,
    ListMarketplaceSearchResultsRequest,
)
from carl.core.models import Header, JsonValue
from carl.core.work import WorkState
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkerSettings,
    WorkOutcome,
)
from carl.facebook_refresh_workers import (
    RefreshWorkerDependencies,
    _search_child_delay_ns,
    _search_phase,
)
from carl.facebook_search_workers import FacebookSearchWorkerDependencies
from carl.facebook_workers import FacebookWorkerDependencies, build_facebook_worker_registry
from carl.io.httpx import ClientHttpxAcquirer, DirectHttpxAcquirer
from carl.io.network_activity import NetworkActivityScheduler
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, execute_lease
from carl.review import ReviewApplication

MINUTE_NS = 60_000_000_000
SETTINGS = WorkerSettings(
    worker_count=1,
    lease_duration_ns=60_000_000_000,
    renewal_interval_ns=10_000_000_000,
    idle_poll_interval_ns=10_000_000,
)


@pytest.mark.parametrize(
    ("state", "eligible_at", "expected"),
    (
        ("pending", 100 + 10 * MINUTE_NS, 10 * MINUTE_NS),
        ("pending", 100, 5_000_000_000),
        ("pending", 99, 5_000_000_000),
        ("pending", None, 5_000_000_000),
        ("leased", 100 + 10 * MINUTE_NS, 5_000_000_000),
    ),
)
def test_parent_delay_polls_only_runnable_or_leased_search_children(
    state: str, eligible_at: int | None, expected: int
) -> None:
    assert (
        _search_child_delay_ns({"state": state, "eligible_at_utc_ns": eligible_at}, 100) == expected
    )


@dataclass
class _Clock:
    now: int = 1_800_000_000_000_000_000

    def __call__(self) -> int:
        return self.now


@dataclass
class _PartitionSessionFactory:
    challenges: int
    malformed: bool = False
    challenge_location: str = "90"
    challenges_by_partition: dict[str, int] = field(default_factory=dict)
    transport_failures_by_partition: dict[str, int] = field(default_factory=dict)
    login_walls_by_partition: dict[str, int] = field(default_factory=dict)
    sessions: list[str] = field(default_factory=list)
    requests: list[tuple[str, str]] = field(default_factory=list)

    def challenge(self, location: str) -> bool:
        remaining = self.challenges_by_partition.get(location)
        if remaining is not None:
            if remaining == 0:
                return False
            self.challenges_by_partition[location] = remaining - 1
            return True
        if location == self.challenge_location and self.challenges > 0:
            self.challenges -= 1
            return True
        return False

    @asynccontextmanager
    async def __call__(self, identifier: str) -> AsyncGenerator[_FakeSession]:
        self.sessions.append(identifier)

        def respond(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/marketplace/":
                self.requests.append((identifier, "bootstrap"))
                body = (
                    b'<html><form id="checkpointSubmitButton"></form></html>'
                    if self.challenge("bootstrap")
                    else _session_bootstrap()
                )
            else:
                assert request.url.path == "/ajax/route-definition/"
                form = parse_qs(request.content.decode("ascii"), strict_parsing=True)
                minimum = parse_qs(urlsplit(form["route_url"][0]).query)["minPrice"][0]
                self.requests.append((identifier, minimum))
                if self.login_walls_by_partition.get(minimum, 0) > 0:
                    self.login_walls_by_partition[minimum] -= 1
                    body = (
                        b'for (;;);{"jsmods":{"require":[["ServerRedirect","redirectPageTo",[], '
                        b'["/login/?next=marketplace",true,false]]]}}'
                    )
                elif self.challenge(minimum) or (minimum == "90" and self.malformed):
                    if self.malformed:
                        body = b'for (;;);{"__type":"last_response"}'
                    else:
                        body = b'for (;;);{"__type":"first_response","error":"checkpoint captcha"}'
                elif self.transport_failures_by_partition.get(minimum, 0) > 0:
                    self.transport_failures_by_partition[minimum] -= 1
                    raise httpx.ConnectError("synthetic connection failure", request=request)
                else:
                    # A distinct card per partition exposes missing earlier successes.
                    body = _route_definition().replace(
                        json.dumps(
                            _feed(ids=("1", "2"), cursor="cursor-1", has_next_page=True)
                        ).encode(),
                        json.dumps(
                            _feed(
                                ids=({"0": "1", "90": "2", "180": "3"}[minimum],),
                                cursor=f"cursor-{minimum}",
                                has_next_page=False,
                            )
                        ).encode(),
                    )
            return httpx.Response(
                200, headers={"Content-Type": "application/json"}, stream=_Stream(body)
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            session = _FakeSession(
                identifier=identifier,
                acquirer=ClientHttpxAcquirer(
                    client=client,
                    expected_routing=("proton", "personal", "carl"),
                    routing_observation={
                        "network_session_identifier": identifier,
                        "state": "ready",
                    },
                    authentication="anonymous_guest_session",
                    protected_request_headers=frozenset({b"cookie", b"x-fb-lsd"}),
                    protected_response_headers=frozenset({b"set-cookie"}),
                ),
            )
            try:
                yield session
            finally:
                session.closed = True


@final
class _Harness:
    def __init__(self, path: Path, factory: _PartitionSessionFactory):
        self.path = path
        self.factory = factory
        self.clock = _Clock()
        self.identifiers = count()

    def new_identifier(self) -> str:
        return f"challenge-{next(self.identifiers)}"

    def app(self, database: Database) -> ReviewApplication:
        return ReviewApplication(
            database,
            self.path.parent,
            new_identifier=self.new_identifier,
            utc_now_ns=self.clock,
            code_provenance=_async_provenance,
        )

    async def create(self, database: Database) -> tuple[str, str]:
        search = await self.app(database).create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=(
                    FacebookSearchTargetSpecification(
                        search=CreateSearchRequest(
                            request=FacebookSearchRequest(
                                query="telescope",
                                location=SearchFacebookLocation(identifier="456"),
                                radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
                                price=SearchPriceRange(currency="USD", maximum=Decimal("250")),
                            ),
                            traversal=SearchTraversalPolicy(maximum_pages=10),
                            traversal_strategy=OverlappingPricePartitionSearchTraversalStrategy(
                                width=Decimal("100"),
                                overlap=Decimal("10"),
                                order=SearchPricePartitionOrder.ASCENDING,
                            ),
                            network_path=("proton", "personal", "carl"),
                        ),
                    ),
                )
            )
        )
        return search.record_identifier, search.targets[0].executions[0].work_identifier

    async def run(self, database: Database, work_identifier: str) -> WorkOutcome:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(
                database=database,
                acquirer=DirectHttpxAcquirer(),
                new_identifier=self.new_identifier,
            ),
            FacebookSearchWorkerDependencies(
                database=database,
                session_factory=self.factory,
                navigation_headers=(Header(name=b"User-Agent", value=b"test-browser"),),
                new_identifier=self.new_identifier,
                utc_now_ns=self.clock,
                monotonic_ns=perf_counter_ns,
                network_activity_scheduler=NetworkActivityScheduler(
                    database=database,
                    new_identifier=self.new_identifier,
                    utc_now_ns=self.clock,
                    sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
                    permit_duration_ns=SETTINGS.lease_duration_ns,
                ),
            ),
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="challenge-worker",
            lease_token=self.new_identifier(),
            lease_duration_ns=SETTINGS.lease_duration_ns,
            utc_now_ns=self.clock,
            event_identifier=self.new_identifier(),
        )
        assert claim.lease is not None
        assert claim.lease.work_item_identifier == work_identifier
        outcome = await execute_lease(
            database=database,
            registry=registry,
            settings=SETTINGS,
            services=WorkerRuntimeServices(
                new_identifier=self.new_identifier,
                utc_now_ns=self.clock,
                monotonic_ns=perf_counter_ns,
                code_provenance=_async_provenance,
                invocation=lambda: {"kind": "test"},
            ),
            lease=claim.lease,
        )
        assert outcome is not None
        return outcome

    async def parent(self, database: Database, work_identifier: str) -> WorkOutcome:
        child = await database.work(work_identifier)
        payload = CollectSearchPayload.model_validate_json(json.dumps(child["payload"]))
        return await _search_phase(
            RefreshSearchPayload(
                base_search_run_record_identifier="source-search-run",
                search_work_identifier=work_identifier,
                search=payload,
                item_routing=payload.routing,
                image_routing=payload.routing,
            ),
            AttemptContext(
                work_item_identifier="refresh-parent",
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="parent-operation",
            ),
            RefreshWorkerDependencies(
                database=database,
                new_identifier=self.new_identifier,
                utc_now_ns=self.clock,
            ),
        )


@pytest.mark.anyio
async def test_partition_challenge_resumes_after_restart_and_preserves_public_cards(
    tmp_path: Path,
) -> None:
    factory = _PartitionSessionFactory(challenges=1)
    harness = _Harness(tmp_path / "carl.sqlite3", factory)
    async with Database.managed(harness.path, initialize=True) as database:
        search_identifier, work_identifier = await harness.create(database)
        first = await harness.run(database, work_identifier)
        assert isinstance(first, RetryWork)
        assert first.delay_ns == 10 * MINUTE_NS
        work = await database.work(work_identifier)
        assert work["state"] == WorkState.PENDING
        result = cast(dict[str, JsonValue], work["result"])
        assert result["partition_retry_checkpoint"] is not None
        eligible = work["eligible_at_utc_ns"]
        assert eligible == harness.clock.now + first.delay_ns
        assert isinstance(eligible, int)
        harness.clock.now = eligible

    # The retry must recover solely from durable state, not a live connection/session.
    async with Database.managed(harness.path) as database:
        outcome = await harness.run(database, work_identifier)
        assert isinstance(outcome, CompletedWork)
        work = await database.work(work_identifier)
        assert work["state"] == WorkState.COMPLETED
        result = cast(dict[str, JsonValue], work["result"])
        assert result["pages_processed"] == 3
        assert result["listing_observations"] == 3
        assert result["unique_listings"] == 3
        run_identifier = result["search_run_record_identifier"]
        assert isinstance(run_identifier, str)
        app = harness.app(database)
        public = await app.list_marketplace_search_results(
            ListMarketplaceSearchResultsRequest(search_record_identifier=search_identifier)
        )
        assert {result.external_identifier for result in public.results} == {"1", "2", "3"}
        assert public.execution_warnings == ()
        composed = await app.list_composed_search(
            ListComposedSearchRequest(
                search_run_record_identifier=run_identifier,
                filters=ComposedListingFilters(statuses=(ListingStatus.UNKNOWN,)),
            )
        )
        assert {listing.listing_identifier for listing in composed.listings} == {"1", "2", "3"}
        earlier = await app.get_composed_listing(
            GetComposedListingRequest(
                listing_identifier="1", search_run_record_identifier=run_identifier
            )
        )
        assert earlier.title is not None
        assert earlier.title.value == "Listing 1"

    assert len(set(factory.sessions)) == 2
    assert [request for _, request in factory.requests] == [
        "bootstrap",
        "0",
        "90",
        "bootstrap",
        "90",
        "180",
    ]


@pytest.mark.anyio
async def test_partition_challenge_exhausts_five_attempts_with_long_backoff(tmp_path: Path) -> None:
    factory = _PartitionSessionFactory(challenges=5)
    harness = _Harness(tmp_path / "carl.sqlite3", factory)
    async with Database.managed(harness.path, initialize=True) as database:
        _, work_identifier = await harness.create(database)
    for attempt in range(1, 6):
        async with Database.managed(harness.path) as database:
            outcome = await harness.run(database, work_identifier)
            work = await database.work(work_identifier)
            assert work["attempt"] == attempt
            if attempt < 5:
                assert isinstance(outcome, RetryWork)
                assert outcome.delay_ns == (10, 20, 40, 60)[attempt - 1] * MINUTE_NS
                assert work["state"] == WorkState.PENDING
                eligible = work["eligible_at_utc_ns"]
                assert eligible == harness.clock.now + outcome.delay_ns
                assert isinstance(eligible, int)
                harness.clock.now = eligible
            else:
                assert isinstance(outcome, TerminalFailureWork)
                assert work["state"] == WorkState.TERMINAL_FAILURE
                assert cast(dict[str, JsonValue], outcome.error)["response_kind"] == "bot_challenge"
                parent = await harness.parent(database, work_identifier)
                assert isinstance(parent, TerminalFailureWork)
                assert cast(dict[str, JsonValue], parent.result)["stage"] == "search_failed"
                assert await database.work(work_identifier) == work
    assert len(set(factory.sessions)) == 5
    assert [request for _, request in factory.requests].count("0") == 1
    assert [request for _, request in factory.requests].count("90") == 5


@pytest.mark.anyio
async def test_interrupted_guest_login_resumes_same_partition_with_fresh_session(
    tmp_path: Path,
) -> None:
    factory = _PartitionSessionFactory(challenges=0, login_walls_by_partition={"90": 1})
    harness = _Harness(tmp_path / "carl.sqlite3", factory)
    async with Database.managed(harness.path, initialize=True) as database:
        _, work_identifier = await harness.create(database)
        first = await harness.run(database, work_identifier)
        assert isinstance(first, RetryWork)
        reason = cast(dict[str, JsonValue], first.reason)
        assert reason["response_kind"] == "login_page"
        assert reason["guest_access_interrupted"] is True
        assert first.delay_ns == 10 * MINUTE_NS
        assert cast(dict[str, JsonValue], first.result)["partition_retry_checkpoint"] is not None
        harness.clock.now += first.delay_ns
    async with Database.managed(harness.path) as database:
        final = await harness.run(database, work_identifier)
        assert isinstance(final, CompletedWork)
        assert cast(dict[str, JsonValue], final.result)["pages_processed"] == 3
        assert cast(dict[str, JsonValue], final.result)["unique_listings"] == 3
    assert len(set(factory.sessions)) == 2
    assert [request for _, request in factory.requests] == [
        "bootstrap",
        "0",
        "90",
        "bootstrap",
        "90",
        "180",
    ]


@pytest.mark.anyio
async def test_initial_guest_auth_wall_is_terminal_without_retry(tmp_path: Path) -> None:
    factory = _PartitionSessionFactory(challenges=0, login_walls_by_partition={"0": 1})
    harness = _Harness(tmp_path / "carl.sqlite3", factory)
    async with Database.managed(harness.path, initialize=True) as database:
        _, work_identifier = await harness.create(database)
        outcome = await harness.run(database, work_identifier)
        assert isinstance(outcome, TerminalFailureWork)
        error = cast(dict[str, JsonValue], outcome.error)
        assert error == {
            "kind": "search_route_definition_response_failure",
            "response_kind": "login_page",
        }
    assert len(factory.sessions) == 1
    assert [request for _, request in factory.requests] == ["bootstrap", "0"]


@pytest.mark.anyio
@pytest.mark.parametrize(("challenges", "login_walls"), ((2, 3), (0, 5)))
async def test_guest_login_and_challenge_share_one_bounded_partition_budget(
    tmp_path: Path, challenges: int, login_walls: int
) -> None:
    factory = _PartitionSessionFactory(
        challenges=challenges, login_walls_by_partition={"90": login_walls}
    )
    harness = _Harness(tmp_path / "carl.sqlite3", factory)
    async with Database.managed(harness.path, initialize=True) as database:
        _, work_identifier = await harness.create(database)
        for attempt in range(1, 6):
            outcome = await harness.run(database, work_identifier)
            if attempt < 5:
                assert isinstance(outcome, RetryWork)
                assert outcome.delay_ns == (10, 20, 40, 60)[attempt - 1] * MINUTE_NS
                assert cast(dict[str, JsonValue], outcome.reason)["response_kind"] == (
                    "login_page" if attempt <= login_walls else "bot_challenge"
                )
                harness.clock.now += outcome.delay_ns
            else:
                assert isinstance(outcome, TerminalFailureWork)
                assert cast(dict[str, JsonValue], outcome.error)["decision"] == "retry_exhausted"
                assert cast(dict[str, JsonValue], outcome.error)["response_kind"] == (
                    "login_page" if login_walls == 5 else "bot_challenge"
                )
                parent = await harness.parent(database, work_identifier)
                assert isinstance(parent, TerminalFailureWork)
    assert len(set(factory.sessions)) == 5
    assert [request for _, request in factory.requests].count("0") == 1
    assert [request for _, request in factory.requests].count("90") == 5


def test_login_retry_requires_proven_guest_progress_and_preserves_exhaustion() -> None:
    initial = {"kind": "search_price_partition_extraction_failure", "response_kind": "login_page"}
    assert not is_transient_search_failure(initial)
    assert not retryable_search_failure(initial, attempt=1)
    interrupted = {**initial, "guest_access_interrupted": True, "challenge_attempt": 2}
    assert is_transient_search_failure(interrupted)
    # Successful intervening partitions can make the physical attempt exceed
    # five while this partition still has a bounded independent retry budget.
    assert retryable_search_failure(interrupted, attempt=11)
    assert not retryable_search_failure({**interrupted, "decision": "retry_exhausted"}, attempt=1)


@pytest.mark.anyio
async def test_missing_partition_protocol_remains_terminal_without_retry(tmp_path: Path) -> None:
    harness = _Harness(
        tmp_path / "carl.sqlite3", _PartitionSessionFactory(challenges=0, malformed=True)
    )
    async with Database.managed(harness.path, initialize=True) as database:
        _, work_identifier = await harness.create(database)
        outcome = await harness.run(database, work_identifier)
        assert isinstance(outcome, TerminalFailureWork)
        error = cast(dict[str, JsonValue], outcome.error)
        assert error["kind"] == "search_price_partition_extraction_failure"
        assert error["response_kind"] == "search_payload_absent"
        work = await database.work(work_identifier)
        assert work["state"] == WorkState.TERMINAL_FAILURE
        assert work["attempt"] == 1


@pytest.mark.anyio
async def test_refresh_parent_waits_until_challenged_search_is_eligible(tmp_path: Path) -> None:
    harness = _Harness(tmp_path / "carl.sqlite3", _PartitionSessionFactory(challenges=1))
    async with Database.managed(harness.path, initialize=True) as database:
        _, work_identifier = await harness.create(database)
        child_outcome = await harness.run(database, work_identifier)
        assert isinstance(child_outcome, RetryWork)
        parent = await harness.parent(database, work_identifier)
        assert isinstance(parent, RetryWork)
        assert parent.delay_ns == child_outcome.delay_ns
        assert parent.delay_ns > 5_000_000_000
        assert cast(dict[str, JsonValue], parent.result)["stage"] == "collecting_search"


@pytest.mark.anyio
@pytest.mark.parametrize("location", ("bootstrap", "0"))
async def test_initial_challenge_reopens_session_and_finishes_all_partitions(
    tmp_path: Path, location: str
) -> None:
    factory = _PartitionSessionFactory(challenges=1, challenge_location=location)
    harness = _Harness(tmp_path / "carl.sqlite3", factory)
    async with Database.managed(harness.path, initialize=True) as database:
        search_identifier, work_identifier = await harness.create(database)
        first = await harness.run(database, work_identifier)
        assert isinstance(first, RetryWork)
        assert first.delay_ns == 10 * MINUTE_NS
        work = await database.work(work_identifier)
        eligible = work["eligible_at_utc_ns"]
        assert isinstance(eligible, int)
        harness.clock.now = eligible
    async with Database.managed(harness.path) as database:
        completed = await harness.run(database, work_identifier)
        assert isinstance(completed, CompletedWork)
        assert cast(dict[str, JsonValue], completed.result)["pages_processed"] == 3
        public = await harness.app(database).list_marketplace_search_results(
            ListMarketplaceSearchResultsRequest(search_record_identifier=search_identifier)
        )
        assert {result.external_identifier for result in public.results} == {"1", "2", "3"}
    assert len(set(factory.sessions)) == 2
    assert [request for _, request in factory.requests].count("bootstrap") == 2
    assert [request for _, request in factory.requests].count("0") == (
        1 if location == "bootstrap" else 2
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "transport_failures", (0, 1), ids=("partition_reset", "separate_transport_budget")
)
async def test_successful_partition_resets_challenge_budget_for_next_partition(
    tmp_path: Path,
    transport_failures: int,
) -> None:
    factory = _PartitionSessionFactory(
        challenges=0,
        challenges_by_partition={"90": 4, "180": 1},
        transport_failures_by_partition={"180": transport_failures},
    )
    harness = _Harness(tmp_path / "carl.sqlite3", factory)
    async with Database.managed(harness.path, initialize=True) as database:
        search_identifier, work_identifier = await harness.create(database)
    delays = tuple(minutes * MINUTE_NS for minutes in (10, 20, 40, 60, 10))
    if transport_failures:
        # Five physical challenge attempts leave the first transport retry at one second.
        delays += (1_000_000_000,)
    for delay in delays:
        async with Database.managed(harness.path) as database:
            outcome = await harness.run(database, work_identifier)
            assert isinstance(outcome, RetryWork)
            assert outcome.delay_ns == delay
            work = await database.work(work_identifier)
            eligible = work["eligible_at_utc_ns"]
            assert isinstance(eligible, int)
            harness.clock.now = eligible
    async with Database.managed(harness.path) as database:
        completed = await harness.run(database, work_identifier)
        assert isinstance(completed, CompletedWork)
        work = await database.work(work_identifier)
        assert work["attempt"] == 6 + transport_failures
        assert cast(dict[str, JsonValue], work["result"])["pages_processed"] == 3
        public = await harness.app(database).list_marketplace_search_results(
            ListMarketplaceSearchResultsRequest(search_record_identifier=search_identifier)
        )
        assert {result.external_identifier for result in public.results} == {"1", "2", "3"}
    assert len(set(factory.sessions)) == 6 + transport_failures
    routes = [request for _, request in factory.requests]
    assert routes.count("0") == 1
    assert routes.count("90") == 5
    assert routes.count("180") == 2 + transport_failures


@pytest.mark.anyio
async def test_inconsistent_checkpoint_policy_is_rejected_before_opening_session(
    tmp_path: Path,
) -> None:
    factory = _PartitionSessionFactory(challenges=1)
    harness = _Harness(tmp_path / "carl.sqlite3", factory)
    async with Database.managed(harness.path, initialize=True) as database:
        _, work_identifier = await harness.create(database)
        first = await harness.run(database, work_identifier)
        assert isinstance(first, RetryWork)
        work = await database.work(work_identifier)
        result = cast(dict[str, JsonValue], work["result"])
        checkpoint = cast(dict[str, JsonValue], result["partition_retry_checkpoint"])
        original_digest = checkpoint["request_sha256"]
        state = cast(dict[str, JsonValue], checkpoint["traversal_state"])
        policy = cast(dict[str, JsonValue], state["policy"])
        policy["maximum_pages"] = 9
        assert checkpoint["request_sha256"] == original_digest
        # Deliberate durable corruption: normal APIs never rewrite a pending checkpoint.
        async with database._connections.writer() as connection:
            _ = await connection.execute(
                """
                UPDATE work_items SET result_json=?
                WHERE id=? AND state='pending' AND attempt=1
                """,
                (encode_json(result), work_identifier),
            )
        eligible = work["eligible_at_utc_ns"]
        assert isinstance(eligible, int)
        harness.clock.now = eligible
    before_requests = tuple(factory.requests)
    async with Database.managed(harness.path) as database:
        outcome = await harness.run(database, work_identifier)
        assert isinstance(outcome, TerminalFailureWork)
        assert (
            cast(dict[str, JsonValue], outcome.error)["kind"]
            == "search_partition_retry_checkpoint_invalid"
        )
        assert (await database.work(work_identifier))["state"] == WorkState.TERMINAL_FAILURE
    assert len(factory.sessions) == 1
    assert tuple(factory.requests) == before_requests
