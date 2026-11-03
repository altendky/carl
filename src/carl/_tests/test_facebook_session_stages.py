"""Resident Facebook price buckets checkpoint each HTTP/CPU phase offline."""

# Fixtures reuse synthetic protocol responses and inspect bounded stage metadata.
# pyright: reportPrivateUsage=false

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from time import time_ns
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import anyio
import httpx
import pytest

import carl.facebook_session_stages as facebook_stages
from carl._tests.test_ebay_workers import _directories
from carl._tests.test_facebook_search_workers import (
    _feed,
    _provenance,
    _route_definition,
    _session_bootstrap,
    _Stream,
)
from carl.core.facebook_search import (
    OverlappingPricePartitionSearchTraversalStrategy,
    SearchPricePartitionOrder,
    SearchTraversalPolicy,
)
from carl.core.facebook_work import (
    CollectSearchPayload,
    FacebookSearchRequest,
    SearchDistanceUnit,
    SearchFacebookLocation,
    SearchPriceRange,
    SearchRadius,
    collect_search_work,
)
from carl.core.models import Header, JsonValue, StrictModel
from carl.core.stages import StageAttemptContext, StageDefinition, WaitWork
from carl.core.work import WorkCapability, WorkRequester
from carl.facebook_session_stages import (
    BoundedFacebookSession,
    FacebookSessionStageDependencies,
    SearchPhasePayload,
    build_facebook_session_stage_registry,
)
from carl.io.facebook_search import FacebookSearchSessionFailure
from carl.io.http_checkpoints import HttpCheckpoint
from carl.io.httpx import Acquisition, ClientHttpxAcquirer, IdentifierFactory
from carl.io.proton import SharedProtonWireproxyManager
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.sqlite import Database
from carl.io.stage_publication import StageHandlerResult
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


@dataclass
class _Clock:
    now: int = field(default_factory=time_ns)
    reference: int = field(default_factory=time_ns)

    def __call__(self) -> int:
        return self.now + time_ns() - self.reference


@dataclass
class _Session:
    identifier: str
    acquirer: ClientHttpxAcquirer
    closed: bool = False

    def retained_bytes(self) -> int:
        return self.acquirer.retained_bytes()

    def is_closed(self) -> bool:
        return self.closed

    def active_observation(self) -> dict[str, JsonValue]:
        return {"network_session_identifier": self.identifier, "state": "ready"}

    def completed_observation(self) -> dict[str, JsonValue]:
        assert self.closed
        return {"network_session_identifier": self.identifier, "state": "stopped"}


@dataclass
class _Sessions:
    challenges: int = 0
    transport_failures: int = 0
    cleanup_failure: bool = False
    cleanup_runtime_error: bool = False
    requests: list[tuple[str, str, str]] = field(default_factory=list)
    sessions: list[_Session] = field(default_factory=list)
    clock: _Clock | None = None
    form_requests: list[str] = field(default_factory=list)

    @asynccontextmanager
    async def __call__(self, identifier: str) -> AsyncIterator[BoundedFacebookSession]:
        if self.clock is not None:
            self.clock.now += 1_000_000_000

        def respond(request: httpx.Request) -> httpx.Response:
            if self.clock is not None:
                self.clock.now += 1_000_000_000
            if request.url.path == "/marketplace/":
                phase = "bootstrap"
                body = _session_bootstrap()
                headers = {
                    "Content-Type": "text/html; charset=utf-8",
                    "Set-Cookie": "guest=resident-secret-cookie; Secure; Path=/",
                }
            else:
                form = parse_qs(request.content.decode("ascii"), strict_parsing=True)
                self.form_requests.append(form["__req"][0])
                phase = parse_qs(urlsplit(form["route_url"][0]).query).get("minPrice", ["0"])[0]
                if phase == "90" and self.transport_failures:
                    self.transport_failures -= 1
                    self.requests.append((identifier, phase, request.headers.get("cookie", "")))
                    raise httpx.ConnectError("synthetic transport failure", request=request)
                headers = {"Content-Type": "application/json; charset=utf-8"}
                if phase == "90" and self.challenges:
                    self.challenges -= 1
                    body = b'for (;;);{"__type":"first_response","error":"checkpoint captcha"}'
                else:
                    original = json.dumps(
                        _feed(ids=("1", "2"), cursor="cursor-1", has_next_page=True)
                    )
                    replacement = json.dumps(
                        _feed(
                            ids=(str(int(phase) + 1), str(int(phase) + 2)),
                            cursor="cursor-1",
                            has_next_page=True,
                        )
                    )
                    body = _route_definition().replace(original.encode(), replacement.encode())
            self.requests.append((identifier, phase, request.headers.get("cookie", "")))
            return httpx.Response(200, headers=headers, stream=_Stream(body))

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(respond), follow_redirects=False
        ) as client:
            session = _Session(
                identifier,
                ClientHttpxAcquirer(
                    client=client,
                    expected_routing=("proton", "fixture", "search"),
                    routing_observation={"state": "ready"},
                    authentication="anonymous_guest_session",
                    protected_request_headers=frozenset({b"cookie", b"x-fb-lsd"}),
                    protected_response_headers=frozenset({b"set-cookie"}),
                ),
            )
            self.sessions.append(session)
            try:
                yield session
            finally:
                session.closed = True
        if self.cleanup_failure:
            if self.cleanup_runtime_error:
                raise RuntimeError("synthetic cleanup failure")
            raise FacebookSearchSessionFailure("client_cleanup_failed", provider="fixture")


async def _seed(
    database: Database, registry: StageHandlerRegistry, *, query: str = "telescope"
) -> StageDefinition:
    payload = CollectSearchPayload(
        request=FacebookSearchRequest(
            query=query,
            location=SearchFacebookLocation(identifier="456"),
            radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
            price=SearchPriceRange(currency="USD", maximum=Decimal("180")),
        ),
        traversal=SearchTraversalPolicy(maximum_pages=10, maximum_results=100),
        traversal_strategy=OverlappingPricePartitionSearchTraversalStrategy(
            width=Decimal("90"), order=SearchPricePartitionOrder.ASCENDING
        ),
        routing=("proton", "fixture", "search"),
    )
    work = collect_search_work(identifier=str(uuid4()), payload=payload, not_before_utc_ns=0)
    _ = await database.enqueue_work(
        work,
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier=work.identifier, context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    adapter = registry.require_adapter(
        WorkCapability(kind=work.kind, payload_schema_version=work.payload_schema_version)
    )
    return adapter.initial_stage(work, str(uuid4()), str(uuid4()))


def _registry(
    database: Database,
    tmp_path: Path,
    pool: ResidentSessionPool,
    sessions: _Sessions,
    clock: _Clock,
    *,
    independent: bool = False,
) -> StageHandlerRegistry:
    sessions.clock = clock
    return build_facebook_session_stage_registry(
        FacebookSessionStageDependencies(
            database=database,
            directories=_directories(tmp_path),
            new_identifier=lambda: str(uuid4()),
            proton_manager=SharedProtonWireproxyManager(),
            residents=pool,
            session_factory=sessions,
            navigation_headers=(Header(name=b"User-Agent", value=b"fixture-browser"),),
            utc_now_ns=clock,
            retry_scope="independent_session" if independent else "stack",
        )
    )


def _pool(database: Database, clock: _Clock) -> ResidentSessionPool:
    return ResidentSessionPool(
        replace(stage_manager(database), utc_now_ns=clock),
        StageHandlerRegistry(adapters=(), handlers=()),
        _provenance(),
        {},
        poll_seconds=0.01,
    )


async def _submit(pool: ResidentSessionPool, stage: StageDefinition) -> None:
    adapter = pool.registry.adapters[0]
    with anyio.fail_after(10):
        assert await pool.submit(stage, component=adapter.component)
        await pool.wait_idle()
    assert not pool.errors


async def test_two_buckets_share_one_cookie_session_across_durable_cpu_phases(
    tmp_path: Path,
) -> None:
    clock, sessions = _Clock(), _Sessions()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool = _pool(database, clock)
        pool.registry = _registry(database, tmp_path, pool, sessions, clock)
        stage = await _seed(database, pool.registry)
        clock.now = time_ns()
        async with pool.managed():
            await _submit(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        assert work["result"]["pages_processed"] == 2 and work["result"]["unique_listings"] == 4
        assert [request[1] for request in sessions.requests] == ["bootstrap", "0", "90"]
        assert len(sessions.sessions) == 1 and sessions.sessions[0].closed
        assert sessions.form_requests == ["1", "2"]
        assert all("resident-secret-cookie" in request[2] for request in sessions.requests[1:])
        assert (await database.operation(stage.operation_identifier))["state"] == "completed"
        bootstrap_records = await database.records_by_kind(
            ("carl", "facebook", "search_session_bootstrap_extraction")
        )
        assert len(bootstrap_records) == 1
        assert bootstrap_records[0][1]["extractor"]["component_parts"] == [
            "carl",
            "facebook",
            "extract",
            "embedded_json_blocks",
        ]
        progress = await database.records_by_kind(("carl", "facebook", "search_stage_progress"))
        assert all(
            "synthetic-lsd" not in json.dumps(value)
            and "resident-secret-cookie" not in json.dumps(value)
            for _, value in progress
        )
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT definition_json FROM work_stages WHERE work_item_id = ?",
                (stage.work_item_identifier,),
            )
            definitions = await cursor.fetchall()
        assert len(definitions) >= 9
        for (raw,) in definitions:
            assert isinstance(raw, str)
            assert len(raw.encode()) < 65_536 and "synthetic-lsd" not in raw


@pytest.mark.parametrize("independent", (False, True))
async def test_challenge_retry_resumes_completed_bucket_without_resetting_its_budget(
    tmp_path: Path, independent: bool
) -> None:
    clock, sessions = _Clock(), _Sessions(challenges=1)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool = _pool(database, clock)
        pool.registry = _registry(
            database, tmp_path, pool, sessions, clock, independent=independent
        )
        stage = await _seed(database, pool.registry)
        clock.now = time_ns()
        async with pool.managed():
            await _submit(pool, stage)
            work = await database.work(stage.work_item_identifier)
            assert work["state"] == "pending" and work["attempt"] == 1
            assert work["result"]["partition_retry_checkpoint"]["challenge_attempts"] == 1
            assert sessions.sessions[0].closed
            clock.now = work["eligible_at_utc_ns"]
            replacement = stage.model_copy(
                update={"identifier": str(uuid4()), "operation_identifier": str(uuid4())}
            )
            await _submit(pool, replacement)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 2
        assert work["result"]["unique_listings"] == 4
        assert [request[1] for request in sessions.requests] == [
            "bootstrap",
            "0",
            "90",
            "bootstrap",
            "90",
        ]
        assert len(sessions.sessions) == 2
        assert sessions.form_requests == ["1", "2", "1"]


@pytest.mark.parametrize("independent", (False, True))
async def test_transport_failure_never_defers_siblings(tmp_path: Path, independent: bool) -> None:
    clock, sessions = _Clock(), _Sessions(transport_failures=1)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool = _pool(database, clock)
        pool.registry = _registry(
            database, tmp_path, pool, sessions, clock, independent=independent
        )
        stage = await _seed(database, pool.registry)
        sibling = await _seed(database, pool.registry, query="other telescope")
        before = (await database.work(sibling.work_item_identifier))["eligible_at_utc_ns"]
        clock.now = time_ns()
        async with pool.managed():
            await _submit(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "pending"
        assert work["result"]["retry_scope"] == ("independent_session" if independent else "stack")
        assert (
            work["result"]["partition_retry_checkpoint"]["traversal_state"]["partitions_processed"]
            == 1
        )
        after = (await database.work(sibling.work_item_identifier))["eligible_at_utc_ns"]
        assert after == before
        assert sessions.sessions[0].closed


@pytest.mark.parametrize("runtime_error", (False, True))
async def test_cleanup_failure_preserves_completed_pages_without_an_acquisition_retry(
    tmp_path: Path,
    runtime_error: bool,
) -> None:
    clock, sessions = _Clock(), _Sessions(cleanup_failure=True, cleanup_runtime_error=runtime_error)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool = _pool(database, clock)
        pool.registry = _registry(database, tmp_path, pool, sessions, clock)
        stage = await _seed(database, pool.registry)
        clock.now = time_ns()
        async with pool.managed():
            await _submit(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        assert work["result"]["pages_processed"] == 2
        assert work["result"]["session_cleanup_failure"]["kind"] == "resident_cleanup_failure"
        assert [request[1] for request in sessions.requests] == ["bootstrap", "0", "90"]


async def test_parked_after_partition_capture_replays_cpu_without_repeating_paid_page(
    tmp_path: Path,
) -> None:
    clock, sessions = _Clock(), _Sessions()
    parked: list[StageDefinition] = []
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool = _pool(database, clock)
        registry = _registry(database, tmp_path, pool, sessions, clock)
        capture_handler = next(
            handler for handler in registry.handlers if handler.kind[-1] == "partition_capture"
        )

        async def park(payload: StrictModel, context: StageAttemptContext) -> StageHandlerResult:
            outcome = await capture_handler.execute(payload.model_dump(mode="json"), context)
            assert isinstance(outcome, WaitWork)
            if not parked:
                continuation = outcome.continuation.model_copy(
                    update={"not_before_utc_ns": clock.now + 60_000_000_000}
                )
                parked.append(continuation)
                return outcome.model_copy(update={"continuation": continuation})
            return outcome

        pool.registry = StageHandlerRegistry(
            registry.adapters,
            tuple(
                TypedStageHandler(
                    kind=handler.kind,
                    schema_version=1,
                    payload_type=SearchPhasePayload,
                    handler=park,
                )
                if handler is capture_handler
                else handler
                for handler in registry.handlers
            ),
        )
        stage = await _seed(database, pool.registry)
        clock.now = time_ns()
        async with pool.managed():
            await _submit(pool, stage)
            assert parked and [request[1] for request in sessions.requests] == ["bootstrap", "0"]
            assert (await database.work(stage.work_item_identifier))["attempt"] == 1
            clock.now = parked[0].not_before_utc_ns
            pool.registry = registry
            await _submit(pool, parked[0])
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        assert len(work["operations"]) == 1
        assert sessions.form_requests == ["1", "1"]
        assert [request[1] for request in sessions.requests] == [
            "bootstrap",
            "0",
            "bootstrap",
            "90",
        ]
        assert work["result"]["unique_listings"] == 4


@pytest.mark.parametrize("lost_phase", ("bootstrap", "0"))
async def test_lost_capture_cannot_dispatch_again_in_the_same_logical_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    lost_phase: str,
) -> None:
    class LostCapture(BaseException):
        pass

    clock, sessions = _Clock(), _Sessions()
    checkpoint = facebook_stages.checkpoint_acquisition
    interrupted = False

    async def lose_capture(
        acquisition: Acquisition,
        *,
        data_directory: Path,
        new_identifier: IdentifierFactory,
    ) -> HttpCheckpoint:
        nonlocal interrupted
        if not interrupted and sessions.requests[-1][1] == lost_phase:
            interrupted = True
            raise LostCapture()
        return await checkpoint(
            acquisition, data_directory=data_directory, new_identifier=new_identifier
        )

    monkeypatch.setattr(facebook_stages, "checkpoint_acquisition", lose_capture)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool = _pool(database, clock)
        pool.registry = _registry(database, tmp_path, pool, sessions, clock)
        stage = await _seed(database, pool.registry)
        clock.now = time_ns()
        async with pool.managed():
            with anyio.fail_after(10):
                assert await pool.submit(stage, component=pool.registry.adapters[0].component)
                await pool.wait_idle()
            assert list(pool.errors.values()) == ["LostCapture"]
            assert interrupted and sessions.sessions[0].closed
            before = list(sessions.requests)
            async with database._connections.reader() as connection:
                cursor = await connection.execute(
                    "SELECT definition_json FROM work_stages WHERE work_item_id = ? AND state = 'pending'",
                    (stage.work_item_identifier,),
                )
                pending = await cursor.fetchall()
            assert len(pending) == 1 and isinstance(pending[0][0], str)
            continuation = StageDefinition.model_validate_json(pending[0][0])
            pool.errors.clear()
            await _submit(pool, continuation)
            assert sessions.requests == before
        work = await database.work(stage.work_item_identifier)
        assert work["attempt"] == 1 and len(work["operations"]) == 1
        assert work["state"] == "pending"
        assert work["result"]["acquisition"]["kind"] == "uncertain_previous_dispatch"
        assert work["result"]["acquisition"]["failure_phase"] == "uncertain_previous_dispatch"
        assert all(session.closed for session in sessions.sessions)
