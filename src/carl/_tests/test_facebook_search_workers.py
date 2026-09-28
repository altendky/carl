import json
import sqlite3
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass
from decimal import Decimal
from itertools import count
from pathlib import Path
from time import perf_counter_ns, time_ns
from urllib.parse import parse_qs

import httpx
import pytest

from carl.core.facebook_refresh import RefreshSearchPayload, SearchRunOrigin, refresh_search_work
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
from carl.core.models import CodeProvenance, Header, JsonValue
from carl.core.work import WorkRequester, WorkState
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkerSettings,
)
from carl.facebook_search_workers import (
    FacebookSearchWorkerDependencies,
    search_acquisition_failure_work,
    search_session_failure_work,
)
from carl.facebook_workers import FacebookWorkerDependencies, build_facebook_worker_registry
from carl.io.facebook_search import FacebookSearchHttpSession, FacebookSearchSessionFailure
from carl.io.httpx import (
    AcquisitionFailure,
    ClientHttpxAcquirer,
    DirectHttpxAcquirer,
    HttpFormAcquirer,
)
from carl.io.network_activity import NetworkActivityScheduler
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, execute_lease
from carl.review import ReviewApplication


class _Stream(httpx.AsyncByteStream):
    def __init__(self, content: bytes):
        self.content = content

    async def __aiter__(self):
        yield self.content


def _feed(*, ids: tuple[str, ...], cursor: str, has_next_page: bool) -> dict[str, object]:
    return {
        "session_id": "feed-session",
        "page_info": {"has_next_page": has_next_page, "end_cursor": cursor},
        "edges": [
            {
                "cursor": f"edge-{index}",
                "node": {
                    "id": f"story-{index}",
                    "listing": {
                        "id": identifier,
                        "marketplace_listing_title": f"Listing {identifier}",
                    },
                },
            }
            for index, identifier in enumerate(ids)
        ],
    }


def _session_bootstrap() -> bytes:
    value = {
        "require": [
            ["LSD", [], {"token": "synthetic-lsd"}, 1],
            [
                "SiteData",
                [],
                {
                    "hsi": "123456",
                    "__spin_r": 987,
                    "__spin_b": "trunk",
                    "__spin_t": 654,
                },
                2,
            ],
        ],
    }
    return (
        "<html><body>Marketplace"
        '<script type="application/json">' + json.dumps(value) + "</script></body></html>"
    ).encode()


def _route_definition() -> bytes:
    preloader_identifier = "search-preloader"
    frames = (
        {
            "__type": "first_response",
            "preloaders": [
                {
                    "preloaderID": preloader_identifier,
                    "queryName": "CometMarketplaceSearchContentContainerQuery",
                    "queryID": "987654321",
                    "variables": {
                        "count": 24,
                        "cursor": None,
                        "savedSearchQuery": "telescope",
                        "buyLocation": {"latitude": 1.0, "longitude": 2.0},
                        "topicPageParams": {"location_id": "456"},
                        "params": {"browse_request_params": {"filter_radius_km": 97}},
                    },
                }
            ],
        },
        {
            "__type": "preloader",
            "id": preloader_identifier,
            "result": {
                "result": {
                    "data": {
                        "marketplace_search": {
                            "feed_units": _feed(
                                ids=("1", "2"),
                                cursor="cursor-1",
                                has_next_page=True,
                            )
                        }
                    }
                }
            },
        },
        {"__type": "last_response"},
    )
    return "\n".join(f"for (;;);{json.dumps(frame)}" for frame in frames).encode()


def _pagination() -> bytes:
    return json.dumps(
        {
            "data": {
                "marketplace_search": {
                    "feed_units": _feed(
                        ids=("2", "3"),
                        cursor="cursor-2",
                        has_next_page=True,
                    )
                }
            }
        }
    ).encode()


@dataclass
class _FakeSession(FacebookSearchHttpSession):
    identifier: str
    acquirer: HttpFormAcquirer
    closed: bool = False

    def active_observation(self) -> dict[str, JsonValue]:
        return {"network_session_identifier": self.identifier, "state": "ready"}

    def completed_observation(self) -> dict[str, JsonValue]:
        assert self.closed
        return {"network_session_identifier": self.identifier, "state": "stopped"}


class _FakeSessionFactory:
    def __init__(self, requests: list[httpx.Request]):
        self.requests = requests
        self.opens = 0

    @asynccontextmanager
    async def __call__(self, identifier: str):
        self.opens += 1

        def respond(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            if request.url.path == "/marketplace/":
                body = _session_bootstrap()
                media_type = "text/html"
                headers = {
                    "Content-Type": f"{media_type}; charset=utf-8",
                    "Set-Cookie": "anonymous_session=value; Path=/; Secure",
                }
            elif request.url.path == "/ajax/route-definition/":
                body = _route_definition()
                media_type = "application/json"
                headers = {"Content-Type": f"{media_type}; charset=utf-8"}
            else:
                body = _pagination()
                media_type = "application/json"
                headers = {"Content-Type": f"{media_type}; charset=utf-8"}
            return httpx.Response(
                200,
                headers=headers,
                stream=_Stream(body),
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(respond),
            follow_redirects=False,
        ) as client:
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


def test_search_session_lock_contention_retries_then_exhausts() -> None:
    error = FacebookSearchSessionFailure(
        "wireproxy_device_identity_busy",
        provider="proton",
        diagnostic={"lock": "device"},
    )
    context = AttemptContext(
        work_item_identifier="search-work",
        lease_token="lease",
        worker_identifier="worker",
        attempt=1,
        operation_identifier="operation",
    )

    retry = search_session_failure_work(
        error=error,
        context=context,
        search_run_identifier="search-run",
        routing=("proton", "personal", "carl"),
    )
    exhausted = search_session_failure_work(
        error=error,
        context=context.model_copy(update={"attempt": 6}),
        search_run_identifier="search-run",
        routing=("proton", "personal", "carl"),
    )
    operator_retry = search_session_failure_work(
        error=error,
        context=context.model_copy(update={"attempt": 7}),
        search_run_identifier="search-run",
        routing=("proton", "personal", "carl"),
        policy_attempt=1,
    )

    assert isinstance(retry, RetryWork)
    assert retry.delay_ns == 1_000_000_000
    assert isinstance(operator_retry, RetryWork)
    assert operator_retry.delay_ns == 1_000_000_000
    assert retry.reason["code"] == "wireproxy_device_identity_busy"
    assert retry.reason["diagnostic"] == {"lock": "device"}
    assert isinstance(exhausted, TerminalFailureWork)
    assert exhausted.error["decision"] == "retry_exhausted"


def test_search_transport_failure_uses_exponential_backoff_and_larger_budget() -> None:
    error = AcquisitionFailure(
        "failed",
        result={
            "hops": [],
            "stopping_condition": "transport_failure",
            "exception_type": "ConnectError",
            "routing": {
                "observed": {
                    "provider_session": {
                        "endpoint": {"host": "127.0.0.1", "port": 52643},
                        "observed_exit_ip": "203.0.113.45",
                    }
                }
            },
        },
    )
    context = AttemptContext(
        work_item_identifier="search-work",
        lease_token="lease",
        worker_identifier="worker",
        attempt=4,
        operation_identifier="operation",
    )

    retry = search_acquisition_failure_work(
        error=error,
        context=context,
        search_run_identifier="search-run",
    )
    exhausted = search_acquisition_failure_work(
        error=error,
        context=context.model_copy(update={"attempt": 6}),
        search_run_identifier="search-run",
    )

    assert isinstance(retry, RetryWork)
    assert retry.delay_ns == 8_000_000_000
    assert retry.reason["exception_type"] == "ConnectError"
    assert retry.result["acquisition"]["routing"]["observed"]["provider_session"] == {
        "endpoint": {"host": "127.0.0.1", "port": 52643},
        "observed_exit_ip": "203.0.113.45",
    }
    assert isinstance(exhausted, TerminalFailureWork)
    assert exhausted.error["decision"] == "retry_exhausted"


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="dirty",
        package_version="test",
        python_implementation="test",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


@pytest.mark.anyio
async def test_search_worker_uses_one_session_and_checkpoints_every_page(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    identifiers = count()
    requests: list[httpx.Request] = []
    factory = _FakeSessionFactory(requests)

    def new_identifier() -> str:
        return f"identifier-{next(identifiers)}"

    payload = CollectSearchPayload(
        request=FacebookSearchRequest(
            query="telescope",
            location=SearchFacebookLocation(identifier="456"),
            radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
            price=SearchPriceRange(currency="USD", maximum=Decimal("600")),
        ),
        traversal=SearchTraversalPolicy(maximum_pages=3, maximum_results=3),
        routing=("proton", "personal", "carl"),
    )
    settings = WorkerSettings(
        worker_count=1,
        lease_duration_ns=1_000_000_000,
        renewal_interval_ns=100_000_000,
        idle_poll_interval_ns=10_000_000,
    )
    async with Database.managed(path, initialize=True) as database:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(
                database=database,
                acquirer=DirectHttpxAcquirer(),
                new_identifier=new_identifier,
            ),
            FacebookSearchWorkerDependencies(
                database=database,
                session_factory=factory,
                navigation_headers=(Header(name=b"User-Agent", value=b"test-browser"),),
                new_identifier=new_identifier,
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                network_activity_scheduler=NetworkActivityScheduler(
                    database=database,
                    new_identifier=new_identifier,
                    utc_now_ns=time_ns,
                    sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
                    permit_duration_ns=settings.lease_duration_ns,
                ),
            ),
        )
        await database.enqueue_work(
            collect_search_work(
                identifier="search-work",
                payload=payload,
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier="search-request",
                kind=("carl", "mcp", "create_search"),
                identifier="test-search",
                context={},
            ),
            event_identifier=new_identifier(),
            enqueued_at_utc_ns=time_ns(),
        )
        await database.attach_work_request(
            "search-work",
            WorkRequester(
                request_identifier="later-refresh-request",
                kind=("carl", "facebook", "search_refresh", "search"),
                identifier="later-refresh-work",
                context={},
            ),
            event_identifier=new_identifier(),
            requested_at_utc_ns=time_ns(),
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=settings.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=new_identifier(),
        )
        assert claim.lease is not None
        outcome = await execute_lease(
            database=database,
            registry=registry,
            settings=settings,
            services=WorkerRuntimeServices(
                new_identifier=new_identifier,
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                code_provenance=lambda: _async_provenance(),
                invocation=lambda: {"kind": "test"},
            ),
            lease=claim.lease,
        )
        work = await database.work("search-work")
        search_runs = await ReviewApplication(
            database=database, repository_root=tmp_path
        ).list_search_runs()

    assert isinstance(outcome, CompletedWork)
    assert factory.opens == 1
    assert [request.method for request in requests] == ["GET", "POST", "POST"]
    assert requests[0].url.path == "/marketplace/"
    assert requests[1].headers["Cookie"] == "anonymous_session=value"
    assert requests[1].url.path == "/ajax/route-definition/"
    route_form = parse_qs(requests[1].content.decode("ascii"), strict_parsing=True)
    assert route_form["__req"] == ["1"]
    assert route_form["route_url"] == [
        "/marketplace/456/search?query=telescope&maxPrice=600&exact=false&radius=97"
    ]
    pagination_form = parse_qs(requests[2].content.decode("ascii"), strict_parsing=True)
    assert pagination_form["__req"] == ["2"]
    pagination_variables = json.loads(pagination_form["variables"][0])
    assert pagination_variables["cursor"] == "cursor-1"
    assert pagination_variables["count"] == 24
    assert work["state"] == WorkState.COMPLETED
    assert work["result"]["pages_processed"] == 2
    assert work["result"]["listing_observations"] == 4
    assert work["result"]["unique_listings"] == 3
    assert work["result"]["stopping_reason"] == "maximum_results"
    assert len(search_runs.search_runs) == 1
    fresh_summary = search_runs.search_runs[0]
    assert fresh_summary.origin is SearchRunOrigin.FRESH
    assert fresh_summary.refresh_source_run_record_identifier is None
    assert fresh_summary.ended_at_utc is not None
    assert fresh_summary.started_at_utc <= fresh_summary.ended_at_utc

    with closing(sqlite3.connect(path)) as connection:
        rows = connection.execute(
            """
            SELECT kind_parts_json, count(*)
            FROM objects
            GROUP BY kind_parts_json
            """
        ).fetchall()
        extraction_rows = connection.execute(
            """
            SELECT records.value_json
            FROM records
            JOIN objects ON objects.id = records.object_id
            WHERE objects.kind_parts_json = '["carl","facebook","search_page_extraction"]'
            ORDER BY json_extract(records.value_json, '$.page_ordinal')
            """
        ).fetchall()
        session_extraction_json = connection.execute(
            """
            SELECT records.value_json
            FROM records
            JOIN objects ON objects.id = records.object_id
            WHERE objects.kind_parts_json =
                '["carl","facebook","search_session_bootstrap_extraction"]'
            """
        ).fetchone()[0]
        activity_rows = connection.execute(
            """
            SELECT id, kind_parts_json, ordinal, state
            FROM network_activities
            ORDER BY ordinal
            """
        ).fetchall()
        acquisition_activity_identifiers = {
            row[0]
            for row in connection.execute(
                """
                SELECT json_extract(records.value_json, '$.network_activity_identifier')
                FROM records
                JOIN objects ON objects.id = records.object_id
                WHERE objects.kind_parts_json = '["carl","http","acquisition"]'
                """
            ).fetchall()
        }
    counts = {kind: amount for kind, amount in rows}
    assert counts['["carl","http","acquisition"]'] == 3
    assert counts['["carl","facebook","search_listing_occurrence"]'] == 4
    assert counts['["carl","facebook","search_page_extraction"]'] == 2
    assert counts['["carl","facebook","route_definition_json_frame"]'] == 3
    assert counts['["carl","facebook","search_session_bootstrap_extraction"]'] == 1
    assert counts['["carl","facebook","search_run"]'] == 1
    session_extraction = json.loads(session_extraction_json)
    lsd_evidence = session_extraction["protected_session_material_evidence"]["lsd"]
    assert lsd_evidence["state"] == "redacted"
    assert lsd_evidence["character_count"] == len("synthetic-lsd")
    assert lsd_evidence["sources"][0]["json_path"] == ["require", 0, 2, "token"]
    assert "synthetic-lsd" not in session_extraction_json
    assert [(json.loads(row[1])[-1], row[2], row[3]) for row in activity_rows] == [
        ("search_session_bootstrap", 1, "completed"),
        ("search_route_definition", 2, "completed"),
        ("search_pagination", 3, "completed"),
    ]
    assert acquisition_activity_identifiers == {row[0] for row in activity_rows}
    extractions = [json.loads(row[0]) for row in extraction_rows]
    assert [value["extractor"]["component_parts"] for value in extractions] == [
        ["carl", "facebook", "extract", "search_route_definition"],
        ["carl", "facebook", "extract", "search_pagination"],
    ]


@pytest.mark.anyio
async def test_search_worker_collects_overlapping_price_partitions_without_cursor_requests(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    identifiers = count()
    requests: list[httpx.Request] = []
    factory = _FakeSessionFactory(requests)

    def new_identifier() -> str:
        return f"identifier-{next(identifiers)}"

    payload = CollectSearchPayload(
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
        routing=("proton", "personal", "carl"),
    )
    settings = WorkerSettings(
        worker_count=1,
        lease_duration_ns=1_000_000_000,
        renewal_interval_ns=100_000_000,
        idle_poll_interval_ns=10_000_000,
    )
    async with Database.managed(path, initialize=True) as database:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(
                database=database,
                acquirer=DirectHttpxAcquirer(),
                new_identifier=new_identifier,
            ),
            FacebookSearchWorkerDependencies(
                database=database,
                session_factory=factory,
                navigation_headers=(Header(name=b"User-Agent", value=b"test-browser"),),
                new_identifier=new_identifier,
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                network_activity_scheduler=NetworkActivityScheduler(
                    database=database,
                    new_identifier=new_identifier,
                    utc_now_ns=time_ns,
                    sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
                    permit_duration_ns=settings.lease_duration_ns,
                ),
            ),
        )
        refresh_payload = RefreshSearchPayload(
            base_search_run_record_identifier="source-search-run",
            search_work_identifier="partitioned-search-work",
            search=payload,
            item_routing=("decodo", "personal", "carl"),
            image_routing=("proton", "personal", "carl"),
        )
        await database.enqueue_work(
            refresh_search_work(
                identifier="refresh-work",
                payload=refresh_payload,
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier="refresh-request",
                kind=("carl", "mcp", "request_search_refresh"),
                identifier="source-search-run",
                context={},
            ),
            event_identifier=new_identifier(),
            enqueued_at_utc_ns=time_ns(),
        )
        await database.enqueue_work(
            collect_search_work(
                identifier="partitioned-search-work",
                payload=payload,
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier="partitioned-search-request",
                kind=("carl", "facebook", "search_refresh", "search"),
                identifier="refresh-work",
                context={},
            ),
            event_identifier=new_identifier(),
            enqueued_at_utc_ns=time_ns(),
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=settings.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=new_identifier(),
        )
        assert claim.lease is not None
        outcome = await execute_lease(
            database=database,
            registry=registry,
            settings=settings,
            services=WorkerRuntimeServices(
                new_identifier=new_identifier,
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                code_provenance=lambda: _async_provenance(),
                invocation=lambda: {"kind": "test"},
            ),
            lease=claim.lease,
        )
        work = await database.work("partitioned-search-work")
        search_runs = await ReviewApplication(
            database=database, repository_root=tmp_path
        ).list_search_runs()

    assert isinstance(outcome, CompletedWork)
    assert factory.opens == 1
    assert [request.url.path for request in requests] == [
        "/marketplace/",
        "/ajax/route-definition/",
        "/ajax/route-definition/",
        "/ajax/route-definition/",
    ]
    route_forms = [
        parse_qs(request.content.decode("ascii"), strict_parsing=True) for request in requests[1:]
    ]
    assert [form["__req"] for form in route_forms] == [["1"], ["2"], ["3"]]
    assert [form["route_url"] for form in route_forms] == [
        ["/marketplace/456/search?query=telescope&minPrice=0&maxPrice=100&exact=false&radius=97"],
        ["/marketplace/456/search?query=telescope&minPrice=90&maxPrice=190&exact=false&radius=97"],
        ["/marketplace/456/search?query=telescope&minPrice=180&maxPrice=250&exact=false&radius=97"],
    ]
    assert work["state"] == WorkState.COMPLETED
    assert work["result"]["pages_processed"] == 3
    assert work["result"]["listing_observations"] == 6
    assert work["result"]["unique_listings"] == 2
    assert work["result"]["stopping_reason"] == "price_partitions_exhausted"
    assert work["result"]["saturated_price_partitions"] == [1, 2, 3]
    assert len(search_runs.search_runs) == 1
    refresh_summary = search_runs.search_runs[0]
    assert refresh_summary.origin is SearchRunOrigin.REFRESH
    assert refresh_summary.refresh_source_run_record_identifier == "source-search-run"


async def _async_provenance() -> CodeProvenance:
    return _provenance()
