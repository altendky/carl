"""Offline homepage cookie bootstrap and its separately retained evidence."""

# pyright: reportPrivateUsage=false, reportUnusedCallResult=false

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast, final
from unittest.mock import AsyncMock
from uuid import uuid4

import anyio
import httpx
import pytest

from carl._tests.test_ebay import _Acquirer
from carl._tests.test_ebay_cooldown import _enqueue, _fail
from carl._tests.test_ebay_workers import _directories, _provenance
from carl._tests.test_wreq import _decodo_settings
from carl.core.configuration import CarlConfiguration, EbaySearchConfiguration
from carl.core.ebay import (
    EBAY_NAVIGATION_HEADER_ORDER,
    CollectEbaySearchPayload,
    EbaySearchRequest,
    ebay_search_url,
    ebay_search_work_constraint,
)
from carl.core.http import RequestPlan
from carl.core.json import decode_json
from carl.core.models import CodeProvenance, JsonValue
from carl.core.routing import DecodoProduct
from carl.core.work import NetworkActivityState
from carl.core.worker import AttemptContext, RetryWork
from carl.ebay import collect_configured_ebay_search
from carl.ebay_workers import EbaySearchWorkerDependencies, build_ebay_worker_registry
from carl.io import network_activity
from carl.io.configuration import LoadedCarlConfiguration
from carl.io.decodo import DecodoProxySettings
from carl.io.httpx import Acquisition, AcquisitionFailure, IdentifierFactory
from carl.io.sqlite import Database
from carl.io.wreq import WreqTransportSettings

_HOME = "https://www.ebay.com/"
_FIRST = b'<li class="s-card"><a href="/itm/256123456789">Scope</a></li><a class="pagination__next" href="/sch/i.html?_pgn=2">Next</a>'
_SECOND = b'<li class="s-card"><a href="/itm/256987654321">Another scope</a></li>'


@final
class _CookieSession:
    """Use a real cookie jar against an in-process, network-free eBay stub."""

    def __init__(
        self,
        *,
        ordinal: int,
        homepage_status: int,
        search_status: int,
        homepage_failure: str | None,
        search_timeout: bool,
        search_statuses: tuple[int, ...] | None,
    ) -> None:
        self.requests: list[tuple[str, str | None]] = []
        self.homepage_failure = homepage_failure
        self.closed = False
        statuses = iter(search_statuses) if search_statuses is not None else None

        def respond(request: httpx.Request) -> httpx.Response:
            self.requests.append((str(request.url), request.headers.get("cookie")))
            if str(request.url) == _HOME:
                if homepage_failure == "deadline":
                    raise TimeoutError("Homepage request deadline")
                if homepage_failure == "transport":
                    raise AcquisitionFailure(
                        "Homepage transport failed",
                        result={"stopping_condition": "transport_failure", "hops": []},
                    )
                return httpx.Response(
                    homepage_status,
                    content=b"Homepage evidence",
                    headers={"set-cookie": f"bm_s=bootstrap-{ordinal}; Path=/; Secure"},
                )
            assert f"bm_s=bootstrap-{ordinal}" in request.headers.get("cookie", "")
            if search_timeout:
                raise TimeoutError("Search request deadline")
            body = _SECOND if request.url.params.get("_pgn") == "2" else _FIRST
            status = next(statuses) if statuses is not None else search_status
            if status == 0:
                raise TimeoutError("Search request deadline")
            return httpx.Response(
                status,
                content=body,
                headers={"set-cookie": "bm_sv=retry; Path=/; Secure"} if status == 403 else {},
            )

        self.client = httpx.AsyncClient(transport=httpx.MockTransport(respond))

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        response = await self.client.get(plan.url)
        return await _Acquirer(response.content, status_code=response.status_code).acquire(
            plan, new_identifier
        )


@final
class _CookieSessions:
    def __init__(
        self,
        *,
        homepage_status: int = 200,
        search_status: int = 200,
        homepage_failure: str | None = None,
        search_timeout: bool = False,
        search_statuses: tuple[int, ...] | None = None,
    ) -> None:
        self.sessions: list[_CookieSession] = []
        self.homepage_status = homepage_status
        self.search_status = search_status
        self.homepage_failure = homepage_failure
        self.search_timeout = search_timeout
        self.search_statuses = search_statuses

    @asynccontextmanager
    async def session(self, _new_identifier: Callable[[], str]) -> AsyncGenerator[_CookieSession]:
        session = _CookieSession(
            ordinal=len(self.sessions),
            homepage_status=self.homepage_status,
            search_status=self.search_status,
            homepage_failure=self.homepage_failure,
            search_timeout=self.search_timeout,
            search_statuses=self.search_statuses,
        )
        self.sessions.append(session)
        try:
            async with session.client:
                yield session
        finally:
            session.closed = True


def _configure(
    monkeypatch: pytest.MonkeyPatch,
    sessions: _CookieSessions,
    policy: EbaySearchConfiguration | None = None,
    route_settings: DecodoProxySettings | None = None,
) -> None:
    def load(path: Path) -> LoadedCarlConfiguration:
        return LoadedCarlConfiguration(
            configuration=CarlConfiguration.model_construct(
                ebay_search=policy or EbaySearchConfiguration()
            ),
            document_sha256="a" * 64,
            source_path=path,
        )

    def settings(*_arguments: object) -> tuple[DecodoProxySettings, None, WreqTransportSettings]:
        return route_settings or _decodo_settings(), None, WreqTransportSettings()

    def managed(**_arguments: object) -> _CookieSessions:
        return sessions

    async def provenance(_path: Path) -> CodeProvenance:
        return await _provenance()

    monkeypatch.setattr("carl.ebay.load_configuration", load)
    monkeypatch.setattr("carl.ebay.decodo_wreq_stack_settings", settings)
    monkeypatch.setattr("carl.ebay.ManagedDecodoWreqAcquirer", managed)
    monkeypatch.setattr("carl.ebay.collect_code_provenance_async", provenance)


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["403", "search_timeout", "homepage_deadline"])
@pytest.mark.parametrize(
    ("product", "ports", "port_end", "independent"),
    [
        (DecodoProduct.RESIDENTIAL_PROXY, (), None, True),
        (DecodoProduct.MOBILE_PROXY, (), None, True),
        (DecodoProduct.DATACENTER_PROXY, (), None, False),
        (DecodoProduct.DATACENTER_PROXY, (), 19000, True),
        (DecodoProduct.DATACENTER_PROXY, (11000, 11001), None, True),
        (DecodoProduct.DATACENTER_PROXY, (11000,), None, False),
        (DecodoProduct.DATACENTER_PROXY, (), 11000, False),
    ],
    ids=["residential", "mobile", "fixed-dc", "range-dc", "pool-dc", "one-port-dc", "one-range-dc"],
)
async def test_verified_independent_sessions_keep_retry_without_cooling_siblings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    product: DecodoProduct,
    ports: tuple[int, ...],
    port_end: int | None,
    independent: bool,
) -> None:
    settings = _decodo_settings()
    route = settings.route.model_copy(
        update={
            "product": product,
            "endpoint": settings.route.endpoint.model_copy(update={"port": 11000}),
        }
    )
    settings = settings.model_copy(
        update={"route": route, "endpoint_ports": ports, "endpoint_port_end": port_end}
    )
    sessions = _CookieSessions(
        search_status=403 if failure == "403" else 200,
        search_timeout=failure == "search_timeout",
        homepage_failure="deadline" if failure == "homepage_deadline" else None,
    )
    _configure(monkeypatch, sessions, route_settings=settings)
    path, now = tmp_path / "test.sqlite3", 1_000_000_000_000
    deadline = now + 600_000_000_000
    async with Database.managed(path, initialize=True) as database:
        await database.register_constraint(ebay_search_work_constraint(), registered_at_utc_ns=now)
        await _enqueue(database, "blocked", now)
        await _enqueue(database, "sibling", now)
        await _fail(database, tmp_path, "blocked", now, collector=collect_configured_ebay_search)
        work = await database.work("blocked")
        assert work["state"] == "pending"
        assert work["eligible_at_utc_ns"] == deadline
        assert work["attempt"] == 1
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT data_json FROM work_events WHERE work_item_id=? AND event_kind='released'",
                ("blocked",),
            )
            row = await cursor.fetchone()
        assert row is not None
        released = decode_json(str(row[0]))
        assert isinstance(released, dict)
        reason = cast(dict[str, JsonValue], cast(dict[str, JsonValue], released)["reason"])
        assert reason["maximum_attempts"] == 5
        assert ("cooldown" not in reason) == independent
        await _enqueue(database, "future", now + 1)
        for identifier in ("sibling", "future"):
            eligible = (await database.work(identifier))["eligible_at_utc_ns"]
            assert eligible <= now + 1 if independent else eligible == deadline
    assert sessions.sessions[0].closed
    async with Database.managed(path) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(database, _directories(tmp_path), lambda: str(uuid4()))
        )
        waiting = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="test",
            lease_token=str(uuid4()),
            lease_duration_ns=60_000_000_000,
            utc_now_ns=lambda: now + 1,
            event_identifier=str(uuid4()),
            eligible_identifiers=("blocked",),
        )
        assert waiting.lease is None and waiting.next_eligible_at_utc_ns == deadline
        sibling = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="test",
            lease_token=str(uuid4()),
            lease_duration_ns=60_000_000_000,
            utc_now_ns=lambda: now + 1,
            event_identifier=str(uuid4()),
            eligible_identifiers=("sibling",),
        )
        assert (sibling.lease is not None) == independent
        if sibling.lease is None:
            assert sibling.next_eligible_at_utc_ns == deadline


@pytest.mark.anyio
@pytest.mark.parametrize("homepage_status", [200, 403])
async def test_homepage_cookies_reach_all_search_pages_and_evidence_is_separate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, homepage_status: int
) -> None:
    sessions = _CookieSessions(homepage_status=homepage_status)
    _configure(monkeypatch, sessions)
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        result = await collect_configured_ebay_search(
            database,
            directories=_directories(tmp_path),
            request=EbaySearchRequest(query="scope", maximum_pages=2),
            new_identifier=lambda: str(uuid4()),
        )
        assert result["state"] == "completed"
        assert result["page_count"] == 2
        assert result["listing_count"] == 2
        homepage = cast(dict[str, JsonValue], result["homepage_acquisition"])
        assert homepage["http_status"] == homepage_status
        homepage_id = str(homepage["acquisition_record_identifier"])
        _, _, homepage_record = await database.get_record(homepage_id)
        assert isinstance(homepage_record, dict)
        assert homepage_record["effective_url"] == _HOME
        plan = cast(dict[str, JsonValue], homepage_record["request_plan"])
        assert plan["header_order"] == list(EBAY_NAVIGATION_HEADER_ORDER)
        assert homepage_id not in cast(list[JsonValue], result["acquisition_record_identifiers"])
        operation = await database.operation(str(homepage["acquisition_operation_identifier"]))
        assert operation["state"] == "completed"
        hops = cast(list[dict[str, JsonValue]], homepage_record["hops"])
        response = cast(dict[str, JsonValue], hops[-1]["response"])
        body = cast(dict[str, JsonValue], response["body"])
        _, content = await database.get_artifact(str(body["artifact_id"]))
        assert content == b"Homepage evidence"
        _, _, manifest = await database.get_record(str(result["search_run_record_identifier"]))
        assert isinstance(manifest, dict)
        assert manifest["homepage_acquisition"] == homepage
        assert len(cast(list[JsonValue], manifest["pages"])) == 2
        _, inputs, _ = await database.object_operation_relations(
            str(result["search_run_record_identifier"])
        )
        assert homepage_id in {item.object_identifier for item in inputs}
    assert sessions.sessions[0].requests == [
        (_HOME, None),
        *([(_HOME, "bm_s=bootstrap-0")] if homepage_status == 403 else []),
        (ebay_search_url("scope"), "bm_s=bootstrap-0"),
        (ebay_search_url("scope", page_number=2), "bm_s=bootstrap-0"),
    ]
    assert sessions.sessions[0].closed


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["search_timeout", "homepage_finalization_timeout"])
async def test_bootstrap_and_search_timeout_use_worker_retry_policy_with_retained_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    sessions = _CookieSessions(homepage_status=403, search_timeout=failure == "search_timeout")
    _configure(monkeypatch, sessions)
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:

        async def checkpoint(**_arguments: object) -> None:
            # This handler-level assertion does not create a synthetic lease.
            return None

        monkeypatch.setattr(database, "publish_leased_operation_checkpoint", checkpoint)
        if failure == "homepage_finalization_timeout":
            monkeypatch.setattr(
                network_activity, "NETWORK_ACTIVITY_FINALIZATION_TIMEOUT_SECONDS", 0.001
            )
            original_finish = database.finish_network_activity
            first = True

            async def finish(
                *,
                network_activity_identifier: str,
                admission_token: str | None,
                state: NetworkActivityState,
                ended_at_utc_ns: int,
                result: JsonValue,
                event_identifier: str,
            ) -> None:
                nonlocal first
                if first:
                    first = False
                    await anyio.sleep_forever()
                await original_finish(
                    network_activity_identifier=network_activity_identifier,
                    admission_token=admission_token,
                    state=state,
                    ended_at_utc_ns=ended_at_utc_ns,
                    result=result,
                    event_identifier=event_identifier,
                )

            monkeypatch.setattr(database, "finish_network_activity", finish)
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(database, _directories(tmp_path), lambda: str(uuid4()))
        )
        outcome = await registry.handlers[0].execute(
            CollectEbaySearchPayload(request=EbaySearchRequest(query="scope")).as_json(),
            AttemptContext(
                work_item_identifier="search",
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="operation",
            ),
        )
        assert isinstance(outcome, RetryWork)
        assert outcome.delay_ns == 600_000_000_000
        assert isinstance(outcome.reason, dict)
        reason = cast(dict[str, JsonValue], outcome.reason)
        assert reason["maximum_attempts"] == 5
        assert reason["retry_scope"] == "independent_session"
        assert "cooldown" not in reason
        acquisition = cast(dict[str, JsonValue], outcome.result["acquisition"])
        if failure == "search_timeout":
            assert acquisition["failure_phase"] == "request_timeout"
            homepage = cast(dict[str, JsonValue], acquisition["homepage_acquisition"])
            assert homepage["http_status"] == 403
            homepage_id = str(homepage["acquisition_record_identifier"])
            assert len(sessions.sessions[0].requests) == 3
        else:
            assert acquisition["failure_phase"] == "network_activity_finalization_timeout"
            homepage_id = str(acquisition["acquisition_record_identifier"])
            assert sessions.sessions[0].requests == [(_HOME, None)]
        assert any(
            item.name == ("search", "homepage", "acquisition")
            and item.object_identifier == homepage_id
            for item in outcome.inputs
        )
        _, _, retained = await database.get_record(homepage_id)
        assert isinstance(retained, dict)
        hops = cast(list[dict[str, JsonValue]], retained["hops"])
        response = cast(dict[str, JsonValue], hops[-1]["response"])
        body = cast(dict[str, JsonValue], response["body"])
        _, content = await database.get_artifact(str(body["artifact_id"]))
        assert content == b"Homepage evidence"
        assert sessions.sessions[0].closed


@pytest.mark.anyio
async def test_each_new_attempt_bootstraps_its_own_cookie_jar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions = _CookieSessions(homepage_status=403)
    _configure(monkeypatch, sessions)
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        for _ in range(2):
            result = await collect_configured_ebay_search(
                database,
                directories=_directories(tmp_path),
                request=EbaySearchRequest(query="scope", maximum_pages=1),
            )
            assert result["page_count"] == 1
    assert [session.requests for session in sessions.sessions] == [
        [
            (_HOME, None),
            (_HOME, "bm_s=bootstrap-0"),
            (ebay_search_url("scope"), "bm_s=bootstrap-0"),
        ],
        [
            (_HOME, None),
            (_HOME, "bm_s=bootstrap-1"),
            (ebay_search_url("scope"), "bm_s=bootstrap-1"),
        ],
    ]
    assert all(session.closed for session in sessions.sessions)


@pytest.mark.anyio
async def test_bootstrap_does_not_mask_a_search_403(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions = _CookieSessions(homepage_status=403, search_status=403)
    _configure(monkeypatch, sessions)
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        result = await collect_configured_ebay_search(
            database,
            directories=_directories(tmp_path),
            request=EbaySearchRequest(query="scope"),
        )
    assert result["state"] == "response_failed"
    assert result["stopping_reason"] == "http_error"
    assert result["page_count"] == 1
    assert result["listing_count"] == 0
    assert len(sessions.sessions[0].requests) == 9


@pytest.mark.anyio
@pytest.mark.parametrize("homepage_failure", ["transport", "deadline"])
async def test_homepage_transport_failure_is_retained_and_never_starts_search(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, homepage_failure: str
) -> None:
    sessions = _CookieSessions(homepage_failure=homepage_failure)
    _configure(monkeypatch, sessions)
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        with pytest.raises(AcquisitionFailure) as raised:
            await collect_configured_ebay_search(
                database,
                directories=_directories(tmp_path),
                request=EbaySearchRequest(query="scope"),
            )
        assert raised.value.result["stopping_condition"] == "transport_failure"
        if homepage_failure == "deadline":
            assert raised.value.result["exception_type"] == "TimeoutError"
            assert raised.value.result["failed_request"] == {"method": "GET", "url": _HOME}
        assert await database.records_by_kind(("carl", "ebay", "search_run")) == ()
        assert await database.records_by_kind(("carl", "http", "acquisition")) == ()
    assert sessions.sessions[0].requests == [(_HOME, None)]
    assert sessions.sessions[0].closed


@pytest.mark.anyio
async def test_seventh_attempt_recovers_with_same_cookie_jar_and_page_two_retries_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions = _CookieSessions(search_statuses=(*([403] * 6), 200, 429, 200))
    _configure(monkeypatch, sessions)
    sleep = AsyncMock()
    monkeypatch.setattr("carl.ebay.anyio.sleep", sleep)
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        result = await collect_configured_ebay_search(
            database,
            directories=_directories(tmp_path),
            request=EbaySearchRequest(query="scope", maximum_pages=2),
        )
        assert result["state"] == "completed"
        assert result["page_count"] == result["listing_count"] == 2
        _, _, manifest = await database.get_record(str(result["search_run_record_identifier"]))
        assert isinstance(manifest, dict)
        pages = cast(list[dict[str, JsonValue]], manifest["pages"])
        attempts = [cast(list[dict[str, JsonValue]], page["attempts"]) for page in pages]
        assert [[attempt["attempt"] for attempt in page] for page in attempts] == [
            list(range(1, 8)),
            [1, 2],
        ]
        _, inputs, _ = await database.object_operation_relations(
            str(result["search_run_record_identifier"])
        )
        linked = {item.object_identifier for item in inputs}
        for page_attempts in attempts:
            plans: list[JsonValue] = []
            for attempt in page_attempts:
                acquisition_id = str(attempt["acquisition_record_identifier"])
                assert acquisition_id in linked
                assert str(attempt["extraction_record_identifier"]) in linked
                _, _, record = await database.get_record(acquisition_id)
                assert isinstance(record, dict)
                assert record["request_attempt"] == attempt["attempt"]
                plans.append(record["request_plan"])
                activity = await database.network_activity(
                    str(record["network_activity_identifier"])
                )
                assert activity["attempt"] == attempt["attempt"]
            assert all(plan == plans[0] for plan in plans)
        occurrences = await database.records_by_kind(("carl", "ebay", "search_listing_occurrence"))
        assert len(occurrences) == 2
    assert len(sessions.sessions) == 1 and sessions.sessions[0].closed
    requests = sessions.sessions[0].requests
    assert [url for url, _ in requests] == [
        _HOME,
        *([ebay_search_url("scope")] * 7),
        *([ebay_search_url("scope", page_number=2)] * 2),
    ]
    assert all("bm_sv=retry" in (cookie or "") for _, cookie in requests[2:])
    sleep.assert_not_awaited()


@pytest.mark.anyio
async def test_configured_holdoff_applies_to_homepage_retry_search_retry_and_pagination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions = _CookieSessions(homepage_status=403, search_statuses=(403, 200, 200))
    _configure(
        monkeypatch,
        sessions,
        EbaySearchConfiguration(minimum_holdoff_seconds=4.0, maximum_holdoff_seconds=8.0),
    )
    sampled: list[tuple[float, float]] = []

    def sample(minimum: float, maximum: float) -> float:
        sampled.append((minimum, maximum))
        return 5.5

    sleep = AsyncMock()
    monkeypatch.setattr("carl.ebay.uniform", sample)
    monkeypatch.setattr("carl.ebay.anyio.sleep", sleep)
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        result = await collect_configured_ebay_search(
            database,
            directories=_directories(tmp_path),
            request=EbaySearchRequest(query="scope", maximum_pages=2),
        )
        assert result["state"] == "completed"
    assert len(sessions.sessions[0].requests) == 5
    assert sampled == [(4.0, 8.0)] * 4
    assert [call.args for call in sleep.await_args_list] == [(5.5,)] * 4


@pytest.mark.anyio
@pytest.mark.parametrize("status", [200, 500])
async def test_unrecognized_and_server_errors_do_not_consume_local_block_retry_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    sessions = _CookieSessions(search_status=status)
    _configure(monkeypatch, sessions)
    # A generic failure body contains no search cards or challenge markers.
    monkeypatch.setattr("carl._tests.test_ebay_homepage_bootstrap._FIRST", b"Unexpected response")
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        result = await collect_configured_ebay_search(
            database, directories=_directories(tmp_path), request=EbaySearchRequest(query="scope")
        )
        assert result["state"] == "response_failed"
    assert len(sessions.sessions[0].requests) == 2


@pytest.mark.anyio
async def test_page_two_timeout_after_403_retains_attempts_and_uses_worker_retry_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions = _CookieSessions(search_statuses=(200, 403, 0))
    _configure(monkeypatch, sessions)
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:

        async def checkpoint(**_arguments: object) -> None:
            return None

        monkeypatch.setattr(database, "publish_leased_operation_checkpoint", checkpoint)
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(database, _directories(tmp_path), lambda: str(uuid4()))
        )
        outcome = await registry.handlers[0].execute(
            CollectEbaySearchPayload(
                request=EbaySearchRequest(query="scope", maximum_pages=2)
            ).as_json(),
            AttemptContext(
                work_item_identifier="search",
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="operation",
            ),
        )
        assert isinstance(outcome, RetryWork)
        assert outcome.delay_ns == 600_000_000_000
        assert isinstance(outcome.reason, dict)
        reason = cast(dict[str, JsonValue], outcome.reason)
        assert reason["retry_scope"] == "independent_session"
        assert "cooldown" not in reason
        acquisition = cast(dict[str, JsonValue], outcome.result["acquisition"])
        assert acquisition["failure_phase"] == "request_timeout"
        attempts = cast(list[dict[str, JsonValue]], acquisition["search_page_attempts"])
        assert len(attempts) == 2
        linked = {item.object_identifier for item in outcome.inputs}
        for attempt in attempts:
            assert attempt["acquisition_record_identifier"] in linked
            assert attempt["extraction_record_identifier"] in linked
        homepage = cast(dict[str, JsonValue], acquisition["homepage_acquisition"])
        assert homepage["acquisition_record_identifier"] in linked
        assert len(await database.records_by_kind(("carl", "http", "acquisition"))) == 3
        assert await database.records_by_kind(("carl", "ebay", "search_run")) == ()
    assert len(sessions.sessions[0].requests) == 4
    assert sessions.sessions[0].closed
