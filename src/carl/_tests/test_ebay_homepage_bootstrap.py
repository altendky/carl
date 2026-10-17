"""Offline homepage cookie bootstrap and its separately retained evidence."""

# pyright: reportPrivateUsage=false, reportUnusedCallResult=false

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast, final
from uuid import uuid4

import anyio
import httpx
import pytest

from carl._tests.test_ebay import _Acquirer
from carl._tests.test_ebay_workers import _directories, _provenance
from carl._tests.test_wreq import _decodo_settings
from carl.core.ebay import (
    EBAY_NAVIGATION_HEADER_ORDER,
    CollectEbaySearchPayload,
    EbaySearchRequest,
    ebay_search_url,
)
from carl.core.http import RequestPlan
from carl.core.models import CodeProvenance, JsonValue
from carl.core.work import NetworkActivityState
from carl.core.worker import AttemptContext, RetryWork
from carl.ebay import collect_configured_ebay_search
from carl.ebay_workers import EbaySearchWorkerDependencies, build_ebay_worker_registry
from carl.io import network_activity
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
    ) -> None:
        self.requests: list[tuple[str, str | None]] = []
        self.homepage_failure = homepage_failure
        self.closed = False

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
            assert request.headers.get("cookie") == f"bm_s=bootstrap-{ordinal}"
            if search_timeout:
                raise TimeoutError("Search request deadline")
            body = _SECOND if request.url.params.get("_pgn") == "2" else _FIRST
            return httpx.Response(search_status, content=body)

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
    ) -> None:
        self.sessions: list[_CookieSession] = []
        self.homepage_status = homepage_status
        self.search_status = search_status
        self.homepage_failure = homepage_failure
        self.search_timeout = search_timeout

    @asynccontextmanager
    async def session(self, _new_identifier: Callable[[], str]) -> AsyncGenerator[_CookieSession]:
        session = _CookieSession(
            ordinal=len(self.sessions),
            homepage_status=self.homepage_status,
            search_status=self.search_status,
            homepage_failure=self.homepage_failure,
            search_timeout=self.search_timeout,
        )
        self.sessions.append(session)
        try:
            async with session.client:
                yield session
        finally:
            session.closed = True


def _configure(monkeypatch: pytest.MonkeyPatch, sessions: _CookieSessions) -> None:
    def load(_path: Path) -> None:
        return None

    def settings(*_arguments: object) -> tuple[DecodoProxySettings, None, WreqTransportSettings]:
        return _decodo_settings(), None, WreqTransportSettings()

    def managed(**_arguments: object) -> _CookieSessions:
        return sessions

    async def provenance(_path: Path) -> CodeProvenance:
        return await _provenance()

    monkeypatch.setattr("carl.ebay.load_configuration", load)
    monkeypatch.setattr("carl.ebay.decodo_wreq_stack_settings", settings)
    monkeypatch.setattr("carl.ebay.ManagedDecodoWreqAcquirer", managed)
    monkeypatch.setattr("carl.ebay.collect_code_provenance_async", provenance)


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
        assert "cooldown" in reason
        acquisition = cast(dict[str, JsonValue], outcome.result["acquisition"])
        if failure == "search_timeout":
            assert acquisition["failure_phase"] == "request_timeout"
            homepage = cast(dict[str, JsonValue], acquisition["homepage_acquisition"])
            assert homepage["http_status"] == 403
            homepage_id = str(homepage["acquisition_record_identifier"])
            assert len(sessions.sessions[0].requests) == 2
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
        [(_HOME, None), (ebay_search_url("scope"), "bm_s=bootstrap-0")],
        [(_HOME, None), (ebay_search_url("scope"), "bm_s=bootstrap-1")],
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
    assert len(sessions.sessions[0].requests) == 2


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
