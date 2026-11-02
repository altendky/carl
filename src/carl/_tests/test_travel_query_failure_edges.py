"""Paid travel responses survive malformed numbers and metadata cleanup failures."""

# Reuse the actual offline stage fixture and immutable checkpoint readers.
# pyright: reportPrivateUsage=false

import gzip
import json
import sqlite3
from pathlib import Path

import httpx
import pytest

import carl.io.travel_queries as query_module
import carl.travel_stages as stage_module
from carl._tests.test_http_capture import _Stream
from carl._tests.test_travel_stages import _case, _charges, _finish, _record, _run
from carl.core.stages import WaitWork
from carl.core.worker import CompletedWork
from carl.io.http_checkpoints import CHECKPOINT_KIND, restore_acquisition
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.mark.parametrize("geocode", (False, True))
async def test_huge_provider_integer_completes_unavailable_and_keeps_raw_capture(
    tmp_path: Path, geocode: bool
) -> None:
    response = (
        {
            "features": [
                {"geometry": {"coordinates": [21, 11]}, "properties": {"confidence": 10**1000}}
            ]
        }
        if geocode
        else {"distances": [[10**1000]], "durations": [[1]]}
    )
    raw = gzip.compress(json.dumps(response, separators=(",", ":")).encode())
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"Content-Encoding": "gzip", "Content-Type": "application/json"},
            stream=_Stream(raw),
        )

    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1, "http": 1}
    ) as database:
        case = await _case(database, tmp_path, geocode=geocode)
        case.service.queries.transport = httpx.MockTransport(respond)
        prepared = await _run(database, case, case.initial)
        assert isinstance(prepared, WaitWork)
        captured = await _run(database, case, prepared.continuation)
        assert isinstance(captured, WaitWork)
        checkpoint = next(record for record in captured.records if record.kind == CHECKPOINT_KIND)
        acquisition = await restore_acquisition(database, checkpoint.identifier)
        assert acquisition.bodies[0].content == raw
        assert acquisition.bodies[0].representation["content_decoded"] is False
        outcome = await _finish(database, case, captured.continuation)
        assert isinstance(outcome, CompletedWork)
        estimate = (await _record(database, outcome)).estimate
        assert estimate.status == "unavailable"
        assert estimate.distance_meters is None and estimate.duration_seconds is None
        assert estimate.unavailable_reason == (
            "location_not_found" if geocode else "provider_invalid_response"
        )
        assert len(requests) == 1
        assert await _charges(database, case.initial.operation_identifier) == 1
        assert (await database.work(case.initial.work_item_identifier))["attempt"] == 1
        _, retained = await database.get_artifact(acquisition.bodies[0].identifier)
        assert retained == raw


async def test_provider_cookie_is_redacted_and_http_uses_cpu_prepared_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[httpx.Request] = []
    raw = b'{"distances":[[12000]],"durations":[[900]]}'

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={
                "Content-Type": "application/json",
                "Set-Cookie": "session=provider-private-cookie",
            },
            stream=_Stream(raw),
        )

    def unexpected_rebuild(*_args: object):
        raise AssertionError("HTTP capture must use its persisted CPU-prepared request")

    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1, "http": 1}
    ) as database:
        case = await _case(database, tmp_path)
        case.service.queries.transport = httpx.MockTransport(respond)
        prepared = await _run(database, case, case.initial)
        assert isinstance(prepared, WaitWork)
        prepared_content = prepared.continuation.payload["request_content"]
        assert isinstance(prepared_content, str)
        monkeypatch.setattr(stage_module, "hosted_request", unexpected_rebuild)
        monkeypatch.setattr(query_module, "hosted_request", unexpected_rebuild)
        captured = await _run(database, case, prepared.continuation)
        assert isinstance(captured, WaitWork)
        checkpoint = next(record for record in captured.records if record.kind == CHECKPOINT_KIND)
        acquisition = await restore_acquisition(database, checkpoint.identifier)
        assert len(requests) == 1 and requests[0].content == prepared_content.encode()
        assert requests[0].headers["Authorization"] == "test-secret-not-a-real-key"
        evidence = json.dumps(acquisition.record)
        assert "provider-private-cookie" not in evidence
        assert "test-secret-not-a-real-key" not in evidence
        cookie = next(
            header
            for header in acquisition.record["hops"][0]["response"]["headers"]
            if header["name_latin1"].lower() == "set-cookie"
        )
        assert cookie["value"]["state"] == "redacted"
        assert acquisition.bodies[0].content == raw
        outcome = await _finish(database, case, captured.continuation)
        assert isinstance(outcome, CompletedWork)
        assert (await _record(database, outcome)).estimate.status == "ready"
        assert len(requests) == 1


@pytest.mark.parametrize("failure_type", (sqlite3.OperationalError, OSError))
async def test_completed_paid_capture_survives_cooldown_cache_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_type: type[Exception]
) -> None:
    raw = b'{"error":"offline provider quota response"}'
    calls = 0

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            429, headers={"Content-Type": "application/json"}, stream=_Stream(raw)
        )

    async def failed_cache_write(_deadline: float) -> float:
        raise failure_type("private cache failure details must not enter evidence")

    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1, "http": 1}
    ) as database:
        case = await _case(database, tmp_path)
        case.service.queries.transport = httpx.MockTransport(respond)
        monkeypatch.setattr(case.service.queries.cache, "block_until", failed_cache_write)
        prepared = await _run(database, case, case.initial)
        assert isinstance(prepared, WaitWork)
        captured = await _run(database, case, prepared.continuation)
        assert isinstance(captured, WaitWork)
        checkpoint = next(record for record in captured.records if record.kind == CHECKPOINT_KIND)
        acquisition = await restore_acquisition(database, checkpoint.identifier)
        assert acquisition.bodies[0].content == raw
        assert acquisition.record["cooldown_cache_failure"] == {"type": failure_type.__name__}
        assert "private cache failure details" not in json.dumps(acquisition.record)
        outcome = await _finish(database, case, captured.continuation)
        assert isinstance(outcome, CompletedWork)
        estimate = (await _record(database, outcome)).estimate
        assert (
            estimate.status == "unavailable" and estimate.unavailable_reason == "provider_http_429"
        )
        assert calls == 1 and await _charges(database, case.initial.operation_identifier) == 1
        _, retained = await database.get_artifact(acquisition.bodies[0].identifier)
        assert retained == raw
