"""Offline resident eBay sessions rotate capture and CPU publication permits."""

# pyright: reportPrivateUsage=false

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from time import time_ns
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import anyio
import pytest

import carl.ebay_session_stages as stages
from carl._tests.test_ebay import _Acquirer, _provenance
from carl._tests.test_ebay_item_workers import ITEM, _item_html
from carl._tests.test_ebay_workers import _directories
from carl.core.configuration import EbayItemConfiguration, EbaySearchConfiguration
from carl.core.ebay import CollectEbaySearchPayload, EbaySearchRequest, collect_ebay_search_work
from carl.core.ebay_items import CollectEbayItemPayload, EbayItemRequest, collect_ebay_item_work
from carl.core.http import RequestPlan
from carl.core.residents import ResidentRequestCharge
from carl.core.stages import StageAttemptContext, StageDefinition
from carl.core.work import WorkCapability, WorkRequester
from carl.ebay_session_stages import (
    BoundedEbayAcquirer,
    EbaySessionStageDependencies,
    SessionPhasePayload,
    build_ebay_session_stage_registry,
)
from carl.io.httpx import Acquisition, AcquisitionFailure, IdentifierFactory
from carl.io.proton import SharedProtonWireproxyManager
from carl.io.resident_sessions import ResidentSessionPool
from carl.io.sqlite import Database
from carl.io.stage_publication import StageHandlerResult, StagePublication
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


def _html(page: int = 1, *, next_page: bool = False) -> bytes:
    return (
        '<html><ul><li class="s-card">'
        f'<a href="https://www.ebay.com/itm/25612345678{page}"></a>'
        f'<div class="s-card__title">Bench Scope {page}</div>'
        '<span class="s-card__price">$49.95</span></li></ul>'
        '<script>{"plConfigToken":"resident-pagination-token"}</script>'
        + (
            '<a class="pagination__next" href="https://www.ebay.com/sch/i.html?_nkw=scope&amp;_pgn=2">Next</a>'
            if next_page
            else ""
        )
        + "</html>"
    ).encode()


@dataclass
class _Client:
    responses: list[tuple[bytes, int] | AcquisitionFailure]
    plans: list[RequestPlan] = field(default_factory=list)
    closed: bool = False

    def is_closed(self) -> bool:
        return self.closed

    def retained_bytes(self) -> int:
        # Fixture responses belong to the test, not the production session value.
        return 256

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        raise AssertionError("Resident stages must capture without transport-side decoding")

    async def capture(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        assert not self.closed
        self.plans.append(plan)
        response = self.responses.pop(0)
        if isinstance(response, AcquisitionFailure):
            raise response
        return await _Acquirer(response[0], status_code=response[1]).acquire(plan, new_identifier)


@dataclass
class _Sessions:
    responses: list[tuple[bytes, int] | AcquisitionFailure]
    clients: list[_Client] = field(default_factory=list)

    @asynccontextmanager
    async def __call__(self, identifier: str) -> AsyncGenerator[BoundedEbayAcquirer]:
        client = _Client(self.responses)
        self.clients.append(client)
        try:
            yield client
        finally:
            client.closed = True


async def _setup(
    database: Database,
    tmp_path: Path,
    sessions: _Sessions,
    *,
    item: bool = False,
    attempts: int = 3,
    fetch: bool = False,
    home_attempts: int = 2,
) -> tuple[ResidentSessionPool, StageDefinition]:
    pool = ResidentSessionPool(
        stage_manager(database), StageHandlerRegistry((), ()), _provenance(), {}, poll_seconds=0.005
    )
    pool.registry = build_ebay_session_stage_registry(
        EbaySessionStageDependencies(
            database,
            _directories(tmp_path),
            lambda: str(uuid4()),
            SharedProtonWireproxyManager(),
            pool,
            session_factory=sessions,
            search_policy=EbaySearchConfiguration(
                maximum_page_attempts=attempts, maximum_homepage_attempts=home_attempts
            ),
            item_policy=EbayItemConfiguration(maximum_page_attempts=attempts),
            fetch_pagination=fetch,
        )
    )
    work = (
        collect_ebay_item_work(
            identifier=str(uuid4()),
            payload=CollectEbayItemPayload(request=EbayItemRequest(item_identifier=ITEM)),
        )
        if item
        else collect_ebay_search_work(
            not_before_utc_ns=0,
            identifier=str(uuid4()),
            payload=CollectEbaySearchPayload(
                request=EbaySearchRequest(query="scope", maximum_pages=2)
            ),
        )
    )
    await database.enqueue_work(
        work,
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier=work.identifier, context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    adapter = pool.registry.require_adapter(
        WorkCapability(kind=work.kind, payload_schema_version=work.payload_schema_version)
    )
    return pool, adapter.initial_stage(work, str(uuid4()), str(uuid4()))


async def _run(pool: ResidentSessionPool, stage: StageDefinition) -> None:
    work = await pool.manager.database.work(stage.work_item_identifier)
    adapter = pool.registry.require_adapter(
        WorkCapability(
            kind=tuple(work["kind"]), payload_schema_version=work["payload_schema_version"]
        )
    )
    async with pool.managed():
        with anyio.fail_after(10):
            assert await pool.submit(stage, component=adapter.component)
            await pool.wait_idle()
    assert not pool.errors


async def test_search_resident_keeps_session_and_private_fetch_token_across_phases(
    tmp_path: Path,
) -> None:
    sessions = _Sessions([(b"home", 200), (_html(next_page=True), 200), (_html(2), 200)])
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, stage = await _setup(database, tmp_path, sessions, fetch=True)
        await _run(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "completed"
        assert work["attempt"] == 1
        assert work["result"]["page_count"] == 2
        assert len(sessions.clients) == 1 and sessions.clients[0].closed
        plans = sessions.clients[0].plans
        assert len(plans) == 3
        assert (
            next(header.value for header in plans[-1].headers if header.name == b"x-pl-config")
            == b"resident-pagination-token"
        )
        progress = await database.records_by_kind(stages._PROGRESS)
        assert all("resident-pagination-token" not in json.dumps(value) for _, value in progress)
        for _, acquisition in await database.records_by_kind(("carl", "http", "acquisition")):
            assert isinstance(acquisition, dict)
            operation = await database.operation(str(acquisition["operation_identifier"]))
            assert operation["state"] == "completed"
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT definition_json FROM work_stages WHERE work_item_id = ?",
                    (stage.work_item_identifier,),
                )
            ).fetchall()
        assert len(rows) == 7
        assert all(
            len(str(row[0]).encode()) < 65_536 and "resident-pagination-token" not in str(row[0])
            for row in rows
        )


async def test_home_and_page_challenges_preserve_same_session_attempt_budgets(
    tmp_path: Path,
) -> None:
    sessions = _Sessions([(b"blocked", 403), (b"home", 200), (b"blocked", 403), (_html(), 200)])
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, stage = await _setup(database, tmp_path, sessions)
        await _run(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        assert len(sessions.clients) == 1 and len(sessions.clients[0].plans) == 4
        assert not any(header.name == b"Referer" for header in sessions.clients[0].plans[0].headers)
        assert any(header.name == b"Referer" for header in sessions.clients[0].plans[1].headers)
        acquisitions = await database.records_by_kind(("carl", "http", "acquisition"))
        assert [value["request_attempt"] for _, value in acquisitions] == [1, 2, 1, 2]


async def test_search_exhausted_local_budget_uses_outer_retry_without_reset(tmp_path: Path) -> None:
    sessions = _Sessions([(b"home", 200), (b"blocked", 429), (b"blocked", 429), (b"blocked", 429)])
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, stage = await _setup(database, tmp_path, sessions)
        await _run(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "pending" and work["attempt"] == 1
        assert work["result"]["state"] == "retryable_failure"
        assert len(sessions.clients) == 1 and len(sessions.clients[0].plans) == 4


@pytest.mark.parametrize("blocked", (False, True))
async def test_item_same_session_loop_and_followup(tmp_path: Path, blocked: bool) -> None:
    responses: list[tuple[bytes, int] | AcquisitionFailure] = [(_item_html(), 200)]
    if blocked:
        responses.insert(0, (b"<title>Pardon Our Interruption</title>", 403))
    sessions = _Sessions(responses)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, stage = await _setup(database, tmp_path, sessions, item=True)
        await _run(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "completed"
        assert (
            work["result"]["request_attempts"] == 2
            if blocked
            else work["result"]["request_attempts"] == 1
        )
        follow = await database.work(work["result"]["extraction_work_identifier"])
        assert follow["state"] == "pending"
        assert len(sessions.clients) == 1 and sessions.clients[0].closed


async def test_cpu_checkpoint_corruption_is_terminal_without_another_capture(
    tmp_path: Path,
) -> None:
    sessions = _Sessions([(b"home", 200), (_html(), 200)])
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, stage = await _setup(database, tmp_path, sessions)
        original = pool.registry.handlers[-1]

        async def corrupt(
            payload: SessionPhasePayload, context: StageAttemptContext
        ) -> StageHandlerResult:
            for path in tmp_path.glob("responses/sha256/*/*.body"):
                path.write_bytes(b"broken")
            return await original.execute(payload.model_dump(mode="json"), context)

        pool.registry = replace(
            pool.registry,
            handlers=(
                *pool.registry.handlers[:-1],
                TypedStageHandler(original.kind, 1, SessionPhasePayload, corrupt),
            ),
        )
        await _run(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "terminal_failure"
        assert work["result"]["state"] == "checkpoint_failed"
        assert len(sessions.clients[0].plans) == 1


async def test_checkpoint_storage_failure_is_terminal_after_one_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions = _Sessions([(b"home", 200), (_html(), 200)])

    async def fail(acquisition: Acquisition, **kwargs: object):
        raise OSError("disk full")

    monkeypatch.setattr(stages, "checkpoint_acquisition", fail)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, stage = await _setup(database, tmp_path, sessions)
        await _run(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "terminal_failure"
        assert len(sessions.clients[0].plans) == 1


async def test_transport_failure_releases_session_and_preserves_retry_policy(
    tmp_path: Path,
) -> None:
    sessions = _Sessions(
        [
            AcquisitionFailure(
                "offline failure",
                result={
                    "stopping_condition": "transport_failure",
                    "exception_type": "TimeoutError",
                },
            )
        ]
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, stage = await _setup(database, tmp_path, sessions)
        await _run(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "pending"
        assert sessions.clients[0].closed and len(sessions.clients[0].plans) == 1


@pytest.mark.parametrize("item", (False, True))
async def test_partial_transport_failure_keeps_external_raw_evidence(
    tmp_path: Path, item: bool
) -> None:
    acquisition = await _Acquirer(b"partial retained evidence").acquire(
        RequestPlan(url="https://www.ebay.com/"), lambda: str(uuid4())
    )
    failure = AcquisitionFailure(
        "read failed",
        result={
            **acquisition.record,
            "stopping_condition": "transport_failure",
            "failure_phase": "body_read",
        },
        bodies=acquisition.bodies,
    )
    sessions = _Sessions([failure])
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, stage = await _setup(database, tmp_path, sessions, item=item)
        await _run(pool, stage)
        work = await database.work(stage.work_item_identifier)
        assert work["state"] == "pending" and work["attempt"] == 1
        metadata = await database.get_external_artifact_metadata(acquisition.bodies[0].identifier)
        assert metadata is not None
        _, content = await database.get_artifact(acquisition.bodies[0].identifier)
        assert content == b"partial retained evidence"
        if not item:
            records = await database.records_by_kind(("carl", "http", "acquisition"))
            assert len(records) == 1
            assert isinstance(records[0][1], dict)
            failed = await database.operation(str(records[0][1]["operation_identifier"]))
            assert failed["state"] == "failed"
        assert sessions.clients[0].closed and len(sessions.clients[0].plans) == 1


async def test_parked_cpu_checkpoint_reopens_database_without_refetching_paid_page(
    tmp_path: Path,
) -> None:
    sessions = _Sessions(
        [(b"home", 200), (_html(next_page=True), 200), (b"new home", 200), (_html(2), 200)]
    )
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        pool, initial = await _setup(database, tmp_path, sessions, fetch=True)
        original = pool.registry.handlers[1]

        async def park_page(
            payload: SessionPhasePayload, context: StageAttemptContext
        ) -> StageHandlerResult:
            result = await original.execute(payload.model_dump(mode="json"), context)
            if isinstance(result, StagePublication) and len(sessions.clients[0].plans) == 2:
                outcome = result.outcome
                assert isinstance(outcome, stages.WaitWork)
                continuation = outcome.continuation.model_copy(
                    update={"not_before_utc_ns": time_ns() + 60_000_000_000}
                )
                return StagePublication(
                    outcome.model_copy(update={"continuation": continuation}), result.children
                )
            return result

        pool.registry = replace(
            pool.registry,
            handlers=(
                pool.registry.handlers[0],
                TypedStageHandler(original.kind, 1, SessionPhasePayload, park_page),
                pool.registry.handlers[2],
            ),
        )
        await _run(pool, initial)
        work = await database.work(initial.work_item_identifier)
        assert work["state"] in {"pending", "leased"} and work["attempt"] == 1
        assert len(sessions.clients[0].plans) == 2 and sessions.clients[0].closed
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT id FROM work_stages WHERE work_item_id = ? AND state = 'pending'",
                    (initial.work_item_identifier,),
                )
            ).fetchall()
        assert len(rows) == 1
        pending_identifier = str(rows[0][0])

    async with Database.managed(path) as database:
        manager = replace(stage_manager(database), utc_now_ns=lambda: time_ns() + 61_000_000_000)
        pool = ResidentSessionPool(
            manager, StageHandlerRegistry((), ()), _provenance(), {}, poll_seconds=0.005
        )
        pool.registry = build_ebay_session_stage_registry(
            EbaySessionStageDependencies(
                database,
                _directories(tmp_path),
                lambda: str(uuid4()),
                SharedProtonWireproxyManager(),
                pool,
                session_factory=sessions,
                fetch_pagination=True,
            )
        )
        pending = await database.stage(pending_identifier)
        await _run(pool, pending)
        work = await database.work(initial.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        assert work["result"]["page_count"] == 2
        assert len(sessions.clients) == 2 and all(client.closed for client in sessions.clients)
        assert len(sessions.clients[1].plans) == 2
        assert sessions.clients[1].plans[0].url == "https://www.ebay.com/"
        assert not any(header.name == b"Referer" for header in sessions.clients[1].plans[0].headers)
        assert "_pgn=2" in sessions.clients[1].plans[1].url
        assert not any(
            header.name == b"x-pl-config" for header in sessions.clients[1].plans[1].headers
        )


async def test_unsupported_capture_is_terminal_and_closes_opened_provider(tmp_path: Path) -> None:
    closed = False

    @asynccontextmanager
    async def legacy_factory(identifier: str) -> AsyncGenerator[BoundedEbayAcquirer]:
        nonlocal closed
        try:
            yield cast(BoundedEbayAcquirer, cast(object, SimpleNamespace(identifier=identifier)))
        finally:
            closed = True

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, initial = await _setup(database, tmp_path, _Sessions([]))
        pool.registry = build_ebay_session_stage_registry(
            EbaySessionStageDependencies(
                database,
                _directories(tmp_path),
                lambda: str(uuid4()),
                SharedProtonWireproxyManager(),
                pool,
                session_factory=legacy_factory,
            )
        )
        await _run(pool, initial)
        work = await database.work(initial.work_item_identifier)
        assert work["state"] == "terminal_failure" and closed
        assert work["result"]["state"] == "configuration_failed"
        assert await database.records_by_kind(("carl", "http", "stage_acquisition")) == ()


class _CrashAfterCapture(BaseException):
    pass


@pytest.mark.parametrize("item", (False, True))
async def test_recovered_capture_persists_charged_ordinal_instead_of_response_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, item: bool
) -> None:
    sessions = _Sessions(
        [(_item_html(), 200)] * 2 if item else [(b"home", 200), (b"home", 200), (_html(), 200)]
    )
    checkpoint = stages.checkpoint_acquisition
    crashed = False

    async def crash_once(acquisition: Acquisition, **kwargs: object):
        nonlocal crashed
        if not crashed:
            crashed = True
            raise _CrashAfterCapture()
        return await checkpoint(
            acquisition,
            data_directory=cast(Path, kwargs["data_directory"]),
            new_identifier=cast(IdentifierFactory, kwargs["new_identifier"]),
        )

    monkeypatch.setattr(stages, "checkpoint_acquisition", crash_once)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, initial = await _setup(database, tmp_path, sessions, item=item)
        adapter = pool.registry.adapters[-1] if item else pool.registry.adapters[0]
        async with pool.managed():
            assert await pool.submit(initial, component=adapter.component)
            await pool.wait_idle()
        assert list(pool.errors.values()) == ["_CrashAfterCapture"]
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT id FROM work_stages WHERE work_item_id = ? AND state = 'pending'",
                    (initial.work_item_identifier,),
                )
            ).fetchall()
        assert len(rows) == 1
        pending = await database.stage(str(rows[0][0]))
        pool = ResidentSessionPool(
            stage_manager(database),
            StageHandlerRegistry((), ()),
            _provenance(),
            {},
            poll_seconds=0.005,
        )
        pool.registry = build_ebay_session_stage_registry(
            EbaySessionStageDependencies(
                database,
                _directories(tmp_path),
                lambda: str(uuid4()),
                SharedProtonWireproxyManager(),
                pool,
                session_factory=sessions,
                item_policy=EbayItemConfiguration(maximum_page_attempts=3),
            )
        )
        await _run(pool, pending)
        work = await database.work(initial.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        acquisitions = await database.records_by_kind(("carl", "http", "acquisition"))
        assert acquisitions[0][1]["request_attempt"] == 2
        if item:
            assert work["result"]["request_attempts"] == 2
            assert len(work["result"]["acquisition_record_identifiers"]) == 1


async def test_idempotent_request_charge_never_dispatches_a_second_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    charge = Database.charge_resident_request

    async def repeated(
        database: Database,
        context: StageAttemptContext,
        *,
        request_identity: tuple[str, ...],
        maximum_attempts: int,
        now_utc_ns: int,
    ) -> ResidentRequestCharge | None:
        first = await charge(
            database,
            context,
            request_identity=request_identity,
            maximum_attempts=maximum_attempts,
            now_utc_ns=now_utc_ns,
        )
        assert first is not None and first.newly_charged
        return await charge(
            database,
            context,
            request_identity=request_identity,
            maximum_attempts=maximum_attempts,
            now_utc_ns=now_utc_ns,
        )

    monkeypatch.setattr(Database, "charge_resident_request", repeated)
    sessions = _Sessions([])
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, initial = await _setup(database, tmp_path, sessions)
        await _run(pool, initial)
        work = await database.work(initial.work_item_identifier)
        assert work["state"] == "pending" and not sessions.clients
        assert work["result"]["acquisition"]["failure_phase"] == "uncertain_previous_dispatch"


async def test_item_retry_inherits_uncertain_requests_and_labels_seventh_dispatch(
    tmp_path: Path,
) -> None:
    responses: list[tuple[bytes, int] | AcquisitionFailure] = [
        (b"<title>Pardon Our Interruption</title>", 403)
    ] * 5
    responses.extend(
        [
            AcquisitionFailure(
                "request failed", result={"stopping_condition": "transport_failure"}
            ),
            (_item_html(), 200),
        ]
    )
    sessions = _Sessions(responses)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, initial = await _setup(database, tmp_path, sessions, item=True, attempts=7)
        await _run(pool, initial)
        work = await database.work(initial.work_item_identifier)
        assert work["state"] == "pending" and work["result"]["request_attempts"] == 6
        assert len(work["result"]["acquisition_record_identifiers"]) == 5
        pool = ResidentSessionPool(
            replace(stage_manager(database), utc_now_ns=lambda: time_ns() + 3_000_000_000),
            StageHandlerRegistry((), ()),
            _provenance(),
            {},
            poll_seconds=0.005,
        )
        pool.registry = build_ebay_session_stage_registry(
            EbaySessionStageDependencies(
                database,
                _directories(tmp_path),
                lambda: str(uuid4()),
                SharedProtonWireproxyManager(),
                pool,
                session_factory=sessions,
                item_policy=EbayItemConfiguration(maximum_page_attempts=7),
            )
        )
        definition = collect_ebay_item_work(
            identifier=initial.work_item_identifier,
            payload=CollectEbayItemPayload(request=EbayItemRequest(item_identifier=ITEM)),
        )
        adapter = pool.registry.require_adapter(
            WorkCapability(
                kind=definition.kind, payload_schema_version=definition.payload_schema_version
            )
        )
        await _run(pool, adapter.initial_stage(definition, str(uuid4()), str(uuid4())))
        work = await database.work(initial.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 2
        assert work["result"]["request_attempts"] == 7
        assert sum(len(client.plans) for client in sessions.clients) == 7
        _, _, acquisition = await database.get_record(
            work["result"]["acquisition_record_identifier"]
        )
        assert isinstance(acquisition, dict)
        assert acquisition["request_attempt"] == 7
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT attempt, ordinal FROM network_activities WHERE id = ?",
                    (acquisition["network_activity_identifier"],),
                )
            ).fetchall()
        assert rows == [(7, 7)]


@pytest.mark.parametrize("phase", ("homepage", "page", "item"))
async def test_uncertain_dispatches_remain_bounded_across_resident_incarnations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    maximum = 2 if phase == "homepage" else 7 if phase == "item" else 3
    sessions = _Sessions(
        [(b"home", 200)] * 10
        if phase == "homepage"
        else [(_item_html(), 200)] * 10
        if phase == "item"
        else [(b"home", 200), (_html(), 200)] * 10
    )
    checkpoint = stages.checkpoint_acquisition

    async def crash_after_capture(acquisition: Acquisition, **kwargs: object):
        url = acquisition.record.get("effective_url")
        crash = isinstance(url, str) and (
            phase == "item"
            or (phase == "homepage" and url == "https://www.ebay.com/")
            or (phase == "page" and "/sch/" in url)
        )
        if crash:
            raise _CrashAfterCapture()
        # Preserve the typed public signature while forwarding the actual call.
        return await checkpoint(
            acquisition,
            data_directory=cast(Path, kwargs["data_directory"]),
            new_identifier=cast(IdentifierFactory, kwargs["new_identifier"]),
        )

    monkeypatch.setattr(stages, "checkpoint_acquisition", crash_after_capture)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        pool, initial = await _setup(
            database,
            tmp_path,
            sessions,
            item=phase == "item",
            attempts=maximum,
            home_attempts=20 if phase == "page" else 2,
        )
        pending = initial
        for incarnation in range(maximum + 1):
            if incarnation:
                pool = ResidentSessionPool(
                    stage_manager(database),
                    StageHandlerRegistry((), ()),
                    _provenance(),
                    {},
                    poll_seconds=0.005,
                )
                pool.registry = build_ebay_session_stage_registry(
                    EbaySessionStageDependencies(
                        database,
                        _directories(tmp_path),
                        lambda: str(uuid4()),
                        SharedProtonWireproxyManager(),
                        pool,
                        session_factory=sessions,
                        search_policy=EbaySearchConfiguration(
                            maximum_page_attempts=3,
                            maximum_homepage_attempts=20 if phase == "page" else 2,
                        ),
                        item_policy=EbayItemConfiguration(maximum_page_attempts=maximum),
                    )
                )
            work = await database.work(initial.work_item_identifier)
            adapter = pool.registry.require_adapter(
                WorkCapability(
                    kind=tuple(work["kind"]), payload_schema_version=work["payload_schema_version"]
                )
            )
            async with pool.managed():
                with anyio.fail_after(10):
                    assert await pool.submit(pending, component=adapter.component)
                    await pool.wait_idle()
            assert all(client.closed for client in sessions.clients)
            work = await database.work(initial.work_item_identifier)
            assert work["attempt"] == 1
            if incarnation < maximum:
                assert list(pool.errors.values()) == ["_CrashAfterCapture"]
                async with database._connections.reader() as connection:
                    rows = await (
                        await connection.execute(
                            "SELECT id FROM work_stages WHERE work_item_id = ? AND state = 'pending'",
                            (initial.work_item_identifier,),
                        )
                    ).fetchall()
                assert len(rows) == 1
                pending = await database.stage(str(rows[0][0]))
                assert pending.operation_identifier == initial.operation_identifier
            else:
                assert not pool.errors and work["state"] == "pending"
        plans = [plan for client in sessions.clients for plan in client.plans]
        target_plans = [
            plan
            for plan in plans
            if phase == "item"
            or (phase == "homepage" and plan.url == "https://www.ebay.com/")
            or (phase == "page" and "/sch/" in plan.url)
        ]
        assert len(target_plans) == maximum
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT request_identity_json, max(ordinal) FROM resident_request_charges WHERE operation_id = ? GROUP BY request_identity_json",
                    (initial.operation_identifier,),
                )
            ).fetchall()
        identity = ["ebay", "page", "1"] if phase == "page" else ["ebay", phase]
        assert next(row[1] for row in rows if json.loads(str(row[0])) == identity) == maximum
        if phase == "item":
            # None of the seven paid requests reached the acquisition checkpoint.
            # The next logical operation must still inherit their budget usage.
            assert (
                await database.retained_ebay_item_acquisitions(
                    initial.work_item_identifier, before_attempt=2
                )
                == ()
            )
            assert (
                await database.retained_ebay_item_request_attempts(
                    initial.work_item_identifier, before_attempt=2
                )
                == 7
            )
            assert (
                await database.retained_ebay_item_request_attempts(
                    initial.work_item_identifier, before_attempt=2, retry_budget_start_attempt=1
                )
                == 0
            )
            pool = ResidentSessionPool(
                replace(stage_manager(database), utc_now_ns=lambda: time_ns() + 3_000_000_000),
                StageHandlerRegistry((), ()),
                _provenance(),
                {},
                poll_seconds=0.005,
            )
            pool.registry = build_ebay_session_stage_registry(
                EbaySessionStageDependencies(
                    database,
                    _directories(tmp_path),
                    lambda: str(uuid4()),
                    SharedProtonWireproxyManager(),
                    pool,
                    session_factory=sessions,
                    item_policy=EbayItemConfiguration(maximum_page_attempts=7),
                )
            )
            definition = collect_ebay_item_work(
                identifier=initial.work_item_identifier,
                payload=CollectEbayItemPayload(request=EbayItemRequest(item_identifier=ITEM)),
            )
            adapter = pool.registry.require_adapter(
                WorkCapability(
                    kind=definition.kind, payload_schema_version=definition.payload_schema_version
                )
            )
            await _run(pool, adapter.initial_stage(definition, str(uuid4()), str(uuid4())))
            work = await database.work(initial.work_item_identifier)
            assert work["state"] == "terminal_failure" and work["attempt"] == 2
            assert work["result"]["request_attempts"] == 7
            assert sum(len(client.plans) for client in sessions.clients) == 7
