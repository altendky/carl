"""Real production discovery and resident admission drive eBay's phased collectors."""

# Reuse offline transport fixtures while inspecting persisted ownership fences.
# pyright: reportPrivateUsage=false

import json
from dataclasses import replace
from pathlib import Path
from time import time_ns
from uuid import uuid4

import anyio
import pytest

import carl.ebay_session_stages as ebay_stages
from carl import work_runtime
from carl._tests.test_ebay_item_workers import ITEM, _item_html
from carl._tests.test_ebay_session_stages import _html, _Sessions
from carl._tests.test_ebay_workers import _directories
from carl.core.configuration import EbayItemConfiguration, EbaySearchConfiguration
from carl.core.ebay import CollectEbaySearchPayload, EbaySearchRequest, collect_ebay_search_work
from carl.core.ebay_items import CollectEbayItemPayload, EbayItemRequest, collect_ebay_item_work
from carl.core.models import JsonValue
from carl.core.work import WorkDefinition, WorkRequester
from carl.io.http_checkpoints import HttpCheckpoint
from carl.io.httpx import Acquisition, AcquisitionFailure, IdentifierFactory
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _install(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, sessions: _Sessions) -> None:
    original = work_runtime.build_session_stage_registry

    def build(
        *,
        facebook: work_runtime.FacebookSessionStageDependencies,
        ebay: work_runtime.EbaySessionStageDependencies,
    ) -> StageHandlerRegistry:
        return original(
            facebook=facebook,
            ebay=replace(
                ebay,
                session_factory=sessions,
                search_policy=EbaySearchConfiguration(
                    maximum_homepage_attempts=2, maximum_page_attempts=2
                ),
                item_policy=EbayItemConfiguration(maximum_page_attempts=2),
                fetch_pagination=True,
            ),
        )

    monkeypatch.setattr(work_runtime, "build_session_stage_registry", build)
    monkeypatch.setattr(work_runtime, "user_directories", lambda: _directories(tmp_path))


async def _enqueue(database: Database, *, item: bool) -> WorkDefinition:
    work = (
        collect_ebay_item_work(
            identifier=str(uuid4()),
            payload=CollectEbayItemPayload(request=EbayItemRequest(item_identifier=ITEM)),
        )
        if item
        else collect_ebay_search_work(
            identifier=str(uuid4()),
            payload=CollectEbaySearchPayload(
                request=EbaySearchRequest(query="scope", maximum_pages=2)
            ),
            not_before_utc_ns=0,
        )
    )
    await database.enqueue_work(
        work,
        WorkRequester(
            request_identifier=str(uuid4()),
            kind=("fixture",),
            identifier=work.identifier,
            context={},
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    return work


async def _completed(database: Database, identifier: str) -> dict[str, JsonValue]:
    with anyio.fail_after(20):
        while True:
            row = (await database.work_progress((identifier,)))[0]
            if row["state"] not in {"pending", "leased", "cancelling"}:
                assert row["state"] == "completed", row
                return await database.work(identifier)
            await anyio.sleep(0.02)


@pytest.mark.parametrize("item", (False, True))
async def test_production_dispatch_starts_one_resident_and_preserves_request_budgets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, item: bool
) -> None:
    responses: list[tuple[bytes, int] | AcquisitionFailure] = (
        [(b"<title>Pardon Our Interruption</title>", 403), (_item_html(), 200)]
        if item
        else [
            (b"blocked", 403),
            (b"home", 200),
            (b"blocked", 403),
            (_html(next_page=True), 200),
            (_html(2), 200),
        ]
    )
    sessions = _Sessions(responses)
    _install(monkeypatch, tmp_path, sessions)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        definition = await _enqueue(database, item=item)
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=(definition.identifier,), worker_count=1
        ):
            work = await _completed(database, definition.identifier)
        assert work["attempt"] == 1
        assert len(sessions.clients) == 1 and sessions.clients[0].closed
        assert not sessions.responses
        plans = sessions.clients[0].plans
        assert len(plans) == (2 if item else 5)
        if not item:
            assert any(
                header.name == b"x-pl-config" and header.value == b"resident-pagination-token"
                for header in plans[-1].headers
            )
        async with database._connections.reader() as connection:
            residents = await (
                await connection.execute(
                    "SELECT id,operation_id,generation,state FROM resident_sessions WHERE work_item_id=?",
                    (definition.identifier,),
                )
            ).fetchall()
            assert len(residents) == 1
            resident, operation, generation, state = residents[0]
            assert generation == 1 and state == "closed"
            phases = await (
                await connection.execute(
                    "SELECT json_extract(definition_json,'$.resident_session_identifier'),state "
                    "FROM work_stages WHERE work_item_id=?",
                    (definition.identifier,),
                )
            ).fetchall()
            assert len(phases) == 1 + 2 * len(plans)
            assert all(tuple(phase) == (resident, "completed") for phase in phases)
            charges = await (
                await connection.execute(
                    "SELECT request_identity_json,ordinal,maximum_attempts,resident_id "
                    "FROM resident_request_charges WHERE operation_id=? "
                    "ORDER BY request_identity_json,ordinal",
                    (operation,),
                )
            ).fetchall()
            assert len(charges) == len(plans)
            assert all(charge[2] == 2 and charge[3] == resident for charge in charges)
            assert [charge[1] for charge in charges] == ([1, 2] if item else [1, 2, 1, 2, 1])
            activities = await (
                await connection.execute(
                    "SELECT network_session_identifier,state FROM network_activities WHERE operation_id=?",
                    (operation,),
                )
            ).fetchall()
            assert len(activities) == len(plans)
            assert all(tuple(activity) == (resident, "completed") for activity in activities)
        assert (await database.operation(str(operation)))["state"] == "completed"
        acquisitions = await database.records_by_kind(("carl", "http", "acquisition"))
        assert len(acquisitions) == len(plans)
        for identifier, acquisition in acquisitions:
            assert isinstance(acquisition, dict)
            producer, _, outputs = await database.object_operation_relations(identifier)
            assert (producer == operation) is item
            assert any(output.object_identifier == identifier for output in outputs)
            child = await database.operation(producer)
            assert child["state"] == "completed"
            assert isinstance(child["code_provenance"], dict)
            assert child["code_provenance"]["python_version"]
        if item:
            result = work["result"]
            assert isinstance(result, dict)
            extraction = result["extraction_work_identifier"]
            assert isinstance(extraction, str)
            assert (await database.work(extraction))["state"] == "pending"
        # Reopening the real dispatcher cannot rerun completed work or its excluded followups.
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=(definition.identifier,), worker_count=1
        ):
            await anyio.sleep(0.1)
        assert len(sessions.clients) == 1


@pytest.mark.parametrize("item", (False, True))
async def test_production_runtime_reentry_rebinds_resident_without_resetting_dispatch_charge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, item: bool
) -> None:
    sessions = _Sessions(
        [(_item_html(), 200), (_item_html(), 200)]
        if item
        else [(b"home", 200), (b"home", 200), (_html(), 200)]
    )
    _install(monkeypatch, tmp_path, sessions)
    original = ebay_stages.checkpoint_acquisition
    captured = anyio.Event()

    async def pause_after_capture(
        acquisition: Acquisition, *, data_directory: Path, new_identifier: IdentifierFactory
    ) -> HttpCheckpoint:
        if not captured.is_set():
            captured.set()
            await anyio.sleep_forever()
        return await original(
            acquisition, data_directory=data_directory, new_identifier=new_identifier
        )

    monkeypatch.setattr(ebay_stages, "checkpoint_acquisition", pause_after_capture)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        definition = await _enqueue(database, item=item)
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=(definition.identifier,), worker_count=1
        ):
            with anyio.fail_after(20):
                await captured.wait()
        assert len(sessions.clients) == 1 and sessions.clients[0].closed
        async with work_runtime.managed_worker_pool(
            database, tmp_path, eligible_identifiers=(definition.identifier,), worker_count=1
        ):
            work = await _completed(database, definition.identifier)
        assert work["attempt"] == 1
        assert len(sessions.clients) == 2 and all(client.closed for client in sessions.clients)
        assert not sessions.responses
        async with database._connections.reader() as connection:
            residents = await (
                await connection.execute(
                    "SELECT id,generation,state FROM resident_sessions WHERE work_item_id=? ORDER BY generation",
                    (definition.identifier,),
                )
            ).fetchall()
            assert [row[1] for row in residents] == [1, 2]
            assert residents[-1][2] == "closed"
            charges = await (
                await connection.execute(
                    "SELECT charge.ordinal,charge.resident_id FROM resident_request_charges AS charge "
                    "JOIN work_operations AS owner ON owner.operation_id=charge.operation_id "
                    "WHERE owner.work_item_id=? AND charge.request_identity_json=? ORDER BY ordinal",
                    (
                        definition.identifier,
                        json.dumps(["ebay", "item" if item else "homepage"], separators=(",", ":")),
                    ),
                )
            ).fetchall()
            assert [row[0] for row in charges] == [1, 2]
            assert [row[1] for row in charges] == [row[0] for row in residents]
