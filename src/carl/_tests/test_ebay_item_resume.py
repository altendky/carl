"""Committed item responses preserve bounded recovery across worker attempts."""

# pyright: reportPrivateUsage=false

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path

import anyio
import pytest

from carl._tests.test_ebay_item_workers import (
    ITEM,
    ITEM_URL,
    _Acquirer,
    _enqueue,
    _identifiers,
    _item_html,
    _mapping,
    _registry,
    _Response,
    _run_next,
)
from carl.core.components import Component, ComponentId
from carl.core.ebay_items import (
    COLLECT_EBAY_ITEM_WORK_KIND,
    EXTRACT_EBAY_ITEM_WORK_KIND,
    CollectEbayItemPayload,
    EbayItemRequest,
    collect_ebay_item_work,
    ebay_item_plan,
)
from carl.core.http import RequestPlan
from carl.core.models import NamedOutput, RecordDraft
from carl.core.worker import AttemptContext, RetryWork
from carl.ebay_item_workers import _body_artifacts
from carl.io.httpx import Acquisition, AcquisitionFailure, IdentifierFactory
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry


async def _queue(database: Database, identifiers: Callable[[], str]) -> None:
    await _enqueue(
        database,
        collect_ebay_item_work(
            identifier="item-work",
            payload=CollectEbayItemPayload(request=EbayItemRequest(item_identifier=ITEM)),
        ),
        identifiers,
    )


@pytest.mark.anyio
async def test_three_legacy_responses_leave_only_four_new_requests(tmp_path: Path) -> None:
    identifiers = _identifiers()
    pages = _Acquirer({ITEM_URL: _Response(b"Forbidden", status_code=403)})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _queue(database, identifiers)
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)

        async def legacy_collect(
            payload: CollectEbayItemPayload, _context: AttemptContext
        ) -> RetryWork:
            acquisition = await pages.acquire(
                ebay_item_plan(payload.request, network_path=("decodo", "personal", "carl")),
                identifiers,
            )
            identifier = identifiers()
            return RetryWork(
                records=(
                    RecordDraft(
                        identifier=identifier,
                        kind=("carl", "http", "acquisition"),
                        schema_version=1,
                        value={
                            **acquisition.record,
                            "purpose": "ebay_item",
                            "ebay_item_request": payload.request.model_dump(mode="json"),
                        },
                    ),
                ),
                artifacts=_body_artifacts(acquisition),
                outputs=(NamedOutput(name=("acquisition",), object_identifier=identifier),),
                delay_ns=0,
                reason={"kind": "ebay_item_response_failure", "classification": "challenge"},
                result={"state": "retryable_failure", "acquisition_record_identifier": identifier},
            )

        original = next(
            handler
            for handler in registry.handlers
            if handler.capability.kind == COLLECT_EBAY_ITEM_WORK_KIND
        )
        legacy = WorkHandlerRegistry(
            handlers=(
                TypedWorkHandler(
                    capability=original.capability,
                    component=Component(ComponentId(("test", "legacy", "item")), 1, legacy_collect),
                    payload_type=CollectEbayItemPayload,
                    handler=legacy_collect,
                ),
            )
        )
        for attempt in range(1, 4):
            work = await _run_next(database, legacy, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
            assert work["attempt"] == attempt
            assert work["state"] == "pending"

        final = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        assert final["attempt"] == 4
        assert final["state"] == "terminal_failure"
        assert len(pages.plans) == 7
        assert len(await database.records_by_kind(("carl", "http", "acquisition"))) == 7
        result = _mapping(final["result"])
        assert result["request_attempts"] == 7
        assert len(result["acquisition_record_identifiers"]) == 7
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT count(*) FROM operation_inputs WHERE name_parts_json LIKE '%previous_acquisition%'"
            )
            assert await cursor.fetchone() == (3,)


@pytest.mark.anyio
async def test_seventh_success_survives_session_cleanup_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifiers = _identifiers()

    class EventuallySuccessful(_Acquirer):
        async def acquire(
            self, plan: RequestPlan, new_identifier: IdentifierFactory
        ) -> Acquisition:
            if len(self.plans) == 6:
                self.responses[ITEM_URL] = _Response(_item_html())
            return await super().acquire(plan, new_identifier)

    pages = EventuallySuccessful({ITEM_URL: _Response(b"Forbidden", status_code=403)})
    closed = False

    @asynccontextmanager
    async def session(
        acquirer: _Acquirer, _new_identifier: Callable[[], str]
    ) -> AsyncIterator[_Acquirer]:
        nonlocal closed
        assert not closed, "Committed success must be reused without reopening a session"
        try:
            yield acquirer
        finally:
            closed = True
        raise AcquisitionFailure(
            "test session close failure", result={"stopping_condition": "transport_failure"}
        )

    monkeypatch.setattr("carl.ebay_item_workers._item_session", session)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _queue(database, identifiers)
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        interrupted = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        assert interrupted["state"] == "pending"
        assert closed
        assert len(pages.plans) == 7
        assert _mapping(interrupted["result"])["request_attempts"] == 7
        await anyio.sleep(1.01)
        resumed = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        assert resumed["state"] == "completed"
        assert _mapping(resumed["result"])["reused"] is True
        assert _mapping(resumed["result"])["request_attempts"] == 7
        assert len(pages.plans) == 7
        extracted = await _run_next(database, registry, identifiers, EXTRACT_EBAY_ITEM_WORK_KIND)
        assert extracted["state"] == "completed"
        assert _mapping(extracted["result"])["classification"] == "detail"


@pytest.mark.anyio
async def test_transport_failure_preserves_prior_blocked_response_budget(tmp_path: Path) -> None:
    identifiers = _identifiers()

    class Interrupted(_Acquirer):
        failed = False

        async def acquire(
            self, plan: RequestPlan, new_identifier: IdentifierFactory
        ) -> Acquisition:
            if len(self.plans) == 3 and not self.failed:
                self.failed = True
                raise AcquisitionFailure(
                    "test transport failure", result={"stopping_condition": "transport_failure"}
                )
            return await super().acquire(plan, new_identifier)

    pages = Interrupted({ITEM_URL: _Response(b"Forbidden", status_code=403)})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _queue(database, identifiers)
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        interrupted = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        assert interrupted["state"] == "pending"
        assert _mapping(interrupted["result"])["request_attempts"] == 3
        await anyio.sleep(1.01)
        resumed = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        assert resumed["state"] == "terminal_failure"
        assert _mapping(resumed["result"])["request_attempts"] == 7
        assert len(pages.plans) == 7
        assert len(await database.records_by_kind(("carl", "http", "acquisition"))) == 7
