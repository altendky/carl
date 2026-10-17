"""Blocked item responses recover within one leased work item and session."""

# pyright: reportPrivateUsage=false

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import override

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
    _string,
)
from carl.core.ebay_items import (
    COLLECT_EBAY_ITEM_WORK_KIND,
    EXTRACT_EBAY_ITEM_WORK_KIND,
    CollectEbayItemPayload,
    EbayItemRequest,
    EbayItemResponseKind,
    ExtractEbayItemPayload,
    collect_ebay_item_work,
    extract_ebay_item,
    extract_ebay_item_work,
)
from carl.core.http import RequestPlan
from carl.core.work import WorkState
from carl.io.httpx import Acquisition, IdentifierFactory
from carl.io.sqlite import Database

AKAMAI = (
    b"<html><h1>Access Denied</h1>Reference #0.123 <p>https://errors.edgesuite.net/0.123</p></html>"
)


@pytest.mark.parametrize(
    "status,body", [(403, b"Forbidden"), (429, b"Rate limited"), (200, AKAMAI)]
)
def test_http_blocks_and_akamai_pages_are_item_challenges(status: int, body: bytes) -> None:
    extraction = extract_ebay_item(
        body.decode(), item_identifier=ITEM, effective_url=ITEM_URL, status_code=status
    )
    assert extraction.classification == EbayItemResponseKind.CHALLENGE
    assert extraction.issues == ({"kind": "blocked_response", "http_status": status},)
    assert extraction.title is None


@pytest.mark.anyio
@pytest.mark.parametrize("recover", (False, True))
@pytest.mark.parametrize("status,body", [(403, b"Forbidden"), (200, AKAMAI)])
async def test_seven_item_attempts_share_session_and_checkpoint_each_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recover: bool, status: int, body: bytes
) -> None:
    identifiers = _identifiers()
    session_events: list[str] = []
    session_open = False

    async def unexpected_sleep(_seconds: float) -> None:
        pytest.fail("The default item policy must not sleep between blocked responses")

    monkeypatch.setattr("carl.ebay_item_workers.item_retry_sleep", unexpected_sleep)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:

        class CheckpointAcquirer(_Acquirer):
            @override
            async def acquire(
                self, plan: RequestPlan, new_identifier: IdentifierFactory
            ) -> Acquisition:
                assert session_open
                # Earlier responses must survive even if this lease is interrupted
                # before the next request finishes.
                retained = await database.records_by_kind(("carl", "http", "acquisition"))
                assert len(retained) == len(self.plans)
                for identifier in self.body_identifiers:
                    assert (await database.get_artifact(identifier))[1] == body
                if recover and len(self.plans) == 6:
                    self.responses[ITEM_URL] = _Response(_item_html())
                return await super().acquire(plan, new_identifier)

        pages = CheckpointAcquirer({ITEM_URL: _Response(body, status_code=status)})

        @asynccontextmanager
        async def session(
            acquirer: _Acquirer, _new_identifier: Callable[[], str]
        ) -> AsyncGenerator[_Acquirer]:
            nonlocal session_open
            assert acquirer is pages
            assert not session_open
            session_events.append("opened")
            session_open = True
            try:
                yield pages
            finally:
                session_open = False
                session_events.append("closed")

        monkeypatch.setattr("carl.ebay_item_workers._item_session", session)
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        _ = await _enqueue(
            database,
            collect_ebay_item_work(
                identifier="item-work",
                payload=CollectEbayItemPayload(request=EbayItemRequest(item_identifier=ITEM)),
            ),
            identifiers,
        )
        collected = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        assert collected["attempt"] == 1
        assert session_events == ["opened", "closed"]
        assert not session_open
        assert len(pages.plans) == 7
        assert len(await database.records_by_kind(("carl", "http", "acquisition"))) == 7
        for index, identifier in enumerate(pages.body_identifiers):
            assert (await database.get_artifact(identifier))[1] == (
                _item_html() if recover and index == 6 else body
            )
        result = _mapping(collected["result"])
        if recover:
            assert collected["state"] == "completed"
            assert result["state"] == "acquired"
            assert await database.work_state(_string(result["extraction_work_identifier"])) == (
                WorkState.PENDING
            )
            extracted = await _run_next(
                database, registry, identifiers, EXTRACT_EBAY_ITEM_WORK_KIND
            )
            assert extracted["state"] == "completed"
            assert _mapping(extracted["result"])["classification"] == "detail"
        else:
            assert collected["state"] == "terminal_failure"
            assert result["state"] == "terminal_failure"
            assert "extraction_work_identifier" not in result
            error = _mapping(collected["error"])
            assert error["classification"] == "challenge"
            assert error["maximum_attempts"] == 7
            assert not await database.records_by_kind(("carl", "ebay", "listing_observation"))


@pytest.mark.anyio
async def test_legacy_blocked_acquisition_cannot_complete_extraction(tmp_path: Path) -> None:
    identifiers = _identifiers()
    pages = _Acquirer({ITEM_URL: _Response(AKAMAI, status_code=403)})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        _ = await _enqueue(
            database,
            collect_ebay_item_work(
                identifier="item-work",
                payload=CollectEbayItemPayload(request=EbayItemRequest(item_identifier=ITEM)),
            ),
            identifiers,
        )
        collected = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        assert collected["state"] == "terminal_failure"
        acquisition = _string(_mapping(collected["result"])["acquisition_record_identifier"])
        # Retained pre-fix acquisitions can still be explicitly extracted, but
        # offline error pages must not be recorded as successfully extracted.
        _ = await _enqueue(
            database,
            extract_ebay_item_work(
                identifier="legacy-extraction",
                payload=ExtractEbayItemPayload(
                    request=EbayItemRequest(item_identifier=ITEM),
                    acquisition_record_identifier=acquisition,
                ),
            ),
            identifiers,
        )
        extracted = await _run_next(database, registry, identifiers, EXTRACT_EBAY_ITEM_WORK_KIND)
        assert extracted["state"] == "terminal_failure"
        assert _mapping(extracted["result"])["state"] == "response_failed"
        assert _mapping(extracted["result"])["classification"] == "challenge"
        assert _mapping(extracted["error"])["kind"] == "ebay_item_response_failure"
