"""Blocked item evidence retries acquisition without completing unusable work."""

# pyright: reportPrivateUsage=false

from datetime import UTC, datetime
from pathlib import Path

import pytest

from carl._tests.test_ebay_item_workers import (
    ITEM,
    ITEM_URL,
    _Acquirer,
    _enqueue,
    _identifiers,
    _mapping,
    _provenance,
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
from carl.core.models import BytesDraft
from carl.core.worker import AttemptContext, RetryWork, TerminalFailureWork
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
@pytest.mark.parametrize("attempt", range(1, 6))
@pytest.mark.parametrize("status,body", [(403, b"Forbidden"), (200, AKAMAI)])
async def test_blocked_item_uses_full_search_retry_window_and_preserves_evidence(
    tmp_path: Path, attempt: int, status: int, body: bytes
) -> None:
    identifiers = _identifiers()
    pages = _Acquirer({ITEM_URL: _Response(body, status_code=status)})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        handler = next(
            h for h in registry.handlers if h.capability.kind == COLLECT_EBAY_ITEM_WORK_KIND
        )
        await database.begin_operation(
            operation_id="operation",
            component=handler.component,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        outcome = await handler.execute(
            CollectEbayItemPayload(request=EbayItemRequest(item_identifier=ITEM)).as_json(),
            AttemptContext(
                work_item_identifier="item-work",
                lease_token="lease",
                worker_identifier="worker",
                attempt=attempt,
                operation_identifier="operation",
            ),
        )
        assert isinstance(outcome, RetryWork if attempt < 5 else TerminalFailureWork)
        reason = _mapping(outcome.reason if isinstance(outcome, RetryWork) else outcome.error)
        assert reason["classification"] == "challenge"
        assert reason["maximum_attempts"] == 5
        assert reason["policy_attempt"] == attempt
        if isinstance(outcome, RetryWork):
            assert outcome.delay_ns == min(3600, 600 * 2 ** (attempt - 1)) * 1_000_000_000
        assert _mapping(outcome.result)["state"] == (
            "retryable_failure" if attempt < 5 else "terminal_failure"
        )
        assert "extraction_work_identifier" not in _mapping(outcome.result)
        assert len(outcome.records) == 1
        assert len(outcome.artifacts) == 1
        assert isinstance(outcome.artifacts[0], BytesDraft)
        assert outcome.artifacts[0].content == body


@pytest.mark.anyio
async def test_queued_block_is_pending_and_legacy_extraction_cannot_complete(
    tmp_path: Path,
) -> None:
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
        assert collected["state"] == "pending"
        assert _mapping(collected["result"])["state"] == "retryable_failure"
        assert not await database.records_by_kind(("carl", "ebay", "listing_observation"))
        acquisition = _string(_mapping(collected["result"])["acquisition_record_identifier"])
        _, retained = await database.get_artifact(pages.body_identifiers[0])
        assert retained == AKAMAI
        # A retained pre-fix acquisition can still be explicitly extracted, but
        # its offline error page must not be recorded as successfully extracted.
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
