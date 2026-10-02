"""Source-scoped atomic recovery never launches acquisition itself."""

# Reuse the existing offline test fixtures, which deliberately remain private.
# pyright: reportPrivateUsage=false

from pathlib import Path
from time import time_ns

import pytest

from carl._tests.test_ebay_item_workers import (
    IMAGE_URL,
    ITEM,
    SECOND_ITEM,
    _Acquirer,
    _enqueue,
    _identifiers,
    _registry,
    _Response,
    _retain_records,
    _run_next,
)
from carl._tests.test_ebay_refresh_workers import _source_records
from carl.core.components import Component, ComponentId
from carl.core.ebay import EbaySearchRequest
from carl.core.ebay_items import (
    COLLECT_EBAY_IMAGE_WORK_KIND,
    CollectEbayImagePayload,
    collect_ebay_image_work,
)
from carl.core.ebay_refresh import (
    REFRESH_EBAY_SEARCH_WORK_KIND,
    RefreshEbaySearchPayload,
    refresh_ebay_search_work,
)
from carl.core.models import RecordDraft
from carl.core.work import WorkCapability
from carl.core.worker import AttemptContext, CompletedWork
from carl.ebay_retry import retry_ebay_image_failures
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry


def _anchor() -> None:
    pass


@pytest.mark.anyio
@pytest.mark.parametrize("source", ("run", "refresh"))
async def test_retry_is_bounded_scoped_and_retains_history(tmp_path: Path, source: str) -> None:
    identifiers = _identifiers()
    images = _Acquirer({IMAGE_URL: _Response(b"not an image", "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        records: list[RecordDraft] = list(_source_records("run", (ITEM,)))
        for index, item in enumerate((ITEM, ITEM, SECOND_ITEM), start=1):
            records.extend(
                (
                    RecordDraft(
                        identifier=f"observation{index}",
                        kind=("carl", "ebay", "listing_observation"),
                        schema_version=1,
                        value={"item_identifier": item, "classification": "detail"},
                    ),
                    RecordDraft(
                        identifier=f"reference{index}",
                        kind=("carl", "ebay", "gallery_image_reference"),
                        schema_version=1,
                        value={
                            "item_identifier": item,
                            "observation_record_identifier": f"observation{index}",
                            "url": IMAGE_URL,
                        },
                    ),
                )
            )
        await _retain_records(database, tuple(records), identifiers)
        registry = _registry(database, tmp_path, identifiers, image_acquirer=images)
        for index, item in enumerate((ITEM, ITEM, SECOND_ITEM), start=1):
            _ = await _enqueue(
                database,
                collect_ebay_image_work(
                    identifier=f"image{index}",
                    payload=CollectEbayImagePayload(
                        item_identifier=item,
                        observation_record_identifier=f"observation{index}",
                        reference_record_identifier=f"reference{index}",
                        url=IMAGE_URL,
                    ),
                ),
                identifiers,
            )
            failed = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
            assert failed["state"] == "terminal_failure"
        if source == "refresh":

            async def complete(
                _payload: RefreshEbaySearchPayload, _context: AttemptContext
            ) -> CompletedWork:
                return CompletedWork(result={"observation_record_identifiers": ["observation1"]})

            source_registry = WorkHandlerRegistry(
                handlers=(
                    TypedWorkHandler(
                        capability=WorkCapability(
                            kind=REFRESH_EBAY_SEARCH_WORK_KIND, payload_schema_version=1
                        ),
                        component=Component(ComponentId(("test", "refresh_source")), 1, _anchor),
                        payload_type=RefreshEbaySearchPayload,
                        handler=complete,
                    ),
                )
            )
            _ = await _enqueue(
                database,
                refresh_ebay_search_work(
                    identifier="refresh",
                    payload=RefreshEbaySearchPayload(
                        base_search_run_record_identifier="run",
                        search_work_identifier="unused",
                        search=EbaySearchRequest(query="eyepiece"),
                    ),
                ),
                identifiers,
            )
            _ = await _run_next(
                database, source_registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND
            )
        before = await database.work("image1")
        matched, retried = await retry_ebay_image_failures(
            database, source, 1, time_ns(), identifiers
        )
        assert matched == (2 if source == "run" else 1)
        assert retried == ("image1",)
        after = await database.work("image1")
        assert after["state"] == "pending"
        assert after["attempt"] == before["attempt"] == 1
        assert after["operations"] == before["operations"]
        assert after["result"] is None and after["error"] is None
        assert (await database.work("image3"))["state"] == "terminal_failure"
        assert len(images.plans) == 3
        matched_again, retry_again = await retry_ebay_image_failures(
            database, source, 1, time_ns(), identifiers
        )
        assert matched_again == (1 if source == "run" else 0)
        assert retry_again == (("image2",) if source == "run" else ())
        assert len(images.plans) == 3


@pytest.mark.anyio
async def test_event_failure_rolls_back_retry_updates(tmp_path: Path) -> None:
    identifiers = _identifiers()
    images = _Acquirer({IMAGE_URL: _Response(b"not an image", "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                *_source_records("run", (ITEM,)),
                RecordDraft(
                    identifier="observation",
                    kind=("carl", "ebay", "listing_observation"),
                    schema_version=1,
                    value={"item_identifier": ITEM},
                ),
                RecordDraft(
                    identifier="reference",
                    kind=("carl", "ebay", "gallery_image_reference"),
                    schema_version=1,
                    value={
                        "item_identifier": ITEM,
                        "observation_record_identifier": "observation",
                        "url": IMAGE_URL,
                    },
                ),
            ),
            identifiers,
        )
        _ = await _enqueue(
            database,
            collect_ebay_image_work(
                identifier="image",
                payload=CollectEbayImagePayload(
                    item_identifier=ITEM,
                    observation_record_identifier="observation",
                    reference_record_identifier="reference",
                    url=IMAGE_URL,
                ),
            ),
            identifiers,
        )
        _ = await _run_next(
            database,
            _registry(database, tmp_path, identifiers, image_acquirer=images),
            identifiers,
            COLLECT_EBAY_IMAGE_WORK_KIND,
        )
        before = await database.work("image")

        def fail() -> str:
            raise RuntimeError("event generation failed")

        with pytest.raises(RuntimeError, match="event generation failed"):
            _ = await retry_ebay_image_failures(database, "run", 1, time_ns(), fail)
        after = await database.work("image")
        assert after == before
