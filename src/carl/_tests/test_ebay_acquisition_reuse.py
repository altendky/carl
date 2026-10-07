"""Acquisition sharing preserves independent item budgets and observation lineage."""

from pathlib import Path

import pytest

from carl._tests.test_ebay_item_workers import (
    DESCRIPTION_URL,
    IMAGE_URL,
    ITEM,
    ITEM_URL,
    SECOND_ITEM,
    _Acquirer,
    _enqueue,
    _identifiers,
    _item_html,
    _mapping,
    _png,
    _registry,
    _Response,
    _retain_records,
    _run_next,
    _string,
)
from carl.core.ebay_items import (
    COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    COLLECT_EBAY_IMAGE_WORK_KIND,
    COLLECT_EBAY_ITEM_WORK_KIND,
    EXTRACT_EBAY_ITEM_WORK_KIND,
    CollectEbayDescriptionPayload,
    CollectEbayImagePayload,
    CollectEbayItemPayload,
    EbayItemRequest,
    collect_ebay_description_work,
    collect_ebay_image_work,
    collect_ebay_item_work,
)
from carl.core.models import RecordDraft
from carl.core.work import WorkState
from carl.io.sqlite import Database


@pytest.mark.anyio
async def test_queued_item_requests_share_raw_acquisition_before_extraction(
    tmp_path: Path,
) -> None:
    identifiers = _identifiers()
    pages = _Acquirer({ITEM_URL: _Response(_item_html())})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        for maximum_images in (0, 20):
            await _enqueue(
                database,
                collect_ebay_item_work(
                    identifier=identifiers(),
                    payload=CollectEbayItemPayload(
                        request=EbayItemRequest(item_identifier=ITEM, maximum_images=maximum_images)
                    ),
                ),
                identifiers,
            )
        first = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        second = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        first_result, second_result = _mapping(first["result"]), _mapping(second["result"])
        assert first_result["reused"] is False
        assert second_result["reused"] is True
        assert (
            first_result["acquisition_record_identifier"]
            == second_result["acquisition_record_identifier"]
        )
        assert (
            first_result["extraction_work_identifier"]
            != second_result["extraction_work_identifier"]
        )
        assert len(pages.plans) == 1
        for result in (first_result, second_result):
            assert await database.work_state(_string(result["extraction_work_identifier"])) is (
                WorkState.PENDING
            )
        first_extraction = await _run_next(
            database, registry, identifiers, EXTRACT_EBAY_ITEM_WORK_KIND
        )
        second_extraction = await _run_next(
            database, registry, identifiers, EXTRACT_EBAY_ITEM_WORK_KIND
        )
        assert _mapping(first_extraction["result"])["image_work_identifiers"] == []
        assert len(_mapping(second_extraction["result"])["image_work_identifiers"]) == 1


@pytest.mark.anyio
async def test_item_collection_after_prior_completion_still_fetches_fresh_page(
    tmp_path: Path,
) -> None:
    identifiers = _identifiers()
    pages = _Acquirer({ITEM_URL: _Response(_item_html())})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        for maximum_images in (0, 20):
            await _enqueue(
                database,
                collect_ebay_item_work(
                    identifier=identifiers(),
                    payload=CollectEbayItemPayload(
                        request=EbayItemRequest(item_identifier=ITEM, maximum_images=maximum_images)
                    ),
                ),
                identifiers,
            )
            work = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
            assert _mapping(work["result"])["reused"] is False
        assert len(pages.plans) == 2


@pytest.mark.anyio
async def test_challenge_acquisition_is_not_shared_with_another_queued_item(
    tmp_path: Path,
) -> None:
    identifiers = _identifiers()
    pages = _Acquirer({ITEM_URL: _Response(b"<html>Pardon Our Interruption</html>")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        for maximum_images in (0, 20):
            await _enqueue(
                database,
                collect_ebay_item_work(
                    identifier=identifiers(),
                    payload=CollectEbayItemPayload(
                        request=EbayItemRequest(item_identifier=ITEM, maximum_images=maximum_images)
                    ),
                ),
                identifiers,
            )
        for _ in range(2):
            work = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
            assert _mapping(work["result"])["reused"] is False
        assert len(pages.plans) == 2


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("second_url", "shared"),
    (
        ("https://thumbs.ebayimg.com/images/g/item/s-l1600.png", True),
        ("https://i.ebayimg.com/images/g/item/s-l500.png", False),
        ("https://i.ebayimg.com/images/g/item/s-l1600.jpg", False),
        ("https://i.ebayimg.com/images/g/item/s-l1600.png?crop=1", False),
    ),
)
async def test_images_share_cdn_identity_but_not_different_renditions(
    tmp_path: Path, second_url: str, shared: bool
) -> None:
    identifiers = _identifiers()
    images = _Acquirer(
        {
            IMAGE_URL: _Response(_png(), "image/png"),
            second_url: _Response(_png(), "image/png"),
        }
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, image_acquirer=images)
        references = tuple(
            RecordDraft(
                identifier=f"reference-{index}",
                kind=("carl", "ebay", "gallery_image_reference"),
                schema_version=1,
                value={
                    "item_identifier": item,
                    "observation_record_identifier": f"observation-{index}",
                    "url": url,
                    "gallery_order": 0,
                },
            )
            for index, (item, url) in enumerate(((ITEM, IMAGE_URL), (SECOND_ITEM, second_url)))
        )
        await _retain_records(database, references, identifiers)
        for index, (item, url) in enumerate(((ITEM, IMAGE_URL), (SECOND_ITEM, second_url))):
            await _enqueue(
                database,
                collect_ebay_image_work(
                    identifier=identifiers(),
                    payload=CollectEbayImagePayload(
                        item_identifier=item,
                        observation_record_identifier=f"observation-{index}",
                        reference_record_identifier=f"reference-{index}",
                        url=url,
                    ),
                ),
                identifiers,
            )
        await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        second = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        assert _mapping(second["result"])["reused"] is shared
        assert len(images.plans) == (1 if shared else 2)
        results = await database.records_by_kind(("carl", "ebay", "image_result"))
        assert len(results) == 2
        source, own = _mapping(results[0][1]), _mapping(results[1][1])
        assert own["item_identifier"] == SECOND_ITEM
        assert own["observation_record_identifier"] == "observation-1"
        assert own["reference_record_identifier"] == "reference-1"
        assert own["url"] == second_url
        if shared:
            assert own["reused_from_result_record_identifier"] == results[0][0]
            assert own["image_artifact_identifier"] == source["image_artifact_identifier"]
            assert source["url"] == IMAGE_URL
            _, inputs, _ = await database.object_operation_relations(results[1][0])
            assert any(
                edge.name == ("reused_image_result",) and edge.object_identifier == results[0][0]
                for edge in inputs
            )


@pytest.mark.anyio
async def test_queued_descriptions_share_fetch_but_keep_own_observation(tmp_path: Path) -> None:
    identifiers = _identifiers()
    pages = _Acquirer({DESCRIPTION_URL: _Response(b"<html><p>Original seller text.</p></html>")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        await _retain_records(
            database,
            tuple(
                RecordDraft(
                    identifier=f"observation-{index}",
                    kind=("carl", "ebay", "listing_observation"),
                    schema_version=1,
                    value={"item_identifier": ITEM, "description_url": DESCRIPTION_URL},
                )
                for index in range(2)
            ),
            identifiers,
        )
        for index in range(2):
            await _enqueue(
                database,
                collect_ebay_description_work(
                    identifier=identifiers(),
                    payload=CollectEbayDescriptionPayload(
                        request=EbayItemRequest(item_identifier=ITEM),
                        observation_record_identifier=f"observation-{index}",
                        url=DESCRIPTION_URL,
                    ),
                ),
                identifiers,
            )
        await _run_next(database, registry, identifiers, COLLECT_EBAY_DESCRIPTION_WORK_KIND)
        second = await _run_next(
            database, registry, identifiers, COLLECT_EBAY_DESCRIPTION_WORK_KIND
        )
        assert _mapping(second["result"])["reused"] is True
        assert len(pages.plans) == 1
        results = await database.records_by_kind(("carl", "ebay", "description_result"))
        assert len(results) == 2
        source, own = _mapping(results[0][1]), _mapping(results[1][1])
        assert source["observation_record_identifier"] == "observation-0"
        assert own["observation_record_identifier"] == "observation-1"
        assert own["description"] == "Original seller text."
        assert own["reused_from_result_record_identifier"] == results[0][0]
        _, inputs, _ = await database.object_operation_relations(results[1][0])
        assert any(
            edge.name == ("reused_description",) and edge.object_identifier == results[0][0]
            for edge in inputs
        )


@pytest.mark.anyio
@pytest.mark.parametrize("same_acquisition", (True, False))
async def test_later_description_only_reuses_same_parent_item_acquisition(
    tmp_path: Path, same_acquisition: bool
) -> None:
    identifiers = _identifiers()
    pages = _Acquirer({DESCRIPTION_URL: _Response(b"<html><p>Original seller text.</p></html>")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        for index in range(2):
            await _retain_records(
                database,
                (
                    RecordDraft(
                        identifier=f"observation-{index}",
                        kind=("carl", "ebay", "listing_observation"),
                        schema_version=1,
                        value={
                            "item_identifier": ITEM,
                            "description_url": DESCRIPTION_URL,
                            "acquisition_record_identifier": (
                                "item-acquisition-0"
                                if same_acquisition
                                else f"item-acquisition-{index}"
                            ),
                        },
                    ),
                ),
                identifiers,
            )
            await _enqueue(
                database,
                collect_ebay_description_work(
                    identifier=identifiers(),
                    payload=CollectEbayDescriptionPayload(
                        request=EbayItemRequest(item_identifier=ITEM),
                        observation_record_identifier=f"observation-{index}",
                        url=DESCRIPTION_URL,
                    ),
                ),
                identifiers,
            )
            work = await _run_next(
                database, registry, identifiers, COLLECT_EBAY_DESCRIPTION_WORK_KIND
            )
            assert _mapping(work["result"])["reused"] is (index == 1 and same_acquisition)
        assert len(pages.plans) == (1 if same_acquisition else 2)


@pytest.mark.anyio
@pytest.mark.parametrize("source_state", ("failed", "saved"))
async def test_image_does_not_reuse_failed_or_missing_artifact_source(
    tmp_path: Path, source_state: str
) -> None:
    identifiers = _identifiers()
    images = _Acquirer({IMAGE_URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, image_acquirer=images)
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier="source-image",
                    kind=("carl", "ebay", "image_result"),
                    schema_version=1,
                    value={
                        "state": source_state,
                        "url": IMAGE_URL,
                        "image_artifact_identifier": "missing-artifact",
                    },
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
        await _enqueue(
            database,
            collect_ebay_image_work(
                identifier=identifiers(),
                payload=CollectEbayImagePayload(
                    item_identifier=ITEM,
                    observation_record_identifier="observation",
                    reference_record_identifier="reference",
                    url=IMAGE_URL,
                ),
            ),
            identifiers,
        )
        work = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        assert work["state"] == "completed"
        assert _mapping(work["result"])["reused"] is False
        assert len(images.plans) == 1
