"""Offline resource identity and cross-worker acquisition exclusion."""

from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest

from carl._tests.test_ebay_item_workers import (
    DESCRIPTION_URL,
    ITEM,
    ITEM_URL,
    _Acquirer,
    _enqueue,
    _identifiers,
    _item_html,
    _mapping,
    _registry,
    _Response,
    _retain_records,
    _run_next,
)
from carl.core.acquisition_identity import (
    acquisition_resource_identity,
    ebay_image_identity,
    facebook_image_identity,
)
from carl.core.ebay_items import (
    COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    COLLECT_EBAY_IMAGE_WORK_KIND,
    COLLECT_EBAY_ITEM_WORK_KIND,
    CollectEbayDescriptionPayload,
    CollectEbayImagePayload,
    CollectEbayItemPayload,
    EbayItemRequest,
    collect_ebay_description_work,
    collect_ebay_image_work,
    collect_ebay_item_work,
)
from carl.core.models import RecordDraft
from carl.core.work import ClaimResult, WorkCapability, WorkDefinition
from carl.io.sqlite import Database

EBAY = "https://i.ebayimg.com/images/g/photo-id/s-l1600.jpg"
FACEBOOK = "https://scontent-one.xx.fbcdn.net/v/t39.84726-6/photo.jpg"


def test_recognized_cdn_renditions_ignore_host_but_not_transform() -> None:
    assert ebay_image_identity(EBAY) == ebay_image_identity(EBAY.replace("i.", "thumbs.", 1))
    assert facebook_image_identity(FACEBOOK + "?oh=first&oe=1", "123") == (
        facebook_image_identity(
            FACEBOOK.replace("scontent-one", "scontent-two") + "?oe=2&oh=second", "123"
        )
    )
    assert facebook_image_identity(FACEBOOK, "123") != facebook_image_identity(FACEBOOK, "456")


@pytest.mark.parametrize("suffix", ["?crop=1", "?format=avif", "?unknown=1"])
def test_unknown_query_parameters_are_part_of_image_identity(suffix: str) -> None:
    assert ebay_image_identity(EBAY) != ebay_image_identity(EBAY + suffix)
    assert facebook_image_identity(FACEBOOK) != facebook_image_identity(FACEBOOK + suffix)


def test_rendition_resolution_format_crop_and_duplicate_parameter_order_are_preserved() -> None:
    assert ebay_image_identity(EBAY) != ebay_image_identity(EBAY.replace("1600", "500"))
    assert ebay_image_identity(EBAY) != ebay_image_identity(EBAY.replace("jpg", "webp"))
    assert facebook_image_identity(FACEBOOK + "?stp=crop-one") != facebook_image_identity(
        FACEBOOK + "?stp=crop-two"
    )
    for identify, url in ((ebay_image_identity, EBAY), (facebook_image_identity, FACEBOOK)):
        assert identify(url + "?fit=one&fit=two") != identify(url + "?fit=two&fit=one")
        assert identify(url + "?a=1&b=2") == identify(url + "?b=2&a=1")


@pytest.mark.parametrize(
    "url",
    [
        "https://i.ebayimg.com/unknown/photo.jpg",
        "https://i.ebayimg.com.evil.invalid/images/g/photo-id/s-l1600.jpg",
        "https://i.ebayimg.com:123/images/g/photo-id/s-l1600.jpg",
        "http://i.ebayimg.com/images/g/photo-id/s-l1600.jpg",
    ],
)
def test_unknown_or_unapproved_images_fall_back_to_exact_url(url: str) -> None:
    assert ebay_image_identity(url)[2] == "exact_url"
    assert ebay_image_identity(url) != ebay_image_identity(EBAY)


def test_resource_identity_excludes_evidence_and_item_image_budget() -> None:
    item_kind = ("carl", "ebay", "collect", "item")
    assert acquisition_resource_identity(
        item_kind, {"request": {"item_identifier": "123456789", "maximum_images": 0}}
    ) == acquisition_resource_identity(
        item_kind, {"request": {"item_identifier": "123456789", "maximum_images": 20}}
    )
    first = CollectEbayImagePayload(
        item_identifier="123456789",
        observation_record_identifier="one",
        reference_record_identifier="ref-one",
        url=EBAY,
    )
    second = first.model_copy(
        update={"observation_record_identifier": "two", "url": EBAY.replace("i.", "thumbs.", 1)}
    )
    assert acquisition_resource_identity(
        COLLECT_EBAY_IMAGE_WORK_KIND, first.as_json()
    ) == acquisition_resource_identity(COLLECT_EBAY_IMAGE_WORK_KIND, second.as_json())


@pytest.mark.anyio
async def test_same_resource_claims_are_exclusive_but_other_images_stay_parallel(
    tmp_path: Path,
) -> None:
    path = tmp_path / "evidence.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        identifiers = _identifiers()
        for identifier, url in (
            ("one", EBAY),
            ("two", EBAY.replace("i.", "thumbs.", 1)),
            ("other", EBAY.replace("photo-id", "another")),
        ):
            await _enqueue(
                database,
                collect_ebay_image_work(
                    identifier=identifier,
                    payload=CollectEbayImagePayload(
                        item_identifier="123456789",
                        observation_record_identifier=identifier,
                        reference_record_identifier=f"ref-{identifier}",
                        url=url,
                    ),
                ),
                identifiers,
            )
        # Independent runtimes must observe the other runtime's live lease.
        # Claims are sequential here, but their lease lifetimes overlap.
        async with Database.managed(path) as second_database:
            claims: list[ClaimResult] = []

            async def claim(selected_database: Database, identifier: str | None) -> None:
                claims.append(
                    await selected_database.claim_work(
                        supported_capabilities=(
                            WorkCapability(
                                kind=COLLECT_EBAY_IMAGE_WORK_KIND, payload_schema_version=1
                            ),
                        ),
                        worker_identifier=identifier or "unrestricted",
                        lease_token=str(uuid4()),
                        lease_duration_ns=60_000_000_000,
                        utc_now_ns=time_ns,
                        event_identifier=str(uuid4()),
                        eligible_identifiers=(identifier,) if identifier is not None else None,
                    )
                )

            await claim(database, "one")
            await claim(second_database, "two")
            assert sum(claim.lease is not None for claim in claims) == 1
            # A duplicate at the queue head must not hide unrelated images
            # from a normal unrestricted worker claim.
            await claim(second_database, None)
            assert claims[-1].lease is not None
            assert claims[-1].lease.work_item_identifier == "other"


@pytest.mark.anyio
@pytest.mark.parametrize("source", ["item", "description"])
async def test_reused_wrapper_cannot_make_an_old_acquisition_fresh(
    tmp_path: Path, source: str
) -> None:
    identifiers = _identifiers()
    pages = _Acquirer(
        {
            ITEM_URL: _Response(_item_html()),
            DESCRIPTION_URL: _Response(b"<html><p>Seller description.</p></html>"),
        }
    )
    kind = COLLECT_EBAY_ITEM_WORK_KIND if source == "item" else COLLECT_EBAY_DESCRIPTION_WORK_KIND

    def definition(index: int) -> WorkDefinition:
        request = EbayItemRequest(item_identifier=ITEM, maximum_images=(0, 20, 1)[index])
        if source == "item":
            return collect_ebay_item_work(
                identifier=identifiers(), payload=CollectEbayItemPayload(request=request)
            )
        return collect_ebay_description_work(
            identifier=identifiers(),
            payload=CollectEbayDescriptionPayload(
                request=request,
                observation_record_identifier=f"observation-{index}",
                url=DESCRIPTION_URL,
            ),
        )

    async with Database.managed(tmp_path / "evidence.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        if source == "description":
            await _retain_records(
                database,
                tuple(
                    RecordDraft(
                        identifier=f"observation-{index}",
                        kind=("carl", "ebay", "listing_observation"),
                        schema_version=1,
                        value={
                            "item_identifier": ITEM,
                            "description_url": DESCRIPTION_URL,
                            "acquisition_record_identifier": "old-parent"
                            if index < 2
                            else "new-parent",
                        },
                    )
                    for index in range(3)
                ),
                identifiers,
            )
        await _enqueue(database, definition(0), identifiers)
        await _enqueue(database, definition(1), identifiers)
        original = await _run_next(database, registry, identifiers, kind)
        assert _mapping(original["result"])["reused"] is False
        # This fresh intent is accepted after the original fetch, but before a
        # queued wrapper completes by reusing that original acquisition.
        await _enqueue(database, definition(2), identifiers)
        wrapper = await _run_next(database, registry, identifiers, kind)
        assert _mapping(wrapper["result"])["reused"] is True
        fresh = await _run_next(database, registry, identifiers, kind)
        assert _mapping(fresh["result"])["reused"] is False
        assert len(pages.plans) == 2
