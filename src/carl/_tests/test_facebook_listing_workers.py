"""Offline selected-item details share Facebook's existing durable pipeline."""

# Reuse the existing offline test fixtures, which deliberately remain private.
# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl._tests.test_ebay_item_workers import (
    _Acquirer,
    _enqueue,
    _identifiers,
    _mapping,
    _png,
    _Response,
    _run_next,
)
from carl._tests.test_facebook import HTML
from carl.core.facebook_images import COLLECT_IMAGE_WORK_KIND
from carl.core.facebook_listing import (
    REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
    RequestFacebookListingDetailsPayload,
    request_facebook_listing_details_work,
)
from carl.core.facebook_work import COLLECT_ITEM_WORK_KIND, EXTRACT_ITEM_WORK_KIND
from carl.facebook_image_workers import ImageWorkerDependencies, build_image_worker_registry
from carl.facebook_listing_workers import (
    FacebookListingWorkerDependencies,
    build_facebook_listing_worker_registry,
)
from carl.facebook_workers import FacebookWorkerDependencies, build_facebook_worker_registry
from carl.io.image_files import ImageFileStore
from carl.io.sqlite import Database
from carl.io.worker import WorkHandlerRegistry

URL = "https://www.facebook.com/marketplace/item/123/"
IMAGE1 = "https://scontent.xx.fbcdn.net/one.jpg"
IMAGE2 = "https://scontent.xx.fbcdn.net/two.jpg"


@pytest.mark.anyio
@pytest.mark.parametrize("maximum_images", (0, 1, 2))
async def test_selected_listing_acquires_details_and_bounded_gallery_then_reuses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    maximum_images: int,
) -> None:
    monkeypatch.setattr("carl.facebook_listing_workers._WAIT_NS", 0)
    monkeypatch.setattr("carl.facebook_listing_workers.brave_navigation_headers", lambda: ())
    identifiers = _identifiers()
    html = HTML.replace("https://example.invalid/one.jpg", IMAGE1).replace(
        "https://example.invalid/two.jpg", IMAGE2
    )
    pages = _Acquirer({URL: _Response(html.encode())})
    images = _Acquirer(
        {IMAGE1: _Response(_png(), "image/png"), IMAGE2: _Response(_png(), "image/png")}
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:

        def registry() -> WorkHandlerRegistry:
            return WorkHandlerRegistry(
                handlers=(
                    *build_facebook_listing_worker_registry(
                        FacebookListingWorkerDependencies(database, identifiers)
                    ).handlers,
                    *build_facebook_worker_registry(
                        FacebookWorkerDependencies(database, pages, identifiers)
                    ).handlers,
                    *build_image_worker_registry(
                        ImageWorkerDependencies(
                            database, images, ImageFileStore(tmp_path), identifiers
                        )
                    ).handlers,
                )
            )

        payload = RequestFacebookListingDetailsPayload(
            listing_identifier="123", maximum_images=maximum_images
        )
        _ = await _enqueue(
            database,
            request_facebook_listing_details_work(identifier="details-one", payload=payload),
            identifiers,
        )
        for kind in (
            REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
            COLLECT_ITEM_WORK_KIND,
            EXTRACT_ITEM_WORK_KIND,
            REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
            REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
        ):
            _ = await _run_next(database, registry(), identifiers, kind)
        collecting = await _run_next(
            database, registry(), identifiers, REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND
        )
        if maximum_images:
            assert collecting["state"] == "pending"
            for _ in range(maximum_images):
                _ = await _run_next(database, registry(), identifiers, COLLECT_IMAGE_WORK_KIND)
            completed = await _run_next(
                database, registry(), identifiers, REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND
            )
        else:
            completed = collecting
        assert completed["state"] == "completed"
        assert _mapping(completed["result"])["state"] == "completed"
        assert len(pages.plans) == 1
        assert len(images.plans) == maximum_images
        _ = await _enqueue(
            database,
            request_facebook_listing_details_work(identifier="details-two", payload=payload),
            identifiers,
        )
        reused = completed
        for _ in range(3):
            reused = await _run_next(
                database, registry(), identifiers, REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND
            )
        assert reused["state"] == "completed"
        assert _mapping(reused["result"])["reused_item_page"] is True
        assert _mapping(reused["result"])["reused_images"] == maximum_images
        assert len(pages.plans) == 1 and len(images.plans) == maximum_images
        assert not await database.records_by_kind(("carl", "facebook", "search_run"))
        _ = await _enqueue(
            database,
            request_facebook_listing_details_work(
                identifier="details-three", payload=payload.model_copy(update={"refresh": True})
            ),
            identifiers,
        )
        refreshed = await _run_next(
            database, registry(), identifiers, REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND
        )
        assert _mapping(refreshed["result"])["reused_item_page"] is False
        _ = await _run_next(database, registry(), identifiers, COLLECT_ITEM_WORK_KIND)
        assert len(pages.plans) == 2


@pytest.mark.anyio
async def test_selected_listing_unusable_page_is_not_successful(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("carl.facebook_listing_workers._WAIT_NS", 0)
    monkeypatch.setattr("carl.facebook_listing_workers.brave_navigation_headers", lambda: ())
    identifiers = _identifiers()
    pages = _Acquirer({URL: _Response(b'<html><form id="login_form"></form></html>')})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = WorkHandlerRegistry(
            handlers=(
                *build_facebook_listing_worker_registry(
                    FacebookListingWorkerDependencies(database, identifiers)
                ).handlers,
                *build_facebook_worker_registry(
                    FacebookWorkerDependencies(database, pages, identifiers)
                ).handlers,
            )
        )
        _ = await _enqueue(
            database,
            request_facebook_listing_details_work(
                identifier="details",
                payload=RequestFacebookListingDetailsPayload(listing_identifier="123"),
            ),
            identifiers,
        )
        for kind in (
            REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
            COLLECT_ITEM_WORK_KIND,
            EXTRACT_ITEM_WORK_KIND,
        ):
            _ = await _run_next(database, registry, identifiers, kind)
        failed = await _run_next(
            database, registry, identifiers, REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND
        )
        assert failed["state"] == "terminal_failure"
        assert _mapping(failed["error"])["kind"] == "listing_detail_extraction_failed"
