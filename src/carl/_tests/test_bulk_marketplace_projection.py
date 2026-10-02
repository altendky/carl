"""Offline parity and bounded-query tests for eBay bulk-review evidence reads."""

# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl._tests.test_ebay_analysis import ITEM, _application, _seed
from carl._tests.test_marketplace_listing import _publish, _record
from carl._tests.test_mixed_workspace import _complete
from carl.core.composed_projection import GetComposedListingRequest
from carl.core.review import RequestAnalysisRequest
from carl.ebay_analysis_workers import request_ebay_listing_analysis
from carl.io.sqlite import Database
from carl.marketplace_projection import _boundary, _ebay_bulk_evidence, _ebay_projection


@pytest.mark.anyio
async def test_bulk_ebay_histories_preserve_scalar_gallery_sale_and_revision_parity(
    tmp_path: Path,
) -> None:
    items = ("256000000001", "256000000002")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, (_record("older-acquisition", "acquisition"),))
        await _publish(database, (_record("newer-acquisition", "acquisition"),))
        await _publish(
            database,
            (
                _record("run", "search_run"),
                *(
                    record
                    for item in items
                    for record in (
                        _record(
                            f"sale-{item}",
                            "search_listing_occurrence",
                            item_identifier=item,
                            search_run_record_identifier="run",
                            acquisition_record_identifier="older-acquisition",
                            listing_state="sold",
                            sold_price="$40.00",
                            sold_date="2026-09-29",
                        ),
                        _record(
                            f"detail-{item}",
                            "listing_observation",
                            item_identifier=item,
                            classification="detail",
                            title=f"Scope {item}",
                            acquisition_record_identifier="newer-acquisition",
                            displayed_price="$50.00",
                            gallery_urls=[f"https://images/{item}.jpg"],
                            description_url=f"https://description/{item}",
                        ),
                        # Followup grouping must not depend on optional item_identifier values.
                        _record(
                            f"reference-{item}",
                            "gallery_image_reference",
                            observation_record_identifier=f"detail-{item}",
                            url=f"https://images/{item}.jpg",
                        ),
                        _record(
                            f"image-{item}",
                            "image_result",
                            observation_record_identifier=f"detail-{item}",
                            reference_record_identifier=f"reference-{item}",
                            url=f"https://images/{item}.jpg",
                            state="saved",
                            sha256=f"hash-{item}",
                        ),
                        _record(
                            f"description-{item}",
                            "description_result",
                            observation_record_identifier=f"detail-{item}",
                            state="saved",
                            description=f"Seller description {item}",
                        ),
                    )
                ),
                *(
                    _record(
                        f"active-{item}-{index}",
                        "search_listing_occurrence",
                        item_identifier=item,
                        search_run_record_identifier="run",
                        acquisition_record_identifier="newer-acquisition",
                        listing_state="active",
                        title=f"Card {item}",
                        displayed_price="$50.00",
                    )
                    for item in items
                    for index in range(105)
                ),
                # Offline re-extraction of old acquisition must not displace the newer detail.
                *(
                    _record(
                        f"old-detail-{item}-{index}",
                        "listing_observation",
                        item_identifier=item,
                        classification="detail",
                        title="Old title",
                        acquisition_record_identifier="older-acquisition",
                    )
                    for item in items
                    for index in range(105)
                ),
            ),
        )
        as_of = await database.current_completion_boundary()
        boundary = await _boundary(database)
        prefetched = await _ebay_bulk_evidence(
            database, items, ("run",), boundary=boundary, as_of=as_of
        )
        for item in items:
            assert len(prefetched[item].observations) == 101
            assert len(prefetched[item].cards) == 101
            assert len(prefetched[item].sold_cards) == 1
            request = GetComposedListingRequest(
                listing_identifier=f"ebay:{item}", maximum_gallery_images=0, maximum_analyses=0
            )
            scalar = await _ebay_projection(
                database, request, ("run",), boundary=boundary, as_of=as_of
            )
            bulk = await _ebay_projection(
                database,
                request,
                ("run",),
                boundary=boundary,
                as_of=as_of,
                prefetched=prefetched[item],
            )
            assert bulk == scalar
            assert bulk.title and bulk.title.value == f"Scope {item}"
            assert bulk.description and bulk.description.value == f"Seller description {item}"
            assert bulk.gallery and bulk.gallery.saved_image_count == 1
            assert bulk.gallery.images == ()
            assert bulk.last_sale is not None
            assert "item_observation_history_truncated" in bulk.warnings


@pytest.mark.anyio
async def test_bulk_ebay_keeps_hidden_analysis_revision_metadata(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database)
        app, guide = await _application(database, tmp_path)
        requested = await request_ebay_listing_analysis(
            app,
            RequestAnalysisRequest(
                listing_observation_record_identifier="observation",
                product_guide_record_identifier=guide,
            ),
        )
        await _complete(
            database,
            requested.work_identifier,
            records=(_record("analysis", "item_analysis", state="completed", warnings=[]),),
            result={"state": "completed"},
        )
        as_of = await database.current_completion_boundary()
        boundary = await _boundary(database)
        prefetched = await _ebay_bulk_evidence(
            database, (ITEM,), (), boundary=boundary, as_of=as_of
        )
        request = GetComposedListingRequest(
            listing_identifier=f"ebay:{ITEM}",
            product_guide_record_identifier=guide,
            maximum_gallery_images=0,
            maximum_analyses=0,
        )
        scalar = await _ebay_projection(database, request, (), boundary=boundary, as_of=as_of)
        bulk = await _ebay_projection(
            database, request, (), boundary=boundary, as_of=as_of, prefetched=prefetched[ITEM]
        )
        assert len(prefetched[ITEM].analyses) == 1
        assert bulk == scalar
        assert bulk.analyses == () and bulk.analyses_truncated


@pytest.mark.anyio
async def test_bulk_ebay_reads_missing_image_work_state_in_one_batch(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _seed(database)
        app, guide = await _application(database, tmp_path)
        requested = await request_ebay_listing_analysis(
            app,
            RequestAnalysisRequest(
                listing_observation_record_identifier="observation",
                product_guide_record_identifier=guide,
            ),
        )
        pending_url = "https://images/pending.jpg"
        await _publish(
            database,
            (
                _record(
                    "reprocessed-detail",
                    "listing_observation",
                    item_identifier=ITEM,
                    classification="detail",
                    acquisition_record_identifier="acquisition",
                    gallery_urls=[pending_url],
                ),
                _record(
                    "pending-reference",
                    "gallery_image_reference",
                    observation_record_identifier="reprocessed-detail",
                    url=pending_url,
                    work_identifier=requested.work_identifier,
                ),
            ),
        )
        as_of = await database.current_completion_boundary()
        boundary = await _boundary(database)
        prefetched = await _ebay_bulk_evidence(
            database, (ITEM,), (), boundary=boundary, as_of=as_of
        )
        request = GetComposedListingRequest(listing_identifier=f"ebay:{ITEM}")
        scalar = await _ebay_projection(database, request, (), boundary=boundary, as_of=as_of)
        bulk = await _ebay_projection(
            database, request, (), boundary=boundary, as_of=as_of, prefetched=prefetched[ITEM]
        )
        assert prefetched[ITEM].work_states[requested.work_identifier] == "pending"
        assert bulk == scalar
        assert bulk.gallery and bulk.gallery.images[0].descriptor.download_state == "pending"


@pytest.mark.anyio
async def test_bulk_ebay_prefetch_queries_are_chunk_bounded_and_snapshot_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import carl.marketplace_projection as projection_module

    items = tuple(str(256000000000 + index) for index in range(100))
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            tuple(
                _record(
                    f"detail-{item}",
                    "listing_observation",
                    item_identifier=item,
                    classification="detail",
                    title="Before",
                    gallery_urls=[],
                )
                for item in items
            ),
        )
        as_of = await database.current_completion_boundary()
        boundary = await _boundary(database)
        await _publish(
            database,
            tuple(
                _record(
                    f"later-{item}",
                    "listing_observation",
                    item_identifier=item,
                    classification="detail",
                    title="After",
                    gallery_urls=[],
                )
                for item in items
            ),
        )
        original = projection_module._records
        read_count = 0

        async def counted(*args: object, **kwargs: object) -> tuple[projection_module._Record, ...]:
            nonlocal read_count
            read_count += 1
            return await original(*args, **kwargs)  # pyright: ignore[reportArgumentType]

        monkeypatch.setattr(projection_module, "_records", counted)
        prefetched = await _ebay_bulk_evidence(database, items, (), boundary=boundary, as_of=as_of)
        assert read_count == 6  # 3 histories plus 3 followup batches; independent of item count.

        async def no_reads(
            *args: object, **kwargs: object
        ) -> tuple[projection_module._Record, ...]:
            raise AssertionError("Composition must not query retained evidence per item")

        monkeypatch.setattr(projection_module, "_records", no_reads)
        monkeypatch.setattr(projection_module, "_analyses", no_reads)
        for item in items:
            projected = await _ebay_projection(
                database,
                GetComposedListingRequest(listing_identifier=f"ebay:{item}"),
                (),
                boundary=boundary,
                as_of=as_of,
                prefetched=prefetched[item],
            )
            assert projected.title and projected.title.value == "Before"
