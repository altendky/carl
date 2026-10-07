"""Offline acquisition sharing keeps separate Facebook gallery evidence edges."""

# pyright: reportPrivateUsage=false

import gc
from collections.abc import Iterator
from pathlib import Path
from time import time_ns

import pytest

from carl._tests.test_ebay_item_workers import (
    _Acquirer,
    _async_provenance,
    _enqueue,
    _identifiers,
    _mapping,
    _png,
    _Response,
    _retain_records,
    _run_next,
)
from carl._tests.test_facebook import HTML
from carl._tests.test_pipeline_search_occurrences import _attempt
from carl.core.facebook import FacebookItemResponseKind
from carl.core.facebook_images import (
    COLLECT_IMAGE_WORK_KIND,
    CollectImagePayload,
    GalleryImageReference,
    SavedImageCandidate,
    collect_image_work,
    plan_image_followups,
)
from carl.core.facebook_listing import (
    REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
    RequestFacebookListingDetailsPayload,
    request_facebook_listing_details_work,
)
from carl.core.facebook_refresh import RefreshSearchPayload
from carl.core.facebook_work import (
    COLLECT_ITEM_WORK_KIND,
    EXTRACT_ITEM_WORK_KIND,
    CollectItemPayload,
    SuccessfulItemPageResult,
    collect_item_work,
)
from carl.core.http import RequestPlan
from carl.core.models import RecordDraft
from carl.core.worker import CompletedWork
from carl.facebook_image_workers import (
    ImageWorkerDependencies,
    build_image_worker_registry,
    publish_shared_image_reuses,
)
from carl.facebook_listing_workers import (
    FacebookListingWorkerDependencies,
    build_facebook_listing_worker_registry,
)
from carl.facebook_refresh_workers import RefreshWorkerDependencies, _image_phase
from carl.facebook_workers import FacebookWorkerDependencies, build_facebook_worker_registry
from carl.io.image_files import ImageFileStore
from carl.io.sqlite import Database
from carl.io.worker import WorkHandlerRegistry

URL = "https://scontent-lga3-1.xx.fbcdn.net/v/t39.84726-6/photo.jpg?oh=one&oe=abc&stp=c0.0.100.100a"
OTHER = (
    URL.replace("scontent-lga3-1", "scontent-lhr8-2")
    .replace("oh=one", "oh=two")
    .replace("oe=abc", "oe=def")
)


@pytest.fixture
def _disable_automatic_gc() -> Iterator[None]:
    """Stale SQLite cursors must not depend on GC to release their snapshots."""
    enabled = gc.isenabled()
    gc.disable()
    try:
        yield
    finally:
        if enabled:
            gc.enable()


def _reference(url: str, listing: str = "123") -> GalleryImageReference:
    return GalleryImageReference(
        listing_id=listing,
        listing_observation_record_identifier=f"observation-{listing}",
        acquisition_record_identifier=f"acquisition-{listing}",
        block_index=0,
        json_path=("images", 0),
        original_url=url,
        photo_id="photo-1",
        gallery_order=0,
        declared_width=4,
        declared_height=3,
    )


def test_cdn_signature_equivalence_preserves_crop_and_unknown_parameters() -> None:
    first, second = _reference(URL), _reference(OTHER, "456")
    assert first.rendition_identity == second.rendition_identity
    assert plan_image_followups((first, second), (), 1).download_groups == ((0, 1),)
    saved = SavedImageCandidate(
        image_result_record_identifier="saved",
        source_photo_id="photo-1",
        original_url=URL,
        width=4,
        height=3,
    )
    assert plan_image_followups((second,), (saved,), 0).reuse_decisions
    for url in (
        OTHER.replace("c0.0.100.100a", "c10.0.100.100a"),
        OTHER + "&unknown=1",
        OTHER.replace("photo.jpg", "different.jpg"),
    ):
        reference = _reference(url)
        assert reference.rendition_identity != first.rendition_identity
        assert not plan_image_followups((reference,), (saved,), 0).reuse_decisions
        assert plan_image_followups((reference,), (saved,), 1).download_groups == ((0,),)


@pytest.mark.anyio
async def test_late_planned_image_reuses_saved_cdn_variant_with_own_reference(
    tmp_path: Path,
) -> None:
    identifiers = _identifiers()
    first, second = _reference(URL), _reference(OTHER, "456")
    images = _Acquirer({URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            tuple(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=reference.model_dump(mode="json"),
                )
                for identifier, reference in (
                    ("reference-first", first),
                    ("reference-second", second),
                )
            ),
            identifiers,
        )
        registry = build_image_worker_registry(
            ImageWorkerDependencies(database, images, ImageFileStore(tmp_path), identifiers)
        )
        # Different routes deliberately retain distinct work, both planned before the first result.
        for identifier, reference, route in (("first", first, "one"), ("second", second, "two")):
            await _enqueue(
                database,
                collect_image_work(
                    identifier=identifier,
                    payload=CollectImagePayload(
                        reference_record_identifier=f"reference-{identifier}",
                        reference=reference,
                        request_plan=RequestPlan(
                            url=reference.original_url,
                            routing=("decodo", "personal", route),
                            follow_redirects=False,
                        ),
                    ),
                    not_before_utc_ns=0,
                ),
                identifiers,
            )
        completed_first = await _run_next(database, registry, identifiers, COLLECT_IMAGE_WORK_KIND)
        completed_second = await _run_next(database, registry, identifiers, COLLECT_IMAGE_WORK_KIND)
        assert len(images.plans) == 1
        first_result = _mapping(completed_first["result"])["image_result_record_identifier"]
        second_result = _mapping(completed_second["result"])["image_result_record_identifier"]
        assert isinstance(first_result, str) and isinstance(second_result, str)
        assert first_result != second_result
        _, _, value = await database.get_record(second_result)
        assert _mapping(value)["image_reference_record_identifier"] == "reference-second"
        assert _mapping(value)["reused_image_result_record_identifier"] == first_result
        assert set(
            await database.resolved_facebook_image_results_by_reference(
                ("reference-first", "reference-second")
            )
        ) == {"reference-first", "reference-second"}


@pytest.mark.anyio
async def test_active_shared_download_binds_each_consuming_reference(tmp_path: Path) -> None:
    identifiers = _identifiers()
    first, second = _reference(URL), _reference(URL, "456")
    images = _Acquirer({URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            tuple(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=reference.model_dump(mode="json"),
                )
                for identifier, reference in (
                    ("reference-first", first),
                    ("reference-second", second),
                )
            ),
            identifiers,
        )
        jobs = []
        for identifier, reference in (("first", first), ("second", second)):
            jobs.append(
                await _enqueue(
                    database,
                    collect_image_work(
                        identifier=identifier,
                        payload=CollectImagePayload(
                            reference_record_identifier=f"reference-{identifier}",
                            reference=reference,
                            request_plan=RequestPlan(
                                url=reference.original_url,
                                routing=("decodo", "personal", "images"),
                                follow_redirects=False,
                            ),
                        ),
                        not_before_utc_ns=0,
                    ),
                    identifiers,
                )
            )
        assert jobs[0] == jobs[1]
        registry = build_image_worker_registry(
            ImageWorkerDependencies(database, images, ImageFileStore(tmp_path), identifiers)
        )
        await _run_next(database, registry, identifiers, COLLECT_IMAGE_WORK_KIND)
        context = await _attempt(database, "consumer", "facebook", work_kind=("test", "consumer"))
        for _ in range(2):
            await publish_shared_image_reuses(
                database=database,
                context=context,
                reference_identifiers=("reference-second",),
                checkpoint={"stage": "complete"},
                utc_now_ns=time_ns,
            )
        resolved = await database.resolved_facebook_image_results_by_reference(
            ("reference-first", "reference-second")
        )
        assert resolved["reference-first"][0] == resolved["reference-second"][0]
        assert len(await database.facebook_image_reuse_resolutions(("reference-second",))) == 1
        assert len(images.plans) == 1


@pytest.mark.anyio
async def test_fresh_signed_alias_is_not_attached_to_expired_url_work(tmp_path: Path) -> None:
    identifiers = _identifiers()
    first, second = _reference(URL), _reference(OTHER, "456")
    images = _Acquirer(
        {
            URL: _Response(b"expired signed URL", "text/html", status_code=403),
            OTHER: _Response(_png(), "image/png"),
        }
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            tuple(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=reference.model_dump(mode="json"),
                )
                for identifier, reference in (
                    ("reference-first", first),
                    ("reference-second", second),
                )
            ),
            identifiers,
        )
        jobs = []
        for identifier, reference in (("first", first), ("second", second)):
            jobs.append(
                await _enqueue(
                    database,
                    collect_image_work(
                        identifier=identifier,
                        payload=CollectImagePayload(
                            reference_record_identifier=f"reference-{identifier}",
                            reference=reference,
                            request_plan=RequestPlan(
                                url=reference.original_url,
                                routing=("decodo", "personal", "images"),
                                follow_redirects=False,
                            ),
                        ),
                        not_before_utc_ns=0,
                    ),
                    identifiers,
                )
            )
        assert jobs[0] != jobs[1]
        registry = build_image_worker_registry(
            ImageWorkerDependencies(database, images, ImageFileStore(tmp_path), identifiers)
        )
        failed = await _run_next(database, registry, identifiers, COLLECT_IMAGE_WORK_KIND)
        assert failed["state"] == "terminal_failure"
        fresh = await _run_next(database, registry, identifiers, COLLECT_IMAGE_WORK_KIND)
        assert fresh["state"] == "completed"
        assert [plan.url for plan in images.plans] == [URL, OTHER]
        result_identifier = _mapping(fresh["result"])["image_result_record_identifier"]
        assert isinstance(result_identifier, str)
        _, _, value = await database.get_record(result_identifier)
        assert _mapping(value)["image_reference_record_identifier"] == "reference-second"
        assert set(
            await database.resolved_facebook_image_results_by_reference(
                ("reference-first", "reference-second")
            )
        ) == {"reference-second"}


@pytest.mark.anyio
async def test_item_dispatch_reuses_overlap_but_later_refresh_fetches_again(tmp_path: Path) -> None:
    identifiers = _identifiers()
    url = "https://www.facebook.com/marketplace/item/123/"
    pages = _Acquirer({url: _Response(HTML.encode())})
    payload = CollectItemPayload(
        listing_id="123", request_plan=RequestPlan(url=url, routing=("decodo", "personal", "items"))
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(database, pages, identifiers)
        )
        for identifier in ("first", "overlap"):
            # Simulate legacy identities so the dispatch-time race guard is exercised.
            definition = collect_item_work(
                identifier=identifier, payload=payload, not_before_utc_ns=0
            ).model_copy(update={"deduplication_identity": (identifier,)})
            await _enqueue(database, definition, identifiers)
        first = await _run_next(database, registry, identifiers, COLLECT_ITEM_WORK_KIND)
        overlap = await _run_next(database, registry, identifiers, COLLECT_ITEM_WORK_KIND)
        assert len(pages.plans) == 1
        assert _mapping(overlap["result"])["reused_acquisition"] is True
        assert (
            _mapping(first["result"])["acquisition_record_identifier"]
            == _mapping(overlap["result"])["acquisition_record_identifier"]
        )
        await _run_next(database, registry, identifiers, EXTRACT_ITEM_WORK_KIND)
        await _enqueue(
            database,
            collect_item_work(identifier="later-refresh", payload=payload, not_before_utc_ns=0),
            identifiers,
        )
        later = await _run_next(database, registry, identifiers, COLLECT_ITEM_WORK_KIND)
        assert later["state"] == "completed"
        assert len(pages.plans) == 2


@pytest.mark.anyio
async def test_item_dispatch_does_not_reuse_completed_challenge_response(tmp_path: Path) -> None:
    identifiers = _identifiers()
    url = "https://www.facebook.com/marketplace/item/123/"
    pages = _Acquirer({url: _Response(b'<html><form id="login_form"></form></html>')})
    payload = CollectItemPayload(
        listing_id="123", request_plan=RequestPlan(url=url, routing=("decodo", "personal", "items"))
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(database, pages, identifiers)
        )
        for identifier in ("blocked", "overlap"):
            await _enqueue(
                database,
                collect_item_work(
                    identifier=identifier, payload=payload, not_before_utc_ns=0
                ).model_copy(update={"deduplication_identity": (identifier,)}),
                identifiers,
            )
        blocked = await _run_next(database, registry, identifiers, COLLECT_ITEM_WORK_KIND)
        assert blocked["state"] == "completed"
        pages.responses[url] = _Response(HTML.encode())
        overlap = await _run_next(database, registry, identifiers, COLLECT_ITEM_WORK_KIND)
        assert overlap["state"] == "completed"
        assert len(pages.plans) == 2
        assert not _mapping(overlap["result"]).get("reused_acquisition")


@pytest.mark.anyio
@pytest.mark.parametrize("damage", ("missing", "corrupt"))
async def test_invalid_saved_image_is_refetched_not_reported_as_cached_success(
    tmp_path: Path,
    damage: str,
) -> None:
    identifiers = _identifiers()
    reference = _reference(URL)
    images = _Acquirer({URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier="reference",
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=reference.model_dump(mode="json"),
                ),
                RecordDraft(
                    identifier="reference-consumer",
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=_reference(OTHER, "456").model_dump(mode="json"),
                ),
            ),
            identifiers,
        )
        registry = build_image_worker_registry(
            ImageWorkerDependencies(database, images, ImageFileStore(tmp_path), identifiers)
        )
        payload = CollectImagePayload(
            reference_record_identifier="reference",
            reference=reference,
            request_plan=RequestPlan(
                url=URL, routing=("decodo", "personal", "images"), follow_redirects=False
            ),
        )
        await _enqueue(
            database,
            collect_image_work(identifier="first", payload=payload, not_before_utc_ns=0),
            identifiers,
        )
        first = await _run_next(database, registry, identifiers, COLLECT_IMAGE_WORK_KIND)
        result_identifier = _mapping(first["result"])["image_result_record_identifier"]
        assert isinstance(result_identifier, str)
        _, _, saved = await database.get_record(result_identifier)
        locator = _mapping(saved)["image_file_locator"]
        assert isinstance(locator, str)
        path = tmp_path / locator
        if damage == "missing":
            path.unlink()
        else:
            path.write_bytes(b"corrupt cached bytes")
        context = await _attempt(database, "consumer", "facebook", work_kind=("test", "consumer"))
        # The shared-result path must not publish a reuse edge to damaged bytes either.
        await publish_shared_image_reuses(
            database=database,
            context=context,
            reference_identifiers=("reference-consumer",),
            checkpoint={"stage": "complete"},
            utc_now_ns=time_ns,
        )
        assert not await database.facebook_image_reuse_resolutions()
        await _enqueue(
            database,
            collect_image_work(identifier="retry", payload=payload, not_before_utc_ns=0),
            identifiers,
        )
        retried = await _run_next(database, registry, identifiers, COLLECT_IMAGE_WORK_KIND)
        assert len(images.plans) == 2
        assert not _mapping(retried["result"]).get("reused_image_result_record_identifier")
        if damage == "missing":
            assert retried["state"] == "completed"
            assert _mapping(retried["result"])["state"] == "saved"
        else:
            # The storage layer rejects replacement of an existing corrupt file.
            assert retried["state"] == "terminal_failure"
            assert _mapping(retried["result"])["state"] == "failed"


@pytest.mark.anyio
@pytest.mark.usefixtures("_disable_automatic_gc")
async def test_listing_details_plan_refetches_missing_saved_gallery_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("carl.facebook_listing_workers._WAIT_NS", 0)
    monkeypatch.setattr("carl.facebook_listing_workers.brave_navigation_headers", lambda: ())
    identifiers = _identifiers()
    item_url = "https://www.facebook.com/marketplace/item/123/"
    html = HTML.replace("https://example.invalid/one.jpg", URL).replace(
        "https://example.invalid/two.jpg", OTHER
    )
    pages = _Acquirer({item_url: _Response(html.encode())})
    images = _Acquirer({URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = WorkHandlerRegistry(
            handlers=(
                *build_facebook_listing_worker_registry(
                    FacebookListingWorkerDependencies(database, identifiers)
                ).handlers,
                *build_facebook_worker_registry(
                    FacebookWorkerDependencies(database, pages, identifiers)
                ).handlers,
                *build_image_worker_registry(
                    ImageWorkerDependencies(database, images, ImageFileStore(tmp_path), identifiers)
                ).handlers,
            )
        )
        payload = RequestFacebookListingDetailsPayload(listing_identifier="123", maximum_images=1)
        await _enqueue(
            database,
            request_facebook_listing_details_work(identifier="first", payload=payload),
            identifiers,
        )
        for kind in (
            REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
            COLLECT_ITEM_WORK_KIND,
            EXTRACT_ITEM_WORK_KIND,
            REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
            REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
            REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
            COLLECT_IMAGE_WORK_KIND,
            REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
        ):
            await _run_next(database, registry, identifiers, kind)
        saved = await database.saved_facebook_image_results()
        assert len(saved) == 1
        locator = saved[0][1]["image_file_locator"]
        assert isinstance(locator, str)
        (tmp_path / locator).unlink()
        await _enqueue(
            database,
            request_facebook_listing_details_work(identifier="second", payload=payload),
            identifiers,
        )
        collecting = {}
        for _ in range(3):
            collecting = await _run_next(
                database, registry, identifiers, REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND
            )
        assert collecting["state"] == "pending"
        assert _mapping(collecting["result"])["reused_images"] == 0
        await _run_next(database, registry, identifiers, COLLECT_IMAGE_WORK_KIND)
        finished = await _run_next(
            database, registry, identifiers, REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND
        )
        assert finished["state"] == "completed"
        assert len(pages.plans) == 1 and len(images.plans) == 2


@pytest.mark.anyio
async def test_refresh_cached_exact_url_preserves_new_observation_gallery_edge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifiers = _identifiers()
    original = _reference(URL)
    images = _Acquirer({URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier="old-reference",
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=original.model_dump(mode="json"),
                ),
            ),
            identifiers,
        )
        registry = build_image_worker_registry(
            ImageWorkerDependencies(database, images, ImageFileStore(tmp_path), identifiers)
        )
        await _enqueue(
            database,
            collect_image_work(
                identifier="image",
                payload=CollectImagePayload(
                    reference_record_identifier="old-reference",
                    reference=original,
                    request_plan=RequestPlan(
                        url=URL, routing=("decodo", "personal", "images"), follow_redirects=False
                    ),
                ),
                not_before_utc_ns=0,
            ),
            identifiers,
        )
        image = await _run_next(database, registry, identifiers, COLLECT_IMAGE_WORK_KIND)
        image_result_identifier = _mapping(image["result"])["image_result_record_identifier"]
        await _retain_records(
            database,
            (
                *(
                    RecordDraft(
                        identifier=identifier,
                        kind=("carl", "facebook", "search_run"),
                        schema_version=1,
                        value={"traversal": {"unique_listing_identifiers": ["123"]}},
                    )
                    for identifier in ("base", "refreshed")
                ),
                RecordDraft(
                    identifier="new-observation",
                    kind=("carl", "facebook", "listing_observation"),
                    schema_version=1,
                    value={
                        "listing_id": "123",
                        "images": [
                            {
                                "original_url": URL,
                                "role": "listing_gallery",
                                "gallery_order": 0,
                                "photo_id": "photo-1",
                                "declared_dimensions": {"width": 4, "height": 3},
                                "source": {
                                    "acquisition_record_id": "new-acquisition",
                                    "block_index": 0,
                                    "json_path": ["images", 0],
                                },
                            }
                        ],
                    },
                ),
            ),
            identifiers,
        )
        successful = SuccessfulItemPageResult(
            listing_id="123",
            acquisition_record_identifier="new-acquisition",
            observation_record_identifier="new-observation",
            response_classification=FacebookItemResponseKind.FULL_LISTING,
            acquisition_completion_sequence=1,
            extraction_completion_sequence=2,
        )

        async def retained_results(_identifiers):
            return (successful,)

        monkeypatch.setattr(database, "successful_facebook_item_page_results", retained_results)
        context = await _attempt(database, "refresh", "facebook", work_kind=("test", "refresh"))
        payload = RefreshSearchPayload.model_validate_json("""{
            "base_search_run_record_identifier":"base", "search_work_identifier":"search",
            "search":{"request":{"query":"telescope","location":{"kind":"facebook_location","identifier":"123"},"radius":{"value":60,"unit":"miles"}},"traversal":{"maximum_pages":1},"routing":["decodo","personal","search"]},
            "item_routing":["decodo","personal","items"], "image_routing":["decodo","personal","images"]
        }""")
        dependencies = RefreshWorkerDependencies(
            database, identifiers, code_provenance=_async_provenance
        )
        for _ in range(2):
            outcome = await _image_phase(
                payload,
                {"refreshed_search_run_record_identifier": "refreshed", "stage": "items_complete"},
                context,
                dependencies,
            )
            assert isinstance(outcome, CompletedWork)
            assert _mapping(outcome.result)["already_saved_exact_renditions"] == 1
            assert _mapping(outcome.result)["new_image_collections"] == 0
        references = await database.facebook_gallery_reference_identifiers(("new-observation",))
        assert len(references) == 1
        new_reference_identifier = next(iter(references.values()))
        resolved = await database.resolved_facebook_image_results_by_reference(
            (new_reference_identifier,)
        )
        assert resolved[new_reference_identifier][0] == image_result_identifier
        assert (
            len(await database.facebook_image_reuse_resolutions((new_reference_identifier,))) == 1
        )
        assert len(images.plans) == 1
