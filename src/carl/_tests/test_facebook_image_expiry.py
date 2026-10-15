"""Expired gallery signatures renew once and never dispatch doomed downloads."""

# Reuse deliberately private worker fixtures.
# pyright: reportPrivateUsage=false

from pathlib import Path
from typing import cast

import pytest

from carl import facebook_refresh_workers
from carl._tests.test_ebay_item_workers import (
    _Acquirer,
    _enqueue,
    _identifiers,
    _Response,
    _retain_records,
    _run_next,
)
from carl._tests.test_ebay_workers import _directories
from carl._tests.test_facebook_images import _png, _reference
from carl._tests.test_pipeline_search_occurrences import _attempt
from carl.core.facebook import FacebookItemResponseKind
from carl.core.facebook_images import (
    COLLECT_IMAGE_WORK_KIND,
    CollectImagePayload,
    SavedImageCandidate,
    collect_image_work,
    facebook_image_url_expiry_utc_ns,
    plan_image_followups,
)
from carl.core.facebook_refresh import REFRESH_SEARCH_WORK_KIND, RefreshSearchPayload
from carl.core.facebook_work import SuccessfulItemPageResult
from carl.core.http import RequestPlan
from carl.core.image_failures import image_acquisition_failure_kind
from carl.core.json import encode_json
from carl.core.models import JsonValue, RecordDraft
from carl.core.refresh_recovery import refresh_failure_summary
from carl.core.worker import AttemptContext, CompletedWork, RetryWork, TerminalFailureWork
from carl.facebook_image_workers import ImageWorkerDependencies, build_image_worker_registry
from carl.facebook_refresh_workers import RefreshWorkerDependencies
from carl.facebook_routed_workers import build_routed_facebook_worker_registry
from carl.io.httpx import Acquisition, IdentifierFactory
from carl.io.image_files import ImageFileStore
from carl.io.sqlite import Database
from carl.review import ReviewApplication

NOW = 1_800_000_000_000_000_000
BASE_URL = "https://scontent.xx.fbcdn.net/photo.jpg?oh=signed"
EXPIRED = BASE_URL + f"&oe={(NOW // 1_000_000_000) - 1:X}"
FRESH = BASE_URL + f"&oe={(NOW // 1_000_000_000) + 100:X}"


@pytest.mark.parametrize(
    ("suffix", "expiry"),
    [
        ("&oe=10", 16_000_000_000),
        ("&oe=ABcd", 0xABCD * 1_000_000_000),
        ("", None),
        ("&oe=", None),
        ("&oe=not-hex", None),
        ("&oe=0x10", None),
        ("&oe=+10", None),
        ("&oe=10&oe=20", None),
        ("&oe=10&oe=10", None),
    ],
)
def test_fbcdn_expiry_parser_is_conservative(suffix: str, expiry: int | None) -> None:
    assert facebook_image_url_expiry_utc_ns(BASE_URL + suffix) == expiry
    assert facebook_image_url_expiry_utc_ns("https://example.com/photo?oe=10") is None


@pytest.mark.parametrize(
    ("url", "expired"),
    [(EXPIRED, True), (FRESH, False), (BASE_URL, False), (BASE_URL + "&oe=bad!", False)],
)
def test_planning_checks_expiry_but_reuses_validated_saved_bytes(url: str, expired: bool) -> None:
    reference = _reference().model_copy(update={"original_url": url})
    plan = plan_image_followups((reference,), (), 1, observed_at_utc_ns=NOW)
    assert plan.download_groups == (() if expired else ((0,),))
    assert plan.expired_reference_indices == ((0,) if expired else ())
    saved = SavedImageCandidate(
        image_result_record_identifier="saved",
        source_photo_id=reference.photo_id,
        original_url=url,
        width=720,
        height=960,
    )
    reused = plan_image_followups((reference,), (saved,), 1, observed_at_utc_ns=NOW)
    assert len(reused.reuse_decisions) == 1
    assert reused.expired_reference_indices == ()
    assert not plan_image_followups(
        (reference,), (), 0, observed_at_utc_ns=NOW
    ).expired_reference_indices


def test_expired_403_uses_request_time_not_current_time() -> None:
    record: dict[str, JsonValue] = {
        "hops": [
            {
                "request": {"url": EXPIRED, "started_at_utc": "2027-01-15T08:00:00+00:00"},
                "response": {"status_code": 403},
            }
        ]
    }
    assert image_acquisition_failure_kind(record) == "expired_url"
    record["hops"][0]["request"]["started_at_utc"] = "2000-01-01T00:00:00+00:00"
    assert image_acquisition_failure_kind(record, observed_at_utc_ns=NOW) == "site_http_403"


class _MustNotAcquire:
    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        pytest.fail("Expired URLs must not reach an image transport")


@pytest.mark.anyio
async def test_worker_rejects_expiry_relative_to_injected_clock_without_network(
    tmp_path: Path,
) -> None:
    identifiers = _identifiers()
    reference = _reference().model_copy(update={"original_url": EXPIRED})
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        registry = build_image_worker_registry(
            ImageWorkerDependencies(
                database,
                _MustNotAcquire(),
                ImageFileStore(tmp_path),
                identifiers,
                utc_now_ns=lambda: NOW,
            )
        )
        payload = CollectImagePayload(
            reference_record_identifier="reference",
            reference=reference,
            request_plan=RequestPlan(
                url=EXPIRED, follow_redirects=False, routing=("proton", "personal", "test")
            ),
        )
        outcome = await registry.handlers[0].execute(
            payload.model_dump(mode="json"),
            AttemptContext(
                work_item_identifier="image",
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="operation",
            ),
        )
        assert isinstance(outcome, TerminalFailureWork)
        assert outcome.error["kind"] == "expired_url"
        assert outcome.result["state"] == "gallery_stale"
        assert not outcome.records


@pytest.mark.anyio
async def test_worker_can_reuse_a_validated_image_after_its_url_expires(tmp_path: Path) -> None:
    identifiers = _identifiers()
    reference = _reference().model_copy(update={"original_url": EXPIRED})
    payload = CollectImagePayload(
        reference_record_identifier="reference",
        reference=reference,
        request_plan=RequestPlan(
            url=EXPIRED, follow_redirects=False, routing=("proton", "personal", "test")
        ),
    )
    acquirer = _Acquirer({EXPIRED: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier="reference",
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=reference.model_dump(mode="json"),
                ),
            ),
            identifiers,
        )
        before_expiry = facebook_image_url_expiry_utc_ns(EXPIRED)
        assert before_expiry is not None
        before_expiry -= 1
        initial_registry = build_image_worker_registry(
            ImageWorkerDependencies(
                database,
                acquirer,
                ImageFileStore(tmp_path),
                identifiers,
                utc_now_ns=lambda: before_expiry,
            )
        )
        await _enqueue(
            database,
            collect_image_work(identifier="image", payload=payload, not_before_utc_ns=0),
            identifiers,
        )
        saved = await _run_next(database, initial_registry, identifiers, COLLECT_IMAGE_WORK_KIND)
        assert saved["state"] == "completed"
        assert len(acquirer.plans) == 1
        registry = build_image_worker_registry(
            ImageWorkerDependencies(
                database,
                _MustNotAcquire(),
                ImageFileStore(tmp_path),
                identifiers,
                utc_now_ns=lambda: NOW,
            )
        )
        reused = await registry.handlers[0].execute(
            payload.model_dump(mode="json"),
            AttemptContext(
                work_item_identifier="late",
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="late-operation",
            ),
        )
        assert isinstance(reused, CompletedWork)
        assert reused.result["state"] == "saved"
        assert reused.result["reused_image_result_record_identifier"]


@pytest.mark.anyio
async def test_routed_worker_skips_expired_url_before_configuration_or_session_setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "carl.facebook_routed_workers.load_configuration",
        lambda *_: pytest.fail("Expired image must not open a routed provider session"),
    )
    expired = BASE_URL + "&oe=1"
    reference = _reference().model_copy(update={"original_url": expired})
    payload = CollectImagePayload(
        reference_record_identifier="reference",
        reference=reference,
        request_plan=RequestPlan(
            url=expired, follow_redirects=False, routing=("proton", "personal", "test")
        ),
    )
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        registry = build_routed_facebook_worker_registry(
            database=database, directories=_directories(tmp_path), new_identifier=_identifiers()
        )
        handler = next(
            handler
            for handler in registry.handlers
            if handler.capability.kind == COLLECT_IMAGE_WORK_KIND
        )
        outcome = await handler.execute(
            payload.model_dump(mode="json"),
            AttemptContext(
                work_item_identifier="image",
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="operation",
            ),
        )
        assert isinstance(outcome, TerminalFailureWork)
        assert outcome.error["kind"] == "expired_url"


def _observation(url: str) -> dict[str, JsonValue]:
    return {
        "listing_id": "123",
        "images": [
            {
                "original_url": url,
                "role": "listing_gallery",
                "gallery_order": 0,
                "photo_id": "photo-123",
                "declared_dimensions": {"width": 720, "height": 960},
                "source": {
                    "acquisition_record_id": "old-acquisition",
                    "block_index": 0,
                    "json_path": ["images", 0],
                },
            }
        ],
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("replacement_url", "maximum_images", "prior_images"),
    [(FRESH, 1, 0), (EXPIRED, 1, 0), (None, 1, 0), (EXPIRED, 1, 1)],
    ids=["fresh", "still-expired", "renewal-failed", "exhausted-image-budget"],
)
async def test_reused_observation_gets_one_bounded_gallery_renewal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement_url: str | None,
    maximum_images: int,
    prior_images: int,
) -> None:
    identifiers = _identifiers()
    payload = RefreshSearchPayload.model_validate(
        {
            "base_search_run_record_identifier": "base",
            "search_work_identifier": "search",
            "search": {
                "request": {
                    "query": "telescope",
                    "location": {"kind": "facebook_location", "identifier": "123"},
                    "radius": {"value": 60, "unit": "miles"},
                },
                "traversal": {"maximum_pages": 1},
                "routing": ("proton", "personal", "test"),
            },
            "item_routing": ("decodo", "personal", "test"),
            "image_routing": ("proton", "personal", "test"),
            "maximum_images": maximum_images,
        }
    )
    monkeypatch.setattr(facebook_refresh_workers, "brave_navigation_headers", lambda: ())
    monkeypatch.setattr("carl._tests.test_pipeline_search_occurrences.time_ns", lambda: NOW)
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                *(
                    RecordDraft(
                        identifier=identifier,
                        kind=("carl", "facebook", "search_run"),
                        schema_version=1,
                        value={
                            "request": payload.search.request.model_dump(mode="json"),
                            "traversal": {"unique_listing_identifiers": ["123"]},
                        },
                    )
                    for identifier in ("base", "fresh")
                ),
                RecordDraft(
                    identifier="old-observation",
                    kind=("carl", "facebook", "listing_observation"),
                    schema_version=1,
                    value=_observation(EXPIRED),
                ),
                RecordDraft(
                    identifier="new-observation",
                    kind=("carl", "facebook", "listing_observation"),
                    schema_version=1,
                    value=_observation(replacement_url or EXPIRED),
                ),
            ),
            identifiers,
        )
        selected = "old-observation"

        async def retained_results(
            _listing_ids: tuple[str, ...],
        ) -> tuple[SuccessfulItemPageResult, ...]:
            return (
                SuccessfulItemPageResult(
                    listing_id="123",
                    acquisition_record_identifier="old-acquisition",
                    observation_record_identifier=selected,
                    response_classification=FacebookItemResponseKind.FULL_LISTING,
                    acquisition_completion_sequence=1,
                    extraction_completion_sequence=2,
                ),
            )

        monkeypatch.setattr(database, "successful_facebook_item_page_results", retained_results)
        context = await _attempt(
            database,
            "refresh",
            "facebook",
            work_kind=REFRESH_SEARCH_WORK_KIND,
            payload=payload.model_dump(mode="json"),
        )
        dependencies = RefreshWorkerDependencies(database, identifiers, utc_now_ns=lambda: NOW)
        checkpoint = (
            {
                "item_retry_generation_identifier": "retry",
                "prior_image_collection_count": prior_images,
            }
            if prior_images
            else {}
        )
        first = await facebook_refresh_workers._item_phase(
            payload, "fresh", context, dependencies, checkpoint=checkpoint
        )
        assert isinstance(first, RetryWork)
        if maximum_images <= prior_images:
            assert first.result["reused_successful_item_pages"] == 1
            assert first.result["item_collections"] == 0
            assert first.result["gallery_refresh_listing_identifiers"] == []
            images = await facebook_refresh_workers._image_phase(
                payload, first.result, context, dependencies
            )
            assert isinstance(images, CompletedWork)
            assert images.result["new_image_collections"] == 0
            assert images.result["expired_image_renditions_skipped"] == 0
            async with database._connections.reader() as connection:
                cursor = await connection.execute("SELECT count(*) FROM work_items")
                assert await cursor.fetchone() == (1,)
            return
        assert first.result["gallery_refresh_listing_identifiers"] == ["123"]
        item_ids = first.result["item_collection_work_identifiers"]
        assert len(item_ids) == 1
        second = await facebook_refresh_workers._item_phase(
            payload, "fresh", context, dependencies, checkpoint=first.result
        )
        assert isinstance(second, RetryWork)
        assert second.result["item_collection_work_identifiers"] == item_ids
        async with database._connections.writer() as connection:
            _ = await connection.execute(
                "UPDATE work_items SET state=?,result_json='{}' WHERE id=?",
                ("terminal_failure" if replacement_url is None else "completed", item_ids[0]),
            )
        selected = "old-observation" if replacement_url is None else "new-observation"
        collected = await facebook_refresh_workers._item_phase(
            payload,
            "fresh",
            context,
            dependencies,
            checkpoint={} if replacement_url is None else second.result,
        )
        assert isinstance(collected, RetryWork)
        image_outcome = await facebook_refresh_workers._image_phase(
            payload, collected.result, context, dependencies
        )
        assert collected.result["item_failures"] == 0
        assert collected.result["gallery_refresh_failures"] == (1 if replacement_url is None else 0)
        if replacement_url != FRESH:
            assert isinstance(image_outcome, CompletedWork)
            assert image_outcome.result["gallery_state"] == "stale"
            assert image_outcome.result["expired_image_renditions_skipped"] == 1
            assert image_outcome.result["new_image_collections"] == 0
            assert refresh_failure_summary(image_outcome.result).severity == "partial_failure"
            # The stored image plan preserves skip counts on restart too.
            repeated = await facebook_refresh_workers._image_phase(
                payload, collected.result, context, dependencies
            )
            assert repeated.result["expired_image_renditions_skipped"] == 1
            async with database._connections.writer() as connection:
                _ = await connection.execute(
                    "UPDATE work_items SET state='completed',result_json=?,lease_token=NULL,lease_owner=NULL,lease_expires_at_utc_ns=NULL WHERE id='refresh'",
                    (encode_json(image_outcome.result),),
                )
            status = await ReviewApplication(
                database, tmp_path, utc_now_ns=lambda: NOW
            ).get_work_status("refresh")
            assert status.details is None
            assert status.outcome == "partial_failure"
            assert status.search_refresh_progress is not None
            assert status.search_refresh_progress.gallery_state == "stale"
            assert status.search_refresh_progress.expired_image_renditions_skipped == 1
            assert status.search_refresh_progress.gallery_stale_listing_identifiers == ("123",)
            assert status.search_refresh_progress.gallery_warnings == (
                "expired_gallery_urls_require_item_refresh",
            )
        else:
            assert isinstance(image_outcome, RetryWork)
            image_ids = image_outcome.result["image_collection_work_identifiers"]
            assert len(image_ids) == 1
            image = await database.work(cast(str, image_ids[0]))
            assert image["payload"]["request_plan"]["url"] == FRESH
        assert (
            len(
                await database.requested_work_identifiers(
                    requester_kind=("carl", "facebook", "search_refresh", "item"),
                    requester_identifier=facebook_refresh_workers._stable_identifier(
                        "refresh", "item_requester", "123"
                    ),
                )
            )
            == 1
        )
