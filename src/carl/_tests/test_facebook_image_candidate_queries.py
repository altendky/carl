"""Offline coverage for bounded Facebook image-reuse queries and gallery planning."""

# Reuse the existing offline fixtures and inspect the database's reader instrumentation.
# pyright: reportPrivateUsage=false

from pathlib import Path
from typing import cast

import pytest

from carl._tests.test_ebay_item_workers import _identifiers, _retain_records
from carl._tests.test_facebook_acquisition_reuse import OTHER, URL, _reference
from carl._tests.test_pipeline_search_occurrences import _attempt
from carl.core.facebook_listing import RequestFacebookListingDetailsPayload
from carl.core.models import JsonValue, RecordDraft
from carl.core.worker import RetryWork
from carl.facebook_listing_workers import FacebookListingWorkerDependencies, _image_phase
from carl.io.sqlite import Database


def _saved(
    identifier: str,
    url: str,
    photo_id: str | None,
    *,
    state: str = "saved",
    marketplace: str = "facebook",
) -> RecordDraft:
    return RecordDraft(
        identifier=identifier,
        kind=("carl", marketplace, "image_result"),
        schema_version=1,
        value={"state": state, "original_url": url, "source_photo_id": photo_id},
    )


@pytest.mark.anyio
async def test_image_candidates_preserve_url_photo_and_cdn_coverage_without_duplicates(
    tmp_path: Path,
) -> None:
    identifiers = _identifiers()
    unrelated_url = URL.replace("photo.jpg", "different.jpg")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                _saved("exact-url", URL, "different-photo"),
                _saved("same-photo", unrelated_url, "photo-1"),
                _saved("cdn-path", OTHER, None),
                _saved("matches-all", OTHER, "photo-1"),
                _saved("unrelated", unrelated_url, "unrelated-photo"),
                _saved("not-saved", URL, "photo-1", state="failed"),
                _saved("other-marketplace", URL, "photo-1", marketplace="ebay"),
            ),
            identifiers,
        )
        await _retain_records(database, (_saved("uncommitted-owner", URL, "photo-1"),), identifiers)
        async with database._connections.writer() as connection:
            _ = await connection.execute(
                """
                UPDATE operations SET state='started', ended_at_utc=NULL, duration_ns=NULL
                WHERE id=(SELECT created_by_operation_id FROM objects WHERE id=?)
                """,
                ("uncommitted-owner",),
            )
        references = (_reference(URL), _reference(URL), _reference(OTHER, "456"))
        candidates = await database.saved_facebook_image_candidates_for_references(references)
        assert [identifier for identifier, _ in candidates] == [
            "exact-url",
            "same-photo",
            "cdn-path",
            "matches-all",
        ]
        assert candidates == await database.saved_facebook_image_candidates_for_references(
            references
        )
        assert await database.saved_facebook_image_candidates_for_references(()) == ()


@pytest.mark.anyio
async def test_image_candidate_query_does_not_rescan_history_per_image(tmp_path: Path) -> None:
    """Use VM steps, not wall time, to catch the formerly quadratic join plan."""
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                *(
                    _saved(
                        f"irrelevant-{index}",
                        URL.replace("photo.jpg", f"different-{index}.jpg"),
                        f"different-photo-{index}",
                    )
                    for index in range(256)
                ),
                _saved("matching", OTHER, "photo-1"),
            ),
            identifiers,
        )
        progress_calls = 0

        def count_steps() -> bool:
            nonlocal progress_calls
            progress_calls += 1
            return progress_calls > 100

        # Nesting uses this same reader, so instrumentation covers the production query.
        async with database._connections.reader() as connection:
            await connection.set_progress_handler(count_steps, nsteps=1000, id="candidate-budget")
            try:
                candidates = await database.saved_facebook_image_candidates_for_references(
                    (_reference(URL),)
                )
            finally:
                await connection.set_progress_handler(None, id="candidate-budget")
        assert [identifier for identifier, _ in candidates] == ["matching"]
        assert progress_calls <= 100


@pytest.mark.anyio
async def test_listing_gallery_plan_only_reads_its_observation_references(tmp_path: Path) -> None:
    identifiers = _identifiers()
    reference = _reference(URL)
    observation: dict[str, JsonValue] = {
        "listing_id": reference.listing_id,
        "images": [
            {
                "original_url": reference.original_url,
                "role": reference.role,
                "gallery_order": reference.gallery_order,
                "photo_id": reference.photo_id,
                "declared_dimensions": {
                    "width": reference.declared_width,
                    "height": reference.declared_height,
                },
                "source": {
                    "acquisition_record_id": reference.acquisition_record_identifier,
                    "block_index": reference.block_index,
                    "json_path": list(reference.json_path),
                },
            }
        ],
    }
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier=reference.listing_observation_record_identifier,
                    kind=("carl", "facebook", "listing_observation"),
                    schema_version=1,
                    value=observation,
                ),
                RecordDraft(
                    identifier="existing-reference",
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=reference.model_dump(mode="json"),
                ),
                RecordDraft(
                    identifier="unrelated-history",
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    # This unrelated observation must not be loaded or validated by this plan.
                    value={"listing_observation_record_identifier": "different-observation"},
                ),
            ),
            identifiers,
        )
        context = await _attempt(
            database, "gallery-planning", "facebook", work_kind=("test", "gallery-planning")
        )
        outcome = await _image_phase(
            RequestFacebookListingDetailsPayload(listing_identifier="123", maximum_images=0),
            {"observation_record_identifier": reference.listing_observation_record_identifier},
            context,
            FacebookListingWorkerDependencies(database, identifiers),
        )
        assert isinstance(outcome, RetryWork)
        assert isinstance(outcome.result, dict)
        plan_identifier = cast(dict[str, JsonValue], outcome.result)["image_plan_record_identifier"]
        assert isinstance(plan_identifier, str)
        _, _, plan = await database.get_record(plan_identifier)
        assert isinstance(plan, dict)
        assert plan["reference_record_identifiers"] == ["existing-reference"]
        assert plan["download_groups"] == []
