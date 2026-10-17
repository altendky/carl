"""Refresh planning scopes historical reads and resumes retained decisions."""

# Reuse deliberately private offline worker fixtures.
# pyright: reportPrivateUsage=false

from collections.abc import Sequence
from pathlib import Path

import pytest

from carl import facebook_refresh_workers as workers
from carl._tests.test_ebay_item_workers import _identifiers, _provenance, _retain_records
from carl._tests.test_facebook_image_expiry import EXPIRED, NOW, _observation
from carl._tests.test_facebook_images import _reference
from carl._tests.test_pipeline_search_occurrences import _attempt
from carl.core.components import Component, ComponentId
from carl.core.facebook import FacebookItemResponseKind
from carl.core.facebook_images import GalleryImageReference, ImageReuseMatchKind
from carl.core.facebook_refresh import REFRESH_SEARCH_WORK_KIND, RefreshSearchPayload
from carl.core.facebook_work import SuccessfulItemPageResult
from carl.core.models import BytesDraft, JsonValue, NamedOutput, RecordDraft
from carl.core.worker import CompletedWork, TerminalFailureWork
from carl.io.sqlite import Database


def _payload() -> RefreshSearchPayload:
    return RefreshSearchPayload.model_validate(
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
        }
    )


async def _retain_shared_image(database: Database) -> None:
    await database.begin_operation(
        operation_id="saved-image-operation",
        component=Component(ComponentId(("test", "saved_image")), 1, _payload),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-10-17T00:00:00+00:00",
    )
    await database.complete_operation(
        operation_id="saved-image-operation",
        records=(
            RecordDraft(
                identifier="saved-image",
                kind=("carl", "facebook", "image_result"),
                schema_version=1,
                value={
                    "state": "saved",
                    "source_photo_id": "photo-123",
                    "original_url": EXPIRED,
                    "width": 720,
                    "height": 960,
                    "image_artifact_identifier": "shared-image-bytes",
                },
            ),
        ),
        artifacts=(
            BytesDraft(
                identifier="shared-image-bytes",
                kind=("test", "image"),
                media_type="image/png",
                representation={},
                content=b"retained bytes with verified storage integrity",
            ),
        ),
        outputs=(
            NamedOutput(name=("image",), object_identifier="saved-image"),
            NamedOutput(name=("body",), object_identifier="shared-image-bytes"),
        ),
        result={},
        ended_at_utc="2026-10-17T00:00:01+00:00",
        duration_ns=1,
    )


@pytest.mark.anyio
async def test_expired_gallery_checks_batch_listings_and_validate_shared_bytes_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifiers = _identifiers()
    results = tuple(
        SuccessfulItemPageResult(
            listing_id=str(index + 1),
            acquisition_record_identifier="old-acquisition",
            observation_record_identifier=f"observation-{index}",
            response_classification=FacebookItemResponseKind.FULL_LISTING,
            acquisition_completion_sequence=1,
            extraction_completion_sequence=2,
        )
        for index in range(1005)
    )
    observations: list[RecordDraft] = []
    for index, result in enumerate(results):
        observation = _observation(
            EXPIRED if index < 1004 else EXPIRED.replace("photo.jpg", "missing.jpg")
        )
        observation["listing_id"] = result.listing_id
        if index == 1004:
            observation["images"][0]["photo_id"] = "missing-photo"
        observations.append(
            RecordDraft(
                identifier=result.observation_record_identifier,
                kind=("carl", "facebook", "listing_observation"),
                schema_version=1,
                value=observation,
            )
        )
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _retain_records(database, tuple(observations), identifiers)
        await _retain_shared_image(database)
        candidate_sizes: list[int] = []
        artifact_reads: list[str] = []
        original_candidates = database.saved_facebook_image_candidates_for_references
        original_artifact = database.get_artifact

        async def candidates(
            references: tuple[GalleryImageReference, ...],
        ) -> tuple[tuple[str, dict[str, JsonValue]], ...]:
            candidate_sizes.append(len(references))
            return await original_candidates(references)

        async def artifact(identifier: str) -> tuple[dict[str, JsonValue], bytes]:
            artifact_reads.append(identifier)
            return await original_artifact(identifier)

        monkeypatch.setattr(database, "saved_facebook_image_candidates_for_references", candidates)
        monkeypatch.setattr(database, "get_artifact", artifact)
        stale = await workers._expired_galleries_needing_refresh(
            results,
            workers.RefreshWorkerDependencies(database, identifiers, utc_now_ns=lambda: NOW),
        )
        assert stale == frozenset({"1005"})
        assert candidate_sizes == [1000, 5]
        assert artifact_reads == ["shared-image-bytes"]


@pytest.mark.anyio
@pytest.mark.parametrize("legacy_plan", (False, True))
@pytest.mark.parametrize("artifact_failure", (None, "missing", "corrupt"))
async def test_stored_image_plan_resumes_without_global_history_and_replays_reuse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    legacy_plan: bool,
    artifact_failure: str | None,
) -> None:
    identifiers = _identifiers()
    payload = _payload()
    reference = _reference().model_copy(update={"original_url": EXPIRED})
    plan: dict[str, JsonValue] = {
        "reference_record_identifiers": ["reference"],
        "selected_reference_record_identifiers": [],
        "expired_reference_record_identifiers": [],
        "already_saved_exact_renditions": 1,
        "reused_source_photo_images": 0,
    }
    if not legacy_plan:
        plan["reuse_decisions"] = [
            {
                "reference_record_identifier": "reference",
                "source_image_result_record_identifier": "saved-image",
                "match_kind": ImageReuseMatchKind.EXACT_RENDITION.value,
            }
        ]
        plan["resumed_image_extraction_work_identifiers"] = []
    monkeypatch.setattr("carl._tests.test_pipeline_search_occurrences.time_ns", lambda: NOW)
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _retain_shared_image(database)
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
                    identifier=workers._stable_identifier("refresh", "image_plan"),
                    kind=("carl", "facebook", "image_followup_plan"),
                    schema_version=1,
                    value=plan,
                ),
            ),
            identifiers,
        )
        context = await _attempt(
            database, "refresh", "facebook", work_kind=REFRESH_SEARCH_WORK_KIND
        )

        async def forbidden(*_args: object, **_kwargs: object) -> None:
            pytest.fail("A stored image plan must not reconstruct global historical state")

        for name in (
            "successful_facebook_item_page_results",
            "facebook_gallery_reference_identifiers",
            "saved_facebook_image_renditions",
            "pending_facebook_image_extractions",
        ):
            monkeypatch.setattr(database, name, forbidden)
        if not legacy_plan:
            monkeypatch.setattr(
                database, "saved_facebook_image_candidates_for_references", forbidden
            )
        original_artifact = database.get_artifact
        artifact_reads: list[str] = []

        async def artifact(identifier: str) -> tuple[dict[str, JsonValue], bytes]:
            artifact_reads.append(identifier)
            assert identifier == "shared-image-bytes"
            if artifact_failure == "missing":
                raise FileNotFoundError("Saved image file disappeared after the plan checkpoint")
            if artifact_failure == "corrupt":
                raise ValueError("Saved image file failed its stored hash verification")
            return await original_artifact(identifier)

        monkeypatch.setattr(database, "get_artifact", artifact)

        async def provenance():
            return _provenance()

        dependencies = workers.RefreshWorkerDependencies(
            database, identifiers, utc_now_ns=lambda: NOW, code_provenance=provenance
        )
        for _ in range(2):
            outcome = await workers._image_phase(
                payload, {"refreshed_search_run_record_identifier": "fresh"}, context, dependencies
            )
            if artifact_failure is not None:
                assert isinstance(outcome, TerminalFailureWork)
                assert outcome.error["kind"] == "refresh_image_reuse_evidence_unavailable"
                assert outcome.result["state"] == "terminal_failure"
                assert not await database.facebook_image_reuse_resolutions(("reference",))
                continue
            assert isinstance(outcome, CompletedWork)
            assert outcome.result["already_saved_exact_renditions"] == 1
            assert outcome.result["new_image_collections"] == 0
            assert len(await database.facebook_image_reuse_resolutions(("reference",))) == 1
        assert artifact_reads == ["shared-image-bytes", "shared-image-bytes"]


@pytest.mark.anyio
async def test_fresh_image_plan_only_loads_selected_observation_references(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifiers = _identifiers()
    payload = _payload()
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
                    identifier="observation",
                    kind=("carl", "facebook", "listing_observation"),
                    schema_version=1,
                    value={"listing_id": "123", "images": []},
                ),
                RecordDraft(
                    identifier="unrelated-malformed-reference",
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value={"listing_observation_record_identifier": "unrelated"},
                ),
            ),
            identifiers,
        )

        async def successful(
            _listing_ids: Sequence[str],
        ) -> tuple[SuccessfulItemPageResult, ...]:
            return (
                SuccessfulItemPageResult(
                    listing_id="123",
                    acquisition_record_identifier="acquisition",
                    observation_record_identifier="observation",
                    response_classification=FacebookItemResponseKind.FULL_LISTING,
                    acquisition_completion_sequence=1,
                    extraction_completion_sequence=2,
                ),
            )

        original_references = database.facebook_gallery_reference_identifiers
        scopes: list[tuple[str, ...]] = []

        async def references(
            observation_identifiers: Sequence[str] | None = None,
        ) -> dict[GalleryImageReference, str]:
            assert observation_identifiers is not None
            scopes.append(tuple(observation_identifiers))
            return await original_references(observation_identifiers)

        monkeypatch.setattr(database, "successful_facebook_item_page_results", successful)
        monkeypatch.setattr(database, "facebook_gallery_reference_identifiers", references)
        context = await _attempt(
            database, "refresh", "facebook", work_kind=REFRESH_SEARCH_WORK_KIND
        )
        outcome = await workers._image_phase(
            payload,
            {"refreshed_search_run_record_identifier": "fresh"},
            context,
            workers.RefreshWorkerDependencies(database, identifiers, utc_now_ns=lambda: NOW),
        )
        assert isinstance(outcome, CompletedWork)
        assert scopes == [("observation",)]
