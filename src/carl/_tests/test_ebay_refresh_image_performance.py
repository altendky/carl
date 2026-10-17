"""Image refresh planning stays bounded by the selected observations and plan."""

# pyright: reportPrivateUsage=false

from collections.abc import Callable, Sequence
from pathlib import Path

import pytest

from carl._tests.test_ebay_item_workers import (
    IMAGE_URL,
    ITEM,
    _enqueue,
    _identifiers,
    _mapping,
    _png,
    _retain_records,
    _run_next,
)
from carl.core.ebay import EbaySearchRequest
from carl.core.ebay_refresh import (
    REFRESH_EBAY_SEARCH_WORK_KIND,
    RefreshEbaySearchPayload,
    refresh_ebay_search_work,
)
from carl.core.json import decode_json
from carl.core.models import JsonValue, RecordDraft
from carl.core.worker import AttemptContext, RetryWork
from carl.ebay_refresh_workers import (
    EbayRefreshWorkerDependencies,
    _identifier,
    build_ebay_refresh_worker_registry,
)
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry


def _observation() -> RecordDraft:
    return RecordDraft(
        identifier="observation",
        kind=("carl", "ebay", "listing_observation"),
        schema_version=1,
        value={"item_identifier": ITEM, "gallery_urls": [IMAGE_URL]},
    )


def _reference(identifier: str = "reference", *, observation: str = "observation") -> RecordDraft:
    return RecordDraft(
        identifier=identifier,
        kind=("carl", "ebay", "gallery_image_reference"),
        schema_version=1,
        value={
            "item_identifier": ITEM,
            "observation_record_identifier": observation,
            "url": IMAGE_URL,
            "gallery_order": 0,
        },
    )


async def _seed_checkpoint(
    database: Database,
    identifiers: Callable[[], str],
    *,
    persisted_plan: bool = False,
) -> WorkHandlerRegistry:
    registry = build_ebay_refresh_worker_registry(
        EbayRefreshWorkerDependencies(database, identifiers)
    )
    original = registry.handlers[0]

    async def checkpoint(_payload: RefreshEbaySearchPayload, _context: AttemptContext) -> RetryWork:
        return RetryWork(
            delay_ns=0,
            reason={"kind": "test_resume_image_phase"},
            result={
                "stage": "collecting_images" if persisted_plan else "items_complete",
                "observation_record_identifiers": ["observation"],
                "item_failures": 0,
                "description_failures": 0,
            },
        )

    seed_registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=original.capability,
                component=original.component,
                payload_type=RefreshEbaySearchPayload,
                handler=checkpoint,
            ),
        )
    )
    await _enqueue(
        database,
        refresh_ebay_search_work(
            identifier="refresh",
            payload=RefreshEbaySearchPayload(
                base_search_run_record_identifier="base",
                search_work_identifier="search",
                search=EbaySearchRequest(query="eyepiece"),
                maximum_images=1,
            ),
        ),
        identifiers,
    )
    await _run_next(database, seed_registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
    return registry


@pytest.mark.anyio
async def test_persisted_image_plan_resume_skips_gallery_and_candidate_planning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifiers = _identifiers()
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        plan_identifier = _identifier("refresh", "image_plan")
        await _retain_records(
            database,
            (
                _observation(),
                _reference(),
                RecordDraft(
                    identifier=plan_identifier,
                    kind=("carl", "ebay", "image_followup_plan"),
                    schema_version=1,
                    value={"reference_record_identifiers": ["reference"]},
                ),
            ),
            identifiers,
        )
        registry = await _seed_checkpoint(database, identifiers, persisted_plan=True)

        async def no_history(_kind: tuple[str, ...]) -> tuple[tuple[str, JsonValue], ...]:
            pytest.fail("A persisted image plan must not reload global record history")

        async def no_candidates(
            _urls: tuple[str, ...],
        ) -> tuple[tuple[str, dict[str, JsonValue]], ...]:
            pytest.fail("A persisted image plan must not repeat saved-rendition planning")

        monkeypatch.setattr(database, "records_by_kind", no_history)
        monkeypatch.setattr(database, "saved_ebay_image_candidates", no_candidates)
        resumed = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert resumed["state"] == "pending"
        result = _mapping(resumed["result"])
        assert result["stage"] == "collecting_images"
        assert result["image_followup_plan_record_identifier"] == plan_identifier
        assert len(result["image_collection_work_identifiers"]) == 1


@pytest.mark.anyio
async def test_fast_first_image_cannot_finalize_before_deferred_duplicate_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifiers = _identifiers()
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                _observation(),
                _reference(),
                _observation().model_copy(update={"identifier": "second-observation"}),
                _reference("second-reference", observation="second-observation"),
                RecordDraft(
                    identifier=_identifier("refresh", "image_plan"),
                    kind=("carl", "ebay", "image_followup_plan"),
                    schema_version=1,
                    value={"reference_record_identifiers": ["reference", "second-reference"]},
                ),
            ),
            identifiers,
        )
        registry = await _seed_checkpoint(database, identifiers, persisted_plan=True)
        progress = database.work_progress

        async def completed_before_final_read(
            work_identifiers: Sequence[str],
        ) -> tuple[dict[str, JsonValue], ...]:
            works = await progress(work_identifiers)
            # Simulate the first image committing after its duplicate reference
            # was deferred, but before the coordinator reads final child state.
            return tuple(
                {**work, "state": "completed", "result": {"state": "saved"}} for work in works
            )

        monkeypatch.setattr(database, "work_progress", completed_before_final_read)
        resumed = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert resumed["state"] == "pending"
        result = _mapping(resumed["result"])
        assert result["stage"] == "collecting_images"
        assert len(result["image_collection_work_identifiers"]) == 1


@pytest.mark.anyio
async def test_image_planning_does_not_materialize_unrelated_historical_references(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifiers = _identifiers()
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    unrelated = "unrelated-gallery-history-must-stay-unread"
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (_observation(), _reference(), _reference("other-reference", observation=unrelated)),
            identifiers,
        )
        registry = await _seed_checkpoint(database, identifiers)

        def selected_json_only(value: str | bytes) -> JsonValue:
            assert unrelated not in (value.decode() if isinstance(value, bytes) else value), (
                "Image planning materialized a gallery reference outside its observation scope"
            )
            return decode_json(value)

        monkeypatch.setattr("carl.io.sqlite.decode_json", selected_json_only)
        planned = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert planned["state"] == "pending"
        _, _, plan = await database.get_record(_identifier("refresh", "image_plan"))
        assert _mapping(plan)["reference_record_identifiers"] == ["reference"]


@pytest.mark.anyio
@pytest.mark.parametrize("newest_corrupt", (False, True))
async def test_planning_checks_only_newest_valid_artifact_for_each_saved_rendition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, newest_corrupt: bool
) -> None:
    identifiers = _identifiers()
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                _observation(),
                _reference(),
                *(
                    RecordDraft(
                        identifier=f"saved-{label}",
                        kind=("carl", "ebay", "image_result"),
                        schema_version=1,
                        value={
                            "state": "saved",
                            "url": IMAGE_URL,
                            "image_artifact_identifier": f"artifact-{label}",
                        },
                    )
                    for label in ("oldest", "middle", "newest")
                ),
            ),
            identifiers,
        )
        registry = await _seed_checkpoint(database, identifiers)
        checked: list[str] = []

        async def verify_selected_artifact(identifier: str) -> tuple[dict[str, JsonValue], bytes]:
            checked.append(identifier)
            assert identifier != "artifact-oldest", (
                "Planning must stop checking historical candidates once this rendition is valid"
            )
            if identifier == "artifact-newest" and newest_corrupt:
                raise ValueError("test corrupt artifact")
            return {}, _png()

        monkeypatch.setattr(database, "get_artifact", verify_selected_artifact)
        planned = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert planned["state"] == "pending"
        assert checked == (
            ["artifact-newest", "artifact-middle"] if newest_corrupt else ["artifact-newest"]
        )
        _, _, plan = await database.get_record(_identifier("refresh", "image_plan"))
        assert _mapping(plan)["reference_record_identifiers"] == ["reference"]
