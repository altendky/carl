"""Cached galleries bind evidence in batches instead of dispatching image jobs."""

# pyright: reportPrivateUsage=false

from collections.abc import Callable
from pathlib import Path
from time import time_ns
from typing import Any, cast

import anyio
import pytest

from carl._tests.test_ebay_item_workers import (
    IMAGE_URL,
    ITEM,
    _Acquirer,
    _enqueue,
    _identifiers,
    _mapping,
    _png,
    _reference,
    _registry,
    _Response,
    _retain_records,
    _run_next,
)
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
from carl.core.models import JsonValue, RecordDraft
from carl.core.worker import AttemptContext, RetryWork, WorkOutcome
from carl.ebay_analysis_workers import select_ebay_analysis_evidence
from carl.ebay_refresh_workers import (
    _IMAGE_REUSE_BATCH_SIZE,
    EbayRefreshWorkerDependencies,
    build_ebay_refresh_worker_registry,
    ebay_refresh_child_work_states,
)
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry


async def _source(
    database: Database, path: Path, identifiers: Callable[[], str]
) -> tuple[str, str, _Acquirer]:
    images = _Acquirer({IMAGE_URL: _Response(_png(), "image/png")})
    await _retain_records(database, (_reference(),), identifiers)
    _ = await _enqueue(
        database,
        collect_ebay_image_work(
            identifier="source-image",
            payload=CollectEbayImagePayload(
                item_identifier=ITEM,
                observation_record_identifier="observation-1",
                reference_record_identifier="reference-1",
                url=IMAGE_URL,
            ),
        ),
        identifiers,
    )
    work = await _run_next(
        database,
        _registry(database, path, identifiers, image_acquirer=images),
        identifiers,
        COLLECT_EBAY_IMAGE_WORK_KIND,
    )
    result = _mapping(work["result"])
    return (
        str(result["image_result_record_identifier"]),
        str(result["image_artifact_identifier"]),
        images,
    )


async def _observations(
    database: Database,
    identifiers: Callable[[], str],
    count: int,
    *,
    missing_url: str | None = None,
) -> tuple[str, ...]:
    observations = tuple(f"observation-{index + 2}" for index in range(count))
    await _retain_records(
        database,
        tuple(
            RecordDraft(
                identifier=identifier,
                kind=("carl", "ebay", "listing_observation"),
                schema_version=1,
                value={
                    "item_identifier": ITEM,
                    "classification": "detail",
                    "gallery_urls": [
                        missing_url if index == count - 1 and missing_url else IMAGE_URL
                    ],
                },
            )
            for index, identifier in enumerate(observations)
        ),
        identifiers,
    )
    return observations


async def _start(
    database: Database,
    path: Path,
    identifiers: Callable[[], str],
    observations: tuple[str, ...],
    images: _Acquirer,
    *,
    root: str = "refresh",
    maximum_images: int = 1,
) -> WorkHandlerRegistry:
    original = build_ebay_refresh_worker_registry(
        EbayRefreshWorkerDependencies(database, identifiers)
    ).handlers[0]

    async def images_phase(
        payload: RefreshEbaySearchPayload, context: AttemptContext
    ) -> WorkOutcome:
        if (await database.work(context.work_item_identifier))["result"] is None:
            return RetryWork(
                delay_ns=0,
                reason={},
                result={
                    "stage": "items_complete",
                    "observation_record_identifiers": list(observations),
                    "item_failures": 0,
                    "description_failures": 0,
                },
            )
        return await original.execute(payload.model_dump(mode="json"), context)

    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=original.capability,
                component=original.component,
                payload_type=RefreshEbaySearchPayload,
                handler=images_phase,
            ),
            *_registry(database, path, identifiers, image_acquirer=images).handlers,
        )
    )
    _ = await _enqueue(
        database,
        refresh_ebay_search_work(
            identifier=root,
            payload=RefreshEbaySearchPayload(
                base_search_run_record_identifier=root,
                search_work_identifier=f"search-{root}",
                search=EbaySearchRequest(query="eyepiece"),
                maximum_images=maximum_images,
            ),
        ),
        identifiers,
    )
    _ = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
    return registry


@pytest.mark.anyio
async def test_large_cached_gallery_batches_integrity_and_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        source, artifact, images = await _source(database, tmp_path, identifiers)
        count = _IMAGE_REUSE_BATCH_SIZE * 2 + 3
        observations = await _observations(database, identifiers, count)
        registry = await _start(database, tmp_path, identifiers, observations, images)
        get_artifact = database.get_artifact
        checked: list[str] = []

        async def verify(identifier: str) -> tuple[dict[str, JsonValue], bytes]:
            checked.append(identifier)
            return await get_artifact(identifier)

        publish = database.publish_leased_operation_checkpoint
        batches: list[int] = []

        async def publish_batch(**kwargs: Any) -> None:
            records = cast(tuple[RecordDraft, ...], kwargs["records"])
            image_count = sum(record.kind == ("carl", "ebay", "image_result") for record in records)
            if image_count:
                batches.append(image_count)
            await publish(**kwargs)

        monkeypatch.setattr(database, "get_artifact", verify)
        monkeypatch.setattr(database, "publish_leased_operation_checkpoint", publish_batch)
        completed = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert completed["state"] == "completed"
        result = _mapping(completed["result"])
        assert result["new_images_saved"] == result["new_images_reused"] == count
        assert result["new_image_collections"] == 0
        assert result["image_collection_work_identifiers"] == []
        assert checked == [artifact]
        assert batches == [_IMAGE_REUSE_BATCH_SIZE, _IMAGE_REUSE_BATCH_SIZE, 3]
        assert len(images.plans) == 1
        assert (await ebay_refresh_child_work_states(database, "refresh"))["images"] == (
            "completed",
        ) * count
        for observation in (observations[0], observations[-1]):
            selection = await select_ebay_analysis_evidence(database, observation)
            assert len(selection.included) == 1
            binding = selection.included[0]
            _, _, value = await database.get_record(binding.image_result_record_identifier)
            assert _mapping(value)["reused_from_result_record_identifier"] == source
            _, inputs, _ = await database.object_operation_relations(
                binding.image_result_record_identifier
            )
            assert any(edge.object_identifier == source for edge in inputs)
            assert any(
                edge.object_identifier == binding.gallery_image_reference_record_identifier
                for edge in inputs
            )


@pytest.mark.anyio
async def test_mixed_cached_and_missing_gallery_only_dispatches_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    missing_url = IMAGE_URL.replace("item", "uncached")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, _, images = await _source(database, tmp_path, identifiers)
        images.responses[missing_url] = _Response(_png(), "image/png")
        observations = await _observations(database, identifiers, 3, missing_url=missing_url)
        registry = await _start(database, tmp_path, identifiers, observations, images)
        pending = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert pending["state"] == "pending"
        assert len(_mapping(pending["result"])["image_collection_work_identifiers"]) == 1
        _ = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        completed = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        result = _mapping(completed["result"])
        assert completed["state"] == "completed"
        assert result["new_images_saved"] == 3
        assert result["new_images_reused"] == 2
        assert result["new_image_collections"] == 1
        assert len(images.plans) == 2


@pytest.mark.anyio
@pytest.mark.parametrize("remove_valid", (False, True))
async def test_corrupt_candidate_falls_back_to_valid_source_or_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, remove_valid: bool
) -> None:
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        source, artifact, images = await _source(database, tmp_path, identifiers)
        metadata, _ = await database.get_artifact(artifact)
        if remove_valid:
            locator = _mapping(metadata["storage"])["locator"]
            assert isinstance(locator, str)
            (database.path.parent / locator).unlink()
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier="corrupt-newest",
                    kind=("carl", "ebay", "image_result"),
                    schema_version=1,
                    value={
                        "state": "saved",
                        "url": IMAGE_URL,
                        "image_artifact_identifier": "missing-artifact",
                    },
                ),
            ),
            identifiers,
        )
        observations = await _observations(database, identifiers, 1)
        registry = await _start(database, tmp_path, identifiers, observations, images)
        work = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        if remove_valid:
            assert work["state"] == "pending"
            _ = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
            work = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert work["state"] == "completed"
        result = _mapping(work["result"])
        assert result["new_images_saved"] == 1
        assert result["new_images_reused"] == int(not remove_valid)
        assert len(images.plans) == (2 if remove_valid else 1)
        if not remove_valid:
            selection = await select_ebay_analysis_evidence(database, observations[0])
            _, _, binding = await database.get_record(
                selection.included[0].image_result_record_identifier
            )
            assert _mapping(binding)["reused_from_result_record_identifier"] == source


@pytest.mark.anyio
async def test_checkpoint_resume_preserves_bindings_and_their_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        source, _, images = await _source(database, tmp_path, identifiers)
        observations = await _observations(database, identifiers, _IMAGE_REUSE_BATCH_SIZE + 3)
        registry = await _start(database, tmp_path, identifiers, observations, images)
        publish = database.publish_leased_operation_checkpoint

        async def interrupt_after_binding(**kwargs: Any) -> None:
            await publish(**kwargs)
            if any(record.kind == ("carl", "ebay", "image_result") for record in kwargs["records"]):
                raise RuntimeError("simulated process interruption after checkpoint")

        monkeypatch.setattr(
            database, "publish_leased_operation_checkpoint", interrupt_after_binding
        )
        interrupted = await _run_next(
            database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND
        )
        assert interrupted["state"] == "terminal_failure"
        bindings = _mapping(_mapping(interrupted["result"])["image_reuse_result_identifiers"])
        assert len(bindings) == _IMAGE_REUSE_BATCH_SIZE
        first_binding = str(next(iter(bindings.values())))
        _, _, first = await database.get_record(first_binding)
        operation = _mapping(first)["operation_identifier"]
        assert isinstance(operation, str)
        monkeypatch.setattr(database, "publish_leased_operation_checkpoint", publish)
        await database.retry_terminal_work_from_operation(
            work_item_identifier="refresh",
            checkpoint_operation_identifier=operation,
            retried_at_utc_ns=time_ns(),
            event_identifier=identifiers(),
            reason={"kind": "test_recovery"},
        )
        completed = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert completed["state"] == "completed"
        assert _mapping(completed["result"])["new_images_saved"] == len(observations)
        assert (
            len(await database.records_by_kind(("carl", "ebay", "image_result")))
            == len(observations) + 1
        )
        assert (
            _mapping(_mapping(completed["result"])["image_reuse_result_identifiers"])[
                str(_mapping(first)["reference_record_identifier"])
            ]
            == first_binding
        )
        for observation in (observations[0], observations[-1]):
            selection = await select_ebay_analysis_evidence(database, observation)
            identifier = selection.included[0].image_result_record_identifier
            _, _, value = await database.get_record(identifier)
            assert _mapping(value)["reused_from_result_record_identifier"] == source
            _, inputs, outputs = await database.object_operation_relations(identifier)
            assert any(edge.object_identifier == source for edge in inputs)
            assert any(edge.object_identifier == identifier for edge in outputs)


@pytest.mark.anyio
async def test_cancellation_before_batch_publication_keeps_only_completed_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, _, images = await _source(database, tmp_path, identifiers)
        observations = await _observations(database, identifiers, 2)
        registry = await _start(database, tmp_path, identifiers, observations, images)
        publish = database.publish_leased_operation_checkpoint

        async def cancel_before_binding(**kwargs: Any) -> None:
            if any(record.kind == ("carl", "ebay", "image_result") for record in kwargs["records"]):
                _ = await database.cancel_work(
                    "refresh", reason="stop", caller="test", requested_at_utc_ns=time_ns()
                )
            await publish(**kwargs)

        monkeypatch.setattr(database, "publish_leased_operation_checkpoint", cancel_before_binding)
        cancelled = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert cancelled["state"] == "cancelled"
        assert cancelled["error"] is None
        assert len(await database.records_by_kind(("carl", "ebay", "image_result"))) == 1
        assert (await ebay_refresh_child_work_states(database, "refresh"))["images"] == ()


@pytest.mark.anyio
async def test_corrupt_binding_download_then_restoration_counts_each_reference_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    missing_url = IMAGE_URL.replace("item", "uncached")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, artifact, images = await _source(database, tmp_path, identifiers)
        images.responses[missing_url] = _Response(_png(), "image/png")
        observations = await _observations(database, identifiers, 3, missing_url=missing_url)
        registry = await _start(database, tmp_path, identifiers, observations, images)
        pending = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert pending["state"] == "pending"
        _ = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        metadata, _ = await database.get_artifact(artifact)
        locator = _mapping(metadata["storage"])["locator"]
        assert isinstance(locator, str)
        (database.path.parent / locator).unlink()
        pending = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert pending["state"] == "pending"
        _ = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        completed = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        result = _mapping(completed["result"])
        assert completed["state"] == "completed"
        assert result["new_images_saved"] == 3
        assert result["new_images_reused"] == 1
        assert result["new_image_collections"] == 2
        assert len((await ebay_refresh_child_work_states(database, "refresh"))["images"]) == 3


@pytest.mark.anyio
async def test_overlapping_roots_share_reference_without_binding_id_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, _, images = await _source(database, tmp_path, identifiers)
        observations = await _observations(database, identifiers, 1)
        registry = await _start(database, tmp_path, identifiers, observations, images)
        _ = await _enqueue(
            database,
            refresh_ebay_search_work(
                identifier="second-refresh",
                payload=RefreshEbaySearchPayload(
                    base_search_run_record_identifier="second-refresh",
                    search_work_identifier="unused-second",
                    search=EbaySearchRequest(query="eyepiece"),
                    maximum_images=1,
                ),
            ),
            identifiers,
        )
        started, release = anyio.Event(), anyio.Event()
        publish = database.publish_leased_operation_checkpoint

        async def pause_first_binding(**kwargs: Any) -> None:
            if kwargs["work_item_identifier"] == "refresh" and any(
                record.kind == ("carl", "ebay", "image_result") for record in kwargs["records"]
            ):
                started.set()
                await release.wait()
            await publish(**kwargs)

        monkeypatch.setattr(database, "publish_leased_operation_checkpoint", pause_first_binding)

        async def first_root() -> None:
            first = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
            assert first["state"] == "completed"

        with anyio.fail_after(10):
            async with anyio.create_task_group() as tasks:
                _ = tasks.start_soon(first_root)
                await started.wait()
                _ = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
                second = await _run_next(
                    database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND
                )
                assert second["state"] == "completed"
                release.set()
        assert (
            len(await database.records_by_kind(("carl", "ebay", "gallery_image_reference"))) == 2
        )  # source + shared reference
        results = await database.records_by_kind(("carl", "ebay", "image_result"))
        assert len(results) == 3
        assert len({identifier for identifier, _ in results}) == 3
        assert len((await select_ebay_analysis_evidence(database, observations[0])).included) == 1
        assert len(images.plans) == 1
