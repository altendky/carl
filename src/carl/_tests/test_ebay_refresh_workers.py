"""Offline end-to-end parity and restart checkpoints for eBay refresh."""

# Reuse the existing offline test fixtures, which deliberately remain private.
# pyright: reportPrivateUsage=false

from pathlib import Path
from typing import cast

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
)
from carl.core.components import Component, ComponentId
from carl.core.ebay import (
    COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_EBAY_SEARCH_WORK_KIND,
    CollectEbaySearchPayload,
    EbaySearchRequest,
)
from carl.core.ebay_items import (
    COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    COLLECT_EBAY_IMAGE_WORK_KIND,
    COLLECT_EBAY_ITEM_WORK_KIND,
    EXTRACT_EBAY_ITEM_WORK_KIND,
    CollectEbayItemPayload,
)
from carl.core.ebay_refresh import (
    REFRESH_EBAY_SEARCH_WORK_KIND,
    RefreshEbaySearchPayload,
    refresh_ebay_search_work,
)
from carl.core.models import NamedOutput, RecordDraft
from carl.core.refresh_recovery import RetryItemFailuresRequest
from carl.core.work import WorkCapability
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.ebay_analysis_workers import select_ebay_analysis_evidence
from carl.ebay_refresh_workers import (
    EbayRefreshWorkerDependencies,
    build_ebay_refresh_worker_registry,
    ebay_refresh_child_work_states,
)
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry
from carl.review import ReviewApplication


def _source_records(run: str, items: tuple[str, ...]) -> tuple[RecordDraft, ...]:
    return (
        RecordDraft(
            identifier=run,
            kind=("carl", "ebay", "search_run"),
            schema_version=1,
            value={
                "request": EbaySearchRequest(query="eyepiece").model_dump(mode="json"),
                "listing_occurrence_record_identifiers": [
                    f"{run}-occurrence-{index}" for index in range(len(items))
                ],
            },
        ),
        *(
            RecordDraft(
                identifier=f"{run}-occurrence-{index}",
                kind=("carl", "ebay", "search_listing_occurrence"),
                schema_version=1,
                value={"item_identifier": item},
            )
            for index, item in enumerate(items)
        ),
    )


def _anchor() -> None:
    pass


def _search_handler(
    *, fail: bool = False, run: str = "fresh"
) -> TypedWorkHandler[CollectEbaySearchPayload]:
    async def search(
        _payload: CollectEbaySearchPayload, _context: AttemptContext
    ) -> CompletedWork | TerminalFailureWork:
        if fail:
            return TerminalFailureWork(error={"kind": "blocked"}, result={"state": "failed"})
        records = _source_records(run, (SECOND_ITEM, ITEM, SECOND_ITEM))
        return CompletedWork(
            records=records,
            outputs=tuple(
                NamedOutput(name=("record", str(index)), object_identifier=record.identifier)
                for index, record in enumerate(records)
            ),
            result={"search_run_record_identifier": run},
        )

    return TypedWorkHandler(
        capability=WorkCapability(
            kind=COLLECT_EBAY_SEARCH_WORK_KIND,
            payload_schema_version=COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
        ),
        component=Component(ComponentId(("test", "ebay", "refresh", "search")), 1, _anchor),
        payload_type=CollectEbaySearchPayload,
        handler=search,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("shared_image", (True, False, "cdn"))
async def test_refresh_waits_for_details_descriptions_and_globally_bounded_images(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    shared_image: bool | str,
) -> None:
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    second_url = f"https://www.ebay.com/itm/{SECOND_ITEM}"
    second_description = DESCRIPTION_URL.replace(ITEM, SECOND_ITEM)
    second_image = IMAGE_URL if shared_image else IMAGE_URL.replace("item", "other")
    if shared_image == "cdn":
        second_image = IMAGE_URL.replace("i.ebayimg.com", "thumbs.ebayimg.com")
    pages = _Acquirer(
        {
            ITEM_URL: _Response(_item_html()),
            second_url: _Response(
                _item_html()
                .replace(ITEM.encode(), SECOND_ITEM.encode())
                .replace(IMAGE_URL.encode(), second_image.encode())
            ),
            DESCRIPTION_URL: _Response(b"<p>First description</p>"),
            second_description: _Response(b"<p>Second description</p>"),
        }
    )
    images = _Acquirer(
        {IMAGE_URL: _Response(_png(), "image/png"), second_image: _Response(_png(), "image/png")}
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(database, _source_records("base", (ITEM,)), identifiers)

        def registry() -> WorkHandlerRegistry:
            return WorkHandlerRegistry(
                handlers=(
                    _search_handler(),
                    *build_ebay_refresh_worker_registry(
                        EbayRefreshWorkerDependencies(database, identifiers)
                    ).handlers,
                    *_registry(
                        database, tmp_path, identifiers, item_acquirer=pages, image_acquirer=images
                    ).handlers,
                )
            )

        payload = RefreshEbaySearchPayload(
            base_search_run_record_identifier="base",
            search_work_identifier="search",
            search=EbaySearchRequest(query="eyepiece"),
            maximum_items=2,
            maximum_images=1,
        )
        refresh = await _enqueue(
            database, refresh_ebay_search_work(identifier="refresh", payload=payload), identifiers
        )
        first = await _run_next(database, registry(), identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert _mapping(first["result"])["stage"] == "collecting_search"
        _ = await _run_next(database, registry(), identifiers, COLLECT_EBAY_SEARCH_WORK_KIND)
        _ = await _run_next(database, registry(), identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        _ = await _run_next(database, registry(), identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        for _ in range(2):
            collected = await _run_next(
                database, registry(), identifiers, COLLECT_EBAY_ITEM_WORK_KIND
            )
            assert _mapping(_mapping(collected["payload"])["request"])["maximum_images"] == 0
        for _ in range(2):
            _ = await _run_next(database, registry(), identifiers, EXTRACT_EBAY_ITEM_WORK_KIND)
        waiting = await _run_next(database, registry(), identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert _mapping(waiting["result"])["stage"] == "extracting_items"
        assert tuple(
            state.value
            for state in (await ebay_refresh_child_work_states(database, refresh))["descriptions"]
        ) == ("pending", "pending")
        assert not images.plans
        for _ in range(2):
            _ = await _run_next(
                database, registry(), identifiers, COLLECT_EBAY_DESCRIPTION_WORK_KIND
            )
        _ = await _run_next(database, registry(), identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        _ = await _run_next(database, registry(), identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert len(await database.records_by_kind(("carl", "ebay", "image_followup_plan"))) == 1
        _ = await _run_next(database, registry(), identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        # Rebuilding the registry simulates restart; fixed plan survives and the
        # duplicate URL is subsequently reused for the other item relationship.
        if shared_image:
            _ = await _run_next(database, registry(), identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
            _ = await _run_next(database, registry(), identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        completed = await _run_next(
            database, registry(), identifiers, REFRESH_EBAY_SEARCH_WORK_KIND
        )
        assert completed["state"] == "completed"
        result = _mapping(completed["result"])
        assert result["state"] == "completed"
        assert result["selected_unique_listings"] == 2
        assert result["new_listing_identifiers"] == [SECOND_ITEM]
        assert result["new_images_saved"] == (2 if shared_image else 1)
        assert len(images.plans) == 1
        states = await ebay_refresh_child_work_states(database, refresh)
        assert len(states["item_pages"]) == len(states["item_extractions"]) == 2
        assert len(states["images"]) == (2 if shared_image else 1)
        assert not states["image_extractions"]
        assert len(states["descriptions"]) == 2
        assert len(await database.records_by_kind(("carl", "ebay", "image_followup_plan"))) == 1


@pytest.mark.anyio
async def test_refresh_search_failure_stops_before_item_collection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(database, _source_records("base", (ITEM,)), identifiers)
        registry = WorkHandlerRegistry(
            handlers=(
                _search_handler(fail=True),
                *build_ebay_refresh_worker_registry(
                    EbayRefreshWorkerDependencies(database, identifiers)
                ).handlers,
            )
        )
        _ = await _enqueue(
            database,
            refresh_ebay_search_work(
                identifier="refresh",
                payload=RefreshEbaySearchPayload(
                    base_search_run_record_identifier="base",
                    search_work_identifier="search",
                    search=EbaySearchRequest(query="eyepiece"),
                ),
            ),
            identifiers,
        )
        _ = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        _ = await _run_next(database, registry, identifiers, COLLECT_EBAY_SEARCH_WORK_KIND)
        failed = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        assert failed["state"] == "terminal_failure"
        assert _mapping(failed["error"])["kind"] == "refresh_search_collection_failed"
        assert not await database.records_by_kind(("carl", "ebay", "listing_observation"))


def test_refresh_deduplication_excludes_generated_child_identifier() -> None:
    payload = RefreshEbaySearchPayload(
        base_search_run_record_identifier="base",
        search_work_identifier="search1",
        search=EbaySearchRequest(query="eyepiece"),
    )
    changed = payload.model_copy(update={"search_work_identifier": "search2"})
    assert (
        refresh_ebay_search_work(identifier="one", payload=payload).deduplication_identity
        == refresh_ebay_search_work(identifier="two", payload=changed).deduplication_identity
    )


@pytest.mark.anyio
async def test_retry_failed_item_finishes_same_refresh_without_repeating_search(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    budgets: list[int] = []

    async def failed_item(
        _payload: CollectEbayItemPayload, context: AttemptContext
    ) -> TerminalFailureWork:
        budgets.append(context.retry_attempt())
        return TerminalFailureWork(
            error={"kind": "ebay_acquisition_failure"},
            result={"acquisition": {"stopping_condition": "transport_failure"}},
        )

    pages = _Acquirer(
        {
            f"https://www.ebay.com/itm/{SECOND_ITEM}": _Response(
                _item_html().replace(ITEM.encode(), SECOND_ITEM.encode())
            ),
            DESCRIPTION_URL.replace(ITEM, SECOND_ITEM): _Response(b"<p>Restored description</p>"),
        }
    )
    images = _Acquirer({IMAGE_URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(database, _source_records("base", (ITEM,)), identifiers)
        refresh_handlers = build_ebay_refresh_worker_registry(
            EbayRefreshWorkerDependencies(database, identifiers)
        ).handlers
        registry = WorkHandlerRegistry(
            handlers=(
                _search_handler(),
                *refresh_handlers,
                TypedWorkHandler(
                    capability=WorkCapability(
                        kind=COLLECT_EBAY_ITEM_WORK_KIND, payload_schema_version=1
                    ),
                    component=Component(
                        ComponentId(("test", "ebay", "failed_item")), 1, failed_item
                    ),
                    payload_type=CollectEbayItemPayload,
                    handler=failed_item,
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
                    maximum_items=1,
                    maximum_images=1,
                ),
            ),
            identifiers,
        )
        for kind in (
            REFRESH_EBAY_SEARCH_WORK_KIND,
            COLLECT_EBAY_SEARCH_WORK_KIND,
            REFRESH_EBAY_SEARCH_WORK_KIND,
            REFRESH_EBAY_SEARCH_WORK_KIND,
            COLLECT_EBAY_ITEM_WORK_KIND,
            REFRESH_EBAY_SEARCH_WORK_KIND,
            REFRESH_EBAY_SEARCH_WORK_KIND,
        ):
            await _run_next(database, registry, identifiers, kind)
        assert (await database.work("refresh"))["state"] == "terminal_failure"
        app = ReviewApplication(database, tmp_path, new_identifier=identifiers)
        recovered = await app.retry_item_failures(
            RetryItemFailuresRequest(refresh_work_identifier="refresh")
        )
        assert recovered.retried == 1
        registry = WorkHandlerRegistry(
            handlers=(
                _search_handler(),
                *refresh_handlers,
                *_registry(
                    database, tmp_path, identifiers, item_acquirer=pages, image_acquirer=images
                ).handlers,
            )
        )
        for kind in (
            REFRESH_EBAY_SEARCH_WORK_KIND,
            COLLECT_EBAY_ITEM_WORK_KIND,
            EXTRACT_EBAY_ITEM_WORK_KIND,
            COLLECT_EBAY_DESCRIPTION_WORK_KIND,
            REFRESH_EBAY_SEARCH_WORK_KIND,
            REFRESH_EBAY_SEARCH_WORK_KIND,
            COLLECT_EBAY_IMAGE_WORK_KIND,
            REFRESH_EBAY_SEARCH_WORK_KIND,
        ):
            await _run_next(database, registry, identifiers, kind)
        final = await database.work("refresh")
        assert final["state"] == "completed"
        assert _mapping(final["result"])["item_failures"] == 0
        assert _mapping(final["result"])["new_images_saved"] == 1
        assert len((await database.work("search"))["operations"]) == 1
        assert len(await database.records_by_kind(("carl", "ebay", "image_followup_plan"))) == 2
        assert len(pages.plans) == 2
        assert budgets == [1]


@pytest.mark.anyio
async def test_completed_challenge_extraction_is_counted_as_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    pages = _Acquirer(
        {
            f"https://www.ebay.com/itm/{SECOND_ITEM}": _Response(
                b"<html>Pardon our interruption</html>"
            )
        }
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(database, _source_records("base", (ITEM,)), identifiers)
        registry = WorkHandlerRegistry(
            handlers=(
                _search_handler(),
                *build_ebay_refresh_worker_registry(
                    EbayRefreshWorkerDependencies(database, identifiers)
                ).handlers,
                *_registry(database, tmp_path, identifiers, item_acquirer=pages).handlers,
            )
        )
        _ = await _enqueue(
            database,
            refresh_ebay_search_work(
                identifier="refresh",
                payload=RefreshEbaySearchPayload(
                    base_search_run_record_identifier="base",
                    search_work_identifier="search",
                    search=EbaySearchRequest(query="eyepiece"),
                    maximum_items=1,
                ),
            ),
            identifiers,
        )
        for kind in (
            REFRESH_EBAY_SEARCH_WORK_KIND,
            COLLECT_EBAY_SEARCH_WORK_KIND,
            REFRESH_EBAY_SEARCH_WORK_KIND,
            REFRESH_EBAY_SEARCH_WORK_KIND,
            COLLECT_EBAY_ITEM_WORK_KIND,
            EXTRACT_EBAY_ITEM_WORK_KIND,
            REFRESH_EBAY_SEARCH_WORK_KIND,
        ):
            _ = await _run_next(database, registry, identifiers, kind)
        completed = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        result = _mapping(completed["result"])
        assert completed["state"] == "terminal_failure"
        assert result["state"] == "failed"
        assert _mapping(completed["error"])["kind"] == "mostly_failed_refresh"
        assert result["item_failures"] == 1
        assert result["new_images_saved"] == 0
        assert not await database.records_by_kind(("carl", "ebay", "gallery_image_reference"))


@pytest.mark.anyio
@pytest.mark.parametrize("stack", ("ebay_anonymous", "different"))
async def test_item_phase_reuses_only_usable_matching_stack_pages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stack: str
) -> None:
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                *_source_records("base", (ITEM,)),
                RecordDraft(
                    identifier="old-acquisition",
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value={},
                ),
                RecordDraft(
                    identifier="old-observation",
                    kind=("carl", "ebay", "listing_observation"),
                    schema_version=1,
                    value={
                        "item_identifier": SECOND_ITEM,
                        "classification": "detail",
                        "acquisition_record_identifier": "old-acquisition",
                        "request": {"stack_identifier": stack},
                    },
                ),
            ),
            identifiers,
        )
        registry = WorkHandlerRegistry(
            handlers=(
                _search_handler(),
                *build_ebay_refresh_worker_registry(
                    EbayRefreshWorkerDependencies(database, identifiers)
                ).handlers,
            )
        )
        _ = await _enqueue(
            database,
            refresh_ebay_search_work(
                identifier="refresh",
                payload=RefreshEbaySearchPayload(
                    base_search_run_record_identifier="base",
                    search_work_identifier="search",
                    search=EbaySearchRequest(query="eyepiece"),
                    maximum_items=1,
                ),
            ),
            identifiers,
        )
        for kind in (
            REFRESH_EBAY_SEARCH_WORK_KIND,
            COLLECT_EBAY_SEARCH_WORK_KIND,
            REFRESH_EBAY_SEARCH_WORK_KIND,
        ):
            _ = await _run_next(database, registry, identifiers, kind)
        collecting = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
        child = await database.work(
            str(_mapping(collecting["result"])["item_collection_work_identifiers"][0])
        )
        expected = (
            EXTRACT_EBAY_ITEM_WORK_KIND
            if stack == "ebay_anonymous"
            else COLLECT_EBAY_ITEM_WORK_KIND
        )
        assert child["kind"] == list(expected)


@pytest.mark.anyio
async def test_two_completed_refreshes_have_distinct_observations_and_valid_analysis_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    url = f"https://www.ebay.com/itm/{SECOND_ITEM}"
    description_url = DESCRIPTION_URL.replace(ITEM, SECOND_ITEM)
    pages = _Acquirer(
        {
            url: _Response(_item_html().replace(ITEM.encode(), SECOND_ITEM.encode())),
            description_url: _Response(b"<p>Seller description</p>"),
        }
    )
    images = _Acquirer({IMAGE_URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(database, _source_records("base", (ITEM,)), identifiers)
        observations: list[str] = []
        for round_number in (1, 2):
            registry = WorkHandlerRegistry(
                handlers=(
                    _search_handler(run=f"fresh-{round_number}"),
                    *build_ebay_refresh_worker_registry(
                        EbayRefreshWorkerDependencies(database, identifiers)
                    ).handlers,
                    *_registry(
                        database, tmp_path, identifiers, item_acquirer=pages, image_acquirer=images
                    ).handlers,
                )
            )
            _ = await _enqueue(
                database,
                refresh_ebay_search_work(
                    identifier=f"refresh-{round_number}",
                    payload=RefreshEbaySearchPayload(
                        base_search_run_record_identifier="base",
                        search_work_identifier=f"search-{round_number}",
                        search=EbaySearchRequest(query="eyepiece"),
                        maximum_items=1,
                        maximum_images=1,
                    ),
                ),
                identifiers,
            )
            for kind in (
                REFRESH_EBAY_SEARCH_WORK_KIND,
                COLLECT_EBAY_SEARCH_WORK_KIND,
                REFRESH_EBAY_SEARCH_WORK_KIND,
                REFRESH_EBAY_SEARCH_WORK_KIND,
            ):
                _ = await _run_next(database, registry, identifiers, kind)
            if round_number == 1:
                _ = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
            _ = await _run_next(database, registry, identifiers, EXTRACT_EBAY_ITEM_WORK_KIND)
            _ = await _run_next(database, registry, identifiers, COLLECT_EBAY_DESCRIPTION_WORK_KIND)
            _ = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
            _ = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
            _ = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
            completed = await _run_next(
                database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND
            )
            assert completed["state"] == "completed"
            result = _mapping(completed["result"])
            assert result["state"] == "completed"
            identifiers_value = result["observation_record_identifiers"]
            assert isinstance(identifiers_value, list)
            observation = cast(list[str], identifiers_value)[0]
            observations.append(observation)
            selection = await select_ebay_analysis_evidence(database, observation)
            assert len(selection.included) == 1
            assert not selection.unavailable and not selection.unretained_gallery_orders
        assert observations[0] != observations[1]
        assert len(images.plans) == 1
        assert len([plan for plan in pages.plans if plan.url == url]) == 1


@pytest.mark.anyio
async def test_image_phase_reuses_reference_when_coordinators_share_one_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("carl.ebay_refresh_workers._WAIT_NS", 0)
    identifiers = _identifiers()
    images = _Acquirer({IMAGE_URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier="shared-observation",
                    kind=("carl", "ebay", "listing_observation"),
                    schema_version=1,
                    value={
                        "item_identifier": SECOND_ITEM,
                        "classification": "detail",
                        "gallery_urls": [IMAGE_URL],
                    },
                ),
            ),
            identifiers,
        )
        coordinator = build_ebay_refresh_worker_registry(
            EbayRefreshWorkerDependencies(database, identifiers)
        ).handlers[0]

        async def start_at_images(
            payload: RefreshEbaySearchPayload, context: AttemptContext
        ) -> WorkOutcome:
            if (await database.work(context.work_item_identifier))["result"] is None:
                # Concurrent requests may share one pending extraction, and
                # thus arrive at the image stage with the same observation.
                return RetryWork(
                    delay_ns=0,
                    reason={},
                    result={
                        "stage": "items_complete",
                        "observation_record_identifiers": ["shared-observation"],
                        "item_failures": 0,
                        "description_failures": 0,
                    },
                )
            return await coordinator.execute(payload.model_dump(mode="json"), context)

        registry = WorkHandlerRegistry(
            handlers=(
                TypedWorkHandler(
                    capability=coordinator.capability,
                    component=coordinator.component,
                    payload_type=RefreshEbaySearchPayload,
                    handler=start_at_images,
                ),
                *_registry(database, tmp_path, identifiers, image_acquirer=images).handlers,
            )
        )
        for round_number in (1, 2):
            _ = await _enqueue(
                database,
                refresh_ebay_search_work(
                    identifier=f"refresh-{round_number}",
                    payload=RefreshEbaySearchPayload(
                        base_search_run_record_identifier="unused",
                        search_work_identifier=f"unused-{round_number}",
                        search=EbaySearchRequest(query="eyepiece"),
                        maximum_images=1,
                    ),
                ),
                identifiers,
            )
            _ = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
            _ = await _run_next(database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND)
            _ = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
            completed = await _run_next(
                database, registry, identifiers, REFRESH_EBAY_SEARCH_WORK_KIND
            )
            assert completed["state"] == "completed"
            assert (
                len((await select_ebay_analysis_evidence(database, "shared-observation")).included)
                == 1
            )
        assert len(await database.records_by_kind(("carl", "ebay", "gallery_image_reference"))) == 1
        assert len(images.plans) == 1
