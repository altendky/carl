"""Offline durable eBay item, description, and gallery-image worker tests."""

import json
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from itertools import count
from pathlib import Path
from time import perf_counter_ns, time_ns
from types import SimpleNamespace
from typing import cast

import apsw
import pytest
from PIL import Image

import carl.ebay_item_workers as ebay_item_workers
from carl.core.components import Component, ComponentId
from carl.core.ebay_items import (
    COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    COLLECT_EBAY_IMAGE_WORK_KIND,
    COLLECT_EBAY_ITEM_WORK_KIND,
    EXTRACT_EBAY_ITEM_WORK_KIND,
    CollectEbayDescriptionPayload,
    CollectEbayImagePayload,
    CollectEbayItemPayload,
    EbayItemRequest,
    collect_ebay_description_work,
    collect_ebay_image_work,
    collect_ebay_item_work,
)
from carl.core.http import RequestPlan
from carl.core.marketplace_images import MARKETPLACE_IMAGE_SCOPE
from carl.core.models import CodeProvenance, JsonValue, NamedOutput, RecordDraft
from carl.core.work import WorkDefinition, WorkRequester, WorkState
from carl.core.worker import AttemptContext, RetryWork, TerminalFailureWork, WorkerSettings
from carl.ebay_item_workers import EbayItemWorkerDependencies, build_ebay_item_worker_registry
from carl.io.facebook_images import FacebookImageHttpSession, FacebookImageSessionFailure
from carl.io.httpx import AcquiredBody, Acquisition, AcquisitionFailure, IdentifierFactory
from carl.io.paths import CarlDirectories
from carl.io.proton import ProtonSession, ProtonSessionManager
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, WorkHandlerRegistry, execute_lease

ITEM = "123456789012"
SECOND_ITEM = "223456789012"
ITEM_URL = f"https://www.ebay.com/itm/{ITEM}"
IMAGE_URL = "https://i.ebayimg.com/images/g/item/s-l1600.png"
DESCRIPTION_URL = f"https://vi.vipr.ebaydesc.com/itmdesc/{ITEM}"
ITEM_ROUTE = ("decodo", "personal", "carl")
IMAGE_ROUTE = ("proton", "personal", "carl")


def _mapping(value: JsonValue) -> dict[str, JsonValue]:
    assert isinstance(value, dict)
    return value


def _string(value: JsonValue) -> str:
    assert isinstance(value, str)
    return value


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="dirty",
        package_version="test",
        python_implementation="test",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


async def _async_provenance() -> CodeProvenance:
    return _provenance()


def _identifiers() -> Callable[[], str]:
    values = count()
    return lambda: f"ebay-item-test-{next(values)}"


def _png() -> bytes:
    stream = BytesIO()
    with Image.new("RGB", (4, 3), color=(12, 34, 56)) as image:
        image.save(stream, format="PNG")
    return stream.getvalue()


def _item_html() -> bytes:
    return (
        f'<html><link rel="canonical" href="{ITEM_URL}">'
        '<h1 class="x-item-title__mainTitle">Workshop vise</h1>'
        '<div class="x-price-primary">US $35.00</div>'
        '<div class="x-item-condition-text">Used</div>'
        '<div class="ux-image-carousel">'
        f'<img src="{IMAGE_URL}"><img src="{IMAGE_URL}"></div>'
        '<div class="recommendations"><img src="https://i.ebayimg.com/unrelated.png"></div>'
        f'<iframe id="desc_ifr" src="{DESCRIPTION_URL}"></iframe></html>'
    ).encode()


@dataclass(frozen=True)
class _Response:
    content: bytes
    media_type: str = "text/html; charset=utf-8"
    status_code: int = 200
    effective_url: str | None = None
    complete: bool = True


class _Acquirer:
    def __init__(self, responses: dict[str, _Response], *, fail_transport: bool = False):
        self.responses = responses
        self.fail_transport = fail_transport
        self.plans: list[RequestPlan] = []
        self.body_identifiers: list[str] = []

    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        self.plans.append(plan)
        response = self.responses[plan.url]
        body_identifier = new_identifier()
        self.body_identifiers.append(body_identifier)
        record: dict[str, JsonValue] = {
            "effective_url": response.effective_url or plan.url,
            "routing": {"configured": list(plan.routing)},
            "stopping_condition": "terminal_response",
            "hops": [
                {
                    "request": {"url": plan.url},
                    "response": {
                        "status_code": response.status_code,
                        "headers": [
                            {"name_latin1": "Content-Type", "value_latin1": response.media_type}
                        ],
                        "body": (
                            {
                                "state": "available",
                                "artifact_id": body_identifier,
                                "bytes": len(response.content),
                            }
                            if response.complete
                            else {"state": "unavailable", "reason": "incomplete_body"}
                        ),
                    },
                }
            ],
        }
        bodies = (
            (
                AcquiredBody(
                    identifier=body_identifier,
                    media_type=response.media_type,
                    representation={"kind": "content_decoded_http_body", "content_decoded": True},
                    content=response.content,
                ),
            )
            if response.complete
            else ()
        )
        if self.fail_transport:
            record["stopping_condition"] = "transport_failure"
            raise AcquisitionFailure("test transport failure", result=record, bodies=bodies)
        return Acquisition(record=record, bodies=bodies)


def _registry(
    database: Database,
    root: Path,
    identifiers: Callable[[], str],
    *,
    item_acquirer: _Acquirer | None = None,
    image_acquirer: _Acquirer | None = None,
) -> WorkHandlerRegistry:
    return build_ebay_item_worker_registry(
        EbayItemWorkerDependencies(
            database=database,
            directories=CarlDirectories(
                config=root / "config",
                data=root / "data",
                cache=root / "cache",
                state=root / "state",
                runtime=root / "runtime",
            ),
            new_identifier=identifiers,
            proton_manager=cast(ProtonSessionManager, object()),
            item_acquirer=item_acquirer,
            image_acquirer=image_acquirer,
        )
    )


async def _enqueue(
    database: Database, definition: WorkDefinition, identifiers: Callable[[], str]
) -> str:
    enqueued = await database.enqueue_work(
        definition,
        WorkRequester(
            request_identifier=identifiers(),
            kind=("test", "ebay", "request"),
            identifier=definition.identifier,
            context={},
        ),
        event_identifier=identifiers(),
        enqueued_at_utc_ns=time_ns(),
    )
    return enqueued.work_item_identifier


async def _run_next(
    database: Database,
    registry: WorkHandlerRegistry,
    identifiers: Callable[[], str],
    kind: tuple[str, ...],
) -> dict[str, JsonValue]:
    settings = WorkerSettings(
        worker_count=1,
        lease_duration_ns=60_000_000_000,
        renewal_interval_ns=10_000_000_000,
        idle_poll_interval_ns=1_000_000,
    )
    claim = await database.claim_work(
        supported_capabilities=tuple(
            capability for capability in registry.capabilities if capability.kind == kind
        ),
        worker_identifier=identifiers(),
        lease_token=identifiers(),
        lease_duration_ns=settings.lease_duration_ns,
        utc_now_ns=time_ns,
        event_identifier=identifiers(),
    )
    assert claim.lease is not None
    await execute_lease(
        database=database,
        registry=registry,
        settings=settings,
        services=WorkerRuntimeServices(
            new_identifier=identifiers,
            utc_now_ns=time_ns,
            monotonic_ns=perf_counter_ns,
            code_provenance=_async_provenance,
            invocation=lambda: {},
        ),
        lease=claim.lease,
    )
    return await database.work(claim.lease.work_item_identifier)


async def _retain_records(
    database: Database, records: tuple[RecordDraft, ...], identifiers: Callable[[], str]
) -> None:
    operation_identifier = identifiers()
    await database.begin_operation(
        operation_id=operation_identifier,
        component=Component(ComponentId(("test", "ebay", "source")), 1, _png),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc=datetime.now(UTC).isoformat(),
    )
    await database.complete_operation(
        operation_id=operation_identifier,
        records=records,
        artifacts=(),
        outputs=tuple(
            NamedOutput(name=("record", str(index)), object_identifier=record.identifier)
            for index, record in enumerate(records)
        ),
        result={},
        ended_at_utc=datetime.now(UTC).isoformat(),
        duration_ns=1,
    )


def _reference(
    identifier: str = "reference-1", *, item: str = ITEM, observation: str = "observation-1"
) -> RecordDraft:
    return RecordDraft(
        identifier=identifier,
        kind=("carl", "ebay", "gallery_image_reference"),
        schema_version=1,
        value={
            "item_identifier": item,
            "observation_record_identifier": observation,
            "acquisition_record_identifier": "item-acquisition-1",
            "url": IMAGE_URL,
            "gallery_order": 0,
        },
    )


def _image_payload(
    reference: str = "reference-1", *, item: str = ITEM, observation: str = "observation-1"
) -> CollectEbayImagePayload:
    return CollectEbayImagePayload(
        item_identifier=item,
        observation_record_identifier=observation,
        reference_record_identifier=reference,
        url=IMAGE_URL,
    )


@pytest.mark.anyio
async def test_durable_item_pipeline_retains_html_and_schedules_description_and_images(
    tmp_path: Path,
) -> None:
    identifiers = _identifiers()
    html = _item_html()
    description = b"<html><body><p>Seller says: smooth screw and original jaws.</p></body></html>"
    image = _png()
    pages = _Acquirer({ITEM_URL: _Response(html), DESCRIPTION_URL: _Response(description)})
    images = _Acquirer({IMAGE_URL: _Response(image, "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(
            database, tmp_path, identifiers, item_acquirer=pages, image_acquirer=images
        )
        work_identifier = await _enqueue(
            database,
            collect_ebay_item_work(
                identifier=identifiers(),
                payload=CollectEbayItemPayload(request=EbayItemRequest(item_identifier=ITEM)),
            ),
            identifiers,
        )
        collected = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        assert collected["state"] == WorkState.COMPLETED.value
        assert await database.work_state(work_identifier) == WorkState.COMPLETED
        acquisition_identifier = _string(
            _mapping(collected["result"])["acquisition_record_identifier"]
        )
        _, _, acquisition = await database.get_record(acquisition_identifier)
        assert _mapping(acquisition)["purpose"] == "ebay_item"
        metadata, retained_html = await database.get_artifact(pages.body_identifiers[0])
        assert retained_html == html
        assert _mapping(metadata["representation"])["content_decoded"] is True
        extraction_identifier = _string(_mapping(collected["result"])["extraction_work_identifier"])
        assert await database.work_state(extraction_identifier) == WorkState.PENDING
        assert pages.plans[0].routing == ITEM_ROUTE

        extracted = await _run_next(database, registry, identifiers, EXTRACT_EBAY_ITEM_WORK_KIND)
        assert extracted["state"] == WorkState.COMPLETED.value
        result = _mapping(extracted["result"])
        assert result["classification"] == "detail"
        observation_identifier = _string(result["observation_record_identifier"])
        _, _, value = await database.get_record(observation_identifier)
        observation = _mapping(value)
        assert observation["title"] == "Workshop vise"
        assert observation["acquisition_record_identifier"] == acquisition_identifier
        assert observation["gallery_urls"] == [IMAGE_URL]
        decoded_identifier = _string(observation["decoded_body_artifact_identifier"])
        _, decoded_html = await database.get_artifact(decoded_identifier)
        assert decoded_html == html
        assert len(cast(list[JsonValue], result["image_work_identifiers"])) == 1
        assert len(cast(list[JsonValue], result["description_work_identifiers"])) == 1
        references = await database.records_by_kind(("carl", "ebay", "gallery_image_reference"))
        assert len(references) == 1
        assert _mapping(references[0][1])["observation_record_identifier"] == observation_identifier

        described = await _run_next(
            database, registry, identifiers, COLLECT_EBAY_DESCRIPTION_WORK_KIND
        )
        assert described["state"] == WorkState.COMPLETED.value
        descriptions = await database.records_by_kind(("carl", "ebay", "description_result"))
        assert len(descriptions) == 1
        assert _mapping(descriptions[0][1])["description"] == (
            "Seller says: smooth screw and original jaws."
        )
        assert (
            _mapping(descriptions[0][1])["observation_record_identifier"] == observation_identifier
        )
        assert (await database.get_artifact(pages.body_identifiers[1]))[1] == description
        assert pages.plans[1].routing == ITEM_ROUTE
        assert pages.plans[1].follow_redirects is False

        imaged = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        assert imaged["state"] == WorkState.COMPLETED.value
        tagged_kinds: list[tuple[str, ...]] = []
        with apsw.Connection(str(database.path), flags=apsw.SQLITE_OPEN_READONLY) as connection:
            tagged_kinds = [
                tuple(json.loads(kind))
                for kind, scope in connection.execute(
                    """
                    SELECT a.kind_parts_json,s.scope_identity_json
                    FROM network_activities AS a
                    JOIN network_activity_scopes AS s ON s.network_activity_id=a.id
                    WHERE s.scope_kind='network_activity_kind'
                    """
                )
                if tuple(json.loads(scope)) == MARKETPLACE_IMAGE_SCOPE.identity
            ]
        assert tagged_kinds == [("carl", "ebay", "network_activity", "gallery_image")]
        image_result = _mapping(imaged["result"])
        artifact_identifier = _string(image_result["image_artifact_identifier"])
        image_metadata, content = await database.get_artifact(artifact_identifier)
        assert content == image
        assert _mapping(image_metadata["storage"])["backend"] == "filesystem"
        assert image_metadata["media_type"] == "image/png"
        assert len(tuple((tmp_path / "images").rglob("*.png"))) == 1
        with pytest.raises(KeyError):
            await database.get_artifact(images.body_identifiers[0])
        saved_images = await database.records_by_kind(("carl", "ebay", "image_result"))
        saved = _mapping(saved_images[0][1])
        assert (saved["width"], saved["height"]) == (4, 3)
        assert saved["reference_record_identifier"] == references[0][0]
        _, _, image_acquisition = await database.get_record(
            _string(saved["acquisition_record_identifier"])
        )
        hops = cast(list[JsonValue], _mapping(image_acquisition)["hops"])
        body = _mapping(_mapping(_mapping(hops[-1])["response"])["body"])
        assert body["artifact_id"] == artifact_identifier
        assert body["retained_as"] == "validated_image_file"
        assert images.plans[0].routing == IMAGE_ROUTE
        assert images.plans[0].follow_redirects is False


@pytest.mark.anyio
async def test_exact_url_image_reuse_preserves_new_listing_reference(tmp_path: Path) -> None:
    identifiers = _identifiers()
    images = _Acquirer({IMAGE_URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, image_acquirer=images)
        await _retain_records(
            database,
            (
                _reference(),
                _reference("reference-2", item=SECOND_ITEM, observation="observation-2"),
            ),
            identifiers,
        )
        await _enqueue(
            database,
            collect_ebay_image_work(identifier=identifiers(), payload=_image_payload()),
            identifiers,
        )
        first = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        assert first["state"] == WorkState.COMPLETED.value
        await _enqueue(
            database,
            collect_ebay_image_work(
                identifier=identifiers(),
                payload=_image_payload(
                    "reference-2", item=SECOND_ITEM, observation="observation-2"
                ),
            ),
            identifiers,
        )
        second = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        assert second["state"] == WorkState.COMPLETED.value
        assert _mapping(second["result"])["reused"] is True
        results = await database.records_by_kind(("carl", "ebay", "image_result"))
        assert len(results) == 2
        first_value, second_value = (_mapping(value) for _, value in results)
        assert second_value["reused_from_result_record_identifier"] == results[0][0]
        assert second_value["reference_record_identifier"] == "reference-2"
        assert second_value["observation_record_identifier"] == "observation-2"
        assert second_value["item_identifier"] == SECOND_ITEM
        assert first_value["reference_record_identifier"] == "reference-1"
        assert first_value["item_identifier"] == ITEM
        assert first_value["image_artifact_identifier"] == second_value["image_artifact_identifier"]
        assert len(images.plans) == 1
        assert len(tuple((tmp_path / "images").rglob("*.png"))) == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    "response",
    (
        _Response(b"not an image", "image/png"),
        _Response(_png(), "text/html"),
        _Response(_png(), "image/png", status_code=404),
        _Response(_png(), "image/png", effective_url=IMAGE_URL + "?different=1"),
        _Response(_png(), "image/png", complete=False),
    ),
    ids=("invalid-content", "nonimage-mime", "http-error", "different-url", "incomplete-body"),
)
async def test_invalid_image_keeps_failure_evidence_without_dangling_body_artifacts(
    tmp_path: Path, response: _Response
) -> None:
    identifiers = _identifiers()
    images = _Acquirer({IMAGE_URL: response})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, image_acquirer=images)
        await _retain_records(database, (_reference(),), identifiers)
        await _enqueue(
            database,
            collect_ebay_image_work(identifier=identifiers(), payload=_image_payload()),
            identifiers,
        )
        work = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        assert work["state"] == WorkState.TERMINAL_FAILURE.value
        results = await database.records_by_kind(("carl", "ebay", "image_result"))
        assert len(results) == 1
        failure = _mapping(results[0][1])
        assert failure["state"] == "failed"
        assert failure["reference_record_identifier"] == "reference-1"
        assert "image_artifact_identifier" not in failure
        _, _, value = await database.get_record(_string(failure["acquisition_record_identifier"]))
        hops = cast(list[JsonValue], _mapping(value)["hops"])
        body = _mapping(_mapping(_mapping(hops[-1])["response"])["body"])
        assert body["state"] == "unavailable"
        assert "artifact_id" not in body
        with pytest.raises(KeyError):
            await database.get_artifact(images.body_identifiers[0])
        assert not (tmp_path / "images").exists()


@pytest.mark.anyio
async def test_transport_retry_retains_complete_prior_hop_body(tmp_path: Path) -> None:
    identifiers = _identifiers()
    pages = _Acquirer({ITEM_URL: _Response(b"complete redirect-hop evidence")}, fail_transport=True)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        await _enqueue(
            database,
            collect_ebay_item_work(
                identifier=identifiers(),
                payload=CollectEbayItemPayload(request=EbayItemRequest(item_identifier=ITEM)),
            ),
            identifiers,
        )
        work = await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        assert work["state"] == WorkState.PENDING.value
        result = _mapping(work["result"])
        assert result["state"] == "acquisition_failed"
        assert _mapping(result["acquisition"])["stopping_condition"] == "transport_failure"
        metadata, content = await database.get_artifact(pages.body_identifiers[0])
        assert content == b"complete redirect-hop evidence"
        assert metadata["media_type"] == "text/html; charset=utf-8"
        assert await database.records_by_kind(("carl", "ebay", "listing_observation")) == ()
        activity_identifier = _string(
            _mapping(result["acquisition"])["network_activity_identifier"]
        )
        activity = await database.network_activity(activity_identifier)
        assert activity["state"] == "failed"
        assert _mapping(activity["result"])["stopping_condition"] == "transport_failure"
        snapshot = await database.activity_snapshot(
            captured_at_utc_ns=time_ns(), recent_window_ns=60_000_000_000, maximum_rows=10
        )
        assert snapshot.network.recent_failed == 1
        assert (
            next(path for path in snapshot.network_paths if path.path == ITEM_ROUTE).recent_failed
            == 1
        )


@pytest.mark.anyio
async def test_zero_image_budget_still_retains_gallery_and_collects_description(
    tmp_path: Path,
) -> None:
    identifiers = _identifiers()
    pages = _Acquirer({ITEM_URL: _Response(_item_html())})
    images = _Acquirer({})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(
            database, tmp_path, identifiers, item_acquirer=pages, image_acquirer=images
        )
        await _enqueue(
            database,
            collect_ebay_item_work(
                identifier=identifiers(),
                payload=CollectEbayItemPayload(
                    request=EbayItemRequest(item_identifier=ITEM, maximum_images=0)
                ),
            ),
            identifiers,
        )
        await _run_next(database, registry, identifiers, COLLECT_EBAY_ITEM_WORK_KIND)
        extracted = await _run_next(database, registry, identifiers, EXTRACT_EBAY_ITEM_WORK_KIND)
        assert extracted["state"] == WorkState.COMPLETED.value
        result = _mapping(extracted["result"])
        assert result["image_work_identifiers"] == []
        assert len(cast(list[JsonValue], result["description_work_identifiers"])) == 1
        _, _, observation = await database.get_record(
            _string(result["observation_record_identifier"])
        )
        assert _mapping(observation)["gallery_urls"] == [IMAGE_URL]
        assert images.plans == []


@pytest.mark.anyio
async def test_image_request_must_match_retained_reference_before_acquisition(
    tmp_path: Path,
) -> None:
    identifiers = _identifiers()
    images = _Acquirer({})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, image_acquirer=images)
        await _retain_records(database, (_reference(),), identifiers)
        await _enqueue(
            database,
            collect_ebay_image_work(
                identifier=identifiers(), payload=_image_payload(item=SECOND_ITEM)
            ),
            identifiers,
        )
        work = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        assert work["state"] == WorkState.TERMINAL_FAILURE.value
        assert images.plans == []
        assert await database.records_by_kind(("carl", "ebay", "image_result")) == ()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "response",
    (
        _Response(b"<p>Do not accept an HTTP error as description.</p>", status_code=404),
        _Response(b"<p>Do not accept a changed URL.</p>", effective_url=DESCRIPTION_URL + "?new=1"),
        _Response(b"<p>Incomplete description.</p>", complete=False),
        _Response(b"<html><body><h1>Pardon our interruption</h1></body></html>"),
        _Response(b"<p>Do not accept non-HTML content as description.</p>", "application/json"),
    ),
    ids=("http-error", "different-url", "incomplete-body", "http-200-challenge", "nonhtml-mime"),
)
async def test_description_failure_retains_available_body_evidence(
    tmp_path: Path, response: _Response
) -> None:
    identifiers = _identifiers()
    pages = _Acquirer({DESCRIPTION_URL: response})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers, item_acquirer=pages)
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier="observation-1",
                    kind=("carl", "ebay", "listing_observation"),
                    schema_version=1,
                    value={"item_identifier": ITEM, "description_url": DESCRIPTION_URL},
                ),
            ),
            identifiers,
        )
        await _enqueue(
            database,
            collect_ebay_description_work(
                identifier=identifiers(),
                payload=CollectEbayDescriptionPayload(
                    request=EbayItemRequest(item_identifier=ITEM),
                    observation_record_identifier="observation-1",
                    url=DESCRIPTION_URL,
                ),
            ),
            identifiers,
        )
        work = await _run_next(database, registry, identifiers, COLLECT_EBAY_DESCRIPTION_WORK_KIND)
        assert work["state"] == WorkState.TERMINAL_FAILURE.value
        results = await database.records_by_kind(("carl", "ebay", "description_result"))
        assert len(results) == 1
        failure = _mapping(results[0][1])
        assert failure["state"] == "failed"
        assert failure["description"] is None
        assert failure["observation_record_identifier"] == "observation-1"
        _, _, acquisition = await database.get_record(
            _string(failure["acquisition_record_identifier"])
        )
        assert _mapping(acquisition)["purpose"] == "ebay_description"
        if response.complete:
            assert (await database.get_artifact(pages.body_identifiers[0]))[1] == response.content
        else:
            with pytest.raises(KeyError):
                await database.get_artifact(pages.body_identifiers[0])


@pytest.mark.anyio
async def test_image_session_cleanup_failure_retries_without_retaining_unvalidated_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifiers = _identifiers()
    images = _Acquirer({IMAGE_URL: _Response(_png(), "image/png")})

    @asynccontextmanager
    async def failed_session(identifier: str) -> AsyncIterator[FacebookImageHttpSession]:
        yield FacebookImageHttpSession(
            identifier=identifier,
            acquirer=images,
            provider_session=cast(ProtonSession, object()),
        )
        raise FacebookImageSessionFailure("test_close_failure", {"state": "closed"})

    def factory(
        *, manager: object, settings: object
    ) -> Callable[[str], AbstractAsyncContextManager[FacebookImageHttpSession]]:
        del manager, settings
        return failed_session

    def unused_settings(*_args: object) -> None:
        return None

    monkeypatch.setattr(
        ebay_item_workers,
        "load_configuration",
        lambda *_args: SimpleNamespace(
            configuration=SimpleNamespace(resolve_network_path=lambda path: path)
        ),
    )
    monkeypatch.setattr(ebay_item_workers, "proton_settings", unused_settings)
    monkeypatch.setattr(ebay_item_workers, "ProtonFacebookImageSessionFactory", factory)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers)
        await _retain_records(database, (_reference(),), identifiers)
        await _enqueue(
            database,
            collect_ebay_image_work(identifier=identifiers(), payload=_image_payload()),
            identifiers,
        )
        work = await _run_next(database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND)
        assert work["state"] == WorkState.PENDING.value
        acquisition = _mapping(_mapping(work["result"])["acquisition"])
        assert acquisition["stopping_condition"] == "transport_failure"
        assert acquisition["failure_phase"] == "session_close"
        hops = cast(list[JsonValue], acquisition["hops"])
        body = _mapping(_mapping(_mapping(hops[-1])["response"])["body"])
        assert body["state"] == "unavailable"
        assert "artifact_id" not in body
        with pytest.raises(KeyError):
            await database.get_artifact(images.body_identifiers[0])
        assert await database.records_by_kind(("carl", "ebay", "image_result")) == ()
        assert not (tmp_path / "images").exists()


@pytest.mark.anyio
@pytest.mark.parametrize("attempt", (1, 2, 3))
async def test_transient_image_session_startup_retries_with_a_bounded_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, attempt: int
) -> None:
    identifiers = _identifiers()
    images = _Acquirer({})

    @asynccontextmanager
    async def failed_session(identifier: str) -> AsyncIterator[FacebookImageHttpSession]:
        if identifier != "never-selected-session":
            raise FacebookImageSessionFailure(
                "wireproxy_startup_timeout", {"state": "startup_failed"}
            )
        yield FacebookImageHttpSession(
            identifier=identifier,
            acquirer=images,
            provider_session=cast(ProtonSession, object()),
        )

    def factory(
        *, manager: object, settings: object
    ) -> Callable[[str], AbstractAsyncContextManager[FacebookImageHttpSession]]:
        del manager, settings
        return failed_session

    def unused_settings(*_args: object) -> None:
        return None

    monkeypatch.setattr(
        ebay_item_workers,
        "load_configuration",
        lambda *_args: SimpleNamespace(
            configuration=SimpleNamespace(resolve_network_path=lambda path: path)
        ),
    )
    monkeypatch.setattr(ebay_item_workers, "proton_settings", unused_settings)
    monkeypatch.setattr(ebay_item_workers, "ProtonFacebookImageSessionFactory", factory)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, tmp_path, identifiers)
        await _retain_records(database, (_reference(),), identifiers)
        handler = next(
            handler
            for handler in registry.handlers
            if handler.capability.kind == COLLECT_EBAY_IMAGE_WORK_KIND
        )
        await database.begin_operation(
            operation_id="image-operation",
            component=handler.component,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        outcome = await handler.execute(
            _image_payload().as_json(),
            AttemptContext(
                work_item_identifier="image-work",
                lease_token="image-lease",
                worker_identifier="image-worker",
                attempt=attempt,
                operation_identifier="image-operation",
            ),
        )
        if attempt < 3:
            assert isinstance(outcome, RetryWork)
            assert outcome.delay_ns == 2 ** (attempt - 1) * 1_000_000_000
            assert _mapping(outcome.reason)["code"] == "wireproxy_startup_timeout"
            assert _mapping(outcome.result)["state"] == "session_failed"
        else:
            assert isinstance(outcome, TerminalFailureWork)
            assert _mapping(outcome.error)["code"] == "wireproxy_startup_timeout"
            assert _mapping(outcome.result)["state"] == "failed"
        assert outcome.artifacts == ()
        assert outcome.records == ()
        assert images.plans == []
        assert not (tmp_path / "images").exists()
        snapshot = await database.activity_snapshot(
            captured_at_utc_ns=time_ns(), recent_window_ns=60_000_000_000, maximum_rows=10
        )
        assert snapshot.network.recent_failed == 1
        assert (
            next(path for path in snapshot.network_paths if path.path == IMAGE_ROUTE).recent_failed
            == 1
        )
