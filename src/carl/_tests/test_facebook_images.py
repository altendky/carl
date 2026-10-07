"""Image selection, durable acquisition, and offline validation tests."""

import gzip
import hashlib
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from io import BytesIO
from itertools import count
from pathlib import Path
from time import perf_counter_ns, time_ns
from typing import cast

import anyio
import httpx
import pytest
from PIL import Image

import carl.core.facebook_images as facebook_images
from carl.core.acquisition_identity import acquisition_resource_identity
from carl.core.components import Component, ComponentId
from carl.core.facebook_images import (
    COLLECT_IMAGE_WORK_KIND,
    COLLECT_IMAGE_WORK_SCHEMA_VERSION,
    IMAGE_SESSION_MAXIMUM_ACTIVE,
    CollectImagePayload,
    GalleryImageReference,
    ImageFailureSourceKind,
    ImageReuseMatchKind,
    RetryImageFailuresRequest,
    SavedImageCandidate,
    collect_image_work,
    gallery_references,
    image_network_constraints,
    image_reuse_record,
    legacy_image_network_constraint_identifiers,
    plan_image_followups,
    verify_image,
)
from carl.core.facebook_refresh import REFRESH_SEARCH_WORK_KIND
from carl.core.facebook_work import (
    facebook_item_network_constraints,
    facebook_network_path_constraint,
    facebook_search_network_constraints,
)
from carl.core.http import RequestPlan
from carl.core.models import BytesDraft, CodeProvenance, Header, NamedOutput, RecordDraft
from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
    WorkState,
)
from carl.core.worker import AttemptContext, RetryWork, TerminalFailureWork, WorkerSettings
from carl.facebook_image_workers import (
    COLLECT_FACEBOOK_IMAGE,
    PLAN_FACEBOOK_IMAGE_FOLLOWUPS,
    REUSE_FACEBOOK_GALLERY_IMAGE,
    ImageWorkerDependencies,
    build_image_component_registry,
    build_image_worker_registry,
    image_session_failure_work,
)
from carl.io.facebook_images import (
    FacebookImageSessionFailure,
    ProtonFacebookImageSessionFactory,
)
from carl.io.httpx import ClientHttpxAcquirer
from carl.io.image_files import ImageFileStore
from carl.io.image_migration import externalize_saved_images
from carl.io.proton import (
    ManagedProtonSession,
    ManagedProtonTransportFailure,
    ProtonWireproxyManager,
    ProtonWireproxySettings,
)
from carl.io.sqlite import Database
from carl.io.worker import (
    TypedWorkHandler,
    WorkerRuntimeServices,
    WorkHandlerRegistry,
    execute_lease,
)
from carl.review import ReviewApplication

URL = "https://scontent-lga3-1.xx.fbcdn.net/v/t39.84726-6/p.jpg?oh=signed%2Bvalue&oe=abc"
ROUTE = ("proton", "personal", "test")


def _reference(
    *, listing_id: str = "123", observation: str = "observation-123"
) -> GalleryImageReference:
    return GalleryImageReference(
        listing_id=listing_id,
        listing_observation_record_identifier=observation,
        acquisition_record_identifier="item-acquisition-123",
        block_index=4,
        json_path=("result", "listing_photos", 0, "image"),
        original_url=URL,
        role="listing_gallery",
        gallery_order=0,
        photo_id="photo-123",
        declared_width=720,
        declared_height=960,
    )


def _png() -> bytes:
    buffer = BytesIO()
    with Image.new("RGB", (3, 2), color=(20, 40, 60)) as image:
        image.save(buffer, format="PNG")
    return buffer.getvalue()


@pytest.mark.anyio
async def test_image_file_store_resolves_a_relative_database_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    image = _png()
    stored = await ImageFileStore(Path("data")).publish(
        content=image,
        media_type="image/png",
        sha256=hashlib.sha256(image).hexdigest(),
    )

    assert stored.path.is_absolute()
    assert stored.path.read_bytes() == image


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


def test_gallery_planner_keeps_edges_but_deduplicates_requests() -> None:
    first = _reference()
    second = _reference(listing_id="456", observation="observation-456")
    third = _reference(listing_id="789", observation="observation-789").model_copy(
        update={"photo_id": "different-photo", "original_url": URL + "2"}
    )
    plan = plan_image_followups((first, second, third), (), 1)
    assert plan.download_groups == ((0, 1),)
    assert not plan.reuse_decisions
    assert len(plan_image_followups((first, third), (), 2).download_groups) == 2

    candidate = SavedImageCandidate(
        image_result_record_identifier="saved-result",
        source_photo_id=first.photo_id,
        original_url=URL,
        width=720,
        height=960,
    )
    exact = plan_image_followups((first,), (candidate,), 0)
    assert not exact.download_groups
    assert exact.reuse_decisions[0].match_kind is ImageReuseMatchKind.EXACT_RENDITION

    refreshed = first.model_copy(update={"original_url": URL.replace("oe=abc", "oe=def")})
    reused = plan_image_followups((refreshed,), (candidate,), 1)
    assert not reused.download_groups
    assert (
        reused.reuse_decisions[0].match_kind is ImageReuseMatchKind.SOURCE_PHOTO_ADEQUATE_DIMENSIONS
    )
    too_small = candidate.model_copy(update={"width": 719})
    assert plan_image_followups((refreshed,), (too_small,), 1).download_groups == ((0,),)
    unidentified = refreshed.model_copy(update={"photo_id": None})
    assert plan_image_followups((unidentified,), (candidate,), 1).download_groups == ((0,),)

    observation = {
        "listing_id": "123",
        "images": [
            {
                "original_url": URL,
                "role": "listing_gallery",
                "gallery_order": 0,
                "photo_id": "photo-123",
                "declared_dimensions": {"width": 720, "height": 960},
                "source": {
                    "acquisition_record_id": "item-acquisition-123",
                    "block_index": 4,
                    "json_path": ["result", "listing_photos", 0, "image"],
                },
            }
        ],
    }
    assert gallery_references(
        observation_identifier="observation-123", observation=observation
    ) == (first,)


@pytest.mark.anyio
async def test_recorded_source_photo_reuse_resolves_a_new_gallery_reference(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.begin_operation(
            operation_id="source-operation",
            component=Component(ComponentId(("test", "source")), 1, _reference),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.complete_operation(
            operation_id="source-operation",
            records=(
                RecordDraft(
                    identifier="source-reference",
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=_reference().model_dump(mode="json"),
                ),
                RecordDraft(
                    identifier="refreshed-reference",
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=_reference()
                    .model_copy(update={"original_url": URL + "&refreshed=1"})
                    .model_dump(mode="json"),
                ),
                RecordDraft(
                    identifier="source-result",
                    kind=("carl", "facebook", "image_result"),
                    schema_version=1,
                    value={
                        "state": "saved",
                        "image_reference_record_identifier": "source-reference",
                        "source_photo_id": "photo-123",
                        "original_url": URL,
                        "image_artifact_identifier": "source-image",
                        "width": 720,
                        "height": 960,
                    },
                ),
            ),
            artifacts=(),
            outputs=(
                NamedOutput(name=("source_reference",), object_identifier="source-reference"),
                NamedOutput(name=("refreshed_reference",), object_identifier="refreshed-reference"),
                NamedOutput(name=("source_result",), object_identifier="source-result"),
            ),
            result={},
            ended_at_utc=datetime.now(UTC).isoformat(),
            duration_ns=1,
        )
        await database.begin_operation(
            operation_id="reuse-operation",
            component=build_image_component_registry().require(REUSE_FACEBOOK_GALLERY_IMAGE),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
            inputs=(
                (("gallery_image_reference", "00000000"), "refreshed-reference"),
                (("source_image_result", "00000000"), "source-result"),
            ),
        )
        await database.complete_operation(
            operation_id="reuse-operation",
            records=(
                RecordDraft(
                    identifier="reuse-record",
                    kind=("carl", "facebook", "image_reuse"),
                    schema_version=1,
                    value=image_reuse_record(
                        ImageReuseMatchKind.SOURCE_PHOTO_ADEQUATE_DIMENSIONS
                    ).model_dump(mode="json"),
                ),
            ),
            artifacts=(),
            outputs=(
                NamedOutput(name=("image_reuse", "00000000"), object_identifier="reuse-record"),
            ),
            result={},
            ended_at_utc=datetime.now(UTC).isoformat(),
            duration_ns=1,
        )

        resolutions = await database.facebook_image_reuse_resolutions()
        assert len(resolutions) == 1
        assert resolutions[0].gallery_image_reference_record_identifier == "refreshed-reference"
        assert await database.facebook_image_reuse_resolutions(("source-reference",)) == ()
        assert len(await database.facebook_image_reuse_resolutions(("refreshed-reference",))) == 1
        resolved = await database.resolved_facebook_image_results_by_reference()
        assert resolved["source-reference"][0] == "source-result"
        assert resolved["refreshed-reference"][0] == "source-result"
        assert await database.resolved_facebook_image_results_by_reference(
            ("refreshed-reference",)
        ) == {"refreshed-reference": ("source-result", resolved["source-reference"][1])}
        exact_renditions = await database.saved_facebook_image_results_for_renditions(
            (("photo-123", URL),)
        )
        assert tuple(identifier for identifier, _ in exact_renditions) == ("source-result",)


def test_image_validation_checks_decoded_bytes_and_format() -> None:
    raw = _png()
    verified = verify_image(
        gzip.compress(raw),
        [
            {"name_latin1": "Content-Type", "value_latin1": "image/png"},
            {"name_latin1": "Content-Encoding", "value_latin1": "gzip"},
        ],
        {"kind": "http_message_content", "content_decoded": False},
    )
    assert verified.content == raw
    assert (verified.width, verified.height, verified.media_type) == (3, 2, "image/png")
    assert verified.content_encodings_removed == ("gzip",)
    already_decoded = verify_image(
        raw,
        [
            {"name_latin1": "Content-Type", "value_latin1": "image/png"},
            {"name_latin1": "Content-Encoding", "value_latin1": "gzip"},
        ],
        {"kind": "content_decoded_http_body", "content_decoded": True},
    )
    assert already_decoded.content == raw
    for representation in (
        {"kind": "validated_http_image_body"},
        {"kind": "decoded_image_file"},
    ):
        assert verify_image(raw, [], representation).content == raw
    with pytest.raises(ValueError, match="Content-Type"):
        verify_image(
            raw,
            [{"name_latin1": "Content-Type", "value_latin1": "text/html"}],
            {"kind": "content_decoded_http_body", "content_decoded": True},
        )
    with pytest.raises(ValueError, match="decoding or verification"):
        verify_image(
            b"broken image",
            [{"name_latin1": "Content-Type", "value_latin1": "image/png"}],
            {"kind": "content_decoded_http_body", "content_decoded": True},
        )


def test_image_validation_rejects_excessive_declared_pixels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(facebook_images, "MAX_IMAGE_PIXELS", 5)
    with pytest.raises(ValueError, match="decoding or verification"):
        verify_image(
            _png(),
            [{"name_latin1": "Content-Type", "value_latin1": "image/png"}],
            {"kind": "content_decoded_http_body", "content_decoded": True},
        )


def test_image_url_and_route_are_validated() -> None:
    with pytest.raises(ValueError, match="Facebook CDN"):
        GalleryImageReference.model_validate(
            _reference().model_dump() | {"original_url": "https://example.com/image.png"}
        )
    payload = CollectImagePayload(
        reference_record_identifier="reference",
        reference=_reference(),
        request_plan=RequestPlan(url=URL, follow_redirects=False, routing=ROUTE),
    )
    resource_identity = acquisition_resource_identity(
        COLLECT_IMAGE_WORK_KIND, payload.model_dump(mode="json", by_alias=True)
    )
    assert resource_identity is not None
    assert collect_image_work(
        identifier="work", payload=payload, not_before_utc_ns=0
    ).deduplication_identity == (
        *resource_identity,
        "fetch_url",
        hashlib.sha256(URL.encode("utf-8")).hexdigest(),
    )
    other_route = payload.model_copy(
        update={
            "request_plan": RequestPlan(
                url=URL, follow_redirects=False, routing=("proton", "personal", "other")
            )
        }
    )
    assert (
        collect_image_work(
            identifier="other-work", payload=other_route, not_before_utc_ns=0
        ).deduplication_identity
        != collect_image_work(
            identifier="work", payload=payload, not_before_utc_ns=0
        ).deduplication_identity
    )
    constraints = image_network_constraints(ROUTE)
    assert len(constraints) == 6
    shared_path_constraint = facebook_network_path_constraint(ROUTE)
    assert shared_path_constraint in constraints
    assert shared_path_constraint in facebook_item_network_constraints(ROUTE)
    assert shared_path_constraint in facebook_search_network_constraints(ROUTE)
    assert {constraint.identifier for constraint in constraints}.isdisjoint(
        legacy_image_network_constraint_identifiers(ROUTE)
    )
    assert (
        "carl",
        "facebook",
        "search",
        "network_activity_rate",
        *ROUTE,
    ) in legacy_image_network_constraint_identifiers(ROUTE)
    concurrency = {
        (constraint.subject_kind, constraint.scope.kind): constraint.maximum_active
        for constraint in constraints
        if isinstance(constraint, ConcurrencyConstraint)
    }
    assert concurrency == {
        (SchedulingSubjectKind.WORK_ITEM, SchedulingScopeKind.WORK_KIND): 25,
        (SchedulingSubjectKind.NETWORK_ACTIVITY, SchedulingScopeKind.NETWORK_ACTIVITY_KIND): 25,
    }
    assert {
        (constraint.maximum_starts, constraint.period_ns)
        for constraint in constraints
        if isinstance(constraint, SlidingWindowRateConstraint)
    } == {(3, 1_000_000_000), (180, 60_000_000_000)}
    assert (
        build_image_component_registry().require(PLAN_FACEBOOK_IMAGE_FOLLOWUPS).implementation
        is plan_image_followups
    )


def test_image_session_lock_contention_retries_then_retains_diagnostics() -> None:
    first = image_session_failure_work(
        code="wireproxy_device_identity_busy",
        diagnostic={"lock": "device"},
        exit_code=None,
        context=AttemptContext(
            work_item_identifier="image-work",
            worker_identifier="worker",
            lease_token="lease",
            attempt=1,
            operation_identifier="operation-1",
        ),
    )
    exhausted = image_session_failure_work(
        code="wireproxy_device_identity_busy",
        diagnostic={"lock": "device"},
        exit_code=None,
        context=AttemptContext(
            work_item_identifier="image-work",
            worker_identifier="worker",
            lease_token="lease",
            attempt=3,
            operation_identifier="operation-3",
        ),
    )

    assert isinstance(first, RetryWork)
    assert first.reason == {
        "kind": "facebook_image_session_failure",
        "code": "wireproxy_device_identity_busy",
        "diagnostic": {"lock": "device"},
        "exit_code": None,
        "decision": "retry",
    }
    assert isinstance(exhausted, TerminalFailureWork)
    assert exhausted.error == {
        "kind": "facebook_image_session_failure",
        "code": "wireproxy_device_identity_busy",
        "diagnostic": {"lock": "device"},
        "exit_code": None,
        "decision": "retry_exhausted",
    }


def test_permanent_image_session_failure_is_terminal() -> None:
    outcome = image_session_failure_work(
        code="proton_exit_ip_version_mismatch",
        diagnostic={"expected": "version_4", "observed": "version_6"},
        exit_code=None,
        context=AttemptContext(
            work_item_identifier="image-work",
            worker_identifier="worker",
            lease_token="lease",
            attempt=1,
            operation_identifier="operation-1",
        ),
    )

    assert isinstance(outcome, TerminalFailureWork)
    assert outcome.error["decision"] == "terminal"


@pytest.mark.anyio
async def test_proton_transport_failure_retains_session_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingContext:
        async def __aenter__(self) -> ManagedProtonSession:
            raise ManagedProtonTransportFailure(
                "wireproxy_device_identity_busy",
                exit_code=17,
                diagnostic={"lock": "device"},
            )

        async def __aexit__(self, *args: object) -> None:
            del args

    def failing_open(
        manager: ProtonWireproxyManager, settings: ProtonWireproxySettings
    ) -> FailingContext:
        del manager, settings
        return FailingContext()

    monkeypatch.setattr(ProtonWireproxyManager, "open", failing_open)

    factory = ProtonFacebookImageSessionFactory(
        manager=ProtonWireproxyManager(),
        settings=cast(ProtonWireproxySettings, object()),
    )

    with pytest.raises(FacebookImageSessionFailure) as raised:
        async with factory("session"):
            pass

    assert raised.value.code == "wireproxy_device_identity_busy"
    assert raised.value.exit_code == 17
    assert raised.value.diagnostic == {"lock": "device"}


@pytest.mark.anyio
async def test_terminal_image_failures_can_be_retried_by_search_run_or_refresh(
    tmp_path: Path,
) -> None:
    identifiers = count()

    def new_identifier() -> str:
        return f"retry-id-{next(identifiers)}"

    async def fail_image(
        payload: CollectImagePayload, context: AttemptContext
    ) -> TerminalFailureWork:
        del payload, context
        return TerminalFailureWork(
            error={"kind": "legacy_image_failure"},
            result={"state": "image_session_failed"},
        )

    settings = WorkerSettings(
        worker_count=1,
        lease_duration_ns=1_000_000_000,
        renewal_interval_ns=100_000_000,
        idle_poll_interval_ns=10_000_000,
    )
    registry = WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=COLLECT_IMAGE_WORK_KIND,
                    payload_schema_version=COLLECT_IMAGE_WORK_SCHEMA_VERSION,
                ),
                component=build_image_component_registry().require(COLLECT_FACEBOOK_IMAGE),
                payload_type=CollectImagePayload,
                handler=fail_image,
            ),
        )
    )
    services = WorkerRuntimeServices(
        new_identifier=new_identifier,
        utc_now_ns=time_ns,
        monotonic_ns=perf_counter_ns,
        code_provenance=_async_provenance,
        invocation=lambda: {"kind": "test"},
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.begin_operation(
            operation_id="source-operation",
            component=Component(ComponentId(("test", "source")), 1, _reference),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.complete_operation(
            operation_id="source-operation",
            records=(
                RecordDraft(
                    identifier="search-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value={},
                ),
                RecordDraft(
                    identifier="image-plan",
                    kind=("carl", "facebook", "image_followup_plan"),
                    schema_version=1,
                    value={"search_run_record_identifiers": ["search-run"]},
                ),
            ),
            artifacts=(),
            outputs=(
                NamedOutput(name=("search_run",), object_identifier="search-run"),
                NamedOutput(name=("image_plan",), object_identifier="image-plan"),
            ),
            result={},
            ended_at_utc=datetime.now(UTC).isoformat(),
            duration_ns=1,
        )
        _ = await database.enqueue_work(
            WorkDefinition(
                identifier="refresh",
                kind=REFRESH_SEARCH_WORK_KIND,
                payload_schema_version=1,
                payload={},
                deduplication_identity=("test-refresh",),
                not_before_utc_ns=0,
                scopes=(
                    SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                    SchedulingScope(
                        kind=SchedulingScopeKind.WORK_KIND,
                        identity=REFRESH_SEARCH_WORK_KIND,
                    ),
                ),
            ),
            WorkRequester(
                request_identifier="refresh-request",
                kind=("test", "requester"),
                identifier="refresh-source",
                context={},
            ),
            event_identifier=new_identifier(),
            enqueued_at_utc_ns=time_ns(),
        )
        _ = await database.enqueue_work(
            collect_image_work(
                identifier="failed-image",
                payload=CollectImagePayload(
                    reference_record_identifier="image-reference",
                    reference=_reference(),
                    request_plan=RequestPlan(
                        url=URL,
                        follow_redirects=False,
                        routing=ROUTE,
                    ),
                ),
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier="image-request",
                kind=("carl", "facebook", "search_refresh", "image"),
                identifier="image-requester",
                context={
                    "image_followup_plan_record_identifier": "image-plan",
                    "search_refresh_work_identifier": "refresh",
                },
            ),
            event_identifier=new_identifier(),
            enqueued_at_utc_ns=time_ns(),
        )
        application = ReviewApplication(database=database, repository_root=tmp_path)

        async def fail_next_attempt() -> None:
            claim = await database.claim_work(
                supported_capabilities=registry.capabilities,
                worker_identifier="worker",
                lease_token=new_identifier(),
                lease_duration_ns=settings.lease_duration_ns,
                utc_now_ns=time_ns,
                event_identifier=new_identifier(),
                eligible_identifiers=("failed-image",),
            )
            assert claim.lease is not None
            _ = await execute_lease(
                database=database,
                registry=registry,
                settings=settings,
                services=services,
                lease=claim.lease,
            )
            assert await database.work_state("failed-image") is WorkState.TERMINAL_FAILURE

        await fail_next_attempt()
        by_run = await application.retry_image_failures(
            RetryImageFailuresRequest(source_identifier="search-run")
        )
        assert by_run.source_kind is ImageFailureSourceKind.SEARCH_RUN
        assert by_run.matched_terminal_failures == by_run.retried == 1
        assert await database.work_state("failed-image") is WorkState.PENDING

        await fail_next_attempt()
        by_refresh = await application.retry_image_failures(
            RetryImageFailuresRequest(source_identifier="refresh")
        )
        assert by_refresh.source_kind is ImageFailureSourceKind.SEARCH_REFRESH
        assert by_refresh.matched_terminal_failures == by_refresh.retried == 1
        retried = await database.work("failed-image")
        assert retried["state"] == WorkState.PENDING.value
        assert retried["error"] is None


@pytest.mark.anyio
async def test_hundreds_of_terminal_images_are_retried_in_one_bounded_call(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    image_count = 400
    async with Database.managed(path, initialize=True):
        pass
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO work_items(
                id, kind_parts_json, payload_schema_version, payload_json,
                deduplication_identity_json, state, priority,
                eligible_at_utc_ns, created_at_utc_ns, attempt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "refresh",
                '["carl","facebook","work","refresh_search"]',
                1,
                "{}",
                '["refresh"]',
                "completed",
                0,
                0,
                1,
                0,
            ),
        )
        connection.execute(
            """
            INSERT INTO work_events(id, work_item_id, event_kind, recorded_at_utc_ns, data_json)
            VALUES ('refresh-completed', 'refresh', 'completed', 1, '{}')
            """
        )
        connection.executemany(
            """
            INSERT INTO operations(
                id, component_parts_json, output_schema_version,
                code_provenance_json, invocation_json, configuration_json,
                state, started_at_utc, ended_at_utc, duration_ns,
                result_json, error_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    f"operation-{index}",
                    '["carl","facebook","collect","gallery_image"]',
                    2,
                    "{}",
                    "{}",
                    "{}",
                    "failed",
                    "2026-09-26T00:00:00+00:00",
                    "2026-09-26T00:00:01+00:00",
                    1,
                    '{"state":"terminal_failure"}',
                    '{"kind":"unhandled_handler_error"}',
                )
                for index in range(image_count)
            ),
        )
        connection.executemany(
            """
            INSERT INTO work_items(
                id, kind_parts_json, payload_schema_version, payload_json,
                deduplication_identity_json, state, priority,
                eligible_at_utc_ns, created_at_utc_ns, attempt,
                result_json, error_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    f"image-{index}",
                    '["carl","facebook","work","collect_image"]',
                    1,
                    "{}",
                    f'["image-{index}"]',
                    "terminal_failure",
                    0,
                    0,
                    index + 2,
                    1,
                    '{"state":"terminal_failure"}',
                    '{"kind":"unhandled_handler_error"}',
                )
                for index in range(image_count)
            ),
        )
        connection.executemany(
            """
            INSERT INTO work_operations(
                operation_id, work_item_id, attempt, lease_token, worker_identifier
            ) VALUES (?, ?, 1, ?, ?)
            """,
            (
                (f"operation-{index}", f"image-{index}", f"lease-{index}", "legacy-worker")
                for index in range(image_count)
            ),
        )
        connection.executemany(
            """
            INSERT INTO work_scopes(work_item_id, scope_kind, scope_identity_json)
            VALUES (?, 'overall', '[]')
            """,
            ((f"image-{index}",) for index in range(image_count)),
        )
        connection.executemany(
            """
            INSERT INTO work_scopes(work_item_id, scope_kind, scope_identity_json)
            VALUES (?, 'work_kind', '["carl","facebook","work","collect_image"]')
            """,
            ((f"image-{index}",) for index in range(image_count)),
        )
        connection.executemany(
            """
            INSERT INTO work_requests(
                id, work_item_id, requester_kind_parts_json,
                requester_identifier, requested_at_utc_ns, context_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    f"request-{index}",
                    f"image-{index}",
                    '["carl","facebook","search_refresh","image"]',
                    f"requester-{index}",
                    1,
                    '{"search_refresh_work_identifier":"refresh"}',
                )
                for index in range(image_count)
            ),
        )
        connection.executemany(
            """
            INSERT INTO work_events(
                id, work_item_id, event_kind, recorded_at_utc_ns, data_json
            ) VALUES (?, ?, 'terminal_failure', 1, '{}')
            """,
            ((f"failed-{index}", f"image-{index}") for index in range(image_count)),
        )

    async with Database.managed(path) as database:
        application = ReviewApplication(database=database, repository_root=tmp_path)
        with anyio.fail_after(2):
            result = await application.retry_image_failures(
                RetryImageFailuresRequest(
                    source_identifier="refresh",
                    maximum_items=image_count,
                )
            )

        assert result.matched_terminal_failures == image_count
        assert result.retried == image_count
        assert result.remaining_terminal_failures == 0
        old_worker = await database.claim_work(
            supported_capabilities=(
                WorkCapability(kind=COLLECT_IMAGE_WORK_KIND, payload_schema_version=1),
            ),
            worker_identifier="pre-fix-worker",
            lease_token="pre-fix-lease",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: time_ns(),
            event_identifier="pre-fix-claim",
        )
        assert old_worker.lease is None
        current_leases = []
        for index in range(IMAGE_SESSION_MAXIMUM_ACTIVE):
            current_worker = await database.claim_work(
                supported_capabilities=(
                    WorkCapability(
                        kind=COLLECT_IMAGE_WORK_KIND,
                        payload_schema_version=COLLECT_IMAGE_WORK_SCHEMA_VERSION,
                    ),
                ),
                worker_identifier=f"current-worker-{index}",
                lease_token=f"current-lease-{index}",
                lease_duration_ns=1_000_000_000,
                utc_now_ns=time_ns,
                event_identifier=f"current-claim-{index}",
            )
            assert current_worker.lease is not None
            current_leases.append(current_worker.lease)
        availability_checks = 0
        original_availability = database._constraint_availability

        async def counted_availability(*args, **kwargs):
            nonlocal availability_checks
            availability_checks += 1
            return await original_availability(*args, **kwargs)

        monkeypatch.setattr(database, "_constraint_availability", counted_availability)
        with anyio.fail_after(1):
            blocked_worker = await database.claim_work(
                supported_capabilities=(
                    WorkCapability(
                        kind=COLLECT_IMAGE_WORK_KIND,
                        payload_schema_version=COLLECT_IMAGE_WORK_SCHEMA_VERSION,
                    ),
                ),
                worker_identifier="another-current-worker",
                lease_token="another-current-lease",
                lease_duration_ns=1_000_000_000,
                utc_now_ns=time_ns,
                event_identifier="another-current-claim",
            )

        assert blocked_worker.lease is None
        assert blocked_worker.next_eligible_at_utc_ns == min(
            lease.expires_at_utc_ns for lease in current_leases
        )
        assert availability_checks == 1


@pytest.mark.anyio
@pytest.mark.parametrize("response_kind", ["image", "html", "broken", "incomplete"])
async def test_image_worker_retains_response_and_validation_result(
    tmp_path: Path, response_kind: str
) -> None:
    identifiers = count()

    def new_identifier() -> str:
        return f"id-{next(identifiers)}"

    class BrokenStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"partial"
            raise httpx.ReadError("interrupted fixture")

    class CompleteStream(httpx.AsyncByteStream):
        def __init__(self, content: bytes):
            self.content = content

        async def __aiter__(self):
            yield self.content

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == URL
        assert request.headers["accept-encoding"] == "identity"
        if response_kind == "incomplete":
            return httpx.Response(200, headers={"Content-Type": "image/png"}, stream=BrokenStream())
        if response_kind == "html":
            return httpx.Response(
                200,
                headers={"Content-Type": "text/html"},
                stream=CompleteStream(b"<html>login</html>"),
            )
        if response_kind == "broken":
            return httpx.Response(
                200, headers={"Content-Type": "image/png"}, stream=CompleteStream(b"not a png")
            )
        return httpx.Response(
            200, headers={"Content-Type": "image/png"}, stream=CompleteStream(_png())
        )

    settings = WorkerSettings(
        worker_count=1,
        lease_duration_ns=1_000_000_000,
        renewal_interval_ns=100_000_000,
        idle_poll_interval_ns=10_000_000,
    )
    services = WorkerRuntimeServices(
        new_identifier=new_identifier,
        utc_now_ns=time_ns,
        monotonic_ns=perf_counter_ns,
        code_provenance=_async_provenance,
        invocation=lambda: {"kind": "test"},
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.begin_operation(
            operation_id="plan-operation",
            component=build_image_component_registry().require(PLAN_FACEBOOK_IMAGE_FOLLOWUPS),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.complete_operation(
            operation_id="plan-operation",
            records=(
                RecordDraft(
                    identifier="reference",
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=_reference().model_dump(mode="json"),
                ),
            ),
            artifacts=(),
            outputs=(NamedOutput(name=("reference",), object_identifier="reference"),),
            result={},
            ended_at_utc=datetime.now(UTC).isoformat(),
            duration_ns=1,
        )
        assert (await database.facebook_gallery_reference_identifiers())[
            _reference()
        ] == "reference"
        assert (
            await database.facebook_gallery_reference_identifiers(
                (_reference().listing_observation_record_identifier,)
            )
        )[_reference()] == "reference"
        assert await database.facebook_gallery_reference_identifiers(("unrelated",)) == {}
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), trust_env=False
        ) as client:
            registry = build_image_worker_registry(
                ImageWorkerDependencies(
                    database=database,
                    acquirer=ClientHttpxAcquirer(
                        client=client,
                        expected_routing=ROUTE,
                        routing_observation={"kind": "test_proton"},
                        authentication="anonymous",
                    ),
                    image_files=ImageFileStore(tmp_path),
                    new_identifier=new_identifier,
                )
            )
            payload = CollectImagePayload(
                reference_record_identifier="reference",
                reference=_reference(),
                request_plan=RequestPlan(
                    url=URL,
                    headers=(Header(name=b"Accept", value=b"image/*"),),
                    follow_redirects=False,
                    routing=ROUTE,
                    compression=("identity",),
                ),
            )
            unrelated_reference = GalleryImageReference.model_validate(
                _reference().model_dump()
                | {"photo_id": "other-photo", "original_url": URL + "&other=1"}
            )
            await database.enqueue_work(
                collect_image_work(
                    identifier="unrelated",
                    payload=CollectImagePayload(
                        reference_record_identifier="unrelated-reference",
                        reference=unrelated_reference,
                        request_plan=RequestPlan(
                            url=unrelated_reference.original_url,
                            follow_redirects=False,
                            routing=("proton", "personal", "different-route"),
                        ),
                    ),
                    not_before_utc_ns=0,
                ),
                WorkRequester(
                    request_identifier="unrelated-request",
                    kind=("carl", "test", "requester"),
                    identifier="other-run",
                    context={},
                ),
                event_identifier=new_identifier(),
                enqueued_at_utc_ns=time_ns(),
            )
            await database.enqueue_work(
                collect_image_work(identifier="collect", payload=payload, not_before_utc_ns=0),
                WorkRequester(
                    request_identifier="request",
                    kind=("carl", "test", "requester"),
                    identifier="run",
                    context={},
                ),
                event_identifier=new_identifier(),
                enqueued_at_utc_ns=time_ns(),
            )
            claim = await database.claim_work(
                supported_capabilities=(
                    WorkCapability(
                        kind=COLLECT_IMAGE_WORK_KIND,
                        payload_schema_version=COLLECT_IMAGE_WORK_SCHEMA_VERSION,
                    ),
                ),
                worker_identifier="worker",
                lease_token="collect-lease",
                lease_duration_ns=settings.lease_duration_ns,
                utc_now_ns=time_ns,
                event_identifier=new_identifier(),
                eligible_identifiers=("collect",),
            )
            assert claim.lease is not None
            assert claim.lease.work_item_identifier == "collect"
            assert await database.work_state("unrelated") is WorkState.PENDING
            await execute_lease(
                database=database,
                registry=registry,
                settings=settings,
                services=services,
                lease=claim.lease,
            )
            collection = await database.work("collect")
            if response_kind == "incomplete":
                assert collection["state"] == WorkState.PENDING.value
                operation = await database.operation(
                    collection["operations"][0]["operation_identifier"]
                )
                assert (
                    operation["result"]["acquisition"]["stopping_condition"] == "transport_failure"
                )
                assert (
                    operation["result"]["acquisition"]["hops"][0]["response"]["body"]["state"]
                    == "unavailable"
                )
                return
            expected_state = (
                WorkState.COMPLETED.value
                if response_kind == "image"
                else WorkState.TERMINAL_FAILURE.value
            )
            assert collection["state"] == expected_state
            operation = await database.operation(
                collection["operations"][0]["operation_identifier"]
            )
            assert operation["component_parts"] == [
                "carl",
                "facebook",
                "collect",
                "gallery_image",
            ]
            assert operation["output_schema_version"] == 4
            acquisition_id = collection["result"]["acquisition_record_identifier"]
            _, _, acquisition = await database.get_record(acquisition_id)
            assert acquisition["request_plan"]["url"] == URL
            assert acquisition["image_reference_record_identifier"] == "reference"
            assert not await database.pending_facebook_image_extractions()
            if response_kind == "image":
                result_id = collection["result"]["image_result_record_identifier"]
                _, _, result = await database.get_record(result_id)
                assert result["state"] == "saved"
                assert (result["width"], result["height"], result["mime_type"]) == (
                    3,
                    2,
                    "image/png",
                )
                metadata, data = await database.get_artifact(result["image_artifact_identifier"])
                assert data == _png()
                assert (
                    verify_image(
                        data,
                        acquisition["hops"][-1]["response"]["headers"],
                        metadata["representation"],
                    ).content
                    == data
                )
                assert metadata["storage"]["backend"] == "filesystem"
                assert result["producer"] == {
                    "component_parts": ["carl", "facebook", "collect", "gallery_image"],
                    "output_schema_version": 4,
                }
                with closing(sqlite3.connect(tmp_path / "carl.sqlite3")) as connection:
                    assert connection.execute("SELECT count(*) FROM artifacts").fetchone() == (0,)
                    assert connection.execute(
                        "SELECT count(*) FROM external_artifacts"
                    ).fetchone() == (1,)
                    assert connection.execute("SELECT count(*) FROM content").fetchone() == (0,)
                assert result["image_file_locator"] == metadata["storage"]["locator"]
                assert (
                    acquisition["hops"][-1]["response"]["body"]["artifact_id"]
                    == result["image_artifact_identifier"]
                )
                assert acquisition["hops"][-1]["response"]["body"]["retained_as"] == (
                    "validated_image_file"
                )
                assert ("photo-123", URL) in await database.saved_facebook_image_renditions()
            else:
                result_id = collection["result"]["image_result_record_identifier"]
                _, _, result = await database.get_record(result_id)
                assert result["state"] == "failed"
                assert acquisition["hops"][-1]["response"]["body"]["state"] == "unavailable"
                assert acquisition["hops"][-1]["response"]["body"]["representation"] == {
                    "kind": "content_decoded_http_body",
                    "http_hop_index": 0,
                    "transfer_framing_removed": True,
                    "content_decoded": True,
                    "exact_wire_bytes": False,
                    "content_encoding_headers": [],
                    "storage_compression": {"kind": "none"},
                }
                assert not tuple((tmp_path / "images").rglob("*.*"))
                assert not await database.saved_facebook_image_renditions()


@pytest.mark.anyio
async def test_saved_image_migration_externalizes_body_and_image_artifacts(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    image = _png()
    digest = hashlib.sha256(image).hexdigest()
    acquisition = {
        "effective_url": URL,
        "hops": [
            {
                "response": {
                    "status_code": 200,
                    "headers": [{"name_latin1": "Content-Type", "value_latin1": "image/png"}],
                    "body": {
                        "state": "available",
                        "artifact_id": "response-body",
                        "bytes": len(image),
                    },
                }
            }
        ],
    }
    result = {
        "state": "saved",
        "acquisition_record_identifier": "acquisition",
        "image_reference_record_identifier": "reference",
        "listing_id": "123",
        "source_photo_id": "photo-123",
        "original_url": URL,
        "image_artifact_identifier": "image-file",
        "sha256": digest,
        "mime_type": "image/png",
        "format": "PNG",
        "width": 3,
        "height": 2,
    }
    async with Database.managed(path, initialize=True) as database:
        await database.begin_operation(
            operation_id="legacy-operation",
            component=Component(ComponentId(("carl", "test", "legacy_image")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.complete_operation(
            operation_id="legacy-operation",
            records=(
                RecordDraft(
                    identifier="acquisition",
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value=acquisition,
                ),
                RecordDraft(
                    identifier="image-result",
                    kind=("carl", "facebook", "image_result"),
                    schema_version=1,
                    value=result,
                ),
                RecordDraft(
                    identifier="repeated-image-result",
                    kind=("carl", "facebook", "image_result"),
                    schema_version=1,
                    value={**result, "image_artifact_identifier": "repeated-image-file"},
                ),
            ),
            artifacts=(
                BytesDraft(
                    identifier="response-body",
                    kind=("carl", "http", "response_body"),
                    media_type="image/png",
                    representation={"kind": "http_message_content", "content_decoded": False},
                    content=image,
                ),
                BytesDraft(
                    identifier="image-file",
                    kind=("carl", "facebook", "image_file"),
                    media_type="image/png",
                    representation={"kind": "decoded_image_file"},
                    content=image,
                ),
                BytesDraft(
                    identifier="repeated-image-file",
                    kind=("carl", "facebook", "image_file"),
                    media_type="image/png",
                    representation={"kind": "decoded_image_file"},
                    content=image,
                ),
            ),
            outputs=(
                NamedOutput(name=("acquisition",), object_identifier="acquisition"),
                NamedOutput(name=("body",), object_identifier="response-body"),
                NamedOutput(name=("image",), object_identifier="image-file"),
                NamedOutput(name=("result",), object_identifier="image-result"),
                NamedOutput(name=("repeated_result",), object_identifier="repeated-image-result"),
                NamedOutput(name=("repeated_image",), object_identifier="repeated-image-file"),
            ),
            result={},
            ended_at_utc=datetime.now(UTC).isoformat(),
            duration_ns=1,
        )
        summary = await externalize_saved_images(
            database=database,
            image_files=ImageFileStore(tmp_path),
            migration_operation_identifier="migration-operation",
        )
        assert summary == {
            "saved_image_results": 2,
            "migrated_image_results": 2,
            "externalized_artifacts": 3,
        }
        body_metadata, body_content = await database.get_artifact("response-body")
        image_metadata, image_content = await database.get_artifact("image-file")
        assert body_content == image_content == image
        assert (
            verify_image(
                image_content,
                [{"name_latin1": "Content-Type", "value_latin1": "image/png"}],
                image_metadata["representation"],
            ).content
            == image
        )
        assert body_metadata["storage"] == image_metadata["storage"]
        assert body_metadata["storage"]["backend"] == "filesystem"
        _, _, updated_result = await database.get_record("image-result")
        _, _, repeated_result = await database.get_record("repeated-image-result")
        _, _, updated_acquisition = await database.get_record("acquisition")
        assert updated_result["image_file_locator"] == body_metadata["storage"]["locator"]
        assert repeated_result["image_file_locator"] == body_metadata["storage"]["locator"]
        assert updated_result["image_storage_migration_operation_identifier"] == (
            "migration-operation"
        )
        assert updated_acquisition["hops"][-1]["response"]["body"]["artifact_id"] == ("image-file")
        assert (
            updated_acquisition["hops"][-1]["response"]["body"][
                "image_storage_migration_operation_identifier"
            ]
            == "migration-operation"
        )
        repeated_summary = await externalize_saved_images(
            database=database,
            image_files=ImageFileStore(tmp_path),
            migration_operation_identifier="later-migration-operation",
        )
        assert repeated_summary["externalized_artifacts"] == 0
        _, _, unchanged_result = await database.get_record("image-result")
        assert unchanged_result["image_storage_migration_operation_identifier"] == (
            "migration-operation"
        )

    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT count(*) FROM content").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM external_artifacts").fetchone() == (3,)
