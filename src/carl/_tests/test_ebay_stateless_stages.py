"""eBay stateless stages checkpoint paid responses before CPU admission."""

# The fixtures seed retained legacy evidence and inspect durable permit counts.
# pyright: reportPrivateUsage=false

import gzip
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import time_ns
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import anyio
import httpcore
import httpx
import pytest

import carl.ebay_stateless_stages as stages
from carl._tests.test_ebay_item_workers import (
    DESCRIPTION_URL,
    IMAGE_URL,
    ITEM,
    ITEM_URL,
    _Acquirer,
    _enqueue,
    _image_payload,
    _item_html,
    _mapping,
    _png,
    _provenance,
    _reference,
    _Response,
    _retain_records,
)
from carl.core.components import Component
from carl.core.ebay_items import (
    CollectEbayDescriptionPayload,
    EbayItemRequest,
    ExtractEbayItemPayload,
    collect_ebay_description_work,
    collect_ebay_image_work,
    extract_ebay_item_work,
)
from carl.core.http import RequestPlan
from carl.core.models import ExternalFileDraft, NamedOutput, RecordDraft
from carl.core.resources import ResourceRequest
from carl.core.stages import StageDefinition, StageOutcome, StageWaitKind, WaitWork
from carl.core.work import WorkCapability, WorkDefinition
from carl.core.worker import CompletedWork, RetryWork, TerminalFailureWork
from carl.ebay import _body_artifacts
from carl.ebay_item_workers import EbayItemWorkerDependencies
from carl.io.facebook_images import FacebookImageSessionFailure
from carl.io.httpx import (
    Acquisition,
    AcquisitionFailure,
    DirectHttpxAcquirer,
    HttpAcquirer,
    IdentifierFactory,
)
from carl.io.paths import CarlDirectories
from carl.io.proton import ProtonSessionManager
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import StageManager, stage_manager

pytestmark = pytest.mark.anyio


def _identifier() -> str:
    return str(uuid4())


class _Capture(_Acquirer):
    async def acquire(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        pytest.fail("Staged HTTP must call capture explicitly")

    async def capture(self, plan: RequestPlan, new_identifier: IdentifierFactory) -> Acquisition:
        return await super().acquire(plan, new_identifier)


def _dependencies(
    database: Database,
    root: Path,
    capture: HttpAcquirer | None,
) -> EbayItemWorkerDependencies:
    return EbayItemWorkerDependencies(
        database=database,
        directories=CarlDirectories(
            config=root / "config",
            data=root,
            cache=root / "cache",
            state=root / "state",
            runtime=root / "runtime",
        ),
        new_identifier=_identifier,
        proton_manager=cast(ProtonSessionManager, object()),
        image_acquirer=capture,
        item_acquirer=capture,
    )


async def _start(
    database: Database,
    registry: StageHandlerRegistry,
    definition: WorkDefinition,
) -> tuple[StageDefinition, Component]:
    _ = await _enqueue(database, definition, _identifier)
    adapter = registry.require_adapter(
        WorkCapability(
            kind=definition.kind, payload_schema_version=definition.payload_schema_version
        )
    )
    stage = adapter.initial_stage(definition, _identifier(), _identifier())
    await database.register_stage(stage, created_at_utc_ns=time_ns())
    return stage, adapter.component


async def _run(
    manager: StageManager,
    registry: StageHandlerRegistry,
    stage: StageDefinition,
    component: Component,
) -> StageOutcome:
    async with manager.admit(
        stage.identifier, component=component, provenance=_provenance(), invocation={}
    ) as admission:
        assert admission.execution is not None, admission.result
        return await registry.execute(admission.execution)


async def _held(database: Database, name: str) -> int:
    async with database._connections.reader() as connection:
        cursor = await connection.execute(
            "SELECT coalesce(sum(units), 0) FROM resource_allocations a "
            "JOIN resource_permits p ON p.token=a.permit_token WHERE p.state='held' AND a.name=?",
            (name,),
        )
        row = await cursor.fetchone()
    assert row is not None
    return int(row[0])


async def _image_start(
    database: Database,
    root: Path,
    capture: HttpAcquirer | None,
) -> tuple[StageHandlerRegistry, StageDefinition, Component]:
    await _retain_records(database, (_reference(),), _identifier)
    registry = stages.build_ebay_stateless_stage_registry(_dependencies(database, root, capture))
    initial, component = await _start(
        database,
        registry,
        collect_ebay_image_work(identifier=_identifier(), payload=_image_payload()),
    )
    return registry, initial, component


async def _to_cpu(
    manager: StageManager,
    registry: StageHandlerRegistry,
    stage: StageDefinition,
    component: Component,
) -> WaitWork:
    prepared = await _run(manager, registry, stage, component)
    assert isinstance(prepared, WaitWork)
    assert prepared.continuation.resources == (ResourceRequest(name="http"),)
    captured = await _run(manager, registry, prepared.continuation, component)
    assert isinstance(captured, WaitWork)
    assert captured.continuation.resources == (ResourceRequest(name="cpu"),)
    assert captured.continuation.network_activity is None
    return captured


@pytest.mark.parametrize("valid", (False, True))
@pytest.mark.parametrize("error_kind", ("provider", "oserror", "runtime", "acquisition"))
async def test_complete_response_survives_default_provider_teardown_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    valid: bool,
    error_kind: str,
) -> None:
    content = _png() if valid else b"invalid completed image"
    capture = _Capture({IMAGE_URL: _Response(content, "image/png")})
    close_error = (
        FacebookImageSessionFailure("wireproxy_cleanup_failed", {"exception_type": "OSError"}, 3)
        if error_kind == "provider"
        else OSError("must-not-retain-secret")
        if error_kind == "oserror"
        else AcquisitionFailure(
            "must-not-retain-secret",
            result={
                "stopping_condition": "transport_failure",
                "failure_phase": "client_close",
                "exception_type": "RuntimeError",
            },
        )
        if error_kind == "acquisition"
        else RuntimeError("must-not-retain-secret")
    )

    def unavailable_completed_observation():
        pytest.fail("A failed teardown cannot require a completed provider observation")

    @asynccontextmanager
    async def factory(_identifier: str) -> AsyncIterator[object]:
        yield SimpleNamespace(
            acquirer=capture, completed_observation=unavailable_completed_observation
        )
        raise close_error

    monkeypatch.setattr(
        stages,
        "load_configuration",
        lambda _path: SimpleNamespace(
            configuration=SimpleNamespace(resolve_network_path=lambda path: path)
        ),
    )
    monkeypatch.setattr(stages, "decodo_settings", lambda *_args: (object(), object()))
    monkeypatch.setattr(stages, "DecodoFacebookImageSessionFactory", lambda **_kwargs: factory)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry, initial, component = await _image_start(database, tmp_path, None)
        manager = stage_manager(database)
        captured = await _to_cpu(manager, registry, initial, component)
        checkpoint_value = _mapping(captured.records[0].value)
        acquisition = _mapping(checkpoint_value["acquisition"])
        assert acquisition["stopping_condition"] == "terminal_response"
        assert "must-not-retain-secret" not in json.dumps(acquisition)
        cleanup = _mapping(acquisition["session_cleanup_failure"])
        assert cleanup["type"] == type(close_error).__name__
        if error_kind == "provider":
            assert cleanup["code"] == "wireproxy_cleanup_failed"
            assert cleanup["diagnostic"] == {"exception_type": "OSError"}
            assert cleanup["exit_code"] == 3
        elif error_kind == "acquisition":
            assert cleanup["failure_phase"] == "client_close"
            assert cleanup["exception_type"] == "RuntimeError"
        body = captured.artifacts[0]
        assert isinstance(body, ExternalFileDraft)
        assert body.sha256 == hashlib.sha256(content).hexdigest()
        outcome = await _run(manager, registry, captured.continuation, component)
        assert isinstance(outcome, CompletedWork if valid else TerminalFailureWork)
        assert not isinstance(outcome, RetryWork)
        assert len(capture.plans) == 1
        work = await database.work(initial.work_item_identifier)
        assert work["attempt"] == 1
        assert work["state"] == ("completed" if valid else "terminal_failure")
        public_acquisition = next(
            record.value
            for record in outcome.records
            if record.kind == ("carl", "http", "acquisition")
        )
        assert _mapping(public_acquisition)["session_cleanup_failure"] == cleanup
        if valid:
            artifact_identifier = _mapping(outcome.result)["image_artifact_identifier"]
            assert isinstance(artifact_identifier, str)
            metadata, saved = await database.get_artifact(artifact_identifier)
            assert saved == content
            assert metadata["sha256"] == hashlib.sha256(content).hexdigest()


@pytest.mark.parametrize("description", (False, True))
async def test_complete_httpx_close_failure_is_validated_without_refetch(
    tmp_path: Path,
    description: bool,
) -> None:
    content = b"<p>Paid description</p>" if description else _png()
    encoded = gzip.compress(content)
    requests = 0

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield encoded

        async def aclose(self) -> None:
            raise httpcore.ProtocolError("must-not-retain-secret")

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(
            200,
            headers={
                "Content-Type": "text/html" if description else "image/png",
                "Content-Encoding": "gzip",
            },
            stream=Stream(),
        )

    capture = DirectHttpxAcquirer(httpx.MockTransport(respond))
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        if description:
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
                _identifier,
            )
            registry = stages.build_ebay_stateless_stage_registry(
                _dependencies(database, tmp_path, capture)
            )
            initial, component = await _start(
                database,
                registry,
                collect_ebay_description_work(
                    identifier=_identifier(),
                    payload=CollectEbayDescriptionPayload(
                        request=EbayItemRequest(item_identifier=ITEM),
                        observation_record_identifier="observation-1",
                        url=DESCRIPTION_URL,
                    ),
                ),
            )
        else:
            registry, initial, component = await _image_start(database, tmp_path, capture)
        manager = stage_manager(database)
        captured = await _to_cpu(manager, registry, initial, component)
        checkpoint = _mapping(_mapping(captured.records[0].value)["acquisition"])
        assert checkpoint["stopping_condition"] == "terminal_response"
        assert _mapping(checkpoint["session_cleanup_failure"])["failure_phase"] == "response_close"
        assert "must-not-retain-secret" not in json.dumps(checkpoint)
        outcome = await _run(manager, registry, captured.continuation, component)
        assert isinstance(outcome, CompletedWork) and requests == 1
        if description:
            result_identifier = _mapping(outcome.result)["description_result_record_identifier"]
            assert isinstance(result_identifier, str)
            assert (
                _mapping((await database.get_record(result_identifier))[2])["description"]
                == "Paid description"
            )
        else:
            artifact = _mapping(outcome.result)["image_artifact_identifier"]
            assert isinstance(artifact, str)
            assert (await database.get_artifact(artifact))[1] == content


@pytest.mark.parametrize("provider", ("decodo", "proton"))
async def test_provider_closes_before_checkpoint_wait_and_scarce_cpu_never_holds_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provider: str,
) -> None:
    capture = _Capture({IMAGE_URL: _Response(_png(), "image/png")})
    closed = False

    @asynccontextmanager
    async def factory(_identifier: str) -> AsyncIterator[object]:
        nonlocal closed
        try:
            yield SimpleNamespace(
                acquirer=capture, completed_observation=lambda: {"closed": closed}
            )
        finally:
            closed = True

    monkeypatch.setattr(
        stages,
        "load_configuration",
        lambda _path: SimpleNamespace(
            configuration=SimpleNamespace(
                resolve_network_path=lambda _path: (provider, "personal", "carl")
            )
        ),
    )
    monkeypatch.setattr(stages, "decodo_settings", lambda *_args: (object(), object()))
    monkeypatch.setattr(stages, "proton_settings", lambda *_args: object())
    monkeypatch.setattr(stages, "DecodoFacebookImageSessionFactory", lambda **_kwargs: factory)
    monkeypatch.setattr(stages, "ProtonFacebookImageSessionFactory", lambda **_kwargs: factory)
    checkpoint = stages.checkpoint_acquisition

    async def checkpoint_after_close(*args, **kwargs):
        assert closed
        return await checkpoint(*args, **kwargs)

    monkeypatch.setattr(stages, "checkpoint_acquisition", checkpoint_after_close)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"http": 1, "cpu": 1}
    ) as database:
        registry, initial, component = await _image_start(database, tmp_path, None)
        manager = stage_manager(database)
        prepared = await _run(manager, registry, initial, component)
        assert isinstance(prepared, WaitWork)
        assert await _held(database, "http") == await _held(database, "cpu") == 0

        async def cpu_blocker(started: anyio.Event, release: anyio.Event) -> None:
            async with manager.resources.admit(ResourceRequest(name="cpu")):
                started.set()
                await release.wait()

        started, release = anyio.Event(), anyio.Event()
        captured: WaitWork | None = None
        with anyio.fail_after(5):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(cpu_blocker, started, release)
                await started.wait()
                try:
                    outcome = await _run(manager, registry, prepared.continuation, component)
                    assert isinstance(outcome, WaitWork)
                    captured = outcome
                    assert closed and len(capture.plans) == 1
                    assert capture.plans[0].routing[0] == provider
                    async with manager.admit(
                        captured.continuation.identifier,
                        component=component,
                        provenance=_provenance(),
                        invocation={},
                    ) as admission:
                        assert admission.execution is None
                        assert admission.result.wait_kind == StageWaitKind.RESOURCE
                    assert await _held(database, "http") == 0
                    async with manager.resources.admit(ResourceRequest(name="http")):
                        assert await _held(database, "http") == 1
                finally:
                    release.set()
        assert captured is not None
        outcome = await _run(manager, registry, captured.continuation, component)
        assert isinstance(outcome, CompletedWork)
        assert outcome.result["state"] == "saved"
        assert len(capture.plans) == 1


async def test_reopen_restores_checkpoint_and_reuses_validated_image_without_refetch(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    capture = _Capture({IMAGE_URL: _Response(_png(), "image/png")})
    async with Database.managed(path, initialize=True) as database:
        registry, initial, component = await _image_start(database, tmp_path, capture)
        captured = await _to_cpu(stage_manager(database), registry, initial, component)
        continuation = captured.continuation.identifier
        operation = initial.operation_identifier
        assert captured.artifacts and all(
            isinstance(a, ExternalFileDraft) and a.locator.startswith("responses/")
            for a in captured.artifacts
        )
    async with Database.managed(path) as database:
        registry = stages.build_ebay_stateless_stage_registry(
            _dependencies(database, tmp_path, capture)
        )
        outcome = await _run(
            stage_manager(database), registry, await database.stage(continuation), component
        )
        assert isinstance(outcome, CompletedWork) and outcome.result["reused"] is False
        assert len(capture.plans) == 1
        work = await database.work(initial.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        assert len(work["operations"]) == 1
        assert (await database.operation(operation))["state"] == "completed"
        assert any(item.name == ("http_checkpoint",) for item in outcome.inputs)
        await _retain_records(
            database, (_reference("reference-2", observation="observation-2"),), _identifier
        )
        again, component = await _start(
            database,
            registry,
            collect_ebay_image_work(
                identifier=_identifier(),
                payload=_image_payload("reference-2", observation="observation-2"),
            ),
        )
        reused = await _run(stage_manager(database), registry, again, component)
        assert isinstance(reused, CompletedWork) and reused.result["reused"] is True
        assert len(capture.plans) == 1


@pytest.mark.parametrize("failure", ("invalid_image", "corrupt_checkpoint", "image_storage"))
async def test_cpu_failures_retain_http_evidence_and_never_reacquire(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    capture = _Capture(
        {IMAGE_URL: _Response(b"invalid" if failure == "invalid_image" else _png(), "image/png")}
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry, initial, component = await _image_start(database, tmp_path, capture)
        manager = stage_manager(database)
        captured = await _to_cpu(manager, registry, initial, component)
        if failure == "corrupt_checkpoint":
            body = captured.artifacts[0]
            assert isinstance(body, ExternalFileDraft)
            (tmp_path / body.locator).write_bytes(b"x" * body.size)
        elif failure == "image_storage":

            async def fail_storage(*_args, **_kwargs):
                raise OSError("offline storage failure")

            monkeypatch.setattr("carl.ebay_item_workers.ImageFileStore.publish", fail_storage)
        outcome = await _run(manager, registry, captured.continuation, component)
        assert isinstance(outcome, TerminalFailureWork)
        assert len(capture.plans) == 1
        assert any(item.name == ("http_checkpoint",) for item in outcome.inputs)
        assert (await database.work(initial.work_item_identifier))["state"] == "terminal_failure"
        assert (await database.get_record(captured.records[0].identifier))[0] == (
            "carl",
            "http",
            "stage_acquisition",
        )
        assert await _held(database, "http") == await _held(database, "cpu") == 0


@pytest.mark.parametrize("partial", (False, True))
async def test_checkpoint_storage_failure_is_terminal_without_false_saved_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    partial: bool,
) -> None:
    async def fail_checkpoint(*_args, **_kwargs):
        raise OSError("offline storage failure")

    monkeypatch.setattr(stages, "checkpoint_acquisition", fail_checkpoint)
    capture = _Capture({IMAGE_URL: _Response(_png(), "image/png")}, fail_transport=partial)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry, initial, component = await _image_start(database, tmp_path, capture)
        manager = stage_manager(database)
        prepared = await _run(manager, registry, initial, component)
        assert isinstance(prepared, WaitWork)
        outcome = await _run(manager, registry, prepared.continuation, component)
        assert isinstance(outcome, TerminalFailureWork)
        assert outcome.result["state"] == "failed"
        assert outcome.error == {"kind": "checkpoint_storage_failure", "type": "OSError"}
        assert outcome.result["acquisition"]["stopping_condition"] == (
            "transport_failure" if partial else "terminal_response"
        )
        assert len(capture.plans) == 1
        assert await database.records_by_kind(("carl", "ebay", "image_result")) == ()
        assert await _held(database, "http") == 0


async def test_final_publication_rollback_resumes_cpu_without_repeating_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = _Capture({IMAGE_URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry, initial, component = await _image_start(database, tmp_path, capture)
        manager = stage_manager(database)
        captured = await _to_cpu(manager, registry, initial, component)
        original = database.complete_leased_operation

        async def fail_publication(*_args, **_kwargs):
            raise OSError("offline database publication failure")

        monkeypatch.setattr(database, "complete_leased_operation", fail_publication)
        with pytest.raises(OSError, match="offline database publication failure"):
            await _run(manager, registry, captured.continuation, component)
        assert await database.records_by_kind(("carl", "ebay", "image_result")) == ()
        assert await _held(database, "http") == await _held(database, "cpu") == 0
        monkeypatch.setattr(database, "complete_leased_operation", original)
        outcome = await _run(manager, registry, captured.continuation, component)
        assert isinstance(outcome, CompletedWork)
        assert len(capture.plans) == 1
        assert len(await database.records_by_kind(("carl", "ebay", "image_result"))) == 1
        assert (await database.work(initial.work_item_identifier))["attempt"] == 1


async def test_cancelled_cpu_continuation_keeps_checkpoint_and_publishes_no_image(
    tmp_path: Path,
) -> None:
    capture = _Capture({IMAGE_URL: _Response(_png(), "image/png")})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry, initial, component = await _image_start(database, tmp_path, capture)
        captured = await _to_cpu(stage_manager(database), registry, initial, component)
        _ = await database.cancel_work(
            initial.work_item_identifier,
            reason="test",
            caller="test",
            requested_at_utc_ns=time_ns(),
        )
        assert (await database.work(initial.work_item_identifier))["state"] == "cancelled"
        assert (await database.operation(initial.operation_identifier))["result"] == {
            "state": "cancelled"
        }
        assert (await database.get_record(captured.records[0].identifier))[0] == (
            "carl",
            "http",
            "stage_acquisition",
        )
        assert await database.records_by_kind(("carl", "ebay", "image_result")) == ()
        assert len(capture.plans) == 1 and await _held(database, "http") == 0


@pytest.mark.parametrize("description", (False, True))
async def test_partial_capture_retains_bodies_and_existing_transport_retry_budget(
    tmp_path: Path,
    description: bool,
) -> None:
    url = DESCRIPTION_URL if description else IMAGE_URL
    capture = _Capture({url: _Response(b"paid partial evidence")}, fail_transport=True)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        if description:
            await _retain_records(
                database,
                (
                    RecordDraft(
                        identifier="observation-1",
                        kind=("carl", "ebay", "listing_observation"),
                        schema_version=1,
                        value={"item_identifier": ITEM, "description_url": url},
                    ),
                ),
                _identifier,
            )
            registry = stages.build_ebay_stateless_stage_registry(
                _dependencies(database, tmp_path, capture)
            )
            initial, component = await _start(
                database,
                registry,
                collect_ebay_description_work(
                    identifier=_identifier(),
                    payload=CollectEbayDescriptionPayload(
                        request=EbayItemRequest(item_identifier=ITEM),
                        observation_record_identifier="observation-1",
                        url=url,
                    ),
                ),
            )
        else:
            registry, initial, component = await _image_start(database, tmp_path, capture)
        manager = stage_manager(database)
        captured = await _to_cpu(manager, registry, initial, component)
        outcome = await _run(manager, registry, captured.continuation, component)
        assert isinstance(outcome, RetryWork) and outcome.delay_ns == 0
        assert outcome.reason == {"kind": "ebay_transport_failure"}
        assert captured.artifacts and len(capture.plans) == 1
        assert outcome.artifacts == ()
        assert (await database.work(initial.work_item_identifier))["attempt"] == 1
        assert (await database.get_artifact(captured.artifacts[0].identifier))[
            1
        ] == b"paid partial evidence"


@pytest.mark.parametrize("valid", (False, True))
async def test_description_preserves_results_and_checkpoint_body_references(
    tmp_path: Path,
    valid: bool,
) -> None:
    capture = _Capture(
        {
            DESCRIPTION_URL: _Response(
                b"<p>Useful <b>description</b></p>" if valid else b"", "text/html"
            )
        }
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
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
            _identifier,
        )
        registry = stages.build_ebay_stateless_stage_registry(
            _dependencies(database, tmp_path, capture)
        )
        initial, component = await _start(
            database,
            registry,
            collect_ebay_description_work(
                identifier=_identifier(),
                payload=CollectEbayDescriptionPayload(
                    request=EbayItemRequest(item_identifier=ITEM),
                    observation_record_identifier="observation-1",
                    url=DESCRIPTION_URL,
                ),
            ),
        )
        manager = stage_manager(database)
        captured = await _to_cpu(manager, registry, initial, component)
        outcome = await _run(manager, registry, captured.continuation, component)
        assert isinstance(outcome, CompletedWork if valid else TerminalFailureWork)
        assert outcome.artifacts == ()
        assert any(
            output.name == ("body", "0")
            and output.object_identifier == captured.artifacts[0].identifier
            for output in captured.outputs
        )
        assert all(
            output.object_identifier != captured.artifacts[0].identifier
            for output in outcome.outputs
        )
        result_identifier = outcome.result["description_result_record_identifier"]
        result = (await database.get_record(result_identifier))[2]
        assert result["state"] == ("saved" if valid else "failed")
        assert result["description"] == ("Useful description" if valid else None)
        assert len(capture.plans) == 1


async def test_image_payload_validation_and_missing_capture_do_not_dispatch(tmp_path: Path) -> None:
    class AcquireOnly:
        def __init__(self) -> None:
            self.plans: list[RequestPlan] = []

        async def acquire(
            self, plan: RequestPlan, new_identifier: IdentifierFactory
        ) -> Acquisition:
            del new_identifier
            self.plans.append(plan)
            pytest.fail("An acquire-only collector must be rejected before dispatch")

    for invalid_reference in (False, True):
        root = tmp_path / str(invalid_reference)
        root.mkdir()
        legacy = AcquireOnly()
        async with Database.managed(root / "carl.sqlite3", initialize=True) as database:
            registry, initial, component = await _image_start(database, root, legacy)
            if invalid_reference:
                stage = initial.model_copy(
                    update={
                        "identifier": _identifier(),
                        "payload": {
                            **initial.payload,
                            "payload": _image_payload(item="223456789012").model_dump(mode="json"),
                        },
                    }
                )
                # Replace before registration by testing the typed prepare handler directly.
                context = stages.StageAttemptContext(
                    work_item_identifier=initial.work_item_identifier,
                    lease_token="test",
                    worker_identifier="test",
                    attempt=1,
                    operation_identifier=initial.operation_identifier,
                    stage_identifier=stage.identifier,
                    execution=1,
                )
                outcome = await registry.require_stage(stage).execute(stage.payload, context)
            else:
                outcome = await _run(stage_manager(database), registry, initial, component)
            assert isinstance(outcome, TerminalFailureWork)
            if not invalid_reference:
                assert _mapping(outcome.error)["code"] == "http_acquirer_does_not_support_capture"
            assert legacy.plans == []


async def test_item_extraction_is_cpu_only_and_preserves_follow_on_work(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        request = EbayItemRequest(item_identifier=ITEM)
        acquired = await _Acquirer({ITEM_URL: _Response(_item_html())}).acquire(
            RequestPlan(url=ITEM_URL), _identifier
        )
        operation = _identifier()
        component = (
            stages.build_ebay_item_worker_registry(_dependencies(database, tmp_path, None))
            .handlers[0]
            .component
        )
        await database.begin_operation(
            operation_id=operation,
            component=component,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.complete_operation(
            operation_id=operation,
            records=(
                RecordDraft(
                    identifier="item-acquisition",
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value={
                        **acquired.record,
                        "purpose": "ebay_item",
                        "ebay_item_request": request.model_dump(mode="json"),
                    },
                ),
            ),
            artifacts=_body_artifacts(acquired),
            outputs=(
                NamedOutput(name=("acquisition",), object_identifier="item-acquisition"),
                *(
                    NamedOutput(name=("body", str(index)), object_identifier=body.identifier)
                    for index, body in enumerate(acquired.bodies)
                ),
            ),
            result={},
            ended_at_utc=datetime.now(UTC).isoformat(),
            duration_ns=1,
        )
        registry = stages.build_ebay_stateless_stage_registry(
            _dependencies(database, tmp_path, None)
        )
        initial, component = await _start(
            database,
            registry,
            extract_ebay_item_work(
                identifier=_identifier(),
                payload=ExtractEbayItemPayload(
                    request=request, acquisition_record_identifier="item-acquisition"
                ),
            ),
        )
        assert (
            initial.resources == (ResourceRequest(name="cpu"),) and initial.network_activity is None
        )
        outcome = await _run(stage_manager(database), registry, initial, component)
        assert isinstance(outcome, CompletedWork)
        assert outcome.follow_on_work
        assert (
            _mapping(
                (await database.records_by_kind(("carl", "ebay", "listing_observation")))[0][1]
            )["title"]
            == "Workshop vise"
        )
        assert await _held(database, "http") == await _held(database, "cpu") == 0
