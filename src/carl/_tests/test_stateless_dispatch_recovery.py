"""Stateless paid captures cannot repeat across execution interruption or replay."""

# Synthetic evidence fixtures inspect exact durable authorization/publication state.
# pyright: reportPrivateUsage=false

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from time import time_ns
from types import ModuleType
from typing import Literal, cast
from uuid import uuid4

import anyio
import pytest

import carl.ebay_stateless_stages as ebay_stages
import carl.facebook_stateless_stages as facebook_stages
from carl._tests.test_ebay_item_workers import (
    DESCRIPTION_URL,
    IMAGE_URL,
    ITEM,
    _png,
    _provenance,
    _Response,
    _retain_records,
)
from carl._tests.test_ebay_stateless_stages import (
    _Capture as EbayCapture,
)
from carl._tests.test_ebay_stateless_stages import (
    _dependencies as ebay_dependencies,
)
from carl._tests.test_ebay_stateless_stages import (
    _image_start as ebay_image_start,
)
from carl._tests.test_ebay_stateless_stages import (
    _start as ebay_start,
)
from carl._tests.test_ebay_workers import _directories
from carl._tests.test_facebook import HTML
from carl._tests.test_facebook_stateless_stages import (
    _Capture as FacebookCapture,
)
from carl._tests.test_facebook_stateless_stages import (
    _Clock,
    _Session,
)
from carl._tests.test_facebook_stateless_stages import (
    _register as facebook_register,
)
from carl._tests.test_facebook_stateless_stages import (
    _registry as facebook_registry,
)
from carl._tests.test_facebook_stateless_stages import (
    _seed as facebook_seed,
)
from carl.core.components import Component
from carl.core.ebay_items import (
    CollectEbayDescriptionPayload,
    EbayItemRequest,
    collect_ebay_description_work,
)
from carl.core.facebook_work import CollectItemPayload, collect_item_work
from carl.core.http import RequestPlan
from carl.core.models import JsonValue, RecordDraft
from carl.core.stages import (
    StageAttemptContext,
    StageDefinition,
    StageOutcome,
    StageRequestCharge,
    WaitWork,
    stage_network_activity,
)
from carl.core.worker import CompletedWork, RetryWork, TerminalFailureWork
from carl.facebook_stateless_stages import FacebookStatelessStageDependencies, ImageCaptureSession
from carl.io.httpx import Acquisition
from carl.io.proton import SharedProtonWireproxyManager
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio
type _Kind = Literal["ebay_image", "ebay_description", "facebook_image", "facebook_item"]
_KINDS: tuple[_Kind, ...] = ("ebay_image", "ebay_description", "facebook_image", "facebook_item")


@dataclass(frozen=True)
class _Case:
    registry: StageHandlerRegistry
    initial: StageDefinition
    component: Component
    captures: Callable[[], int]
    rebuild: Callable[[Database], StageHandlerRegistry]
    module: ModuleType


async def _case(database: Database, root: Path, kind: _Kind) -> _Case:
    if kind == "facebook_item":
        url = "https://www.facebook.com/marketplace/item/123/"
        capture = EbayCapture({url: _Response(HTML.encode(), "text/html; charset=utf-8")})

        def build(db: Database) -> StageHandlerRegistry:
            return facebook_stages.build_facebook_stateless_stage_registry(
                FacebookStatelessStageDependencies(
                    db,
                    _directories(root),
                    lambda: str(uuid4()),
                    SharedProtonWireproxyManager(),
                    item_acquirer=capture,
                )
            )

        registry = build(database)
        initial, adapter = await facebook_register(
            database,
            registry,
            collect_item_work(
                identifier=str(uuid4()),
                payload=CollectItemPayload(
                    listing_id="123",
                    request_plan=RequestPlan(url=url, routing=("decodo", "fixture", "item")),
                ),
                not_before_utc_ns=0,
            ),
        )
        return _Case(
            registry, initial, adapter.component, lambda: len(capture.plans), build, facebook_stages
        )
    if kind == "facebook_image":
        capture = FacebookCapture()
        registry = facebook_registry(database, root, capture)
        initial, adapter = await facebook_seed(database, registry)
        return _Case(
            registry,
            initial,
            adapter.component,
            lambda: capture.captures,
            lambda db: facebook_registry(db, root, capture),
            facebook_stages,
        )
    capture = EbayCapture(
        {
            IMAGE_URL: _Response(_png(), "image/png"),
            DESCRIPTION_URL: _Response(b"<p>Retained description</p>", "text/html"),
        }
    )
    if kind == "ebay_image":
        registry, initial, component = await ebay_image_start(database, root, capture)
    else:
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier="description-observation",
                    kind=("carl", "ebay", "listing_observation"),
                    schema_version=1,
                    value={"item_identifier": ITEM, "description_url": DESCRIPTION_URL},
                ),
            ),
            lambda: str(uuid4()),
        )
        registry = ebay_stages.build_ebay_stateless_stage_registry(
            ebay_dependencies(database, root, capture)
        )
        initial, component = await ebay_start(
            database,
            registry,
            collect_ebay_description_work(
                identifier=str(uuid4()),
                payload=CollectEbayDescriptionPayload(
                    request=EbayItemRequest(item_identifier=ITEM),
                    observation_record_identifier="description-observation",
                    url=DESCRIPTION_URL,
                ),
            ),
        )
    return _Case(
        registry,
        initial,
        component,
        lambda: len(capture.plans),
        lambda db: ebay_stages.build_ebay_stateless_stage_registry(
            ebay_dependencies(db, root, capture)
        ),
        ebay_stages,
    )


async def _run(
    database: Database, registry: StageHandlerRegistry, stage: StageDefinition, component: Component
) -> StageOutcome:
    manager = stage_manager(database)
    with anyio.fail_after(10):
        while True:
            async with manager.admit(
                stage.identifier, component=component, provenance=_provenance(), invocation={}
            ) as admission:
                if admission.execution is not None:
                    return await registry.execute(admission.execution)
                deadline = admission.result.next_eligible_at_utc_ns
            await anyio.sleep(
                0.01 if deadline is None else min(0.1, max(0.001, (deadline - time_ns()) / 1e9))
            )


async def _charge_count(database: Database, operation: str) -> int:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                "SELECT count(*) FROM resident_request_charges WHERE operation_id = ?", (operation,)
            )
        ).fetchall()
    count = rows[0][0]
    assert isinstance(count, int)
    return count


async def _assert_authorization(database: Database, http: StageDefinition, kind: _Kind) -> None:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                "SELECT request_identity_json, ordinal, maximum_attempts, execution, resident_id, stage_id FROM resident_request_charges WHERE operation_id = ?",
                (http.operation_identifier,),
            )
        ).fetchall()
    marketplace, purpose = kind.split("_", 1)
    assert rows == [(f'["{marketplace}","{purpose}"]', 1, 1, 1, None, http.identifier)]


class _CrashAfterPaidCapture(BaseException):
    pass


@pytest.mark.parametrize("kind", _KINDS)
async def test_crash_before_checkpoint_cannot_buy_the_same_stateless_request_again(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: _Kind,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        case = await _case(database, tmp_path, kind)
        prepared = await _run(database, case.registry, case.initial, case.component)
        assert isinstance(prepared, WaitWork)
        http = prepared.continuation
        checkpoint = case.module.checkpoint_acquisition

        async def crash(acquisition: Acquisition, **kwargs: object):
            del acquisition, kwargs
            raise _CrashAfterPaidCapture()

        monkeypatch.setattr(case.module, "checkpoint_acquisition", crash)
        with pytest.raises(_CrashAfterPaidCapture):
            _ = await _run(database, case.registry, http, case.component)
        assert case.captures() == 1
        assert await _charge_count(database, http.operation_identifier) == 1
        await _assert_authorization(database, http, kind)
        assert await database.records_by_kind(("carl", "http", "stage_acquisition")) == ()
        monkeypatch.setattr(case.module, "checkpoint_acquisition", checkpoint)
        replay = await _run(database, case.registry, http, case.component)
        assert isinstance(replay, RetryWork)
        assert case.captures() == 1
        assert await _charge_count(database, http.operation_identifier) == 1
        await _assert_authorization(database, http, kind)
        work = await database.work(http.work_item_identifier)
        assert work["attempt"] == 1 and work["state"] == "pending"
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT execution FROM work_stages WHERE id = ?", (http.identifier,)
                )
            ).fetchall()
        assert rows == [(2,)]


@pytest.mark.parametrize("kind", _KINDS)
async def test_checkpointed_cpu_replay_after_database_reopen_never_recaptures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: _Kind,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        case = await _case(database, tmp_path, kind)
        prepared = await _run(database, case.registry, case.initial, case.component)
        assert isinstance(prepared, WaitWork)
        captured = await _run(database, case.registry, prepared.continuation, case.component)
        assert isinstance(captured, WaitWork)
        await _assert_authorization(database, prepared.continuation, kind)
        assert captured.records[0].kind == ("carl", "http", "stage_acquisition")
        checkpoint = captured.records[0].value
        assert isinstance(checkpoint, dict)
        acquisition = cast(dict[str, JsonValue], checkpoint)["acquisition"]
        assert isinstance(acquisition, dict)
        network = stage_network_activity(prepared.continuation, 1)
        assert network is not None
        assert acquisition["network_activity_identifier"] == network.identifier
        cpu = captured.continuation
        raw = {artifact.identifier for artifact in captured.artifacts}
        assert raw and case.captures() == 1

        async def interrupt_publication(*args: object, **kwargs: object):
            del args, kwargs
            raise _CrashAfterPaidCapture()

        monkeypatch.setattr(database, "complete_leased_operation", interrupt_publication)
        with pytest.raises(_CrashAfterPaidCapture):
            _ = await _run(database, case.registry, cpu, case.component)
        assert await _charge_count(database, cpu.operation_identifier) == 1

    async with Database.managed(path) as database:
        registry = case.rebuild(database)
        outcome = await _run(
            database, registry, await database.stage(cpu.identifier), case.component
        )
        assert isinstance(outcome, CompletedWork)
        assert case.captures() == 1
        assert await _charge_count(database, cpu.operation_identifier) == 1
        assert not raw.intersection(artifact.identifier for artifact in outcome.artifacts)
        assert not raw.intersection(output.object_identifier for output in outcome.outputs)
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT object_id FROM operation_outputs WHERE operation_id = ?",
                    (cpu.operation_identifier,),
                )
            ).fetchall()
        identifiers = [str(row[0]) for row in rows]
        assert len(identifiers) == len(set(identifiers))
        assert all(identifiers.count(identifier) == 1 for identifier in raw)


@pytest.mark.parametrize("kind", _KINDS)
async def test_duplicate_authorization_does_not_open_a_second_stateless_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: _Kind,
) -> None:
    original = Database.charge_stage_request

    async def repeated(
        database: Database,
        context: StageAttemptContext,
        *,
        request_identity: tuple[str, ...],
        maximum_attempts: int,
        now_utc_ns: int,
    ) -> StageRequestCharge | None:
        first = await original(
            database,
            context,
            request_identity=request_identity,
            maximum_attempts=maximum_attempts,
            now_utc_ns=now_utc_ns,
        )
        assert first is not None and first.newly_charged
        return await original(
            database,
            context,
            request_identity=request_identity,
            maximum_attempts=maximum_attempts,
            now_utc_ns=now_utc_ns,
        )

    monkeypatch.setattr(Database, "charge_stage_request", repeated)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        case = await _case(database, tmp_path, kind)
        prepared = await _run(database, case.registry, case.initial, case.component)
        assert isinstance(prepared, WaitWork)
        outcome = await _run(database, case.registry, prepared.continuation, case.component)
        assert isinstance(outcome, RetryWork) and case.captures() == 0
        assert await _charge_count(database, prepared.continuation.operation_identifier) == 1


@pytest.mark.parametrize("expires_during_open", (False, True))
async def test_expired_signed_url_consumes_no_dispatch_authorization(
    tmp_path: Path,
    expires_during_open: bool,
) -> None:
    capture, clock = FacebookCapture(), _Clock(time_ns())
    closed = False

    @asynccontextmanager
    async def factory(identifier: str) -> AsyncGenerator[ImageCaptureSession]:
        nonlocal closed
        assert identifier
        clock.now = (int("FFFFFFFF", 16) + 1) * 1_000_000_000
        session = _Session(capture)
        try:
            yield session
        finally:
            session.closed = closed = True

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = facebook_registry(
            database,
            tmp_path,
            capture,
            clock=clock,
            sessions=factory if expires_during_open else None,
        )
        initial, adapter = await facebook_seed(database, registry)
        prepared = await _run(database, registry, initial, adapter.component)
        assert isinstance(prepared, WaitWork)
        if not expires_during_open:
            clock.now = (int("FFFFFFFF", 16) + 1) * 1_000_000_000
        outcome = await _run(database, registry, prepared.continuation, adapter.component)
        assert isinstance(outcome, TerminalFailureWork)
        assert capture.captures == 0
        assert await _charge_count(database, initial.operation_identifier) == 0
        assert closed == expires_during_open
