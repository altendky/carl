"""Offline travel refreshes exercise durable stages and actual resource admission."""

# Exact lease, request authorization and output ownership are integration contracts.
# pyright: reportPrivateUsage=false

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from time import time_ns
from uuid import uuid4

import anyio
import httpx
import pytest

import carl.io.native_routing_pool as pool_module
import carl.travel_stages as travel_stages
from carl._tests.test_ebay_item_workers import _provenance
from carl._tests.test_ebay_stateless_stages import _start
from carl._tests.test_travel_providers import _local_settings
from carl._tests.test_travel_service import _configured, _destination
from carl.core.components import Component
from carl.core.native_routing import NativeRoutingDataset
from carl.core.stages import StageDefinition, StageOutcome, StageWaitKind, WaitWork
from carl.core.travel import TravelDestination
from carl.core.travel_refresh import (
    TRAVEL_ESTIMATE_RECORD_KIND,
    RequestListingRouteRefresh,
    TravelEstimateRecord,
    TravelRefreshPayload,
    refresh_listing_route_work,
    travel_destination_identity,
)
from carl.core.worker import CompletedWork, TerminalFailureWork
from carl.io.http_checkpoints import checkpoint_acquisition
from carl.io.native_routing_pool import native_routing_pool
from carl.io.resource_permits import current_resource_manager
from carl.io.routing import ConfiguredTravelService
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import stage_manager
from carl.travel_stages import TravelStageDependencies, build_travel_stage_registry

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@dataclass
class _Clock:
    value: int = field(default_factory=time_ns)

    def now(self) -> float:
        return self.value / 1_000_000_000

    def now_ns(self) -> int:
        return self.value

    async def unexpected_sleep(self, _: float) -> None:
        raise AssertionError("A queued travel stage must persist a wait instead of sleeping")


@dataclass
class _Transport:
    database: Database
    requests: list[httpx.Request] = field(default_factory=list)

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        # The actual wire entry is below HTTP admission and outside CPU work.
        assert await _held(self.database, "http") == 1
        assert await _held(self.database, "cpu") == 0
        assert request.headers["authorization"] == "test-secret-not-a-real-key"
        self.requests.append(request)
        if request.url.path == "/geocode/search":
            return httpx.Response(
                200,
                json={
                    "features": [
                        {"geometry": {"coordinates": [21, 11]}, "properties": {"confidence": 1}}
                    ]
                },
            )
        assert request.url.path == "/v2/matrix/driving-car"
        assert json.loads(request.content)["sources"] == ["0"]
        return httpx.Response(200, json={"distances": [[12000.0]], "durations": [[900.0]]})


async def _held(database: Database, name: str) -> int:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                "SELECT coalesce(sum(a.units),0) FROM resource_allocations a JOIN resource_permits p ON p.token=a.permit_token WHERE p.state='held' AND a.name=?",
                (name,),
            )
        ).fetchall()
    assert isinstance(rows[0][0], int)
    return rows[0][0]


async def _charges(database: Database, operation: str) -> int:
    async with database._connections.reader() as connection:
        rows = await (
            await connection.execute(
                "SELECT count(*) FROM resident_request_charges WHERE operation_id=?", (operation,)
            )
        ).fetchall()
    assert isinstance(rows[0][0], int)
    return rows[0][0]


@dataclass
class _Case:
    service: ConfiguredTravelService
    registry: StageHandlerRegistry
    initial: StageDefinition
    component: Component
    payload: TravelRefreshPayload
    clock: _Clock
    transport: _Transport
    configuration: Path


def _registry(
    database: Database, service: ConfiguredTravelService, clock: _Clock
) -> StageHandlerRegistry:
    return build_travel_stage_registry(
        TravelStageDependencies(database, service, lambda: str(uuid4()), utc_now_ns=clock.now_ns)
    )


async def _case(
    database: Database, root: Path, *, geocode: bool = False, local: bool = False
) -> _Case:
    directories, configuration = _configured(root)
    if local:
        _ = configuration.write_text(configuration.read_text() + _local_settings(configuration))
    clock, transport = _Clock(), _Transport(database)
    service = ConfiguredTravelService(
        directories=directories,
        transport=httpx.MockTransport(transport),
        now=clock.now,
        sleep=clock.unexpected_sleep,
    )
    destination = TravelDestination(label="Example City, ZZ") if geocode else _destination()
    payload = TravelRefreshPayload(
        request=RequestListingRouteRefresh(
            request_identifier=str(uuid4()), listing_identifier="1234567890"
        ),
        destination=destination,
        origin_identity=await service.origin_identity(),
        destination_identity=travel_destination_identity(destination),
    )
    registry = _registry(database, service, clock)
    initial, component = await _start(database, registry, refresh_listing_route_work(payload))
    return _Case(service, registry, initial, component, payload, clock, transport, configuration)


async def _run(database: Database, case: _Case, stage: StageDefinition) -> StageOutcome:
    manager = replace(stage_manager(database), utc_now_ns=case.clock.now_ns)
    for _ in range(20):
        async with manager.admit(
            stage.identifier, component=case.component, provenance=_provenance(), invocation={}
        ) as admission:
            if admission.execution is None:
                deadline = admission.result.next_eligible_at_utc_ns
                assert deadline is not None, admission.result
                # Pacing stores seconds as floats; resume clearly after the
                # deadline instead of relying on nanosecond float roundtrips.
                case.clock.value = max(case.clock.value + 1_000_000, deadline + 1_000_000)
                continue
            expected = {request.name: request.units for request in stage.resources}
            assert await _held(database, "http") == expected.get("http", 0)
            assert await _held(database, "cpu") == expected.get("cpu", 0)
            with anyio.fail_after(10):
                outcome = await case.registry.execute(admission.execution)
        assert await _held(database, "http") == 0 and await _held(database, "cpu") == 0
        return outcome
    raise AssertionError("Travel stage never became eligible")


async def _finish(
    database: Database, case: _Case, stage: StageDefinition
) -> CompletedWork | TerminalFailureWork:
    for _ in range(30):
        outcome = await _run(database, case, stage)
        if isinstance(outcome, (CompletedWork, TerminalFailureWork)):
            return outcome
        assert isinstance(outcome, WaitWork)
        stage = outcome.continuation
    raise AssertionError("Travel stage chain did not complete")


async def _record(database: Database, outcome: CompletedWork) -> TravelEstimateRecord:
    assert len(outcome.records) == 1 and outcome.records[0].kind == TRAVEL_ESTIMATE_RECORD_KIND
    kind, schema, value = await database.get_record(outcome.records[0].identifier)
    assert kind == TRAVEL_ESTIMATE_RECORD_KIND and schema == 1
    return TravelEstimateRecord.model_validate(value)


@pytest.mark.parametrize("geocode", [False, True])
async def test_hosted_refresh_has_separate_resources_and_schema1_review_reader_result(
    tmp_path: Path, geocode: bool
) -> None:
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1, "http": 1}
    ) as database:
        case = await _case(database, tmp_path, geocode=geocode)
        outcome = await _finish(database, case, case.initial)
        assert isinstance(outcome, CompletedWork)
        record = await _record(database, outcome)
        assert record.destination == case.payload.destination
        assert record.estimate.status == "ready" and record.estimate.distance_meters == 12000
        assert record.estimate.duration_seconds == 900
        assert record.origin_identity == case.payload.origin_identity
        assert [request.url.path for request in case.transport.requests] == (
            ["/geocode/search", "/v2/matrix/driving-car"] if geocode else ["/v2/matrix/driving-car"]
        )
        assert await _charges(database, case.initial.operation_identifier) == (2 if geocode else 1)
        # This is the exact frozen-boundary reader used by ReviewApplication.
        retained = await database.latest_travel_estimate_records(
            (case.payload.request.listing_identifier,),
            origin_identity=case.payload.origin_identity,
            destination_identities={
                case.payload.request.listing_identifier: case.payload.destination_identity
            },
            as_of_completion_sequence=await database.current_completion_boundary(),
        )
        assert retained[case.payload.request.listing_identifier][0] == record.record_identifier
        work = await database.work(case.initial.work_item_identifier)
        assert work["state"] == "completed" and work["attempt"] == 1
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT name_parts_json,object_id FROM operation_outputs WHERE operation_id=?",
                    (case.initial.operation_identifier,),
                )
            ).fetchall()
        assert len({row[0] for row in rows}) == len(rows)
        assert len({row[1] for row in rows}) == len(rows)
        assert "test-secret" not in json.dumps(
            await database.operation(case.initial.operation_identifier)
        )
        assert "test-secret" not in json.dumps(
            await database.records_by_kind(("carl", "http", "stage_acquisition"))
        )


async def test_persisted_pacing_wait_holds_no_permit_and_spends_no_dispatch_charge(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        case = await _case(database, tmp_path)
        first = await case.service.cache.try_gate(
            "matrix", action_identifier="another-client", now=case.clock.now()
        )
        assert first.granted
        prepared = await _run(database, case, case.initial)
        assert isinstance(prepared, WaitWork) and prepared.continuation.kind[-1] == "capture"
        delayed = await _run(database, case, prepared.continuation)
        assert isinstance(delayed, WaitWork) and delayed.wait_kind == StageWaitKind.TIME
        assert delayed.continuation.not_before_utc_ns > case.clock.now_ns()
        assert (
            case.transport.requests == []
            and await _charges(database, case.initial.operation_identifier) == 0
        )
        manager = replace(stage_manager(database), utc_now_ns=case.clock.now_ns)
        async with manager.admit(
            delayed.continuation.identifier,
            component=case.component,
            provenance=_provenance(),
            invocation={},
        ) as admission:
            assert admission.execution is None and admission.result.wait_kind == StageWaitKind.TIME
        assert await _held(database, "http") == 0 and await _held(database, "cpu") == 0
        outcome = await _finish(database, case, delayed.continuation)
        assert isinstance(outcome, CompletedWork) and len(case.transport.requests) == 1
        assert await _charges(database, case.initial.operation_identifier) == 1


class _Interrupted(BaseException):
    pass


async def test_unknown_dispatch_after_capture_cannot_query_again_in_same_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        case = await _case(database, tmp_path)
        prepared = await _run(database, case, case.initial)
        assert isinstance(prepared, WaitWork)
        original = checkpoint_acquisition

        async def crash(*_args: object, **_kwargs: object):
            raise _Interrupted

        monkeypatch.setattr(travel_stages, "checkpoint_acquisition", crash)
        with pytest.raises(_Interrupted):
            _ = await _run(database, case, prepared.continuation)
        assert (
            len(case.transport.requests) == 1
            and await _charges(database, case.initial.operation_identifier) == 1
        )
        monkeypatch.setattr(travel_stages, "checkpoint_acquisition", original)
        outcome = await _finish(database, case, prepared.continuation)
        assert isinstance(outcome, CompletedWork)
        record = await _record(database, outcome)
        assert record.estimate.unavailable_reason == "provider_uncertain_dispatch"
        assert (
            len(case.transport.requests) == 1
            and await _charges(database, case.initial.operation_identifier) == 1
        )
        assert (await database.work(case.initial.work_item_identifier))["attempt"] == 1


async def test_captured_cpu_replay_after_database_reopen_keeps_operation_and_never_refetches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        case = await _case(database, tmp_path)
        prepared = await _run(database, case, case.initial)
        assert isinstance(prepared, WaitWork)
        captured = await _run(database, case, prepared.continuation)
        assert isinstance(captured, WaitWork) and captured.artifacts
        body_ids = {artifact.identifier for artifact in captured.artifacts}
        metadata = [
            await database.get_external_artifact_metadata(identifier) for identifier in body_ids
        ]
        assert all(item is not None for item in metadata)

        async def interrupt(*_args: object, **_kwargs: object):
            raise _Interrupted

        monkeypatch.setattr(database, "complete_leased_operation", interrupt)
        with pytest.raises(_Interrupted):
            _ = await _run(database, case, captured.continuation)
        assert len(case.transport.requests) == 1

    async with Database.managed(path) as database:
        case.transport.database = database
        case.registry = _registry(database, case.service, case.clock)
        restored = await database.stage(captured.continuation.identifier)
        assert restored.operation_identifier == case.initial.operation_identifier
        outcome = await _finish(database, case, restored)
        assert isinstance(outcome, CompletedWork) and len(case.transport.requests) == 1
        assert (await _record(database, outcome)).estimate.status == "ready"
        assert (await database.work(case.initial.work_item_identifier))["attempt"] == 1
        assert await _charges(database, restored.operation_identifier) == 1
        assert not body_ids.intersection(artifact.identifier for artifact in outcome.artifacts)
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT object_id FROM operation_outputs WHERE operation_id=?",
                    (restored.operation_identifier,),
                )
            ).fetchall()
        assert all(sum(row[0] == identifier for row in rows) == 1 for identifier in body_ids)


async def test_configuration_change_before_wire_publishes_origin_changed_without_charge(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        case = await _case(database, tmp_path)
        prepared = await _run(database, case, case.initial)
        assert isinstance(prepared, WaitWork)
        _ = case.configuration.write_text(
            case.configuration.read_text().replace("latitude=10.0", "latitude=9.0")
        )
        outcome = await _finish(database, case, prepared.continuation)
        assert isinstance(outcome, TerminalFailureWork) and outcome.error == {
            "kind": "origin_changed"
        }
        assert outcome.records == () and case.transport.requests == []
        assert await _charges(database, case.initial.operation_identifier) == 0


async def test_config_stat_race_after_capture_preserves_completed_response_without_refetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        case = await _case(database, tmp_path)
        prepared = await _run(database, case, case.initial)
        assert isinstance(prepared, WaitWork)
        captured = await _run(database, case, prepared.continuation)
        assert isinstance(captured, WaitWork) and len(case.transport.requests) == 1
        original = case.service.queries.snapshot
        raced = False

        async def snapshot():
            nonlocal raced
            result = await original()
            if not raced:
                raced = True
                _ = case.configuration.write_text(
                    case.configuration.read_text() + "\n# unrelated config rewrite\n"
                )
            return result

        monkeypatch.setattr(case.service.queries, "snapshot", snapshot)
        interrupted = await _run(database, case, captured.continuation)
        assert raced and isinstance(interrupted, WaitWork)
        monkeypatch.setattr(case.service.queries, "snapshot", original)
        outcome = await _finish(database, case, interrupted.continuation)
        assert isinstance(outcome, CompletedWork)
        assert (await _record(database, outcome)).estimate.status == "ready"
        assert len(case.transport.requests) == 1
        assert await _charges(database, case.initial.operation_identifier) == 1


async def test_busy_native_gate_waits_without_cpu_then_real_warm_child_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pool_module, "DEFAULT_BACKEND_MODULE", "carl._tests.native_routing_backend")
    async with Database.managed(
        tmp_path / "carl.sqlite3",
        initialize=True,
        resource_capacities={"cpu": 1, "routing_engines": 1},
    ) as database:
        case = await _case(database, tmp_path, local=True)
        manager = current_resource_manager()
        assert manager is not None
        pool = native_routing_pool(manager)
        borrow = await pool.try_borrow(
            NativeRoutingDataset(
                dataset_path=case.configuration.parent / "fixture.osrm",
                dataset_identifier="test-map-v1",
            )
        )
        assert borrow is not None
        prepared = await _run(database, case, case.initial)
        assert isinstance(prepared, WaitWork) and prepared.continuation.kind[-1] == "native_gate"
        blocked = await _run(database, case, prepared.continuation)
        assert isinstance(blocked, WaitWork) and blocked.wait_kind == StageWaitKind.OWNERSHIP
        assert blocked.continuation.resources == ()
        assert await _held(database, "http") == 0 and await _held(database, "cpu") == 0
        assert case.transport.requests == []
        await borrow.aclose()
        outcome = await _finish(database, case, blocked.continuation)
        assert isinstance(outcome, CompletedWork)
        record = await _record(database, outcome)
        assert record.estimate.provider == "osrm" and record.estimate.status == "ready"
        assert record.estimate.distance_meters is not None and record.estimate.distance_meters > 0
        assert record.estimate.duration_seconds == 1
        assert await _charges(database, case.initial.operation_identifier) == 0
        assert await _held(database, "routing_engines") == 1
