"""Recovery preserves travel observation time and bounded checkpoint provenance."""

# Offline stage fixtures expose exact dispatch and producer fences.
# pyright: reportPrivateUsage=false

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from carl._tests.test_travel_checkpoints import _publish, _state
from carl._tests.test_travel_stages import _case, _charges, _finish, _record, _registry, _run
from carl.core.stages import WaitWork
from carl.core.travel_phases import Matrix
from carl.core.worker import CompletedWork, TerminalFailureWork
from carl.io.sqlite import Database
from carl.io.travel_checkpoints import checkpoint_travel_state, restore_travel_state

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.mark.parametrize("geocode", (False, True))
async def test_cache_permission_failure_is_terminal_without_wire_or_dispatch_charge(
    tmp_path: Path, geocode: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        case = await _case(database, tmp_path, geocode=geocode)
        cache = case.service.cache
        await cache.put("seed", {}, case.clock.now() + 100)
        cache.directories.routing_cache_file.chmod(0o644)
        outcome = await _finish(database, case, case.initial)
        assert isinstance(outcome, TerminalFailureWork)
        assert outcome.error == {
            "kind": "travel_stage_input_or_checkpoint_failure",
            "type": "ConfigurationFailure",
        }
        assert case.transport.requests == []
        assert await _charges(database, case.initial.operation_identifier) == 0
        work = await database.work(case.initial.work_item_identifier)
        assert work["state"] == "terminal_failure" and work["attempt"] == 1


async def test_capture_time_and_cache_expiry_survive_delayed_cpu_replay_after_reopen(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        case = await _case(database, tmp_path)
        prepared = await _run(database, case, case.initial)
        assert isinstance(prepared, WaitWork)
        captured = await _run(database, case, prepared.continuation)
        assert isinstance(captured, WaitWork)
        reference = captured.continuation.payload
        assert isinstance(reference, dict)
        observed = reference["observed_at_utc_ns"]
        state_identifier = reference["state_record_identifier"]
        assert type(observed) is int and isinstance(state_identifier, str)
        state = await restore_travel_state(
            database, state_identifier, operation_identifier=case.initial.operation_identifier
        )
        assert isinstance(state.awaiting, Matrix)
        route_key = state.awaiting.groups[0].key
        configuration = state.snapshot.configuration
        assert configuration is not None
        ttl = configuration.cache_ttl_seconds
        case.clock.value += 600_000_000_000
        stage = captured.continuation
    async with Database.managed(path) as database:
        case.transport.database = database
        case.registry = _registry(database, case.service, case.clock)
        outcome = await _finish(database, case, stage)
        assert isinstance(outcome, CompletedWork)
        record = await _record(database, outcome)
        estimated_at = datetime.fromtimestamp(observed / 1_000_000_000, UTC).isoformat()
        expires_at = datetime.fromtimestamp(observed / 1_000_000_000 + ttl, UTC).isoformat()
        assert record.estimate.status == "ready"
        assert record.estimate.estimated_at_utc == estimated_at
        assert record.estimate.expires_at_utc == expires_at
        assert record.recorded_at_utc == datetime.fromtimestamp(case.clock.now(), UTC).isoformat()
        cached = await case.service.cache.get(route_key, case.clock.now())
        assert isinstance(cached, dict)
        assert cached["estimated_at_utc"] == estimated_at and cached["expires_at_utc"] == expires_at
        assert len(case.transport.requests) == 1
        assert await _charges(database, case.initial.operation_identifier) == 1


async def test_stage_producer_checks_never_load_operation_output_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        case = await _case(database, tmp_path)
        prepared = await _run(database, case, case.initial)
        assert isinstance(prepared, WaitWork)
        captured = await _run(database, case, prepared.continuation)
        assert isinstance(captured, WaitWork)
        reference = captured.continuation.payload
        assert isinstance(reference, dict)
        state_identifier = reference["state_record_identifier"]
        acquisition_identifier = reference["acquisition_record_identifier"]
        assert isinstance(state_identifier, str) and isinstance(acquisition_identifier, str)
        _, _, value = await database.get_record(state_identifier)
        assert isinstance(value, dict)
        artifact_identifier = value["state_artifact_identifier"]
        assert isinstance(artifact_identifier, str)
        original = database.object_operation_relations
        inspected: list[str] = []

        async def producer(identifier: str, *, maximum_outputs: int | None = None):
            assert maximum_outputs == 0, "Producer-only checks must not load unbounded outputs"
            inspected.append(identifier)
            return await original(identifier, maximum_outputs=maximum_outputs)

        monkeypatch.setattr(database, "object_operation_relations", producer)
        outcome = await _finish(database, case, captured.continuation)
        assert isinstance(outcome, CompletedWork)
        assert {state_identifier, artifact_identifier, acquisition_identifier} <= set(inspected)
        assert len(case.transport.requests) == 1


async def test_state_manifest_cannot_borrow_another_operations_artifact_before_reading_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        checkpoint = await checkpoint_travel_state(
            database, _state(tmp_path), new_identifier=lambda: str(uuid4())
        )
        artifact_identifier = checkpoint.artifacts[0].identifier
        artifact_operation = await _publish(
            database,
            replace(
                checkpoint,
                records=(),
                outputs=tuple(
                    output
                    for output in checkpoint.outputs
                    if output.object_identifier == artifact_identifier
                ),
            ),
        )
        record_operation = await _publish(
            database,
            replace(
                checkpoint,
                artifacts=(),
                outputs=tuple(
                    output
                    for output in checkpoint.outputs
                    if output.object_identifier == checkpoint.record_identifier
                ),
            ),
        )
        assert artifact_operation != record_operation

        async def no_bytes(*_args: object, **_kwargs: object):
            raise AssertionError("Wrong-producer state reached artifact content")

        monkeypatch.setattr(database, "get_artifact", no_bytes)
        with pytest.raises(ValueError, match="artifact does not match"):
            await restore_travel_state(
                database, checkpoint.record_identifier, operation_identifier=record_operation
            )
