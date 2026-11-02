"""Travel continuations reference bounded, verified external state within one attempt."""

# Corruption fixtures deliberately inspect storage and reject reads before allocation.
# pyright: reportPrivateUsage=false

from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

import carl.io.sqlite as sqlite
import carl.io.travel_checkpoints as checkpoints
from carl._tests.test_worker import _provenance
from carl.core.components import Component, ComponentId
from carl.core.configuration import GeonamesConfiguration, OsrmConfiguration, TravelConfiguration
from carl.core.travel import TravelCoordinate, TravelDestination
from carl.core.travel_phases import TravelSnapshot, TravelState, start_travel
from carl.io.sqlite import Database
from carl.io.travel_checkpoints import (
    TRAVEL_STATE_ARTIFACT_KIND,
    TravelCheckpoint,
    checkpoint_travel_state,
    restore_travel_state,
)

pytestmark = pytest.mark.anyio


def _state(root: Path) -> TravelState:
    return start_travel(
        TravelSnapshot(
            configuration=TravelConfiguration(
                origin=TravelCoordinate(latitude=10, longitude=20),
                provider="osrm",
                geocoding_provider="geonames",
            ),
            osrm=OsrmConfiguration(dataset_path=root / "roads.osrm", dataset_identifier="roads-v1"),
            geonames=GeonamesConfiguration(
                database_path=root / "places.sqlite3", dataset_identifier="places-v1"
            ),
        ),
        (TravelDestination(label="Example City"),),
        mode="refresh",
        expected_origin_identity="selected-origin",
    )


async def _publish(database: Database, checkpoint: TravelCheckpoint) -> str:
    operation = str(uuid4())
    await database.begin_operation(
        operation_id=operation,
        component=Component(ComponentId(("test", "travel_state")), 1, lambda: None),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-11-02T12:00:00+00:00",
    )
    await database.complete_operation(
        operation_id=operation,
        records=checkpoint.records,
        artifacts=checkpoint.artifacts,
        outputs=checkpoint.outputs,
        result={},
        ended_at_utc="2026-11-02T12:00:01+00:00",
        duration_ns=1_000_000_000,
    )
    return operation


async def test_state_survives_restart_with_small_manifest_and_correct_artifact_kind(
    tmp_path: Path,
) -> None:
    path, state = tmp_path / "carl.sqlite3", _state(tmp_path)
    async with Database.managed(path, initialize=True) as database:
        checkpoint = await checkpoint_travel_state(
            database, state, new_identifier=lambda: str(uuid4())
        )
        operation = await _publish(database, checkpoint)
        assert checkpoint.artifacts[0].kind == TRAVEL_STATE_ARTIFACT_KIND
        assert len(checkpoint.records[0].model_dump_json()) < 1000
        async with database._connections.reader() as connection:
            row = await (await connection.execute("SELECT count(*) FROM artifacts")).fetchone()
            assert row == (0,)
    async with Database.managed(path) as database:
        assert (
            await restore_travel_state(
                database, checkpoint.record_identifier, operation_identifier=operation
            )
            == state
        )
        with pytest.raises(ValueError, match="logical attempt"):
            _ = await restore_travel_state(
                database, checkpoint.record_identifier, operation_identifier="another-operation"
            )


async def test_oversized_manifest_rejects_before_artifact_materialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        checkpoint = await checkpoint_travel_state(
            database, _state(tmp_path), new_identifier=lambda: str(uuid4())
        )
        value = checkpoint.records[0].value
        assert isinstance(value, dict)
        changed = checkpoint.records[0].model_copy(
            update={"value": {**value, "size": checkpoints.MAX_TRAVEL_STATE_BYTES + 1}}
        )
        checkpoint = replace(checkpoint, records=(changed,))
        operation = await _publish(database, checkpoint)

        async def no_read(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("Oversized state reached artifact materialization")

        monkeypatch.setattr(database, "get_artifact", no_read)
        with pytest.raises(ValueError, match="metadata"):
            _ = await restore_travel_state(
                database, checkpoint.record_identifier, operation_identifier=operation
            )


async def test_growing_state_file_is_rejected_before_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        checkpoint = await checkpoint_travel_state(
            database, _state(tmp_path), new_identifier=lambda: str(uuid4())
        )
        operation = await _publish(database, checkpoint)
        artifact = checkpoint.artifacts[0]
        (tmp_path / artifact.locator).write_bytes(b"x" * (artifact.size + 1))

        def no_read(*_args: object) -> bytes:
            raise AssertionError("Grown state reached file read")

        monkeypatch.setattr(sqlite.os, "read", no_read)
        with pytest.raises(ValueError, match="maximum_bytes"):
            _ = await restore_travel_state(
                database, checkpoint.record_identifier, operation_identifier=operation
            )


async def test_state_byte_bound_precedes_filesystem_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        monkeypatch.setattr(checkpoints, "MAX_TRAVEL_STATE_BYTES", 64)
        with pytest.raises(ValueError, match="byte bound"):
            _ = await checkpoint_travel_state(
                database, _state(tmp_path), new_identifier=lambda: str(uuid4())
            )
        assert not (tmp_path / "responses").exists()
