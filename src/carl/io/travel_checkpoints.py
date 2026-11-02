"""Bounded external travel state referenced by small, immutable stage records."""

from collections.abc import Callable
from dataclasses import dataclass

from carl.core.models import ExternalFileDraft, NamedOutput, RecordDraft
from carl.core.travel_phases import TravelState
from carl.io.http_checkpoints import publish_checkpoint_artifact
from carl.io.sqlite import Database

MAX_TRAVEL_STATE_BYTES = 1024 * 1024
TRAVEL_STATE_KIND = ("carl", "travel", "stage_state")
TRAVEL_STATE_ARTIFACT_KIND = ("carl", "travel", "state_json")


@dataclass(frozen=True, slots=True)
class TravelCheckpoint:
    record_identifier: str
    records: tuple[RecordDraft, ...]
    artifacts: tuple[ExternalFileDraft, ...]
    outputs: tuple[NamedOutput, ...]


async def checkpoint_travel_state(
    database: Database, state: TravelState, *, new_identifier: Callable[[], str]
) -> TravelCheckpoint:
    content = state.model_dump_json().encode()
    artifact = await publish_checkpoint_artifact(
        content,
        data_directory=database.path.parent,
        identifier=new_identifier(),
        kind=TRAVEL_STATE_ARTIFACT_KIND,
        media_type="application/json",
        representation={"kind": "travel_stage_state", "schema_version": 1},
        maximum_bytes=MAX_TRAVEL_STATE_BYTES,
    )
    record = RecordDraft(
        identifier=new_identifier(),
        kind=TRAVEL_STATE_KIND,
        schema_version=1,
        value={
            "state_artifact_identifier": artifact.identifier,
            "sha256": artifact.sha256,
            "size": artifact.size,
        },
    )
    return TravelCheckpoint(
        record.identifier,
        (record,),
        (artifact,),
        (
            NamedOutput(
                name=("travel", "state", record.identifier), object_identifier=record.identifier
            ),
            NamedOutput(
                name=("travel", "state", record.identifier, "json"),
                object_identifier=artifact.identifier,
            ),
        ),
    )


async def restore_travel_state(
    database: Database, identifier: str, *, operation_identifier: str
) -> TravelState:
    kind, schema, value = await database.get_record(identifier)
    operation, _, _ = await database.object_operation_relations(identifier, maximum_outputs=0)
    if (
        kind != TRAVEL_STATE_KIND
        or schema != 1
        or not isinstance(value, dict)
        or operation != operation_identifier
    ):
        raise ValueError("Travel state belongs to another logical attempt or schema")
    artifact_identifier = value.get("state_artifact_identifier")
    size, digest = value.get("size"), value.get("sha256")
    if (
        not isinstance(artifact_identifier, str)
        or type(size) is not int
        or not 0 <= size <= MAX_TRAVEL_STATE_BYTES
        or not isinstance(digest, str)
    ):
        raise ValueError("Travel state checkpoint metadata is malformed")
    metadata = await database.get_external_artifact_metadata(artifact_identifier)
    artifact_operation, _, _ = await database.object_operation_relations(
        artifact_identifier, maximum_outputs=0
    )
    if (
        artifact_operation != operation_identifier
        or metadata is None
        or metadata.get("size") != size
        or metadata.get("sha256") != digest
        or metadata.get("media_type") != "application/json"
        or metadata.get("representation") != {"kind": "travel_stage_state", "schema_version": 1}
    ):
        raise ValueError("Travel state checkpoint artifact does not match its manifest")
    _, content = await database.get_artifact(artifact_identifier, maximum_bytes=size)
    return TravelState.model_validate_json(content)
