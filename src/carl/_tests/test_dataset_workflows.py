"""Dataset journals bound durable metadata without copying source files or secrets."""

from pathlib import Path
from time import time_ns

import pytest
from pydantic import ValidationError

from carl.core.dataset_workflows import (
    DatasetWorkflowBinding,
    DatasetWorkflowDefinition,
    DatasetWorkflowFence,
    DatasetWorkflowJournal,
)
from carl.core.travel_data import TravelDataSetupRequest


def _definition() -> DatasetWorkflowDefinition:
    return DatasetWorkflowDefinition(
        workflow_identifier="offline-workflow",
        request=TravelDataSetupRequest(request_identifier="offline", provider="geonames"),
        recipe_identifier="geonames-v1",
        configuration_identity="public-config-hash",
    )


def _journal() -> DatasetWorkflowJournal:
    definition = _definition()
    return DatasetWorkflowJournal(
        definition=definition,
        fence=DatasetWorkflowFence(
            workflow_identifier=definition.workflow_identifier, generation=1, token="1" * 32
        ),
        lock_identity=(1, 2),
        revision=1,
        updated_at_utc_ns=time_ns(),
    )


@pytest.mark.parametrize("identifier", ("../outside", "a/b", "", "a" * 201))
def test_definition_rejects_unsafe_workflow_paths(identifier: str) -> None:
    with pytest.raises(ValidationError):
        _ = DatasetWorkflowDefinition.model_validate(
            {**_definition().model_dump(), "workflow_identifier": identifier}
        )


def test_binding_is_genuine_absolute_and_definition_is_frozen() -> None:
    with pytest.raises(ValidationError):
        _ = DatasetWorkflowBinding(
            database_path=Path("relative.sqlite3"),
            work_item_identifier="work",
            operation_identifier="operation",
        )
    with pytest.raises(ValidationError):
        _definition().recipe_identifier = "changed"  # type: ignore[misc]


@pytest.mark.parametrize("field", ("plan", "checkpoint"))
def test_checkpoint_limits_apply_to_encoded_utf8_and_finite_json(field: str) -> None:
    value = _journal().model_dump()
    value[field] = {"source": "é" * 40_000}
    with pytest.raises(ValidationError, match="64 KiB"):
        _ = DatasetWorkflowJournal.model_validate(value)
    value[field] = {"source": float("nan")}
    with pytest.raises(ValidationError):
        _ = DatasetWorkflowJournal.model_validate(value)


def test_journal_supports_more_than_stage_payload_but_bounds_total_and_fence() -> None:
    value = _journal().model_dump()
    value["receipt_references"] = tuple("a" * 1000 for _ in range(100))
    journal = DatasetWorkflowJournal.model_validate(value)
    assert len(journal.model_dump_json()) > 65_536
    value["receipt_references"] = tuple("a" * 1500 for _ in range(1000))
    with pytest.raises(ValidationError, match="1 MiB"):
        _ = DatasetWorkflowJournal.model_validate(value)
    value = _journal().model_dump()
    value["fence"] = DatasetWorkflowFence(
        workflow_identifier="another", generation=1, token="1" * 32
    )
    with pytest.raises(ValidationError, match="agree"):
        _ = DatasetWorkflowJournal.model_validate(value)
