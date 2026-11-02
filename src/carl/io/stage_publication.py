"""In-memory child evidence published under a parent stage's exact fence."""

from dataclasses import dataclass

from carl.core.components import Component
from carl.core.models import ArtifactDraft, JsonValue, NamedInput, NamedOutput, RecordDraft
from carl.core.stages import StageOutcome


@dataclass(frozen=True, slots=True)
class ChildOperationDraft:
    identifier: str
    component: Component
    configuration: JsonValue
    inputs: tuple[NamedInput, ...]
    records: tuple[RecordDraft, ...]
    artifacts: tuple[ArtifactDraft, ...]
    outputs: tuple[NamedOutput, ...]
    result: JsonValue
    started_at_utc: str
    ended_at_utc: str
    duration_ns: int
    error: JsonValue = None


@dataclass(frozen=True, slots=True)
class StagePublication:
    outcome: StageOutcome
    children: tuple[ChildOperationDraft, ...] = ()

    def __post_init__(self) -> None:
        if len(self.children) > 32:
            raise ValueError("A stage may publish at most 32 child operations")
        identifiers = tuple(child.identifier for child in self.children)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("Child operation identifiers must be unique")


type StageHandlerResult = StageOutcome | StagePublication
