"""Durable stage descriptions separate logical attempts from execution leases."""

from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from carl.core.json import decode_json, encode_json
from carl.core.models import (
    ArtifactDraft,
    JsonStringEnumeration,
    JsonValue,
    NamedInput,
    NamedOutput,
    RecordDraft,
    StrictModel,
)
from carl.core.resources import ResourceLease, ResourceRequest
from carl.core.work import (
    NetworkActivityAdmission,
    NetworkActivityDefinition,
    WorkDefinition,
    WorkLease,
)
from carl.core.worker import AttemptContext, CompletedWork, RetryWork, TerminalFailureWork


class StageState(JsonStringEnumeration):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class StageWaitKind(JsonStringEnumeration):
    RESOURCE = "resource"
    TIME = "time"
    DEPENDENCY = "dependency"
    CONNECTIVITY = "connectivity"
    OWNERSHIP = "ownership"


class StageRequestCharge(StrictModel):
    """Only a newly committed charge permits one physical request invocation."""

    ordinal: int = Field(ge=1)
    newly_charged: bool


class StageDefinition(StrictModel):
    """A bounded persisted continuation; secrets and response buffers stay elsewhere."""

    identifier: str = Field(min_length=1)
    work_item_identifier: str = Field(min_length=1)
    operation_identifier: str = Field(min_length=1)
    kind: tuple[str, ...] = Field(min_length=1)
    schema_version: Literal[1] = 1
    payload: JsonValue
    resources: tuple[ResourceRequest, ...] = Field(default=(), max_length=32)
    not_before_utc_ns: int = Field(default=0, ge=0)
    dependency_work_identifiers: tuple[str, ...] = Field(default=(), max_length=1000)
    checkpoint_inputs: tuple[NamedInput, ...] = Field(default=(), max_length=1000)
    resident_session_identifier: str | None = Field(default=None, min_length=1)
    network_activity: NetworkActivityDefinition | None = None

    @field_validator("kind", "dependency_work_identifiers")
    @classmethod
    def validate_parts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not part for part in value):
            raise ValueError("Stage identities require nonempty parts")
        return value

    @model_validator(mode="after")
    def validate_requirements(self) -> "StageDefinition":
        if len(encode_json(self.payload).encode("utf-8")) > 65_536:
            raise ValueError("Stage payload exceeds the 64 KiB continuation limit")
        names = tuple(request.name for request in self.resources)
        if len(names) != len(set(names)):
            raise ValueError("Stage resource budgets must be unique")
        if len(self.dependency_work_identifiers) != len(set(self.dependency_work_identifiers)):
            raise ValueError("Stage dependencies must be unique")
        if self.work_item_identifier in self.dependency_work_identifiers:
            raise ValueError("A stage cannot depend on its own logical work")
        if self.network_activity is not None:
            if self.network_activity.operation_identifier != self.operation_identifier:
                raise ValueError("Stage and network activity must share a logical operation")
            if "http" not in names:
                raise ValueError("Network stages require an HTTP resource reservation")
        return self


class StageLease(StrictModel):
    definition: StageDefinition
    coordination_scope: str = Field(min_length=1)
    token: str = Field(min_length=1)
    execution: int = Field(ge=1)
    work_lease: WorkLease
    resource_lease: ResourceLease | None = None
    network_admission: NetworkActivityAdmission | None = None

    @model_validator(mode="after")
    def validate_fences(self) -> "StageLease":
        if self.work_lease.token != self.token:
            raise ValueError("Execution and work publication fences must agree")
        if self.work_lease.work_item_identifier != self.definition.work_item_identifier:
            raise ValueError("Stage execution must own its logical work")
        if self.resource_lease is not None and self.resource_lease.token != self.token:
            raise ValueError("Execution and resource fences must agree")
        expected_resources = tuple(
            sorted(self.definition.resources, key=lambda request: request.name)
        )
        if expected_resources:
            if self.resource_lease is None or self.resource_lease.allocations != expected_resources:
                raise ValueError("Stage reservation must match all required resources")
            if self.resource_lease.owner != self.work_lease.worker_identifier:
                raise ValueError("Stage resource and execution owners must agree")
            if self.resource_lease.scope != self.coordination_scope:
                raise ValueError("Stage resources must belong to the execution coordination scope")
        elif self.resource_lease is not None:
            raise ValueError("Resource-free stages cannot carry unrelated reservations")
        if self.network_admission is not None and self.network_admission.token != self.token:
            raise ValueError("Execution and network admission fences must agree")
        network = stage_network_activity(self.definition, self.execution)
        if network is None:
            if self.network_admission is not None:
                raise ValueError("Offline stages cannot carry unrelated network admission")
        elif (
            self.network_admission is None
            or self.network_admission.activity_identifier != network.identifier
        ):
            raise ValueError("Stage admission must match its exact network activity")
        return self


class StageClaimResult(StrictModel):
    lease: StageLease | None
    next_eligible_at_utc_ns: int | None = Field(default=None, ge=0)
    wait_kind: StageWaitKind | None = None


class WaitWork(StrictModel):
    """A continuation within an attempt, never an acquisition retry."""

    kind: Literal["wait"] = "wait"
    inputs: tuple[NamedInput, ...] = ()
    records: tuple[RecordDraft, ...] = ()
    artifacts: tuple[ArtifactDraft, ...] = ()
    outputs: tuple[NamedOutput, ...] = ()
    continuation: StageDefinition
    wait_kind: StageWaitKind
    result: JsonValue


type StageOutcome = Annotated[
    WaitWork | CompletedWork | RetryWork | TerminalFailureWork,
    Field(discriminator="kind"),
]


class StageAttemptContext(AttemptContext):
    stage_identifier: str = Field(min_length=1)
    execution: int = Field(ge=1)
    network_admission: NetworkActivityAdmission | None = None
    resident_session_identifier: str | None = Field(default=None, min_length=1)


def stage_network_activity(
    definition: StageDefinition, execution: int
) -> NetworkActivityDefinition | None:
    """Give every physical execution its own activity while preserving attempt identity."""

    if execution < 1:
        raise ValueError("Stage execution ordinal must be positive")
    template = definition.network_activity
    if template is None:
        return None
    return template.model_copy(
        update={
            "identifier": f"{template.identifier}:stage:{definition.identifier}:execution:{execution}",
        }
    )


def initial_work_stage(
    work: WorkDefinition,
    *,
    identifier: str,
    operation_identifier: str,
    kind: tuple[str, ...],
    resources: tuple[ResourceRequest, ...] = (),
) -> StageDefinition:
    """Adapt retained work payloads without rewriting their kind/schema or history."""

    return StageDefinition(
        identifier=identifier,
        work_item_identifier=work.identifier,
        operation_identifier=operation_identifier,
        kind=kind,
        payload={
            "work_kind": list(work.kind),
            "payload_schema_version": work.payload_schema_version,
            "payload": decode_json(encode_json(work.payload)),
        },
        resources=resources,
        not_before_utc_ns=work.not_before_utc_ns,
    )
