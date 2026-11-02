"""Persisted continuations retain logical work and fence each execution."""

import pytest
from pydantic import ValidationError

from carl.core.json import encode_json
from carl.core.models import JsonValue, NamedInput
from carl.core.resources import ResourceLease, ResourceRequest
from carl.core.stages import (
    StageDefinition,
    StageLease,
    StageWaitKind,
    WaitWork,
    initial_work_stage,
    stage_network_activity,
)
from carl.core.work import (
    NetworkActivityAdmission,
    NetworkActivityDefinition,
    SchedulingScope,
    SchedulingScopeKind,
    WorkDefinition,
    WorkLease,
)
from carl.core.worker import RetryWork


def _work() -> WorkDefinition:
    return WorkDefinition(
        identifier="work-1",
        kind=("retained", "work"),
        payload_schema_version=7,
        payload={"retained": ["original", {"value": 3}]},
        deduplication_identity=("original",),
        not_before_utc_ns=123,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=("retained", "work")),
        ),
    )


def _network() -> NetworkActivityDefinition:
    return NetworkActivityDefinition(
        identifier="network-1",
        kind=("test", "http"),
        operation_identifier="operation-1",
        network_session_identifier="session-1",
        ordinal=1,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND, identity=("test", "http")
            ),
        ),
    )


def _stage(**overrides: JsonValue) -> StageDefinition:
    values: dict[str, JsonValue] = {
        "identifier": "stage-1",
        "work_item_identifier": "work-1",
        "operation_identifier": "operation-1",
        "kind": ("test", "continuation"),
        "payload": {"checkpoint": "record-1"},
    }
    values.update(overrides)
    return StageDefinition.model_validate(values)


def _lease(**overrides: JsonValue) -> StageLease:
    work = _work()
    values: dict[str, JsonValue] = {
        "definition": _stage(
            resources=(ResourceRequest(name="http"),),
            network_activity=_network(),
            resident_session_identifier="session-1",
        ),
        "token": "execution-1",
        "coordination_scope": "host:test",
        "execution": 1,
        "work_lease": WorkLease(
            work_item_identifier=work.identifier,
            token="execution-1",
            worker_identifier="worker-1",
            attempt=1,
            expires_at_utc_ns=100,
            kind=work.kind,
            payload_schema_version=work.payload_schema_version,
            payload=work.payload,
        ),
        "resource_lease": ResourceLease(
            scope="host:test",
            token="execution-1",
            owner="worker-1",
            allocations=(ResourceRequest(name="http"),),
            acquired_at_utc_ns=1,
            expires_at_utc_ns=100,
        ),
        "network_admission": NetworkActivityAdmission(
            activity_identifier="network-1:stage:stage-1:execution:1",
            token="execution-1",
            admitted_at_utc_ns=1,
            permit_expires_at_utc_ns=100,
        ),
    }
    values.update(overrides)
    return StageLease.model_validate(values)


def test_initial_stage_preserves_retained_work_payload_kind_and_schema() -> None:
    work = _work()
    original = work.model_dump_json()
    resources = (ResourceRequest(name="cpu", units=2),)
    stage = initial_work_stage(
        work,
        identifier="stage-1",
        operation_identifier="operation-1",
        kind=("new", "stage"),
        resources=resources,
    )
    assert work.model_dump_json() == original
    assert stage.kind == ("new", "stage") and stage.schema_version == 1
    assert stage.payload == {
        "work_kind": ["retained", "work"],
        "payload_schema_version": 7,
        "payload": work.payload,
    }
    assert stage.work_item_identifier == work.identifier
    assert stage.operation_identifier == "operation-1"
    assert stage.not_before_utc_ns == work.not_before_utc_ns
    assert stage.resources == resources
    assert StageDefinition.model_validate_json(stage.model_dump_json()) == stage
    with pytest.raises(ValidationError, match="frozen"):
        stage.kind = ("changed",)
    with pytest.raises(ValidationError, match="frozen"):
        work.payload_schema_version = 1
    stage.payload["payload"]["retained"][1]["value"] = 4
    assert work.model_dump_json() == original
    work.payload["retained"].append("later change")
    assert stage.payload["payload"]["retained"] == ["original", {"value": 4}]


def test_wait_is_a_continuation_instead_of_an_acquisition_retry() -> None:
    stage = _stage(not_before_utc_ns=1000, dependency_work_identifiers=("child-1",))
    wait = WaitWork(
        continuation=stage, wait_kind=StageWaitKind.DEPENDENCY, result={"state": "waiting"}
    )
    retry = RetryWork(delay_ns=1000, reason={"kind": "transport"}, result={})
    assert WaitWork.model_validate_json(wait.model_dump_json()) == wait
    assert wait.continuation is stage and not isinstance(wait, RetryWork)
    with pytest.raises(ValidationError):
        _ = RetryWork.model_validate(wait.model_dump())
    with pytest.raises(ValidationError):
        _ = WaitWork.model_validate(retry.model_dump())


@pytest.mark.parametrize(
    "overrides",
    (
        {"resources": ({"name": "http", "units": 0},)},
        {"resources": ({"name": "http", "units": True},)},
        {"resources": ({"name": " http"},)},
        {"resources": ({"name": "http"}, {"name": "http"})},
        {"resources": tuple({"name": f"resource-{index}"} for index in range(33))},
        {"kind": ()},
        {"kind": ("test", "")},
        {"dependency_work_identifiers": ("child-1", "child-1")},
        {"dependency_work_identifiers": ("work-1",)},
        {"dependency_work_identifiers": ("",)},
        {"not_before_utc_ns": -1},
        {"resident_session_identifier": ""},
    ),
)
def test_invalid_stage_requirements_are_rejected(overrides: dict[str, JsonValue]) -> None:
    with pytest.raises(ValidationError):
        _ = _stage(**overrides)


def test_network_stage_requires_http_capacity_and_matching_operation() -> None:
    with pytest.raises(ValidationError, match="HTTP resource reservation"):
        _ = _stage(network_activity=_network(), resources=(ResourceRequest(name="cpu"),))
    with pytest.raises(ValidationError, match="share a logical operation"):
        _ = _stage(
            operation_identifier="other-operation",
            network_activity=_network(),
            resources=(ResourceRequest(name="http"),),
        )


@pytest.mark.parametrize(("character", "count"), (("x", 65_534), ("é", 32_767)))
def test_stage_payload_limit_counts_encoded_utf8_bytes(character: str, count: int) -> None:
    payload = character * count
    assert len(encode_json(payload).encode("utf-8")) == 65_536
    assert _stage(payload=payload).payload == payload
    with pytest.raises(ValidationError, match="64 KiB"):
        _ = _stage(payload=payload + character)


def test_checkpoint_and_dependency_references_are_bounded() -> None:
    dependencies = tuple(f"child-{index}" for index in range(1000))
    checkpoints = tuple(
        NamedInput(name=("checkpoint", str(index)), object_identifier=f"record-{index}")
        for index in range(1000)
    )
    stage = _stage(dependency_work_identifiers=dependencies, checkpoint_inputs=checkpoints)
    assert len(stage.dependency_work_identifiers) == len(stage.checkpoint_inputs) == 1000
    with pytest.raises(ValidationError):
        _ = _stage(dependency_work_identifiers=(*dependencies, "extra-child"))
    with pytest.raises(ValidationError):
        _ = _stage(
            checkpoint_inputs=(*checkpoints, NamedInput(name=("extra",), object_identifier="r"))
        )


def test_execution_roundtrip_retains_each_publication_fence() -> None:
    lease = _lease()
    assert StageLease.model_validate_json(lease.model_dump_json()) == lease
    assert lease.work_lease.token == lease.token
    assert lease.resource_lease is not None and lease.resource_lease.token == lease.token
    assert lease.network_admission is not None and lease.network_admission.token == lease.token


@pytest.mark.parametrize(
    ("field", "changes", "message"),
    (
        ("work_lease", {"token": "stale"}, "work publication fences"),
        ("work_lease", {"work_item_identifier": "other-work"}, "own its logical work"),
        ("resource_lease", {"token": "stale"}, "resource fences"),
        ("network_admission", {"token": "stale"}, "network admission fences"),
    ),
)
def test_execution_rejects_mismatched_fences(
    field: str, changes: dict[str, str], message: str
) -> None:
    values = _lease().model_dump()
    values[field].update(changes)
    with pytest.raises(ValidationError, match=message):
        _ = StageLease.model_validate(values)


@pytest.mark.parametrize("execution", (0, -1))
def test_execution_ordinal_must_be_positive(execution: int) -> None:
    with pytest.raises(ValidationError):
        _ = _lease(execution=execution)


@pytest.mark.parametrize(
    ("field", "changes", "message"),
    (
        ("resource_lease", None, "all required resources"),
        ("resource_lease", {"owner": "other-worker"}, "owners must agree"),
        ("resource_lease", {"scope": "host:other"}, "coordination scope"),
        (
            "resource_lease",
            {"allocations": ({"name": "cpu", "units": 1},)},
            "all required resources",
        ),
        (
            "resource_lease",
            {"allocations": ({"name": "http", "units": 2},)},
            "all required resources",
        ),
        (
            "resource_lease",
            {"allocations": ({"name": "cpu", "units": 1}, {"name": "http", "units": 1})},
            "all required resources",
        ),
        ("network_admission", None, "exact network activity"),
        ("network_admission", {"activity_identifier": "network-1"}, "exact network activity"),
        (
            "network_admission",
            {"activity_identifier": "network-1:stage:other-stage:execution:1"},
            "exact network activity",
        ),
        (
            "network_admission",
            {"activity_identifier": "network-1:stage:stage-1:execution:2"},
            "exact network activity",
        ),
    ),
)
def test_execution_rejects_missing_or_unrelated_admission(
    field: str, changes: dict[str, JsonValue] | None, message: str
) -> None:
    values = _lease().model_dump()
    if changes is None:
        values[field] = None
    else:
        values[field].update(changes)
    with pytest.raises(ValidationError, match=message):
        _ = StageLease.model_validate(values)


def test_resource_free_and_offline_executions_reject_unrelated_admission() -> None:
    with pytest.raises(ValidationError, match="Resource-free stages"):
        _ = _lease(definition=_stage(), network_admission=None)
    with pytest.raises(ValidationError, match="Offline stages"):
        _ = _lease(
            definition=_stage(resources=(ResourceRequest(name="http"),)),
        )
    lease = _lease(definition=_stage(), resource_lease=None, network_admission=None)
    assert lease.resource_lease is None and lease.network_admission is None
    assert lease.coordination_scope == "host:test"
    values = lease.model_dump()
    del values["coordination_scope"]
    with pytest.raises(ValidationError, match="coordination_scope"):
        _ = StageLease.model_validate(values)


def test_multiple_resource_requirements_match_canonical_allocation_order() -> None:
    values = _lease().model_dump()
    values["definition"]["resources"] = (
        {"name": "http", "units": 1},
        {"name": "cpu", "units": 2},
    )
    values["resource_lease"]["allocations"] = (
        {"name": "cpu", "units": 2},
        {"name": "http", "units": 1},
    )
    lease = StageLease.model_validate(values)
    assert lease.resource_lease is not None
    assert tuple(request.name for request in lease.resource_lease.allocations) == ("cpu", "http")


def test_network_executions_have_separate_admission_identity_with_one_logical_attempt() -> None:
    template = _network()
    stage = _stage(network_activity=template, resources=(ResourceRequest(name="http"),))
    first = stage_network_activity(stage, 1)
    second = stage_network_activity(stage, 2)
    assert first is not None and second is not None
    assert first.identifier == "network-1:stage:stage-1:execution:1"
    assert second.identifier == "network-1:stage:stage-1:execution:2"
    assert first.model_dump(exclude={"identifier"}) == template.model_dump(exclude={"identifier"})
    assert second.model_dump(exclude={"identifier"}) == first.model_dump(exclude={"identifier"})
    assert stage.network_activity == template
    assert stage_network_activity(_stage(), 1) is None
    with pytest.raises(ValueError, match="positive"):
        _ = stage_network_activity(stage, 0)
