"""Scheduler policy changes preserve old decisions without retaining old limits."""

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

import pytest

from carl.core.components import Component, ComponentId
from carl.core.json import encode_json
from carl.core.models import CodeProvenance
from carl.core.work import (
    HoldoffDecision,
    NetworkActivityDefinition,
    NetworkActivityState,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
    WorkDefinition,
    WorkRequester,
)
from carl.io.sqlite import DATABASE_SCHEMA, Database

_OLD_SHA256 = "b3fa83413034992e01f84abf8ffe4993864cfad23b4792442b38b60a47b8dca6"
_NETWORK_KIND = ("carl", "test", "network_activity")
_NETWORK_SCOPE = SchedulingScope(
    kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND, identity=_NETWORK_KIND
)
_OVERALL_SCOPE = SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=())
_WORK_SCOPE = SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=("test",))


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="dirty",
        package_version="test",
        python_implementation="test",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


def _activity(identifier: str, ordinal: int) -> NetworkActivityDefinition:
    return NetworkActivityDefinition(
        identifier=identifier,
        kind=_NETWORK_KIND,
        operation_identifier="policy-operation",
        network_session_identifier="session",
        ordinal=ordinal,
        scopes=(_OVERALL_SCOPE, _NETWORK_SCOPE),
    )


@pytest.mark.anyio
async def test_v1_migration_and_atomic_constraint_supersession(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    old_network = SlidingWindowRateConstraint(
        identifier=("test", "network", "v1"),
        subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
        scope=_NETWORK_SCOPE,
        maximum_starts=1,
        period_ns=100,
    )
    new_network = old_network.model_copy(
        update={"identifier": ("test", "network", "v2"), "maximum_starts": 2}
    )
    old_work = UniformHoldoffConstraint(
        identifier=("test", "work", "v1"),
        scope=_WORK_SCOPE,
        minimum_ns=99,
        maximum_ns=99,
    )
    new_work = old_work.model_copy(
        update={"identifier": ("test", "work", "v2"), "minimum_ns": 1, "maximum_ns": 1}
    )

    async with Database.managed(path, initialize=True) as database:
        await database.begin_operation(
            operation_id="policy-operation",
            component=Component(ComponentId(("carl", "test", "policy")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.register_constraint(old_network, registered_at_utc_ns=1)
        await database.register_constraint(old_work, registered_at_utc_ns=1)
        await database.create_network_activity(
            _activity("old-activity", 1),
            created_at_utc_ns=10,
            event_identifier="old-created",
            sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
        )
        old_admission = await database.try_admit_network_activity(
            network_activity_identifier="old-activity",
            admission_token="old-token",
            permit_duration_ns=100,
            now_utc_ns=10,
            event_identifier="old-admitted",
        )
        assert old_admission.admission is not None
        await database.finish_network_activity(
            network_activity_identifier="old-activity",
            admission_token="old-token",
            state=NetworkActivityState.COMPLETED,
            ended_at_utc_ns=11,
            result={},
            event_identifier="old-completed",
        )

    # Recreate the exact old metadata and table set to exercise a real v1 upgrade.
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("DROP TABLE scheduling_constraint_retirements")
        previous = DATABASE_SCHEMA.model_copy(update={"version": 1})
        connection.executemany(
            "UPDATE schema_metadata SET value = ? WHERE key = ?",
            (
                (encode_json(previous.model_dump(mode="json")), "schema_identity_json"),
                ("1", "schema_version"),
                (_OLD_SHA256, "schema_definition_sha256"),
            ),
        )
        connection.commit()

    async with Database.managed(path) as database:
        await database.supersede_constraints(
            retired_identifiers=(old_network.identifier, old_work.identifier),
            replacements=(new_network, new_work),
            operation_identifier="policy-operation",
            at_utc_ns=20,
            reason="image-policy-v2",
        )
        await database.supersede_constraints(
            retired_identifiers=(old_network.identifier, old_work.identifier),
            replacements=(new_network, new_work),
            operation_identifier="policy-operation",
            at_utc_ns=21,
            reason="image-policy-v2",
        )
        await database.supersede_constraints(
            retired_identifiers=(old_network.identifier,),
            replacements=(new_network, new_work),
            operation_identifier="policy-operation",
            at_utc_ns=22,
            reason="later-policy",
        )

        for ordinal in (2, 3):
            identifier = f"new-{ordinal}"
            await database.create_network_activity(
                _activity(identifier, ordinal),
                created_at_utc_ns=20 + ordinal,
                event_identifier=f"created-{ordinal}",
                sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
            )
            result = await database.try_admit_network_activity(
                network_activity_identifier=identifier,
                admission_token=f"token-{ordinal}",
                permit_duration_ns=100,
                now_utc_ns=20 + ordinal,
                event_identifier=f"admitted-{ordinal}",
            )
            assert result.admission is not None
        await database.create_network_activity(
            _activity("new-4", 4),
            created_at_utc_ns=24,
            event_identifier="created-4",
            sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
        )
        third = await database.try_admit_network_activity(
            network_activity_identifier="new-4",
            admission_token="token-4",
            permit_duration_ns=100,
            now_utc_ns=24,
            event_identifier="blocked-4",
        )
        assert third.admission is None
        assert third.next_eligible_at_utc_ns == 122

        enqueued = await database.enqueue_work(
            WorkDefinition(
                identifier="work",
                kind=("carl", "test", "work"),
                payload_schema_version=1,
                payload={},
                deduplication_identity=("one",),
                not_before_utc_ns=0,
                scopes=(_OVERALL_SCOPE, _WORK_SCOPE),
            ),
            WorkRequester(
                request_identifier="work-request",
                kind=("carl", "test", "requester"),
                identifier="source",
                context={},
            ),
            event_identifier="work-enqueued",
            enqueued_at_utc_ns=30,
            holdoff_decisions=(
                HoldoffDecision(constraint_identifier=new_work.identifier, sampled_delay_ns=1),
            ),
        )
        assert enqueued.eligible_at_utc_ns == 31

    with closing(sqlite3.connect(path)) as connection:
        metadata = dict(connection.execute("SELECT key, value FROM schema_metadata"))
        definitions = connection.execute("SELECT count(*) FROM scheduling_constraints").fetchone()
        retirements = connection.execute(
            "SELECT operation_id, reason FROM scheduling_constraint_retirements ORDER BY identifier_parts_json"
        ).fetchall()
        old_event = connection.execute(
            "SELECT data_json FROM network_activity_events WHERE id = 'old-admitted'"
        ).fetchone()
        old_start = connection.execute(
            "SELECT count(*) FROM rate_starts WHERE constraint_identifier_parts_json = ?",
            (encode_json(list(old_network.identifier)),),
        ).fetchone()
    assert metadata["schema_version"] == str(DATABASE_SCHEMA.version)
    assert definitions == (4,)
    assert retirements == [("policy-operation", "image-policy-v2")] * 2
    assert old_event is not None
    assert json.loads(old_event[0])["constraints"][0]["identifier"] == list(old_network.identifier)
    assert old_start == (1,)

    async with Database.managed(path) as database:
        assert (await database.network_activity("old-activity"))["state"] == "completed"
