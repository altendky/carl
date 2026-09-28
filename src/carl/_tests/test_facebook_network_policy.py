"""Marketplace page pacing supersedes old limits without losing provenance."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from carl.cli import _activate_facebook_page_network_policy
from carl.core.components import Component, ComponentId
from carl.core.facebook_work import (
    ITEM_PAGE_NETWORK_ACTIVITY_KIND,
    facebook_item_network_activity,
    legacy_facebook_network_constraint_identifiers,
)
from carl.core.models import CodeProvenance
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
)
from carl.io.sqlite import Database


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


@pytest.mark.anyio
async def test_page_policy_retires_route_and_origin_limits(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    routing = ("proton", "personal", "test")
    legacy = legacy_facebook_network_constraint_identifiers(routing)
    origin = SchedulingScope(
        kind=SchedulingScopeKind.REMOTE_ORIGIN,
        identity=("https", "www.facebook.com", "443"),
    )
    old_constraints = (
        SlidingWindowRateConstraint(
            identifier=legacy[0],
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=origin,
            maximum_starts=12,
            period_ns=60_000_000_000,
        ),
        SlidingWindowRateConstraint(
            identifier=legacy[1],
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=routing),
            maximum_starts=12,
            period_ns=60_000_000_000,
        ),
        UniformHoldoffConstraint(
            identifier=legacy[3],
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                identity=ITEM_PAGE_NETWORK_ACTIVITY_KIND,
            ),
            minimum_ns=2_000_000_000,
            maximum_ns=5_000_000_000,
        ),
    )
    async with Database.managed(path, initialize=True) as database:
        for constraint in old_constraints:
            await database.register_constraint(constraint, registered_at_utc_ns=1)
        await _activate_facebook_page_network_policy(database, routing)
        await database.begin_operation(
            operation_id="activity-operation",
            component=Component(ComponentId(("test", "network_activity")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-22T00:00:00+00:00",
            inputs=(),
        )
        for ordinal in range(1, 14):
            now_ns = ordinal * 1_000_000_000
            identifier = f"item-{ordinal}"
            await database.create_network_activity(
                facebook_item_network_activity(
                    identifier=identifier,
                    operation_identifier="activity-operation",
                    network_session_identifier="session",
                    attempt=1,
                    routing=routing,
                ),
                created_at_utc_ns=now_ns,
                event_identifier=f"created-{ordinal}",
                sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
            )
            activity = await database.network_activity(identifier)
            assert activity["eligible_at_utc_ns"] == now_ns
            admitted = await database.try_admit_network_activity(
                network_activity_identifier=identifier,
                admission_token=f"token-{ordinal}",
                permit_duration_ns=1_000_000_000,
                now_utc_ns=now_ns,
                event_identifier=f"admitted-{ordinal}",
            )
            assert admitted.admission is not None

    with closing(sqlite3.connect(path)) as connection:
        retired = {
            tuple(json.loads(row[0]))
            for row in connection.execute(
                "SELECT identifier_parts_json FROM scheduling_constraint_retirements"
            )
        }
        policy_records = connection.execute(
            "SELECT count(*) FROM objects WHERE kind_parts_json = ?",
            (json.dumps(["carl", "facebook", "network_policy_activation"], separators=(",", ":")),),
        ).fetchone()
    assert retired == {constraint.identifier for constraint in old_constraints}
    assert policy_records == (1,)
