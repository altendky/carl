"""Marketplace page pacing supersedes old limits without losing provenance."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import TypedDict

import pytest

from carl.cli import _activate_facebook_page_network_policy
from carl.core.components import Component, ComponentId
from carl.core.facebook_images import image_network_activity
from carl.core.facebook_work import (
    ITEM_PAGE_NETWORK_ACTIVITY_KIND,
    SEARCH_SESSION_BOOTSTRAP_NETWORK_ACTIVITY_KIND,
    facebook_item_network_activity,
    facebook_network_path_constraint,
    facebook_search_network_activity,
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
from carl.io.network_activity import network_activity_definition
from carl.io.sqlite import Database


class _ActivityContext(TypedDict):
    operation_identifier: str
    network_session_identifier: str
    attempt: int


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


@pytest.mark.anyio
@pytest.mark.parametrize("provider", ("decodo", "proton"))
async def test_request_budget_counts_http_once_and_excludes_setup_and_other_sources(
    tmp_path: Path, provider: str
) -> None:
    routing = (provider, "personal", "test")
    budget = facebook_network_path_constraint(routing).model_copy(update={"maximum_starts": 3})
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.register_constraint(budget, registered_at_utc_ns=1)
        await database.begin_operation(
            operation_id="operation",
            component=Component(ComponentId(("test", "request_budget")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-10-18T00:00:00+00:00",
        )
        common = _ActivityContext(
            operation_identifier="operation",
            network_session_identifier="session",
            attempt=1,
        )
        # All three actual Facebook HTTP paths share the same route allowance.
        requests = (
            image_network_activity(
                identifier="image",
                routing=routing,
                url="https://scontent.example.fbcdn.net/image.jpg",
                **common,
            ),
            facebook_item_network_activity(identifier="item", routing=routing, **common),
            facebook_search_network_activity(
                identifier="search",
                kind=SEARCH_SESSION_BOOTSTRAP_NETWORK_ACTIVITY_KIND,
                routing=routing,
                ordinal=1,
                **common,
            ),
        )
        for activity in requests:
            await database.create_network_activity(
                activity,
                created_at_utc_ns=10,
                event_identifier=activity.identifier + "-created",
                sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
            )
            result = await database.try_admit_network_activity(
                network_activity_identifier=activity.identifier,
                admission_token=activity.identifier + "-token",
                permit_duration_ns=100,
                now_utc_ns=10,
                event_identifier=activity.identifier + "-admitted",
            )
            assert result.admission is not None

        # Exhausting the HTTP budget must not block local setup or eBay on that route.
        for kind in (
            ("carl", "facebook", "network_activity", "image_session_open"),
            ("carl", "facebook", "network_activity", "search_session_open"),
            ("carl", "ebay", "network_activity", "gallery_image"),
        ):
            identifier = kind[-1]
            activity = network_activity_definition(
                identifier=identifier,
                kind=kind,
                network_path=routing,
                **common,
            )
            await database.create_network_activity(
                activity,
                created_at_utc_ns=10,
                event_identifier=identifier + "-created",
                sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
            )
            result = await database.try_admit_network_activity(
                network_activity_identifier=identifier,
                admission_token=identifier + "-token",
                permit_duration_ns=100,
                now_utc_ns=10,
                event_identifier=identifier + "-admitted",
            )
            assert result.admission is not None

        fourth = image_network_activity(
            identifier="fourth",
            routing=routing,
            url="https://scontent.example.fbcdn.net/fourth.jpg",
            **common,
        )
        await database.create_network_activity(
            fourth,
            created_at_utc_ns=10,
            event_identifier="fourth-created",
            sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
        )
        waiting = await database.try_admit_network_activity(
            network_activity_identifier="fourth",
            admission_token="fourth-token",
            permit_duration_ns=100,
            now_utc_ns=10,
            event_identifier="fourth-waiting",
        )
        assert waiting.admission is None
        assert waiting.next_eligible_at_utc_ns == 10 + budget.period_ns
        admitted = await database.try_admit_network_activity(
            network_activity_identifier="fourth",
            admission_token="fourth-token",
            permit_duration_ns=100,
            now_utc_ns=10 + budget.period_ns,
            event_identifier="fourth-admitted",
        )
        assert admitted.admission is not None

        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT subject_identifier FROM rate_starts ORDER BY subject_identifier"
            )
            assert [row[0] for row in await cursor.fetchall()] == [
                "fourth",
                "image",
                "item",
                "search",
            ]
