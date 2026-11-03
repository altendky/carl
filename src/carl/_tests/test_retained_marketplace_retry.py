"""Connectivity review cannot replay an obsolete acquisition delay."""

# The private retained-review schema is the compatibility boundary under test.
# pyright: reportPrivateUsage=false

from datetime import UTC, datetime
from pathlib import Path

import pytest

from carl._tests.test_worker import _provenance
from carl.core.components import Component, ComponentId
from carl.core.facebook_work import COLLECT_SEARCH_WORK_KIND
from carl.core.models import RecordDraft
from carl.core.stages import StageAttemptContext, StageDefinition
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkDefinition, WorkRequester
from carl.core.worker import RetryWork
from carl.io.connectivity import PROBE_INTERVAL_NS, ConnectivityMonitor
from carl.io.sqlite import Database
from carl.io.stage_policy import StagePolicy, _Review


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("kind", "reason", "available", "expected"),
    (
        (COLLECT_SEARCH_WORK_KIND, "search_acquisition_failure", True, 0),
        (COLLECT_SEARCH_WORK_KIND, "search_acquisition_failure", False, PROBE_INTERVAL_NS),
        (
            ("carl", "facebook", "work", "analyze_item"),
            "search_acquisition_failure",
            True,
            600_000_000_000,
        ),
        (COLLECT_SEARCH_WORK_KIND, "user_wait", True, 600_000_000_000),
    ),
)
async def test_retained_retry_review_preserves_outage_and_custom_recovery(
    tmp_path: Path, kind: tuple[str, ...], reason: str, available: bool, expected: int
) -> None:
    value = {
        "kind": "retry",
        "delay_ns": 600_000_000_000,
        "reason": {"kind": reason},
        "result": {"retained_evidence": "same"},
    }
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.begin_operation(
            operation_id="old-record-operation",
            component=Component(ComponentId(("test", "retained_retry")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.complete_operation(
            operation_id="old-record-operation",
            records=(
                RecordDraft(
                    identifier="retained",
                    kind=("carl", "runtime", "connectivity_failure"),
                    schema_version=1,
                    value=value,
                ),
            ),
            artifacts=(),
            outputs=(),
            result={},
            ended_at_utc=datetime.now(UTC).isoformat(),
            duration_ns=0,
        )
        await database.enqueue_work(
            WorkDefinition(
                identifier="work",
                kind=kind,
                payload_schema_version=1,
                payload={},
                deduplication_identity=("retained",),
                not_before_utc_ns=0,
                scopes=(
                    SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                    SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
                ),
            ),
            WorkRequester(
                request_identifier="request", kind=("test",), identifier="retained", context={}
            ),
            event_identifier="enqueued",
            enqueued_at_utc_ns=1,
        )
        payload = _Review(record_identifier="retained", epoch="epoch", generation=0)
        await database.register_stage(
            StageDefinition(
                identifier="review",
                work_item_identifier="work",
                operation_identifier="review-operation",
                kind=("carl", "runtime", "connectivity_review"),
                payload=payload.model_dump(mode="json"),
            ),
            created_at_utc_ns=1,
        )
        policy = StagePolicy(
            monitor=ConnectivityMonitor(database, lambda: "probe", lambda: 1),
            epoch="epoch",
            available=available,
        )
        outcome = await policy.review(
            payload,
            StageAttemptContext(
                work_item_identifier="work",
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="review-operation",
                stage_identifier="review",
                execution=1,
            ),
        )
        assert isinstance(outcome, RetryWork) and outcome.delay_ns == expected
        _, _, retained = await database.get_record("retained")
        assert retained == value
