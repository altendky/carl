"""Retained retry budgets and audited pending-item policy changes, without HTTP."""

# pyright: reportPrivateUsage=false

from pathlib import Path
from typing import cast
from uuid import uuid4

import apsw
import pytest

from carl._tests.test_ebay_item_workers import _provenance
from carl.core.components import Component, ComponentId
from carl.core.ebay_items import COLLECT_EBAY_ITEM_WORK_KIND
from carl.core.ebay_refresh import REFRESH_EBAY_SEARCH_WORK_KIND
from carl.core.models import JsonValue, NamedOutput, RecordDraft
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.io.sqlite import Database

NOW = 1_000_000_000_000
BLOCKED: dict[str, JsonValue] = {
    "kind": "ebay_item_response_failure",
    "classification": "challenge",
}


async def _enqueue(
    database: Database,
    identifier: str,
    *,
    kind: tuple[str, ...] = COLLECT_EBAY_ITEM_WORK_KIND,
    stack: str = "ebay_anonymous",
    not_before: int = NOW,
) -> None:
    await database.enqueue_work(
        WorkDefinition(
            identifier=identifier,
            kind=kind,
            payload_schema_version=1,
            payload={"request": {"stack_identifier": stack}},
            deduplication_identity=(identifier,),
            not_before_utc_ns=not_before,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
            ),
        ),
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier=identifier, context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=NOW,
    )


async def _start(database: Database, identifier: str) -> tuple[str, str]:
    work = await database.work(identifier)
    token, operation = str(uuid4()), str(uuid4())
    claim = await database.claim_work(
        supported_capabilities=(
            WorkCapability(kind=tuple(cast(list[str], work["kind"])), payload_schema_version=1),
        ),
        eligible_identifiers=(identifier,),
        worker_identifier="test",
        lease_token=token,
        lease_duration_ns=60_000_000_000,
        utc_now_ns=lambda: NOW,
        event_identifier=str(uuid4()),
    )
    assert claim.lease is not None
    await database.begin_leased_operation(
        work_item_identifier=identifier,
        lease_token=token,
        worker_identifier="test",
        lease_duration_ns=60_000_000_000,
        utc_now_ns=lambda: NOW,
        event_identifier=str(uuid4()),
        operation_id=operation,
        component=Component(ComponentId(("test", "item_retry_storage")), 1, lambda: None),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-10-17T00:00:00+00:00",
    )
    return token, operation


async def _retry(
    database: Database,
    identifier: str,
    token: str,
    operation: str,
    *,
    reason: JsonValue = BLOCKED,
    result: JsonValue = None,
    records: tuple[RecordDraft, ...] = (),
    delay: int = 1_000_000_000,
) -> None:
    await database.retry_leased_operation(
        work_item_identifier=identifier,
        lease_token=token,
        worker_identifier="test",
        utc_now_ns=lambda: NOW,
        delay_ns=delay,
        event_identifier=str(uuid4()),
        operation_id=operation,
        reason=reason,
        result=result,
        ended_at_utc="2026-10-17T00:00:01+00:00",
        duration_ns=1,
        records=records,
        outputs=tuple(
            NamedOutput(
                name=("acquisition", record.identifier), object_identifier=record.identifier
            )
            for record in records
        ),
    )


def _record(identifier: str, *, purpose: str = "ebay_item") -> RecordDraft:
    return RecordDraft(
        identifier=identifier,
        kind=("carl", "http", "acquisition"),
        schema_version=1,
        value={"purpose": purpose},
    )


@pytest.mark.anyio
async def test_retained_item_acquisitions_include_failed_attempts_and_checkpoints(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _enqueue(database, "item")
        token, operation = await _start(database, "item")
        await _retry(
            database,
            "item",
            token,
            operation,
            records=(
                _record("first"),
                _record("description", purpose="ebay_description"),
                _record("not-acquisition").model_copy(
                    update={"kind": ("carl", "ebay", "listing_observation")}
                ),
            ),
            delay=0,
        )
        token, operation = await _start(database, "item")
        await database.publish_leased_operation_checkpoint(
            work_item_identifier="item",
            lease_token=token,
            worker_identifier="test",
            utc_now_ns=lambda: NOW,
            operation_id=operation,
            records=(_record("checkpoint"),),
            artifacts=(),
            outputs=(NamedOutput(name=("checkpoint",), object_identifier="checkpoint"),),
            checkpoint_result={},
        )
        await _retry(database, "item", token, operation, records=(_record("second"),), delay=0)
        token, operation = await _start(database, "item")
        await database.publish_leased_operation_checkpoint(
            work_item_identifier="item",
            lease_token=token,
            worker_identifier="test",
            utc_now_ns=lambda: NOW,
            operation_id=operation,
            records=(_record("active"),),
            artifacts=(),
            outputs=(NamedOutput(name=("active",), object_identifier="active"),),
            checkpoint_result={},
        )
        await _enqueue(database, "other")
        other_token, other_operation = await _start(database, "other")
        await _retry(database, "other", other_token, other_operation, records=(_record("other"),))
        assert await database.retained_ebay_item_acquisitions("item", before_attempt=3) == (
            "first",
            "checkpoint",
            "second",
        )
        assert await database.retained_ebay_item_acquisitions(
            "item", before_attempt=3, retry_budget_start_attempt=1
        ) == ("checkpoint", "second")
        assert await database.retained_ebay_item_acquisitions("item", before_attempt=4) == (
            "first",
            "checkpoint",
            "second",
            "active",
        )


@pytest.mark.anyio
async def test_policy_cutover_wakes_only_pending_blocked_items_and_their_waiting_parents(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        for identifier, stack, reason in (
            ("blocked", "ebay_anonymous", BLOCKED),
            ("other-stack", "other", BLOCKED),
            ("transport", "ebay_anonymous", {"kind": "ebay_transport_failure"}),
            ("newer-unrecognized", "ebay_anonymous", BLOCKED),
            ("runnable", "ebay_anonymous", BLOCKED),
        ):
            await _enqueue(database, identifier, stack=stack)
            token, operation = await _start(database, identifier)
            await _retry(
                database,
                identifier,
                token,
                operation,
                reason=reason,
                result={"retained": identifier},
                delay=0 if identifier in {"newer-unrecognized", "runnable"} else 1_000_000_000,
            )
        token, operation = await _start(database, "newer-unrecognized")
        await _retry(
            database,
            "newer-unrecognized",
            token,
            operation,
            reason={"kind": "ebay_item_response_failure", "classification": "unrecognized"},
        )
        await _enqueue(database, "never-attempted", not_before=NOW + 1_000_000_000)
        await _enqueue(database, "leased")
        token, operation = await _start(database, "leased")
        await _retry(database, "leased", token, operation, delay=0)
        await _start(database, "leased")
        await _enqueue(database, "terminal")
        token, operation = await _start(database, "terminal")
        await database.terminally_fail_leased_operation(
            work_item_identifier="terminal",
            lease_token=token,
            worker_identifier="test",
            utc_now_ns=lambda: NOW,
            event_identifier=str(uuid4()),
            operation_id=operation,
            error=BLOCKED,
            result={},
            ended_at_utc="2026-10-17T00:00:01+00:00",
            duration_ns=1,
        )
        for identifier, children, stage in (
            ("parent", ["blocked"], "collecting_items"),
            ("unrelated-parent", ["other-stack"], "collecting_items"),
            ("wrong-stage", ["blocked"], "collecting_images"),
        ):
            await _enqueue(database, identifier, kind=REFRESH_EBAY_SEARCH_WORK_KIND)
            token, operation = await _start(database, identifier)
            await _retry(
                database,
                identifier,
                token,
                operation,
                reason={"kind": "waiting_for_items"},
                result={"stage": stage, "item_collection_work_identifiers": children},
            )
        before = {
            identifier: await database.work(identifier)
            for identifier in (
                "blocked",
                "other-stack",
                "transport",
                "newer-unrecognized",
                "runnable",
                "never-attempted",
                "leased",
                "terminal",
                "parent",
                "unrelated-parent",
                "wrong-stage",
            )
        }
        assert await database.expedite_pending_ebay_item_retries(
            at_utc_ns=NOW + 1, event_batch_identifier="cutover"
        ) == (("blocked",), ("parent",))
        for identifier, previous in before.items():
            current = await database.work(identifier)
            if identifier in {"blocked", "parent"}:
                assert current["eligible_at_utc_ns"] == NOW + 1
                for key in ("attempt", "result", "payload", "operations", "state"):
                    assert current[key] == previous[key]
            else:
                assert current == previous
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT work_item_id, data_json FROM work_events WHERE id LIKE 'cutover-%' ORDER BY id"
            )
            rows = await cursor.fetchall()
        assert tuple(row[0] for row in rows) == ("blocked", "parent")
        for row in rows:
            assert isinstance(row[1], str)
            assert '"previous_eligible_at_utc_ns":1001000000000' in row[1]
        assert await database.expedite_pending_ebay_item_retries(
            at_utc_ns=NOW + 1, event_batch_identifier="repeat"
        ) == ((), ())
        assert await database.expedite_pending_ebay_item_retries(
            at_utc_ns=NOW + 1, event_batch_identifier="other", stack_identifier="other"
        ) == (("other-stack",), ("unrelated-parent",))


@pytest.mark.anyio
async def test_policy_cutover_rolls_back_when_an_audit_event_cannot_be_written(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        for identifier in ("first", "second"):
            await _enqueue(database, identifier)
            token, operation = await _start(database, identifier)
            await _retry(database, identifier, token, operation)
        async with database._connections.writer() as connection:
            await connection.execute(
                "INSERT INTO work_events(id, work_item_id, event_kind, recorded_at_utc_ns, data_json) "
                "VALUES ('collision-1', 'first', 'enqueued', ?, '{}')",
                (NOW,),
            )
        before = {identifier: await database.work(identifier) for identifier in ("first", "second")}
        with pytest.raises(
            apsw.ConstraintError, match=r"UNIQUE constraint failed: work_events\.id"
        ):
            await database.expedite_pending_ebay_item_retries(
                at_utc_ns=NOW + 1, event_batch_identifier="collision"
            )
        assert {identifier: await database.work(identifier) for identifier in before} == before
