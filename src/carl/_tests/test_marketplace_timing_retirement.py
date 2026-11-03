"""Policy upgrades remove only proven waits and retain physical/budget fences."""

# Private access seeds retained policy and checks exact CAS/rollback boundaries.
# pyright: reportPrivateUsage=false

from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from itertools import count
from pathlib import Path

import pytest

import carl.io.marketplace_timing_retirement as retirement
from carl._tests.test_stage_storage import (
    COMPONENT,
    DURATION,
    NOW,
    _claim,
    _definition,
    _enqueue,
)
from carl._tests.test_worker import _provenance
from carl.core.configuration import EbayItemConfiguration, EbaySearchConfiguration
from carl.core.ebay_items import (
    COLLECT_EBAY_ITEM_WORK_KIND,
    CollectEbayItemPayload,
    EbayItemRequest,
)
from carl.core.facebook_work import facebook_item_network_activity
from carl.core.json import decode_json, encode_json
from carl.core.marketplace_timing import legacy_marketplace_timing_constraints
from carl.core.models import CodeProvenance, JsonValue, NamedInput, RecordDraft
from carl.core.resources import ResourceRequest
from carl.core.stages import StageWaitKind, WaitWork
from carl.core.work import (
    Constraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    UniformHoldoffConstraint,
    WorkCapability,
    WorkDefinition,
    WorkEventKind,
    WorkRequester,
)
from carl.ebay_session_stages import SessionProgress
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
def identifiers() -> Iterator[str]:
    return (f"retirement-{number}" for number in count())


async def _upgrade(database: Database, identifiers: Iterator[str], now: int = NOW + 100) -> None:
    await retirement.retire_marketplace_timing(
        database,
        provenance=_provenance(),
        invocation={},
        new_identifier=lambda: next(identifiers),
        now_utc_ns=now,
    )


async def _item(
    database: Database,
    identifier: str,
    deadline: int = 0,
    *,
    kind: tuple[str, ...] = COLLECT_EBAY_ITEM_WORK_KIND,
) -> None:
    await database.enqueue_work(
        WorkDefinition(
            identifier=identifier,
            kind=kind,
            payload_schema_version=1,
            payload={},
            deduplication_identity=(identifier,),
            not_before_utc_ns=deadline,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
            ),
        ),
        WorkRequester(
            request_identifier=f"request-{identifier}",
            kind=("test",),
            identifier=identifier,
            context={},
        ),
        event_identifier=f"enqueue-{identifier}",
        enqueued_at_utc_ns=NOW,
    )


async def _retry(
    database: Database, identifier: str, reason: str = "ebay_transport_failure"
) -> None:
    claim = await database.claim_work(
        supported_capabilities=(
            WorkCapability(kind=COLLECT_EBAY_ITEM_WORK_KIND, payload_schema_version=1),
        ),
        worker_identifier=identifier,
        lease_token=f"token-{identifier}",
        lease_duration_ns=DURATION,
        utc_now_ns=lambda: NOW,
        event_identifier=f"claim-{identifier}",
        eligible_identifiers=(identifier,),
    )
    assert claim.lease is not None
    await database.begin_leased_operation(
        work_item_identifier=identifier,
        lease_token=claim.lease.token,
        worker_identifier=identifier,
        lease_duration_ns=DURATION,
        utc_now_ns=lambda: NOW,
        event_identifier=f"begin-{identifier}",
        operation_id=f"operation-{identifier}",
        component=COMPONENT,
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc=datetime.now(UTC).isoformat(),
    )
    await database.retry_leased_operation(
        work_item_identifier=identifier,
        lease_token=claim.lease.token,
        worker_identifier=identifier,
        utc_now_ns=lambda: NOW + 1,
        event_identifier=f"retry-{identifier}",
        operation_id=f"operation-{identifier}",
        delay_ns=1000,
        reason={"kind": reason},
        result={"evidence": "retained"},
        ended_at_utc=datetime.now(UTC).isoformat(),
        duration_ns=1,
    )


async def _deadline(database: Database, identifier: str) -> int:
    return int((await database.work(identifier))["eligible_at_utc_ns"])


async def test_retry_release_preserves_history_and_excludes_outages_and_explicit_waits(
    tmp_path: Path, identifiers: Iterator[str]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier in ("retry", "outage", "explicit"):
            await _item(database, identifier)
            await _retry(
                database,
                identifier,
                "connectivity_outage" if identifier == "outage" else "ebay_transport_failure",
            )
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET eligible_at_utc_ns=? WHERE id='explicit'", (NOW + 2000,)
            )
            await database._append_work_event(
                connection,
                event_identifier="explicit-delay",
                work_item_identifier="explicit",
                event_kind=WorkEventKind.ENQUEUED,
                recorded_at_utc_ns=NOW + 2,
                data={"eligible_at_utc_ns": NOW + 2000, "reason": {"kind": "explicit_not_before"}},
            )
        before = await database.work("retry")
        await _upgrade(database, identifiers)
        after = await database.work("retry")
        assert await _deadline(database, "retry") == NOW + 100
        assert await _deadline(database, "outage") == NOW + 1001
        assert await _deadline(database, "explicit") == NOW + 2000
        assert after["attempt"] == before["attempt"] == 1
        assert after["result"] == before["result"] == {"evidence": "retained"}
        assert after["operations"] == before["operations"]
        await _upgrade(database, identifiers)
        assert await _deadline(database, "retry") == NOW + 100


async def test_shared_backoff_restores_independent_future_not_before(
    tmp_path: Path, identifiers: Iterator[str]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _item(database, "sibling", NOW + 500, kind=retirement._EBAY_SEARCH)
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET eligible_at_utc_ns=? WHERE id='sibling'", (NOW + 2000,)
            )
            await database._append_work_event(
                connection,
                event_identifier="shared",
                work_item_identifier="sibling",
                event_kind=WorkEventKind.ENQUEUED,
                recorded_at_utc_ns=NOW + 1,
                data={
                    "eligible_at_utc_ns": NOW + 2000,
                    "reason": {"kind": "ebay_search_shared_cooldown"},
                },
            )
        await _upgrade(database, identifiers)
        assert await _deadline(database, "sibling") == NOW + 500
        assert (await database.work("sibling"))["attempt"] == 0


def _holdoff() -> UniformHoldoffConstraint:
    return next(
        item
        for item in legacy_marketplace_timing_constraints()
        if isinstance(item, UniformHoldoffConstraint)
        and item.identifier == ("carl", "facebook", "item", "network_activity_holdoff", "v2")
    )


async def test_pending_network_recomputes_only_removed_samples_and_preserves_admitted_streams(
    tmp_path: Path, identifiers: Iterator[str]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        known = _holdoff()
        custom = UniformHoldoffConstraint(
            identifier=("custom", "request_holdoff"),
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=known.scope,
            minimum_ns=40_000_000,
            maximum_ns=40_000_000,
        )
        for constraint in (known, custom):
            await database.register_constraint(constraint, registered_at_utc_ns=NOW)
        await database.begin_operation(
            operation_id="capture",
            component=COMPONENT,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        for identifier in ("pending", "admitted"):
            definition = facebook_item_network_activity(
                identifier=identifier,
                operation_identifier="capture",
                network_session_identifier="session",
                attempt=1,
                routing=("proton", "personal", "route"),
            ).model_copy(update={"not_before_utc_ns": NOW + 1000})
            await database.create_network_activity(
                definition,
                created_at_utc_ns=NOW,
                event_identifier=f"create-{identifier}",
                sample_uniform_holdoff_ns=lambda _low, high: high,
            )
        admitted = await database.try_admit_network_activity(
            network_activity_identifier="admitted",
            admission_token="stream-token",
            permit_duration_ns=DURATION,
            now_utc_ns=NOW + 200_000_000,
            event_identifier="admitted",
        )
        assert admitted.admission is not None
        stream_before = await database.network_activity("admitted")
        await _upgrade(database, identifiers)
        assert (await database.network_activity("pending"))[
            "eligible_at_utc_ns"
        ] == NOW + 40_000_000
        assert await database.network_activity("admitted") == stream_before
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT identifier_parts_json FROM scheduling_constraint_retirements"
                )
            ).fetchall()
        assert rows == [(encode_json(list(known.identifier)),)]


async def test_private_ebay_stage_is_replaced_without_rewriting_progress_or_old_definition(
    tmp_path: Path, identifiers: Iterator[str]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "work", kind=COLLECT_EBAY_ITEM_WORK_KIND)
        initial = _definition(resources=()).model_copy(update={"kind": retirement._PREPARE})
        await database.register_stage(initial, created_at_utc_ns=NOW)
        claim = await _claim(database)
        assert claim.lease is not None
        progress = SessionProgress(
            item=CollectEbayItemPayload(request=EbayItemRequest(item_identifier="123456789012")),
            network_path=("decodo", "personal", "route"),
            search_policy=EbaySearchConfiguration(),
            item_policy=EbayItemConfiguration(minimum_holdoff_seconds=2, maximum_holdoff_seconds=2),
            retry_scope="stack",
            run_identifier="run",
            phase="item",
        )
        continuation = initial.model_copy(
            update={
                "identifier": "capture-stage",
                "kind": retirement._CAPTURE,
                "payload": {"progress_identifier": "progress"},
                "resources": (ResourceRequest(name="http"),),
                "not_before_utc_ns": NOW + 2_000_000_000,
                "checkpoint_inputs": (
                    NamedInput(name=("progress",), object_identifier="progress"),
                ),
            }
        )
        await database.suspend_stage(
            claim.lease,
            WaitWork(continuation=continuation, wait_kind=StageWaitKind.TIME, result={}),
            now_utc_ns=NOW + 1,
            records=(
                RecordDraft(
                    identifier="progress",
                    kind=retirement._PROGRESS,
                    schema_version=1,
                    value=progress.model_dump(mode="json"),
                ),
            ),
        )
        record_before = await database.get_record("progress")
        await _upgrade(database, identifiers)
        assert await database.stage("capture-stage") == continuation
        assert await database.get_record("progress") == record_before
        async with database._connections.reader() as connection:
            active = await (
                await connection.execute(
                    "SELECT id,definition_json FROM work_stages WHERE work_item_id='work' AND state='pending'"
                )
            ).fetchone()
        assert active is not None and active[0] != "capture-stage"
        replacement = decode_json(str(active[1]))
        assert isinstance(replacement, dict)
        assert replacement["not_before_utc_ns"] == NOW + 100
        assert replacement["operation_identifier"] == continuation.operation_identifier
        assert replacement["payload"] == continuation.payload
        assert (await database.work("work"))["attempt"] == 1


class _Interrupted(Exception):
    pass


async def test_compare_and_swap_keeps_a_newer_explicit_deadline(
    tmp_path: Path, identifiers: Iterator[str]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _item(database, "retry")
        await _retry(database, "retry")
        async with database._connections.reader() as connection:
            row = await (
                await connection.execute(
                    "SELECT rowid,id,kind_parts_json,state,attempt,eligible_at_utc_ns,created_at_utc_ns FROM work_items WHERE id='retry'"
                )
            ).fetchone()
        assert row is not None
        plan = await retirement._work_plan(database, row, NOW + 100)
        assert plan is not None
        async with database._connections.writer() as connection:
            # Even an attachment after planning invalidates the exact event fence.
            await database._append_work_event(
                connection,
                event_identifier="later",
                work_item_identifier="retry",
                event_kind=WorkEventKind.REQUESTER_ATTACHED,
                recorded_at_utc_ns=NOW + 101,
                data={"request_identifier": "later-consumer"},
            )
        await retirement._audit(
            database,
            category="work",
            provenance=_provenance(),
            invocation={},
            new_identifier=lambda: next(identifiers),
            now=NOW + 102,
            changes=(plan,),
        )
        assert await _deadline(database, "retry") == NOW + 1001


async def test_retirement_and_eligibility_roll_back_if_audit_publication_fails(
    tmp_path: Path, identifiers: Iterator[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _item(database, "retry")
        await _retry(database, "retry")
        original = database.complete_operation

        async def fail(*args: object, **kwargs: object) -> None:
            del args, kwargs
            raise _Interrupted("audit publication")

        monkeypatch.setattr(database, "complete_operation", fail)
        with pytest.raises(_Interrupted):
            await _upgrade(database, identifiers)
        assert await _deadline(database, "retry") == NOW + 1001
        monkeypatch.setattr(database, "complete_operation", original)
        await _upgrade(database, identifiers)
        assert await _deadline(database, "retry") == NOW + 100


async def test_constraint_page_restart_keeps_committed_audit_and_custom_policy(
    tmp_path: Path, identifiers: Iterator[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from carl.core.facebook_work import facebook_network_path_constraint

    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        for index in range(140):
            definition = facebook_network_path_constraint(
                ("proton", "personal", f"route-{index:04d}")
            )
            if index == 139:
                definition = definition.model_copy(update={"maximum_starts": 17})
            await database.register_constraint(definition, registered_at_utc_ns=NOW)
        original = retirement._audit
        calls = 0

        async def stop_after_page(
            db: Database,
            *,
            category: str,
            provenance: CodeProvenance,
            invocation: JsonValue,
            new_identifier: Callable[[], str],
            now: int,
            changes: tuple[retirement._Plan, ...] = (),
            constraints: tuple[Constraint, ...] = (),
        ) -> None:
            nonlocal calls
            await original(
                db,
                category=category,
                provenance=provenance,
                invocation=invocation,
                new_identifier=new_identifier,
                now=now,
                changes=changes,
                constraints=constraints,
            )
            calls += 1
            if calls == 1:
                raise _Interrupted("committed first page")

        with monkeypatch.context() as patch:
            patch.setattr(retirement, "_audit", stop_after_page)
            with pytest.raises(_Interrupted):
                await _upgrade(database, identifiers)
        async with database._connections.reader() as connection:
            before = await (
                await connection.execute(
                    "SELECT identifier_parts_json,operation_id FROM scheduling_constraint_retirements ORDER BY identifier_parts_json"
                )
            ).fetchall()
        assert len(before) == 128
    async with Database.managed(path) as database:
        await _upgrade(database, identifiers)
        async with database._connections.reader() as connection:
            after = await (
                await connection.execute(
                    "SELECT identifier_parts_json,operation_id FROM scheduling_constraint_retirements ORDER BY identifier_parts_json"
                )
            ).fetchall()
        assert len(after) == 139
        assert after[:128] == before


async def test_pending_work_cursor_crosses_equal_deadline_priority_and_creation_ties(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _item(database, "template", NOW + 1000)
        async with database._connections.writer() as connection:
            await connection.executemany(
                """INSERT INTO work_items(id,kind_parts_json,payload_schema_version,payload_json,
                deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns)
                SELECT ?,kind_parts_json,payload_schema_version,payload_json,?,'pending',?,?,?
                FROM work_items WHERE id='template'""",
                [
                    (
                        f"tie-{i:03d}",
                        encode_json([f"tie-{i:03d}"]),
                        i % 7 - 3,
                        NOW + 1000 + i // 37,
                        NOW + i % 3,
                    )
                    for i in range(300)
                ],
            )
            expected = await (
                await connection.execute(
                    "SELECT id FROM work_items WHERE state='pending' AND eligible_at_utc_ns>? ORDER BY eligible_at_utc_ns,priority DESC,created_at_utc_ns,rowid",
                    (NOW + 100,),
                )
            ).fetchall()
        visited = []
        async for rows in retirement._pending_pages(
            database,
            "work_items",
            "id,kind_parts_json,state,attempt,eligible_at_utc_ns,created_at_utc_ns",
            NOW + 100,
        ):
            assert len(rows) <= 128
            visited.extend(row[1] for row in rows)
        assert visited == [row[0] for row in expected]


async def test_future_candidate_scan_does_not_grow_with_ten_thousand_archived_rows(
    tmp_path: Path, identifiers: Iterator[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _item(database, "future", NOW + 1000)

        async def measure() -> int:
            steps = 0

            def progress() -> bool:
                nonlocal steps
                steps += 1
                return False

            async with database._connections.reader() as connection:
                await connection.set_progress_handler(progress, nsteps=1, id="timing-upgrade-cost")
                try:
                    await _upgrade(database, identifiers)
                finally:
                    await connection.set_progress_handler(None, id="timing-upgrade-cost")
            return steps

        before = await measure()
        await database.begin_operation(
            operation_id="history-op",
            component=COMPONENT,
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        async with database._connections.writer() as connection:
            await connection.execute(
                """WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000)
                INSERT INTO work_items(id,kind_parts_json,payload_schema_version,payload_json,deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns)
                SELECT 'history-'||i,kind_parts_json,payload_schema_version,'{"DO_NOT_DECODE_ARCHIVE":true}',json_array('history-'||i),'completed',0,?,? FROM n CROSS JOIN work_items WHERE id='future'""",
                (NOW, NOW),
            )
            await connection.execute(
                """INSERT INTO stage_attempts(operation_id,work_item_id,state,created_at_utc_ns)
                SELECT 'stage-op-'||id,id,'completed',? FROM work_items WHERE state='completed'""",
                (NOW,),
            )
            definition = _definition(resources=()).model_dump_json()
            await connection.execute(
                """INSERT INTO work_stages(id,work_item_id,operation_id,definition_json,state,eligible_at_utc_ns,created_at_utc_ns)
                SELECT 'stage-'||id,id,'stage-op-'||id,json_set(?,'$.identifier','stage-'||id,'$.work_item_identifier',id,'$.operation_identifier','stage-op-'||id),'completed',?,? FROM work_items WHERE state='completed'""",
                (definition, NOW, NOW),
            )
            await connection.execute(
                """INSERT INTO network_activities(id,kind_parts_json,operation_id,network_session_identifier,ordinal,attempt,state,eligible_at_utc_ns,created_at_utc_ns)
                SELECT 'network-'||id,'["carl","ebay","network_activity","item_page"]','history-op','session',1,1,'completed',?,? FROM work_items WHERE state='completed'""",
                (NOW, NOW),
            )
        original = retirement.decode_json

        def bounded(value: str | bytes):
            assert "DO_NOT_DECODE_ARCHIVE" not in str(value)
            return original(value)

        monkeypatch.setattr(retirement, "decode_json", bounded)
        after = await measure()
        assert before > 0 and after <= before + 1000, (before, after)
        assert await _deadline(database, "future") == NOW + 1000
