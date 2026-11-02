"""Acquisition ownership scales with equal keys, preserving exact dispatch fencing."""

from __future__ import annotations

# Bulk suspended metadata and actual query plans exercise the storage cost boundary.
# pyright: reportPrivateUsage=false
import subprocess
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

import anyio
import apsw
import pytest

import carl.io.sqlite as sqlite_io
from carl._tests.test_acquisition_identity import EBAY
from carl._tests.test_stage_storage import DURATION, NOW, _claim, _scalar
from carl._tests.test_worker import _requester
from carl.core.acquisition_identity import acquisition_resource_identity_json
from carl.core.ebay_items import (
    COLLECT_EBAY_IMAGE_WORK_KIND,
    CollectEbayImagePayload,
    collect_ebay_image_work,
)
from carl.core.json import encode_json
from carl.core.models import JsonValue
from carl.core.resources import ResourceRequest
from carl.core.stages import StageClaimResult, StageDefinition, StageWaitKind, WaitWork
from carl.core.work import ClaimResult, WorkCapability, WorkDefinition
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
CAPABILITY = WorkCapability(kind=COLLECT_EBAY_IMAGE_WORK_KIND, payload_schema_version=1)


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _image(identifier: str, url: str, *, peer: bool = False) -> WorkDefinition:
    return collect_ebay_image_work(
        identifier=identifier,
        payload=CollectEbayImagePayload(
            item_identifier="123456789",
            observation_record_identifier=("PEER_PAYLOAD-" if peer else "SELECTED_PAYLOAD-")
            + identifier,
            reference_record_identifier=f"reference-{identifier}",
            url=url,
        ),
    )


async def _enqueue(database: Database, work: WorkDefinition) -> None:
    await database.enqueue_work(
        work,
        _requester(work.identifier),
        event_identifier=f"enqueued-{work.identifier}",
        enqueued_at_utc_ns=NOW,
    )


def _stage(work: str, identifier: str | None = None) -> StageDefinition:
    return StageDefinition(
        identifier=identifier or f"stage-{work}",
        work_item_identifier=work,
        operation_identifier=f"operation-{work}",
        kind=("test", "sharing"),
        payload={"work_ref": work},
        resources=(),
    )


async def _pause(
    database: Database,
    work: WorkDefinition,
    *,
    continuation_resources: tuple[ResourceRequest, ...] = (),
) -> StageDefinition:
    await _enqueue(database, work)
    initial = _stage(work.identifier)
    await database.register_stage(initial, created_at_utc_ns=NOW)
    claim = await _claim(database, initial.identifier, token=f"initial-{work.identifier}")
    assert claim.lease is not None
    continuation = initial.model_copy(
        update={
            "identifier": f"paused-{work.identifier}",
            "resources": continuation_resources,
        }
    )
    await database.suspend_stage(
        claim.lease,
        WaitWork(
            continuation=continuation, wait_kind=StageWaitKind.RESOURCE, result={"phase": "paused"}
        ),
        now_utc_ns=NOW + 1,
    )
    return continuation


async def _exact(
    database: Database, identifier: str, *, now: int = NOW + 2, token: str = "selected"
) -> ClaimResult:
    return await database.claim_work(
        supported_capabilities=(CAPABILITY,),
        eligible_identifiers=(identifier,),
        worker_identifier=f"owner-{token}",
        lease_token=token,
        lease_duration_ns=DURATION,
        utc_now_ns=lambda: now,
        event_identifier=f"claim-{token}",
    )


async def _bulk_paused(database: Database, start: int, count: int) -> None:
    """Bulk peers retain the same active-attempt/pending-stage shape as _pause."""
    works = [
        _image(f"bulk-{index:05d}", EBAY.replace("photo-id", f"photo-{index}"), peer=True)
        for index in range(start, start + count)
    ]
    async with database._connections.writer() as connection:
        await connection.executemany(
            "INSERT INTO work_items(id,kind_parts_json,payload_schema_version,payload_json,deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns,lease_token,lease_owner,lease_expires_at_utc_ns,attempt,acquisition_resource_identity_json) VALUES (?,?,?,?,?,'leased',0,?,?,?,'bulk-owner',?,1,?)",
            [
                (
                    work.identifier,
                    encode_json(list(work.kind)),
                    work.payload_schema_version,
                    encode_json(work.payload),
                    encode_json(list(work.deduplication_identity)),
                    NOW,
                    NOW,
                    f"token-{work.identifier}",
                    NOW + 1,
                    acquisition_resource_identity_json(work.kind, work.payload),
                )
                for work in works
            ],
        )
        await connection.executemany(
            "INSERT INTO work_scopes SELECT ?,scope_kind,scope_identity_json FROM work_scopes WHERE work_item_id='real-peer'",
            [(work.identifier,) for work in works],
        )
        await connection.executemany(
            "INSERT INTO stage_attempts VALUES (?,?,1,'active',?)",
            [(f"operation-{work.identifier}", work.identifier, NOW) for work in works],
        )
        await connection.executemany(
            "INSERT INTO operations(id,component_parts_json,output_schema_version,started_at_utc,code_provenance_json,invocation_json,configuration_json,state) VALUES (?,'[\"test\",\"sharing\"]',1,'2027-01-15T08:00:00Z','{}','{}','{}','started')",
            [(f"operation-{work.identifier}",) for work in works],
        )
        await connection.executemany(
            "INSERT INTO work_operations VALUES (?,?,1,?,'bulk-owner')",
            [
                (f"operation-{work.identifier}", work.identifier, f"token-{work.identifier}")
                for work in works
            ],
        )
        await connection.executemany(
            "INSERT INTO work_stages(id,work_item_id,operation_id,definition_json,state,execution,eligible_at_utc_ns,wait_kind,created_at_utc_ns) VALUES (?,?,?,?,'pending',1,?,'resource',?)",
            [
                (
                    f"paused-{work.identifier}",
                    work.identifier,
                    f"operation-{work.identifier}",
                    _stage(work.identifier, f"paused-{work.identifier}").model_dump_json(),
                    NOW + 1,
                    NOW,
                )
                for work in works
            ],
        )


class _Rollback(Exception):
    pass


async def _measure[T](
    database: Database, action: Callable[[], Awaitable[T]]
) -> tuple[T, int, tuple[str, ...]]:
    steps = 0
    statements: list[tuple[str, apsw.Bindings]] = []
    checkpoints: list[tuple[int, str]] = []

    def progress() -> bool:
        nonlocal steps
        steps += 100
        return False

    def trace(_cursor: apsw.Cursor, statement: str, bindings: apsw.Bindings | None) -> bool:
        checkpoints.append((steps, statement))
        if (
            statement.lstrip().upper().startswith("SELECT")
            and "acquisition_resource_identity_json = ?" in statement
        ):
            assert isinstance(bindings, (tuple, list))
            statements.append((statement, tuple(bindings)))
        return True

    result: T | None = None
    try:
        async with database._connections.writer() as connection:
            await connection.set_progress_handler(progress, nsteps=100, id="indexed-sharing-cost")
            connection.set_exec_trace(trace)
            try:
                result = await action()
            finally:
                connection.set_exec_trace(None)
                await connection.set_progress_handler(None, id="indexed-sharing-cost")
            raise _Rollback
    except _Rollback:
        pass
    assert result is not None
    plans: list[str] = []
    async with database._connections.reader() as connection:
        for statement, bindings in statements:
            plans.extend(
                [
                    str(row[3])
                    async for row in await connection.execute(
                        "EXPLAIN QUERY PLAN " + statement, bindings
                    )
                ]
            )
    assert statements, "Admission did not execute the exact canonical-key query"
    assert any(
        "work_items_live_acquisition" in detail and "acquisition_resource_identity_json=?" in detail
        for detail in plans
    )
    costs = tuple(
        f"{end - start} steps: {' '.join(statement.split())[:240]}"
        for (start, statement), (end, _) in zip(
            checkpoints, (*checkpoints[1:], (steps, "")), strict=True
        )
        if end - start >= 1000
    )
    return result, steps, (*plans, *costs)


@pytest.mark.parametrize("resume", (False, True))
async def test_exact_admission_does_not_decode_or_scan_ten_thousand_distinct_suspended_peers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, resume: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _ = await _pause(
            database, _image("real-peer", EBAY.replace("photo-id", "real-peer"), peer=True)
        )
        selected = _image("selected", EBAY.replace("photo-id", "selected"))
        if resume:
            continuation = await _pause(database, selected)

            async def action() -> StageClaimResult:
                return await _claim(database, continuation.identifier, token="resume", now=NOW + 2)
        else:
            await _enqueue(database, selected)

            async def action() -> ClaimResult:
                return await _exact(database, "selected")

        original = sqlite_io.decode_json
        selected_decodes = 0

        def decode(value: str | bytes) -> JsonValue:
            nonlocal selected_decodes
            assert "PEER_PAYLOAD-" not in str(value), "Admission decoded an unrelated peer body"
            if "SELECTED_PAYLOAD-" in str(value):
                selected_decodes += 1
            return original(value)

        await _bulk_paused(database, 0, 32)
        monkeypatch.setattr(sqlite_io, "decode_json", decode)
        before, before_steps, _ = await _measure(database, action)
        assert before.lease is not None
        before_decodes = selected_decodes
        monkeypatch.setattr(sqlite_io, "decode_json", original)
        await _bulk_paused(database, 32, 9968)
        monkeypatch.setattr(sqlite_io, "decode_json", decode)
        selected_decodes = 0
        after, after_steps, diagnostics = await _measure(database, action)
        assert after.lease is not None
        assert selected_decodes == before_decodes and selected_decodes > 0
        assert after_steps <= before_steps + 2000, diagnostics
        assert (await database.work("selected"))["attempt"] == (1 if resume else 0)


@pytest.mark.parametrize(
    "peer", ("live", "cancelling", "expired_active", "expired_cancelling_active", "expired_legacy")
)
async def test_equal_key_blockers_preserve_deadline_and_no_partial_admission(
    tmp_path: Path, peer: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        source = _image("source", EBAY)
        other = _image("other-intent", EBAY.replace("i.", "thumbs.", 1))
        if peer == "expired_active":
            _ = await _pause(database, source)
        else:
            await _enqueue(database, source)
            if peer == "expired_cancelling_active":
                stage = _stage("source")
                await database.register_stage(stage, created_at_utc_ns=NOW)
                assert (await _claim(database, stage.identifier)).lease is not None
            else:
                assert (await _exact(database, "source", now=NOW, token="source")).lease is not None
            if peer in ("cancelling", "expired_cancelling_active"):
                await database.cancel_work(
                    "source", reason="test", caller="test", requested_at_utc_ns=NOW + 1
                )
        await _enqueue(database, other)
        before_operations = await _scalar(database, "SELECT count(*) FROM operations")
        now = (
            NOW + DURATION + 1
            if peer in ("expired_active", "expired_cancelling_active", "expired_legacy")
            else NOW + 2
        )
        claim = await _exact(database, other.identifier, now=now)
        if peer == "expired_legacy":
            assert claim.lease is not None
            return
        assert claim.lease is None
        expected = NOW + DURATION if peer in ("live", "cancelling") else now + 100_000_000
        assert claim.next_eligible_at_utc_ns == expected
        assert (await database.work(other.identifier))["attempt"] == 0
        assert await _scalar(database, "SELECT count(*) FROM operations") == before_operations
        assert (
            await _scalar(
                database, "SELECT count(*) FROM work_operations WHERE work_item_id='other-intent'"
            )
            == 0
        )


async def test_resume_excludes_self_but_equal_key_peer_decline_preserves_lease_and_operation(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        stage = await _pause(
            database,
            _image("real-peer", EBAY),
            continuation_resources=(ResourceRequest(name="cpu"),),
        )
        async with database._connections.writer() as connection:
            source = _image("historical-duplicate", EBAY.replace("i.", "thumbs.", 1))
            await connection.execute(
                "INSERT INTO work_items(id,kind_parts_json,payload_schema_version,payload_json,deduplication_identity_json,state,priority,eligible_at_utc_ns,created_at_utc_ns,lease_token,lease_owner,lease_expires_at_utc_ns,attempt,acquisition_resource_identity_json) VALUES (?,?,1,?,?,'leased',0,?,?,'duplicate-token','duplicate-owner',?,1,?)",
                (
                    source.identifier,
                    encode_json(list(source.kind)),
                    encode_json(source.payload),
                    encode_json(list(source.deduplication_identity)),
                    NOW,
                    NOW,
                    NOW + DURATION,
                    acquisition_resource_identity_json(source.kind, source.payload),
                ),
            )
        before = await database.work("real-peer")
        blocked = await _claim(database, stage.identifier, token="blocked-resume", now=NOW + 2)
        assert blocked.lease is None and blocked.wait_kind is StageWaitKind.OWNERSHIP
        assert blocked.next_eligible_at_utc_ns == NOW + 2 + 100_000_000
        assert await database.work("real-peer") == before
        assert before["attempt"] == 1
        assert await _scalar(database, "SELECT count(*) FROM stage_execution_tokens") == 1
        assert await _scalar(database, "SELECT count(*) FROM resource_permits") == 0
        assert (await database.operation(stage.operation_identifier))["state"] == "started"
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET state='completed',lease_token=NULL,lease_owner=NULL,lease_expires_at_utc_ns=NULL WHERE id='historical-duplicate'"
            )
        resumed = await _claim(database, stage.identifier, token="free-resume", now=NOW + 3)
        assert resumed.lease is not None and resumed.lease.work_lease.attempt == 1
        assert resumed.lease.definition.operation_identifier == stage.operation_identifier


async def test_initial_claim_uses_earliest_equal_key_blocker_deadline(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for identifier in ("later", "earlier", "candidate"):
            await _enqueue(database, _image(identifier, EBAY))
        assert (await _exact(database, "later", now=NOW, token="later")).lease is not None
        # Historical duplicate leases can coexist; their insertion order must not
        # choose a later wake-up than the earliest still-live equal resource.
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET state='leased',attempt=1,lease_token='earlier',lease_owner='earlier-owner',lease_expires_at_utc_ns=? WHERE id='earlier'",
                (NOW + DURATION // 2,),
            )
        result = await _exact(database, "candidate")
        assert result.lease is None
        assert result.next_eligible_at_utc_ns == NOW + DURATION // 2
        assert (await database.work("candidate"))["attempt"] == 0


@pytest.mark.parametrize("same_resource", (False, True))
async def test_real_processes_exclude_normalized_aliases_but_different_images_run_parallel(
    tmp_path: Path, same_resource: bool
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _enqueue(database, _image("one", EBAY))
        await _enqueue(
            database,
            _image(
                "two",
                EBAY.replace("i.", "thumbs.", 1)
                if same_resource
                else EBAY.replace("photo-id", "different"),
            ),
        )
    script = """
import sys
from pathlib import Path
import anyio
from carl._tests.test_indexed_acquisition_sharing import _exact
from carl.io.sqlite import Database
async def main():
    async with Database.managed(Path(sys.argv[1])) as database:
        Path(sys.argv[2]).write_text('ready')
        while not Path(sys.argv[3]).exists(): await anyio.sleep(.01)
        result=await _exact(database,sys.argv[4],token=sys.argv[4])
        print('yes' if result.lease is not None else 'no')
anyio.run(main,backend='asyncio')
"""
    children: list[anyio.abc.Process] = []
    try:
        for identifier in ("one", "two"):
            children.append(
                await anyio.open_process(
                    (
                        sys.executable,
                        "-c",
                        script,
                        str(path),
                        str(tmp_path / f"ready-{identifier}"),
                        str(tmp_path / "go"),
                        identifier,
                    ),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            )
        with anyio.fail_after(20):
            while not all(
                (tmp_path / f"ready-{identifier}").exists() for identifier in ("one", "two")
            ):
                await anyio.sleep(0.01)
            (tmp_path / "go").touch()
            results: list[bool] = []
            for child in children:
                assert child.stdout is not None
                results.append((await child.stdout.receive(64)).strip() == b"yes")
                assert await child.wait() == 0
        assert sum(results) == (1 if same_resource else 2)
    finally:
        with anyio.CancelScope(shield=True):
            for child in children:
                if child.returncode is None:
                    child.kill()
                    await child.wait()
                await child.aclose()
