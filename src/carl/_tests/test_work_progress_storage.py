"""Progress subscriptions preserve publication fences and logical attempts."""

# Storage/partial-publication assertions deliberately inspect the durable ledger.
# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl._tests.test_stage_storage import NOW, _claim, _definition, _enqueue
from carl.core.models import NamedOutput, RecordDraft
from carl.core.stages import StageLease, StageWaitKind, WaitWork
from carl.core.work import WorkRequester
from carl.core.work_progress import ProgressWait, WorkProgressWatch
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
GROUP = ("test", "consumer", "images")


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _checkpoint(database: Database, lease: StageLease, result: dict[str, int]) -> None:
    await database.publish_leased_operation_checkpoint(
        work_item_identifier=lease.definition.work_item_identifier,
        lease_token=lease.token,
        worker_identifier=lease.work_lease.worker_identifier,
        utc_now_ns=lambda: NOW + 10,
        operation_id=lease.definition.operation_identifier,
        records=(),
        artifacts=(),
        outputs=(),
        checkpoint_result=result,
    )


@pytest.mark.parametrize("group", (False, True))
async def test_partial_checkpoint_wakes_exact_consumer_without_new_attempt(
    tmp_path: Path, group: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for work in ("parent", "child"):
            await _enqueue(database, work)
            await database.register_stage(
                _definition(work, identifier="stage-" + work, resources=()), created_at_utc_ns=NOW
            )
        await database.attach_work_request(
            "child",
            WorkRequester(
                request_identifier="consumer",
                kind=GROUP,
                identifier="parent",
                parent_work_identifier="parent",
                context={},
            ),
            event_identifier="attach",
            requested_at_utc_ns=NOW,
        )
        baseline = await database.work_progress_watermark()
        parent = await _claim(database, "stage-parent", token="parent-token")
        assert parent.lease is not None
        continuation = _definition("parent", identifier="waiting-parent", resources=())
        await database.suspend_stage(
            parent.lease,
            WaitWork(
                continuation=continuation,
                wait_kind=StageWaitKind.DEPENDENCY,
                result={"waiting": True},
                progress_wait=ProgressWait(
                    observed_through_revision=baseline,
                    watch=WorkProgressWatch(
                        consumer_requester_kinds=(GROUP,) if group else (),
                        work_identifiers=() if group else ("child",),
                    ),
                ),
            ),
            now_utc_ns=NOW + 1,
        )
        assert (
            await _claim(database, continuation.identifier, token="blocked", now=NOW + 2)
        ).wait_kind is StageWaitKind.DEPENDENCY
        child = await _claim(database, "stage-child", token="child-token", now=NOW + 2)
        assert child.lease is not None
        await _checkpoint(database, child.lease, {"pages": 1})
        for _ in range(10):
            if not (await database.drain_progress_notifications(now_utc_ns=NOW + 11)).more:
                break
        resumed = await _claim(database, continuation.identifier, token="resumed", now=NOW + 12)
        assert resumed.lease is not None
        assert (
            resumed.lease.definition.operation_identifier
            == parent.lease.definition.operation_identifier
        )
        assert resumed.lease.work_lease.attempt == 1
        assert (await database.work("child"))["state"] == "leased"


async def test_unchanged_checkpoint_without_evidence_does_not_advance_revision(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _enqueue(database, "child")
        await database.register_stage(_definition("child", resources=()), created_at_utc_ns=NOW)
        child = await _claim(database, token="child-token")
        assert child.lease is not None
        await _checkpoint(database, child.lease, {"pages": 1})
        revision = await database.work_progress_watermark()
        await _checkpoint(database, child.lease, {"pages": 1})
        assert await database.work_progress_watermark() == revision


async def _drain(database: Database) -> None:
    for _ in range(100):
        if not (await database.drain_progress_notifications(now_utc_ns=NOW + 20)).more:
            return
    raise AssertionError("Progress backlog did not drain")


async def _sleep(
    database: Database,
    parent: str,
    baseline: int,
    *,
    group: bool = True,
    children: tuple[str, ...] = ("child",),
) -> str:
    claim = await _claim(database, "stage-" + parent, token="token-" + parent)
    assert claim.lease is not None
    continuation = _definition(parent, identifier="waiting-" + parent, resources=())
    await database.suspend_stage(
        claim.lease,
        WaitWork(
            continuation=continuation,
            wait_kind=StageWaitKind.DEPENDENCY,
            result={"waiting": True},
            progress_wait=ProgressWait(
                observed_through_revision=baseline,
                watch=WorkProgressWatch(
                    consumer_requester_kinds=(GROUP,) if group else (),
                    work_identifiers=() if group else children,
                ),
            ),
        ),
        now_utc_ns=NOW + 1,
    )
    return continuation.identifier


async def _setup_shared(database: Database, count: int) -> tuple[StageLease, int]:
    await _enqueue(database, "child")
    await database.register_stage(
        _definition("child", identifier="stage-child", resources=()), created_at_utc_ns=NOW
    )
    for index in range(count):
        parent = f"parent-{index:02}"
        await _enqueue(database, parent)
        await database.register_stage(
            _definition(parent, identifier="stage-" + parent, resources=()), created_at_utc_ns=NOW
        )
        await database.attach_work_request(
            "child",
            WorkRequester(
                request_identifier="edge-" + parent,
                kind=GROUP,
                identifier=parent,
                parent_work_identifier=parent,
                context={},
            ),
            event_identifier="event-" + parent,
            requested_at_utc_ns=NOW,
        )
    claim = await _claim(database, "stage-child", token="child-token")
    assert claim.lease is not None
    return claim.lease, await database.work_progress_watermark()


async def test_noisy_shared_child_finishes_frozen_pass_and_late_group_registration_sees_revision(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        child, baseline = await _setup_shared(database, 11)
        for index in range(10):
            await _sleep(database, f"parent-{index:02}", baseline)
        await _drain(database)
        await _checkpoint(database, child, {"pages": 1})
        for ordinal in range(6):
            await database.drain_progress_notifications(
                now_utc_ns=NOW + 20, source_limit=1, edge_limit=2, total_limit=2
            )
            await _checkpoint(database, child, {"pages": ordinal + 2})
        async with database._connections.reader() as connection:
            flags = await (
                await connection.execute(
                    "SELECT progress_waiting FROM work_stages WHERE id LIKE 'waiting-parent-%' ORDER BY id"
                )
            ).fetchall()
        assert len(flags) == 10 and all(row[0] == 0 for row in flags)
        await _drain(database)
        late = await _sleep(database, "parent-10", baseline)
        assert (await _claim(database, late, token="late-resume", now=NOW + 21)).lease is not None


class _Abort(Exception):
    pass


async def test_source_publication_rollback_keeps_revision_and_parent_sleep_atomic(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        child, baseline = await _setup_shared(database, 1)
        waiting = await _sleep(database, "parent-00", baseline)
        await _drain(database)
        before = await database.work_progress_watermark()
        with pytest.raises(_Abort):
            async with database.transaction():
                await _checkpoint(database, child, {"pages": 1})
                raise _Abort
        assert await database.work_progress_watermark() == before
        assert not (await database.drain_progress_notifications(now_utc_ns=NOW + 20)).more
        assert (
            await _claim(database, waiting, token="still-asleep", now=NOW + 21)
        ).wait_kind is StageWaitKind.DEPENDENCY
        await _checkpoint(database, child, {"pages": 1})
        await _drain(database)
        assert (await _claim(database, waiting, token="awake", now=NOW + 22)).lease is not None


async def test_large_explicit_watch_builds_resource_free_in_bounded_chunks(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _enqueue(database, "parent")
        await database.register_stage(
            _definition("parent", identifier="stage-parent", resources=()), created_at_utc_ns=NOW
        )
        identifiers = tuple(f"child-{index:04}" for index in range(1025))
        for identifier in identifiers:
            await _enqueue(database, identifier)
        baseline = await database.work_progress_watermark()
        waiting = await _sleep(database, "parent", baseline, group=False, children=identifiers)
        async with database._connections.reader() as connection:
            header = await (
                await connection.execute(
                    "SELECT state,chunk_count,next_chunk FROM progress_wait_builds WHERE stage_id=?",
                    (waiting,),
                )
            ).fetchone()
            sizes = await (
                await connection.execute(
                    "SELECT json_array_length(identifiers_json) FROM progress_wait_source_chunks ORDER BY ordinal"
                )
            ).fetchall()
        assert header is not None and tuple(header) == ("building", 9, 0)
        assert [row[0] for row in sizes] == [128] * 8 + [1]
        assert (
            await _claim(database, waiting, token="building-block", now=NOW + 2)
        ).wait_kind is StageWaitKind.DEPENDENCY
        await database.register_stage(
            _definition(identifiers[-1], identifier="source-stage", resources=()),
            created_at_utc_ns=NOW,
        )
        source = await _claim(database, "source-stage", token="source-token", now=NOW + 2)
        assert source.lease is not None
        await _checkpoint(database, source.lease, {"pages": 1})
    async with Database.managed(path) as database:
        await _drain(database)
        assert (
            await _claim(database, waiting, token="after-restart", now=NOW + 22)
        ).lease is not None
        assert (await database.work("parent"))["attempt"] == 1


async def test_new_evidence_with_unchanged_checkpoint_is_meaningful_progress(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        child, _ = await _setup_shared(database, 1)
        await _checkpoint(database, child, {"pages": 1})
        await _drain(database)
        baseline = await database.work_progress_watermark()
        waiting = await _sleep(database, "parent-00", baseline)
        await _drain(database)
        await database.publish_leased_operation_checkpoint(
            work_item_identifier="child",
            lease_token=child.token,
            worker_identifier=child.work_lease.worker_identifier,
            utc_now_ns=lambda: NOW + 10,
            operation_id=child.definition.operation_identifier,
            records=(
                RecordDraft(
                    identifier="new-page",
                    kind=("test", "page"),
                    schema_version=1,
                    value={"page": 2},
                ),
            ),
            artifacts=(),
            outputs=(NamedOutput(name=("page", "2"), object_identifier="new-page"),),
            checkpoint_result={"pages": 1},
        )
        await _drain(database)
        assert (
            await _claim(database, waiting, token="new-evidence", now=NOW + 22)
        ).lease is not None


async def test_identical_stage_can_sleep_and_wake_twice_with_fresh_watch_fences(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        child, _ = await _setup_shared(database, 1)
        definition = await database.stage("stage-parent-00")
        for ordinal in range(2):
            baseline = await database.work_progress_watermark()
            parent = await _claim(
                database, definition.identifier, token=f"parent-cycle-{ordinal}", now=NOW + ordinal
            )
            assert parent.lease is not None
            await database.suspend_stage(
                parent.lease,
                WaitWork(
                    continuation=definition,
                    wait_kind=StageWaitKind.DEPENDENCY,
                    result={"cycle": ordinal},
                    progress_wait=ProgressWait(
                        observed_through_revision=baseline,
                        watch=WorkProgressWatch(work_identifiers=("child",)),
                    ),
                ),
                now_utc_ns=NOW + ordinal + 1,
            )
            await _checkpoint(database, child, {"pages": ordinal + 1})
            await _drain(database)
        resumed = await _claim(database, definition.identifier, token="final-cycle", now=NOW + 22)
        assert resumed.lease is not None and resumed.lease.work_lease.attempt == 1
        async with database._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT state,stage_id FROM progress_wait_builds ORDER BY id"
                )
            ).fetchall()
        assert len(rows) == 2 and all(tuple(row) == ("inactive", None) for row in rows)


async def test_quiet_source_gets_a_turn_while_noisy_fanout_keeps_advancing(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        noisy, _ = await _setup_shared(database, 10)
        for work in ("quiet-parent", "quiet-child"):
            await _enqueue(database, work)
            await database.register_stage(
                _definition(work, identifier="stage-" + work, resources=()), created_at_utc_ns=NOW
            )
        await database.attach_work_request(
            "quiet-child",
            WorkRequester(
                request_identifier="quiet-edge",
                kind=GROUP,
                identifier="quiet-parent",
                parent_work_identifier="quiet-parent",
                context={},
            ),
            event_identifier="quiet-event",
            requested_at_utc_ns=NOW,
        )
        quiet = await _claim(database, "stage-quiet-child", token="quiet-token")
        assert quiet.lease is not None
        baseline = await database.work_progress_watermark()
        for index in range(10):
            await _sleep(database, f"parent-{index:02}", baseline)
        waiting = await _sleep(database, "quiet-parent", baseline)
        await _drain(database)
        await _checkpoint(database, noisy, {"pages": 1})
        await _checkpoint(database, quiet.lease, {"pages": 1})
        for ordinal in range(2):
            await database.drain_progress_notifications(
                now_utc_ns=NOW + 20, source_limit=1, edge_limit=1, total_limit=1
            )
            await _checkpoint(database, noisy, {"pages": ordinal + 2})
        assert (
            await _claim(database, waiting, token="quiet-resume", now=NOW + 21)
        ).lease is not None


async def test_completed_child_attachment_behind_cursor_wakes_new_group_atomically(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        child, baseline = await _setup_shared(database, 2)
        # This waiter explicitly knows the child before it has a typed edge.
        await _enqueue(database, "a-new-parent")
        await database.register_stage(
            _definition("a-new-parent", identifier="stage-a-new-parent", resources=()),
            created_at_utc_ns=NOW,
        )
        waiting = await _sleep(database, "a-new-parent", baseline, group=False)
        async with database.transaction():
            await database.complete_stage(child, now_utc_ns=NOW + 10)
            await database.complete_leased_operation(
                work_item_identifier="child",
                lease_token=child.token,
                worker_identifier=child.work_lease.worker_identifier,
                utc_now_ns=lambda: NOW + 10,
                event_identifier="completed-child",
                operation_id=child.definition.operation_identifier,
                records=(),
                artifacts=(),
                outputs=(),
                result={"complete": True},
                ended_at_utc="2027-01-01T00:00:00+00:00",
                duration_ns=1,
            )
        # Drain through the first existing parent; new alphabetically-earlier
        # consumer membership is then behind the fixed source cursor.
        for _ in range(4):
            await database.drain_progress_notifications(
                now_utc_ns=NOW + 20, source_limit=1, edge_limit=1, total_limit=1
            )
        await database.attach_work_request(
            "child",
            WorkRequester(
                request_identifier="new-edge",
                kind=GROUP,
                identifier="a-new-parent",
                parent_work_identifier="a-new-parent",
                context={},
            ),
            event_identifier="new-edge-event",
            requested_at_utc_ns=NOW + 21,
        )
        # Register a fresh group-only wait with the original read baseline after
        # removing the old explicit watch through an ordinary wake/reclaim.
        await _drain(database)
        claim = await _claim(database, waiting, token="attached-resume", now=NOW + 22)
        assert claim.lease is not None
        continuation = _definition("a-new-parent", identifier="new-group-wait", resources=())
        await database.suspend_stage(
            claim.lease,
            WaitWork(
                continuation=continuation,
                wait_kind=StageWaitKind.DEPENDENCY,
                result={},
                progress_wait=ProgressWait(
                    observed_through_revision=baseline,
                    watch=WorkProgressWatch(consumer_requester_kinds=(GROUP,)),
                ),
            ),
            now_utc_ns=NOW + 23,
        )
        assert (
            await _claim(database, continuation.identifier, token="group-resume", now=NOW + 24)
        ).lease is not None


def test_progress_copy_invalidates_cached_descriptor_fingerprint() -> None:
    original = ProgressWait(
        observed_through_revision=0, watch=WorkProgressWatch(work_identifiers=("first",))
    )
    fingerprint = original.fingerprint
    changed = original.model_copy(update={"watch": WorkProgressWatch(work_identifiers=("second",))})
    assert "fingerprint" not in changed.__dict__
    assert changed.fingerprint != fingerprint
    assert original.fingerprint == fingerprint
    assert original.model_copy(update={"observed_through_revision": 1}).fingerprint != fingerprint


async def test_changed_descriptor_copy_cannot_reuse_prepared_watch_source(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, baseline = await _setup_shared(database, 1)
        await _enqueue(database, "different-child")
        parent = await _claim(database, "stage-parent-00", token="prepared-token")
        assert parent.lease is not None
        progress = ProgressWait(
            observed_through_revision=baseline, watch=WorkProgressWatch(work_identifiers=("child",))
        )
        wait = WaitWork(
            continuation=_definition("parent-00", identifier="prepared-continuation", resources=()),
            wait_kind=StageWaitKind.DEPENDENCY,
            result={},
            progress_wait=progress,
        )
        await database.prepare_stage_progress_wait(parent.lease, wait, now_utc_ns=NOW + 1)
        changed = wait.model_copy(
            update={
                "progress_wait": progress.model_copy(
                    update={"watch": WorkProgressWatch(work_identifiers=("different-child",))}
                )
            }
        )
        with pytest.raises(ValueError, match="cannot change"):
            await database.suspend_stage(parent.lease, changed, now_utc_ns=NOW + 2)
        assert (await database.work("parent-00"))["result"] is None


async def test_legacy_reference_edges_follow_consumer_context_and_never_old_producer(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _setup_shared(database, 1)
        await _enqueue(database, "old-producer")
        await database.register_stage(
            _definition("old-producer", identifier="old-stage", resources=()), created_at_utc_ns=NOW
        )
        producer = await _claim(database, "old-stage", token="producer-token")
        assert producer.lease is not None
        await database.publish_leased_operation_checkpoint(
            work_item_identifier="old-producer",
            lease_token=producer.lease.token,
            worker_identifier=producer.lease.work_lease.worker_identifier,
            utc_now_ns=lambda: NOW + 10,
            operation_id=producer.lease.definition.operation_identifier,
            records=(
                RecordDraft(
                    identifier="retained-reference",
                    kind=("test", "reference"),
                    schema_version=1,
                    value={},
                ),
            ),
            artifacts=(),
            outputs=(),
            checkpoint_result={},
        )
        kind = ("carl", "facebook", "gallery_image_reference")
        for identifier, context in (
            ("known", {"search_refresh_work_identifier": "parent-00"}),
            ("unknown", {}),
        ):
            await database.attach_work_request(
                "child",
                WorkRequester(
                    request_identifier="legacy-" + identifier,
                    kind=kind,
                    identifier="retained-reference",
                    context=context,
                ),
                event_identifier="legacy-event-" + identifier,
                requested_at_utc_ns=NOW + 11,
            )
        assert await database.missing_consumer_edges("parent-00", kind, ("child",)) == ()
        assert await database.missing_consumer_edges("old-producer", kind, ("child",)) == ("child",)
