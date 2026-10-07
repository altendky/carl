"""Search-spec revisions keep one watch identity and immutable executions."""

from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from time import time_ns
from uuid import uuid4

import anyio
import pytest

from carl._tests.test_marketplace_listing import _publish, _record
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_review_workspace import _provenance, _search_run_value
from carl.core.components import Component, ComponentId
from carl.core.ebay import EbaySearchRequest
from carl.core.facebook_work import CreateSearchRequest
from carl.core.json import encode_json
from carl.core.marketplace_search import (
    MARKETPLACE_SEARCH_EXECUTION_KIND,
    MARKETPLACE_SEARCH_KIND,
    MARKETPLACE_SEARCH_TARGET_KIND,
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    FacebookSearchTargetSpecification,
)
from carl.core.models import CodeProvenance, RecordDraft
from carl.core.review_workspace import (
    CreateReviewWorkspaceRequest,
    CreateWorkspaceSearchRequest,
    RequestWorkspaceRefreshRequest,
    RetryWorkspaceSearchTrackRequest,
    ReviseWorkspaceSearchTrackRequest,
    SetWorkspaceSearchTrackEnabledRequest,
)
from carl.core.work import (
    EnqueueResult,
    HoldoffDecision,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.io.sqlite import Database
from carl.marketplace_projection import expand_search_runs
from carl.review import ReviewApplication, ReviewInputError


def _facebook_search(maximum: str = "3000") -> CreateSearchRequest:
    search = CreateSearchRequest.model_validate_json(
        encode_json(
            {
                "request": _search_run_value("foosball")["request"],
                "traversal": {"maximum_pages": 10},
            }
        )
    )
    assert search.request.price is not None
    return search.model_copy(
        update={
            "request": search.request.model_copy(
                update={
                    "price": search.request.price.model_copy(update={"maximum": Decimal(maximum)})
                }
            )
        }
    )


def _facebook_run(identifier: str, search: CreateSearchRequest) -> RecordDraft:
    return RecordDraft(
        identifier=identifier,
        kind=("carl", "facebook", "search_run"),
        schema_version=1,
        value={
            **_search_run_value(search.request.query),
            "request": search.request.model_dump(mode="json"),
            "routing": list(search.network_path),
            "traversal_strategy": search.traversal_strategy.model_dump(mode="json"),
            "traversal": {
                "policy": search.traversal.model_dump(mode="json"),
                "unique_listing_identifiers": [],
            },
        },
    )


async def _application(database: Database, path: Path) -> ReviewApplication:
    async def provenance() -> CodeProvenance:
        return _provenance()

    return ReviewApplication(database, path, code_provenance=provenance)


async def _fail_search(database: Database, work_identifier: str, *, ebay: bool) -> None:
    work = await database.work(work_identifier)
    token, operation = str(uuid4()), str(uuid4())
    claim = await database.claim_work(
        supported_capabilities=(
            WorkCapability(
                kind=tuple(work["kind"]), payload_schema_version=int(work["payload_schema_version"])
            ),
        ),
        eligible_identifiers=(work_identifier,),
        worker_identifier="test-worker",
        lease_token=token,
        lease_duration_ns=60_000_000_000,
        utc_now_ns=time_ns,
        event_identifier=str(uuid4()),
    )
    assert claim.lease is not None
    await database.begin_leased_operation(
        work_item_identifier=work_identifier,
        lease_token=token,
        worker_identifier="test-worker",
        lease_duration_ns=60_000_000_000,
        utc_now_ns=time_ns,
        event_identifier=str(uuid4()),
        operation_id=operation,
        component=Component(ComponentId(("test", "track_revision_failure")), 1, lambda: None),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-10-06T00:00:00+00:00",
    )
    await database.terminally_fail_leased_operation(
        work_item_identifier=work_identifier,
        lease_token=token,
        worker_identifier="test-worker",
        utc_now_ns=time_ns,
        event_identifier=str(uuid4()),
        operation_id=operation,
        error={
            "kind": "ebay_search_response_failure" if ebay else "search_acquisition_failure",
            "classification": "error_page",
            "http_status": 403,
            "decision": "retry_exhausted",
        },
        result={"state": "failed"},
        ended_at_utc="2026-10-06T00:00:01+00:00",
        duration_ns=1,
    )


@pytest.mark.anyio
async def test_attached_historical_proton_run_refreshes_directly_on_decodo(tmp_path: Path) -> None:
    original = _facebook_search().model_copy(
        update={"network_path": ("proton", "personal", "historical-proton")}
    )
    retained = _facebook_run("historical-run", original)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, (retained,))
        app = await _application(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Old watch", search_run_record_identifier=retained.identifier
            )
        )
        versions = await app.list_workspace_search_track_versions(
            workspace.record_identifier, retained.identifier
        )
        assert versions[0].search == FacebookSearchTargetSpecification(search=original)
        assert versions[0].version == 1
        refresh = await app.request_workspace_refresh(
            RequestWorkspaceRefreshRequest(workspace_record_identifier=workspace.record_identifier)
        )
        work = await database.work(refresh.work_identifier)
        assert work["payload"]["search"]["routing"] == ["decodo", "personal", "datacenter"]
        assert work["payload"]["image_routing"] == ["decodo", "personal", "datacenter"]
        _, _, value = await database.get_record(retained.identifier)
        assert value == retained.value
        unchanged = await app.list_workspace_search_track_versions(
            workspace.record_identifier, retained.identifier
        )
        assert unchanged == versions


@pytest.mark.anyio
@pytest.mark.parametrize("legacy", (False, True))
async def test_group_v1_refresh_inherits_explicit_neutral_route_but_not_legacy_proton_default(
    tmp_path: Path, legacy: bool
) -> None:
    original = _facebook_search().model_copy(
        update={"network_path": ("proton", "personal", "custom")}
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = await _application(database, tmp_path)
        if legacy:
            original = CreateSearchRequest.model_validate(
                {
                    "request": original.request.model_dump(mode="json"),
                    "traversal": original.traversal.model_dump(mode="json"),
                    "proton_route": "carl",
                }
            )
            queued = await app.create_search(original)
            group_identifier, track_identifier = "legacy-group", "legacy-target"
            timestamp = "2026-09-29T00:00:00+00:00"
            await _publish(
                database,
                (
                    RecordDraft(
                        identifier=group_identifier,
                        kind=MARKETPLACE_SEARCH_KIND,
                        schema_version=1,
                        value={"record_identifier": group_identifier, "created_at_utc": timestamp},
                    ),
                    RecordDraft(
                        identifier=track_identifier,
                        kind=MARKETPLACE_SEARCH_TARGET_KIND,
                        schema_version=1,
                        value={
                            "record_identifier": track_identifier,
                            "search_record_identifier": group_identifier,
                            "created_at_utc": timestamp,
                            "specification": {
                                "marketplace": "facebook",
                                "search": {
                                    "request": original.request.model_dump(mode="json"),
                                    "traversal": original.traversal.model_dump(mode="json"),
                                    "proton_route": "carl",
                                },
                            },
                        },
                    ),
                    RecordDraft(
                        identifier="legacy-execution",
                        kind=MARKETPLACE_SEARCH_EXECUTION_KIND,
                        schema_version=1,
                        value={
                            "record_identifier": "legacy-execution",
                            "search_record_identifier": group_identifier,
                            "target_record_identifier": track_identifier,
                            "work_identifier": queued.work_identifier,
                            "requested_at_utc": timestamp,
                        },
                    ),
                ),
            )
            work_identifier = queued.work_identifier
        else:
            group = await app.create_marketplace_search(
                CreateMarketplaceSearchRequest(
                    targets=(FacebookSearchTargetSpecification(search=original),)
                )
            )
            target = (await app.get_marketplace_search(group.record_identifier)).targets[0]
            group_identifier = group.record_identifier
            track_identifier = target.record_identifier
            work_identifier = target.executions[0].work_identifier
        await _complete(
            database,
            work_identifier,
            records=(_facebook_run("group-run", original),),
            result={"search_run_record_identifier": "group-run"},
        )
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Group watch", search_run_record_identifier=group_identifier
            )
        )
        refresh = await app.request_workspace_refresh(
            RequestWorkspaceRefreshRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=track_identifier,
            )
        )
        work = await database.work(refresh.work_identifier)
        expected = ("decodo", "personal", "datacenter") if legacy else original.network_path
        assert work["payload"]["search"]["routing"] == list(expected)
        assert work["payload"]["image_routing"] == ["decodo", "personal", "datacenter"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "original_network_path",
    (
        ("decodo", "personal", "datacenter"),
        ("decodo", "personal", "historical-mobile"),
        ("proton", "personal", "historical-proton"),
    ),
)
async def test_facebook_price_revision_preserves_track_and_refresh_ancestry(
    tmp_path: Path, original_network_path: tuple[str, ...]
) -> None:
    original = _facebook_search().model_copy(update={"network_path": original_network_path})
    narrowed = _facebook_search("300")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, (_facebook_run("original-run", original),))
        app = await _application(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Foosball", search_run_record_identifier="original-run"
            )
        )
        initial_versions = await app.list_workspace_search_track_versions(
            workspace.record_identifier, "original-run"
        )
        assert len(initial_versions) == 1
        assert initial_versions[0].version == 1
        assert initial_versions[0].record_identifier is None
        assert initial_versions[0].search == FacebookSearchTargetSpecification(search=original)
        revised = await app.revise_workspace_search_track(
            ReviseWorkspaceSearchTrackRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier="original-run",
                expected_version=1,
                search=narrowed,
            )
        )
        assert revised.track_identifier == "original-run"
        assert revised.origin_search_run_record_identifier == "original-run"
        assert revised.current_search_run_record_identifier == "original-run"
        assert revised.search_specification_version == 2
        assert revised.search_specification == FacebookSearchTargetSpecification(search=narrowed)
        versions = await app.list_workspace_search_track_versions(
            workspace.record_identifier, "original-run"
        )
        assert [version.version for version in versions] == [1, 2]
        assert versions[0].search == FacebookSearchTargetSpecification(search=original)
        assert versions[1].search == FacebookSearchTargetSpecification(search=narrowed)
        assert versions[0].record_identifier is not None
        assert versions[1].previous_record_identifier == versions[0].record_identifier
        assert versions[1].record_identifier == revised.search_specification_record_identifier
        refresh = await app.request_workspace_refresh(
            RequestWorkspaceRefreshRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=revised.track_identifier,
            )
        )
        work = await database.work(refresh.work_identifier)
        assert work["payload"]["base_search_run_record_identifier"] == "original-run"
        assert work["payload"]["search"]["request"]["price"]["maximum"] == "300"
        assert work["payload"]["search"]["routing"] == ["decodo", "personal", "datacenter"]
        assert work["payload"]["image_routing"] == ["decodo", "personal", "datacenter"]
        await _complete(
            database,
            refresh.work_identifier,
            records=(_facebook_run("narrowed-run", narrowed),),
            result={"refreshed_search_run_record_identifier": "narrowed-run"},
        )
        updated = await app.get_review_workspace(workspace.record_identifier)
        assert len(updated.search_tracks) == 1
        assert updated.search_tracks[0].track_identifier == revised.track_identifier
        assert updated.search_tracks[0].origin_search_run_record_identifier == "original-run"
        assert updated.search_tracks[0].current_search_run_record_identifier == "narrowed-run"
        runs = await expand_search_runs(
            database,
            ("narrowed-run",),
            as_of_completion_sequence=await database.current_completion_boundary(),
        )
        assert runs == ("narrowed-run", "original-run")


@pytest.mark.anyio
async def test_revision_has_optimistic_version_fence_and_unchanged_spec_is_noop(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, (_facebook_run("original-run", _facebook_search()),))
        app = await _application(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Foosball", search_run_record_identifier="original-run"
            )
        )
        request = ReviseWorkspaceSearchTrackRequest(
            workspace_record_identifier=workspace.record_identifier,
            track_identifier="original-run",
            expected_version=1,
            search=_facebook_search("300"),
        )
        revised = await app.revise_workspace_search_track(request)
        with pytest.raises(ReviewInputError):
            await app.revise_workspace_search_track(
                request.model_copy(update={"search": _facebook_search("200")})
            )
        unchanged = await app.revise_workspace_search_track(
            request.model_copy(update={"expected_version": 2})
        )
        assert unchanged.search_specification_version == 2
        assert (
            unchanged.search_specification_record_identifier
            == revised.search_specification_record_identifier
        )
        latest = await app.revise_workspace_search_track(
            request.model_copy(update={"expected_version": 2, "search": _facebook_search("200")})
        )
        assert latest.search_specification_version == 3


@pytest.mark.anyio
async def test_revision_preserves_disabled_state_and_rejects_other_marketplace(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, (_facebook_run("original-run", _facebook_search()),))
        app = await _application(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Foosball", search_run_record_identifier="original-run"
            )
        )
        await app.set_workspace_search_track_enabled(
            SetWorkspaceSearchTrackEnabledRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier="original-run",
                enabled=False,
            )
        )
        request = ReviseWorkspaceSearchTrackRequest(
            workspace_record_identifier=workspace.record_identifier,
            track_identifier="original-run",
            expected_version=1,
            search=_facebook_search("300"),
        )
        with pytest.raises(ReviewInputError):
            await app.revise_workspace_search_track(
                request.model_copy(
                    update={
                        "search": EbaySearchTargetSpecification(
                            search=EbaySearchRequest(query="foosball")
                        )
                    }
                )
            )
        with pytest.raises(ReviewInputError):
            await app.revise_workspace_search_track(
                request.model_copy(update={"track_identifier": "unknown"})
            )
        revised = await app.revise_workspace_search_track(request)
        assert revised.search_specification_version == 2
        assert not revised.enabled


@pytest.mark.anyio
async def test_revision_does_not_rewrite_already_queued_refresh(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, (_facebook_run("original-run", _facebook_search()),))
        app = await _application(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Foosball", search_run_record_identifier="original-run"
            )
        )
        request = RequestWorkspaceRefreshRequest(
            workspace_record_identifier=workspace.record_identifier
        )
        early = await app.request_workspace_refresh(request)
        before = await database.work(early.work_identifier)
        await app.revise_workspace_search_track(
            ReviseWorkspaceSearchTrackRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier="original-run",
                expected_version=1,
                search=_facebook_search("300"),
            )
        )
        after = await database.work(early.work_identifier)
        assert after["payload"] == before["payload"]
        with pytest.raises(ReviewInputError, match=r"(?i)(wait|pending|active|flight|settle)"):
            await app.request_workspace_refresh(request)
        await _complete(
            database,
            early.work_identifier,
            records=(_facebook_run("early-run", _facebook_search()),),
            result={"refreshed_search_run_record_identifier": "early-run"},
        )
        later = await app.request_workspace_refresh(request)
        assert later.work_identifier != early.work_identifier
        new_work = await database.work(later.work_identifier)
        assert new_work["payload"]["search"]["request"]["price"]["maximum"] == "300"
        assert new_work["payload"]["base_search_run_record_identifier"] == "early-run"
        assert after["payload"]["search"]["request"]["price"]["maximum"] == "3000"


@pytest.mark.anyio
async def test_track_revisions_are_workspace_local_even_with_same_origin_run(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        original = _facebook_search()
        await _publish(database, (_facebook_run("original-run", original),))
        app = await _application(database, tmp_path)
        workspaces = tuple(
            [
                await app.create_review_workspace(
                    CreateReviewWorkspaceRequest(
                        name=name, search_run_record_identifier="original-run"
                    )
                )
                for name in ("Foosball cheap", "Foosball all")
            ]
        )
        await app.revise_workspace_search_track(
            ReviseWorkspaceSearchTrackRequest(
                workspace_record_identifier=workspaces[0].record_identifier,
                track_identifier="original-run",
                expected_version=1,
                search=_facebook_search("300"),
            )
        )
        untouched = await app.get_review_workspace(workspaces[1].record_identifier)
        assert untouched.search_tracks[0].search_specification_version == 1
        assert untouched.search_tracks[0].search_specification == FacebookSearchTargetSpecification(
            search=original
        )
        history = await app.list_workspace_search_track_versions(
            workspaces[1].record_identifier, "original-run"
        )
        assert len(history) == 1 and history[0].record_identifier is None


@pytest.mark.anyio
async def test_first_revision_compare_and_swap_is_atomic_under_interleaved_calls(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, (_facebook_run("original-run", _facebook_search()),))
        app = await _application(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Foosball", search_run_record_identifier="original-run"
            )
        )
        arrived = 0
        barrier = anyio.Event()

        async def synchronized_provenance() -> CodeProvenance:
            nonlocal arrived
            arrived += 1
            if arrived == 2:
                barrier.set()
            await barrier.wait()
            return _provenance()

        # Both calls have read version 1 before either enters the publication transaction.
        editors = tuple(
            ReviewApplication(database, tmp_path, code_provenance=synchronized_provenance)
            for _ in range(2)
        )
        versions: list[int] = []
        conflicts: list[str] = []

        async def edit(editor: ReviewApplication, maximum: str) -> None:
            try:
                track = await editor.revise_workspace_search_track(
                    ReviseWorkspaceSearchTrackRequest(
                        workspace_record_identifier=workspace.record_identifier,
                        track_identifier="original-run",
                        expected_version=1,
                        search=_facebook_search(maximum),
                    )
                )
            except ReviewInputError as error:
                conflicts.append(str(error))
            else:
                versions.append(track.search_specification_version)

        with anyio.fail_after(10):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(edit, editors[0], "300")
                tasks.start_soon(edit, editors[1], "200")
        assert arrived == 2
        assert versions == [2]
        assert len(conflicts) == 1 and "conflict" in conflicts[0].lower()
        history = await app.list_workspace_search_track_versions(
            workspace.record_identifier, "original-run"
        )
        assert [record.version for record in history] == [1, 2]
        assert len({record.record_identifier for record in history}) == 2


@pytest.mark.anyio
@pytest.mark.parametrize("ebay", (False, True))
async def test_refresh_enqueue_rejects_revision_changed_after_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ebay: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        original = (
            EbaySearchTargetSpecification(search=EbaySearchRequest(query="foosball"))
            if ebay
            else FacebookSearchTargetSpecification(search=_facebook_search())
        )
        changed = (
            EbaySearchTargetSpecification(search=EbaySearchRequest(query="foosball table"))
            if ebay
            else FacebookSearchTargetSpecification(search=_facebook_search("300"))
        )
        latest = (
            EbaySearchTargetSpecification(search=EbaySearchRequest(query="foosball tables"))
            if ebay
            else FacebookSearchTargetSpecification(search=_facebook_search("200"))
        )
        record = (
            _record("original-run", "search_run", request=original.search.model_dump(mode="json"))
            if isinstance(original, EbaySearchTargetSpecification)
            else _facebook_run("original-run", original.search)
        )
        await _publish(database, (record,))
        app = await _application(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Foosball", search_run_record_identifier="original-run"
            )
        )
        revision = ReviseWorkspaceSearchTrackRequest(
            workspace_record_identifier=workspace.record_identifier,
            track_identifier="original-run",
            expected_version=1,
            search=changed,
        )
        await app.revise_workspace_search_track(revision)
        original_enqueue = Database.enqueue_work
        intercepted: list[str] = []

        async def intercept(
            self: Database,
            definition: WorkDefinition,
            requester: WorkRequester,
            *,
            event_identifier: str,
            enqueued_at_utc_ns: int,
            holdoff_decisions: Sequence[HoldoffDecision] = (),
        ) -> EnqueueResult:
            if requester.kind == ("carl", "mcp", "request_workspace_refresh") and not intercepted:
                intercepted.append(definition.identifier)
                await app.revise_workspace_search_track(
                    revision.model_copy(update={"expected_version": 2, "search": latest})
                )
            return await original_enqueue(
                self,
                definition,
                requester,
                event_identifier=event_identifier,
                enqueued_at_utc_ns=enqueued_at_utc_ns,
                holdoff_decisions=holdoff_decisions,
            )

        monkeypatch.setattr(Database, "enqueue_work", intercept)
        with pytest.raises(ReviewInputError, match="conflict"):
            await app.request_workspace_refresh(
                RequestWorkspaceRefreshRequest(
                    workspace_record_identifier=workspace.record_identifier
                )
            )
        assert len(intercepted) == 1
        with pytest.raises(KeyError):
            await database.work(intercepted[0])
        assert (
            await database.requested_work_identifiers(
                requester_kind=("carl", "mcp", "request_workspace_refresh"),
                requester_identifier=workspace.record_identifier,
            )
            == ()
        )
        updated = await app.get_review_workspace(workspace.record_identifier)
        assert updated.search_tracks[0].search_specification_version == 3
        assert updated.search_tracks[0].search_specification == latest


@pytest.mark.anyio
@pytest.mark.parametrize("ebay", (False, True))
async def test_failed_initial_search_retries_current_revision_under_same_track(
    tmp_path: Path, ebay: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, (_facebook_run("original-run", _facebook_search()),))
        app = await _application(database, tmp_path)
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Foosball", search_run_record_identifier="original-run"
            )
        )
        original = (
            EbaySearchTargetSpecification(search=EbaySearchRequest(query="foosball"))
            if ebay
            else FacebookSearchTargetSpecification(search=_facebook_search())
        )
        changed = (
            EbaySearchTargetSpecification(
                search=EbaySearchRequest(query="foosball table", maximum_pages=2)
            )
            if ebay
            else FacebookSearchTargetSpecification(search=_facebook_search("300"))
        )
        created = await app.create_workspace_search(
            CreateWorkspaceSearchRequest(
                workspace_record_identifier=workspace.record_identifier, search=original
            )
        )
        await _fail_search(database, created.work_identifier, ebay=ebay)
        before = await database.work(created.work_identifier)
        await app.revise_workspace_search_track(
            ReviseWorkspaceSearchTrackRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=created.track_identifier,
                expected_version=1,
                search=changed,
            )
        )
        retried = await app.retry_workspace_search_track(
            RetryWorkspaceSearchTrackRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=created.track_identifier,
            )
        )
        assert retried.track_identifier == created.track_identifier
        assert retried.work_identifier != created.work_identifier
        old_work = await database.work(created.work_identifier)
        assert old_work["payload"] == before["payload"]
        assert old_work["state"] == "terminal_failure"
        work = await database.work(retried.work_identifier)
        expected_payload = (
            changed.search.model_dump(mode="json")
            if isinstance(changed, EbaySearchTargetSpecification)
            else changed.search.request.model_dump(mode="json")
        )
        assert work["payload"]["request"] == expected_payload
        pending = await app.get_review_workspace(workspace.record_identifier)
        pending_track = next(
            track
            for track in pending.search_tracks
            if track.track_identifier == created.track_identifier
        )
        assert len(pending.search_tracks) == 2
        assert pending_track.search_specification_version == 2
        assert pending_track.creation_work_identifier == retried.work_identifier
        assert pending_track.current_search_run_record_identifier is None
        record = (
            _record("retry-run", "search_run", request=changed.search.model_dump(mode="json"))
            if isinstance(changed, EbaySearchTargetSpecification)
            else _facebook_run("retry-run", changed.search)
        )
        await _complete(
            database,
            retried.work_identifier,
            records=(record,),
            result={"search_run_record_identifier": "retry-run"},
        )
        completed = await app.get_review_workspace(workspace.record_identifier)
        completed_track = next(
            track
            for track in completed.search_tracks
            if track.track_identifier == created.track_identifier
        )
        assert len(completed.search_tracks) == 2
        assert completed_track.track_identifier == created.track_identifier
        assert completed_track.origin_search_run_record_identifier == "retry-run"
        assert completed_track.current_search_run_record_identifier == "retry-run"


@pytest.mark.anyio
@pytest.mark.parametrize("track_kind", ("original", "created", "group"))
async def test_ebay_revision_supports_each_existing_track_identity(
    tmp_path: Path, track_kind: str
) -> None:
    original = EbaySearchRequest(query="foosball", maximum_pages=5)
    changed = EbaySearchRequest(query="foosball table", maximum_pages=2)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = await _application(database, tmp_path)
        if track_kind == "group":
            group = await app.create_marketplace_search(
                CreateMarketplaceSearchRequest(
                    targets=(EbaySearchTargetSpecification(search=original),)
                )
            )
            workspace = await app.create_review_workspace(
                CreateReviewWorkspaceRequest(
                    name="Foosball", search_run_record_identifier=group.record_identifier
                )
            )
            target = (await app.get_marketplace_search(group.record_identifier)).targets[0]
            await _complete(
                database,
                target.executions[0].work_identifier,
                records=(
                    _record("ebay-run", "search_run", request=original.model_dump(mode="json")),
                ),
                result={"search_run_record_identifier": "ebay-run"},
            )
            track_identifier = target.record_identifier
        else:
            await _publish(
                database,
                (_record("ebay-run", "search_run", request=original.model_dump(mode="json")),),
            )
            workspace = await app.create_review_workspace(
                CreateReviewWorkspaceRequest(
                    name="Foosball", search_run_record_identifier="ebay-run"
                )
            )
            track_identifier = "ebay-run"
            if track_kind == "created":
                created = await app.create_workspace_search(
                    CreateWorkspaceSearchRequest(
                        workspace_record_identifier=workspace.record_identifier,
                        search=EbaySearchTargetSpecification(search=original),
                    )
                )
                await _complete(
                    database,
                    created.work_identifier,
                    records=(
                        _record(
                            "created-run", "search_run", request=original.model_dump(mode="json")
                        ),
                    ),
                    result={"search_run_record_identifier": "created-run"},
                )
                track_identifier = created.track_identifier
        before = next(
            track
            for track in (await app.get_review_workspace(workspace.record_identifier)).search_tracks
            if track.track_identifier == track_identifier
        )
        revised = await app.revise_workspace_search_track(
            ReviseWorkspaceSearchTrackRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=track_identifier,
                expected_version=1,
                search=EbaySearchTargetSpecification(search=changed),
            )
        )
        assert revised.query == "foosball table"
        assert revised.search_specification_version == 2
        assert revised.track_identifier == before.track_identifier
        assert (
            revised.origin_search_run_record_identifier
            == before.origin_search_run_record_identifier
        )
        refresh = await app.request_workspace_refresh(
            RequestWorkspaceRefreshRequest(
                workspace_record_identifier=workspace.record_identifier,
                track_identifier=track_identifier,
            )
        )
        work = await database.work(refresh.work_identifier)
        assert work["payload"]["search"] == changed.model_dump(mode="json")
        assert (
            work["payload"]["base_search_run_record_identifier"]
            == before.current_search_run_record_identifier
        )
