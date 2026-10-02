"""Public workflow parity and collision safety using a real temporary database."""

import sqlite3
from pathlib import Path
from time import time_ns
from typing import cast
from uuid import uuid4

import pytest

from carl._tests.test_marketplace_listing import _publish, _record
from carl._tests.test_review_workspace import _provenance, _search_run_value
from carl.core.analysis_batch import SelectionAnalysesRequest
from carl.core.components import Component, ComponentId
from carl.core.ebay import EbaySearchRequest
from carl.core.facebook_work import CreateSearchRequest
from carl.core.json import encode_json
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    FacebookSearchTargetSpecification,
)
from carl.core.models import CodeProvenance, NamedOutput, RecordDraft
from carl.core.review import CreateProductGuideRequest, ProductGuideDetails
from carl.core.review_workspace import (
    AcquireReviewBatchRequest,
    AddWorkspaceProductGuideRequest,
    CreateReviewWorksetRequest,
    CreateReviewWorkspaceRequest,
    CreateSelectionSnapshotRequest,
    ListingIdsSelection,
    ListingReviewInput,
    ListWorkspaceListingsRequest,
    RecordListingReviewsRequest,
    ReleaseReviewClaimRequest,
    RequestWorkspaceRefreshRequest,
    ReviewState,
    ReviewWorkset,
    WorkspaceProductGuideVersionPolicy,
)
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.io.sqlite import Database
from carl.review import ReviewApplication


async def _complete(
    database: Database,
    work_identifier: str,
    *,
    records: tuple[RecordDraft, ...] = (),
    result: dict[str, object],
) -> None:
    work = await database.work(work_identifier)
    token, operation = str(uuid4()), str(uuid4())
    claim = await database.claim_work(
        supported_capabilities=(
            WorkCapability(
                kind=tuple(work["kind"]), payload_schema_version=int(work["payload_schema_version"])
            ),
        ),
        worker_identifier="test-worker",
        lease_token=token,
        lease_duration_ns=60_000_000_000,
        utc_now_ns=time_ns,
        event_identifier=str(uuid4()),
    )
    assert claim.lease and claim.lease.work_item_identifier == work_identifier
    await database.begin_leased_operation(
        work_item_identifier=work_identifier,
        lease_token=token,
        worker_identifier="test-worker",
        lease_duration_ns=60_000_000_000,
        utc_now_ns=time_ns,
        event_identifier=str(uuid4()),
        operation_id=operation,
        component=Component(ComponentId(("test", "mixed")), 1, lambda: None),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-09-29T00:00:00+00:00",
    )
    await database.complete_leased_operation(
        work_item_identifier=work_identifier,
        lease_token=token,
        worker_identifier="test-worker",
        utc_now_ns=time_ns,
        event_identifier=str(uuid4()),
        operation_id=operation,
        records=records,
        artifacts=(),
        outputs=tuple(
            NamedOutput(name=("record", r.identifier), object_identifier=r.identifier)
            for r in records
        ),
        result=result,
        ended_at_utc="2026-09-29T00:00:01+00:00",
        duration_ns=1,
    )


@pytest.mark.anyio
async def test_mixed_review_batch_fills_across_reviewed_and_claimed_listings(
    tmp_path: Path,
) -> None:
    facebook_items = tuple(str(100000000000 + index) for index in range(9))
    ebay_items = tuple(str(256123450000 + index) for index in range(101))
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:

        async def provenance() -> CodeProvenance:
            return _provenance()

        app = ReviewApplication(database, tmp_path, code_provenance=provenance)
        facebook = CreateSearchRequest.model_validate_json(
            encode_json(
                {
                    "request": _search_run_value("scope")["request"],
                    "traversal": {"maximum_pages": 1},
                }
            )
        )
        group = await app.create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=(
                    FacebookSearchTargetSpecification(search=facebook),
                    EbaySearchTargetSpecification(search=EbaySearchRequest(query="scope")),
                )
            )
        )
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Paged mixed", search_run_record_identifier=group.record_identifier
            )
        )
        await _complete(
            database,
            group.targets[1].executions[0].work_identifier,
            records=(
                _record("ebay-run", "search_run", request={"query": "scope"}, listing_count=101),
                *(
                    _record(
                        f"ebay-card-{index}",
                        "search_listing_occurrence",
                        search_run_record_identifier="ebay-run",
                        item_identifier=item,
                        title="eBay scope",
                    )
                    for index, item in enumerate(ebay_items)
                ),
            ),
            result={"search_run_record_identifier": "ebay-run", "state": "completed"},
        )
        fb_run = _search_run_value("scope")
        fb_run["search_run_identifier"] = "fb-internal"
        cast(dict[str, object], fb_run["traversal"])["unique_listing_identifiers"] = list(
            facebook_items
        )
        await _complete(
            database,
            group.targets[0].executions[0].work_identifier,
            records=(
                RecordDraft(
                    identifier="fb-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value=fb_run,
                ),
                *(
                    RecordDraft(
                        identifier=f"fb-card-{index}",
                        kind=("carl", "facebook", "search_listing_occurrence"),
                        schema_version=1,
                        value={
                            "search_run_identifier": "fb-internal",
                            "acquisition_record_identifier": "fb-acquisition",
                            "listing_identifier": item,
                            "page_ordinal": 0,
                            "edge_index": index,
                            "original": {
                                "marketplace_listing_title": "Facebook scope",
                                "is_live": True,
                            },
                        },
                    )
                    for index, item in enumerate(facebook_items)
                ),
            ),
            result={"search_run_record_identifier": "fb-run"},
        )
        prior = await app.acquire_review_batch(
            AcquireReviewBatchRequest(
                request_identifier="prior",
                owner_identifier="other-agent",
                workspace_record_identifier=workspace.record_identifier,
                page_size=8,
            )
        )
        assert prior.lease is not None
        assert len(prior.batch.items) == 8
        await app.record_listing_reviews(
            RecordListingReviewsRequest(
                request_identifier="review-four",
                workspace_record_identifier=workspace.record_identifier,
                batch_record_identifier=prior.batch.record_identifier,
                claim_token=prior.lease.claim_token,
                claim_owner_identifier="other-agent",
                reviews=tuple(
                    ListingReviewInput(
                        listing_identifier=item.projection.listing_identifier,
                        projection_revision=item.projection.projection_revision,
                    )
                    for item in prior.batch.items[:4]
                ),
            )
        )
        request = AcquireReviewBatchRequest(
            request_identifier="paged",
            owner_identifier="review-agent",
            workspace_record_identifier=workspace.record_identifier,
            page_size=100,
            include_review_states=(ReviewState.UNREVIEWED, ReviewState.STALE),
            maximum_scan_pages=100,
        )
        acquired = await app.acquire_review_batch(request)
        assert acquired.lease is not None
        identifiers = {item.projection.listing_identifier for item in acquired.batch.items}
        assert len(identifiers) == 100
        assert acquired.batch.scanned_page_count == 2
        assert identifiers == set(acquired.lease.listing_identifiers)
        assert identifiers.isdisjoint(
            item.projection.listing_identifier for item in prior.batch.items
        )
        assert any(identifier.startswith("ebay:") for identifier in identifiers)
        assert any(not identifier.startswith("ebay:") for identifier in identifiers)
        assert acquired.batch.next_cursor is not None
        assert await app.acquire_review_batch(request) == acquired
        continuation = await app.acquire_review_batch(
            AcquireReviewBatchRequest(
                request_identifier="continuation",
                owner_identifier="review-agent",
                workspace_record_identifier=workspace.record_identifier,
                page_size=100,
                cursor=acquired.batch.next_cursor,
            )
        )
        remaining = {item.projection.listing_identifier for item in continuation.batch.items}
        assert len(remaining) == 2
        assert remaining.isdisjoint(identifiers)
        assert continuation.batch.next_cursor is None
        assert identifiers | remaining | {
            item.projection.listing_identifier for item in prior.batch.items
        } == (set(facebook_items) | {f"ebay:{item}" for item in ebay_items})


@pytest.mark.anyio
async def test_mixed_group_workspace_keeps_colliding_ids_distinct(tmp_path: Path) -> None:
    item = "256123456789"
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:

        async def provenance() -> CodeProvenance:
            return _provenance()

        app = ReviewApplication(database, tmp_path, code_provenance=provenance)
        facebook = CreateSearchRequest.model_validate_json(
            encode_json(
                {
                    "request": _search_run_value("scope")["request"],
                    "traversal": {"maximum_pages": 1},
                }
            )
        )
        group = await app.create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=(
                    FacebookSearchTargetSpecification(search=facebook),
                    EbaySearchTargetSpecification(search=EbaySearchRequest(query="scope")),
                )
            )
        )
        workspace = await app.create_review_workspace(
            CreateReviewWorkspaceRequest(
                name="Mixed", search_run_record_identifier=group.record_identifier
            )
        )
        pending = await app.get_workspace_work_status(workspace.record_identifier)
        assert pending.queued_count == 2 and not pending.idle and not pending.successful
        fb_run = _search_run_value("scope")
        fb_run["search_run_identifier"] = "fb-internal"
        cast(dict[str, object], fb_run["traversal"])["unique_listing_identifiers"] = [item]
        await _complete(
            database,
            group.targets[0].executions[0].work_identifier,
            records=(
                RecordDraft(
                    identifier="fb-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value=fb_run,
                ),
                RecordDraft(
                    identifier="fb-card",
                    kind=("carl", "facebook", "search_listing_occurrence"),
                    schema_version=1,
                    value={
                        "search_run_identifier": "fb-internal",
                        "acquisition_record_identifier": "fb-acquisition",
                        "listing_identifier": item,
                        "page_ordinal": 0,
                        "edge_index": 0,
                        "original": {
                            "marketplace_listing_title": "Facebook scope",
                            "is_live": True,
                        },
                    },
                ),
            ),
            result={"search_run_record_identifier": "fb-run"},
        )
        await _complete(
            database,
            group.targets[1].executions[0].work_identifier,
            records=(
                _record(
                    "ebay-run",
                    "search_run",
                    request=EbaySearchRequest(query="scope").model_dump(mode="json"),
                    listing_occurrence_record_identifiers=["ebay-card"],
                    listing_count=1,
                ),
                _record(
                    "ebay-card",
                    "search_listing_occurrence",
                    search_run_record_identifier="ebay-run",
                    item_identifier=item,
                    title="eBay scope",
                ),
            ),
            result={"search_run_record_identifier": "ebay-run", "state": "completed"},
        )
        workspace = await app.get_review_workspace(workspace.record_identifier)
        assert len(workspace.search_tracks) == 2
        summaries = await app.list_search_runs()
        assert {summary.marketplace for summary in summaries.search_runs} == {"facebook", "ebay"}
        membership = await app.get_search_run_listings("ebay-run")
        assert membership.listing_identifiers == (f"ebay:{item}",)
        page = await app.list_workspace_listings(
            ListWorkspaceListingsRequest(workspace_record_identifier=workspace.record_identifier)
        )
        assert {listing.listing_identifier for listing in page.listings} == {item, f"ebay:{item}"}
        assert {listing.title for listing in page.listings} == {"Facebook scope", "eBay scope"}
        workset = await app.create_review_workset(
            CreateReviewWorksetRequest(
                workspace_record_identifier=workspace.record_identifier,
                name="Both",
                listing_identifiers=(item, f"ebay:{item}"),
            )
        )
        assert isinstance(workset, ReviewWorkset)
        assert workset.listing_identifiers == (item, f"ebay:{item}")
        snapshot = await app.create_selection_snapshot(
            CreateSelectionSnapshotRequest(
                workspace_record_identifier=workspace.record_identifier,
                selection=ListingIdsSelection(listing_identifiers=(item, f"ebay:{item}")),
            )
        )
        assert {entry.listing_identifier for entry in snapshot.items} == {item, f"ebay:{item}"}
        acquisition = await app.acquire_review_batch(
            AcquireReviewBatchRequest(
                request_identifier="claim-mixed",
                owner_identifier="agent",
                workspace_record_identifier=workspace.record_identifier,
            )
        )
        assert acquisition.lease and set(acquisition.lease.listing_identifiers) == {
            item,
            f"ebay:{item}",
        }
        release = await app.release_review_claim(
            ReleaseReviewClaimRequest(
                request_identifier="release-mixed",
                claim_token=acquisition.lease.claim_token,
                owner_identifier="agent",
            )
        )
        assert release.released_listing_count == 2
        refreshes = []
        for track in workspace.search_tracks:
            refresh = await app.request_workspace_refresh(
                RequestWorkspaceRefreshRequest(
                    workspace_record_identifier=workspace.record_identifier,
                    track_identifier=track.track_identifier,
                )
            )
            refreshes.append(refresh)
            kind = tuple((await database.work(refresh.work_identifier))["kind"])
            expected_source = (
                "ebay" if track.current_search_run_record_identifier == "ebay-run" else "facebook"
            )
            assert kind == ("carl", expected_source, "work", "refresh_search")
            await _complete(
                database,
                refresh.work_identifier,
                result={
                    "refreshed_search_run_record_identifier": track.current_search_run_record_identifier,
                    "state": "completed_with_failures"
                    if expected_source == "ebay"
                    else "completed",
                },
            )
        settled = await app.get_workspace_work_status(workspace.record_identifier)
        assert settled.idle and not settled.successful
        assert settled.completed_with_failures_count == 1
        assert settled.terminal_failure_count == 0
        assert settled.failed_work[0].error_kind == "completed_with_failures"
        guide = await app.create_product_guide(
            CreateProductGuideRequest(
                identity=("scope",), display_name="Optics", text="Identify this optical equipment."
            )
        )
        assert isinstance(guide, ProductGuideDetails)
        await app.add_workspace_product_guide(
            AddWorkspaceProductGuideRequest(
                workspace_record_identifier=workspace.record_identifier,
                alias="Optics",
                product_guide_record_identifier=guide.record_identifier,
                version_policy=WorkspaceProductGuideVersionPolicy.PINNED,
            )
        )
        # Search-card-only listings are visible, but not silently eligible for detail analysis.
        assert guide.record_identifier
        assert len(refreshes) == 2
        preview = await app.preview_selection_analyses(
            SelectionAnalysesRequest(workspace_record_identifier=workspace.record_identifier)
        )
        assert preview.selected_observations == 0
        fixture_kind = ("test", "evidence")
        for fixture, records in (
            (
                "acquisition-fixture",
                (
                    RecordDraft(
                        identifier="fb-item-acquisition",
                        kind=("carl", "http", "acquisition"),
                        schema_version=1,
                        value={},
                    ),
                    _record("ebay-item-acquisition", "acquisition"),
                ),
            ),
            (
                "detail-fixture",
                (
                    RecordDraft(
                        identifier="fb-detail",
                        kind=("carl", "facebook", "listing_observation"),
                        schema_version=1,
                        value={
                            "listing_id": item,
                            "acquisition_record_id": "fb-item-acquisition",
                            "response_classification": {"kind": "full_listing"},
                            "images": [],
                            "fields": {
                                "title": {
                                    "state": "present",
                                    "evidence": [
                                        {"state": "present", "normalized": "Facebook scope"}
                                    ],
                                }
                            },
                        },
                    ),
                    _record(
                        "ebay-detail",
                        "listing_observation",
                        item_identifier=item,
                        acquisition_record_identifier="ebay-item-acquisition",
                        classification="detail",
                        title="eBay scope",
                        gallery_urls=[],
                    ),
                ),
            ),
        ):
            await database.enqueue_work(
                WorkDefinition(
                    identifier=fixture,
                    kind=fixture_kind,
                    payload_schema_version=1,
                    payload={},
                    deduplication_identity=(fixture,),
                    not_before_utc_ns=0,
                    scopes=(
                        SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                        SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=fixture_kind),
                    ),
                ),
                WorkRequester(
                    request_identifier=str(uuid4()), kind=("test",), identifier=fixture, context={}
                ),
                event_identifier=str(uuid4()),
                enqueued_at_utc_ns=time_ns(),
            )
            await _complete(database, fixture, records=records, result={})
        request = SelectionAnalysesRequest(
            workspace_record_identifier=workspace.record_identifier, allow_incomplete_gallery=True
        )
        preview = await app.preview_selection_analyses(request)
        assert preview.selected_observations == 2
        requested = await app.request_selection_analyses(request)
        payload = (await database.work(requested.work_identifier))["payload"]
        assert set(payload["listing_observation_record_identifiers"]) == {
            "fb-detail",
            "ebay-detail",
        }


@pytest.mark.anyio
async def test_v8_claim_migration_preserves_existing_rows(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await _publish(database, (_record("workspace", "workspace"), _record("batch", "batch")))
    with sqlite3.connect(path) as connection:
        connection.execute("ALTER TABLE review_listing_claims RENAME TO new_claims")
        connection.execute("""CREATE TABLE review_listing_claims (
            workspace_record_id TEXT NOT NULL, listing_identifier TEXT NOT NULL CHECK (
                length(listing_identifier)>0 AND listing_identifier NOT GLOB '*[^0-9]*'),
            batch_record_id TEXT NOT NULL, claim_token TEXT NOT NULL, owner_identifier TEXT NOT NULL,
            acquired_at_utc_ns INTEGER NOT NULL, lease_expires_at_utc_ns INTEGER NOT NULL,
            PRIMARY KEY(workspace_record_id, listing_identifier),
            FOREIGN KEY(workspace_record_id) REFERENCES objects(id),
            FOREIGN KEY(batch_record_id) REFERENCES objects(id)) STRICT""")
        connection.execute("DROP TABLE new_claims")
        connection.execute(
            "INSERT INTO review_listing_claims VALUES ('workspace','256123456789','batch','token','owner',1,100)"
        )
        connection.executemany(
            "UPDATE schema_metadata SET value=? WHERE key=?",
            ((value, key) for key, value in Database._v8_metadata().items()),
        )
    async with Database.managed(path) as database:
        await database.validate_schema()
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT * FROM review_listing_claims").fetchone() == (
            "workspace",
            "256123456789",
            "batch",
            "token",
            "owner",
            1,
            100,
        )
        connection.execute(
            "INSERT INTO review_listing_claims VALUES ('workspace','ebay:256123456789','batch','ebay-token','owner',1,100)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO review_listing_claims VALUES ('workspace','ebay:256123456789x','batch','bad','owner',1,100)"
            )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
