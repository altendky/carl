"""Mixed bulk rejection must prepare revisions in batches, not per listing."""

# Deliberately reuse private offline fixtures and inspect SQLite query counts.
# pyright: reportPrivateUsage=false

from pathlib import Path
from typing import cast

import anyio
import pytest

from carl._tests.test_marketplace_listing import _record
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_review_workspace import _provenance, _search_run_value
from carl.core.composed_projection import GetComposedListingRequest
from carl.core.ebay import EbaySearchRequest
from carl.core.facebook_work import CreateSearchRequest
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
    FacebookSearchTargetSpecification,
)
from carl.core.models import CodeProvenance, RecordDraft
from carl.core.review_errors import ReviewInputError
from carl.core.review_workspace import (
    CreateReviewWorkspaceRequest,
    RecordWorkspaceBulkReviewRequest,
    ReviewDisposition,
    ReviewWorkspace,
)
from carl.io.sqlite import Database
from carl.marketplace_projection import _facebook_projection
from carl.review import ReviewApplication


async def _workspace(app: ReviewApplication, count: int) -> ReviewWorkspace:
    run = _search_run_value("scope")
    facebook = CreateSearchRequest.model_validate(
        {"request": run["request"], "traversal": {"maximum_pages": 1}}
    )
    group = await app.create_marketplace_search(
        CreateMarketplaceSearchRequest(
            targets=(
                FacebookSearchTargetSpecification(search=facebook),
                EbaySearchTargetSpecification(search=EbaySearchRequest(query="scope")),
            )
        )
    )
    ids = tuple(str(100000000000 + index) for index in range(count))
    run["search_run_identifier"] = "fb-internal"
    cast(dict[str, object], run["traversal"])["unique_listing_identifiers"] = list(ids)
    await _complete(
        app.database,
        group.targets[0].executions[0].work_identifier,
        records=(
            RecordDraft(
                identifier="fb-run",
                kind=("carl", "facebook", "search_run"),
                schema_version=1,
                value=run,
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
                        "original": {"marketplace_listing_title": "Eyepiece", "is_live": True},
                    },
                )
                for index, item in enumerate(ids)
            ),
        ),
        result={"search_run_record_identifier": "fb-run"},
    )
    await _complete(
        app.database,
        group.targets[1].executions[0].work_identifier,
        records=(
            _record("ebay-run", "search_run", request={"query": "scope"}, listing_count=count),
            *(
                _record(
                    f"ebay-card-{index}",
                    "search_listing_occurrence",
                    search_run_record_identifier="ebay-run",
                    item_identifier=str(256123450000 + index),
                    title="Eyepiece",
                    displayed_price="$10.00",
                    listing_state="active",
                )
                for index in range(count)
            ),
        ),
        result={"search_run_record_identifier": "ebay-run"},
    )
    workspace = await app.create_review_workspace(
        CreateReviewWorkspaceRequest(
            name="Mixed bulk",
            search_run_record_identifier=group.record_identifier,
        )
    )
    return await app.get_review_workspace(workspace.record_identifier)


@pytest.mark.anyio
async def test_bulk_rejections_batch_prepare_publish_and_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def provenance() -> CodeProvenance:
        return _provenance()

    async def no_per_listing_facebook(*_args: object, **_kwargs: object):
        pytest.fail("Bulk rejection must use batched Facebook projection reads")

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path, code_provenance=provenance)
        workspace = await _workspace(app, 500)
        monkeypatch.setattr(
            "carl.marketplace_projection._facebook_projection", no_per_listing_facebook
        )
        queries: list[str] = []

        def trace(_cursor: object, statement: str, _bindings: object) -> bool:
            if statement.lstrip().upper().startswith(("SELECT", "WITH")):
                queries.append(statement)
            return True

        for connection in (*database._connections._readers, database._connections._writer):
            connection.set_exec_trace(trace)
        request = RecordWorkspaceBulkReviewRequest(
            request_identifier="bulk",
            workspace_record_identifier=workspace.record_identifier,
            disposition=ReviewDisposition.REJECTED,
            exclude_listing_identifiers=("100000000000",),
        )
        with anyio.fail_after(20):
            result = await app.record_workspace_bulk_review(request)
        assert result.candidate_listings_examined == 1000
        assert result.selection_member_count == 1000
        assert result.recorded_count == 999
        assert result.explicitly_excluded_count == 1
        assert len(queries) < 200, f"Bulk preparation executed {len(queries)} read queries"
        queries.clear()
        replay = await app.record_workspace_bulk_review(request)
        assert replay == result
        assert len(queries) == 1
        reviews = await database.latest_listing_reviews(
            workspace_record_identifier=workspace.record_identifier,
            listing_identifiers=("100000000000", "100000000001", "ebay:256123450000"),
        )
        assert "100000000000" not in reviews
        assert reviews["100000000001"].disposition is ReviewDisposition.REJECTED
        assert reviews["ebay:256123450000"].disposition is ReviewDisposition.REJECTED


@pytest.mark.anyio
async def test_mixed_bulk_limit_rejects_without_recording_reviews(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        workspace = await _workspace(app, 3)
        with pytest.raises(ReviewInputError, match="limit"):
            await app.record_workspace_bulk_review(
                RecordWorkspaceBulkReviewRequest(
                    request_identifier="bounded",
                    workspace_record_identifier=workspace.record_identifier,
                    disposition=ReviewDisposition.REJECTED,
                    maximum_candidate_listings_examined=5,
                )
            )
        assert not await database.latest_listing_reviews(
            workspace_record_identifier=workspace.record_identifier,
            listing_identifiers=("100000000000", "ebay:256123450000"),
        )


@pytest.mark.anyio
async def test_mixed_bulk_preserves_bounded_ancestry_revision_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_scope = ReviewApplication._projection_search_scope

    async def bounded_scope(
        application: ReviewApplication,
        primary_search_run_record_identifier: str,
        additional_search_run_record_identifiers: tuple[str, ...],
        *,
        as_of_completion_sequence: int,
        maximum_runs: int,
    ):
        ancestry = await original_scope(
            application,
            primary_search_run_record_identifier,
            additional_search_run_record_identifiers,
            as_of_completion_sequence=as_of_completion_sequence,
            maximum_runs=maximum_runs,
        )
        return ancestry.model_copy(update={"older_ancestry_truncated": True})

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        workspace = await _workspace(app, 1)
        monkeypatch.setattr(ReviewApplication, "_projection_search_scope", bounded_scope)
        as_of, examined, projections = await app._workspace_bulk_review_projections(
            workspace,
            maximum_candidate_listings_examined=10,
        )
        assert examined == 2
        facebook = next(p for p in projections if p.listing_identifier == "100000000000")
        expected = await _facebook_projection(
            app,
            GetComposedListingRequest(
                listing_identifier="100000000000",
                maximum_gallery_images=0,
                maximum_analyses=0,
            ),
            ("fb-run",),
            as_of,
        )
        assert facebook.projection_revision == expected.projection_revision
        assert facebook.search_membership is not None
        assert facebook.search_membership.older_ancestry_truncated
