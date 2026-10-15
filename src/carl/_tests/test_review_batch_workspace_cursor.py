"""Short workspace continuations resolve durable batches without changing replay identity."""

# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl._tests.test_review_response_details import _provenance
from carl.core.components import Component, ComponentId
from carl.core.models import JsonValue, RecordDraft
from carl.core.review_errors import ReviewInputError
from carl.core.review_response_pages import PagedReviewBatchAcquisition, workspace_batch_cursor
from carl.core.review_workspace import (
    REVIEW_BATCH_KIND,
    AcquireReviewBatchRequest,
    ReviewBatch,
    ReviewBatchAcquisition,
    review_mutation_request_sha256,
)
from carl.io.sqlite import Database
from carl.review import ReviewApplication


def _batch(cursor: str | None) -> ReviewBatch:
    return ReviewBatch(
        record_identifier="previous-batch",
        workspace_record_identifier="workspace",
        created_at_utc="2026-10-15T00:00:00+00:00",
        items=(),
        next_cursor=cursor,
        scanned_page_count=1,
    )


@pytest.mark.anyio
async def test_workspace_cursor_is_durable_scoped_and_preserves_legacy_tokens(
    tmp_path: Path,
) -> None:
    batch = _batch("raw-frozen-run-set-" + "x" * 6_000)
    path = tmp_path / "continuations.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        await database.publish_records_operation(
            component=Component(ComponentId(("test", "batch")), 1, lambda: None),
            operation_identifier="seed-batch",
            records=(
                RecordDraft(
                    identifier=batch.record_identifier,
                    kind=REVIEW_BATCH_KIND,
                    schema_version=1,
                    value=batch.model_dump(mode="json"),
                ),
            ),
            inputs=(),
            outputs=(),
            provenance=_provenance(),
            invocation={},
            started_at_utc=batch.created_at_utc,
            ended_at_utc=batch.created_at_utc,
            duration_ns=0,
            result={"state": "completed"},
        )
    async with Database.managed(path) as database:
        application = ReviewApplication(database, tmp_path)
        assert (
            await application._review_batch_workspace_cursor(
                "workspace", workspace_batch_cursor(batch)
            )
            == batch.next_cursor
        )
        assert await application._review_batch_workspace_cursor("workspace", None) is None
        assert (
            await application._review_batch_workspace_cursor("workspace", "legacy-token")
            == "legacy-token"
        )
        with pytest.raises(ReviewInputError, match="another workspace"):
            await application._review_batch_workspace_cursor("other", workspace_batch_cursor(batch))
        assert (
            await application.get_review_batch(batch.record_identifier)
        ).next_cursor == batch.next_cursor


@pytest.mark.anyio
@pytest.mark.parametrize("raw_cursor", [None, "batch:another"])
async def test_workspace_cursor_rejects_exhausted_or_chained_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raw_cursor: str | None
) -> None:
    async def get_batch(_self: ReviewApplication, _identifier: str) -> ReviewBatch:
        return _batch(raw_cursor)

    monkeypatch.setattr(ReviewApplication, "get_review_batch", get_batch)
    async with Database.managed(tmp_path / "invalid.sqlite3", initialize=True) as database:
        with pytest.raises(ReviewInputError):
            await ReviewApplication(database, tmp_path)._review_batch_workspace_cursor(
                "workspace", "batch:previous-batch"
            )


@pytest.mark.anyio
async def test_acquisition_replay_uses_original_short_cursor_before_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = AcquireReviewBatchRequest(
        workspace_record_identifier="workspace",
        request_identifier="replay",
        owner_identifier="owner",
        cursor="batch:previous-batch",
        compact=True,
    )
    stored = ReviewBatchAcquisition(batch=_batch(None), lease=None)
    expected_hash = review_mutation_request_sha256("acquire_batch", request)

    async def replay(
        _self: Database, *, action: str, request_identifier: str, request_sha256: str
    ) -> JsonValue:
        assert (action, request_identifier, request_sha256) == (
            "acquire_batch",
            "replay",
            expected_hash,
        )
        return stored.model_dump(mode="json")

    async def resolve(_self: ReviewApplication, _workspace: str, _cursor: str | None) -> str | None:
        pytest.fail("A replay must not depend on resolving its previous cursor")

    monkeypatch.setattr(Database, "review_mutation_replay", replay)
    monkeypatch.setattr(ReviewApplication, "_review_batch_workspace_cursor", resolve)
    async with Database.managed(tmp_path / "replay.sqlite3", initialize=True) as database:
        result = await ReviewApplication(database, tmp_path).acquire_review_batch(request)
        assert isinstance(result, PagedReviewBatchAcquisition)
        assert result.batch.record_identifier == stored.batch.record_identifier
