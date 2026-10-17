"""Refresh planning reads bounded indexed scopes without loading work histories."""

# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl._tests.test_ebay_item_workers import _identifiers, _retain_records
from carl._tests.test_worker import _definition, _requester
from carl.core.models import RecordDraft
from carl.core.work import WorkRequester
from carl.io.sqlite import Database


@pytest.mark.anyio
async def test_refresh_batch_reads_keep_order_duplicates_and_missing_errors(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            tuple(
                RecordDraft(
                    identifier=identifier,
                    kind=("test", "record"),
                    schema_version=2,
                    value={"identifier": identifier},
                )
                for identifier in ("first", "second")
            ),
            _identifiers(),
        )
        rows = await database.records_by_identifiers(("second", "first", "second"))
        assert rows == (
            ("second", ("test", "record"), 2, {"identifier": "second"}),
            ("first", ("test", "record"), 2, {"identifier": "first"}),
            ("second", ("test", "record"), 2, {"identifier": "second"}),
        )
        with pytest.raises(KeyError):
            await database.records_by_identifiers(("first", "missing"))
        for identifier in ("one", "two"):
            await database.enqueue_work(
                _definition(identifier=identifier, kind=("test", "work")),
                _requester(identifier),
                event_identifier=f"enqueue-{identifier}",
                enqueued_at_utc_ns=1,
            )
        progress = await database.work_progress(("two", "one", "two"))
        assert [row["identifier"] for row in progress] == ["two", "one", "two"]
        assert all(row["kind"] == ["test", "work"] for row in progress)
        assert all(row["state"] == "pending" and row["attempt"] == 0 for row in progress)
        assert all(row["eligible_at_utc_ns"] == 1 for row in progress)
        assert all("operations" not in row and "payload" not in row for row in progress)
        with pytest.raises(KeyError):
            await database.work_progress(("one", "missing"))
        for method in (database.work_progress, database.records_by_identifiers):
            assert await method(()) == ()
            with pytest.raises(ValueError):
                await method(("first",) * 1001)


@pytest.mark.anyio
async def test_ebay_gallery_scope_includes_checkpoint_owners_and_excludes_unrelated_records(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier="first-reference",
                    kind=("carl", "ebay", "gallery_image_reference"),
                    schema_version=1,
                    value={"observation_record_identifier": "wanted"},
                ),
                RecordDraft(
                    identifier="unrelated",
                    kind=("carl", "ebay", "gallery_image_reference"),
                    schema_version=1,
                    value={"observation_record_identifier": "other"},
                ),
                RecordDraft(
                    identifier="wrong-kind",
                    kind=("test", "record"),
                    schema_version=1,
                    value={"observation_record_identifier": "wanted"},
                ),
            ),
            _identifiers(),
        )
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE operations SET state='started' WHERE id="
                "(SELECT created_by_operation_id FROM objects WHERE id='first-reference')"
            )
        assert await database.ebay_gallery_references(("wanted", "wanted")) == (
            ("first-reference", {"observation_record_identifier": "wanted"}),
        )
        assert await database.ebay_gallery_references(()) == ()


@pytest.mark.anyio
async def test_batch_requester_reads_order_distinct_work_and_allow_missing_requesters(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        for identifier in ("one", "two"):
            await database.enqueue_work(
                _definition(identifier=identifier, kind=("test", "work")),
                _requester(identifier),
                event_identifier=f"enqueue-{identifier}",
                enqueued_at_utc_ns=1,
            )
        for index, (requester, work) in enumerate(
            (("first", "two"), ("first", "one"), ("first", "two"), ("second", "one"))
        ):
            await database.attach_work_request(
                work,
                WorkRequester(
                    request_identifier=f"request-{index}",
                    kind=("test", "requester"),
                    identifier=requester,
                    context={},
                ),
                event_identifier=f"attach-{index}",
                requested_at_utc_ns=2 + index,
            )
        assert await database.requested_work_for_requesters(
            requester_kind=("test", "requester"),
            requester_identifiers=("second", "missing", "first"),
        ) == (("second", "one"), ("first", "two"), ("first", "one"))
