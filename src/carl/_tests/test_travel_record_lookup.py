"""Immutable travel records use exact listing seeks and frozen completions."""

# pyright: reportPrivateUsage=false

from pathlib import Path
from time import time_ns
from typing import cast

import apsw
import pytest

from carl._tests.test_marketplace_listing import _publish
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_work import _definition, _requester
from carl.core.models import RecordDraft
from carl.io.sqlite import Database


def _record(
    identifier: str,
    *,
    listing: str = "123",
    origin: str = "home",
    destination: str = "location-one",
    schema_version: int = 1,
    kind: tuple[str, ...] = ("carl", "travel", "estimate"),
) -> RecordDraft:
    return RecordDraft(
        identifier=identifier,
        kind=kind,
        schema_version=schema_version,
        value={
            "listing_id": listing,
            "origin_identity": origin,
            "destination_identity": destination,
            "estimate": {"status": "ready", "distance_meters": 100},
        },
    )


async def _completed(database: Database, identifier: str, *records: RecordDraft) -> None:
    await database.enqueue_work(
        _definition().model_copy(
            update={"identifier": identifier, "deduplication_identity": (identifier,)}
        ),
        _requester().model_copy(update={"request_identifier": f"request-{identifier}"}),
        event_identifier=f"enqueued-{identifier}",
        enqueued_at_utc_ns=time_ns(),
    )
    await _complete(database, identifier, records=records, result={})


@pytest.mark.anyio
async def test_travel_lookup_matches_origin_destination_and_frozen_completion(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _completed(database, "first", _record("first-record"))
        await _completed(
            database,
            "second",
            _record("second-record"),
            _record("tie-winner"),
            _record("ebay-record", listing="ebay:123456789"),
        )
        boundary = await database.current_completion_boundary()
        await _completed(database, "later", _record("later-record"))
        await _completed(database, "other-origin", _record("other-origin", origin="elsewhere"))
        await _completed(
            database, "other-location", _record("other-location", destination="location-two")
        )
        await _completed(database, "other-schema", _record("other-schema", schema_version=2))
        await _completed(database, "other-kind", _record("other-kind", kind=("test", "record")))
        await _publish(database, (_record("without-work-completion"),))

        selected = await database.latest_travel_estimate_records(
            ("123", "ebay:123456789", "999", "123"),
            origin_identity="home",
            destination_identities={
                "123": "location-one",
                "ebay:123456789": "location-one",
                "999": "location-one",
            },
            as_of_completion_sequence=boundary,
        )
        assert selected == {
            "123": ("tie-winner", _record("tie-winner").value),
            "ebay:123456789": (
                "ebay-record",
                _record("ebay-record", listing="ebay:123456789").value,
            ),
        }
        latest = await database.latest_travel_estimate_records(
            ("123",),
            origin_identity="home",
            destination_identities={"123": "location-one"},
            as_of_completion_sequence=await database.current_completion_boundary(),
        )
        assert latest["123"][0] == "later-record"
        assert (
            await database.latest_travel_estimate_records(
                ("123",),
                origin_identity="home",
                destination_identities={"123": "location-two"},
                as_of_completion_sequence=await database.current_completion_boundary(),
            )
        )["123"][0] == "other-location"
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE operations SET state = 'failed' WHERE id = "
                "(SELECT created_by_operation_id FROM objects WHERE id = 'later-record')"
            )
        assert (
            await database.latest_travel_estimate_records(
                ("123",),
                origin_identity="home",
                destination_identities={"123": "location-one"},
                as_of_completion_sequence=await database.current_completion_boundary(),
            )
        )["123"][0] == "tie-winner"


@pytest.mark.anyio
async def test_travel_lookup_plan_seeks_requested_listing_index(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _completed(database, "first", _record("first-record"))
        query: tuple[str, tuple[apsw.SQLiteValue, ...]] | None = None

        def trace(_cursor: object, statement: str, bindings: object) -> bool:
            nonlocal query
            if "travel_ranked AS MATERIALIZED" in statement:
                query = statement, cast("tuple[apsw.SQLiteValue, ...]", bindings)
            return True

        listing_ids = tuple(str(index + 123) for index in range(1000))
        async with database._connections.reader() as connection:
            connection.set_exec_trace(trace)
            try:
                selected = await database.latest_travel_estimate_records(
                    listing_ids,
                    origin_identity="home",
                    destination_identities=dict.fromkeys(listing_ids, "location-one"),
                    as_of_completion_sequence=await database.current_completion_boundary(),
                )
            finally:
                connection.set_exec_trace(None)
            assert query is not None
            cursor = await connection.execute("EXPLAIN QUERY PLAN " + query[0], query[1])
            plan = [str(row[3]) for row in await cursor.fetchall()]
        assert selected["123"][0] == "first-record"
        assert any(
            "SEARCH record USING INDEX records_listing_observation_listing (<expr>=?)" in step
            for step in plan
        )
        assert not any("SCAN record" in step for step in plan)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("listing_ids", "origin", "destinations", "boundary"),
    [
        (("123",) * 1001, "home", {"123": "location-one"}, 0),
        (("invalid",), "home", {"invalid": "location-one"}, 0),
        (("123",), "", {"123": "location-one"}, 0),
        (("123",), "home", {}, 0),
        (("123",), "home", {"123": ""}, 0),
        (("123",), "home", {"123": "location-one"}, -1),
    ],
)
async def test_travel_lookup_rejects_invalid_bounds(
    tmp_path: Path,
    listing_ids: tuple[str, ...],
    origin: str,
    destinations: dict[str, str],
    boundary: int,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        with pytest.raises(ValueError, match="Travel estimate lookup"):
            await database.latest_travel_estimate_records(
                listing_ids,
                origin_identity=origin,
                destination_identities=destinations,
                as_of_completion_sequence=boundary,
            )
        assert (
            await database.latest_travel_estimate_records(
                (), origin_identity="home", destination_identities={}, as_of_completion_sequence=0
            )
            == {}
        )
