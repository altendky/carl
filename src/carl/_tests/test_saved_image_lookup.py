"""Indexed image reuse preserves evidence and does not scan unrelated history."""

# pyright: reportPrivateUsage=false

import sqlite3
from collections.abc import Awaitable, Callable
from pathlib import Path

import anyio
import pytest

from carl._tests.test_ebay_item_workers import _identifiers, _retain_records
from carl._tests.test_facebook_acquisition_reuse import OTHER, URL, _reference
from carl.core.models import JsonValue, RecordDraft
from carl.io.sqlite import Database

EBAY_URL = "https://i.ebayimg.com/images/g/AbC/s-l1600.jpg?quality=80"


def _saved(
    identifier: str,
    *,
    marketplace: str,
    url: str,
    photo_id: str | None = None,
    state: str = "saved",
) -> RecordDraft:
    value: dict[str, JsonValue] = {"state": state}
    if marketplace == "facebook":
        value.update(original_url=url, source_photo_id=photo_id)
    else:
        value["url"] = url
    return RecordDraft(
        identifier=identifier,
        kind=("carl", marketplace, "image_result"),
        schema_version=1,
        value=value,
    )


def _restore_version_eleven(path: Path) -> None:
    """Remove only v12 additions to leave the genuine preceding schema layout."""

    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE saved_image_lookup")
        connection.execute("DROP INDEX records_ebay_gallery_observation")
        connection.executemany(
            "UPDATE schema_metadata SET value=? WHERE key=?",
            ((value, key) for key, value in Database._v11_metadata().items()),
        )


async def _vm_steps[Result](
    database: Database, query: Callable[[], Awaitable[Result]]
) -> tuple[Result, int]:
    steps = 0

    def count() -> bool:
        nonlocal steps
        steps += 100
        return False

    async with database._connections.reader() as connection:
        await connection.set_progress_handler(count, nsteps=100, id="image-lookup-budget")
        try:
            result = await query()
        finally:
            await connection.set_progress_handler(None, id="image-lookup-budget")
    return result, steps


@pytest.mark.anyio
@pytest.mark.parametrize("marketplace", ["facebook", "ebay"])
async def test_image_candidate_cost_tracks_requested_keys_not_retained_history(
    tmp_path: Path, marketplace: str
) -> None:
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        url = URL if marketplace == "facebook" else EBAY_URL

        async def query():
            if marketplace == "facebook":
                return await database.saved_facebook_image_candidates_for_references(
                    (
                        *(
                            _reference(URL.replace("photo.jpg", f"absent-{index}.jpg"), str(index))
                            for index in range(64)
                        ),
                        _reference(URL),
                    )
                )
            return await database.saved_ebay_image_candidates(
                (
                    *(EBAY_URL.replace("/AbC/", f"/absent-{index}/") for index in range(64)),
                    EBAY_URL,
                )
            )

        await _retain_records(
            database,
            (_saved("matching", marketplace=marketplace, url=url, photo_id="photo-1"),),
            identifiers,
        )
        before, before_steps = await _vm_steps(database, query)
        await _retain_records(
            database,
            tuple(
                _saved(
                    f"irrelevant-{index}",
                    marketplace=marketplace,
                    url=(
                        URL.replace("photo.jpg", f"different-{index}.jpg")
                        if marketplace == "facebook"
                        else EBAY_URL.replace("/AbC/", f"/different-{index}/")
                    ),
                    photo_id=f"different-photo-{index}",
                )
                for index in range(4096)
            ),
            identifiers,
        )
        after, after_steps = await _vm_steps(database, query)
        assert before == after
        assert [identifier for identifier, _ in after] == ["matching"]
        assert after_steps <= before_steps + 1000
        assert after_steps < 20_000
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT lookup.result_object_id
                FROM json_each(?) AS requested
                CROSS JOIN saved_image_lookup AS lookup
                  ON lookup.marketplace=?
                 AND lookup.lookup_kind=json_extract(requested.value, '$[0]')
                 AND lookup.lookup_value=json_extract(requested.value, '$[1]')
                """,
                ('[["url","missing"]]', marketplace),
            )
            plan = " ".join(str(row[3]) for row in await cursor.fetchall())
        assert "SEARCH lookup USING PRIMARY KEY" in plan


@pytest.mark.anyio
async def test_ebay_lookup_preserves_exact_url_and_cross_host_path_candidates(
    tmp_path: Path,
) -> None:
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                _saved("exact", marketplace="ebay", url=EBAY_URL),
                _saved(
                    "cross-host",
                    marketplace="ebay",
                    url=EBAY_URL.replace("i.ebayimg", "i2.ebayimg"),
                ),
                _saved(
                    "other-transform",
                    marketplace="ebay",
                    url=EBAY_URL.replace("quality=80", "quality=50"),
                ),
                _saved("other-size", marketplace="ebay", url=EBAY_URL.replace("1600", "500")),
                _saved("unknown-path", marketplace="ebay", url="https://i.ebayimg.com/unknown.jpg"),
                _saved("failed-result", marketplace="ebay", url=EBAY_URL, state="failed"),
                _saved("wrong-marketplace", marketplace="facebook", url=EBAY_URL),
            ),
            identifiers,
        )
        for state in ("started", "failed"):
            await _retain_records(
                database,
                (_saved(f"{state}-owner", marketplace="ebay", url=EBAY_URL),),
                identifiers,
            )
            async with database._connections.writer() as connection:
                await connection.execute(
                    "UPDATE operations SET state=? WHERE id="
                    "(SELECT created_by_operation_id FROM objects WHERE id=?)",
                    (state, f"{state}-owner"),
                )
        result = await database.saved_ebay_image_candidates((EBAY_URL, EBAY_URL))
        assert [identifier for identifier, _ in result] == [
            "exact",
            "cross-host",
            "other-transform",
        ]
        assert [
            identifier
            for identifier, _ in await database.saved_ebay_image_candidates(
                ("https://i.ebayimg.com/unknown.jpg",)
            )
        ] == ["unknown-path"]
        assert await database.saved_ebay_image_candidates(()) == ()


@pytest.mark.anyio
async def test_version_eleven_migration_backfills_once_and_indexes_future_publications(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "test.sqlite3"
    identifiers = _identifiers()
    async with Database.managed(path, initialize=True) as database:
        await _retain_records(
            database,
            (
                _saved("facebook", marketplace="facebook", url=OTHER, photo_id="photo-1"),
                _saved("ebay", marketplace="ebay", url=EBAY_URL),
            ),
            identifiers,
        )
    _restore_version_eleven(path)
    migrations = 0
    original = Database._migrate_saved_image_lookup

    async def counted(self, connection):
        nonlocal migrations
        migrations += 1
        await original(self, connection)

    monkeypatch.setattr(Database, "_migrate_saved_image_lookup", counted)
    async with Database.managed(path) as database:
        assert [
            identifier
            for identifier, _ in await database.saved_facebook_image_candidates_for_references(
                (_reference(URL),)
            )
        ] == ["facebook"]
        assert [
            identifier for identifier, _ in await database.saved_ebay_image_candidates((EBAY_URL,))
        ] == ["ebay"]
        await _retain_records(
            database,
            (_saved("new-ebay", marketplace="ebay", url=EBAY_URL),),
            identifiers,
        )
        assert [
            identifier for identifier, _ in await database.saved_ebay_image_candidates((EBAY_URL,))
        ] == [
            "ebay",
            "new-ebay",
        ]
    async with Database.managed(path):
        pass
    assert migrations == 1
    with sqlite3.connect(path) as connection:
        assert (
            dict(connection.execute("SELECT key,value FROM schema_metadata"))
            == Database._expected_metadata()
        )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.anyio
@pytest.mark.parametrize("migration", [False, True])
async def test_cancellation_rolls_back_derived_keys_and_evidence_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, migration: bool
) -> None:
    path = tmp_path / "test.sqlite3"
    identifiers = _identifiers()
    async with Database.managed(path, initialize=True) as database:
        if migration:
            await _retain_records(
                database,
                (_saved("historic", marketplace="facebook", url=OTHER, photo_id="photo-1"),),
                identifiers,
            )
    if migration:
        _restore_version_eleven(path)
    original = Database._publish_saved_image_lookup

    with anyio.CancelScope() as scope:

        async def cancelled(connection, rows):
            await original(connection, rows)
            if rows:
                scope.cancel()
                await anyio.lowlevel.checkpoint()

        monkeypatch.setattr(Database, "_publish_saved_image_lookup", staticmethod(cancelled))
        async with Database.managed(path) as database:
            if migration:
                pytest.fail("Migration should have been cancelled")
            await _retain_records(
                database,
                (_saved("cancelled", marketplace="ebay", url=EBAY_URL),),
                identifiers,
            )
            pytest.fail("Publication should have been cancelled")
    assert scope.cancelled_caught
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT 1 FROM objects WHERE id='cancelled'").fetchone() is None
        if migration:
            assert (
                dict(connection.execute("SELECT key,value FROM schema_metadata"))
                == Database._v11_metadata()
            )
            assert (
                connection.execute(
                    "SELECT 1 FROM sqlite_schema WHERE name='saved_image_lookup'"
                ).fetchone()
                is None
            )
        else:
            assert connection.execute("SELECT count(*) FROM saved_image_lookup").fetchone() == (0,)
