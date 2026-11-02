"""Historical acquisition keys migrate without rewriting evidence or ownership."""

# Historical fixtures and corruption boundaries intentionally inspect SQL metadata.
# pyright: reportPrivateUsage=false

import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

import carl.io.sqlite as sqlite
from carl._tests.test_stage_storage import NOW
from carl._tests.test_work_dispatch_migration import _rows, _seed_live_ownership
from carl.core.acquisition_identity import (
    acquisition_resource_identity,
    acquisition_resource_identity_json,
)
from carl.core.json import encode_json
from carl.core.models import JsonValue
from carl.core.stages import StageDefinition
from carl.core.work import WorkCapability
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
_COLUMN = "acquisition_resource_identity_json"
_INDEX = "work_items_live_acquisition"
_HISTORICAL_HASHES = {
    # Exact _SCHEMA strings, copied without edits from the named commits.
    # v12: 8a0ff24e751da3686a91705623e0edd17cbe06cb
    12: "93b88a76a1bbc95d29c0cc72876560e5354da0d44250a483f9f5afe177ad2241",
    # v18: a92a5be42d0b68061c7e68fe53415180f0c6a48a
    18: "e1451850f5bd245e419c06b205c2d4f7b2193220dbdf54b8ce122271f3bffc45",
}
_EBAY_IMAGE = "https://i.ebayimg.com/images/g/photo/s-l1600.jpg?fit=one&fit=two"
_FACEBOOK_IMAGE = "https://scontent-one.xx.fbcdn.net/v/t39.84726-6/photo.jpg?oh=old&stp=crop"
_CASES: tuple[tuple[tuple[str, ...], dict[str, JsonValue]], ...] = (
    (("carl", "ebay", "collect", "item"), {"request": {"item_identifier": "123456789"}}),
    (
        ("carl", "ebay", "collect", "description"),
        {"request": {}, "url": "https://example.test/description?x=1"},
    ),
    (("carl", "ebay", "collect", "image"), {"url": _EBAY_IMAGE}),
    (
        ("carl", "facebook", "work", "collect_image"),
        {
            "reference": {"original_url": _FACEBOOK_IMAGE, "photo_id": "photo"},
            "request_plan": {
                "url": _FACEBOOK_IMAGE,
                "headers": [{"name": "X-Route", "value": "raw"}],
                "routing": ["direct"],
            },
        },
    ),
    (
        ("carl", "facebook", "work", "collect_item"),
        {"listing_id": "123456789", "request_plan": {"url": "https://example.test/item"}},
    ),
)


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _old_metadata(version: int) -> dict[str, str]:
    return {
        "schema_identity_json": encode_json(
            {"namespace": "carl", "domain": "storage", "backend": "sqlite", "version": version}
        ),
        "schema_version": str(version),
        "schema_definition_sha256": _HISTORICAL_HASHES[version],
    }


def _create_legacy(path: Path, version: int) -> None:
    schema = (Path(__file__).parent / "data" / f"schema_v{version}.sql").read_bytes()
    assert hashlib.sha256(schema).hexdigest() == _HISTORICAL_HASHES[version]
    assert _COLUMN.encode() not in schema and _INDEX.encode() not in schema
    with closing(sqlite3.connect(path)) as connection:
        _ = connection.execute("PRAGMA foreign_keys = ON")
        _ = connection.executescript(schema.decode())
        _ = connection.executemany(
            "INSERT INTO schema_metadata(key,value) VALUES (?,?)", _old_metadata(version).items()
        )
        connection.commit()
        assert len(connection.execute("PRAGMA table_info(work_items)").fetchall()) == 15


def _expected_key(kind: tuple[str, ...], payload: JsonValue) -> str | None:
    identity = acquisition_resource_identity(kind, payload)
    return None if identity is None else json.dumps(identity, sort_keys=True, separators=(",", ":"))


def _seed_identity_history(path: Path, *, modern: bool = True) -> dict[str, str | None]:
    expected: dict[str, str | None] = {}
    states = (
        (
            "pending",
            "leased",
            "suspended",
            "completed",
            "terminal_failure",
            "cancelled",
            "cancelling",
        )
        if modern
        else ("pending", "leased", "completed", "terminal_failure")
    )
    with closing(sqlite3.connect(path)) as connection:
        _ = connection.execute("PRAGMA foreign_keys = ON")

        def insert(identifier: str, kind: tuple[str, ...], payload: JsonValue, state: str) -> None:
            actual_state = "leased" if state == "suspended" else state
            held = actual_state in {"leased", "cancelling"}
            _ = connection.execute(
                """
                INSERT INTO work_items(
                    id,kind_parts_json,payload_schema_version,payload_json,
                    deduplication_identity_json,state,priority,eligible_at_utc_ns,
                    created_at_utc_ns,lease_token,lease_owner,lease_expires_at_utc_ns,
                    attempt,result_json,error_json
                ) VALUES (?,?,1,?,?,?,0,0,?,?,?,?,1,?,?)
                """,
                (
                    identifier,
                    encode_json(list(kind)),
                    encode_json(payload),
                    encode_json([identifier]),
                    actual_state,
                    NOW - 100,
                    f"token-{identifier}" if held else None,
                    "old-owner" if held else None,
                    NOW - 1 if state == "suspended" else NOW + 1_000_000_000 if held else None,
                    encode_json({"retained": identifier}),
                    encode_json({"old_failure": True}) if state == "terminal_failure" else None,
                ),
            )
            expected[identifier] = _expected_key(kind, payload)
            if state == "suspended":
                definition = StageDefinition(
                    identifier=f"stage-{identifier}",
                    work_item_identifier=identifier,
                    operation_identifier=f"operation-{identifier}",
                    kind=("test", "suspended_acquisition"),
                    payload={"checkpoint": "retained"},
                    resources=(),
                )
                _ = connection.execute(
                    "INSERT INTO stage_attempts VALUES (?,?,1,'active',?)",
                    (definition.operation_identifier, identifier, NOW - 100),
                )
                _ = connection.execute(
                    """INSERT INTO work_stages(
                        id,work_item_id,operation_id,definition_json,state,
                        execution,eligible_at_utc_ns,created_at_utc_ns
                    ) VALUES (?,?,?,?,'pending',0,0,?)""",
                    (
                        definition.identifier,
                        identifier,
                        definition.operation_identifier,
                        definition.model_dump_json(),
                        NOW - 100,
                    ),
                )

        for index, (kind, payload) in enumerate(_CASES):
            for state in states:
                insert(f"case-{index}-{state}", kind, payload, state)
            insert(f"malformed-{index}", kind, {}, "pending")
        insert(
            "explicit-malformed-stack",
            _CASES[0][0],
            {"request": {"item_identifier": "123", "stack_identifier": None}},
            "completed",
        )
        # Same raw plan with an explicit default must remain a distinct key.
        insert(
            "explicit-plan-default",
            _CASES[4][0],
            {
                "listing_id": "123456789",
                "request_plan": {"url": "https://example.test/item", "follow_redirects": False},
            },
            "completed",
        )
        insert("unsupported", ("other", "kind"), {"sentinel": "unrelated payload"}, "pending")
        # Cross multiple backfill pages, including terminal history only.
        for index in range(270):
            insert(
                f"history-{index:04}",
                _CASES[2][0],
                {"url": f"{_EBAY_IMAGE}&history={index}"},
                "completed",
            )
        connection.commit()
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    return expected


def _assert_migrated(path: Path, expected: dict[str, str | None]) -> None:
    with closing(sqlite3.connect(path)) as connection:
        assert (
            dict(connection.execute("SELECT key,value FROM schema_metadata"))
            == Database._expected_metadata()
        )
        assert sqlite.DATABASE_SCHEMA.version == 21
        actual = dict(connection.execute(f"SELECT id,{_COLUMN} FROM work_items"))
        assert {identifier: actual[identifier] for identifier in expected} == expected
        assert expected["case-4-completed"] != expected["explicit-plan-default"]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        columns = tuple(
            row[2] for row in connection.execute(f"PRAGMA index_xinfo('{_INDEX}')") if row[5]
        )
        assert columns == ("kind_parts_json", _COLUMN, "lease_expires_at_utc_ns", "id")
        assert (
            next(
                row[4]
                for row in connection.execute("PRAGMA index_list(work_items)")
                if row[1] == _INDEX
            )
            == 1
        )
        indexed = {
            row[0]
            for row in connection.execute(
                f"SELECT id FROM work_items INDEXED BY {_INDEX} "
                + f"WHERE {_COLUMN} IS NOT NULL AND state IN ('leased','cancelling')"
            )
        }
        wanted = {
            row[0]
            for row in connection.execute(
                "SELECT id FROM work_items WHERE state IN ('leased','cancelling')"
            )
            if actual[row[0]] is not None
        }
        assert indexed == wanted


async def test_v18_backfills_every_state_and_preserves_all_existing_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "v18.sqlite3"
    _create_legacy(path, 18)
    resident_stage, stateless_stage = await _seed_live_ownership(path)
    expected = _seed_identity_history(path)
    before = _rows(path)
    async with Database.managed(path) as database:
        _assert_migrated(path, expected)
        assert _rows(path) == before
        assert await database.validate_stage(resident_stage, now_utc_ns=NOW)
        assert await database.validate_stage(stateless_stage, now_utc_ns=NOW)

    async def no_repeat(_connection: object) -> None:
        raise AssertionError("Current-version reopen repeated historical backfill")

    monkeypatch.setattr(
        Database, "_migrate_acquisition_resource_identities", staticmethod(no_repeat)
    )
    async with Database.managed(path):
        _assert_migrated(path, expected)
    assert _rows(path) == before


async def test_genuine_v12_table_width_upgrades_before_new_index_and_backfill(
    tmp_path: Path,
) -> None:
    path = tmp_path / "v12.sqlite3"
    _create_legacy(path, 12)
    _ = await _seed_live_ownership(path)
    expected = _seed_identity_history(path, modern=False)
    before = _rows(path)
    async with Database.managed(path):
        _assert_migrated(path, expected)
    after = _rows(path)
    assert {name: after[name] for name in before} == before


async def test_failure_mid_backfill_rolls_back_column_index_rows_and_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "rollback.sqlite3"
    _create_legacy(path, 18)
    expected = _seed_identity_history(path)
    before = _rows(path)
    with closing(sqlite3.connect(path)) as connection:
        schema = connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall()
    original = acquisition_resource_identity_json
    calls = 0

    def fail_midway(kind: tuple[str, ...], payload: JsonValue) -> str | None:
        nonlocal calls
        calls += 1
        if calls == 5:
            raise RuntimeError("injected acquisition backfill failure")
        return original(kind, payload)

    with monkeypatch.context() as patch:
        patch.setattr(sqlite, "acquisition_resource_identity_json", fail_midway)
        with pytest.raises(RuntimeError, match="injected acquisition backfill failure"):
            async with Database.managed(path):
                pytest.fail("Partial backfill was accepted")
    assert calls == 5
    assert _rows(path) == before
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall() == schema
        assert dict(connection.execute("SELECT key,value FROM schema_metadata")) == _old_metadata(
            18
        )
    async with Database.managed(path):
        _assert_migrated(path, expected)


@pytest.mark.parametrize("change_after_preflight", (False, True))
async def test_unknown_hash_is_rejected_before_any_column_or_index_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change_after_preflight: bool
) -> None:
    path = tmp_path / "unknown.sqlite3"
    _create_legacy(path, 18)
    _ = _seed_identity_history(path)

    def corrupt_metadata() -> None:
        with closing(sqlite3.connect(path)) as connection:
            _ = connection.execute(
                "UPDATE schema_metadata SET value=? WHERE key='schema_definition_sha256'",
                ("0" * 64,),
            )
            connection.commit()

    if change_after_preflight:
        preflight = Database._preflight_path

        async def change_metadata(_cls: type[Database], file: Path, *, allow_empty: bool) -> None:
            await preflight(file, allow_empty=allow_empty)
            corrupt_metadata()

        monkeypatch.setattr(Database, "_preflight_path", classmethod(change_metadata))
    else:
        corrupt_metadata()
    before = _rows(path)
    with closing(sqlite3.connect(path)) as connection:
        schema = connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall()
    with pytest.raises(RuntimeError, match="Unsupported database schema metadata"):
        async with Database.managed(path):
            pytest.fail("Unknown schema hash was accepted")
    assert _rows(path) == before
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM sqlite_schema ORDER BY name").fetchall() == schema
        assert (
            dict(connection.execute("SELECT key,value FROM schema_metadata"))[
                "schema_definition_sha256"
            ]
            == "0" * 64
        )


async def test_terminal_manual_retry_retains_key_and_existing_exclusion(tmp_path: Path) -> None:
    path = tmp_path / "retry.sqlite3"
    _create_legacy(path, 18)
    _ = await _seed_live_ownership(path)
    expected = _seed_identity_history(path)
    identifier = "case-2-terminal_failure"
    with closing(sqlite3.connect(path)) as connection:
        _ = connection.execute(
            "INSERT INTO work_operations VALUES ('retained-evidence',?,1,'old-token','old-owner')",
            (identifier,),
        )
        connection.commit()
    async with Database.managed(path) as database:
        await database.retry_terminal_work_from_operation(
            work_item_identifier=identifier,
            checkpoint_operation_identifier="retained-evidence",
            retried_at_utc_ns=NOW,
            event_identifier="manual-retry",
            reason={"manual": True},
        )
        claim = await database.claim_work(
            supported_capabilities=(WorkCapability(kind=_CASES[2][0], payload_schema_version=1),),
            worker_identifier="new-worker",
            lease_token="new-token",
            lease_duration_ns=1_000_000_000,
            utc_now_ns=lambda: NOW,
            event_identifier="claim-retry",
            eligible_identifiers=(identifier,),
        )
        assert claim.lease is None
        assert claim.next_eligible_at_utc_ns is not None
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute(
            f"SELECT state,{_COLUMN} FROM work_items WHERE id=?", (identifier,)
        ).fetchone() == ("pending", expected[identifier])


async def test_fresh_and_reopened_current_schema_has_key_without_backfill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def unexpected_backfill(_connection: object) -> None:
        raise AssertionError("Fresh/current database invoked historical backfill")

    monkeypatch.setattr(
        Database, "_migrate_acquisition_resource_identities", staticmethod(unexpected_backfill)
    )
    path = tmp_path / "fresh.sqlite3"
    for initialize in (True, False):
        async with Database.managed(path, initialize=initialize):
            with closing(sqlite3.connect(path)) as connection:
                assert _COLUMN in {
                    row[1] for row in connection.execute("PRAGMA table_info(work_items)")
                }
                assert connection.execute(
                    "SELECT name FROM sqlite_schema WHERE name=?", (_INDEX,)
                ).fetchone() == (_INDEX,)
