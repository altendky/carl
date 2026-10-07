import json
import sqlite3
from pathlib import Path

import pytest

from carl.cli import _default_database, _print_work_result, collect, search
from carl.core.facebook_work import COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION


def test_default_database_uses_platform_user_data_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))

    assert _default_database() == tmp_path / "carl" / "carl.sqlite3"


def test_item_collection_rejects_noncanonical_url_before_database_creation(
    tmp_path: Path,
) -> None:
    database = tmp_path / "must-not-exist.sqlite3"

    with pytest.raises(ValueError, match="canonical HTTPS Facebook Marketplace item URL"):
        collect(
            "https://example.com/item/123/",
            database=database,
        )

    assert not database.exists()


def test_failed_work_result_prints_then_exits_nonzero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        _print_work_result({"state": "terminal_failure", "error": {"kind": "expected"}})

    assert raised.value.code == 1
    assert '"state": "terminal_failure"' in capsys.readouterr().out


def test_completed_work_result_prints_without_exiting(capsys: pytest.CaptureFixture[str]) -> None:
    _print_work_result({"state": "completed"})

    assert '"state": "completed"' in capsys.readouterr().out


def test_search_queues_without_running_an_inline_worker(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "carl.sqlite3"

    search(
        "telescope",
        facebook_location="456",
        radius=60,
        maximum_pages=1,
        database=database,
    )

    result = json.loads(capsys.readouterr().out)
    assert result["created"]
    assert result["state"] == "pending"
    with sqlite3.connect(database) as connection:
        state, attempt, schema_version, payload_json = connection.execute(
            "SELECT state, attempt, payload_schema_version, payload_json FROM work_items WHERE id = ?",
            (result["work_identifier"],),
        ).fetchone()
    assert (state, attempt, schema_version) == (
        "pending",
        0,
        COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    )
    assert json.loads(payload_json)["routing"] == ["decodo", "personal", "datacenter"]


def test_search_explicit_legacy_proton_option_requests_actual_proton(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "carl.sqlite3"
    search(
        "telescope",
        facebook_location="456",
        radius=60,
        maximum_pages=1,
        proton_route="dedicated-proton",
        database=database,
    )
    result = json.loads(capsys.readouterr().out)
    with sqlite3.connect(database) as connection:
        (payload_json,) = connection.execute(
            "SELECT payload_json FROM work_items WHERE id = ?", (result["work_identifier"],)
        ).fetchone()
    assert json.loads(payload_json)["routing"] == ["proton", "personal", "dedicated-proton"]
