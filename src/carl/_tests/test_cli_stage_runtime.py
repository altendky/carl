"""Foreground commands own a scoped common runtime instead of claiming legacy work."""

# The injected runtime publishes terminal rows without invoking marketplace providers.
# pyright: reportPrivateUsage=false

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

import anyio
import pytest

import carl.cli as cli
from carl.core.ebay import COLLECT_EBAY_SEARCH_WORK_KIND
from carl.core.facebook_work import COLLECT_ITEM_WORK_KIND, EXTRACT_ITEM_WORK_KIND
from carl.core.models import JsonValue
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkDefinition,
    WorkRequester,
    WorkState,
)
from carl.io.claude import ClaudeCli
from carl.io.sqlite import Database


async def _enqueue(database: Database, identifier: str) -> None:
    await database.enqueue_work(
        WorkDefinition(
            identifier=identifier,
            kind=("fixture", "unrelated"),
            payload_schema_version=1,
            payload={},
            deduplication_identity=(identifier,),
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(
                    kind=SchedulingScopeKind.WORK_KIND, identity=("fixture", "unrelated")
                ),
            ),
        ),
        WorkRequester(
            request_identifier=f"request-{identifier}",
            kind=("fixture", "request"),
            identifier=identifier,
            context={},
        ),
        event_identifier=f"event-{identifier}",
        enqueued_at_utc_ns=1,
    )


class _SelectedWork:
    def __init__(self, identifiers: tuple[str, ...]) -> None:
        self.states = dict.fromkeys(identifiers, WorkState.PENDING)

    async def work_state(self, identifier: str) -> WorkState:
        return self.states[identifier]

    async def work(self, identifier: str) -> dict[str, JsonValue]:
        return {"identifier": identifier, "state": self.states[identifier].value}


@pytest.mark.anyio
async def test_foreground_scope_is_bounded_exact_and_preserves_analysis_options(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    identifiers = tuple(f"selected-{index}" for index in range(1001))
    selected = _SelectedWork(identifiers)
    calls: list[tuple[str, ...]] = []
    client = ClaudeCli("explicit-claude")

    @asynccontextmanager
    async def pool(
        database: Database,
        repository_root: Path,
        *,
        eligible_identifiers: tuple[str, ...] | None = None,
        claude: ClaudeCli | None = None,
        worker_count: int | None = None,
    ) -> AsyncGenerator[None]:
        assert database is selected and repository_root.is_absolute()
        assert claude is client and worker_count == 3
        assert eligible_identifiers is not None
        calls.append(eligible_identifiers)
        for identifier in eligible_identifiers:
            selected.states[identifier] = WorkState.COMPLETED
        yield

    monkeypatch.setattr(cli, "managed_worker_pool", pool)
    results = await cli._run_foreground_work(
        cast(Database, cast(object, selected)),
        (*identifiers, identifiers[0]),
        worker_count=3,
        claude=client,
        progress_label="selected",
    )
    assert tuple(map(len, calls)) == (1000, 1)
    assert tuple(identifier for batch in calls for identifier in batch) == identifiers
    assert [result["identifier"] for result in results] == [*identifiers, identifiers[0]]
    assert "selected: 1000/1001 finished, 0 failed" in capsys.readouterr().err


@pytest.mark.anyio
async def test_foreground_cancellation_closes_owned_pool_and_empty_scope_starts_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = _SelectedWork(("selected",))
    selected.states["selected"] = WorkState.CANCELLING
    entered, closed = anyio.Event(), anyio.Event()

    @asynccontextmanager
    async def pool(
        _database: Database, _repository_root: Path, **_options: object
    ) -> AsyncGenerator[None]:
        entered.set()
        try:
            yield
        finally:
            closed.set()

    monkeypatch.setattr(cli, "managed_worker_pool", pool)
    assert await cli._run_foreground_work(cast(Database, cast(object, selected)), ()) == []
    assert not entered.is_set()

    async def drive() -> None:
        await cli._run_foreground_work(cast(Database, cast(object, selected)), ("selected",))

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(drive)
        await entered.wait()
        await anyio.lowlevel.checkpoint()
        assert not closed.is_set(), "Cancelling work still needs its runtime's cleanup"
        tasks.cancel_scope.cancel()
    assert closed.is_set()


def test_item_command_drives_only_collection_then_explicit_extraction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "carl.sqlite3"

    async def seed() -> None:
        async with Database.managed(path, initialize=True) as database:
            await _enqueue(database, "unrelated")

    anyio.run(seed, backend="trio")
    calls: list[tuple[str, ...]] = []

    @asynccontextmanager
    async def pool(
        database: Database,
        _repository_root: Path,
        *,
        eligible_identifiers: tuple[str, ...] | None = None,
        claude: ClaudeCli | None = None,
        worker_count: int | None = None,
    ) -> AsyncGenerator[None]:
        assert eligible_identifiers is not None and len(eligible_identifiers) == 1
        assert "unrelated" not in eligible_identifiers
        assert claude is None and worker_count == 1
        calls.append(eligible_identifiers)
        identifier = eligible_identifiers[0]
        work = await database.work(identifier)
        result: dict[str, JsonValue] = {}
        if work["kind"] == list(COLLECT_ITEM_WORK_KIND):
            await database.enqueue_work(
                WorkDefinition(
                    identifier="extraction",
                    kind=EXTRACT_ITEM_WORK_KIND,
                    payload_schema_version=1,
                    payload={"acquisition_record_identifier": "fixture-acquisition"},
                    deduplication_identity=("extraction",),
                    not_before_utc_ns=0,
                    scopes=(
                        SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                        SchedulingScope(
                            kind=SchedulingScopeKind.WORK_KIND, identity=EXTRACT_ITEM_WORK_KIND
                        ),
                    ),
                ),
                WorkRequester(
                    request_identifier="extract-request",
                    kind=("fixture", "request"),
                    identifier=identifier,
                    context={},
                ),
                event_identifier="extract-enqueued",
                enqueued_at_utc_ns=1,
            )
            result["extraction_work_identifier"] = "extraction"
        else:
            assert identifier == "extraction"
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET state='completed',result_json=? WHERE id=?",
                (json.dumps(result), identifier),
            )
        yield

    monkeypatch.setattr(cli, "managed_worker_pool", pool)
    cli.collect(
        "https://www.facebook.com/marketplace/item/123/",
        database=path,
        browser_like=False,
    )
    result = json.loads(capsys.readouterr().out)
    assert result["state"] == "completed"
    assert len(calls) == 2 and calls[1] == ("extraction",)

    async def verify() -> None:
        async with Database.managed(path) as database:
            assert await database.work_state("unrelated") is WorkState.PENDING

    anyio.run(verify, backend="trio")


@pytest.mark.parametrize("failed", (False, True))
def test_ebay_command_queues_search_before_scoped_foreground_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failed: bool,
) -> None:
    captured: list[str] = []

    @asynccontextmanager
    async def pool(
        database: Database,
        _repository_root: Path,
        *,
        eligible_identifiers: tuple[str, ...] | None = None,
        claude: ClaudeCli | None = None,
        worker_count: int | None = None,
    ) -> AsyncGenerator[None]:
        assert eligible_identifiers is not None and len(eligible_identifiers) == 1
        identifier = eligible_identifiers[0]
        captured.append(identifier)
        work = await database.work(identifier)
        assert work["kind"] == list(COLLECT_EBAY_SEARCH_WORK_KIND)
        payload = work["payload"]
        assert isinstance(payload, dict)
        assert payload["request"] == {
            "query": "telescope",
            "stack_identifier": "fixture-stack",
            "maximum_pages": 2,
            "listing_state": "active",
        }
        result = {"search_run_record_identifier": "retained-run", "pages": []}
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET state=?,result_json=? WHERE id=?",
                ("terminal_failure" if failed else "completed", json.dumps(result), identifier),
            )
        yield

    monkeypatch.setattr(cli, "managed_worker_pool", pool)
    if failed:
        with pytest.raises(SystemExit) as raised:
            cli.ebay_search(
                "telescope", stack="fixture-stack", maximum_pages=2, database=tmp_path / "carl.db"
            )
        assert raised.value.code == 1
    else:
        cli.ebay_search(
            "telescope", stack="fixture-stack", maximum_pages=2, database=tmp_path / "carl.db"
        )
    result = json.loads(capsys.readouterr().out)
    assert len(captured) == 1
    if failed:
        assert result["state"] == "terminal_failure"
    else:
        assert result == {"search_run_record_identifier": "retained-run", "pages": []}
