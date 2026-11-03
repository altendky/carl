"""Public scheduler diagnostics stay bounded, private, and visibly qualified."""

# pyright: reportPrivateUsage=false

import json
from contextlib import AsyncExitStack
from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest
from mcp import Client
from mcp.types import CallToolResult
from rich.console import Console

from carl._tests.mcp_response_helpers import tool_response_page
from carl._tests.test_pipeline_search_occurrences import _attempt
from carl.core.activity import (
    ActivitySnapshot,
    BoundedWorkActivity,
    NetworkActivityCounts,
    WorkKindActivity,
)
from carl.core.json import encode_json
from carl.core.scheduler_activity import (
    SchedulerActivity,
    SchedulerCoverage,
    SchedulerResourceActivity,
    SchedulerStageCounts,
    SchedulerTimingSummary,
    SchedulerWorkCounts,
    SchedulerWorkStatus,
)
from carl.core.stages import StageWaitKind
from carl.core.work import (
    NetworkActivityDefinition,
    SchedulingScope,
    SchedulingScopeKind,
    WorkState,
)
from carl.io.activity import activity_dashboard
from carl.io.mcp_response_budget import (
    HARD_RESPONSE_BYTES,
    TARGET_RESPONSE_BYTES,
    response_bytes,
    response_json,
)
from carl.io.processes import database_process_snapshot
from carl.io.sqlite import Database
from carl.mcp_server import build_server
from carl.review import ReviewApplication

_SECRET = "PRIVATE_HEADER_TOKEN_DO_NOT_PUBLISH"


def _procfs(directory: Path, database: Path, count: int = 10) -> Path:
    root = directory / "proc"
    for index in range(count):
        process = root / str(100 + index)
        (process / "fd").mkdir(parents=True)
        (process / "fd" / "7").symlink_to(database)
        (process / "cwd").symlink_to(directory, target_is_directory=True)
        (process / "cmdline").write_bytes(
            b"/usr/bin/python3\0/private/path/carl\0work\0--database\0"
            + bytes(database)
            + b"\0--private-option\0"
            + _SECRET.encode()
            + b"\0"
        )
    return root


def test_process_snapshot_bounds_peers_without_exposing_argv_or_cwd(tmp_path: Path) -> None:
    database = tmp_path / "carl.sqlite3"
    database.touch()
    snapshot = database_process_snapshot(
        database,
        proc_root=_procfs(tmp_path, database),
        current_process_identifier=109,
    )
    assert snapshot.total_count == 10 and len(snapshot.processes) == 8
    assert snapshot.processes[0].process_identifier == 109
    assert all(process.command == "python3 (carl work)" for process in snapshot.processes)
    assert all(process.working_directory is None for process in snapshot.processes)
    assert _SECRET not in snapshot.model_dump_json()
    assert str(tmp_path) not in snapshot.model_dump_json()
    assert database_process_snapshot(database, proc_root=tmp_path / "absent").total_count == 0
    (tmp_path / "proc" / "100" / "cmdline").write_bytes(
        b"carl\0work\0--database\0" + b"x" * 100_000
    )
    assert database_process_snapshot(database, proc_root=tmp_path / "proc").total_count == 10
    (tmp_path / "proc" / "100" / "cmdline").write_bytes(b"\0" + _SECRET.encode() + b"\0")
    assert (
        _SECRET
        not in database_process_snapshot(database, proc_root=tmp_path / "proc").model_dump_json()
    )


def test_dashboard_distinguishes_logical_waits_physical_holds_and_unknown_timing() -> None:
    snapshot = ActivitySnapshot(
        captured_at_utc_ns=10_000_000_000,
        recent_window_ns=5_000_000_000,
        work_kinds=(
            WorkKindActivity(
                kind=("test",),
                pending=0,
                leased=1,
                completed=0,
                terminal_failure=0,
                recent_completed=0,
                recent_terminal_failure=0,
            ),
        ),
        active_work=(
            BoundedWorkActivity(
                identifier="waiting-work",
                kind=("test",),
                state=WorkState.LEASED,
                attempt=1,
                created_at_utc_ns=0,
                eligible_at_utc_ns=0,
                lease_expires_at_utc_ns=1,
                worker_identifier="worker",
                subject="[red]legitimate query[/red]",
                stage="waiting",
                error_kind=None,
            ),
        ),
        recent_terminal_work=(),
        network=NetworkActivityCounts(
            pending=0,
            admitted=0,
            completed=0,
            skipped=0,
            failed=0,
            cancelled=0,
            recent_completed=0,
            recent_skipped=0,
            recent_failed=0,
            recent_cancelled=0,
        ),
        network_paths=(),
        active_network=(),
        process_omitted_count=2,
        scheduler=SchedulerActivity(
            captured_at_utc_ns=10_000_000_000,
            scope_handle="scope-opaque",
            scope_configured=True,
            work_counts=SchedulerWorkCounts(leased=1),
            resources=(
                SchedulerResourceActivity(
                    name="cpu", capacity=4, held_units=3, expired_held_units=1
                ),
                SchedulerResourceActivity(name="analysis", capacity=None, held_units=0),
            ),
            stage_counts=SchedulerStageCounts(pending=1, progress_waiting=1),
            samples=(
                SchedulerWorkStatus(
                    work_handle="work-opaque",
                    work_identifier="waiting-work",
                    logical_state=WorkState.LEASED,
                    stage_state="pending",
                    recorded_wait_kind=StageWaitKind.DEPENDENCY,
                    recorded_observation_reason="resource",
                    recorded_observation_at_utc_ns=5_000_000_000,
                    progress_state="armed",
                    observed_blockers=("dependency",),
                    observed_at_utc_ns=10_000_000_000,
                    checks_complete=True,
                ),
            ),
            timings=(SchedulerTimingSummary(metric="physical_hold"),),
            coverage=SchedulerCoverage(
                method="current_metadata", examined_rows=8, row_limit=8, truncated=True
            ),
        ),
    )
    console = Console(record=True, width=240, color_system=None)
    console.print(activity_dashboard(snapshot, Path("carl.sqlite3")))
    rendered = console.export_text()
    assert "waiting dependency" in rendered and "stale" not in rendered
    assert "Expired, still held" in rendered
    assert "held units remain charged during cleanup" in rendered
    assert "[red]legitimate query[/red]" in rendered
    assert "unavailable" in rendered and "unknown" in rendered
    assert "2 more omitted" in rendered and "(truncated)" in rendered


@pytest.mark.anyio
async def test_work_status_base_and_scheduler_share_snapshot_during_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        source = await _attempt(database, "snapshot-work", "facebook")
        read_work = Database.work
        changed = False
        async with Database.managed(path) as concurrent:

            async def work(target: Database, identifier: str):
                nonlocal changed
                value = await read_work(target, identifier)
                if target is database and not changed:
                    changed = True
                    async with concurrent._connections.writer() as connection:
                        await connection.execute(
                            "UPDATE work_items SET state='completed',lease_token=NULL,lease_owner=NULL,lease_expires_at_utc_ns=NULL WHERE id=?",
                            (identifier,),
                        )
                return value

            monkeypatch.setattr(Database, "work", work)
            status = await ReviewApplication(database, tmp_path).get_work_status(
                source.work_item_identifier
            )
        assert changed and status.state is WorkState.LEASED
        assert status.scheduler is not None
        assert status.scheduler.logical_state is status.state
        assert status.scheduler.observed_at_utc_ns == status.runtime.observed_at_utc_ns
        assert (await read_work(database, source.work_item_identifier))["state"] == "completed"


@pytest.mark.anyio
@pytest.mark.parametrize("protocol", (False, True), ids=("direct-sdk", "mcp-client"))
@pytest.mark.parametrize("stress", (False, True), ids=("default", "large-groups"))
@pytest.mark.parametrize("tool_name", ("get_work_status", "get_activity_snapshot"))
async def test_default_diagnostics_wire_budget_and_secret_sentinels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, protocol: bool, stress: bool, tool_name: str
) -> None:
    path = tmp_path / "carl.sqlite3"
    proc_root = _procfs(tmp_path, path)

    def processes(database: Path, source: str | None = None):
        return database_process_snapshot(
            database, source, proc_root=proc_root, current_process_identifier=109
        )

    monkeypatch.setattr("carl.review.database_process_snapshot", processes)
    async with Database.managed(path, initialize=True) as database:
        source = await _attempt(
            database,
            "private-work",
            "facebook",
            payload={
                "request": {
                    "query": "legitimate eyepiece query",
                    "headers": {"Authorization": _SECRET},
                },
                "private_config": _SECRET,
            },
        )
        async with database._connections.writer() as connection:
            await connection.execute(
                "UPDATE work_items SET result_json=?,error_json=?,lease_token=? WHERE id=?",
                (
                    encode_json({"stage": "collecting_search", "raw": _SECRET}),
                    encode_json({"kind": "http_transport_failure", "message": _SECRET}),
                    _SECRET,
                    source.work_item_identifier,
                ),
            )
            await connection.execute(
                "INSERT INTO network_connectivity(singleton,paused,next_probe_utc_ns,checked_at_utc_ns,result_json) VALUES(1,1,0,?,?)",
                (
                    time_ns(),
                    encode_json(
                        {
                            "reachable": False,
                            "checks": [{"host": "test", "reachable": False}],
                            "raw_headers": {"Authorization": _SECRET},
                        }
                    ),
                ),
            )
        if stress:
            for index in range(40):
                _ = await _attempt(
                    database,
                    f"long-work-{index}-" + "w" * 1000,
                    "facebook",
                    work_kind=("custom", "kind", f"group-{index}-" + "界" * 1000),
                )
            for index in range(20):
                await database.create_network_activity(
                    NetworkActivityDefinition(
                        identifier=f"network-{index}-" + "n" * 1000,
                        kind=("custom", "network"),
                        operation_identifier=source.operation_identifier,
                        network_session_identifier="session-" + "s" * 1000,
                        ordinal=index + 1,
                        scopes=(
                            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                            SchedulingScope(
                                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                                identity=("custom", "network"),
                            ),
                            SchedulingScope(
                                kind=SchedulingScopeKind.NETWORK_PATH,
                                identity=("custom", f"route-{index}-" + "r" * 1000),
                            ),
                        ),
                    ),
                    created_at_utc_ns=time_ns(),
                    event_identifier=str(uuid4()),
                    sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
                )
        server = build_server(ReviewApplication(database, tmp_path))
        async with AsyncExitStack() as stack:
            endpoint = await stack.enter_async_context(Client(server)) if protocol else server
            for name, arguments in (
                (
                    tool_name,
                    {"work_identifier": source.work_item_identifier}
                    if tool_name == "get_work_status"
                    else {"maximum_rows": 100}
                    if stress
                    else {},
                ),
            ):
                result = await endpoint.call_tool(name, arguments)
                assert isinstance(result, CallToolResult) and not result.is_error
                fragments: list[str] = []
                while True:
                    assert response_bytes(result) <= TARGET_RESPONSE_BYTES < HARD_RESPONSE_BYTES
                    assert _SECRET not in response_json(result)
                    page = tool_response_page(result)
                    if page is None:
                        value = result.structured_content
                        break
                    assert page.payload_format == "structured_json"
                    fragments.append(page.json_fragment)
                    if not page.has_more:
                        value = json.loads("".join(fragments))
                        acknowledgement = await endpoint.call_tool(
                            "get_tool_response_page", {"cursor": page.cursor, "acknowledge": True}
                        )
                        assert isinstance(acknowledgement, CallToolResult)
                        assert not acknowledgement.is_error
                        assert response_bytes(acknowledgement) <= TARGET_RESPONSE_BYTES
                        assert _SECRET not in response_json(acknowledgement)
                        break
                    result = await endpoint.call_tool(
                        "get_tool_response_page", {"cursor": page.next_cursor}
                    )
                    assert isinstance(result, CallToolResult) and not result.is_error
                assert isinstance(value, dict) and value["scheduler"] is not None
                assert _SECRET not in json.dumps(value)
                if name == "get_activity_snapshot":
                    assert len(value["database_processes"]) == 8
                    assert value["process_omitted_count"] == 2
                    assert len(value["work_kinds"]) <= 32 and len(value["network_paths"]) <= 16
                    assert any(
                        row["subject"] == "legitimate eyepiece query"
                        for row in value["active_work"]
                    )
                    if stress:
                        assert value["work_kind_groups_truncated"]
                        assert value["network_path_groups_truncated"]
                        assert any(row["path"][0] == "custom" for row in value["network_paths"])
                if not stress:
                    assert not fragments, "Ordinary default diagnostics should fit one response"
