"""Durable activity snapshots and their terminal dashboard."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from rich.console import Console

from carl.core.components import Component, ComponentId
from carl.core.models import CodeProvenance
from carl.core.work import (
    NetworkActivityDefinition,
    NetworkActivityState,
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.io.activity import activity_dashboard
from carl.io.processes import database_process_activity
from carl.io.sqlite import Database


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="dirty",
        package_version="test",
        python_implementation="test",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


def _work(identifier: str, kind: tuple[str, ...], payload: dict[str, object]) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=kind,
        payload_schema_version=1,
        payload=payload,
        deduplication_identity=(identifier,),
        not_before_utc_ns=0,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
        ),
    )


def _requester(identifier: str) -> WorkRequester:
    return WorkRequester(
        request_identifier=f"request-{identifier}",
        kind=("carl", "test", "requester"),
        identifier="source",
        context={},
    )


def test_database_process_activity_reports_current_and_peer_processes(tmp_path: Path) -> None:
    database = tmp_path / "carl.sqlite3"
    database.touch()
    proc_root = tmp_path / "proc"
    for process_identifier, command, target in (
        (101, "carl", database),
        (202, "python", Path(f"{database}-wal")),
    ):
        process = proc_root / str(process_identifier)
        descriptors = process / "fd"
        descriptors.mkdir(parents=True)
        (process / "comm").write_text(f"{command}\n")
        (process / "cwd").symlink_to(tmp_path, target_is_directory=True)
        (descriptors / "7").symlink_to(target)

    command_process = proc_root / "303"
    (command_process / "fd").mkdir(parents=True)
    (command_process / "cmdline").write_bytes(
        b"/usr/bin/python\0/home/test/.venv/bin/carl\0mcp\0--database\0" + bytes(database) + b"\0"
    )
    (command_process / "cwd").symlink_to(tmp_path, target_is_directory=True)

    processes = database_process_activity(
        database,
        "a" * 64,
        proc_root=proc_root,
        current_process_identifier=101,
    )

    assert tuple(process.process_identifier for process in processes) == (101, 202, 303)
    assert processes[0].current_process
    assert processes[0].source_tree_sha256 == "a" * 64
    assert processes[0].database_files == ("database",)
    assert not processes[1].current_process
    assert processes[1].source_tree_sha256 is None
    assert processes[1].database_files == ("wal",)
    assert processes[2].command.endswith(f"carl mcp --database {database}")
    assert processes[2].database_files == ()


@pytest.mark.anyio
async def test_activity_snapshot_includes_work_and_network_path_usage(tmp_path: Path) -> None:
    search_kind = ("carl", "facebook", "work", "collect_search")
    item_kind = ("carl", "facebook", "work", "collect_item")
    network_kind = ("carl", "facebook", "network_activity", "search_page")
    proton_path = ("proton", "personal", "carl")
    decodo_path = ("decodo", "personal", "carl")
    mullvad_path = ("mullvad", "personal", "carl")
    database_path = tmp_path / "carl.sqlite3"
    async with Database.managed(database_path, initialize=True) as database:
        await database.enqueue_work(
            _work("search-work", search_kind, {"request": {"query": "telescope"}}),
            _requester("search"),
            event_identifier="search-enqueued",
            enqueued_at_utc_ns=10,
        )
        await database.enqueue_work(
            _work("item-work", item_kind, {"listing_id": "123"}),
            _requester("item"),
            event_identifier="item-enqueued",
            enqueued_at_utc_ns=10,
        )
        claimed = await database.claim_work(
            supported_capabilities=(WorkCapability(kind=search_kind, payload_schema_version=1),),
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=1_000,
            utc_now_ns=lambda: 20,
            event_identifier="search-claimed",
        )
        assert claimed.lease is not None
        await database.begin_operation(
            operation_id="network-operation",
            component=Component(ComponentId(("carl", "test", "network")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.create_network_activity(
            NetworkActivityDefinition(
                identifier="network-activity",
                kind=network_kind,
                operation_identifier="network-operation",
                network_session_identifier="network-session",
                ordinal=1,
                scopes=(
                    SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                    SchedulingScope(
                        kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                        identity=network_kind,
                    ),
                    SchedulingScope(
                        kind=SchedulingScopeKind.NETWORK_PATH,
                        identity=proton_path,
                    ),
                ),
            ),
            created_at_utc_ns=20,
            event_identifier="network-created",
            sample_uniform_holdoff_ns=lambda minimum, _maximum: minimum,
        )
        admitted = await database.try_admit_network_activity(
            network_activity_identifier="network-activity",
            admission_token="network-token",
            permit_duration_ns=1_000,
            now_utc_ns=20,
            event_identifier="network-admitted",
        )
        assert admitted.admission is not None
        await database.create_network_activity(
            NetworkActivityDefinition(
                identifier="historical-network-activity",
                kind=network_kind,
                operation_identifier="network-operation",
                network_session_identifier="historical-network-session",
                ordinal=2,
                scopes=(
                    SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                    SchedulingScope(
                        kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                        identity=network_kind,
                    ),
                    SchedulingScope(
                        kind=SchedulingScopeKind.NETWORK_PATH,
                        identity=mullvad_path,
                    ),
                ),
            ),
            created_at_utc_ns=20,
            event_identifier="historical-network-created",
            sample_uniform_holdoff_ns=lambda minimum, _maximum: minimum,
        )
        historical_admission = await database.try_admit_network_activity(
            network_activity_identifier="historical-network-activity",
            admission_token="historical-network-token",
            permit_duration_ns=1_000,
            now_utc_ns=20,
            event_identifier="historical-network-admitted",
        )
        assert historical_admission.admission is not None
        await database.finish_network_activity(
            network_activity_identifier="historical-network-activity",
            admission_token="historical-network-token",
            state=NetworkActivityState.COMPLETED,
            ended_at_utc_ns=30,
            result={"kind": "request_attempt_completed"},
            event_identifier="historical-network-completed",
        )
        await database.create_network_activity(
            NetworkActivityDefinition(
                identifier="held-network-activity",
                kind=("carl", "facebook", "network_activity", "item_page"),
                operation_identifier="network-operation",
                network_session_identifier="decodo-session",
                ordinal=2,
                not_before_utc_ns=3_000,
                scopes=(
                    SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                    SchedulingScope(
                        kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                        identity=("carl", "facebook", "network_activity", "item_page"),
                    ),
                    SchedulingScope(
                        kind=SchedulingScopeKind.NETWORK_PATH,
                        identity=decodo_path,
                    ),
                ),
            ),
            created_at_utc_ns=20,
            event_identifier="held-network-created",
            sample_uniform_holdoff_ns=lambda minimum, _maximum: minimum,
        )

        snapshot = await database.activity_snapshot(
            captured_at_utc_ns=2_000,
            recent_window_ns=1_000,
            maximum_rows=10,
        )

    by_kind = {activity.kind: activity for activity in snapshot.work_kinds}
    assert by_kind[search_kind].leased == 1
    assert by_kind[item_kind].pending == 1
    assert snapshot.active_work[0].subject == "telescope"
    assert snapshot.active_work[1].subject == "listing 123"
    assert snapshot.network.admitted == 1
    by_path = {activity.path: activity for activity in snapshot.network_paths}
    assert by_path[proton_path].admitted == 1
    assert by_path[decodo_path].pending == 1
    assert by_path[mullvad_path].completed == 1
    assert by_path[mullvad_path].recent_completed == 0
    assert snapshot.active_network[0].path == proton_path
    assert snapshot.active_network[1].path == decodo_path

    console = Console(record=True, width=180, color_system=None)
    console.print(activity_dashboard(snapshot, database_path))
    rendered = console.export_text()
    assert "Network-layer usage" in rendered
    assert '["proton","personal","carl"]' in rendered
    assert '["decodo","personal","carl"]' in rendered
    assert '["mullvad","personal","carl"]' in rendered
    assert "held" in rendered
    assert "telescope" in rendered
    assert "worker" in rendered
