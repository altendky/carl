"""Durable analysis phases preserve captured evidence without relaunching a child."""

# Integration fixtures intentionally exercise private bounded stage boundaries.
# pyright: reportPrivateUsage=false

import json
import sys
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest

import carl.io.analysis_stage_workers as stages
from carl._tests.test_facebook_stateless_stages import _register, _run, _store
from carl._tests.test_item_analysis import _provenance
from carl.core.analysis_stages import MAX_ANALYSIS_STDOUT_BYTES, PreparedAnalysis
from carl.core.item_analysis import (
    ANALYZE_ITEM_WORK_KIND,
    AnalyzeItemPayload,
    analyze_item_work,
)
from carl.core.models import NamedInput, RecordDraft
from carl.core.stages import StageDefinition, WaitWork
from carl.core.work import WorkCapability
from carl.core.worker import CompletedWork, RetryWork, TerminalFailureWork
from carl.facebook_analysis_workers import (
    ANALYZE_FACEBOOK_ITEM,
    AnalysisWorkerDependencies,
    build_analysis_component_registry,
    facebook_analysis_outcome,
)
from carl.io.analysis_stage_workers import build_analysis_stage_registry
from carl.io.claude import ClaudeCapture, ClaudeCli, ClaudeRun
from carl.io.resource_permits import current_resource_manager
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, WorkStageAdapter
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _cli(root: Path, mode: str = "success") -> ClaudeCli:
    executable = root / "fake-analysis"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import json, sys, time\n"
        "from pathlib import Path\n"
        f"calls = Path({str(root / 'calls')!r})\n"
        "calls.write_text(calls.read_text() + 'launch\\n' if calls.exists() else 'launch\\n')\n"
        "assert json.loads(Path('listing.json').read_text()) == {'title': 'retained listing'}\n"
        "print(json.dumps({'type':'system','subtype':'init','claude_code_version':'test-v1'}), flush=True)\n"
        + ("time.sleep(10)\n" if mode == "timeout" else "")
        + (
            "print('fixture stderr',file=sys.stderr)\nraise SystemExit(7)\n"
            if mode == "failure"
            else ""
        )
        + "print(json.dumps({'result':'retained report','is_error':False}))\n"
    )
    executable.chmod(0o700)
    return ClaudeCli(str(executable))


async def _prepared(payload: AnalyzeItemPayload, database: Database) -> PreparedAnalysis:
    del database
    return PreparedAnalysis(
        listing_input=b'{"title": "retained listing"}',
        prompt=b"Identify exact saved evidence.",
        images=(),
        inputs=(
            NamedInput(
                name=("listing_analysis_evidence",),
                object_identifier=payload.evidence_set_record_identifier,
            ),
            NamedInput(
                name=("product_guide",), object_identifier=payload.product_guide_record_identifier
            ),
        ),
    )


def _registry(database: Database, cli: ClaudeCli) -> StageHandlerRegistry:
    return build_analysis_stage_registry(
        dependencies=AnalysisWorkerDependencies(database, cli, lambda: str(uuid4())),
        marketplace="facebook",
        component=build_analysis_component_registry().require(ANALYZE_FACEBOOK_ITEM),
        capabilities=tuple(
            WorkCapability(kind=ANALYZE_ITEM_WORK_KIND, payload_schema_version=version)
            for version in (3, 4)
        ),
        payload_type=AnalyzeItemPayload,
        prepare=_prepared,
        outcome_builder=facebook_analysis_outcome,
    )


async def _seed(database: Database) -> None:
    await _store(
        database,
        tuple(
            RecordDraft(
                identifier=identifier, kind=("test", "analysis_input"), schema_version=1, value={}
            )
            for identifier in ("evidence", "guide")
        ),
    )


async def _initial(
    database: Database,
    registry: StageHandlerRegistry,
    *,
    version: int = 4,
    timeout: int = 10,
    large: bool = False,
) -> tuple[StageDefinition, WorkStageAdapter]:
    await _seed(database)
    payload = AnalyzeItemPayload(
        evidence_set_record_identifier="evidence",
        product_guide_record_identifier="guide",
        timeout_seconds=timeout,
        claude_version="v" * 70_000 if large else None,
    )
    work = analyze_item_work(identifier="analysis-work", payload=payload).model_copy(
        update={"payload_schema_version": version}
    )
    return await _register(database, registry, work)


@pytest.mark.parametrize("version", (3, 4))
async def test_analysis_phases_restart_and_publish_exact_owned_outputs(
    tmp_path: Path, version: int
) -> None:
    path = tmp_path / "carl.sqlite3"
    cli = _cli(tmp_path)
    async with Database.managed(path, initialize=True) as database:
        registry = _registry(database, cli)
        first, adapter = await _initial(database, registry, version=version, large=True)
        assert len(json.dumps(first.payload)) < 1000
        assert "payload" not in first.payload
        prepared = await _run(registry, stage_manager(database), first, adapter)
        assert isinstance(prepared, WaitWork)
        assert prepared.continuation.resources[0].name == "analysis"
        assert not (tmp_path / "calls").exists()
        captured = await _run(registry, stage_manager(database), prepared.continuation, adapter)
        assert isinstance(captured, WaitWork)
        assert captured.continuation.resources[0].name == "cpu"
        assert len(json.dumps(captured.continuation.payload)) < 1000
        publish = captured.continuation
    async with Database.managed(path, initialize=False) as database:
        registry = _registry(database, cli)
        adapter = registry.require_adapter(
            WorkCapability(kind=ANALYZE_ITEM_WORK_KIND, payload_schema_version=version)
        )
        completed = await _run(registry, stage_manager(database), publish, adapter)
        assert isinstance(completed, CompletedWork)
        assert completed.artifacts == ()
        assert tuple(output.name for output in completed.outputs) == (("item_analysis",),)
        record = completed.records[0]
        assert record.schema_version == 5
        assert record.value["analysis_text"] == "retained report"
        assert record.value["claude"]["argv"] == list(
            cli.argv(
                "Identify exact saved evidence.",
                "claude-sonnet-5-5",
                AnalyzeItemPayload(
                    evidence_set_record_identifier="evidence",
                    product_guide_record_identifier="guide",
                ).effort,
                8,
            )
        )
        _, inputs, outputs = await database.object_operation_relations(record.identifier)
        assert {(item.name, item.object_identifier) for item in inputs} >= {
            (("listing_analysis_evidence",), "evidence"),
            (("product_guide",), "guide"),
        }
        names = [item.name for item in outputs]
        for name in (
            ("listing_input",),
            ("prompt",),
            ("claude", "stdout"),
            ("claude", "stderr"),
            ("item_analysis",),
        ):
            assert names.count(name) == 1
        assert (tmp_path / "calls").read_text() == "launch\n"


async def test_uncertain_capture_checkpoint_never_launches_again(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, _cli(tmp_path))
        first, adapter = await _initial(database, registry)
        prepared = await _run(registry, stage_manager(database), first, adapter)
        assert isinstance(prepared, WaitWork)
        manager = stage_manager(database)
        async with manager.admit(
            prepared.continuation.identifier,
            component=adapter.component,
            provenance=_provenance(),
            invocation={},
        ) as admission:
            assert admission.execution is not None
            unpublished = await registry.run(admission.execution)
            assert isinstance(unpublished, WaitWork)
        failed = await _run(registry, manager, prepared.continuation, adapter)
        assert isinstance(failed, TerminalFailureWork)
        assert failed.error["kind"] == "claude_uncertain_previous_dispatch"
        assert (tmp_path / "calls").read_text() == "launch\n"


@pytest.mark.parametrize(
    "mode,expected", (("failure", TerminalFailureWork), ("timeout", RetryWork))
)
async def test_failure_capture_retains_all_phase_evidence(
    tmp_path: Path, mode: str, expected: type[TerminalFailureWork] | type[RetryWork]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, _cli(tmp_path, mode))
        first, adapter = await _initial(database, registry, timeout=1)
        prepared = await _run(registry, stage_manager(database), first, adapter)
        assert isinstance(prepared, WaitWork)
        captured = await _run(registry, stage_manager(database), prepared.continuation, adapter)
        assert isinstance(captured, WaitWork)
        failed = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(failed, expected)
        assert failed.records[0].value["state"] == "failed"
        _, _, outputs = await database.object_operation_relations(failed.records[0].identifier)
        assert {output.name for output in outputs} >= {
            ("listing_input",),
            ("prompt",),
            ("claude", "stdout"),
            ("claude", "stderr"),
            ("item_analysis",),
        }
        assert (tmp_path / "calls").read_text() == "launch\n"


async def test_publish_replay_does_not_launch_or_duplicate_capture(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, _cli(tmp_path))
        first, adapter = await _initial(database, registry)
        prepared = await _run(registry, stage_manager(database), first, adapter)
        assert isinstance(prepared, WaitWork)
        captured = await _run(registry, stage_manager(database), prepared.continuation, adapter)
        assert isinstance(captured, WaitWork)
        manager = stage_manager(database)
        async with manager.admit(
            captured.continuation.identifier,
            component=adapter.component,
            provenance=_provenance(),
            invocation={},
        ) as admission:
            assert admission.execution is not None
            assert isinstance(await registry.run(admission.execution), CompletedWork)
        completed = await _run(registry, manager, captured.continuation, adapter)
        assert isinstance(completed, CompletedWork)
        assert (tmp_path / "calls").read_text() == "launch\n"


async def test_completed_capture_survives_workspace_cleanup_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = stages.temporary_analysis_directory

    @asynccontextmanager
    async def failing_cleanup(root: Path | None = None) -> AsyncIterator[Path]:
        async with original(root) as directory:
            yield directory
        raise RuntimeError("synthetic cleanup detail must not be retained")

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, _cli(tmp_path))
        first, adapter = await _initial(database, registry)
        prepared = await _run(registry, stage_manager(database), first, adapter)
        assert isinstance(prepared, WaitWork)
        monkeypatch.setattr(stages, "temporary_analysis_directory", failing_cleanup)
        captured = await _run(registry, stage_manager(database), prepared.continuation, adapter)
        assert isinstance(captured, WaitWork)
        assert captured.result == {
            "state": "analysis_captured",
            "cleanup_failure_type": "RuntimeError",
        }
        completed = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(completed, CompletedWork)
        assert completed.records[0].value["analysis_text"] == "retained report"
        assert (tmp_path / "calls").read_text() == "launch\n"


async def test_malformed_queued_payload_is_terminal_before_child(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, _cli(tmp_path))
        await _seed(database)
        work = analyze_item_work(
            identifier="malformed-work",
            payload=AnalyzeItemPayload(
                evidence_set_record_identifier="evidence", product_guide_record_identifier="guide"
            ),
        ).model_copy(update={"payload": {"invalid": "retained legacy payload"}})
        first, adapter = await _register(database, registry, work)
        failed = await _run(registry, stage_manager(database), first, adapter)
        assert isinstance(failed, TerminalFailureWork)
        assert failed.error == {"kind": "analysis_input_failure", "type": "ValidationError"}
        assert not (tmp_path / "calls").exists()


async def test_oversized_capture_budget_is_rejected_before_launch(tmp_path: Path) -> None:
    cli = _cli(tmp_path)
    oversized = ClaudeCli(cli.executable, maximum_stdout_bytes=MAX_ANALYSIS_STDOUT_BYTES + 1)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = _registry(database, oversized)
        first, adapter = await _initial(database, registry)
        failed = await _run(registry, stage_manager(database), first, adapter)
        assert isinstance(failed, TerminalFailureWork)
        assert failed.error["kind"] == "analysis_input_failure"
        assert "output limits" in failed.error["message"]
        assert not (tmp_path / "calls").exists()


async def test_publish_parser_uses_cpu_thread_and_reuses_stage_permit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CARL_RESOURCE_CPU", "1")
    loop_thread = threading.get_ident()
    parser_threads: list[int] = []
    original = stages.parse_claude_capture
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        resources = current_resource_manager()
        assert resources is not None and resources.capacities["cpu"] == 1

        def parse_in_thread(capture: ClaudeCapture) -> ClaudeRun:
            parser_threads.append(threading.get_ident())
            assert threading.get_ident() != loop_thread
            assert current_resource_manager() is resources
            return original(capture)

        monkeypatch.setattr(stages, "parse_claude_capture", parse_in_thread)
        registry = _registry(database, _cli(tmp_path))
        first, adapter = await _initial(database, registry)
        prepared = await _run(registry, stage_manager(database), first, adapter)
        assert isinstance(prepared, WaitWork)
        captured = await _run(registry, stage_manager(database), prepared.continuation, adapter)
        assert isinstance(captured, WaitWork)
        assert parser_threads == []
        completed = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(completed, CompletedWork)
        assert len(parser_threads) == 1
