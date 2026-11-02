"""Real eBay evidence keeps its public analysis contract across durable phases."""

# These integration assertions distinguish physical reservations from logical work.
# pyright: reportPrivateUsage=false

import json
from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest

from carl._tests.test_ebay_analysis import _application, _seed
from carl._tests.test_item_analysis import _provenance
from carl.core.components import Component
from carl.core.item_analysis import ClaudeEffort
from carl.core.resources import ResourceRequest
from carl.core.review import RequestAnalysisRequest
from carl.core.stages import StageDefinition, StageOutcome, WaitWork
from carl.core.work import WorkCapability
from carl.core.worker import CompletedWork, RetryWork, TerminalFailureWork
from carl.ebay_analysis_stages import build_ebay_analysis_stage_registry
from carl.ebay_analysis_workers import request_ebay_listing_analysis
from carl.facebook_analysis_workers import AnalysisWorkerDependencies
from carl.io.claude import ClaudeCapture, ClaudeCli
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry
from carl.io.stages import stage_manager
from carl.review import ReviewApplication

pytestmark = pytest.mark.anyio


async def _held(database: Database, name: str) -> int:
    async with database._connections.reader() as connection:
        cursor = await connection.execute(
            "SELECT coalesce(sum(a.units),0) FROM resource_allocations a "
            "JOIN resource_permits p ON p.token=a.permit_token WHERE p.state='held' AND a.name=?",
            (name,),
        )
        row = await cursor.fetchone()
    assert row is not None
    return int(row[0])


async def _start(
    database: Database, root: Path
) -> tuple[StageHandlerRegistry, StageDefinition, Component]:
    await _seed(database)
    application, guide = await _application(database, root)
    requested = await request_ebay_listing_analysis(
        application,
        RequestAnalysisRequest(
            listing_observation_record_identifier="observation",
            product_guide_record_identifier=guide,
        ),
    )
    registry = build_ebay_analysis_stage_registry(
        AnalysisWorkerDependencies(
            database, ClaudeCli(executable="offline-claude"), application.new_identifier
        )
    )
    work = await database.work_definition(requested.work_identifier)
    adapter = registry.require_adapter(
        WorkCapability(kind=work.kind, payload_schema_version=work.payload_schema_version)
    )
    initial = adapter.initial_stage(work, str(uuid4()), str(uuid4()))
    await database.register_stage(initial, created_at_utc_ns=time_ns())
    return registry, initial, adapter.component


async def _execute(
    database: Database, registry: StageHandlerRegistry, stage: StageDefinition, component: Component
) -> StageOutcome:
    async with stage_manager(database).admit(
        stage.identifier, component=component, provenance=_provenance(), invocation={}
    ) as admission:
        assert admission.execution is not None, admission.result
        return await registry.execute(admission.execution)


def _fake_capture(
    monkeypatch: pytest.MonkeyPatch, database: Database, failure: str | None
) -> list[ClaudeCapture]:
    captures: list[ClaudeCapture] = []

    async def capture(
        self: ClaudeCli,
        *,
        directory: Path,
        prompt: str,
        model: str,
        effort: ClaudeEffort,
        timeout_seconds: int,
        maximum_turns: int | None = None,
    ) -> ClaudeCapture:
        assert await _held(database, "analysis") == 1
        assert await _held(database, "cpu") == 0
        assert "single saved eBay listing" in prompt and "barrel size" in prompt
        listing = json.loads((directory / "listing.json").read_text())
        assert listing["fields"]["title"] == "Eyepiece"
        assert listing["description_state"] == "saved"
        assert (directory / "images" / "000.png").read_bytes() == b"offline image bytes"
        output = b'{"type":"system","subtype":"init","claude_code_version":"2.1.278"}\n'
        if failure != "claude_timeout":
            output += b'{"type":"result","result":"Evidence-backed item report","is_error":false,"num_turns":2}\n'
        result = ClaudeCapture(
            argv=self.argv(prompt, model, effort, maximum_turns),
            started_at_utc_ns=1,
            ended_at_utc_ns=2,
            duration_ns=1,
            exit_code=None if failure == "claude_timeout" else 7 if failure else 0,
            stdout=output,
            stderr=b"offline process diagnostics" if failure else b"",
            timed_out=failure == "claude_timeout",
        )
        captures.append(result)
        return result

    monkeypatch.setattr(ClaudeCli, "capture", capture)
    return captures


@pytest.mark.parametrize("failure", (None, "claude_timeout", "claude_process_failure"))
async def test_ebay_analysis_phases_preserve_evidence_aliases_and_retry_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str | None
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        captures = _fake_capture(monkeypatch, database, failure)
        registry, initial, component = await _start(database, tmp_path)
        assert component.output_schema_version == 1
        prepared = await _execute(database, registry, initial, component)
        assert isinstance(prepared, WaitWork)
        assert prepared.continuation.resources == (ResourceRequest(name="analysis"),)
        assert not captures and await _held(database, "cpu") == 0
        captured = await _execute(database, registry, prepared.continuation, component)
        assert isinstance(captured, WaitWork)
        assert captured.continuation.resources == (ResourceRequest(name="cpu"),)
        assert len(captures) == 1 and await _held(database, "analysis") == 0
        outcome = await _execute(database, registry, captured.continuation, component)
        expected = (
            RetryWork
            if failure == "claude_timeout"
            else TerminalFailureWork
            if failure
            else CompletedWork
        )
        assert isinstance(outcome, expected)
        assert outcome.result["failure_kind"] == failure
        report = outcome.records[0]
        assert report.kind == ("carl", "ebay", "item_analysis") and report.schema_version == 5
        assert report.value["claude"]["argv"] == list(captures[0].argv)
        assert report.value["claude"]["version"] == "2.1.278"
        public = await ReviewApplication(database, tmp_path).get_listing_analysis(report.identifier)
        assert public.descriptor.listing_observation_record_identifier == "observation"
        assert public.text == ("Evidence-backed item report" if failure is None else None)
        _, _, outputs = await database.object_operation_relations(report.identifier)
        aliases = {edge.name: edge.object_identifier for edge in outputs}
        for name in (
            ("listing_input",),
            ("prompt",),
            ("claude", "stdout"),
            ("claude", "stderr"),
            ("item_analysis",),
        ):
            assert sum(edge.name == name for edge in outputs) == 1
        assert len({edge.object_identifier for edge in outputs}) == len(outputs)
        assert (await database.get_artifact(aliases[("claude", "stdout")]))[1] == captures[0].stdout
        assert (await database.get_artifact(aliases[("prompt",)]))[1].decode() in captures[0].argv
        if isinstance(outcome, RetryWork):
            assert outcome.delay_ns == 30_000_000_000
            assert outcome.reason["maximum_attempts"] == 3


async def test_ebay_saved_capture_publishes_after_database_restart_without_new_invocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        captures = _fake_capture(monkeypatch, database, None)
        registry, initial, component = await _start(database, tmp_path)
        prepared = await _execute(database, registry, initial, component)
        assert isinstance(prepared, WaitWork)
        captured = await _execute(database, registry, prepared.continuation, component)
        assert isinstance(captured, WaitWork)
        expected_argv = list(captures[0].argv)
    async with Database.managed(path) as database:
        registry = build_ebay_analysis_stage_registry(
            AnalysisWorkerDependencies(
                database, ClaudeCli(executable="changed-after-restart"), lambda: str(uuid4())
            )
        )
        result = await _execute(database, registry, captured.continuation, component)
        assert isinstance(result, CompletedWork) and len(captures) == 1
        assert result.records[0].value["claude"]["argv"] == expected_argv
        assert (await database.work(initial.work_item_identifier))["attempt"] == 1
