"""Analysis checkpoints retain bounded exact evidence without subprocess replay."""

# Private helpers expose atomic-file and corruption boundaries, entirely offline.
# pyright: reportPrivateUsage=false

import hashlib
import json
import os
import sys
import threading
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import anyio
import pytest

import carl.io.analysis_checkpoints as checkpoints
import carl.io.claude as claude_module
from carl._tests.test_item_analysis import _provenance
from carl.core.analysis_stages import AnalysisImage, PreparedAnalysis
from carl.core.components import Component, ComponentId
from carl.core.item_analysis import ClaudeEffort
from carl.core.models import BytesDraft, NamedInput, NamedOutput
from carl.core.resources import ResourceRequest
from carl.io.analysis_checkpoints import (
    AnalysisCheckpoint,
    checkpoint_analysis_inputs,
    checkpoint_claude_capture,
    read_analysis_input_manifest,
    restore_analysis_inputs,
    restore_claude_capture,
)
from carl.io.claude import ClaudeCapture, ClaudeCli, parse_claude_capture
from carl.io.resource_permits import current_resource_manager
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio
_COMPONENT = Component(ComponentId(("test", "analysis_checkpoints")), 1, lambda: None)


def _id() -> str:
    return str(uuid4())


async def _store(database: Database, checkpoint: AnalysisCheckpoint) -> None:
    operation = _id()
    await database.begin_operation(
        operation_id=operation,
        component=_COMPONENT,
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-11-01T00:00:00+00:00",
    )
    await database.complete_operation(
        operation_id=operation,
        records=checkpoint.records,
        artifacts=checkpoint.artifacts,
        outputs=checkpoint.outputs,
        result={},
        ended_at_utc="2026-11-01T00:00:01+00:00",
        duration_ns=1,
    )


async def _prepared(database: Database, *, external: bool = False) -> PreparedAnalysis:
    content, identifier = b"exact retained image bytes\x00\xff", _id()
    artifact = (
        checkpoints._publish(
            database.path.parent, identifier, ("test", "image"), "image/png", {}, content
        )
        if external
        else BytesDraft(
            identifier=identifier,
            kind=("test", "image"),
            media_type="image/png",
            representation={},
            content=content,
        )
    )
    operation = _id()
    await database.begin_operation(
        operation_id=operation,
        component=_COMPONENT,
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-11-01T00:00:00+00:00",
    )
    await database.complete_operation(
        operation_id=operation,
        records=(),
        artifacts=(artifact,),
        outputs=(NamedOutput(name=("image",), object_identifier=identifier),),
        result={},
        ended_at_utc="2026-11-01T00:00:01+00:00",
        duration_ns=1,
    )
    return PreparedAnalysis(
        b'{"listing":"exact bytes"}',
        b"exact prompt\x00\xc3\xa9",
        (
            AnalysisImage(
                artifact_identifier=identifier,
                sha256=hashlib.sha256(content).hexdigest(),
                media_type="image/png",
                byte_length=len(content),
                filename="images/003.png",
                gallery_order=3,
            ),
        ),
        (NamedInput(name=("source_image",), object_identifier=identifier),),
    )


async def _inputs(
    database: Database, *, external: bool = False
) -> tuple[PreparedAnalysis, AnalysisCheckpoint]:
    prepared = await _prepared(database, external=external)
    checkpoint = await checkpoint_analysis_inputs(
        database,
        prepared,
        marketplace="facebook",
        configuration={"model": "fixture-model"},
        new_identifier=_id,
    )
    await _store(database, checkpoint)
    return prepared, checkpoint


def _capture(**changes: object) -> ClaudeCapture:
    original = ClaudeCapture(
        argv=("/runtime/fake-claude", "-p", "exact prompt é", "--model", "fixture-model"),
        started_at_utc_ns=1,
        ended_at_utc_ns=3,
        duration_ns=2,
        exit_code=0,
        stdout=b'{"type":"system","subtype":"init","claude_code_version":"fixture-version"}\n{"type":"result","result":"exact report","is_error":false}\n',
        stderr=b"exact stderr\x00\xff",
    )
    return replace(original, **changes)


@pytest.mark.parametrize("external", (False, True))
@pytest.mark.parametrize("marketplace", ("facebook", "ebay"))
async def test_inputs_roundtrip_legacy_and_external_images_with_external_large_configuration(
    tmp_path: Path, external: bool, marketplace
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        prepared = await _prepared(database, external=external)
        configuration = {"model": "fixture-model", "claude_version": "v" * 70_000}
        checkpoint = await checkpoint_analysis_inputs(
            database,
            prepared,
            marketplace=marketplace,
            configuration=configuration,
            new_identifier=_id,
        )
        await _store(database, checkpoint)
        manifest = await read_analysis_input_manifest(database, checkpoint.record_identifier)
        restored = await restore_analysis_inputs(database, checkpoint.record_identifier)
        assert manifest.configuration == configuration and manifest.marketplace == marketplace
        assert (
            restored.prompt == prepared.prompt and restored.listing_input == prepared.listing_input
        )
        assert restored.inputs == prepared.inputs
        image = restored.images[0]
        assert image.filename == "images/003.png" and image.gallery_order == 3
        assert (image.artifact_identifier == prepared.images[0].artifact_identifier) == external
        metadata = await database.get_external_artifact_metadata(image.artifact_identifier)
        assert metadata is not None and metadata["sha256"] == image.sha256
        _, body = await database.get_artifact(image.artifact_identifier)
        assert body == b"exact retained image bytes\x00\xff"
        _, _, record = await database.get_record(checkpoint.record_identifier)
        assert "configuration" not in record and "v" * 70_000 not in json.dumps(record)
        assert prepared.prompt.decode() not in json.dumps(record)
        assert len({output.object_identifier for output in checkpoint.outputs}) == len(
            checkpoint.outputs
        )
        for artifact in checkpoint.artifacts:
            assert artifact.locator.startswith("analysis/sha256/")
            assert artifact.kind != ("carl", "http", "response_body")


@pytest.mark.parametrize(
    "flags",
    (
        {},
        {"timed_out": True, "exit_code": None},
        {"output_limited": True, "exit_code": None},
        {"launch_failed": True, "exit_code": None, "stdout": b""},
    ),
)
async def test_raw_capture_roundtrip_preserves_exact_argv_streams_and_classification(
    tmp_path: Path, flags: dict[str, object]
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, inputs = await _inputs(database)
        raw = _capture(**flags)
        checkpoint = await checkpoint_claude_capture(
            database, raw, input_checkpoint_identifier=inputs.record_identifier, new_identifier=_id
        )
        await _store(database, checkpoint)
        restored = await restore_claude_capture(
            database,
            checkpoint.record_identifier,
            input_checkpoint_identifier=inputs.record_identifier,
        )
        assert restored == raw and parse_claude_capture(restored) == parse_claude_capture(raw)
        _, _, record = await database.get_record(checkpoint.record_identifier)
        assert "argv" not in record and "stdout" not in record and "stderr" not in record
        assert "exact prompt" not in json.dumps(record) and "exact report" not in json.dumps(record)
        assert {output.name for output in checkpoint.outputs} >= {
            ("claude", "stdout"),
            ("claude", "stderr"),
        }


@pytest.mark.parametrize(
    "damage", ("missing", "symlink", "corruption", "traversal", "oversized", "wrong_input")
)
async def test_restore_rejects_missing_unsafe_corrupt_or_misattributed_capture(
    tmp_path: Path, damage: str
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, inputs = await _inputs(database)
        checkpoint = await checkpoint_claude_capture(
            database,
            _capture(),
            input_checkpoint_identifier=inputs.record_identifier,
            new_identifier=_id,
        )
        await _store(database, checkpoint)
        stdout = next(
            artifact for artifact in checkpoint.artifacts if artifact.kind[-1] == "stdout"
        )
        path = database.path.parent / stdout.locator
        if damage == "missing":
            path.unlink()
        elif damage == "symlink":
            target = tmp_path / "outside"
            _ = target.write_bytes(path.read_bytes())
            path.unlink()
            path.symlink_to(target)
        elif damage == "corruption":
            _ = path.write_bytes(b"x" * stdout.size)
        elif damage in {"traversal", "oversized"}:
            async with database._connections.writer() as connection:
                if damage == "traversal":
                    _ = await connection.execute(
                        "UPDATE external_artifacts SET locator='../outside' WHERE object_id=?",
                        (stdout.identifier,),
                    )
                else:
                    _ = await connection.execute(
                        "UPDATE external_artifacts SET size=? WHERE object_id=?",
                        (checkpoints.MAX_ANALYSIS_STDOUT_BYTES + 1, stdout.identifier),
                    )
        with pytest.raises((ValueError, OSError)):
            _ = await restore_claude_capture(
                database,
                checkpoint.record_identifier,
                input_checkpoint_identifier="different-input"
                if damage == "wrong_input"
                else inputs.record_identifier,
            )


async def test_input_metadata_bounds_are_checked_before_any_external_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, inputs = await _inputs(database)
        manifest = await read_analysis_input_manifest(database, inputs.record_identifier)
        async with database._connections.writer() as connection:
            _ = await connection.execute(
                "UPDATE external_artifacts SET size=? WHERE object_id=?",
                (checkpoints.MAX_ANALYSIS_INPUT_BYTES + 1, manifest.images[0].artifact_identifier),
            )

        def unexpected(*args: object, **kwargs: object):
            del args, kwargs
            pytest.fail("Metadata bounds must precede every external read")

        monkeypatch.setattr(checkpoints, "_read_external", unexpected)
        with pytest.raises(ValueError):
            _ = await restore_analysis_inputs(database, inputs.record_identifier)


async def test_aggregate_bounds_precede_file_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        prepared = await _prepared(database)
        monkeypatch.setattr(checkpoints, "MAX_ANALYSIS_INPUT_BYTES", 10)
        with pytest.raises(ValueError, match="bound"):
            _ = await checkpoint_analysis_inputs(
                database, prepared, marketplace="facebook", configuration={}, new_identifier=_id
            )
        assert not (tmp_path / "analysis").exists()


@pytest.mark.parametrize("combined", (False, True))
async def test_capture_bounds_precede_file_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, combined: bool
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, inputs = await _inputs(database)
        files = set(tmp_path.rglob("*.blob"))
        monkeypatch.setattr(
            checkpoints,
            "MAX_ANALYSIS_OUTPUT_BYTES" if combined else "MAX_ANALYSIS_STDOUT_BYTES",
            10,
        )
        with pytest.raises(ValueError, match="bound"):
            _ = await checkpoint_claude_capture(
                database,
                _capture(stdout=b"x" * 11),
                input_checkpoint_identifier=inputs.record_identifier,
                new_identifier=_id,
            )
        assert set(tmp_path.rglob("*.blob")) == files


async def test_failed_file_write_leaves_no_partial_or_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        prepared = await _prepared(database)
        monkeypatch.setattr(checkpoints.os, "write", lambda descriptor, content: 0)
        with pytest.raises(OSError, match="progress"):
            _ = await checkpoint_analysis_inputs(
                database, prepared, marketplace="ebay", configuration={}, new_identifier=_id
            )
        assert not list(tmp_path.rglob("*.blob")) and not list(tmp_path.rglob("*.tmp"))


async def test_atomic_dedup_preserves_distinct_producers_and_exact_aliases(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        prepared = await _prepared(database)
        results: list[AnalysisCheckpoint] = []

        async def publish() -> None:
            results.append(
                await checkpoint_analysis_inputs(
                    database, prepared, marketplace="ebay", configuration={}, new_identifier=_id
                )
            )

        async with anyio.create_task_group() as tasks:
            _ = tasks.start_soon(publish)
            _ = tasks.start_soon(publish)
        assert len(results) == 2
        assert {artifact.locator for artifact in results[0].artifacts} == {
            artifact.locator for artifact in results[1].artifacts
        }
        assert not {artifact.identifier for artifact in results[0].artifacts}.intersection(
            artifact.identifier for artifact in results[1].artifacts
        )
        for result in results:
            await _store(database, result)
            assert (
                await restore_analysis_inputs(database, result.record_identifier)
            ).prompt == prepared.prompt
        assert not list(tmp_path.rglob("*.tmp"))


async def test_input_file_can_be_hardlinked_while_integrity_read_is_in_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, inputs = await _inputs(database)
        manifest = await read_analysis_input_manifest(database, inputs.record_identifier)
        metadata = await database.get_external_artifact_metadata(
            manifest.images[0].artifact_identifier
        )
        assert metadata is not None
        original_read, linked = os.read, False

        def link_on_read(descriptor: int, size: int) -> bytes:
            nonlocal linked
            source = Path(f"/proc/self/fd/{descriptor}").resolve()
            if source.name.endswith(".blob") and not linked:
                os.link(source, tmp_path / "workspace-link")
                linked = True
            return original_read(descriptor, size)

        monkeypatch.setattr(checkpoints.os, "read", link_on_read)
        assert (await restore_analysis_inputs(database, inputs.record_identifier)).prompt
        assert linked


async def test_duplicate_generated_identifiers_are_rejected_before_any_write(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        prepared = await _prepared(database)
        with pytest.raises(ValueError, match="unique"):
            _ = await checkpoint_analysis_inputs(
                database,
                prepared,
                marketplace="facebook",
                configuration={},
                new_identifier=lambda: "same-identifier",
            )
        assert not (tmp_path / "analysis").exists()


async def test_cancellation_during_file_write_waits_for_atomic_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    original = checkpoints._publish

    def slow(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(checkpoints, "_publish", slow)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        prepared = await _prepared(database)
        scopes: list[anyio.CancelScope] = []
        results: list[AnalysisCheckpoint] = []

        async def publish() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                results.append(
                    await checkpoint_analysis_inputs(
                        database,
                        prepared,
                        marketplace="facebook",
                        configuration={},
                        new_identifier=_id,
                    )
                )

        async with anyio.create_task_group() as tasks:
            _ = tasks.start_soon(publish)
            with anyio.fail_after(5):
                while not entered.is_set():
                    await anyio.sleep(0.001)
            scopes[0].cancel()
            release.set()
        assert not results and not list(tmp_path.rglob("*.tmp"))
        for path in tmp_path.rglob("*.blob"):
            assert hashlib.sha256(path.read_bytes()).hexdigest() == path.stem


async def test_capture_rejects_stream_from_another_producer(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, inputs = await _inputs(database)
        first = await checkpoint_claude_capture(
            database,
            _capture(),
            input_checkpoint_identifier=inputs.record_identifier,
            new_identifier=_id,
        )
        second = await checkpoint_claude_capture(
            database,
            _capture(stdout=b"other stream"),
            input_checkpoint_identifier=inputs.record_identifier,
            new_identifier=_id,
        )
        await _store(database, first)
        await _store(database, second)
        foreign_stdout = next(
            output.object_identifier
            for output in second.outputs
            if output.name == ("claude", "stdout")
        )
        _, _, value = await database.get_record(first.record_identifier)
        async with database._connections.writer() as connection:
            _ = await connection.execute(
                "UPDATE records SET value_json=? WHERE object_id=?",
                (
                    json.dumps({**value, "stdout_identifier": foreign_stdout}),
                    first.record_identifier,
                ),
            )
        with pytest.raises(ValueError, match="producer"):
            _ = await restore_claude_capture(
                database,
                first.record_identifier,
                input_checkpoint_identifier=inputs.record_identifier,
            )


async def test_raw_capture_does_not_parse_until_cpu_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "fake-claude"
    _ = executable.write_text(
        f'#!{sys.executable}\nfrom pathlib import Path\nPath(\'ran\').touch()\nprint(\'{{"type":"system","subtype":"init","claude_code_version":"fixture"}}\')\nprint(\'{{"result":"report","is_error":false}}\')\n'
    )
    executable.chmod(0o700)
    original, parsed = claude_module.parse_claude_capture, []

    def parse(capture: ClaudeCapture):
        parsed.append(capture)
        return original(capture)

    monkeypatch.setattr(claude_module, "parse_claude_capture", parse)
    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"analysis": 1, "cpu": 1}
    ) as database:
        manager = current_resource_manager()
        assert manager is not None
        results = []

        async def run() -> None:
            results.append(
                await ClaudeCli(str(executable)).run(
                    directory=tmp_path,
                    prompt="exact",
                    model="fixture-model",
                    effort=ClaudeEffort.MEDIUM,
                    timeout_seconds=5,
                )
            )

        # CPU capacity must release before joining the task waiting for that capacity.
        async with anyio.create_task_group() as tasks:  # noqa: SIM117
            async with manager.admit(ResourceRequest(name="cpu")):
                _ = tasks.start_soon(run)
                with anyio.fail_after(5):
                    while True:
                        async with database._connections.reader() as connection:
                            rows = await (
                                await connection.execute(
                                    "SELECT count(*) FROM resource_allocations a JOIN resource_permits p ON p.token=a.permit_token WHERE p.state='held' AND a.name='analysis' "
                                )
                            ).fetchall()
                        if (tmp_path / "ran").exists() and rows == [(0,)]:
                            break
                        await anyio.sleep(0.01)
                assert not parsed and not results
        assert len(parsed) == len(results) == 1 and results[0].text == "report"
