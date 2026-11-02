"""Checkpoint verification cannot make subsequent materialization reads unbounded."""

# Reuse offline checkpoint fixtures to exercise the exact staged read seam.
# pyright: reportPrivateUsage=false

from dataclasses import replace
from pathlib import Path

import pytest

import carl.io.sqlite as sqlite
from carl._tests.test_analysis_checkpoints import _inputs
from carl._tests.test_analysis_stages import _cli, _initial, _registry
from carl._tests.test_facebook_stateless_stages import _run
from carl._tests.test_stage_storage import _scalar
from carl.core.analysis_stages import (
    MAX_ANALYSIS_OUTPUT_BYTES,
    MAX_ANALYSIS_STDERR_BYTES,
    MAX_ANALYSIS_STDOUT_BYTES,
)
from carl.core.stages import WaitWork
from carl.core.worker import TerminalFailureWork
from carl.io.analysis_checkpoints import restore_analysis_inputs
from carl.io.analysis_stage_workers import materialize_analysis_inputs
from carl.io.sqlite import Database
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("external", (False, True))
async def test_post_restore_image_growth_is_rejected_before_materialization_read(
    tmp_path: Path, external: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        _, checkpoint = await _inputs(database, external=external)
        prepared = await restore_analysis_inputs(database, checkpoint.record_identifier)
        image = prepared.images[0]
        metadata = await database.get_external_artifact_metadata(image.artifact_identifier)
        assert metadata is not None
        storage = metadata["storage"]
        assert isinstance(storage, dict) and isinstance(storage["locator"], str)
        source = tmp_path / storage["locator"]
        # Simulate corruption after the bounded checkpoint verification. The
        # second integrity check must reject actual size before loading bytes.
        source.write_bytes(b"x" * (image.byte_length + 1))
        directory = tmp_path / "child"
        directory.mkdir(mode=0o700)

        def no_read(*_args: object) -> bytes:
            raise AssertionError("Oversized checkpoint image reached an unbounded read")

        monkeypatch.setattr(sqlite.os, "read", no_read)
        with pytest.raises(ValueError, match="maximum_bytes"):
            await materialize_analysis_inputs(database, prepared, directory)
        assert not (directory / image.filename).exists()


@pytest.mark.parametrize(
    "attribute,maximum",
    (
        ("maximum_stdout_bytes", MAX_ANALYSIS_STDOUT_BYTES),
        ("maximum_stderr_bytes", MAX_ANALYSIS_STDERR_BYTES),
        ("maximum_output_bytes", MAX_ANALYSIS_OUTPUT_BYTES),
    ),
)
async def test_restarted_capture_rechecks_current_output_limits_before_charge_or_child(
    tmp_path: Path, attribute: str, maximum: int
) -> None:
    path = tmp_path / "carl.sqlite3"
    cli = _cli(tmp_path)
    async with Database.managed(path, initialize=True) as database:
        registry = _registry(database, cli)
        initial, adapter = await _initial(database, registry)
        prepared = await _run(registry, stage_manager(database), initial, adapter)
        assert isinstance(prepared, WaitWork)
    async with Database.managed(path) as database:
        registry = _registry(database, replace(cli, **{attribute: maximum + 1}))
        failed = await _run(registry, stage_manager(database), prepared.continuation, adapter)
        assert isinstance(failed, TerminalFailureWork)
        assert failed.error["kind"] == "analysis_input_failure"
        assert "output limits" in failed.error["message"]
        assert not (tmp_path / "calls").exists()
        assert await _scalar(database, "SELECT count(*) FROM resident_request_charges") == 0
