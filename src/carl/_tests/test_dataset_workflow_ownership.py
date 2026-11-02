"""The durable file owner composes with actual supervised command/process death."""

import hashlib
import json
import os
import subprocess
import sys
from contextlib import AsyncExitStack
from pathlib import Path

import anyio
import pytest

from carl.core.dataset_downloads import DatasetSource, FileReceipt
from carl.core.dataset_workflows import DatasetWorkflowDefinition
from carl.core.native_build import NativeBuildSpec
from carl.core.travel_data import TravelDataFailure, TravelDataSetupRequest
from carl.io.dataset_downloads import verify_dataset_file
from carl.io.dataset_workflow_owner import DatasetWorkflowBusy, open_dataset_workflow
from carl.io.native_build_receipts import read_build_receipt
from carl.io.paths import CarlDirectories
from carl.io.sqlite import Database
from carl.io.travel_data import TravelDataManager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def test_carl_death_preserves_verified_download_and_fences_native_recovery(
    tmp_path: Path, anyio_backend: str
) -> None:
    directories = CarlDirectories(
        **{name: tmp_path / name for name in ("config", "data", "cache", "state", "runtime")}
    )
    directories.data.mkdir(mode=0o700)
    definition = DatasetWorkflowDefinition(
        workflow_identifier="fixture-workflow",
        request=TravelDataSetupRequest(
            request_identifier="fixture-request", provider="geonames", activate=False
        ),
        recipe_identifier="offline-workflow-recipe",
        configuration_identity="offline-config-v1",
    )
    ready = tmp_path / "owner-ready.json"
    async with Database.managed(directories.database_file, initialize=True):
        process = await anyio.open_process(
            (
                sys.executable,
                "-m",
                "carl._tests.dataset_workflow_parent",
                directories.model_dump_json(),
                definition.model_dump_json(),
                str(ready),
                anyio_backend,
            ),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        pidfds: list[int] = []
        try:
            with anyio.fail_after(20):
                while not ready.exists():
                    if process.returncode is not None:
                        diagnostic = (
                            await process.stderr.receive(4096)
                            if process.stderr is not None
                            else b""
                        )
                        pytest.fail(f"Fixture Carl exited: {diagnostic!r}")
                    await anyio.sleep(0.01)
                info = json.loads(ready.read_bytes())
                directory = Path(info["directory"])
                while not (directory / "started").exists():
                    assert process.returncode is None, "Fixture Carl exited before native launch"
                    await anyio.sleep(0.01)
            spec = NativeBuildSpec.model_validate_json(json.dumps(info["spec"]))
            receipt = FileReceipt.model_validate_json(json.dumps(info["receipt"]))
            pidfds = [
                os.pidfd_open(int((directory / name).read_text()))
                for name in ("command.pid", "descendant.pid")
            ]
            # This exercises the real foreground activation entrypoint while
            # another Carl process owns the workflow and native descendants.
            with pytest.raises(TravelDataFailure) as busy:
                await TravelDataManager(directories).activate("geonames", "missing")
            assert busy.value.reason == "travel_data_busy"
            with pytest.raises(DatasetWorkflowBusy):
                async with open_dataset_workflow(directories, definition, resume=True):
                    pytest.fail("A live native command lost its global workflow guard")
            process.kill()
            await process.wait()
            with anyio.fail_after(20):
                for descriptor in pidfds:
                    await anyio.wait_readable(descriptor)
                while not (spec.receipt_directory / "complete.json").exists():
                    await anyio.sleep(0.01)
            capture = read_build_receipt(spec)
            assert capture is not None and capture.result is not None
            assert capture.result.reason == "cancelled"
            # COMPLETE visibility can precede the supervisor's last FD close.
            async with AsyncExitStack() as stack:
                with anyio.fail_after(20):
                    while True:
                        try:
                            owner = await stack.enter_async_context(
                                open_dataset_workflow(directories, definition, resume=True)
                            )
                            break
                        except DatasetWorkflowBusy:
                            await anyio.sleep(0.01)
                assert owner.directory == directory
                assert owner.fence.generation > info["fence"]["generation"]
                source = DatasetSource(
                    identifier="fixture-source",
                    name="source.pbf",
                    url="https://dataset.invalid/fixture.pbf",
                    maximum_bytes=1024,
                )
                restored = await verify_dataset_file(source, receipt, directory=owner.directory)
                assert (
                    restored.sha256
                    == hashlib.sha256(b"verified offline dataset source\n").hexdigest()
                )
                # A restart must preserve the durable completed-file reference,
                # as well as the actual source bytes and native command receipt.
                assert receipt.sha256 in owner.journal.model_dump_json()
                await owner.finish("cancelled")
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
            for descriptor in pidfds:
                os.close(descriptor)
            await process.aclose()
        with pytest.raises(TravelDataFailure) as missing:
            await TravelDataManager(directories).activate("geonames", "missing")
        assert missing.value.reason == "dataset_invalid"
