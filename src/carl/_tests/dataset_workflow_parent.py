"""Offline Carl process used to test durable dataset ownership after SIGKILL."""

import json
import sys
from pathlib import Path
from uuid import uuid4

import anyio
import httpx

from carl.core.dataset_downloads import DatasetSource
from carl.core.dataset_workflows import DatasetWorkflowDefinition
from carl.core.native_build import NativeBuildSpec
from carl.io.dataset_downloads import download_dataset_source
from carl.io.dataset_workflow_owner import open_dataset_workflow
from carl.io.native_builds import run_native_build_command
from carl.io.paths import CarlDirectories
from carl.io.sqlite import Database


class _Source(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"verified offline dataset source\n"


async def main() -> None:
    directories = CarlDirectories.model_validate_json(sys.argv[1])
    definition = DatasetWorkflowDefinition.model_validate_json(sys.argv[2])
    ready = Path(sys.argv[3])
    source = DatasetSource(
        identifier="fixture-source",
        name="source.pbf",
        url="https://dataset.invalid/fixture.pbf",
        maximum_bytes=1024,
    )
    async with (
        Database.managed(directories.database_file),
        open_dataset_workflow(directories, definition) as owner,
    ):
        await owner.freeze_plan({"source": source.model_dump(mode="json")})
        receipt = await download_dataset_source(
            source,
            directory=owner.directory,
            execution_identifier="fixture-download",
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=_Source())),
        )
        await owner.checkpoint("downloaded", {"receipt": receipt.model_dump(mode="json")})
        spec = NativeBuildSpec(
            recipe_identifier="offline-workflow-recipe",
            phase_identifier="native-fixture",
            input_identity=receipt.sha256,
            execution_identifier=str(uuid4()),
            argv=(
                sys.executable,
                "-m",
                "carl._tests.native_build_command",
                "fork-block",
                str(owner.directory),
            ),
            directory=owner.directory,
            receipt_directory=owner.directory / "native-receipts",
            threads=2,
            timeout_seconds=30,
        )
        pending = ready.with_suffix(".new")
        pending.write_text(
            json.dumps(
                {
                    "directory": str(owner.directory),
                    "fence": owner.fence.model_dump(mode="json"),
                    "receipt": receipt.model_dump(mode="json"),
                    "spec": spec.model_dump(mode="json"),
                }
            )
        )
        pending.replace(ready)
        await run_native_build_command(spec, workflow_lock_fd=owner.lock_fd)


if __name__ == "__main__":
    anyio.run(main, backend=sys.argv[4])
