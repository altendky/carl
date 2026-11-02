"""Disposable Carl parent for abrupt-death ownership regression tests."""

import fcntl
import os
import sys
from pathlib import Path

import anyio

from carl.core.native_build import NativeBuildSpec
from carl.io.native_build_protocol import NativeBuildProcess


async def main() -> None:
    root = Path(sys.argv[1])
    receipts = root / "receipts"
    receipts.mkdir(mode=0o700)
    descriptors: list[int] = []
    for name in ("cpu.lock", "workflow.lock", "phase.lock"):
        descriptor = os.open(root / name, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        descriptors.append(descriptor)
    spec = NativeBuildSpec(
        recipe_identifier="fixture-v1",
        phase_identifier="extract",
        input_identity="fixture-input",
        execution_identifier="fixture-execution",
        directory=root,
        receipt_directory=receipts,
        threads=2,
        timeout_seconds=10,
        argv=(sys.executable, "-m", "carl._tests.native_build_command", "fork-block", str(root)),
    )
    owner = await NativeBuildProcess.start(spec, guard_descriptors=tuple(descriptors))
    (root / "supervisor.pid").write_text(str(owner.pid))
    try:
        await owner.wait()
    finally:
        await owner.aclose()
        for descriptor in descriptors:
            os.close(descriptor)


if __name__ == "__main__":
    anyio.run(main, backend="trio")
