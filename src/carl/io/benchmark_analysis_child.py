"""Local stream-JSON workload; never invokes a model, tool, or network client."""

import argparse
import json
import os
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--delay-ms", type=int, required=True)
    _ = parser.add_argument("--control", type=Path, required=True)
    _ = parser.add_argument("--identifier", required=True)
    _ = parser.add_argument("--gated", action="store_true")
    arguments = parser.parse_args()
    if (
        not 0 <= arguments.delay_ms <= 10_000
        or len(arguments.identifier) != 64
        or any(character not in "0123456789abcdef" for character in arguments.identifier)
    ):
        raise ValueError("Invalid bounded benchmark child parameters")
    _ = json.loads(Path("listing.json").read_text())
    control: Path = arguments.control
    control.mkdir(mode=0o700, parents=True, exist_ok=True)
    _ = (control / f"{arguments.identifier}.started").write_text(str(os.getpid()))
    print(
        json.dumps(
            {"type": "system", "subtype": "init", "claude_code_version": "offline-benchmark-v1"}
        ),
        flush=True,
    )
    if arguments.gated:
        deadline = time.monotonic() + 120
        while not (control / "release").exists():
            if time.monotonic() > deadline:
                raise TimeoutError("Benchmark control deadline")
            time.sleep(0.01)
    time.sleep(arguments.delay_ms / 1000)
    print(
        json.dumps(
            {
                "result": "Synthetic offline benchmark result. No research performed.",
                "is_error": False,
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
