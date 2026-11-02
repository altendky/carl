"""Explicit offline command processes for native-build supervision tests."""

import json
import os
import sys
import time
from pathlib import Path


def main() -> int:
    mode, root = sys.argv[1], Path(sys.argv[2])
    (root / "command.pid").write_text(str(os.getpid()))
    (root / "threads").write_text(os.environ.get("OSMIUM_POOL_THREADS", "missing"))
    if mode == "guards":
        descriptors = tuple(int(value) for value in sys.argv[3].split(","))
        (root / "inherited-guards").write_text(
            json.dumps([os.fstat(value).st_ino for value in descriptors])
        )
    if mode in {"block", "fork-block", "fork-exit", "escape-exit"}:
        if mode != "block":
            pid = os.fork()
            if pid == 0:
                if mode == "escape-exit":
                    os.setsid()
                (root / "descendant.pid").write_text(str(os.getpid()))
                while True:
                    time.sleep(0.01)
            while not (root / "descendant.pid").exists():
                time.sleep(0.01)
            if mode in {"fork-exit", "escape-exit"}:
                return 0
        (root / "started").write_text("ready")
        while not (root / "proceed").exists():
            time.sleep(0.01)
    if mode == "noisy":
        while True:
            os.write(1, b"x" * 65536)
            os.write(2, b"e" * 65536)
    if mode == "finite-noisy":
        os.write(1, b"HEAD\n" + b"x" * 100_000 + b"TAIL-FINISHED\n")
        os.write(2, b"stderr-finished\n")
    elif mode == "receipt-fault":
        Path(sys.argv[3]).joinpath("complete.json").mkdir()
    else:
        print("fixture-version")
        print("fixture-diagnostic", file=sys.stderr)
    (root / "finished").write_text("done")
    return 7 if mode == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
