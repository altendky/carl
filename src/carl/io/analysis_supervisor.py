"""Keep an inherited execution guard until the analysis subprocess has exited.

This module is a private subprocess entry point. It deliberately survives its
Carl parent's death, retains the guard, and enforces its own execution deadline.
"""

import os
import signal
import subprocess
import sys
from contextlib import suppress


def main() -> int:
    descriptor = int(sys.argv[1])
    timeout = int(sys.argv[2])
    os.fstat(descriptor)  # Fail closed if admission ownership was not inherited.
    child: subprocess.Popen[bytes] | None = None
    stopping = False

    def terminate(signum: int, frame: object) -> None:
        nonlocal stopping
        del signum, frame
        stopping = True
        if child is not None:
            with suppress(ProcessLookupError):
                os.killpg(child.pid, signal.SIGKILL)

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    try:
        child = subprocess.Popen(
            sys.argv[3:],
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            pass_fds=(descriptor,),
        )
        if stopping:
            terminate(signal.SIGTERM, None)
        try:
            return child.wait(timeout=timeout + 5)
        except subprocess.TimeoutExpired:
            terminate(signal.SIGTERM, None)
            child.wait()
            return 124
    finally:
        if child is not None:
            terminate(signal.SIGTERM, None)
            child.wait()
        # No LOCK_UN: the parent may still be unwinding and recording completion.
        os.close(descriptor)


if __name__ == "__main__":
    sys.exit(main())
