"""Exec a child after asking Linux to terminate it when its parent dies."""

from __future__ import annotations

import ctypes
import os
import signal
import sys

_PR_SET_PDEATHSIG = 1


def _set_parent_death_signal(expected_parent_pid: int) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = (
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    )
    prctl.restype = ctypes.c_int
    if prctl(_PR_SET_PDEATHSIG, signal.SIGTERM, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    # The parent may have exited between spawning us and configuring prctl.
    if os.getppid() != expected_parent_pid:
        os.kill(os.getpid(), signal.SIGTERM)


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("expected parent PID and command")
    expected_parent_pid = int(sys.argv[1])
    command = sys.argv[2:]
    _set_parent_death_signal(expected_parent_pid)
    os.execv(command[0], command)


if __name__ == "__main__":
    main()
