"""Arm parent death, preserve guards, and replace this PID with the actual command."""

import errno
import os
import stat
import sys

from carl.io.native_routing_worker import arm_parent_death


def main() -> int:
    try:
        expected_parent = int(sys.argv[1])
        guards = tuple(int(value) for value in sys.argv[2].split(","))
        argv = sys.argv[3:]
        arm_parent_death(expected_parent)
        info = os.stat(argv[0])
        if not stat.S_ISREG(info.st_mode) or info.st_mode & (stat.S_ISUID | stat.S_ISGID):
            return 126
        try:
            if os.getxattr(argv[0], "security.capability"):
                return 126
        except OSError as error:
            if error.errno not in {errno.ENODATA, errno.ENOTSUP, errno.EOPNOTSUPP}:
                return 126
        for descriptor in guards:
            os.fstat(descriptor)
            os.set_inheritable(descriptor, True)
        os.execvpe(argv[0], argv, os.environ)
    except Exception:
        # No argument, path, environment or native exception text enters logs.
        return 127
    return 127


if __name__ == "__main__":
    raise SystemExit(main())
