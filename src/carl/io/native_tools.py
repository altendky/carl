"""Resolve the actual OSRM tool without an intermediate subprocess wrapper."""

import importlib.util
import os
import shutil
import stat
from pathlib import Path
from typing import Literal


class NativeBuildToolError(RuntimeError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _native_executable(path: Path) -> Path:
    try:
        resolved = path.resolve(strict=True)
        information = resolved.stat()
        if (
            not stat.S_ISREG(information.st_mode)
            or information.st_mode & (stat.S_ISUID | stat.S_ISGID)
            or not os.access(resolved, os.X_OK)
        ):
            raise NativeBuildToolError("osrm_native_executable_required")
        with resolved.open("rb") as stream:
            if stream.read(4) != b"\x7fELF":
                raise NativeBuildToolError("osrm_native_executable_required")
        return resolved
    except (OSError, RuntimeError) as error:
        if isinstance(error, NativeBuildToolError):
            raise
        raise NativeBuildToolError("osrm_native_executable_unavailable") from None


def osrm_build_executable(
    operation: Literal["extract", "partition", "customize"],
    *,
    package_directory: Path | None = None,
) -> Path:
    """Keep the wrapper's package/build/PATH precedence while returning its actual ELF."""
    if operation not in {"extract", "partition", "customize"}:
        raise ValueError("Unsupported OSRM build operation")
    if package_directory is None:
        specification = importlib.util.find_spec("osrm")
        if specification is None or specification.origin is None:
            raise NativeBuildToolError("osrm_packaged_tool_missing")
        package_directory = Path(specification.origin).parent
    name = "osrm-" + operation
    packaged = package_directory / "bin" / name
    if packaged.is_file():
        return _native_executable(packaged)
    # Match the installed wrapper's editable/CMake fallback without executing it.
    project_root = package_directory.parent.parent.parent
    for directory in sorted(project_root.glob("build/*/"), reverse=True):
        candidate = directory / name
        if candidate.is_file():
            return _native_executable(candidate)
    found = shutil.which(name)
    if found is not None:
        return _native_executable(Path(found))
    raise NativeBuildToolError("osrm_packaged_tool_missing")
