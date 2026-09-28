"""Read execution provenance from the local process and Git checkout."""

import hashlib
import platform
import subprocess
import sys
from importlib import metadata
from pathlib import Path

import anyio
import sniffio

from carl import __version__
from carl.core.models import CodeProvenance, DependencyVersion, JsonValue

_DEPENDENCIES = (
    "anyio",
    "apsw",
    "brotli",
    "cyclopts",
    "httpcore",
    "httpx",
    "platformdirs",
    "pydantic",
    "sniffio",
    "socksio",
    "trio",
    "zstandard",
)


def _git(repository: Path, *arguments: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repository), *arguments],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return completed.stdout.strip()


def collect_code_provenance(repository: Path) -> CodeProvenance:
    commit_hash = _git(repository, "rev-parse", "HEAD")
    status = _git(repository, "status", "--porcelain=v1", "--untracked-files=normal")
    worktree_state = "unknown" if status is None else ("dirty" if status else "clean")
    dependencies = []
    for name in _DEPENDENCIES:
        try:
            version = metadata.version(name)
        except metadata.PackageNotFoundError:
            version = None
        dependencies.append(DependencyVersion(name=name, version=version))
    lockfile = repository / "uv.lock"
    lockfile_sha256 = (
        hashlib.sha256(lockfile.read_bytes()).hexdigest() if lockfile.is_file() else None
    )
    return CodeProvenance(
        repository_url=_git(repository, "remote", "get-url", "origin"),
        commit_hash=commit_hash,
        worktree_state=worktree_state,
        package_version=__version__,
        python_implementation=platform.python_implementation(),
        python_version=platform.python_version(),
        dependencies=tuple(dependencies),
        lockfile_sha256=lockfile_sha256,
    )


def source_tree_sha256(repository: Path) -> str:
    """Identify the complete Python source tree without retaining its contents."""

    digest = hashlib.sha256()
    source_root = repository / "src" / "carl"
    for path in sorted(source_root.rglob("*.py")):
        relative = path.relative_to(repository).as_posix().encode()
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


async def collect_code_provenance_async(repository: Path) -> CodeProvenance:
    """Collect provenance without blocking the async event-loop thread."""

    return await anyio.to_thread.run_sync(
        collect_code_provenance,
        repository,
        abandon_on_cancel=True,
    )


async def source_tree_sha256_async(repository: Path) -> str:
    """Identify source without blocking the async event-loop thread."""

    return await anyio.to_thread.run_sync(
        source_tree_sha256,
        repository,
        abandon_on_cancel=True,
    )


def process_invocation() -> dict[str, JsonValue]:
    try:
        async_library: JsonValue = {
            "state": "available",
            "name": sniffio.current_async_library(),
        }
    except sniffio.AsyncLibraryNotFoundError:
        async_library = {"state": "not_applicable"}
    return {
        "original_argv": list(sys.orig_argv),
        "application_argv": list(sys.argv),
        "executable": sys.executable,
        "execution_model": "in_process_python",
        "async_library": async_library,
        "subprocess_exit_code": {"state": "not_applicable"},
        "stderr": {"state": "not_applicable"},
    }
