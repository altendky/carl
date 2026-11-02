"""Native build resolution keeps tool precedence without the guard-dropping wrapper."""

import importlib.util
from pathlib import Path

import pytest

from carl.io.native_tools import NativeBuildToolError, osrm_build_executable


def _executable(path: Path, content: bytes = b"\x7fELFfixture") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ = path.write_bytes(content)
    path.chmod(0o755)
    return path


def test_packaged_binary_precedes_cmake_and_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = tmp_path / "project" / "src" / "package" / "osrm"
    selected = _executable(package / "bin" / "osrm-extract")
    _ = _executable(package.parent.parent.parent / "build" / "z" / "osrm-extract")
    _ = _executable(tmp_path / "path" / "osrm-extract")
    monkeypatch.setenv("PATH", str(tmp_path / "path"))
    assert osrm_build_executable("extract", package_directory=package) == selected


def test_editable_and_path_fallbacks_return_actual_executable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = tmp_path / "project" / "src" / "package" / "osrm"
    package.mkdir(parents=True)
    older = _executable(package.parent.parent.parent / "build" / "a" / "osrm-partition")
    newer = _executable(package.parent.parent.parent / "build" / "z" / "osrm-partition")
    fallback = _executable(tmp_path / "path" / "osrm-partition")
    monkeypatch.setenv("PATH", str(tmp_path / "path"))
    assert osrm_build_executable("partition", package_directory=package) == newer
    newer.unlink()
    older.unlink()
    assert osrm_build_executable("partition", package_directory=package) == fallback


def test_existing_wrapper_script_is_rejected_without_silent_tool_substitution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = tmp_path / "osrm"
    _ = _executable(package / "bin" / "osrm-customize", b"#!/bin/sh\nexit 0\n")
    _ = _executable(tmp_path / "path" / "osrm-customize")
    monkeypatch.setenv("PATH", str(tmp_path / "path"))
    with pytest.raises(NativeBuildToolError, match="native_executable_required"):
        _ = osrm_build_executable("customize", package_directory=package)


def test_missing_tool_is_a_sanitized_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package = tmp_path / "osrm"
    package.mkdir()
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    with pytest.raises(NativeBuildToolError, match=r"^osrm_packaged_tool_missing$"):
        _ = osrm_build_executable("extract", package_directory=package)


def test_installed_tool_resolves_to_packaged_native_binary() -> None:
    if importlib.util.find_spec("osrm") is None:
        pytest.skip("Native package is not installed")
    path = osrm_build_executable("extract")
    assert path.name == "osrm-extract"
    with path.open("rb") as stream:
        assert stream.read(4) == b"\x7fELF"
