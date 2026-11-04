"""Opt-in CI manifests and an exact audit of distributed pytest results."""

from __future__ import annotations

import argparse
import json
import math
import platform
import re
import sys
from collections import Counter
from collections.abc import Generator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import TypedDict, cast
from xml.etree import ElementTree

import pytest
from coverage import CoverageData
from coverage.exceptions import CoverageException

_PROPERTY = "carl_nodeid"
_MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
_MAX_NODEIDS = 100_000


class ShardResult(TypedDict):
    python_version: str
    shard: int
    selected: int
    passed: int
    skipped: int
    pytest_elapsed_seconds: float
    junit_test_seconds: float
    stored_test_seconds: float


class AuditResult(TypedDict):
    schema_version: int
    total_executions: int
    shards: list[ShardResult]
    coverage_files: list[str]


@dataclass
class _Manifest:
    path: Path
    started: float = field(default_factory=perf_counter)
    full: list[str] = field(default_factory=list)
    selected: list[str] = field(default_factory=list)

    def write(self, *, exit_code: int | None = None) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        _ = self.path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "python_version": platform.python_version(),
                    "full_nodeids": self.full,
                    "selected_nodeids": self.selected,
                    "elapsed_seconds": perf_counter() - self.started,
                    "exit_code": exit_code,
                },
                indent=2,
            )
            + "\n"
        )


_MANIFEST = pytest.StashKey[_Manifest]()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.getgroup("carl-ci").addoption(
        "--shard-manifest", type=Path, default=None, help="Write the CI shard audit manifest."
    )


def pytest_configure(config: pytest.Config) -> None:
    path = cast(Path | None, config.getoption("shard_manifest"))
    if path is not None:
        config.stash[_MANIFEST] = _Manifest(path)


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> Generator[None, object]:
    manifest = config.stash.get(_MANIFEST, None)
    if manifest is not None:
        manifest.full = [item.nodeid for item in items]
    yield
    if manifest is not None:
        manifest.selected = [item.nodeid for item in items]
        for item in items:
            item.user_properties.append((_PROPERTY, item.nodeid))
        manifest.write()


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    manifest = session.config.stash.get(_MANIFEST, None)
    if manifest is not None:
        manifest.write(exit_code=int(exitstatus))


def _read(path: Path) -> bytes:
    if path.stat().st_size > _MAX_ARTIFACT_BYTES:
        raise ValueError(f"Artifact exceeds size bound: {path}")
    return path.read_bytes()


def _object(path: Path) -> dict[str, object]:
    value: object = json.loads(_read(path))
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"Expected a JSON object: {path}")
    return cast(dict[str, object], value)


def _number(value: object, source: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"Invalid nonnegative duration in {source}")
    return float(value)


def _nodeids(value: object, source: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or len(value) > _MAX_NODEIDS
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise ValueError(f"Expected a nonempty bounded nodeid list in {source}")
    result = cast(list[str], value)
    if len(set(result)) != len(result):
        raise ValueError(f"Duplicate nodeids in {source}")
    return result


def _junit(path: Path) -> tuple[list[str], int, float]:
    root = ElementTree.fromstring(_read(path))
    nodeids: list[str] = []
    skipped = 0
    seconds = 0.0
    for case in root.iter("testcase"):
        properties = [
            prop.get("value")
            for prop in case.findall("./properties/property")
            if prop.get("name") == _PROPERTY
        ]
        if len(properties) != 1 or not properties[0]:
            raise ValueError(f"JUnit testcase lacks one {_PROPERTY} property: {path}")
        nodeids.append(properties[0])
        if len(nodeids) > _MAX_NODEIDS:
            raise ValueError(f"JUnit testcase count exceeds bound: {path}")
        if case.find("failure") is not None or case.find("error") is not None:
            raise ValueError(f"JUnit reports a failed or errored test: {path}")
        skipped += int(case.find("skipped") is not None)
        try:
            duration = float(case.get("time", ""))
        except ValueError as error:
            raise ValueError(f"JUnit testcase lacks a valid duration: {path}") from error
        seconds += _number(duration, str(path))
    return nodeids, skipped, seconds


def _coverage(path: Path) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"Missing or empty coverage input: {path}")
    try:
        data = CoverageData(basename=str(path))
        data.read()
        if not data.has_arcs() or not data.measured_files():
            raise ValueError(f"Coverage input lacks branch measurements: {path}")
    except CoverageException as error:
        raise ValueError(f"Invalid coverage input: {path}: {error}") from error


def audit(
    artifacts: Path,
    versions: Sequence[str],
    shards: int,
    *,
    coverage_version: str = "3.13.18",
) -> AuditResult:
    """Reject missing, duplicated, omitted or partially executed shard results."""
    if (
        not versions
        or len(set(versions)) != len(versions)
        or any(re.fullmatch(r"\d+\.\d+\.\d+", version) is None for version in versions)
        or not 1 <= shards <= 64
        or coverage_version not in versions
    ):
        raise ValueError("Invalid Python versions, coverage version or shard count")
    results: list[ShardResult] = []
    coverage_files: list[str] = []
    total = 0
    for version in versions:
        reference: list[str] | None = None
        executed: Counter[str] = Counter()
        for shard in range(1, shards + 1):
            directory = artifacts / f"test-results-{version}-{shard}"
            manifest_path = directory / "shard-manifest.json"
            manifest = _object(manifest_path)
            if (
                manifest.get("schema_version") != 1
                or type(manifest.get("exit_code")) is not int
                or manifest["exit_code"] != 0
                or manifest.get("python_version") != version
            ):
                raise ValueError(f"Invalid or unsuccessful shard manifest: {manifest_path}")
            full = _nodeids(manifest.get("full_nodeids"), str(manifest_path))
            selected = _nodeids(manifest.get("selected_nodeids"), str(manifest_path))
            if reference is None:
                reference = full
            elif Counter(full) != Counter(reference):
                raise ValueError(f"Full collections differ for Python {version}")
            if not set(selected) <= set(full):
                raise ValueError(f"Selected tests are outside the full collection: {manifest_path}")
            actual, skipped, test_seconds = _junit(directory / "test-results.xml")
            if Counter(actual) != Counter(selected):
                raise ValueError(f"JUnit tests differ from selected tests: {directory}")
            durations_path = directory / ".test_durations"
            durations = _object(durations_path)
            if set(durations) != set(selected):
                raise ValueError(f"Stored durations differ from selected tests: {durations_path}")
            duration_seconds = sum(
                _number(value, str(durations_path)) for value in durations.values()
            )
            executed.update(selected)
            total += len(selected)
            results.append(
                {
                    "python_version": version,
                    "shard": shard,
                    "selected": len(selected),
                    "passed": len(selected) - skipped,
                    "skipped": skipped,
                    "pytest_elapsed_seconds": _number(
                        manifest.get("elapsed_seconds"), str(manifest_path)
                    ),
                    "junit_test_seconds": test_seconds,
                    "stored_test_seconds": duration_seconds,
                }
            )
            if version == coverage_version:
                coverage = directory / f".coverage.{version}.{shard}"
                _coverage(coverage)
                coverage_files.append(str(coverage))
        if executed != Counter(reference or ()):
            raise ValueError(f"Shard union is not exactly the full collection for Python {version}")
    return {
        "schema_version": 1,
        "total_executions": total,
        "shards": results,
        "coverage_files": coverage_files,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--artifacts", type=Path, required=True)
    _ = parser.add_argument("--versions", nargs="+", required=True)
    _ = parser.add_argument("--shards", type=int, default=4)
    _ = parser.add_argument("--coverage-version", default="3.13.18")
    arguments = parser.parse_args(argv)
    try:
        result = audit(
            arguments.artifacts,
            arguments.versions,
            arguments.shards,
            coverage_version=arguments.coverage_version,
        )
    except (OSError, ValueError, ElementTree.ParseError) as error:
        print(f"Shard audit failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
