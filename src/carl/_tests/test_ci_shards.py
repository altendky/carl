"""Shard audits require exact collection and execution identities, including skips."""

# Tiny pytest sessions exercise the real splitting, hook order and JUnit output.
# pyright: reportPrivateUsage=false

import json
import platform
from collections import Counter
from pathlib import Path
from typing import cast
from xml.etree import ElementTree

import pytest
from coverage import CoverageData

from carl._tests import ci_shards

pytest_plugins = ("pytester",)
VERSIONS = ("3.13.18", "3.14.10")
NODEIDS = tuple(f"test_example.py::test_item[{index}]" for index in range(8))


def _json(path: Path, value: object) -> None:
    _ = path.write_text(json.dumps(value))


def _junit(
    path: Path,
    selected: list[str],
    *,
    skip: str | None = None,
    error: str | None = None,
) -> None:
    root = ElementTree.Element("testsuites")
    suite = ElementTree.SubElement(root, "testsuite")
    for nodeid in selected:
        case = ElementTree.SubElement(suite, "testcase", name="ignored-name", time="0.125")
        properties = ElementTree.SubElement(case, "properties")
        _ = ElementTree.SubElement(properties, "property", name="carl_nodeid", value=nodeid)
        if nodeid == skip:
            _ = ElementTree.SubElement(case, "skipped")
        if error is not None:
            _ = ElementTree.SubElement(case, error)
    ElementTree.ElementTree(root).write(path)


def _shard(root: Path, version: str = VERSIONS[0], shard: int = 1) -> Path:
    return root / f"test-results-{version}-{shard}"


def _coverage(path: Path, *, branches: bool = True) -> None:
    data = CoverageData(basename=str(path))
    if branches:
        data.add_arcs({"fixture.py": {(1, 2), (2, -1)}})
    else:
        data.add_lines({"fixture.py": {1, 2}})
    data.write()


def _artifacts(root: Path) -> Path:
    for version in VERSIONS:
        for shard in range(1, 5):
            directory = _shard(root, version, shard)
            directory.mkdir(parents=True)
            selected = list(NODEIDS[(shard - 1) * 2 : shard * 2])
            _json(
                directory / "shard-manifest.json",
                {
                    "schema_version": 1,
                    "python_version": version,
                    "full_nodeids": list(NODEIDS),
                    "selected_nodeids": selected,
                    "elapsed_seconds": 0.5,
                    "exit_code": 0,
                },
            )
            _junit(directory / "test-results.xml", selected, skip=NODEIDS[-1])
            _json(directory / ".test_durations", dict.fromkeys(selected, 0.125))
            if version == VERSIONS[0]:
                _coverage(directory / f".coverage.{version}.{shard}")
    return root


def test_complete_audit_reports_counts_skips_times_and_coverage_inputs(tmp_path: Path) -> None:
    result = ci_shards.audit(_artifacts(tmp_path), VERSIONS, 4)
    assert result["total_executions"] == 16
    assert len(result["coverage_files"]) == 4
    shards = result["shards"]
    assert len(shards) == 8
    assert sum(shard["passed"] for shard in shards) == 14
    assert sum(shard["skipped"] for shard in shards) == 2
    assert all(shard["pytest_elapsed_seconds"] == 0.5 for shard in shards)
    assert all(
        shard["junit_test_seconds"] == shard["stored_test_seconds"] == 0.25 for shard in shards
    )


@pytest.mark.parametrize(
    "filename",
    ("shard-manifest.json", "test-results.xml", ".test_durations", ".coverage.3.13.18.1"),
)
def test_missing_artifact_is_rejected(tmp_path: Path, filename: str) -> None:
    _ = _artifacts(tmp_path)
    (_shard(tmp_path) / filename).unlink()
    with pytest.raises((OSError, ValueError)):
        _ = ci_shards.audit(tmp_path, VERSIONS, 4)


def _manifest(directory: Path) -> dict[str, object]:
    return json.loads((directory / "shard-manifest.json").read_text())


def test_equal_total_with_duplicate_and_omitted_shard_tests_is_rejected(tmp_path: Path) -> None:
    _ = _artifacts(tmp_path)
    directory = _shard(tmp_path, shard=4)
    manifest = _manifest(directory)
    selected = list(NODEIDS[:2])
    manifest["selected_nodeids"] = selected
    _json(directory / "shard-manifest.json", manifest)
    _junit(directory / "test-results.xml", selected)
    _json(directory / ".test_durations", dict.fromkeys(selected, 0.125))
    # Every shard's report is internally consistent and the total still equals
    # eight; identity comparison must catch two duplicates and two omissions.
    with pytest.raises(ValueError, match="union"):
        _ = ci_shards.audit(tmp_path, VERSIONS, 4)


@pytest.mark.parametrize("field", ("full_nodeids", "selected_nodeids"))
@pytest.mark.parametrize("malformed", ("duplicate", "empty"))
def test_manifest_requires_nonempty_unique_nodeids(
    tmp_path: Path, field: str, malformed: str
) -> None:
    _ = _artifacts(tmp_path)
    directory = _shard(tmp_path)
    manifest = _manifest(directory)
    manifest[field] = [NODEIDS[0], NODEIDS[0]] if malformed == "duplicate" else []
    _json(directory / "shard-manifest.json", manifest)
    with pytest.raises(ValueError, match="nodeid"):
        _ = ci_shards.audit(tmp_path, VERSIONS, 4)


def test_equivalent_full_collection_order_is_accepted(tmp_path: Path) -> None:
    _ = _artifacts(tmp_path)
    directory = _shard(tmp_path, shard=2)
    manifest = _manifest(directory)
    manifest["full_nodeids"] = list(reversed(NODEIDS))
    _json(directory / "shard-manifest.json", manifest)
    assert ci_shards.audit(tmp_path, VERSIONS, 4)["total_executions"] == 16


def test_missing_nodeid_in_another_full_collection_is_rejected(tmp_path: Path) -> None:
    _ = _artifacts(tmp_path)
    directory = _shard(tmp_path, shard=2)
    manifest = _manifest(directory)
    manifest["full_nodeids"] = list(NODEIDS[:-1])
    _json(directory / "shard-manifest.json", manifest)
    with pytest.raises(ValueError, match="collections differ"):
        _ = ci_shards.audit(tmp_path, VERSIONS, 4)


@pytest.mark.parametrize("malformed", ("missing", "duplicate", "outside"))
def test_junit_must_match_selected_nodeids_exactly(tmp_path: Path, malformed: str) -> None:
    _ = _artifacts(tmp_path)
    selected = list(NODEIDS[:1]) if malformed == "missing" else [NODEIDS[0], NODEIDS[0]]
    if malformed == "outside":
        selected = list(NODEIDS[2:4])
    _junit(_shard(tmp_path) / "test-results.xml", selected)
    with pytest.raises(ValueError, match="JUnit tests differ"):
        _ = ci_shards.audit(tmp_path, VERSIONS, 4)


@pytest.mark.parametrize("malformed", ("missing", "duplicate"))
def test_junit_requires_exactly_one_stable_nodeid_property(tmp_path: Path, malformed: str) -> None:
    _ = _artifacts(tmp_path)
    path = _shard(tmp_path) / "test-results.xml"
    tree = ElementTree.parse(path)
    properties = tree.getroot().find("./testsuite/testcase/properties")
    assert properties is not None
    if malformed == "missing":
        properties.clear()
    else:
        _ = ElementTree.SubElement(properties, "property", name="carl_nodeid", value=NODEIDS[0])
    tree.write(path)
    with pytest.raises(ValueError, match="property"):
        _ = ci_shards.audit(tmp_path, VERSIONS, 4)


@pytest.mark.parametrize("error", ("failure", "error"))
def test_failed_testcases_are_rejected_even_if_manifest_claims_success(
    tmp_path: Path, error: str
) -> None:
    _ = _artifacts(tmp_path)
    _junit(_shard(tmp_path) / "test-results.xml", list(NODEIDS[:2]), error=error)
    with pytest.raises(ValueError, match="failed or errored"):
        _ = ci_shards.audit(tmp_path, VERSIONS, 4)


@pytest.mark.parametrize(
    "field,value",
    (("exit_code", 1), ("exit_code", None), ("exit_code", False), ("python_version", "3.14.10")),
)
def test_unsuccessful_or_wrong_version_manifest_is_rejected(
    tmp_path: Path, field: str, value: object
) -> None:
    _ = _artifacts(tmp_path)
    directory = _shard(tmp_path)
    manifest = _manifest(directory)
    manifest[field] = value
    _json(directory / "shard-manifest.json", manifest)
    with pytest.raises(ValueError, match="manifest"):
        _ = ci_shards.audit(tmp_path, VERSIONS, 4)


def test_duration_artifact_must_contain_only_this_shards_measurements(tmp_path: Path) -> None:
    _ = _artifacts(tmp_path)
    _json(_shard(tmp_path) / ".test_durations", dict.fromkeys(NODEIDS, 0.125))
    with pytest.raises(ValueError, match="durations differ"):
        _ = ci_shards.audit(tmp_path, VERSIONS, 4)


@pytest.mark.parametrize("malformed", ("corrupt", "lines_only", "empty"))
def test_one_invalid_coverage_input_cannot_silently_reduce_combined_coverage(
    tmp_path: Path, malformed: str
) -> None:
    _ = _artifacts(tmp_path)
    path = _shard(tmp_path) / f".coverage.{VERSIONS[0]}.1"
    path.unlink()
    if malformed == "corrupt":
        _ = path.write_bytes(b"corrupt nonempty coverage data")
    elif malformed == "empty":
        path.touch()
    else:
        _coverage(path, branches=False)
    with pytest.raises(ValueError, match=r"[Cc]overage"):
        _ = ci_shards.audit(tmp_path, VERSIONS, 4)


@pytest.mark.parametrize("duration", (-1.0, float("nan"), True))
def test_invalid_duration_values_are_rejected(tmp_path: Path, duration: object) -> None:
    _ = _artifacts(tmp_path)
    _json(_shard(tmp_path) / ".test_durations", {NODEIDS[0]: duration, NODEIDS[1]: 0.125})
    with pytest.raises(ValueError, match="duration"):
        _ = ci_shards.audit(tmp_path, VERSIONS, 4)


def test_cli_emits_summary_and_fails_closed_on_missing_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _ = _artifacts(tmp_path)
    arguments = ["--artifacts", str(tmp_path), "--versions", *VERSIONS, "--shards", "4"]
    assert ci_shards.main(arguments) == 0
    assert json.loads(capsys.readouterr().out)["total_executions"] == 16
    (_shard(tmp_path) / "test-results.xml").unlink()
    assert ci_shards.main(arguments) == 1
    assert "Shard audit failed" in capsys.readouterr().err


def test_manifest_plugin_is_inert_without_its_flag(pytester: pytest.Pytester) -> None:
    _ = pytester.makepyfile("def test_plain(): pass")
    result = pytester.runpytest("-p", "carl._tests.ci_shards", "--junitxml=plain.xml")
    result.assert_outcomes(passed=1)
    assert not (pytester.path / "shard-manifest.json").exists()
    assert "carl_nodeid" not in (pytester.path / "plain.xml").read_text()


def test_conftest_plugin_load_preserves_package_initialization_coverage(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    coverage_path = pytester.path / "child.coverage"
    monkeypatch.setenv("COVERAGE_FILE", str(coverage_path))
    _ = pytester.makeconftest('pytest_plugins = ("carl._tests.ci_shards",)')
    _ = pytester.makepyfile(
        """
        def test_version():
            import carl
            assert isinstance(carl.__version__, str) and carl.__version__
        """
    )
    result = pytester.runpytest_subprocess(
        "--shard-manifest=shard-manifest.json",
        "--junitxml=test-results.xml",
        "--cov=carl",
        "--cov-report=",
        "--cov-branch",
    )
    result.assert_outcomes(passed=1)
    manifest = _manifest(pytester.path)
    assert manifest["exit_code"] == 0
    assert len(cast(list[str], manifest["full_nodeids"])) == 1
    assert manifest["selected_nodeids"] == manifest["full_nodeids"]
    executed, skipped, _ = ci_shards._junit(pytester.path / "test-results.xml")
    assert executed == manifest["selected_nodeids"] and skipped == 0
    data = CoverageData(basename=str(coverage_path))
    data.read()
    package_init = Path(ci_shards.__file__).resolve().parents[1] / "__init__.py"
    # The package currently has one executable statement: its version assignment.
    assert data.has_arcs() and data.lines(str(package_init))


def test_real_split_hook_and_junit_keep_all_selected_skips(pytester: pytest.Pytester) -> None:
    _ = pytester.makepyfile(
        """
        import pytest
        @pytest.mark.parametrize('number', range(8))
        def test_number(number):
            if number == 7:
                pytest.skip('planned skip')
        """
    )
    version = platform.python_version()
    artifacts = pytester.path / "artifacts"
    for shard in range(1, 5):
        directory = _shard(artifacts, version, shard)
        directory.mkdir(parents=True)
        result = pytester.runpytest(
            "-p",
            "carl._tests.ci_shards",
            "--splits=4",
            f"--group={shard}",
            "--splitting-algorithm=least_duration",
            f"--shard-manifest={directory / 'shard-manifest.json'}",
            f"--junitxml={directory / 'test-results.xml'}",
            "--store-durations",
            "--clean-durations",
            f"--durations-path={directory / '.test_durations'}",
        )
        assert result.ret == pytest.ExitCode.OK
        _coverage(directory / f".coverage.{version}.{shard}")
    result = ci_shards.audit(artifacts, (version,), 4, coverage_version=version)
    assert result["total_executions"] == 8
    shards = result["shards"]
    assert sum(shard["passed"] for shard in shards) == 7
    assert sum(shard["skipped"] for shard in shards) == 1
    manifests = [_manifest(_shard(artifacts, version, shard)) for shard in range(1, 5)]
    assert Counter(
        nodeid for manifest in manifests for nodeid in cast(list[str], manifest["selected_nodeids"])
    ) == Counter(cast(list[str], manifests[0]["full_nodeids"]))
