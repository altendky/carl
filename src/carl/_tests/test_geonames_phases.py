"""Frozen sources and fresh candidates survive GeoNames phase interruption."""

# Offline tests inspect candidate and immutable source evidence.
# pyright: reportPrivateUsage=false

import hashlib
import io
import json
import threading
import zipfile
from collections.abc import Mapping
from pathlib import Path

import anyio
import pytest

import carl.io.geonames as geonames
from carl._tests.test_geonames import _archive, _transport
from carl._tests.test_streaming_http import _held
from carl.core.dataset_downloads import FileReceipt
from carl.core.geonames_build import GeonamesBuildPlan, GeonamesCandidate
from carl.io.dataset_downloads import download_dataset_source
from carl.io.geonames import (
    GeonamesFailure,
    LocalGeonamesGeocoder,
    build_geonames_candidate,
    existing_geonames_dataset,
    geonames_dataset_identifier,
    prepare_geonames_build,
    publish_geonames_candidate,
    verify_geonames_candidate,
)
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def _sources(
    plan: GeonamesBuildPlan, directory: Path, *, bad_places: bytes | None = None
) -> tuple[FileReceipt, ...]:
    transport = _transport(bad_places=bad_places)
    return tuple(
        [
            await download_dataset_source(
                source,
                directory=directory,
                execution_identifier=f"source-{index}",
                transport=transport,
            )
            for index, source in enumerate(plan.sources)
        ]
    )


async def test_frozen_sources_exact_identity_and_publish_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        directory, root = tmp_path / "workflow", tmp_path / "published"
        plan = prepare_geonames_build()
        assert GeonamesBuildPlan.model_validate_json(plan.model_dump_json()) == plan
        receipts = await _sources(plan, directory)
        expected = (
            "geonames-"
            + hashlib.sha256(
                json.dumps(
                    {
                        "schema_version": 2,
                        "country_code": "US",
                        "sources": [
                            (source.url, receipt.sha256)
                            for source, receipt in zip(plan.sources, receipts, strict=True)
                        ],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()[:24]
        )
        monkeypatch.setattr(
            geonames, "_SOURCES", (("different.zip", "https://example.test/changed", 1),)
        )
        assert geonames_dataset_identifier(plan, receipts) == expected
        candidate = await build_geonames_candidate(
            plan, receipts, directory=directory, execution_identifier="geonames-0"
        )
        assert GeonamesCandidate.model_validate_json(candidate.model_dump_json()) == candidate

        def unexpected(*_args: object, **_kwargs: object):
            raise AssertionError("Completed candidates must never rerun SQLite construction")

        monkeypatch.setattr(geonames, "_build_database", unexpected)
        assert (
            await build_geonames_candidate(
                plan, receipts, directory=directory, execution_identifier="geonames-0"
            )
            == candidate
        )
        manifest = await publish_geonames_candidate(
            plan, receipts, candidate, directory=directory, root=root
        )
        assert not (directory / candidate.candidate_locator).exists()
        assert (
            await verify_geonames_candidate(
                plan, receipts, candidate, directory=directory, root=root
            )
            == manifest
        )
        assert (
            await publish_geonames_candidate(
                plan, receipts, candidate, directory=directory, root=root
            )
            == manifest
        )
        value = json.loads(manifest.read_text())
        manifest.parent.chmod(0o755)
        (manifest.parent / "places.sqlite3").chmod(0o644)
        manifest.chmod(0o644)
        assert await existing_geonames_dataset(plan, receipts, root=root) == manifest
        assert value["dataset_identifier"] == expected
        assert value["counts"] == {"places": 4, "postal_records": 2, "states": 3}
        assert all((directory / receipt.locator).exists() for receipt in receipts)
        assert {path.name for path in manifest.parent.iterdir()} == {
            "places.sqlite3",
            "manifest.json",
        }
        assert (
            await LocalGeonamesGeocoder(manifest.parent / "places.sqlite3").geocode(
                "Greencastle, PA", "US"
            )
        ).latitude == 39.8


async def test_partial_candidate_requires_fresh_execution_without_source_loss(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        directory = tmp_path / "workflow"
        plan = prepare_geonames_build()
        receipts = await _sources(plan, directory)
        for part in (
            directory / "candidates",
            directory / "candidates" / "geonames",
            directory / "candidates" / "geonames" / "geonames-0",
        ):
            part.mkdir(mode=0o700)
        partial = directory / "candidates" / "geonames" / "geonames-0" / "places.sqlite3"
        _ = partial.write_bytes(b"interrupted SQLite output")
        with pytest.raises(GeonamesFailure, match="geonames_candidate_uncertain"):
            _ = await build_geonames_candidate(
                plan, receipts, directory=directory, execution_identifier="geonames-0"
            )
        candidate = await build_geonames_candidate(
            plan, receipts, directory=directory, execution_identifier="geonames-1"
        )
        assert candidate.candidate_locator.endswith("geonames-1")
        assert partial.read_bytes() == b"interrupted SQLite output"
        assert all((directory / receipt.locator).exists() for receipt in receipts)


@pytest.mark.parametrize("damage", ["source", "database", "manifest"])
async def test_corrupt_inputs_or_candidate_never_publish(tmp_path: Path, damage: str) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        directory, root = tmp_path / "workflow", tmp_path / "published"
        plan = prepare_geonames_build()
        receipts = await _sources(plan, directory)
        if damage == "source":
            path = directory / receipts[0].locator
            _ = path.write_bytes(b"x" * receipts[0].bytes)
            with pytest.raises(GeonamesFailure):
                _ = await build_geonames_candidate(
                    plan, receipts, directory=directory, execution_identifier="geonames-0"
                )
        else:
            candidate = await build_geonames_candidate(
                plan, receipts, directory=directory, execution_identifier="geonames-0"
            )
            path = (
                directory
                / candidate.candidate_locator
                / ("places.sqlite3" if damage == "database" else "manifest.json")
            )
            _ = path.write_bytes(b"x" * path.stat().st_size)
            with pytest.raises(GeonamesFailure, match="geonames_invalid_dataset"):
                _ = await publish_geonames_candidate(
                    plan, receipts, candidate, directory=directory, root=root
                )
        assert not list(root.glob("geonames-*"))
        assert all((directory / receipt.locator).exists() for receipt in receipts)


@pytest.mark.parametrize("invalid", ["duplicate_member", "oversize_member", "long_row"])
async def test_zip_member_and_row_bounds_preserved(tmp_path: Path, invalid: str) -> None:
    if invalid == "duplicate_member":
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            archive.writestr("US.txt", "one")
            with pytest.warns(UserWarning, match="Duplicate name"):
                archive.writestr("US.txt", "two")
        content = stream.getvalue()
    elif invalid == "oversize_member":
        value = bytearray(_archive("small"))
        header = value.index(b"PK\x01\x02")
        value[header + 24 : header + 28] = (2_000_000_001).to_bytes(4, "little")
        content = bytes(value)
    else:
        content = _archive("x" * 100_001 + "\n")
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        directory = tmp_path / "workflow"
        plan = prepare_geonames_build()
        receipts = await _sources(plan, directory, bad_places=content)
        with pytest.raises(GeonamesFailure, match="geonames_invalid_source"):
            _ = await build_geonames_candidate(
                plan, receipts, directory=directory, execution_identifier="geonames-0"
            )


async def test_cancelled_sqlite_phase_waits_for_thread_and_completed_candidate_replays(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    original = geonames._build_database

    def blocked(
        stage: Path,
        identifier: str,
        *,
        source_paths: Mapping[str, Path] | None = None,
        source_receipts: Mapping[str, FileReceipt] | None = None,
        source_directory: Path | None = None,
    ) -> dict[str, int]:
        entered.set()
        assert release.wait(10)
        return original(
            stage,
            identifier,
            source_paths=source_paths,
            source_receipts=source_receipts,
            source_directory=source_directory,
        )

    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        directory = tmp_path / "workflow"
        plan = prepare_geonames_build()
        receipts = await _sources(plan, directory)
        monkeypatch.setattr(geonames, "_build_database", blocked)
        scopes: list[anyio.CancelScope] = []

        async def call() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                _ = await build_geonames_candidate(
                    plan, receipts, directory=directory, execution_identifier="geonames-0"
                )

        async with anyio.create_task_group() as tasks:
            _ = tasks.start_soon(call)
            try:
                with anyio.fail_after(10):
                    while not entered.is_set():
                        await anyio.sleep(0.005)
                scopes[0].cancel()
                await anyio.lowlevel.checkpoint()
                assert await _held(database, "cpu") == 1 and await _held(database, "http") == 0
            finally:
                release.set()
        assert await _held(database, "cpu") == 0
        monkeypatch.setattr(geonames, "_build_database", original)
        candidate = await build_geonames_candidate(
            plan, receipts, directory=directory, execution_identifier="geonames-0"
        )
        verified = await verify_geonames_candidate(plan, receipts, candidate, directory=directory)
        assert verified.is_file()


async def test_completed_marker_missing_provenance_is_not_relearned(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True):
        directory = tmp_path / "workflow"
        plan = prepare_geonames_build()
        receipts = await _sources(plan, directory)
        candidate = await build_geonames_candidate(
            plan, receipts, directory=directory, execution_identifier="geonames-0"
        )
        path = directory / candidate.candidate_locator / "manifest.json"
        value = json.loads(path.read_text())
        del value["license_url"]
        _ = path.write_text(json.dumps(value))
        with pytest.raises(GeonamesFailure, match="geonames_invalid_dataset"):
            _ = await build_geonames_candidate(
                plan, receipts, directory=directory, execution_identifier="geonames-0"
            )
