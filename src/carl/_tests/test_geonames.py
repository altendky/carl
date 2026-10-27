"""Full-country builds are transactional; locality lookups remain offline/exact."""

import io
import json
import sqlite3
import zipfile
from pathlib import Path

import anyio
import httpx
import pytest

from carl.core.travel import TravelCoordinate
from carl.io.geonames import GeonamesFailure, LocalGeonamesGeocoder, build_geonames_dataset


def _archive(contents: str, *, member: str = "US.txt") -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(zipfile.ZipInfo(member, date_time=(2026, 10, 18, 0, 0, 0)), contents)
        # This is never extracted or read.
        archive.writestr(
            zipfile.ZipInfo("../../should-not-exist.txt", date_time=(2026, 10, 18, 0, 0, 0)),
            "untrusted",
        )
    return stream.getvalue()


def _place(
    identifier: int,
    name: str,
    state: str,
    latitude: float,
    *,
    aliases: str = "",
    feature: str = "P",
) -> str:
    return "\t".join(
        [
            str(identifier),
            name,
            name,
            aliases,
            str(latitude),
            "-77.7",
            feature,
            "PPL",
            "US",
            "",
            state,
            "001",
            "",
            "",
            "1000",
            "",
            "100",
            "America/New_York",
            "2026-10-01",
        ]
    )


def _postal(code: str, name: str, state: str, latitude: float) -> str:
    return "\t".join(
        ["US", code, name, state, state, "County", "001", "", "", str(latitude), "-77.7", "4"]
    )


def _transport(
    *, extra: str = "", bad_places: bytes | None = None, status: int = 200
) -> httpx.MockTransport:
    places = "\n".join(
        [
            _place(1, "Greencastle", "PA", 39.8, aliases="Green Castle,Gréencastle"),
            _place(2, "Springfield", "PA", 40),
            _place(3, "Springfield", "IL", 39),
            _place(4, "Hidden Hamlet", "PA", 40.5),
            _place(5, "Not A City", "PA", 40, feature="S"),
            extra,
        ]
    )
    postal = "\n".join(
        [_postal("17225", "Greencastle", "PA", 39.81), _postal("62701", "Springfield", "IL", 39.01)]
    )
    admin = "US.PA\tPennsylvania\tPennsylvania\t1\nUS.IL\tIllinois\tIllinois\t2\nCA.ON\tOntario\tOntario\t3\nUS.MD\tMaryland\tMaryland\t4\n"

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("admin1CodesASCII.txt"):
            data = admin.encode()
        elif request.url.path.startswith("/export/zip/"):
            data = _archive(postal)
        else:
            data = bad_places if bad_places is not None else _archive(places)
        return httpx.Response(
            status,
            content=data,
            headers={"last-modified": "Sun, 18 Oct 2026 04:00:00 GMT", "etag": '"fixture"'},
        )

    return httpx.MockTransport(respond)


@pytest.fixture
async def manifest(tmp_path: Path) -> Path:
    return await build_geonames_dataset(tmp_path / "geonames", transport=_transport())


@pytest.mark.anyio
@pytest.mark.parametrize(
    "label",
    [
        "Greencastle, PA",
        "Greencastle, Pennsylvania",
        "Greencastle PA",
        "Greencastle Pennsylvania",
        "Greencastle, PA, US",
        "Greencastle PA USA",
        "Greencastle, Pennsylvania, United States",
        "Green Castle, PA",
        "GRÉENCASTLE, pa",
        "Greencastle PA U.S.A.",
        "Greencastle, PA US",
    ],
)
async def test_city_state_aliases_and_country_suffixes(manifest: Path, label: str) -> None:
    geocoder = LocalGeonamesGeocoder(manifest.parent / "places.sqlite3")
    assert await geocoder.geocode(label, "US") == TravelCoordinate(latitude=39.8, longitude=-77.7)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "label",
    [
        "17225",
        "17225-1234",
        "17225-1234 USA",
        "Greencastle PA 17225",
        "Greencastle, PA 17225-1234, US",
        "PA 17225",
        "Greencastle, PA 17225-1234 USA",
    ],
)
async def test_zip_codes_are_approximate_postal_coordinates(manifest: Path, label: str) -> None:
    geocoder = LocalGeonamesGeocoder(manifest.parent / "places.sqlite3")
    assert await geocoder.geocode(label, None) == TravelCoordinate(latitude=39.81, longitude=-77.7)


@pytest.mark.anyio
async def test_ambiguous_names_are_not_ranked_by_proximity_population_or_state(
    manifest: Path,
) -> None:
    geocoder = LocalGeonamesGeocoder(manifest.parent / "places.sqlite3")
    with pytest.raises(GeonamesFailure, match=r"^geonames_location_ambiguous$"):
        await geocoder.geocode("Springfield", "US")
    assert (await geocoder.geocode("Springfield, IL", "US")).latitude == 39
    assert (await geocoder.geocode("Hidden Hamlet, PA", "US")).latitude == 40.5


@pytest.mark.anyio
@pytest.mark.parametrize(
    "label",
    [
        "Not A City, PA",
        "Greencastel PA",
        "123 Main St, Greencastle, PA",
        "Greencastle, ZZ",
        "Greencastle, PA, Canada",
        "Greencastle IL 17225",
        "Springfield PA 62701",
        "",
        "Greencastle,,PA",
    ],
)
async def test_no_fuzzy_or_street_address_guesses(manifest: Path, label: str) -> None:
    geocoder = LocalGeonamesGeocoder(manifest.parent / "places.sqlite3")
    with pytest.raises(GeonamesFailure, match=r"^geonames_location_not_found$"):
        await geocoder.geocode(label, "US")


@pytest.mark.anyio
async def test_country_scope_and_missing_corrupt_databases(tmp_path: Path, manifest: Path) -> None:
    path = tmp_path / "missing.sqlite3"
    with pytest.raises(GeonamesFailure, match=r"^geonames_not_ready$"):
        await LocalGeonamesGeocoder(path).geocode("Greencastle PA", "US")
    assert not path.exists()
    _ = path.write_text("not sqlite")
    with pytest.raises(GeonamesFailure, match=r"^geonames_invalid_dataset$"):
        await LocalGeonamesGeocoder(path).geocode("Greencastle PA", "US")
    with pytest.raises(GeonamesFailure, match=r"^geonames_country_not_supported$"):
        await LocalGeonamesGeocoder(manifest.parent / "places.sqlite3").geocode(
            "Greencastle PA", "CA"
        )


@pytest.mark.anyio
async def test_published_dataset_provenance_idempotence_and_changed_source(tmp_path: Path) -> None:
    root = tmp_path / "datasets"
    first = await build_geonames_dataset(root, transport=_transport())
    original_bytes = first.read_bytes()
    data = json.loads(original_bytes)
    assert data["provider"] == "geonames" and data["country_code"] == "US"
    assert data["counts"] == {"places": 4, "postal_records": 2, "states": 3}
    assert len(data["sources"]) == 3 and all(
        len(source["sha256"]) == 64 for source in data["sources"]
    )
    assert all(source["last_modified"] and source["etag"] for source in data["sources"])
    assert data["license_url"] == "https://creativecommons.org/licenses/by/4.0/"
    assert set(path.name for path in first.parent.iterdir()) == {"manifest.json", "places.sqlite3"}
    assert not (tmp_path / "should-not-exist.txt").exists()
    assert await build_geonames_dataset(root, transport=_transport()) == first
    assert first.read_bytes() == original_bytes
    changed = await build_geonames_dataset(
        root, transport=_transport(extra=_place(99, "New Place", "PA", 40))
    )
    assert changed != first and first.read_bytes() == original_bytes
    assert all(not path.name.startswith(".building") for path in root.iterdir())
    progress = json.loads((root / "progress.json").read_text())
    assert progress["stage"] == "completed" and progress["reason"] is None
    assert progress["dataset_identifier"] == changed.parent.name


@pytest.mark.anyio
@pytest.mark.parametrize(
    "bad_places", [b"not a zip", _archive("bad\trow"), _archive("data", member="../US.txt")]
)
async def test_failed_build_never_publishes_or_leaves_stages(
    tmp_path: Path, bad_places: bytes
) -> None:
    with pytest.raises(GeonamesFailure):
        await build_geonames_dataset(tmp_path, transport=_transport(bad_places=bad_places))
    assert [path.name for path in tmp_path.iterdir()] == ["progress.json"]
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["stage"] == "failed" and progress["reason"].startswith("geonames_")


@pytest.mark.anyio
async def test_http_failures_are_sanitized_and_failed_refresh_preserves_existing_dataset(
    tmp_path: Path,
) -> None:
    original = await build_geonames_dataset(tmp_path, transport=_transport())
    before = original.read_bytes()
    with pytest.raises(GeonamesFailure, match=r"^geonames_download_http_503$"):
        await build_geonames_dataset(tmp_path, transport=_transport(status=503))
    assert original.read_bytes() == before
    assert {path.name for path in tmp_path.iterdir()} == {original.parent.name, "progress.json"}
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["stage"] == "failed" and progress["reason"] == "geonames_download_http_503"


@pytest.mark.anyio
async def test_lookup_has_no_http_client_or_database_writes(
    manifest: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("Offline lookup must not create an HTTP client")

    monkeypatch.setattr(httpx, "AsyncClient", forbidden)
    database = manifest.parent / "places.sqlite3"
    before = database.read_bytes()
    assert (await LocalGeonamesGeocoder(database).geocode("Greencastle PA", None)).latitude == 39.8
    assert database.read_bytes() == before
    assert not database.with_name("places.sqlite3-journal").exists()


@pytest.mark.anyio
async def test_same_state_duplicates_remain_ambiguous_and_corrupt_version_is_refused(
    tmp_path: Path,
) -> None:
    manifest = await build_geonames_dataset(
        tmp_path, transport=_transport(extra=_place(99, "Greencastle", "PA", 42))
    )
    geocoder = LocalGeonamesGeocoder(manifest.parent / "places.sqlite3")
    with pytest.raises(GeonamesFailure, match=r"^geonames_location_ambiguous$"):
        await geocoder.geocode("Greencastle, PA", "US")
    with sqlite3.connect(manifest.parent / "places.sqlite3") as connection:
        connection.execute("UPDATE metadata SET value = '999' WHERE key = 'schema_version'")
    with pytest.raises(GeonamesFailure, match=r"^geonames_invalid_dataset$"):
        await geocoder.geocode("Greencastle PA", "US")
    with pytest.raises(GeonamesFailure, match=r"^geonames_invalid_dataset$"):
        await build_geonames_dataset(
            tmp_path, transport=_transport(extra=_place(99, "Greencastle", "PA", 42))
        )


@pytest.mark.anyio
async def test_parallel_builders_publish_one_complete_version(tmp_path: Path) -> None:
    results: list[Path] = []

    async def build() -> None:
        results.append(await build_geonames_dataset(tmp_path, transport=_transport()))

    async with anyio.create_task_group() as group:
        for _ in range(3):
            group.start_soon(build)
    assert len(results) == 3 and len(set(results)) == 1
    assert {path.name for path in tmp_path.iterdir()} == {results[0].parent.name, "progress.json"}
    assert json.loads((tmp_path / "progress.json").read_text())["stage"] == "completed"
    assert (
        await LocalGeonamesGeocoder(results[0].parent / "places.sqlite3").geocode("17225", "US")
    ).latitude == 39.81


@pytest.mark.anyio
async def test_cancelled_download_cleans_its_partial_stage(tmp_path: Path) -> None:
    class WaitingStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"partial zip"
            await anyio.sleep_forever()

    transport = httpx.MockTransport(lambda _: httpx.Response(200, stream=WaitingStream()))
    with pytest.raises(TimeoutError):
        with anyio.fail_after(0.05):
            await build_geonames_dataset(tmp_path, transport=transport)
    assert [path.name for path in tmp_path.iterdir()] == ["progress.json"]
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["stage"] == "cancelled" and progress["reason"] == "geonames_build_cancelled"


@pytest.mark.anyio
async def test_network_errors_do_not_echo_upstream_details(tmp_path: Path) -> None:
    def fail(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("untrusted upstream text")

    with pytest.raises(GeonamesFailure, match=r"^geonames_download_failed$"):
        await build_geonames_dataset(tmp_path, transport=httpx.MockTransport(fail))
    assert [path.name for path in tmp_path.iterdir()] == ["progress.json"]
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["stage"] == "failed" and progress["reason"] == "geonames_download_failed"
    assert "untrusted" not in (tmp_path / "progress.json").read_text()


@pytest.mark.anyio
async def test_download_limit_and_country_rejection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "carl.io.geonames._SOURCES",
        (("places.zip", "https://download.geonames.org/export/dump/US.zip", 1),),
    )
    with pytest.raises(GeonamesFailure, match=r"^geonames_download_too_large$"):
        await build_geonames_dataset(tmp_path, transport=_transport())
    with pytest.raises(GeonamesFailure, match=r"^geonames_country_not_supported$"):
        await build_geonames_dataset(tmp_path, country_code="../../unsafe", transport=_transport())
    assert [path.name for path in tmp_path.iterdir()] == ["progress.json"]
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["stage"] == "failed" and progress["reason"] == "geonames_country_not_supported"


@pytest.mark.anyio
async def test_progress_reports_download_sources_and_database_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from carl.io import geonames

    stages: list[dict[str, object]] = []
    original_progress = geonames._progress

    def observe(root: Path, build_identifier: str, stage: str, **details: object) -> None:
        original_progress(root, build_identifier, stage, **details)
        stages.append(json.loads((root / "progress.json").read_text()))

    monkeypatch.setattr(geonames, "_progress", observe)
    await build_geonames_dataset(tmp_path, transport=_transport())
    assert [item["stage"] for item in stages] == [
        "download",
        "download",
        "download",
        "build_database",
        "completed",
    ]
    assert [item["name"] for item in stages[:3]] == ["places.zip", "postal.zip", "admin1.txt"]
    assert [item["source_number"] for item in stages[:3]] == [1, 2, 3]
    assert all(item["source_count"] == 3 for item in stages[:3])
    assert len({item["build_identifier"] for item in stages}) == 1
    assert all(item["updated_at_utc"] and item["provider"] == "geonames" for item in stages)


@pytest.mark.anyio
async def test_canonical_cities_take_precedence_over_other_places_alternate_names(
    tmp_path: Path,
) -> None:
    # Public GeoNames IDs: Vienna has alternate name Baltimore; Fredericktown has Frederick.
    # https://sws.geonames.org/4372341/about.rdf
    # https://sws.geonames.org/4355627/about.rdf
    records = "\n".join(
        [
            _place(4347778, "Baltimore", "MD", 39.29038, aliases="Charm City"),
            _place(4372341, "Vienna", "MD", 38.48484, aliases="Baltimore"),
            _place(4355585, "Frederick", "MD", 39.41427),
            _place(4355627, "Fredericktown", "MD", 39.36622, aliases="Frederick"),
        ]
    )
    manifest = await build_geonames_dataset(tmp_path, transport=_transport(extra=records))
    assert json.loads(manifest.read_text())["schema_version"] == 2
    geocoder = LocalGeonamesGeocoder(manifest.parent / "places.sqlite3")
    assert (await geocoder.geocode("Baltimore, MD", "US")).latitude == 39.29038
    assert (await geocoder.geocode("Frederick, Maryland", "US")).latitude == 39.41427
    assert (await geocoder.geocode("Charm City, MD", "US")).latitude == 39.29038
    assert (await geocoder.geocode("Vienna, MD", "US")).latitude == 38.48484


@pytest.mark.anyio
async def test_multiple_canonical_matches_and_multiple_alias_matches_remain_ambiguous(
    tmp_path: Path,
) -> None:
    records = "\n".join(
        [
            _place(101, "Duplicate", "MD", 39.1, aliases="Shared Nickname"),
            _place(102, "Duplicate", "MD", 39.2, aliases="Shared Nickname"),
            _place(103, "Another Town", "MD", 39.3, aliases="Duplicate"),
        ]
    )
    manifest = await build_geonames_dataset(tmp_path, transport=_transport(extra=records))
    geocoder = LocalGeonamesGeocoder(manifest.parent / "places.sqlite3")
    for label in ("Duplicate, MD", "Shared Nickname, MD"):
        with pytest.raises(GeonamesFailure, match=r"^geonames_location_ambiguous$"):
            await geocoder.geocode(label, "US")


@pytest.mark.anyio
async def test_previous_schema_is_readable_with_its_original_ambiguity_rules(
    tmp_path: Path,
) -> None:
    records = "\n".join(
        [
            _place(4347778, "Baltimore", "MD", 39.29038),
            _place(4372341, "Vienna", "MD", 38.48484, aliases="Baltimore"),
        ]
    )
    manifest = await build_geonames_dataset(tmp_path, transport=_transport(extra=records))
    database = manifest.parent / "places.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.executescript("""
            ALTER TABLE names RENAME TO names_v2;
            CREATE TABLE names AS SELECT name, place_identifier FROM names_v2;
            DROP TABLE names_v2;
            UPDATE metadata SET value = '1' WHERE key = 'schema_version';
        """)
    geocoder = LocalGeonamesGeocoder(database)
    assert (await geocoder.geocode("Greencastle PA", "US")).latitude == 39.8
    with pytest.raises(GeonamesFailure, match=r"^geonames_location_ambiguous$"):
        await geocoder.geocode("Baltimore, MD", "US")
