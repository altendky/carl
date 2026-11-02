"""Immutable GeoNames locality datasets and exact, entirely offline lookup.

Coordinates describe localities or ZIP areas, never a seller's street address.
No proximity or population ranking resolves ambiguous names implicitly.
"""

import hashlib
import io
import json
import math
import os
import re
import shutil
import sqlite3
import stat
import unicodedata
import uuid
import zipfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import anyio
import httpx

from carl.core.dataset_downloads import (
    DatasetFileError,
    DatasetSource,
    FileReceipt,
    dataset_source_fingerprint,
    validate_dataset_execution_identifier,
)
from carl.core.geonames_build import GeonamesBuildPlan, GeonamesCandidate, GeonamesCounts
from carl.core.travel import TravelCoordinate
from carl.io.dataset_downloads import download_dataset_source, geonames_download_metadata
from carl.io.resource_permits import ResourceLeaseLost, current_resource_manager, run_cpu
from carl.io.streaming_http import open_dataset_directory, open_dataset_file

_SCHEMA_VERSION = 2
_DATABASE_NAME = "places.sqlite3"
_COUNTRY_NAMES = frozenset({"us", "usa", "united states", "united states of america"})
_ZIP = re.compile(r"(?<!\d)(\d{5})(?:-\d{4})?$")
_SOURCE_BASE = "https://download.geonames.org/export"
_SOURCES = (
    ("places.zip", f"{_SOURCE_BASE}/dump/US.zip", 160_000_000),
    ("postal.zip", f"{_SOURCE_BASE}/zip/US.zip", 10_000_000),
    ("admin1.txt", f"{_SOURCE_BASE}/dump/admin1CodesASCII.txt", 10_000_000),
)


class GeonamesFailure(Exception):
    """A stable reason suitable for user-visible unavailable results."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _normalize(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value).casefold()
    # Periods/apostrophes disappear; other punctuation separates words.
    return " ".join(
        "".join(
            character
            if character.isalnum()
            else ""
            if unicodedata.combining(character) or character in ".'\u2019"
            else " "
            for character in decomposed
        ).split()
    )


def _read_metadata(connection: sqlite3.Connection) -> dict[str, str]:
    values = dict(connection.execute("SELECT key, value FROM metadata"))
    if values.get("schema_version") not in {"1", "2"} or values.get("country_code") != "US":
        raise GeonamesFailure("geonames_invalid_dataset")
    if not values.get("dataset_identifier"):
        raise GeonamesFailure("geonames_invalid_dataset")
    return values


class LocalGeonamesGeocoder:
    """Read a published database without creating files or using a network."""

    def __init__(self, database_path: Path):
        self.database_path = database_path

    async def geocode(self, label: str, country_code: str | None) -> TravelCoordinate:
        return await run_cpu(self._geocode, label, country_code)

    def _geocode(self, label: str, country_code: str | None) -> TravelCoordinate:
        if country_code is not None and _normalize(country_code) not in _COUNTRY_NAMES:
            raise GeonamesFailure("geonames_country_not_supported")
        if not label.strip() or len(label) > 500:
            raise GeonamesFailure("geonames_location_not_found")
        if not self.database_path.is_file():
            raise GeonamesFailure("geonames_not_ready")
        try:
            uri = self.database_path.resolve().as_uri() + "?mode=ro&immutable=1"
            with closing(sqlite3.connect(uri, uri=True)) as connection:
                metadata = _read_metadata(connection)
                states = dict(connection.execute("SELECT name, code FROM state_names"))
                city, state, postal = _parse_label(label, states)
                if postal is not None:
                    query = "SELECT DISTINCT latitude, longitude FROM postal WHERE postal_code = ?"
                    parameters: list[str] = [postal]
                    if state is not None:
                        query += " AND state_code = ?"
                        parameters.append(state)
                    if city:
                        query += " AND name = ?"
                        parameters.append(city)
                    rows = connection.execute(query + " LIMIT 2", parameters).fetchall()
                else:
                    priority = "n.is_canonical" if metadata["schema_version"] == "2" else "0"
                    query = (
                        f"SELECT DISTINCT p.identifier, p.latitude, p.longitude, {priority} AS canonical "
                        "FROM names AS n JOIN places AS p ON p.identifier = n.place_identifier "
                        "WHERE n.name = ?"
                    )
                    parameters = [city]
                    if state is not None:
                        query += " AND p.state_code = ?"
                        parameters.append(state)
                    matches = connection.execute(
                        query + " ORDER BY canonical DESC LIMIT 2", parameters
                    ).fetchall()
                    # Primary/ascii names are explicit source evidence, unlike popularity or proximity.
                    # Keep uncertainty within the best matching tier; aliases only fill absent canonical names.
                    rows = [(row[1], row[2]) for row in matches if row[3] == matches[0][3]]
                if not rows:
                    raise GeonamesFailure("geonames_location_not_found")
                if len(rows) != 1:
                    raise GeonamesFailure("geonames_location_ambiguous")
                return TravelCoordinate(latitude=rows[0][0], longitude=rows[0][1])
        except (sqlite3.Error, OSError, ValueError, TypeError) as error:
            raise GeonamesFailure("geonames_invalid_dataset") from error


def _parse_label(label: str, states: dict[str, str]) -> tuple[str, str | None, str | None]:
    parts = [part.strip() for part in label.strip().split(",")]
    if any(not part for part in parts):
        raise GeonamesFailure("geonames_location_not_found")
    if len(parts) > 1 and _normalize(parts[-1]) in _COUNTRY_NAMES:
        parts.pop()
    # Common unpunctuated suffixes such as "Greencastle PA USA".
    parts[-1] = re.sub(
        r"\s+(?:U\.?S\.?(?:A\.?)?|United\s+States(?:\s+of\s+America)?)\s*$",
        "",
        parts[-1],
        flags=re.IGNORECASE,
    )
    postal_match = _ZIP.search(parts[-1])
    postal = postal_match[1] if postal_match is not None else None
    if postal_match is not None:
        parts[-1] = parts[-1][: postal_match.start()].strip()
        if not parts[-1]:
            parts.pop()
    if len(parts) > 2:
        raise GeonamesFailure("geonames_location_not_found")
    if len(parts) == 2:
        state = states.get(_normalize(parts[1]))
        if state is None:
            raise GeonamesFailure("geonames_location_not_found")
        city = _normalize(parts[0])
    elif parts:
        text = _normalize(parts[0])
        if postal is not None and text in states:
            return "", states[text], postal
        state = None
        city = text
        for alias in sorted(states, key=len, reverse=True):
            if text.endswith(" " + alias):
                city = text[: -(len(alias) + 1)]
                state = states[alias]
                break
    else:
        city, state = "", None
    if not city and postal is None:
        raise GeonamesFailure("geonames_location_not_found")
    return city, state, postal


def _lines(
    path: Path,
    *,
    member: str | None = None,
    receipt: FileReceipt | None = None,
    directory: Path | None = None,
) -> Iterator[list[str]]:
    """Read only the expected member, never extract an archive onto disk."""
    descriptor = (
        open_dataset_file(directory, receipt.locator)
        if directory is not None and receipt is not None
        else os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    )
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_mode & 0o077
        ):
            raise GeonamesFailure("geonames_invalid_source")
        if receipt is not None:
            digest = hashlib.sha256()
            observed = 0
            while chunk := os.read(descriptor, 1024 * 1024):
                observed += len(chunk)
                if observed > receipt.bytes:
                    raise GeonamesFailure("geonames_invalid_source")
                digest.update(chunk)
            if observed != receipt.bytes or digest.hexdigest() != receipt.sha256:
                raise GeonamesFailure("geonames_invalid_source")
            _ = os.lseek(descriptor, 0, os.SEEK_SET)
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            if member is None:
                with io.TextIOWrapper(source, encoding="utf-8") as text:
                    yield from _text_rows(text)
            else:
                with zipfile.ZipFile(source) as archive:
                    matches = [item for item in archive.infolist() if item.filename == member]
                    if len(matches) != 1 or matches[0].file_size > 2_000_000_000:
                        raise GeonamesFailure("geonames_invalid_source")
                    with (
                        archive.open(matches[0]) as binary,
                        io.TextIOWrapper(binary, encoding="utf-8") as text,
                    ):
                        yield from _text_rows(text)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise GeonamesFailure("geonames_invalid_source")
    finally:
        os.close(descriptor)


def _text_rows(stream: io.TextIOBase) -> Iterator[list[str]]:
    while line := stream.readline(100_001):
        if len(line) > 100_000:
            raise GeonamesFailure("geonames_invalid_source")
        if line.strip():
            yield line.rstrip("\r\n").split("\t")


def _coordinate(latitude: str, longitude: str) -> tuple[float, float]:
    lat, lon = float(latitude), float(longitude)
    if (
        not math.isfinite(lat)
        or not math.isfinite(lon)
        or not -90 <= lat <= 90
        or not -180 <= lon <= 180
    ):
        raise GeonamesFailure("geonames_invalid_source")
    return lat, lon


def _build_database(
    stage: Path,
    identifier: str,
    *,
    source_paths: Mapping[str, Path] | None = None,
    source_receipts: Mapping[str, FileReceipt] | None = None,
    source_directory: Path | None = None,
) -> dict[str, int]:
    database = stage / _DATABASE_NAME
    descriptor = os.open(database, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(descriptor)
    paths = (
        source_paths
        if source_paths is not None
        else {name: stage / name for name in ("places.zip", "postal.zip", "admin1.txt")}
    )
    receipts = source_receipts or {}
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.executescript("""
            PRAGMA journal_mode = DELETE;
            CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
            CREATE TABLE state_names (name TEXT PRIMARY KEY, code TEXT NOT NULL) WITHOUT ROWID;
            CREATE TABLE places (
                identifier INTEGER PRIMARY KEY, state_code TEXT NOT NULL,
                latitude REAL NOT NULL, longitude REAL NOT NULL
            );
            CREATE TABLE names (
                name TEXT NOT NULL, place_identifier INTEGER NOT NULL,
                is_canonical INTEGER NOT NULL CHECK (is_canonical IN (0, 1)),
                PRIMARY KEY (name, place_identifier)
            ) WITHOUT ROWID;
            CREATE TABLE postal (
                postal_code TEXT NOT NULL, name TEXT NOT NULL, state_code TEXT NOT NULL,
                latitude REAL NOT NULL, longitude REAL NOT NULL,
                PRIMARY KEY (postal_code, name, state_code, latitude, longitude)
            ) WITHOUT ROWID;
        """)
        connection.executemany(
            "INSERT INTO metadata VALUES (?, ?)",
            [
                ("schema_version", str(_SCHEMA_VERSION)),
                ("country_code", "US"),
                ("dataset_identifier", identifier),
            ],
        )
        state_codes: set[str] = set()
        for fields in _lines(
            paths["admin1.txt"], receipt=receipts.get("admin1.txt"), directory=source_directory
        ):
            if len(fields) != 4:
                raise GeonamesFailure("geonames_invalid_source")
            if not fields[0].startswith("US."):
                continue
            code = fields[0].split(".", 1)[1]
            state_codes.add(code)
            connection.executemany(
                "INSERT OR IGNORE INTO state_names VALUES (?, ?)",
                [(_normalize(name), code) for name in (code, fields[1], fields[2])],
            )
        if not state_codes:
            raise GeonamesFailure("geonames_empty_source")
        count = 0
        places: list[tuple[int, str, float, float]] = []
        aliases: list[tuple[str, int, int]] = []
        for fields in _lines(
            paths["places.zip"],
            member="US.txt",
            receipt=receipts.get("places.zip"),
            directory=source_directory,
        ):
            if len(fields) != 19:
                raise GeonamesFailure("geonames_invalid_source")
            if fields[6] != "P" or fields[8] != "US":
                continue
            identifier_value = int(fields[0])
            if identifier_value <= 0:
                raise GeonamesFailure("geonames_invalid_source")
            latitude, longitude = _coordinate(fields[4], fields[5])
            places.append((identifier_value, fields[10], latitude, longitude))
            names = {_normalize(name) for name in (fields[1], fields[2], *fields[3].split(","))}
            canonical = {_normalize(fields[1]), _normalize(fields[2])}
            aliases.extend(
                (name, identifier_value, int(name in canonical)) for name in names if name
            )
            count += 1
            if len(places) >= 1000:
                connection.executemany("INSERT INTO places VALUES (?, ?, ?, ?)", places)
                connection.executemany("INSERT OR IGNORE INTO names VALUES (?, ?, ?)", aliases)
                places.clear()
                aliases.clear()
        connection.executemany("INSERT INTO places VALUES (?, ?, ?, ?)", places)
        connection.executemany("INSERT OR IGNORE INTO names VALUES (?, ?, ?)", aliases)
        postal_count = 0
        postals: list[tuple[str, str, str, float, float]] = []
        for fields in _lines(
            paths["postal.zip"],
            member="US.txt",
            receipt=receipts.get("postal.zip"),
            directory=source_directory,
        ):
            if len(fields) != 12:
                raise GeonamesFailure("geonames_invalid_source")
            if fields[0] != "US":
                continue
            if re.fullmatch(r"\d{5}", fields[1]) is None:
                raise GeonamesFailure("geonames_invalid_source")
            latitude, longitude = _coordinate(fields[9], fields[10])
            postals.append((fields[1], _normalize(fields[2]), fields[4], latitude, longitude))
            postal_count += 1
            if len(postals) >= 1000:
                connection.executemany(
                    "INSERT OR IGNORE INTO postal VALUES (?, ?, ?, ?, ?)", postals
                )
                postals.clear()
        connection.executemany("INSERT OR IGNORE INTO postal VALUES (?, ?, ?, ?, ?)", postals)
        if not count or not postal_count:
            raise GeonamesFailure("geonames_empty_source")
        connection.execute("ANALYZE")
        connection.commit()
    return {"places": count, "postal_records": postal_count, "states": len(state_codes)}


def _file_info(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(descriptor, "rb") as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid():
            raise GeonamesFailure("geonames_invalid_dataset")
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
        after = os.fstat(source.fileno())
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise GeonamesFailure("geonames_invalid_dataset")
    return {"name": path.name, "bytes": before.st_size, "sha256": digest.hexdigest()}


def prepare_geonames_build(*, country_code: str = "US") -> GeonamesBuildPlan:
    if country_code != "US":
        raise GeonamesFailure("geonames_country_not_supported")
    return GeonamesBuildPlan(
        sources=tuple(
            DatasetSource(
                identifier=name, name=name, url=url, maximum_bytes=limit, follow_redirects=True
            )
            for name, url, limit in tuple(_SOURCES)
        )
    )


def _source_metadata(
    plan: GeonamesBuildPlan, receipts: Sequence[FileReceipt]
) -> list[dict[str, object]]:
    if len(receipts) != len(plan.sources):
        raise GeonamesFailure("geonames_invalid_source")
    result: list[dict[str, object]] = []
    for source, receipt in zip(plan.sources, receipts, strict=True):
        if (
            receipt.source_identifier != source.identifier
            or receipt.requested_url != source.url
            or receipt.source_fingerprint != dataset_source_fingerprint(source)
            or (source.maximum_bytes is not None and receipt.bytes > source.maximum_bytes)
            or (source.expected_sha256 is not None and receipt.sha256 != source.expected_sha256)
            or (source.expected_md5 is not None and receipt.md5 != source.expected_md5)
        ):
            raise GeonamesFailure("geonames_invalid_source")
        result.append(geonames_download_metadata(source, receipt))
    return result


def geonames_dataset_identifier(plan: GeonamesBuildPlan, receipts: Sequence[FileReceipt]) -> str:
    sources = _source_metadata(plan, receipts)
    value = json.dumps(
        {
            "schema_version": plan.schema_version,
            "country_code": plan.country_code,
            "sources": [(source["url"], source["sha256"]) for source in sources],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return "geonames-" + hashlib.sha256(value.encode()).hexdigest()[:24]


def _read_manifest(path: Path, *, published: bool = False) -> dict[str, object]:
    if published:
        root = open_dataset_directory(path.parent.parent)
        try:
            parent = os.open(
                path.parent.name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=root,
            )
        finally:
            os.close(root)
        if os.fstat(parent).st_uid != os.getuid():
            os.close(parent)
            raise GeonamesFailure("geonames_invalid_dataset")
    else:
        parent = open_dataset_directory(path.parent)
    try:
        descriptor = os.open(
            path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=parent
        )
    finally:
        os.close(parent)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or not 0 < info.st_size <= 1_048_576
        ):
            raise GeonamesFailure("geonames_invalid_dataset")
        content = stream.read(info.st_size + 1)
        if len(content) != info.st_size:
            raise GeonamesFailure("geonames_invalid_dataset")
    value = json.loads(content)
    if not isinstance(value, dict):
        raise GeonamesFailure("geonames_invalid_dataset")
    return cast(dict[str, object], value)


def _check_database(path: Path, identifier: str) -> None:
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)) as connection:
        metadata = dict(connection.execute("SELECT key,value FROM metadata"))
        if (
            metadata.get("schema_version") != str(_SCHEMA_VERSION)
            or metadata.get("country_code") != "US"
            or metadata.get("dataset_identifier") != identifier
        ):
            raise GeonamesFailure("geonames_invalid_dataset")
        for query in (
            "SELECT name,code FROM state_names LIMIT 0",
            "SELECT identifier,state_code,latitude,longitude FROM places LIMIT 0",
            "SELECT name,place_identifier,is_canonical FROM names LIMIT 0",
            "SELECT postal_code,name,state_code,latitude,longitude FROM postal LIMIT 0",
        ):
            _ = connection.execute(query)
        if connection.execute("PRAGMA quick_check").fetchone() != ("ok",):
            raise GeonamesFailure("geonames_invalid_dataset")


def _validate_candidate(
    plan: GeonamesBuildPlan,
    receipts: Sequence[FileReceipt],
    stage: Path,
    candidate: GeonamesCandidate | None = None,
    *,
    exact_sources: bool = True,
) -> GeonamesCandidate:
    identifier = geonames_dataset_identifier(plan, receipts)
    manifest = _read_manifest(stage / "manifest.json", published=not exact_sources)
    expected = _source_metadata(plan, receipts)
    sources = manifest.get("sources")
    if (
        manifest.get("dataset_identifier") != identifier
        or manifest.get("provider") != "geonames"
        or manifest.get("schema_version") != 2
        or manifest.get("country_code") != "US"
        or not isinstance(sources, list)
        or len(cast(list[object], sources)) != len(expected)
        or [
            (
                cast(dict[str, object], source).get("url"),
                cast(dict[str, object], source).get("sha256"),
            )
            for source in cast(list[object], sources)
            if isinstance(source, dict)
        ]
        != [(source["url"], source["sha256"]) for source in expected]
    ):
        raise GeonamesFailure("geonames_invalid_dataset")
    if exact_sources and (
        sources != expected
        or {path.name for path in stage.iterdir()} != {"places.sqlite3", "manifest.json"}
    ):
        raise GeonamesFailure("geonames_invalid_dataset")
    if (
        manifest.get("attribution")
        != "GeoNames, licensed under Creative Commons Attribution 4.0 (CC BY 4.0)"
        or manifest.get("license_url") != "https://creativecommons.org/licenses/by/4.0/"
        or manifest.get("source_documentation")
        != "https://download.geonames.org/export/dump/readme.txt"
        or manifest.get("precision") != "locality_or_postal_area; not a street address"
        or manifest.get("resolution_policy")
        != "exact canonical/ascii names before alternate names; ambiguity within either tier is rejected"
    ):
        raise GeonamesFailure("geonames_invalid_dataset")
    created = manifest.get("created_at_utc")
    if (
        not isinstance(created, str)
        or len(created) > 64
        or datetime.fromisoformat(created).tzinfo is None
    ):
        raise GeonamesFailure("geonames_invalid_dataset")
    info = _file_info(stage / _DATABASE_NAME)
    if manifest.get("files") != [info]:
        raise GeonamesFailure("geonames_invalid_dataset")
    counts = GeonamesCounts.model_validate(manifest.get("counts"))
    _check_database(stage / _DATABASE_NAME, identifier)
    digest = _file_info(stage / "manifest.json")["sha256"]
    if candidate is not None and (
        candidate.dataset_identifier != identifier
        or candidate.manifest_sha256 != digest
        or candidate.database_sha256 != info["sha256"]
        or candidate.database_bytes != info["bytes"]
        or candidate.counts != counts
    ):
        raise GeonamesFailure("geonames_invalid_dataset")
    return GeonamesCandidate(
        dataset_identifier=identifier,
        candidate_locator=candidate.candidate_locator
        if candidate is not None
        else "candidates/geonames/unused",
        manifest_sha256=digest,
        database_sha256=info["sha256"],
        database_bytes=info["bytes"],
        counts=counts,
    )


def _build_candidate(
    plan: GeonamesBuildPlan,
    receipts: Sequence[FileReceipt],
    directory: Path,
    execution_identifier: str,
) -> GeonamesCandidate:
    identifier = geonames_dataset_identifier(plan, receipts)
    sources = _source_metadata(plan, receipts)
    locator = f"candidates/geonames/{execution_identifier}"
    stage = directory / locator
    for parent in (directory, directory / "candidates", directory / "candidates" / "geonames"):
        os.close(open_dataset_directory(parent, create=True))
    try:
        stage.mkdir(mode=0o700)
    except FileExistsError:
        if not (stage / "manifest.json").exists():
            raise GeonamesFailure("geonames_candidate_uncertain") from None
        return _validate_candidate(plan, receipts, stage).model_copy(
            update={"candidate_locator": locator}
        )
    paths = {
        source.name: directory / receipt.locator
        for source, receipt in zip(plan.sources, receipts, strict=True)
    }
    source_receipts = {
        source.name: receipt for source, receipt in zip(plan.sources, receipts, strict=True)
    }
    counts = _build_database(
        stage,
        identifier,
        source_paths=paths,
        source_receipts=source_receipts,
        source_directory=directory,
    )
    manifest = {
        "dataset_identifier": identifier,
        "provider": "geonames",
        "schema_version": _SCHEMA_VERSION,
        "country_code": "US",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "sources": sources,
        "files": [_file_info(stage / _DATABASE_NAME)],
        "counts": counts,
        "attribution": "GeoNames, licensed under Creative Commons Attribution 4.0 (CC BY 4.0)",
        "license_url": "https://creativecommons.org/licenses/by/4.0/",
        "source_documentation": "https://download.geonames.org/export/dump/readme.txt",
        "precision": "locality_or_postal_area; not a street address",
        "resolution_policy": "exact canonical/ascii names before alternate names; ambiguity within either tier is rejected",
    }
    content = (json.dumps(manifest, indent=2) + "\n").encode()
    with (stage / _DATABASE_NAME).open("rb") as stream:
        os.fsync(stream.fileno())
    temporary = stage / "manifest.json.tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        _ = stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    _ = temporary.rename(stage / "manifest.json")
    descriptor = open_dataset_directory(stage)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return _validate_candidate(plan, receipts, stage).model_copy(
        update={"candidate_locator": locator}
    )


def _verified_candidate(
    plan: GeonamesBuildPlan,
    receipts: Sequence[FileReceipt],
    candidate: GeonamesCandidate,
    directory: Path,
    root: Path | None,
) -> Path:
    identifier = geonames_dataset_identifier(plan, receipts)
    if candidate.dataset_identifier != identifier:
        raise GeonamesFailure("geonames_invalid_dataset")
    if root is not None and (root / identifier).exists():
        _ = _validate_candidate(plan, receipts, root / identifier, exact_sources=False)
        return root / identifier / "manifest.json"
    stage = directory / candidate.candidate_locator
    _ = _validate_candidate(plan, receipts, stage, candidate)
    return stage / "manifest.json"


def _existing_dataset(
    plan: GeonamesBuildPlan, receipts: Sequence[FileReceipt], root: Path
) -> Path | None:
    target = root / geonames_dataset_identifier(plan, receipts)
    try:
        _ = target.lstat()
    except FileNotFoundError:
        return None
    _ = _validate_candidate(plan, receipts, target, exact_sources=False)
    return target / "manifest.json"


async def existing_geonames_dataset(
    plan: GeonamesBuildPlan, receipts: Sequence[FileReceipt], *, root: Path
) -> Path | None:
    if current_resource_manager() is None:
        raise ResourceLeaseLost("GeoNames verification requires a managed resource context")
    try:
        return await run_cpu(_existing_dataset, plan, tuple(receipts), root)
    except GeonamesFailure:
        raise
    except (
        OSError,
        ValueError,
        TypeError,
        sqlite3.Error,
        KeyError,
        RecursionError,
        DatasetFileError,
    ):
        raise GeonamesFailure("geonames_invalid_dataset") from None


async def build_geonames_candidate(
    plan: GeonamesBuildPlan,
    receipts: Sequence[FileReceipt],
    *,
    directory: Path,
    execution_identifier: str,
) -> GeonamesCandidate:
    if current_resource_manager() is None:
        raise ResourceLeaseLost("GeoNames builds require a managed resource context")
    _ = validate_dataset_execution_identifier(execution_identifier)
    try:
        return await run_cpu(
            _build_candidate, plan, tuple(receipts), directory, execution_identifier
        )
    except GeonamesFailure:
        raise
    except (
        OSError,
        ValueError,
        TypeError,
        sqlite3.Error,
        zipfile.BadZipFile,
        KeyError,
        NotImplementedError,
        EOFError,
        DatasetFileError,
    ):
        raise GeonamesFailure("geonames_build_failed") from None


async def verify_geonames_candidate(
    plan: GeonamesBuildPlan,
    receipts: Sequence[FileReceipt],
    candidate: GeonamesCandidate,
    *,
    directory: Path,
    root: Path | None = None,
) -> Path:
    if current_resource_manager() is None:
        raise ResourceLeaseLost("GeoNames verification requires a managed resource context")
    try:
        return await run_cpu(_verified_candidate, plan, tuple(receipts), candidate, directory, root)
    except GeonamesFailure:
        raise
    except (OSError, ValueError, TypeError, sqlite3.Error, KeyError, DatasetFileError):
        raise GeonamesFailure("geonames_invalid_dataset") from None


def _publish_candidate(
    plan: GeonamesBuildPlan,
    receipts: Sequence[FileReceipt],
    candidate: GeonamesCandidate,
    directory: Path,
    root: Path,
) -> Path:
    os.close(open_dataset_directory(root, create=True))
    manifest = _verified_candidate(plan, receipts, candidate, directory, root)
    target = root / candidate.dataset_identifier
    if manifest.parent == target:
        return manifest
    stage = manifest.parent
    try:
        _ = stage.rename(target)
    except OSError:
        if not target.exists():
            raise
        _ = _validate_candidate(plan, receipts, target, exact_sources=False)
    for parent in (root, stage.parent):
        descriptor = open_dataset_directory(parent)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return target / "manifest.json"


async def publish_geonames_candidate(
    plan: GeonamesBuildPlan,
    receipts: Sequence[FileReceipt],
    candidate: GeonamesCandidate,
    *,
    directory: Path,
    root: Path,
) -> Path:
    if current_resource_manager() is None:
        raise ResourceLeaseLost("GeoNames publishing requires a managed resource context")
    try:
        return await run_cpu(_publish_candidate, plan, tuple(receipts), candidate, directory, root)
    except GeonamesFailure:
        raise
    except (OSError, ValueError, TypeError, sqlite3.Error, KeyError, DatasetFileError):
        raise GeonamesFailure("geonames_invalid_dataset") from None


def _progress(root: Path, build_identifier: str, stage: str, **details: Any) -> None:
    """Publish complete, bounded progress records, including concurrent builds."""
    temporary = root / f".progress-{uuid.uuid4().hex}.json"
    value = {
        "provider": "geonames",
        "build_identifier": build_identifier,
        "stage": stage,
        "updated_at_utc": datetime.now(UTC).isoformat(),
        **details,
    }
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(root / "progress.json")
    finally:
        temporary.unlink(missing_ok=True)


async def build_geonames_dataset(
    root: Path, *, country_code: str = "US", transport: httpx.AsyncBaseTransport | None = None
) -> Path:
    """Download full US localities/ZIPs, publish a version, and return its manifest.

    Builds are explicit maintenance, not worker startup. No config is activated.
    Failed/cancelled stages are removed; a live version is never overwritten.
    """
    if current_resource_manager() is None:
        raise ResourceLeaseLost("GeoNames builds require a managed resource context")
    build_identifier = uuid.uuid4().hex
    stage = root / f".building-{build_identifier}"
    outcome = "cancelled"
    reason: str | None = "geonames_build_cancelled"
    dataset_identifier: str | None = None
    try:
        os.close(open_dataset_directory(root, create=True))
        stage.mkdir(mode=0o700, parents=True)
        plan = prepare_geonames_build(country_code=country_code)
        receipts: list[FileReceipt] = []
        for source_number, source in enumerate(plan.sources, start=1):
            _progress(
                root,
                build_identifier,
                "download",
                source_number=source_number,
                source_count=len(plan.sources),
                name=source.name,
            )
            receipts.append(
                await download_dataset_source(
                    source,
                    directory=stage,
                    execution_identifier=f"source-{source_number}",
                    transport=transport,
                )
            )
        _progress(root, build_identifier, "build_database")
        manifest = await existing_geonames_dataset(plan, receipts, root=root)
        if manifest is None:
            candidate = await build_geonames_candidate(
                plan, receipts, directory=stage, execution_identifier="geonames-0"
            )
            manifest = await publish_geonames_candidate(
                plan, receipts, candidate, directory=stage, root=root
            )
        dataset_identifier = manifest.parent.name
        outcome, reason = "completed", None
        return manifest
    except GeonamesFailure as error:
        outcome, reason = "failed", error.reason
        raise
    except DatasetFileError as error:
        reason = (
            f"geonames_download_http_{error.status_code}"
            if error.status_code is not None
            else "geonames_download_too_large"
            if error.reason == "dataset_download_too_large"
            else "geonames_download_failed"
        )
        outcome = "failed"
        raise GeonamesFailure(reason) from None
    except httpx.HTTPStatusError as error:
        outcome, reason = "failed", f"geonames_download_http_{error.response.status_code}"
        raise GeonamesFailure(reason) from error
    except httpx.HTTPError as error:
        outcome, reason = "failed", "geonames_download_failed"
        raise GeonamesFailure(reason) from error
    except (
        OSError,
        ValueError,
        TypeError,
        sqlite3.Error,
        zipfile.BadZipFile,
        KeyError,
        NotImplementedError,
        EOFError,
    ) as error:
        outcome, reason = "failed", "geonames_build_failed"
        raise GeonamesFailure(reason) from error
    except anyio.get_cancelled_exc_class():
        raise
    except BaseException:
        outcome, reason = "failed", "geonames_build_failed"
        raise
    finally:
        with anyio.CancelScope(shield=True):
            try:
                if root.is_dir():
                    _progress(
                        root,
                        build_identifier,
                        outcome,
                        reason=reason,
                        dataset_identifier=dataset_identifier,
                    )
            except OSError:
                # Preserve the original build error if the destination stopped being writable.
                pass
            finally:
                await anyio.to_thread.run_sync(lambda: shutil.rmtree(stage, ignore_errors=True))
