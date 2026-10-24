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
import unicodedata
import uuid
import zipfile
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import anyio
import httpx

from carl.core.travel import TravelCoordinate

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
        return await anyio.to_thread.run_sync(self._geocode, label, country_code)

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


def _lines(path: Path, *, member: str | None = None) -> Iterator[list[str]]:
    """Read only the expected member, never extract an archive onto disk."""
    if member is None:
        with path.open(encoding="utf-8") as stream:
            yield from _text_rows(stream)
        return
    with zipfile.ZipFile(path) as archive:
        matches = [item for item in archive.infolist() if item.filename == member]
        if len(matches) != 1 or matches[0].file_size > 2_000_000_000:
            raise GeonamesFailure("geonames_invalid_source")
        with archive.open(matches[0]) as binary, io.TextIOWrapper(binary, encoding="utf-8") as text:
            yield from _text_rows(text)


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


def _build_database(stage: Path, identifier: str) -> dict[str, int]:
    database = stage / _DATABASE_NAME
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
        for fields in _lines(stage / "admin1.txt"):
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
        for fields in _lines(stage / "places.zip", member="US.txt"):
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
        for fields in _lines(stage / "postal.zip", member="US.txt"):
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
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return {"name": path.name, "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


async def _download(
    client: httpx.AsyncClient, stage: Path, name: str, url: str, limit: int
) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    async with client.stream("GET", url) as response:
        response.raise_for_status()
        async with await anyio.open_file(stage / name, "wb") as output:
            async for chunk in response.aiter_bytes(256 * 1024):
                size += len(chunk)
                if size > limit:
                    raise GeonamesFailure("geonames_download_too_large")
                digest.update(chunk)
                _ = await output.write(chunk)
        if not size:
            raise GeonamesFailure("geonames_empty_source")
        return {
            "name": name,
            "url": url,
            "response_url": str(response.url),
            "bytes": size,
            "sha256": digest.hexdigest(),
            "downloaded_at_utc": datetime.now(UTC).isoformat(),
            "last_modified": response.headers.get("last-modified"),
            "etag": response.headers.get("etag"),
        }


def _publish(stage: Path, root: Path, sources: list[dict[str, Any]]) -> Path:
    identity = json.dumps(
        {
            "schema_version": _SCHEMA_VERSION,
            "country_code": "US",
            "sources": [(source["url"], source["sha256"]) for source in sources],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    identifier = "geonames-" + hashlib.sha256(identity.encode()).hexdigest()[:24]
    target = root / identifier
    if target.exists():
        return _verify_existing(target, identifier)
    counts = _build_database(stage, identifier)
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
    _ = (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for name, _, _ in _SOURCES:
        (stage / name).unlink()
    for path in stage.iterdir():
        with path.open("rb") as stream:
            os.fsync(stream.fileno())
    try:
        stage.rename(target)
    except OSError:
        if not target.exists():
            raise
        return _verify_existing(target, identifier)
    return target / "manifest.json"


def _verify_existing(target: Path, identifier: str) -> Path:
    manifest_path = target / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if (
        not isinstance(manifest, dict)
        or manifest.get("dataset_identifier") != identifier
        or manifest.get("provider") != "geonames"
    ):
        raise GeonamesFailure("geonames_invalid_dataset")
    if manifest.get("files") != [_file_info(target / _DATABASE_NAME)]:
        raise GeonamesFailure("geonames_invalid_dataset")
    return manifest_path


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
    build_identifier = uuid.uuid4().hex
    stage = root / f".building-{build_identifier}"
    outcome = "cancelled"
    reason: str | None = "geonames_build_cancelled"
    dataset_identifier: str | None = None
    try:
        stage.mkdir(parents=True)
        if country_code != "US":
            raise GeonamesFailure("geonames_country_not_supported")
        async with httpx.AsyncClient(
            transport=transport, timeout=120, follow_redirects=True
        ) as client:
            sources = []
            for source_number, source in enumerate(_SOURCES, start=1):
                _progress(
                    root,
                    build_identifier,
                    "download",
                    source_number=source_number,
                    source_count=len(_SOURCES),
                    name=source[0],
                )
                sources.append(await _download(client, stage, *source))
        _progress(root, build_identifier, "build_database")
        manifest = await anyio.to_thread.run_sync(_publish, stage, root, sources)
        dataset_identifier = manifest.parent.name
        outcome, reason = "completed", None
        return manifest
    except GeonamesFailure as error:
        outcome, reason = "failed", error.reason
        raise
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
