"""Shared validation of immutable published local travel datasets."""

import hashlib
import importlib.metadata
import json
import re
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from carl.core.travel_data import TravelDataFailure


def read_travel_manifest(
    path: Path, provider: str, identifier: str, *, verify: bool
) -> dict[str, Any]:
    try:
        if path.is_symlink() or path.parent.is_symlink() or path.stat().st_size > 1_048_576:
            raise ValueError
        value = json.loads(path.read_bytes())
        if not isinstance(value, dict):
            raise ValueError
        if value["dataset_identifier"] != identifier or value.get("provider", "osrm") != provider:
            raise ValueError
        files = value["files"]
        if not isinstance(files, list) or not 1 <= len(files) <= 100:
            raise ValueError
        names = set()
        for item in files:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("bytes"), int)
                or isinstance(item["bytes"], bool)
                or item["bytes"] < 0
                or not isinstance(item.get("sha256"), str)
                or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None
            ):
                raise ValueError
            name = item["name"]
            if (
                not isinstance(name, str)
                or Path(name).name != name
                or name in (".", "..")
                or name in names
            ):
                raise ValueError
            names.add(name)
            target = path.parent / name
            if (
                target.is_symlink()
                or not target.is_file()
                or target.stat().st_size != item["bytes"]
            ):
                raise ValueError
            if verify:
                with target.open("rb") as stream:
                    if hashlib.file_digest(stream, "sha256").hexdigest() != item["sha256"]:
                        raise ValueError
        if provider == "osrm":
            version = importlib.metadata.version("osrm-bindings")
            if (
                str(value["osrm_version"]).removeprefix("v") != version
                or value["algorithm"] != "MLD"
            ):
                raise TravelDataFailure(
                    "osrm_incompatible",
                    "Rebuild with carl travel update osrm using the installed OSRM version.",
                )
            if (
                not {"region.osrm.mldgr", "region.osrm.partition", "region.osrm.cell_metrics"}
                <= names
            ):
                raise ValueError
        elif "places.sqlite3" not in names:
            raise ValueError
        else:
            with closing(
                sqlite3.connect((path.parent / "places.sqlite3").as_uri() + "?mode=ro", uri=True)
            ) as connection:
                metadata = dict(connection.execute("SELECT key,value FROM metadata"))
                if (
                    metadata.get("schema_version") not in ("1", "2")
                    or metadata.get("country_code") != "US"
                    or metadata.get("dataset_identifier") != identifier
                ):
                    raise ValueError
                if metadata["schema_version"] == "2":
                    connection.execute("SELECT is_canonical FROM names LIMIT 0")
                for query in (
                    "SELECT name,code FROM state_names LIMIT 0",
                    "SELECT identifier,state_code,latitude,longitude FROM places LIMIT 0",
                    "SELECT name,place_identifier FROM names LIMIT 0",
                    "SELECT postal_code,name,state_code,latitude,longitude FROM postal LIMIT 0",
                ):
                    connection.execute(query)
                if verify and connection.execute("PRAGMA quick_check").fetchone() != ("ok",):
                    raise ValueError
        return value
    except TravelDataFailure:
        raise
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        sqlite3.Error,
        importlib.metadata.PackageNotFoundError,
    ):
        raise TravelDataFailure(
            "dataset_invalid",
            f"Local {provider} data is missing or invalid. Run carl travel setup {provider}.",
        ) from None
