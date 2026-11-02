"""Compatibility entrypoint for explicit owned OSRM dataset construction."""

from pathlib import Path

import httpx

from carl.core.travel import TravelCoordinate
from carl.io.dataset_setup import build_osrm_dataset_direct
from carl.io.osrm_build import (
    OsrmBuildError as OsrmBuildError,
)
from carl.io.osrm_build import (
    abandon_osrm_attempt as abandon_osrm_attempt,
)
from carl.io.osrm_build import (
    build_osrm_attempt as build_osrm_attempt,
)
from carl.io.osrm_build import (
    create_osrm_attempt as create_osrm_attempt,
)
from carl.io.osrm_build import (
    freeze_osrm_plan as freeze_osrm_plan,
)
from carl.io.osrm_build import (
    osrm_command_spec as osrm_command_spec,
)
from carl.io.osrm_build import (
    osrm_download_sources as osrm_download_sources,
)
from carl.io.osrm_build import (
    osrm_index_source as osrm_index_source,
)
from carl.io.osrm_build import (
    osrm_probe_sources as osrm_probe_sources,
)
from carl.io.osrm_build import (
    parse_osrm_checksum as parse_osrm_checksum,
)
from carl.io.osrm_build import (
    prepare_osrm_seed as prepare_osrm_seed,
)
from carl.io.osrm_build import (
    publish_osrm_dataset as publish_osrm_dataset,
)
from carl.io.osrm_build import (
    select_osrm_sources as select_osrm_sources,
)
from carl.io.osrm_build import (
    verify_osrm_attempt as verify_osrm_attempt,
)


async def build_osrm_dataset(
    root: Path,
    *,
    origin: TravelCoordinate,
    radius_miles: float = 250,
    threads: int = 4,
    osmium_executable: str = "osmium",
    transport: httpx.AsyncBaseTransport | None = None,
) -> Path:
    """Compose the durable owned workflow; never launch unsupervised native tools."""
    return await build_osrm_dataset_direct(
        root,
        origin=origin,
        radius_miles=radius_miles,
        threads=threads,
        osmium_executable=osmium_executable,
        transport=transport,
    )
