"""Foreground local dataset administration, independent of the work queue."""

import json
import sys
from collections.abc import Awaitable, Callable
from uuid import uuid4

import anyio
from cyclopts import App

from carl.core.models import StrictModel
from carl.core.travel_data import TravelDataFailure, TravelDataProvider, TravelDataSetupRequest
from carl.io.configuration import ConfigurationFailure
from carl.io.paths import user_directories
from carl.io.travel_data import TravelDataManager

travel_app = App(
    name="travel", help="Set up, inspect, update and select local routing/geocoding data"
)


def _perform[T: StrictModel](operation: Callable[[], Awaitable[T]]) -> T:
    try:
        result = anyio.run(operation, backend="trio")
    except TravelDataFailure as error:
        print(json.dumps({"state": "failed", "reason": error.reason, "message": error.message}))
        raise SystemExit(1) from None
    except ConfigurationFailure:
        print(
            json.dumps(
                {
                    "state": "failed",
                    "reason": "configuration_unavailable",
                    "message": "Find the private config.toml with carl locations, then create or repair it using the documented configuration example.",
                }
            )
        )
        raise SystemExit(1) from None
    print(result.model_dump_json())
    return result


@travel_app.command
def status(*, verify: bool = False) -> None:
    """Report readiness, age, corrective commands and current build stage; optionally verify file hashes."""
    result = _perform(lambda: TravelDataManager(user_directories()).status(verify=verify))
    selected = {result.routing_provider, result.geocoding_provider}
    if verify and (
        result.configuration_message is not None
        or any(item.provider in selected and item.state != "ready" for item in result.providers)
    ):
        raise SystemExit(1)


@travel_app.command
def datasets(*, limit: int = 20, cursor: str | None = None) -> None:
    """List installed versions with bounded pagination; inactive versions permit rollback."""
    _perform(lambda: TravelDataManager(user_directories()).datasets(limit=limit, cursor=cursor))


@travel_app.command
def setup(
    provider: TravelDataProvider,
    *,
    activate: bool = True,
    radius_miles: float = 250,
    threads: int = 4,
) -> None:
    """Download/build and verify data now, without a worker; activate only after success.

    GeoNames covers US places and ZIPs. OSRM builds a buffered regional road map
    and requires osmium-tool on PATH, substantial memory, disk and build time.
    """
    request = TravelDataSetupRequest(
        request_identifier="cli-" + str(uuid4()),
        provider=provider,
        activate=activate,
        radius_miles=radius_miles,
        threads=threads,
    )
    print(
        "Preparing "
        + provider
        + "; inspect another terminal with carl travel status. Existing data remains active until verification and activation finish.",
        file=sys.stderr,
    )
    if provider == "osrm":
        print(
            "The previous 250-mile map required about 23 GiB peak RAM, 30 minutes preprocessing and 8.84 GiB installed disk. Updates need room for both versions and temporary files.",
            file=sys.stderr,
        )
    _perform(lambda: TravelDataManager(user_directories()).setup(request))


@travel_app.command
def update(
    provider: TravelDataProvider,
    *,
    activate: bool = True,
    radius_miles: float = 250,
    threads: int = 4,
) -> None:
    """Explicitly rebuild from current source data, retaining the old version for rollback."""
    setup(provider, activate=activate, radius_miles=radius_miles, threads=threads)


@travel_app.command
def activate(provider: TravelDataProvider, dataset_identifier: str) -> None:
    """Verify an installed version and atomically select it, including rollback to an older version."""
    _perform(lambda: TravelDataManager(user_directories()).activate(provider, dataset_identifier))


@travel_app.command
def verify() -> None:
    """Verify checksums/integrity of configured local data without downloading or rebuilding."""
    status(verify=True)
