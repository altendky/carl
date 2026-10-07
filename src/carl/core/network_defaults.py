"""Explicit acquisition routes and historical provider-specific request compatibility."""

from typing import Annotated, cast

from pydantic import AfterValidator, BeforeValidator

DEFAULT_DATACENTER_NETWORK_PATH = ("decodo", "personal", "datacenter")


def _parse_network_path(value: object) -> object:
    return tuple(value) if isinstance(value, list) else value


def _validate_network_path(value: tuple[str, ...]) -> tuple[str, ...]:
    if len(value) != 3 or any(not part or part != part.strip() for part in value):
        raise ValueError("Network paths require three nonempty, trimmed identity parts")
    if value[0] not in ("decodo", "proton"):
        raise ValueError("This acquisition supports Decodo or Proton network paths")
    return value


def _validate_route_identifier(value: str) -> str:
    if not value or value != value.strip():
        raise ValueError("Route identifiers must be nonempty and trimmed")
    return value


type AcquisitionNetworkPath = Annotated[
    tuple[str, ...], BeforeValidator(_parse_network_path), AfterValidator(_validate_network_path)
]
type LegacyProtonRoute = Annotated[str, AfterValidator(_validate_route_identifier)]


def populate_legacy_proton_paths(
    value: object, *, legacy_field: str, network_fields: tuple[str, ...]
) -> object:
    """Materialize real Proton paths for explicitly supplied historical route options."""

    if not isinstance(value, dict):
        return value
    data = cast(dict[str, object], value)
    route = data.get(legacy_field)
    if not isinstance(route, str):
        return value
    result = dict(data)
    for field in network_fields:
        if result.get(field) is None:
            result[field] = ("proton", "personal", route)
    return result


def validate_legacy_proton_paths(route: str | None, *network_paths: tuple[str, ...] | None) -> None:
    if route is not None and any(path != ("proton", "personal", route) for path in network_paths):
        raise ValueError("Legacy Proton route options conflict with the explicit network path")
