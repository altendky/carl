"""Pure, versioned configuration values for concrete network routes."""

from typing import Annotated, Literal, cast

from pydantic import BeforeValidator, Field, field_validator, model_validator

from carl.core.models import ConfigurationSchemaIdentity, JsonValue, StrictModel
from carl.core.routing import (
    BrightDataProduct,
    BrightDataRouteIdentity,
    DecodoProduct,
    DecodoRouteIdentity,
    InternetProtocolVersion,
    MullvadRouteIdentity,
    NetworkProvider,
    ProtonRouteIdentity,
    RemoteProxyEndpoint,
    WireproxyIdentity,
)


def _tuple_from_toml(value: object) -> object:
    return tuple(cast(list[object], value)) if isinstance(value, list) else value


def _bright_data_product_from_toml(value: object) -> object:
    return BrightDataProduct(value) if isinstance(value, str) else value


def _decodo_product_from_toml(value: object) -> object:
    return DecodoProduct(value) if isinstance(value, str) else value


def _internet_protocol_version_from_toml(value: object) -> object:
    return InternetProtocolVersion(value) if isinstance(value, str) else value


type TomlParts = Annotated[tuple[str, ...], BeforeValidator(_tuple_from_toml)]
type TomlBrightDataProduct = Annotated[
    BrightDataProduct, BeforeValidator(_bright_data_product_from_toml)
]
type TomlDecodoProduct = Annotated[DecodoProduct, BeforeValidator(_decodo_product_from_toml)]
type TomlInternetProtocolVersion = Annotated[
    InternetProtocolVersion, BeforeValidator(_internet_protocol_version_from_toml)
]


class WireproxyToolConfiguration(StrictModel):
    executable_path: str = Field(min_length=1)
    version: str = Field(min_length=1)
    binary_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("executable_path")
    @classmethod
    def validate_absolute_path(cls, value: str) -> str:
        if not value.startswith("/") or "\x00" in value:
            raise ValueError("wireproxy executable_path must be absolute")
        return value

    def identity(self) -> WireproxyIdentity:
        return WireproxyIdentity(version=self.version, binary_sha256=self.binary_sha256)


class BrightDataEndpointConfiguration(StrictModel):
    host: str = Field(pattern=r"^(?:[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?)$")
    port: int = Field(ge=1, le=65535)


class BrightDataRouteConfiguration(StrictModel):
    provider: Literal[NetworkProvider.BRIGHT_DATA]
    network_path: TomlParts
    account_identifier: str = Field(min_length=1)
    proxy_username: str = Field(pattern=r"^[^:\s]+$")
    credential_reference: TomlParts
    zone_identifier: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    product: TomlBrightDataProduct
    endpoint: BrightDataEndpointConfiguration
    max_idle_seconds: float = Field(default=240.0, gt=0, lt=300)

    @field_validator("network_path", "credential_reference")
    @classmethod
    def validate_parts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Structured references require nonempty parts")
        return value

    @model_validator(mode="after")
    def validate_route(self) -> "BrightDataRouteConfiguration":
        if self.network_path[0] != self.provider.value:
            raise ValueError("network_path must begin with its provider")
        if "-session-" in self.proxy_username or self.proxy_username.endswith("-const"):
            raise ValueError("Bright Data session options are managed by Carl")
        return self

    def route_identity(self) -> BrightDataRouteIdentity:
        return BrightDataRouteIdentity(
            network_path=self.network_path,
            account_identifier=self.account_identifier,
            proxy_username=self.proxy_username,
            credential_reference=self.credential_reference,
            zone_identifier=self.zone_identifier,
            product=self.product,
            endpoint=RemoteProxyEndpoint(host=self.endpoint.host, port=self.endpoint.port),
        )


class DecodoEndpointConfiguration(StrictModel):
    host: str = Field(pattern=r"^(?:[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?)$")
    port: int = Field(ge=1, le=65535)


class DecodoRouteConfiguration(StrictModel):
    provider: Literal[NetworkProvider.DECODO]
    network_path: TomlParts
    account_identifier: str = Field(min_length=1)
    proxy_username: str = Field(pattern=r"^[^:\s]+$")
    credential_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    product: TomlDecodoProduct
    endpoint: DecodoEndpointConfiguration
    country_code: str = Field(default="us", pattern=r"^[a-z]{2}$")
    session_duration_minutes: int = Field(default=10, ge=1, le=1440)

    @field_validator("network_path")
    @classmethod
    def validate_parts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Structured references require nonempty parts")
        return value

    @model_validator(mode="after")
    def validate_route(self) -> "DecodoRouteConfiguration":
        if self.network_path[0] != self.provider.value:
            raise ValueError("network_path must begin with its provider")
        managed_parts = ("-country-", "-session-", "-sessionduration-")
        if self.proxy_username.startswith("user-") or any(
            part in self.proxy_username for part in managed_parts
        ):
            raise ValueError("Decodo username prefix, country, and session options are managed")
        if self.product == DecodoProduct.DATACENTER_PROXY and not (
            10000 <= self.endpoint.port <= 63000
        ):
            raise ValueError(
                "Decodo datacenter requires rotating port 10000 or static port 10001-63000"
            )
        return self

    def credential_reference(self) -> tuple[str, ...]:
        return ("carl", "configuration", "decodo", self.credential_id)

    def route_identity(self) -> DecodoRouteIdentity:
        return DecodoRouteIdentity(
            network_path=self.network_path,
            account_identifier=self.account_identifier,
            proxy_username=self.proxy_username,
            credential_reference=self.credential_reference(),
            product=self.product,
            endpoint=RemoteProxyEndpoint(host=self.endpoint.host, port=self.endpoint.port),
            country_code=self.country_code,
        )


class ProtonRouteConfiguration(StrictModel):
    provider: Literal[NetworkProvider.PROTON]
    network_path: TomlParts
    account_identifier: str = Field(min_length=1)
    configuration_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    peer_endpoint: str = Field(min_length=1)
    internet_protocol_version: TomlInternetProtocolVersion = InternetProtocolVersion.DUAL_STACK
    startup_timeout_seconds: float = Field(default=60.0, gt=0, le=300)
    shutdown_timeout_seconds: float = Field(default=10.0, gt=0, le=60)

    @field_validator("network_path")
    @classmethod
    def validate_parts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Structured references require nonempty parts")
        return value

    @model_validator(mode="after")
    def validate_route(self) -> "ProtonRouteConfiguration":
        if self.network_path[0] != self.provider.value:
            raise ValueError("network_path must begin with its provider")
        return self

    def configuration_reference(self) -> tuple[str, ...]:
        return ("carl", "configuration", "proton", self.configuration_id)

    def route_identity(self) -> ProtonRouteIdentity:
        reference = self.configuration_reference()
        return ProtonRouteIdentity(
            network_path=self.network_path,
            account_identifier=self.account_identifier,
            device_identity_reference=reference,
            configuration_reference=reference,
            peer_endpoint=self.peer_endpoint,
            internet_protocol_version=self.internet_protocol_version,
        )


class MullvadRouteConfiguration(StrictModel):
    provider: Literal[NetworkProvider.MULLVAD]
    network_path: TomlParts
    account_identifier: str = Field(min_length=1)
    configuration_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    relay_hostname: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)+$")
    startup_timeout_seconds: float = Field(default=60.0, gt=0, le=300)
    shutdown_timeout_seconds: float = Field(default=10.0, gt=0, le=60)

    @field_validator("network_path")
    @classmethod
    def validate_parts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Structured references require nonempty parts")
        return value

    @model_validator(mode="after")
    def validate_route(self) -> "MullvadRouteConfiguration":
        if self.network_path[0] != self.provider.value:
            raise ValueError("network_path must begin with its provider")
        return self

    def configuration_reference(self) -> tuple[str, ...]:
        return ("carl", "configuration", "mullvad", self.configuration_id)

    def route_identity(self) -> MullvadRouteIdentity:
        reference = self.configuration_reference()
        return MullvadRouteIdentity(
            network_path=self.network_path,
            account_identifier=self.account_identifier,
            device_identity_reference=reference,
            configuration_reference=reference,
            relay_hostname=self.relay_hostname,
        )


type RouteConfiguration = Annotated[
    BrightDataRouteConfiguration
    | DecodoRouteConfiguration
    | MullvadRouteConfiguration
    | ProtonRouteConfiguration,
    Field(discriminator="provider"),
]


class HttpxTransportConfiguration(StrictModel):
    identifier: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    implementation: Literal["httpx"]


class WreqTransportConfiguration(StrictModel):
    identifier: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    implementation: Literal["wreq"]
    emulation_profile: Literal["chrome_153"] = "chrome_153"


type HttpTransportConfiguration = Annotated[
    HttpxTransportConfiguration | WreqTransportConfiguration,
    Field(discriminator="implementation"),
]


class AcquisitionStackConfiguration(StrictModel):
    identifier: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    http_transport: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    network_path: TomlParts

    @field_validator("network_path")
    @classmethod
    def validate_network_path(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Structured references require nonempty parts")
        return value


class NetworkRouteOverride(StrictModel):
    """Explicit runtime cutover, including work queued before a provider change."""

    requested_network_path: TomlParts
    network_path: TomlParts

    @field_validator("requested_network_path", "network_path")
    @classmethod
    def validate_parts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Structured references require nonempty parts")
        return value


class CarlConfiguration(StrictModel):
    schema_identity: ConfigurationSchemaIdentity = Field(alias="schema")
    wireproxy: WireproxyToolConfiguration
    http_transports: Annotated[
        tuple[HttpTransportConfiguration, ...], BeforeValidator(_tuple_from_toml)
    ] = ()
    acquisition_stacks: Annotated[
        tuple[AcquisitionStackConfiguration, ...], BeforeValidator(_tuple_from_toml)
    ] = ()
    routes: Annotated[tuple[RouteConfiguration, ...], BeforeValidator(_tuple_from_toml)]
    route_overrides: Annotated[
        tuple[NetworkRouteOverride, ...], BeforeValidator(_tuple_from_toml)
    ] = ()

    @model_validator(mode="after")
    def validate_unique_components(self) -> "CarlConfiguration":
        paths = tuple(route.network_path for route in self.routes)
        if len(paths) != len(set(paths)):
            raise ValueError("Duplicate network_path")
        transport_identifiers = tuple(item.identifier for item in self.http_transports)
        if len(transport_identifiers) != len(set(transport_identifiers)):
            raise ValueError("Duplicate HTTP transport identifier")
        stack_identifiers = tuple(item.identifier for item in self.acquisition_stacks)
        if len(stack_identifiers) != len(set(stack_identifiers)):
            raise ValueError("Duplicate acquisition stack identifier")
        available_transports = set(transport_identifiers)
        available_paths = set(paths)
        overridden_paths = tuple(item.requested_network_path for item in self.route_overrides)
        if len(overridden_paths) != len(set(overridden_paths)):
            raise ValueError("Duplicate network route override")
        for override in self.route_overrides:
            if override.requested_network_path not in available_paths:
                raise ValueError("Route override references an unknown requested network path")
            if override.network_path not in available_paths:
                raise ValueError("Route override references an unknown network path")
            if override.network_path in overridden_paths:
                raise ValueError("Network route overrides must not chain or cycle")
        for stack in self.acquisition_stacks:
            if stack.http_transport not in available_transports:
                raise ValueError("Acquisition stack references an unknown HTTP transport")
            if stack.network_path not in available_paths:
                raise ValueError("Acquisition stack references an unknown network path")
        return self

    def resolve_network_path(self, requested_network_path: tuple[str, ...]) -> tuple[str, ...]:
        for override in self.route_overrides:
            if override.requested_network_path == requested_network_path:
                return override.network_path
        return requested_network_path

    def require_route(self, network_path: tuple[str, ...]) -> RouteConfiguration:
        for route in self.routes:
            if route.network_path == network_path:
                return route
        raise KeyError(network_path)

    def require_http_transport(self, identifier: str) -> HttpTransportConfiguration:
        for transport in self.http_transports:
            if transport.identifier == identifier:
                return transport
        raise KeyError(identifier)

    def require_acquisition_stack(self, identifier: str) -> AcquisitionStackConfiguration:
        for stack in self.acquisition_stacks:
            if stack.identifier == identifier:
                return stack
        raise KeyError(identifier)

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", by_alias=True)
