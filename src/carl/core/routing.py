"""Safe, serializable identities for explicit network routes."""

from ipaddress import IPv4Address, IPv6Address
from typing import Literal

from pydantic import Field, field_validator

from carl.core.models import (
    ConfigurationDocumentIdentity,
    Header,
    JsonStringEnumeration,
    JsonValue,
    StrictModel,
)

type WireproxyShutdownState = Literal["terminated", "killed", "already_exited", "unconfirmed"]


class NetworkProvider(JsonStringEnumeration):
    BRIGHT_DATA = "bright_data"
    DECODO = "decodo"
    MULLVAD = "mullvad"
    PROTON = "proton"


class ItemNetworkProvider(JsonStringEnumeration):
    DECODO = "decodo"
    MULLVAD = "mullvad"
    PROTON = "proton"


class BatchItemNetworkProvider(JsonStringEnumeration):
    DECODO = "decodo"
    MULLVAD = "mullvad"


class InternetProtocolVersion(JsonStringEnumeration):
    DUAL_STACK = "dual_stack"
    VERSION_4 = "version_4"
    VERSION_6 = "version_6"


class ProxyScheme(JsonStringEnumeration):
    HTTP = "http"
    SOCKS5H = "socks5h"


class LocalSocks5Endpoint(StrictModel):
    scheme: Literal[ProxyScheme.SOCKS5H] = ProxyScheme.SOCKS5H
    host: IPv4Address = IPv4Address("127.0.0.1")
    port: int = Field(ge=1, le=65535)

    @field_validator("host")
    @classmethod
    def validate_loopback(cls, value: IPv4Address) -> IPv4Address:
        if not value.is_loopback:
            raise ValueError("Managed SOCKS5 endpoints must be loopback-only")
        return value

    @property
    def url(self) -> str:
        return f"{self.scheme.value}://{self.host}:{self.port}"


class WireproxyIdentity(StrictModel):
    version: str = Field(min_length=1)
    binary_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class BrightDataProduct(JsonStringEnumeration):
    DATACENTER_PROXY = "datacenter_proxy"
    RESIDENTIAL_PROXY = "residential_proxy"
    ISP_PROXY = "isp_proxy"
    MOBILE_PROXY = "mobile_proxy"


class DecodoProduct(JsonStringEnumeration):
    DATACENTER_PROXY = "datacenter_proxy"
    RESIDENTIAL_PROXY = "residential_proxy"
    MOBILE_PROXY = "mobile_proxy"


class RemoteProxyEndpoint(StrictModel):
    scheme: Literal[ProxyScheme.HTTP] = ProxyScheme.HTTP
    host: str = Field(pattern=r"^(?:[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?)$")
    port: int = Field(ge=1, le=65535)

    @property
    def url(self) -> str:
        return f"{self.scheme.value}://{self.host}:{self.port}"


class BrightDataRouteIdentity(StrictModel):
    provider: Literal[NetworkProvider.BRIGHT_DATA] = NetworkProvider.BRIGHT_DATA
    network_path: tuple[str, ...]
    account_identifier: str = Field(min_length=1)
    proxy_username: str = Field(pattern=r"^[^:\s]+$")
    credential_reference: tuple[str, ...]
    zone_identifier: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    product: BrightDataProduct
    endpoint: RemoteProxyEndpoint

    @field_validator("network_path", "credential_reference")
    @classmethod
    def validate_parts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Route identity parts must be nonempty")
        return value


class BrightDataSessionObservation(StrictModel):
    route: BrightDataRouteIdentity
    configuration: ConfigurationDocumentIdentity
    session_record_identifier: str = Field(min_length=1)
    proxy_username_shape: Literal["zone_username_with_sticky_session"]
    constant_peer: Literal[True] = True
    provider_idle_expiry_seconds: Literal[300] = 300
    configured_idle_guard_seconds: float = Field(gt=0, lt=300)
    tls_verification: Literal["system_roots"]
    started_at_utc: str
    ended_at_utc: str
    requests_started: int = Field(ge=0)
    requests_completed: int = Field(ge=0)
    failed: bool
    shutdown_state: Literal["closed"]

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class DecodoRouteIdentity(StrictModel):
    provider: Literal[NetworkProvider.DECODO] = NetworkProvider.DECODO
    network_path: tuple[str, ...]
    account_identifier: str = Field(min_length=1)
    proxy_username: str = Field(pattern=r"^[^:\s]+$")
    credential_reference: tuple[str, ...]
    product: DecodoProduct
    endpoint: RemoteProxyEndpoint
    country_code: str = Field(pattern=r"^[a-z]{2}$")

    @field_validator("network_path", "credential_reference")
    @classmethod
    def validate_parts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Route identity parts must be nonempty")
        return value


class DecodoSessionObservation(StrictModel):
    route: DecodoRouteIdentity
    configuration: ConfigurationDocumentIdentity
    session_record_identifier: str = Field(min_length=1)
    proxy_username_shape: Literal[
        "base_username_with_country_sticky_session_and_duration", "base_username_with_country"
    ]
    sticky_peer_requested: bool = True
    configured_session_duration_minutes: int | None = Field(ge=1, le=1440)
    tls_verification: Literal["system_roots"]
    started_at_utc: str
    ended_at_utc: str
    requests_started: int = Field(ge=0)
    requests_completed: int = Field(ge=0)
    failed: bool
    shutdown_state: Literal["closed"]

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class ProtonRouteIdentity(StrictModel):
    provider: Literal[NetworkProvider.PROTON] = NetworkProvider.PROTON
    network_path: tuple[str, ...]
    account_identifier: str = Field(min_length=1)
    device_identity_reference: tuple[str, ...]
    configuration_reference: tuple[str, ...]
    peer_endpoint: str = Field(min_length=1)
    internet_protocol_version: InternetProtocolVersion = InternetProtocolVersion.DUAL_STACK

    @field_validator("network_path", "device_identity_reference", "configuration_reference")
    @classmethod
    def validate_parts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Route identity parts must be nonempty")
        return value


class ProtonHealthProbeObservation(StrictModel):
    url: Literal["https://ip.me/"]
    method: Literal["GET"]
    timeout_seconds: float
    request_headers: tuple[Header, ...]
    started_at_utc: str
    ended_at_utc: str
    duration_ns: int = Field(ge=0)
    status_code: int
    response_headers: tuple[Header, ...]
    response_body_utf8: str
    response_body_bytes: int = Field(ge=0)
    observed_exit_ip: IPv4Address | IPv6Address
    verification_scope: Literal["proxied_public_ip_observed"]


class ProtonSessionObservation(StrictModel):
    route: ProtonRouteIdentity
    configuration: ConfigurationDocumentIdentity
    endpoint: LocalSocks5Endpoint
    wireproxy: WireproxyIdentity
    process_argv: tuple[str, ...]
    observed_exit_ip: IPv4Address | IPv6Address
    health_probe: ProtonHealthProbeObservation
    started_at_utc: str
    proxy_listening_at_utc: str
    validated_at_utc: str
    stopped_at_utc: str
    shutdown_state: WireproxyShutdownState
    exit_code: int | None

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class MullvadRouteIdentity(StrictModel):
    provider: Literal[NetworkProvider.MULLVAD] = NetworkProvider.MULLVAD
    network_path: tuple[str, ...]
    account_identifier: str = Field(min_length=1)
    device_identity_reference: tuple[str, ...]
    configuration_reference: tuple[str, ...]
    relay_hostname: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)+$")

    @field_validator("network_path", "device_identity_reference", "configuration_reference")
    @classmethod
    def validate_parts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Route identity parts must be nonempty")
        return value


class MullvadHealthProbeObservation(StrictModel):
    url: Literal["https://am.i.mullvad.net/json"]
    method: Literal["GET"]
    timeout_seconds: float
    request_headers: tuple[Header, ...]
    started_at_utc: str
    ended_at_utc: str
    duration_ns: int = Field(ge=0)
    status_code: int
    response_headers: tuple[Header, ...]
    response_body_utf8: str
    response_body_bytes: int = Field(ge=0)
    response_json: JsonValue
    mullvad_exit_ip: Literal[True]
    observed_exit_hostname: str | None


class MullvadSessionObservation(StrictModel):
    route: MullvadRouteIdentity
    configuration: ConfigurationDocumentIdentity
    endpoint: LocalSocks5Endpoint
    wireproxy: WireproxyIdentity
    process_argv: tuple[str, ...]
    observed_exit_ip: IPv4Address
    health_test: Literal["mullvad_exit_ip_probe_passed"]
    health_probe: MullvadHealthProbeObservation
    archive_permission_policy: Literal["strict", "experimental_override"]
    started_at_utc: str
    ready_at_utc: str
    stopped_at_utc: str
    shutdown_state: WireproxyShutdownState

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")
