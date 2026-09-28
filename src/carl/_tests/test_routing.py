from ipaddress import IPv4Address

import pytest
from pydantic import ValidationError

from carl.core.models import (
    ConfigurationDocumentIdentity,
    ConfigurationSchemaIdentity,
    Domain,
    Namespace,
)
from carl.core.routing import (
    LocalSocks5Endpoint,
    MullvadHealthProbeObservation,
    MullvadRouteIdentity,
    MullvadSessionObservation,
    NetworkProvider,
    ProxyScheme,
    WireproxyIdentity,
)


def test_mullvad_route_and_session_observation_serialize_without_delimited_identity() -> None:
    route = MullvadRouteIdentity(
        network_path=("mullvad", "personal", "us-was-wg-001"),
        account_identifier="personal",
        device_identity_reference=("archive", "device-1"),
        configuration_reference=("carl", "configuration", "mullvad", "carl"),
        relay_hostname="us-was-wg-001",
    )
    observation = MullvadSessionObservation(
        route=route,
        configuration=ConfigurationDocumentIdentity(
            schema=ConfigurationSchemaIdentity(
                namespace=Namespace.CARL, domain=Domain.CONFIGURATION, version=1
            ),
            document_sha256="c" * 64,
        ),
        endpoint=LocalSocks5Endpoint(port=12345),
        wireproxy=WireproxyIdentity(version="1.1.3", binary_sha256="a" * 64),
        process_argv=("/opt/wireproxy", "--silent", "--config", "/run/private.conf"),
        observed_exit_ip=IPv4Address("203.0.113.1"),
        health_test="mullvad_exit_ip_probe_passed",
        health_probe=MullvadHealthProbeObservation(
            url="https://am.i.mullvad.net/json",
            method="GET",
            timeout_seconds=10,
            request_headers=(),
            started_at_utc="2026-09-19T00:00:00+00:00",
            ended_at_utc="2026-09-19T00:00:01+00:00",
            duration_ns=1,
            status_code=200,
            response_headers=(),
            response_body_utf8='{"ip":"203.0.113.1","mullvad_exit_ip":true}',
            response_body_bytes=48,
            response_json={"ip": "203.0.113.1", "mullvad_exit_ip": True},
            mullvad_exit_ip=True,
            observed_exit_hostname="us-was-wg-001",
        ),
        archive_permission_policy="strict",
        started_at_utc="2026-09-19T00:00:00+00:00",
        ready_at_utc="2026-09-19T00:00:01+00:00",
        stopped_at_utc="2026-09-19T00:00:02+00:00",
        shutdown_state="terminated",
    )

    assert route.provider is NetworkProvider.MULLVAD
    assert observation.endpoint.scheme is ProxyScheme.SOCKS5H
    assert observation.as_json()["route"]["network_path"] == [
        "mullvad",
        "personal",
        "us-was-wg-001",
    ]


def test_managed_socks_endpoint_must_be_loopback() -> None:
    with pytest.raises(ValidationError):
        LocalSocks5Endpoint(host=IPv4Address("192.0.2.1"), port=1080)
