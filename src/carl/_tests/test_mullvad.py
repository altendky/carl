import base64
import ipaddress
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

import httpx
import pytest

from carl.core.models import (
    ConfigurationDocumentIdentity,
    ConfigurationSchemaIdentity,
    Domain,
    Namespace,
)
from carl.core.routing import (
    LocalSocks5Endpoint,
    MullvadRouteIdentity,
    WireproxyIdentity,
)
from carl.io.mullvad import (
    ManagedMullvadTransportFailure,
    MullvadWireproxyManager,
    MullvadWireproxySettings,
    _probe_exit_ip,
    _render_configuration,
)
from carl.io.wireproxy import ManagedWireproxyProcess, WireproxyProcessManager


def _configuration() -> bytes:
    private = base64.b64encode(b"p" * 32).decode()
    public = base64.b64encode(b"u" * 32).decode()
    return f"""[Interface]
PrivateKey = {private}
Address = 10.64.1.2/32
PostUp = must-not-run

[Peer]
PublicKey = {public}
Endpoint = 198.51.100.4:51820
AllowedIPs = 0.0.0.0/0
""".encode()


def _settings(tmp_path: Path) -> MullvadWireproxySettings:
    return MullvadWireproxySettings(
        route=MullvadRouteIdentity(
            network_path=("mullvad", "personal", "carl"),
            account_identifier="personal",
            device_identity_reference=("carl", "configuration", "mullvad", "carl"),
            configuration_reference=("carl", "configuration", "mullvad", "carl"),
            relay_hostname="us-was-wg-001",
        ),
        configuration=ConfigurationDocumentIdentity(
            schema=ConfigurationSchemaIdentity(
                namespace=Namespace.CARL, domain=Domain.CONFIGURATION, version=1
            ),
            document_sha256="c" * 64,
        ),
        wireproxy=WireproxyIdentity(version="1.1.3", binary_sha256="a" * 64),
        wireproxy_path=tmp_path / "wireproxy",
        configuration_content=_configuration(),
        runtime_directory=tmp_path / "runtime",
    )


def test_settings_hide_secrets_and_render_only_allowlisted_fields(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    safe = settings.safe_configuration()
    rendered = _render_configuration(settings, 31080).decode()

    assert "configuration_content" not in safe
    assert "wireproxy_path" not in safe
    assert "PostUp" not in rendered
    assert "DNS = 10.64.0.1" in rendered
    assert "MTU = 1280" in rendered
    assert len(settings.device_lock_identity) == 32


@pytest.mark.anyio
async def test_health_probe_binds_observed_exit_to_selected_relay() -> None:
    body = b'{"ip":"203.0.113.7","mullvad_exit_ip":true,"mullvad_exit_ip_hostname":"us-was-wg-001"}'
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=body, request=request)
    )

    address, probe = await _probe_exit_ip(
        LocalSocks5Endpoint(port=31080),
        expected_relay_hostname="us-was-wg-001",
        timeout_seconds=2,
        transport=transport,
    )

    assert address == ipaddress.IPv4Address("203.0.113.7")
    assert probe.observed_exit_hostname == "us-was-wg-001"

    with pytest.raises(ManagedMullvadTransportFailure):
        await _probe_exit_ip(
            LocalSocks5Endpoint(port=31080),
            expected_relay_hostname="us-nyc-wg-301",
            timeout_seconds=2,
            transport=transport,
        )


class _FakeProcessManager:
    @asynccontextmanager
    async def open(self, **kwargs: object) -> AsyncGenerator[ManagedWireproxyProcess]:
        assert kwargs["device_lock_identity"] == _settings(Path("/tmp")).device_lock_identity
        process = ManagedWireproxyProcess(
            endpoint=LocalSocks5Endpoint(port=31080),
            wireproxy=WireproxyIdentity(version="1.1.3", binary_sha256="a" * 64),
            process_argv=("wireproxy", "--config", "private"),
            started_at_utc="2026-09-20T00:00:00+00:00",
            proxy_listening_at_utc="2026-09-20T00:00:01+00:00",
        )
        try:
            yield process
        finally:
            process.stopped_at_utc = "2026-09-20T00:00:02+00:00"
            process.shutdown_state = "terminated"


@pytest.mark.anyio
async def test_manager_uses_generic_process_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)

    async def probe(*args: object, **kwargs: object):
        del args, kwargs
        body = {"ip": "203.0.113.7", "mullvad_exit_ip": True}
        from carl.core.routing import MullvadHealthProbeObservation

        return ipaddress.IPv4Address("203.0.113.7"), MullvadHealthProbeObservation(
            url="https://am.i.mullvad.net/json",
            method="GET",
            timeout_seconds=60,
            request_headers=(),
            started_at_utc="a",
            ended_at_utc="b",
            duration_ns=1,
            status_code=200,
            response_headers=(),
            response_body_utf8="{}",
            response_body_bytes=2,
            response_json=body,
            mullvad_exit_ip=True,
            observed_exit_hostname="us-was-wg-001",
        )

    monkeypatch.setattr("carl.io.mullvad._probe_exit_ip", probe)
    manager = MullvadWireproxyManager(
        cast(WireproxyProcessManager, cast(object, _FakeProcessManager()))
    )
    async with manager.open(settings) as session:
        assert session.active_observation()["configuration"]["document_sha256"] == "c" * 64
    assert session.completed_observation().shutdown_state == "terminated"
