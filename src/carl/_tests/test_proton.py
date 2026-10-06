import base64
import ipaddress
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import anyio
import httpx
import pytest

from carl.core.http import RequestPlan
from carl.core.models import (
    ConfigurationDocumentIdentity,
    ConfigurationSchemaIdentity,
    Domain,
    Namespace,
)
from carl.core.routing import (
    InternetProtocolVersion,
    LocalSocks5Endpoint,
    ProtonHealthProbeObservation,
    ProtonRouteIdentity,
    WireproxyIdentity,
)
from carl.io.httpx import Acquisition, AcquisitionFailure, RouteConfigurationFailure
from carl.io.proton import (
    ManagedProtonHttpAcquirer,
    ManagedProtonSession,
    ManagedProtonTransportFailure,
    ProtonWireproxyManager,
    ProtonWireproxySettings,
    SharedProtonWireproxyManager,
    _probe_exit_ip,
    _render_configuration,
)
from carl.io.wireproxy import ManagedWireproxyProcess, WireproxyProcessManager


class _Stream(httpx.AsyncByteStream):
    def __init__(self, content: bytes):
        self.content = content

    async def __aiter__(self):
        yield self.content


def _settings(tmp_path: Path) -> ProtonWireproxySettings:
    private_key = base64.b64encode(b"p" * 32).decode()
    public_key = base64.b64encode(b"u" * 32).decode()
    configuration = tmp_path / "proton.conf"
    configuration.write_text(
        f"""[Interface]
PrivateKey = {private_key}
Address = 10.2.0.2/32
DNS = 10.2.0.1
PostUp = must-not-run

[Peer]
PublicKey = {public_key}
Endpoint = server.example:51820
AllowedIPs = 0.0.0.0/0
"""
    )
    configuration.chmod(0o600)
    binary = tmp_path / "wireproxy"
    binary.write_bytes(b"binary")
    binary.chmod(0o700)
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    return ProtonWireproxySettings(
        route=ProtonRouteIdentity(
            network_path=("proton", "personal", "image"),
            account_identifier="personal",
            device_identity_reference=("proton", "device", "one"),
            configuration_reference=("proton", "wireguard", "image"),
            peer_endpoint="server.example:51820",
        ),
        configuration=ConfigurationDocumentIdentity(
            schema=ConfigurationSchemaIdentity(
                namespace=Namespace.CARL, domain=Domain.CONFIGURATION, version=1
            ),
            document_sha256="c" * 64,
        ),
        wireproxy=WireproxyIdentity(version="1.1.3", binary_sha256="a" * 64),
        wireproxy_path=binary,
        configuration_content=configuration.read_bytes(),
        runtime_directory=runtime,
    )


def test_safe_settings_exclude_secret_bearing_and_host_paths(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    safe = settings.safe_configuration()
    rendered = _render_configuration(settings, 31080).decode()

    assert "configuration_content" not in safe
    assert "wireproxy_path" not in safe
    assert "runtime_directory" not in safe
    assert safe["route"]["peer_endpoint"] == "server.example:51820"
    assert "PostUp" not in rendered
    assert "BindAddress = 127.0.0.1:31080" in rendered


@pytest.mark.anyio
async def test_probe_records_observed_proxied_ip() -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={"Content-Type": "text/plain", "Date": "today"},
            stream=_Stream(b"2001:db8::7\n"),
            request=request,
        )
    )

    observed_ip, probe = await _probe_exit_ip(
        LocalSocks5Endpoint(port=31080),
        timeout_seconds=2,
        transport=transport,
    )

    assert observed_ip == ipaddress.IPv6Address("2001:db8::7")
    assert probe.verification_scope == "proxied_public_ip_observed"
    assert probe.response_body_utf8 == "2001:db8::7\n"
    assert probe.duration_ns > 0


@pytest.mark.anyio
async def test_probe_rejects_non_ip_response() -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, stream=_Stream(b"not an address"), request=request)
    )

    with pytest.raises(ManagedProtonTransportFailure) as raised:
        await _probe_exit_ip(
            LocalSocks5Endpoint(port=31080),
            timeout_seconds=2,
            transport=transport,
        )

    assert raised.value.code == "proton_egress_probe_failed"
    assert raised.value.diagnostic == {"exception_type": "ValueError"}


def _probe_observation() -> ProtonHealthProbeObservation:
    return ProtonHealthProbeObservation(
        url="https://ip.me/",
        method="GET",
        timeout_seconds=2,
        request_headers=(),
        started_at_utc="2026-09-20T00:00:00+00:00",
        ended_at_utc="2026-09-20T00:00:01+00:00",
        duration_ns=1,
        status_code=200,
        response_headers=(),
        response_body_utf8="203.0.113.7\n",
        response_body_bytes=12,
        observed_exit_ip=ipaddress.IPv4Address("203.0.113.7"),
        verification_scope="proxied_public_ip_observed",
    )


class _FakeProcessManager:
    @asynccontextmanager
    async def open(self, **_kwargs: object) -> AsyncGenerator[ManagedWireproxyProcess]:
        process = ManagedWireproxyProcess(
            endpoint=LocalSocks5Endpoint(port=31080),
            wireproxy=WireproxyIdentity(version="1.1.3", binary_sha256="a" * 64),
            process_argv=("wireproxy", "--config", "/private/runtime/wireproxy.conf"),
            started_at_utc="2026-09-20T00:00:00+00:00",
            proxy_listening_at_utc="2026-09-20T00:00:01+00:00",
        )
        try:
            yield process
        finally:
            process.stopped_at_utc = "2026-09-20T00:00:02+00:00"
            process.shutdown_state = "terminated"


@pytest.mark.anyio
async def test_proton_manager_records_completed_managed_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)

    async def probe(
        endpoint: LocalSocks5Endpoint,
        *,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> tuple[ipaddress.IPv4Address, ProtonHealthProbeObservation]:
        assert endpoint.port == 31080
        assert timeout_seconds == settings.startup_timeout_seconds
        assert transport is None
        return ipaddress.IPv4Address("203.0.113.7"), _probe_observation()

    monkeypatch.setattr("carl.io.proton._probe_exit_ip", probe)
    manager = ProtonWireproxyManager(
        cast(WireproxyProcessManager, cast(object, _FakeProcessManager()))
    )

    async with manager.open(settings) as session:
        assert session.active_observation()["state"] == "ready"
        assert session.active_observation()["configuration"]["document_sha256"] == "c" * 64

    assert session.completed_observation()["shutdown_state"] == "terminated"
    assert session.completed_observation()["observed_exit_ip"] == "203.0.113.7"


@pytest.mark.anyio
async def test_proton_manager_rejects_wrong_exit_ip_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    settings = settings.model_copy(
        update={
            "route": settings.route.model_copy(
                update={"internet_protocol_version": InternetProtocolVersion.VERSION_4}
            )
        }
    )

    async def probe(
        endpoint: LocalSocks5Endpoint,
        *,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> tuple[ipaddress.IPv6Address, ProtonHealthProbeObservation]:
        del endpoint, timeout_seconds, transport
        observation = _probe_observation().model_copy(
            update={
                "response_body_utf8": "2001:db8::7\n",
                "observed_exit_ip": ipaddress.IPv6Address("2001:db8::7"),
            }
        )
        return ipaddress.IPv6Address("2001:db8::7"), observation

    monkeypatch.setattr("carl.io.proton._probe_exit_ip", probe)
    manager = ProtonWireproxyManager(
        cast(WireproxyProcessManager, cast(object, _FakeProcessManager()))
    )

    with pytest.raises(ManagedProtonTransportFailure) as raised:
        async with manager.open(settings):
            pytest.fail("wrong-version session must not be yielded")

    assert raised.value.code == "proton_exit_ip_version_mismatch"
    assert raised.value.diagnostic == {"expected": "version_4", "observed": "version_6"}


class _FakeProtonManager:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.opens = 0
        self.closes = 0

    @asynccontextmanager
    async def open(self, settings: ProtonWireproxySettings) -> AsyncGenerator[ManagedProtonSession]:
        self.opens += 1
        if self.fail:
            raise ManagedProtonTransportFailure("proton_route_start_failed", exit_code=3)
        process = ManagedWireproxyProcess(
            endpoint=LocalSocks5Endpoint(port=31080),
            wireproxy=settings.wireproxy,
            process_argv=("wireproxy", "--config", "/private/runtime/wireproxy.conf"),
            started_at_utc=datetime.now(UTC).isoformat(),
            proxy_listening_at_utc=datetime.now(UTC).isoformat(),
        )
        session = ManagedProtonSession(
            route=settings.route,
            configuration=settings.configuration,
            process=process,
            observed_exit_ip=ipaddress.IPv4Address("203.0.113.7"),
            health_probe=_probe_observation(),
            validated_at_utc=datetime.now(UTC).isoformat(),
        )
        try:
            yield session
        finally:
            self.closes += 1
            process.stopped_at_utc = datetime.now(UTC).isoformat()
            process.shutdown_state = "terminated"


@pytest.mark.anyio
async def test_shared_proton_manager_reuses_transport_until_runtime_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    process_manager = _FakeProcessManager()
    opens = 0

    async def probe(
        endpoint: LocalSocks5Endpoint,
        *,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> tuple[ipaddress.IPv4Address, ProtonHealthProbeObservation]:
        nonlocal opens
        del endpoint, timeout_seconds, transport
        opens += 1
        return ipaddress.IPv4Address("203.0.113.7"), _probe_observation()

    monkeypatch.setattr("carl.io.proton._probe_exit_ip", probe)
    manager = ProtonWireproxyManager(cast(WireproxyProcessManager, cast(object, process_manager)))
    shared = SharedProtonWireproxyManager(manager)

    async with shared:
        async with shared.open(settings) as first:
            assert first.endpoint.port == 31080
            assert first.active_observation()["transport_scope"] == "worker_runtime"
            async with shared.open(settings) as second:
                assert second.endpoint == first.endpoint
                assert opens == 1
            assert second.completed_observation()["transport_state_at_release"] == "ready"
        first_completed = first.completed_observation()
        async with shared.open(settings) as third:
            assert third.endpoint == first.endpoint
        assert first.provider_session.process.stopped_at_utc is None
        assert opens == 1

    assert first.provider_session.process.stopped_at_utc == "2026-09-20T00:00:02+00:00"
    assert first_completed["state"] == "released"


@pytest.mark.anyio
async def test_shared_proton_manager_replaces_transport_after_transport_failure(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    provider = _FakeProtonManager()
    shared = SharedProtonWireproxyManager(cast(ProtonWireproxyManager, cast(object, provider)))

    async with shared:
        failed_session = None
        with pytest.raises(AcquisitionFailure):
            async with shared.open(settings) as opened:
                failed_session = opened
                raise AcquisitionFailure(
                    "dead local proxy",
                    result={"hops": [], "stopping_condition": "transport_failure"},
                )
        assert provider.opens == 1
        assert provider.closes == 1
        assert failed_session is not None
        assert failed_session.completed_observation()["transport_state_at_release"] == "failed"

        async with shared.open(settings) as replacement:
            assert replacement.endpoint.port == 31080
        assert provider.opens == 2

    assert provider.closes == 2


class _ExitingProtonManager(_FakeProtonManager):
    @asynccontextmanager
    async def open(self, settings: ProtonWireproxySettings) -> AsyncGenerator[ManagedProtonSession]:
        self.opens += 1
        child = await anyio.open_process(["/bin/true"] if self.opens == 1 else ["/bin/sleep", "60"])
        process = ManagedWireproxyProcess(
            endpoint=LocalSocks5Endpoint(port=31080 + self.opens),
            wireproxy=settings.wireproxy,
            process_argv=("fake-wireproxy",),
            started_at_utc=datetime.now(UTC).isoformat(),
            proxy_listening_at_utc=datetime.now(UTC).isoformat(),
            child=child,
        )
        try:
            yield ManagedProtonSession(
                route=settings.route,
                configuration=settings.configuration,
                process=process,
                observed_exit_ip=ipaddress.IPv4Address("203.0.113.7"),
                health_probe=_probe_observation(),
                validated_at_utc=datetime.now(UTC).isoformat(),
            )
        finally:
            self.closes += 1
            if child.returncode is None:
                child.terminate()
            await child.wait()
            process.exited.set()
            process.stopped_at_utc = datetime.now(UTC).isoformat()
            process.shutdown_state = "already_exited"


class _FailingFirstCloseProtonManager(_ExitingProtonManager):
    @asynccontextmanager
    async def open(self, settings: ProtonWireproxySettings) -> AsyncGenerator[ManagedProtonSession]:
        try:
            async with super().open(settings) as session:
                yield session
        finally:
            if self.closes == 1:
                raise RuntimeError("synthetic cleanup failure")


@pytest.mark.anyio
async def test_shared_proton_manager_reaps_and_replaces_unexpected_exit(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    provider = _ExitingProtonManager()

    async with anyio.create_task_group() as task_group:
        shared = SharedProtonWireproxyManager(
            cast(ProtonWireproxyManager, cast(object, provider)),
            watcher_task_group=task_group,
        )
        async with shared:
            async with shared.open(settings) as first:
                first_child = first.provider_session.process.child
                assert first_child is not None
            with anyio.fail_after(2):
                while provider.closes == 0:
                    await anyio.sleep(0.01)
            assert first_child.returncode == 0

            async with shared.open(settings) as replacement:
                assert replacement.endpoint.port == 31082
                assert provider.opens == 2
        task_group.cancel_scope.cancel()


@pytest.mark.anyio
async def test_shared_proton_watcher_contains_cleanup_failure_and_unblocks_replacement(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings(tmp_path)
    provider = _FailingFirstCloseProtonManager()

    async with anyio.create_task_group() as task_group:
        shared = SharedProtonWireproxyManager(
            cast(ProtonWireproxyManager, cast(object, provider)),
            watcher_task_group=task_group,
        )
        async with shared:
            async with shared.open(settings):
                pass
            with anyio.fail_after(2):
                while provider.closes == 0:
                    await anyio.sleep(0.01)

            async with shared.open(settings) as replacement:
                assert replacement.endpoint.port == 31082

            assert provider.opens == 2
            assert "shared_proton_close: RuntimeError" in caplog.text
        task_group.cancel_scope.cancel()


class _FakeSocksAcquirer:
    def __init__(self, **_kwargs: object):
        pass

    async def acquire(self, plan: RequestPlan, new_identifier: object) -> Acquisition:
        del plan, new_identifier
        return Acquisition(record={"hops": []}, bodies=())


@pytest.mark.anyio
@pytest.mark.parametrize("ending", ("usage", "runtime"))
async def test_shared_proton_cancelled_cleanup_waits_for_bookkeeping_lock(
    tmp_path: Path, ending: str
) -> None:
    settings = _settings(tmp_path)
    provider = _FakeProtonManager()
    shared = SharedProtonWireproxyManager(cast(ProtonWireproxyManager, cast(object, provider)))
    lock_held = anyio.Event()

    async def hold_lock() -> None:
        async with shared._lock:
            lock_held.set()
            await anyio.sleep(0.01)

    async with anyio.create_task_group() as task_group:
        if ending == "usage":
            async with shared:
                with anyio.CancelScope() as scope:
                    async with shared.open(settings):
                        shared._entries[settings.device_lock_identity].invalidated = True
                        task_group.start_soon(hold_lock)
                        await lock_held.wait()
                        scope.cancel()
                        await anyio.sleep_forever()
                assert scope.cancelled_caught
                assert not shared._entries
                assert provider.closes == 1
                async with shared.open(settings):
                    pass
                assert provider.opens == 2
            assert provider.closes == 2
        else:
            with anyio.CancelScope() as scope:
                async with shared:
                    async with shared.open(settings):
                        pass
                    task_group.start_soon(hold_lock)
                    await lock_held.wait()
                    scope.cancel()
                    await anyio.sleep_forever()
            assert scope.cancelled_caught
            assert not shared._entries
            assert provider.closes == 1


@pytest.mark.anyio
@pytest.mark.parametrize("ending", ("normal", "body_error", "cancel"))
async def test_shared_proton_closes_all_entries_and_preserves_primary_error(
    tmp_path: Path, ending: str, caplog: pytest.LogCaptureFixture
) -> None:
    class FailingCloseManager(_FakeProtonManager):
        @asynccontextmanager
        async def open(
            self, settings: ProtonWireproxySettings
        ) -> AsyncGenerator[ManagedProtonSession]:
            async with super().open(settings) as session:
                try:
                    yield session
                finally:
                    raise OSError("secret provider cleanup diagnostic")

    settings = _settings(tmp_path)
    second_settings = settings.model_copy(
        update={
            "configuration_content": settings.configuration_content.replace(
                base64.b64encode(b"p" * 32), base64.b64encode(b"q" * 32)
            )
        }
    )
    provider = FailingCloseManager()
    shared = SharedProtonWireproxyManager(cast(ProtonWireproxyManager, cast(object, provider)))
    original = RuntimeError("original body error")

    async def run(scope: anyio.CancelScope) -> None:
        async with shared:
            async with shared.open(settings):
                pass
            async with shared.open(second_settings):
                pass
            if ending == "cancel":
                scope.cancel()
                await anyio.sleep_forever()
            if ending == "body_error":
                raise original

    with anyio.CancelScope() as scope:
        if ending == "normal":
            with pytest.raises(ExceptionGroup) as raised_group:
                await run(scope)
            assert len(raised_group.value.exceptions) == 2
        elif ending == "body_error":
            with pytest.raises(RuntimeError) as raised:
                await run(scope)
            assert raised.value is original
            assert len(original.__notes__) == 2
        else:
            await run(scope)
    assert scope.cancelled_caught is (ending == "cancel")
    assert provider.closes == 2
    assert not shared._entries
    assert "secret provider cleanup diagnostic" not in caplog.text


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ("search", "images"))
@pytest.mark.parametrize("ending", ("cancel", "body_error"))
async def test_facebook_proton_client_close_failure_preserves_primary_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, ending: str
) -> None:
    from carl.io.facebook_images import ProtonFacebookImageSessionFactory
    from carl.io.facebook_search import ProtonFacebookSearchSessionFactory

    settings = _settings(tmp_path)
    provider = _FakeProtonManager()
    closed = False

    class FailingClient(httpx.AsyncClient):
        async def aclose(self) -> None:
            nonlocal closed
            await anyio.lowlevel.checkpoint()
            await super().aclose()
            closed = True
            raise OSError("secret provider diagnostic")

    client = FailingClient(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    monkeypatch.setattr("carl.io.facebook_search.httpx.AsyncClient", lambda **_: client)
    factory_class = (
        ProtonFacebookSearchSessionFactory
        if kind == "search"
        else ProtonFacebookImageSessionFactory
    )
    factory = factory_class(
        manager=cast(ProtonWireproxyManager, cast(object, provider)), settings=settings
    )
    original = RuntimeError("original body error")
    with anyio.CancelScope() as scope:
        if ending == "cancel":
            async with factory("session"):
                scope.cancel()
                await anyio.sleep_forever()
        else:
            with pytest.raises(RuntimeError) as raised:
                async with factory("session"):
                    raise original
            assert raised.value is original
            assert "secret provider diagnostic" not in repr(original.__notes__)
    assert scope.cancelled_caught is (ending == "cancel")
    assert closed
    assert provider.closes == 1


@pytest.mark.anyio
async def test_managed_proton_acquirer_closes_route_and_has_no_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr("carl.io.proton.LocalSocks5HttpxAcquirer", _FakeSocksAcquirer)
    acquirer = ManagedProtonHttpAcquirer(
        manager=cast(ProtonWireproxyManager, cast(object, _FakeProtonManager())),
        settings=settings,
    )

    acquisition = await acquirer.acquire(
        RequestPlan(url="https://example.com/", routing=settings.route.network_path),
        lambda: "identifier",
    )
    assert acquisition.record["routing"]["observed"]["shutdown_state"] == "terminated"

    with pytest.raises(RouteConfigurationFailure):
        await acquirer.acquire(
            RequestPlan(url="https://example.com/", routing=("direct",)),
            lambda: "identifier",
        )

    failing = ManagedProtonHttpAcquirer(
        manager=cast(
            ProtonWireproxyManager,
            cast(object, _FakeProtonManager(fail=True)),
        ),
        settings=settings,
    )
    with pytest.raises(AcquisitionFailure) as raised:
        await failing.acquire(
            RequestPlan(url="https://example.com/", routing=settings.route.network_path),
            lambda: "identifier",
        )
    assert raised.value.result["route_failure"] == {
        "code": "proton_route_start_failed",
        "exit_code": 3,
        "diagnostic": {},
    }
