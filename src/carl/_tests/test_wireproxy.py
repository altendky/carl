import base64
import hashlib
import os
import signal
import stat
import sys
from pathlib import Path
from typing import cast

import anyio
import pytest

from carl.core.routing import InternetProtocolVersion, NetworkProvider, WireproxyIdentity
from carl.io.httpx import RouteConfigurationFailure
from carl.io.wireproxy import (
    ManagedWireproxyFailure,
    WireproxyProcessManager,
    _release_lock,
    _stop_process,
    _try_acquire_lock,
    _verify_wireproxy,
    _wait_for_lock,
    read_private_configuration,
    render_wireguard_configuration,
)


class _AlreadyExitedProcess:
    returncode = 15

    def __init__(self) -> None:
        self.waits = 0

    async def wait(self) -> int:
        self.waits += 1
        return self.returncode


@pytest.mark.anyio
async def test_stop_process_reaps_already_exited_child() -> None:
    process = _AlreadyExitedProcess()

    state = await _stop_process(cast(anyio.abc.Process, cast(object, process)), timeout_seconds=1)

    assert state == "already_exited"
    assert process.waits == 1


def _configuration() -> bytes:
    private_key = base64.b64encode(b"p" * 32).decode()
    public_key = base64.b64encode(b"u" * 32).decode()
    return f"""[Interface]
PrivateKey = {private_key}
Address = 10.2.0.2/32, fd00::2/128
DNS = 10.2.0.1, fd00::1
MTU = 1420
PostUp = must-not-run

[Peer]
PublicKey = {public_key}
Endpoint = [2001:db8::4]:51820
AllowedIPs = 0.0.0.0/0, ::/0
PersistentKeepalive = 25
""".encode()


def test_configuration_rebuild_preserves_network_fields_and_drops_hooks() -> None:
    rendered = render_wireguard_configuration(_configuration(), port=31080).decode()

    assert "Address = 10.2.0.2/32, fd00::2/128" in rendered
    assert "DNS = 10.2.0.1, fd00::1" in rendered
    assert "MTU = 1420" in rendered
    assert "Endpoint = [2001:db8::4]:51820" in rendered
    assert "AllowedIPs = 0.0.0.0/0, ::/0" in rendered
    assert "PostUp" not in rendered
    assert "BindAddress = 127.0.0.1:31080" in rendered


def test_configuration_requires_routed_dns_and_expected_peer() -> None:
    without_dns = _configuration().replace(b"DNS = 10.2.0.1, fd00::1\n", b"")
    with pytest.raises(ValueError):
        render_wireguard_configuration(without_dns, port=31080)
    with pytest.raises(ValueError):
        render_wireguard_configuration(
            _configuration(),
            port=31080,
            expected_peer_endpoint="wrong.example:51820",
        )


def test_configuration_can_select_one_internet_protocol_version() -> None:
    rendered = render_wireguard_configuration(
        _configuration(),
        port=31080,
        internet_protocol_version=InternetProtocolVersion.VERSION_4,
    ).decode()

    assert "Address = 10.2.0.2/32\n" in rendered
    assert "DNS = 10.2.0.1\n" in rendered
    assert "AllowedIPs = 0.0.0.0/0\n" in rendered
    assert "fd00" not in rendered
    assert "::/0" not in rendered


def test_configuration_rejects_unsupported_internet_protocol_version() -> None:
    with pytest.raises(ValueError):
        render_wireguard_configuration(
            _configuration().replace(b"DNS = 10.2.0.1, fd00::1", b"DNS = fd00::1"),
            port=31080,
            internet_protocol_version=InternetProtocolVersion.VERSION_4,
        )


def test_private_configuration_rejects_broad_permissions_and_symlinks(tmp_path: Path) -> None:
    configuration = tmp_path / "proton.conf"
    configuration.write_bytes(_configuration())
    configuration.chmod(0o644)

    with pytest.raises(RouteConfigurationFailure) as broad:
        read_private_configuration(
            configuration,
            unavailable_code="unavailable",
            permissions_code="permissions",
        )
    assert broad.value.code == "permissions"

    configuration.chmod(0o600)
    link = tmp_path / "link.conf"
    os.symlink(configuration, link)
    with pytest.raises(RouteConfigurationFailure) as symlink:
        read_private_configuration(
            link,
            unavailable_code="unavailable",
            permissions_code="permissions",
        )
    assert symlink.value.code == "permissions"


@pytest.mark.anyio
async def test_wireproxy_binary_version_and_hash_are_both_verified(tmp_path: Path) -> None:
    binary = tmp_path / "wireproxy"
    binary.write_text("#!/bin/sh\necho 'wireproxy, version 1.1.3'\n")
    binary.chmod(0o700)
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()

    await _verify_wireproxy(
        wireproxy=WireproxyIdentity(version="1.1.3", binary_sha256=digest),
        wireproxy_path=binary,
    )

    with pytest.raises(RouteConfigurationFailure) as mismatch:
        await _verify_wireproxy(
            wireproxy=WireproxyIdentity(version="1.1.3", binary_sha256="a" * 64),
            wireproxy_path=binary,
        )
    assert mismatch.value.code == "wireproxy_hash_mismatch"


@pytest.mark.anyio
async def test_lock_contention_is_a_transient_managed_failure(tmp_path: Path) -> None:
    path = tmp_path / "device.lock"
    first = _try_acquire_lock(path)
    assert first is not None
    try:
        with pytest.raises(ManagedWireproxyFailure) as raised:
            await _wait_for_lock(
                path,
                timeout_seconds=0.01,
                failure_code="wireproxy_device_identity_busy",
            )
        assert raised.value.code == "wireproxy_device_identity_busy"
    finally:
        _release_lock(first)


@pytest.mark.anyio
async def test_child_inherits_device_lock_until_it_exits(tmp_path: Path) -> None:
    path = tmp_path / "device.lock"
    first = _try_acquire_lock(path)
    assert first is not None
    child = await anyio.open_process(
        ["/bin/sleep", "60"],
        pass_fds=(first,),
    )
    os.close(first)
    try:
        assert _try_acquire_lock(path) is None
    finally:
        child.terminate()
        await child.wait()
    replacement = _try_acquire_lock(path)
    assert replacement is not None
    _release_lock(replacement)


@pytest.mark.anyio
async def test_parent_death_exec_rejects_a_missing_expected_parent() -> None:
    completed = await anyio.run_process(
        [
            sys.executable,
            "-m",
            "carl.io.parent_death_exec",
            str(os.getpid() + 10_000_000),
            "/bin/sleep",
            "60",
        ],
        check=False,
    )

    assert completed.returncode == -signal.SIGTERM


@pytest.mark.anyio
async def test_manager_shields_process_and_secret_cleanup_from_level_cancellation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binary = tmp_path / "wireproxy"
    binary.write_text("#!/bin/sh\nexec sleep 60\n")
    binary.chmod(0o700)
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)

    async def verify(*, wireproxy: WireproxyIdentity, wireproxy_path: Path) -> None:
        del wireproxy, wireproxy_path

    async def wait_for_listener(
        process: anyio.abc.Process,
        endpoint: object,
        *,
        timeout_seconds: float,
    ) -> None:
        del process, endpoint
        assert timeout_seconds == 2

    monkeypatch.setattr("carl.io.wireproxy._verify_wireproxy", verify)
    monkeypatch.setattr("carl.io.wireproxy._wait_until_listening", wait_for_listener)

    completed_session = None
    with anyio.CancelScope() as scope:
        async with WireproxyProcessManager().open(
            provider=NetworkProvider.PROTON,
            wireproxy=WireproxyIdentity(version="1.1.3", binary_sha256="a" * 64),
            wireproxy_path=binary,
            runtime_directory=runtime,
            device_lock_identity=b"one",
            render_configuration=lambda port: render_wireguard_configuration(
                _configuration(), port=port
            ),
            startup_timeout_seconds=2,
            shutdown_timeout_seconds=2,
        ) as opened_session:
            completed_session = opened_session
            executed_binary = Path(opened_session.process_argv[0])
            assert executed_binary != binary
            assert executed_binary.read_bytes() == binary.read_bytes()
            assert stat.S_IMODE(executed_binary.stat().st_mode) == 0o500
            assert Path(opened_session.process_argv[-1]).stat().st_mode & 0o777 == 0o600
            scope.cancel()
            await anyio.sleep_forever()

    assert completed_session is not None
    assert completed_session.shutdown_state == "terminated"
    assert completed_session.stopped_at_utc is not None
    assert tuple(runtime.glob("proton-wireproxy-*")) == ()


@pytest.mark.anyio
async def test_unconfirmed_process_stop_retains_lock_and_secret_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binary = tmp_path / "wireproxy"
    binary.write_text("#!/bin/sh\nexec sleep 60\n")
    binary.chmod(0o700)
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)

    async def verify(*, wireproxy: WireproxyIdentity, wireproxy_path: Path) -> None:
        del wireproxy, wireproxy_path

    async def wait_for_listener(
        process: anyio.abc.Process,
        endpoint: object,
        *,
        timeout_seconds: float,
    ) -> None:
        del process, endpoint, timeout_seconds

    async def fail_stop(process: anyio.abc.Process, *, timeout_seconds: float) -> str:
        del timeout_seconds
        process.terminate()
        await process.wait()
        raise TimeoutError

    monkeypatch.setattr("carl.io.wireproxy._verify_wireproxy", verify)
    monkeypatch.setattr("carl.io.wireproxy._wait_until_listening", wait_for_listener)
    monkeypatch.setattr("carl.io.wireproxy._stop_process", fail_stop)

    with pytest.raises(ManagedWireproxyFailure) as raised, anyio.CancelScope() as scope:
        async with WireproxyProcessManager().open(
            provider=NetworkProvider.PROTON,
            wireproxy=WireproxyIdentity(version="1.1.3", binary_sha256="a" * 64),
            wireproxy_path=binary,
            runtime_directory=runtime,
            device_lock_identity=b"retained",
            render_configuration=lambda port: render_wireguard_configuration(
                _configuration(), port=port
            ),
            startup_timeout_seconds=2,
            shutdown_timeout_seconds=2,
        ):
            scope.cancel()
            await anyio.sleep_forever()

    assert raised.value.code == "wireproxy_cleanup_failed"
    assert raised.value.diagnostic["failures"] == ["process_termination_unconfirmed"]
    assert raised.value.diagnostic["primary_exception_type"] == "Cancelled"
    assert tuple(runtime.glob("proton-wireproxy-*/wireproxy.conf"))
    device_locks = tuple(runtime.glob("wireproxy-device-*.lock"))
    assert len(device_locks) == 1
    assert _try_acquire_lock(device_locks[0]) is None
