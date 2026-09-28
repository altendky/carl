"""Provider-neutral WireGuard configuration and managed wireproxy lifecycle."""

import base64
import configparser
import fcntl
import hashlib
import ipaddress
import os
import re
import socket
import stat
import subprocess
import sys
import tempfile
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import anyio

from carl.core.models import JsonValue
from carl.core.routing import (
    InternetProtocolVersion,
    LocalSocks5Endpoint,
    NetworkProvider,
    WireproxyIdentity,
    WireproxyShutdownState,
)
from carl.io.httpx import RouteConfigurationFailure

_MAX_CONFIG_BYTES = 64 * 1024
_MAX_WIREPROXY_BYTES = 256 * 1024 * 1024
_HOSTNAME = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$")


class ManagedWireproxyFailure(Exception):
    def __init__(
        self,
        code: str,
        *,
        exit_code: int | None = None,
        diagnostic: dict[str, JsonValue] | None = None,
    ):
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code
        self.diagnostic = diagnostic or {}


@dataclass(slots=True)
class ManagedWireproxyProcess:
    endpoint: LocalSocks5Endpoint
    wireproxy: WireproxyIdentity
    process_argv: tuple[str, ...]
    started_at_utc: str
    proxy_listening_at_utc: str
    stopped_at_utc: str | None = None
    shutdown_state: WireproxyShutdownState | None = None
    exit_code: int | None = None
    exited: anyio.Event = field(default_factory=anyio.Event, repr=False)
    child: anyio.abc.Process | None = field(default=None, repr=False)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_wireproxy(source: Path, destination: Path) -> None:
    try:
        source_info = source.lstat()
    except OSError:
        raise RouteConfigurationFailure("wireproxy_unavailable") from None
    if (
        not stat.S_ISREG(source_info.st_mode)
        or source_info.st_uid not in {0, os.getuid()}
        or not source_info.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        or stat.S_IMODE(source_info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH)
    ):
        raise RouteConfigurationFailure("unsafe_wireproxy_executable")
    source_descriptor: int | None = None
    destination_descriptor: int | None = None
    try:
        source_descriptor = os.open(source, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        opened = os.fstat(source_descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid not in {0, os.getuid()}
            or not opened.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            or stat.S_IMODE(opened.st_mode) & (stat.S_IWGRP | stat.S_IWOTH)
            or (opened.st_dev, opened.st_ino) != (source_info.st_dev, source_info.st_ino)
            or opened.st_size > _MAX_WIREPROXY_BYTES
        ):
            raise RouteConfigurationFailure("unsafe_wireproxy_executable")
        destination_descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o500,
        )
        os.fchmod(destination_descriptor, 0o500)
        remaining = opened.st_size
        while remaining:
            chunk = os.read(source_descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise RouteConfigurationFailure("wireproxy_snapshot_incomplete")
            offset = 0
            while offset < len(chunk):
                offset += os.write(destination_descriptor, chunk[offset:])
            remaining -= len(chunk)
        if os.read(source_descriptor, 1):
            raise RouteConfigurationFailure("wireproxy_snapshot_incomplete")
        os.fsync(destination_descriptor)
    except OSError:
        raise RouteConfigurationFailure("wireproxy_snapshot_failed") from None
    finally:
        if source_descriptor is not None:
            os.close(source_descriptor)
        if destination_descriptor is not None:
            os.close(destination_descriptor)
        if destination.exists() and sys.exception() is not None:
            destination.unlink(missing_ok=True)


def _key(value: str) -> str:
    try:
        decoded = base64.b64decode(value, validate=True)
    except (TypeError, ValueError):
        raise ValueError("Invalid WireGuard key") from None
    if len(decoded) != 32:
        raise ValueError("Invalid WireGuard key")
    return value


def wireguard_device_lock_identity(raw: bytes) -> bytes:
    """Derive an in-memory lock identity from the WireGuard private key."""

    if len(raw) > _MAX_CONFIG_BYTES:
        raise ValueError("WireGuard configuration exceeds limit")
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    try:
        parser.read_string(raw.decode("utf-8-sig"))
        encoded = parser["Interface"]["PrivateKey"].strip()
        private_key = base64.b64decode(encoded, validate=True)
        if len(private_key) != 32:
            raise ValueError("Invalid WireGuard key")
    except (KeyError, UnicodeError, configparser.Error, ValueError):
        raise ValueError("Invalid WireGuard configuration") from None
    return hashlib.sha256(b"carl wireguard device lock v1\0" + private_key).digest()


def _endpoint(value: str) -> str:
    text = value.strip()
    if text.startswith("["):
        closing = text.find("]")
        if closing < 0 or closing + 1 >= len(text) or text[closing + 1] != ":":
            raise ValueError("Invalid WireGuard endpoint")
        host = str(ipaddress.IPv6Address(text[1:closing]))
        rendered_host = f"[{host}]"
        port_text = text[closing + 2 :]
    else:
        host_text, port_text = text.rsplit(":", 1)
        try:
            rendered_host = str(ipaddress.IPv4Address(host_text))
        except ipaddress.AddressValueError:
            if not _HOSTNAME.fullmatch(host_text):
                raise ValueError("Invalid WireGuard endpoint") from None
            rendered_host = host_text
    port = int(port_text)
    if not 1 <= port <= 65_535:
        raise ValueError("Invalid WireGuard endpoint")
    return f"{rendered_host}:{port}"


def render_wireguard_configuration(
    raw: bytes,
    *,
    port: int,
    dns_override: tuple[str, ...] | None = None,
    mtu_override: int | None = None,
    expected_peer_endpoint: str | None = None,
    internet_protocol_version: InternetProtocolVersion = InternetProtocolVersion.DUAL_STACK,
) -> bytes:
    """Rebuild only WireGuard fields required by wireproxy."""

    if len(raw) > _MAX_CONFIG_BYTES:
        raise ValueError("WireGuard configuration exceeds limit")
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    try:
        parser.read_string(raw.decode("utf-8-sig"))
        if set(parser.sections()) != {"Interface", "Peer"}:
            raise ValueError("Expected one Interface and one Peer")
        interface = parser["Interface"]
        peer = parser["Peer"]
        private_key = _key(interface["PrivateKey"].strip())
        parsed_addresses = tuple(
            ipaddress.ip_interface(value.strip()) for value in interface["Address"].split(",")
        )
        public_key = _key(peer["PublicKey"].strip())
        endpoint = _endpoint(peer["Endpoint"])
        if expected_peer_endpoint is not None and endpoint != _endpoint(expected_peer_endpoint):
            raise ValueError("WireGuard peer endpoint does not match route identity")
        allowed_networks = tuple(
            ipaddress.ip_network(value.strip()) for value in peer["AllowedIPs"].split(",")
        )
        keepalive = int(peer.get("PersistentKeepalive", "25"))
        if not 0 <= keepalive <= 65_535:
            raise ValueError("Invalid persistent keepalive")
        preshared_key = peer.get("PresharedKey")
        if preshared_key is not None:
            preshared_key = _key(preshared_key.strip())
        dns_values = dns_override
        if dns_values is None:
            configured_dns = interface.get("DNS")
            dns_values = (
                tuple(
                    str(ipaddress.ip_address(value.strip())) for value in configured_dns.split(",")
                )
                if configured_dns
                else ()
            )
        if not dns_values:
            raise ValueError("WireGuard DNS is required")
        parsed_dns = tuple(ipaddress.ip_address(value) for value in dns_values)
        selected_version = {
            InternetProtocolVersion.VERSION_4: 4,
            InternetProtocolVersion.VERSION_6: 6,
        }.get(internet_protocol_version)
        addresses = tuple(
            str(address)
            for address in parsed_addresses
            if selected_version is None or address.version == selected_version
        )
        allowed_networks = tuple(
            network
            for network in allowed_networks
            if selected_version is None or network.version == selected_version
        )
        parsed_dns = tuple(
            address
            for address in parsed_dns
            if selected_version is None or address.version == selected_version
        )
        dns_values = tuple(str(address) for address in parsed_dns)
        if not addresses or not allowed_networks or not parsed_dns:
            raise ValueError("WireGuard configuration does not support selected IP version")
        if any(
            not any(
                address.version == network.version and address in network
                for network in allowed_networks
            )
            for address in parsed_dns
        ):
            raise ValueError("WireGuard DNS is outside AllowedIPs")
        mtu = mtu_override
        if mtu is None and interface.get("MTU") is not None:
            mtu = int(interface["MTU"])
        if mtu is not None and not 576 <= mtu <= 65_535:
            raise ValueError("Invalid MTU")
    except (KeyError, UnicodeError, configparser.Error, ValueError):
        raise ValueError("Invalid WireGuard configuration") from None

    lines = [
        "[Interface]",
        f"PrivateKey = {private_key}",
        f"Address = {', '.join(addresses)}",
    ]
    if dns_values:
        lines.append(f"DNS = {', '.join(dns_values)}")
    if mtu is not None:
        lines.append(f"MTU = {mtu}")
    lines.extend(
        [
            "",
            "[Peer]",
            f"PublicKey = {public_key}",
            f"Endpoint = {endpoint}",
            f"AllowedIPs = {', '.join(str(network) for network in allowed_networks)}",
            f"PersistentKeepalive = {keepalive}",
        ]
    )
    if preshared_key is not None:
        lines.append(f"PresharedKey = {preshared_key}")
    lines.extend(["", "[Socks5]", f"BindAddress = 127.0.0.1:{port}", ""])
    return "\n".join(lines).encode()


def read_private_configuration(
    path: Path, *, unavailable_code: str, permissions_code: str
) -> bytes:
    try:
        info = path.lstat()
    except OSError:
        raise RouteConfigurationFailure(unavailable_code) from None
    mode = stat.S_IMODE(info.st_mode)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or mode & (stat.S_IRWXG | stat.S_IRWXO)
    ):
        raise RouteConfigurationFailure(permissions_code)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError:
        raise RouteConfigurationFailure(unavailable_code) from None
    try:
        opened_info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened_info.st_mode)
            or opened_info.st_uid != os.getuid()
            or stat.S_IMODE(opened_info.st_mode) & (stat.S_IRWXG | stat.S_IRWXO)
            or (opened_info.st_dev, opened_info.st_ino) != (info.st_dev, info.st_ino)
        ):
            raise RouteConfigurationFailure(permissions_code)
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(_MAX_CONFIG_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(raw) > _MAX_CONFIG_BYTES:
        raise RouteConfigurationFailure("wireguard_configuration_too_large")
    return raw


def _ensure_private_runtime_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stat.S_IMODE(path.stat().st_mode) & (stat.S_IRWXG | stat.S_IRWXO):
        raise RouteConfigurationFailure("wireproxy_runtime_directory_permissions")


def _reserve_loopback_port() -> socket.socket:
    reservation = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        reservation.bind(("127.0.0.1", 0))
    except BaseException:
        reservation.close()
        raise
    return reservation


def _try_acquire_lock(path: Path) -> int | None:
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(descriptor)
        return None
    return descriptor


async def _wait_for_lock(path: Path, *, timeout_seconds: float, failure_code: str) -> int:
    try:
        with anyio.fail_after(timeout_seconds):
            while True:
                descriptor = _try_acquire_lock(path)
                if descriptor is not None:
                    return descriptor
                await anyio.sleep(0.05)
    except TimeoutError:
        raise ManagedWireproxyFailure(failure_code) from None


def _release_lock(descriptor: int) -> None:
    try:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


async def _verify_wireproxy(*, wireproxy: WireproxyIdentity, wireproxy_path: Path) -> None:
    try:
        binary_stat = wireproxy_path.lstat()
    except OSError:
        raise RouteConfigurationFailure("wireproxy_unavailable") from None
    if (
        not stat.S_ISREG(binary_stat.st_mode)
        or binary_stat.st_uid not in {0, os.getuid()}
        or not os.access(wireproxy_path, os.X_OK)
        or stat.S_IMODE(binary_stat.st_mode) & (stat.S_IWGRP | stat.S_IWOTH)
    ):
        raise RouteConfigurationFailure("unsafe_wireproxy_executable")
    digest = await anyio.to_thread.run_sync(_sha256_file, wireproxy_path)
    if digest != wireproxy.binary_sha256:
        raise RouteConfigurationFailure("wireproxy_hash_mismatch")
    try:
        with anyio.fail_after(10):
            completed = await anyio.run_process(
                [str(wireproxy_path), "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=False,
            )
    except (OSError, TimeoutError):
        raise RouteConfigurationFailure("wireproxy_version_unavailable") from None
    expected = f"wireproxy, version {wireproxy.version}".encode()
    if completed.returncode != 0 or completed.stdout.strip() != expected:
        raise RouteConfigurationFailure("wireproxy_version_mismatch")


async def _wait_until_listening(
    process: anyio.abc.Process,
    endpoint: LocalSocks5Endpoint,
    *,
    timeout_seconds: float,
) -> None:
    with anyio.fail_after(timeout_seconds):
        while True:
            if process.returncode is not None:
                raise ManagedWireproxyFailure(
                    "wireproxy_exited_during_startup", exit_code=process.returncode
                )
            try:
                with anyio.fail_after(0.5):
                    stream = await anyio.connect_tcp(str(endpoint.host), endpoint.port)
                await stream.aclose()
                await anyio.sleep(0.05)
                if process.returncode is None:
                    return
            except (OSError, TimeoutError):
                await anyio.sleep(0.1)


async def _stop_process(
    process: anyio.abc.Process, *, timeout_seconds: float
) -> WireproxyShutdownState:
    if process.returncode is not None:
        await process.wait()
        return "already_exited"
    process.terminate()
    with anyio.move_on_after(timeout_seconds / 2) as terminated:
        await process.wait()
    if not terminated.cancel_called:
        return "terminated"
    if process.returncode is None:
        process.kill()
    with anyio.fail_after(timeout_seconds / 2):
        await process.wait()
    return "killed"


class WireproxyProcessManager:
    @asynccontextmanager
    async def open(
        self,
        *,
        provider: NetworkProvider,
        wireproxy: WireproxyIdentity,
        wireproxy_path: Path,
        runtime_directory: Path,
        device_lock_identity: bytes,
        render_configuration: Callable[[int], bytes],
        startup_timeout_seconds: float,
        shutdown_timeout_seconds: float,
    ) -> AsyncGenerator[ManagedWireproxyProcess]:
        port_reservation: socket.socket | None = None
        port_lock: int | None = None
        device_lock: int | None = None
        folder: Path | None = None
        configuration_path: Path | None = None
        wireproxy_snapshot_path: Path | None = None
        process: anyio.abc.Process | None = None
        process_exit: anyio.Event | None = None
        session: ManagedWireproxyProcess | None = None
        port: int | None = None
        try:
            await anyio.to_thread.run_sync(_ensure_private_runtime_directory, runtime_directory)
            lock_name = hashlib.sha256(
                provider.value.encode() + b"\0" + device_lock_identity
            ).hexdigest()
            device_lock = await _wait_for_lock(
                runtime_directory / f"wireproxy-device-{lock_name}.lock",
                timeout_seconds=startup_timeout_seconds,
                failure_code="wireproxy_device_identity_busy",
            )
            port_lock = await _wait_for_lock(
                runtime_directory / "wireproxy-port-allocation.lock",
                timeout_seconds=startup_timeout_seconds,
                failure_code="wireproxy_port_allocation_busy",
            )
            with anyio.CancelScope(shield=True):
                port_reservation = _reserve_loopback_port()
                port = int(port_reservation.getsockname()[1])
                content = await anyio.to_thread.run_sync(render_configuration, port)
                folder = Path(
                    tempfile.mkdtemp(prefix=f"{provider.value}-wireproxy-", dir=runtime_directory)
                )
                os.chmod(folder, 0o700)
                wireproxy_snapshot_path = folder / "wireproxy"
                await anyio.to_thread.run_sync(
                    _snapshot_wireproxy, wireproxy_path, wireproxy_snapshot_path
                )
                configuration_path = folder / "wireproxy.conf"
                descriptor = os.open(
                    configuration_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                    0o600,
                )
                try:
                    with os.fdopen(descriptor, "wb", closefd=False) as stream:
                        stream.write(content)
                        stream.flush()
                        os.fsync(stream.fileno())
                finally:
                    os.close(descriptor)
                del content
            if wireproxy_snapshot_path is None:
                raise AssertionError("wireproxy snapshot was not created")
            await _verify_wireproxy(
                wireproxy=wireproxy,
                wireproxy_path=wireproxy_snapshot_path,
            )
            process_argv = (
                str(wireproxy_snapshot_path),
                "--silent",
                "--config",
                str(configuration_path),
            )
            guarded_process_argv = (
                sys.executable,
                "-m",
                "carl.io.parent_death_exec",
                str(os.getpid()),
                *process_argv,
            )
            if port_reservation is None:
                raise AssertionError("Loopback port was not reserved")
            if port is None:
                raise AssertionError("Loopback port was not selected")
            port_reservation.close()
            port_reservation = None
            started_at_utc = _utc_now()
            try:
                process = await anyio.open_process(
                    guarded_process_argv,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                    pass_fds=(device_lock,) if device_lock is not None else (),
                )
            except OSError:
                raise ManagedWireproxyFailure("wireproxy_start_failed") from None
            process_exit = anyio.Event()
            endpoint = LocalSocks5Endpoint(port=port)
            try:
                await _wait_until_listening(
                    process,
                    endpoint,
                    timeout_seconds=startup_timeout_seconds,
                )
            except TimeoutError:
                raise ManagedWireproxyFailure("wireproxy_startup_timeout") from None
            with anyio.CancelScope(shield=True):
                if port_lock is not None:
                    _release_lock(port_lock)
                    port_lock = None
            session = ManagedWireproxyProcess(
                endpoint=endpoint,
                wireproxy=wireproxy,
                process_argv=process_argv,
                started_at_utc=started_at_utc,
                proxy_listening_at_utc=_utc_now(),
                exited=process_exit,
                child=process,
            )
            yield session
        finally:
            active_exception = sys.exception()
            cleanup_failures: list[str] = []
            shutdown_state: WireproxyShutdownState = "already_exited"
            process_stop_confirmed = process is None
            with anyio.CancelScope(shield=True):
                if port_reservation is not None:
                    port_reservation.close()
                if process is not None:
                    try:
                        shutdown_state = await _stop_process(
                            process, timeout_seconds=shutdown_timeout_seconds
                        )
                        process_stop_confirmed = True
                        if process_exit is not None:
                            process_exit.set()
                        if session is not None:
                            session.exit_code = process.returncode
                    except (OSError, TimeoutError):
                        shutdown_state = "unconfirmed"
                        cleanup_failures.append("process_termination_unconfirmed")
                if (
                    process_exit is not None
                    and not process_exit.is_set()
                    and process_stop_confirmed
                ):
                    await process_exit.wait()
                if session is not None:
                    session.shutdown_state = shutdown_state
                    session.stopped_at_utc = _utc_now()
                if process_stop_confirmed:
                    try:
                        if configuration_path is not None:
                            configuration_path.unlink(missing_ok=True)
                        if wireproxy_snapshot_path is not None:
                            wireproxy_snapshot_path.unlink(missing_ok=True)
                        if folder is not None:
                            folder.rmdir()
                    except OSError:
                        cleanup_failures.append("secret_configuration_cleanup_failed")
                if device_lock is not None and process_stop_confirmed:
                    _release_lock(device_lock)
                if port_lock is not None:
                    _release_lock(port_lock)
            if cleanup_failures:
                diagnostic: dict[str, JsonValue] = {
                    "failures": cleanup_failures,
                    "primary_exception_type": (
                        type(active_exception).__name__ if active_exception is not None else None
                    ),
                }
                if active_exception is not None:
                    active_exception.add_note(
                        f"wireproxy cleanup failed: {', '.join(cleanup_failures)}"
                    )
                    result = getattr(active_exception, "result", None)
                    if isinstance(result, dict):
                        result["route_cleanup_failure"] = diagnostic
                    active_diagnostic = getattr(active_exception, "diagnostic", None)
                    if isinstance(active_diagnostic, dict):
                        active_diagnostic["wireproxy_cleanup_failure"] = diagnostic
                raise ManagedWireproxyFailure(
                    "wireproxy_cleanup_failed", diagnostic=diagnostic
                ) from active_exception
