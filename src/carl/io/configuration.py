"""Safe loading and private material import for Carl configuration."""

import hashlib
import io
import os
import secrets
import stat
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from pydantic import SecretStr, ValidationError

from carl.core.configuration import (
    BrightDataRouteConfiguration,
    CarlConfiguration,
    DecodoRouteConfiguration,
    MullvadRouteConfiguration,
    ProtonRouteConfiguration,
)
from carl.core.models import ConfigurationDocumentIdentity, JsonValue, StrictModel
from carl.io.bright_data import BrightDataProxySettings
from carl.io.decodo import (
    DecodoCredentials,
    DecodoProxySettings,
    StaticDecodoCredentialSource,
)
from carl.io.mullvad import MullvadWireproxySettings
from carl.io.paths import CarlDirectories
from carl.io.proton import ProtonWireproxySettings
from carl.io.wireproxy import (
    read_private_configuration,
    render_wireguard_configuration,
)

_MAX_CONFIGURATION_BYTES = 1024 * 1024
_MAX_WIREGUARD_CONFIGURATION_BYTES = 64 * 1024


class ConfigurationFailure(Exception):
    code: str

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class ConfigurationLoadFailure(ConfigurationFailure):
    pass


class ProtonConfigurationImportFailure(ConfigurationFailure):
    pass


class MullvadConfigurationImportFailure(ConfigurationFailure):
    pass


class DecodoCredentialConfigurationFailure(ConfigurationFailure):
    pass


@dataclass(frozen=True, slots=True)
class LoadedCarlConfiguration:
    configuration: CarlConfiguration
    document_sha256: str
    source_path: Path = field(repr=False)

    def identity(self) -> ConfigurationDocumentIdentity:
        return ConfigurationDocumentIdentity(
            schema=self.configuration.schema_identity,
            document_sha256=self.document_sha256,
        )


class ImportedProtonConfiguration(StrictModel):
    configuration_id: str
    configuration_reference: tuple[str, ...]

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class ImportedMullvadConfiguration(StrictModel):
    configuration_id: str
    configuration_reference: tuple[str, ...]

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class ConfiguredDecodoCredential(StrictModel):
    credential_id: str
    credential_reference: tuple[str, ...]

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


def _read_regular_file(
    path: Path,
    *,
    maximum_bytes: int,
    require_private: bool,
    failure: type[ConfigurationFailure],
    unavailable_code: str,
    permissions_code: str,
) -> bytes:
    try:
        original = path.lstat()
    except OSError:
        raise failure(unavailable_code) from None
    forbidden = stat.S_IRWXG | stat.S_IRWXO if require_private else stat.S_IWGRP | stat.S_IWOTH
    if (
        not stat.S_ISREG(original.st_mode)
        or original.st_uid != os.getuid()
        or stat.S_IMODE(original.st_mode) & forbidden
    ):
        raise failure(permissions_code)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError:
        raise failure(unavailable_code) from None
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or stat.S_IMODE(opened.st_mode) & forbidden
            or (opened.st_dev, opened.st_ino) != (original.st_dev, original.st_ino)
        ):
            raise failure(permissions_code)
        chunks = bytearray()
        while len(chunks) <= maximum_bytes:
            chunk = os.read(descriptor, min(65_536, maximum_bytes + 1 - len(chunks)))
            if not chunk:
                break
            chunks.extend(chunk)
        final = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
        ):
            raise failure("source_changed_during_read")
    finally:
        os.close(descriptor)
    if len(chunks) > maximum_bytes:
        raise failure("file_too_large")
    return bytes(chunks)


def load_configuration(path: Path) -> LoadedCarlConfiguration:
    raw = _read_regular_file(
        path,
        maximum_bytes=_MAX_CONFIGURATION_BYTES,
        require_private=True,
        failure=ConfigurationLoadFailure,
        unavailable_code="configuration_unavailable",
        permissions_code="configuration_permissions",
    )
    try:
        parsed = tomllib.loads(raw.decode("utf-8"))
        configuration = CarlConfiguration.model_validate(parsed)
    except (UnicodeError, tomllib.TOMLDecodeError, ValidationError):
        raise ConfigurationLoadFailure("invalid_configuration") from None
    return LoadedCarlConfiguration(
        configuration=configuration,
        document_sha256=hashlib.sha256(raw).hexdigest(),
        source_path=path,
    )


def _require_private_directory(
    path: Path, failure: type[ConfigurationFailure] = ProtonConfigurationImportFailure
) -> None:
    try:
        path.mkdir(mode=0o700, exist_ok=True)
        info = path.lstat()
    except OSError:
        raise failure("private_directory_unavailable") from None
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & (stat.S_IRWXG | stat.S_IRWXO)
    ):
        raise failure("private_directory_permissions")


def _write_all(descriptor: int, content: bytes) -> None:
    offset = 0
    while offset < len(content):
        offset += os.write(descriptor, content[offset:])


def _valid_private_identifier(value: str) -> bool:
    return bool(
        value
        and len(value) <= 64
        and value[0].isascii()
        and value[0].isalnum()
        and all(
            character.isascii() and (character.isalnum() or character in "_-")
            for character in value
        )
    )


def configure_decodo_credential(
    proxy_password: str,
    *,
    directories: CarlDirectories,
    credential_id: str,
) -> ConfiguredDecodoCredential:
    if not _valid_private_identifier(credential_id):
        raise DecodoCredentialConfigurationFailure("invalid_credential_id")
    if not proxy_password or "\n" in proxy_password or "\r" in proxy_password:
        raise DecodoCredentialConfigurationFailure("invalid_proxy_password")
    target = directories.decodo_credential_directory
    for directory in (directories.config, directories.config / "private", target):
        _require_private_directory(directory, DecodoCredentialConfigurationFailure)
    destination_name = f"{credential_id}.password"
    temporary_name = f".configure-{secrets.token_hex(16)}.tmp"
    directory_descriptor = os.open(target, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY)
    temporary_descriptor: int | None = None
    published = False
    failure_code: str | None = None
    cleanup_failed = False
    try:
        temporary_descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory_descriptor,
        )
        _write_all(temporary_descriptor, proxy_password.encode("utf-8"))
        os.fsync(temporary_descriptor)
        os.close(temporary_descriptor)
        temporary_descriptor = None
        try:
            os.link(
                temporary_name,
                destination_name,
                src_dir_fd=directory_descriptor,
                dst_dir_fd=directory_descriptor,
            )
        except FileExistsError:
            failure_code = "credential_already_exists"
        else:
            published = True
            try:
                os.fsync(directory_descriptor)
            except OSError:
                failure_code = "credential_published_unconfirmed"
    except OSError:
        failure_code = (
            "credential_published_unconfirmed" if published else "credential_write_failed"
        )
    finally:
        if temporary_descriptor is not None:
            try:
                os.close(temporary_descriptor)
            except OSError:
                cleanup_failed = True
        try:
            os.unlink(temporary_name, dir_fd=directory_descriptor)
        except FileNotFoundError:
            pass
        except OSError:
            cleanup_failed = True
        try:
            os.close(directory_descriptor)
        except OSError:
            cleanup_failed = True
    if cleanup_failed and failure_code is None:
        failure_code = (
            "credential_published_unconfirmed" if published else "credential_write_failed"
        )
    if failure_code is not None:
        raise DecodoCredentialConfigurationFailure(failure_code)
    return ConfiguredDecodoCredential(
        credential_id=credential_id,
        credential_reference=("carl", "configuration", "decodo", credential_id),
    )


def import_proton_configuration(
    source_path: Path,
    *,
    directories: CarlDirectories,
    configuration_id: str,
) -> ImportedProtonConfiguration:
    if (
        not configuration_id
        or len(configuration_id) > 64
        or not configuration_id[0].isalnum()
        or not configuration_id[0].isascii()
        or any(
            not (character.isascii() and (character.isalnum() or character in "_-"))
            for character in configuration_id
        )
    ):
        raise ProtonConfigurationImportFailure("invalid_configuration_id")
    raw = _read_regular_file(
        source_path,
        maximum_bytes=_MAX_WIREGUARD_CONFIGURATION_BYTES,
        require_private=False,
        failure=ProtonConfigurationImportFailure,
        unavailable_code="source_configuration_unavailable",
        permissions_code="source_configuration_permissions",
    )
    try:
        _ = render_wireguard_configuration(raw, port=1)
    except ValueError:
        raise ProtonConfigurationImportFailure("invalid_wireguard_configuration") from None

    private = directories.config / "private"
    proton = directories.proton_configuration_directory
    for directory in (directories.config, private, proton):
        _require_private_directory(directory)
    destination_name = f"{configuration_id}.conf"
    temporary_name = f".import-{secrets.token_hex(16)}.tmp"
    directory_descriptor = os.open(proton, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY)
    temporary_descriptor: int | None = None
    published = False
    failure_code: str | None = None
    cleanup_failed = False
    try:
        temporary_descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory_descriptor,
        )
        _write_all(temporary_descriptor, raw)
        os.fsync(temporary_descriptor)
        os.close(temporary_descriptor)
        temporary_descriptor = None
        try:
            os.link(
                temporary_name,
                destination_name,
                src_dir_fd=directory_descriptor,
                dst_dir_fd=directory_descriptor,
            )
        except FileExistsError:
            failure_code = "configuration_already_exists"
        else:
            published = True
            try:
                os.fsync(directory_descriptor)
            except OSError:
                failure_code = "configuration_import_published_unconfirmed"
    except OSError:
        failure_code = (
            "configuration_import_published_unconfirmed"
            if published
            else "configuration_import_failed"
        )
    finally:
        if temporary_descriptor is not None:
            try:
                os.close(temporary_descriptor)
            except OSError:
                cleanup_failed = True
        try:
            os.unlink(temporary_name, dir_fd=directory_descriptor)
        except FileNotFoundError:
            pass
        except OSError:
            cleanup_failed = True
        try:
            os.close(directory_descriptor)
        except OSError:
            cleanup_failed = True
    if cleanup_failed and failure_code is None:
        failure_code = (
            "configuration_import_published_unconfirmed"
            if published
            else "configuration_import_failed"
        )
    if failure_code is not None:
        raise ProtonConfigurationImportFailure(failure_code)
    return ImportedProtonConfiguration(
        configuration_id=configuration_id,
        configuration_reference=("carl", "configuration", "proton", configuration_id),
    )


def _mullvad_archive_member(archive_content: bytes, relay_hostname: str) -> bytes:
    try:
        with ZipFile(io.BytesIO(archive_content)) as archive:
            name = f"{relay_hostname}.conf"
            matches = [member for member in archive.infolist() if member.filename == name]
            if len(matches) != 1:
                raise ValueError
            member = matches[0]
            mode = member.external_attr >> 16
            file_type = stat.S_IFMT(mode)
            if member.flag_bits & 1 or member.file_size > _MAX_WIREGUARD_CONFIGURATION_BYTES:
                raise ValueError
            if stat.S_ISLNK(mode) or file_type not in (0, stat.S_IFREG):
                raise ValueError
            content = archive.read(member)
    except (BadZipFile, KeyError, OSError, ValueError):
        raise ConfigurationLoadFailure("invalid_mullvad_configuration_archive") from None
    if len(content) > _MAX_WIREGUARD_CONFIGURATION_BYTES:
        raise ConfigurationLoadFailure("invalid_mullvad_configuration_archive")
    return content


def import_mullvad_configuration_archive(
    source_path: Path,
    *,
    directories: CarlDirectories,
    configuration_id: str,
) -> ImportedMullvadConfiguration:
    if (
        not configuration_id
        or not configuration_id.isascii()
        or not configuration_id.replace("_", "").replace("-", "").isalnum()
    ):
        raise MullvadConfigurationImportFailure("invalid_configuration_id")
    raw = _read_regular_file(
        source_path,
        maximum_bytes=16 * 1024 * 1024,
        require_private=False,
        failure=MullvadConfigurationImportFailure,
        unavailable_code="source_configuration_unavailable",
        permissions_code="source_configuration_permissions",
    )
    try:
        with ZipFile(io.BytesIO(raw)) as archive:
            members = archive.infolist()
            if not members or len(members) > 10_000:
                raise ValueError
            names = [member.filename for member in members]
            if len(names) != len(set(names)):
                raise ValueError
            for member in members:
                mode = member.external_attr >> 16
                file_type = stat.S_IFMT(mode)
                if (
                    not member.filename.endswith(".conf")
                    or "/" in member.filename
                    or member.flag_bits & 1
                    or member.file_size > _MAX_WIREGUARD_CONFIGURATION_BYTES
                    or stat.S_ISLNK(mode)
                    or file_type not in (0, stat.S_IFREG)
                ):
                    raise ValueError
    except (BadZipFile, OSError, ValueError):
        raise MullvadConfigurationImportFailure("invalid_mullvad_configuration_archive") from None
    target = directories.mullvad_configuration_directory
    for directory in (directories.config, directories.config / "private", target):
        _require_private_directory(directory, MullvadConfigurationImportFailure)
    destination = target / f"{configuration_id}.zip"
    try:
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
    except FileExistsError:
        raise MullvadConfigurationImportFailure("configuration_already_exists") from None
    except OSError:
        raise MullvadConfigurationImportFailure("configuration_import_failed") from None
    try:
        _write_all(descriptor, raw)
        os.fsync(descriptor)
    except OSError:
        destination.unlink(missing_ok=True)
        raise MullvadConfigurationImportFailure("configuration_import_failed") from None
    finally:
        os.close(descriptor)
    return ImportedMullvadConfiguration(
        configuration_id=configuration_id,
        configuration_reference=("carl", "configuration", "mullvad", configuration_id),
    )


def bright_data_settings(
    loaded: LoadedCarlConfiguration, network_path: tuple[str, ...]
) -> BrightDataProxySettings:
    try:
        route = loaded.configuration.require_route(network_path)
    except KeyError:
        raise ConfigurationLoadFailure("route_not_found") from None
    if not isinstance(route, BrightDataRouteConfiguration):
        raise ConfigurationLoadFailure("route_provider_mismatch")
    return BrightDataProxySettings(
        route=route.route_identity(),
        configuration=loaded.identity(),
        max_idle_seconds=route.max_idle_seconds,
    )


def decodo_settings(
    loaded: LoadedCarlConfiguration,
    directories: CarlDirectories,
    network_path: tuple[str, ...],
) -> tuple[DecodoProxySettings, StaticDecodoCredentialSource]:
    try:
        route = loaded.configuration.require_route(network_path)
    except KeyError:
        raise ConfigurationLoadFailure("route_not_found") from None
    if not isinstance(route, DecodoRouteConfiguration):
        raise ConfigurationLoadFailure("route_provider_mismatch")
    credential_path = directories.decodo_credential_directory / f"{route.credential_id}.password"
    raw = _read_regular_file(
        credential_path,
        maximum_bytes=16 * 1024,
        require_private=True,
        failure=ConfigurationLoadFailure,
        unavailable_code="decodo_credential_unavailable",
        permissions_code="decodo_credential_permissions",
    )
    try:
        password = raw.decode("utf-8")
    except UnicodeError:
        raise ConfigurationLoadFailure("invalid_decodo_credential") from None
    if not password or "\n" in password or "\r" in password:
        raise ConfigurationLoadFailure("invalid_decodo_credential")
    return (
        DecodoProxySettings(
            route=route.route_identity(),
            configuration=loaded.identity(),
            session_duration_minutes=route.session_duration_minutes,
        ),
        StaticDecodoCredentialSource(DecodoCredentials(proxy_password=SecretStr(password))),
    )


def proton_settings(
    loaded: LoadedCarlConfiguration,
    directories: CarlDirectories,
    network_path: tuple[str, ...],
) -> ProtonWireproxySettings:
    try:
        route = loaded.configuration.require_route(network_path)
    except KeyError:
        raise ConfigurationLoadFailure("route_not_found") from None
    if not isinstance(route, ProtonRouteConfiguration):
        raise ConfigurationLoadFailure("route_provider_mismatch")
    configuration_path = (
        directories.proton_configuration_directory / f"{route.configuration_id}.conf"
    )
    raw = read_private_configuration(
        configuration_path,
        unavailable_code="proton_configuration_unavailable",
        permissions_code="proton_configuration_permissions",
    )
    try:
        _ = render_wireguard_configuration(
            raw,
            port=1,
            expected_peer_endpoint=route.peer_endpoint,
            internet_protocol_version=route.internet_protocol_version,
        )
    except ValueError:
        raise ConfigurationLoadFailure("invalid_proton_configuration") from None
    return ProtonWireproxySettings(
        route=route.route_identity(),
        configuration=loaded.identity(),
        wireproxy=loaded.configuration.wireproxy.identity(),
        wireproxy_path=Path(loaded.configuration.wireproxy.executable_path),
        configuration_content=raw,
        runtime_directory=directories.wireproxy_runtime_directory,
        startup_timeout_seconds=route.startup_timeout_seconds,
        shutdown_timeout_seconds=route.shutdown_timeout_seconds,
    )


def mullvad_settings(
    loaded: LoadedCarlConfiguration,
    directories: CarlDirectories,
    network_path: tuple[str, ...],
) -> MullvadWireproxySettings:
    try:
        route = loaded.configuration.require_route(network_path)
    except KeyError:
        raise ConfigurationLoadFailure("route_not_found") from None
    if not isinstance(route, MullvadRouteConfiguration):
        raise ConfigurationLoadFailure("route_provider_mismatch")
    archive_path = directories.mullvad_configuration_directory / f"{route.configuration_id}.zip"
    archive = _read_regular_file(
        archive_path,
        maximum_bytes=16 * 1024 * 1024,
        require_private=True,
        failure=ConfigurationLoadFailure,
        unavailable_code="mullvad_configuration_archive_unavailable",
        permissions_code="mullvad_configuration_archive_permissions",
    )
    content = _mullvad_archive_member(archive, route.relay_hostname)
    try:
        _ = render_wireguard_configuration(
            content, port=1, dns_override=("10.64.0.1",), mtu_override=1280
        )
    except ValueError:
        raise ConfigurationLoadFailure("invalid_mullvad_configuration") from None
    return MullvadWireproxySettings(
        route=route.route_identity(),
        configuration=loaded.identity(),
        wireproxy=loaded.configuration.wireproxy.identity(),
        wireproxy_path=Path(loaded.configuration.wireproxy.executable_path),
        configuration_content=content,
        runtime_directory=directories.wireproxy_runtime_directory,
        startup_timeout_seconds=route.startup_timeout_seconds,
        shutdown_timeout_seconds=route.shutdown_timeout_seconds,
    )
