import base64
import json
import os
import stat
from pathlib import Path
from zipfile import ZipFile

import pytest
from pydantic import ValidationError

from carl.core.configuration import CarlConfiguration
from carl.core.routing import (
    BrightDataProduct,
    DecodoProduct,
    InternetProtocolVersion,
    NetworkProvider,
)
from carl.io.configuration import (
    ConfigurationLoadFailure,
    DecodoCredentialConfigurationFailure,
    MullvadConfigurationImportFailure,
    ProtonConfigurationImportFailure,
    bright_data_settings,
    configure_decodo_credential,
    decodo_settings,
    decodo_wreq_stack_settings,
    import_mullvad_configuration_archive,
    import_proton_configuration,
    load_configuration,
    mullvad_settings,
    proton_settings,
)
from carl.io.paths import CarlDirectories, user_directories


def _directories(tmp_path: Path) -> CarlDirectories:
    return CarlDirectories(
        config=tmp_path / "config",
        data=tmp_path / "data",
        cache=tmp_path / "cache",
        state=tmp_path / "state",
        runtime=tmp_path / "runtime",
    )


def _wireguard_configuration(private: bytes = b"p" * 32) -> bytes:
    private_key = base64.b64encode(private).decode()
    public_key = base64.b64encode(b"u" * 32).decode()
    return f"""[Interface]
PrivateKey = {private_key}
Address = 10.2.0.2/32
DNS = 10.2.0.1

[Peer]
PublicKey = {public_key}
Endpoint = server.example:51820
AllowedIPs = 0.0.0.0/0
""".encode()


def _configuration_document(wireproxy_path: Path) -> bytes:
    return f"""[schema]
namespace = "carl"
domain = "configuration"
version = 1

[wireproxy]
executable_path = "{wireproxy_path}"
version = "1.1.3"
binary_sha256 = "{"a" * 64}"

[[http_transports]]
identifier = "browser_chrome_153"
implementation = "wreq"
emulation_profile = "chrome_153"

[[acquisition_stacks]]
identifier = "ebay_anonymous"
http_transport = "browser_chrome_153"
network_path = ["decodo", "personal", "carl"]

[[routes]]
provider = "bright_data"
network_path = ["bright_data", "personal", "marketplace_search"]
account_identifier = "personal"
proxy_username = "brd-customer-account-zone-marketplace"
credential_reference = ["one_password", "Test Vault", "Test Item", "password"]
zone_identifier = "marketplace-search"
product = "residential_proxy"
max_idle_seconds = 180.0

[routes.endpoint]
host = "brd.superproxy.io"
port = 33335

[[routes]]
provider = "decodo"
network_path = ["decodo", "personal", "carl"]
account_identifier = "personal"
proxy_username = "example"
credential_id = "carl"
product = "residential_proxy"
country_code = "us"
session_duration_minutes = 15

[routes.endpoint]
host = "gate.decodo.com"
port = 7000

[[routes]]
provider = "proton"
network_path = ["proton", "personal", "image"]
account_identifier = "personal"
configuration_id = "image"
peer_endpoint = "server.example:51820"
internet_protocol_version = "version_4"
startup_timeout_seconds = 30.0
shutdown_timeout_seconds = 8.0

[[routes]]
provider = "mullvad"
network_path = ["mullvad", "personal", "carl"]
account_identifier = "personal"
configuration_id = "carl"
relay_hostname = "us-was-wg-001"
""".encode()


def _write_configuration(tmp_path: Path) -> tuple[CarlDirectories, Path]:
    directories = _directories(tmp_path)
    directories.config.mkdir(mode=0o700)
    path = directories.configuration_file
    path.write_bytes(_configuration_document(tmp_path / "wireproxy"))
    path.chmod(0o600)
    return directories, path


def test_explicit_network_route_cutover(tmp_path: Path) -> None:
    directories, path = _write_configuration(tmp_path)
    with path.open("ab") as stream:
        stream.write(b"""\n[[route_overrides]]
requested_network_path = ["proton", "personal", "image"]
network_path = ["decodo", "personal", "carl"]
""")
    config = load_configuration(directories.configuration_file).configuration
    assert config.resolve_network_path(("proton", "personal", "image")) == (
        "decodo",
        "personal",
        "carl",
    )
    assert config.resolve_network_path(("proton", "personal", "other")) == (
        "proton",
        "personal",
        "other",
    )


@pytest.mark.parametrize("invalid_kind", ["duplicate", "missing", "cycle"])
def test_reject_invalid_network_route_cutovers(tmp_path: Path, invalid_kind: str) -> None:
    directories, _ = _write_configuration(tmp_path)
    value = load_configuration(directories.configuration_file).configuration.as_json()
    override = {
        "requested_network_path": ["proton", "personal", "image"],
        "network_path": ["decodo", "personal", "carl"],
    }
    if invalid_kind == "duplicate":
        overrides = [override, override]
    elif invalid_kind == "missing":
        overrides = [{**override, "network_path": ["decodo", "personal", "missing"]}]
    else:
        overrides = [
            override,
            {
                "requested_network_path": override["network_path"],
                "network_path": override["requested_network_path"],
            },
        ]
    value["route_overrides"] = overrides
    with pytest.raises(ValidationError):
        CarlConfiguration.model_validate_json(json.dumps(value))


def test_datacenter_configuration(tmp_path: Path) -> None:
    directories, path = _write_configuration(tmp_path)
    path.write_bytes(
        path.read_bytes()
        .replace(b'product = "residential_proxy"', b'product = "datacenter_proxy"')
        .replace(b"port = 7000", b"port = 10001")
    )
    loaded = load_configuration(directories.configuration_file)
    configure_decodo_credential("test-password", credential_id="carl", directories=directories)
    settings, _ = decodo_settings(loaded, directories, ("decodo", "personal", "carl"))
    assert settings.route.product == DecodoProduct.DATACENTER_PROXY
    assert settings.route.endpoint.port == 10001


def test_user_directories_follow_platform_conventions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    values = {
        "XDG_CONFIG_HOME": tmp_path / "configuration",
        "XDG_DATA_HOME": tmp_path / "data",
        "XDG_CACHE_HOME": tmp_path / "cache",
        "XDG_STATE_HOME": tmp_path / "state",
        "XDG_RUNTIME_DIR": tmp_path / "runtime",
    }
    for name, path in values.items():
        monkeypatch.setenv(name, str(path))

    directories = user_directories()

    assert directories.config == values["XDG_CONFIG_HOME"] / "carl"
    assert directories.data == values["XDG_DATA_HOME"] / "carl"
    assert directories.cache == values["XDG_CACHE_HOME"] / "carl"
    assert directories.state == values["XDG_STATE_HOME"] / "carl"
    assert directories.runtime == values["XDG_RUNTIME_DIR"] / "carl"
    assert directories.database_file == values["XDG_DATA_HOME"] / "carl" / "carl.sqlite3"
    assert directories.image_directory == values["XDG_DATA_HOME"] / "carl" / "images"
    assert directories.mcp_error_log_file == values["XDG_STATE_HOME"] / "carl" / "mcp-errors.jsonl"


def test_loads_strict_versioned_configuration_and_builds_bright_data_settings(
    tmp_path: Path,
) -> None:
    _, path = _write_configuration(tmp_path)

    loaded = load_configuration(path)
    settings = bright_data_settings(loaded, ("bright_data", "personal", "marketplace_search"))

    assert loaded.configuration.schema_identity.version == 1
    assert len(loaded.document_sha256) == 64
    assert loaded.identity().as_json()["schema"]["domain"] == "configuration"
    assert settings.route.provider is NetworkProvider.BRIGHT_DATA
    assert settings.route.product is BrightDataProduct.RESIDENTIAL_PROXY
    assert settings.route.zone_identifier == "marketplace-search"
    assert settings.route.credential_reference == (
        "one_password",
        "Test Vault",
        "Test Item",
        "password",
    )
    assert settings.max_idle_seconds == 180


def test_configures_and_loads_private_decodo_credential(tmp_path: Path) -> None:
    directories, path = _write_configuration(tmp_path)

    configured = configure_decodo_credential(
        "proxy-password-secret",
        directories=directories,
        credential_id="carl",
    )
    settings, source = decodo_settings(
        load_configuration(path), directories, ("decodo", "personal", "carl")
    )

    destination = directories.decodo_credential_directory / "carl.password"
    assert configured.credential_reference == (
        "carl",
        "configuration",
        "decodo",
        "carl",
    )
    assert destination.read_text() == "proxy-password-secret"
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert settings.route.provider is NetworkProvider.DECODO
    assert settings.route.product is DecodoProduct.RESIDENTIAL_PROXY
    assert settings.route.endpoint.host == "gate.decodo.com"
    assert settings.session_duration_minutes == 15
    assert "proxy-password-secret" not in repr((settings, source))

    with pytest.raises(DecodoCredentialConfigurationFailure, match="credential_already_exists"):
        configure_decodo_credential(
            "replacement",
            directories=directories,
            credential_id="carl",
        )


def test_resolves_configured_ebay_wreq_decodo_stack(tmp_path: Path) -> None:
    directories, path = _write_configuration(tmp_path)
    _ = configure_decodo_credential(
        "proxy-password-secret",
        directories=directories,
        credential_id="carl",
    )

    route, credential_source, transport = decodo_wreq_stack_settings(
        load_configuration(path), directories, "ebay_anonymous"
    )

    assert route.route.network_path == ("decodo", "personal", "carl")
    assert credential_source.credentials.proxy_password.get_secret_value() == (
        "proxy-password-secret"
    )
    assert transport.implementation == "wreq"
    assert transport.emulation_profile == "Chrome153"


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (b'http_transport = "browser_chrome_153"', b'http_transport = "missing"'),
        (
            b'network_path = ["decodo", "personal", "carl"]',
            b'network_path = ["decodo", "missing"]',
        ),
    ],
)
def test_configuration_rejects_dangling_acquisition_stack_references(
    tmp_path: Path, old: bytes, new: bytes
) -> None:
    _, path = _write_configuration(tmp_path)
    path.write_bytes(path.read_bytes().replace(old, new, 1))
    path.chmod(0o600)

    with pytest.raises(ConfigurationLoadFailure, match="invalid_configuration"):
        load_configuration(path)


@pytest.mark.parametrize(
    "replacement",
    [
        b"version = 2",
        b'password = "must-not-be-accepted"',
        b'network_path = ["proton", "wrong"]',
    ],
)
def test_invalid_configuration_fails_without_echoing_content(
    tmp_path: Path, replacement: bytes
) -> None:
    _, path = _write_configuration(tmp_path)
    content = path.read_bytes()
    if replacement.startswith(b"version"):
        content = content.replace(b"version = 1", replacement, 1)
    elif replacement.startswith(b"password"):
        content = content.replace(b'account_identifier = "personal"', replacement, 1)
    else:
        content = content.replace(
            b'network_path = ["bright_data", "personal", "marketplace_search"]',
            replacement,
            1,
        )
    path.write_bytes(content)
    path.chmod(0o600)

    with pytest.raises(ConfigurationLoadFailure) as raised:
        load_configuration(path)

    assert raised.value.code == "invalid_configuration"
    assert str(raised.value) == "invalid_configuration"


def test_configuration_requires_private_regular_file(tmp_path: Path) -> None:
    _, path = _write_configuration(tmp_path)
    path.chmod(0o644)

    with pytest.raises(ConfigurationLoadFailure) as raised:
        load_configuration(path)

    assert raised.value.code == "configuration_permissions"


def test_imports_proton_configuration_privately_without_modifying_source(
    tmp_path: Path,
) -> None:
    directories = _directories(tmp_path)
    source = tmp_path / "downloaded.conf"
    original = _wireguard_configuration()
    source.write_bytes(original)
    source.chmod(0o644)

    imported = import_proton_configuration(
        source, directories=directories, configuration_id="image"
    )
    destination = directories.proton_configuration_directory / "image.conf"

    assert imported.configuration_reference == (
        "carl",
        "configuration",
        "proton",
        "image",
    )
    assert source.read_bytes() == original
    assert stat.S_IMODE(source.stat().st_mode) == 0o644
    assert destination.read_bytes() == original
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert stat.S_IMODE(directories.config.stat().st_mode) == 0o700
    assert stat.S_IMODE(directories.proton_configuration_directory.stat().st_mode) == 0o700

    with pytest.raises(ProtonConfigurationImportFailure) as raised:
        import_proton_configuration(source, directories=directories, configuration_id="image")
    assert raised.value.code == "configuration_already_exists"


def test_import_rejects_symlink_and_group_writable_source(tmp_path: Path) -> None:
    directories = _directories(tmp_path)
    source = tmp_path / "source.conf"
    source.write_bytes(_wireguard_configuration())
    source.chmod(0o620)

    with pytest.raises(ProtonConfigurationImportFailure) as writable:
        import_proton_configuration(source, directories=directories, configuration_id="image")
    assert writable.value.code == "source_configuration_permissions"

    source.chmod(0o600)
    link = tmp_path / "linked.conf"
    link.symlink_to(source)
    with pytest.raises(ProtonConfigurationImportFailure) as symlink:
        import_proton_configuration(link, directories=directories, configuration_id="image")
    assert symlink.value.code == "source_configuration_permissions"


def test_import_reports_uncertain_state_if_directory_sync_fails_after_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directories = _directories(tmp_path)
    source = tmp_path / "source.conf"
    source.write_bytes(_wireguard_configuration())
    source.chmod(0o600)
    real_fsync = os.fsync
    calls = 0

    def fail_second_fsync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError
        real_fsync(descriptor)

    monkeypatch.setattr("carl.io.configuration.os.fsync", fail_second_fsync)

    with pytest.raises(ProtonConfigurationImportFailure) as raised:
        import_proton_configuration(source, directories=directories, configuration_id="image")

    assert raised.value.code == "configuration_import_published_unconfirmed"
    assert (directories.proton_configuration_directory / "image.conf").exists()


def test_proton_runtime_settings_use_fixed_import_and_hide_secret_material(
    tmp_path: Path,
) -> None:
    directories, path = _write_configuration(tmp_path)
    source = tmp_path / "downloaded.conf"
    source.write_bytes(_wireguard_configuration())
    source.chmod(0o600)
    import_proton_configuration(source, directories=directories, configuration_id="image")

    settings = proton_settings(
        load_configuration(path), directories, ("proton", "personal", "image")
    )
    safe = settings.safe_configuration()

    assert settings.configuration_content == _wireguard_configuration()
    assert settings.runtime_directory == directories.wireproxy_runtime_directory
    assert len(settings.device_lock_identity) == 32
    assert settings.route.internet_protocol_version is InternetProtocolVersion.VERSION_4
    assert "configuration_content" not in safe
    assert "runtime_directory" not in safe
    assert "device_lock_identity" not in safe
    assert base64.b64encode(b"p" * 32).decode() not in repr(settings)


def test_wireguard_lock_identity_follows_device_not_configuration_name(tmp_path: Path) -> None:
    directories, path = _write_configuration(tmp_path)
    source = tmp_path / "downloaded.conf"
    source.write_bytes(_wireguard_configuration())
    source.chmod(0o600)
    import_proton_configuration(source, directories=directories, configuration_id="image")

    first = proton_settings(load_configuration(path), directories, ("proton", "personal", "image"))
    duplicate = directories.proton_configuration_directory / "duplicate.conf"
    duplicate.write_bytes(_wireguard_configuration())
    duplicate.chmod(0o600)
    document = path.read_bytes().replace(
        b'configuration_id = "image"', b'configuration_id = "duplicate"'
    )
    path.write_bytes(document)
    path.chmod(0o600)
    second = proton_settings(load_configuration(path), directories, ("proton", "personal", "image"))

    assert first.device_lock_identity == second.device_lock_identity


def test_missing_or_wrong_provider_route_fails_closed(tmp_path: Path) -> None:
    directories, path = _write_configuration(tmp_path)
    loaded = load_configuration(path)

    with pytest.raises(ConfigurationLoadFailure, match="route_not_found"):
        bright_data_settings(loaded, ("bright_data", "missing"))
    with pytest.raises(ConfigurationLoadFailure, match="route_provider_mismatch"):
        proton_settings(loaded, directories, ("bright_data", "personal", "marketplace_search"))


def test_configuration_id_cannot_escape_private_directory(tmp_path: Path) -> None:
    source = tmp_path / "source.conf"
    source.write_bytes(_wireguard_configuration())
    source.chmod(0o600)

    with pytest.raises(ProtonConfigurationImportFailure, match="invalid_configuration_id"):
        import_proton_configuration(
            source,
            directories=_directories(tmp_path),
            configuration_id="../outside",
        )

    assert not (tmp_path / "outside.conf").exists()


def test_imports_and_resolves_exact_mullvad_relay(tmp_path: Path) -> None:
    directories, path = _write_configuration(tmp_path)
    source = tmp_path / "mullvad.zip"
    with ZipFile(source, "w") as archive:
        archive.writestr("us-was-wg-001.conf", _wireguard_configuration())
    source.chmod(0o600)

    imported = import_mullvad_configuration_archive(
        source, directories=directories, configuration_id="carl"
    )
    settings = mullvad_settings(
        load_configuration(path), directories, ("mullvad", "personal", "carl")
    )

    assert imported.configuration_reference == ("carl", "configuration", "mullvad", "carl")
    assert settings.route.relay_hostname == "us-was-wg-001"
    assert settings.configuration_content == _wireguard_configuration()
    assert "configuration_content" not in settings.safe_configuration()

    with pytest.raises(MullvadConfigurationImportFailure, match="configuration_already_exists"):
        import_mullvad_configuration_archive(
            source, directories=directories, configuration_id="carl"
        )
