"""Centralized platform-specific locations for Carl's local state."""

from pathlib import Path

from platformdirs import PlatformDirs
from pydantic import field_validator

from carl.core.models import StrictModel


class CarlDirectories(StrictModel):
    config: Path
    data: Path
    cache: Path
    state: Path
    runtime: Path

    @field_validator("config", "data", "cache", "state", "runtime")
    @classmethod
    def require_absolute(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("Carl directories must be absolute")
        return value

    @property
    def configuration_file(self) -> Path:
        return self.config / "config.toml"

    @property
    def database_file(self) -> Path:
        return self.data / "carl.sqlite3"

    @property
    def image_directory(self) -> Path:
        return self.data / "images"

    @property
    def proton_configuration_directory(self) -> Path:
        return self.config / "private" / "proton"

    @property
    def decodo_credential_directory(self) -> Path:
        return self.config / "private" / "decodo"

    @property
    def mullvad_configuration_directory(self) -> Path:
        return self.config / "private" / "mullvad"

    @property
    def wireproxy_runtime_directory(self) -> Path:
        return self.runtime / "wireproxy"


def user_directories() -> CarlDirectories:
    locations = PlatformDirs(appname="carl", appauthor=False)
    return CarlDirectories(
        config=locations.user_config_path,
        data=locations.user_data_path,
        cache=locations.user_cache_path,
        state=locations.user_state_path,
        runtime=locations.user_runtime_path,
    )
