"""Bounded command and result values for one physically owned native build phase."""

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator

from carl.core.models import StrictModel

MAX_NATIVE_BUILD_SPEC_BYTES = 65_536
MAX_NATIVE_BUILD_LOG_BYTES = 64 * 1024 * 1024


class NativeBuildSpec(StrictModel):
    recipe_identifier: str = Field(min_length=1, max_length=256)
    phase_identifier: str = Field(min_length=1, max_length=128)
    input_identity: str = Field(min_length=1, max_length=256)
    execution_identifier: str = Field(min_length=1, max_length=128)
    argv: tuple[str, ...] = Field(min_length=1, max_length=256)
    directory: Path
    receipt_directory: Path
    threads: int = Field(ge=1, le=16)
    timeout_seconds: float = Field(gt=0, le=86_400)
    stdout_limit_bytes: int = Field(default=8 * 1024 * 1024, ge=1, le=MAX_NATIVE_BUILD_LOG_BYTES)
    stderr_limit_bytes: int = Field(default=8 * 1024 * 1024, ge=1, le=MAX_NATIVE_BUILD_LOG_BYTES)
    combined_limit_bytes: int = Field(default=16 * 1024 * 1024, ge=1, le=MAX_NATIVE_BUILD_LOG_BYTES)
    on_output_limit: Literal["terminate", "truncate"] = "terminate"

    @field_validator("directory", "receipt_directory")
    @classmethod
    def absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute() or len(str(value).encode()) > 4096:
            raise ValueError("Native build paths must be bounded and absolute")
        return value

    @field_validator("argv")
    @classmethod
    def bounded_arguments(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any("\x00" in argument or len(argument.encode()) > 16_384 for argument in value):
            raise ValueError("Native build arguments must be bounded and contain no NUL")
        if not value[0] or not Path(value[0]).is_absolute():
            raise ValueError("Native build executables must be absolute")
        return value

    @model_validator(mode="after")
    def bounded_spec(self) -> "NativeBuildSpec":
        if len(self.model_dump_json().encode()) > MAX_NATIVE_BUILD_SPEC_BYTES:
            raise ValueError("Native build specification exceeds 64 KiB")
        return self


class NativeBuildResult(StrictModel):
    execution_identifier: str = Field(min_length=1, max_length=128)
    reason: Literal[
        "completed",
        "exit_nonzero",
        "timeout",
        "output_limit",
        "cancelled",
        "launch_failure",
        "supervisor_failure",
    ]
    returncode: int | None
    stdout_head: str = Field(max_length=4096)
    stdout_bytes: int = Field(ge=0, le=MAX_NATIVE_BUILD_LOG_BYTES)
    stderr_bytes: int = Field(ge=0, le=MAX_NATIVE_BUILD_LOG_BYTES)
    stdout_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    stderr_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    output_truncated: bool = False

    @model_validator(mode="after")
    def coherent_exit(self) -> "NativeBuildResult":
        if self.reason == "completed" and self.returncode != 0:
            raise ValueError("A completed native build requires exit code zero")
        if self.reason == "exit_nonzero" and (self.returncode is None or self.returncode == 0):
            raise ValueError("A failed native exit requires a nonzero exit code")
        return self
