"""Bounded source plans and filesystem transfer receipts for dataset downloads."""

import hashlib
import re
from datetime import datetime
from pathlib import PurePosixPath
from typing import Literal
from urllib.parse import unquote, urlsplit

from pydantic import Field, field_validator, model_validator

from carl.core.models import StrictModel

MAX_DATASET_METADATA_BYTES = 16_384
MAX_DATASET_FILE_BYTES = 2**63 - 1
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,199}\Z")


def validate_dataset_url(value: str) -> str:
    if (
        len(value.encode()) > 4096
        or "\\" in value
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
    ):
        raise ValueError("Dataset URLs must be bounded HTTPS URLs")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or parsed.port == 0
    ):
        raise ValueError("Dataset URLs require HTTPS without credentials or fragments")
    if any(unquote(part) in (".", "..") for part in parsed.path.split("/")):
        raise ValueError("Dataset URL paths cannot traverse directories")
    return value


def validate_dataset_execution_identifier(value: str) -> str:
    if _TOKEN.fullmatch(value) is None:
        raise ValueError("Dataset execution identifiers must be bounded safe tokens")
    return value


class DatasetSource(StrictModel):
    identifier: str = Field(min_length=1, max_length=200)
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")
    url: str = Field(min_length=1, max_length=4096)
    maximum_bytes: int | None = Field(default=None, ge=1, le=MAX_DATASET_FILE_BYTES)
    expected_md5: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    expected_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    follow_redirects: bool = False

    @field_validator("identifier")
    @classmethod
    def bounded_identifier(cls, value: str) -> str:
        if (
            value != value.strip()
            or len(value.encode()) > 200
            or any(ord(character) < 32 or ord(character) == 127 for character in value)
        ):
            raise ValueError("Dataset source identifiers must be bounded and trimmed")
        return value

    @field_validator("url")
    @classmethod
    def safe_url(cls, value: str) -> str:
        return validate_dataset_url(value)

    @model_validator(mode="after")
    def bounded_metadata(self) -> "DatasetSource":
        if len(self.model_dump_json().encode()) > MAX_DATASET_METADATA_BYTES:
            raise ValueError("Dataset source metadata exceeds 16 KiB")
        return self


def dataset_source_fingerprint(source: DatasetSource) -> str:
    return hashlib.sha256(source.model_dump_json().encode()).hexdigest()


class _FileMetadata(StrictModel):
    schema_version: Literal[1] = 1
    source_identifier: str = Field(min_length=1, max_length=200)
    source_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    execution_identifier: str = Field(min_length=1, max_length=200)
    locator: str = Field(min_length=1, max_length=1024)
    requested_url: str = Field(min_length=1, max_length=4096)
    response_url: str = Field(min_length=1, max_length=4096)
    downloaded_at_utc: str = Field(min_length=1, max_length=64)
    last_modified: str | None = Field(default=None, max_length=1024)
    etag: str | None = Field(default=None, max_length=1024)
    cleanup_error_type: str | None = Field(default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")

    @field_validator("source_identifier")
    @classmethod
    def bounded_identifier(cls, value: str) -> str:
        return DatasetSource.bounded_identifier(value)

    @field_validator("requested_url", "response_url")
    @classmethod
    def safe_url(cls, value: str) -> str:
        return validate_dataset_url(value)

    @field_validator("execution_identifier")
    @classmethod
    def safe_execution(cls, value: str) -> str:
        return validate_dataset_execution_identifier(value)

    @field_validator("last_modified", "etag")
    @classmethod
    def bounded_header(cls, value: str | None) -> str | None:
        if value is not None and (
            len(value.encode()) > 1024
            or any(ord(character) < 32 or ord(character) == 127 for character in value)
        ):
            raise ValueError("Dataset response metadata must be bounded header values")
        return value

    @field_validator("downloaded_at_utc")
    @classmethod
    def timezone_required(cls, value: str) -> str:
        if datetime.fromisoformat(value).tzinfo is None:
            raise ValueError("Dataset download time requires a timezone")
        return value

    @model_validator(mode="after")
    def exact_locator_and_bound(self) -> "_FileMetadata":
        expected = f"downloads/{self.source_fingerprint}/{self.execution_identifier}.data"
        if self.locator != expected or PurePosixPath(self.locator).as_posix() != self.locator:
            raise ValueError("Dataset locator does not match its source and execution")
        if len(self.model_dump_json().encode()) > MAX_DATASET_METADATA_BYTES:
            raise ValueError("Dataset transfer metadata exceeds 16 KiB")
        return self


class DatasetPreparation(_FileMetadata):
    kind: Literal["dataset_prepared"] = "dataset_prepared"


class DatasetTransfer(_FileMetadata):
    kind: Literal["dataset_transfer"] = "dataset_transfer"
    bytes: int = Field(ge=1, le=MAX_DATASET_FILE_BYTES)


class FileReceipt(_FileMetadata):
    kind: Literal["dataset_file"] = "dataset_file"
    bytes: int = Field(ge=1, le=MAX_DATASET_FILE_BYTES)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    md5: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")


class DatasetFileError(RuntimeError):
    """Only bounded reason codes cross a dataset transport or file boundary."""

    def __init__(self, reason: str, *, status_code: int | None = None) -> None:
        self.reason: str = reason
        self.status_code: int | None = status_code
        super().__init__(reason)
