"""Source-neutral immutable values passed between Carl's core and adapters."""

from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    GetCoreSchemaHandler,
    field_serializer,
    field_validator,
)
from pydantic_core import core_schema

type JsonValue = Any


class JsonStringEnumeration(StrEnum):
    """A strict enum that accepts its documented string representation at JSON boundaries."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        schema = handler(source_type)

        def parse_string(value: object) -> object:
            if isinstance(value, str) and not isinstance(value, cls):
                try:
                    return cls(value)
                except ValueError:
                    pass
            return value

        return core_schema.no_info_before_validator_function(parse_string, schema)


class StrictModel(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
    )


class Namespace(JsonStringEnumeration):
    CARL = "carl"


class Domain(JsonStringEnumeration):
    CONFIGURATION = "configuration"
    STORAGE = "storage"


class StorageBackend(JsonStringEnumeration):
    SQLITE = "sqlite"


class StorageSchemaIdentity(StrictModel):
    namespace: Namespace
    domain: Domain
    backend: StorageBackend
    version: int = Field(ge=1)


class ConfigurationSchemaIdentity(StrictModel):
    namespace: Literal[Namespace.CARL]
    domain: Literal[Domain.CONFIGURATION]
    version: Literal[1]


class ConfigurationDocumentIdentity(StrictModel):
    schema_identity: ConfigurationSchemaIdentity = Field(alias="schema")
    document_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", by_alias=True)


class Header(StrictModel):
    """One HTTP header field; repetition and ordering are significant."""

    name: bytes = Field(
        validation_alias=AliasChoices("name", "name_latin1"),
        serialization_alias="name_latin1",
    )
    value: bytes = Field(
        validation_alias=AliasChoices("value", "value_latin1"),
        serialization_alias="value_latin1",
    )

    @field_validator("name", "value", mode="before")
    @classmethod
    def deserialize_latin1(cls, value: object) -> object:
        return value.encode("latin-1") if isinstance(value, str) else value

    @field_serializer("name", "value", when_used="json")
    def serialize_latin1(self, value: bytes) -> str:
        return value.decode("latin-1")

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", by_alias=True)


class DependencyVersion(StrictModel):
    name: str
    version: str | None


class CodeProvenance(StrictModel):
    repository_url: str | None
    commit_hash: str | None
    worktree_state: Literal["clean", "dirty", "unknown"]
    package_version: str
    python_implementation: str
    python_version: str
    dependencies: tuple[DependencyVersion, ...]
    lockfile_sha256: str | None

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class RecordDraft(StrictModel):
    identifier: str = Field(min_length=1)
    kind: tuple[str, ...]
    schema_version: int = Field(ge=1)
    value: JsonValue

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Record kinds require nonempty parts")
        return value


class BytesDraft(StrictModel):
    identifier: str = Field(min_length=1)
    kind: tuple[str, ...]
    media_type: str | None
    representation: JsonValue
    content: bytes

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Artifact kinds require nonempty parts")
        return value


class ExternalFileDraft(StrictModel):
    identifier: str = Field(min_length=1)
    kind: tuple[str, ...]
    media_type: str | None
    representation: JsonValue
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)
    locator: str = Field(min_length=1)

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Artifact kinds require nonempty parts")
        return value

    @field_validator("locator")
    @classmethod
    def validate_locator(cls, value: str) -> str:
        path = Path(value)
        if path.is_absolute() or value != path.as_posix() or not path.parts:
            raise ValueError("External artifact locator must be a normalized relative path")
        if any(part in {"", ".", ".."} for part in path.parts):
            raise ValueError("External artifact locator cannot traverse directories")
        return value


type ArtifactDraft = BytesDraft | ExternalFileDraft


class NamedOutput(StrictModel):
    name: tuple[str, ...]
    object_identifier: str = Field(min_length=1)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Output names require nonempty parts")
        return value


class NamedInput(StrictModel):
    name: tuple[str, ...]
    object_identifier: str = Field(min_length=1)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Input names require nonempty parts")
        return value
