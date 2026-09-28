"""Strict JSON validation and serialization at Carl's boundaries."""

from pydantic import ConfigDict, TypeAdapter
from pydantic import JsonValue as PydanticJsonValue

from carl.core.models import JsonValue

_JSON = TypeAdapter(PydanticJsonValue, config=ConfigDict(strict=True, allow_inf_nan=False))


def validate_json(value: object) -> JsonValue:
    """Return a recursively validated JSON value without coercion."""

    return _JSON.validate_python(value)


def decode_json(value: str | bytes) -> JsonValue:
    """Decode and validate JSON text."""

    return _JSON.validate_json(value)


def encode_json(value: object, *, indent: int | None = None) -> str:
    """Validate and encode a compact UTF-8 JSON value."""

    validated = validate_json(value)
    return _JSON.dump_json(validated, indent=indent).decode("utf-8")
