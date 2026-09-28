import pytest
from pydantic import ValidationError

from carl.core.json import decode_json, encode_json
from carl.core.models import Domain, Header, Namespace, StorageBackend
from carl.io.sqlite import DATABASE_SCHEMA


def test_json_boundary_round_trips_without_ascii_escaping() -> None:
    encoded = encode_json({"label": "café", "values": [1, None, True]})

    assert encoded == '{"label":"café","values":[1,null,true]}'
    assert decode_json(encoded) == {"label": "café", "values": [1, None, True]}


@pytest.mark.parametrize("value", [{"tuple": (1, 2)}, {"number": float("nan")}])
def test_json_boundary_rejects_non_json_values(value: object) -> None:
    with pytest.raises(ValidationError):
        encode_json(value)


def test_header_uses_typed_latin1_serialization() -> None:
    header = Header(name=b"X-Test", value=b"caf\xe9")

    assert header.model_dump(mode="json", by_alias=True) == {
        "name_latin1": "X-Test",
        "value_latin1": "café",
    }


def test_schema_identity_uses_typed_segments_and_serializes_strings() -> None:
    assert DATABASE_SCHEMA.namespace is Namespace.CARL
    assert DATABASE_SCHEMA.domain is Domain.STORAGE
    assert DATABASE_SCHEMA.backend is StorageBackend.SQLITE
    assert DATABASE_SCHEMA.model_dump(mode="json") == {
        "namespace": "carl",
        "domain": "storage",
        "backend": "sqlite",
        "version": 7,
    }
