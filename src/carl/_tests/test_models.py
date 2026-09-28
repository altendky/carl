"""Shared serialization-boundary model behavior."""

import pytest
from pydantic import ValidationError

from carl.core.models import JsonStringEnumeration, StrictModel


class _ExampleEnumeration(JsonStringEnumeration):
    VALUE = "value"


class _ExampleModel(StrictModel):
    enumeration: _ExampleEnumeration


def test_json_string_enumeration_accepts_its_serialized_value_in_python_mode() -> None:
    value = _ExampleModel.model_validate({"enumeration": "value"})

    assert value.enumeration is _ExampleEnumeration.VALUE
    assert value.model_dump(mode="json") == {"enumeration": "value"}


@pytest.mark.parametrize("value", ["unknown", 1, True, None])
def test_json_string_enumeration_retains_strict_rejection(value: object) -> None:
    with pytest.raises(ValidationError):
        _ExampleModel.model_validate({"enumeration": value})
