from functools import partial

import pytest

from carl.core.components import Component, ComponentId, Registry


def test_registry_rejects_duplicate_structured_identifiers() -> None:
    identifier = ComponentId(("carl", "test", "producer"))
    component = partial(Component, identifier, 1, lambda: None)

    with pytest.raises(ValueError, match="Duplicate"):
        Registry((component(), component()))


def test_identifier_parts_do_not_depend_on_string_delimiters() -> None:
    identifiers = Registry(
        (
            Component(ComponentId(("a.b", "c")), 1, lambda: None),
            Component(ComponentId(("a", "b.c")), 1, lambda: None),
        )
    )

    assert len(identifiers.components) == 2
