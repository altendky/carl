"""Explicit registration for code that produces retained outputs."""

from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ComponentId:
    """A structured identifier whose parts never require delimiter parsing."""

    parts: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.parts or any(not part for part in self.parts):
            raise ValueError("Component identifiers require nonempty parts")


@dataclass(frozen=True, slots=True)
class Component:
    identifier: ComponentId
    output_schema_version: int
    implementation: Callable[..., object] = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.output_schema_version < 1:
            raise ValueError("Output schema versions start at one")


@dataclass(frozen=True, slots=True)
class Registry:
    components: tuple[Component, ...]

    def __post_init__(self) -> None:
        identifiers = tuple(component.identifier for component in self.components)
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("Duplicate component identifier")

    def require(self, identifier: ComponentId) -> Component:
        for component in self.components:
            if component.identifier == identifier:
                return component
        raise KeyError(identifier.parts)
