"""Immutable, composition-scoped references used to declare scientific wiring."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CompositeRef:
    """A builder-local reference, never a future or an artifact."""

    owner: str
    call: str | None
    role: str


@dataclass(frozen=True)
class CompositeCall:
    """Expose declared outputs while constructing a composition."""

    owner: str
    name: str
    outputs: tuple[str, ...]

    def output(self, role: str) -> CompositeRef:
        """Reference one declared output stream without reading its values."""
        if role not in self.outputs:
            msg = f"Unknown output role {role!r}; available: {self.outputs}"
            raise ValueError(msg)
        return CompositeRef(self.owner, self.name, role)
