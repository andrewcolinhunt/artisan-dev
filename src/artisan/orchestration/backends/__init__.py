"""Backend namespace and resolution for execution backends.

Usage::

    from artisan.orchestration.backends import Backend

    pipeline.run(MyOp, inputs=..., backend=Backend.SLURM)
    pipeline.run(MyOp, inputs=..., backend="slurm")  # string shorthand
"""

from __future__ import annotations

from artisan.orchestration.backends.base import BackendBase
from artisan.orchestration.backends.local import LocalBackend
from artisan.orchestration.backends.slurm import SlurmBackend


class Backend:
    """Pre-built backend instances for IDE discoverability.

    Usage::

        from artisan.orchestration.backends import Backend

        pipeline.run(MyOp, inputs=..., backend=Backend.SLURM)
    """

    LOCAL = LocalBackend()
    SLURM = SlurmBackend()


_REGISTRY: dict[str, BackendBase] = {b.name: b for b in [Backend.LOCAL, Backend.SLURM]}


def resolve_backend(backend: str | BackendBase) -> BackendBase:
    """Resolve a backend from a string key or pass through an instance.

    Args:
        backend: Backend instance or string name (e.g. "local", "slurm").

    Returns:
        Resolved BackendBase instance.

    Raises:
        ValueError: If string key is not in the registry.
    """
    if isinstance(backend, BackendBase):
        return backend
    if backend not in _REGISTRY:
        msg = f"Unknown backend: {backend!r}. Available: {sorted(_REGISTRY)}"
        raise ValueError(msg)
    return _REGISTRY[backend]


__all__ = [
    "Backend",
    "BackendBase",
    "resolve_backend",
]
