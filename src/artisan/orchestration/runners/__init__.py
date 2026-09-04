"""Built-in runner namespace and resolution for step execution.

Usage::

    from artisan.orchestration.runners import Runner

    pipeline.run(MyOp, inputs=..., step_runner=Runner.LOCAL)

External providers are passed as runner instances rather than registered by
name in Artisan core.
"""

from __future__ import annotations

from artisan.orchestration.runners.base import RunnerBase
from artisan.orchestration.runners.local import LocalRunner


class Runner:
    """Built-in runner instances for IDE discoverability.

    Usage::

        from artisan.orchestration.runners import Runner

        pipeline.run(MyOp, inputs=..., step_runner=Runner.LOCAL)
    """

    LOCAL = LocalRunner()


_REGISTRY: dict[str, RunnerBase] = {Runner.LOCAL.name: Runner.LOCAL}


def resolve_runner(step_runner: str | RunnerBase) -> RunnerBase:
    """Resolve a runner from a string key or pass through an instance.

    Args:
        step_runner: Runner instance or built-in string name (currently
            ``"local"``).

    Returns:
        Resolved RunnerBase instance.

    Raises:
        ValueError: If string key is not in the registry.
    """
    if isinstance(step_runner, RunnerBase):
        return step_runner
    if step_runner not in _REGISTRY:
        msg = f"Unknown step_runner: {step_runner!r}. Available: {sorted(_REGISTRY)}"
        raise ValueError(msg)
    return _REGISTRY[step_runner]


__all__ = [
    "Runner",
    "RunnerBase",
    "resolve_runner",
]
