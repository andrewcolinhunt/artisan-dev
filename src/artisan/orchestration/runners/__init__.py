"""Runner namespace and resolution for step execution.

Usage::

    from artisan.orchestration.runners import Runner

    pipeline.run(MyOp, inputs=..., step_runner=Runner.SLURM)
    pipeline.run(MyOp, inputs=..., step_runner="slurm")  # string shorthand
"""

from __future__ import annotations

from artisan.orchestration.runners.base import RunnerBase
from artisan.orchestration.runners.local import LocalRunner
from artisan.orchestration.runners.slurm import SlurmRunner
from artisan.orchestration.runners.slurm_intra import SlurmIntraRunner


class Runner:
    """Pre-built runner instances for IDE discoverability.

    Usage::

        from artisan.orchestration.runners import Runner

        pipeline.run(MyOp, inputs=..., step_runner=Runner.SLURM)
    """

    LOCAL = LocalRunner()
    SLURM = SlurmRunner()
    SLURM_INTRA = SlurmIntraRunner()


_REGISTRY: dict[str, RunnerBase] = {
    b.name: b for b in [Runner.LOCAL, Runner.SLURM, Runner.SLURM_INTRA]
}


def resolve_runner(step_runner: str | RunnerBase) -> RunnerBase:
    """Resolve a runner from a string key or pass through an instance.

    Args:
        step_runner: Runner instance or string name (e.g. "local", "slurm").

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
