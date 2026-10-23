"""Normalize per-step overrides for validation, preparation, and dispatch.

Cache fields affect the instantiated operation's ``effective_config_payload``.
Runtime fields do not enter that config hash. Although output-affecting,
``params`` is classified as runtime here because ``_prepare_step_spec`` hashes
the fully merged parameters separately.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, overload

from pydantic import BaseModel

from artisan.schemas.enums import CachePolicy, FailurePolicy, GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.compute import ComputeProvider
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec

if TYPE_CHECKING:
    # Keep the runner import type-only to avoid a schemas/orchestration cycle.
    from artisan.orchestration.runners.base import RunnerBase


@overload
def _normalize_patch(
    value: dict[str, Any] | BaseModel | None,
    model_cls: type[BaseModel],
    *,
    allow_str: Literal[False] = False,
) -> dict[str, Any] | None: ...


@overload
def _normalize_patch(
    value: str | dict[str, Any] | BaseModel | None,
    model_cls: type[BaseModel],
    *,
    allow_str: Literal[True],
) -> str | dict[str, Any] | None: ...


def _normalize_patch(
    value: str | dict[str, Any] | BaseModel | None,
    model_cls: type[BaseModel],
    *,
    allow_str: bool = False,
) -> str | dict[str, Any] | None:
    """Normalize a typed model or mapping to a detached patch.

    Typed models contribute only fields the caller explicitly supplied.
    Mapping inputs retain every present key. Both forms are deep-copied so
    later caller mutation cannot change preparation or persisted metadata.

    Args:
        value: A ``model_cls`` instance, a dict, ``None``, or (when
            ``allow_str``) a bare string active-provider selector.
        model_cls: The Pydantic model whose instances are normalized.
        allow_str: Whether a bare string passes through unchanged (the
            active-provider selector form used by environment /
            compute_provider).

    Returns:
        ``None``, the passthrough string, or a dict.
    """
    if value is None:
        return None
    if allow_str and isinstance(value, str):
        return value
    if isinstance(value, model_cls):
        return deepcopy(value.model_dump(mode="python", exclude_unset=True))
    if isinstance(value, dict):
        return deepcopy(value)
    msg = f"Expected {model_cls.__name__} or dict, got {type(value).__name__}"
    raise TypeError(msg)


@dataclass(frozen=True)
class StepOverrides:
    """Per-step user overrides normalized to detached patch dictionaries.

    Frozen so fields cannot be reassigned; nested patch data is framework-owned
    because ``from_user`` detaches it from caller containers.

    Attributes:
        params: User-provided parameter overrides.
        step_runner: Step-runner selector (string name or instance). Not
            coerced.
        runner_resources: Runner-resource overrides (cpus, memory_gb, ...).
        batch_strategy: Batching/scheduling overrides.
        environment: Environment override (active-selector string or dict).
        tool: Tool overrides.
        compute_provider: Compute-provider override (string or dict).
        compute_resources: Hardware-resource overrides.
        failure_policy: Per-step failure policy override.
        cache_policy: Whole-step cache policy, or None to inherit the default.
        group_by: Per-step multi-input pairing-strategy override.
        compact: Whether to run Delta Lake compaction after commit.
        skip_cache: Bypass cache lookups for this step.
        name: Custom step name.
    """

    params: dict[str, Any] | None = None
    step_runner: str | RunnerBase | None = None
    runner_resources: dict[str, Any] | None = None
    batch_strategy: dict[str, Any] | None = None
    environment: str | dict[str, Any] | None = None
    tool: dict[str, Any] | None = None
    compute_provider: str | dict[str, Any] | None = None
    compute_resources: dict[str, Any] | None = None
    failure_policy: FailurePolicy | None = None
    cache_policy: CachePolicy | None = None
    group_by: GroupByStrategy | None = None
    compact: bool = True
    skip_cache: bool = False
    name: str | None = None

    _CACHE_FIELDS: ClassVar[tuple[str, ...]] = (
        "environment",
        "tool",
        "compute_provider",
        "compute_resources",
        "group_by",
    )
    _RUNTIME_FIELDS: ClassVar[tuple[str, ...]] = (
        "params",
        "step_runner",
        "runner_resources",
        "batch_strategy",
        "failure_policy",
        "cache_policy",
        "compact",
        "skip_cache",
        "name",
    )

    @classmethod
    def from_user(
        cls,
        *,
        params: dict[str, Any] | None = None,
        step_runner: str | RunnerBase | None = None,
        runner_resources: dict[str, Any] | RunnerResources | None = None,
        batch_strategy: dict[str, Any] | BatchStrategy | None = None,
        environment: str | dict[str, Any] | Environments | None = None,
        tool: dict[str, Any] | ToolSpec | None = None,
        compute_provider: str | dict[str, Any] | ComputeProvider | None = None,
        compute_resources: dict[str, Any] | ComputeResources | None = None,
        failure_policy: FailurePolicy | None = None,
        cache_policy: CachePolicy | None = None,
        group_by: GroupByStrategy | None = None,
        compact: bool = True,
        skip_cache: bool = False,
        name: str | None = None,
    ) -> StepOverrides:
        """Build a ``StepOverrides`` from raw public-API keyword arguments.

        Normalizes the six typed-or-dict knobs to detached patches. Typed
        models use field presence, not comparisons with schema defaults, so
        explicit default-valued fields and explicit ``None`` values survive.
        ``step_runner`` is left untouched, and scalar knobs pass through.

        Returns:
            A frozen ``StepOverrides`` with all overrides coerced.

        Raises:
            TypeError: If cache_policy is not a CachePolicy member or None.
        """
        policy_value: object = cache_policy
        if policy_value is not None and not isinstance(policy_value, CachePolicy):
            msg = (
                "cache_policy must be CachePolicy.ALL_SUCCEEDED, "
                "CachePolicy.STEP_COMPLETED, or None"
            )
            raise TypeError(msg)
        return cls(
            params=deepcopy(params),
            step_runner=step_runner,
            runner_resources=_normalize_patch(runner_resources, RunnerResources),
            batch_strategy=_normalize_patch(batch_strategy, BatchStrategy),
            environment=_normalize_patch(environment, Environments, allow_str=True),
            tool=_normalize_patch(tool, ToolSpec),
            compute_provider=_normalize_patch(
                compute_provider, ComputeProvider, allow_str=True
            ),
            compute_resources=_normalize_patch(compute_resources, ComputeResources),
            failure_policy=failure_policy,
            cache_policy=cache_policy,
            group_by=group_by,
            compact=compact,
            skip_cache=skip_cache,
            name=name,
        )
