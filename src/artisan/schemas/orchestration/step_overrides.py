"""Per-step user-override carrier for the pipeline dispatch path.

``StepOverrides`` bundles the thirteen per-step override knobs that
``PipelineManager.run``/``submit`` accept into one frozen record. It is
constructed once at the public API boundary via ``from_user`` (which
coerces typed-or-dict inputs to canonical dict-or-str form) and threaded
by reference through validation, spec-id hashing, and dispatch.

Two ``ClassVar`` tuples classify every field as either a *cache field*
(folded into the ``config_overrides`` hash channel via ``cache_payload``)
or a *runtime field*. A unit test asserts the classification is total, so
a newly added override cannot silently escape the cache key. ``params`` is
output-affecting but keeps its own full-merged-params hash channel in
``_prepare_step_spec``; it is therefore a runtime field here, not a
``config_overrides`` field.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel

from artisan.schemas.enums import FailurePolicy, GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.compute import ComputeProvider
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec

if TYPE_CHECKING:
    # RunnerBase lives in orchestration/runners/; importing it at runtime
    # would create the first schemas->orchestration cycle. It is used as a
    # string annotation only (the ExecutionContext pattern), and from_user
    # does not coerce step_runner, so no runtime import is needed.
    from artisan.orchestration.runners.base import RunnerBase


def _coerce(
    value: str | dict[str, Any] | BaseModel | None,
    model_cls: type[BaseModel],
    *,
    allow_str: bool = False,
) -> str | dict[str, Any] | None:
    """Normalize a typed-or-dict override to canonical dict-or-str form.

    A typed Pydantic model is dumped to a dict via
    ``model_dump(exclude_defaults=True)`` so downstream hashing and
    override application see a single shape; a raw dict passes through
    unchanged.

    Args:
        value: A ``model_cls`` instance, a dict, ``None``, or (when
            ``allow_str``) a bare string active-provider selector.
        model_cls: The Pydantic model whose instances are dumped to dicts.
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
        return value.model_dump(exclude_defaults=True)
    return value


def _to_json(value: Any) -> Any:
    """Dump a Pydantic model to a JSON-safe dict; pass other values through.

    Mirrors the legacy ``_merge_config_overrides._to_dict`` so the cache
    payload stays JSON-serializable and dict-vs-model forms hash alike.
    ``from_user`` coerces models to dicts up front, so in practice this is
    a passthrough — it exists to reproduce the legacy behavior exactly.
    """
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    return value


@dataclass(frozen=True)
class StepOverrides:
    """Per-step user overrides, coerced to canonical dict-or-str form.

    Constructed once at the public API boundary via ``from_user``; threaded
    by reference through validation, hashing, and dispatch. Frozen so an
    override set cannot mutate mid-flight.

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
    group_by: GroupByStrategy | None = None
    compact: bool = True
    skip_cache: bool = False
    name: str | None = None

    # Fields folded into the config_overrides hash channel (cache_payload).
    _CACHE_FIELDS: ClassVar[tuple[str, ...]] = (
        "environment",
        "tool",
        "compute_provider",
        "compute_resources",
        "group_by",
    )
    # Fields that do not enter the config_overrides hash channel. ``params``
    # is hashed via the separate merged-params channel; the rest are pure
    # runtime/dispatch knobs.
    _RUNTIME_FIELDS: ClassVar[tuple[str, ...]] = (
        "params",
        "step_runner",
        "runner_resources",
        "batch_strategy",
        "failure_policy",
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
        group_by: GroupByStrategy | None = None,
        compact: bool = True,
        skip_cache: bool = False,
        name: str | None = None,
    ) -> StepOverrides:
        """Build a ``StepOverrides`` from raw public-API keyword arguments.

        Coerces the six typed-or-dict knobs to canonical dict-or-str form so
        every downstream consumer sees a single shape. ``step_runner`` is
        left untouched (it is resolved to a runner instance later), and the
        scalar knobs pass through unchanged.

        Returns:
            A frozen ``StepOverrides`` with all overrides coerced.
        """
        return cls(
            params=params,
            step_runner=step_runner,
            runner_resources=_coerce(runner_resources, RunnerResources),
            batch_strategy=_coerce(batch_strategy, BatchStrategy),
            environment=_coerce(environment, Environments, allow_str=True),
            tool=_coerce(tool, ToolSpec),
            compute_provider=_coerce(compute_provider, ComputeProvider, allow_str=True),
            compute_resources=_coerce(compute_resources, ComputeResources),
            failure_policy=failure_policy,
            group_by=group_by,
            compact=compact,
            skip_cache=skip_cache,
            name=name,
        )

    def cache_payload(self) -> dict[str, Any] | None:
        """Build the ``config_overrides`` hash payload from the cache fields.

        Reproduces the legacy ``_merge_config_overrides`` byte-for-byte, so
        existing caches stay valid. The omit rules are deliberately
        non-uniform: ``tool`` is omitted when falsy (an empty dict), the
        other three dict/str fields when ``None``; ``group_by`` is
        serialized via its ``.value``. Any Pydantic model surviving on a
        field is dumped via ``model_dump(mode="json")`` (``from_user`` has
        already coerced models to dicts, so this is a passthrough in
        practice).

        Returns:
            The merged override dict, or ``None`` when no cache field is set.
        """
        merged: dict[str, Any] = {}
        if self.environment is not None:
            merged["environment"] = _to_json(self.environment)
        if self.tool:
            merged["tool"] = _to_json(self.tool)
        if self.compute_provider is not None:
            merged["compute_provider"] = _to_json(self.compute_provider)
        if self.compute_resources is not None:
            merged["compute_resources"] = _to_json(self.compute_resources)
        if self.group_by is not None:
            merged["group_by"] = self.group_by.value
        return merged or None
