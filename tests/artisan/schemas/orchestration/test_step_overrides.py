"""Tests for the ``StepOverrides`` per-step override carrier.

Three layers:

- **Classification completeness** — every dataclass field is classified
  into exactly one of ``_CACHE_FIELDS`` / ``_RUNTIME_FIELDS``, so a new
  override cannot silently escape the cache key.
- **from_user coercion** — each typed-or-dict knob lands in canonical
  dict-or-str form; ``step_runner`` and scalars pass through untouched.
- **cache_payload golden values** — the config-overrides payload matches
  hardcoded golden values (byte-equivalent to the legacy merge the refactor
  replaced, so existing caches stay valid).
"""

from __future__ import annotations

from dataclasses import fields

import pytest

from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.environment_spec import DockerEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.orchestration.step_overrides import StepOverrides

# ---------------------------------------------------------------------------
# Classification completeness
# ---------------------------------------------------------------------------


def test_every_field_classified_exactly_once() -> None:
    """Each dataclass field is in exactly one of _CACHE_FIELDS/_RUNTIME_FIELDS.

    This is the escape-hatch closer: adding a field to StepOverrides without
    classifying it (or double-classifying it) fails here, forcing the
    author to decide whether it enters the cache key.
    """
    field_names = {f.name for f in fields(StepOverrides)}
    cache = set(StepOverrides._CACHE_FIELDS)
    runtime = set(StepOverrides._RUNTIME_FIELDS)

    assert cache & runtime == set(), "a field is in both cache and runtime sets"
    assert cache | runtime == field_names, (
        "unclassified field(s): "
        f"{field_names - (cache | runtime)}; "
        "stale classification(s): "
        f"{(cache | runtime) - field_names}"
    )


# ---------------------------------------------------------------------------
# from_user coercion
# ---------------------------------------------------------------------------


class TestFromUserCoercion:
    """from_user normalizes typed-or-dict knobs to canonical dict-or-str."""

    def test_typed_models_dump_exclude_defaults(self) -> None:
        ov = StepOverrides.from_user(
            runner_resources=RunnerResources(cpus=8),
            batch_strategy=BatchStrategy(artifacts_per_unit=4),
            environment=Environments(
                active="docker", docker=DockerEnvironmentSpec(image="img:v2")
            ),
            tool=ToolSpec(executable="bash"),
            compute_provider=ComputeProvider(
                active="modal", modal=ModalComputeConfig()
            ),
            compute_resources=ComputeResources(gpu="A100", memory_gb=16),
        )
        assert ov.runner_resources == {"cpus": 8}
        assert ov.batch_strategy == {"artifacts_per_unit": 4}
        assert ov.environment == {"active": "docker", "docker": {"image": "img:v2"}}
        assert ov.tool == {"executable": "bash"}
        assert ov.compute_provider == {"active": "modal", "modal": {}}
        assert ov.compute_resources == {"gpu": "A100", "memory_gb": 16}

    def test_dicts_pass_through_unchanged(self) -> None:
        ov = StepOverrides.from_user(
            runner_resources={"cpus": 2},
            tool={"executable": "python"},
            compute_resources={"gpu": "H100"},
        )
        assert ov.runner_resources == {"cpus": 2}
        assert ov.tool == {"executable": "python"}
        assert ov.compute_resources == {"gpu": "H100"}

    def test_string_selectors_pass_through(self) -> None:
        ov = StepOverrides.from_user(environment="docker", compute_provider="modal")
        assert ov.environment == "docker"
        assert ov.compute_provider == "modal"

    def test_none_stays_none(self) -> None:
        ov = StepOverrides.from_user()
        assert ov.environment is None
        assert ov.tool is None
        assert ov.compute_provider is None
        assert ov.compute_resources is None
        assert ov.runner_resources is None
        assert ov.batch_strategy is None

    def test_step_runner_and_scalars_uncoerced(self) -> None:
        ov = StepOverrides.from_user(
            step_runner="slurm",
            group_by=GroupByStrategy.ZIP,
            compact=False,
            skip_cache=True,
            name="custom",
        )
        assert ov.step_runner == "slurm"
        assert ov.group_by is GroupByStrategy.ZIP
        assert ov.compact is False
        assert ov.skip_cache is True
        assert ov.name == "custom"

    def test_frozen(self) -> None:
        ov = StepOverrides.from_user(environment="docker")
        with pytest.raises(AttributeError):
            ov.environment = "local"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# cache_payload golden values
# ---------------------------------------------------------------------------
#
# A transient characterization test pinned cache_payload against the legacy
# config-overrides merge (byte-for-byte, for every coerced input form) until
# that function was deleted in the same PR. The golden values below plus the
# recorded step-spec hashes in
# tests/artisan/utils/test_hash_stability_recorded.py are the permanent guard
# that cache keys stay byte-identical.


def test_cache_payload_golden_values() -> None:
    """Hardcoded payload values — the permanent guard on cache-key stability."""
    assert StepOverrides.from_user().cache_payload() is None
    assert StepOverrides.from_user(environment="docker").cache_payload() == {
        "environment": "docker"
    }
    # Falsy tool (empty dict) is omitted; a real dict is kept.
    assert StepOverrides.from_user(tool={}).cache_payload() is None
    assert StepOverrides.from_user(tool={"executable": "bash"}).cache_payload() == {
        "tool": {"executable": "bash"}
    }
    assert StepOverrides.from_user(compute_provider="modal").cache_payload() == {
        "compute_provider": "modal"
    }
    assert StepOverrides.from_user(
        compute_resources={"gpu": "A100"}
    ).cache_payload() == {"compute_resources": {"gpu": "A100"}}
    # group_by serialized via .value.
    assert StepOverrides.from_user(
        group_by=GroupByStrategy.CROSS_PRODUCT
    ).cache_payload() == {"group_by": "cross_product"}


def test_cache_payload_ignores_runtime_fields() -> None:
    """Runtime-only fields never enter the config_overrides payload."""
    ov = StepOverrides.from_user(
        params={"a": 1},
        step_runner="slurm",
        runner_resources={"cpus": 8},
        batch_strategy={"artifacts_per_unit": 4},
        compact=False,
        skip_cache=True,
        name="custom",
    )
    assert ov.cache_payload() is None
