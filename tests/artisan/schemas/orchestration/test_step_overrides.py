"""Tests for the ``StepOverrides`` per-step override carrier.

Three layers:

- **Classification completeness** — every dataclass field is classified
  into exactly one of ``_CACHE_FIELDS`` / ``_RUNTIME_FIELDS``, so a new
  override cannot silently escape the cache decision.
- **from_user coercion** — each typed-or-dict knob lands in canonical
  dict-or-str form; ``step_runner`` and scalars pass through untouched.
- **Cache-field / payload correspondence** — every cache-classified knob
  maps to an instance field that ``effective_config_payload`` actually
  reads, so a knob cannot be wired into ``instantiate_operation`` yet
  silently skipped by the hash.
"""

from __future__ import annotations

from dataclasses import fields

import pytest

from artisan.operations.examples.data_transformer import DataTransformer
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
from artisan.utils.hashing import effective_config_payload

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

    def test_typed_models_dump_explicit_fields(self) -> None:
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

    def test_omitted_typed_fields_do_not_enter_patch(self) -> None:
        ov = StepOverrides.from_user(
            runner_resources=RunnerResources(),
            batch_strategy=BatchStrategy(),
            compute_resources=ComputeResources(),
        )

        assert ov.runner_resources == {}
        assert ov.batch_strategy == {}
        assert ov.compute_resources == {}

    def test_explicit_schema_defaults_and_none_survive(self) -> None:
        ov = StepOverrides.from_user(
            runner_resources=RunnerResources(cpus=1),
            batch_strategy=BatchStrategy(artifacts_per_unit=1),
            compute_resources=ComputeResources(gpu=None),
        )

        assert ov.runner_resources == {"cpus": 1}
        assert ov.batch_strategy == {"artifacts_per_unit": 1}
        assert ov.compute_resources == {"gpu": None}

    def test_explicit_empty_containers_survive(self) -> None:
        ov = StepOverrides.from_user(
            runner_resources=RunnerResources(extra={}),
            compute_provider=ComputeProvider(
                modal=ModalComputeConfig(secrets=[], volumes={}, env={})
            ),
        )

        assert ov.runner_resources == {"extra": {}}
        assert ov.compute_provider == {
            "modal": {"secrets": [], "volumes": {}, "env": {}}
        }

    def test_nested_typed_fields_follow_nested_presence(self) -> None:
        ov = StepOverrides.from_user(
            environment=Environments(
                docker=DockerEnvironmentSpec(image="img:v2", gpu=False)
            )
        )

        assert ov.environment == {"docker": {"image": "img:v2", "gpu": False}}

    def test_dict_values_are_preserved(self) -> None:
        ov = StepOverrides.from_user(
            runner_resources={"cpus": 2},
            tool={"executable": "python"},
            compute_resources={"gpu": "H100"},
        )
        assert ov.runner_resources == {"cpus": 2}
        assert ov.tool == {"executable": "python"}
        assert ov.compute_resources == {"gpu": "H100"}

    def test_caller_mappings_are_deep_copied(self) -> None:
        params = {"config": {"values": [1]}}
        runner_resources = {"extra": {"queue": "cpu"}}
        batch_strategy = {"artifacts_per_unit": 2}
        environment = {"docker": {"env": {"MODE": "fast"}}}
        tool = {"subcommand": "run"}
        compute_provider = {"modal": {"env": {"MODE": "fast"}}}
        compute_resources = {"memory_gb": 8}

        ov = StepOverrides.from_user(
            params=params,
            runner_resources=runner_resources,
            batch_strategy=batch_strategy,
            environment=environment,
            tool=tool,
            compute_provider=compute_provider,
            compute_resources=compute_resources,
        )
        params["config"]["values"].append(2)
        runner_resources["extra"]["queue"] = "gpu"
        batch_strategy["artifacts_per_unit"] = 99
        environment["docker"]["env"]["MODE"] = "slow"
        tool["subcommand"] = "other"
        compute_provider["modal"]["env"]["MODE"] = "slow"
        compute_resources["memory_gb"] = 99

        assert ov.params == {"config": {"values": [1]}}
        assert ov.runner_resources == {"extra": {"queue": "cpu"}}
        assert ov.batch_strategy == {"artifacts_per_unit": 2}
        assert ov.environment == {"docker": {"env": {"MODE": "fast"}}}
        assert ov.tool == {"subcommand": "run"}
        assert ov.compute_provider == {"modal": {"env": {"MODE": "fast"}}}
        assert ov.compute_resources == {"memory_gb": 8}

    def test_typed_model_patches_are_deep_copied(self) -> None:
        modal = ModalComputeConfig(
            secrets=["auth"],
            env={"MODE": "fast"},
        )
        provider = ComputeProvider(modal=modal)

        ov = StepOverrides.from_user(compute_provider=provider)
        modal.secrets.append("later")
        modal.env["MODE"] = "slow"

        assert ov.compute_provider == {
            "modal": {"secrets": ["auth"], "env": {"MODE": "fast"}}
        }

    def test_root_empty_mapping_remains_empty_patch(self) -> None:
        ov = StepOverrides.from_user(
            runner_resources={},
            environment={},
            tool={},
            compute_provider={},
        )

        assert ov.runner_resources == {}
        assert ov.environment == {}
        assert ov.tool == {}
        assert ov.compute_provider == {}

    def test_wrong_typed_model_is_rejected(self) -> None:
        with pytest.raises(TypeError, match="Expected RunnerResources or dict"):
            StepOverrides.from_user(
                runner_resources=BatchStrategy()  # type: ignore[arg-type]
            )

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
            step_runner="external_test",
            group_by=GroupByStrategy.ZIP,
            compact=False,
            skip_cache=True,
            name="custom",
        )
        assert ov.step_runner == "external_test"
        assert ov.group_by is GroupByStrategy.ZIP
        assert ov.compact is False
        assert ov.skip_cache is True
        assert ov.name == "custom"

    def test_frozen(self) -> None:
        ov = StepOverrides.from_user(environment="docker")
        with pytest.raises(AttributeError):
            ov.environment = "local"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Cache-field / payload correspondence
# ---------------------------------------------------------------------------
#
# The classification guard above proves every override *knob* is classified;
# this guard proves every cache-classified knob reaches the hash through a
# concrete instance field that ``effective_config_payload`` reads. Without
# it, someone could add a cache-affecting knob, wire it into
# ``instantiate_operation``, but forget to read the resulting instance field
# in the payload — and the completeness test would still pass.

# Maps each cache knob (``StepOverrides`` field name) to the instance field
# ``effective_config_payload`` reads for it. ``environment`` mutates the
# ``environments`` instance field; the rest share their name.
_CACHE_KNOB_TO_INSTANCE_FIELD = {
    "environment": "environments",
    "tool": "tool",
    "compute_provider": "compute_provider",
    "compute_resources": "compute_resources",
    "group_by": "group_by",
}


def test_cache_fields_correspond_to_hashed_instance_fields() -> None:
    """Every cache knob maps to an instance field the payload actually reads.

    ``version`` is the one payload key with no ``StepOverrides`` counterpart
    (it is op-code identity, not a user override) and is asserted separately.
    """
    expected = {_CACHE_KNOB_TO_INSTANCE_FIELD[f] for f in StepOverrides._CACHE_FIELDS}
    payload = effective_config_payload(DataTransformer())
    hashed = set(payload.keys()) - {"version"}
    assert hashed == expected
    assert "version" in payload
