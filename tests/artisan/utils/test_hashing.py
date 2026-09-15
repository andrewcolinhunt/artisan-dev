"""Tests for hashing utilities."""

from __future__ import annotations

import json
from io import BytesIO
from typing import Any, ClassVar

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.step_executor import instantiate_operation
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.operation_config.environment_spec import DockerEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.hashing import (
    CacheInputIdentity,
    compute_artifact_id,
    compute_content_digest,
    compute_execution_spec_id,
    compute_step_spec_id,
    compute_stream_digest,
    effective_config_payload,
)


class TestComputeArtifactId:
    """Tests for compute_artifact_id."""

    def test_deterministic(self) -> None:
        data = b"same input"
        assert compute_artifact_id("data", data, {}) == compute_artifact_id(
            "data", data, {}
        )

    def test_different_content_different_hash(self) -> None:
        assert compute_artifact_id("data", b"a", {}) != compute_artifact_id(
            "data", b"b", {}
        )

    def test_returns_32_char_hex(self) -> None:
        result = compute_artifact_id("data", b"anything", {})
        assert len(result) == 32
        int(result, 16)  # validates hex

    def test_type_domain_separates_identical_content(self) -> None:
        assert compute_artifact_id("data", b"same", {}) != compute_artifact_id(
            "metric", b"same", {}
        )

    def test_length_framing_separates_component_boundaries(self) -> None:
        assert compute_artifact_id("ab", b"c", {}) != compute_artifact_id(
            "a", b"bc", {}
        )


class TestContentDigests:
    def test_content_and_stream_digest_match(self) -> None:
        content = b"a" * (1024 * 1024 + 17)
        assert compute_stream_digest(BytesIO(content)) == (
            compute_content_digest(content),
            len(content),
        )


def _cache_input(
    role: str,
    artifact_id: str,
    *,
    position: int = 0,
    artifact_type: str = "data",
    group_id: str | None = None,
) -> CacheInputIdentity:
    return CacheInputIdentity(
        role=role,
        group_id=group_id,
        position=position,
        artifact_type=artifact_type,
        artifact_id=artifact_id,
    )


class TestCacheIdentityStructure:
    def test_step_and_execution_domains_are_distinct(self) -> None:
        inputs = {"data": [_cache_input("data", "a" * 32)]}
        assert compute_step_spec_id("op", 0, {}, inputs) != compute_execution_spec_id(
            "op", inputs, {}
        )

    def test_role_mapping_order_is_irrelevant(self) -> None:
        first = {
            "data": [_cache_input("data", "a" * 32)],
            "config": [_cache_input("config", "b" * 32, artifact_type="config")],
        }
        second = {"config": first["config"], "data": first["data"]}
        assert compute_execution_spec_id("op", first) == compute_execution_spec_id(
            "op", second
        )

    def test_item_order_and_multiplicity_affect_identity(self) -> None:
        a = _cache_input("data", "a" * 32)
        b = _cache_input("data", "b" * 32, position=1)
        reversed_items = [
            _cache_input("data", "b" * 32),
            _cache_input("data", "a" * 32, position=1),
        ]
        assert compute_execution_spec_id("op", {"data": [a, b]}) != (
            compute_execution_spec_id("op", {"data": reversed_items})
        )
        assert compute_execution_spec_id("op", {"data": [a]}) != (
            compute_execution_spec_id(
                "op",
                {"data": [a, _cache_input("data", "a" * 32, position=1)]},
            )
        )


# ---------------------------------------------------------------------------
# effective_config_payload — minimal ops isolating one class default each
# ---------------------------------------------------------------------------


class _BareOp(OperationDefinition):
    """No tool, no group_by, default environments/provider/resources."""

    name: ClassVar[str] = "hashing_bare_op"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {}

    def execute_function(self, inputs: Any) -> None:
        return None


class _DockerConfiguredOp(_BareOp):
    """Docker is configured but local remains active by default."""

    name: ClassVar[str] = "hashing_docker_configured_op"
    environments: Environments = Environments(
        docker=DockerEnvironmentSpec(image="lab/img:v1")
    )


class _ImageV1Op(_BareOp):
    """Class-default docker image v1 — no per-step override needed."""

    name: ClassVar[str] = "hashing_image_v1_op"
    environments: Environments = Environments(
        active="docker", docker=DockerEnvironmentSpec(image="lab/img:v1")
    )


class _ImageV2Op(_BareOp):
    """Same as _ImageV1Op but the class-default image is bumped to v2."""

    name: ClassVar[str] = "hashing_image_v2_op"
    environments: Environments = Environments(
        active="docker", docker=DockerEnvironmentSpec(image="lab/img:v2")
    )


class _ToolOp(OperationDefinition):
    """Command op with a class-default tool argv."""

    name: ClassVar[str] = "hashing_tool_op"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {}
    tool: ToolSpec = ToolSpec(executable="mycmd")

    def execute_command(self, inputs: Any) -> list[str]:
        return ["mycmd"]


class _GroupByOp(_BareOp):
    """Class-default group_by, no override."""

    name: ClassVar[str] = "hashing_groupby_op"
    group_by: GroupByStrategy | None = GroupByStrategy.CROSS_PRODUCT


class _V2Op(_BareOp):
    """Identical config to _BareOp but a bumped ``version``."""

    name: ClassVar[str] = "hashing_v2_op"
    version: ClassVar[str] = "2"


class TestEffectiveConfigPayload:
    """effective_config_payload serializes cache-affecting config off the op."""

    def test_bare_op_emits_all_six_keys(self) -> None:
        payload = effective_config_payload(_BareOp())
        assert set(payload) == {
            "version",
            "environments",
            "tool",
            "compute_provider",
            "compute_resources",
            "group_by",
        }
        assert payload["version"] == "1"
        assert payload["tool"] is None
        assert payload["group_by"] is None
        assert isinstance(payload["environments"], dict)
        assert isinstance(payload["compute_provider"], dict)
        assert isinstance(payload["compute_resources"], dict)

    def test_class_default_image_appears_without_override(self) -> None:
        payload = effective_config_payload(_ImageV1Op())
        assert payload["environments"]["active"] == "docker"
        assert payload["environments"]["docker"]["image"] == "lab/img:v1"

    def test_class_default_tool_argv_appears_without_override(self) -> None:
        payload = effective_config_payload(_ToolOp())
        assert payload["tool"]["executable"] == "mycmd"

    def test_class_default_group_by_appears_without_override(self) -> None:
        payload = effective_config_payload(_GroupByOp())
        assert payload["group_by"] == "cross_product"

    def test_per_step_override_changes_payload(self) -> None:
        base = effective_config_payload(_DockerConfiguredOp())
        instance = instantiate_operation(
            _DockerConfiguredOp, StepOverrides.from_user(environment="docker")
        )
        overridden = effective_config_payload(instance)
        assert overridden != base
        assert overridden["environments"]["active"] == "docker"

    def test_version_bump_changes_payload(self) -> None:
        assert effective_config_payload(_BareOp())["version"] == "1"
        assert effective_config_payload(_V2Op())["version"] == "2"
        assert effective_config_payload(_BareOp()) != effective_config_payload(_V2Op())

    def test_payload_is_json_round_trippable(self) -> None:
        payload = effective_config_payload(_ImageV1Op())
        assert json.loads(json.dumps(payload)) == payload

    def test_class_default_image_bump_flips_step_and_execution_spec_ids(self) -> None:
        """The motivating scenario: bumping only a class-default image (no
        per-step override) flips both the step and the execution spec id.

        The two ops differ solely in ``environments.docker.image``; the op
        name passed to the primitives is held fixed, so the image is the only
        variable. Under the old typed-override path both ids were identical.
        """
        v1 = effective_config_payload(_ImageV1Op())
        v2 = effective_config_payload(_ImageV2Op())
        assert v1 != v2

        step_v1 = compute_step_spec_id("mpnn_design", 0, None, {}, config_overrides=v1)
        step_v2 = compute_step_spec_id("mpnn_design", 0, None, {}, config_overrides=v2)
        assert step_v1 != step_v2

        exec_v1 = compute_execution_spec_id(
            "mpnn_design", {}, None, config_overrides=v1
        )
        exec_v2 = compute_execution_spec_id(
            "mpnn_design", {}, None, config_overrides=v2
        )
        assert exec_v1 != exec_v2
