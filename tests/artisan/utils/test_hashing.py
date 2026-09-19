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

    def test_content_digest_returns_32_char_hex(self):
        """Content digests use 32 lowercase hex characters."""
        content = b"test content"
        artifact_id = compute_content_digest(content)

        assert len(artifact_id) == 32
        assert all(c in "0123456789abcdef" for c in artifact_id)

    def test_content_digest_deterministic(self):
        """Same content should always produce same ID."""
        content = b"deterministic test"
        id1 = compute_content_digest(content)
        id2 = compute_content_digest(content)

        assert id1 == id2

    def test_content_digest_different_content_different_id(self):
        """Different content should produce different IDs."""
        id1 = compute_content_digest(b"content A")
        id2 = compute_content_digest(b"content B")

        assert id1 != id2


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


def _typed_inputs(inputs: dict[str, list[str]]) -> dict[str, list[CacheInputIdentity]]:
    """Add the concrete type, role, group, and position cache dimensions."""
    return {
        role: [
            CacheInputIdentity(role, None, position, "metric", artifact_id)
            for position, artifact_id in enumerate(artifact_ids)
        ]
        for role, artifact_ids in inputs.items()
    }


def _execution_spec_from_ids(
    *,
    operation_name: str,
    inputs: dict[str, list[str]],
    params=None,
    config_overrides=None,
) -> str:
    """Call the production cache hash with typed test identities."""
    return compute_execution_spec_id(
        operation_name,
        _typed_inputs(inputs),
        params,
        config_overrides,
    )


class TestComputeExecutionSpecId:
    """Tests for _execution_spec_from_ids()."""

    def test_deterministic_output(self):
        """Same inputs produce same output."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["abc123" + "0" * 26, "def456" + "0" * 26]},
            params={"tolerance": 0.1},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["abc123" + "0" * 26, "def456" + "0" * 26]},
            params={"tolerance": 0.1},
        )
        assert spec1 == spec2
        assert len(spec1) == 32  # xxh3_128 hex length

    def test_artifact_order_is_preserved(self):
        """Occurrence order within a role remains part of the cache identity."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["bbb" + "0" * 29, "aaa" + "0" * 29]},  # Unsorted
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["aaa" + "0" * 29, "bbb" + "0" * 29]},  # Sorted
        )
        assert spec1 != spec2

    def test_multi_role_inputs_role_order_irrelevant(self):
        """Role-key ordering of the inputs dict doesn't affect the hash.

        Roles are sorted before hashing, so the dict-construction order
        is irrelevant. Role *assignment* of each artifact_id still matters
        — see ``test_execution_spec_id_differs_per_role_assignment``.
        """
        spec1 = _execution_spec_from_ids(
            operation_name="transform",
            inputs={
                "primary": ["aaa" + "0" * 29],
                "reference": ["bbb" + "0" * 29],
            },
        )
        spec2 = _execution_spec_from_ids(
            operation_name="transform",
            inputs={
                "reference": ["bbb" + "0" * 29],
                "primary": ["aaa" + "0" * 29],
            },
        )
        assert spec1 == spec2

    def test_execution_spec_id_differs_per_role_assignment(self):
        """Swapping which role an artifact_id belongs to changes the spec_id.

        Equal ID multisets with different role assignments represent
        different execution units.
        """
        a, b = "a" * 32, "b" * 32
        spec_a_primary = _execution_spec_from_ids(
            operation_name="transform",
            inputs={"primary": [a], "reference": [b]},
        )
        spec_b_primary = _execution_spec_from_ids(
            operation_name="transform",
            inputs={"primary": [b], "reference": [a]},
        )
        assert spec_a_primary != spec_b_primary

    def test_params_key_order_irrelevant(self):
        """Params dict key order doesn't affect hash."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            params={"a": 1, "b": 2},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            params={"b": 2, "a": 1},  # Different order
        )
        assert spec1 == spec2

    def test_different_operation_different_hash(self):
        """Different operation names produce different hashes."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="minimize",
            inputs={"data": ["a" * 32]},
        )
        assert spec1 != spec2

    def test_different_artifacts_different_hash(self):
        """Different artifact IDs produce different hashes."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["b" * 32]},
        )
        assert spec1 != spec2

    def test_empty_inputs(self):
        """Handles empty inputs gracefully (generative ops)."""
        spec = _execution_spec_from_ids(
            operation_name="generate",
            inputs={},
            params=None,
        )
        assert len(spec) == 32

    def test_different_params_different_hash(self):
        """Different merged params produce different spec_id."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            params={"tolerance": 0.1},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            params={"tolerance": 0.2},
        )
        assert spec1 != spec2

    def test_duplicate_artifact_ids_preserve_multiplicity(self):
        """Multiplicity within a role is preserved (not deduped) in the hash.

        ``[A, A]`` and ``[A]`` produce different spec_ids — repeated
        artifacts reflect the actual unit shape (e.g. aligned CROSS_PRODUCT
        primaries) rather than collapsing into a content-only fingerprint.
        """
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["a" * 32, "a" * 32]},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
        )
        assert spec1 != spec2

    def test_config_overrides_none_same_as_empty(self):
        """config_overrides=None produces same spec_id as empty dict."""
        spec_none = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            config_overrides=None,
        )
        spec_empty = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            config_overrides={},
        )
        assert spec_none == spec_empty

    def test_config_overrides_changes_hash(self):
        """Different config_overrides produce different spec_id."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
            config_overrides=None,
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
            config_overrides={"image": "/path/to/image.sif"},
        )
        assert spec1 != spec2

    def test_config_overrides_deterministic(self):
        """Same config_overrides produce same spec_id."""
        kwargs = {
            "operation_name": "relax",
            "inputs": {"data": ["a" * 32]},
            "config_overrides": {"image": "/opt/image.sif", "gpu": True},
        }
        assert _execution_spec_from_ids(**kwargs) == _execution_spec_from_ids(**kwargs)

    def test_config_overrides_with_path_objects(self):
        """Path objects in config_overrides serialize correctly."""
        from pathlib import Path

        spec = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            config_overrides={"image": Path("/opt/containers/relax.sif")},
        )
        assert len(spec) == 32


def _step_spec_from_ids(
    *,
    operation_name: str,
    step_number: int,
    params: dict[str, Any] | None,
    inputs: dict[str, tuple[str, str]],
    config_overrides: dict[str, Any] | None = None,
) -> str:
    """Call the production step hash with concrete typed test identities."""
    typed = {
        role: [CacheInputIdentity(role, None, 0, artifact_type, artifact_id)]
        for role, (artifact_id, artifact_type) in inputs.items()
    }
    return compute_step_spec_id(
        operation_name,
        step_number,
        params,
        typed,
        config_overrides,
    )


class TestComputeStepSpecId:
    """Tests for step_spec_id computation."""

    def test_deterministic(self):
        """Same inputs produce same spec_id."""
        spec1 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v2"},
            inputs={"data": ("abc123", "data")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v2"},
            inputs={"data": ("abc123", "data")},
        )
        assert spec1 == spec2
        assert len(spec1) == 32

    def test_upstream_change_cascades(self):
        """Different concrete input artifact identities change the step hash."""
        spec1 = _step_spec_from_ids(
            operation_name="ToolB",
            step_number=2,
            params=None,
            inputs={"data": ("upstream_v1", "data")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="ToolB",
            step_number=2,
            params=None,
            inputs={"data": ("upstream_v2", "data")},
        )
        assert spec1 != spec2

    def test_param_change(self):
        """Different params produce different spec_id."""
        spec1 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v1"},
            inputs={"data": ("abc123", "data")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v2"},
            inputs={"data": ("abc123", "data")},
        )
        assert spec1 != spec2

    def test_role_matters(self):
        """Same upstream but different role produces different spec_id."""
        spec1 = _step_spec_from_ids(
            operation_name="Score",
            step_number=2,
            params=None,
            inputs={"data": ("abc123", "data")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="Score",
            step_number=2,
            params=None,
            inputs={"scored": ("abc123", "data")},
        )
        assert spec1 != spec2

    def test_step_number_matters(self):
        """Same operation at different positions produces different spec_id."""
        spec1 = _step_spec_from_ids(
            operation_name="Score",
            step_number=1,
            params=None,
            inputs={"data": ("abc123", "data")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="Score",
            step_number=3,
            params=None,
            inputs={"data": ("abc123", "data")},
        )
        assert spec1 != spec2

    def test_empty_inputs(self):
        """Generative ops with no inputs produce valid spec_id."""
        spec = _step_spec_from_ids(
            operation_name="Generate",
            step_number=0,
            params={"count": 10},
            inputs={},
        )
        assert len(spec) == 32

    def test_none_params(self):
        """None params produces same spec_id as empty dict."""
        spec_none = _step_spec_from_ids(
            operation_name="Op",
            step_number=0,
            params=None,
            inputs={},
        )
        spec_empty = _step_spec_from_ids(
            operation_name="Op",
            step_number=0,
            params={},
            inputs={},
        )
        assert spec_none == spec_empty

    def test_config_overrides_none_same_as_empty(self):
        """config_overrides=None produces same spec_id as empty dict."""
        spec_none = _step_spec_from_ids(
            operation_name="Op",
            step_number=0,
            params=None,
            inputs={},
            config_overrides=None,
        )
        spec_empty = _step_spec_from_ids(
            operation_name="Op",
            step_number=0,
            params=None,
            inputs={},
            config_overrides={},
        )
        assert spec_none == spec_empty

    def test_config_overrides_changes_hash(self):
        """Different config_overrides produce different spec_id."""
        spec1 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params=None,
            inputs={"data": ("abc123", "data")},
            config_overrides=None,
        )
        spec2 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params=None,
            inputs={"data": ("abc123", "data")},
            config_overrides={"image": "/path/to/image.sif"},
        )
        assert spec1 != spec2

    def test_config_overrides_deterministic(self):
        """Same config_overrides produce same spec_id."""
        kwargs = {
            "operation_name": "ToolC",
            "step_number": 1,
            "params": None,
            "inputs": {"data": ("abc123", "data")},
            "config_overrides": {"image": "/path/to/image.sif", "gpu": True},
        }
        assert _step_spec_from_ids(**kwargs) == _step_spec_from_ids(**kwargs)

    def test_config_overrides_with_path_objects(self):
        """Path objects in config_overrides serialize correctly."""
        from pathlib import Path

        spec1 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params=None,
            inputs={},
            config_overrides={"image": Path("/opt/containers/tool_c.sif")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params=None,
            inputs={},
            config_overrides={"image": Path("/opt/containers/tool_c.sif")},
        )
        assert spec1 == spec2
        assert len(spec1) == 32
