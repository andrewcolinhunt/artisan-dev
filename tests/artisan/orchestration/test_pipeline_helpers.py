"""Tests for PipelineManager module-level helper functions."""

from __future__ import annotations

import re

from artisan.orchestration.pipeline_manager import (
    _extract_name_from_run_id,
    _extract_source_steps,
    _generate_run_id,
    _generate_step_run_id,
    _qualified_name,
    _serialize_input_refs,
)
from artisan.schemas.orchestration.output_reference import OutputReference


class TestGenerateRunId:
    """Tests for _generate_run_id."""

    def test_format(self):
        """Contains name, timestamp pattern, and uuid hex."""
        run_id = _generate_run_id("example_pipeline")
        assert run_id.startswith("example_pipeline_")
        # Format: name_YYYYMMDD_HHMMSS_hex8
        parts = run_id.split("_")
        # "example_pipeline" -> ["example", "pipeline", date, time, hex]
        assert len(parts) >= 4
        # Last part is 8 hex chars
        assert len(parts[-1]) == 8
        assert re.match(r"^[0-9a-f]{8}$", parts[-1])


class TestGenerateStepRunId:
    """Tests for _generate_step_run_id."""

    def test_length(self):
        """32-char hex string."""
        step_run_id = _generate_step_run_id("spec_abc123")
        assert len(step_run_id) == 32
        assert re.match(r"^[0-9a-f]{32}$", step_run_id)

    def test_different_specs_different_ids(self):
        """Different spec_ids produce different run_ids."""
        id1 = _generate_step_run_id("spec_a")
        id2 = _generate_step_run_id("spec_b")
        assert id1 != id2


class TestQualifiedName:
    """Tests for _qualified_name."""

    def test_returns_module_qualname(self):
        """Returns module.qualname."""
        from artisan.operations.curator.filter import Filter

        name = _qualified_name(Filter)
        assert "artisan.operations.curator.filter" in name
        assert "Filter" in name


class TestExtractSourceSteps:
    """Tests for _extract_source_steps."""

    def test_dict_input(self):
        """Dict with OutputReferences."""
        inputs = {
            "data": OutputReference(source_step=0, role="data"),
            "metrics": OutputReference(source_step=1, role="scores"),
        }
        assert _extract_source_steps(inputs) == {0, 1}

    def test_list_input(self):
        """List of OutputReferences."""
        inputs = [
            OutputReference(source_step=2, role="data"),
            OutputReference(source_step=3, role="data"),
        ]
        assert _extract_source_steps(inputs) == {2, 3}

    def test_none_input(self):
        """None returns empty set."""
        assert _extract_source_steps(None) == set()

    def test_dict_with_literal_ids(self):
        """Dict with literal artifact IDs (not OutputReferences)."""
        inputs = {"data": ["id1", "id2"]}
        assert _extract_source_steps(inputs) == set()


class TestSerializeInputRefs:
    """Tests for _serialize_input_refs."""

    def test_round_trip_output_ref(self):
        """OutputReference serialization is valid JSON."""
        import json

        inputs = {"data": OutputReference(source_step=0, role="data")}
        result = _serialize_input_refs(inputs)
        parsed = json.loads(result)
        assert parsed["data"]["type"] == "output_ref"
        assert parsed["data"]["source_step"] == 0
        assert parsed["data"]["role"] == "data"

    def test_none_input(self):
        """None serializes to 'null'."""
        assert _serialize_input_refs(None) == "null"

    def test_list_of_refs(self):
        """List of OutputReferences."""
        import json

        inputs = [
            OutputReference(source_step=0, role="data"),
            OutputReference(source_step=1, role="data"),
        ]
        result = _serialize_input_refs(inputs)
        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["type"] == "output_ref"

    def test_list_of_paths(self):
        """List of file paths."""
        import json

        inputs = ["/data/a.dat", "/data/b.dat"]
        result = _serialize_input_refs(inputs)
        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["type"] == "literal"


class TestExtractNameFromRunId:
    """Tests for _extract_name_from_run_id."""

    def test_simple_name(self):
        """Single-word name."""
        assert _extract_name_from_run_id("test_20260215_120000_abcd1234") == "test"

    def test_underscore_name(self):
        """Name containing underscores."""
        assert (
            _extract_name_from_run_id("example_pipeline_20260214_103000_a1b2c3d4")
            == "example_pipeline"
        )


class TestBuildInputSpec:
    """Tests for PipelineManager._build_input_spec."""

    def test_output_refs(self):
        """Dict OutputReferences mapped to upstream spec_ids."""
        from pathlib import Path

        from artisan.orchestration.pipeline_manager import PipelineManager
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=Path("/tmp/delta"),
            staging_root=Path("/tmp/staging"),
        )
        pm = PipelineManager(config)
        pm._step_spec_ids[0] = "upstream_spec_abc"

        inputs = {"data": OutputReference(source_step=0, role="data")}
        spec = pm._build_input_spec(inputs)
        assert spec == {"data": ("upstream_spec_abc", "data")}

    def test_file_paths(self):
        """File paths produce _file_paths key with hash."""
        from pathlib import Path

        from artisan.orchestration.pipeline_manager import PipelineManager
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=Path("/tmp/delta"),
            staging_root=Path("/tmp/staging"),
        )
        pm = PipelineManager(config)

        inputs = ["/data/b.dat", "/data/a.dat"]
        spec = pm._build_input_spec(inputs)
        assert "_file_paths" in spec
        assert len(spec["_file_paths"][0]) == 32  # hash

        # Order independent
        inputs2 = ["/data/a.dat", "/data/b.dat"]
        spec2 = pm._build_input_spec(inputs2)
        assert spec["_file_paths"] == spec2["_file_paths"]

    def test_merged_streams(self):
        """List of OutputReferences produce _merged_streams key."""
        from pathlib import Path

        from artisan.orchestration.pipeline_manager import PipelineManager
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=Path("/tmp/delta"),
            staging_root=Path("/tmp/staging"),
        )
        pm = PipelineManager(config)
        pm._step_spec_ids[0] = "spec_a"
        pm._step_spec_ids[1] = "spec_b"

        inputs = [
            OutputReference(source_step=0, role="data"),
            OutputReference(source_step=1, role="data"),
        ]
        spec = pm._build_input_spec(inputs)
        assert "_merged_streams" in spec

    def test_none_inputs(self):
        """None returns empty dict."""
        from pathlib import Path

        from artisan.orchestration.pipeline_manager import PipelineManager
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=Path("/tmp/delta"),
            staging_root=Path("/tmp/staging"),
        )
        pm = PipelineManager(config)
        assert pm._build_input_spec(None) == {}

    def test_literal_ids(self):
        """Dict with literal artifact ID lists."""
        from pathlib import Path

        from artisan.orchestration.pipeline_manager import PipelineManager
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=Path("/tmp/delta"),
            staging_root=Path("/tmp/staging"),
        )
        pm = PipelineManager(config)

        inputs = {"data": ["artifact_id_1", "artifact_id_2"]}
        spec = pm._build_input_spec(inputs)
        assert "data" in spec
        assert len(spec["data"][0]) == 32  # hash
        assert spec["data"][1] == ""  # empty role for literals
