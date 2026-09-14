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
