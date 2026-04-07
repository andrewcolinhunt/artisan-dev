"""Tests for operation lifecycle input models."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from artisan.schemas import ExecuteInput, PostprocessInput, PreprocessInput
from artisan.schemas.artifact.metric import MetricArtifact


class TestPreprocessInput:
    """Tests for PreprocessInput dataclass."""

    def test_create_minimal(self, tmp_path: Path):
        """Create with only required field."""
        inp = PreprocessInput(preprocess_dir=str(tmp_path))
        assert inp.preprocess_dir == str(tmp_path)
        assert inp.input_artifacts == {}
        assert inp.metadata == {}

    def test_create_with_input_artifacts(self, tmp_path: Path):
        """Create with input artifacts (with materialized_path)."""
        input_file = tmp_path / "input.json"
        input_file.write_text('{"score": 0.5}')

        artifact = MetricArtifact.draft(
            content={"score": 0.5},
            original_name="input.json",
            step_number=0,
        )
        artifact.materialized_path = str(input_file)

        inp = PreprocessInput(
            preprocess_dir=str(tmp_path),
            input_artifacts={"metric": [artifact]},
        )
        assert "metric" in inp.input_artifacts
        assert len(inp.input_artifacts["metric"]) == 1
        assert inp.input_artifacts["metric"][0].materialized_path == str(input_file)

    def test_create_with_metadata(self, tmp_path: Path):
        """Create with metadata."""
        inp = PreprocessInput(
            preprocess_dir=str(tmp_path),
            metadata={"extra": "data"},
        )
        assert inp.metadata["extra"] == "data"

    def test_not_frozen(self, tmp_path: Path):
        """PreprocessInput is mutable (not frozen)."""
        inp = PreprocessInput(preprocess_dir=str(tmp_path))
        # Should not raise - dataclass is not frozen
        inp.input_artifacts = {"new": []}
        assert inp.input_artifacts == {"new": []}


class TestExecuteInput:
    """Tests for ExecuteInput dataclass."""

    def test_create_minimal(self, tmp_path: Path):
        """Create with only required field."""
        inp = ExecuteInput(execute_dir=str(tmp_path))
        assert inp.execute_dir == str(tmp_path)
        assert inp.inputs == {}
        assert inp.metadata == {}

    def test_create_with_prepared_inputs(self, tmp_path: Path):
        """Create with inputs from preprocess."""
        config_path = str(tmp_path / "config.json")
        Path(config_path).touch()

        inp = ExecuteInput(
            execute_dir=str(tmp_path),
            inputs={"config": config_path, "count": 5},
        )
        assert inp.inputs["config"] == config_path
        assert inp.inputs["count"] == 5

    def test_create_with_metadata(self, tmp_path: Path):
        """Create with metadata escape hatch."""
        inp = ExecuteInput(
            execute_dir=str(tmp_path),
            inputs={"data": "value"},
            metadata={"timeout": 300, "retry": True},
        )
        assert inp.metadata["timeout"] == 300
        assert inp.metadata["retry"] is True

    def test_frozen(self, tmp_path: Path):
        """ExecuteInput is immutable (frozen dataclass)."""
        inp = ExecuteInput(execute_dir=str(tmp_path))
        with pytest.raises(dataclasses.FrozenInstanceError):
            inp.inputs = {"new": "value"}


class TestPostprocessInput:
    """Tests for PostprocessInput dataclass."""

    def test_create_minimal(self, tmp_path: Path):
        """Create with required fields."""
        inp = PostprocessInput(step_number=0, postprocess_dir=str(tmp_path))
        assert inp.postprocess_dir == str(tmp_path)
        assert inp.step_number == 0
        assert inp.file_outputs == []
        assert inp.memory_outputs is None
        assert inp.input_artifacts == {}
        assert inp.metadata == {}

    def test_create_with_memory_outputs(self, tmp_path: Path):
        """Create with dict as memory outputs."""
        inp = PostprocessInput(
            step_number=1,
            postprocess_dir=str(tmp_path),
            memory_outputs={"status": "success", "metrics": {"accuracy": 1.5}},
        )
        assert inp.memory_outputs["status"] == "success"
        assert inp.memory_outputs["metrics"]["accuracy"] == 1.5

    def test_create_with_memory_outputs_any_type(self, tmp_path: Path):
        """Create with various types as memory outputs."""
        # Can be anything - string, int, list, complex object
        for mem in [0, "result", [1, 2, 3], {"nested": {"data": True}}]:
            inp = PostprocessInput(
                step_number=0,
                postprocess_dir=str(tmp_path),
                memory_outputs=mem,
            )
            assert inp.memory_outputs == mem

    def test_create_with_file_outputs(self, tmp_path: Path):
        """Create with files from execute_dir."""
        output_files = [
            str(tmp_path / "output1.dat"),
            str(tmp_path / "output2.dat"),
        ]
        for f in output_files:
            Path(f).touch()

        inp = PostprocessInput(
            step_number=1,
            postprocess_dir=str(tmp_path),
            file_outputs=output_files,
        )
        assert len(inp.file_outputs) == 2
        assert str(tmp_path / "output1.dat") in inp.file_outputs

    def test_create_with_input_artifacts(self, tmp_path: Path):
        """Create with input artifacts for naming and lineage."""
        artifact = MetricArtifact.draft(
            content={"score": 0.5},
            original_name="input.json",
            step_number=0,
        )
        artifact.materialized_path = str(tmp_path / "input.json")

        inp = PostprocessInput(
            step_number=1,
            postprocess_dir=str(tmp_path),
            input_artifacts={"metric": [artifact]},
        )
        assert len(inp.input_artifacts["metric"]) == 1
        assert inp.input_artifacts["metric"][0].original_name == "input"  # Stem only

    def test_step_number_required(self, tmp_path: Path):
        """step_number is required."""
        with pytest.raises(TypeError):
            PostprocessInput(postprocess_dir=str(tmp_path))

    def test_not_frozen(self, tmp_path: Path):
        """PostprocessInput is mutable (not frozen)."""
        inp = PostprocessInput(step_number=0, postprocess_dir=str(tmp_path))
        # Should not raise - dataclass is not frozen
        inp.memory_outputs = "new_result"
        assert inp.memory_outputs == "new_result"


def _make_artifact(name: str) -> MetricArtifact:
    """Helper to create a draft MetricArtifact with a given name."""
    return MetricArtifact.draft(
        content={"name": name},
        original_name=name,
        step_number=0,
    )


class TestPreprocessInputGrouped:
    """Tests for PreprocessInput.grouped() method."""

    def test_grouped_two_roles(self, tmp_path: Path):
        """Yields correct dicts for 2-role inputs."""
        s1, s2 = _make_artifact("s1.dat"), _make_artifact("s2.dat")
        c1, c2 = _make_artifact("c1.json"), _make_artifact("c2.json")

        inp = PreprocessInput(
            preprocess_dir=str(tmp_path),
            input_artifacts={"data": [s1, s2], "config": [c1, c2]},
        )
        groups = list(inp.grouped())
        assert len(groups) == 2
        assert groups[0] == {"data": s1, "config": c1}
        assert groups[1] == {"data": s2, "config": c2}

    def test_grouped_three_roles(self, tmp_path: Path):
        """Yields correct dicts for 3-role inputs."""
        s1 = _make_artifact("s1.dat")
        c1 = _make_artifact("c1.json")
        m1 = _make_artifact("m1.json")

        inp = PreprocessInput(
            preprocess_dir=str(tmp_path),
            input_artifacts={
                "data": [s1],
                "config": [c1],
                "metric": [m1],
            },
        )
        groups = list(inp.grouped())
        assert len(groups) == 1
        assert groups[0] == {"data": s1, "config": c1, "metric": m1}

    def test_grouped_mismatched_lengths(self, tmp_path: Path):
        """Raises ValueError on mismatched list lengths (strict zip)."""
        s1, s2 = _make_artifact("s1.dat"), _make_artifact("s2.dat")
        c1 = _make_artifact("c1.json")

        inp = PreprocessInput(
            preprocess_dir=str(tmp_path),
            input_artifacts={"data": [s1, s2], "config": [c1]},
        )
        with pytest.raises(ValueError, match="zip"):
            list(inp.grouped())

    def test_grouped_empty_inputs(self, tmp_path: Path):
        """Yields nothing when input_artifacts is empty."""
        inp = PreprocessInput(preprocess_dir=str(tmp_path), input_artifacts={})
        groups = list(inp.grouped())
        assert groups == []

    def test_grouped_single_role(self, tmp_path: Path):
        """Yields single-key dicts for a single role."""
        s1, s2 = _make_artifact("s1.dat"), _make_artifact("s2.dat")

        inp = PreprocessInput(
            preprocess_dir=str(tmp_path),
            input_artifacts={"data": [s1, s2]},
        )
        groups = list(inp.grouped())
        assert len(groups) == 2
        assert groups[0] == {"data": s1}
        assert groups[1] == {"data": s2}


class TestPostprocessInputGrouped:
    """Tests for PostprocessInput.grouped() method."""

    def test_grouped_two_roles(self, tmp_path: Path):
        """Yields correct dicts for 2-role inputs."""
        s1, s2 = _make_artifact("s1.dat"), _make_artifact("s2.dat")
        c1, c2 = _make_artifact("c1.json"), _make_artifact("c2.json")

        inp = PostprocessInput(
            step_number=1,
            postprocess_dir=str(tmp_path),
            input_artifacts={"data": [s1, s2], "config": [c1, c2]},
        )
        groups = list(inp.grouped())
        assert len(groups) == 2
        assert groups[0] == {"data": s1, "config": c1}
        assert groups[1] == {"data": s2, "config": c2}

    def test_grouped_three_roles(self, tmp_path: Path):
        """Yields correct dicts for 3-role inputs."""
        s1 = _make_artifact("s1.dat")
        c1 = _make_artifact("c1.json")
        m1 = _make_artifact("m1.json")

        inp = PostprocessInput(
            step_number=1,
            postprocess_dir=str(tmp_path),
            input_artifacts={
                "data": [s1],
                "config": [c1],
                "metric": [m1],
            },
        )
        groups = list(inp.grouped())
        assert len(groups) == 1
        assert groups[0] == {"data": s1, "config": c1, "metric": m1}

    def test_grouped_mismatched_lengths(self, tmp_path: Path):
        """Raises ValueError on mismatched list lengths (strict zip)."""
        s1, s2 = _make_artifact("s1.dat"), _make_artifact("s2.dat")
        c1 = _make_artifact("c1.json")

        inp = PostprocessInput(
            step_number=1,
            postprocess_dir=str(tmp_path),
            input_artifacts={"data": [s1, s2], "config": [c1]},
        )
        with pytest.raises(ValueError, match="zip"):
            list(inp.grouped())

    def test_grouped_empty_inputs(self, tmp_path: Path):
        """Yields nothing when input_artifacts is empty."""
        inp = PostprocessInput(
            step_number=1,
            postprocess_dir=str(tmp_path),
            input_artifacts={},
        )
        groups = list(inp.grouped())
        assert groups == []

    def test_grouped_single_role(self, tmp_path: Path):
        """Yields single-key dicts for a single role."""
        s1, s2 = _make_artifact("s1.dat"), _make_artifact("s2.dat")

        inp = PostprocessInput(
            step_number=1,
            postprocess_dir=str(tmp_path),
            input_artifacts={"data": [s1, s2]},
        )
        groups = list(inp.grouped())
        assert len(groups) == 2
        assert groups[0] == {"data": s1}
        assert groups[1] == {"data": s2}


class TestInputSpecMaterialize:
    """Tests for materialize field on InputSpec."""

    def test_materialize_default_true(self):
        """materialize defaults to True."""
        from artisan.schemas import InputSpec

        spec = InputSpec()
        assert spec.materialize is True

    def test_materialize_explicit_false(self):
        """Can set materialize to False."""
        from artisan.schemas import ArtifactTypes, InputSpec

        spec = InputSpec(
            artifact_type=ArtifactTypes.METRIC,
            materialize=False,
            description="In-memory config",
        )
        assert spec.materialize is False

    def test_hash_includes_materialize(self):
        """Hash function includes materialize field."""
        from artisan.schemas import InputSpec

        spec1 = InputSpec(materialize=True)
        spec2 = InputSpec(materialize=False)

        assert hash(spec1) != hash(spec2)


class TestAssociatedArtifacts:
    """Tests for associated_artifacts() accessor on input models."""

    def test_preprocess_associated_artifacts_returns_list(self, tmp_path: Path):
        """PreprocessInput.associated_artifacts() returns correct list."""
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        metric = MetricArtifact.draft(
            content={"score": 0.5}, original_name="test.json", step_number=0
        ).finalize()
        assoc = ExecutionConfigArtifact.draft(
            content={"param": "value"},
            original_name="test_config.json",
            step_number=1,
        ).finalize()

        inp = PreprocessInput(
            preprocess_dir=str(tmp_path),
            input_artifacts={"metric": [metric]},
            _associated={
                (metric.artifact_id, "config"): [assoc],
            },
        )

        result = inp.associated_artifacts(metric, "config")
        assert result == [assoc]

    def test_preprocess_associated_artifacts_empty(self, tmp_path: Path):
        """Returns empty list when no associations exist."""
        metric = MetricArtifact.draft(
            content={"score": 0.5}, original_name="test.json", step_number=0
        ).finalize()

        inp = PreprocessInput(preprocess_dir=tmp_path)
        assert inp.associated_artifacts(metric, "data_annotation") == []

    def test_postprocess_associated_artifacts_returns_list(self, tmp_path: Path):
        """PostprocessInput.associated_artifacts() returns correct list."""
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        metric = MetricArtifact.draft(
            content={"score": 0.8}, original_name="test.json", step_number=0
        ).finalize()
        assoc = ExecutionConfigArtifact.draft(
            content={"param": "value"},
            original_name="test_config.json",
            step_number=1,
        ).finalize()

        inp = PostprocessInput(
            step_number=1,
            postprocess_dir=str(tmp_path),
            input_artifacts={"metric": [metric]},
            _associated={
                (metric.artifact_id, "config"): [assoc],
            },
        )

        result = inp.associated_artifacts(metric, "config")
        assert result == [assoc]

    def test_postprocess_associated_artifacts_empty(self, tmp_path: Path):
        """Returns empty list when no associations exist."""
        metric = MetricArtifact.draft(
            content={"score": 0.8}, original_name="test.json", step_number=0
        ).finalize()

        inp = PostprocessInput(step_number=1, postprocess_dir=tmp_path)
        assert inp.associated_artifacts(metric, "data_annotation") == []

    def test_associated_default_empty(self, tmp_path: Path):
        """_associated defaults to empty dict."""
        inp = PreprocessInput(preprocess_dir=tmp_path)
        assert inp._associated == {}

        inp2 = PostprocessInput(step_number=0, postprocess_dir=tmp_path)
        assert inp2._associated == {}
