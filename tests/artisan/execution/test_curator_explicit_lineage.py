"""Tests for curator _handle_artifact_result honoring ArtifactResult.lineage."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar
from unittest.mock import MagicMock, patch

from artisan.execution.executors.curator import _handle_artifact_result
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


def _csv_bytes(text: str) -> bytes:
    return text.encode("utf-8")


class _TestOp(OperationDefinition):
    """Minimal operation for testing."""

    name: str = "test_curator_op"

    class InputRole:
        DATA = "data"

    class OutputRole:
        DATA = "data"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "data": InputSpec(artifact_type="data"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "data": OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["data"]},
        ),
    }


def _make_data(artifact_id: str, name: str) -> DataArtifact:
    return DataArtifact(
        artifact_type="data",
        artifact_id=artifact_id,
        origin_step_number=0,
        content=_csv_bytes(f"a\n{name}\n"),
        original_name=name,
        extension=".csv",
        size_bytes=10,
    )


def _make_context() -> ExecutionContext:
    return MagicMock(spec=ExecutionContext, execution_run_id="run-123")


def _make_unit() -> ExecutionUnit:
    unit = MagicMock(spec=ExecutionUnit)
    unit.group_ids = None
    return unit


class TestCuratorHonorsExplicitLineage:
    """Curator uses ArtifactResult.lineage when provided."""

    @patch("artisan.execution.executors.curator.record_execution_success")
    @patch("artisan.execution.executors.curator.build_artifact_edges_from_dict")
    @patch("artisan.execution.executors.curator.build_edges")
    @patch("artisan.execution.executors.curator.validate_lineage_integrity")
    def test_explicit_lineage_used(
        self,
        mock_validate_integrity,
        mock_build_edges,
        mock_build_artifact_edges,
        mock_record_success,
    ):
        """When result.lineage is set, capture_lineage_metadata is not called."""
        input_art = _make_data("a" * 32, "input_data")
        output_art = DataArtifact.draft(
            content=_csv_bytes("a\n1\n"),
            original_name="output_data.csv",
            step_number=1,
        )

        explicit_lineage = {
            "data": [
                LineageMapping(
                    draft_original_name="output_data",
                    source_artifact_id="a" * 32,
                    source_role="data",
                )
            ]
        }

        result = ArtifactResult(
            artifacts={"data": [output_art]},
            lineage=explicit_lineage,
        )

        mock_build_edges.return_value = []
        mock_build_artifact_edges.return_value = []
        mock_record_success.return_value = MagicMock()

        with patch(
            "artisan.execution.executors.curator.capture_lineage_metadata"
        ) as mock_capture:
            _handle_artifact_result(
                result=result,
                operation=_TestOp(),
                artifact_store=MagicMock(),
                execution_context=_make_context(),
                unit=_make_unit(),
                inputs={"data": ["a" * 32]},
                input_artifacts={"data": [input_art]},
                timestamp_end=datetime.now(UTC),
            )

            # capture_lineage_metadata should NOT be called
            mock_capture.assert_not_called()
            # validate_lineage_integrity should be called with explicit lineage
            mock_validate_integrity.assert_called_once()


class TestCuratorFallsBackToStemInference:
    """Curator falls back to capture_lineage_metadata when lineage is None."""

    @patch("artisan.execution.executors.curator.record_execution_success")
    @patch("artisan.execution.executors.curator.build_artifact_edges_from_dict")
    @patch("artisan.execution.executors.curator.build_edges")
    @patch("artisan.execution.executors.curator.capture_lineage_metadata")
    def test_none_lineage_triggers_stem_inference(
        self,
        mock_capture,
        mock_build_edges,
        mock_build_artifact_edges,
        mock_record_success,
    ):
        """When result.lineage is None, capture_lineage_metadata is called."""
        input_art = _make_data("a" * 32, "input_data")
        output_art = DataArtifact.draft(
            content=_csv_bytes("a\n1\n"),
            original_name="input_data.csv",
            step_number=1,
        )

        result = ArtifactResult(
            artifacts={"data": [output_art]},
            lineage=None,
        )

        mock_capture.return_value = {}
        mock_build_edges.return_value = []
        mock_build_artifact_edges.return_value = []
        mock_record_success.return_value = MagicMock()

        _handle_artifact_result(
            result=result,
            operation=_TestOp(),
            artifact_store=MagicMock(),
            execution_context=_make_context(),
            unit=_make_unit(),
            inputs={"data": ["a" * 32]},
            input_artifacts={"data": [input_art]},
            timestamp_end=datetime.now(UTC),
        )

        mock_capture.assert_called_once()
