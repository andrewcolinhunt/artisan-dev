"""Tests for table_schemas.py"""

from __future__ import annotations

import polars as pl

from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
    FRAMEWORK_SCHEMAS,
    NON_PARTITIONED_TABLES,
    create_empty_dataframe,
    get_schema,
)

DATA_SCHEMA = DataArtifact.POLARS_SCHEMA
METRICS_SCHEMA = MetricArtifact.POLARS_SCHEMA
FILE_REFS_SCHEMA = FileRefArtifact.POLARS_SCHEMA
CONFIGS_SCHEMA = ExecutionConfigArtifact.POLARS_SCHEMA


class TestArtifactSchemaDefinitions:
    """Tests for artifact model schemas (owned by models via POLARS_SCHEMA)."""

    def test_file_refs_has_required_columns(self):
        """file_refs has all columns from v3 design."""
        required = {
            "artifact_id",
            "origin_step_number",
            "content_hash",
            "path",
            "size_bytes",
            "metadata",
            "original_name",
            "extension",
            "external_path",
        }
        assert required == set(FILE_REFS_SCHEMA.keys())

    def test_file_refs_no_content_column(self):
        """file_refs does NOT have a content column (metadata only)."""
        assert "content" not in FILE_REFS_SCHEMA

    def test_data_has_required_columns(self):
        """data has all columns from v3 design."""
        required = {
            "artifact_id",
            "origin_step_number",
            "content",
            "original_name",
            "extension",
            "size_bytes",
            "columns",
            "row_count",
            "metadata",
            "external_path",
        }
        assert required == set(DATA_SCHEMA.keys())

    def test_data_content_is_binary(self):
        """data.content is Binary type."""
        assert DATA_SCHEMA["content"] == pl.Binary

    def test_metrics_has_required_columns(self):
        """metrics has all columns from v3 design."""
        required = {
            "artifact_id",
            "origin_step_number",
            "content",
            "original_name",
            "extension",
            "metadata",
            "external_path",
        }
        assert required == set(METRICS_SCHEMA.keys())


class TestFrameworkSchemaDefinitions:
    """Tests for framework schema dictionaries."""

    def test_executions_has_required_columns(self):
        """executions has all columns (lightweight, no inputs/outputs)."""
        required = {
            "execution_run_id",
            "execution_spec_id",
            "origin_step_number",
            "operation_name",
            "params",
            "user_overrides",
            "timestamp_start",
            "timestamp_end",
            "source_worker",
            "compute_backend",
            "success",
            "error",
            "tool_output",
            "worker_log",
            "metadata",
        }
        assert required == set(EXECUTIONS_SCHEMA.keys())

    def test_executions_no_inputs_outputs_columns(self):
        """executions does NOT have inputs/outputs columns."""
        assert "inputs" not in EXECUTIONS_SCHEMA
        assert "outputs" not in EXECUTIONS_SCHEMA

    def test_execution_edges_has_required_columns(self):
        """execution_edges has all columns for normalized input/output edges."""
        required = {
            "execution_run_id",
            "direction",
            "role",
            "artifact_id",
        }
        assert required == set(EXECUTION_EDGES_SCHEMA.keys())

    def test_execution_edges_all_string_columns(self):
        """All execution_edges columns are String type."""
        for col_name, col_type in EXECUTION_EDGES_SCHEMA.items():
            assert col_type == pl.String, f"{col_name} should be pl.String"

    def test_execution_edges_in_framework_schemas(self):
        """EXECUTION_EDGES is registered in FRAMEWORK_SCHEMAS."""
        assert TablePath.EXECUTION_EDGES in FRAMEWORK_SCHEMAS

    def test_non_partitioned_tables_includes_execution_edges(self):
        """execution_edges is in NON_PARTITIONED_TABLES."""
        assert TablePath.EXECUTION_EDGES in NON_PARTITIONED_TABLES
        assert TablePath.ARTIFACT_INDEX in NON_PARTITIONED_TABLES
        assert TablePath.ARTIFACT_EDGES in NON_PARTITIONED_TABLES

    def test_artifact_index_has_required_columns(self):
        """artifact_index has all columns from v3 design."""
        required = {"artifact_id", "artifact_type", "origin_step_number", "metadata"}
        assert required == set(ARTIFACT_INDEX_SCHEMA.keys())

    def test_framework_schemas_have_metadata_column(self):
        """Most framework tables have a metadata column.

        Exceptions: artifact_edges and execution_edges (pure edge tables).
        """
        tables_without_metadata = {
            TablePath.ARTIFACT_EDGES,
            TablePath.EXECUTION_EDGES,
        }
        for table_name, schema in FRAMEWORK_SCHEMAS.items():
            if table_name in tables_without_metadata:
                continue
            assert "metadata" in schema, f"{table_name} missing metadata column"
            assert schema["metadata"] == pl.String

    def test_framework_schemas_uses_tablepath_enum(self):
        """FRAMEWORK_SCHEMAS keys are TablePath enum values."""
        for key in FRAMEWORK_SCHEMAS:
            assert isinstance(key, TablePath)


class TestArtifactTypeDefTablePaths:
    """Tests for artifact type -> table path mapping via registry."""

    def test_data_table_path(self):
        """ArtifactTypeDef maps data -> artifacts/data."""
        assert ArtifactTypeDef.get_table_path("data") == "artifacts/data"

    def test_file_ref_table_path(self):
        """ArtifactTypeDef maps file_ref -> artifacts/file_refs."""
        assert ArtifactTypeDef.get_table_path("file_ref") == "artifacts/file_refs"

    def test_metric_table_path(self):
        """ArtifactTypeDef maps metric -> artifacts/metrics."""
        assert ArtifactTypeDef.get_table_path("metric") == "artifacts/metrics"

    def test_config_table_path(self):
        """ArtifactTypeDef maps config -> artifacts/configs."""
        assert ArtifactTypeDef.get_table_path("config") == "artifacts/configs"

    def test_all_registered_types_have_table_path(self):
        """All registered artifact types have a table_path."""
        for key, td in ArtifactTypeDef.get_all().items():
            assert td.table_path, f"{key} missing table_path"

    def test_all_registered_types_have_polars_schema(self):
        """All registered artifact types have a polars_schema."""
        for key, td in ArtifactTypeDef.get_all().items():
            schema = td.polars_schema()
            assert isinstance(schema, dict), f"{key} polars_schema not a dict"
            assert "artifact_id" in schema, f"{key} schema missing artifact_id"


class TestSchemaRegistry:
    """Tests for schema lookup functions."""

    def test_get_schema_valid_table(self):
        """get_schema returns correct schema for valid framework table."""
        schema = get_schema(TablePath.EXECUTIONS)
        assert schema == EXECUTIONS_SCHEMA

    def test_get_schema_all_framework_tables(self):
        """get_schema works for all framework TablePath values."""
        tables_without_metadata = {
            TablePath.ARTIFACT_EDGES,
            TablePath.EXECUTION_EDGES,
        }
        for table_path in TablePath:
            schema = get_schema(table_path)
            assert isinstance(schema, dict)
            if table_path not in tables_without_metadata:
                assert "metadata" in schema


class TestCreateEmptyDataframe:
    """Tests for create_empty_dataframe."""

    def test_creates_empty_df_with_correct_schema(self):
        """create_empty_dataframe produces DataFrame with correct columns."""
        df = create_empty_dataframe(TablePath.EXECUTIONS)
        assert df.shape == (0, len(EXECUTIONS_SCHEMA))
        assert set(df.columns) == set(EXECUTIONS_SCHEMA.keys())

    def test_empty_df_column_types(self):
        """Empty DataFrame has correct column types."""
        df = create_empty_dataframe(TablePath.ARTIFACT_INDEX)
        assert df.schema["artifact_id"] == pl.String
        assert df.schema["origin_step_number"] == pl.Int32

    def test_create_empty_dataframe_all_framework_tables(self):
        """create_empty_dataframe works for all framework TablePath values."""
        for table_path in TablePath:
            df = create_empty_dataframe(table_path)
            assert df.shape[0] == 0  # Empty
            assert len(df.columns) > 0  # Has columns


class TestDataFrameCreation:
    """Tests for creating DataFrames from schemas."""

    def test_create_data_dataframe(self):
        """Create a valid data DataFrame."""
        data = {
            "artifact_id": ["a" * 32],
            "origin_step_number": [0],
            "content": [b"col_a,col_b\n1,2\n"],
            "original_name": ["dataset"],
            "extension": [".csv"],
            "size_bytes": [17],
            "columns": ['["col_a","col_b"]'],
            "row_count": [1],
            "metadata": ["{}"],
            "external_path": [None],
        }
        df = pl.DataFrame(data, schema=DATA_SCHEMA)
        assert df.shape == (1, 10)

    def test_create_executions_dataframe_lightweight(self):
        """Create executions DataFrame (lightweight, no inputs/outputs)."""
        from datetime import datetime

        data = {
            "execution_run_id": ["r" * 32],
            "execution_spec_id": ["s" * 32],
            "origin_step_number": [1],
            "operation_name": ["relax"],
            "params": ["{}"],
            "user_overrides": ["{}"],
            "timestamp_start": [datetime.now()],
            "timestamp_end": [datetime.now()],
            "source_worker": [0],
            "compute_backend": ["local"],
            "success": [True],
            "error": [None],
            "tool_output": [None],
            "worker_log": [None],
            "metadata": ["{}"],
        }
        df = pl.DataFrame(data, schema=EXECUTIONS_SCHEMA)
        assert df.shape == (1, 15)  # 15 columns, no inputs/outputs

    def test_create_execution_edges_dataframe(self):
        """Create execution_edges DataFrame with input/output edges."""
        data = {
            "execution_run_id": ["r" * 32, "r" * 32, "r" * 32],
            "direction": ["input", "input", "output"],
            "role": ["data", "data", "processed"],
            "artifact_id": ["a" * 32, "b" * 32, "c" * 32],
        }
        df = pl.DataFrame(data, schema=EXECUTION_EDGES_SCHEMA)
        assert df.shape == (3, 4)  # 3 rows (edges), 4 columns
        assert df.filter(pl.col("direction") == "input").shape[0] == 2
        assert df.filter(pl.col("direction") == "output").shape[0] == 1


class TestArtifactEdgesSchema:
    """Tests for ARTIFACT_EDGES_SCHEMA."""

    def test_artifact_edges_has_required_columns(self):
        """artifact_edges has all required columns (9 total, includes group_id + step_boundary)."""
        required = {
            "execution_run_id",
            "source_artifact_id",
            "target_artifact_id",
            "source_artifact_type",
            "target_artifact_type",
            "source_role",
            "target_role",
            "group_id",
            "step_boundary",
        }
        assert required == set(ARTIFACT_EDGES_SCHEMA.keys())
        assert len(ARTIFACT_EDGES_SCHEMA) == 9

    def test_artifact_edges_no_inference_method(self):
        """artifact_edges does NOT have inference_method column."""
        assert "inference_method" not in ARTIFACT_EDGES_SCHEMA

    def test_artifact_edges_no_metadata(self):
        """artifact_edges does NOT have a metadata column (pure edge table)."""
        assert "metadata" not in ARTIFACT_EDGES_SCHEMA

    def test_artifact_edges_no_parent_child_columns(self):
        """artifact_edges uses source/target, not parent/child."""
        assert "parent_artifact_id" not in ARTIFACT_EDGES_SCHEMA
        assert "child_artifact_id" not in ARTIFACT_EDGES_SCHEMA
        assert "parent_artifact_type" not in ARTIFACT_EDGES_SCHEMA
        assert "child_artifact_type" not in ARTIFACT_EDGES_SCHEMA
        assert "parent_role" not in ARTIFACT_EDGES_SCHEMA
        assert "child_role" not in ARTIFACT_EDGES_SCHEMA

    def test_artifact_edges_in_framework_schemas(self):
        """ARTIFACT_EDGES is registered in FRAMEWORK_SCHEMAS."""
        assert TablePath.ARTIFACT_EDGES in FRAMEWORK_SCHEMAS

    def test_artifact_edges_column_types(self):
        """All columns except step_boundary are String; step_boundary is Boolean."""
        for col_name, col_type in ARTIFACT_EDGES_SCHEMA.items():
            if col_name == "step_boundary":
                assert col_type == pl.Boolean, f"{col_name} should be pl.Boolean"
            else:
                assert col_type == pl.String, f"{col_name} should be pl.String"

    def test_create_empty_artifact_edges_dataframe(self):
        """Create empty artifact_edges DataFrame."""
        df = create_empty_dataframe(TablePath.ARTIFACT_EDGES)

        assert len(df) == 0
        assert "execution_run_id" in df.columns
        assert "source_artifact_id" in df.columns
        assert "target_artifact_id" in df.columns
        assert "source_artifact_type" in df.columns
        assert "target_artifact_type" in df.columns
        assert "source_role" in df.columns
        assert "target_role" in df.columns
        assert "group_id" in df.columns

    def test_create_artifact_edges_dataframe_with_data(self):
        """Create artifact_edges DataFrame with sample data."""
        data = {
            "execution_run_id": ["e" * 32],
            "source_artifact_id": ["s" * 32],
            "target_artifact_id": ["t" * 32],
            "source_artifact_type": ["data"],
            "target_artifact_type": ["metric"],
            "source_role": ["data"],
            "target_role": ["energy"],
            "group_id": [None],
            "step_boundary": [True],
        }
        df = pl.DataFrame(data, schema=ARTIFACT_EDGES_SCHEMA)
        assert df.shape == (1, 9)
        assert df["source_artifact_type"][0] == "data"
        assert df["target_artifact_type"][0] == "metric"
        assert df["group_id"][0] is None
        assert df["step_boundary"][0] is True

    def test_artifact_edges_group_edges(self):
        """Test creating GROUP-related edges in artifact_edges."""
        member_to_group = {
            "execution_run_id": ["e" * 32],
            "source_artifact_id": ["m" * 32],
            "target_artifact_id": ["g" * 32],
            "source_artifact_type": ["data"],
            "target_artifact_type": ["group"],
            "source_role": ["input_data"],
            "target_role": ["comparison_group"],
            "group_id": [None],
            "step_boundary": [True],
        }
        df1 = pl.DataFrame(member_to_group, schema=ARTIFACT_EDGES_SCHEMA)
        assert df1["target_artifact_type"][0] == "group"

        group_to_output = {
            "execution_run_id": ["e" * 32],
            "source_artifact_id": ["g" * 32],
            "target_artifact_id": ["r" * 32],
            "source_artifact_type": ["group"],
            "target_artifact_type": ["metric"],
            "source_role": ["comparison_group"],
            "target_role": ["accuracy"],
            "group_id": [None],
            "step_boundary": [True],
        }
        df2 = pl.DataFrame(group_to_output, schema=ARTIFACT_EDGES_SCHEMA)
        assert df2["source_artifact_type"][0] == "group"

    def test_artifact_edges_group_id_column(self):
        """Test that group_id column exists and is nullable String."""
        assert "group_id" in ARTIFACT_EDGES_SCHEMA
        assert ARTIFACT_EDGES_SCHEMA["group_id"] == pl.String

    def test_artifact_edges_group_id_with_value(self):
        """Test creating artifact_edges with group_id set."""
        data = {
            "execution_run_id": ["e" * 32, "e" * 32],
            "source_artifact_id": ["s" * 32, "t" * 32],
            "target_artifact_id": ["u" * 32, "u" * 32],
            "source_artifact_type": ["data", "data"],
            "target_artifact_type": ["metric", "metric"],
            "source_role": ["data", "data"],
            "target_role": ["energy", "energy"],
            "group_id": ["g" * 32, "g" * 32],
            "step_boundary": [True, True],
        }
        df = pl.DataFrame(data, schema=ARTIFACT_EDGES_SCHEMA)
        assert df.shape == (2, 9)
        assert df["group_id"][0] == "g" * 32
        assert df["group_id"][1] == "g" * 32


class TestTablePathEnum:
    """Tests for TablePath enum values."""

    def test_artifact_edges_enum_value(self):
        """ARTIFACT_EDGES enum exists with correct value."""
        assert TablePath.ARTIFACT_EDGES == "provenance/artifact_edges"

    def test_all_tablepath_values_unique(self):
        """All TablePath enum values are unique."""
        values = [str(t) for t in TablePath]
        assert len(values) == len(set(values))
