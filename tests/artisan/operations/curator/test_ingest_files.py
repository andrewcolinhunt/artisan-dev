"""Unit tests for the IngestFiles abstract base class.

Tests cover:
1. Abstract contract: convert_file() must be implemented
2. Execution via a minimal concrete subclass
3. Empty input handling
4. Output role detection from subclass outputs dict
5. Class attributes (inputs, FILE_REF type)
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import ClassVar
from unittest.mock import Mock

import polars as pl
import pytest

from artisan.operations.curator.ingest_files import IngestFiles
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.output_spec import OutputSpec


def _df(ids: list[str]) -> pl.DataFrame:
    return pl.DataFrame({"artifact_id": ids})


def make_file_ref(
    path: str,
    content_hash: str = "a" * 32,
    size_bytes: int = 100,
    step_number: int = 0,
) -> FileRefArtifact:
    """Helper to create a finalized FileRefArtifact for testing."""
    from pathlib import Path as P

    return FileRefArtifact.draft(
        path=path,
        content_hash=content_hash,
        size_bytes=size_bytes,
        step_number=step_number,
        original_name=P(path).stem,
        extension=P(path).suffix,
    ).finalize()


class ConcreteIngest(IngestFiles):
    """Minimal concrete subclass for testing the abstract base."""

    class OutputRole(StrEnum):
        data = auto()

    name = "concrete_ingest"
    description = "Test ingest subclass"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.data: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            description="Test output",
        ),
    }

    def convert_file(self, file_ref: FileRefArtifact, step_number: int) -> Artifact:
        content = file_ref.read_content()
        filename = f"{file_ref.original_name}{file_ref.extension or ''}"
        return DataArtifact.draft(
            content=content,
            original_name=filename,
            step_number=step_number,
            external_path=file_ref.path,
        )


def _mock_store_with_refs(file_refs: list[FileRefArtifact]) -> Mock:
    """Create a mock ArtifactStore that returns file refs from get_artifacts_by_type."""
    store = Mock()
    store.get_artifacts_by_type.return_value = {fr.artifact_id: fr for fr in file_refs}
    return store


class TestIngestFilesAbstract:
    """Tests for the abstract contract."""

    def test_cannot_instantiate_without_convert_file(self):
        """Instantiating IngestFiles directly raises TypeError."""
        with pytest.raises(TypeError, match="convert_file"):
            IngestFiles()

    def test_subclass_with_convert_file_is_instantiable(self):
        """A concrete subclass can be instantiated."""
        op = ConcreteIngest()
        assert op.name == "concrete_ingest"


class TestIngestFilesExecution:
    """Tests for execute_curator() via ConcreteIngest."""

    def test_should_convert_single_file(self, tmp_path):
        """Happy path: one file ref -> one output artifact."""
        path = tmp_path / "test.dat"
        path.write_bytes(b"test content")
        file_ref = make_file_ref(str(path))

        op = ConcreteIngest()
        store = _mock_store_with_refs([file_ref])
        result = op.execute_curator(
            inputs={"file": _df([file_ref.artifact_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert len(result.artifacts["data"]) == 1
        assert result.artifacts["data"][0].content == b"test content"

    def test_should_convert_multiple_files(self, tmp_path):
        """Multiple file refs -> multiple output artifacts."""
        refs = []
        for i, name in enumerate(["a.dat", "b.dat", "c.dat"]):
            p = tmp_path / name
            p.write_bytes(f"content_{i}".encode())
            refs.append(make_file_ref(str(p), content_hash=f"{chr(97 + i)}" * 32))

        op = ConcreteIngest()
        store = _mock_store_with_refs(refs)
        result = op.execute_curator(
            inputs={"file": _df([r.artifact_id for r in refs])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert len(result.artifacts["data"]) == 3

    def test_should_fail_for_empty_input(self):
        """Empty file list returns ArtifactResult(success=False)."""
        op = ConcreteIngest()
        result = op.execute_curator(
            inputs={"file": _df([])},
            step_number=0,
            artifact_store=Mock(),
        )

        assert not result.success
        assert "No input files" in result.error

    def test_should_fail_for_missing_file_key(self):
        """Missing 'file' key returns ArtifactResult(success=False)."""
        op = ConcreteIngest()
        result = op.execute_curator(
            inputs={},
            step_number=0,
            artifact_store=Mock(),
        )

        assert not result.success
        assert "No input files" in result.error

    def test_output_role_uses_first_key_from_outputs(self, tmp_path):
        """Output role is derived from the first key in the subclass outputs dict."""
        path = tmp_path / "test.dat"
        path.write_bytes(b"data")
        file_ref = make_file_ref(str(path))

        op = ConcreteIngest()
        store = _mock_store_with_refs([file_ref])
        result = op.execute_curator(
            inputs={"file": _df([file_ref.artifact_id])},
            step_number=0,
            artifact_store=store,
        )

        assert "data" in result.artifacts
        assert len(result.artifacts) == 1


class TestIngestFilesClassAttributes:
    """Tests for IngestFiles class attributes."""

    def test_inputs_has_file_role(self):
        """IngestFiles declares a 'file' input role."""
        assert "file" in IngestFiles.inputs

    def test_file_input_is_file_ref_type(self):
        """The 'file' input expects FILE_REF artifacts."""
        assert IngestFiles.inputs["file"].artifact_type == ArtifactTypes.FILE_REF

    def test_file_input_is_required(self):
        """The 'file' input is required."""
        assert IngestFiles.inputs["file"].required is True
