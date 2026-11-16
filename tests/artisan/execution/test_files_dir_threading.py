"""Invocation file output directories are explicit, local and isolated per slot."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from artisan.execution.invocation import InvocationPaths, prepare_invocation
from artisan.operations.examples.file_generator import FileGenerator
from artisan.schemas.specs.input_models import ExecuteInput, FrozenInputContext


class TestExecuteInputFilesDir:
    """Tests for the files_dir field on ExecuteInput."""

    def test_defaults_to_none(self, tmp_path: Path) -> None:
        ei = ExecuteInput(execute_dir=str(tmp_path))
        assert ei.files_dir is None

    def test_accepts_path(self, tmp_path: Path) -> None:
        files_dir = str(tmp_path / "files")
        ei = ExecuteInput(execute_dir=str(tmp_path), files_dir=files_dir)
        assert ei.files_dir == files_dir

    def test_frozen(self, tmp_path: Path) -> None:
        ei = ExecuteInput(execute_dir=str(tmp_path))
        with pytest.raises(FrozenInstanceError):
            ei.files_dir = str(tmp_path)  # type: ignore[misc]


@pytest.mark.parametrize("enabled", [False, True])
def test_file_directory_is_explicit_and_isolated_per_slot(
    tmp_path: Path, enabled: bool
) -> None:
    paths = InvocationPaths.create(tmp_path, files_enabled=enabled)
    prepared = prepare_invocation(
        FileGenerator(),
        FrozenInputContext("files", {}),
        paths,
        batch_size=2,
        materialization_references={},
    )
    assert len(prepared.execute_inputs) == 2
    if enabled:
        expected = [
            str(tmp_path / "files_outputs" / f"artifact_{index}") for index in range(2)
        ]
        assert [item.files_dir for item in prepared.execute_inputs] == expected
        assert all(Path(path).is_dir() for path in expected)
    else:
        assert [item.files_dir for item in prepared.execute_inputs] == [None, None]
        assert not (tmp_path / "files_outputs").exists()
