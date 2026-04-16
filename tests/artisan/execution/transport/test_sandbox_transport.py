"""Tests for sandbox transport functions."""

from __future__ import annotations

import pytest

from artisan.execution.transport.sandbox_transport import (
    _MAX_SNAPSHOT_BYTES,
    restore_sandbox,
    snapshot_outputs,
    snapshot_sandbox,
    snapshot_sandbox_for_artifact,
)
from artisan.schemas.specs.input_models import ExecuteInput


class TestSnapshotSandbox:
    def test_round_trip_preserves_files(self, tmp_path):
        """snapshot_sandbox → restore_sandbox preserves file content."""
        root = tmp_path / "sandbox"
        (root / "materialized_inputs" / "data").mkdir(parents=True)
        (root / "materialized_inputs" / "data" / "input.json").write_bytes(
            b'{"key": "value"}'
        )
        (root / "preprocess").mkdir()
        (root / "preprocess" / "prep.txt").write_bytes(b"preprocessed")

        snapshot = snapshot_sandbox(str(root))

        assert len(snapshot) == 2
        assert snapshot["materialized_inputs/data/input.json"] == b'{"key": "value"}'
        assert snapshot["preprocess/prep.txt"] == b"preprocessed"

        # Restore to a different root
        restore_root = tmp_path / "restored"
        restore_root.mkdir()
        restore_sandbox(str(restore_root), snapshot)

        restored_input = restore_root / "materialized_inputs" / "data" / "input.json"
        assert restored_input.read_bytes() == b'{"key": "value"}'
        restored_prep = restore_root / "preprocess" / "prep.txt"
        assert restored_prep.read_bytes() == b"preprocessed"

    def test_empty_sandbox_returns_empty_dict(self, tmp_path):
        """Empty sandbox (in-memory operations) returns empty dict."""
        root = tmp_path / "sandbox"
        root.mkdir()
        assert snapshot_sandbox(str(root)) == {}

    def test_only_input_dirs_walked(self, tmp_path):
        """Only _INPUT_DIRS are walked; postprocess/ is ignored."""
        root = tmp_path / "sandbox"
        (root / "materialized_inputs").mkdir(parents=True)
        (root / "materialized_inputs" / "a.txt").write_bytes(b"included")
        (root / "postprocess").mkdir()
        (root / "postprocess" / "b.txt").write_bytes(b"excluded")
        (root / "other").mkdir()
        (root / "other" / "c.txt").write_bytes(b"excluded")

        snapshot = snapshot_sandbox(str(root))
        assert len(snapshot) == 1
        assert "materialized_inputs/a.txt" in snapshot

    def test_binary_content_preserved(self, tmp_path):
        """Binary file content survives round-trip."""
        root = tmp_path / "sandbox"
        (root / "execute").mkdir(parents=True)
        binary_data = bytes(range(256))
        (root / "execute" / "binary.bin").write_bytes(binary_data)

        snapshot = snapshot_sandbox(str(root))
        assert snapshot["execute/binary.bin"] == binary_data

    def test_size_limit_raises(self, tmp_path):
        """Exceeding 50 MB raises ValueError with clear message."""
        root = tmp_path / "sandbox"
        (root / "execute").mkdir(parents=True)
        # Write a file that exceeds the limit
        big_data = b"x" * (_MAX_SNAPSHOT_BYTES + 1)
        (root / "execute" / "huge.bin").write_bytes(big_data)

        with pytest.raises(ValueError, match="50 MB limit"):
            snapshot_sandbox(str(root))

    def test_execute_dir_included(self, tmp_path):
        """Files in execute/ are included in the snapshot."""
        root = tmp_path / "sandbox"
        (root / "execute").mkdir(parents=True)
        (root / "execute" / "output.txt").write_bytes(b"data")

        snapshot = snapshot_sandbox(str(root))
        assert "execute/output.txt" in snapshot


class TestSnapshotOutputs:
    def test_round_trip_preserves_files(self, tmp_path):
        """snapshot_outputs → restore_sandbox preserves output files."""
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()
        (execute_dir / "result.json").write_bytes(b'{"score": 0.95}')
        (execute_dir / "sub").mkdir()
        (execute_dir / "sub" / "nested.txt").write_bytes(b"nested")

        snapshot = snapshot_outputs(str(execute_dir))
        assert len(snapshot) == 2

        restore_dir = tmp_path / "restored"
        restore_dir.mkdir()
        restore_sandbox(str(restore_dir), snapshot)

        assert (restore_dir / "result.json").read_bytes() == b'{"score": 0.95}'
        assert (restore_dir / "sub" / "nested.txt").read_bytes() == b"nested"

    def test_empty_dir_returns_empty_dict(self, tmp_path):
        """Empty execute_dir returns empty dict."""
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()
        assert snapshot_outputs(str(execute_dir)) == {}

    def test_nonexistent_dir_returns_empty_dict(self, tmp_path):
        """Non-existent execute_dir returns empty dict."""
        assert snapshot_outputs(str(tmp_path / "nonexistent")) == {}

    def test_size_limit_raises(self, tmp_path):
        """Exceeding 50 MB raises ValueError."""
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()
        big_data = b"x" * (_MAX_SNAPSHOT_BYTES + 1)
        (execute_dir / "huge.bin").write_bytes(big_data)

        with pytest.raises(ValueError, match="50 MB limit"):
            snapshot_outputs(str(execute_dir))


class TestRestoreSandbox:
    def test_creates_directories(self, tmp_path):
        """restore_sandbox creates intermediate directories."""
        root = tmp_path / "root"
        root.mkdir()
        snapshot = {"a/b/c/file.txt": b"content"}

        restore_sandbox(str(root), snapshot)

        assert (root / "a" / "b" / "c" / "file.txt").read_bytes() == b"content"

    def test_empty_snapshot_noop(self, tmp_path):
        """Empty snapshot creates no files."""
        root = tmp_path / "root"
        root.mkdir()
        restore_sandbox(str(root), {})
        assert list(root.iterdir()) == []


class TestSnapshotSandboxForArtifact:
    """Tests for per-artifact sandbox snapshots."""

    def _make_sandbox(self, tmp_path):
        """Create a sandbox with two materialized inputs and a preprocess file."""
        root = tmp_path / "sandbox"
        mat = root / "materialized_inputs"
        mat.mkdir(parents=True)
        (mat / "a.json").write_bytes(b'{"a": 1}')
        (mat / "b.json").write_bytes(b'{"b": 2}')
        (root / "preprocess").mkdir()
        (root / "preprocess" / "config.yaml").write_bytes(b"shared: true")
        (root / "execute").mkdir()
        return root

    def test_captures_only_referenced_files(self, tmp_path):
        """Only the materialized file referenced by execute_input is captured."""
        root = self._make_sandbox(tmp_path)
        a_path = str(root / "materialized_inputs" / "a.json")

        ei = ExecuteInput(
            execute_dir=str(root / "execute" / "artifact_0"),
            inputs={"source": a_path},
        )

        snapshot = snapshot_sandbox_for_artifact(str(root), ei)

        assert "materialized_inputs/a.json" in snapshot
        assert "materialized_inputs/b.json" not in snapshot

    def test_includes_preprocess_dir(self, tmp_path):
        """The shared preprocess/ directory is always included."""
        root = self._make_sandbox(tmp_path)
        a_path = str(root / "materialized_inputs" / "a.json")

        ei = ExecuteInput(
            execute_dir=str(root / "execute" / "artifact_0"),
            inputs={"source": a_path},
        )

        snapshot = snapshot_sandbox_for_artifact(str(root), ei)

        assert "preprocess/config.yaml" in snapshot
        assert snapshot["preprocess/config.yaml"] == b"shared: true"

    def test_nested_input_paths_discovered(self, tmp_path):
        """File paths nested in dicts/lists are discovered."""
        root = self._make_sandbox(tmp_path)
        a_path = str(root / "materialized_inputs" / "a.json")
        b_path = str(root / "materialized_inputs" / "b.json")

        ei = ExecuteInput(
            execute_dir=str(root / "execute" / "artifact_0"),
            inputs={
                "items": [
                    {"config_path": a_path, "name": "first"},
                    {"config_path": b_path, "name": "second"},
                ]
            },
        )

        snapshot = snapshot_sandbox_for_artifact(str(root), ei)

        assert "materialized_inputs/a.json" in snapshot
        assert "materialized_inputs/b.json" in snapshot

    def test_non_path_values_ignored(self, tmp_path):
        """Scalar values that aren't file paths produce no errors."""
        root = self._make_sandbox(tmp_path)

        ei = ExecuteInput(
            execute_dir=str(root / "execute" / "artifact_0"),
            inputs={"count": 5, "name": "test", "flag": True},
        )

        snapshot = snapshot_sandbox_for_artifact(str(root), ei)

        # Only preprocess files — no materialized inputs referenced
        assert len(snapshot) == 1
        assert "preprocess/config.yaml" in snapshot

    def test_size_limit_enforced(self, tmp_path):
        """Exceeding 50 MB raises ValueError."""
        root = tmp_path / "sandbox"
        mat = root / "materialized_inputs"
        mat.mkdir(parents=True)
        big_data = b"x" * (_MAX_SNAPSHOT_BYTES + 1)
        (mat / "huge.bin").write_bytes(big_data)
        (root / "execute").mkdir()

        ei = ExecuteInput(
            execute_dir=str(root / "execute" / "artifact_0"),
            inputs={"data": str(mat / "huge.bin")},
        )

        with pytest.raises(ValueError, match="50 MB limit"):
            snapshot_sandbox_for_artifact(str(root), ei)

    def test_no_preprocess_dir_ok(self, tmp_path):
        """Works when no preprocess/ directory exists."""
        root = tmp_path / "sandbox"
        mat = root / "materialized_inputs"
        mat.mkdir(parents=True)
        (mat / "a.json").write_bytes(b'{"a": 1}')
        (root / "execute").mkdir()

        ei = ExecuteInput(
            execute_dir=str(root / "execute" / "artifact_0"),
            inputs={"source": str(mat / "a.json")},
        )

        snapshot = snapshot_sandbox_for_artifact(str(root), ei)

        assert len(snapshot) == 1
        assert "materialized_inputs/a.json" in snapshot

    def test_empty_inputs_returns_preprocess_only(self, tmp_path):
        """No file references → only preprocess files."""
        root = self._make_sandbox(tmp_path)

        ei = ExecuteInput(
            execute_dir=str(root / "execute" / "artifact_0"),
            inputs={},
        )

        snapshot = snapshot_sandbox_for_artifact(str(root), ei)

        assert len(snapshot) == 1
        assert "preprocess/config.yaml" in snapshot
