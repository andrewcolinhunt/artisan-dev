"""Tests for sandbox transport functions."""

from __future__ import annotations

import subprocess

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

        files, empty_dirs = snapshot_sandbox(str(root))

        assert len(files) == 2
        assert files["materialized_inputs/data/input.json"] == b'{"key": "value"}'
        assert files["preprocess/prep.txt"] == b"preprocessed"
        assert empty_dirs == []

        # Restore to a different root
        restore_root = tmp_path / "restored"
        restore_root.mkdir()
        restore_sandbox(str(restore_root), files, empty_dirs=empty_dirs)

        restored_input = restore_root / "materialized_inputs" / "data" / "input.json"
        assert restored_input.read_bytes() == b'{"key": "value"}'
        restored_prep = restore_root / "preprocess" / "prep.txt"
        assert restored_prep.read_bytes() == b"preprocessed"

    def test_empty_sandbox_returns_empty_dict(self, tmp_path):
        """Empty sandbox (in-memory operations) returns empty dict."""
        root = tmp_path / "sandbox"
        root.mkdir()
        files, empty_dirs = snapshot_sandbox(str(root))
        assert files == {}
        assert empty_dirs == []

    def test_only_input_dirs_walked(self, tmp_path):
        """Only _INPUT_DIRS are walked; postprocess/ is ignored."""
        root = tmp_path / "sandbox"
        (root / "materialized_inputs").mkdir(parents=True)
        (root / "materialized_inputs" / "a.txt").write_bytes(b"included")
        (root / "postprocess").mkdir()
        (root / "postprocess" / "b.txt").write_bytes(b"excluded")
        (root / "other").mkdir()
        (root / "other" / "c.txt").write_bytes(b"excluded")

        files, empty_dirs = snapshot_sandbox(str(root))
        assert len(files) == 1
        assert "materialized_inputs/a.txt" in files
        # postprocess/ and other/ are outside _INPUT_DIRS, so their
        # empty children are NOT captured even though they exist.
        assert "postprocess" not in empty_dirs
        assert "other" not in empty_dirs

    def test_binary_content_preserved(self, tmp_path):
        """Binary file content survives round-trip."""
        root = tmp_path / "sandbox"
        (root / "execute").mkdir(parents=True)
        binary_data = bytes(range(256))
        (root / "execute" / "binary.bin").write_bytes(binary_data)

        files, _ = snapshot_sandbox(str(root))
        assert files["execute/binary.bin"] == binary_data

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

        files, _ = snapshot_sandbox(str(root))
        assert "execute/output.txt" in files


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

        files, _ = snapshot_sandbox_for_artifact(str(root), ei)

        assert "materialized_inputs/a.json" in files
        assert "materialized_inputs/b.json" not in files

    def test_includes_preprocess_dir(self, tmp_path):
        """The shared preprocess/ directory is always included."""
        root = self._make_sandbox(tmp_path)
        a_path = str(root / "materialized_inputs" / "a.json")

        ei = ExecuteInput(
            execute_dir=str(root / "execute" / "artifact_0"),
            inputs={"source": a_path},
        )

        files, _ = snapshot_sandbox_for_artifact(str(root), ei)

        assert "preprocess/config.yaml" in files
        assert files["preprocess/config.yaml"] == b"shared: true"

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

        files, _ = snapshot_sandbox_for_artifact(str(root), ei)

        assert "materialized_inputs/a.json" in files
        assert "materialized_inputs/b.json" in files

    def test_non_path_values_ignored(self, tmp_path):
        """Scalar values that aren't file paths produce no errors."""
        root = self._make_sandbox(tmp_path)

        ei = ExecuteInput(
            execute_dir=str(root / "execute" / "artifact_0"),
            inputs={"count": 5, "name": "test", "flag": True},
        )

        files, _ = snapshot_sandbox_for_artifact(str(root), ei)

        # Only preprocess files — no materialized inputs referenced
        assert len(files) == 1
        assert "preprocess/config.yaml" in files

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

        files, _ = snapshot_sandbox_for_artifact(str(root), ei)

        assert len(files) == 1
        assert "materialized_inputs/a.json" in files

    def test_empty_inputs_returns_preprocess_only(self, tmp_path):
        """No file references → only preprocess files."""
        root = self._make_sandbox(tmp_path)

        ei = ExecuteInput(
            execute_dir=str(root / "execute" / "artifact_0"),
            inputs={},
        )

        files, _ = snapshot_sandbox_for_artifact(str(root), ei)

        assert len(files) == 1
        assert "preprocess/config.yaml" in files


class TestEmptyDirRoundTrip:
    """Snapshot + restore preserves empty directory shells."""

    def test_captures_empty_dir_under_input_dirs(self, tmp_path):
        """An empty leaf directory under _INPUT_DIRS is recorded."""
        root = tmp_path / "sandbox"
        (root / "preprocess").mkdir(parents=True)  # empty
        (root / "execute").mkdir()  # empty
        (root / "materialized_inputs").mkdir()  # empty

        files, empty_dirs = snapshot_sandbox(str(root))

        assert files == {}
        assert set(empty_dirs) == {"preprocess", "execute", "materialized_inputs"}

    def test_restore_recreates_empty_dirs(self, tmp_path):
        """restore_sandbox mkdirs empty_dirs entries."""
        new_root = tmp_path / "restored"
        new_root.mkdir()

        restore_sandbox(str(new_root), {}, empty_dirs=["execute", "preprocess/nested"])

        assert (new_root / "execute").is_dir()
        assert (new_root / "preprocess" / "nested").is_dir()

    def test_restore_order_dirs_before_files(self, tmp_path):
        """Files and empty dirs coexist — both land correctly."""
        new_root = tmp_path / "restored"
        new_root.mkdir()

        restore_sandbox(
            str(new_root),
            {"preprocess/spec.json": b'{"k": 1}'},
            empty_dirs=["execute"],
        )

        assert (new_root / "preprocess" / "spec.json").read_bytes() == b'{"k": 1}'
        assert (new_root / "execute").is_dir()

    def test_per_artifact_exec_dir_captured(self, tmp_path):
        """snapshot_sandbox_for_artifact records empty execute/artifact_i/."""
        root = tmp_path / "sandbox"
        (root / "preprocess").mkdir(parents=True)
        (root / "preprocess" / "shared.yaml").write_bytes(b"x")
        artifact_exec = root / "execute" / "artifact_0"
        artifact_exec.mkdir(parents=True)  # empty per-artifact dir

        ei = ExecuteInput(execute_dir=str(artifact_exec), inputs={})

        files, empty_dirs = snapshot_sandbox_for_artifact(str(root), ei)

        assert "preprocess/shared.yaml" in files
        assert "execute/artifact_0" in empty_dirs


class TestSubprocessCwdRegression:
    """Load-bearing regression for the Modal subprocess-cwd bug.

    Locally, the creator lifecycle (`create_sandbox` +
    `prep_unit`) always mkdirs `execute/artifact_i/` before execute
    runs. Over Modal transport, only FILES cross the wire — empty
    dirs do not. An op that does `subprocess.Popen(cwd=execute_dir)`
    on a fresh (restored) sandbox root therefore ENOENTs at Popen
    time.

    These tests exercise the round-trip directly (no Modal mock)
    to pin the `empty_dirs` contract: if someone drops it, the
    negative-control test fails with exactly the bug we're fixing.
    """

    def _build_sandbox_with_empty_artifact_exec(self, tmp_path):
        """Mirror prep_unit's per-artifact sandbox shape."""
        sandbox_root = tmp_path / "local_sandbox"
        (sandbox_root / "preprocess").mkdir(parents=True)
        (sandbox_root / "preprocess" / "design.json").write_bytes(b'{"k": 1}')
        artifact_exec = sandbox_root / "execute" / "artifact_0"
        artifact_exec.mkdir(parents=True)  # empty leaf, like prep_unit creates
        return sandbox_root, artifact_exec

    def test_subprocess_popen_cwd_works_after_restore_to_fresh_root(self, tmp_path):
        """With empty_dirs threaded, subprocess.run(cwd=execute_dir) succeeds."""
        sandbox_root, artifact_exec = self._build_sandbox_with_empty_artifact_exec(
            tmp_path
        )

        ei = ExecuteInput(execute_dir=str(artifact_exec), inputs={})
        files, empty_dirs = snapshot_sandbox_for_artifact(str(sandbox_root), ei)

        fresh_root = tmp_path / "fresh_sandbox"  # does not exist yet
        restore_sandbox(str(fresh_root), files, empty_dirs=empty_dirs)

        fresh_exec = fresh_root / "execute" / "artifact_0"
        result = subprocess.run(["true"], cwd=str(fresh_exec), check=True)
        assert result.returncode == 0

    def test_subprocess_popen_cwd_fails_without_fix_contract(self, tmp_path):
        """Negative control: dropping empty_dirs reintroduces the bug.

        This test documents that the empty_dirs plumbing is
        load-bearing. If a future refactor silently drops it from
        any call site (snapshot, router, or _execute_on_modal),
        this test passes — which means the POSITIVE test above
        would also regress — so CI catches it.
        """
        sandbox_root, artifact_exec = self._build_sandbox_with_empty_artifact_exec(
            tmp_path
        )

        ei = ExecuteInput(execute_dir=str(artifact_exec), inputs={})
        files, _ = snapshot_sandbox_for_artifact(str(sandbox_root), ei)

        fresh_root = tmp_path / "fresh_sandbox"
        # Drop empty_dirs — reproduce the pre-fix restore behavior.
        restore_sandbox(str(fresh_root), files)

        fresh_exec = fresh_root / "execute" / "artifact_0"
        with pytest.raises(FileNotFoundError):
            subprocess.run(["true"], cwd=str(fresh_exec), check=True)
