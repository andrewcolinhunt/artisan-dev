"""Tests for immutable runtime paths and resolved worker identity."""

from __future__ import annotations

from pathlib import Path

import cloudpickle
import pytest
from pydantic import ValidationError

from artisan.schemas.execution.runtime_environment import RuntimeEnvironment


class TestRuntimeEnvironmentWorkerId:
    def test_defaults_to_local_worker(self) -> None:
        env = RuntimeEnvironment(delta_root="/delta", staging_root="/staging")
        assert env.worker_id == 0

    @pytest.mark.parametrize("worker_id", [-(2**31), -1, 0, 42, 2**31 - 1])
    def test_accepts_signed_int32(self, worker_id: int) -> None:
        env = RuntimeEnvironment(
            delta_root="/delta", staging_root="/staging", worker_id=worker_id
        )
        assert env.worker_id == worker_id
        with pytest.raises(ValidationError, match="frozen"):
            env.worker_id = 7

    @pytest.mark.parametrize("worker_id", [True, False, 7.0, "7", -(2**31) - 1, 2**31])
    def test_rejects_invalid_worker_id(self, worker_id: object) -> None:
        with pytest.raises(ValidationError):
            RuntimeEnvironment(
                delta_root="/delta", staging_root="/staging", worker_id=worker_id
            )

    @pytest.mark.parametrize("transport", ["json", "cloudpickle"])
    def test_serialized_runtime_retains_frozen_worker_identity(
        self, transport: str
    ) -> None:
        env = RuntimeEnvironment(
            delta_root="/delta",
            staging_root="/staging",
            worker_id=42,
            worker_id_env_var="PROVIDER_WORKER_ID",
        )
        restored = (
            RuntimeEnvironment.model_validate_json(env.model_dump_json())
            if transport == "json"
            else cloudpickle.loads(cloudpickle.dumps(env))
        )
        assert restored == env
        assert restored is not env
        assert restored.worker_id == 42
        assert restored.worker_id_env_var == "PROVIDER_WORKER_ID"
        with pytest.raises(ValidationError, match="frozen"):
            restored.worker_id = 7


class TestRuntimeEnvironmentFilesRoot:
    """Tests for the files_root field on RuntimeEnvironment."""

    def test_files_root_defaults_to_none(self, tmp_path: Path) -> None:
        """files_root is None when not provided."""
        env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
        )
        assert env.files_root is None

    def test_files_root_accepts_value(self, tmp_path: Path) -> None:
        """files_root stores the provided path."""
        files_root = str(tmp_path / "files")
        env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            files_root=files_root,
        )
        assert env.files_root == files_root

    def test_files_root_frozen(self, tmp_path: Path) -> None:
        """files_root cannot be mutated (frozen model)."""
        env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            files_root=str(tmp_path / "files"),
        )
        try:
            env.files_root = str(tmp_path / "other")  # type: ignore[misc]
            pytest.fail("Should have raised")
        except Exception:
            pass  # Expected: ValidationError on frozen model
