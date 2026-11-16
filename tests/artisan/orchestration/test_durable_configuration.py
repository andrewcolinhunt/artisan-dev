"""Secret-safe pending rows, executed evidence, cache reuse, and resumed results."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from fixtures.retained_pipeline import NativePipelineRunner
from pydantic import BaseModel, Field, SecretStr

from artisan.operations.base import OperationDefinition
from artisan.operations.curator import IngestCSV
from artisan.orchestration import PipelineManager
from artisan.schemas import ArtifactResult, CSVArtifact, StorageConfig, TablePath
from artisan.schemas.orchestration.step_lifecycle import (
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage import ArtifactStore


class NestedSecret(BaseModel):
    value: SecretStr = Field(validation_alias="supplied")


class DurableCurator(OperationDefinition):
    name = "durable_configuration_curator"

    class Params(BaseModel):
        """Private and public curator configuration.

        Attributes:
            nested: Nested private credential.
            token_count: Credential scalar.
            secret_flag: Credential Boolean.
            label: Public diagnostic.
            fail: Whether authored execution fails.
        """

        nested: NestedSecret
        token_count: int = 2
        secret_flag: bool = True
        label: str = "public value"
        fail: bool = False

    params: Params

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        value = self.params.nested.value.get_secret_value()
        if self.params.fail:
            message = f"failed with {value}"
            raise RuntimeError(message)
        result = ArtifactResult(metadata={"alias": value, "public": self.params.label})
        result.add_artifact(
            "actual",
            CSVArtifact.draft(value.encode(), "original.txt", step_number),
            sources={},
        )
        return result


def _manager(tmp_path: Path) -> PipelineManager:
    return PipelineManager.create(
        "durable_config",
        str(tmp_path / "durable-store"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
        preserve_staging=True,
        storage=StorageConfig(options={"token": "durable-storage-secret"}),
    )


def _rows(manager: PipelineManager, table: str) -> pl.DataFrame:
    path = Path(manager.config.delta_root) / table
    return pl.read_delta(path) if path.exists() else pl.DataFrame()


@pytest.mark.parametrize("outcome", ["success", "failure", "skip", "cancel"])
def test_initial_pending_row_and_every_terminal_copy_are_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with _manager(tmp_path) as manager:
        raw = "durable-nested-plaintext"
        params = {"nested": {"supplied": raw}, "fail": outcome == "failure"}
        if outcome == "skip":
            manager._stopped = True
        if outcome == "cancel":
            manager.cancel()
        create = manager.tracker.create_attempt
        observed: list[str] = []
        resource_secret = "durable-runner-extra-secret"

        def create_attempt(record: StepStartRecord) -> StepState:
            state = create(record)
            pending = _rows(manager, TablePath.STEPS).filter(
                pl.col("step_run_id") == record.step_run_id
            )
            assert pending["status"].to_list() == ["pending"]
            for secret in (raw, resource_secret, "durable-storage-secret"):
                assert secret not in str(pending.to_dicts())
            options = json.loads(pending["compute_options_json"][0])
            assert options["dynamic_output_roles"] is True
            assert options["application_modules"] == []
            observed.append(record.params_json)
            return state

        monkeypatch.setattr(manager.tracker, "create_attempt", create_attempt)
        result = manager.run(
            DurableCurator,
            params=params,
            runner_resources={"extra": {"api_token": resource_secret}},
            step_runner=NativePipelineRunner(manager.committer),
        )
        assert (
            result.status
            == {
                "success": StepStatus.SUCCEEDED,
                "failure": StepStatus.FAILED,
                "skip": StepStatus.SKIPPED,
                "cancel": StepStatus.CANCELLED,
            }[outcome]
        )
        rows = _rows(manager, TablePath.STEPS)
        assert observed
        assert set(rows["params_json"]) == set(observed)
        assert len(set(observed)) == 1
        for secret in (raw, resource_secret, "durable-storage-secret"):
            assert secret not in str(rows.to_dicts())
            assert secret not in str(_rows(manager, TablePath.EXECUTIONS).to_dicts())
        for evidence in Path(manager.config.staging_root).rglob("executions.parquet"):
            assert raw not in str(pl.read_parquet(evidence).to_dicts())
        if outcome == "success":
            assert result.metadata["alias"] == "<redacted>"
            assert result.metadata["public"] == "public value"
            assert _rows(manager, "artifacts/csvs")["content"].to_list() == [
                raw.encode()
            ]
        assert params["nested"]["supplied"] == raw
        assert raw not in caplog.text
        for document in Path(manager.config.delta_root).rglob("*.json"):
            assert raw not in document.read_text()
    assert manager.log_path is not None
    assert raw not in Path(manager.log_path).read_text()


def test_invalid_private_parameter_rejects_before_pending(tmp_path: Path) -> None:
    with _manager(tmp_path) as manager:
        params: dict[str, Any] = {
            "nested": {"supplied": "private-value"},
            "token_count": "invalid-credential-number",
        }
        with pytest.raises(ValueError):
            manager.submit(DurableCurator, params=params)
        assert not manager.executions
        assert _rows(manager, TablePath.STEPS).is_empty()
        assert params["nested"]["supplied"] == "private-value"


def test_dynamic_curator_marker_overlap_survives_cache_and_reopen(
    tmp_path: Path,
) -> None:
    params = {"nested": {"supplied": "red"}}
    with _manager(tmp_path) as manager:
        first = manager.run(
            DurableCurator,
            params=params,
            step_runner=NativePipelineRunner(manager.committer),
        )
        assert first.status == StepStatus.SUCCEEDED
        assert first.metadata["alias"] == "<redacted>"
    with PipelineManager.resume(
        manager.config.delta_root,
        manager.config.staging_root,
        storage=manager.config.storage,
        preserve_staging=True,
    ) as resumed:
        cached = resumed.run(
            DurableCurator,
            params=params,
            step_runner=NativePipelineRunner(resumed.committer),
        )
        assert cached.status == StepStatus.SUCCEEDED
        assert cached.disposition == StepDisposition.CACHE_HIT
        assert cached.metadata["alias"] == "<redacted>"
        assert _rows(resumed, TablePath.EXECUTIONS).height == 1
        assert _rows(resumed, TablePath.CACHE_REUSE).height == 1
        assert cached.output_roles == first.output_roles == frozenset({"actual"})
    with _manager(tmp_path) as fresh:
        cached_again = fresh.run(
            DurableCurator,
            params=params,
            step_runner=NativePipelineRunner(fresh.committer),
        )
        assert cached_again.disposition == StepDisposition.CACHE_HIT
        assert cached_again.metadata["alias"] == "<redacted>"
        assert _rows(fresh, TablePath.EXECUTIONS).height == 1
        assert _rows(fresh, TablePath.CACHE_REUSE).height == 2
    with PipelineManager.resume(
        fresh.config.delta_root,
        fresh.config.staging_root,
        pipeline_run_id=fresh.config.pipeline_run_id,
        storage=fresh.config.storage,
    ) as reopened:
        assert reopened[-1].metadata["alias"] == "<redacted>"


def test_rejected_input_uri_remains_redacted_in_pending_and_failure(
    tmp_path: Path,
) -> None:
    with _manager(tmp_path) as manager:
        result = manager.run(
            IngestCSV,
            inputs=["https://:durable-uri-password@host.invalid/input.csv"],
            step_runner=NativePipelineRunner(manager.committer),
        )
        assert result.status == StepStatus.FAILED
        assert "durable-uri-password" not in str(
            _rows(manager, TablePath.STEPS).to_dicts()
        )


def test_private_curator_parameter_digest_preserves_typed_secret_values() -> None:
    from artisan.execution.curator_definition import capture_curator_definition
    from artisan.orchestration.engine.pipeline_submission import freeze_submission

    snapshots = []
    for value in ("private-first", "private-second"):
        submission = freeze_submission(
            DurableCurator, params={"nested": {"supplied": value}}
        )
        restored = submission.instantiate()
        assert restored.params.nested.value.get_secret_value() == value
        contract = capture_curator_definition(restored)
        assert value not in contract.model_dump_json()
        assert value not in repr(submission)
        snapshots.append(contract.params_digest)
    assert snapshots[0] != snapshots[1]


def test_scientific_secret_parameters_remain_rejected_without_disclosure() -> None:
    from fixtures.composition_operations import Transform

    from artisan.composites.base.compiler import compile_definition
    from artisan.orchestration.engine.pipeline_submission import freeze_submission

    class PrivateCreator(Transform):
        name = "private_scientific_rejected"

        class Params(BaseModel):
            """Private fixture parameters.

            Attributes:
                credential: Typed secret which cannot enter a scientific recipe.
            """

            credential: SecretStr

        params: Params

    for prepare in (compile_definition, freeze_submission):
        with pytest.raises(ValueError) as error:
            prepare(PrivateCreator, params={"credential": "private-scientific-value"})
        assert "private-scientific-value" not in str(error.value)
