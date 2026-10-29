"""Secret-safe initial attempts, execution seals, cache reuse, and recovery."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from pydantic import BaseModel, Field, SecretStr

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.ingest_csv import IngestCSV
from artisan.orchestration.pipeline_manager import (
    PipelineManager,
    _serialize_input_refs,
)
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage.core.committed_scan import read_committed


class NestedSecret(BaseModel):
    value: SecretStr = Field(validation_alias="supplied")


class DurableCurator(OperationDefinition):
    name = "durable_configuration_curator"

    class Params(BaseModel):
        nested: NestedSecret = Field(description="Nested typed input.")
        token_count: int = Field(default=2, description="Credential scalar.")
        secret_flag: bool = Field(default=True, description="Credential Boolean.")
        label: str = Field(default="public value", description="Public diagnostic.")
        fail: bool = Field(default=False, description="Fail execution.")

    params: Params

    def execute_curator(self, inputs, step_number, artifact_store):
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
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
        preserve_staging=True,
        default_step_runner=LocalRunner(default_max_workers=2),
    )


def _rows(manager: PipelineManager, table: TablePath) -> pl.DataFrame:
    return read_committed(
        manager.config.delta_root, table, fs=manager.config.storage.filesystem()
    )


@pytest.mark.parametrize("outcome", ["success", "failure", "invalid", "skip", "cancel"])
def test_initial_pending_row_and_every_terminal_copy_are_safe(
    tmp_path, monkeypatch, outcome
):
    manager = _manager(tmp_path)
    raw = "durable-nested-plaintext"
    env_secret = "durable-nested-environment"
    params: dict[str, Any] = {"nested": {"supplied": raw}, "fail": outcome == "failure"}
    if outcome == "invalid":
        params["token_count"] = "invalid-credential-number"
    if outcome == "skip":
        manager._stopped = True
    if outcome == "cancel":
        manager.cancel()
    create = manager._step_tracker.create_attempt
    observed = []
    resource_secret = "durable-runner-extra-secret"

    def create_attempt(record):
        create(record)
        rows = pl.read_delta(tmp_path / "delta" / "orchestration" / "steps")
        pending = rows.filter(pl.col("step_run_id") == record.step_run_id)
        assert pending["status"].to_list() == ["pending"]
        assert raw not in str(pending.to_dicts())
        assert env_secret not in str(pending.to_dicts())
        assert resource_secret not in str(pending.to_dicts())
        assert "invalid-credential-number" not in str(pending.to_dicts())
        options = json.loads(pending["compute_options_json"][0])
        assert options["dynamic_output_roles"] is True
        assert options["application_modules"] == []
        assert options["pipeline_default_local_runner"]["default_max_workers"] == 2
        assert options["environment"]["local"]["env"]["PUBLIC"] == "/public/root"
        observed.append(record.params_json)

    monkeypatch.setattr(manager._step_tracker, "create_attempt", create_attempt)
    result = manager.run(
        DurableCurator,
        params=params,
        environment={
            "local": LocalEnvironmentSpec(
                env={"HIDDEN": env_secret, "PUBLIC": "/public/root"},
                public_env=("PUBLIC",),
            )
        },
        runner_resources={"extra": {"api_token": resource_secret}},
        compact=False,
    )
    assert (
        result.status
        is {
            "success": StepStatus.SUCCEEDED,
            "failure": StepStatus.FAILED,
            "invalid": StepStatus.FAILED,
            "skip": StepStatus.SKIPPED,
            "cancel": StepStatus.CANCELLED,
        }[outcome]
    )
    rows = _rows(manager, TablePath.STEPS)
    assert len(observed) == 1
    assert rows["params_json"].unique().to_list() == observed
    for secret in (raw, env_secret, resource_secret, "invalid-credential-number"):
        assert secret not in str(rows.to_dicts())
        assert secret not in str(_rows(manager, TablePath.EXECUTIONS).to_dicts())
    for seal in (tmp_path / "staging").rglob("executions.parquet"):
        assert raw not in str(pl.read_parquet(seal).to_dicts())
    if outcome == "success":
        assert result.metadata["alias"] == "<redacted>"
        assert result.metadata["public"] == "public value"
        assert _rows(manager, "artifacts/csvs")["content"].to_list() == [raw.encode()]
    assert params["nested"]["supplied"] == raw
    manager.finalize()


def test_dynamic_curator_marker_overlap_survives_cache_and_reopen(tmp_path):
    params = {"nested": {"supplied": "red"}}
    manager = _manager(tmp_path)
    first = manager.run(DurableCurator, params=params, compact=False)
    assert first.status is StepStatus.SUCCEEDED
    assert first.metadata["alias"] == "<redacted>"
    manager.finalize()
    resumed = PipelineManager.resume(
        str(tmp_path / "delta"), str(tmp_path / "staging"), preserve_staging=True
    )
    cached = resumed.run(DurableCurator, params=params, compact=False)
    assert cached.status is StepStatus.SUCCEEDED
    assert cached.metadata["alias"] == "<redacted>"
    assert _rows(resumed, TablePath.EXECUTIONS).height == 1
    assert _rows(resumed, TablePath.CACHE_REUSE).height == 1
    assert cached.output_roles == first.output_roles == frozenset({"actual"})
    resumed.finalize()
    fresh = _manager(tmp_path)
    whole_cached = fresh.run(DurableCurator, params=params, compact=False)
    assert whole_cached.status is StepStatus.SUCCEEDED
    assert whole_cached.metadata["alias"] == "<redacted>"
    assert _rows(fresh, TablePath.EXECUTIONS).height == 1
    assert _rows(fresh, TablePath.CACHE_REUSE).height == 2
    fresh.finalize()
    reopened = PipelineManager.resume(
        str(tmp_path / "delta"), str(tmp_path / "staging")
    )
    assert reopened[-1].metadata["alias"] == "<redacted>"
    reopened.finalize()


def test_rejected_input_uri_is_safe_and_graph_references_are_exact(
    tmp_path, monkeypatch
):
    manager = _manager(tmp_path)
    uri = "https://:durable-uri-password@host.invalid/input.csv"
    from unittest.mock import Mock

    fs = Mock()
    fs.exists.return_value = False
    monkeypatch.setattr("artisan.schemas.execution.fs.resolve_fs", lambda *_: (fs, uri))
    result = manager.run(IngestCSV, inputs=[uri], compact=False)
    assert result.status is StepStatus.FAILED
    assert "durable-uri-password" not in str(_rows(manager, TablePath.STEPS).to_dicts())
    reference = OutputReference(source_step=2, role="data", artifact_type="csv")
    assert json.loads(_serialize_input_refs({"data": reference})) == {
        "data": {
            "type": "output_ref",
            "source_step": 2,
            "role": "data",
            "artifact_type": "csv",
        }
    }
    manager.finalize()
