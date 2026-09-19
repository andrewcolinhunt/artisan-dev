"""Run-scoped cross-pipeline import and typed hydration tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import Mock

import polars as pl
import pytest
from fixtures import run_outputs
from fixtures.run_outputs import commit_outputs
from pydantic import ValidationError

from artisan.errors import ArtifactIntegrityError
from artisan.operations.curator.ingest_pipeline_step import IngestPipelineStep
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.curator_result import ArtifactResult


def _operation(root: Path, **params: object) -> IngestPipelineStep:
    """Build a source-run import with optional selection overrides."""
    return IngestPipelineStep(
        params={
            "source_delta_root": str(root),
            "source_run_id": "source-run",
            "source_step": 0,
            **params,
        }
    )


def _execute(operation: IngestPipelineStep) -> ArtifactResult:
    """Execute at a destination position distinct from source origins."""
    return operation.execute_curator({}, 7, Mock())


def _data(content: bytes, step_number: int = 0, **metadata: object) -> DataArtifact:
    """Create a finalized source artifact."""
    return DataArtifact.draft(
        content=content,
        original_name="source.csv",
        step_number=step_number,
        metadata=metadata,
    ).finalize()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_run_id", ""),
        ("source_run_id", " \t"),
        ("source_step", -1),
    ],
)
def test_params_reject_invalid_selection(
    tmp_path: Path, field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        _operation(tmp_path, **{field: value})


def test_params_require_run_and_trim_run_id(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="source_run_id"):
        IngestPipelineStep.Params(source_delta_root=str(tmp_path), source_step=0)
    operation = _operation(tmp_path, source_run_id=" source-public-id \n")
    assert operation.params.source_run_id == "source-public-id"
    assert operation.params.include_prior_steps is False
    assert operation.version == "2"
    assert operation.cacheable is False


@pytest.mark.parametrize(("include_prior", "expected"), [(False, 1), (True, 3)])
def test_ingest_selects_accepted_union_and_sorts_hydrated_ids(
    tmp_path: Path,
    include_prior: bool,
    expected: int,
) -> None:
    root = tmp_path / "source"
    artifacts = sorted(
        [_data(b"a"), _data(b"b"), _data(b"c")],
        key=lambda artifact: artifact.artifact_id,
        reverse=True,
    )
    step0, _ = commit_outputs(str(root), artifacts=artifacts)
    step1, _ = commit_outputs(
        str(root),
        number=1,
        output_ids={
            "first": [artifacts[0].artifact_id],
            "second": [artifacts[0].artifact_id],
        },
    )
    commit_outputs(
        str(root),
        run_id="other-run",
        number=1,
        artifacts=[_data(b"other", step_number=1)],
    )
    result = _execute(
        _operation(root, source_step=1, include_prior_steps=include_prior)
    )
    imported = result.artifacts["data"]
    assert result.success
    assert len(imported) == expected
    source_ids = [
        artifact.metadata["imported_from"]["artifact_id"] for artifact in imported
    ]
    assert source_ids == sorted(source_ids)
    assert all(artifact.origin_step_number == 7 for artifact in imported)
    assert result.metadata["ingest_source"] == {
        "pipeline_run_id": "source-run",
        "source_step": 1,
        "include_prior_steps": include_prior,
        "step_run_ids": sorted([step0, step1]) if include_prior else [step1],
    }


@pytest.mark.parametrize(
    ("type_filter", "expected"), [(None, ["data", "metric"]), ("metric", ["metric"])]
)
def test_ingest_type_filter_limits_contributing_attempts(
    tmp_path: Path,
    type_filter: str | None,
    expected: list[str],
) -> None:
    data_step, _ = commit_outputs(str(tmp_path), artifacts=[_data(b"test")])
    metric = MetricArtifact.draft(
        content={"score": 2}, original_name="metric.json", step_number=1
    ).finalize()
    metric_step, _ = commit_outputs(str(tmp_path), number=1, artifacts=[metric])
    result = _execute(
        _operation(
            tmp_path, source_step=1, include_prior_steps=True, artifact_type=type_filter
        )
    )
    assert list(result.artifacts) == expected
    assert result.metadata["ingest_source"]["step_run_ids"] == (
        sorted([data_step, metric_step]) if type_filter is None else [metric_step]
    )
    assert result.artifacts["metric"][0].content == metric.content


def test_ingest_preserves_content_and_replaces_only_owned_metadata(
    tmp_path: Path,
) -> None:
    original = _data(
        b"content",
        annotation={"label": "keep"},
        imported_from_step=9,
        imported_from={"pipeline_run_id": "older-run"},
    )
    source_metadata = dict(original.metadata)
    commit_outputs(str(tmp_path), artifacts=[original])
    operation = _operation(tmp_path)
    imported = _execute(operation).artifacts["data"][0]
    assert imported.content == original.content
    assert imported.original_name == original.original_name
    assert imported.extension == original.extension
    assert imported.artifact_id != original.artifact_id
    assert imported.metadata == {
        "annotation": {"label": "keep"},
        "imported_from": {
            "pipeline_run_id": "source-run",
            "artifact_id": original.artifact_id,
            "origin_step_number": 0,
        },
    }
    assert original.metadata == source_metadata
    assert operation._to_draft(original, 7).artifact_id == imported.artifact_id
    assert operation._to_draft(original, 12).artifact_id == imported.artifact_id
    assert (
        _operation(tmp_path / "moved")._to_draft(original, 7).artifact_id
        == imported.artifact_id
    )
    assert original.metadata == source_metadata
    assert (
        _operation(tmp_path, source_run_id="different")
        ._to_draft(original, 7)
        .artifact_id
        != imported.artifact_id
    )


@pytest.mark.parametrize(
    ("params", "match"),
    [
        ({"source_run_id": "unknown-run"}, "Unknown source run 'unknown-run'"),
        ({"source_step": 99}, "has no step 99"),
    ],
)
def test_ingest_rejects_invalid_selection(
    tmp_path: Path, params: dict, match: str
) -> None:
    commit_outputs(str(tmp_path), artifacts=[_data(b"test")])
    with pytest.raises(ValueError, match=match):
        _execute(_operation(tmp_path, **params))


@pytest.mark.parametrize("empty_step", [False, True])
def test_ingest_empty_selection_has_run_boundary_and_type_context(
    tmp_path: Path, empty_step: bool
) -> None:
    commit_outputs(str(tmp_path), artifacts=[] if empty_step else [_data(b"test")])
    result = _execute(
        _operation(tmp_path, artifact_type="metric", include_prior_steps=True)
    )
    assert not result.success
    assert "source-run" in result.error
    assert "through step 0" in result.error
    assert "metric" in result.error


def test_ingest_missing_source_path_fails(tmp_path: Path) -> None:
    result = _execute(_operation(tmp_path / "absent"))
    assert not result.success
    assert "does not exist" in result.error


def test_ingest_missing_typed_content_fails_before_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts = [_data(b"one"), _data(b"two")]
    original = run_outputs.commit_test_step

    def omit_content(
        root: str,
        staging: str,
        steps: list[dict[str, object]],
        tables: dict[str, pl.DataFrame],
        **kwargs: Any,
    ) -> None:
        tables["artifacts/data"] = tables["artifacts/data"].filter(
            pl.col("artifact_id") != artifacts[0].artifact_id
        )
        original(root, staging, steps, tables, **kwargs)

    with monkeypatch.context() as context:
        context.setattr(run_outputs, "commit_test_step", omit_content)
        commit_outputs(str(tmp_path), artifacts=artifacts)
    with pytest.raises(ArtifactIntegrityError, match=artifacts[0].artifact_id):
        _execute(_operation(tmp_path))
