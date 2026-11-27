"""Public inspection selects exact retained metadata and optional observations."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import arro3.core as ac
import polars as pl
import pytest
from deltalake import write_deltalake
from fixtures.candidate_outputs import prepare_scientific
from fixtures.inspection_curator import commit_curator_record
from fixtures.retained_cache_isolation_store import build_cache_isolation_store
from fixtures.retained_inspection import commit_inspection_attempt, inspection_store
from fixtures.retained_results import retained_case
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import ArtisanError, StoreIntegrityError
from artisan.schemas import CSVArtifact, FileRefArtifact, MetricArtifact
from artisan.schemas.enums import CachePolicy, TablePath
from artisan.schemas.execution.assignment import DeliveryIdentity
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.storage.io.retained_store import open_configured_store
from artisan.storage.io.step_result_plan import StepUnitFailure
from artisan.utils.hashing import compute_content_digest
from artisan.utils.log_paths import worker_log_path
from artisan.visualization.inspect import (
    _build_details,
    inspect_commands,
    inspect_csv,
    inspect_failures,
    inspect_metrics,
    inspect_pipeline,
    inspect_step,
    inspect_worker_log,
)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    root = tmp_path / "delta"
    inspection_store(root)
    return root


def _data(value: int, *, step: int = 0, name: str = "data.csv") -> CSVArtifact:
    return CSVArtifact.draft(f"value\n{value}\n".encode(), name, step).finalize()


def _metric(
    values: dict[str, object], *, step: int = 0, name: str = "metric.json"
) -> MetricArtifact:
    return MetricArtifact.draft(values, name, step).finalize()


def test_public_failures_preserve_whole_partial_reuse_and_global_original_identity(
    root: Path, tmp_path: Path
) -> None:
    committer = inspection_store(root)
    case = retained_case(tmp_path, kind="alias")
    source, admission, receipts = prepare_scientific(committer, case, count=3)
    committer.accept(receipts[0])
    failed = DeliveryIdentity.new(admission.assignments[1].attempt)
    source.finish(
        admission.step_run_id,
        failures={
            1: StepUnitFailure(error="scientific failure", failed_delivery=failed),
            2: StepUnitFailure(error="infrastructure without a delivery"),
        },
    )
    original = inspect_failures(root, pipeline_run_id="synthetic-pipeline")
    current, selected, _ = prepare_scientific(
        committer, case, step_run_id="b" * 32, count=3
    )
    hit = current.lookup(selected.step_run_id, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    current.reuse(hit)
    assert inspect_failures(root, pipeline_run_id="synthetic-pipeline").equals(original)
    assert inspect_failures(root).equals(original)
    assert original["execution_run_id"].to_list() == [failed.delivery_id]
    assert original["timestamp_start"].to_list() == [None]
    assert original["log"].to_list() == [None]


@pytest.mark.parametrize(
    "status",
    ["succeeded", "partial", "pending", "running", "failed", "cancelled", "skipped"],
)
def test_inspection_distinguishes_current_history_and_origin(
    root: Path, status: str
) -> None:
    old, _ = commit_inspection_attempt(
        root, outputs={"output": [_data(1), _metric({"value": 1})]}
    )
    usable = status in {"succeeded", "partial"}
    current, _ = commit_inspection_attempt(
        root,
        status=status,
        outputs={"output": [_data(2), _metric({"value": 2})] if usable else []},
    )
    assert inspect_step(root, 0, step_run_id=old).height == 2
    assert inspect_metrics(root, step_run_id=old)["value"].to_list() == [1]
    assert inspect_csv(root, step_run_id=old)["value"].to_list() == [1]
    selected = inspect_step(root, 0, pipeline_run_id="source-run")
    assert selected.height == (2 if usable else 0)
    assert selected.sort("artifact_type").equals(
        inspect_step(root, 0, step_run_id=current).sort("artifact_type")
    )
    metrics = inspect_metrics(root, pipeline_run_id="source-run")
    assert metrics.height == int(usable)
    if usable:
        assert metrics["value"].to_list() == [2]
    if usable:
        assert inspect_csv(root, pipeline_run_id="source-run")["value"].to_list() == [2]
    else:
        with pytest.raises(ValueError, match="No matching"):
            inspect_csv(root, pipeline_run_id="source-run")
    assert inspect_step(root, 0).height == (4 if usable else 2)
    assert sorted(inspect_csv(root)["value"].to_list()) == ([1, 2] if usable else [1])


@pytest.mark.parametrize("reader", [inspect_step, inspect_metrics, inspect_csv])
@pytest.mark.parametrize("selection", ["unknown", "wrong_run", "wrong_step"])
def test_inspection_rejects_inconsistent_explicit_attempt(
    root: Path, reader, selection: str
) -> None:
    attempt, _ = commit_inspection_attempt(root)
    kwargs = {"step_number": 0, "step_run_id": attempt}
    if selection == "unknown":
        kwargs["step_run_id"] = "a" * 32
    elif selection == "wrong_run":
        kwargs["pipeline_run_id"] = "other"
    else:
        kwargs["step_number"] = 1
    with pytest.raises(ValueError, match="attempt"):
        reader(root, **kwargs)


def test_inspect_pipeline_basic_and_distinct_role_counts(root: Path) -> None:
    data = [_data(value, name=f"data-{value}.csv") for value in range(3)]
    commit_inspection_attempt(
        root, name="data_generator", outputs={"data": data, "same": [data[0], data[0]]}
    )
    commit_inspection_attempt(
        root,
        number=1,
        name="metric_calculator",
        outputs={
            "metrics": [
                _metric({"score": value}, step=1, name=f"m-{value}.json")
                for value in range(3)
            ]
        },
    )
    result = inspect_pipeline(root)
    assert result.columns == ["step", "operation", "status", "produced", "duration"]
    assert result["produced"].to_list() == ["3 csv", "3 metric"]
    assert result["operation"].to_list() == ["data_generator", "metric_calculator"]


@pytest.mark.parametrize(
    "status",
    ["succeeded", "partial", "pending", "running", "failed", "cancelled", "skipped"],
)
def test_inspect_pipeline_preserves_authoritative_status(
    root: Path, status: str
) -> None:
    commit_inspection_attempt(root, name="filter", status=status)
    row = inspect_pipeline(root).row(0, named=True)
    assert row["status"] == status
    assert row["produced"] == (
        "1 passed" if status in {"succeeded", "partial"} else "-"
    )
    assert row["duration"] == ("1.0s" if status in {"succeeded", "partial"} else "-")


def test_initialized_empty_inspection_has_typed_empty_results(root: Path) -> None:
    assert inspect_pipeline(root).is_empty()
    assert inspect_failures(root).is_empty()
    assert inspect_step(root, 0).is_empty()
    assert inspect_metrics(root).is_empty()
    with pytest.raises(ValueError, match="No matching"):
        inspect_csv(root)


def test_inspect_failures_structured_and_plain_curator_records(root: Path) -> None:
    envelope = ArtisanError(
        "op_execute_failed",
        "boom",
        error_type="compute",
        recovery_hint="REPORT_TO_USER",
        field="params.scale",
        suggestions=["scale_factor"],
    ).to_dict()
    structured = commit_curator_record(
        root, number=1, error="boom traceback", envelope=envelope
    )
    plain = commit_curator_record(root, number=2, error="ValueError: bad")
    commit_curator_record(root, number=3, success=True)
    result = inspect_failures(root)
    assert result.height == 2
    row = result.filter(pl.col("execution_run_id") == structured).row(0, named=True)
    assert (
        row["step"],
        row["code"],
        row["recovery_hint"],
        row["field"],
        row["suggestions"],
        row["error"],
    ) == (
        1,
        "op_execute_failed",
        "REPORT_TO_USER",
        "params.scale",
        ["scale_factor"],
        "boom traceback",
    )
    assert row["log"] is None
    row = result.filter(pl.col("execution_run_id") == plain).row(0, named=True)
    assert all(
        row[key] is None for key in ("code", "recovery_hint", "field", "suggestions")
    )
    assert row["error"] == "ValueError: bad"


def test_failure_history_excludes_diagnostics_and_current_retry_masks_prior(
    root: Path,
) -> None:
    _, original = commit_inspection_attempt(
        root, status="failed", error="original failure"
    )
    commit_inspection_attempt(root, status="pending")
    commit_inspection_attempt(
        root, run_id="diagnostic", status="failed", replay_of="a" * 32
    )
    assert inspect_failures(root, pipeline_run_id="source-run").is_empty()
    rows = inspect_failures(root)
    assert rows["execution_run_id"].to_list() == [original]
    assert rows["timestamp_start"].to_list() == [None]
    assert rows["log"].to_list() == [None]


def test_infrastructure_failure_does_not_invent_execution(root: Path) -> None:
    commit_inspection_attempt(root, status="failed", failed_delivery=False)
    assert inspect_pipeline(root)["status"].to_list() == ["failed"]
    assert inspect_failures(root).is_empty()


def test_inspect_failures_pipeline_run_id_filter(root: Path) -> None:
    one = commit_curator_record(root, run_id="one")
    commit_curator_record(root, run_id="other")
    assert inspect_failures(root, pipeline_run_id="one")[
        "execution_run_id"
    ].to_list() == [one]


def test_inspect_step_content_metadata_and_metrics(root: Path) -> None:
    commit_inspection_attempt(
        root,
        outputs={
            "data": [_data(3)],
            "metric": [_metric({"score": 0.123456, "nested": {"energy": -1.23456}})],
        },
    )
    rows = inspect_step(root, 0).sort("artifact_type")
    assert rows["artifact_type"].to_list() == ["csv", "metric"]
    assert rows["name"].to_list() == ["data", "metric"]
    assert "1 rows" in rows["details"][0]
    assert "score" in rows["details"][1]
    metric = inspect_metrics(root, round_digits=4).row(0, named=True)
    assert metric["score"] == 0.1235
    assert metric["nested.energy"] == -1.2346
    assert inspect_metrics(root, 9).is_empty()


def test_inspect_csv_name_step_and_concatenation(root: Path) -> None:
    commit_inspection_attempt(
        root, outputs={"data": [_data(1, name="one.csv"), _data(2, name="two.csv")]}
    )
    commit_inspection_attempt(
        root, number=1, outputs={"data": [_data(3, step=1, name="three.csv")]}
    )
    assert inspect_csv(root, name="one")["value"].to_list() == [1]
    assert inspect_csv(root, step_number=1)["value"].to_list() == [3]
    assert sorted(inspect_csv(root, step_number=0)["value"].to_list()) == [1, 2]
    with pytest.raises(ValueError, match="No matching"):
        inspect_csv(root, name="missing")


def test_inspect_pipeline_forwards_exact_storage_binding(root: Path) -> None:
    committer = inspection_store(root)
    fs = LocalFileSystem()
    options = {"AWS_REGION": "synthetic-region"}
    with patch(
        "artisan.visualization.inspect.open_configured_store", return_value=committer
    ) as opener:
        inspect_pipeline(
            root, files_root=committer.files_root, fs=fs, storage_options=options
        )
    opener.assert_called_once_with(
        root, files_root=committer.files_root, fs=fs, storage_options=options
    )


def test_run_scoped_inspection_projects_cached_outputs_without_cross_run_leaks(
    tmp_path: Path,
) -> None:
    store = build_cache_isolation_store(tmp_path)
    pipeline = inspect_pipeline(store.root, pipeline_run_id=store.current_run)
    assert pipeline["step"].to_list() == [0, 5]
    assert pipeline["produced"].to_list() == ["1 csv", "1 metric"]
    assert inspect_step(store.root, 5, pipeline_run_id=store.current_run)[
        "name"
    ].to_list() == ["source_metric"]
    assert inspect_metrics(store.root, 5, pipeline_run_id=store.current_run).select(
        "step", "score"
    ).row(0) == (5, 0.9)
    assert inspect_csv(
        store.root, name="shared", step_number=0, pipeline_run_id=store.current_run
    )["value"].to_list() == [1]
    assert inspect_pipeline(store.root)["operation"].to_list() == [
        "other_data",
        "other_metric",
    ]
    commit_inspection_attempt(
        store.root,
        run_id=store.current_run,
        number=6,
        reused_execution_id=store.source_metric_execution,
    )
    assert inspect_metrics(store.root, pipeline_run_id=store.current_run).select(
        "step", "score"
    ).rows() == [(5, 0.9), (6, 0.9)]


@pytest.mark.parametrize("success", [True, False])
def test_inspect_commands_reads_exact_curator_record(root: Path, success: bool) -> None:
    execution = commit_curator_record(root, success=success)
    value = inspect_commands(root, execution)
    assert value["execution_run_id"] == execution
    assert value["status"] == "complete"
    assert value["recording"]["commands"] == []
    assert value["invocations"] == []
    assert value["omitted_invocations"] == 0


def test_inspect_worker_log_prefers_separate_log_then_exact_embedded(
    root: Path,
) -> None:
    one = commit_curator_record(root, worker_log="embedded one")
    two = commit_curator_record(root, number=1, worker_log="embedded two")
    assert inspect_worker_log(root, one) == "embedded one"
    publish_immutable_bytes(
        LocalFileSystem(), worker_log_path(str(root), one), b"provider one"
    )
    assert inspect_worker_log(root, one) == "provider one"
    assert inspect_worker_log(root, two) == "embedded two"


@pytest.mark.parametrize("raw", [None, "{}", '{"status":"complete"}', "not-json"])
def test_inspect_commands_rejects_malformed_selected_evidence(
    root: Path, raw: str | None
) -> None:
    execution = commit_curator_record(root)
    path = root / TablePath.EXECUTIONS.value
    rows = pl.read_delta(str(path)).with_columns(
        pl.lit(raw, dtype=pl.String).alias("command_recording")
    )
    write_deltalake(str(path), ac.Table.from_arrow(rows), mode="overwrite")
    with pytest.raises(StoreIntegrityError):
        inspect_commands(root, execution)


def test_inspect_commands_does_not_read_unclaimed_staging(root: Path) -> None:
    staging = root / "staging"
    staging.mkdir()
    pl.DataFrame({"execution_run_id": ["a" * 32]}).write_parquet(
        staging / "executions.parquet"
    )
    with pytest.raises(FileNotFoundError):
        inspect_commands(root, "a" * 32)


def test_inspect_failures_orders_source_times_then_identity(root: Path) -> None:
    start = datetime(2026, 9, 19, tzinfo=UTC)
    ties = [
        commit_curator_record(root, number=index, timestamp=start) for index in (0, 1)
    ]
    newer = commit_curator_record(
        root, number=2, timestamp=start + timedelta(seconds=1)
    )
    older = commit_curator_record(
        root, number=3, timestamp=start - timedelta(seconds=1)
    )
    rows = inspect_failures(root)
    assert rows["execution_run_id"].to_list() == [older, *sorted(ties), newer]
    assert rows.schema["timestamp_start"] == pl.Datetime("us", "UTC")


def test_metadata_inspection_does_not_read_external_carriers(
    root: Path, tmp_path: Path
) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"original bytes")
    artifact = FileRefArtifact.draft(
        str(path), compute_content_digest(path.read_bytes()), path.stat().st_size, 0
    ).finalize()
    commit_inspection_attempt(root, outputs={"file": [artifact]})
    committer = open_configured_store(str(root))
    for carrier in Path(committer.files_root).rglob("*"):
        if carrier.is_file():
            carrier.unlink()
    assert inspect_step(root, 0)["artifact_type"].to_list() == ["file_ref"]
    assert inspect_pipeline(root)["produced"].to_list() == ["1 file_ref"]


def test_build_details_data_rows_and_cols() -> None:
    row = {"row_count": 10, "columns": json.dumps(["x", "y", "z"])}
    assert _build_details("csv", row) == "10 rows, 3 cols"


def test_build_details_data_size_fallback() -> None:
    row = {"row_count": None, "columns": None, "size_bytes": 1_500_000}
    assert _build_details("csv", row) == "1.5 MB"


def test_build_details_data_no_info() -> None:
    row = {"row_count": None, "columns": None, "size_bytes": None}
    assert _build_details("csv", row) == "-"


def test_build_details_file_ref_size() -> None:
    row = {"size_bytes": 2_500}
    assert _build_details("file_ref", row) == "2.5 KB"


def test_build_details_metric_keys() -> None:
    content = json.dumps({"mean": 0.5, "std": 0.1}).encode("utf-8")
    row = {"content": content}
    assert _build_details("metric", row) == "mean, std"


def test_build_details_config_params() -> None:
    content = json.dumps({"lr": 0.01, "epochs": 10, "batch": 32}).encode("utf-8")
    row = {"content": content}
    assert _build_details("config", row) == "3 params"


def test_build_details_unknown_type() -> None:
    assert _build_details("unknown", {}) == "-"
