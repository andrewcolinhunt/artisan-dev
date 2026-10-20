# Export Pipeline Results

Save accepted data and metrics from a selected run, or materialize known artifact
IDs as files. These recipes assume a completed run in `delta_root`. For custom artifact
types, [load the application's trusted definitions](configuring-execution.md#load-application-definitions-in-every-process)
before opening its store.

## Choose a run and export a step

Use the same run ID for selection, inspection, and export. See
[run selection](inspecting-provenance.md#select-the-run-you-want-to-inspect) for
how accepted step outputs differ from artifact origin.

```python
from pathlib import Path

from artisan.orchestration import list_runs
from artisan.visualization import inspect_data, inspect_pipeline

delta_root = "runs/delta"
runs = list_runs(delta_root)
run_id = runs.sort("started_at", descending=True)["pipeline_run_id"][0]
print(inspect_pipeline(delta_root, pipeline_run_id=run_id))

output_dir = Path("exported")
output_dir.mkdir(parents=True, exist_ok=True)

# Choose a data-producing step from the overview.
step_number = 0
data = inspect_data(delta_root, step_number=step_number, pipeline_run_id=run_id)
data.write_csv(output_dir / f"step_{step_number}.csv")
```

The CSV combines accepted datasets from that step and retains `_source` to
identify their original names. It includes cached and passthrough outputs
accepted at that logical step, even when their content originated elsewhere.
`inspect_data` raises if no matching data artifacts exist. Run-scoped selection
uses the newest attempt at each logical step; a newer failed attempt does not
fall back to older successful outputs. To export a historical attempt, pass its
exact `step_run_id` to `inspect_data` or `inspect_metrics`. Any run or step
filters you also provide must agree with that attempt.

## Export metrics

Metric keys become columns, with nested keys flattened into names such as
`distribution.median`:

```python
from artisan.visualization import inspect_metrics

metrics = inspect_metrics(delta_root, pipeline_run_id=run_id)
metrics.write_parquet(output_dir / "metrics.parquet")
```

Add `step_number` to select one logical step. This display-oriented reader rounds
floats; choose `round_digits` for the precision you need. Load metric artifacts
directly when you need their original payloads.

## Assess recorded metrics without display rounding

Use `assess_outcomes` when a threshold decision needs the original numbers and
exact artifact references. Select one run, step and output role. Supply the
step number if that run uses the same name at several positions.

The following continues the selected-store example above. Replace the step,
role and criterion with those declared by your application:

```python
from artisan.visualization import (
    AssessmentCheck,
    AssessmentSpec,
    OutcomeSelection,
    assess_outcomes,
)

selection = OutcomeSelection(
    pipeline_run_id=run_id,
    step_name="measure",
    step_number=1,
    role="metrics",
)
criteria = AssessmentSpec(
    label="Recorded score at least 0.8",
    checks=[
        AssessmentCheck(
            check_id="minimum_score",
            metric_path=["score"],
            operator="ge",
            threshold=0.8,
        )
    ],
)
assessment = assess_outcomes(delta_root, selection, criteria)
print(assessment.assessment_status)
(output_dir / "assessment.json").write_text(
    assessment.model_dump_json(indent=2), encoding="utf-8"
)
```

Check the report's execution status and coverage alongside its assessment.
Missing or invalid required measurements make an assessment incomplete, even
when all evaluated values pass. An empty population remains unassessed. A
count-only criterion tests the number of accepted artifacts; it does not prove
that the intended upstream cases produced them.

Each observation is a unique accepted artifact in that attempt and role.
Identical outputs can share one artifact identity. Cached and passthrough
outputs retain their original artifact origin while belonging to the selected
attempt. Partial execution can pass over its accepted outputs without showing
that the whole workflow succeeded.

The report returns the resolved attempt ID. Supply that `step_run_id` in
`OutcomeSelection` to inspect the same historical attempt later. A fresh
selection without it uses the current attempt, including a failed retry with
no available population. Recorded cancellation uncertainty remains uncertainty;
unavailable outputs do not prove that external work stopped.

Save the normalized criteria and report together. Their fingerprints detect
changes but cannot restore deleted source records or supply an atomic store
snapshot. For a read-only observation export without a criterion, use the
public `read_metric_observations` reader with explicit metric paths and metadata
keys. It preserves JSON integers and unrounded decoded floats; clients handling
large integers must parse them without losing precision.

## Compare the same declared cases

Choose both runs explicitly and supply correspondence. This example continues
the assessment example, using a second run ID supplied by you. It assumes each
metric artifact has a `case` metadata key identifying the measured subject:

```python
from artisan.visualization import (
    ComparisonMetric,
    ComparisonSpec,
    MetadataKeyMatch,
    compare_runs,
)

candidate = OutcomeSelection(
    pipeline_run_id="<candidate run ID>",
    step_name="measure",
    step_number=1,
    role="metrics",
)
comparison = compare_runs(
    delta_root,
    baseline=selection,
    candidate=candidate,
    spec=ComparisonSpec(
        label="Score change for matching cases",
        matching=MetadataKeyMatch(key="case"),
        metrics=[
            ComparisonMetric(
                metric_id="score",
                metric_path=["score"],
                direction="higher",
                measurement_id="score",
                measurement_version="1",
                unit="unitless",
                absolute_tolerance=0,
            )
        ],
        assessment=criteria,
    ),
)
(output_dir / "comparison.json").write_text(
    comparison.model_dump_json(indent=2), encoding="utf-8"
)
```

Verify that the measurement declaration is appropriate for both runs. Artisan
checks stored identity and exact matching keys; your declaration supplies the
meaning, version and unit. Integer and string keys remain distinct. Duplicate
keys make correspondence ambiguous and suppress numeric comparison rather than
choosing a representative. When no shared key exists, use `ExplicitPairsMatch`
with known artifact IDs and unique case labels.

Read pair coverage, unmatched cases and regressions before interpreting a mean
delta. A better mean can contain regressions. Missing numeric values remain
incomparable pairs, with separate numeric coverage. The optional assessment
uses each complete selected role, including its unmatched observations.
Neither matching labels nor equal redacted diagnostics establish that the
runs had identical inputs or that a configuration change caused the result.

These readers load the complete selected populations. MCP offers bounded
summary and detail views for the same reports; see
[connecting MCP](connecting-mcp.md) for paging and error handling.

## Materialize an artifact by ID

When you already have artifact IDs, use `ArtifactStore` to load the objects and
write their content. IDs can come from recorded operation inputs, provenance
queries, or the read-only [MCP query tools](connecting-mcp.md).

```python
from artisan.storage import ArtifactStore

store = ArtifactStore(delta_root)
artifact = store.get_artifact("<artifact ID>", artifact_type="data")
path = artifact.materialize_to(str(output_dir))
```

The type hint avoids a separate type lookup. Omit it when the type is unknown.
The store loads content by default; metadata-only objects cannot materialize
embedded content until hydrated.

For several known IDs:

```python
artifacts = store.get_artifacts_by_type(artifact_ids, artifact_type="data")
for artifact in artifacts.values():
    artifact.materialize_to(str(output_dir))
```

`store.provenance.load_artifact_ids_by_type(..., step_numbers=[...])` filters
**first-origin steps across runs**. It is useful for origin analysis, but does
not select the accepted outputs of a run's cached or passthrough step. Use the
run-scoped table export above when you want everything that step accepted.

## Inspect physical storage when diagnosing a store

A raw Delta read bypasses Artisan's logical-completion filtering and integrity
checks. It can reveal rows from incomplete or abandoned commits. Use it to
investigate physical storage, not to determine which results a run accepted.

```python
import polars as pl
from artisan.schemas import TablePath

physical_rows = pl.read_delta(f"{delta_root}/{TablePath.ARTIFACT_INDEX.value}")
```

A manual join with a completion table is not a substitute for the supported
readers' ownership and integrity checks. See
[Storage and Delta Lake](../concepts/storage-and-delta-lake.md) for logical
commits, and [Debug a Recorded Execution](debugging-executions.md) for execution
diagnostics.

## Verify the export

Check the written table against the selected data:

```python
import polars as pl

exported = pl.read_csv(output_dir / f"step_{step_number}.csv")
assert exported.height == data.height
assert "_source" in exported.columns
```

CSV consumers can infer different data types on import. Use Parquet when you
need to preserve the DataFrame schema.

## Related guides

- [Inspect Pipeline Results and Provenance](inspecting-provenance.md) — run
  selection, lineage, and timing.
- [Exploring Results](../tutorials/01-getting-started/02-exploring-results.ipynb) —
  a runnable introduction to inspection.
- [Python API](../reference/python-api.md) — public reader and store entry points.
