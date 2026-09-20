# Export Pipeline Results

Save accepted data and metrics from a selected run, or materialize known artifact
IDs as files. These recipes assume a completed run in `delta_root`.

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
`inspect_data` raises if no matching data artifacts exist.

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

physical_rows = pl.read_delta(f"{delta_root}/{TablePath.ARTIFACT_INDEX}")
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
