# Export Pipeline Results

How to extract pipeline outputs — artifacts, metrics, and execution metadata —
from Delta Lake tables for downstream analysis or file export.

**Prerequisites:** A completed pipeline run with data in `delta_root`. Familiarity
with [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) helps but is
not required.

---

## Minimal working example

```python
from pathlib import Path
from artisan.visualization import inspect_pipeline, inspect_metrics, inspect_data

delta_root = Path("runs/delta")

# Pipeline overview: one row per step
inspect_pipeline(delta_root)

# Metrics as a flat table with one column per metric key
inspect_metrics(delta_root)

# Read a data artifact's CSV content as a DataFrame
inspect_data(delta_root, name="dataset_00000")
```

---

## Quick overview with inspect helpers

Artisan provides read-only helpers (`inspect_pipeline`, `inspect_step`,
`inspect_metrics`, `inspect_data`) that present Delta Lake tables as clean
Polars DataFrames. Use them to quickly survey what a pipeline produced before
deciding what to export.

For full documentation of these helpers — column descriptions, filtering
options, and rounding control — see
[Inspect Pipeline Results and Provenance](inspecting-provenance.md).

---

## Retrieve and materialize artifacts

The inspect helpers return DataFrames. When you need the actual artifact objects
— for example, to write files to disk — use `ArtifactStore`:

```python
from artisan.storage import ArtifactStore

store = ArtifactStore(delta_root)
```

### Load a single artifact

```python
artifact = store.get_artifact("abc123...", artifact_type="data")
```

The `artifact_type` hint avoids an extra index lookup. If omitted, the store
resolves the type from the artifact index automatically.

### Load artifacts in bulk

```python
artifacts = store.get_artifacts_by_type(
    artifact_ids=["abc123...", "def456..."],
    artifact_type="data",
)
# Returns {artifact_id: DataArtifact, ...}
```

### Write artifact content to disk

Data, metric, and file reference artifacts support `materialize_to`, which
writes the artifact's content to a directory and returns the output path:

```python
from pathlib import Path

output_dir = Path("exported/")
output_dir.mkdir(exist_ok=True)

artifact = store.get_artifact("abc123...", artifact_type="data")
path = artifact.materialize_to(output_dir)
# path is e.g. Path("exported/dataset_00000.csv")
```

### Export all data artifacts from a step

```python
artifact_ids = store.load_artifact_ids_by_type("data", step_numbers=[2])
artifacts = store.get_artifacts_by_type(list(artifact_ids), "data")

output_dir = Path("exported/step_2/")
output_dir.mkdir(parents=True, exist_ok=True)

for artifact in artifacts.values():
    artifact.materialize_to(output_dir)
```

---

## Read Delta tables directly

All pipeline state is stored as Delta Lake tables under `delta_root`. You can
read any table with Polars for custom queries beyond what the inspect helpers
provide.

### Framework tables

```python
import polars as pl

# Artifact index — one row per artifact with IDs, types, and step numbers
artifacts = pl.read_delta(str(delta_root / "artifacts/index"))

# Step records — pipeline step status and duration
steps = pl.read_delta(str(delta_root / "orchestration/steps"))

# Execution records — individual operation runs with timing metadata
executions = pl.read_delta(str(delta_root / "orchestration/executions"))

# Provenance: artifact derivation relationships (source -> target)
artifact_edges = pl.read_delta(str(delta_root / "provenance/artifact_edges"))

# Provenance: which artifacts an execution consumed/produced
execution_edges = pl.read_delta(str(delta_root / "provenance/execution_edges"))
```

You can also use the `TablePath` enum to avoid hardcoding path strings:

```python
from artisan.schemas.enums import TablePath

steps = pl.read_delta(str(delta_root / TablePath.STEPS))
```

`TablePath` members are string enums, so they work directly in path
construction without `.value`.

### Artifact content tables

Each artifact type stores its content in a dedicated table:

```python
data = pl.read_delta(str(delta_root / "artifacts/data"))
metrics = pl.read_delta(str(delta_root / "artifacts/metrics"))
configs = pl.read_delta(str(delta_root / "artifacts/configs"))
file_refs = pl.read_delta(str(delta_root / "artifacts/file_refs"))
```

To look up the table path for a given type programmatically:

```python
from artisan.schemas.artifact.registry import ArtifactTypeDef

path = ArtifactTypeDef.get_table_path("data")  # "artifacts/data"
```

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `FileNotFoundError` from inspect helpers | No completed steps at `delta_root` | Verify the pipeline ran and the path is correct |
| `inspect_data` raises `ValueError` | `content` is `None` (not hydrated) | The `DataArtifact` was created without CSV content |
| `inspect_data` raises `ValueError` with "No matching data artifacts found" | Name does not match any `original_name` in the data table | Check the error message for available names |
| `inspect_metrics` returns empty DataFrame | No metric artifacts at that step | Use `inspect_step` to check what artifact types exist |
| `materialize_to` raises `ValueError` | Artifact not hydrated or `original_name` not set | Load the artifact with `hydrate=True` (the default) |
| `pl.read_delta` raises an error | Path does not contain a valid Delta table | Check spelling; use `TablePath` enum values for framework tables |

---

## Verify

Confirm you can read pipeline outputs:

```python
from artisan.visualization import inspect_pipeline, inspect_metrics

df = inspect_pipeline(delta_root)
assert len(df) > 0, "No completed steps found"

metrics = inspect_metrics(delta_root)
assert len(metrics) > 0, "No metrics found"
```

---

## Cross-references

- [Exploring Results](../tutorials/getting-started/02-exploring-results.ipynb) —
  Interactive tutorial for inspect helpers
- [Inspect Pipeline Results and Provenance](inspecting-provenance.md) — Lineage
  traversal, graph visualization, and timing analysis
- [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) — How Artisan
  persists data and the Delta Lake table layout
- [Error Handling](../concepts/error-handling.md) — Understanding step status
  values and failure modes
