# Export Pipeline Results

How to extract pipeline outputs — artifacts, metrics, and execution metadata —
from Delta Lake tables for downstream analysis.

**Prerequisites:** A completed pipeline run with data in `delta_root`.

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

Artisan provides read-only helpers that present Delta Lake tables as clean
Polars DataFrames. Import them from `artisan.visualization`:

```python
from artisan.visualization import (
    inspect_pipeline,
    inspect_step,
    inspect_metrics,
    inspect_data,
)
```

### Pipeline summary

```python
df = inspect_pipeline(delta_root)
```

Returns one row per step with columns: `step`, `operation`, `status`,
`produced`, `duration`.

### Artifacts at a step

```python
df = inspect_step(delta_root, step_number=0)
```

Returns one row per artifact at the given step with columns: `name`,
`artifact_type`, `step`, `details`.

### Metric values

```python
# All metric steps
df = inspect_metrics(delta_root)

# Single step
df = inspect_metrics(delta_root, step_number=2)
```

Parses metric JSON into a flat table. Nested dicts are auto-flattened (e.g.,
`distribution.median` becomes a column). Float values are rounded to 3 decimal
places by default — override with `round_digits`.

### Data artifact contents

```python
# By name — returns the CSV content as a DataFrame
df = inspect_data(delta_root, name="dataset_00000")

# All data at a step — concatenated, with a `_source` column
df = inspect_data(delta_root, step_number=1)
```

---

## Reading Delta tables directly

All pipeline state is stored as Delta Lake tables under `delta_root`. You can
read any table with Polars.

### Framework tables

```python
import polars as pl

# Artifact index — one row per artifact with IDs, types, and step numbers
artifacts = pl.read_delta(str(delta_root / "artifacts/index"))

# Step records — pipeline step status and duration
steps = pl.read_delta(str(delta_root / "orchestration/steps"))

# Execution records — individual operation runs with timing metadata
executions = pl.read_delta(str(delta_root / "orchestration/executions"))

# Provenance: artifact derivation relationships (source → target)
artifact_edges = pl.read_delta(str(delta_root / "provenance/artifact_edges"))

# Provenance: which artifacts an execution consumed/produced
execution_edges = pl.read_delta(str(delta_root / "provenance/execution_edges"))
```

### Artifact content tables

Each artifact type stores its content in a dedicated table:

```python
data = pl.read_delta(str(delta_root / "artifacts/data"))
metrics = pl.read_delta(str(delta_root / "artifacts/metrics"))
configs = pl.read_delta(str(delta_root / "artifacts/configs"))
file_refs = pl.read_delta(str(delta_root / "artifacts/file_refs"))
```

The table path for a given type is determined by the artifact type registry.
You can look it up programmatically:

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
| `inspect_metrics` returns empty DataFrame | No metric artifacts at that step | Use `inspect_step` to check what artifact types exist |
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
