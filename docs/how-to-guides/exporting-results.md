# Exporting Results

How to access pipeline results from Delta Lake tables and export artifact
data for downstream analysis.

**Prerequisites:** A completed pipeline run.

**Key files:** `src/artisan/visualization/inspect.py`,
`src/artisan/storage/core/artifact_store.py`

---

## Quick Inspection

Artisan provides helper functions to inspect pipeline outputs without
writing raw queries:

```python
from artisan.visualization import inspect_pipeline, inspect_metrics, inspect_data

# Pipeline overview: step names, status, artifact counts, durations
inspect_pipeline(delta_root)

# Metrics table: one row per artifact, metric columns auto-flattened
inspect_metrics(delta_root)

# Read a single data artifact's content as a DataFrame
inspect_data(delta_root, name="dataset_00000")

# Read all data artifacts from a specific step
inspect_data(delta_root, step_number=2)
```

---

## Reading Delta Tables Directly

All pipeline state is stored as Delta Lake tables under `delta_root`. You
can read any table with Polars:

```python
import polars as pl

# Artifact index — one row per artifact with IDs, types, and step numbers
artifacts = pl.read_delta(str(delta_root / "artifacts/index"))

# Execution records — operation runs with timing metadata
executions = pl.read_delta(str(delta_root / "orchestration/executions"))

# Step records — pipeline step status and duration
steps = pl.read_delta(str(delta_root / "orchestration/steps"))

# Provenance edges — artifact derivation relationships
artifact_edges = pl.read_delta(str(delta_root / "provenance/artifact_edges"))
```

### Content Tables

Each artifact type stores its content in a dedicated table. The table path
is determined by the artifact type registry:

```python
# Data artifacts
data = pl.read_delta(str(delta_root / "artifacts/data"))

# Metric artifacts
metrics = pl.read_delta(str(delta_root / "artifacts/metrics"))
```

---

## Cross-References

- [Exploring Results](../tutorials/getting-started/02-exploring-results.ipynb) --
  Interactive tutorial for inspect helpers
- [Inspecting Provenance](inspecting-provenance.md) -- Query lineage and
  trace artifact history
- [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) -- How
  Artisan persists data
