# Inspect Pipeline Results and Provenance

How to explore what a pipeline produced, trace where artifacts came from, and
diagnose problems — from quick overviews to full lineage traversal.

**Prerequisites:** A completed pipeline run with data in `delta_root`, and
familiarity with [Provenance System](../concepts/provenance-system.md).

---

## Minimal working example

A complete inspection workflow in a few lines:

```python
from pathlib import Path
from artisan.visualization import inspect_pipeline, inspect_step, inspect_metrics, build_macro_graph

delta_root = Path("runs/delta")

inspect_pipeline(delta_root)       # One row per step: operation, status, counts, duration
inspect_step(delta_root, 0)        # One row per artifact at step 0
inspect_metrics(delta_root, 2)     # Metric values as columns at step 2
build_macro_graph(delta_root)      # Step-level pipeline graph (renders in Jupyter)
```

The rest of this guide covers each inspection technique in detail.

---

## Get a pipeline overview

Start with `inspect_pipeline` to see what happened at each step:

```python
from artisan.visualization import inspect_pipeline

df = inspect_pipeline(delta_root)
```

Returns a Polars DataFrame with one row per step:

| Column | Description |
|--------|-------------|
| `step` | Step number |
| `operation` | Step name |
| `status` | `ok`, `skipped` |
| `produced` | Artifact summary (e.g., `"5 data, 5 metric"` or `"3 passed"` for filters) |
| `duration` | Wall-clock time (e.g., `"2.3s"`) |

To inspect a specific run when `delta_root` contains multiple:

```python
inspect_pipeline(delta_root, pipeline_run_id="run_abc123...")
```

---

## Inspect artifacts at a step

Drill into a step to see individual artifacts:

```python
from artisan.visualization import inspect_step

df = inspect_step(delta_root, step_number=0)
```

Returns one row per artifact with type-specific details:

| Artifact type | `details` column shows |
|---------------|------------------------|
| `data` | `"N rows, M cols"` |
| `metric` | Up to 4 metric key names |
| `config` | `"N params"` |
| `file_ref` | Human-readable file size |

---

## Read metric values

`inspect_metrics` parses metric JSON into a flat table with one column per
metric key:

```python
from artisan.visualization import inspect_metrics

# All metric steps
df = inspect_metrics(delta_root)

# Single step, with rounding control
df = inspect_metrics(delta_root, step_number=2, round_digits=4)
```

Nested metric dicts are automatically flattened (e.g., `distribution.median`
becomes a column).

---

## Read data artifact contents

`inspect_data` reads the CSV content stored in `DataArtifact` entries and returns
it as a Polars DataFrame:

```python
from artisan.visualization import inspect_data

# By name
df = inspect_data(delta_root, name="d0")

# All data at a step (concatenated, with a `_source` column)
df = inspect_data(delta_root, step_number=1)
```

---

## Visualize the pipeline graph

### Macro graph (step-level)

Shows steps as nodes and data flow as edges:

```python
from artisan.visualization import build_macro_graph

build_macro_graph(delta_root)  # Renders inline in Jupyter
```

To save to a file:

```python
from artisan.visualization import render_macro_graph

render_macro_graph(delta_root, output_path=Path("pipeline"), format="svg")
```

### Micro graph (artifact-level)

Shows individual artifacts and their derivation edges:

```python
from artisan.visualization import build_micro_graph

build_micro_graph(delta_root)  # Full graph
build_micro_graph(delta_root, max_step=2)  # Steps 0–2 only
```

To save to a file or render per-step images:

```python
from artisan.visualization import render_micro_graph, render_micro_graph_steps

render_micro_graph(delta_root, output_path=Path("provenance"), format="svg")
render_micro_graph(delta_root, output_path=Path("provenance"), format="png", max_step=3)
render_micro_graph_steps(delta_root, output_dir=Path("steps/"), format="svg")
```

### Interactive stepper (Jupyter widget)

Step through the provenance graph one step at a time with a slider:

```python
from artisan.visualization import display_provenance_stepper

display_provenance_stepper(delta_root)

# Custom output directory for rendered step images
display_provenance_stepper(delta_root, output_dir=Path("my_images/"))
```

### Reading the graph

| Element | Meaning |
|---------|---------|
| Grey boxes | Execution records (one per step) |
| Blue boxes | Artifacts (shade varies by type from a blue palette) |
| Solid arrows with dot tails | Execution provenance (consumed/produced) |
| Orange arrows | Artifact provenance (derived from) |
| Dashed arrows | Backward/passthrough edges |

---

## Trace lineage programmatically

Use `ArtifactStore` when you need lineage data in code rather than as a
visualization.

```python
from artisan.storage import ArtifactStore

store = ArtifactStore(delta_root)
```

### One hop backward (direct parents)

```python
parents = store.get_ancestor_artifact_ids("abc123...")
```

### One hop forward (direct children)

```python
children_map = store.get_descendant_artifact_ids({"abc123..."})
children = children_map.get("abc123...", [])

# Filter by type
metric_children = store.get_descendant_artifact_ids(
    {"abc123..."}, target_artifact_type="metric"
)
```

### Full graph traversal

Load the entire provenance graph or transitively walk ancestors/descendants:

```python
# Backward map: {target_id: [source_ids]}
backward_map = store.provenance.load_backward_map()

# Forward map: {source_id: [target_ids]}
forward_map = store.provenance.load_forward_map()

# Transitive ancestor/descendant queries (no hand-rolled BFS needed)
ancestor_ids = store.provenance.get_ancestor_ids("artifact_abc123...")
descendant_ids = store.provenance.get_descendant_ids("artifact_abc123...")

# Filter transitive results by artifact type
metric_ancestors = store.provenance.get_ancestor_ids(
    "artifact_abc123...", ancestor_type="metric"
)
data_descendants = store.provenance.get_descendant_ids(
    "artifact_abc123...", descendant_type="data"
)
```

For forward traversal using the DataFrame-based walk:

```python
import polars as pl
from artisan.provenance import walk_forward

sources = pl.DataFrame({"artifact_id": [source_id]})
edges = store.load_provenance_edges_df(step_min, step_max, include_target_type=True)
result = walk_forward(sources, edges, target_type="metric")
# result has columns [source_id, target_id]
```

For backward traversal (e.g., matching candidates to their source targets):

```python
from artisan.provenance import walk_backward

candidates = pl.DataFrame({"artifact_id": [candidate_id]})
targets = pl.DataFrame({"artifact_id": [target_id]})
edges_df = store.load_provenance_edges_df(step_min, step_max)
result = walk_backward(candidates, targets, edges_df)
# result has columns [candidate_id, target_id]
```

### Get descendants as full artifact objects

```python
# Returns {source_id: [Artifact, ...]} — loaded and typed
metrics = store.get_associated({"abc123..."}, associated_type="metric")
```

---

## Look up artifact metadata

```python
store = ArtifactStore(delta_root)

# Single lookups
artifact_type = store.get_artifact_type("abc123...")        # "data", "metric", etc.
step_number = store.get_artifact_step_number("abc123...")    # int

# Bulk lookups (single Delta scan each — use these when querying many artifacts)
type_map = store.load_artifact_type_map()       # {artifact_id: type_str}
step_map = store.load_step_number_map()         # {artifact_id: step_number}
name_map = store.load_step_name_map()           # {step_number: step_name}

# Get artifact IDs by type, optionally filtered by step
ids = store.load_artifact_ids_by_type("metric", step_numbers=[2, 3])
```

---

## Profile execution timing

`PipelineTimings` provides step-level and execution-level timing breakdowns:

```python
from artisan.visualization import PipelineTimings

timings = PipelineTimings.from_delta(delta_root)

# Filter to a specific pipeline run
timings = PipelineTimings.from_delta(delta_root, pipeline_run_id="run_abc123...")

# Step-level durations (one row per step, columns per phase)
timings.step_timings()

# Per-execution timings at a specific step
timings.execution_timings(step_number=1)

# Summary statistics (mean, std, min, max per phase)
timings.execution_stats(step_number=1)

# Matplotlib plots
timings.plot_steps()              # Stacked bar chart of step timings
timings.plot_steps(step_numbers=[0, 2, 4])  # Subset of steps
timings.plot_execution_stats()    # Stacked bar chart of mean execution timings
```

---

## Common patterns

### Quick triage after a failed run

```python
# What happened?
df = inspect_pipeline(delta_root)

# Which step failed? Look for low artifact counts or short durations.
# Drill into the suspect step:
inspect_step(delta_root, step_number=2)
```

### Find all metrics derived from a source artifact

```python
import polars as pl
from artisan.provenance import walk_forward
from artisan.storage import ArtifactStore

store = ArtifactStore(delta_root)
sources = pl.DataFrame({"artifact_id": ["source_abc..."]})
step_range = store.get_step_range(pl.Series(["source_abc..."]))
edges = store.load_provenance_edges_df(*step_range, include_target_type=True)

derived = walk_forward(sources, edges, target_type="metric")
# derived has columns [source_id, target_id]
```

### Compare ancestry of two artifacts

```python
ancestors_a = set(store.provenance.get_ancestor_ids("artifact_a"))
ancestors_b = set(store.provenance.get_ancestor_ids("artifact_b"))

shared = ancestors_a & ancestors_b
unique_to_a = ancestors_a - ancestors_b
unique_to_b = ancestors_b - ancestors_a
```

### Query provenance tables directly

For custom analysis beyond what the helpers provide:

```python
import polars as pl

# Artifact provenance edges (source → target)
df_edges = pl.read_delta(str(delta_root / "provenance" / "artifact_edges"))

# Find all children of a specific artifact
children = df_edges.filter(
    pl.col("source_artifact_id") == "abc123..."
).select("target_artifact_id", "target_role")

# Execution provenance edges (artifact ↔ execution)
df_exec = pl.read_delta(str(delta_root / "provenance" / "execution_edges"))

# All artifacts consumed by a specific execution
inputs = df_exec.filter(
    (pl.col("execution_run_id") == "run_xyz...")
    & (pl.col("direction") == "input")
)
```

### Export provenance graphs to files

```python
from artisan.visualization import render_micro_graph, render_micro_graph_steps

# Single graph as PNG
render_micro_graph(delta_root, output_path=Path("provenance"), format="png")

# Limit to a step range
render_micro_graph(delta_root, output_path=Path("provenance"), format="svg", max_step=5)

# Step-by-step frames for animation
paths = render_micro_graph_steps(delta_root, output_dir=Path("frames/"))
# [Path("frames/step_00.svg"), Path("frames/step_01.svg"), ...]
```

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `FileNotFoundError` from inspect helpers | No completed steps in `delta_root` | Verify the pipeline ran and the path is correct |
| Empty provenance map | No artifact edges committed | Check that operations set `infer_lineage_from` on their outputs |
| Orphan artifacts (no parent edges) | Stem matching found 0 or >1 candidates | Ensure output filenames preserve the input filename stem. See [stem matching](../concepts/provenance-system.md) |
| `inspect_metrics` returns empty DataFrame | No metric artifacts at that step | Use `inspect_step` to check what artifact types exist |
| `inspect_data` raises `ValueError` | `content` is `None` (not hydrated) | The DataArtifact was created without CSV content |
| Stepper widget does not render | Missing `ipywidgets` or not in Jupyter | Install: `pip install ipywidgets` |
| Micro graph is unreadable | Too many artifacts | Use `max_step` to limit scope, or query programmatically |

---

## Verify

Confirm provenance is populated:

```python
from artisan.visualization import inspect_pipeline
from artisan.storage import ArtifactStore

# Should return a non-empty DataFrame with one row per step
df = inspect_pipeline(delta_root)
assert len(df) > 0, "No completed steps found"

# Should contain entries linking source and target artifacts
store = ArtifactStore(delta_root)
prov_map = store.provenance.load_backward_map()
assert len(prov_map) > 0, "No provenance edges found"
```

---

## Cross-references

- [Provenance System](../concepts/provenance-system.md) — Dual provenance
  model, stem matching, and design rationale
- [Provenance Graphs Tutorial](../tutorials/analysis/01-provenance-graphs.ipynb) — Interactive
  provenance visualization walkthrough
- [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) — Table
  schemas and Delta Lake layout
- [Export Pipeline Results](exporting-results.md) — Artifact retrieval,
  materialization, and raw Delta table access
- [Error Handling](../concepts/error-handling.md) — Understanding step status
  values and failure modes
