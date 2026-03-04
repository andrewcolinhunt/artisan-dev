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

## Step 1: Get a pipeline overview

Start with `inspect_pipeline` to see what happened at each step:

```python
from artisan.visualization import inspect_pipeline

df = inspect_pipeline(delta_root)
```

Returns a Polars DataFrame with one row per step:

| Column | Description |
|--------|-------------|
| `step` | Step number |
| `operation` | Operation name |
| `status` | `completed` or `skipped` |
| `produced` | Artifact summary (e.g., `"5 data, 5 metric"` or `"3 passed"` for filters) |
| `duration` | Wall-clock time (e.g., `"2.3s"`) |

To inspect a specific run when `delta_root` contains multiple:

```python
inspect_pipeline(delta_root, pipeline_run_id="run_abc123...")
```

---

## Step 2: Inspect artifacts at a step

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
| `file_ref`, `structure` | Human-readable file size |

---

## Step 3: Read metric values

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

## Step 4: Read data artifact contents

`inspect_data` parses the actual CSV content of `DataArtifact` entries:

```python
from artisan.visualization import inspect_data

# By name
df = inspect_data(delta_root, name="d0")

# All data at a step (concatenated, with a `_source` column)
df = inspect_data(delta_root, step_number=1)
```

---

## Step 5: Visualize the pipeline graph

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
render_micro_graph_steps(delta_root, output_dir=Path("steps/"), format="svg")
```

### Interactive stepper (Jupyter widget)

Step through the provenance graph one step at a time with a slider:

```python
from artisan.visualization import display_provenance_stepper

display_provenance_stepper(delta_root)
```

### Reading the graph

| Element | Meaning |
|---------|---------|
| Grey boxes | Execution records (one per step) |
| Coloured boxes | Artifacts (colour varies by type) |
| Solid arrows with dot tails | Execution provenance (consumed/produced) |
| Orange arrows | Artifact provenance (derived from) |
| Dashed arrows | Backward/passthrough edges |

---

## Step 6: Trace lineage programmatically

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

Load the entire provenance graph into memory for BFS traversal:

```python
# Backward map: {target_id: [source_ids]}
backward_map = store.load_provenance_map()

# Forward map: {source_id: [target_ids]}
forward_map = store.load_forward_provenance_map()

# Walk all ancestors of an artifact
def get_all_ancestors(artifact_id: str, backward_map: dict) -> set[str]:
    ancestors = set()
    queue = [artifact_id]
    while queue:
        current = queue.pop()
        for parent in backward_map.get(current, []):
            if parent not in ancestors:
                ancestors.add(parent)
                queue.append(parent)
    return ancestors
```

For forward traversal with a target filter, use the built-in utility:

```python
from artisan.storage.provenance_utils import trace_derived_artifacts

# BFS forward from source, collecting only artifacts in target_ids
derived = trace_derived_artifacts(source_id, forward_map, target_ids)
```

### Get descendants as full artifact objects

```python
# Returns {source_id: [Artifact, ...]} — loaded and typed
metrics = store.get_associated({"abc123..."}, associated_type="metric")
```

---

## Step 7: Look up artifact metadata

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

## Step 8: Profile execution timing

`PipelineTimings` provides step-level and execution-level timing breakdowns:

```python
from artisan.visualization import PipelineTimings

timings = PipelineTimings.from_delta(delta_root)

# Step-level durations (one row per step, columns per phase)
timings.step_timings()

# Per-execution timings at a specific step
timings.execution_timings(step_number=1)

# Summary statistics (mean, std, min, max per phase)
timings.execution_stats(step_number=1)

# Matplotlib plots
timings.plot_steps()              # Stacked bar chart of step timings
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
from artisan.storage import ArtifactStore
from artisan.storage.provenance_utils import trace_derived_artifacts

store = ArtifactStore(delta_root)
forward_map = store.load_forward_provenance_map()
metric_ids = store.load_artifact_ids_by_type("metric")

derived_metrics = trace_derived_artifacts("source_abc...", forward_map, metric_ids)
```

### Compare ancestry of two artifacts

```python
backward_map = store.load_provenance_map()
ancestors_a = get_all_ancestors("artifact_a", backward_map)
ancestors_b = get_all_ancestors("artifact_b", backward_map)

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

# Step-by-step frames for animation
paths = render_micro_graph_steps(delta_root, output_dir=Path("frames/"))
# ["frames/step_00.svg", "frames/step_01.svg", ...]
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
prov_map = store.load_provenance_map()
assert len(prov_map) > 0, "No provenance edges found"
```

---

## Cross-references

- [Provenance System](../concepts/provenance-system.md) — Dual provenance
  model, stem matching, and design rationale
- [Provenance Graphs Tutorial](../tutorials/working-with-results/01-provenance-graphs.ipynb) — Interactive
  provenance visualization walkthrough
- [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) — Table
  schemas and Delta Lake layout
