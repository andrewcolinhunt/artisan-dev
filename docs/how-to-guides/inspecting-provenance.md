# Inspect Pipeline Results and Provenance

Select a run, inspect its accepted outputs, and trace how artifacts relate to
one another. These recipes assume a populated local `delta_root`.

See [Provenance System](../concepts/provenance-system.md) for the distinction
between execution history and artifact lineage.

## Select the run you want to inspect

A store can contain many runs, each with steps numbered from zero. List the runs
first, then keep one run ID throughout your inspection:

```python
from artisan.orchestration import list_runs
from artisan.visualization import inspect_pipeline, inspect_step, inspect_metrics

delta_root = "runs/delta"
runs = list_runs(delta_root)
print(runs)

# Choose the most recently started run, or replace this with a listed run ID.
run_id = runs.sort("started_at", descending=True)["pipeline_run_id"][0]
print(inspect_pipeline(delta_root, pipeline_run_id=run_id))
print(inspect_step(delta_root, step_number=0, pipeline_run_id=run_id))
```

While building a pipeline, obtain this ID from
`pipeline.config.pipeline_run_id`.

**Accepted outputs and artifact origin answer different questions.** Suppose a
run reuses three datasets at step 2 that were first created at step 0 of an
older run. A run-scoped query at step 2 shows those three datasets. Their
`origin_step_number` remains 0.

`inspect_pipeline` defaults to the latest run. In contrast, omitting
`pipeline_run_id` from `inspect_step`, `inspect_metrics`, or `inspect_data`
queries artifacts across runs, and a step filter means their first origin step.
Pass the run ID explicitly when comparing a step's status with its outputs.

The inspection helpers read accepted, integrity-checked results. Raw Delta
queries inspect physical rows and can include incomplete writes; see
[Storage and Delta Lake](../concepts/storage-and-delta-lake.md).

## Read metrics and data

Metric keys become columns, including flattened nested keys such as
`distribution.median`:

```python
# All metrics accepted by this run.
metrics = inspect_metrics(delta_root, pipeline_run_id=run_id)

# Metrics at one logical step, with display rounding.
metrics_at_step = inspect_metrics(
    delta_root, step_number=2, pipeline_run_id=run_id, round_digits=4
)
```

Read stored CSV content with `inspect_data`. A step query concatenates matching
datasets and adds `_source` to identify each artifact's name:

```python
from artisan.visualization import inspect_data

data = inspect_data(delta_root, step_number=0, pipeline_run_id=run_id)
```

Use `name="dataset_00000"` to narrow the query by original name. Use
`inspect_step` first to check available names and types. See
[Export Pipeline Results](exporting-results.md) to save these tables or
materialize individual artifacts.

## Visualize a selected run

A macro graph shows steps and their output roles. A micro graph includes
artifacts and derivation edges:

```python
from artisan.visualization import build_macro_graph, build_micro_graph

build_macro_graph(delta_root, pipeline_run_id=run_id)
build_micro_graph(delta_root, pipeline_run_id=run_id, max_step=2)
```

Both render inline in Jupyter when Graphviz is installed. To save a graph:

```python
from artisan.visualization import render_micro_graph

macro = build_macro_graph(delta_root, pipeline_run_id=run_id)
macro.render(filename="pipeline", format="svg")
render_micro_graph(
    delta_root,
    output_path="provenance",
    format="svg",
    max_step=2,
    pipeline_run_id=run_id,
)
```

Grey boxes represent executions; blue boxes represent artifacts. Execution
arrows show consumption and production. Orange arrows show artifact derivation;
dashed arrows mark backward or passthrough connections.

### Step through a single-run store

The notebook stepper and automatic frame exporter do not accept a run ID. They
use origin steps across the store, so use them for a store containing one run:

```python
from artisan.visualization import display_provenance_stepper

display_provenance_stepper(delta_root)
```

For a multi-run store, generate frames by calling `render_micro_graph` with the
same `pipeline_run_id` and successive `max_step` values. This keeps reused
artifacts at the selected run's logical steps.

## Trace an artifact's lineage

Artifact IDs identify content across runs. The store's lineage methods traverse
the committed artifact graph across that history; they do not restrict results
to a selected run.

```python
from artisan.storage import ArtifactStore

store = ArtifactStore(delta_root)
artifact_id = "<artifact ID to trace>"

parents = store.provenance.get_direct_ancestors(artifact_id)
ancestor_ids = store.provenance.get_ancestor_ids(artifact_id)
metric_ids = store.provenance.get_descendant_ids(artifact_id, descendant_type="metric")
```

The descendant query follows every reachable derivation edge. Do not restrict
it to the source artifact's origin step: descendants can originate in later
steps or other runs.

To load directly associated metrics as artifact objects:

```python
metrics_by_source = store.get_associated({artifact_id}, associated_type="metric")
```

To compare two artifacts:

```python
ancestors_a = set(store.provenance.get_ancestor_ids(artifact_a_id))
ancestors_b = set(store.provenance.get_ancestor_ids(artifact_b_id))
shared = ancestors_a & ancestors_b
```

### Look up origin metadata

Use origin queries when you deliberately want to investigate where content was
first recorded:

```python
artifact_type = store.get_artifact_type(artifact_id)
origin_step = store.provenance.get_artifact_step_number(artifact_id)

# Metrics first recorded at these origin step numbers, across all runs.
metric_ids = store.provenance.load_artifact_ids_by_type("metric", step_numbers=[2, 3])
```

These IDs do not describe the accepted outputs of step 2 or 3 in `run_id`.
For that task, use the run-scoped inspection helpers above.

## Diagnose failures and timing

Start with the selected run's status, then retrieve failed execution details:

```python
from artisan.visualization import inspect_failures

print(inspect_pipeline(delta_root, pipeline_run_id=run_id))
failures = inspect_failures(delta_root, pipeline_run_id=run_id)
print(failures)
```

The failure report includes execution IDs, error codes, recovery hints, and log
locations. Use an execution ID to [inspect command evidence or replay the
execution](debugging-executions.md).

For phase-level timing:

```python
from artisan.visualization import PipelineTimings

timings = PipelineTimings.from_delta(delta_root, pipeline_run_id=run_id)
timings.step_timings()
timings.execution_stats(step_number=1)
timings.plot_steps()
```

Run-scoped execution timing includes work actually performed for that run.
Cached outputs remain visible in results and provenance, but their historical
execution duration is not charged to the current run.

## Check unexpected results

- If a step looks different from the overview, check that both calls use the
  same run ID.
- If metrics are empty, inspect the step's artifact types before assuming it
  produced metrics.
- If lineage is missing, check the operation's `infer_lineage_from`, filename
  stems, and explicit mappings. Generative artifacts intentionally have no
  parents.
- If a graph is too large, narrow `max_step` or trace one artifact in code.
- If the stepper fails to render, use a Jupyter environment with `ipywidgets`
  and Graphviz installed.

## Related guides

- [Exploring Results](../tutorials/01-getting-started/02-exploring-results.ipynb)
  introduces the readers with a runnable pipeline.
- [Provenance Graphs](../tutorials/08-analysis/01-provenance-graphs.ipynb) walks
  through graph interpretation.
- [Python API](../reference/python-api.md) points to supported imports and
  source docstrings for reader arguments and return values.
