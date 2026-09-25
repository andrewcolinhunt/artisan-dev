# Write Curator Operations

How to write operations that route, filter, merge, or ingest artifacts without
heavy computation. Companion to
[Writing Creator Operations](writing-creator-operations.md).

**Prerequisites:** [Operations Model](../concepts/operations-model.md),
[Writing Creator Operations](writing-creator-operations.md).

**Key types:** `OperationDefinition`, `ArtifactResult`, `PassthroughResult`

---

## Minimal working example

A curator operation that merges two artifact streams into one:

```python
from __future__ import annotations

from enum import StrEnum, auto
from typing import ClassVar

import polars as pl

from artisan.operations.base import OperationDefinition
from artisan.schemas import (
    ArtifactTypes,
    InputSpec,
    OutputSpec,
    PassthroughResult,
)
from artisan.storage import ArtifactStore


class SimpleMerge(OperationDefinition):
    """Merge multiple artifact streams into a single output."""

    name = "simple_merge"

    runtime_defined_inputs: ClassVar[bool] = True
    independent_input_streams: ClassVar[bool] = True
    hydrate_inputs: ClassVar[bool] = False

    inputs: ClassVar[dict[str, InputSpec]] = {}

    class OutputRole(StrEnum):
        merged = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.merged: OutputSpec(
            artifact_type=ArtifactTypes.ANY, required=False
        ),
    }

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        merged = (
            pl.concat(inputs.values()).select("artifact_id")
            if inputs
            else pl.DataFrame({"artifact_id": []})
        )
        return PassthroughResult(
            success=True,
            passthrough={"merged": merged["artifact_id"].to_list()},
        )
```

Use it in a pipeline like any other operation:

```python
pipeline.run(
    operation=SimpleMerge,
    name="merge",
    inputs=[output("branch_a", "results"), output("branch_b", "results")],
)
```

---

## Choose curator vs creator

Curator and creator operations solve different problems. Pick the right one
before you start writing code.

| | Creator | Curator |
|---|---------|---------|
| **Purpose** | Heavy computation, file I/O | Route, filter, merge, or ingest artifacts |
| **Execution** | Three phases (`preprocess` / `execute_function` / `postprocess`) | Single `execute_curator` method |
| **Sandboxing** | Full sandbox with file materialization | No working sandbox; memory-isolated subprocess |
| **Dispatch** | Built-in local runner or an external provider instance | Local spawned subprocess; step-runner overrides do not relocate it |
| **Returns** | `ArtifactResult` (always creates new artifacts) | `ArtifactResult` or `PassthroughResult` |

**Choose curator when** the operation routes, filters, merges, or annotates
existing artifacts without heavy computation.

**Choose creator when** the operation runs external tools, processes files, or
needs compute dispatched through an external provider such as a SLURM runner.
Curators stay local even when the pipeline's default step runner is external.

---

## Choose a return type

Curator operations return one of two result types. This choice shapes the rest
of your implementation.

### `PassthroughResult` — route existing artifacts

No new artifacts are created. The output is a subset or union of input artifact
IDs. Used by operations like Filter and Merge.

```python
from artisan.schemas import PassthroughResult

return PassthroughResult(
    success=True,
    passthrough={"output_role": ["artifact_id_1", "artifact_id_2"]},
)
```

The `passthrough` dict maps output role names to lists of artifact ID strings.

### `ArtifactResult` — create new artifacts

New draft artifacts are created and returned. Used by ingest operations that
bring external data into the pipeline. In this fragment, `file_ref` is the
input whose bytes were read into `file_bytes`; the output spec declares
`derives_from={"inputs": ["file"]}`.

```python
from artisan.schemas import ArtifactResult, DataArtifact, LineageMapping

drafts = [
    DataArtifact.draft(
        content=file_bytes,
        original_name="dataset.csv",
        step_number=step_number,
    )
]
return ArtifactResult(
    artifacts={"data": drafts},
    lineage={"data": [LineageMapping(
        draft_index=0, source_role="file", source_artifact_id=file_ref.artifact_id,
    )]},
)
```

The `artifacts` dict maps output role names to lists of draft `Artifact`
objects. Drafts are finalized automatically by the framework after
`execute_curator` returns.

---

## Define the operation class

A curator operation is an `OperationDefinition` subclass that overrides
`execute_curator`. The framework detects curator operations automatically — if
`execute_curator()` is overridden, the operation is treated as a curator.
No explicit flag or registration needed.

For a merge-like operation, `runtime_defined_inputs=True` lets callers choose
input roles, and `independent_input_streams=True` allows different stream lengths.
Use `hydrate_inputs=False` when only IDs are needed. The complete `SimpleMerge`
example above combines these settings; see the
[operation docstrings](../reference/python-api.md) for their exact contracts.

### Input and output specs

When `runtime_defined_inputs=True`, set `inputs` to an empty dict `{}` — input
role names are provided by the caller at pipeline construction time. You do not
need an `InputRole` enum in this case.

When `outputs` is non-empty, you must define an `OutputRole(StrEnum)` inner
class whose values match the `outputs` dict keys. The framework validates this
match at class definition time.

Curator operations skip several validations that apply to creators:

- `derives_from=None` is valid for passthrough outputs. When a curator returns
  drafts in `ArtifactResult`, it must declare parent roles and exact parents,
  using the same validation as creators.
- `preprocess()` is not required, even when inputs are declared

### Method signature

```python
def execute_curator(
    self,
    inputs: dict[str, pl.DataFrame],
    step_number: int,
    artifact_store: ArtifactStore,
) -> PassthroughResult | ArtifactResult:
    ...
```

`inputs` is a dict mapping role names to Polars DataFrames, each with at least
an `artifact_id` column. Operations that need full artifact content hydrate
them from `artifact_store`.

`step_number` is needed when creating draft artifacts.

`artifact_store` provides access to the Delta Lake store for loading artifact
content, metrics, provenance edges, etc.

---

## Implement `execute_curator`

Here are the three common curator patterns with complete implementations.

### Pattern A: Filter (passthrough)

Accept a stream, evaluate each artifact, return the IDs that pass. This
example loads artifact content from the store and keeps only those whose
`original_name` matches a pattern:

```python
from enum import StrEnum, auto

from pydantic import BaseModel, Field

from artisan.schemas import ArtifactTypes


class NameFilter(OperationDefinition):
    name = "name_filter"

    runtime_defined_inputs: ClassVar[bool] = True
    hydrate_inputs: ClassVar[bool] = False

    inputs: ClassVar[dict[str, InputSpec]] = {}

    class OutputRole(StrEnum):
        passthrough = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.passthrough: OutputSpec(
            artifact_type=ArtifactTypes.ANY, required=False
        ),
    }

    class Params(BaseModel):
        contains: str = Field(default="", description="Substring to match.")

    params: Params = Params()

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        pt_df = inputs.get("passthrough", pl.DataFrame({"artifact_id": []}))
        ids = pt_df["artifact_id"].to_list()

        # Load artifacts from the store and filter by name
        artifacts = artifact_store.get_artifacts_by_type(ids, ArtifactTypes.DATA)
        passed = [
            aid for aid, art in artifacts.items()
            if art.original_name and self.params.contains in art.original_name
        ]

        return PassthroughResult(
            success=True,
            passthrough={"passthrough": passed},
        )
```

For metric-based filtering, use the built-in [Filter](#filter) operation instead
of writing a custom one. Filter handles the forward provenance walk needed to
discover descendant metrics.

### Pattern B: Merge (passthrough, multi-stream)

Collect artifacts from multiple input roles into a single output:

```python
class TaggedMerge(OperationDefinition):
    name = "tagged_merge"

    runtime_defined_inputs: ClassVar[bool] = True
    independent_input_streams: ClassVar[bool] = True
    hydrate_inputs: ClassVar[bool] = False

    inputs: ClassVar[dict[str, InputSpec]] = {}

    class OutputRole(StrEnum):
        merged = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.merged: OutputSpec(
            artifact_type=ArtifactTypes.ANY, required=False
        ),
    }

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        merged = pl.concat(inputs.values()).select("artifact_id")
        return PassthroughResult(
            success=True,
            passthrough={"merged": merged["artifact_id"].to_list()},
        )
```

### Pattern C: Ingest (new artifacts)

Create new artifacts from external data. The `IngestFiles` abstract base class
handles the iteration pattern — subclass it and implement `convert_file()`:

```python
from artisan.operations.curator import IngestFiles
from artisan.schemas import DataArtifact, FileRefArtifact


class IngestCSV(IngestFiles):
    name = "ingest_csv"

    class OutputRole(StrEnum):
        data = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.data: OutputSpec(
            artifact_type="data",
            derives_from={"inputs": ["file"]},
        ),
    }

    def convert_file(
        self, file_ref: FileRefArtifact, step_number: int, *, fs=None
    ) -> DataArtifact:
        content = file_ref.read_content(fs=fs)
        filename = f"{file_ref.original_name}{file_ref.extension or ''}"
        return DataArtifact.draft(
            content=content,
            original_name=filename,
            step_number=step_number,
        )
```

Usage:

```python
pipeline.run(operation=IngestCSV, name="ingest", inputs=["/data/a.csv", "/data/b.csv"])
# Raw file paths are auto-promoted to FileRefArtifact before dispatch
```

If `IngestFiles` does not fit your ingestion pattern, implement
`execute_curator` directly and return an `ArtifactResult` with explicit lineage.
`IngestFiles` declares each converted draft's exact `file` parent inside its
conversion loop. Your subclass supplies the output contract and conversion.

Config-producing curators follow the same rule: preserve the intended referenced
parents by declaring them under input roles. Calling
`config.get_artifact_references()` inside the operation can collect exact IDs;
deduplicate repeated references before adding mappings. The executor does not
scan config contents or hydrate source contents to discover parents. Reference
materialization still substitutes paths normally. See
[Config references and ancestry](writing-creator-operations.md#config-references-and-ancestry).

For a genuinely generative role, set `derives_from={"inputs": []}` and return
`lineage={"role": []}` alongside its artifact list. A curator with dynamic
output types and `outputs={}` may emit only roots and must include an empty
lineage list for every returned role. `IngestPipelineStep` uses this deliberately
to import artifacts as new roots, including configs; it does not preserve
source-pipeline ancestry or create edges to foreign IDs.

---

## Built-in curator operations

Before writing a custom curator, check whether a built-in one already does what
you need.

(filter)=
### Filter

Conditional passthrough with structured criteria. Evaluates metrics against
thresholds and returns the artifact IDs that pass.

```python
from artisan.operations.curator import Filter

pipeline.run(
    operation=Filter,
    name="filter",
    inputs={"passthrough": output("prev_step", "results")},
    params={
        "criteria": [
            {"metric": "score", "operator": "gt", "value": 0.5},
        ],
    },
)
```

Criteria use bare field names — Filter auto-discovers associated metrics via
forward provenance walk from the passthrough artifacts. When metrics come from
multiple sources with non-colliding field names, no extra wiring is needed.
When field names collide, add `step` or `step_number` to disambiguate:

```python
pipeline.run(
    operation=Filter,
    name="multi_filter",
    inputs={"passthrough": output("generate", "results")},
    params={
        "criteria": [
            {"metric": "mean_score", "operator": "gt", "value": 0.3},
            {"metric": "score", "operator": "gt", "value": 0.8, "step": "calc_quality"},
        ],
    },
)
```

All criteria are AND'd. Supported operators: `gt`, `ge`, `lt`, `le`, `eq`, `ne`.

Set `passthrough_failures=True` to preview what a filter *would* remove without
actually removing anything — useful for debugging filter thresholds:

```python
pipeline.run(
    operation=Filter,
    name="dry_run_filter",
    inputs={"passthrough": output("generate", "results")},
    params={
        "criteria": [{"metric": "score", "operator": "gt", "value": 0.9}],
        "passthrough_failures": True,
    },
)
```

### Merge

Union multiple artifact streams into one. No content is loaded — pure
passthrough.

```python
from artisan.operations.curator import Merge

# List format (preferred) — auto-generates role names
pipeline.run(
    operation=Merge,
    name="merge",
    inputs=[output("branch_a", "results"), output("branch_b", "results")],
)
# Output role is always "merged": output("merge", "merged")
```

### IngestData

Import files from disk as `DataArtifact` objects:

```python
from artisan.operations.curator import IngestData

pipeline.run(operation=IngestData, name="ingest", inputs=["/data/a.csv", "/data/b.csv"])
# Output role: "data" → output("ingest", "data")
```

### IngestPipelineStep

Import accepted outputs from an explicit run in another pipeline's Delta Lake
store. Use `artisan.orchestration.list_runs` to discover source run IDs:

```python
from artisan.operations.curator import IngestPipelineStep

pipeline.run(
    operation=IngestPipelineStep,
    name="ingest_external",
    params={
        "source_delta_root": "/runs/other_pipeline/delta",
        "source_run_id": "other_pipeline_20260919_120000_a1b2c3d4",
        "source_step": 3,
        "include_prior_steps": True,  # optional: include existing steps 0 through 3
        "artifact_type": "data",  # optional: filter by type
    },
)
```

Omit `include_prior_steps` to select only step 3. Both modes select the latest
attempt at each chosen position and import its accepted outputs, including
cached and passthrough results. Failed executions contribute no artifacts;
successful executions of a partial step do. The boundary must exist, and
selected pending or running attempts fail the import.

Through-N imports the union of outputs through the boundary, with duplicates
removed. If step 0 produces A and B and step 1 filters to A, importing only
step 1 gives A; importing through step 1 gives A and B. Select the final filter
step alone when you want its surviving set.

Each invocation reads the source again, even when its parameters match an
earlier import. Restoring an already accepted destination step with `resume()`
restores that result; submit a new ingest step or run to observe source changes.
Imported artifacts become destination roots, with their immediate source run
and artifact identity in `metadata.imported_from`. The execution's
`ingest_source` metadata records which source attempts contributed.

External files retain their verified source locations. Keep those files
accessible for the lifetime of the imported artifacts. Destination readers
need a filesystem that supports the retained URIs and the required access or
credentials. `source_storage` configures source reads; it does not transfer
credentials or copy file ownership. Missing or modified bytes fail the import.

### InteractiveFilter

Explore metric distributions in a notebook, set thresholds interactively, and
commit the result as a pipeline step. Unlike `Filter` (which requires upfront
criteria), `InteractiveFilter` lets you inspect data before committing to
thresholds.

```python
from artisan.operations.curator import InteractiveFilter

filt = InteractiveFilter(delta_root="/runs/my_pipeline/delta")
filt.load(step_numbers=[1], artifact_type="data")

# Explore metrics
filt.wide_df   # one row per artifact, metrics as columns
filt.tidy_df   # long format: one row per (artifact, metric_name)

# Set criteria and preview
filt.set_criteria([
    {"metric": "score", "operator": "gt", "value": 0.5},
])
filt.summary()           # per-criterion statistics and cumulative funnel
filt.plot()              # histograms with threshold lines (requires matplotlib)
filt.filtered_ids        # artifact IDs that pass
filt.filtered_wide_df    # wide DataFrame filtered to passing rows

# Commit as a pipeline step
result = filt.commit(step_name="interactive_filter")
# result.output("passthrough") is available for downstream steps
```

The `load()` step filter uses artifact origin steps across the store. It does
not select outputs accepted by a particular run. Use it when you intend that
origin-based selection; see [run selection](inspecting-provenance.md#select-the-run-you-want-to-inspect)
for the distinction.

The `load()` method discovers descendant metrics via forward provenance walk.
`set_criteria()` validates metric names against loaded data and checks for
ambiguous field names across steps. `commit()` writes step and execution records
to the Delta store, making the filtered result available for downstream wiring.

---

## Testing

Test curator operations by passing `dict[str, pl.DataFrame]` inputs directly.
Mock the `artifact_store` when the operation queries it:

```python
from unittest.mock import Mock

import polars as pl


def test_merge_combines_streams():
    """Test that merge combines artifact IDs from multiple streams."""
    inputs = {
        "stream_a": pl.DataFrame({"artifact_id": ["id_1", "id_2"]}),
        "stream_b": pl.DataFrame({"artifact_id": ["id_3"]}),
    }

    op = TaggedMerge()
    result = op.execute_curator(
        inputs=inputs,
        step_number=1,
        artifact_store=Mock(),
    )

    assert result.success
    assert set(result.passthrough["merged"]) == {"id_1", "id_2", "id_3"}
```

For operations that load artifact content (e.g., ingest operations), mock the
relevant `artifact_store` methods (`get_artifacts_by_type`, `load_metrics_df`,
etc.).

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `TypeError: must define OutputRole` | Missing `OutputRole(StrEnum)` inner class | Add enum with values matching `outputs` keys |
| `NotImplementedError` from `execute_curator` | Forgot to override the method | Implement `execute_curator` on your subclass |
| Empty `inputs` dict | Input role name mismatch | Check that `pipeline.run(inputs={...})` keys match what the operation expects |
| `ArtifactResult` with unfinalizable drafts | Missing `step_number` on `draft()` | Use the `step_number` parameter |
| `PassthroughResult` with invalid IDs | Passed artifact objects instead of ID strings | Use `artifact.artifact_id`, not the artifact itself |
| Operation used the external runner instead of staying local | Operation overrides `execute_function()` and is therefore a creator | Override `execute_curator()` — curators run in an isolated local subprocess |

---

## Verify

Confirm your operation works end-to-end in a minimal pipeline:

```python
from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.orchestration import StepStatus

pipeline = PipelineManager.create(
    name="test", delta_root="test/delta", staging_root="test/staging",
)
output = pipeline.output
pipeline.run(operation=DataGenerator, name="gen_a", params={"count": 3})
pipeline.run(operation=DataGenerator, name="gen_b", params={"count": 2})
step = pipeline.run(
    operation=SimpleMerge,
    name="merge",
    inputs=[output("gen_a", "datasets"), output("gen_b", "datasets")],
)
assert step.status is StepStatus.SUCCEEDED
pipeline.finalize()
```

---

## Cross-references

- [Writing Creator Operations](writing-creator-operations.md) — three-phase
  creator operations
- [Operations Model](../concepts/operations-model.md) — execution model details
