# Write Curator Operations

Use a curator to route, filter, merge, or ingest artifacts in an isolated local
subprocess. Use a [creator](writing-creator-operations.md) for heavy computation
or external tools.

Before writing a custom curator, check whether a
[built-in operation](#built-in-curator-operations) already does what you need.

**Prerequisites:** [Operations Model](../concepts/operations-model.md),
[Writing Creator Operations](writing-creator-operations.md).

---

## Minimal working example

This example implements a merge to show the curator contract. For ordinary
pipeline merging, use the built-in `Merge` operation.

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
            pl.concat([frame.select("artifact_id") for frame in inputs.values()])
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

| | Creator | Curator |
|---|---------|---------|
| **Purpose** | Heavy computation, file I/O | Route, filter, merge, or ingest artifacts |
| **Execution** | Three phases (`preprocess` / `execute_function` / `postprocess`) | Single `execute_curator` method |
| **Sandboxing** | Full sandbox with file materialization | No working sandbox; memory-isolated subprocess |
| **Dispatch** | Built-in local runner or an external provider instance | Local spawned subprocess; step-runner overrides do not relocate it |
| **Returns** | `ArtifactResult` containing any new artifacts | `ArtifactResult`, `ArtifactStreamResult`, or `PassthroughResult` |

---

## Choose a return type

Return existing IDs with `PassthroughResult`. Return new drafts as a complete
`ArtifactResult` collection or incrementally with `ArtifactStreamResult`.

### `PassthroughResult` — route existing artifacts

Return a subset or union of input IDs, as `Filter` and `Merge` do:

```python
from artisan.schemas import PassthroughResult

return PassthroughResult(
    success=True,
    passthrough={"output_role": ["artifact_id_1", "artifact_id_2"]},
)
```

The `passthrough` dict maps output role names to lists of artifact ID strings.

### `ArtifactResult` — create new artifacts

Artifact-producing operations return new drafts. In this fragment, `file_ref` is the
input read into `file_bytes`; the output spec declares
`derives_from={"inputs": ["file"]}`.

```python
from artisan.schemas import ArtifactResult, CSVArtifact, LineageMapping

drafts = [
    CSVArtifact.draft(
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

### `ArtifactStreamResult` — emit one draft at a time

Use a stream when each output can be converted independently. `IngestFiles`
already provides this behavior: implement `convert_file()` as shown
[below](#pattern-c-ingest-new-artifacts), and the framework stages each draft
before requesting the next conversion.

For a custom ingestion loop, return an iterator of `ArtifactEmission` values.
This fragment assumes resolved `file_refs`, a configured filesystem `fs`, and an
output role `csv` with `derives_from={"inputs": ["file"]}`:

```python
from artisan.schemas import ArtifactEmission, ArtifactStreamResult, CSVArtifact

def emissions():
    for file_ref in file_refs:
        yield ArtifactEmission(
            role="csv",
            artifact=CSVArtifact.draft(
                content=file_ref.model_copy().read_content(fs=fs),
                original_name=f"{file_ref.original_name}{file_ref.extension or ''}",
                step_number=step_number,
            ),
            sources={"file": [file_ref.artifact_id]},
        )

return ArtifactStreamResult(items=emissions(), metadata={"source": "import"})
```

After yielding, do not retain or mutate the draft. Source IDs must be strings;
every output must declare its exact input parents. Streams require static output
roles and support input-parent or root lineage; use `sources={}` for a root.
Sibling-output indices and dynamic roles require `ArtifactResult`.

The complete iterator belongs to one execution and cache unit. A conversion,
validation, iterator-close, or staging failure prevents acceptance of all its
converted outputs. Retained temporary staging is diagnostic evidence, not a
partially imported result. FileRef registration happens before conversion and
may remain in the store after failure. Replay starts again at the first input.

Execution metadata is copied before iteration, including nested values. Changes
made by the iterator do not change the recorded metadata. The framework closes
the iterator on both success and failure when it provides `close()`.

CSV conversion still reads and parses one complete file. Memory includes the
largest conversion, bounded staging buffers, and input/provenance metadata.
`IngestFiles` gives each conversion a disposable FileRef copy so content caching
does not retain all source files. Custom converters must also avoid retaining
their own payload collections. Advance an existing custom ingester's operation
version when adopting this execution contract.

### Preserve result roles and metadata

A curator returning `ArtifactResult` with an empty `outputs` declaration may
return dynamic artifact roles.
After commitment, its `StepResult.output_roles` and `output_types` describe the
roles that contain accepted artifacts. A role containing one type reports that
type; a role containing several types reports `ArtifactTypes.ANY`. Empty dynamic
results have no output roles. Use the completed result's `output(role)` to
reference a dynamic role; asynchronous handles expose the declared contract.
Curators with declared outputs retain those
descriptors, including optional roles with no artifacts.

Operation-authored metadata on all three result models
are available at the top level of `StepResult.metadata`. They survive cache reuse
and pipeline reopening. Reserve `timings`, `skip_reason`, `status`, `cancelled`,
and `skipped` for the framework; returning any of those keys fails the execution.
Step timings describe the current attempt, while execution timings remain on the
execution record.

For example, `IngestPipelineStep` publishes its actual imported roles and retains
`metadata["ingest_source"]`, including the exact source attempt IDs. Reopening
reads the committed result; it does not repeat the import. `IngestPipelineStep`
remains non-cacheable and performs a fresh source read each time it runs.

---

## Define the operation class

A curator subclasses `OperationDefinition` and overrides `execute_curator`;
no operation-type flag is needed. Import its module in every
process that uses the operation; see
[application loading](configuring-execution.md#load-application-definitions-in-every-process).

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

For curator input and output validation:

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
) -> PassthroughResult | ArtifactResult | ArtifactStreamResult:
    ...
```

`inputs` maps roles to Polars DataFrames with at least an `artifact_id` column.
Load content, metrics, or provenance through `artifact_store`. Use `step_number`
when creating drafts.

---

## Implement `execute_curator`

The following classes use the imports from the `SimpleMerge` example.

### Pattern A: Filter (passthrough)

Return the IDs of artifacts whose `original_name` matches a pattern:

```python
from enum import StrEnum, auto

from pydantic import BaseModel, Field

from artisan.schemas import ArtifactTypes


class NameFilter(OperationDefinition):
    name = "name_filter"

    class InputRole(StrEnum):
        passthrough = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.passthrough: InputSpec(
            artifact_type=ArtifactTypes.CSV, hydrate=False, materialize=False
        ),
    }

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
        artifacts = artifact_store.get_artifacts_by_type(ids, ArtifactTypes.CSV)
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
        merged = pl.concat(
            [frame.select("artifact_id") for frame in inputs.values()]
        )
        return PassthroughResult(
            success=True,
            passthrough={"merged": merged["artifact_id"].to_list()},
        )
```

### Pattern C: Ingest (new artifacts)

Subclass `IngestFiles` and implement `convert_file()` to create artifacts from
external files. The base class resolves every input before conversion, then
emits one draft at a time in input order, including repeated occurrences:

```python
from artisan.operations.curator import IngestFiles
from artisan.schemas import CSVArtifact, FileRefArtifact


class IngestProjectCSV(IngestFiles):
    name = "ingest_project_csv"

    class OutputRole(StrEnum):
        csv = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.csv: OutputSpec(
            artifact_type="csv",
            derives_from={"inputs": ["file"]},
        ),
    }

    def convert_file(
        self, file_ref: FileRefArtifact, step_number: int, *, fs=None
    ) -> CSVArtifact:
        content = file_ref.read_content(fs=fs)
        filename = f"{file_ref.original_name}{file_ref.extension or ''}"
        return CSVArtifact.draft(
            content=content,
            original_name=filename,
            step_number=step_number,
        )
```

Usage:

```python
pipeline.run(
    operation=IngestProjectCSV, name="ingest", inputs=["/data/a.csv", "/data/b.csv"]
)
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

For a generative role, set `derives_from={"inputs": []}` and return
`lineage={"role": []}` alongside its artifact list. A curator with dynamic
output types and `outputs={}` may emit only roots and must include an empty
lineage list for every returned role. `IngestPipelineStep` uses this deliberately
to import artifacts as new roots, including configs; it does not preserve
source-pipeline ancestry or create edges to foreign IDs.

---

(built-in-curator-operations)=
## Built-in curator operations

(filter)=
### Filter

Keep artifacts whose metrics pass all criteria:

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

Use bare metric field names. `Filter` finds descendant metrics through provenance;
add `step` or `step_number` when field names collide:

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

Supported operators: `gt`, `ge`, `lt`, `le`, `eq`, `ne`.

Set `passthrough_failures=True` to preview removals while retaining every artifact:

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

Union multiple artifact streams into one. The curator routes IDs without
consuming artifact content. Pipeline preparation still verifies input integrity.

```python
from artisan.operations.curator import Merge

# List format combines the streams into one input role.
pipeline.run(
    operation=Merge,
    name="merge",
    inputs=[output("branch_a", "results"), output("branch_b", "results")],
)
# Output role is always "merged": output("merge", "merged")
```

### IngestCSV

Import CSV files from disk as `CSVArtifact` objects:

```python
from artisan.operations.curator import IngestCSV

pipeline.run(operation=IngestCSV, name="ingest", inputs=["/data/a.csv", "/data/b.csv"])
# Output role: "csv" → output("ingest", "csv")
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
        "artifact_type": "csv",  # optional: filter by type
    },
)
```

Omit `include_prior_steps` to select only step 3. Both modes select the latest
attempt at each chosen position and import its accepted outputs, including
cached and passthrough results. Failed executions contribute no artifacts;
successful executions of a partial step do. The boundary must exist, and
selected pending or running attempts fail the import.

With `include_prior_steps=True`, the import combines outputs through the selected
step and removes duplicates. If step 0 produces A and B and step 1 filters to A,
importing only step 1 gives A; importing through step 1 gives A and B. Select the
final filter step alone when you want its surviving set.

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

Inspect metric distributions in a notebook, choose thresholds, then commit the
filtered result as a pipeline step:

```python
from artisan.operations.curator import InteractiveFilter

filt = InteractiveFilter(delta_root="/runs/my_pipeline/delta")
filt.load(step_numbers=[1], artifact_type="csv")

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
pipeline.run(operation=DataGenerator, name="gen_a", params={"count": 3, "seed": 42})
pipeline.run(operation=DataGenerator, name="gen_b", params={"count": 2, "seed": 43})
step = pipeline.run(
    operation=SimpleMerge,
    name="merge",
    inputs=[output("gen_a", "datasets"), output("gen_b", "datasets")],
)
assert step.status is StepStatus.SUCCEEDED
assert step.output_roles == {"merged"}
assert pipeline.finalize()["overall_success"]
```

---

## Cross-references

- [Writing Creator Operations](writing-creator-operations.md) — three-phase
  creator operations
- [Operations Model](../concepts/operations-model.md) — execution model details
