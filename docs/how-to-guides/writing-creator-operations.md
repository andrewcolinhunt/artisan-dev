# Write Creator Operations

How to build operations that run computation and produce artifacts using the
three-phase lifecycle.

**Prerequisites:** [Operations Model](../concepts/operations-model.md),
[Orientation](../getting-started/orientation.md)

**Key types:** `OperationDefinition`, `PreprocessInput`, `ExecuteInput`,
`PostprocessInput`, `ArtifactResult`

---

## Minimal working examples

### Transform existing data

A creator that consumes artifacts adds an `InputRole`, `inputs` spec, and
`preprocess`:

```python
from __future__ import annotations
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import (
    ArtifactResult,
    DataArtifact,
    ExecuteInput,
    InputSpec,
    OutputSpec,
    PostprocessInput,
    PreprocessInput,
)


class ScaleData(OperationDefinition):
    name = "scale_data"

    class InputRole(StrEnum):
        DATASET = "dataset"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(artifact_type="data", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            derives_from={"inputs": ["dataset"]},
        ),
    }

    class Params(BaseModel):
        factor: float = Field(default=2.0, ge=0.0, description="Multiplier for values.")

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {"datasets": PerArtifact([
            {"path": a.materialized_path, "source_id": a.artifact_id,
             "name": a.original_name}
            for a in inputs.input_artifacts["dataset"]
        ])}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        outputs = []
        for item in inputs.inputs["datasets"]:
            path = Path(item["path"])
            lines = path.read_text().splitlines()
            header, rows = lines[0], lines[1:]
            scaled = []
            for row in rows:
                parts = row.split(",")
                parts[1] = str(float(parts[1]) * self.params.factor)
                scaled.append(",".join(parts))
            out = Path(inputs.execute_dir) / path.name
            out.write_text(header + "\n" + "\n".join(scaled) + "\n")
            outputs.append({"path": str(out), "source_id": item["source_id"],
                            "name": f"{item['name']}_scaled.csv"})
        return {"outputs": outputs}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for item in inputs.memory_outputs["outputs"]:
            draft = DataArtifact.draft(
                content=Path(item["path"]).read_bytes(),
                original_name=item["name"],
                step_number=inputs.step_number,
            )
            result.add_artifact("dataset", draft,
                                sources={"dataset": [item["source_id"]]})
        return result
```

---

## Generate data without inputs

For a source operation, omit `InputRole`, declare `inputs = {}`, and use
`derives_from={"inputs": []}` for its outputs. The default preprocess
returns an empty dict. Its execute method can create files directly:

```python
def execute_function(self, inputs: ExecuteInput) -> None:
    Path(inputs.execute_dir, "hello.csv").write_text("id,value\n1,42\n")
```

Build drafts from those files in postprocess and explicitly return roots:
`ArtifactResult(artifacts={"datasets": drafts}, lineage={"datasets": []})`. See
`DataGenerator` in `artisan.operations.examples` for a complete source operation.

## How data flows through the three phases

For how data flows between the three phases, see
[Operations Model](../concepts/operations-model.md#the-creator-lifecycle).
The summary: `preprocess` adapts inputs (receives `PreprocessInput`, returns a
plain dict), `execute` runs computation (receives `ExecuteInput`, writes files
to `execute_dir`), `postprocess` constructs artifacts from results (receives
`PostprocessInput`, returns `ArtifactResult`).

The framework passes each phase's output to the next. You never call one
phase from another.

---

## Define metadata and role enums

Every operation needs a `name`. Operations with inputs define
`InputRole(StrEnum)` whose values match the `inputs` dict keys. Operations
with outputs define `OutputRole(StrEnum)` whose values match the `outputs`
dict keys. The framework validates this match at class definition time.

```python
class MyOp(OperationDefinition):
    name = "my_op"
    description = "Short human-readable summary"

    class InputRole(StrEnum):
        DATA = "data"

    class OutputRole(StrEnum):
        PROCESSED = "processed"
        SCORES = "scores"
```

Generative operations (no inputs) omit `InputRole`.

---

## Declare inputs and outputs

### Inputs

Each entry maps a role name to an `InputSpec`:

```python
inputs: ClassVar[dict[str, InputSpec]] = {
    InputRole.DATA: InputSpec(artifact_type="data", required=True),
}
```

By default, inputs are hydrated and materialized to files. Use `required=False`
for an optional role. See the [Python API](../reference/python-api.md) for the
complete input contract and available materialization options.

Set `materialize=False` for inputs you process in Python without needing a
file on disk (metrics, configs). When `materialize=False`, access content
directly via `artifact.content` instead of `artifact.materialized_path`.

### Outputs

Each entry maps a role name to an `OutputSpec`. Every creator output must set
`derives_from`, specifying the required and allowed parent roles for every
output. It does not select the parents:

```python
outputs: ClassVar[dict[str, OutputSpec]] = {
    OutputRole.PROCESSED: OutputSpec(
        artifact_type="data",
        derives_from={"inputs": ["data"]},
    ),
    OutputRole.SCORES: OutputSpec(
        artifact_type="metric",
        derives_from={"outputs": ["processed"]},
    ),
}
```

A required output role must be present and contain at least one artifact.
Missing roles and empty required lists fail execution. Set `required=False`
when an output may legitimately be absent. This validates each returned output
role; it does not enforce a fixed number of outputs per input artifact.

### Lineage patterns

| Pattern | Syntax | Use when |
|---------|--------|----------|
| Derived from input(s) | `{"inputs": ["role_name"]}` | Output transforms a named input |
| Derived from output | `{"outputs": ["role_name"]}` | Output derives from another output of the same operation |
| Generative | `{"inputs": []}` | Output has no parents |

`None` is only valid for passthrough outputs. Artifact-producing curators
follow the same explicit contract as creators. `{}` (empty dict) always raises
`ValidationError`. Combined `{"inputs": [...], "outputs": [...]}` is not
supported — use separate output roles instead.

---

## Add parameters

Group algorithm-specific configuration into a nested `Params` model. Use
Pydantic `Field` for defaults and validation:

```python
class Params(BaseModel):
    scale_factor: float = Field(
        default=1.5, ge=0.0, description="Multiplier for values."
    )
    seed: int | None = Field(default=None, description="Random seed.")


params: Params = Params()
```

Access parameter values in lifecycle methods through `self.params`, such as
`self.params.scale_factor`. Override them at the pipeline step level:

```python
pipeline.run(operation=MyOp, inputs=..., params={"scale_factor": 2.0})
```

Parameters are optional. If your operation has no configurable behavior, skip
this (see `MetricCalculator` in `artisan.operations.examples`).

---

## Implement preprocess

**Required** for operations with inputs. The framework raises `TypeError` at
class definition time if missing. Generative operations skip this (the
default returns `{}`).

`preprocess` receives `PreprocessInput` containing the materialized artifacts
and returns a plain dict. Carry the exact source ID and the chosen human name
alongside each file path, as in `ScaleData`:

```python
def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
    return {"datasets": PerArtifact([
        {"path": a.materialized_path, "source_id": a.artifact_id,
         "name": a.original_name}
        for a in inputs.input_artifacts["dataset"]
    ])}
```

`inputs.input_artifacts` is a `dict[str, list[Artifact]]` keyed by role name.
Each artifact's `materialized_path` points to the file the framework wrote to
the sandbox. The input artifacts are always lists, even when `artifacts_per_unit=1`.

Wrap each per-artifact value in `PerArtifact`. With the default per-artifact
dispatch, execute receives a one-element list for that value. An ordinary list
is shared unchanged with every invocation, so using one for file paths repeats
the whole batch. Shared configuration can remain unwrapped. If the operation
intentionally handles the whole batch in one call, declare
`per_artifact_dispatch = False`.

### Non-materialized inputs

For inputs with `materialize=False` in the `InputSpec`, access content
directly instead of using file paths:

```python
def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
    configs = inputs.input_artifacts["config"]
    return {"config": PerArtifact([a.content for a in configs])}
```

### Associated artifacts

Declare `with_associated` on the input spec, then call
`inputs.associated_artifacts(artifact, "data_annotation")` to obtain that input's
related artifacts. Preserve the same input order when preparing values for
`PerArtifact`. See the [input model docstrings](../reference/python-api.md)
for association access and grouping methods.

---

## Implement execute

**Required** for all creator operations. Receives `ExecuteInput` with the dict
from preprocess and a working directory.

Write output files to `inputs.execute_dir`. Access parameters via
`self.params`. The return value is passed to postprocess as
`inputs.memory_outputs` — return computed data when your outputs are in-memory
rather than file-based.

```python
def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
    outputs = []
    for item in inputs.inputs["datasets"]:
        path = Path(item["path"])
        transformed = do_something(path.read_text())  # Your transformation.
        out = Path(inputs.execute_dir) / path.name
        out.write_text(transformed)
        outputs.append({"path": str(out), "source_id": item["source_id"],
                        "name": f"{item['name']}_processed.csv"})
    return {"outputs": outputs}
```

`ExecuteInput` is frozen. Write outputs into its `execute_dir`; do not mutate
the input model or use the working directory from another phase.

---

## Implement postprocess

**Optional.** The default returns `ArtifactResult(success=True)` with no
artifacts. Override when your operation produces output artifacts.

`postprocess` receives `PostprocessInput` with two sources of data:
- `inputs.file_outputs` — all files found in `execute_dir` after execute ran
- `inputs.memory_outputs` — whatever `execute` returned

Build draft artifacts and return them under the declared output role, as in
`ScaleData` above. Leave finalization to the framework.

`original_name` is the human name you choose. The framework preserves it and
never derives parents from it. Include a lineage declaration for every emitted
role, and exact parents for every derived draft. The `add_artifact` helper in
`ScaleData` constructs those records; equivalent manual mappings are also valid.

The framework finalizes and validates the returned drafts. Read the
[public model docstrings](../reference/python-api.md) for additional
`PostprocessInput` and `ArtifactResult` fields.

---

## Common patterns

### Metric outputs (in-memory)

For a generative metric role declared with `derives_from={"inputs": []}`,
return values from execute and explicitly declare a root in postprocess:

```python
def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
    return {"accuracy": 0.95, "f1": 0.87}


def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
    metric = MetricArtifact.draft(
        content=inputs.memory_outputs,
        original_name=f"metrics_{inputs.step_number}",
        step_number=inputs.step_number,
    )
    return ArtifactResult(artifacts={"metrics": [metric]}, lineage={"metrics": []})
```

`MetricArtifact.draft()` accepts a `dict[str, Any]` for `content` (not bytes)
and JSON-encodes it internally.

### Multiple output roles

An operation can produce artifacts of different types in a single step. Declare
each as a separate output role with its own lineage:

```python
class OutputRole(StrEnum):
    DATASETS = "datasets"
    METRICS = "metrics"


outputs: ClassVar[dict[str, OutputSpec]] = {
    OutputRole.DATASETS: OutputSpec(
        artifact_type="data",
        derives_from={"inputs": []},
    ),
    OutputRole.METRICS: OutputSpec(
        artifact_type="metric",
        derives_from={"outputs": ["datasets"]},
    ),
}
```

The `{"outputs": ["datasets"]}` pattern links each metric to a co-produced
dataset. Add the dataset first, retain the index returned by `add_artifact`,
then declare that index as the metric's parent:

```python
result = ArtifactResult()
dataset_index = result.add_artifact("datasets", dataset, sources={})
result.add_artifact("metrics", metric, sources={"datasets": [dataset_index]})
```

See `DataGeneratorWithMetrics` in `artisan.operations.examples` for a complete
implementation, and the
[Co-Produced Outputs tutorial](../tutorials/09-writing-operations/03-co-produced-outputs.ipynb)
for a step-by-step walkthrough of authoring this pattern.

### Explicit lineage

Every artifact-producing operation declares exact parents. `derives_from`
constrains the parent roles; `ArtifactResult.lineage` supplies the actual edges.
Each emitted role must appear in both `artifacts` and `lineage`. For roots, use
an empty list. For an optional role with no drafts, either omit it from both
maps or supply empty lists in both.

Manual mappings and `add_artifact` construct the same records. Given a draft
`config` and the `dataset_id` it references, these two functions are equivalent:

```python
from artisan.schemas import ArtifactResult, ExecutionConfigArtifact, LineageMapping


def manual_config(dataset_id: str, config: ExecutionConfigArtifact) -> ArtifactResult:
    return ArtifactResult(
        artifacts={"config": [config]},
        lineage={"config": [LineageMapping(
            draft_index=0, source_role="dataset", source_artifact_id=dataset_id,
        )]},
    )


def helper_config(dataset_id: str, config: ExecutionConfigArtifact) -> ArtifactResult:
    result = ArtifactResult()
    result.add_artifact("config", config, sources={"dataset": [dataset_id]})
    return result
```

`draft_index` addresses the draft's position within its output role. For a
co-produced parent, use `source_output_index` in the named output role instead
of `source_artifact_id`. Names may repeat; preserve list order after declaring
indices. Invalid indices, wrong-role IDs, missing parents, duplicate mappings,
and undeclared parent roles fail validation.

The helper requires `sources`: strings name input IDs, integers name sibling
output indices, and `{}` declares a root. It never chooses parents. Several IDs
from one role are valid. For an output jointly derived from `data` and
`reference`, its spec lists both input roles and the operation supplies both:

```python
result.add_artifact(
    "aligned", aligned,
    sources={"data": [data.artifact_id], "reference": [reference.artifact_id]},
)
```

Artisan labels the declared parent set with a deterministic group ID. This
preserves the distinction between `S+A` and `S+B`; reference-only inputs do not
become parents because they happened to share an execution.

### Config references and ancestry

A config can contain `{"input": {"$artifact": dataset_id}, "scale_factor": 2}`.
Materialization substitutes the dataset's local path into the tool's config.
The producing operation separately declares `dataset -> config`, using either
form above and `derives_from={"inputs": ["dataset"]}`. The config's stored
reference remains unchanged.

For several referenced parents, an operation may call
`config.get_artifact_references()`, deduplicate those IDs, choose the corresponding
input roles, and declare them. The executor never scans config content for
parents. See `DataTransformerConfig` in `artisan.operations.examples` for a
parameter sweep that preserves every config's dataset ancestry.

### Command operations (external tools)

A command operation declares `ToolSpec` and implements `execute_command`.
Return the argument list; the framework invokes it in the execute directory and
captures its log and command evidence. Prepared per-artifact file values arrive
as scalar paths in this method.

The following complete operation copies each input CSV using the system `cp`
command. It requires `cp` in the selected environment:

```python
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.operations.lineage import match_outputs_to_inputs_by_stem
from artisan.schemas import (
    ArtifactResult,
    DataArtifact,
    InputSpec,
    OutputSpec,
    PostprocessInput,
    PreprocessInput,
    ToolSpec,
)


class CopyCsv(OperationDefinition):
    name = "copy_csv"

    class InputRole(StrEnum):
        DATASET = "dataset"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "dataset": InputSpec(artifact_type="data"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "dataset": OutputSpec(
            artifact_type="data", derives_from={"inputs": ["dataset"]}
        ),
    }
    tool: ToolSpec = ToolSpec(executable="cp")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            "dataset": PerArtifact(
                [a.materialized_path for a in inputs.input_artifacts["dataset"]]
            )
        }

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        source = str(inputs["dataset"])
        return [*self.tool.parts(), source, Path(source).name]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        paths = [Path(f) for f in inputs.file_outputs if f.endswith(".csv")]
        sources = inputs.input_artifacts["dataset"]
        source_by_id = {a.artifact_id: a for a in sources}
        parent_ids = match_outputs_to_inputs_by_stem(
            [str(path) for path in paths],
            [(a.materialized_path, a.artifact_id) for a in sources],
        )
        result = ArtifactResult()
        for path, parent_id in zip(paths, parent_ids, strict=True):
            source = source_by_id[parent_id]
            draft = DataArtifact.draft(
                content=path.read_bytes(),
                original_name=f"{source.original_name}.csv",
                step_number=inputs.step_number,
            )
            result.add_artifact("dataset", draft, sources={"dataset": [parent_id]})
        return result
```

Use relative output paths because the framework sets the command's working
directory. For another external tool, replace `ToolSpec` and the returned
arguments with its CLI. Keep preprocess and postprocess responsible for the
artifact boundary.

Here the operation calls the public matcher explicitly, choosing materialized
input basenames as its candidates. Unmatched or ambiguous names raise an error.
The framework never calls this helper. Tools whose filenames do not identify
parents can write an operation-owned manifest with relative output paths and
exact source IDs, then read it in postprocess. Command transport does not return
Python memory outputs.

`WaitTool` in `artisan.operations.examples` is another complete command example.
A Python `execute_function` that calls `run_command` manually is still a function
operation; that form runs where the lifecycle worker runs. For remote command
execution, follow [Deploy Tool Endpoints](deploying-tool-endpoints.md).

(execute-as-tool)=
### Python body as a command (`execute_as_tool`)

A function op can only run where the lifecycle worker runs. To make a
Python body deployable like a tool — a subprocess locally, a Modal tool
endpoint remotely, a standalone container CLI — set one flag:

```python
class EmbedSequences(OperationDefinition):
    name = "embed_sequences"
    execute_as_tool: ClassVar[bool] = True  # the entire opt-in
    ...

    class Params(BaseModel):
        batch_size: int = Field(default=8, description="Sequences per batch.")

    params: Params = Params()

    def execute_function(
        self, inputs: ExecuteInput
    ) -> None: ...  # read input files, write output files to inputs.execute_dir
```

No `ToolSpec`, no `execute_command` — the framework supplies the command
(`artisan op run <module:Qualname> --params … --inputs …`) and the op is
a command op everywhere: `artisan modal deploy` accepts it,
`compute_provider='modal'` routes it, and a container with the op's
package baked in runs it with no orchestration at all.

The flag declares a **file-shaped contract**, enforced at class
definition where possible:

- Prepared inputs are file paths, JSON-serializable. Wrap per-artifact
  values in `PerArtifact` — raw lists pass through whole, as shared
  data, to every per-artifact subprocess. Scalars belong in `Params`.
- Outputs are files written to `execute_dir`; `execute_function`
  returns `None` (a non-`None` return is a runtime error).
- All per-run config lives in the nested `Params` model — it is the
  only payload that crosses the process and wire boundaries. Top-level
  fields, a `params` field not typed as the nested `Params` class, a
  `ToolSpec`, or an `execute_command` override all fail at import.
- `ExecuteInput.metadata` and `files_dir` are unavailable, and
  `log_path` is a throwaway — log to stdout/stderr, which the framework
  captures to the unit log.
- The op's module and artisan must be importable wherever the command
  runs: baked into the container image (see
  [Op Container Images](op-container-images.md)), installed in the
  active environment locally.

When to choose what:

| Op shape | Use |
|----------|-----|
| Python body, results used in-process, no remote story needed | Plain function op |
| External binary with its own CLI | `tool` + `execute_command()` |
| Python body that should deploy like a tool (GPU model, heavy transform) | `execute_as_tool = True` |

Each command starts a subprocess. For short transforms, measure that overhead
before choosing this execution form.

See `CsvHead` in `artisan.operations.examples` for the complete
reference implementation.

### Multi-input operations

When an operation consumes multiple input roles, set `group_by` to control
how artifacts are paired across roles, and use `inputs.grouped()` in
preprocess:

```python
from artisan.schemas import GroupByStrategy


class AlignOp(OperationDefinition):
    name = "align"
    group_by: GroupByStrategy | None = GroupByStrategy.LINEAGE
    ...

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            "pairs": PerArtifact(
                [
                    {
                        "data": g["data"].materialized_path,
                        "reference": g["reference"].materialized_path,
                        "data_id": g["data"].artifact_id,
                        "reference_id": g["reference"].artifact_id,
                    }
                    for g in inputs.grouped()
                ]
            )
        }
```

| Strategy | Behavior | Use when |
|----------|----------|----------|
| `LINEAGE` | Follows a directed provenance path from a candidate to a target ancestor | Pair an artifact with its ancestor; sibling branches sharing a root do not qualify |
| `ZIP` | Pairs by position (index-aligned) | Inputs in a known, consistent order |
| `CROSS_PRODUCT` | Every combination across roles | Every input combined with every other |
| `NAME` | Pairs artifacts whose `original_name` stems match | Independently-ingested streams that share filename conventions but no ancestry |

#### Pairing by name

Use `NAME` when two (or more) input roles come from independent ingest
operations — no shared ancestry — but their artifacts share filename
conventions. Stems are computed by stripping all extensions
(`data.tar.gz` → `data`, `data.csv` → `data`), so artifacts that
represent the same logical entity in different formats pair naturally.

```python
class JoinByName(OperationDefinition):
    name = "join_by_name"
    group_by: GroupByStrategy | None = GroupByStrategy.NAME

    class InputRole(StrEnum):
        sequences = "sequences"
        annotations = "annotations"
```

Each role must have unique stems among artifacts with an
`original_name`; duplicates raise `ValueError` at the pairing phase.
Note that `run.log` and `run.cfg` collide to the stem `run` and will
trip the uniqueness check — encode semantic suffixes in the base name
(`run_log.txt`, `run_cfg.txt`) when they need to coexist within one
role.

### Resources and execution config

Set defaults on the class. Override per-step at the pipeline level:

```python
class HeavyOp(OperationDefinition):
    name = "heavy_op"
    runner_resources: RunnerResources = RunnerResources(
        cpus=4,
        memory_gb=32,
        gpus=1,
        extra={"slurm_partition": "gpu"},
    )
    batch_strategy: BatchStrategy = BatchStrategy(
        artifacts_per_unit=5,
        estimated_seconds=3600.0,
    )
    ...
```

See [Configuring Execution](configuring-execution.md) for resource and batching recipes.

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `TypeError: must define OutputRole` | Missing `OutputRole(StrEnum)` inner class | Add enum with values matching `outputs` keys |
| `TypeError: must define InputRole` | Missing `InputRole(StrEnum)` inner class | Add enum with values matching `inputs` keys |
| `TypeError: must implement preprocess()` | Creator with non-empty `inputs` but no preprocess | Override `preprocess()` |
| `TypeError: must set derives_from` | Creator output with `derives_from=None` | Set to `{"inputs": [...]}` or `{"inputs": []}` |
| `ValidationError` on `OutputSpec` | Used `{}` for lineage | Use `{"inputs": []}` for generative outputs |
| `ValidationError` on `OutputSpec` | Combined `{"inputs": [...], "outputs": [...]}` | Use separate output roles instead |
| Empty artifacts after postprocess | Wrong file extension filter or missing files | Check `file_outputs` contents in the execute directory |
| Missing lineage | An emitted role or derived draft has no declaration | Return exact parents for each derived draft, and an empty list for roots |
| `ValidationError` on `LineageMapping` | Set both source fields, or neither | Use `source_artifact_id` for input parents or `source_output_index` for co-produced outputs |
| `LineageIntegrityError` | An index is outside the declared role or an input ID belongs to another role | Preserve role-local output order and carry the exact source IDs |
| `ValueError: materialize_as requires materialize=True` | Set `materialize_as` on a non-materialized input | Remove `materialize_as` or set `materialize=True` |

---

## Verify

Test your operation outside a pipeline by constructing inputs directly:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from artisan.schemas import ExecuteInput, PostprocessInput

op = ScaleData(params={"factor": 3.0})

with TemporaryDirectory() as tmp:
    execute_dir = Path(tmp) / "execute"
    execute_dir.mkdir()

    # Write a test input file
    test_csv = Path(tmp) / "test.csv"
    test_csv.write_text("id,value\n1,10\n2,20\n")

    source = DataArtifact.draft(content=test_csv.read_bytes(),
                                original_name="test.csv", step_number=0).finalize()
    source.materialized_path = str(test_csv)
    prepared = op.preprocess(PreprocessInput(
        preprocess_dir=str(Path(tmp) / "pre"), input_artifacts={"dataset": [source]},
    ))
    execute_input = ExecuteInput(execute_dir=str(execute_dir), inputs=prepared)
    result = op.execute_function(execute_input)

    # Run postprocess
    post_input = PostprocessInput(
        step_number=0,
        postprocess_dir=str(Path(tmp) / "post"),
        file_outputs=[str(path) for path in execute_dir.iterdir()],
        memory_outputs=result,
    )
    artifact_result = op.postprocess(post_input)

    assert artifact_result.success
    assert artifact_result.lineage["dataset"][0].source_artifact_id == source.artifact_id
    assert len(artifact_result.artifacts["dataset"]) == 1
    assert (
        artifact_result.artifacts["dataset"][0].content == b"id,value\n1,30.0\n2,60.0\n"
    )
```

For a full integration test, run in a pipeline (defaults to local backend):

```python
from artisan.orchestration import PipelineManager
from artisan.orchestration import StepStatus
from artisan.operations.examples import DataGenerator

pipeline = PipelineManager.create(
    name="test",
    delta_root="test/delta",
    staging_root="test/staging",
)
output = pipeline.output
pipeline.run(operation=DataGenerator, name="source", params={"count": 3})
step = pipeline.run(
    operation=ScaleData,
    inputs={"dataset": output("source", "datasets")},
    batch_strategy={"artifacts_per_unit": 3},
)
assert step.status is StepStatus.SUCCEEDED
assert step.succeeded_count == 3
pipeline.finalize()
```

---

## Cross-references

- [Operations Model](../concepts/operations-model.md) — why the three-phase
  lifecycle exists
- [Configuring Execution](configuring-execution.md) — resources, batching,
  backends
- [Writing Curator Operations](writing-curator-operations.md) — filter, merge,
  ingest operations
- [Build a Pipeline](building-a-pipeline.md) — wiring operations into pipelines
- [Op Container Images](op-container-images.md) — the image contract behind
  `execute_as_tool` and external tool deployment
