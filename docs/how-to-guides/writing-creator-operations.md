# Write Creator Operations

Write a creator to prepare inputs, run computation, and return artifacts with
explicit parents.

**Prerequisites:** [Operations Model](../concepts/operations-model.md),
[Orientation](../getting-started/orientation.md)

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
    CSVArtifact,
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
        InputRole.DATASET: InputSpec(artifact_type="csv", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="csv",
            derives_from={"inputs": ["dataset"]},
        ),
    }

    class Params(BaseModel):
        factor: float = Field(default=2.0, ge=0.0, description="Multiplier for values.")

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            "datasets": PerArtifact(
                [
                    {
                        "path": a.materialized_path,
                        "source_id": a.artifact_id,
                        "name": a.original_name,
                    }
                    for a in inputs.input_artifacts["dataset"]
                ]
            )
        }

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
            outputs.append(
                {
                    "path": str(out),
                    "source_id": item["source_id"],
                    "name": f"{item['name']}_scaled.csv",
                }
            )
        return {"outputs": outputs}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for item in inputs.memory_outputs["outputs"]:
            draft = CSVArtifact.draft(
                content=Path(item["path"]).read_bytes(),
                original_name=item["name"],
                step_number=inputs.step_number,
            )
            result.add_artifact(
                "dataset", draft, sources={"dataset": [item["source_id"]]}
            )
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

Artisan passes prepared inputs to execute, then passes its files and return
value to postprocess. Do not call one phase from another. See
[Operations Model](../concepts/operations-model.md#the-creator-lifecycle) for
the lifecycle and execution boundaries.

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
    InputRole.DATA: InputSpec(artifact_type="csv", required=True),
}
```

By default, inputs are hydrated and materialized to files. Use `required=False`
for an optional role. See the [Python API](../reference/python-api.md) for the
complete input contract and available materialization options.

Set `materialize=False` for inputs you read directly through `artifact.content`,
such as metrics or configs.

### Outputs

Map each output role to an `OutputSpec`. Set `derives_from` to declare its
required and allowed parent roles; select exact parents in postprocess:

```python
outputs: ClassVar[dict[str, OutputSpec]] = {
    OutputRole.PROCESSED: OutputSpec(
        artifact_type="csv",
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

Inside the operation class, group algorithm-specific configuration into a
nested `Params` model. Annotate `params` with that exact model. Use Pydantic
`Field` for defaults and validation:

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

Parameterless operations omit both `Params` and `params`; see `MetricCalculator`
in `artisan.operations.examples`. Subclasses can inherit the pair unchanged or
replace both together. Flat algorithm fields on an operation are rejected.

---

(creator-preprocess)=
## Implement preprocess

Operations with inputs must implement `preprocess`; omitting it raises
`TypeError` at class definition. For generative operations, the default method
returns `{}`.

Return a dict of prepared values from `PreprocessInput`. Carry the exact source
ID and chosen name alongside each file path, as in `ScaleData`:

```python
def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
    return {
        "datasets": PerArtifact(
            [
                {
                    "path": a.materialized_path,
                    "source_id": a.artifact_id,
                    "name": a.original_name,
                }
                for a in inputs.input_artifacts["dataset"]
            ]
        )
    }
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

Every creator implements execute. It receives the prepared dict in `ExecuteInput`.
Write output files to `inputs.execute_dir` and read parameters from `self.params`.
Return in-memory results for postprocess to read through `inputs.memory_outputs`.

```python
def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
    outputs = []
    for item in inputs.inputs["datasets"]:
        path = Path(item["path"])
        transformed = do_something(path.read_text())  # Your transformation.
        out = Path(inputs.execute_dir) / path.name
        out.write_text(transformed)
        outputs.append(
            {
                "path": str(out),
                "source_id": item["source_id"],
                "name": f"{item['name']}_processed.csv",
            }
        )
    return {"outputs": outputs}
```

`ExecuteInput` is frozen. Do not mutate it or use another phase's working directory.

---

## Implement postprocess

Override `postprocess` when the operation produces artifacts; the default returns
`ArtifactResult(success=True)` with none.

`postprocess` receives `PostprocessInput` with two sources of data:

- `inputs.file_outputs` — all files found in `execute_dir` after execute ran
- `inputs.memory_outputs` — whatever `execute` returned

Build drafts under the declared output roles, as in `ScaleData`.
For complete external files, follow [Retain command-generated files](#retain-command-files).

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
        artifact_type="csv",
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

(creator-explicit-lineage)=
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
        lineage={
            "config": [
                LineageMapping(
                    draft_index=0,
                    source_role="dataset",
                    source_artifact_id=dataset_id,
                )
            ]
        },
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
    "aligned",
    aligned,
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

### Read typed config dependencies

When preprocessing needs the referenced artifact objects, declare the config
input with reference resolution enabled:

```python
inputs = {
    "config": InputSpec(
        artifact_type="config", materialize=False, resolve_references=True
    )
}
```

Inside `preprocess` or `postprocess`, call
`inputs.referenced_artifacts(config)` for a config in
`inputs.input_artifacts["config"]`. The returned read-only mapping contains its
direct dependencies, keyed by artifact ID. Each dependency is hydrated and has
a local `materialized_path`; repeated references share the same object. Config
markers stay unchanged, and dependencies remain available through postprocessing.
Malformed IDs, missing or corrupt artifacts, and references to other configs fail
before preprocessing. The input spec must set `materialize=False` and retain
`hydrate=True` (the default); invalid combinations fail when the spec is created.

For endpoint execution, use these dependencies to prepare portable files under
`inputs.preprocess_dir`, then return those files as declared execute inputs. Write
required values or bytes into them; paths inside file contents are not rewritten
when files move to a worker. The typed objects stay with the lifecycle worker.
Check your preparation by moving the prepared files to another directory before
execution and verifying that the result is unchanged.

For example, this preprocess fragment embeds referenced UTF-8 text in a JSON
file. It uses the imports from `ScaleData` plus `json` and
`ExecutionConfigArtifact`:

```python
import json

from artisan.schemas import ExecutionConfigArtifact


def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
    paths = []
    for config in inputs.input_artifacts["config"]:
        if not isinstance(config, ExecutionConfigArtifact):
            raise TypeError("Expected an execution config")
        references = inputs.referenced_artifacts(config)
        payload = {
            "config_id": config.artifact_id,
            "contents": [
                Path(references[ref_id].materialized_path).read_text()
                for ref_id in config.get_artifact_references()
            ],
        }
        path = Path(inputs.preprocess_dir) / f"{config.artifact_id}.json"
        path.write_text(json.dumps(payload))
        paths.append(str(path))
    return {"config": PerArtifact(paths)}
```

The execute phase reads the prepared JSON file. Its contents have no dependency
on the original sandbox paths. Choose a format suited to your tool and data;
large or binary inputs need a different packaging choice.

Declare each output's config parent explicitly. Resolving dependencies does not
add lineage edges. Replay reloads the same referenced IDs and verifies their
content, so the source store and external content must remain available after
working-directory cleanup.
These resolved dependencies are not additional input roles or a pairing scheme.
When adopting a different input or output contract, increment the operation's
`version` to invalidate existing execution caches. Replay also checks the recorded
reference-resolution setting against the current input declaration.

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
    CSVArtifact,
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
        "dataset": InputSpec(artifact_type="csv"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "dataset": OutputSpec(
            artifact_type="csv", derives_from={"inputs": ["dataset"]}
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
            draft = CSVArtifact.draft(
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

(retain-command-files)=
### Retain command-generated files

To retain a complete command output as a `FileRefArtifact`, copy it into
`inputs.files_dir` during postprocessing and draft the reference from that copy.
Artisan relocates referenced files into managed storage, verifies their bytes,
and removes the successful execution's sandbox. Files left in `execute_dir` or
`postprocess_dir` remain temporary. See the `PostprocessInput` docstring through
[Python API lookup](../reference/python-api.md#inspect-the-installed-version)
for the directory's lifetime and configuration contract.

This variant continues the `CopyCsv` example above. It keeps the same command
and explicit filename-matching contract, but returns file references. Each copy
gets its own staging subdirectory so repeated basenames cannot overwrite one
another. The original display name and extension remain separate from that path.

```python
import hashlib
import shutil

import xxhash

from artisan.schemas import FileRefArtifact


class RetainCsvFiles(CopyCsv):
    name = "retain_csv_files"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "dataset": OutputSpec(
            artifact_type="file_ref", derives_from={"inputs": ["dataset"]}
        ),
    }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        if inputs.files_dir is None:
            raise ValueError("Configure managed file storage to retain command outputs")
        paths = [Path(f) for f in inputs.file_outputs if f.endswith(".csv")]
        sources = inputs.input_artifacts["dataset"]
        source_by_id = {a.artifact_id: a for a in sources}
        parent_ids = match_outputs_to_inputs_by_stem(
            [str(path) for path in paths],
            [(a.materialized_path, a.artifact_id) for a in sources],
        )
        result = ArtifactResult()
        for index, (path, parent_id) in enumerate(zip(paths, parent_ids, strict=True)):
            source = source_by_id[parent_id]
            staged = Path(inputs.files_dir) / str(index) / path.name
            staged.parent.mkdir(parents=True)
            shutil.copyfile(path, staged)
            with staged.open("rb") as stream:
                digest = hashlib.file_digest(stream, xxhash.xxh3_128).hexdigest()
            draft = FileRefArtifact.draft(
                path=str(staged),
                content_hash=digest,
                size_bytes=staged.stat().st_size,
                step_number=inputs.step_number,
                original_name=source.original_name,
                extension=source.extension,
            )
            result.add_artifact("dataset", draft, sources={"dataset": [parent_id]})
        return result
```

Hashing reads the copied file incrementally. Copying also leaves the command's
original output available if postprocessing fails. For tools whose filenames do
not identify parents, select files and their exact parent IDs from the tool's
output manifest instead. Separate paths do not force separate artifact IDs:
identical bytes and semantic metadata retain the existing deduplication behavior.

The same postprocess code handles local commands and endpoint downloads. The
managed directory stays on the lifecycle worker; command bodies and remote
workers do not receive its path. Existing endpoint input constraints still apply:
send a batch manifest file when a whole batch must travel under one input role.
Retention requires the whole execution unit to succeed, including postprocessing
and verification. This does not publish partial
state from interrupted commands. Failed uploads can leave unreferenced objects;
their cleanup is outside this workflow.

Run the variant in a fresh temporary store, then hydrate new references after
the pipeline closes. This continuation requires the `CopyCsv` and `RetainCsvFiles`
definitions above and the local `cp` command:

```python
from tempfile import TemporaryDirectory

from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.storage import ArtifactStore

with TemporaryDirectory() as tmp:
    root = Path(tmp)
    pipeline = PipelineManager.create(
        name="retained-files",
        delta_root=str(root / "delta"),
        staging_root=str(root / "staging"),
        working_root=str(root / "working"),
        files_root=str(root / "files"),
    )
    generated = pipeline.run(DataGenerator, params={"count": 2, "seed": 42})
    retained = pipeline.run(
        RetainCsvFiles,
        inputs={"dataset": generated.output("datasets")},
        batch_strategy={"artifacts_per_unit": 2},
    )
    assert pipeline.finalize()["overall_success"]
    assert not any((root / "working").rglob("*.csv"))

    store = ArtifactStore(str(root / "delta"), files_root=str(root / "files"))
    ids = store.provenance.load_artifact_ids_by_type(
        "file_ref", step_numbers=[retained.step_number]
    )
    assert len(ids) == 2
    for artifact_id in ids:
        file_ref = store.get_artifact(artifact_id, artifact_type="file_ref")
        assert file_ref.read_content().startswith(b"id,x,y,z,score")
```

The origin-step lookup here selects files created in this fresh store. For an
existing store with cached steps or multiple runs, select the accepted output
IDs through [run-scoped inspection](inspecting-evidence.md).

(execute-as-tool)=
### Python body as a command (`execute_as_tool`)

Set `execute_as_tool=True` to run a Python body as a local subprocess, Modal
endpoint, or standalone container command:

```python
class EmbedSequences(OperationDefinition):
    name = "embed_sequences"
    execute_as_tool: ClassVar[bool] = True
    ...

    class Params(BaseModel):
        batch_size: int = Field(default=8, description="Sequences per batch.")

    params: Params = Params()

    def execute_function(
        self, inputs: ExecuteInput
    ) -> None: ...  # read input files, write output files to inputs.execute_dir
```

The framework supplies the command
(`artisan op run <module:Qualname> --params … --inputs …`) and uses the command
execution path: `artisan modal deploy` accepts it,
`compute_provider='modal'` routes it, and a container with the operation's
package installed can run it without a pipeline.

This mode exchanges files across process boundaries. Validation happens at
class definition where possible:

- Prepared inputs are file paths, JSON-serializable. Wrap per-artifact
  values in `PerArtifact` — raw lists pass through whole, as shared
  data, to every per-artifact subprocess. Scalars belong in `Params`.
- Outputs are files written to `execute_dir`; `execute_function`
  returns `None` (a non-`None` return is a runtime error).
- Put per-run configuration in the nested `Params` model. Top-level
  fields, a `params` field not typed as the nested `Params` class, a
  `ToolSpec`, or an `execute_command` override all fail at import.
- `ExecuteInput.metadata` and `files_dir` are unavailable. Do not use
  `log_path`; log to stdout/stderr, which the framework
  captures to the unit log.
- The operation's module and Artisan must be importable wherever the command
  runs: installed in the container image (see
  [Op Container Images](op-container-images.md)), installed in the
  active environment locally.

| Op shape | Use |
|----------|-----|
| Python body that runs in the lifecycle worker | Plain function op |
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

Use `NAME` for independent input streams with matching filename stems. Artisan
strips all extensions: `data.tar.gz` and `data.csv` both have the stem `data`.

```python
class JoinByName(OperationDefinition):
    name = "join_by_name"
    group_by: GroupByStrategy | None = GroupByStrategy.NAME

    class InputRole(StrEnum):
        sequences = "sequences"
        annotations = "annotations"
```

Each role must have unique stems among artifacts with an `original_name`;
duplicates raise `ValueError` during pairing. Both `run.log` and `run.cfg` have
the stem `run`. Encode their distinction in the base name
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

    source = CSVArtifact.draft(
        content=test_csv.read_bytes(), original_name="test.csv", step_number=0
    ).finalize()
    source.materialized_path = str(test_csv)
    prepared = op.preprocess(
        PreprocessInput(
            preprocess_dir=str(Path(tmp) / "pre"),
            input_artifacts={"dataset": [source]},
        )
    )
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
    assert (
        artifact_result.lineage["dataset"][0].source_artifact_id == source.artifact_id
    )
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
