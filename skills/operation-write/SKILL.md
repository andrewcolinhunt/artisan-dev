---
name: operation-write
description: Write, scaffold, or review an Artisan pipeline operation. Use this skill when the user asks to create a new operation, write a creator or curator, scaffold an operation class, or review an existing operation for correctness. Trigger on phrases like "write an operation", "create a creator", "new curator", "scaffold operation", or any request involving OperationDefinition subclasses.
---

# Write an Artisan Operation

Write or scaffold the operation described in the user's request. If the
invoking client supplies explicit skill arguments, treat them as the request
details.

Before writing, read at least one example operation from
`src/artisan/operations/examples/` to match the established style. Also read the
base class at `src/artisan/operations/base/operation_definition.py` if you need
to confirm API details.

---

## Decision: Creator or Curator?

Pick one. The framework detects the type by which method you override.

| Type | Override | Use when | Runs on |
|---|---|---|---|
| **Creator** | `execute_function()` or `execute_command()` | Heavy computation, file I/O, external tools | Built-in local runner or an external provider instance |
| **Curator** | `execute_curator()` | Lightweight metadata: filter, merge, ingest, route | Isolated local subprocess |

---

## Creator Operation Template

Declare metadata, inputs, outputs, parameters, runner resources, batch strategy,
and lifecycle methods in that order. Omit declarations that use defaults.

```python
"""One-line module docstring describing what this operation does."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import (
    ArtifactResult,
    BatchStrategy,
    DataArtifact,
    ExecuteInput,
    InputSpec,
    OutputSpec,
    PostprocessInput,
    PreprocessInput,
    RunnerResources,
)


class MyOperation(OperationDefinition):
    """One-line summary of what the operation does.

    Extended description if needed. Explain the algorithm or approach.
    """

    name = "my_operation"
    description = "One-line summary matching the docstring"

    class InputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="data",
            required=True,
            description="Input CSV dataset",
        ),
    }

    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            description="Transformed dataset",
            derives_from={"inputs": ["dataset"]},
        ),
    }

    class Params(BaseModel):
        """Algorithm parameters for MyOperation."""

        factor: float = Field(
            default=2.0, ge=0.0, description="Multiplier for CSV values",
        )

    params: Params = Params()

    runner_resources: RunnerResources = RunnerResources(time_limit="00:30:00")

    batch_strategy: BatchStrategy = BatchStrategy(job_name="my_operation")

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

## Runner selection belongs in the pipeline

Operation classes declare provider-neutral `runner_resources` and
`batch_strategy` defaults. They do not select or register a scheduler runner.
Pipeline code may send a creator to an optional provider by passing its
initialized runner instance:

```python
from artisan_submitit import SlurmRunner

pipeline.run(
    MyOperation,
    inputs=...,
    step_runner=SlurmRunner(slurm_partition="gpu"),
)
```

Core accepts `"local"` as its only string runner name. Curators are always
executed in an isolated local subprocess, even when a pipeline has an external
default runner; do not add a runner to curator definitions or examples.

---

## Curator Operation Template

```python
"""One-line module docstring."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from artisan.operations.base import OperationDefinition
from artisan.schemas import ArtifactTypes, InputSpec, OutputSpec, PassthroughResult

if TYPE_CHECKING:
    from artisan.storage import ArtifactStore


class MyCurator(OperationDefinition):
    """One-line summary."""

    name = "my_curator"
    description = "One-line summary"

    class InputRole(StrEnum):
        stream = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.stream: InputSpec(
            artifact_type=ArtifactTypes.ANY,
            required=True,
            description="Artifacts to process",
        ),
    }

    class OutputRole(StrEnum):
        stream = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.stream: OutputSpec(
            artifact_type=ArtifactTypes.ANY,
            description="Processed artifacts",
        ),
    }

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        """Route artifact IDs without creating new artifacts."""
        ids = inputs["stream"]["artifact_id"].to_list()
        return PassthroughResult(
            success=True,
            passthrough={"stream": ids},
        )
```

---

## Lineage Patterns

Every creator output **must** set `derives_from`. It declares required and
allowed parent roles, never actual parents. Curator passthrough outputs may use
`None`; artifact-producing curators follow the same explicit contract.

| Pattern | Value | When to use |
|---|---|---|
| Derived from input | `{"inputs": ["role_name"]}` | Output traces back to a named input role |
| Generative | `{"inputs": []}` | No parent artifacts (data generation) |
| Output-to-output | `{"outputs": ["other_role"]}` | Co-produced artifact (e.g. metrics alongside data) |
| Curator passthrough | `None` | Curator routing existing artifacts |

Every emitted role must have a `lineage` entry, including present-empty roles.
Every derived draft needs exact parents from all declared roles; no other roles
are allowed. Roots explicitly return empty lists. Omitted optional roles appear
in neither map. Duplicate names are legal; references use role-local indices.

Use `result.add_artifact(role, draft, sources={"input_role": [source_id]})` to
append the draft and its mappings together. String sources are input IDs;
integer sources address co-produced output indices. `sources={}` declares a
root. The helper returns the new draft index and never chooses its parents.
Manual construction is equally supported:

```python
from artisan.schemas import ArtifactResult, LineageMapping

result = ArtifactResult(
    artifacts={"dataset": [draft]},
    lineage={"dataset": [LineageMapping(
        draft_index=0, source_role="dataset", source_artifact_id=source_id,
    )]},
)
```

Several parents in one role are allowed. For jointly necessary parents, supply
the complete set; the framework hashes only that declared set into a group ID.
Never copy an input dispatch group or guess a parent from an input position.

When a tool's outputs encode their parents in filenames, the operation may
explicitly call `match_outputs_to_inputs_by_stem` from
`artisan.operations.lineage`, passing output names and chosen `(name, input_id)`
candidates. It raises on no match or ambiguity. Executors never call it.
Command operations and `execute_as_tool` have no Python return-value transport:
use that helper or an operation-owned file manifest with relative output paths
and exact parent IDs. Do not rely on an executor output-directory parser.

---

## InputSpec Fields

| Field | Type | Default | Effect |
|---|---|---|---|
| `artifact_type` | `str` | `"any"` | Type constraint on accepted artifacts |
| `required` | `bool` | `True` | Pipeline fails if input is missing |
| `materialize` | `bool` | `True` | `True`: write to disk (file path). `False`: in-memory access only |
| `materialize_as` | `str \| None` | `None` | Target format for materialization (e.g. `".csv"`). Requires `materialize=True` |
| `hydrate` | `bool` | `True` | `True`: load content. `False`: ID-only mode |
| `with_associated` | `tuple[str, ...]` | `()` | Auto-resolve related artifacts via provenance |

## OutputSpec Fields

| Field | Type | Effect |
|---|---|---|
| `artifact_type` | `str` | Type of artifact produced |
| `description` | `str` | Human-readable description |
| `required` | `bool` | Whether the output must be non-empty |
| `derives_from` | `dict \| None` | Lineage declaration (see patterns above) |

**`derives_from` constraints:** Empty dict `{}` is **invalid** (raises
`ValidationError`). Combined `{"inputs": [...], "outputs": [...]}` is **not
supported** — use separate output roles instead.

## ExecuteInput Fields

| Field | Type | Default | Effect |
|---|---|---|---|
| `execute_dir` | `str` | — | Directory for writing output files |
| `inputs` | `dict[str, Any]` | `{}` | Prepared inputs from `preprocess()` |
| `log_path` | `str \| None` | `None` | Path for external tool stdout/stderr capture |
| `metadata` | `dict[str, Any]` | `{}` | Extensibility escape hatch from the engine |

## PostprocessInput Fields

| Field | Type | Default | Effect |
|---|---|---|---|
| `step_number` | `int` | — | Current pipeline step number (required for `draft()`) |
| `postprocess_dir` | `str` | — | Directory for postprocess artifacts (rarely needed) |
| `file_outputs` | `list[str]` | `[]` | All files in `execute_dir` after execute completes |
| `memory_outputs` | `Any` | `None` | Whatever `execute_function()` returned |
| `input_artifacts` | `dict[str, list[Artifact]]` | `{}` | Full input context with metadata for output naming and lineage |
| `metadata` | `dict[str, Any]` | `{}` | Extensibility escape hatch from the engine |

`PostprocessInput` also provides `associated_artifacts(artifact, type_str)`
and `grouped()` — the same methods available on `PreprocessInput`. Operations
that need input context in postprocess (e.g., propagating annotations) use
`inputs.input_artifacts` and `inputs.associated_artifacts()` directly.

---

## Variant: Generative Creator (No Inputs)

- Omit `InputRole`
- Set `inputs: ClassVar[dict] = {}`
- Set `derives_from={"inputs": []}` on all outputs
- Return `ArtifactResult(artifacts={"datasets": drafts}, lineage={"datasets": []})`
  or add each root with `sources={}`
- Implement only `execute_function()` and `postprocess()` (no `preprocess()`)

See `src/artisan/operations/examples/data_generator.py`.

## Variant: Multi-input with group_by

- Define multiple roles in `InputRole` and `inputs`
- Set `group_by: GroupByStrategy | None = GroupByStrategy.LINEAGE`
  (or `ZIP`, `CROSS_PRODUCT`, or `NAME`)
- In `preprocess`, iterate paired groups via `inputs.grouped()` and wrap
  per-pair values in `PerArtifact`.

```python
def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
    prepared = []
    for group in inputs.grouped():
        prepared.append(
            {
                "dataset": str(group["dataset"].materialized_path),
                "config": str(group["config"].materialized_path),
                "dataset_id": group["dataset"].artifact_id,
                "config_id": group["config"].artifact_id,
            }
        )
    return {"items": PerArtifact(prepared)}
```

Default per-artifact dispatch slices each `PerArtifact` value for one execute
call. Raw lists are shared unchanged across calls. Use raw lists only when all
calls need the same list; increasing `artifacts_per_unit` must not cause each
call to process every input again.

See `src/artisan/operations/examples/data_transformer_script.py`.

## Variant: External Tool via ToolSpec

- Set `tool: ToolSpec = ToolSpec(executable=SCRIPT_PATH, interpreter="python")`
- Configure `environments: Environments = Environments(local=..., docker=...)`
- For a function op that controls several tool calls, override
  `execute_function()` and call
  `run_command(env, [*self.tool.parts(), *args])`
- For a command op whose execute phase is one framework-managed invocation,
  override `execute_command(inputs: dict[str, Any]) -> list[str]` and return the
  argv; the framework executes it
- Import `format_args` and `run_command` only for the function-op form

See `src/artisan/operations/examples/data_transformer_script.py` for the
function-op form and `src/artisan/operations/examples/wait_tool.py` for the
command-op form.

## Variant: Config Artifacts with $artifact References

- Produce `ExecutionConfigArtifact` drafts with `{"$artifact": artifact_id}`
  placeholders in the content dict
- The framework resolves `$artifact` references to materialized paths at
  execution time
- Explicitly declare the same intended referenced parents in the result. The
  executor does not scan config contents. An operation may call
  `get_artifact_references()`, deduplicate IDs, and assign declared input roles.
- Set `materialize=False` on the input to access artifact IDs without writing
  files to disk

See `src/artisan/operations/examples/data_transformer_config.py`.

## Variant: Output-to-output Lineage

- Use `derives_from={"outputs": ["other_role"]}` when one output derives
  from another co-produced output (e.g. metrics computed alongside data)
- The primary output uses `{"inputs": []}` or `{"inputs": ["role"]}`
- Keep the index returned when adding the primary output; add the derived
  output with `sources={"primary_role": [index]}`. Manual mappings use
  `source_output_index=index`. The spec alone does not create an edge.

See `src/artisan/operations/examples/data_generator_with_metrics.py`.

---

## Validation Rules

The framework validates at class definition time (import). These cause
`TypeError` immediately:

- Must declare exactly one execution slot: `execute_function()`,
  `execute_command()`, or `execute_curator()`
- Creator outputs must set `derives_from` (cannot be `None`)
- Creator operations with inputs must implement `preprocess()`
- Must define `OutputRole(StrEnum)` with values matching `outputs` keys exactly
- Must define `InputRole(StrEnum)` with values matching `inputs` keys exactly
  (unless `inputs` is empty or `runtime_defined_inputs=True`)

---

## Artifact Draft Methods

Use the appropriate `draft()` class method in `postprocess`:

```python
# File-based data (CSV, binary, etc.)
DataArtifact.draft(content=bytes, original_name=str, step_number=int)

# Key-value metrics (JSON-serializable dict)
MetricArtifact.draft(content=dict, original_name=str, step_number=int)

# Execution configs with $artifact references
ExecutionConfigArtifact.draft(content=dict, original_name=str, step_number=int)
```

Choose `original_name` explicitly for people reading the results. The framework
preserves it; names never select parents. Finalization preserves output list
order so explicit indices still reference the same drafts.

---

## Testing Patterns

### Unit test a creator

```python
def test_my_operation(tmp_path):
    op = MyOperation(params=MyOperation.Params(factor=2.0))

    # Prepare input files
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("id,value\n1,0.9\n2,0.3\n")

    execute_dir = tmp_path / "execute"
    execute_dir.mkdir()
    source = DataArtifact.draft(content=input_csv.read_bytes(),
                                original_name="input.csv", step_number=0).finalize()
    source.materialized_path = str(input_csv)
    prepared = op.preprocess(PreprocessInput(
        preprocess_dir=str(tmp_path / "pre"), input_artifacts={"dataset": [source]},
    ))
    execute_input = ExecuteInput(execute_dir=str(execute_dir), inputs=prepared)
    memory_outputs = op.execute_function(execute_input)

    post_input = PostprocessInput(
        step_number=0,
        postprocess_dir=str(tmp_path / "post"),
        file_outputs=[str(path) for path in execute_dir.iterdir()],
        memory_outputs=memory_outputs,
    )
    result = op.postprocess(post_input)
    assert result.success
    assert "dataset" in result.artifacts
    assert result.lineage["dataset"][0].source_artifact_id == source.artifact_id
```

Also run the operation through a pipeline with multiple inputs per execution
unit. Assert output content and lineage for each input; direct method calls do
not exercise the framework's per-artifact dispatch.

### Unit test a curator

```python
def test_my_curator():
    op = MyCurator()
    result = op.execute_curator(
        inputs={"stream": pl.DataFrame({"artifact_id": ["abc123", "def456"]})},
        step_number=1,
        artifact_store=Mock(),
    )
    assert result.success
    assert len(result.passthrough["stream"]) == 2
```

---

## Style Rules

Follow these conventions from the existing examples:

- **Module docstring**: One line, describes what the operation does
- **Class docstring**: Summary line + optional extended description. Do not
  manually write Input/Output Roles sections (auto-generated by the framework)
- **Comments**: Explain non-obvious choices; omit decorative section banners.
- **Section order**: Metadata, Inputs, Outputs, Parameters, Runner Resources,
  Batch Strategy, Lifecycle (omit sections that use defaults)
- **Lifecycle docstrings**: One-line imperative summary (e.g. "Extract
  materialized paths from input artifacts.")
- **name value**: `snake_case` matching the class name's snake_case form
- **Imports**: Group stdlib, then pydantic, then artisan. Use
  `from __future__ import annotations`
- **Params class**: Nest inside the operation class. Use `Field()` with
  `default`, constraints (`ge`, `le`), and `description` for every parameter
- **No bare constants**: Put algorithm-specific values in `Params`, not as
  module-level constants
- **execute_function() is a black box**: It reads files and writes files. No
  framework imports, no Artifact objects, no ArtifactStore access
- **preprocess() bridges in**: Converts Artifact objects to plain paths/dicts
- **postprocess() bridges out**: Converts files/memory_outputs to draft Artifacts
- **Return metadata**: Include operation name and key params in `ArtifactResult.metadata`
