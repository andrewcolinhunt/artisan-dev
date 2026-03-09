# Design: Pipeline Input Validation

## Status: Draft

## Problem

Pipeline wiring errors aren't caught until execution time. Two categories of
mistakes silently pass through `submit()` and only fail deep in the execution
stack:

**Type mismatches** — wiring a `metric` output into a `data` input compiles
fine, then fails at execution when the artifact type doesn't match what the
operation expects:

```python
step0 = pipeline.run(DataGeneratorWithMetrics, ...)
# Bug: wiring metrics into a data-only input — no error until execution
step1 = pipeline.run(DataTransformer, inputs={"dataset": step0.output("metrics")})
```

**Missing required inputs** — omitting a `required=True` input role passes
through `submit()` without complaint, then fails at dispatch:

```python
# Bug: DataTransformer requires "dataset" — no error until execution
step1 = pipeline.run(DataTransformer, inputs={})
```

Both are detectable at `submit()` time with information already available. The
infrastructure exists (`InputSpec.accepts_type()`, `OutputReference.artifact_type`)
but isn't wired up.

## Current Validation in `submit()`

The fail-fast block (lines 762-770 of `pipeline_manager.py`) validates:

| Check | Function | What it catches |
|---|---|---|
| Param key names | `_validate_params()` | Unknown parameter names |
| Resource key names | `_validate_resources()` | Unknown resource fields |
| Execution key names | `_validate_execution()` | Unknown execution fields |
| Command key names | `_validate_command()` | Unknown command fields |
| Input role names | `_validate_input_roles()` | Roles not declared by the operation |

**Not validated:** whether required roles are present, or whether upstream
output types match downstream input types.

## Changes

### Required Inputs Check

Add `_validate_required_inputs()` to the fail-fast block. For each role in
`operation.inputs` where `spec.required is True`, verify it appears in the
provided `inputs` dict.

```python
def _validate_required_inputs(
    operation: type[OperationDefinition],
    inputs: Any,
) -> None:
    """Raise ValueError if any required input roles are missing."""
    if operation.runtime_defined_inputs:
        return
    if not operation.inputs:
        return

    provided_roles: set[str] = set()
    if isinstance(inputs, dict):
        provided_roles = set(inputs.keys())

    missing = [
        role
        for role, spec in operation.inputs.items()
        if spec.required and role not in provided_roles
    ]
    if missing:
        msg = (
            f"Missing required input(s) for {operation.name}: {sorted(missing)}. "
            f"Declared inputs: {sorted(operation.inputs.keys())}"
        )
        raise ValueError(msg)
```

**Skip conditions:**

| Condition | Why skip |
|---|---|
| `runtime_defined_inputs=True` | Roles are user-defined, not declarative (Filter, Merge) |
| Empty `inputs` ClassVar | Generative operation, no inputs expected |
| `inputs` is a list or None | List format is for runtime-defined ops; None means generative |

**Notes:**

- This is the complement of `_validate_input_roles()`. Together they provide
  complete role coverage: no unknown roles (existing), no missing required
  roles (new).
- Optional roles (`required=False`) are allowed to be absent — that's their
  purpose.

### Type Compatibility Check

Add `_validate_input_types()` to the fail-fast block. For each wired input
where we have both an `OutputReference` (with `artifact_type`) and an
`InputSpec` (with `accepts_type()`), verify compatibility.

```python
def _validate_input_types(
    operation: type[OperationDefinition],
    inputs: Any,
) -> None:
    """Raise ValueError if upstream output types don't match input specs."""
    if not isinstance(inputs, dict):
        return
    for role, ref in inputs.items():
        if not isinstance(ref, OutputReference):
            continue
        input_spec = operation.inputs.get(role)
        if input_spec is None:
            continue
        if ref.artifact_type == ArtifactTypes.ANY:
            continue
        if not input_spec.accepts_type(ref.artifact_type):
            msg = (
                f"Type mismatch on input '{role}' for {operation.name}: "
                f"upstream step {ref.source_step} produces '{ref.artifact_type}', "
                f"but '{role}' expects '{input_spec.artifact_type}'"
            )
            raise ValueError(msg)
```

**Skip conditions:**

| Condition | Why skip |
|---|---|
| `inputs` is not a dict | List/None format — no role-level type info |
| Ref is not `OutputReference` | Raw artifact IDs (`list[str]`) — no type info |
| `input_spec is None` | `runtime_defined_inputs` — no spec to check against |
| Upstream type is `ANY` | Curator passthrough — concrete type unknown at wire time |
| Downstream spec is `ANY` | Generic op accepts anything — `accepts_type()` handles this |

**Type matching matrix:**

| Upstream type | Downstream spec | Result | Rationale |
|---|---|---|---|
| `"data"` | `"data"` | Allow | Exact match |
| `"metric"` | `"data"` | **Reject** | Definite mismatch |
| `"data"` | `ANY` | Allow | `accepts_type()` returns True for ANY specs |
| `ANY` | `"data"` | Allow (skip) | Curator passthrough — can't know concrete type |
| `ANY` | `ANY` | Allow (skip) | Both generic |

### Integration Point

Both checks slot into the existing fail-fast block at the top of `submit()`,
after the existing validations:

```python
# Existing
if params:
    _validate_params(operation, params)
if resources:
    _validate_resources(resources)
if execution:
    _validate_execution(execution)
if command:
    _validate_command(operation, command)
_validate_input_roles(operation, inputs)

# New
_validate_required_inputs(operation, inputs)
_validate_input_types(operation, inputs)
```

Both run before `_wait_for_predecessors()`, so errors surface immediately —
not after waiting for upstream steps to finish.

## Non-Goals

- **Graph-level validation** (cycle detection, connected components) — would
  require a "define then run" API change. Out of scope.
- **Binary/executable validation** — checking `shutil.which()` for
  `CommandSpec` operations. Useful but separate concern.
- **Runtime type tracking through curators** — tracking what concrete types
  flow through `ANY` passthrough operations to enable downstream validation.
  Complex, marginal value at this stage.

## Testing

- Type mismatch: wire `metric` output → `data` input, assert `ValueError`
- Type match: wire `data` output → `data` input, assert no error
- ANY downstream: wire `data` output → `ANY` input, assert no error
- ANY upstream: wire `ANY` output → `data` input, assert no error (skip)
- Missing required: omit a `required=True` role, assert `ValueError`
- Optional missing: omit a `required=False` role, assert no error
- Generative op: pass `inputs=None`, assert no error
- Runtime-defined inputs: both checks skip for Filter/Merge
- Raw artifact IDs: type check skips for `list[str]` values
- List input format: both checks skip for `list[OutputReference]`
