# Design: Operation Config Redesign

Supersedes `command-spec-refinements.md`.

## Context

We analyzed Artisan's command and binary management against Nextflow, Snakemake,
Dagster, Prefect, and Airflow (full analysis: `_dev/analysis/command-binary-comparison.md`).

**What we confirmed:**

- Typed command config is a genuine differentiator — no other framework does it
- Per-step overrides already work via `model_copy()` in `instantiate_operation()`
- Portable resources are the industry norm (Nextflow's model is the gold standard)
- No framework validates binaries upfront — opportunity to differentiate

**What the original CommandSpec got wrong:**

`CommandSpec` conflated two concerns: the tool being invoked (`script`,
`interpreter`, `subcommand`, `arg_style`) and the execution environment
(`image`, `binds`, `gpu`, `env`, `venv_path`). These are independent:

- The **tool** is a property of the operation — the author knows what binary
  they're invoking
- The **environment** is a deployment concern — Docker in prod, local in dev
- **Argument formatting** is the operation's business — it belongs in
  `execute()`, not on a spec

The `build_command_from_spec()` match statement was also closed for extension —
adding a new environment type required modifying framework code.

---

## Design Principle

Four distinct config groups on `OperationDefinition`, each independently typed
and overridable per-step:

| Field | Type | Answers |
|---|---|---|
| `tool` | `ToolSpec` | **What** binary/script to invoke |
| `environment` | `EnvironmentSpec` | **Where** to run it (Docker, local, Apptainer, Pixi) |
| `resources` | `ResourceConfig` | **With what** hardware (CPUs, memory, GPUs) |
| `execution` | `ExecutionConfig` | **How** to batch and schedule |

These read as a sentence: run this **tool** in this **environment** with these
**resources** using this **execution** strategy.

All four are instance fields (not ClassVars, not params). All four are
overridable per-step via the existing `model_copy()` system. Pure-Python
operations set `tool` to `None` and leave `environment` at its default.

---

## ToolSpec

Declares what binary or script an operation invokes.

```python
class ToolSpec(BaseModel):
    """Declares the binary or script an operation invokes."""

    executable: str | Path
    interpreter: str | None = None
    subcommand: str | None = None

    def parts(self) -> list[str]:
        """Build command prefix: [interpreter...] executable [subcommand]."""
        if self.interpreter is None:
            prefix = [str(self.executable)]
        else:
            prefix = [*self.interpreter.split(), str(self.executable)]
        if self.subcommand:
            prefix.append(self.subcommand)
        return prefix

    def validate(self) -> None:
        """Check that the executable exists."""
        exe = str(self.executable)
        if not Path(exe).exists() and not shutil.which(exe):
            raise FileNotFoundError(f"Executable not found: {self.executable}")
```

`executable` accepts `str | Path`:

```python
# System binary resolved via PATH
ToolSpec(executable="samtools", subcommand="sort")

# User script with interpreter
ToolSpec(executable=Path(__file__).parent / "scripts" / "transform.py", interpreter="python")

# Absolute path to a specific version
ToolSpec(executable="/opt/tools/gatk-4.4/gatk", subcommand="HaplotypeCaller")
```

---

## EnvironmentSpec

Renamed from `CommandSpec`. Purely about the execution environment that wraps a
command. Each subclass implements `wrap_command()`, `prepare_env()`, and
`validate_environment()`.

No match statement — the class itself knows how to wrap. Adding a new
environment type is just subclassing.

### Base

```python
class EnvironmentSpec(BaseModel):
    """Base class for execution environment configuration."""

    env: dict[str, str] = Field(default_factory=dict)

    def wrap_command(self, cmd: list[str], cwd: Path | None = None) -> list[str]:
        """Wrap a command for this environment. Base returns unchanged."""
        return cmd

    def prepare_env(self) -> dict[str, str] | None:
        """Return env dict for subprocess, or None to inherit."""
        if self.env:
            env = os.environ.copy()
            env.update(self.env)
            return env
        return None

    def validate_environment(self) -> None:
        """Check that the environment is available. Override in subclasses."""
        pass
```

### LocalEnvironmentSpec

```python
class LocalEnvironmentSpec(EnvironmentSpec):
    """Run directly on the host, optionally inside a virtualenv."""

    venv_path: Path | None = None

    def prepare_env(self) -> dict[str, str] | None:
        if self.venv_path:
            env = os.environ.copy()
            env["PATH"] = f"{self.venv_path / 'bin'}:{env.get('PATH', '')}"
            env["VIRTUAL_ENV"] = str(self.venv_path)
            env.update(self.env)
            return env
        return super().prepare_env()
```

### DockerEnvironmentSpec

```python
class DockerEnvironmentSpec(EnvironmentSpec):
    """Run inside a Docker container."""

    image: str  # registry reference, e.g. "biocontainers/samtools:1.17"
    gpu: bool = False
    binds: list[tuple[Path, Path]] = Field(default_factory=list)

    def wrap_command(self, cmd: list[str], cwd: Path | None = None) -> list[str]:
        parts = ["docker", "run", "--rm"]
        if self.gpu:
            parts.extend(["--gpus", "all"])
        if cwd is not None:
            parts.extend(["--volume", f"{cwd.parent}:{cwd.parent}"])
        for host, container in self.binds:
            parts.extend(["--volume", f"{host}:{container}"])
        for k, v in self.env.items():
            parts.extend(["--env", f"{k}={v}"])
        parts.append(self.image)
        parts.extend(cmd)
        return parts

    def validate_environment(self) -> None:
        if not shutil.which("docker"):
            raise FileNotFoundError("Docker is not installed or not on PATH")
```

### ApptainerEnvironmentSpec

```python
class ApptainerEnvironmentSpec(EnvironmentSpec):
    """Run inside an Apptainer (Singularity) container."""

    image: Path  # filesystem path to .sif file
    gpu: bool = False
    binds: list[tuple[Path, Path]] = Field(default_factory=list)

    def wrap_command(self, cmd: list[str], cwd: Path | None = None) -> list[str]:
        parts = ["apptainer", "exec"]
        if self.gpu:
            parts.append("--nv")
        if cwd is not None:
            parts.extend(["--bind", f"{cwd.parent}:{cwd.parent}"])
        for host, container in self.binds:
            parts.extend(["--bind", f"{host}:{container}"])
        for k, v in self.env.items():
            parts.extend(["--env", f"{k}={v}"])
        parts.append(str(self.image))
        parts.extend(cmd)
        return parts

    def validate_environment(self) -> None:
        if not shutil.which("apptainer"):
            raise FileNotFoundError("Apptainer is not installed or not on PATH")
        if not self.image.exists():
            raise FileNotFoundError(f"Container image not found: {self.image}")
```

### PixiEnvironmentSpec

```python
class PixiEnvironmentSpec(EnvironmentSpec):
    """Run inside a Pixi-managed environment."""

    pixi_environment: str = "default"
    manifest_path: Path | None = None

    def wrap_command(self, cmd: list[str], cwd: Path | None = None) -> list[str]:
        parts = ["pixi", "run", "-e", self.pixi_environment]
        if self.manifest_path:
            parts.extend(["--manifest-path", str(self.manifest_path)])
        parts.extend(cmd)
        return parts

    def validate_environment(self) -> None:
        if not shutil.which("pixi"):
            raise FileNotFoundError("Pixi is not installed or not on PATH")
```

---

## Portable ResourceConfig

```python
class ResourceConfig(BaseModel):
    """Portable hardware resource requirements.

    Each backend translates these to its native format.
    The extra dict is an escape hatch for backend-specific settings.
    """

    cpus: int = Field(1, ge=1)
    memory_gb: int = Field(4, ge=1)
    gpus: int = Field(0, ge=0)
    time_limit: str = "01:00:00"
    extra: dict[str, Any] = Field(default_factory=dict)
```

Backend translation:

| Field | SLURM | Local | Future K8s |
|---|---|---|---|
| `cpus` | `--cpus-per-task=N` | thread pool size | `resources.requests.cpu` |
| `memory_gb` | `--mem=NG` | ignored | `resources.requests.memory` |
| `gpus` | `--gres=gpu:N` | ignored | `nvidia.com/gpu` limit |
| `time_limit` | `--time=HH:MM:SS` | ignored | deadline |
| `extra["partition"]` | `--partition=X` | ignored | ignored |
| `extra[...]` | raw sbatch flags | ignored | pod spec fields |

---

## OperationDefinition Changes

```python
class OperationDefinition(BaseModel):
    # ... existing ClassVars (name, description, inputs, outputs, etc.) ...

    tool: ToolSpec | None = None
    environment: EnvironmentSpec = LocalEnvironmentSpec()
    resources: ResourceConfig = ResourceConfig()
    execution: ExecutionConfig = ExecutionConfig()
```

- `tool` is `None` for pure-Python operations. Only set when the operation
  invokes an external binary.
- `environment` defaults to `LocalEnvironmentSpec()`. Existing pure-Python
  operations need no changes.

---

## run_command

Replaces `run_external_command()`. Takes the environment spec and a pre-built
command list. The operation builds the command; the environment wraps it.

```python
def run_command(
    environment: EnvironmentSpec,
    cmd: list[str],
    cwd: Path | None = None,
    timeout: float | None = None,
    stream_output: bool = False,
    log_path: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Execute a command in the given environment."""
    wrapped = environment.wrap_command(cmd, cwd)
    full_cmd = Command(parts=wrapped, string=shlex.join(wrapped))
    env = environment.prepare_env()

    try:
        if stream_output:
            result = _run_with_streaming(full_cmd, cwd, timeout, log_path, env)
        else:
            result = _run_captured(full_cmd, cwd, timeout, env)
    except subprocess.TimeoutExpired as e:
        raise ExternalToolError(
            message=f"Command timed out after {timeout}s",
            command=full_cmd.parts, return_code=-1,
            stdout=e.stdout or "", stderr=e.stderr or "",
            runtime=environment,
        ) from e

    if result.returncode != 0:
        raise ExternalToolError(
            message=f"Command failed with exit code {result.returncode}",
            command=full_cmd.parts, return_code=result.returncode,
            stdout=result.stdout, stderr=result.stderr,
            runtime=environment,
        )
    return result
```

---

## format_args

Stays as an importable utility. Not part of any spec. `ArgStyle` is removed
from all models.

Operations that want structured argument formatting import and use it:

```python
from artisan.utils.external_tools import format_args, ArgStyle

args = format_args({"batch-size": 16, "lr": 0.001}, ArgStyle.ARGPARSE)
cmd = [*self.tool.parts(), *args]
```

Operations with complex argument patterns build `list[str]` directly:

```python
cmd = [*self.tool.parts(), "-@", "4", "--write-index", "-o", "out.bam", "in.bam"]
```

One fix: boolean handling. `True` emits the flag without a value, `False` skips:

```python
format_args({"verbose": True}, ArgStyle.ARGPARSE)   # ["--verbose"]
format_args({"verbose": False}, ArgStyle.ARGPARSE)  # []
```

---

## Pipeline Validation

The pipeline manager validates all operations before any execution begins:

```python
for step in steps:
    op = step.operation
    if op.tool:
        op.tool.validate()
    op.environment.validate_environment()
```

Errors are collected and reported at startup:

```
ValidationError: Pipeline cannot start:
  Step 3 (variant_calling): Executable not found: samtools
  Step 3 (variant_calling): Container image not found: /images/gatk_4.4.sif
```

---

## End-to-End Example

```python
SCRIPT_PATH = Path(__file__).parent / "scripts" / "transform_data.py"

class DataTransformerScript(OperationDefinition):
    name: ClassVar[str] = "data_transformer_script"
    description: ClassVar[str] = "Execute data transformation script"

    # ... inputs, outputs, group_by (unchanged) ...

    tool: ToolSpec = ToolSpec(executable=SCRIPT_PATH, interpreter="python")
    environment: LocalEnvironmentSpec = LocalEnvironmentSpec()
    resources: ResourceConfig = ResourceConfig(cpus=1, memory_gb=4)
    execution: ExecutionConfig = ExecutionConfig(job_name="data_transformer_script")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        # ... unchanged ...

    def execute(self, inputs: ExecuteInput) -> Any:
        for item in inputs.inputs["items"]:
            cmd = [*self.tool.parts(),
                   "--config", item["config_path"],
                   "--output-dir", str(inputs.execute_dir),
                   "--output-basename", item["design_name"]]
            run_command(self.environment, cmd, cwd=inputs.execute_dir)
        return None

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        # ... unchanged ...
```

Same operation, run in Docker at the pipeline level:

```python
pipeline.run(
    DataTransformerScript,
    environment=DockerEnvironmentSpec(image="my-registry/transformer:latest"),
    resources={"cpus": 4, "memory_gb": 16},
)
```

---

## Per-Step Overrides

The existing override system in `instantiate_operation()` extends to cover
`tool` and `environment`:

```python
def instantiate_operation(operation_class, params, tool=None,
                          environment=None, resources=None, execution=None):
    # ... existing param handling ...

    instance = operation_class(**init_kwargs)

    updates = {}
    if tool and instance.tool:
        updates["tool"] = instance.tool.model_copy(update=tool)
    if environment:
        updates["environment"] = instance.environment.model_copy(update=environment)
    if resources:
        updates["resources"] = instance.resources.model_copy(update=resources)
    if execution:
        updates["execution"] = instance.execution.model_copy(update=execution)
    if updates:
        instance = instance.model_copy(update=updates)

    return instance
```

The `pipeline.run()` / `submit()` signatures gain a `tool=` parameter and
rename `command=` to `environment=`:

```python
def run(self, operation, inputs=None, params=None, backend=None,
        tool=None, environment=None, resources=None, execution=None,
        failure_policy=None, compact=True, name=None):
```

Validation functions update similarly:

- `_validate_command()` -> `_validate_environment()`
- New `_validate_tool()` for tool override keys

---

## Migration

### Field renames

| Before | After |
|---|---|
| `command: LocalCommandSpec = LocalCommandSpec(script=..., arg_style=..., interpreter=...)` | `tool: ToolSpec = ToolSpec(executable=..., interpreter=...)` + `environment: LocalEnvironmentSpec = LocalEnvironmentSpec()` |
| `resources: ResourceConfig(cpus_per_task=4, partition="gpu", gres="gpu:1")` | `resources: ResourceConfig(cpus=4, gpus=1, extra={"partition": "gpu"})` |

### Override API

| Before | After |
|---|---|
| `pipeline.run(Op, command={"image": ...})` | `pipeline.run(Op, environment={"image": ...})` |
| `pipeline.run(Op, resources={"cpus_per_task": 4})` | `pipeline.run(Op, resources={"cpus": 4})` |
| N/A | `pipeline.run(Op, tool={"executable": ...})` |

### What gets removed

- `CommandSpec` and all subclasses (`command_spec.py`)
- `build_command_from_spec()` and all `_build_*_from_spec()` functions
- The `match` statement in `external_tools.py`
- `run_external_command()` (replaced by `run_command()`)
- `script`, `interpreter`, `subcommand`, `arg_style` from environment config

### What gets added

- `ToolSpec` model (`tool_spec.py`)
- `EnvironmentSpec` hierarchy (`environment_spec.py`)
- `run_command()` function
- `tool` field on `OperationDefinition`
- `environment` field on `OperationDefinition` (replaces `command`)
- Startup validation in pipeline manager

### What stays unchanged

- `format_args()`, `ArgStyle`, `to_cli_value()` as utilities
- `Command` dataclass, `ExternalToolError`
- `_run_with_streaming()`, `_run_captured()`, `_kill_process_group()`
- `ExecutionConfig`
- Per-step override mechanism (extended, not replaced)

---

## Implementation Order

- **New model files.** Create `tool_spec.py` and `environment_spec.py`. Define
  `ToolSpec`, `EnvironmentSpec` hierarchy. Unit tests for `parts()`,
  `wrap_command()`, `prepare_env()`, `validate()`, `validate_environment()`.

- **run_command.** New function in `external_tools.py`. Keep
  `run_external_command` temporarily as a deprecated wrapper.

- **OperationDefinition.** Add `tool` and `environment` fields. Update example
  operations.

- **Pipeline manager / step executor.** Wire `tool` and `environment` into
  overrides. Add startup validation. Rename `command` -> `environment`.

- **Portable ResourceConfig.** Rename fields. Update SLURM backend. Update
  local backend.

- **Remove old code.** Delete `command_spec.py`, `build_command_from_spec`,
  `_build_*_from_spec`, `run_external_command`.

- **format_args boolean fix.** Standalone change.

---

## Files Affected

| File | Changes |
|---|---|
| `schemas/operation_config/tool_spec.py` | **New** |
| `schemas/operation_config/environment_spec.py` | **New** |
| `schemas/operation_config/command_spec.py` | **Delete** after migration |
| `schemas/operation_config/resource_config.py` | Rename fields |
| `utils/external_tools.py` | Add `run_command()`, remove build/match code, fix boolean in `format_args` |
| `operations/base/operation_definition.py` | Add `tool`, rename `command` -> `environment` |
| `orchestration/pipeline_manager.py` | Rename overrides, add startup validation |
| `orchestration/engine/step_executor.py` | Update override handling |
| `orchestration/backends/slurm.py` | Translate portable ResourceConfig |
| `orchestration/backends/local.py` | Drop SLURM warning, use new field names |
| `orchestration/backends/base.py` | Update references |
| `orchestration/chain_builder.py` | Rename `command` -> `environment` in API |
| `operations/examples/data_transformer_script.py` | Use ToolSpec + EnvironmentSpec |
| `operations/examples/*.py` | Update ResourceConfig field names |
| `operations/curator/*` | Update if referencing ResourceConfig |
| Tests | Update all CommandSpec/ResourceConfig usage |
