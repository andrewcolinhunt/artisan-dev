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
- No framework has a clean multi-environment declaration — operations should
  declare their own environment configs for each runtime type

**What the original CommandSpec got wrong:**

`CommandSpec` conflated two concerns: the tool being invoked (`script`,
`interpreter`, `subcommand`, `arg_style`) and the execution environment
(`image`, `binds`, `gpu`, `env`, `venv_path`). These are independent:

- The **tool** is a property of the operation — the author knows what binary
  they're invoking
- The **environment** is a deployment concern — Docker in prod, local in dev,
  Apptainer on HPC — but the operation author knows the right image, binds,
  and GPU flags for each
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
| `environments` | `Environments` | **Where** to run it (declares configs for each runtime type) |
| `resources` | `ResourceConfig` | **With what** hardware (CPUs, memory, GPUs) |
| `execution` | `ExecutionConfig` | **How** to batch and schedule |

The operation author declares environment configs for every runtime they
support. The pipeline operator selects which one is active. This is a genuine
differentiator — no other framework supports structured, per-operation,
multi-environment declarations.

All four are instance fields (not ClassVars, not params). All four are
overridable per-step via the existing `model_copy()` system. Pure-Python
operations leave `tool` as `None` and `environments` at its default.

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

## EnvironmentSpec Hierarchy

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

## Environments Model

A Pydantic model that holds all environment configs for an operation and
selects which one is active. Typed fields for each environment type ensure
validation, autocomplete, and inspectability. `None` for unconfigured types.

```python
class Environments(BaseModel):
    """Multi-environment configuration for an operation.

    Each field holds the config for one environment type. The active field
    selects which one is used at runtime. Unconfigured types are None.
    """

    active: str = "local"
    local: LocalEnvironmentSpec = Field(default_factory=LocalEnvironmentSpec)
    docker: DockerEnvironmentSpec | None = None
    apptainer: ApptainerEnvironmentSpec | None = None
    pixi: PixiEnvironmentSpec | None = None

    def current(self) -> EnvironmentSpec:
        """Return the active environment spec."""
        env = getattr(self, self.active, None)
        if env is None:
            raise ValueError(
                f"Environment '{self.active}' is not configured. "
                f"Available: {self.available()}"
            )
        return env

    def available(self) -> list[str]:
        """Return names of configured environments."""
        return [
            name for name in ("local", "docker", "apptainer", "pixi")
            if getattr(self, name) is not None
        ]
```

Operations declare their environments with configs for each runtime they
support:

```python
class AlignReads(OperationDefinition):
    tool: ToolSpec = ToolSpec(executable="samtools", subcommand="sort")

    environments: Environments = Environments(
        local=LocalEnvironmentSpec(),
        docker=DockerEnvironmentSpec(
            image="biocontainers/samtools:1.17",
        ),
        apptainer=ApptainerEnvironmentSpec(
            image=Path("/images/samtools_1.17.sif"),
        ),
    )

    resources: ResourceConfig = ResourceConfig(cpus=4, memory_gb=16)
```

Pure-Python operations use the default (local only, no external tool):

```python
class DataTransformer(OperationDefinition):
    # tool is None (default) — no external binary
    # environments is Environments() (default) — local only
    resources: ResourceConfig = ResourceConfig(cpus=1, memory_gb=4)
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
    environments: Environments = Environments()
    resources: ResourceConfig = ResourceConfig()
    execution: ExecutionConfig = ExecutionConfig()
```

- `tool` is `None` for pure-Python operations. Only set when the operation
  invokes an external binary.
- `environments` defaults to `Environments()` (local only). Operations that
  support multiple runtimes declare configs for each.

---

## run_command

Replaces `run_external_command()`. Takes an environment spec and a pre-built
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

Operations get the active environment via `self.environments.current()`:

```python
def execute(self, inputs: ExecuteInput) -> Any:
    env = self.environments.current()
    cmd = [*self.tool.parts(), "-@", "4", "-o", "out.bam", "in.bam"]
    run_command(env, cmd, cwd=inputs.execute_dir)
```

---

## format_args

`ArgStyle` is removed entirely (no backwards compatibility). `format_args()`
stays as an importable utility that formats `dict[str, Any]` into CLI argument
lists. It takes no style parameter — it produces `--key value` format
(the most common convention).

```python
def format_args(params: dict[str, Any]) -> list[str]:
    result = []
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            if value:
                result.append(f"--{key}")
            continue
        result.extend([f"--{key}", to_cli_value(value)])
    return result
```

Usage:

```python
from artisan.utils.external_tools import format_args

args = format_args({"batch-size": 16, "lr": 0.001, "verbose": True})
# ["--batch-size", "16", "--lr", "0.001", "--verbose"]

cmd = [*self.tool.parts(), *args]
```

Operations with non-standard argument patterns build `list[str]` directly:

```python
# Hydra-style
cmd = [*self.tool.parts(), "batch_size=16", "sampler.steps=200"]

# Mixed positional and flags
cmd = [*self.tool.parts(), "-@", "4", "--write-index", "-o", "out.bam", "in.bam"]
```

---

## Pipeline Validation

The pipeline manager validates all operations before any execution begins.
Validation checks the active environment (as selected by the pipeline default
or per-step override):

```python
for step in steps:
    op = step.operation
    if op.tool:
        op.tool.validate()
    op.environments.current().validate_environment()
```

Errors are collected and reported at startup:

```
ValidationError: Pipeline cannot start:
  Step 3 (variant_calling): Executable not found: samtools
  Step 3 (variant_calling): Container image not found: /images/gatk_4.4.sif
```

---

## Pipeline API and Per-Step Overrides

### pipeline.run() / submit() signature

```python
def run(
    self,
    operation: type[OperationDefinition],
    inputs=None,
    params: dict[str, Any] | None = None,
    backend: str | BackendBase | None = None,
    environment: str | dict[str, Any] | None = None,  # replaces command=
    tool: dict[str, Any] | None = None,                # new
    resources: dict[str, Any] | None = None,
    execution: dict[str, Any] | None = None,
    failure_policy: FailurePolicy | None = None,
    compact: bool = True,
    name: str | None = None,
) -> StepResult:
```

The `environment=` parameter accepts two forms:

**String** — select which environment to activate:

```python
pipeline.run(AlignReads, environment="docker")
# → sets environments.active = "docker"
```

**Dict** — override fields on the Environments model (can include selection):

```python
# Select and override
pipeline.run(AlignReads, environment={"active": "docker", "docker": {"image": "v2"}})

# Override the active environment's fields
pipeline.run(AlignReads, environment={"docker": {"gpu": True}})
```

### Pipeline-level default environment

The pipeline sets a default environment for all steps:

```python
pipeline = Pipeline(store=store, backend="slurm", environment="apptainer")

pipeline.run(AlignReads)                           # → apptainer
pipeline.run(TrainModel, environment="docker")     # → docker (overridden)
pipeline.run(PureOp)                               # → local (no tool, default)
```

### instantiate_operation changes

```python
def instantiate_operation(
    operation_class, params,
    tool=None, environment=None, resources=None, execution=None,
):
    # ... existing param handling ...
    instance = operation_class(**init_kwargs)

    updates = {}

    if tool and instance.tool:
        updates["tool"] = instance.tool.model_copy(update=tool)

    if environment is not None:
        if isinstance(environment, str):
            updates["environments"] = instance.environments.model_copy(
                update={"active": environment}
            )
        else:
            updates["environments"] = instance.environments.model_copy(
                update=environment
            )

    if resources:
        updates["resources"] = instance.resources.model_copy(update=resources)
    if execution:
        updates["execution"] = instance.execution.model_copy(update=execution)

    if updates:
        instance = instance.model_copy(update=updates)
    return instance
```

### Validation

```python
def _validate_environment(operation, environment):
    """Validate environment override before execution."""
    if isinstance(environment, str):
        envs = operation.environments if hasattr(operation, 'environments') else Environments()
        if environment not in envs.available():
            raise ValueError(
                f"Environment '{environment}' not configured on {operation.name}. "
                f"Available: {envs.available()}"
            )
    else:
        valid_keys = set(Environments.model_fields)
        unknown = set(environment) - valid_keys
        if unknown:
            raise ValueError(
                f"Unknown environment keys: {sorted(unknown)}. "
                f"Valid keys: {sorted(valid_keys)}"
            )

def _validate_tool(operation, tool):
    """Validate tool override keys."""
    if not hasattr(operation, 'tool') or operation.tool is None:
        raise ValueError(
            f"Operation '{operation.name}' has no tool to override"
        )
    valid_keys = set(ToolSpec.model_fields)
    unknown = set(tool) - valid_keys
    if unknown:
        raise ValueError(
            f"Unknown tool keys: {sorted(unknown)}. "
            f"Valid keys: {sorted(valid_keys)}"
        )
```

---

## End-to-End Examples

### External tool with multiple environments

```python
SCRIPT_PATH = Path(__file__).parent / "scripts" / "transform_data.py"

class DataTransformerScript(OperationDefinition):
    name: ClassVar[str] = "data_transformer_script"
    description: ClassVar[str] = "Execute data transformation script"

    # ... inputs, outputs, group_by (unchanged) ...

    tool: ToolSpec = ToolSpec(executable=SCRIPT_PATH, interpreter="python")
    environments: Environments = Environments(
        local=LocalEnvironmentSpec(),
        docker=DockerEnvironmentSpec(image="my-registry/transformer:latest"),
    )
    resources: ResourceConfig = ResourceConfig(cpus=1, memory_gb=4)
    execution: ExecutionConfig = ExecutionConfig(job_name="data_transformer_script")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        # ... unchanged ...

    def execute(self, inputs: ExecuteInput) -> Any:
        env = self.environments.current()
        for item in inputs.inputs["items"]:
            cmd = [*self.tool.parts(),
                   "--config", item["config_path"],
                   "--output-dir", str(inputs.execute_dir),
                   "--output-basename", item["design_name"]]
            run_command(env, cmd, cwd=inputs.execute_dir)
        return None

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        # ... unchanged ...
```

### Pipeline usage across deployment contexts

```python
# Local development
pipeline = Pipeline(store=store)
pipeline.run(DataTransformerScript)  # runs locally

# HPC with Apptainer
pipeline = Pipeline(store=store, backend="slurm", environment="apptainer")
pipeline.run(AlignReads)                             # apptainer
pipeline.run(AlignReads, resources={"cpus": 16})     # apptainer + more CPUs
pipeline.run(TrainModel, environment="docker")       # docker override for this step

# Per-step tweaks
pipeline.run(AlignReads, environment="docker")
pipeline.run(AlignReads, environment={"active": "docker", "docker": {"image": "samtools:1.18"}})
pipeline.run(AlignReads, tool={"executable": "/opt/samtools-dev/bin/samtools"})
```

### Pure-Python operation (no tool, no environment config needed)

```python
class DataTransformer(OperationDefinition):
    name: ClassVar[str] = "data_transformer"
    # tool is None (default)
    # environments is Environments() (default — local only)
    resources: ResourceConfig = ResourceConfig(cpus=1, memory_gb=4)

    def execute(self, inputs: ExecuteInput) -> Any:
        # Pure Python — no run_command(), no self.tool, no self.environments
        for input_path in input_files:
            # ... process in Python ...
```

---

## Backends vs Environments

These are different layers that compose independently:

- **Backend** = how to dispatch and schedule work across workers (local
  ProcessPool, SLURM job arrays). Per-pipeline or per-step. Controls Prefect
  flow/task runner.
- **Environment** = how to wrap a single command (Docker, Apptainer, local,
  Pixi). Per-operation. Controls what goes around `subprocess.run()`.

| Backend | Environment | What happens |
|---|---|---|
| Local | local | Process pool on this machine, commands run directly |
| Local | docker | Process pool on this machine, commands wrapped in `docker run` |
| SLURM | local | Dispatch to cluster nodes, commands run directly on node |
| SLURM | apptainer | Dispatch to cluster nodes, commands wrapped in `apptainer exec` |

The backend reads `ResourceConfig` to configure the scheduler. It does not
know about `EnvironmentSpec`. The environment wrapping happens inside the
worker when `execute()` calls `run_command()`.

---

## Migration

### Field renames

| Before | After |
|---|---|
| `command: LocalCommandSpec = LocalCommandSpec(script=..., arg_style=..., interpreter=...)` | `tool: ToolSpec = ToolSpec(executable=..., interpreter=...)` + `environments: Environments = Environments(...)` |
| `resources: ResourceConfig(cpus_per_task=4, partition="gpu", gres="gpu:1")` | `resources: ResourceConfig(cpus=4, gpus=1, extra={"partition": "gpu"})` |

### Override API

| Before | After |
|---|---|
| `pipeline.run(Op, command={"image": ...})` | `pipeline.run(Op, environment="docker")` or `pipeline.run(Op, environment={"active": "docker", "docker": {"image": ...}})` |
| `pipeline.run(Op, resources={"cpus_per_task": 4})` | `pipeline.run(Op, resources={"cpus": 4})` |
| N/A | `pipeline.run(Op, tool={"executable": ...})` |

### What gets removed

- `CommandSpec` and all subclasses (`command_spec.py`)
- `build_command_from_spec()` and all `_build_*_from_spec()` functions
- The `match` statement in `external_tools.py`
- `run_external_command()` (replaced by `run_command()`)
- `ArgStyle` enum entirely
- `script`, `interpreter`, `subcommand`, `arg_style` from environment config

### What gets added

- `ToolSpec` model (`tool_spec.py`)
- `EnvironmentSpec` hierarchy (`environment_spec.py`)
- `Environments` model (`environments.py`)
- `run_command()` function
- `tool` field on `OperationDefinition`
- `environments` field on `OperationDefinition` (replaces `command`)
- `environment=` parameter on `Pipeline` constructor
- `environment=` and `tool=` parameters on `pipeline.run()` / `submit()`
- Startup validation in pipeline manager

### What stays unchanged

- `format_args()`, `to_cli_value()` as utilities (ArgStyle removed, format_args
  simplified to `--key value` only)
- `Command` dataclass, `ExternalToolError`
- `_run_with_streaming()`, `_run_captured()`, `_kill_process_group()`
- `ExecutionConfig`
- Per-step override mechanism (extended, not replaced)

---

## Implementation Order

- **New model files.** Create `tool_spec.py`, `environment_spec.py`, and
  `environments.py`. Define `ToolSpec`, `EnvironmentSpec` hierarchy, and
  `Environments` model. Unit tests for `parts()`, `wrap_command()`,
  `prepare_env()`, `validate()`, `validate_environment()`, `current()`,
  `available()`.

- **run_command.** New function in `external_tools.py`. Keep
  `run_external_command` temporarily as a deprecated wrapper. Simplify
  `format_args` (remove `ArgStyle`, produce `--key value` only, fix booleans).

- **OperationDefinition.** Add `tool` and `environments` fields. Update example
  operations.

- **Pipeline manager / step executor.** Add `environment=` and `tool=` to
  `run()`/`submit()`. Wire into `instantiate_operation()`. Add pipeline-level
  default environment. Add startup validation. Rename `command=` ->
  `environment=`.

- **Portable ResourceConfig.** Rename fields. Update SLURM backend. Update
  local backend.

- **Remove old code.** Delete `command_spec.py`, `build_command_from_spec`,
  `_build_*_from_spec`, `run_external_command`, `ArgStyle`.

---

## Files Affected

| File | Changes |
|---|---|
| `schemas/operation_config/tool_spec.py` | **New** |
| `schemas/operation_config/environment_spec.py` | **New** — EnvironmentSpec hierarchy |
| `schemas/operation_config/environments.py` | **New** — Environments model |
| `schemas/operation_config/command_spec.py` | **Delete** after migration |
| `schemas/operation_config/resource_config.py` | Rename fields |
| `utils/external_tools.py` | Add `run_command()`, simplify `format_args`, remove build/match code and `ArgStyle` |
| `operations/base/operation_definition.py` | Add `tool`, replace `command` with `environments` |
| `orchestration/pipeline_manager.py` | Add `environment=`/`tool=` to run/submit, add pipeline default, rename overrides, add startup validation |
| `orchestration/engine/step_executor.py` | Update override handling |
| `orchestration/backends/slurm.py` | Translate portable ResourceConfig |
| `orchestration/backends/local.py` | Drop SLURM warning, use new field names |
| `orchestration/backends/base.py` | Update references |
| `orchestration/chain_builder.py` | Rename `command=` -> `environment=` in API |
| `operations/examples/data_transformer_script.py` | Use ToolSpec + Environments |
| `operations/examples/*.py` | Update ResourceConfig field names |
| `operations/curator/*` | Update if referencing ResourceConfig |
| Tests | Update all CommandSpec/ResourceConfig usage |
