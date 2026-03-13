# Design: CommandSpec & ResourceConfig Refinements

## Context

We conducted a comparative analysis of how Artisan manages command and binary
information against five major workflow frameworks: Nextflow, Snakemake, Dagster,
Prefect, and Airflow. The full analysis is at
`_dev/analysis/command-binary-comparison.md`.

### What we learned

Artisan's typed `CommandSpec` hierarchy is a genuine differentiator. No other
framework represents commands as structured, typed data:

- **Nextflow/Snakemake** use opaque Bash strings — no static analysis, no
  introspection, shell escaping bugs are the #1 user complaint.
- **Dagster/Prefect/Airflow** have no command abstraction at all — external
  binaries are invoked via ad-hoc `subprocess.run()` calls buried in function
  bodies. The framework doesn't know an external binary is being invoked.

Our approach enables programmatic introspection (`op.command.image`), type-safe
configuration, testable command building, and correct per-step overrides via
`model_copy()`. These are real advantages worth preserving.

We also confirmed that per-step overrides already work — `pipeline.run()` and
`submit()` accept `resources=`, `execution=`, `command=`, and `params=` dicts,
applied via `instantiate_operation()` with fail-fast validation. This matches
what Nextflow/Snakemake offer via config layering.

### What needs to change

Three structural issues and several minor refinements emerged from the analysis.

---

## Changes

### Portable ResourceConfig

**Problem**: `ResourceConfig` is hardcoded to SLURM. Every field name
(`partition`, `gres`, `cpus_per_task`, `extra_slurm_kwargs`) is SLURM-specific.
The local backend already warns when SLURM-specific resources are configured —
this makes the coupling explicit. If we add Kubernetes or cloud backends, the
current model doesn't transfer.

Nextflow's approach is the gold standard: portable directives (`cpus`, `memory`,
`time`, `accelerator`) that each executor translates into backend-native
parameters.

**Current** (`resource_config.py`):

```python
class ResourceConfig(BaseModel):
    partition: str = "cpu"
    gres: str | None = None
    cpus_per_task: int = Field(1, ge=1)
    mem_gb: int = Field(4, ge=1)
    time_limit: str = "01:00:00"
    extra_slurm_kwargs: dict[str, Any] = Field(default_factory=dict)
```

**Proposed**:

```python
class ResourceConfig(BaseModel):
    cpus: int = Field(1, ge=1)
    memory_gb: int = Field(4, ge=1)
    gpus: int = Field(0, ge=0)
    time_limit: str = "01:00:00"
    extra: dict[str, Any] = Field(default_factory=dict)
```

Each backend translates these portable fields:

- **SLURM**: `cpus` → `--cpus-per-task`, `memory_gb` → `--mem`, `gpus` →
  `--gres=gpu:N`, `time_limit` → `--time`, `extra` → passed through as raw
  sbatch flags (including `partition`, which becomes an `extra` field since
  it's SLURM-specific)
- **Local**: uses `cpus` to inform thread pool sizing, ignores GPU/time
- **Future K8s**: `cpus` → `resources.requests.cpu`, `memory_gb` →
  `resources.requests.memory`, `gpus` → `nvidia.com/gpu` limits

The `extra` dict is the escape hatch for backend-specific needs:

```python
# SLURM-specific
resources = ResourceConfig(
    cpus=4, memory_gb=16, gpus=1, time_limit="02:00:00",
    extra={"partition": "gpu", "account": "mylab"},
)

# Future K8s-specific
resources = ResourceConfig(
    cpus=4, memory_gb=16, gpus=1,
    extra={"node_selector": {"cloud.google.com/gke-accelerator": "nvidia-tesla-t4"}},
)
```

**Migration**: rename `cpus_per_task` → `cpus`, drop `partition`/`gres` into
`extra`, rename `extra_slurm_kwargs` → `extra`. Update `slurm.py` backend to
read from the new fields + pull `partition` from `extra`. Update local backend
to drop the SLURM-specific warning (it's no longer needed — the model is
portable).

### CommandSpec Registry (Move build_command to the class)

**Problem**: Adding a new runtime requires modifying the `match` statement in
`build_command_from_spec()` in `external_tools.py`. This is closed for
extension — third-party users can't add runtimes without patching framework
code.

**Current** (`external_tools.py:220-231`):

```python
def build_command_from_spec(command, args, cwd=None):
    match command:
        case ApptainerCommandSpec():
            parts = _build_apptainer_from_spec(command, args, cwd)
        case DockerCommandSpec():
            parts = _build_docker_from_spec(command, args, cwd)
        case PixiCommandSpec():
            parts = _build_pixi_from_spec(command, args)
        case LocalCommandSpec():
            parts = _build_local_from_spec(command, args)
        case _:
            raise TypeError(...)
```

**Proposed**: Move `build_command()` onto the `CommandSpec` class as an abstract
method. Each subclass implements its own command building. The existing builder
functions become the method bodies.

```python
class CommandSpec(BaseModel):
    executable: str | Path
    arg_style: ArgStyle
    subcommand: str | None = None
    interpreter: str | None = None

    def executable_parts(self) -> list[str]:
        if self.interpreter is None:
            return [str(self.executable)]
        return [*self.interpreter.split(), str(self.executable)]

    def build_command(self, args: list[str], cwd: Path | None = None) -> list[str]:
        parts = self.executable_parts()
        if self.subcommand:
            parts.append(self.subcommand)
        parts.extend(args)
        return parts


class DockerCommandSpec(CommandSpec):
    image: str
    gpu: bool = False
    binds: list[tuple[Path, Path]] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)

    def build_command(self, args: list[str], cwd: Path | None = None) -> list[str]:
        parts = ["docker", "run", "--rm"]
        if self.gpu:
            parts.extend(["--gpus", "all"])
        if cwd is not None:
            parts.extend(["--volume", f"{cwd.parent}:{cwd.parent}"])
        for host, container in self.binds:
            parts.extend(["--volume", f"{host}:{container}"])
        for k, v in self.env.items():
            parts.extend(["--env", f"{k}={v}"])
        parts.append(str(self.image))
        parts.extend(self.executable_parts())
        if self.subcommand:
            parts.append(self.subcommand)
        parts.extend(args)
        return parts
```

`build_command_from_spec()` becomes a thin wrapper:

```python
def build_command_from_spec(command: CommandSpec, args: list[str], cwd: Path | None = None) -> Command:
    parts = command.build_command(args, cwd)
    return Command(parts=parts, string=shlex.join(parts))
```

Now anyone can add a runtime by subclassing `CommandSpec` — no framework changes.

### Binary Validation (Fail Fast)

**Problem**: If a user configures `executable="samtools"` and samtools isn't
installed, they won't find out until that operation runs mid-pipeline. That
could be after hours of upstream computation. No framework does this well — it's
a chance to differentiate.

**Proposed**: Add a `validate_environment()` method to `CommandSpec`. Each
subclass checks that its binary/image/runtime exists. The framework calls this
before pipeline execution starts.

```python
class CommandSpec(BaseModel):
    # ...

    def validate_environment(self) -> None:
        """Check that the command can run. Raises if not."""
        exe = str(self.executable)
        if not Path(exe).exists() and not shutil.which(exe):
            raise FileNotFoundError(f"Executable not found: {self.executable}")


class DockerCommandSpec(CommandSpec):
    def validate_environment(self) -> None:
        if not shutil.which("docker"):
            raise FileNotFoundError("Docker is not installed or not on PATH")
        # Optionally: check image exists locally


class ApptainerCommandSpec(CommandSpec):
    def validate_environment(self) -> None:
        if not shutil.which("apptainer"):
            raise FileNotFoundError("Apptainer is not installed or not on PATH")
        if not self.image.exists():
            raise FileNotFoundError(f"Container image not found: {self.image}")


class PixiCommandSpec(CommandSpec):
    def validate_environment(self) -> None:
        if not shutil.which("pixi"):
            raise FileNotFoundError("Pixi is not installed or not on PATH")
```

The pipeline manager calls `operation.command.validate_environment()` for all
steps at pipeline startup, before any execution begins. Error message:

```
ValidationError: Step 3 (variant_calling) cannot run:
  Container image not found: /images/gatk_4.4.sif
```

### Rename `script` to `executable`

**Problem**: The field name `script` connotes a file the user wrote. For system
binaries (`samtools`, `bwa`, `gatk`), it reads awkwardly:

```python
command = LocalCommandSpec(script=Path("samtools"), ...)  # "samtools" isn't a "script"
```

**Proposed**: Rename `script` → `executable` and the method `script_parts()` →
`executable_parts()`. Also accept `str | Path` to support PATH-resolved
binaries naturally:

```python
class CommandSpec(BaseModel):
    executable: str | Path   # was: script: Path

    def executable_parts(self) -> list[str]:  # was: script_parts()
        if self.interpreter is None:
            return [str(self.executable)]
        return [*self.interpreter.split(), str(self.executable)]
```

Now both patterns read naturally:

```python
# System binary on PATH
command = LocalCommandSpec(executable="samtools", subcommand="sort", ...)

# User's script with full path
command = LocalCommandSpec(executable=SCRIPT_PATH, interpreter="python", ...)
```

### Rename `image: Path` to `image: str`

**Problem**: Docker images are registry references (`quay.io/biocontainers/samtools:1.17`),
not filesystem paths. `image: Path` is semantically wrong for Docker and
forces awkward `Path("quay.io/...")` usage. Apptainer images ARE filesystem
paths (`.sif` files), so the type should differ.

**Proposed**:

```python
class DockerCommandSpec(CommandSpec):
    image: str   # registry reference, e.g. "biocontainers/samtools:1.17"

class ApptainerCommandSpec(CommandSpec):
    image: Path  # filesystem path to .sif file
```

### Add `ShellCommandSpec` for Pipes and Redirects

**Problem**: Some commands genuinely need shell features — pipes, redirects,
process substitution. The current model can't express
`samtools sort input.bam | samtools view -b > output.bam`. Users would have to
wrap this in `bash -c "..."` which defeats the typed model.

**Proposed**: A `ShellCommandSpec` that takes a command string and runs it via
the shell:

```python
class ShellCommandSpec(CommandSpec):
    """For commands requiring shell features (pipes, redirects, etc.)."""
    shell: str = "/bin/bash"
    env: dict[str, str] = Field(default_factory=dict)

    def build_command(self, args: list[str], cwd: Path | None = None) -> list[str]:
        # executable is the shell command string
        # args are appended (unusual but allows parameterization)
        cmd_string = str(self.executable)
        if args:
            cmd_string += " " + " ".join(args)
        return [self.shell, "-c", cmd_string]
```

Usage:

```python
command = ShellCommandSpec(
    executable="samtools sort {input} | samtools view -b > {output}",
    arg_style=ArgStyle.ARGPARSE,  # unused but required by base — consider making optional
)
```

Note: `arg_style` being required on the base class is awkward for
`ShellCommandSpec`. Consider making it optional with a default, or moving it off
the base class entirely (it's really a concern of `format_args()`, not
`CommandSpec`).

### ArgStyle Expansion

**Problem**: Only two styles exist (`HYDRA`: `key=value`, `ARGPARSE`:
`--key value`). Many tools use positional args, short flags (`-t 4`), or mixed
styles.

> **NOTE**: This needs more thought. Argument formatting is deceptively
> complicated. Considerations include:
>
> - Positional args have no key — they're order-dependent. This doesn't fit the
>   current `dict[str, Any]` input to `format_args()`.
> - Short flags (`-t 4`) vs long flags (`--threads 4`) — some tools use both,
>   sometimes the same tool uses short for some args and long for others.
> - Boolean flags — ARGPARSE style currently emits `--verbose true` but many
>   tools expect `--verbose` (presence = true, absence = false).
> - Repeated flags (`--include foo --include bar`) vs list values
>   (`--include foo,bar`).
> - Equals-style long flags (`--threads=4` vs `--threads 4`).
> - Tools that mix positional and keyword args
>   (`samtools sort -@ 4 input.bam -o output.bam`).
>
> A simple enum expansion may not cover these cases well. We may need a
> richer argument specification model, or we may decide that `format_args()` is
> a convenience for simple cases and complex argument construction should just
> happen in `execute()`. Revisit after the other changes land.

---

## Implementation Order

The changes have minimal interdependencies but should be ordered for clean
commits:

- **Rename `script` → `executable`, `script_parts()` → `executable_parts()`**.
  Pure rename. Update all references in `command_spec.py`, `external_tools.py`,
  and example operations. Also change `image: Path` → `image: str` on
  `DockerCommandSpec`.

- **Move `build_command()` onto CommandSpec classes.** Move each
  `_build_*_from_spec()` function body into the corresponding class's
  `build_command()` method. Simplify `build_command_from_spec()` to delegate.
  Delete the `match` statement and the standalone builder functions.

- **Add `validate_environment()`.** Add the method to each CommandSpec subclass.
  Wire it into pipeline startup (call for all operations before any execution).

- **Add `ShellCommandSpec`.** New subclass. Since `build_command()` is now on the
  class, this requires no changes to `external_tools.py`.

- **Portable ResourceConfig.** Rename fields, update SLURM backend to translate,
  update local backend to drop the warning, update all operation examples and
  tests.

- **ArgStyle** — defer deeper design work. The current two styles work for
  existing operations. Revisit when we hit a concrete tool that needs something
  different.

---

## Files Affected

| File | Changes |
|---|---|
| `schemas/operation_config/command_spec.py` | Rename field/method, add `build_command()`, add `validate_environment()`, add `ShellCommandSpec`, change Docker `image` type |
| `utils/external_tools.py` | Simplify `build_command_from_spec()`, remove `_build_*` functions |
| `schemas/operation_config/resource_config.py` | Rename fields to portable names |
| `orchestration/backends/slurm.py` | Translate portable ResourceConfig → SLURM flags |
| `orchestration/backends/local.py` | Drop SLURM-specific warning |
| `orchestration/backends/base.py` | Update ResourceConfig references |
| `orchestration/pipeline_manager.py` | Add validation call at pipeline startup |
| `orchestration/engine/step_executor.py` | Update ResourceConfig references |
| `operations/examples/data_transformer_script.py` | Update field names |
| `operations/examples/data_transformer.py` | Update ResourceConfig if present |
| `operations/curator/*` | Update ResourceConfig if present |
| Tests | Update all ResourceConfig and CommandSpec usage |
