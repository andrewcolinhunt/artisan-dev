# Command & Binary Management: Artisan vs. Industry

Comparative analysis of how Artisan and five major workflow frameworks handle
command specification, binary/tool management, resource configuration, and
extensibility.

---

## Executive Summary

Artisan's `CommandSpec` hierarchy is a **genuine differentiator**. Most
frameworks (Dagster, Prefect, Airflow) have no first-class command/binary
abstraction — users write ad-hoc `subprocess.run()` calls. The bioinformatics
frameworks (Nextflow, Snakemake) have richer command models but use opaque
string-based shell blocks rather than typed Python objects.

Artisan's approach is well-positioned but has gaps worth addressing:
- No framework-level auto-invocation of declared commands
- No binary discovery, validation, or version tracking
- ResourceConfig is SLURM-specific rather than portable
- No per-step command/resource overrides in pipelines
- ArgStyle covers only two formats

---

## Feature Matrix

| Capability | Artisan | Nextflow | Snakemake | Dagster | Prefect | Airflow |
|---|---|---|---|---|---|---|
| **Command as first-class object** | CommandSpec hierarchy | No (Bash strings) | No (shell strings) | No (Python body) | No (Python body) | Operator class |
| **Typed command config** | Yes (Pydantic) | No | No | No | No | Partial (operator params) |
| **Multiple runtime backends** | Local, Docker, Apptainer, Pixi | Docker, Singularity, Conda, Modules | Docker, Singularity, Conda, Modules | K8s, Docker (via tags) | Docker, K8s (via work pools) | Docker, K8s, Celery |
| **Per-task container** | Yes (per CommandSpec) | Yes (per process) | Yes (per rule) | Yes (via tags/Pipes) | No (per flow only) | Yes (per operator) |
| **Conda/env integration** | Pixi only | Conda, Spack, Modules | Conda, Modules | None | None | Virtualenv only |
| **Binary path resolution** | Manual (absolute Path) | Assumes on PATH | Assumes on PATH | Assumes on PATH | Assumes on PATH | Assumes on PATH |
| **Binary version tracking** | None | Manual convention | None | None | None | None |
| **Argument formatting** | ArgStyle (HYDRA, ARGPARSE) | Bash interpolation | Bash interpolation | N/A (Python) | N/A (Python) | Jinja2 templating |
| **Resource model** | ResourceConfig (SLURM-specific) | Portable directives | threads + resources dict | K8s-specific tags | Per-flow work pool | Executor-specific |
| **Dynamic resources** | No | Yes (closures + attempt) | Yes (callables + attempt) | No | No | No |
| **GPU support** | gres field + container flags | `accelerator` directive | resources dict | K8s resource limits | Work pool config | K8s resource limits |
| **Per-step overrides** | No | Yes (nextflow.config) | Yes (profiles) | Yes (run config) | No (per-flow) | Yes (executor_config) |
| **Reusable tool wrappers** | OperationDefinition subclass | nf-core modules (1200+) | Wrapper repo (hundreds) | Integration packages | Integration packages | Provider packages (80+) |
| **External process protocol** | None | N/A (Bash scripts) | N/A (Bash scripts) | Pipes (JSON protocol) | None | None |
| **Provenance/lineage** | Core feature | None built-in | None built-in | Asset lineage | None built-in | None built-in |

---

## Detailed Comparison by Concern

### Command Specification

**Artisan** uses a typed `CommandSpec` hierarchy (base, Local, Docker, Apptainer,
Pixi) declared as class attributes on `OperationDefinition`. The operation's
`execute()` method explicitly calls `run_external_command()`. This is explicit
and testable but requires boilerplate.

**Nextflow** uses raw Bash in `script:` blocks with Groovy interpolation. Simple
but error-prone (escaping issues, no static analysis). The `shell:` variant
fixes the `$` escaping problem. Template files externalize complex scripts.

**Snakemake** offers five directive types (`shell:`, `run:`, `script:`,
`wrapper:`, `notebook:`). The `wrapper:` system is notable — versioned,
community-maintained tool definitions with bundled Conda environments.

**Dagster/Prefect/Airflow** are Python-first — the function body IS the command.
External binaries are invoked via `subprocess.run()` or thin wrappers
(`dagster-shell`, `prefect-shell`, `BashOperator`). Dagster Pipes is the most
sophisticated external process protocol.

**Assessment**: Artisan's typed CommandSpec is superior to Bash strings and
ad-hoc subprocess calls. The main gap vs. Snakemake/Nextflow is that those
frameworks auto-execute the declared command, while Artisan operations must
manually call `run_external_command()`.

### Binary/Tool Management

No framework truly manages binaries. All assume tools are pre-installed
(on PATH, in a container, or in a Conda environment). The differentiation is in
how environments are declared:

- **Nextflow**: `container`, `conda`, `module`, `spack` directives per process
- **Snakemake**: `conda:`, `container:`, `envmodules:` per rule
- **Artisan**: Runtime encoded in CommandSpec type (Local vs Docker vs Apptainer vs Pixi)
- **Dagster/Prefect/Airflow**: Docker images at executor/infrastructure level

**Assessment**: Artisan's approach is sound — the CommandSpec type determines the
runtime. What's missing compared to Nextflow/Snakemake:
- No Conda environment YAML integration (only Pixi)
- No environment module support (relevant for HPC users)
- No binary validation (check that the tool exists before execution)

### Resource Configuration

**Nextflow** has the best model: portable `cpus`, `memory`, `time`, `accelerator`
directives that map to any executor. Dynamic closures with `task.attempt` enable
retry escalation. Labels group resource profiles.

**Snakemake** is similar: `threads` + `resources` dict with callable values for
dynamic allocation. Less structured than Nextflow (arbitrary string keys).

**Artisan's** `ResourceConfig` maps directly to SLURM sbatch flags (`partition`,
`gres`, `cpus_per_task`, `mem_gb`, `time_limit`, `extra_slurm_kwargs`). This is
clear and functional for SLURM but not portable to other executors.

**Dagster/Prefect/Airflow** have no portable resource model — resources are
specified in executor-specific formats (K8s pod specs, work pool configs).

**Assessment**: Artisan's ResourceConfig works but is SLURM-coupled. Nextflow's
approach (portable directives translated per-executor) is the gold standard.

### Argument Formatting

**Artisan** supports two `ArgStyle` modes:
- `HYDRA`: `key=value`
- `ARGPARSE`: `--key value`

**Nextflow/Snakemake** use Bash string interpolation — no structured argument
formatting. Arguments are just part of the shell string.

**Assessment**: Artisan's typed argument formatting is a nice feature that reduces
shell quoting bugs. Could be extended with additional styles (positional args,
short flags, environment variable passing).

### Extensibility

| Framework | Add a new tool | Add a new runtime | Add a new executor |
|---|---|---|---|
| **Artisan** | Subclass OperationDefinition | Subclass CommandSpec + update match statement | Not supported yet |
| **Nextflow** | Write a process | Core team only (container engines) | Plugin API (Gradle) |
| **Snakemake** | Write a rule or wrapper | Core team only | Plugin API (Python entry points) |
| **Dagster** | Create a Resource + ops | Implement Pipes client | Implement Executor interface |
| **Prefect** | Create a Block + tasks | Create a Worker + work pool type | Same as runtime |
| **Airflow** | Create an Operator + Hook | Create a provider package | Implement Executor interface |

**Assessment**: Artisan's extensibility model (subclass OperationDefinition) is
clean and Pythonic. The gap is in runtime extensibility — adding a new
CommandSpec subclass requires modifying `build_command_from_spec()` in
`external_tools.py` (closed for modification). A registry pattern would make
this open for extension.

---

## Recommendations

### Strengths to Preserve

- **Typed CommandSpec hierarchy** — genuinely better than string-based commands
- **Separation of command, resources, and operation logic** — clean architecture
- **ArgStyle abstraction** — reduces shell escaping bugs
- **CommandSpec as data** — inspectable, testable, serializable
- **Optional commands** — pure-Python operations need no CommandSpec

### Gaps to Consider

**High impact, aligns with industry norms:**

- **Portable resource model**: Abstract ResourceConfig away from SLURM-specific
  fields. Define portable fields (cpus, memory_gb, gpu_count, time_limit) that
  get translated per executor, similar to Nextflow's approach. Keep
  executor-specific overrides as an escape hatch.

- **CommandSpec registry**: Replace the match statement in
  `build_command_from_spec()` with a registry pattern so new CommandSpec types
  can be added without modifying core code. Each spec type registers its own
  builder.

- **Binary validation**: Add an optional `validate()` method to CommandSpec that
  checks whether the binary/image exists before execution. Fail fast with a
  clear error rather than failing mid-pipeline.

**Medium impact, improves UX:**

- **Auto-execute mode**: For operations that are pure wrappers around external
  commands (no pre/post processing), allow declaring the command + argument
  mapping and have the framework invoke it automatically. This eliminates the
  boilerplate of writing `execute()` methods that just call
  `run_external_command()`.

- **Per-step command/resource overrides**: Allow pipeline steps to override the
  operation's default CommandSpec or ResourceConfig. Nextflow's config layering
  and Snakemake's profiles show this is important for deployment flexibility.

- **Dynamic resources**: Support resource values as callables, particularly
  for retry escalation (memory × attempt). Both Nextflow and Snakemake
  demonstrate this pattern.

- **Additional ArgStyles**: Consider adding `POSITIONAL`, `SHORT_FLAG` (`-k
  value`), and `ENV_VAR` styles to cover more CLI conventions.

**Lower priority, nice to have:**

- **Conda environment YAML support**: A `CondaCommandSpec` that activates a
  Conda environment defined by a YAML file (like Snakemake's `conda:`
  directive). Relevant for users not on Pixi.

- **Environment module support**: A `ModuleCommandSpec` for HPC users who use
  Lmod/TCL modules. Low effort, high value for that audience.

- **Version capture**: A convention or hook for operations to report the version
  of the tool they ran, stored in artifact metadata/provenance. Nextflow's
  nf-core `versions.yml` pattern is a good model.

---

## Framework Philosophies

| Framework | Philosophy | Command model follows from... |
|---|---|---|
| **Nextflow** | "Pipelines are Bash scripts with metadata" | Scripts are strings; framework handles environment + scheduling |
| **Snakemake** | "Rules produce files from files" | Commands transform files; framework manages Conda + DAG |
| **Dagster** | "Assets are the unit of orchestration" | Python functions are the computation; Pipes bridges external |
| **Prefect** | "Workflows are just Python" | Any Python code is a task; infrastructure is separate |
| **Airflow** | "Operators model every type of work" | Each integration is a specialized operator class |
| **Artisan** | "Operations produce typed artifacts" | Commands are typed data; operations control execution |

Artisan's position is unique: it has the typed command model of the
bioinformatics frameworks combined with the Python-class extensibility of
Dagster/Airflow. The main opportunity is making the resource model portable
and the command registry open for extension.
