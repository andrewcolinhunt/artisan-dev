# Artisan

A Python framework for computational workflows that others can understand,
reuse, and extend.

Artisan helps researchers and AI agents build analyses from reusable tools and
previous computational work. It defines how tools exchange data, handles their
execution, and records how individual results were produced.

The goal is for each investigation to contribute methods and evidence that
later work can use. Tested integrations, reusable workflows, and inspectable
results become available to another researcher or agent without having to
reconstruct the original work.

Artisan grew out of computational protein design. Other fields can use the
same framework with their own tools and data types.

---

## Why Artisan?

### Work with scientific concepts

A package for a field gives its tools a shared vocabulary. In protein design,
this includes structures, sequences, annotations, measurements, and named
methods. Tool developers handle input preparation and output interpretation in
reusable integrations. Researchers combine those tools using familiar
scientific concepts and parameters.

Each integration specifies the inputs it accepts, its settings, and the outputs
it produces. Artisan checks inputs and outputs against those requirements,
helping tools written by different people work together.

### Use accumulated history in new computations

Artisan preserves a queryable history of computational work. It connects each
computation to its inputs and outputs, and each individual result to the data
it was derived from. These relationships remain explicit even when a tool
processes many inputs in a batch.

Later steps and new analyses can use that history to compare methods, evaluate
related results, revisit earlier inputs, and decide what to compute next. For
example, compare protein designs from previous experiments, trace promising
results to their original designs and settings, then select them for further
evaluation.

The recorded history remains available across analyses using the same storage,
including intermediate results and how they were produced.

### Build on earlier work

A tested integration can serve many workflows. Tests and fixes stay with it,
so improvements are available wherever it is used.

Another researcher or agent can inspect how a result was produced, apply a new
evaluation to stored data, or extend an existing workflow. Artisan can reuse
cached results when the declared inputs and settings match a previous
computation. Later investigations can build on both the methods and the
evidence from earlier work.

Artisan runs locally with worker processes. Optional runner providers and tool
endpoints let you run the same operations in other compute environments.

See the [orientation](docs/getting-started/orientation.md) for the execution and
storage model, including its limits.

> **Status:** This project is in early development. APIs may change between
> releases.

---

## Quick start

**Prerequisites:** [Pixi 0.83.0](https://pixi.sh), Linux (x86_64 or aarch64) or
macOS (Apple Silicon). Pixi supplies Python 3.12 and the other dependencies.

```bash
# Install Pixi (if needed)
curl -fsSL https://pixi.sh/install.sh | PIXI_VERSION=v0.83.0 bash
# Restart your terminal if Pixi was newly installed.

# Clone and install
git clone https://github.com/dexterity-systems/artisan.git
cd artisan

pixi install
pixi run setup

# Verify
pixi run python -c "import artisan; print('Artisan installed successfully')"
```

→ **[Installation](docs/getting-started/installation.md)** for Graphviz verification
and notebook setup; **[Getting Started guide](docs/getting-started/index.md)** for
your first pipeline and the mental model behind the framework.

---

## Quick example

This pipeline reuses the built-in example operations to generate datasets,
transform them, and compute metrics. `Filter` follows provenance from each
transformed dataset to its metrics to select results.

```python
from artisan.orchestration import PipelineManager
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.operations.curator import Filter

pipeline = PipelineManager.create(
    name="my_pipeline",
    delta_root="runs/delta",
    staging_root="runs/staging",
    working_root="runs/working",
)
output = pipeline.output

# Generate datasets -> transform -> compute metrics -> filter by score
pipeline.run(operation=DataGenerator, name="generate", params={"count": 5, "seed": 42})
pipeline.run(
    operation=DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
    params={"scale_factor": 2.0, "seed": 42},
)
pipeline.run(
    operation=MetricCalculator,
    name="score",
    inputs={"dataset": output("transform", "dataset")},
)
pipeline.run(
    operation=Filter,
    name="filter",
    inputs={"passthrough": output("transform", "dataset")},
    params={
        "criteria": [
            {"metric": "distribution.median", "operator": "gt", "value": 0.5},
        ]
    },
)

result = pipeline.finalize()
```

## Development setup

### Environments

Pixi manages three environments, all sharing a single dependency solve:

| Environment | Activate with | Purpose |
| ----------- | ------------- | ------- |
| `default` | `pixi run …` | Core runtime — everything needed to run pipelines |
| `dev` | `pixi run -e dev …` | Testing, linting, formatting, notebooks, read-only MCP server |
| `docs` | `pixi run -e docs …` | Documentation building (Jupyter Book 2) |

Prepare the dev environment and explicitly install the repository's Git hooks:

```bash
pixi install -e dev
pixi run -e dev setup
pixi run -e dev install-hooks
```

`setup` prepares Graphviz in the selected environment. To use that environment
in notebooks, run `pixi run -e dev install-kernel` separately. Kernel
registration sets the **Artisan** kernel to the environment that ran the task.

### Pixi lockfile workflow

Use ordinary `pixi install` and `pixi run` for local development. CI and agent
tasks unrelated to environment changes use `--locked` to prevent incidental
lockfile updates. Review required lockfile changes alongside dependency or
platform edits and commit them together. Comments and task-only edits do not
require relocking. See
[reviewing lockfile changes](docs/getting-started/using-pixi.md#reviewing-lockfile-changes)
for the refresh and review workflow.

Each Git worktree uses its own Pixi environments. Run commands from the checkout
you are editing; see [working in multiple worktrees](docs/getting-started/using-pixi.md#working-in-multiple-worktrees).

### Running tests

```bash
pixi run -e dev test
pixi run -e dev test-unit
```

`test` includes local notebooks and S3 tests; it needs Docker/MinIO or a
configured S3 endpoint. Modal checks are separate. See the
[development task list](docs/getting-started/using-pixi.md#dev-environment) for
focused suites and their resource requirements.

### Formatting and linting

```bash
pixi run -e dev fmt
```

### Shell completions

Enable tab-completion for `pixi` commands and tasks:

```bash
# Bash — add to ~/.bashrc
echo 'eval "$(pixi completion --shell bash)"' >> ~/.bashrc

# Zsh — add to ~/.zshrc
echo 'eval "$(pixi completion --shell zsh)"' >> ~/.zshrc
```

Restart your shell or `source` the file to activate.

---

## Documentation

See the [changelog](CHANGELOG.md) for release changes and upgrade requirements.

```bash
pixi run -e docs docs-build
pixi run -e docs docs-serve
pixi run -e docs docs-clean
```

- **[Getting Started](docs/getting-started/index.md)** — Installation and first
  steps
- **[Tutorials](docs/tutorials/index.md)** — Interactive notebooks from first
  steps through advanced patterns
- **[How-to Guides](docs/how-to-guides/index.md)** — Task-oriented guides for
  building pipelines, writing operations, and more
- **[Concepts](docs/concepts/index.md)** — Architecture, design principles, and
  system internals
- **[Reference](docs/reference/index.md)** — Public API lookup and terminology

---

## Coding agent integration

Start your coding agent in the repository and have it read
[AGENTS.md](AGENTS.md) for shared project conventions. Artisan also includes
portable Agent Skills for scaffolding artifact types, operations, composites,
and pipelines, and for inspecting existing runs. The skill packages live under
`skills/`; agents that support `.agents/skills/` discover the same files there.

| Skill | Description |
|-------|-------------|
| `artifact-write` | Scaffold or review a custom artifact model and its registration |
| `operation-write` | Scaffold or review an `OperationDefinition` subclass |
| `composite-write` | Scaffold or review a `CompositeDefinition` subclass |
| `pipeline-write` | Scaffold a pipeline script composing operations |
| `pipeline-operate` | Inspect runs, diagnose failures, trace outputs, and assess or compare available evidence |

For another repository, install complete skill folders, including their
references, through your agent's supported skill installation mechanism.
Claude Code users can also use the repository's plugin distribution. See
[Using Coding Agents](docs/getting-started/using-coding-agents.md) for setup,
example prompts, and personal settings.

`pipeline-operate` uses local Python or CLI inspection when the project's
environment can access the selected store. Base Artisan includes these readers,
offline guidance and request/response schemas. No MCP server is needed:

```bash
pixi run artisan guide show pipeline-operate
pixi run artisan inspect list --json
```

The guidance identifies the executing package version and the exact text being
read. Installing the Python package makes this guidance available; host skill
installation is separate. See [Inspecting Evidence](docs/how-to-guides/inspecting-evidence.md)
for local queries and [Exporting Evidence](docs/how-to-guides/exporting-evidence.md)
for complete saved assessments and comparisons.

For a selected remote connection, [connect an MCP client](docs/how-to-guides/connecting-mcp.md)
to the optional read-only `artisan-mcp` server and use its advertised schemas.
An explicit remote target stays selected even when a local store contains the
same run ID.

---

## Architecture

Artisan is a domain-agnostic pipeline framework. It handles execution,
orchestration, storage, provenance tracking, and the base operation interface.
Domain-specific operations extend it by subclassing `OperationDefinition`.

See [Architecture Overview](docs/concepts/architecture-overview.md) for details.
