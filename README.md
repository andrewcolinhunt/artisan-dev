# Artisan

A Python framework for composable, observable computational workflows.

Artisan grew out of computational protein design, where analyses combine many
tools and use the history of individual results to guide further computation.
It handles the tracking and matching that previously required custom code around
existing workflow tools. Other fields can use the same framework with their own
tools and data types.

Artisan helps researchers and AI agents build new analyses from reusable tools
and previous computational work. Wrap Python code and external tools in
operations, combine them into workflows, and reuse useful combinations in other
analyses. Tests and fixes stay with each integration, so improvements are
available wherever it is used.

Operations declare the sources of each new result. Artisan records these
relationships alongside the inputs, parameters, and execution history, forming
a provenance graph of individual results across the analysis. The full graph is
available both to inspect the work and to use its history in further computation.

---

## Why Artisan?

- **Compose tools into new analyses.** Bundle a transformation and its
  evaluations into a reusable workflow, called a composite. Use it in different
  pipelines or combine it with other operations. Each internal step remains
  visible and can be inspected individually.
- **Understand how each result was produced.** Trace an output back to its
  inputs and the operations and parameters involved, even when those operations
  processed many results in a batch. Query results, measurements, execution
  records, and their relationships as dataframes.
- **Automate decisions you can't make in advance.** Transform a collection of
  inputs, evaluate the outputs several steps later, then select the original
  inputs for further work using those evaluations. Artisan follows the recorded
  relationships to find the relevant measurements and match results with their
  sources.
- **Build on work you've already done.** Each tested integration and reusable
  workflow becomes available to future analyses. Stored results remain
  queryable, so you can revisit intermediate work as your questions change.
  Artisan can reuse cached results when an operation's inputs and configuration
  match a previous computation.

Artisan runs locally with worker processes. Optional runner providers and tool
endpoints let you run the same operations in other compute environments.

See the [orientation](docs/getting-started/orientation.md) for the execution and
storage model, including its limits.

> **Status:** This project is in early development. APIs may change between
> releases.

---

## Quick start

**Prerequisites:** [Pixi 0.66.0](https://pixi.sh), Linux (x86_64 or aarch64) or
macOS (Apple Silicon). Pixi supplies Python 3.12 and the other dependencies.

```bash
# Install Pixi (if needed)
curl -fsSL https://pixi.sh/install.sh | PIXI_VERSION=v0.66.0 bash
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
lockfile updates. Review lockfile changes alongside relevant `pyproject.toml`
edits, including tooling edits, and commit them together. See
[reviewing lockfile changes](docs/getting-started/using-pixi.md#reviewing-lockfile-changes)
for the refresh and review workflow.

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
