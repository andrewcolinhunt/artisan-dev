# Artisan

A Python framework for building computational pipelines with automatic
provenance tracking.

Artisan is intended to be more protocol than platform. Operations declare a
contract (typed inputs, typed outputs, parameters) and the framework uses that
contract to wire things together, track what produced what, and store results
as content-addressed artifacts. The computation inside each operation is a
black box: wrap whatever tools you're already using.

Because the contract is explicit and structured, caching, lineage queries, and
portability across environments come for free. The same pipeline runs on a
laptop or an HPC cluster without changes to the operations themselves.

> **Status:** This project is in active development (v0.1). APIs may change
> between releases.

---

## Why Artisan?

**Simple** — Define steps, connect outputs to inputs, run. No boilerplate,
just Python.

**Extensible** — Wrap any tool as an `OperationDefinition`. Declare inputs and
outputs, implement three methods, and the framework handles the rest.

**Reproducible** — Artifacts are content-addressed and provenance is tracked
automatically. Same content, same identity. Every result traces back to the
inputs and parameters that produced it.

**Scale-invariant** — The same pipeline code runs on a laptop or an HPC
cluster. Switch from local to SLURM execution with a single parameter.

**Queryable** — Artifacts, metrics, and provenance live in a single store,
accessible as dataframes. No log parsing, no directory archaeology.

---

## Quick Start

**Prerequisites:** Python 3.12+, [Pixi](https://pixi.sh)

```bash
# Install Pixi (if needed)
curl -fsSL https://pixi.sh/install.sh | bash

# Clone and install
git clone https://github.com/dexterity-systems/artisan.git
cd artisan

pixi install

# Verify
pixi run python -c "import artisan; print('Artisan installed successfully')"

# Start the Prefect server (orchestrates pipeline execution)
pixi run prefect-start

```

---

## IDE Setup (VSCode)

### Python Interpreter

Set the Pixi environment as your VSCode Python interpreter:

```bash
pixi run which python
# Example output: /home/user/artisan/.pixi/envs/default/bin/python
```

In VSCode: `Ctrl+Shift+P` → "Python: Select Interpreter" → paste the path above.

### Jupyter Kernel

Register the Pixi environment as a Jupyter kernel so notebooks use the correct
packages:

```bash
pixi run install-kernel
```

In VSCode: open a `.ipynb` file → click "Select Kernel" → choose **Artisan**.

### VS Code Jupyter Kernel Slowness (Pixi Environments)

If your pixi Jupyter kernel takes 30+ seconds to start in VS Code, the
`Python Environments` extension (`ms-python.vscode-python-envs`) is likely the
cause. It doesn't recognize pixi as a known environment type and spends 30
seconds trying to activate it before timing out.

**Fix:** Uninstall the `Python Environments` extension (`ms-python.vscode-python-envs`)
in VS Code. The core Python extension works fine without it.

Tracked upstream: [microsoft/vscode-python#25804](https://github.com/microsoft/vscode-python/issues/25804)

---

## Quick Example

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
    params={"scale_factor": 2.0},
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

## Development Setup

### Environments

Pixi manages three environments, all sharing a single dependency solve:

| Environment | Activate with | Purpose |
| ----------- | ------------- | ------- |
| `default` | `pixi run …` | Core runtime — everything needed to run pipelines |
| `dev` | `pixi run -e dev …` | Testing, linting, formatting, notebooks |
| `docs` | `pixi run -e docs …` | Documentation building (Jupyter Book 2) |

### Running Tests

```bash
pixi run -e dev test              # Unit (sequential) + integration (parallel)
pixi run -e dev test-unit         # Unit tests only
pixi run -e dev test-integration  # Integration tests only (parallel)
pixi run -e dev test-seq          # All tests sequentially (for debugging)
```

### Formatting and Linting

```bash
pixi run -e dev fmt               # Ruff format + lint with auto-fix
```

### Shell Completions

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

```bash
pixi run -e docs docs-build       # Build HTML docs
pixi run -e docs docs-serve       # Serve locally at http://localhost:8000
pixi run -e docs docs-clean       # Remove build artifacts
```

- **[Getting Started](docs/getting-started/index.md)** — Installation and
  your first pipeline
- **[Tutorials](docs/tutorials/index.md)** — Interactive notebooks from first
  steps through advanced patterns
- **[How-to Guides](docs/how-to-guides/index.md)** — Task-oriented guides for
  building pipelines, writing operations, and more
- **[Concepts](docs/concepts/index.md)** — Architecture, design principles, and
  system internals
- **[Reference](docs/reference/index.md)** — API reference and coding conventions

---

## Claude Code Integration

Artisan includes a [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
plugin with skills for scaffolding operations, pipelines, and documentation.

| Skill | Description |
|-------|-------------|
| `/artisan:write-operation` | Scaffold or review an `OperationDefinition` subclass |
| `/artisan:write-composite` | Scaffold or review a `CompositeDefinition` subclass |
| `/artisan:write-pipeline` | Scaffold a pipeline script composing operations |
| `/artisan:write-docs` | Write or edit documentation pages, tutorials, and guides |

**Marketplace install** (recommended):

```bash
/plugin marketplace add        # register the plugin from this repo
/plugin install                # install registered plugins
```

**Manual fallback** — point Claude Code at the repo root:

```bash
claude --plugin-dir /path/to/artisan-repo
```

See [Installation — Claude Code plugin](docs/getting-started/installation.md#claude-code-plugin)
for details.

---

## Architecture

Artisan is a domain-agnostic pipeline framework. It handles execution,
orchestration, storage, provenance tracking, and the base operation interface.
Domain-specific operations extend it by subclassing `OperationDefinition`.

See [Architecture Overview](docs/concepts/architecture-overview.md) for details.
