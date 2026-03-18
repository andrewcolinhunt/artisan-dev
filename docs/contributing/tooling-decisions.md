# Tooling Decisions

Why the project uses specific tools for environment management, orchestration,
storage, quality, and documentation. Each section states the problem the tool
solves, why it was chosen over alternatives, and what trade-offs come with it.

---

## Environment management: Pixi

The project uses [Pixi](https://pixi.sh) as its environment and task manager.

**The problem.** The project depends on both Python packages (from PyPI) and
non-Python system binaries — PostgreSQL (for Prefect server), Graphviz (`dot`
binary for provenance graph rendering), and Node.js (for Jupyter Book 2). A
pure-pip approach cannot install these.

**Why Pixi:**

- **Mixed dependency resolution.** Conda-forge packages and PyPI packages
  resolved together in a single lockfile (`pixi.lock`). No manual coordination
  between `conda` and `pip`.
- **Multi-environment support.** Three environments — `default` (runtime), `dev`
  (testing and linting), and `docs` (Jupyter Book) — share a single dependency
  solve. Each environment adds only the features it needs.
- **Built-in task runner.** Common commands defined as tasks in `pyproject.toml`
  and run with `pixi run`. No need for a separate Makefile or `nox`/`tox`
  configuration.
- **Reproducible lockfile.** `pixi.lock` pins exact versions for all
  dependencies across all environments. Contributors get identical environments
  regardless of platform.

**The trade-off:** Pixi is less widely known than conda or Poetry. Contributors
need to install it separately. The project mitigates this by documenting the
installation step prominently in the getting-started guide.

---

## Build system: Hatchling

The project uses [Hatchling](https://hatch.pypa.io/) as its Python build
backend.

**Why Hatchling:**

- **Standards-compliant.** Implements PEP 517/518 build backend interface.
- **Minimal configuration.** Build settings fit in a few lines of
  `pyproject.toml` — no `setup.py` or `setup.cfg` needed.
- **`src` layout support.** The project uses `src/artisan/` as its package
  root. Hatchling handles this natively.

---

## Orchestration dispatch: Prefect

The framework uses [Prefect](https://www.prefect.io/) as its orchestration
dispatch layer, not as its workflow engine. Artisan owns pipeline definition,
step sequencing, caching, and provenance. Prefect handles parallel task dispatch
and observability.

**Why Prefect:**

- **Python-native.** No DSL, no YAML. Task dispatch expressed as decorated
  Python functions. Dependencies flow through normal Python variables.
- **TaskRunner extensibility.** The `TaskRunner` interface provides a clean
  extension point for compute backends. The project ships two backends: a
  `ProcessPoolTaskRunner` for local execution and a `SlurmTaskRunner` (via
  `prefect-submitit`) for HPC clusters. Additional backends (Kubernetes, cloud)
  can be added by implementing the same interface.
- **Thin dispatch layer.** Prefect handles task dispatch and provides a UI for
  run observability, but does not own the data model. All artifacts, provenance,
  and pipeline state live in Delta Lake. The framework is not locked to Prefect —
  replacing it would mean swapping the dispatch layer, not rewriting the
  application.

**The trade-off:** Prefect requires a server process for SLURM execution
(workers need a coordination point). The project manages this via
`prefect-start` and `prefect-stop` pixi tasks. For local-only execution, the server is not
required.

**See:** [How Artisan Uses
Prefect](../reference/comparison-to-alternatives.md#comparison-prefect-relationship)
for the dispatch architecture diagram.

---

## SLURM integration: submitit and prefect-submitit

HPC cluster execution uses
[submitit](https://github.com/facebookincubator/submitit) (Meta's SLURM job
submission library) through the
[prefect-submitit](https://github.com/dexterity-systems/prefect-submitit)
bridge.

**Why submitit:**

- **Pythonic job submission.** Submit SLURM jobs as Python function calls, not
  shell scripts. Resource requests (GPUs, memory, time limits) are passed as
  keyword arguments.
- **Job arrays.** Batch many execution units into a single SLURM job array,
  reducing scheduler overhead.

**Why prefect-submitit:**

- **Bridges Prefect and SLURM.** Implements Prefect's `TaskRunner` interface
  using submitit, so the same dispatch code path works for both local and SLURM
  execution.
- **Server discovery.** Manages Prefect server connection for remote SLURM
  workers that need to communicate results back to the orchestrator.

---

## Storage: Delta Lake and Polars

The framework stores all pipeline results — artifacts, provenance edges,
execution records — in [Delta Lake](https://delta.io/) tables, queried via
[Polars](https://pola.rs/).

**The problem.** HPC clusters provide shared filesystems, not managed database
services. A storage solution that requires PostgreSQL or Redis is impractical.
Meanwhile, storing results as loose files creates filesystem bloat (millions of
small files) and makes querying results difficult.

**Why Delta Lake:**

- **No external services.** Delta Lake is Parquet files plus a transaction log.
  No daemon, no connection string, no port to manage. Works on any POSIX
  filesystem including NFS.
- **ACID transactions.** Each commit is atomic. Results from thousands of
  workers are committed in a single transaction — all visible or none visible.
  No partial writes, no corruption from interrupted commits.
- **Columnar storage.** Artifact content, metrics, and metadata stored as table
  rows, not individual files. A pipeline producing 50,000 metrics stores them
  in a single table, not 50,000 JSON files.
- **Ecosystem compatibility.** Delta tables are readable by Polars, DuckDB,
  pandas, Spark, or any Delta-compatible tool. Results are not locked in a
  proprietary format.

**Why Polars (not pandas):**

- **Native Delta Lake support.** `pl.scan_delta()` reads Delta tables directly
  with lazy evaluation and predicate pushdown. No intermediate conversion step.
- **Lazy evaluation.** Queries are optimized before execution. Partition pruning
  happens automatically — querying step 3 reads only step 3's Parquet files.
- **Type-safe.** Column types are enforced, catching schema mismatches at read
  time rather than producing silent data corruption.

**The trade-off:** Delta Lake adds write overhead (transaction log management,
ZSTD compression). This is negligible compared to operation execution time, but
means the framework is not optimized for sub-second microbenchmarks.

**See:** [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) for the
full table architecture and staging-commit pattern.

---

## Content hashing: xxhash

All content-addressed IDs use
[xxHash](https://github.com/Cyan4973/xxHash) (specifically `xxh3_128`).

**Why xxh3_128:**

- **Speed.** xxh3 is one of the fastest non-cryptographic hash functions
  available. Content addressing runs on every artifact, so hash speed directly
  affects pipeline throughput.
- **128-bit output.** 128 bits provides a collision probability of roughly
  1 in 10^38, sufficient for content addressing without cryptographic
  guarantees.
- **Deterministic.** Same content always produces the same 32-character hex
  digest, regardless of platform. This is the foundation of the caching system.

**Why not SHA-256?** Cryptographic security is unnecessary for content
addressing. xxh3_128 is an order of magnitude faster than SHA-256 and the
collision resistance is more than sufficient for this use case.

---

## Schema validation: Pydantic

All data models — artifact schemas, execution configs, pipeline specs, operation
parameters — use [Pydantic](https://docs.pydantic.dev/) for validation.

**Why Pydantic:**

- **Declarative schemas.** Models are plain Python classes with type
  annotations. Validation rules are expressed as types and field constraints,
  not imperative checks.
- **Serialization.** Models serialize to and from JSON/dict representations,
  which is used for parameter hashing (cache keys), execution unit construction,
  and config persistence.
- **Fail-fast validation.** Invalid data raises `ValidationError` at
  construction time with clear field-level error messages. This aligns with the
  framework's fail-fast philosophy.

---

## Formatting and linting: Ruff

The project uses [Ruff](https://docs.astral.sh/ruff/) for formatting and
linting, replacing Black, isort, flake8, and their plugin ecosystem with a
single tool.

**Why Ruff:**

- **Single tool.** Formatting, import sorting, and lint rules in one binary. No
  need to configure or version-lock multiple tools.
- **Speed.** Ruff runs in milliseconds, making it practical to run on every
  save and in pre-commit hooks without noticeable delay.
- **Extensible rule set.** The project enables rules from flake8-bugbear,
  flake8-comprehensions, pylint, pyupgrade, and others — all configured in
  `pyproject.toml` under `[tool.ruff.lint]`.

The project also enforces `from __future__ import annotations` in all files via
Ruff's isort integration (`isort.required-imports`).

---

## Static type checking: mypy

The project uses [mypy](https://mypy.readthedocs.io/) in strict mode for static
type analysis.

**Why mypy:**

- **Strict mode.** Catches type errors that would otherwise surface at runtime,
  particularly in code paths that are difficult to reach in tests.
- **CI integration.** Runs as a pre-commit hook, so type errors are caught
  before code reaches the repository.

---

## Pre-commit hooks

The project uses [pre-commit](https://pre-commit.com/) to run automated checks
before each commit. The hook configuration (`.pre-commit-config.yaml`) includes:

| Hook | Purpose |
|------|---------|
| **ruff** (format + lint) | Code formatting and lint rules |
| **mypy** | Static type checking |
| **prettier** | YAML, Markdown, and JSON formatting |
| **blacken-docs** | Format Python code blocks in documentation |
| **codespell** | Catch common misspellings |
| **shellcheck** | Lint shell scripts |
| **validate-pyproject** | Validate `pyproject.toml` schema |
| **check-jsonschema** | Validate GitHub workflow and ReadTheDocs configs |
| **sp-repo-review** | Scientific Python community standards compliance |
| **pre-commit-hooks** | File hygiene (trailing whitespace, merge conflicts, large files, test naming) |

Pre-commit runs in CI and locally. Install hooks with `pre-commit install` from
the dev environment.

---

## Testing: pytest and pytest-xdist

The project uses [pytest](https://docs.pytest.org/) for all tests, with
[pytest-xdist](https://github.com/pytest-dev/pytest-xdist) for parallel
integration test execution.

**Test organization:**

- **Unit tests** (`@pytest.mark.fast` or unmarked) run sequentially. They are
  fast, isolated, and test individual functions.
- **Integration tests** (`@pytest.mark.slow`) run full pipeline executions
  against real Delta Lake stores. They run in parallel via `pytest -n 4` to
  keep total test time manageable.

**Why sequential unit tests + parallel integration tests?** Unit tests are fast
enough that parallelization overhead is not worth the complexity. Integration
tests are slow enough (seconds each) that running them in parallel provides a
meaningful speedup, and they are designed to be independent (each creates its own
temporary Delta Lake store).

---

## Provenance visualization: Graphviz

Provenance graphs are rendered using
[Graphviz](https://graphviz.org/) (the `dot` layout engine) via the
[graphviz Python package](https://graphviz.readthedocs.io/).

**Why Graphviz:**

- **Automatic layout.** The `dot` engine handles directed graph layout — node
  positioning, edge routing, rank assignment — without manual coordinates. This
  matters because provenance graphs can have hundreds of nodes.
- **Multiple output formats.** SVG for notebooks, PDF for reports, PNG for
  quick inspection. All from the same graph definition.

Graphviz requires a system binary (`dot`), which is why it is installed via
conda-forge rather than pip. The Python `graphviz` package provides the API;
the conda-forge `graphviz` package provides the binary.

---

## Documentation: Jupyter Book 2 and MyST

Documentation is built with [Jupyter Book 2](https://mystmd.org/guide) using
[MyST Markdown](https://mystmd.org/).

**Why Jupyter Book 2:**

- **Executable tutorials.** Tutorials are Jupyter notebooks (`.ipynb`) that
  readers can run. Jupyter Book renders them alongside prose Markdown pages in
  the same site.
- **MyST Markdown.** Richer than standard Markdown — cross-references,
  admonitions, tables of contents, and other directives that technical
  documentation needs.
- **Single-command build.** `jupyter-book build --html` produces a static site
  from the `docs/` directory.

Jupyter Book 2 requires Node.js, which is installed via conda-forge in the
`docs` environment.

---

## Console output: Rich

The framework uses [Rich](https://rich.readthedocs.io/) for styled console
output in logging. Log levels and URLs are colorized via regex highlighting,
providing readable output without requiring a full TUI framework.

---

## AI assistance: Claude Code plugin

**The problem.** Writing operations and pipelines requires knowing framework
conventions, base classes, and patterns. New contributors face a steep ramp-up.

**Why a plugin.** The plugin is defined at the repo root using the inline
marketplace pattern (`.claude-plugin/marketplace.json` with `"source": "./"`).
Skills live in `skills/` at the repo root and are distributed via the Claude Code
marketplace — no `--plugin-dir` flag needed.

**What's included.** Four skills in `skills/`:

- `write-operation` — scaffold or review an `OperationDefinition` subclass
- `write-composite` — scaffold or review a `CompositeDefinition` subclass
- `write-pipeline` — scaffold a pipeline script composing operations
- `write-docs` — write or edit documentation pages, tutorials, and guides

**The trade-off.** Requires [Claude Code](https://docs.anthropic.com/en/docs/claude-code).
Skills need updating when framework APIs change.

---

## See also

- [Design Principles](../concepts/design-principles.md) — Framework design
  decisions that motivated these tool choices
- [Coding Conventions](coding-conventions.md) — Code style and project standards
- [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) — Deep dive
  into the storage architecture
- [Comparison to Alternatives](../reference/comparison-to-alternatives.md) —
  How Artisan relates to other workflow frameworks
