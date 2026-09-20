# Tooling Decisions

The tools below support local development, portable execution, and file-based
storage. This page explains the choices and their limits; setup commands live
in the linked guides.

## Environment management: Pixi

Artisan needs Python packages and system tools, including Graphviz for graphs
and Node.js for the documentation build. Pixi resolves conda and PyPI dependencies
into one lockfile with runtime, development, and documentation environments.

Locked commands use the recorded resolution for each supported platform and
stop when the manifest disagrees with it. Dependency changes update the manifest
and lockfile together. The cost is an additional development tool to install.
See [installation](../getting-started/installation.md) and
[using Pixi](../getting-started/using-pixi.md).

## Packaging: Hatchling

Hatchling builds the wheel and source distribution from the `src` layout.
Hatch-vcs derives the package version from Git. This keeps packaging configuration
in `pyproject.toml` and avoids a separate executable setup script.

## Orchestration: native runners

`PipelineManager` owns step preparation, sequencing, caching, and persistence.
A lifecycle runner submits work, collects ordered results, and handles
cancellation for a step. Local execution uses a Python process pool and does not
require an orchestration service.

External providers implement the same runner contract. Scheduler configuration,
remote job ownership, and provider-specific logs stay in those packages.
The optional `artisan-submitit` provider supplies SLURM execution through
[Submitit](https://github.com/facebookincubator/submitit).

Artisan does not supply a scheduling service or a live orchestration dashboard.
Its persisted state supports inspection of runs and their results. See
[external step-runner providers](coding-conventions.md#external-step-runner-providers)
and [execution flow](../concepts/execution-flow.md).

## Storage: Delta Lake and Polars

Delta Lake stores table data and transaction logs on a filesystem or object
store. It does not require a database server. Polars supplies dataframe queries
and CSV processing. This suits workloads that already have shared storage and
need to inspect artifacts, metrics, and provenance together.

Delta transactions are atomic within one table. Artisan adds a logical commit
boundary across related tables: a completion record controls which effects
supported readers accept. Raw Delta queries bypass that boundary.

Supported readers currently load and verify committed rows before returning a
lazy frame. Later filters do not push into that physical read, so storage
partitioning alone does not guarantee low memory use for large queries.
See [storage and Delta Lake](../concepts/storage-and-delta-lake.md) for the
visibility guarantees, recovery behavior, and external-file handling.

## Identity: xxHash

Artisan uses `xxh3_128` to hash canonical artifact and execution descriptions.
Its speed is useful when identities are computed frequently. It is a
non-cryptographic hash; content addressing is not an authenticity or security
mechanism.

Stable identity depends on the canonical description being hashed, not on the
hash function alone. See
[artifacts and content addressing](../concepts/artifacts-and-content-addressing.md).

## Validation: Pydantic

Pydantic models describe operation parameters, schemas, and configuration.
They validate input and provide JSON schemas for discovery. Dataclasses are
also used for internal and transport results where those responsibilities are
not needed.

Parameter descriptions are read from schema fields and Google-style docstrings.
This lets operation discovery use descriptions maintained with the code. See
[operation authoring](../how-to-guides/writing-creator-operations.md).

## Quality checks: Ruff, mypy, and pytest

Ruff formats Python and checks lint rules. Mypy checks types. Pytest covers
runtime contracts and pipeline integration, with pytest-xdist for selected
parallel suites. Notebook tests execute the tutorial cells.

These checks have different scopes. Notebook execution catches code failures;
assertions are needed to catch incorrect explanations of an otherwise successful
result. A documentation build checks rendering but does not establish that an
example is correct. The [task list](../getting-started/using-pixi.md#dev-environment)
and [documentation guide](writing-docs.md#building-and-previewing-docs) describe
how to run the relevant checks.

## Graphs and documentation

Graphviz lays out provenance graphs and renders them as SVG, PNG, or PDF. Both
the Python package and the `dot` executable are needed; Pixi supplies them.

Jupyter Book 2 uses MyST to build Markdown and notebook pages into a static site.
The site keeps workflow explanations separate from API lookup. Exact API facts
remain in code and docstrings; [Python API lookup](../reference/python-api.md)
shows how to inspect them for the installed version.

## Coding assistance

The authoring skills under `skills/` provide guidance for artifact types,
operations, composites, and pipelines. Compatible local agents discover the
same files through `.agents/skills/`; Claude Code also has a plugin distribution
path. These adapters avoid maintaining separate copies of each skill.

The optional MCP server exposes operation discovery and read-only inspection.
It does not launch or cancel pipelines. See
[agent setup](../getting-started/using-claude-code.md) and
[connecting MCP](../how-to-guides/connecting-mcp.md).
