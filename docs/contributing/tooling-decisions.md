# Tooling Decisions

Why the project uses specific tools for environment management and
orchestration.

---

## Why Pixi

The project uses [Pixi](https://pixi.sh) as its environment and task manager.
The project depends on both Python packages and non-Python binaries that are
only available through conda-forge, which rules out a pure-pip approach.

- **Mixed dependency resolution** — conda-forge packages and PyPI packages
  resolved together in a single lockfile.
- **Multi-environment support** — `default` (runtime), `dev` (testing), and
  `docs` (Jupyter Book) environments share a single dependency solve.
- **Built-in task runner** — common commands defined as tasks in
  `pyproject.toml`, run with `pixi run`.

---

## Why Prefect

The framework uses Prefect as its orchestration dispatch layer.

- **Python-native** — no DSL, no YAML. Dependencies flow through normal
  Python variables.
- **TaskRunner extensibility** — the `TaskRunner` interface provides a clean
  extension point for compute backends. The custom `SlurmTaskRunner` integrates
  SLURM via submitit.
- **Thin dispatch layer** — Prefect handles task dispatch and observability,
  but does not own the data model. All artifacts, provenance, and pipeline state
  live in Delta Lake. The framework is not locked to Prefect.

---

## See also

- [Design Principles](../concepts/design-principles.md) — Framework
  design decisions
- [Coding Conventions](coding-conventions.md) — Code style and project
  standards
