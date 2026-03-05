# Contributing to Artisan

Thank you for your interest in contributing to Artisan! This document explains
how to get started, what we expect, and how we work together.

---

## Before You Start

**Bug fixes** are always welcome as pull requests — no prior discussion needed.

**For anything larger** (new features, refactors, API changes), please
**open a discussion or issue first** before investing significant effort. We ask
contributors to propose a short design document outlining:

- The problem or use case
- Proposed approach and alternatives considered
- Any API or schema changes

This helps us align on direction early and avoid wasted work. We will review
the proposal and give feedback before you start coding.

---

## Development Setup

### Prerequisites

- Python 3.12+
- [Pixi](https://pixi.sh) (manages all dependencies and environments)

### Getting Started

```bash
git clone https://github.com/dexterity-systems/artisan.git
cd artisan
pixi install
```

### Environments

| Environment | Activate with        | Purpose                                       |
| ----------- | -------------------- | --------------------------------------------- |
| `default`   | `pixi run …`         | Core runtime — everything needed to run pipelines |
| `dev`       | `pixi run -e dev …`  | Testing, linting, formatting, notebooks       |
| `docs`      | `pixi run -e docs …` | Documentation building (Jupyter Book 2)       |

### Install Pre-commit Hooks

```bash
pixi run -e dev pre-commit install
```

This ensures linting, formatting, and type checks run automatically on every
commit.

---

## Making Changes

### Workflow

1. Fork the repository and create a branch from `main`.
2. Make your changes in small, focused commits.
3. Add or update tests for any changed behavior.
4. Run the checks below before pushing.
5. Open a pull request against `main`.

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

### Pre-commit (all checks)

```bash
pixi run -e dev pre-commit run --all-files
```

---

## Pull Request Guidelines

- Fill out the pull request template.
- Keep pull requests focused — one concern per PR.
- Include tests for new functionality.
- Make sure CI passes before requesting review.
- Update documentation if you change user-facing behavior.

---

## Code Style

- We use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting.
- All modules must include `from __future__ import annotations` as the first
  import (enforced by Ruff's isort rules).
- Type annotations are expected. We run [mypy](https://mypy-lang.org/) in
  strict mode.
- Follow existing patterns in the codebase for naming, structure, and error
  handling.

---

## Reporting Bugs

Use the [bug report template](https://github.com/dexterity-systems/artisan/issues/new?template=bug_report.yml)
on GitHub. Include:

- Steps to reproduce
- Expected vs. actual behavior
- Python version and OS
- Relevant logs or tracebacks

---

## Security Vulnerabilities

**Do not open a public issue for security vulnerabilities.** See
[SECURITY.md](.github/SECURITY.md) for responsible disclosure instructions.

---

## License

By contributing, you agree that your contributions will be licensed under the
[BSD 3-Clause License](LICENSE).
