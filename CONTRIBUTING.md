# Contributing to Artisan

Set up a development environment, make a focused change, and run the relevant
checks before opening a pull request.

---

## Before you start

**Bug fixes** are always welcome as pull requests — no prior discussion needed.

**For anything larger** (new features, refactors, API changes), please
**open a discussion or issue first** before investing significant effort. We ask
contributors to propose a short design document outlining:

- The problem or use case
- Proposed approach and alternatives considered
- Any API or schema changes

Wait for feedback on the proposal before implementing it.

---

## Development setup

### Prerequisites

- [Pixi 0.66.0](docs/getting-started/installation.md) supplies Python and the
  project dependencies.

### Getting started

```bash
git clone https://github.com/dexterity-systems/artisan.git
cd artisan
pixi install -e dev
pixi run -e dev setup
```

### Environments

| Environment | Activate with | Purpose |
| --- | --- | --- |
| `default` | `pixi run …` | Core runtime for pipelines |
| `dev` | `pixi run -e dev …` | Testing, linting, formatting, notebooks |
| `docs` | `pixi run -e docs …` | Documentation building (Jupyter Book 2) |

### Install pre-commit hooks

```bash
pixi run -e dev install-hooks
```

This ensures linting, formatting, and type checks run automatically on every
commit.

---

## Making changes

### Workflow

1. Fork the repository and create a branch from `main`.
2. Make your changes in small, focused commits.
3. Add or update tests for any changed behavior.
4. Run the checks below before pushing.
5. Open a pull request against `main`.

### Running tests

```bash
pixi run -e dev test
```

This runs unit, integration, S3, and local notebook tests. S3 tests need
Docker/MinIO or a configured endpoint. Modal tests and notebooks run separately.
See the [development tasks](docs/getting-started/using-pixi.md#dev-environment)
for the focused suites and their resource requirements.

When changing documentation, follow the
[documentation guide](docs/contributing/writing-docs.md): verify the examples'
outcomes and build the site. Keep exact API descriptions with the source;
guides should explain tasks and relevant behavior.

### Formatting and linting

```bash
pixi run -e dev fmt               # Ruff format + lint with auto-fix
```

### Pre-commit (all checks)

```bash
pixi run -e dev pre-commit run --all-files
```

---

## Pull request guidelines

- Fill out the pull request template.
- Keep pull requests focused — one concern per PR.
- Include tests for new functionality.
- Make sure CI passes before requesting review.
- Update documentation if you change user-facing behavior.

---

## Code style

- We use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting.
- Use `from __future__ import annotations` as the first import. Ruff enforces
  this except for explicitly configured file-specific exceptions.
- Type annotations are expected. We run [mypy](https://mypy-lang.org/) in
  strict mode.
- Follow existing patterns in the codebase for naming, structure, and error
  handling.

---

## Reporting bugs

Use the [bug report template](https://github.com/dexterity-systems/artisan/issues/new?template=bug_report.yml)
on GitHub. Include:

- Steps to reproduce
- Expected vs. actual behavior
- Python version and OS
- Relevant logs or tracebacks

---

## Security vulnerabilities

**Do not open a public issue for security vulnerabilities.** See
[SECURITY.md](.github/SECURITY.md) for responsible disclosure instructions.

---

## License

By contributing, you agree that your contributions will be licensed under the
[BSD 3-Clause License](LICENSE).
