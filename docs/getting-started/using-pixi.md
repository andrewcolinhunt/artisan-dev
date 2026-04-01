# Using Pixi

Pixi manages Artisan's environments, dependencies, and tasks. This page covers
what you need for day-to-day development. For general Pixi documentation, see the
[official getting started guide](https://pixi.sh/latest/getting_started/).

---

## Running commands in an environment

Artisan defines three environments. Each bundles the dependencies needed for a
specific workflow.

| Environment | Flag | What it includes | When to use it |
|-------------|------|------------------|----------------|
| `default` | *(none)* | Python 3.12, Artisan, Prefect, scientific stack | Running pipelines and scripts |
| `dev` | `-e dev` | Everything in default + pytest, ruff, ipython, build tools | Testing, formatting, debugging |
| `docs` | `-e docs` | Everything in default + jupyter-book, Node.js | Building documentation |

```bash
# Run a command in the default environment
pixi run python script.py

# Run a command in a named environment
pixi run -e dev pytest
pixi run -e docs docs-build
```

All environments share a single solve group, so package versions are consistent
across them.

---

## Opening an interactive shell

`pixi run` is best for one-off commands. For interactive work — debugging,
exploring data, running multiple commands — open a shell instead:

```bash
pixi shell                          # Default environment
pixi shell -e dev                   # Dev environment (pytest, ruff, ipython)
pixi shell -e docs                  # Docs environment (jupyter-book, node)
```

Inside a Pixi shell, commands run directly without the `pixi run` prefix:

```bash
$ pixi shell -e dev
(artisan-dev) $ pytest tests/artisan/storage/
(artisan-dev) $ ruff check src/
(artisan-dev) $ exit                # Return to your regular shell
```

You can have multiple shells open in different terminals — each can use a
different environment.

---

## Working across multiple projects with workspaces

If you work across multiple Pixi projects, workspaces let you run tasks in one
project from another without changing directories.

```bash
# Register a workspace member (name comes from pyproject.toml)
cd /path/to/repo
pixi workspace register

# Or set the name explicitly
pixi workspace register --name custom-name

# Run tasks or open a shell from anywhere
pixi run -w workspace-name task-name
pixi shell -w workspace-name
```

---

## Adding new dependencies

Artisan uses both conda-forge (for Python, system libraries) and PyPI (for
Python-only packages). Use `--pypi` for pure-Python packages and bare `pixi add`
for everything else.

```bash
# Add a conda-forge package to the default feature
pixi add numpy

# Add a PyPI package
pixi add --pypi requests

# Add to a specific feature (dev or docs)
pixi add --feature dev --pypi icecream
pixi add --feature docs nodejs

# Pin a version
pixi add "numpy>=1.24,<2"
pixi add --pypi "requests>=2.31"
```

After adding a dependency, `pixi.lock` updates automatically. Commit both
`pyproject.toml` and `pixi.lock` together.

---

## Using a local copy of a dependency

If you need to edit a dependency's source code — to debug an issue, develop a
feature, or test a fix — you can point Pixi at a local clone instead of the
published package.

Open `pyproject.toml` and add a path dependency override in the
`[tool.pixi.pypi-dependencies]` section:

```toml
[tool.pixi.pypi-dependencies]
some-package = { path = "/path/to/your/local-clone", editable = true }
```

With `editable = true`, Python picks up your local changes immediately — no
reinstall needed. This is the same concept as `pip install -e .` but managed
through Pixi.

When you're done, remove the override and run `pixi install` to restore the
locked version.

---

## Task reference

Tasks are shortcuts defined in `pyproject.toml`. They save you from remembering
flags and command sequences.

### Default environment

| Task | Command | Description |
|------|---------|-------------|
| `prefect-start` | `prefect-server start --bg` | Start the Prefect server in the background |
| `prefect-stop` | `prefect-server stop --force` | Stop the Prefect server |
| `install-kernel` | `python -m ipykernel install ...` | Register the Artisan Jupyter kernel |

### Dev environment

| Task | Command | Description |
|------|---------|-------------|
| `test` | `pytest -m 'not slow' && pytest -m slow -n 4` | Run unit tests (sequential) then integration tests (parallel) |
| `test-unit` | `pytest -m 'not slow'` | Run only unit tests |
| `test-integration` | `pytest -m slow -n 4` | Run only integration tests (parallel) |
| `test-seq` | `pytest` | Run all tests sequentially (useful for debugging) |
| `fmt` | `ruff format . && ruff check --fix .` | Format and lint the codebase |
| `build-dist` | `rm -rf dist/ && python -m build` | Build distribution packages |
| `check-dist` | `python -m twine check dist/*` | Validate distribution packages |
| `upload-testpypi` | `python -m twine upload --repository testpypi dist/*` | Upload to Test PyPI |
| `upload-pypi` | `python -m twine upload dist/*` | Upload to PyPI |

### Docs environment

| Task | Command | Description |
|------|---------|-------------|
| `docs-build` | `jupyter-book build --html` | Build the documentation site |
| `docs-clean` | `jupyter-book clean` | Remove built documentation |
| `docs-serve` | `python -m http.server -d _build/html 8000` | Serve docs locally on port 8000 |

---

## Cross-references

- [Installation](installation.md) — First-time setup and IDE configuration
- [Tooling Decisions](../contributing/tooling-decisions.md) — Why Artisan uses Pixi
- [Pixi documentation](https://pixi.sh/latest/) — Full reference for all Pixi commands
