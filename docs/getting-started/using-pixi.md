# Using Pixi

Pixi manages Artisan's environments, dependencies, and tasks. For general Pixi
documentation, see the
[official getting started guide](https://pixi.sh/latest/getting_started/).

---

## Running commands in an environment

Choose the environment for your task:

| Environment | Flag | What it includes | When to use it |
|-------------|------|------------------|----------------|
| `default` | *(none)* | Python 3.12, Artisan, scientific stack | Running pipelines and scripts |
| `dev` | `-e dev` | Everything in default + pytest, ruff, ipython, build tools, read-only MCP server | Testing, formatting, debugging |
| `docs` | `-e docs` | Everything in default + jupyter-book, Node.js | Building documentation |

```bash
# Run a command in the default environment
pixi run python script.py

# Run a command in a named environment
pixi run -e dev pytest
pixi run -e docs docs-build
```

All environments share a single solve group, so package versions are consistent
across them. `mcp` is a feature included in `dev`, not a separate environment.
For local development, ordinary Pixi commands use or refresh the lockfile as
needed. CI and agents use `--locked` for tasks unrelated to environment changes;
see [reviewing lockfile changes](#reviewing-lockfile-changes).

After installing an environment, run `setup` there to prepare Graphviz. Git
hooks and notebook kernels have separate tasks with explicit side effects:

```bash
pixi install -e dev
pixi run -e dev setup
pixi run -e dev install-hooks
pixi run -e dev install-kernel  # Only when using notebooks
```

`setup` is safe to repeat and only prepares the selected environment.
`install-hooks` installs this repository's pre-commit hook. `install-kernel`
registers a user **Artisan** kernel pointing to the environment that ran it;
running it again from another environment replaces that kernel registration.

---

## Opening an interactive shell

Use `pixi run` for one command or open a shell for interactive work:

```bash
pixi shell                 # Default environment
pixi shell -e dev          # Dev environment (pytest, ruff, ipython)
pixi shell -e docs         # Docs environment (jupyter-book, node)
```

Inside a Pixi shell, commands run directly without the `pixi run` prefix:

```bash
$ pixi shell -e dev
(artisan-dev) $ pytest tests/artisan/storage/
(artisan-dev) $ ruff check src/
(artisan-dev) $ exit                # Return to your regular shell
```

Each terminal can use a different environment.

---

(working-in-multiple-worktrees)=
## Working in multiple worktrees

Run Pixi from the Git worktree you are editing. Each checkout keeps its own
environments under `.pixi/envs/`, with its editable Artisan install pointing at
that checkout. Install the development environment when starting a new worktree:

```bash
cd /path/to/your/worktree
pixi install -e dev
pixi run -e dev python -c "import sys, artisan; print(sys.executable); print(artisan.__file__)"
```

Both printed paths should belong to that worktree. Keep these installed
environments separate: do not copy or symlink another worktree's `.pixi/envs`
directory or use its interpreter. Pixi's shared package cache already reuses
downloads across checkouts. Agents should use `--locked` for routine commands.

---

## Working across multiple projects with workspaces

Workspaces let you run tasks across Pixi projects without changing directories.

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

(reviewing-lockfile-changes)=
## Reviewing lockfile changes

Keep `pixi.lock` in version control. Changes to dependencies, Python requirements,
or supported platforms can require a refresh. Use the existing lockfile when
resolving them, then review the changes:

```bash
pixi lock
git diff -- pyproject.toml pixi.lock
```

With Pixi 0.83.0, comments and task-only edits do not require relocking. Artisan's
version is a tracked value in `pyproject.toml`; source edits, commits, and dirty
working trees do not change it. A version-only edit does not require a lockfile
update; the lock records the local package's dependency requirements.

Check that lockfile changes match the intended manifest edits, including any
dependency version or source changes. Commit required updates with the manifest.
For unexpected changes, investigate the cause before restoring anything. Avoid
deleting the lockfile or running a broad dependency update just to refresh local
package metadata.

CI and agent tasks that should not rewrite the lockfile use `--locked`:

```bash
pixi run --locked -e dev test-unit
```

If this reports a stale lockfile after an intentional manifest edit, refresh
and review it using the commands above, then rerun the check.

After changing the package version, `pixi install` or the next `pixi run`
synchronizes the installed metadata, including in locked mode. Add `-e dev` or
`-e docs` for the environment you use. Editable source code changes need no
reinstall. See [release preparation](../../CONTRIBUTING.md#package-versions-and-releases)
for when to change the package version.

---

## Using a local copy of a dependency

To work on a dependency's source, point Pixi at a local clone. Add a path
override to `pyproject.toml`:

```toml
[tool.pixi.pypi-dependencies]
some-package = { path = "/path/to/your/local-clone", editable = true }
```

With `editable = true`, Python picks up source changes without a reinstall.

Run `pixi install` to resolve the intentional dependency change. When you're
done, remove the override and run `pixi install` again to resolve the published
dependency. Review and commit `pyproject.toml` and `pixi.lock` together for each
dependency change you keep.

---

## Task reference

Tasks are defined in `pyproject.toml`. Run `pixi task list` to see the available
tasks; the [project manifest](https://github.com/dexterity-systems/artisan/blob/main/pyproject.toml)
contains their exact commands. Invoke a task with `pixi run -e ENV TASK`.

### Default environment

| Task | Purpose |
|------|---------|
| `setup` | Prepare Graphviz in the selected environment; safe to repeat |
| `install-kernel` | Register the selected environment as the user Artisan kernel |

### Dev environment

| Task | Purpose and resource needs |
|------|----------------------------|
| `install-hooks` | Install this repository's Git hooks |
| `test` | Run unit, integration, S3, and local tutorial suites; requires local Docker; S3 uses MinIO or a configured endpoint |
| `test-unit` | Unit tests; no external services |
| `test-integration` | End-to-end local pipelines and container checks; requires local Docker |
| `test-docker` | Container cancellation and working-directory checks; requires local Docker |
| `prepare-docker-tests` | Check the selected Docker engine and prepare or reuse the pinned test image |
| `test-s3` | S3 tests; needs Docker/MinIO or `ARTISAN_S3_ENDPOINT` |
| `test-notebook` | CI-runnable tutorial notebooks; excludes Modal notebooks |
| `test-notebook-modal` | Optional Modal tutorials; needs deployed endpoints and credentials, plus object storage for the R2 tutorial |
| `test-seq` | Sequential tests for debugging; requires local Docker, excludes notebooks and Modal, includes S3 |
| `test-modal` | Optional live Modal tests; needs cloud credentials and each test's prerequisites |
| `test-modal-endpoint` | Deploy a test endpoint and exercise R2 delivery; needs Modal/R2 credentials and a worker secret |
| `fmt` | Format and lint the codebase |
| `build-dist` | Build distribution packages |
| `check-dist` | Validate distribution packages |
| `upload-testpypi` | Publish built distributions to Test PyPI |
| `upload-pypi` | Publish built distributions to PyPI |

To use an already-running local MinIO service for S3 checks:

```bash
ARTISAN_S3_ENDPOINT=http://127.0.0.1:9000 pixi run -e dev test-s3
```

The tests create and remove isolated buckets. Set `ARTISAN_S3_ACCESS_KEY` and
`ARTISAN_S3_SECRET_KEY` if the service uses credentials other than `minioadmin`.
The `test` and `test-s3` tasks require S3 checks to run: unavailable storage is
an error. Without an explicit endpoint, tests start an isolated container from
the immutable MinIO image pinned in `tests/conftest.py`. This is a third-party
build of the upstream MinIO source from [coollabsio/minio](https://github.com/coollabsio/minio).
Ad hoc `pytest` runs can still skip when storage is unavailable; add
`--require-s3` when every selected S3 test must execute.

The `test`, `test-integration`, `test-docker`, and `test-seq` tasks require a
reachable local Linux Docker engine, including Docker Desktop on macOS. The
full `test` task checks Docker before starting unit tests. Start Docker or select
a working local context if setup reports an unavailable socket or access denied.
The selected context is preserved; `DOCKER_CONTEXT` takes precedence over
`DOCKER_HOST`.

Setup automatically builds a small image from the pinned Alpine digest and
reuses it on later runs. The first build needs registry access. An optional
`ARTISAN_DOCKER_CWD_TEST_IMAGE` override selects an existing local Linux image
with `sh` and a declared `WORKDIR`; an invalid override fails setup. Image build
or pull failures are errors, not skips. No manual image export is needed for the
default tasks.

Ad hoc `pytest` runs prepare Docker only when a Docker test executes and may
skip if the engine is unavailable. Add `--require-docker` to fail before
collection instead. `--collect-only` never prepares Docker. For saved release
evidence, follow the
[container qualification](../contributing/coding-conventions.md#qualify-docker-commands).

### Docs environment

| Task | Purpose |
|------|---------|
| `docs-build` | Build the documentation site |
| `docs-clean` | Remove built documentation |
| `docs-serve` | Serve built docs locally on port 8000 |

---

## Cross-references

- [Installation](installation.md) — First-time setup and IDE configuration
- [Tooling Decisions](../contributing/tooling-decisions.md) — Why Artisan uses Pixi
- [Pixi documentation](https://pixi.sh/latest/) — Full reference for all Pixi commands
