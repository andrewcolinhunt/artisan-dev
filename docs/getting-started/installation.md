# Installation

Install Artisan from a checkout using the repository's Pixi environment.
The same environment supplies Python, Graphviz, and the notebook kernel.

---

## Prerequisites

- **Platform:** Linux (x86_64 or aarch64) or macOS (Apple Silicon)
- [Pixi](https://pixi.sh) **0.66.0**, which installs Python 3.12 and the other
  dependencies

---

## Before upgrading

Start **fresh Delta and staging roots for store format 8**. Earlier stores and
their staging cannot be migrated or read by this release; keep each older root
with its matching Artisan version to inspect or export its contents before
re-ingesting into a new store. Do not edit its manifest to force an upgrade or
attach old staging to the new Delta root. See
[the store format contract](../concepts/storage-and-delta-lake.md#the-store-format-is-explicit).

Update downstream operations, pipelines, runner providers, and inspection
clients using the [breaking changes in the changelog](../../CHANGELOG.md).
The [public API index](../reference/python-api.md) links to supported imports and
current examples; removed APIs have no compatibility aliases.

Upgrade Modal clients and worker code together and
[explicitly redeploy tool endpoints](../how-to-guides/deploying-tool-endpoints.md).
For SLURM, use a compatible `artisan-submitit` provider and qualify it separately
on your cluster. Core Artisan qualification does not qualify external runner
packages; see [step runner configuration](../how-to-guides/configuring-execution.md#choose-a-step-runner).

---

## Install Artisan

```bash
# Install Pixi (if not already installed)
curl -fsSL https://pixi.sh/install.sh | PIXI_VERSION=v0.66.0 bash
```

:::{tip}
Restart your terminal after installing Pixi so it appears on your `PATH`.
:::

```bash
# Clone the repository
git clone https://github.com/dexterity-systems/artisan.git
cd artisan

# Install the runtime dependencies
pixi install
pixi run setup
```

:::{note}
The first `pixi install` may take several minutes to download dependencies.
`setup` registers Graphviz's layout plugins in the selected environment and is
safe to repeat. Git hooks and Jupyter kernels have separate setup commands below.
:::

Verify the installation:

```bash
pixi run python -c "import artisan; print('Installation OK')"
pixi run python -c "from graphviz import Source; assert b'<svg' in Source('digraph { source -> result }').pipe(format='svg'); print('Graphviz OK')"
```

Both commands should print their `OK` message.
Use ordinary Pixi commands for local work. See
[reviewing lockfile changes](using-pixi.md#reviewing-lockfile-changes) when
editing the project's configuration.

:::{tip}
**Contributors:** prepare the dev environment and install Git hooks:

```bash
pixi install -e dev
pixi run -e dev setup
pixi run -e dev install-hooks
```

Run `setup` in each environment you use to render graphs. The hook suite
(ruff, mypy, codespell, blacken-docs, and supporting checkers) runs on every
commit; the same suite is enforced in CI. Customize via `.pre-commit-config.yaml`.
:::

:::{dropdown} What is Pixi?
Pixi manages environments, dependencies, and tasks. Each clone gets an isolated
environment containing Python and system tools such as Graphviz. The lockfile
records package versions for each supported platform. See
[Tooling Decisions](../contributing/tooling-decisions.md) for why Artisan uses it.
:::

---

## Optional cluster runners

Artisan's local process-pool runner requires no orchestration server. For SLURM,
install the separate `artisan-submitit` package and follow its cluster setup
instructions. It supplies job-array and intra-allocation runners.

---

## Using Pixi day-to-day

See [Using Pixi](using-pixi.md) for environments, tasks, shells,
and workspaces.

---

## IDE setup (VSCode)

### Python interpreter

Set the Pixi environment as your VSCode Python interpreter:

```bash
pixi run which python
# Example output: /home/user/artisan/.pixi/envs/default/bin/python
```

In VSCode: `Ctrl+Shift+P` (Linux) or `Cmd+Shift+P` (macOS) → "Python: Select
Interpreter" → paste the path above.

### Jupyter kernel

Register the Pixi environment as a Jupyter kernel:

```bash
pixi run install-kernel
```

In VSCode: open a `.ipynb` file → click "Select Kernel" → choose **Artisan**.

This registers a user kernel pointing to the environment that ran the task.
To use the dev environment, run `pixi run -e dev install-kernel`;
this replaces the same **Artisan** kernel registration with the dev interpreter.
Environment setup does not choose or overwrite your kernel.

### Kernel slowness (Pixi environments)

If a kernel starts slowly, first confirm that it uses the interpreter registered
above. For a repeated delay accompanied by `Failed to get activated env vars`,
check [the reported Pixi activation issue](https://github.com/microsoft/vscode-python/issues/25804).
That report identifies the `Python Environments` extension
(`ms-python.vscode-python-envs`) as the cause in affected versions. Temporarily
disable it to check whether the same issue explains your delay.

---

## Coding agents

See [Using Coding Agents](using-coding-agents.md) to load Artisan's project
instructions and skills for authoring code and inspecting runs.

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `pixi: command not found` | Pixi not on `PATH` | Restart your terminal, or add `~/.pixi/bin` to your `PATH` manually |
| Thread-spawn panic during `pixi install` | Too many threads on constrained node | `RAYON_NUM_THREADS=4 pixi install` |
| `pixi install` is very slow | First run downloads Python + all deps | Expected on first install — subsequent runs are fast |
| `dot` / Graphviz errors in provenance graphs | Graphviz layout plugins not registered | Run `pixi run setup` in the selected environment; use `-e dev` for dev |
| Jupyter kernel missing "Artisan" option | Kernel not registered | Run `pixi run install-kernel` and restart your notebook |

---

## Next steps

- [Your First Pipeline](../tutorials/01-getting-started/01-first-pipeline.ipynb) — Build and run a pipeline in an interactive notebook
- [Orientation](orientation.md) — How the framework's concepts fit together
