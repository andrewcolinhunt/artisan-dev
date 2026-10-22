# Installation

Install Artisan from a checkout using the repository's Pixi environment.
The same environment supplies Python, Graphviz, and the notebook kernel.

---

## Prerequisites

- **Platform:** Linux (x86_64 or aarch64) or macOS (Apple Silicon)
- [Pixi](https://pixi.sh) **0.66.0** (installs Python 3.12 and all other
  dependencies for you)

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
The first `pixi install` downloads Python and all dependencies, which
may take several minutes. `setup` registers Graphviz's layout plugins in the
selected environment and is safe to repeat. It does not install Git hooks or
register a Jupyter kernel.
:::

Verify the installation:

```bash
pixi run python -c "import artisan; print('Installation OK')"
pixi run python -c "from graphviz import Source; assert b'<svg' in Source('digraph { source -> result }').pipe(format='svg'); print('Graphviz OK')"
```

You should see `Installation OK` and `Graphviz OK` printed to the terminal.
Use ordinary Pixi commands for local work. See
[reviewing lockfile changes](using-pixi.md#reviewing-lockfile-changes) when
editing the project's configuration.

:::{tip}
**Contributors:** prepare the dev environment, then install Git hooks explicitly:

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
Pixi is a project-scoped environment and task manager. Like `venv` or `conda`,
it creates an isolated environment — but Pixi also handles Python itself and
non-Python dependencies such as Graphviz from a single lockfile.
Each clone gets its own environment.

**Why Pixi for this project?** Artisan needs Graphviz alongside Python (plus
Node.js for documentation builds). Pixi resolves them from conda-forge and PyPI in one
lockfile (`pixi.lock`), with a recorded resolution for each supported platform.
See [Tooling Decisions](../contributing/tooling-decisions.md)
for the full rationale.
:::

---

## Optional cluster runners

Artisan includes a native local process-pool runner and requires no orchestration
server. Install a runner provider only when the target infrastructure needs it.
For example, the separate `artisan-submitit` package supplies SLURM job-array
and intra-allocation runners while reusing the same Artisan execution contract.
Provider installation and cluster setup live with that package so a local
Artisan installation does not carry scheduler dependencies.

---

## Using Pixi day-to-day

See [Using Pixi](using-pixi.md) for a full guide to environments, tasks, shells,
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

Register the Pixi environment as a Jupyter kernel so notebooks use the correct
packages:

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

Artisan provides shared project instructions in `AGENTS.md` and portable skills
for scaffolding artifacts and operations, building pipelines, and inspecting
results. Agents that support `.agents/skills/` can discover the bundled skills
from a checkout. See [Using Coding Agents](using-coding-agents.md) for setup and
usage with your agent.

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
- [Orientation](orientation.md) — The mental model behind the framework
