# Installation

This page covers installing Artisan and its dependencies, starting the Prefect
orchestration server, and configuring your editor.

---

## Prerequisites

- **Platform:** Linux (x86_64) or macOS (Apple Silicon)
- [Pixi](https://pixi.sh) package manager (installs Python and all other
  dependencies for you)

---

## Install Artisan

```bash
# Install Pixi (if not already installed)
curl -fsSL https://pixi.sh/install.sh | sh
```

:::{tip}
Restart your terminal after installing Pixi so it appears on your `PATH`.
:::

```bash
# Clone the repository
git clone https://github.com/dexterity-systems/artisan.git
cd artisan

# Install all dependencies (Python 3.12, scientific stack, Prefect, etc.)
pixi install
```

:::{note}
The first `pixi install` downloads Python and all dependencies, which may take
several minutes. Subsequent runs are fast.
:::

Verify the installation:

```bash
pixi run python -c "import artisan; print('Installation OK')"
```

You should see `Installation OK` printed to the terminal.

:::{dropdown} What is Pixi?
Pixi is a project-scoped environment and task manager. Like `venv` or `conda`,
it creates an isolated environment — but Pixi also handles Python itself and
all non-Python dependencies (PostgreSQL, Graphviz, etc.) from a single lockfile.
Each clone gets its own environment.

| Tool | Manages Python? | Manages system deps? | Project-scoped? |
|------|:-:|:-:|:-:|
| venv + pip | No | No | Yes |
| conda | Yes | Yes | No (shared envs) |
| uv | Yes | No | Yes |
| **Pixi** | **Yes** | **Yes** | **Yes** |

**Why Pixi for this project?** Artisan needs PostgreSQL and Graphviz
alongside Python (plus Node.js for documentation builds). Pixi resolves all of them from conda-forge and PyPI in one
lockfile (`pixi.lock`), so every contributor gets an identical environment
regardless of platform. See [Tooling Decisions](../contributing/tooling-decisions.md)
for the full rationale.
:::

---

## Start the Prefect server

```bash
pixi run prefect-start
```

This starts a local Prefect server (backed by PostgreSQL) in the background and
writes a discovery file so Artisan can locate it automatically. The server binds
to a UID-based port to avoid collisions when multiple users share a machine.
Check the terminal output for the URL, then open it in your browser to verify.

To stop the server when you're done:

```bash
pixi run prefect-stop
```

:::{tip}
**On HPC clusters:** Don't run the Prefect server on the head node. Use a
persistent interactive session (long-running CPU allocation). The server must
stay running and accessible while pipelines execute.
:::

:::{note}
**Prefect Cloud** is also supported as an alternative, though the self-hosted
server is recommended for most use cases — especially on HPC clusters. See
[Connect to Prefect](../how-to-guides/connect-to-prefect.md) for details on
both options.
:::

:::{dropdown} What is Prefect?
Artisan uses [Prefect](https://www.prefect.io/) as a dispatch layer for
parallel task execution. Prefect is **not** a workflow engine here — Artisan
owns pipeline definition, step sequencing, caching, and provenance. Prefect
dispatches work to local processes or SLURM nodes and provides an optional
monitoring UI.

The server is required for all execution modes — both local and SLURM. Even
local execution uses Prefect to dispatch tasks to a process pool. See
[Tooling Decisions](../contributing/tooling-decisions.md) for the full
rationale.
:::

### Use an existing server

If you already have a Prefect server running elsewhere, point Artisan to it
instead of starting a local one:

```bash
export PREFECT_SUBMITIT_SERVER=http://<host>:<port>/api
```

When connecting, Artisan checks for a server URL in this order:

| Priority | Source | How to set |
|----------|--------|-----------|
| Highest | Explicit `prefect_server` argument | `PipelineManager.create(prefect_server="...")` |
| | `PREFECT_SUBMITIT_SERVER` env var | `export PREFECT_SUBMITIT_SERVER=http://...` |
| | `PREFECT_API_URL` env var | `export PREFECT_API_URL=http://...` |
| | Discovery file | Written by `pixi run prefect-start` |
| Lowest | Prefect profile | `~/.prefect/profiles.toml` (set via `pixi run prefect cloud login`) |

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

### Kernel slowness (Pixi environments)

If your pixi Jupyter kernel takes 30+ seconds to start in VS Code, the
`Python Environments` extension (`ms-python.vscode-python-envs`) is likely the
cause. It doesn't recognize pixi as a known environment type and spends 30
seconds trying to activate it before timing out.

**Fix:** Uninstall the `Python Environments` extension (`ms-python.vscode-python-envs`)
in VS Code. The core Python extension works fine without it.

Tracked upstream: [microsoft/vscode-python#25804](https://github.com/microsoft/vscode-python/issues/25804)

---

## Claude Code

Artisan ships with a [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
configuration for AI-assisted development — scaffolding operations, building
pipelines, and writing docs. See [Using Claude Code](using-claude-code.md) for
setup and usage.

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `pixi: command not found` | Pixi not on `PATH` | Restart your terminal, or add `~/.pixi/bin` to your `PATH` manually |
| Thread-spawn panic during `pixi install` | Too many threads on constrained node | `RAYON_NUM_THREADS=4 pixi install` |
| `pixi install` is very slow | First run downloads Python + all deps | Expected on first install — subsequent runs are fast |
| `PrefectServerNotFound` when running a pipeline | No Prefect server detected | Run `pixi run prefect-start`, or set `PREFECT_SUBMITIT_SERVER` |
| `PrefectServerUnreachable` | Server URL found but server is not responding | Check that the server process is running (`pixi run prefect-start`) |
| `dot` / Graphviz errors in provenance graphs | Graphviz layout plugins not registered | Run `pixi run dot -c` to register plugins (normally handled automatically) |
| Jupyter kernel missing "Artisan" option | Kernel not registered | Run `pixi run install-kernel` and restart your notebook |

---

## Next steps

- [Your First Pipeline](../tutorials/getting-started/01-first-pipeline.ipynb) — Build and run a pipeline in an interactive notebook
- [Orientation](orientation.md) — The mental model behind the framework
