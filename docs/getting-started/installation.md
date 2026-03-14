# Installation

This page covers installing Artisan and its dependencies, starting the Prefect
orchestration server, and configuring your editor.

---

## Prerequisites

- **Platform:** Linux (x86_64) or macOS (Apple Silicon)
- [Pixi](https://pixi.sh) package manager (installs Python and all other
  dependencies for you)

:::{tip}
**What is Pixi?** Pixi is a project-scoped environment manager. Like `venv` or
`conda`, it creates an isolated environment — but Pixi also handles Python
itself and all native dependencies (Graphviz, PostgreSQL, etc.). Each clone gets
its own environment. No global pollution, no activation conflicts between
projects.

If you're used to `venv` or `conda`: `pixi install` in a directory is like
creating and activating a virtualenv for that directory only. `pixi run <cmd>`
runs a command inside it.
:::

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

:::{tip}
If you see a thread-spawn panic on resource-constrained nodes (common on HPC
login nodes), limit Pixi's thread count:
```bash
RAYON_NUM_THREADS=4 pixi install
```
:::

Verify the installation:

```bash
pixi run python -c "import artisan; print('Installation OK')"
```

You should see `Installation OK` printed to the terminal.

---

## Start the Prefect server

Artisan uses [Prefect](https://www.prefect.io/) to orchestrate pipeline
execution. A Prefect server must be running before you run any pipeline.

```bash
pixi run prefect-start
```

This starts a local Prefect server (backed by PostgreSQL) in the background and
writes a discovery file so Artisan can locate it automatically. You can verify
the server is running by opening <http://localhost:4200> in your browser.

To stop the server when you're done:

```bash
pixi run prefect-stop
```

:::{tip}
**On HPC clusters:** Don't run the Prefect server on the head node. Use a
persistent interactive session (long-running CPU allocation with `screen` or
`tmux`). The server must stay running while pipelines execute.
:::

:::{note}
**Using Prefect Cloud:** You can use [Prefect Cloud](https://www.prefect.io/cloud)
instead of a local server. Set `PREFECT_API_URL` to your cloud workspace URL and
skip the `prefect-start` step. See the Prefect Cloud docs for setup.
:::

### Use an existing server

If you already have a Prefect server running elsewhere, point Artisan to it
instead of starting a local one:

```bash
export PREFECT_SUBMITIT_SERVER=http://<host>:<port>/api
```

When connecting, Artisan checks for a server URL in this order: explicit
`prefect_server` argument passed to `PipelineManager.create()`, the
`PREFECT_SUBMITIT_SERVER` environment variable, the `PREFECT_API_URL`
environment variable, and finally the local discovery file written by
`prefect-start`.

---

## Using Pixi day-to-day

```bash
pixi run <command>              # Run a single command in the environment
pixi run python script.py       # Run a Python script
pixi shell                      # Start an interactive shell (like `source .venv/bin/activate`)
```

All `pixi` commands are project-scoped — run them from the repo directory. If
you use `pixi shell`, you can run `python`, `pytest`, etc. directly without
the `pixi run` prefix.

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

---

## Claude Code

Artisan ships with a [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
plugin for AI-assisted development — scaffolding operations, building pipelines,
and writing docs. See [Using Claude Code](using-claude-code.md) for setup and
usage.

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

- [Quick Start](quick-start.md) — Run your first pipeline in 5 minutes
- [Your First Pipeline](first-pipeline.md) — Build a multi-step pipeline with
  data flow
- [Orientation](orientation.md) — The mental model behind the framework
