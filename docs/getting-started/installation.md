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

## Claude Code plugin

Artisan includes a [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
plugin with framework-aware skills for scaffolding operations, pipelines, and
documentation. The plugin is defined at the repo root and distributed via the
Claude Code marketplace.

**Marketplace install** (recommended) — run these commands inside Claude Code:

```bash
/plugin marketplace add        # register the plugin from this repo
/plugin install                # install registered plugins
```

**Manual fallback** — point Claude Code at the repo root:

```bash
claude --plugin-dir /path/to/artisan-repo
```

| Skill | Description |
|-------|-------------|
| `/artisan:write-operation` | Scaffold or review an `OperationDefinition` subclass |
| `/artisan:write-composite` | Scaffold or review a `CompositeDefinition` subclass |
| `/artisan:write-pipeline` | Scaffold a pipeline script composing operations |
| `/artisan:write-docs` | Write or edit documentation pages, tutorials, and guides |

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `pixi: command not found` | Pixi not on `PATH` | Restart your terminal, or add `~/.pixi/bin` to your `PATH` manually |
| `PrefectServerNotFound` when running a pipeline | No Prefect server detected | Run `pixi run prefect-start`, or set `PREFECT_SUBMITIT_SERVER` |
| `PrefectServerUnreachable` | Server URL found but server is not responding | Check that the server process is running (`pixi run prefect-start`) |
| `dot` / Graphviz errors in provenance graphs | Graphviz layout plugins not registered | Run `pixi run dot -c` to register plugins (normally handled automatically) |
| Jupyter kernel missing "Artisan" option | Kernel not registered | Run `pixi run install-kernel` and restart your notebook |

---

## Next steps

- [Your First Pipeline](first-pipeline.md) — Build and run a complete pipeline
  in ~15 minutes
- [Orientation](orientation.md) — A quick map of the key abstractions before
  diving deeper
