# Connect an MCP Client

Connect a client to Artisan's read-only Model Context Protocol (MCP) server to
discover operations and inspect persisted pipeline results and failure logs.

## Prerequisites

- Python 3.12 or later and an MCP client that supports stdio servers.
- An Artisan store created with the installed release. Follow the
  [first pipeline tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb)
  to create one.
- Familiarity with [provenance](../concepts/provenance-system.md) and the
  [artifact and run terminology](../reference/glossary.md).

## Minimal working configuration

After installing the server, add this stdio server entry to your client,
replacing both absolute paths:

```json
{
  "command": "/absolute/path/to/.venv/bin/artisan-mcp",
  "args": [
    "--transport", "stdio",
    "--delta-root", "/absolute/path/to/runs/delta",
    "--load", "artisan.operations.examples"
  ]
}
```

Your client's surrounding configuration syntax may differ. The executable,
argument list, and environment settings are the portable parts of this entry.
The client starts the process and communicates through its stdin and stdout.

---

## Step 1: Install the server

Install Artisan with its optional MCP dependencies:

```bash
python -m venv .venv
.venv/bin/python -m pip install 'dexterity-artisan[mcp]'
.venv/bin/artisan-mcp --help
```

Use the absolute path to this environment's `artisan-mcp` executable in your
client. The client does not need an activated shell, shell expansion of `~`,
or a working directory inside the repository.

## Step 2: Select the store and operation modules

Set `--delta-root` to the Delta directory itself, such as
`/absolute/path/to/runs/delta`, rather than its parent `runs` directory.
Check the resolved configuration without starting a session:

```bash
/absolute/path/to/.venv/bin/artisan-mcp \
  --delta-root /absolute/path/to/runs/delta \
  --load artisan.operations.examples \
  --print-config
```

This prints JSON and exits. Curator builtins load automatically. Use repeated
`--load` options for additional trusted, installed operation modules; this
option imports modules and does not install packages.

You can provide the same settings through the server entry's environment:

```json
{
  "command": "/absolute/path/to/.venv/bin/artisan-mcp",
  "args": ["--transport", "stdio"],
  "env": {
    "ARTISAN_DELTA_ROOT": "/absolute/path/to/runs/delta",
    "ARTISAN_LOAD_MODULES": "artisan.operations.examples"
  }
}
```

`ARTISAN_LOAD_MODULES` accepts a comma-separated module list. Explicit
`--delta-root` and `--load` flags replace their corresponding environment
settings, so choose one source for each setting when configuring your client.

---

## Verify

Start the server through your client and inspect it in this order:

| Tool | Arguments or selection | Expected result |
|---|---|---|
| `artisan_capabilities` | None | `read_only: true`, no discovery errors or name collisions. The configured root is deliberately withheld. |
| `artisan_list_operations` | `query: "data_transformer"` | The example transformer appears when its module was loaded. |
| `artisan_describe_operation` | `name: "data_transformer"` | Input/output contracts and the parameter schema, including `scale_factor`. |
| `artisan_list_runs` | None | Run IDs from your selected store. Choose one for subsequent calls. |
| `artisan_get_run_status` | `pipeline_run_id` | The chosen run's current step statuses. |
| `artisan_get_step_result` | `pipeline_run_id`, `step_name` | That step's artifact references, grouped by type. |
| `artisan_diagnose_run` | `pipeline_run_id` | Failed execution IDs, error details, and log references when the run failed. |
| `artisan_get_step_logs` | `pipeline_run_id`, `step_name`, `tail_lines: 3` | A bounded tail of the selected step's failure logs. |

The server exposes ten read-only tools; the workflow above uses the tools
needed for initial inspection. Artifact queries and provenance graphs provide
further detail without modifying the store.

For a failed example, follow the
[error visibility tutorial](../tutorials/05-errors-and-control/02-error-visibility.ipynb).
Failure-log reading uses the local standard layout beneath
`<runs_dir>/logs/failures`, where `runs_dir` is the parent of the configured
Delta root. It serves failure logs only. Successful steps return empty lines,
and unavailable local failure logs can also yield an empty tail.

Log requests accept 1–1,000 lines and read at most 100 files and 256 KiB in
aggregate. The response reports `truncated` when content was omitted. Requesting
more than 1,000 lines returns a validation envelope identifying `tail_lines`.

## Troubleshooting

| Symptom | Check |
|---|---|
| The executable is missing or reports missing MCP dependencies | Install `'dexterity-artisan[mcp]'` into the environment named in `command`. |
| The server waits when launched manually | Stdio waits for protocol messages from a client. Use `--print-config` for a check that exits. |
| Store calls return `delta_root_unset` | Supply `--delta-root` or `ARTISAN_DELTA_ROOT`. Catalog discovery remains available without a root. |
| An operation is absent | Inspect the capabilities discovery report, install its package into the server environment, and add its trusted module with `--load`. |
| Failure logs are empty | Confirm the step failed and its local log files remain under the standard runs directory. Successful-step output is not served by this tool. |

## Cross-references

- [Inspecting Provenance](inspecting-provenance.md) — Inspect results through Python.
- [Provenance System](../concepts/provenance-system.md) — How runs, executions, and artifacts relate.
- [Glossary](../reference/glossary.md) — Framework terminology.
