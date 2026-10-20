# Inspect Evidence Locally

Discover the inspection contracts in your installed Artisan version and query
them from Python or the CLI. These interfaces work with the base installation;
an MCP server is optional.

## Find the installed contract

Start in the environment that can access the intended store:

```bash
artisan inspect list --json
artisan inspect describe list_runs --json
artisan guide list --json
artisan guide show pipeline-operate
```

The catalog identifies the installed version and capabilities. Each description
contains complete request and response JSON schemas. The operating guide adds
the workflow: select a store and attempt, inspect execution evidence, then assess
outcomes against explicit criteria. Its version, origin and content digest let
you check which guidance you read. Availability comes from the installed catalog,
not a version number alone.

## Query a store

Supply an existing Delta root, such as the one created by the
[first pipeline tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb):

```bash
artisan inspect run list_runs --delta-root /absolute/path/to/runs/delta --json
printf '%s\n' '{"limit":1}' | artisan inspect run list_runs \
  --request - --delta-root /absolute/path/to/runs/delta --json
```

`--request` accepts a UTF-8 JSON object file or `-` for stdin. Omitting it supplies
an empty object. Invalid JSON, duplicate keys, non-finite numbers, undeclared
fields and incorrect field types fail before querying the store.

Use repeated `--load-module` flags for trusted installed applications whose
operations or artifact types you need. Otherwise `ARTISAN_LOAD_MODULES` supplies
the module list. `--delta-root` takes precedence over `ARTISAN_DELTA_ROOT`.
Application imports can run code; use only modules you trust. Discovery queries
expose import failures, while store queries stop when required application
registration fails.

Local execution can read a configured remote store. Keep the intended store and
credentials consistent when moving between interfaces. If a task specifies an
existing MCP target, use that target rather than guessing an equivalent local
path. See [connect an MCP client](connecting-mcp.md) for that interface.

## Use the same contract from Python

Python callers initialize trusted application definitions explicitly. Replace
the store path before running this example:

```python
from artisan.inspection import describe_inspection, inspect_evidence, list_inspections
from artisan.registry import bootstrap_applications

print(list_inspections().model_dump(mode="json"))
print(describe_inspection("list_runs").model_dump(mode="json"))
bootstrap_applications(["artisan.operations.examples"], purpose="inspect results")
root = "/absolute/path/to/runs/delta"
page = inspect_evidence("list_runs", {"limit": 1}, delta_root=root)
runs = []
while True:
    payload = page.model_dump(mode="json")
    runs.extend(payload["items"])
    follow_up = payload["follow_up"]
    if follow_up is None:
        break
    page = inspect_evidence(
        follow_up["capability"], follow_up["arguments"], delta_root=root
    )
print([run["pipeline_run_id"] for run in runs])
```

Use `artisan.registry.discover` and pass its report as `discovery` when querying
the `capabilities` inspection. `list_inspections` and `describe_inspection` describe
the installed interface without discovering application operations.

## Continue and interpret evidence

Projection version `2` uses transport-neutral `follow_up.capability` and exact
`follow_up.arguments`. CLI, Python and MCP use the same responses and cursor
rules. To continue through MCP, prefix the capability with `artisan_` to obtain
its tool name. Keep the same store representation, selectors and observed
evidence; an equivalent path spelled differently can invalidate a cursor.

Responses are bounded to 48 KiB of canonical JSON and collections to at most 100
rows per page. Selected values remain whole. Inspect collection coverage,
omitted fields and `has_more`; a summary is not a complete evidence export.
These bounds limit response size, not reader memory or query cost. MCP also has
a separate wire-size limit that can reject a response whose duplicated protocol
encoding is too large.

Known query errors are typed Python exceptions or structured CLI/MCP errors.
The CLI writes one JSON object to stdout with `--json`, including on handled
failure (exit 1); argument parsing errors exit 2. Application diagnostics go to
stderr. Unexpected implementation failures remain visible as exceptions.
A successfully returned failure report is evidence about a failed execution,
not a failed inspection request.

On `evidence_view_changed`, discard accumulated pages and restart the query.
On `invalid_evidence_cursor`, restart without the token. Older projection-version
tokens are not supported. Paging over a live store is not an atomic snapshot.
To retain a complete assessment or comparison, [export the evidence](exporting-evidence.md).
