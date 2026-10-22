# Using Coding Agents

Use your coding agent with Artisan's shared project instructions and skills to
write pipelines and inspect runs. Local inspection uses the project's Python
environment and needs no MCP server.

---

## Open the project in your coding agent

Set up Artisan using the [installation guide](installation.md), then open the
project directory in your coding agent. Use an agent that can read project files
and run commands in the project's environment.

Ask the agent to read `AGENTS.md` before making changes. It contains the shared
architecture, testing commands, code style, and Git conventions. Agents that
discover this file automatically can load it directly; otherwise, include its
path or contents in your request. The repository's `CLAUDE.md` and `GEMINI.md`
import the same instructions for their respective tools.

For example:

> Read AGENTS.md and inspect the existing operations. Use the pipeline-write
> skill to build a pipeline that generates data, transforms it, and filters by
> median score. Run it and verify the results.

---

## Artisan skills

| Skill | Description |
|-------|-------------|
| `artifact-write` | Scaffold or review a custom artifact model and its registration |
| `operation-write` | Scaffold or review an `OperationDefinition` subclass |
| `composite-write` | Scaffold or review a `CompositeDefinition` subclass |
| `pipeline-write` | Scaffold a pipeline script composing operations |
| `pipeline-operate` | Inspect runs, diagnose failures, trace outputs, and assess or compare available evidence |

Ask for a skill by name in your request. Some agents also provide a skill picker
or command syntax; use the mechanism supported by your agent.

### How skills are discovered

The canonical skill packages live in `skills/` at the repository root. Each has
a `SKILL.md` entry point and may include supporting files, such as `references/`.
The `.agents/skills/` directory contains symlinks to these packages for agents
that support that discovery location. The `.claude/skills/` directory provides
the same links for Claude Code.

If your agent does not discover these directories, ask it to read the relevant
entry point, such as `skills/operation-write/SKILL.md`, and follow its referenced
files. Native skill installation is optional when the agent can read the files
directly.

### Downstream repos

In a project that depends on Artisan, install the desired skill packages from
`skills/` using your agent's documented skill installation mechanism. Copy each
complete folder, including `references/` when present. You can also point the
agent to the skills in an Artisan checkout. Follow each skill's instructions for
locating the relevant source and documentation in a downstream project.

Installing Artisan or its Python MCP extra does not install skills in your
agent. Claude Code users can also use the
[optional plugin setup](#optional-claude-code-plugin).

### Inspect existing runs

Ask the agent to use `pipeline-operate` with a specific run or question, such as
"Explain why this run failed" or "Compare these runs using this metric and case
key." Supply the store or remote connection so the agent can select the intended
evidence.

For local work, run these commands through your project's environment:

```bash
pixi run --locked artisan guide list --json
pixi run --locked artisan guide show pipeline-operate
pixi run --locked artisan inspect list --json
pixi run --locked artisan inspect describe assess_outcomes --json
```

The guide reports the package version, text source, and digest. In an editable
checkout, guide edits can change the digest without changing the package
version. Check the installed request and response schemas for supported calls
and arguments.

Read another installed topic when the task needs it:

```bash
pixi run --locked artisan guide show pipeline-operate --topic assessment-and-comparison
pixi run --locked artisan guide show pipeline-operate --topic evidence-exports
```

Add `--json` to receive the guide text and its metadata as one JSON object.
These guide and schema commands need no store access. See
[Inspecting Evidence](../how-to-guides/inspecting-evidence.md) to query a store,
or [Exporting Evidence](../how-to-guides/exporting-evidence.md) to save a
complete assessment or comparison.

If an older package lacks guide commands, the skill's portable references
provide a fallback. Check their proposed calls against that installation's
help or public API. A present command that cannot load its resources indicates
an installation problem, not an older interface.

For a store available only through a remote connection,
[connect an MCP client](../how-to-guides/connecting-mcp.md) and use the remote
server's advertised schemas. Local guidance describes the local installation;
it does not establish the remote server's capabilities or filesystem access.
An explicit remote selection stays the target even if a local run has the same
ID.

The skill distinguishes accepted outputs from a passed quality criterion and
reports missing evidence. Inspection alone does not launch, cancel or monitor
jobs.

---

## What you can do

Ask the agent to inspect available operations, composites, and tests before
writing new code. Reuse operations that fit the analysis; add or change an
integration when needed. See
[operation discovery](../how-to-guides/inspecting-evidence.md#find-the-installed-contract)
for inspecting registered operations.

Example requests:

- "Create a custom artifact type for binary content, with tests for persistence
  and hydration."
- "Write an operation that takes CSV files and computes column statistics."
- "Explain how artifact lineage works in this codebase."
- "Why did run `run-id` fail, and which outputs were accepted?"

---

## Tips for effective use

- State the expected inputs and outputs in your request.
- Use skills to scaffold code, then check it against your requirements and
  existing project patterns.
- Ask the agent to run the checks in `AGENTS.md` and report which passed or could
  not run. Review the code and results before committing.

---

## Personal instructions

Keep shared conventions in `AGENTS.md` and personal preferences in your agent's
local settings. Keep personal files out of version control; add an ignore rule
when needed.

For Claude Code, `CLAUDE.local.md` is already gitignored in this repository.

---

(optional-claude-code-plugin)=
## Optional Claude Code plugin

For downstream projects using Claude Code, Artisan also publishes a plugin
through `.claude-plugin/marketplace.json`. Add these entries to the downstream
project's `.claude/settings.json`, preserving any existing settings:

```json
{
  "extraKnownMarketplaces": {
    "artisan": {
      "source": {
        "source": "github",
        "repo": "dexterity-systems/artisan"
      }
    }
  },
  "enabledPlugins": {
    "artisan@artisan": true
  }
}
```

The plugin exposes the same skills with namespaced commands such as
`/artisan:operation-write` and `/artisan:pipeline-operate`.

---

## Cross-references

- [Installation](installation.md) — Installing Artisan and configuring your environment
- [Orientation](orientation.md) — How the framework's concepts fit together
