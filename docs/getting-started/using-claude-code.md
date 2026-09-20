# Using Claude Code

[Claude Code](https://docs.anthropic.com/en/docs/claude-code) is an AI coding
assistant that runs in your terminal. It reads your codebase, edits files, runs
commands, and writes code. Artisan provides shared project instructions and authoring skills for coding
agents working in this repository.

---

## Install Claude Code

Follow the [Claude Code installation guide](https://docs.anthropic.com/en/docs/claude-code)
for current platform requirements and installation commands. Confirm that the
`claude` command is available before continuing.

---

## Start Claude Code in the repo

```bash
cd artisan
claude
```

When Claude Code starts, `CLAUDE.md` imports the repository's shared
`AGENTS.md`. This teaches coding agents about Artisan's architecture, testing
commands, code style, and Git conventions without duplicating instructions.

---

## Artisan skills

The repo ships with framework-specific Agent Skills that teach compatible
coding agents how to write Artisan code that follows project conventions.

| Skill | Description |
|-------|-------------|
| `/artifact-write` | Scaffold or review a custom artifact model and its registration |
| `/operation-write` | Scaffold or review an `OperationDefinition` subclass |
| `/composite-write` | Scaffold or review a `CompositeDefinition` subclass |
| `/pipeline-write` | Scaffold a pipeline script composing operations |

### How skills are discovered

Skill definitions live in `skills/` at the repo root — this remains the
canonical source of truth and the public Claude plugin contract. The repo adds
discovery symlinks for both cross-agent clients and Claude Code:

```
.agents/skills/artifact-write   →  ../../skills/artifact-write
.agents/skills/operation-write  →  ../../skills/operation-write
.agents/skills/composite-write  →  ../../skills/composite-write
.agents/skills/pipeline-write   →  ../../skills/pipeline-write
.claude/skills/artifact-write   →  ../../skills/artifact-write
.claude/skills/operation-write  →  ../../skills/operation-write
.claude/skills/composite-write  →  ../../skills/composite-write
.claude/skills/pipeline-write   →  ../../skills/pipeline-write
```

### Downstream repos

Repos that depend on Artisan can inherit these skills via the Claude Code
marketplace. Artisan publishes a plugin through `.claude-plugin/marketplace.json`.
A downstream repo enables it by adding to `.claude/settings.json`:

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

Skills appear as `/artisan:operation-write`, etc. Claude Code prompts
contributors to trust the marketplace on first launch.

---

## What you can do

A few examples of real workflows:

- **"Create a custom artifact type for binary content"** — Claude writes the
  model, registration, and tests for persistence and hydration.
- **"Write me an operation that takes CSV files and computes column
  statistics"** — Claude scaffolds the class, tests, and docstrings following
  Artisan conventions.
- **"Build a pipeline that generates data, transforms it, and filters by
  median score"** — Claude writes a working pipeline script with correct output
  wiring.
- **"Explain how artifact lineage works in this codebase"** — Claude reads the
  source and explains the provenance system.
- **"Run the tests and fix any failures"** — Claude runs pytest, reads errors,
  and proposes fixes.

The shared instructions point agents to Pixi tasks, test structure, and
formatting rules. Review generated changes and check their behavior using the
same project tasks you would use for handwritten code.

---

## Tips for effective use

- **Be specific** about what you want ("write an operation that computes
  column statistics from CSV input" not "help me with operations").
- **Review generated code** before committing — Claude proposes, you decide.
- **Use skills for scaffolding**, then iterate: `/operation-write` gets
  you most of the way, then refine the details.
- **Let Claude run checks** — ask it to run the pre-PR checklist (`fmt`, `test`,
  `docs-build`).

---

## Personal instructions

`CLAUDE.md` imports the shared `AGENTS.md` rather than copying its commands and
conventions. Read `AGENTS.md` for the current project rules.

You can create a personal `CLAUDE.local.md` for your own preferences (editor
settings, branch conventions, etc.). It's auto-gitignored.

---

## Cross-references

- [Installation](installation.md) — Installing Artisan and configuring your environment
- [Orientation](orientation.md) — The mental model behind the framework
