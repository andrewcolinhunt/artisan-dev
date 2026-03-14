# Using Claude Code

[Claude Code](https://docs.anthropic.com/en/docs/claude-code) is an AI coding
assistant that runs in your terminal. It reads your codebase, edits files, runs
commands, and writes code. Artisan is set up so Claude Code understands the
framework's conventions, architecture, and APIs out of the box.

---

## Install Claude Code

```bash
npm install -g @anthropic-ai/claude-code
```

Verify the installation:

```bash
claude --version
```

See the [Claude Code docs](https://docs.anthropic.com/en/docs/claude-code) for
detailed installation instructions and requirements.

---

## Start Claude Code in the repo

```bash
cd artisan
claude
```

When Claude Code starts, it reads `CLAUDE.md` at the repo root. This file
teaches it about Artisan's architecture, testing commands, code style, and Git
conventions. You don't need to explain the project from scratch each time — the
context is built in.

---

## Artisan skills

The repo ships with framework-specific skills (slash commands) that Claude Code
auto-discovers from the `.claude-plugin/` directory.

If skills aren't available automatically, install them manually. Run these
commands inside the Claude Code interactive session (not in a regular terminal):

```
/plugin marketplace add
/plugin install
```

| Skill | Description |
|-------|-------------|
| `/artisan:write-operation` | Scaffold or review an `OperationDefinition` subclass |
| `/artisan:write-composite` | Scaffold or review a `CompositeDefinition` subclass |
| `/artisan:write-pipeline` | Scaffold a pipeline script composing operations |
| `/artisan:write-docs` | Write or edit documentation pages, tutorials, and guides |

---

## What you can do

A few examples of real workflows:

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

Claude Code knows the `pixi run` commands, the test structure, and the
formatting rules. It follows the same conventions a human contributor would.

---

## Tips for effective use

- **Be specific** about what you want ("write an operation that computes
  column statistics from CSV input" not "help me with operations").
- **Review generated code** before committing — Claude proposes, you decide.
- **Use skills for scaffolding**, then iterate: `/artisan:write-operation` gets
  you most of the way, then refine the details.
- **Let Claude run checks** — ask it to run the pre-PR checklist (`fmt`, `test`,
  `docs-build`).

---

## What `CLAUDE.md` contains

`CLAUDE.md` is a project-level instruction file that Claude Code reads
automatically. It contains environment setup, code style, testing commands, Git
conventions, and an architecture overview. You can read it yourself at
`CLAUDE.md` in the repo root — it's the same instructions a new contributor
would follow.

You can create a personal `CLAUDE.local.md` for your own preferences (editor
settings, branch conventions, etc.). It's auto-gitignored.
