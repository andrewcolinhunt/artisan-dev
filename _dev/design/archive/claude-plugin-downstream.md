# Design: Claude Code Plugin — Downstream Repo Setup

**Date:** 2026-03-12  **Status:** Draft  **Author:** Claude + ach94

---

## Context

Artisan ships a Claude Code plugin with skills for writing operations,
pipelines, composites, and docs. The plugin lives at
`src/artisan/_claude_plugin/` and is distributed via the marketplace system
(see `claude-plugin-distribution.md`).

This document describes what **downstream repos** (projects that depend on
artisan) need to do so their contributors get artisan's skills automatically.

---

## Prerequisites

The artisan public repo (`dexterity-systems/artisan`) must have a
`.claude-plugin/marketplace.json` at its root that registers the plugin.
This is tracked in `claude-plugin-distribution.md`.

---

## Option A: Zero-Setup for Contributors (Recommended)

The repo maintainer commits a `.claude/settings.json` that points to artisan's
marketplace and enables the plugin. Contributors get prompted to install on
first use.

### What the maintainer adds

**`.claude/settings.json`:**

```json
{
  "extraKnownMarketplaces": {
    "artisan": {
      "source": { "source": "github", "repo": "dexterity-systems/artisan" }
    }
  },
  "enabledPlugins": {
    "artisan@artisan": true
  }
}
```

This file is committed to version control.

### Contributor experience

When a contributor opens the project in Claude Code and trusts the project
folder, Claude Code reads `.claude/settings.json`, sees the marketplace and
enabled plugin entries, and prompts the contributor to install. The contributor
accepts once and the plugin persists across sessions.

Skills are available as `/artisan:write-operation`, `/artisan:write-pipeline`,
`/artisan:write-composite`, `/artisan:write-docs`.

### If the repo already has a `.claude/settings.json`

Merge the `extraKnownMarketplaces` and `enabledPlugins` keys into the existing
file. They are additive and don't conflict with other settings.

---

## Option B: Individual User Setup

For contributors who work across many artisan-based repos, a one-time personal
setup avoids per-repo configuration.

```
/plugin marketplace add dexterity-systems/artisan
/plugin install artisan
```

This registers the marketplace and installs the plugin at the user level
(`~/.claude/settings.json`). Skills are then available in all projects.

---

## Option C: CLI Fallback

For users who cannot or prefer not to use the marketplace system:

```bash
claude --plugin-dir $(pixi run claude-plugin-path)
```

Or if artisan is installed via pip:

```bash
claude --plugin-dir $(python -c "import artisan; print(artisan.__path__[0] + '/_claude_plugin')")
```

This is per-session and must be passed on every `claude` invocation.

---

## Recommendation

Use **Option A** for any repo with multiple contributors. It requires a one-time
commit from the maintainer and zero setup from contributors.

Use **Option B** for individual developers who work across many artisan-based
repos and want the skills everywhere without per-repo config.

**Option C** is a fallback only.

---

## Available Skills

Once installed, the following skills are available:

| Skill | Invocation | Description |
|---|---|---|
| Write Operation | `/artisan:write-operation` | Scaffold or review an Artisan operation |
| Write Pipeline | `/artisan:write-pipeline` | Design and write a pipeline |
| Write Composite | `/artisan:write-composite` | Write a composite operation |
| Write Docs | `/artisan:write-docs` | Write documentation pages |

Skills are auto-invoked when Claude detects matching user intent (based on the
skill's `description` field), so explicit `/artisan:*` invocation is optional.

---

## Example: Minimal Downstream Repo

```
my-analysis-project/
├── .claude/
│   └── settings.json       # Marketplace + plugin config (see Option A)
├── CLAUDE.md                # Project conventions
├── pyproject.toml           # artisan as a dependency
└── src/
    └── ...
```
