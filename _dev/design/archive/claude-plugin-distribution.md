# Design: Plugin Marketplace Distribution

**Date:** 2026-03-11  **Status:** In Progress  **Author:** Claude + ach94

---

## Summary

Make artisan's Claude Code skills installable via the official plugin
marketplace system so downstream repos can activate them with minimal setup.
Today, users must pass `--plugin-dir` with a Python path-resolution command
every time they launch Claude Code. The marketplace system replaces this with
a one-time install or zero-touch auto-configuration.

---

## Problem

Artisan ships four Claude Code skills (`write-operation`, `write-composite`,
`write-pipeline`, `write-docs`) inside `src/artisan/_claude_plugin/`. Before the
marketplace migration, the only activation method was:

```bash
claude --plugin-dir $(python -c "import artisan; print(artisan.__path__[0])")/_claude_plugin
```

**Issues with this approach:**

- Must be passed on every `claude` invocation — not persistent
- Requires the user to know the incantation
- Breaks if the wrong Python environment is active
- Not discoverable — users must read the docs to learn it exists

---

## Proposed Solution: Marketplace

Claude Code's plugin marketplace system is the official distribution mechanism.
A marketplace is a git repo (or subdirectory of one) containing a
`.claude-plugin/marketplace.json` that lists available plugins and their
sources.

### Repo structure

The marketplace requires one file at the repo root alongside the existing plugin:

```
artisan/
├── .claude-plugin/
│   └── marketplace.json    ← marketplace catalog
└── src/artisan/_claude_plugin/
    ├── .claude-plugin/
    │   └── plugin.json     ← plugin manifest
    └── skills/             ← skill definitions
```

**`.claude-plugin/marketplace.json`:**

```json
{
  "name": "artisan",
  "owner": {
    "name": "Andrew Colin Hunt"
  },
  "plugins": [
    {
      "name": "artisan",
      "source": "./src/artisan/_claude_plugin",
      "description": "Skills for building pipelines and operations with the Artisan framework"
    }
  ]
}
```

This follows the official marketplace schema: top-level `name` and `owner`
fields, with `plugins` as an array of objects each containing `name`, `source`,
and `description`. The relative path source means the plugin is resolved from
the same repo. No external hosting, no duplication.

### Nothing else changes

The existing `plugin.json` and `skills/` directory remain as-is. The
`--plugin-dir` approach continues to work as a fallback. If a user has both
the marketplace plugin and `--plugin-dir` active, the skills are loaded once
— Claude Code deduplicates by plugin name.

---

## User Experience

### Individual user (one-time setup)

```
/plugin marketplace add dexterity-systems/artisan
/plugin install artisan
```

Skills are then available as `/artisan:write-operation`,
`/artisan:write-pipeline`, etc. in all projects, persistent across sessions.

### Downstream repo (zero setup for contributors)

A repo maintainer adds to `.claude/settings.json`:

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

The `enabledPlugins` format is `plugin_name@marketplace_name`. When a
contributor opens the project in Claude Code and trusts the project folder,
Claude Code reads `.claude/settings.json`, sees the marketplace and enabled
plugin entries, and prompts the contributor to install them. The contributor
accepts once and the plugin persists across sessions.

Note: the official docs show two `extraKnownMarketplaces` source formats — flat
(`"source": "github"`) and nested (`"source": { "source": "github" }`). The
nested form is used here as it appears in the settings reference docs.

### Fallback: pixi task

For users who cannot or prefer not to use the marketplace system, a pixi task
provides the `--plugin-dir` path:

```bash
pixi run claude-plugin-path
# prints: /path/to/site-packages/artisan/_claude_plugin
```

Usage:

```bash
claude --plugin-dir $(pixi run claude-plugin-path)
```

---

## Alternative Sources Considered

The marketplace supports multiple source types for plugins:

| Source type | How it works | Verdict |
|---|---|---|
| **Relative path** | Plugin in same repo as marketplace | **Chosen** — simplest, no duplication, version matches repo |
| `pip` | `{ "source": "pip", "package": "artisan" }` | Tracks installed pip version, but adds pip resolution complexity |
| `github` | `{ "source": "github", "repo": "owner/repo" }` | Redundant when marketplace is already in the repo |
| `git-subdir` | Sparse clone of a subdirectory | Useful for monorepos, unnecessary here |
| `npm` | npm package source | Not applicable for a Python project |

The relative path source was chosen because the marketplace file lives in the
same repo as the plugin. The plugin version always matches the repo checkout,
and there is no external resolution step.

---

## Single Source of Truth

`src/artisan/_claude_plugin/skills/` is the only location for skills. The duplicate
`.claude/skills/` directory is removed.

Plugin skills are namespaced as `/artisan:write-operation`,
`/artisan:write-pipeline`, etc. — both in the artisan dev repo and in
downstream repos. This is consistent and eliminates the sync problem entirely.

| Context | Invocation |
|---|---|
| Working in artisan repo | `/artisan:write-operation` |
| Working in downstream repo | `/artisan:write-operation` |

---

## Implementation

**Already complete:**

- Rename `src/artisan/_plugin/` to `src/artisan/_claude_plugin/`
- Remove `.claude/skills/` directory (the duplicated skills)
- Add `.claude-plugin/marketplace.json` to the public artisan repo root
- Add `claude-plugin-path` pixi task to `pyproject.toml`
- Update README.md "Claude Code Integration" section
- Update `docs/getting-started/installation.md` "Claude Code plugin" section
- Update `docs/contributing/tooling-decisions.md` "AI assistance" section

**Remaining:**

- Verify with `claude --plugin-dir` that the relative path resolves correctly
- Test `/plugin marketplace add` and `/plugin install` against the public repo

---

## Decisions

- **No official marketplace submission** for now. Can revisit later.
- **No auto-update** for now. Users update manually when ready.