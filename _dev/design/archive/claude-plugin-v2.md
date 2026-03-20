# Design: Claude Code Plugin — Structure & Distribution (v2)

**Date:** 2026-03-13  **Status:** Draft  **Author:** Claude + ach94

**Supersedes:** `claude-plugin-distribution.md`, `claude-plugin-downstream.md`

---

## Summary

Move the artisan Claude Code plugin from `src/artisan/_claude_plugin/` to the
repo root and consolidate the marketplace + plugin manifests into a single
`.claude-plugin/` directory. This simplifies the layout, removes the legacy
symlink-era nesting, and aligns with how the plugin spec actually works.

---

## Background

### How Claude Code plugins work

Claude Code plugins are **git-repo-based**, not package-based. Key concepts:

- A **plugin** is defined either by a `.claude-plugin/plugin.json` manifest
  or inline within a marketplace entry (using `strict: false` and explicit
  `skills` paths).
- A **marketplace** is a git repo with a `.claude-plugin/marketplace.json`
  that catalogs available plugins and their locations.
- `skills/`, `commands/`, etc. **must not** go inside `.claude-plugin/` —
  only manifest files belong there. Skill directories live at the plugin root.
- Anthropic's own `anthropics/skills` repo uses the inline pattern: no
  `plugin.json`, skills defined directly in marketplace entries with
  `"source": "./"` and `"strict": false`.

### Why move out of `src/`

The plugin was originally placed at `src/artisan/_claude_plugin/` so it could
be symlinked into `.claude/skills/` and also shipped inside the pip-installable
package (for `--plugin-dir` usage). Now that the marketplace is the primary
distribution mechanism:

- The marketplace resolves plugins from the **git repo**, not from
  pip-installed site-packages
- Having the plugin in `src/` implies it's part of the Python package, but
  the marketplace doesn't use it that way
- The symlink reason no longer applies

---

## Proposed Structure

```
artisan/
├── .claude-plugin/
│   └── marketplace.json     ← single manifest (marketplace + inline plugin)
├── skills/                  ← at repo root (required by plugin spec)
│   ├── write-operation/
│   │   └── SKILL.md
│   ├── write-pipeline/
│   │   └── SKILL.md
│   ├── write-composite/
│   │   └── SKILL.md
│   └── write-docs/
│       └── SKILL.md
├── src/
├── docs/
└── ...
```

No separate `plugin.json`. The plugin is defined inline in the marketplace
entry using `"source": "./"` and `"strict": false`, with explicit skill paths.
This matches the pattern used by Anthropic's own `anthropics/skills` repo.

### Manifest

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
      "source": "./",
      "strict": false,
      "description": "Skills for building pipelines and operations with the Artisan framework",
      "skills": [
        "./skills/write-operation",
        "./skills/write-pipeline",
        "./skills/write-composite",
        "./skills/write-docs"
      ]
    }
  ]
}
```

No changes to the skill `SKILL.md` files themselves — they move as-is.

---

## Distribution

### Marketplace (primary)

The marketplace is the official distribution mechanism. It is git-based: Claude
Code clones the repo and resolves the plugin from the repo checkout.

**Initial setup uses the private dev repo:**

```
/plugin marketplace add andrewcolinhunt/artisan-dev
/plugin install artisan
```

**Eventually switches to the public repo:**

```
/plugin marketplace add dexterity-systems/artisan
/plugin install artisan
```

The only change required is the repo URL in the marketplace add command (and
in any downstream `.claude/settings.json` files). The marketplace.json is
identical in both repos.

### CLI fallback

For users who prefer not to use the marketplace:

```bash
claude --plugin-dir /path/to/artisan-repo
```

This points directly to the repo root, which is now the plugin root. Simpler
than the old `$(python -c "import artisan; ...")` incantation.

The pixi task `claude-plugin-path` is no longer needed and should be removed
from `pyproject.toml`.

---

## Downstream Repo Setup

### Option A: Zero-setup for contributors (recommended)

The repo maintainer commits a `.claude/settings.json`:

```json
{
  "extraKnownMarketplaces": {
    "artisan": {
      "source": {
        "source": "github",
        "repo": "andrewcolinhunt/artisan-dev"
      }
    }
  },
  "enabledPlugins": {
    "artisan@artisan": true
  }
}
```

When a contributor opens the project and trusts the folder, Claude Code
prompts them to install the plugin. They accept once and it persists.

When the public repo is ready, the maintainer updates the repo field to
`dexterity-systems/artisan`.

### Option B: Individual user setup

For users who work across many artisan-based repos:

```
/plugin marketplace add andrewcolinhunt/artisan-dev
/plugin install artisan
```

Skills are then available in all projects.

### Available skills

| Skill | Invocation | Description |
|---|---|---|
| Write Operation | `/write-operation` | Scaffold an Artisan operation |
| Write Pipeline | `/write-pipeline` | Design and write a pipeline |
| Write Composite | `/write-composite` | Write a composite operation |
| Write Docs | `/write-docs` | Write documentation pages |

Skills appear without a namespace prefix. The plugin name (`artisan`) is shown
in the skill description rather than the invocation command.

---

## Migration Steps

- Rewrite `.claude-plugin/marketplace.json` with inline plugin definition (`strict: false`, explicit `skills` paths)
- Delete `src/artisan/_claude_plugin/.claude-plugin/plugin.json` (no longer needed)
- Move `src/artisan/_claude_plugin/skills/` to `skills/` at repo root
- Delete `src/artisan/_claude_plugin/` directory
- Remove `artifacts = ["src/artisan/_claude_plugin/**"]` from `pyproject.toml` `[tool.hatch.build]`
- Remove `claude-plugin-path` task from `pyproject.toml` `[tool.pixi.tasks]`
- Verify with `claude --plugin-dir .` that skills load correctly
- Test `/plugin marketplace add` against the dev repo
- Update docs that reference the old plugin path

---

## Decisions

- **Start with private dev repo**, switch to public repo later
- **No official marketplace submission** for now
- **No separate plugin.json** — inline definition in marketplace.json, matching `anthropics/skills` pattern
- **Remove the pixi task** — the CLI fallback now just points to the repo root
- **No pip-based distribution** — the marketplace is git-based by design
