# Design: Claude Code Plugin Distribution

**Date:** 2026-03-12  **Status:** Draft  **Author:** Claude + ach94

---

## Problem

Artisan ships a Claude Code plugin at `src/artisan/_claude_plugin/` with skills
for writing operations, pipelines, composites, and docs. The goal is that
downstream projects that install artisan as a dependency can run a single command
and get these skills available in their Claude Code sessions.

The current install script (`pixi run install-claude-plugin`) creates a symlink
at `.claude/plugins/artisan`, but the skills don't appear in Claude Code.

### Key constraint

This must work in **any codebase that depends on artisan**, not just the artisan
repo itself. The plugin ships inside the Python package and must be discoverable
after `pip install artisan` or equivalent.

---

## Current State

### What we have

```
src/artisan/_claude_plugin/
├── .claude-plugin/
│   └── plugin.json          # Plugin manifest (name, description, version)
└── skills/
    ├── write-docs/SKILL.md
    ├── write-operation/SKILL.md
    ├── write-pipeline/SKILL.md
    └── write-composite/SKILL.md
```

The plugin directory is included in the wheel via `pyproject.toml`:

```toml
[tool.hatch.build]
artifacts = ["src/artisan/_claude_plugin/.claude-plugin/**"]
```

**Problem:** Only `.claude-plugin/**` is marked as an artifact — the `skills/`
directory is not included, so it gets dropped from the built wheel.

### What the install script does

`scripts/pixi/install-claude-plugin.sh`:

```bash
plugin_src="$(python -c 'import artisan; ...')"  # resolves to site-packages path
mkdir -p .claude/plugins
ln -sfn "$plugin_src" ".claude/plugins/artisan"
```

This creates `.claude/plugins/artisan -> <site-packages>/artisan/_claude_plugin`.

### Why it doesn't work

Claude Code does **not** scan `.claude/plugins/` for plugin discovery. That path
has no special meaning. Plugins are discovered through two mechanisms only:

- The `--plugin-dir` CLI flag
- Marketplace registration and installation

---

## Research: How Claude Code Discovers Skills and Plugins

### Skills (auto-discovered)

Claude Code automatically scans these locations for skills:

| Scope | Path | Discovery |
|-------|------|-----------|
| Personal | `~/.claude/skills/<name>/SKILL.md` | Automatic |
| Project | `.claude/skills/<name>/SKILL.md` | Automatic |
| Nested | `<subdir>/.claude/skills/<name>/SKILL.md` | Automatic (monorepo support) |
| Additional dirs | Via `--add-dir` flag | Automatic |

Skills placed in `.claude/skills/` are auto-discovered with no configuration.
They appear as `/<skill-name>` slash commands. The `SKILL.md` `description`
field drives auto-invocation — Claude matches user intent against descriptions.

Legacy `.claude/commands/<name>.md` files also still work and create equivalent
slash commands.

### Plugins (require explicit loading)

A plugin is a directory containing `.claude-plugin/plugin.json` at its root,
plus component directories (`skills/`, `commands/`, `agents/`, `hooks/`) at the
plugin root level. Plugin skills are namespaced: a skill `hello` in plugin
`my-plugin` becomes `/my-plugin:hello`.

Plugins are loaded via:

| Method | Command | Use case |
|--------|---------|----------|
| CLI flag | `claude --plugin-dir ./path` | Dev/testing, per-session |
| Interactive install | `/plugin install ./path` | Persistent, requires Claude Code session |
| Marketplace install | `/plugin install name@marketplace` | Distribution at scale |

There is no filesystem path that Claude Code scans automatically for plugins.

### Marketplaces (plugin catalogs)

A marketplace is a Git repository (or local directory) containing a
`.claude-plugin/marketplace.json` file that catalogs available plugins.

**Registration:**

```bash
# GitHub shorthand
/plugin marketplace add owner/repo

# Full URL
/plugin marketplace add https://gitlab.com/org/plugins.git

# Local directory
/plugin marketplace add ./my-marketplace

# Specific branch/tag
/plugin marketplace add https://github.com/org/repo.git#v1.0.0
```

**Official marketplace:** `anthropics/claude-plugins-official` is automatically
available. It includes LSP integrations (pyright, typescript, rust-analyzer),
external services (GitHub, Slack, Linear, Sentry), and development workflows.

**Installation flow:**

```
/plugin                              # Opens tabbed UI (Discover, Installed, Marketplaces, Errors)
/plugin install plugin-name@marketplace-name
```

**Installation scopes:**

| Scope | Settings file | Use case |
|-------|--------------|----------|
| `user` | `~/.claude/settings.json` | Personal, all projects (default) |
| `project` | `.claude/settings.json` | Shared via version control |
| `local` | `.claude/settings.local.json` | Project-specific, gitignored |

**Team distribution:** Projects can ship a `.claude/settings.json` with
`extraKnownMarketplaces` and `enabledPlugins` to auto-configure plugins for
all contributors.

Installed plugins are cached at `~/.claude/plugins/cache/` (implementation
detail, not a discovery path).

---

## Options

### Option A: Symlink skills into `.claude/skills/`

Change the install script to symlink individual skill directories into the
auto-discovered `.claude/skills/` path.

**Install script creates:**

```
.claude/skills/
├── artisan-write-docs/       -> <site-packages>/artisan/_claude_plugin/skills/write-docs/
├── artisan-write-operation/  -> <site-packages>/artisan/_claude_plugin/skills/write-operation/
├── artisan-write-pipeline/   -> <site-packages>/artisan/_claude_plugin/skills/write-pipeline/
└── artisan-write-composite/  -> <site-packages>/artisan/_claude_plugin/skills/write-composite/
```

**Skills appear as:** `/artisan-write-docs`, `/artisan-write-operation`, etc.

**Pros:**
- Auto-discovered, zero config after install
- Works alongside project-specific skills
- No Claude Code session required to install (shell script is fine)
- Simplest approach

**Cons:**
- No plugin namespacing (prefix convention `artisan-` instead of `artisan:`)
- Skills dir may be committed to version control; symlinks into it could confuse
  git (mitigated by `.gitignore`)
- Doesn't use the plugin system at all — the `plugin.json` manifest is unused

### Option B: Register as a local marketplace

Ship a `marketplace.json` that references the installed plugin, and have the
install script register it.

**Install script runs:**

```bash
claude plugin marketplace add "$plugin_src"
claude plugin install artisan@artisan
```

**Skills appear as:** `/artisan:write-docs`, `/artisan:write-operation`, etc.

**Pros:**
- Uses the official plugin system with proper namespacing
- Marketplace infrastructure handles versioning and updates
- `plugin.json` manifest is used

**Cons:**
- Requires `claude` CLI available in the install script's environment
- Requires an interactive or at least functional Claude Code installation
- More moving parts (marketplace registration + plugin install)
- Unclear if `claude plugin` commands work non-interactively from a shell script

### Option C: Configure `--plugin-dir` via project settings

Have the install script write the plugin path into `.claude/settings.json` or
`.claude/settings.local.json` so Claude Code picks it up on startup.

**Blocked:** There is no `pluginDirs` setting in Claude Code's configuration
schema. The `--plugin-dir` flag is CLI-only with no persistent equivalent.

### Option D: Hybrid — skills for auto-discovery, keep plugin structure for future

Use Option A for immediate functionality, but retain the plugin directory
structure in the package. When Claude Code adds a persistent `pluginDirs` config
or a package-level plugin discovery mechanism, migrate to the plugin system.

**Pros:**
- Works today
- Forward-compatible — plugin structure is already correct
- No throwaway work

**Cons:**
- Mild complexity: two "views" of the same skills (plugin structure in source,
  flat symlinks in `.claude/skills/`)

---

## Recommendation

**Option D (hybrid).**

The plugin system is the right long-term home, but it currently lacks a
filesystem discovery path or a persistent config equivalent to `--plugin-dir`.
Symlinking into `.claude/skills/` is the only approach that works from a shell
script without requiring a Claude Code session.

### Changes required

**`pyproject.toml`** — include skills in the wheel:

```toml
[tool.hatch.build]
artifacts = ["src/artisan/_claude_plugin/**"]
```

(Change from `.claude-plugin/**` to `**` to capture the entire plugin tree.)

**`scripts/pixi/install-claude-plugin.sh`** — symlink skills, not the plugin
dir:

```bash
plugin_src="$(python -c 'import artisan; ...')"
mkdir -p .claude/skills

for skill_dir in "$plugin_src/skills"/*/; do
    skill_name=$(basename "$skill_dir")
    target=".claude/skills/artisan-${skill_name}"
    if [ -L "$target" ] && [ "$(readlink "$target")" = "$skill_dir" ]; then
        echo "  already installed: $target"
    else
        ln -sfn "$skill_dir" "$target"
        echo "  installed: $target"
    fi
done
```

**`.gitignore`** — ignore the symlinked skills:

```gitignore
.claude/skills/artisan-*
```

**Cleanup** — remove the `.claude-plugin/marketplace.json` at the repo root and
the `.claude/plugins/` symlink, as neither serves a purpose.

### Uninstall script

Add a `pixi run uninstall-claude-plugin` task:

```bash
for link in .claude/skills/artisan-*; do
    [ -L "$link" ] && rm "$link" && echo "removed: $link"
done
```
