# Design: External Notes Repo with Symlinked `_dev/`

## Problem

The `_dev/` directory contains design docs, analysis, scratch notes,
troubleshooting scripts, and release drafts that need version control
and backup, but must never appear in PRs to the public repo.

The current approach — tracking `_dev/` on `ach/dev` and stripping it
before PRs — has cascading problems:

- Feature branches inherit `_dev/` from `ach/dev`, polluting PR diffs
- Stacked PRs require complex cleaning (chained rebase or independent
  `-clean` branches, both with issues)
- The decontamination step is manual, error-prone, and non-standard
- No stacked PR tool supports file exclusion — they all assume clean
  commits

Research across git docs, stacked PR tools (Graphite, ghstack,
git-branchless), Google/Meta workflows, and practitioner forums
confirms: **the established practice is to never commit dev files to
branches that produce PRs.**

## Prior Art Survey

### Current state

- `_dev/` is tracked on `ach/dev` (~60 files, ~1.6MB, mostly markdown)
- `CLAUDE.local.md` is tracked on `ach/dev` at project root
- `pyproject.toml` excludes `_dev/` from mypy, ruff, and codespell
- `.gitignore` does **not** exclude `_dev/` or `CLAUDE.local.md`
- No CI workflows reference `_dev/`
- Some `_dev/` files import from `artisan` (troubleshooting scripts,
  demo notebook)
- Feature branches branch from `ach/dev`, inheriting all dev files

### Approaches considered

| Approach | Verdict |
|----------|---------|
| `.gitignore` + no backup | Too risky — design docs need history |
| Separate adjacent repo (no symlink) | Two repos to manage, context friction |
| Orphan branch + worktree | Fragile, poor IDE support |
| Feature branches from clean targets | Dev files vanish on branch switch |
| Nested repo inside project | Works, but nested `.git/` confuses tools and is vulnerable to parent `git clean` |
| **External repo + symlink** | **Selected — see below** |

## Design

### External notes repo, symlinked into the project

The dev files live in a standalone repo at `~/git/notes/artisan/`.
A symlink at `_dev/` in the project root points to it. The parent
repo's `.gitignore` excludes both `_dev` and `CLAUDE.local.md`.

This follows a `~/git/notes/<project>/` pattern that scales across
repos.

```
~/git/notes/                  ← notes directory (one repo or many)
└── artisan/                  ← dev files for this project
    ├── .git/
    ├── analysis/
    ├── archive/
    ├── design/
    ├── releases/
    ├── troubleshooting/
    ├── todo.md
    ├── scratch.md
    └── CLAUDE.local.md

~/git/artisan-dev/            ← parent repo (public-facing)
├── .gitignore                ← contains "_dev" and "CLAUDE.local.md"
├── _dev -> ~/git/notes/artisan/   ← symlink
├── CLAUDE.local.md -> _dev/CLAUDE.local.md  ← symlink (resolves through _dev)
├── src/
├── tests/
├── docs/
└── CLAUDE.md
```

### Why external + symlink over nested repo

- No nested `.git/` — avoids tool confusion (GitHub Desktop,
  recursive git operations)
- Survives destructive parent git operations (`git clean -fdx`,
  worktree creation/deletion)
- The notes repo is a first-class citizen with its own lifecycle
- Could link the same notes into multiple worktrees if needed
- `~/git/notes/<project>/` scales as a pattern across repos

### `CLAUDE.local.md` handling

Claude Code expects `CLAUDE.local.md` at the project root. The actual
file lives in the notes repo. A symlink at the project root points
to it through the `_dev` symlink:

```
artisan-dev/CLAUDE.local.md → _dev/CLAUDE.local.md
```

macOS resolves chained symlinks transparently. Both `CLAUDE.local.md`
and `_dev` are in the parent's `.gitignore`, so neither the symlinks
nor the targets appear in the parent repo.

### Remote for the notes repo

A single `notes` repo with per-project subdirectories:

| Setting | Value |
|---------|-------|
| Remote | `origin` |
| URL | `https://github.com/andrewcolinhunt/notes.git` (new private repo) |

```
~/git/notes/
├── .git/
├── artisan/          ← symlinked as artisan-dev/_dev
└── other-project/    ← future projects follow the same pattern
```

### What changes in the parent repo

**`.gitignore` additions:**
```
# Dev-only files (tracked in external notes repo, symlinked as _dev/)
_dev
CLAUDE.local.md
```

**`pyproject.toml`** — no changes needed. The `_dev/` exclusions in
mypy, ruff, and codespell are already present and remain correct (they
exclude the directory from linting regardless of git tracking status).

### Branch workflow after migration

| Branch | Role |
|--------|------|
| `main` | Synced with upstream, production-ready |
| `ach/dev` | Long-lived staging branch for integrating work before PRing |
| `feat/*`, `fix/*`, etc. | Feature branches off `ach/dev`, PR to `main` |

`ach/dev` stays as a staging/integration branch — useful for
accumulating and testing multiple changes together before they're
individually ready for PR. The difference from before: `ach/dev` is
now naturally clean (no dev files tracked), so feature branches off it
are also clean. No `-clean` branches needed.

`ach/dev-clean` is eliminated entirely.

### What changes in the workflow

| Before | After |
|--------|-------|
| `_dev/` tracked on `ach/dev` | `_dev/` is an external repo, symlinked in |
| `CLAUDE.local.md` tracked on `ach/dev` | Symlink to `_dev/CLAUDE.local.md` |
| `-clean` branches strip dev files | No `-clean` branches needed |
| Decontamination scan before PR | No decontamination needed |
| Dev files vanish on branch switch | Dev files persist (not in parent git) |
| `ach/dev-clean` ephemeral branch | Eliminated |

### What stays the same

- `ach/dev` remains a long-lived staging/integration branch
- Feature branches off `ach/dev`
- Design doc and analysis conventions (`_dev/design/`, `_dev/analysis/`)
- Commit conventions, PR validation, naming conventions
- Workflow sizing (lightweight/standard/full)
- `CLAUDE.md` (public, tracked in parent)

## Scope

### Migration steps

**Untrack dev files from parent repo (on `ach/dev`):**
- Remove `_dev/` and `CLAUDE.local.md` from git tracking (keeps files
  on disk): `git rm -r --cached _dev/ CLAUDE.local.md`
- Add `_dev` and `CLAUDE.local.md` to `.gitignore`
- Commit: `chore: move dev files to external notes repo`

**Create external notes repo:**
- `mkdir -p ~/git/notes`
- `mv _dev ~/git/notes/artisan`
- `cd ~/git/notes/artisan && git init`
- `mv` the `CLAUDE.local.md` from parent root into
  `~/git/notes/artisan/` (if not already there from the `_dev/` move)
- `git add . && git commit -m "init: migrate dev files from artisan-dev"`
- Create private remote repo and push

**Create symlinks in parent:**
- `ln -s ~/git/notes/artisan _dev`
- `ln -s _dev/CLAUDE.local.md CLAUDE.local.md`
- Verify both are ignored by parent git (`git status` shows clean)

**Workflow and config files to update:**
- `CLAUDE.local.md` — remove references to `ach/dev-clean`, stripping,
  decontamination. Update to reflect symlink setup and notes repo
  location.
- `CLAUDE.md` (project) — update "Personal Overrides" section to
  mention that `CLAUDE.local.md` is typically a symlink from the
  notes repo.
- `~/.claude/CLAUDE.md` (global, new) — create with cross-project
  instructions: the `~/git/notes/<project>/` pattern, that `_dev/`
  is a symlink to the notes repo, and that notes repo changes need
  separate commits/pushes.
- `dev-branch-workflow` skill — simplify. Remove all `-clean` branch
  sections, decontamination, dev-file stripping.

### What to do with existing `ach/dev` history

The `_dev/` files have history on `ach/dev`. The nested repo starts
fresh from the current state. Old history is preserved in `ach/dev`
if needed (`git log --follow -- _dev/`). Don't rewrite history.

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Forgetting to commit/push notes repo separately | Add reminder to workflow skill; could add a pre-push hook |
| Symlink is machine-specific | Each dev sets up their own symlink; documented in setup instructions |
| IDE confusion with symlinked directory | VS Code follows directory symlinks transparently |
| macOS symlink resolution | macOS resolves chained symlinks natively; no known issues |
| `_dev/` scripts that import `artisan` | Still work — they run from the parent repo's pixi environment |
| `git clean -fdx` in parent | Removes the symlink, not the target. Recreate with `ln -s` |
| Docker/rsync not following symlinks | Not a current concern; if needed, use `-L` flag |

## Testing

- Verify `_dev/` symlink resolves correctly from project root
- Verify `CLAUDE.local.md` symlink works with Claude Code
- Verify `_dev/` files persist across parent branch switches
- Verify `_dev/` scripts run correctly (artisan imports still work)
- Verify `pyproject.toml` exclusions still apply
- Verify `_dev` does not appear in `git status` on parent repo
- Verify `git clean -fdx` removes symlink but not target
- Create a test feature branch from `ach/dev` and confirm zero dev files
- Run full validation suite after migration
