# Design: Nested `_dev/` Repo

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

- `_dev/` is tracked on `ach/dev` (72 files, 1.7MB, mostly markdown)
- `CLAUDE.local.md` is tracked on `ach/dev` at project root
- `pyproject.toml` excludes `_dev/` from mypy, ruff, and codespell
- `.gitignore` does **not** exclude `_dev/` or `CLAUDE.local.md`
- No CI workflows reference `_dev/`
- Some `_dev/` scripts import from `artisan` (troubleshooting scripts)
- Feature branches branch from `ach/dev`, inheriting all dev files

### Approaches considered

| Approach | Verdict |
|----------|---------|
| `.gitignore` + no backup | Too risky — design docs need history |
| Separate adjacent repo | Two repos to manage, context friction |
| Orphan branch + worktree | Fragile, poor IDE support |
| Feature branches from clean targets | Dev files vanish on branch switch |
| **Nested repo in `.gitignore`** | **Selected — see below** |

## Design

### Make `_dev/` its own git repo, ignored by the parent

The `_dev/` directory becomes an independent git repo. The parent repo's
`.gitignore` excludes it. Git handles nested repos correctly when the
parent ignores the nested directory.

```
artisan-dev/              ← parent repo (public-facing)
├── .gitignore            ← contains "_dev/" and "CLAUDE.local.md"
├── src/
├── tests/
├── docs/
├── CLAUDE.md             ← public, tracked in parent
└── _dev/                 ← nested repo (private, ignored by parent)
    ├── .git/
    ├── design/
    ├── analysis/
    ├── archive/
    ├── releases/
    ├── troubleshooting/
    ├── todo.md
    ├── scratch.md
    └── CLAUDE.local.md   ← symlinked to project root
```

### `CLAUDE.local.md` handling

Claude Code expects `CLAUDE.local.md` at the project root. The actual
file lives in `_dev/` (tracked in the nested repo). A symlink at the
project root points to it:

```
artisan-dev/CLAUDE.local.md → _dev/CLAUDE.local.md
```

Both `CLAUDE.local.md` and `_dev/` are in the parent's `.gitignore`, so
neither the symlink nor the target appears in the parent repo.

### Remote for the nested repo

The `_dev/` repo pushes to the existing private fork remote:

| Setting | Value |
|---------|-------|
| Remote | `origin` |
| URL | `https://github.com/andrewcolinhunt/artisan-dev-notes.git` (new private repo) |

Alternatively, it could be a branch or separate path in the existing
`andrewcolinhunt/artisan-dev` repo, but a dedicated repo is cleaner.

### What changes in the parent repo

**`.gitignore` additions:**
```
# Dev-only files (tracked in nested _dev/ repo)
_dev/
CLAUDE.local.md
```

**`pyproject.toml`** — no changes needed. The `_dev/` exclusions in
mypy, ruff, and codespell are already present and remain correct (they
exclude the directory from linting regardless of git tracking status).

**`.git/info/exclude`** — not needed. `.gitignore` handles it, and the
`_dev/` and `CLAUDE.local.md` entries are reasonable for any contributor
to see (they document that these paths are intentionally excluded).

### What changes in the workflow

| Before | After |
|--------|-------|
| Branch features from `ach/dev` | Branch features from `main`/`release/*` |
| `_dev/` tracked on `ach/dev` | `_dev/` is its own repo, always on disk |
| `CLAUDE.local.md` tracked on `ach/dev` | Symlink to `_dev/CLAUDE.local.md` |
| `-clean` branches strip dev files | No `-clean` branches needed |
| Decontamination scan before PR | No decontamination needed |
| Dev files vanish on branch switch | Dev files persist (not in parent git) |
| `ach/dev-clean` ephemeral branch | Eliminated |

### What stays the same

- `ach/dev` remains a long-lived branch for integration/testing
- Design doc and analysis conventions (`_dev/design/`, `_dev/analysis/`)
- Commit conventions, PR validation, naming conventions
- Workflow sizing (lightweight/standard/full)
- `CLAUDE.md` (public, tracked in parent)

## Scope

### Migration steps

**Parent repo (on `ach/dev`):**
- Remove `_dev/` and `CLAUDE.local.md` from git tracking:
  `git rm -r --cached _dev/ CLAUDE.local.md`
- Add `_dev/` and `CLAUDE.local.md` to `.gitignore`
- Commit: `chore: move _dev/ to nested repo, gitignore dev-only files`
- Create symlink: `ln -s _dev/CLAUDE.local.md CLAUDE.local.md`

**Nested repo (inside `_dev/`):**
- `cd _dev && git init`
- Move `CLAUDE.local.md` from project root into `_dev/`
- `git add . && git commit -m "init: migrate dev files to nested repo"`
- Create private remote repo and push

**Workflow files to update:**
- `CLAUDE.local.md` — remove references to `ach/dev-clean`, stripping,
  decontamination. Update branching instructions.
- `dev-branch-workflow` skill — simplify significantly. Remove all
  `-clean` branch sections, decontamination, dev-file stripping.
- `implement-design` skill — verify it still works (it defers to
  CLAUDE.local.md, so should be fine).

### What to do with existing `ach/dev` history

The `_dev/` files have history on `ach/dev`. Options:
- **Preserve:** Leave the history in `ach/dev`. The files are simply
  untracked going forward. Old commits still reference them.
- **Transplant:** Use `git log --follow -- _dev/` to preserve awareness
  of history. The nested repo starts fresh, but the old history exists
  in `ach/dev` if needed.

Recommendation: preserve. Don't rewrite `ach/dev` history. The nested
repo starts fresh from the current state.

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Forgetting to commit/push `_dev/` separately | Add a reminder to the workflow skill; could add a pre-push hook |
| IDE confusion with nested repos | VS Code handles nested repos well (shows both in source control). JetBrains may need config. |
| Symlink not working on Windows | Not a current concern (macOS dev environment). If needed later, copy instead of symlink. |
| `_dev/` scripts that import `artisan` | Still work — they run from the parent repo's environment, `_dev/` being a nested repo doesn't affect Python imports |
| New contributor setup | Document in `CLAUDE.local.md` (which they'd create from a template anyway) |

## Open Questions

- **Remote repo name:** `artisan-dev-notes` (new repo) vs a branch in
  `artisan-dev`? Separate repo is cleaner but one more thing to manage.
- **Should `ach/dev` continue to exist?** With dev files in a nested
  repo and features branching from clean targets, `ach/dev` may no
  longer serve a purpose. It could become just `main` with the nested
  `_dev/` overlay. Or it could remain as an integration branch for
  testing before PRs.

## Testing

- Verify `_dev/` files persist across parent branch switches
- Verify `CLAUDE.local.md` symlink works with Claude Code
- Verify `_dev/` scripts run correctly (artisan imports still work)
- Verify `pyproject.toml` exclusions still apply
- Verify `_dev/` does not appear in `git status` on parent repo
- Create a test feature branch from `main` and confirm zero dev files
- Run full validation suite after migration
