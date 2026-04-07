# Design: Cross-Repo Notes Pattern

## Problem

Dev-only files — design docs, analysis, scratch notes, troubleshooting
scripts, release drafts — need version control and backup, but must
never appear in PRs to public repos.

The common approach of tracking these files on a dev branch and
stripping them before PRs has cascading problems:

- Feature branches inherit dev files, polluting PR diffs
- Stacked PRs require complex cleaning at each layer
- The stripping step is manual, error-prone, and non-standard
- No stacked PR tool supports file exclusion

**The established practice is to never commit dev files to branches
that produce PRs.** The solution is to keep dev files in a separate
repo entirely, linked into each project for convenience.

## Design

### A single notes repo, symlinked into each project

All dev files live in `~/git/notes/`, a standalone git repo with
per-project subdirectories. Each project gets a `_dev` symlink
pointing to its subdirectory. The parent repo's `.gitignore` excludes
the symlink.

```
~/git/notes/                        ← single notes repo (private)
├── .git/
├── hooks/                          ← shared hook scripts
│   ├── post-commit
│   └── post-checkout
├── setup.sh                        ← idempotent per-project setup
├── artisan/                        ← dev files for artisan
│   ├── analysis/
│   ├── design/
│   ├── releases/
│   ├── troubleshooting/
│   ├── todo.md
│   ├── scratch.md
│   └── CLAUDE.local.md
└── other-project/                  ← same pattern, different project
    ├── design/
    └── CLAUDE.local.md
```

A project after setup:

```
~/git/some-project/                 ← any project repo
├── .gitignore                      ← contains "_dev" and "CLAUDE.local.md"
├── _dev -> ~/git/notes/project/    ← symlink (created by setup.sh)
├── CLAUDE.local.md -> _dev/CLAUDE.local.md  ← symlink
├── src/
└── ...
```

### Why this approach

- **No nested `.git/`** — avoids tool confusion (GitHub Desktop,
  recursive git operations)
- **Survives destructive parent git operations** — `git clean -fdx`
  removes the symlink but not the target
- **One repo to back up** — all notes across all projects, single
  remote, single `git push`
- **Scales to any number of projects** — `setup.sh` wires up a new
  project in seconds
- **Dev files persist across branch switches** — they're not in parent
  git at all
- **CI is inherently safe** — fresh clones have no symlink, no dev
  files

### `CLAUDE.local.md` handling

Claude Code expects `CLAUDE.local.md` at the project root. The actual
file lives in the notes repo. A symlink at the project root points to
it through the `_dev` symlink:

```
project/CLAUDE.local.md → _dev/CLAUDE.local.md
```

macOS resolves chained symlinks transparently. Both `CLAUDE.local.md`
and `_dev` are in the parent's `.gitignore`, so neither the symlinks
nor the targets appear in the parent repo.

### Remote

| Setting | Value |
|---------|-------|
| Remote | `origin` |
| URL | `https://github.com/andrewcolinhunt/notes.git` (private) |

### Structural enforcement

The workflow is enforced by four layers, from filesystem up. These
are generic — they work for any project using the pattern.

**Filesystem** — `.gitignore` + `.git/info/exclude` prevent staging.
Git will not track ignored files unless explicitly forced with
`git add -f`.

**`post-commit` hook** — after committing in the parent repo, checks
if the notes repo has uncommitted changes and prints a reminder.
Stored at `~/git/notes/hooks/post-commit`, symlinked into each
project by `setup.sh`.

```bash
#!/bin/bash
NOTES_DIR="$HOME/git/notes"
if [ -d "$NOTES_DIR/.git" ]; then
    STATUS=$(git -C "$NOTES_DIR" status --porcelain 2>/dev/null)
    if [ -n "$STATUS" ]; then
        echo ""
        echo "NOTE: ~/git/notes/ has uncommitted changes"
    fi
fi
```

**`post-checkout` hook** — after branch switches or worktree creation,
warns if the `_dev` symlink is missing or broken. Stored at
`~/git/notes/hooks/post-checkout`, symlinked into each project by
`setup.sh`.

```bash
#!/bin/bash
if [ ! -L "_dev" ] || [ ! -e "_dev" ]; then
    echo ""
    echo "WARNING: _dev symlink missing or broken"
    echo "  Re-run: ~/git/notes/setup.sh <project> $(pwd)"
fi
if [ ! -L "CLAUDE.local.md" ] || [ ! -e "CLAUDE.local.md" ]; then
    echo ""
    echo "WARNING: CLAUDE.local.md symlink missing or broken"
    echo "  Re-run: ~/git/notes/setup.sh <project> $(pwd)"
fi
```

**`git notes-save` alias** — one command to commit and push the notes
repo. Reduces friction, which is the main lever for getting notes
committed regularly.

```bash
git config --global alias.notes-save \
  '!cd ~/git/notes && git add -A && git commit -m "sync: $(date +%Y-%m-%d)" && git push'
```

### `setup.sh` — per-project setup

Idempotent script stored in the notes repo. Wires up any project:

```bash
~/git/notes/setup.sh <notes-subdir> <project-dir>
```

What it does:

- Creates `~/git/notes/<subdir>/` if it doesn't exist
- Creates `_dev` symlink in the project dir (skips if already valid)
- Creates `CLAUDE.local.md` symlink if the file exists in notes
- Adds `_dev` and `CLAUDE.local.md` to `.gitignore` (if not present)
- Adds `_dev` and `CLAUDE.local.md` to `.git/info/exclude`
- Symlinks `post-commit` and `post-checkout` hooks from
  `~/git/notes/hooks/` into `.git/hooks/` (warns if hooks already
  exist rather than overwriting)

Edge cases handled:

- `_dev` already exists as a real directory → error, user must move
  it first
- `_dev` is a broken symlink → removes and recreates
- `_dev` is already a valid symlink → skips
- `.gitignore` already has entries → skips
- `.git/hooks/post-commit` already exists → warns, does not overwrite

### Global `~/.claude/CLAUDE.md`

Cross-project instructions for Claude Code. Auto-loaded in every
conversation, in any repo:

```markdown
## Notes Repo

Dev files (design docs, analysis, scratch) live in ~/git/notes/<project>/.
Each project symlinks _dev -> the notes directory. Notes repo changes
need separate git commits and pushes from the parent repo.

- `git notes-save` — commit and push all notes changes
```

### What was considered and rejected

| Idea | Why not |
|------|---------|
| `pre-commit` hook blocking `_dev/` files | `.gitignore` + `.git/info/exclude` already prevent staging; only fires on `git add -f`, a deliberate override |
| `pre-push` hook | Backup for a backup — unnecessary |
| Claude Code `PostToolUse` hook | Fires on every `_dev/` edit, noisy; CLAUDE.local.md already provides context |
| Claude Code `SessionStart`/`Stop` hooks | Duplicates what git hooks do at better moments |
| Launchd auto-commit | Maintenance overhead (breaks on macOS upgrades) for marginal benefit on a notes repo |
| Notes repo git hooks | Adds complexity to what should be the simple repo |
| Health-check script | Run once after setup, never again |
| `git config --global core.hooksPath` | Overrides ALL repos' hooks, too heavy-handed |
| `git init.templateDir` | Applies to ALL new repos, even ones not using the pattern |

## Scope: Artisan Migration

This section covers migrating the first project (artisan-dev) to the
pattern. Future projects just run `setup.sh`.

### One-time setup (do once, ever)

**Create the notes repo:**
- `mkdir -p ~/git/notes`
- `cd ~/git/notes && git init`
- Create `hooks/` directory with `post-commit` and `post-checkout`
  scripts (shown above)
- Create `setup.sh` (described above)
- Create private remote repo and push
- `git config --global alias.notes-save '!cd ~/git/notes && git add -A && git commit -m "sync: $(date +%Y-%m-%d)" && git push'`

**Create global Claude Code config:**
- Write `~/.claude/CLAUDE.md` with notes repo pattern (shown above)

### Artisan-specific migration

**Untrack dev files from parent repo (on `ach/dev`):**
- `git rm -r --cached _dev/ CLAUDE.local.md`
- Add `_dev` and `CLAUDE.local.md` to `.gitignore`
- Commit: `chore: move dev files to external notes repo`

**Move files to notes repo:**
- `mv _dev ~/git/notes/artisan`
- `mv CLAUDE.local.md ~/git/notes/artisan/`
- `cd ~/git/notes && git add artisan/ && git commit -m "init: migrate artisan dev files"`

**Run setup for artisan:**
- `~/git/notes/setup.sh artisan ~/git/artisan-dev`

**Workflow files to update:**
- `CLAUDE.local.md` — remove references to `ach/dev-clean`, stripping,
  decontamination. Update to reflect symlink setup.
- `CLAUDE.md` (project) — update "Personal Overrides" section to
  mention that `CLAUDE.local.md` is typically a symlink from the
  notes repo.
- `dev-branch-workflow` skill — simplify. Remove all `-clean` branch
  sections, decontamination, dev-file stripping.

### Branch workflow after migration

| Branch | Role |
|--------|------|
| `main` | Synced with upstream, production-ready |
| `ach/dev` | Long-lived staging branch for integrating work before PRing |
| `feat/*`, `fix/*`, etc. | Feature branches off `ach/dev`, PR to `main` |

`ach/dev` stays as a staging/integration branch. The difference from
before: it's now naturally clean (no dev files tracked), so feature
branches off it are also clean. No `-clean` branches needed.

`ach/dev-clean` is eliminated entirely.

### What changes in the workflow

| Before | After |
|--------|-------|
| `_dev/` tracked on `ach/dev` | `_dev/` is symlink to notes repo |
| `CLAUDE.local.md` tracked on `ach/dev` | Symlink to `_dev/CLAUDE.local.md` |
| `-clean` branches strip dev files | No `-clean` branches needed |
| Decontamination scan before PR | No decontamination needed |
| Dev files vanish on branch switch | Dev files persist (not in parent git) |
| `ach/dev-clean` ephemeral branch | Eliminated |

### What to do with existing `ach/dev` history

The `_dev/` files have history on `ach/dev`. The notes repo starts
fresh from the current state. Old history is preserved in `ach/dev`
if needed (`git log --follow -- _dev/`). Don't rewrite history.

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Forgetting to commit notes | `post-commit` hook reminds; `git notes-save` alias reduces friction |
| Symlink is machine-specific | `setup.sh` makes creation trivial |
| Symlink broken after `git clean -fdx` | `post-checkout` hook warns; target is safe; re-run `setup.sh` |
| IDE confusion with symlinked directory | VS Code follows directory symlinks transparently |
| macOS symlink resolution | macOS resolves chained symlinks natively; no known issues |
| Scripts in `_dev/` that import project code | Still work — they run from the parent repo's environment |
| `.gitignore` entry accidentally removed | `.git/info/exclude` has the same entries as backup |
| `git stash` in parent | Safe — ignored files are not stashed |
| `git worktree add` | New worktree won't have symlink; `post-checkout` hook warns |
| Fresh clone | Clean, no dev files; run `setup.sh` |
| Project hooks already exist | `setup.sh` warns rather than overwriting |

## Testing

- Verify `setup.sh` is idempotent (run twice, same result)
- Verify `setup.sh` handles edge cases (existing dir, broken symlink)
- Verify `_dev/` symlink resolves correctly from project root
- Verify `CLAUDE.local.md` symlink works with Claude Code
- Verify `_dev/` files persist across parent branch switches
- Verify `_dev/` scripts run correctly (project imports still work)
- Verify `_dev` does not appear in `git status` on parent repo
- Verify `git clean -fdx` removes symlink but not target
- Verify `post-commit` hook fires and detects notes changes
- Verify `post-checkout` hook detects missing symlink
- Verify `git notes-save` commits and pushes
- Create a test feature branch and confirm zero dev files
