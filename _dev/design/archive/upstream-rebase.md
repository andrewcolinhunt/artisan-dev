# Design: Merge upstream/main

## Goal

Merge `upstream/main` into both `main` and `ach/dev` so that a PR from
`origin/main` → `upstream/main` can auto-merge with no conflicts.

## Current State

- **Merge base**: `28d2802` (the initial push to dexterity-systems/artisan)
- **`upstream/main`** (`46dc236`): 18 commits ahead of merge base
- **`origin/main`** (`ba39a88`): ~150 commits ahead of merge base (PR #4 with composites)

## Conflict Surface

Only **2 files** are modified by both sides: `pyproject.toml` and `pixi.lock`.

No conflicts in `src/`, `tests/`, or `docs/` — upstream touched none of those.

### Upstream-only changes (14 files)

**New files (10):**
- `.github/ISSUE_TEMPLATE/bug_report.yml`
- `.github/ISSUE_TEMPLATE/config.yml`
- `.github/ISSUE_TEMPLATE/feature_request.yml`
- `.github/SECURITY.md`
- `.github/pull_request_template.md`
- `.github/workflows/release.yml`
- `CHANGELOG.md`
- `CODE_OF_CONDUCT.md`
- `CONTRIBUTING.md`
- `recipe/meta.yaml`

**Modified files (4):**
- `.github/workflows/ci.yaml`
- `.readthedocs.yaml`
- `pixi.lock`
- `pyproject.toml`

### pyproject.toml: what each side changed

**Upstream changed:**
1. Renamed project `artisan` → `dexterity-artisan`
2. Bumped version `0.1.0` → `0.1.1`
3. Renamed pixi editable dep `artisan` → `dexterity-artisan`
4. Added packaging deps (`build`, `twine`)
5. Added PyPI upload tasks (`build-dist`, `check-dist`, `upload-testpypi`, `upload-pypi`)

**We changed:**
1. Pinned `prefect-submitit = ">=0.1.4"` (was `"*"`)
2. Added `[tool.pixi.activation]` section
3. Changed prefect tasks to use `prefect-server` CLI
4. Added `pythonpath = ["tests"]` to pytest config

**These changes do not overlap** — they touch different sections of the file.

### pixi.lock

Both sides modified this as a consequence of `pyproject.toml` changes. After
resolving `pyproject.toml`, `pixi.lock` must be regenerated.

## Plan

### Step 0: Safety tags

```bash
git tag pre-upstream-merge main
git tag pre-upstream-merge-dev ach/dev
```

If anything goes wrong, reset back to these tags.

### Step 1: Pre-merge fixups on main (before merging upstream)

Apply upstream's `pyproject.toml` changes to our copy so the merge is clean:

1. Rename project `artisan` → `dexterity-artisan`
2. Bump version `0.1.0` → `0.1.1`
3. Rename pixi editable dep `artisan` → `dexterity-artisan`
4. Add packaging deps (`build`, `twine`)
5. Add PyPI upload tasks

Commit this on `main`.

### Step 2: Merge upstream/main into main

```bash
git checkout main
git merge upstream/main
```

Since our `pyproject.toml` now contains all of upstream's changes, the
merge should auto-resolve. For `pixi.lock`, accept either side — it
will be regenerated in the next step.

### Step 3: Regenerate pixi.lock

```bash
~/.pixi/bin/pixi install
```

This regenerates `pixi.lock` with both our new deps and upstream's
rename. Commit the updated lock file.

### Step 4: Verify main

- `git merge-base --is-ancestor upstream/main main` → true
- `git diff upstream/main..main` shows only our changes, no upstream diffs
- No files in `src/`, `tests/`, or `docs/` were touched during merge
- `~/.pixi/bin/pixi run -e dev test-unit` passes

### Step 5: Push main

```bash
git push origin main --force-with-lease
```

### Step 6: Update ach/dev

`ach/dev` = `main` + dev-only files (`_dev/` and `CLAUDE.local.md`).
Note: `skills/` and `.claude-plugin/` already exist on `origin/main`
and survive the reset — they do not need to be restored.

```bash
git checkout ach/dev
git reset --hard main
git checkout ach/dev@{1} -- _dev/ CLAUDE.local.md
git commit -m "dev: restore dev-only files after upstream merge"
git push origin ach/dev --force-with-lease
```

Using `ach/dev@{1}` (the pre-reset reflog entry) ensures all current
dev files are restored, including any added after earlier commits.

### Step 7: Verify ach/dev

- `git diff main..ach/dev --name-only` shows only `_dev/` and `CLAUDE.local.md`
- No `src/`, `tests/`, or `docs/` differences between main and ach/dev

## Risk Assessment

- **Low risk**: no changes to `src/`, `tests/`, or `docs/` — merge is
  purely top-level config files
- **pixi.lock** is the only file that might need regeneration, and that's
  deterministic
- Both branches end up with identical source code, differing only in
  dev-only files (`_dev/`, `CLAUDE.local.md`)
- Safety tags allow instant rollback if anything goes wrong
