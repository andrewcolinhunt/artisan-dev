# Clean Stacked PRs for release/v0.1.2a5

## Problem

We have 5 stacked feature branches that need to PR into
`release/v0.1.2a5`. All branches carry `_dev/` and `CLAUDE.local.md`
(dev-only files) because they were branched from `ach/dev`.

We tried two approaches to clean them:

- **Attempt 1 — only clean the bottom branch.** Dev files appeared in
  incremental PR diffs between dirty branches (PRs 1 and 3 had `_dev/`
  noise). Dev files would also cascade to `release/v0.1.2a5` on merge.

- **Attempt 2 — clean every branch independently.** Each branch got a
  `-clean` variant with dev files stripped. But the clean branches don't
  share ancestry — GitHub computes diffs from the merge-base (which is
  the dirty branch), so stripped files still show as deletions.

The root cause: independently-created clean branches diverge at the
dirty branch tips, not at each other. GitHub's three-dot diff sees files
at the merge-base that don't exist on the head, and shows them as deleted.

## Solution

Build the clean stack as a **chain** where each clean branch descends
from the previous clean branch. This makes the merge-base between
adjacent clean branches the lower clean branch itself — dev files are
already absent on both sides.

### Method: `git rebase --onto`

For each layer (bottom-up), replay the incremental commits onto the
previous clean branch:

```
release/v0.1.2a5
  └── feat/artifact-id-materialization-clean       (strip from dirty)
       └── feat/external-content-artifacts-clean    (rebase onto above)
            └── feat/post-step-sugar-clean          (rebase onto above)
                 └── feat/files-dir-threading-clean (rebase onto above)
                      └── feat/external-artifact-examples-clean (rebase onto above)
```

```bash
# Bottom: already exists — strip dev files from dirty branch
git checkout -b feat/artifact-id-materialization-clean feat/artifact-id-materialization
git rm -rf CLAUDE.local.md _dev/
git commit -m "chore: strip dev-only files for clean PR"

# Each subsequent layer: rebase incremental commits onto previous clean
git rebase --onto feat/artifact-id-materialization-clean \
  feat/artifact-id-materialization \
  feat/external-content-artifacts
# Then rename/create the clean branch from the result

# Repeat up the stack...
```

### Handling conflicts

Some incremental commits touch `_dev/` files (design docs, analysis).
These will conflict during rebase because `_dev/` doesn't exist on the
clean base. Resolution strategy:

- **Commits that only touch `_dev/` files:** skip with `git rebase --skip`
- **Commits that touch both `_dev/` and real code:** resolve by accepting
  the deletion (the `_dev/` parts are gone, keep the real code changes)

### Dealing with existing PRs and branches

Current state on upstream:
- PRs #32-#36 are open, using independently-created clean branches
- Both dirty and clean branches exist on upstream

Steps:
- Close PRs #32-#36
- Delete all `-clean` branches on upstream
- Rebuild the clean stack using the chained rebase method
- Push new clean branches to upstream
- Recreate PRs
- Delete dirty branches from upstream (they're not needed for PRs)

### PR structure (unchanged)

| PR | Head | Base |
|----|------|------|
| PR 1 | `feat/external-artifact-examples-clean` | `feat/files-dir-threading-clean` |
| PR 2 | `feat/files-dir-threading-clean` | `feat/post-step-sugar-clean` |
| PR 3 | `feat/post-step-sugar-clean` | `feat/external-content-artifacts-clean` |
| PR 4 | `feat/external-content-artifacts-clean` | `feat/artifact-id-materialization-clean` |
| PR 5 | `feat/artifact-id-materialization-clean` | `release/v0.1.2a5` |

Merge order: top-down (PR 1 → 2 → 3 → 4 → 5).

## Future prevention

This is a one-time cleanup. Going forward, feature branches will be
created from the clean target (`main` or `release/*`) rather than from
`ach/dev`. Dev files never enter feature branch history, eliminating the
need for clean branches entirely.

See the workflow update (separate doc) for the long-term changes.
