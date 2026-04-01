# CLAUDE.local.md — Dev Overrides

**This file is tracked on `ach/dev` and stripped on `ach/dev-clean` before PRs.**

---

## Repo Layout (Three Repos)

| Directory          | Remote   | URL                                                  | Role             |
| ------------------ | -------- | ---------------------------------------------------- | ---------------- |
| `~/git/artisan`    | origin   | `git@github.com:dexterity-systems/artisan.git`       | Public upstream   |
| `~/git/artisan-dev`| origin   | `https://github.com/andrewcolinhunt/artisan-dev.git` | Private dev fork  |
|                    | upstream | `git@github.com:dexterity-systems/artisan.git`       | Public upstream   |

## Branch Workflow

| Branch           | Lifecycle  | Purpose                                          |
| ---------------- | ---------- | ------------------------------------------------ |
| `ach/dev`        | Long-lived | All development. Default branch. May have dev-only files. |
| `ach/dev-clean`  | Ephemeral  | Created from `ach/dev`, dev files stripped, PR to `main`. |
| `main`           | Stable     | Production-ready. Synced with upstream.          |

**Default branch is `ach/dev`**, not `main`.

## Dev-Only Files

These exist on `ach/dev` but are removed on `ach/dev-clean` before PRs:

- `CLAUDE.local.md`
- `_dev/`

## Pre-PR Decontamination

Before pushing `ach/dev-clean`, use the `/dev-branch-workflow` skill which
includes a decontamination step to scrub user-specific strings (`ach94`,
`andrewhunt`, hardcoded local paths) from the codebase.

## Push Policy

Never push without asking the user first.

## Maintainability

Do not include step numbers or anything else not easily maintainable. If you
number things and those things change, the numbers are off.

---

## MCP Tools

Use **Context7** proactively (without user prompting) when working with external
libraries. Resolve the library ID first, then query docs.

---

## Git Workflow

- Always branch from `ach/dev` before making changes (not `main`)
- Never commit directly to `ach/dev` or `main`
- Commit regularly (after each logical step), not just at the end
- Work isn't done until it's committed
- Never push to origin automatically — always ask the user first

---

## Workflow Sizing

| Size          | Workflow    | Criteria                              |
| ------------- | ----------- | ------------------------------------- |
| Trivial/Small | Lightweight | Single file, no API change, confident |
| Medium        | Standard    | Multi-file, new tests needed          |
| Large         | Full        | New feature, architectural change     |

### Lightweight

Branch, change, verify, commit, then ask user: merge to `ach/dev` or create PR?

### Standard/Full

**Phase 1 — Plan:**

- Understand the request; ask clarifying questions
- Survey codebase for patterns and prior art
- Create design doc at `_dev/design/[name].md` (for medium+ features)

**Phase 2 — Implement:**

- Branch from `ach/dev`
- Implement incrementally with regular commits
- Write tests (happy path, edge cases, errors)
- Create demo in `_dev/demos/` if applicable
- Update docstrings and docs

**Phase 3 — QA:**

- Run all checks (all commands require `~/.pixi/bin/pixi run` prefix, tests
  require `-e dev`): `-e dev fmt` → `-e dev test` → `-e docs docs-build`
- Self-review, then ask user: merge to `ach/dev` or create PR?

---

## Design & Analysis Docs

- Design: `_dev/design/[name].md`
- Analysis: `_dev/analysis/[name].md`

## Demos

Demos live in `_dev/demos/`. Each demo directory contains `demo_*.py`
(executable script) and `README.md`.

```bash
~/.pixi/bin/pixi run python _dev/demos/<demo-dir>/demo_<name>.py
```
