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

## Push Policy

Never push without asking the user first.
