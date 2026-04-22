#!/usr/bin/env bash
#
# One-time post-install fixups. Invoked by `pixi run setup` once after
# cloning. Idempotent: rerunning is cheap and safe.
#
# Add more steps below as the project accretes post-install concerns.
# Keep each step guarded so the script is safe to rerun.
#
# NB: pre-commit hook installation is intentionally NOT wired here yet.
# See _dev/design/0_current/precommit-state-and-reinstatement.md for the
# backlog of hook-failures that must be cleared before `pre-commit install`
# can be auto-run without breaking contributor commits.

set -euo pipefail

# Graphviz: register layout plugins. Conda-forge ships a post-link script for
# this, but pixi skips post-link scripts by default, so without `dot -c` the
# `config8` plugin registry is missing and `dot` fails with "no layout engine
# support for 'dot'".
if [ ! -f "$CONDA_PREFIX/lib/graphviz/config8" ]; then
    echo "setup: registering graphviz layout plugins..."
    dot -c >/dev/null
fi

echo "setup: done."
