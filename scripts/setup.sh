#!/usr/bin/env bash
#
# One-time post-install fixups. Invoked by `pixi run setup` (or
# `pixi run -e dev setup` for the dev-env extras). Idempotent: rerunning
# is cheap and safe.
#
# Add more steps below as the project accretes post-install concerns.
# Keep each step guarded so the script is safe to rerun and doesn't hard-fail
# when an optional tool isn't present (e.g. pre-commit only ships in the
# `dev` feature).

set -euo pipefail

# Graphviz: register layout plugins. Conda-forge ships a post-link script for
# this, but pixi skips post-link scripts by default, so without `dot -c` the
# `config8` plugin registry is missing and `dot` fails with "no layout engine
# support for 'dot'".
if [ ! -f "$CONDA_PREFIX/lib/graphviz/config8" ]; then
    echo "setup: registering graphviz layout plugins..."
    dot -c >/dev/null
fi

# pre-commit: wire this repo's .pre-commit-config.yaml into .git/hooks. Only
# runs if pre-commit is on PATH — it's in the `dev` feature deps, so this
# skips silently in the `default` env.
if command -v pre-commit >/dev/null 2>&1; then
    echo "setup: installing pre-commit git hooks..."
    pre-commit install
fi

echo "setup: done."
