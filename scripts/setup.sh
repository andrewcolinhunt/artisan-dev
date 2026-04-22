#!/usr/bin/env bash
#
# One-time post-install fixups. Invoked by `pixi run setup` once after
# cloning. Idempotent: rerunning is cheap and safe.
#
# Add more steps below as the project accretes post-install concerns.
# Keep each step guarded so the script is safe to rerun.

set -euo pipefail

# Graphviz: register layout plugins. Conda-forge ships a post-link script for
# this, but pixi skips post-link scripts by default, so without `dot -c` the
# `config8` plugin registry is missing and `dot` fails with "no layout engine
# support for 'dot'".
if [ ! -f "$CONDA_PREFIX/lib/graphviz/config8" ]; then
    echo "setup: registering graphviz layout plugins..."
    dot -c >/dev/null
fi

# Pre-commit hooks: auto-install when pre-commit is on PATH (i.e. the user
# ran `pixi run -e dev setup`). The default env does not include pre-commit,
# so contributors who only ran `pixi run setup` without `-e dev` get the
# graphviz fix but no hooks — they can opt in later by running the dev-env
# setup. The full hook suite passes on the tree as of Phase 4 of the
# pre-commit backlog cleanup.
if command -v pre-commit >/dev/null 2>&1; then
    echo "setup: installing pre-commit hooks..."
    pre-commit install >/dev/null
fi

echo "setup: done."
