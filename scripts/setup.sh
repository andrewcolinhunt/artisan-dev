#!/usr/bin/env bash
#
# Prepare Graphviz in the selected Pixi environment after installation.
# Invoked by `pixi run --locked setup`; safe to rerun.

set -euo pipefail

# Graphviz: register layout plugins. Conda-forge ships a post-link script for
# this, but pixi skips post-link scripts by default, so without `dot -c` the
# `config8` plugin registry is missing and `dot` fails with "no layout engine
# support for 'dot'".
if [ ! -f "$CONDA_PREFIX/lib/graphviz/config8" ]; then
    echo "setup: registering graphviz layout plugins..."
    dot -c >/dev/null
fi

echo "setup: Graphviz ready."
