#!/usr/bin/env bash

set -euo pipefail

# Pixi skips the conda-forge post-link script that registers Graphviz layout plugins.
if [ ! -f "$CONDA_PREFIX/lib/graphviz/config8" ]; then
    echo "setup: registering graphviz layout plugins..."
    dot -c >/dev/null
fi

echo "setup: Graphviz ready."
