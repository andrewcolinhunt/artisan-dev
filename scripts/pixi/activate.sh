#!/bin/bash
# Pixi activation script — runs on every `pixi run` / `pixi shell`.
# Keep idempotent and fast.

# Register Graphviz layout plugins (dot, neato, etc.).
# Without this, `dot` fails with "no layout engine support" after install.
if command -v dot &>/dev/null; then
    dot -c 2>/dev/null
fi
