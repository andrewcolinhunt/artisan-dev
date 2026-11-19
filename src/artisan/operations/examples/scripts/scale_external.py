"""Read an explicitly selected scalar checkpoint and write one example result."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    """Run the installed synthetic tool without acquiring or copying weights."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--value", required=True, type=float)
    parser.add_argument("--weights", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    scale = float(args.weights.read_text())
    args.output.write_text(json.dumps({"value": args.value * scale}))


if __name__ == "__main__":
    main()
