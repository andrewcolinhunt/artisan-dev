"""Small JSON bridge executed only by the separately prepared Hatch interpreter."""

from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path


def main() -> None:
    """Select static wheel inputs or build from an already captured snapshot."""
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("select", "build"))
    parser.add_argument("root", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    builder = importlib.import_module("hatchling.builders.wheel").WheelBuilder(
        str(args.root)
    )
    if args.action == "select":
        result = {
            "files": [
                {
                    "source": Path(file.path).relative_to(args.root).as_posix(),
                    "target": file.distribution_path,
                }
                for file in builder.recurse_included_files()
            ],
            "license_files": list(builder.metadata.core.license_files),
        }
    else:
        if args.output is None:
            parser.error("build requires --output")
        wheels = list(builder.build(directory=str(args.output), versions=["standard"]))
        result = {"wheels": wheels}
    print(json.dumps(result))  # noqa: T201


if __name__ == "__main__":
    main()
