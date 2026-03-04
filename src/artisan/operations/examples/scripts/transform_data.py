#!/usr/bin/env python3
"""Apply scaling and noise to a CSV file based on a JSON config.

Standalone script invoked by DataTransformerScript. Reads config with
keys ``input``, ``scale_factor``, ``noise_amplitude``, and ``seed``.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from pathlib import Path


def main() -> None:
    """Parse arguments, read config, and write the transformed CSV."""
    parser = argparse.ArgumentParser(description="Transform CSV data")
    parser.add_argument("--config", required=True, type=Path, help="Config JSON file")
    parser.add_argument("--output-dir", required=True, type=Path, help="Output directory")
    parser.add_argument("--output-basename", type=str, help="Base name for output files")
    args = parser.parse_args()

    config = json.loads(args.config.read_text())
    input_path = Path(config["input"])
    output_basename = args.output_basename or config.get("output_basename") or input_path.stem
    scale_factor = config.get("scale_factor", 1.0)
    noise_amplitude = config.get("noise_amplitude", 0.0)
    seed = config.get("seed")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    numeric_cols = {"x", "y", "z", "score"}

    with input_path.open() as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        rows = list(reader)

    output_path = args.output_dir / f"{output_basename}_variant_0.csv"
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            new_row = dict(row)
            for col in headers:
                if col in numeric_cols:
                    val = float(row[col])
                    val *= scale_factor
                    if noise_amplitude > 0:
                        val += rng.uniform(-noise_amplitude, noise_amplitude)
                    new_row[col] = round(val, 4)
            writer.writerow(new_row)

    print(f"Written: {output_path}")  # noqa: T201


if __name__ == "__main__":
    main()
