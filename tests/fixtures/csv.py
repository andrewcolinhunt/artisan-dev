"""CSV test data generators and fixture paths."""

from __future__ import annotations

import csv
import io
import random
from pathlib import Path

CSV_DIR: Path = Path(__file__).parent / "csv"
"""Directory containing static CSV fixture files."""


def make_csv(rows: int = 5, seed: int = 42) -> bytes:
    """Generate test CSV content with id, x, y, z, score columns.

    Args:
        rows: Number of data rows.
        seed: Random seed for reproducibility.

    Returns:
        CSV content as bytes.
    """
    rng = random.Random(seed)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "x", "y", "z", "score"])
    for i in range(rows):
        writer.writerow(
            [
                i,
                round(rng.uniform(0.0, 10.0), 4),
                round(rng.uniform(0.0, 10.0), 4),
                round(rng.uniform(0.0, 10.0), 4),
                round(rng.uniform(0.0, 1.0), 4),
            ]
        )
    return buf.getvalue().encode("utf-8")
