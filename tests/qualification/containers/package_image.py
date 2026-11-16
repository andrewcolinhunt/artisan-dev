"""Package a prefix installed at its final container path into a derived image."""

from __future__ import annotations

import argparse
import json
import tarfile
from pathlib import Path


def package(prepared: Path, qualification: Path, output: Path, base_image: str) -> None:
    """Generate a compressed build context without making another prefix copy."""
    output.mkdir(parents=True, exist_ok=False)
    with tarfile.open(output / "payload.tar.xz", "w:xz", preset=3) as archive:
        archive.add(prepared, arcname="opt/provider-probe")
        for path in sorted(qualification.rglob("*.py")):
            if "__pycache__" not in path.parts:
                archive.add(
                    path,
                    arcname=str(
                        Path("opt/qualification") / path.relative_to(qualification)
                    ),
                )
    dockerfile = (
        f"FROM {base_image}\n"
        "ADD payload.tar.xz /\n"
        'ENV PATH="/opt/provider-probe/.pixi/envs/worker/bin:/usr/local/bin:/usr/bin:/bin"\n'
        'ENTRYPOINT ["/opt/provider-probe/.pixi/envs/worker/bin/python"]\n'
        "CMD []\n"
    )
    (output / "Dockerfile").write_text(dockerfile)
    (output / "recipe.json").write_text(
        json.dumps(
            {
                "base_image": base_image,
                "prefix": "/opt/provider-probe",
                "payload_bytes": (output / "payload.tar.xz").stat().st_size,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("prepared", type=Path)
    parser.add_argument("qualification", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--base-image", required=True)
    args = parser.parse_args()
    package(args.prepared, args.qualification, args.output, args.base_image)
