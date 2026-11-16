"""Prepare the canonical synthetic tool as OCI and derive its SIF bytes."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from artisan.execution.environments.command_witness import prepare_command_witness
from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
)
from artisan.execution.environments.prepared_oci import prepare_oci_environment
from artisan.execution.environments.prepared_sif import prepare_sif_environment
from artisan.schemas.operation_config.environment import Environment


def main() -> None:
    """Use an existing base image and explicitly installed container launchers."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--base-image", required=True)
    parser.add_argument("--apptainer", type=Path, required=True)
    args = parser.parse_args()
    root = args.cache_root.resolve()
    for name in ("docker-config", "buildx-config", "tmp"):
        (root / name).mkdir(parents=True, exist_ok=True)
    os.environ.update(
        DOCKER_CONFIG=str(root / "docker-config"),
        BUILDX_CONFIG=str(root / "buildx-config"),
        TMPDIR=str(root / "tmp"),
    )
    source = Environment.from_pixi(Path(__file__).parent / "source/pyproject.toml")
    docker = DockerRuntime(Path("/usr/bin/docker"))
    prepare_command_witness(root)
    oci = prepare_oci_environment(
        source, cache_root=root, docker=docker, base_image=args.base_image
    )
    sif = prepare_sif_environment(
        oci, cache_root=root, docker=docker, apptainer=ApptainerRuntime(args.apptainer)
    )
    print(
        json.dumps(
            {
                "oci": oci.digest,
                "image": oci.image_id,
                "sif": sif.digest,
                "sif_path": str(sif.sif),
            }
        )
    )


if __name__ == "__main__":
    main()
