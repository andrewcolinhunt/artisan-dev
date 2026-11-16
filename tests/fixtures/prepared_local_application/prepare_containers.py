"""Explicitly prepare one native worker and two OCI/SIF tools from one manifest."""

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
from artisan.execution.environments.native import prepare_native_environment
from artisan.execution.environments.prepared_oci import prepare_oci_environment
from artisan.execution.environments.prepared_sif import prepare_sif_environment
from artisan.schemas.operation_config.environment import Environment


def main() -> None:
    """Build in an existing captured profile using only explicit owned locations."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--base-image", required=True)
    parser.add_argument("--apptainer", type=Path, required=True)
    args = parser.parse_args()
    root = args.profile.resolve()
    for name in ("docker-config", "buildx-config", "tmp"):
        (root / name).mkdir(exist_ok=True)
    os.environ.update(
        DOCKER_CONFIG=str(root / "docker-config"),
        BUILDX_CONFIG=str(root / "buildx-config"),
        TMPDIR=str(root / "tmp"),
    )
    manifest = root / "source/pixi.toml"
    prepare_command_witness(root / "targets")
    worker = prepare_native_environment(
        Environment.from_pixi(manifest, name="worker"), cache_root=root / "targets"
    )
    (root / "worker-binding.json").write_text(worker.model_dump_json(indent=2))
    print(
        json.dumps({"worker": worker.digest, "prefix": str(worker.prefix)}), flush=True
    )
    docker = DockerRuntime(Path("/usr/bin/docker"))
    apptainer = ApptainerRuntime(args.apptainer)
    for name in ("tool-a", "tool-b"):
        oci = prepare_oci_environment(
            Environment.from_pixi(manifest, name=name),
            cache_root=root / "targets",
            docker=docker,
            base_image=args.base_image,
        )
        (root / f"{name}-oci.json").write_text(oci.model_dump_json(indent=2))
        sif = prepare_sif_environment(
            oci, cache_root=root / "targets", docker=docker, apptainer=apptainer
        )
        (root / f"{name}-sif.json").write_text(sif.model_dump_json(indent=2))
        print(
            json.dumps({"tool": name, "oci": oci.digest, "sif": sif.digest}), flush=True
        )


if __name__ == "__main__":
    main()
