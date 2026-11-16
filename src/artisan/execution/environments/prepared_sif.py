"""Derive exact SIF bytes from an OCI filesystem without Docker image-save."""

from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
    file_digest,
)
from artisan.execution.environments.native import (
    _capture_current_source,
    native_platform,
    wheel_builders_match,
)
from artisan.execution.environments.prepared_oci import verify_oci_environment
from artisan.execution.environments.source import read_environment_inputs
from artisan.schemas.execution.container_environment import (
    OCIDerivation,
    PreparedOCIEnvironment,
    PreparedSIFEnvironment,
)
from artisan.schemas.operation_config.environment import Environment
from artisan.utils.hashing import canonical_json_bytes

SIF_CONVERSION_PROFILE = "source-rootfs-labels-v1"


def prepare_sif_environment(
    oci: PreparedOCIEnvironment,
    *,
    cache_root: Path,
    docker: DockerRuntime,
    apptainer: ApptainerRuntime,
) -> PreparedSIFEnvironment:
    """Convert an immutable prepared OCI through an explicitly identified archive."""
    verify_oci_environment(oci, docker=docker)
    runtime = apptainer.identity()
    key = hashlib.sha256(
        canonical_json_bytes(
            {
                "oci": oci.digest,
                "builder": runtime.model_dump(mode="json"),
                "conversion": SIF_CONVERSION_PROFILE,
            }
        )
    ).hexdigest()
    directory = cache_root.resolve() / "sif-v2" / key
    binding = directory / "binding.json"
    if binding.exists():
        target = PreparedSIFEnvironment.model_validate_json(binding.read_bytes())
        if (
            target.oci != oci
            or target.build_runtime != runtime
            or target.sif != directory / "environment.sif"
        ):
            msg = "Prepared SIF binding disagrees with requested source, builder or locator"
            raise ValueError(msg)
        verify_sif_environment(target, apptainer=apptainer)
        return target
    directory.mkdir(parents=True, exist_ok=False)
    try:
        with tempfile.TemporaryDirectory(
            prefix="sif-convert-", dir=cache_root
        ) as temporary:
            scratch = Path(temporary)
            derivation = _export_archive(docker, oci.image_id, scratch)
            for name in ("cache", "tmp", "config"):
                (scratch / name).mkdir()
            environment = {
                "PATH": os.defpath,
                "HOME": str(scratch),
                "APPTAINER_CACHEDIR": str(scratch / "cache"),
                "APPTAINER_TMPDIR": str(scratch / "tmp"),
                "APPTAINER_CONFIGDIR": str(scratch / "config"),
            }
            sif = directory / "environment.sif"
            subprocess.run(
                [
                    str(apptainer.executable),
                    "build",
                    "--disable-cache",
                    str(sif),
                    f"docker-archive://{scratch / 'image.tar'}",
                ],
                env=environment,
                check=True,
                timeout=1200,
            )
            target = PreparedSIFEnvironment(
                oci=oci,
                derivation=derivation,
                sif=sif,
                sha256=file_digest(sif),
                build_runtime=runtime,
            )
            _verify_sif_metadata(target, apptainer=apptainer)
            temporary_binding = directory / "binding.json.tmp"
            temporary_binding.write_text(target.model_dump_json(indent=2))
            temporary_binding.replace(binding)
            return target
    except Exception:
        shutil.rmtree(directory)
        raise


def lookup_sif_environment(
    oci: PreparedOCIEnvironment,
    *,
    cache_root: Path,
    apptainer: ApptainerRuntime,
) -> PreparedSIFEnvironment:
    """Find an exact derived SIF without conversion or image provisioning."""
    for binding in sorted(cache_root.glob("sif-v2/*/binding.json")):
        target = PreparedSIFEnvironment.model_validate_json(binding.read_bytes())
        if target.oci.digest == oci.digest:
            verify_sif_environment(target, apptainer=apptainer)
            return target
    msg = "No compatible prepared SIF; prepare it explicitly"
    raise ValueError(msg)


def lookup_sif_source_environment(
    declaration: Environment,
    *,
    cache_root: Path,
    apptainer: ApptainerRuntime,
) -> PreparedSIFEnvironment:
    """Resolve a source-bound SIF on a node that need not have Docker installed."""
    inputs = read_environment_inputs(declaration, platform=native_platform())
    matches = []
    for binding in sorted(cache_root.glob("sif-v2/*/binding.json")):
        target = PreparedSIFEnvironment.model_validate_json(binding.read_bytes())
        source = target.oci.native.source
        if (
            target.oci.native.platform != native_platform()
            or source.name != declaration.name
            or not wheel_builders_match(target.oci.native)
        ):
            continue
        try:
            captured = _capture_current_source(inputs, source)
        except (ValueError, FileNotFoundError):
            continue
        if captured.source == source:
            verify_sif_environment(target, apptainer=apptainer)
            matches.append(target)
    if len(matches) != 1:
        msg = "Expected one compatible prepared SIF; prepare an unambiguous target explicitly"
        raise ValueError(msg)
    return matches[0]


def verify_sif_environment(
    target: PreparedSIFEnvironment, *, apptainer: ApptainerRuntime
) -> None:
    """Require the selected completion record and managed SIF without auditing bytes."""
    target = PreparedSIFEnvironment.model_validate(target)
    if target.sif.is_symlink() or not target.sif.is_file():
        msg = "Prepared SIF is unavailable; prepare it explicitly"
        raise ValueError(msg)
    binding = target.sif.parent / "binding.json"
    if binding.is_symlink() or not binding.is_file():
        msg = "Prepared SIF completion record is missing; prepare it explicitly"
        raise ValueError(msg)
    completed = PreparedSIFEnvironment.model_validate_json(binding.read_bytes())
    if completed != target:
        msg = "Prepared SIF completion record differs from the selected target"
        raise ValueError(msg)
    _verify_sif_metadata(target, apptainer=apptainer)


def _verify_sif_metadata(
    target: PreparedSIFEnvironment, *, apptainer: ApptainerRuntime
) -> None:
    """Check the declared source labels without reading the installed filesystem."""
    inspected = json.loads(
        subprocess.check_output(
            [str(apptainer.executable), "inspect", "--json", str(target.sif)],
            text=True,
            timeout=30,
        )
    )
    expected = {
        "org.artisan.prepared.payload": target.oci.payload_digest,
        **_source_labels(
            target.derivation.source_image_id,
            target.derivation.source_config_sha256,
            target.derivation.rootfs_sha256,
        ),
    }
    labels = inspected["data"]["attributes"]["labels"]
    if any(labels.get(name) != value for name, value in expected.items()):
        msg = "SIF preparation evidence differs from its immutable image metadata"
        raise ValueError(msg)


def _export_archive(docker: DockerRuntime, image: str, scratch: Path) -> OCIDerivation:
    metadata = docker.inspect_image(image)
    if metadata["Config"].get("Volumes"):
        msg = "SIF derivation does not support implicit OCI volumes"
        raise ValueError(msg)
    container = docker.output(["create", "--entrypoint", "/bin/true", image]).strip()
    try:
        subprocess.run(
            [
                *docker.command,
                "export",
                "--output",
                str(scratch / "rootfs.tar"),
                container,
            ],
            check=True,
            timeout=600,
        )
    finally:
        subprocess.run(
            [*docker.command, "rm", "-v", container],
            check=True,
            capture_output=True,
            timeout=30,
        )
    rootfs = file_digest(scratch / "rootfs.tar")
    source_config = hashlib.sha256(canonical_json_bytes(metadata["Config"])).hexdigest()
    derived_config = {
        **metadata["Config"],
        "Labels": {
            **(metadata["Config"].get("Labels") or {}),
            **_source_labels(image, source_config, rootfs),
        },
    }
    config = canonical_json_bytes(
        {
            "architecture": metadata["Architecture"],
            "os": metadata["Os"],
            "config": derived_config,
            "rootfs": {"type": "layers", "diff_ids": [f"sha256:{rootfs}"]},
            "history": [{"created_by": "artisan immutable OCI filesystem derivation"}],
        }
    )
    flattened = hashlib.sha256(config).hexdigest()
    manifest = canonical_json_bytes(
        [
            {
                "Config": f"{flattened}.json",
                "RepoTags": ["artisan-environment:derived"],
                "Layers": ["rootfs.tar"],
            }
        ]
    )
    with tarfile.open(scratch / "image.tar", "w") as archive:
        archive.add(scratch / "rootfs.tar", arcname="rootfs.tar")
        for name, content in (
            (f"{flattened}.json", config),
            ("manifest.json", manifest),
        ):
            info = tarfile.TarInfo(name)
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
    return OCIDerivation(
        source_image_id=image,
        source_config_sha256=source_config,
        rootfs_sha256=rootfs,
        flattened_image_id=f"sha256:{flattened}",
        archive_sha256=file_digest(scratch / "image.tar"),
    )


def _source_labels(image: str, source_config: str, rootfs: str) -> dict[str, str]:
    return {
        "org.artisan.oci.source": image,
        "org.artisan.oci.config": source_config,
        "org.artisan.oci.rootfs": rootfs,
    }
